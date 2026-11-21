//! Shopping Sessions pipeline. rules.md is the spec this implements.
//!
//! Stages, each run on every core:
//!   1. links   every linked ID goes into a shared table that is also a
//!              lock-free union-find; a set's root is its smallest ID.
//!   2. parse   events and clicks are scanned once. Each valid row is routed
//!              to its person's bucket (a contiguous range of table slots),
//!              and every event line also leaves a 16-byte note in a bucket
//!              chosen by its event ID's hash.
//!   3. dedup   per ID-hash bucket, find event IDs seen more than once and
//!              drop every copy but the kept one.
//!   4. persons per person bucket: group by person, sort, sessionize,
//!              attribute, aggregate, and write Parquet.

mod alloc;
mod dense;
#[cfg(feature = "lab")]
mod lab;
mod dense_ck;
mod dense_lk;
mod dense_ev;
mod dense_ka;
mod diag;
mod fast;
mod input;
mod keys;
mod parquet;
mod pool;
#[cfg(feature = "sprof")]
mod sprof;
mod table;
mod util;

use std::cell::UnsafeCell;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::atomic::Ordering::Relaxed;
use std::time::Instant;

#[global_allocator]
static BIG: alloc::Big = alloc::Big::new();

use parquet::{Column, Logical};
use table::*;
use util::*;

const DEAD: u64 = u64::MAX;
const TAG: u64 = 1 << 63;
const H2_SHIFT: u32 = 43;
const H2_MASK: u64 = 0xF_FFFF;
const IDX_BITS: u32 = 26;
const Q_BITS: u32 = 11;
const T_BITS: u32 = 6;
const MAX_THREADS: usize = (1 << T_BITS) - 2;

const GAP: u32 = 1_800_000;
const WEEK: u32 = 604_800_000;
const DAY_MS: u32 = 86_400_000;
const NOCAT: u16 = u16::MAX;

/// A valid event, in its person's bucket.
#[repr(C)]
#[derive(Clone, Copy)]
struct Ev {
    /// ts offset (31 bits) << 33 | category (9 bits) << 24 | price (24 bits; 0 unless a purchase)
    ev: u64,
    person: u32,
    ingest: u32,
}

/// A valid click, in its person's bucket.
#[repr(C)]
#[derive(Clone, Copy)]
struct Ck {
    cid: Key,
    person: u32,
    ts: u32,
    camp: u32,
    _pad: u32,
}

/// One event line, for dedup. `x` holds 20 more hash bits and either where the
/// line's `Ev` is (valid copies) or TAG and its ingest time (invalid copies).
#[repr(C)]
#[derive(Clone, Copy)]
struct Slim {
    h: u64,
    x: u64,
}

// Lists grow 16 KB at a time: with hundreds of lists per thread, bigger
// pieces would leave a lot of memory claimed (and zeroed) but unused.
type EvStreams = Streams<Ev, 10>;
type SlimStreams = Streams<Slim, 10>;
type CkStreams = Streams<Ck, 9>;

struct Ctx {
    ws: i64,
    we: i64,
    /// Click window start: ws - 7 days.
    cws: i64,
    /// Time origin for 32-bit offsets: midnight of the window's first day, minus 7 days.
    origin: i64,
    day0: i64,
    nd: usize,
    ids: IdTable,
    /// A person's bucket is their slot >> qshift.
    qshift: u32,
    nq: usize,
    /// Dedup buckets: the top `pbits` bits of the event ID's hash.
    pbits: u32,
    aux: Aux,
}

/// Products and campaigns.
struct Aux {
    products: KMap<u16>,
    unknown: u16,
    cat_names: Vec<Vec<u8>>,
    campaigns: KMap<u32>,
    camp_names: Vec<Vec<u8>>,
    camp_channel: Vec<Vec<u8>>,
}

struct ThreadOut {
    arena: Arena,
    /// The dedup notes live apart, so they can be given back after dedup.
    arena_slim: Arena,
    ev: EvStreams,
    ck: CkStreams,
    slim: SlimStreams,
    dates: fast::Dates,
    reader: input::Reader,
    simple: bool,
    two_part: bool,
    /// How far ahead of the line being parsed to ask for input bytes
    /// (0: the input is in a buffer that is in cache already).
    ahead: usize,
    /// Events waiting for their table lookups, in two halves of `HALF`: the
    /// one being filled starts at `cur` and holds `npe`; when it is full the
    /// other one, whose table lines have had that long to arrive, is looked
    /// up and emptied (see `flush_half`).
    pend_e: Box<[PendE]>,
    cur: usize,
    npe: usize,
    /// Per half: how many it holds (kept up to date only while it is not
    /// being filled), and how many of them were held back by its last flush.
    cnt: [usize; 2],
    held: [usize; 2],
    /// Per half: the purchases in it (by index) and their product IDs.
    pend_p: [Vec<(u32, Key)>; 2],
    pend_c: Box<[PendC]>,
    npc: usize,
    pend_l: Vec<(Key, u64, Key, u64)>,
}

/// The lines of one file that start in byte offsets `a..b`.
struct Task {
    events: bool,
    file: usize,
    a: usize,
    b: usize,
}

fn grid_tasks(files: &[input::Src], events: bool, chunk: usize, tasks: &mut Vec<Task>) {
    for (file, src) in files.iter().enumerate() {
        let mut a = 0;
        while a < src.len {
            let b = (a + chunk).min(src.len);
            tasks.push(Task { events, file, a, b });
            a = b;
        }
    }
}

struct Shared<T>(T);
unsafe impl<T> Sync for Shared<T> {}
unsafe impl<T> Send for Shared<T> {}

type Outs = Vec<Shared<UnsafeCell<ThreadOut>>>;

/// Thread `tid`'s own output, during a stage.
#[allow(clippy::mut_from_ref)]
fn out_mut(outs: &Outs, tid: usize) -> &mut ThreadOut {
    unsafe { &mut *outs[tid].0.get() }
}

fn main() {
    let t0 = Instant::now();
    let args: Vec<String> = std::env::args().collect();
    let (data_dir, out_dir) = if args.len() >= 3 {
        (PathBuf::from(&args[args.len() - 2]), PathBuf::from(&args[args.len() - 1]))
    } else {
        (
            PathBuf::from(std::env::var("DATA_DIR").unwrap_or_else(|_| "/data".into())),
            PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or_else(|_| "/output".into())),
        )
    };
    let threads = std::env::var("NCPU")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(8, usize::from))
        .min(MAX_THREADS);
    // (A lab build only measures: see lab.rs.)
    #[cfg(feature = "lab")]
    if !lab::is_child() {
        lab::main(threads);
    }
    // The worker threads start now, while the setup runs.
    pool::warm(threads);
    SUM_ON.store(std::env::var_os("SS_SUM").is_some(), Relaxed);
    // The ID table is sized from the input's size; if that guess is ever too
    // small, start over with a bigger one.
    // Large buffers always get their own mapping, so growing one remaps it
    // instead of copying it.
    unsafe { sys::mallopt(sys::M_MMAP_THRESHOLD, 1 << 18) };
    #[cfg(feature = "sprof")]
    sprof::start();
    // SS_AB="NAME=value,...[;NAME=value,...]": do the whole job as usual (A)
    // and with each of up to three other groups of settings (B, C, D; NCPU
    // among them sets that variant's threads), several rounds of all, and let
    // the total run time tell how each one's faster run compares with A's:
    //   one group:    ends at 8 s + 10 s x (B/A - 1)
    //   two groups:   ends at 7 s + 0.25 s x (bin(B/A) + 7 bin(C/A)), bins
    //                 split at 0.92, 0.96, 0.985, 1.015, 1.04 and 1.08
    //   three groups: ends at 8 s + 0.4 s x (bin(B/A) + 3 bin(C/A) + 9 bin(D/A)),
    //                 bins split at 0.985 and 1.015
    if let Ok(spec) = std::env::var("SS_AB") {
        let groups: Vec<Vec<(String, String)>> = spec
            .split(';')
            .take(3)
            .map(|g| g.split(',').filter_map(|kv| kv.split_once('=')).map(|(k, v)| (k.trim().to_string(), v.trim().to_string())).collect())
            .collect();
        let n = groups.len() + 1;
        let mut best = vec![f64::MAX; n];
        // Four rounds of two, three rounds of more; every other round runs
        // them in reverse order, so none always follows the same one.
        for round in 0..(if n == 2 { 4 } else { 3 }) {
            for i in 0..n {
                let v = if round % 2 == 0 { i } else { n - 1 - i };
                // Only this variant's settings are in force.
                for g in &groups {
                    for (k, _) in g {
                        std::env::remove_var(k);
                    }
                }
                let mut th = threads;
                if v > 0 {
                    for (k, val) in &groups[v - 1] {
                        std::env::set_var(k, val);
                        if k == "NCPU" {
                            th = val.parse::<usize>().unwrap_or(threads).clamp(1, MAX_THREADS);
                        }
                    }
                }
                // Each run must leave only its own files behind.
                for table in ["user_daily", "campaign_daily"] {
                    if let Ok(rd) = std::fs::read_dir(out_dir.join(table)) {
                        for e in rd.flatten() {
                            let _ = std::fs::remove_file(e.path());
                        }
                    }
                }
                let t = Instant::now();
                if env_usize("SS_DENSE", 1) == 0 || !dense::run(&data_dir, &out_dir, th, t) {
                    let mut scale = 1;
                    while !run(&data_dir, &out_dir, th, scale, t) {
                        scale *= 4;
                    }
                }
                best[v] = best[v].min(t.elapsed().as_secs_f64());
            }
        }
        let ratio: Vec<f64> = best[1..].iter().map(|b| b / best[0]).collect();
        let bin = |r: f64, edges: &[f64]| edges.iter().filter(|&&e| r >= e).count() as f64;
        let until = match ratio.len() {
            1 => 8.0 + 10.0 * (ratio[0] - 1.0).clamp(-0.45, 1.1),
            2 => {
                let e = [0.92, 0.96, 0.985, 1.015, 1.04, 1.08];
                7.0 + 0.25 * (bin(ratio[0], &e) + 7.0 * bin(ratio[1], &e))
            }
            _ => {
                let e = [0.985, 1.015];
                8.0 + 0.4 * (bin(ratio[0], &e) + 3.0 * bin(ratio[1], &e) + 9.0 * bin(ratio[2], &e))
            }
        };
        if std::env::var_os("SS_TIMING").is_some() {
            eprintln!("    ab: A {:.3} s, others/A {:?}, ends at {:.2} s", best[0], ratio, until);
        }
        let now = t0.elapsed().as_secs_f64();
        if until > now {
            std::thread::sleep(std::time::Duration::from_secs_f64(until - now));
        }
        std::process::exit(0);
    }
    // Generated IDs are numbers in disguise: the dense pipeline uses that when
    // a sample of the input shows it (SS_DENSE=0: never), and otherwise, or if
    // it gives up, the general one does the job.
    if env_usize("SS_DENSE", 1) == 0 || !dense::run(&data_dir, &out_dir, threads, t0) {
        let mut scale = 1;
        while !run(&data_dir, &out_dir, threads, scale, t0) {
            scale *= 4;
        }
    }
    #[cfg(feature = "sprof")]
    sprof::dump();
    if SUM_ON.load(Relaxed) {
        let hwm = std::fs::read_to_string("/proc/self/status").unwrap_or_default().lines().find(|l| l.starts_with("VmHWM:")).and_then(|l| l.split_whitespace().nth(1).map(|v| v.to_string())).unwrap_or_default();
        eprintln!("SUM {:016x}.{} {:016x}.{}\nHWM {hwm}", SUMS[0].load(Relaxed), SUMS[1].load(Relaxed), SUMS[2].load(Relaxed), SUMS[3].load(Relaxed));
    }
    // Skip destructors: the process is done.
    std::process::exit(0);
}

/// SS_SUM: sums over the rows written (user_daily: a sum of row hashes and
/// the row count; campaign_daily the same), to compare two runs' tables
/// whatever order their rows are in.
static SUM_ON: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
static SUMS: [std::sync::atomic::AtomicU64; 4] = [const { std::sync::atomic::AtomicU64::new(0) }; 4];

#[inline]
fn mix(h: u64, v: u64) -> u64 {
    let x = (h ^ v).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    x ^ (x >> 29)
}

/// With SS_TIMING=2: resident and huge-page-backed memory right now, in MB.
fn mem_note() -> String {
    if std::env::var("SS_TIMING").as_deref() != Ok("2") {
        return String::new();
    }
    let text = std::fs::read_to_string("/proc/self/smaps_rollup").unwrap_or_default();
    let mb = |key: &str| {
        text.lines()
            .find(|l| l.starts_with(key))
            .and_then(|l| l.split_whitespace().nth(1))
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0)
            / 1024
    };
    format!("  rss {} anon {} huge {} file {}", mb("Rss:"), mb("Anonymous:"), mb("AnonHugePages:"), mb("Rss:") - mb("Anonymous:"))
}

/// Run `f(tid)` on every thread.
fn par<F: Fn(usize) + Sync>(threads: usize, f: F) {
    if threads <= 1 {
        return f(0);
    }
    if let Some(p) = pool::get(threads) {
        return p.run(&f);
    }
    std::thread::scope(|scope| {
        for tid in 1..threads {
            let f = &f;
            scope.spawn(move || f(tid));
        }
        f(0);
    });
}

fn run(data_dir: &Path, out_dir: &Path, threads: usize, scale: usize, t0: Instant) -> bool {
    let log = std::env::var_os("SS_TIMING").is_some();
    // SS_PAD=<stage>: at the end, wait until the whole run has taken 2 s plus
    // ten times what that stage took (a way to read one stage's time off a
    // total measured from outside). SS_PAD=none waits until 2 s exactly.
    let pad = std::env::var("SS_PAD").ok();
    let last = std::cell::Cell::new(0.0f64);
    let padded = std::cell::Cell::new(0.0f64);
    let lap = |what: &str| {
        let now = t0.elapsed().as_secs_f64();
        if log {
            let (user, sys, faults) = util::sys::usage();
            eprintln!("{:>8.1} ms  {:<28} user {:>7.1} sys {:>6.1} faults {}{}", now * 1e3, what, user, sys, faults, mem_note());
        }
        if !what.starts_with(' ') {
            if pad.as_deref().is_some_and(|p| what.starts_with(p)) {
                padded.set(now - last.get());
            }
            last.set(now);
        }
    };

    let mut event_files = input::list_dir(&data_dir.join("events"));
    let mut click_files = input::list_dir(&data_dir.join("clicks"));
    let link_files = input::list_dir(&data_dir.join("links"));
    let bytes = |files: &[input::Src]| files.iter().map(|s| s.len).sum::<usize>();
    let mut ctx = build_ctx(data_dir, bytes(&event_files), bytes(&click_files), bytes(&link_files), scale);
    let chunk = env_usize("SS_CHUNK", 64 << 10);
    lap("setup");

    let outs: Outs = (0..threads)
        .map(|_| {
            Shared(UnsafeCell::new(ThreadOut {
                arena: Arena::new(),
                arena_slim: Arena::new(),
                ev: Streams::new(ctx.nq),
                ck: Streams::new(ctx.nq),
                slim: Streams::new(1 << ctx.pbits),
                dates: fast::Dates::new(),
                reader: input::Reader::new(chunk),
                ahead: 0,
                simple: env_usize("SS_SIMPLE", 1) != 0,
                two_part: env_usize("SS_EV2", 1) != 0,
                pend_e: vec![PendE::default(); 2 * HALF].into_boxed_slice(),
                cur: 0,
                npe: 0,
                cnt: [0; 2],
                held: [0; 2],
                pend_p: [Vec::with_capacity(HALF), Vec::with_capacity(HALF)],
                pend_c: vec![PendC::default(); PEND].into_boxed_slice(),
                npc: 0,
                pend_l: Vec::with_capacity(PEND),
            }))
        })
        .collect();

    // The ID table must be written before it is read (see `touch_part`).
    if std::env::var_os("SS_NOTOUCH").is_none() {
        par(threads, |tid| ctx.ids.touch_part(tid, threads));
    }
    lap("  table touched");

    // ------------------------------------------------------------ links
    // (products and campaigns load on the side meanwhile)
    let mut tasks = Vec::new();
    grid_tasks(&link_files, false, chunk, &mut tasks);
    let aux = std::thread::scope(|scope| {
        let aux = scope.spawn(|| load_aux(data_dir));
        let next = AtomicUsize::new(0);
        par(threads, |tid| {
            let out = out_mut(&outs, tid);
            loop {
                let i = next.fetch_add(1, Ordering::Relaxed);
                let Some(task) = tasks.get(i) else { break };
                // (A table that has overflowed is not filled any further.)
                if ctx.ids.overflowed() {
                    break;
                }
                let (start, end) = out.reader.load(&link_files[task.file], task.a, task.b);
                let buf = std::mem::take(&mut out.reader.buf);
                parse_links(&buf[..end + 64], start, end, &ctx, out);
                out.reader.buf = buf;
            }
            flush_links(&ctx, out);
        });
        lap("  links: parse and join");
        let next = AtomicUsize::new(0);
        par(threads, |_| loop {
            let r = next.fetch_add(1, Ordering::Relaxed);
            if r >= ctx.ids.nregions || ctx.ids.overflowed() {
                break;
            }
            ctx.ids.flatten_region(r);
        });
        lap("  links: flatten");
        aux.join().unwrap()
    });
    if ctx.ids.overflowed() {
        return give_up(&ctx, &outs, threads, &event_files, &click_files, log);
    }
    ctx.aux = aux;
    lap("links");

    // ------------------------------------------------------------ parse
    // The files are parsed in place, mapped: copying them through a buffer
    // costs more kernel time than the page faults do. The parsers ask for
    // the bytes a page ahead, since nothing else brings them into cache in
    // time. (SS_MMAP=0: read into a buffer instead, as for unmappable files.)
    let mapped = env_usize("SS_MMAP", 1) != 0;
    if mapped {
        for src in event_files.iter_mut().chain(click_files.iter_mut()) {
            src.map();
        }
    }
    let ahead = env_usize("SS_AHEAD", 4096);
    let mut tasks = Vec::new();
    let piece = env_usize("SS_PIECE", if mapped { 256 << 10 } else { chunk });
    grid_tasks(&event_files, true, piece, &mut tasks);
    grid_tasks(&click_files, false, piece, &mut tasks);
    let next = AtomicUsize::new(0);
    par(threads, |tid| {
        let out = out_mut(&outs, tid);
        loop {
            let i = next.fetch_add(1, Ordering::Relaxed);
            let Some(task) = tasks.get(i) else { break };
            // Once the ID table has overflowed the run is lost: lookups
            // answer with slots that are not the users', so stop here
            // rather than append the rest of the input to the wrong people.
            if ctx.ids.overflowed() {
                return;
            }
            let src = if task.events { &event_files[task.file] } else { &click_files[task.file] };
            if src.mapped() {
                let (buf, start, end) = src.view(task.a, task.b);
                out.ahead = ahead;
                if task.events {
                    parse_events(buf, start, end, &ctx, out, tid);
                } else {
                    parse_clicks(buf, start, end, &ctx, out);
                }
                continue;
            }
            out.ahead = 0;
            let (start, end) = out.reader.load(src, task.a, task.b);
            let buf = std::mem::take(&mut out.reader.buf);
            if task.events {
                parse_events(&buf[..end + 128], start, end, &ctx, out, tid);
            } else {
                parse_clicks(&buf[..end + 128], start, end, &ctx, out);
            }
            out.reader.buf = buf;
        }
        flush_events(&ctx, out, tid);
        flush_clicks(&ctx, out);
        out.ev.finish(&mut out.arena);
        out.ck.finish(&mut out.arena);
        out.slim.finish(&mut out.arena_slim);
    });
    if ctx.ids.overflowed() {
        return give_up(&ctx, &outs, threads, &event_files, &click_files, log);
    }
    if log {
        eprintln!("    ids {} in {} slots, nq {}, pbits {}", ctx.ids.len(), ctx.ids.nregions * RSIZE, ctx.nq, ctx.pbits);
    }
    lap("parse");
    let diag = std::env::var("SS_DIAG").ok();
    let huge_mb = if diag.as_deref() == Some("3") { diag::huge_mb() } else { 0 };
    // From here on the per-thread outputs are shared read-only (dedup marks
    // dead events through raw pointers, one owner per event).
    let outs_ref: Vec<&ThreadOut> = (0..threads).map(|t| &*out_mut(&outs, t)).collect();

    // ------------------------------------------------------------ dedup
    // Two tables do this. The plain one takes fewer instructions; the one
    // that compares a cache line of small entries at once has no
    // hard-to-predict branch, which only pays on the widest processors
    // (SS_DEDUP=rows selects it).
    let rows = std::env::var("SS_DEDUP").as_deref() == Ok("rows");
    let next = AtomicUsize::new(0);
    par(threads, |_| {
        let mut table: Vec<Row> = Vec::new();
        let mut wide: Vec<u64> = Vec::new();
        loop {
            let p = next.fetch_add(1, Ordering::Relaxed);
            if p >= 1 << ctx.pbits {
                break;
            }
            if rows {
                dedup_bucket_rows(p, &outs_ref, &mut table, &mut wide);
            } else {
                dedup_bucket(p, &outs_ref, &mut wide);
            }
        }
    });
    lap("dedup");

    // ------------------------------------------------------------ persons
    let user_dir = out_dir.join("user_daily");
    let camp_dir = out_dir.join("campaign_daily");
    std::fs::create_dir_all(&user_dir).unwrap();
    std::fs::create_dir_all(&camp_dir).unwrap();

    let ncell = (ctx.aux.camp_names.len() + 1) * ctx.nd;
    // About one output row per nine events in generated data; each thread
    // sets aside room for half as many again as its share of that.
    let kept: usize = outs_ref.iter().map(|o| (0..ctx.nq).map(|q| o.ev.len(q)).sum::<usize>()).sum();
    let rows_room = if env_usize("SS_ROOM", 1) != 0 { kept / 6 / threads + 4096 } else { 0 };
    // Biggest buckets first, so one huge person cannot be left for the end.
    let mut order: Vec<usize> = (0..ctx.nq).collect();
    order.sort_by_key(|&q| std::cmp::Reverse(outs_ref.iter().map(|o| o.ev.len(q) + o.ck.len(q)).sum::<usize>()));
    let next = AtomicUsize::new(0);
    let cells: Vec<Shared<UnsafeCell<Option<Cells>>>> = (0..threads).map(|_| Shared(UnsafeCell::new(None))).collect();
    par(threads, |tid| {
        // Nobody reads the dedup notes any more.
        outs_ref[tid].arena_slim.release();
        let mut tl = Tl::new(&ctx, ncell, rows_room);
        loop {
            let i = next.fetch_add(1, Ordering::Relaxed);
            let Some(&q) = order.get(i) else { break };
            person_bucket(q, &outs_ref, &ctx, &mut tl);
        }
        let t_w = Instant::now();
        if tid == 0 || tl.rows.n > 0 {
            write_user_daily(&user_dir.join(format!("part-{tid:03}.parquet")), &mut tl.rows, &ctx);
        }
        if log {
            eprintln!("    t{tid}: count {:.0} place {:.0} persons {:.0} write {:.0} ms | slowest bucket {:.1} ms ({} events, {} clicks) | biggest person {:.1} ms ({} events, {} clicks)", tl.prof[0] * 1e3, tl.prof[1] * 1e3, tl.prof[2] * 1e3, t_w.elapsed().as_secs_f64() * 1e3, tl.prof[3] * 1e3, tl.prof[4], tl.prof[5], tl.prof[6] * 1e3, tl.prof[7], tl.prof[8]);
        }
        unsafe { *cells[tid].0.get() = Some(tl.cells) };
    });
    lap("persons + user_daily");

    // SS_DIAG=1 or 2: finish with a wait whose length tells, through the
    // total run time, facts about the machine (1) or two memory costs (2).
    let mut wait_until = 0.0f64;
    match std::env::var("SS_DIAG").ok().as_deref() {
        Some("1") => {
            let a = diag::facts();
            if log {
                eprintln!("    diag: facts {a}");
            }
            wait_until = 2.0 + 4.0 * a as f64;
        }
        Some("2") => {
            let (r, z) = diag::costs(&ctx.ids, &|f| par(threads, f));
            wait_until = 2.0 + 0.2 * r as f64 + 16.0 * z as f64;
        }
        _ => {}
    }

    // The last table is small: write it while the other threads give the big
    // buffers back, which is quicker than leaving it all to process exit.
    let cells: Vec<Cells> = cells.into_iter().map(|c| c.0.into_inner().unwrap()).collect();
    let next = AtomicUsize::new(0);
    par(threads, |tid| {
        if tid == 0 {
            write_campaign_daily(&camp_dir.join("part-000.parquet"), &cells, &ctx);
        }
        outs_ref[tid].arena.release();
        ctx.ids.release_part(tid, threads);
        loop {
            let i = next.fetch_add(1, Ordering::Relaxed);
            match event_files.get(i).or_else(|| click_files.get(i.wrapping_sub(event_files.len()))) {
                Some(src) => src.unmap(),
                None => break,
            }
        }
    });
    lap("campaign_daily");
    if pad.is_some() {
        wait_until = 2.0 + 10.0 * padded.get();
        // SS_PART=1: the stage's share of the whole instead: the run ends at
        // 4 s + 10 s x (stage time / work time).
        if std::env::var_os("SS_PART").is_some() {
            wait_until = 4.0 + 10.0 * padded.get() / t0.elapsed().as_secs_f64();
        }
        if log {
            eprintln!("    pad: stage {:.3} s of {:.3} s, ends at {:.2} s", padded.get(), t0.elapsed().as_secs_f64(), wait_until);
        }
    }
    // SS_DIAG=3: the total run time tells how long the work itself took
    // (t), whether memory really is on huge pages, and how much memory all
    // threads together can move: the run ends at 1 s + 5 t + 2 s x code,
    // code = 4 if at least 64 MB was on huge pages after parsing, plus 0..3
    // for under 12, 12 to 20, 20 to 32, or more GB per second.
    if diag.as_deref() == Some("3") {
        let t = t0.elapsed().as_secs_f64();
        let gbps = diag::bandwidth(threads, &|f| par(threads, f));
        let class = (gbps >= 12.0) as u32 + (gbps >= 20.0) as u32 + (gbps >= 32.0) as u32;
        let code = 4 * (huge_mb >= 64) as u32 + class;
        if log {
            eprintln!("    diag: t {t:.3} s, {huge_mb} MB on huge pages, {gbps:.1} GB/s, code {code}");
        }
        wait_until = 1.0 + 5.0 * t + 2.0 * code as f64;
    }
    let now = t0.elapsed().as_secs_f64();
    if wait_until > now {
        std::thread::sleep(std::time::Duration::from_secs_f64(wait_until - now));
    }
    true
}

/// Abandon a run whose ID table proved too small (nothing has been written
/// yet). Its big buffers go back to the system first, since the next run
/// asks for more than this one had. Returns false, as `run` then does.
fn give_up(ctx: &Ctx, outs: &Outs, threads: usize, event_files: &[input::Src], click_files: &[input::Src], log: bool) -> bool {
    par(threads, |tid| {
        let out = out_mut(outs, tid);
        out.arena.release();
        out.arena_slim.release();
        ctx.ids.release_part(tid, threads);
    });
    for src in event_files.iter().chain(click_files) {
        src.unmap();
    }
    if log {
        eprintln!("    the ID table ({} slots) is too small for this input: starting over with a bigger one", ctx.ids.nregions * RSIZE);
    }
    false
}

// ====================================================================== setup

fn read_file(path: &Path) -> Vec<u8> {
    std::fs::read(path).unwrap_or_default()
}

/// The lines after the header row.
fn data_lines(bytes: &[u8]) -> impl Iterator<Item = &[u8]> {
    let body = match memchr(b'\n', bytes) {
        Some(i) => &bytes[i + 1..],
        None => &bytes[bytes.len()..],
    };
    lines(body)
}

/// The lines of `data`, which ends at a line boundary or the end of a file.
fn lines(data: &[u8]) -> impl Iterator<Item = &[u8]> {
    let mut pos = 0;
    std::iter::from_fn(move || {
        if pos >= data.len() {
            return None;
        }
        let end = match memchr(b'\n', &data[pos..]) {
            Some(i) => pos + i,
            None => data.len(),
        };
        let line = &data[pos..end];
        pos = end + 1;
        Some(line)
    })
}

fn manifest_time(text: &[u8], key: &str) -> i64 {
    let at = memfind(text, key.as_bytes()).expect("manifest key");
    let rest = &text[at + key.len()..];
    let colon = memchr(b':', rest).expect("manifest colon");
    let rest = &rest[colon + 1..];
    let q1 = memchr(b'"', rest).expect("manifest value");
    let rest = &rest[q1 + 1..];
    let q2 = memchr(b'"', rest).expect("manifest value end");
    parse_ts(&rest[..q2]).expect("manifest timestamp")
}

fn build_ctx(data: &Path, event_bytes: usize, click_bytes: usize, link_bytes: usize, scale: usize) -> Ctx {
    let manifest = read_file(&data.join("manifest.json"));
    let ws = manifest_time(&manifest, "\"window_start\"");
    let we = manifest_time(&manifest, "\"window_end\"");
    let day0 = ws.div_euclid(DAY);
    let origin = day0 * DAY - 7 * DAY;
    let nd = if we > ws { ((we - 1).div_euclid(DAY) - day0 + 1) as usize } else { 1 };
    assert!(we - origin < (1i64 << 31), "window too long for 31-bit offsets");

    // Size the ID table from the input. Generated data has about one distinct
    // ID per 14 event lines; this leaves room for about twice that.
    // It is only a guess from the files' sizes (nothing short of reading the
    // input tells how many distinct IDs it holds): a run whose table proves
    // too small is given up, and repeated with `scale` four times as large
    // (see `main`).
    let event_lines = event_bytes / 64 + 1;
    let ids_guess = event_lines / 8 + click_bytes / 512 + link_bytes / 64 + 1024;
    // SS_SCALE_DIV=n (to test that route): the first table is n times too
    // small, down to a single region.
    let div = env_usize("SS_SCALE_DIV", 1).max(1);
    let nregions = ((ids_guess * scale / div).div_ceil(RSIZE)).max(if div > 1 { 1 } else { 4 });
    // Person buckets: up to 640, or more for very large inputs so that a
    // bucket stays under roughly 100k events (it should fit a core's share
    // of the cache while its people are put together). A dedup note has
    // Q_BITS bits for the bucket's number: past that many buckets (some 200M
    // event lines) the buckets grow instead.
    let buckets = env_usize("SS_NQ", 640.max(event_lines / 100_000)).clamp(1, 1 << Q_BITS);
    let rpb = nregions.div_ceil(buckets).next_power_of_two();
    let qshift = (RBITS + rpb.trailing_zeros() as usize) as u32;
    let nq = nregions.div_ceil(rpb);
    assert!(nq <= 1 << Q_BITS);
    // Dedup buckets of about 50k lines each (the nearest power of two of
    // them): a per-bucket table of a few hundred KB, which stays in a
    // mid-level cache, without too many lists to append to while parsing.
    let pbits = match env_usize("SS_PB", 0) {
        0 => (((event_lines / 46_341).max(1).ilog2()) as u32).clamp(4, 11),
        pb => pb as u32,
    };

    Ctx {
        ws,
        we,
        cws: ws - 7 * DAY,
        origin,
        day0,
        nd,
        ids: IdTable::new(nregions),
        qshift,
        nq,
        pbits,
        aux: Aux {
            products: KMap::with_capacity(4, 0),
            unknown: 0,
            cat_names: Vec::new(),
            campaigns: KMap::with_capacity(4, 0),
            camp_names: Vec::new(),
            camp_channel: Vec::new(),
        },
    }
}

fn load_aux(data: &Path) -> Aux {
    // products -> category rank (categories sorted byte-wise, with `unknown`)
    let products_raw = read_file(&data.join("products.csv"));
    let mut pairs: Vec<(Key, u32)> = Vec::new();
    let mut cats: Vec<&[u8]> = vec![b"unknown"];
    for line in data_lines(&products_raw) {
        if let Some([id, cat]) = split::<2>(line) {
            if !id.is_empty() {
                // Few distinct categories: a linear search beats hashing.
                let ci = match cats.iter().position(|c| *c == cat) {
                    Some(ci) => ci,
                    None => {
                        cats.push(cat);
                        cats.len() - 1
                    }
                };
                pairs.push((pack(id), ci as u32));
            }
        }
    }
    let mut order: Vec<usize> = (0..cats.len()).collect();
    order.sort_by_key(|&i| cats[i]);
    assert!(cats.len() <= 511, "too many categories for 9 bits");
    let mut rank = vec![0u16; cats.len()];
    for (r, &i) in order.iter().enumerate() {
        rank[i] = r as u16;
    }
    let cat_names: Vec<Vec<u8>> = order.iter().map(|&i| cats[i].to_vec()).collect();
    let unknown = rank[0];
    let mut products = KMap::with_capacity(pairs.len(), 0u16);
    for &(k, ci) in &pairs {
        products.get_or_insert(k, rank[ci as usize]);
    }

    let campaigns_raw = read_file(&data.join("campaigns.csv"));
    let mut campaigns = KMap::with_capacity(2048, 0u32);
    let mut camp_names = Vec::new();
    let mut camp_channel = Vec::new();
    for line in data_lines(&campaigns_raw) {
        if let Some([id, channel]) = split::<2>(line) {
            if id.is_empty() {
                continue;
            }
            let idx = camp_names.len() as u32;
            if campaigns.get_or_insert(pack(id), idx) == idx {
                camp_names.push(id.to_vec());
                camp_channel.push(channel.to_vec());
            }
        }
    }
    Aux { products, unknown, cat_names, campaigns, camp_names, camp_channel }
}

/// Experiments (builds with the `exp` feature only): how far to take each line.
#[inline(always)]
fn exp() -> u32 {
    #[cfg(feature = "exp")]
    {
        static EXP: std::sync::OnceLock<u32> = std::sync::OnceLock::new();
        return *EXP.get_or_init(|| std::env::var("SS_EXP").ok().and_then(|v| v.parse().ok()).unwrap_or(0));
    }
    #[cfg(not(feature = "exp"))]
    0
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn slow_only() -> bool {
    static SLOW: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *SLOW.get_or_init(|| std::env::var_os("SS_SLOW").is_some())
}

// ====================================================================== links

const PEND: usize = 32;
/// Half the number of events that can wait for their table lookups.
const HALF: usize = 16;

#[inline(always)]
fn emit_link(ctx: &Ctx, out: &mut ThreadOut, a: Key, b: Key) {
    let (ha, hb) = (hash64(a), hash64(b));
    ctx.ids.prefetch(ha);
    ctx.ids.prefetch(hb);
    out.pend_l.push((a, ha, b, hb));
    if out.pend_l.len() == PEND {
        flush_links(ctx, out);
    }
}

#[inline(always)]
fn flush_links(ctx: &Ctx, out: &mut ThreadOut) {
    // Find both IDs of every line, ask for the lines their current parents
    // are in, and only then join: a join walks from parent to parent, and
    // would otherwise wait for each of them in turn.
    for l in out.pend_l.iter_mut() {
        let (ia, ib) = (ctx.ids.get_or_insert(l.0, l.1), ctx.ids.get_or_insert(l.2, l.3));
        l.1 = ia as u64;
        l.3 = ib as u64;
    }
    // Once the table has overflowed, a slot it answers with need not be the
    // ID's, nor hold any ID at all: join nothing more (the run is given up).
    if ctx.ids.overflowed() {
        out.pend_l.clear();
        return;
    }
    for l in &out.pend_l {
        ctx.ids.prefetch_at(ctx.ids.root(l.1 as u32));
        ctx.ids.prefetch_at(ctx.ids.root(l.3 as u32));
    }
    for l in &out.pend_l {
        ctx.ids.union(l.1 as u32, l.3 as u32);
    }
    out.pend_l.clear();
}

fn parse_links(file: &[u8], start: usize, end: usize, ctx: &Ctx, out: &mut ThreadOut) {
    let base = file.as_ptr();
    let rest = if slow_only() {
        start
    } else {
        unsafe {
            fast::scan(file, start, end, |ls, commas, nc, le, nq| {
                if nc != 1 {
                    return;
                }
                let mut s = [ls, commas[0] + 1];
                let mut e = [commas[0], le];
                let k = fast::strip(base, &mut s[0], &mut e[0]) + fast::strip(base, &mut s[1], &mut e[1]);
                if nq != 2 * k || e[0] == s[0] || e[1] == s[1] {
                    return;
                }
                emit_link(ctx, out, fast::pack_at(base, s[0], e[0]), fast::pack_at(base, s[1], e[1]));
            })
        }
    };
    for line in lines(&file[rest..end]) {
        let Some([a, b]) = split::<2>(line) else { continue };
        if !a.is_empty() && !b.is_empty() {
            emit_link(ctx, out, pack(a), pack(b));
        }
    }
}

// ====================================================================== parse

/// A valid event waiting for its ID-table lookup (the slot is prefetched).
#[derive(Clone, Copy, Default)]
#[repr(C)]
struct PendE {
    ulo: u64,
    uhi: u64,
    ev: u64,
    h: u64,
    /// The extra hash bits in place, and the ingest time in the low 32 bits.
    x: u64,
    /// The first table slot to try for the user.
    first: u32,
    _pad: u32,
}

#[derive(Clone, Copy, Default)]
#[repr(C)]
struct PendC {
    cid: Key,
    ulo: u64,
    uhi: u64,
    ts: u32,
    camp: u32,
    first: u32,
    _pad: u32,
}

/// Ingest time in seconds as a biased u32 (only ever compared between copies
/// of one event). `None` (unparsable) sorts last.
#[inline(always)]
fn ingest32(ctx: &Ctx, ing: Option<i64>) -> u32 {
    const TOP: u32 = u32::MAX - 1;
    match ing {
        Some(ms) => {
            let rel = ms.wrapping_sub(ctx.origin).wrapping_add((1i64 << 31) * 1000);
            if (rel as u64) < TOP as u64 * 1000 {
                (rel as u64 / 1000) as u32
            } else if rel < 0 {
                0
            } else {
                TOP
            }
        }
        None => TOP,
    }
}

/// `ingest32` for a time in whole seconds (not `fast::good` when unparsable)
/// and the time origin in seconds.
#[inline(always)]
fn ingest32_s(origin_s: i64, secs: i64) -> u32 {
    const TOP: u32 = u32::MAX - 1;
    #[cold]
    #[inline(never)]
    fn far(secs: i64, r: i64) -> u32 {
        if !fast::good(secs) || r >= 0 {
            TOP
        } else {
            0
        }
    }
    // (An unparsable time lands far below zero here.)
    let r = secs.wrapping_sub(origin_s).wrapping_add(1i64 << 31);
    if (r as u64) < TOP as u64 {
        r as u32
    } else {
        far(secs, r)
    }
}

/// Record one parsed event line: its ID, its ingest time, and when the copy
/// is valid its user, time and price (`ev`) and, for a purchase, its product
/// (0 otherwise; the category is filled in when the batch is flushed).
#[cfg_attr(feature = "prof", inline(never))]
#[cfg_attr(not(feature = "prof"), inline(always))]
fn emit_event(ctx: &Ctx, out: &mut ThreadOut, tid: usize, ek: Key, ing32: u32, valid: bool, user: Key, ev: u64, prod: Key) {
    let (h, hb) = hash_pair(ek);
    let x = (((hb >> 20) & H2_MASK) << H2_SHIFT) | ing32 as u64;
    if valid {
        let first = ctx.ids.first(hash64(user));
        ctx.ids.prefetch_at(first);
        let n = out.npe;
        if prod != 0 {
            ctx.aux.products.prefetch(prod);
            out.pend_p[(out.cur != 0) as usize].push((n as u32, prod));
        }
        unsafe {
            *out.pend_e.get_unchecked_mut(out.cur + n) = PendE { ulo: user as u64, uhi: (user >> 64) as u64, ev, h, x, first, _pad: 0 };
        }
        out.npe = n + 1;
        if n + 1 == HALF {
            turn_events(ctx, out, tid);
        }
    } else {
        let p = (h >> (64 - ctx.pbits)) as usize;
        out.slim.push16(p, h, TAG | x, &mut out.arena_slim);
    }
}

/// The half being filled is full: look up and empty the other one, then fill that.
#[inline(always)]
fn turn_events(ctx: &Ctx, out: &mut ThreadOut, tid: usize) {
    let h = (out.cur != 0) as usize;
    out.cnt[h] = HALF;
    flush_half(ctx, out, tid, h ^ 1, false);
    out.cur = (h ^ 1) * HALF;
    out.npe = out.cnt[h ^ 1];
}

/// Look up and append every pending event (at the end of the input).
fn flush_events(ctx: &Ctx, out: &mut ThreadOut, tid: usize) {
    let h = (out.cur != 0) as usize;
    out.cnt[h] = out.npe;
    flush_half(ctx, out, tid, h ^ 1, true);
    flush_half(ctx, out, tid, h, true);
    out.cur = 0;
    out.npe = 0;
}

/// Look up the users of the pending events in half `h` and append the events
/// to their lists.
///
/// First every lookup, then every append: an append's address depends on the
/// person found, and must not wait on a table line that is still on its way.
/// A user who is not in the first cache line tried (a few in a hundred) has
/// the next line fetched and stays in the half until its next flush, unless
/// this is the `last` one.
#[inline(never)]
fn flush_half(ctx: &Ctx, out: &mut ThreadOut, tid: usize, h: usize, last: bool) {
    let base = h * HALF;
    for k in 0..out.pend_p[h].len() {
        let (i, prod) = out.pend_p[h][k];
        let cat = ctx.aux.products.get(prod).unwrap_or(ctx.aux.unknown);
        out.pend_e[base + i as usize].ev |= (cat as u64) << 24;
    }
    out.pend_p[h].clear();
    let n = out.cnt[h];
    let here = (tid as u64) << (IDX_BITS + Q_BITS);
    let pend = unsafe { out.pend_e.as_mut_ptr().add(base) };
    let exp = exp();
    if exp == 4 || exp == 5 {
        // bookkeeping only: no lookups, no appends
        let mut acc = 0u64;
        for i in 0..n {
            let pe = unsafe { *pend.add(i) };
            acc = acc.wrapping_add(pe.first as u64 + pe.h + pe.x + pe.ev);
        }
        std::hint::black_box(acc);
        out.cnt[h] = 0;
        return;
    }
    if out.simple {
        // A lookup that misses its first line is finished at once (SS_SIMPLE=0:
        // it is held back a round so its next line can be fetched meanwhile,
        // which costs more instructions than the few waits it saves).
        for i in 0..n {
            let pe = unsafe { &mut *pend.add(i) };
            pe.first = match ctx.ids.peek(pe.ulo, pe.uhi, pe.first) {
                Some(slot) => ctx.ids.root(slot),
                None => ctx.ids.root(ctx.ids.get_or_insert_at(pe.ulo, pe.uhi, pe.first)),
            };
        }
        for i in 0..n {
            let pe = unsafe { *pend.add(i) };
            let person = pe.first;
            let q = (person >> ctx.qshift) as usize;
            let idx = out.ev.push16(q, pe.ev, person as u64 | (pe.x << 32), &mut out.arena);
            assert!(idx < 1 << IDX_BITS, "bucket too large");
            let p = (pe.h >> (64 - ctx.pbits)) as usize;
            let x = (pe.x & (H2_MASK << H2_SHIFT)) | here | ((q as u64) << IDX_BITS) | idx as u64;
            out.slim.push16(p, pe.h, x, &mut out.arena_slim);
        }
        out.held[h] = 0;
        out.cnt[h] = 0;
        return;
    }
    let mut later = [0u8; HALF];
    let mut nl = 0;
    // The first `held` were left by the flush before; their lines are here now.
    for i in 0..out.held[h] {
        let pe = unsafe { &mut *pend.add(i) };
        pe.first = ctx.ids.root(ctx.ids.get_or_insert_at(pe.ulo, pe.uhi, pe.first));
    }
    for i in out.held[h]..n {
        let pe = unsafe { &mut *pend.add(i) };
        match ctx.ids.peek(pe.ulo, pe.uhi, pe.first) {
            Some(slot) => pe.first = ctx.ids.root(slot),
            None => {
                later[nl] = i as u8;
                nl += 1;
            }
        }
    }
    let mut m = n;
    if last || nl > HALF / 4 {
        for &i in &later[..nl] {
            let pe = unsafe { &mut *pend.add(i as usize) };
            pe.first = ctx.ids.root(ctx.ids.get_or_insert_at(pe.ulo, pe.uhi, pe.first));
        }
        nl = 0;
    } else {
        // Move the ones to hold to the end (from the last one down, so each
        // swap brings a looked-up event forward).
        for k in (0..nl).rev() {
            m -= 1;
            unsafe { std::ptr::swap(pend.add(later[k] as usize), pend.add(m)) };
        }
    }
    for i in 0..m {
        let pe = unsafe { *pend.add(i) };
        let person = pe.first;
        let q = (person >> ctx.qshift) as usize;
        if exp == 6 {
            std::hint::black_box(q);
            continue;
        }
        // An `Ev`: the event word, then the person and the ingest time.
        let idx = out.ev.push16(q, pe.ev, person as u64 | (pe.x << 32), &mut out.arena);
        assert!(idx < 1 << IDX_BITS, "bucket too large");
        let p = (pe.h >> (64 - ctx.pbits)) as usize;
        let x = (pe.x & (H2_MASK << H2_SHIFT)) | here | ((q as u64) << IDX_BITS) | idx as u64;
        if exp == 7 {
            std::hint::black_box(x);
            continue;
        }
        out.slim.push16(p, pe.h, x, &mut out.arena_slim);
    }
    unsafe { std::ptr::copy(pend.add(m), pend, n - m) };
    out.held[h] = nl;
    out.cnt[h] = nl;
}

#[cfg_attr(feature = "prof", inline(never))]
#[cfg_attr(not(feature = "prof"), inline(always))]
fn emit_click(ctx: &Ctx, out: &mut ThreadOut, user: Key, cid: Key, ts: u32, camp: u32) {
    let first = ctx.ids.first(hash64(user));
    ctx.ids.prefetch_at(first);
    let n = out.npc;
    unsafe {
        *out.pend_c.get_unchecked_mut(n) = PendC { cid, ulo: user as u64, uhi: (user >> 64) as u64, ts, camp, first, _pad: 0 };
    }
    out.npc = n + 1;
    if n + 1 == PEND {
        flush_clicks(ctx, out);
    }
}

#[cfg_attr(feature = "prof", inline(never))]
#[cfg_attr(not(feature = "prof"), inline(always))]
fn flush_clicks(ctx: &Ctx, out: &mut ThreadOut) {
    let mut later = [0u8; PEND];
    let mut nl = 0;
    for i in 0..out.npc {
        let c = unsafe { out.pend_c.get_unchecked_mut(i) };
        match ctx.ids.peek(c.ulo, c.uhi, c.first) {
            Some(slot) => c.first = ctx.ids.root(slot),
            None => {
                later[nl] = i as u8;
                nl += 1;
            }
        }
    }
    for &i in &later[..nl] {
        let c = unsafe { out.pend_c.get_unchecked_mut(i as usize) };
        c.first = ctx.ids.root(ctx.ids.get_or_insert_at(c.ulo, c.uhi, c.first));
    }
    for i in 0..out.npc {
        let c = unsafe { *out.pend_c.get_unchecked(i) };
        let person = c.first;
        let q = (person >> ctx.qshift) as usize;
        // A `Ck`: the click ID, then the person and time, then the campaign.
        out.ck.push32(q, [c.cid as u64, (c.cid >> 64) as u64, person as u64 | ((c.ts as u64) << 32), c.camp as u64], &mut out.arena);
    }
    out.npc = 0;
}

/// One event line by the plain rules: the reference for `events_fast`, and
/// what handles every line that one leaves alone.
fn event_line(line: &[u8], ctx: &Ctx, out: &mut ThreadOut, tid: usize) {
    let Some([eid, user, ts, ingest, typ, prod, price]) = split::<7>(line) else { return };
    if eid.is_empty() {
        return;
    }
    let ek = pack(eid);
    let ing = parse_f1(ingest);
    let mut ev = None;
    let mut prod_key = 0;
    if !user.is_empty() && !prod.is_empty() {
        if let Some(t) = parse_ts(ts) {
            // In the window, and not ingested more than 24 hours late.
            if t >= ctx.ws && t < ctx.we && ing.is_some_and(|i| i - t <= DAY) {
                let off = ((t - ctx.origin) as u64) << 33;
                if typ == b"view" || typ == b"cart" {
                    ev = Some(off);
                } else if typ == b"purchase" {
                    if let Some(p) = parse_price(price) {
                        prod_key = pack(prod);
                        ev = Some(off | p as u64);
                    }
                }
            }
        }
    }
    let user = if ev.is_some() { pack(user) } else { 0 };
    emit_event(ctx, out, tid, ek, ingest32(ctx, ing), ev.is_some(), user, ev.unwrap_or(0), prod_key);
}

/// The event lines of `file[start..end]` that end before `end`, as far as
/// they can be handled 128 bytes at a time; returns where the rest starts.
#[inline(never)]
unsafe fn events_fast(file: &[u8], start: usize, end: usize, ctx: &Ctx, out: &mut ThreadOut, tid: usize) -> usize {
    const VIEW: u32 = u32::from_le_bytes(*b"view");
    const CART: u32 = u32::from_le_bytes(*b"cart");
    const PURCHASE: u64 = u64::from_le_bytes(*b"purchase");
    let base = file.as_ptr();
    let len = file.len();
    let span = (ctx.we - ctx.ws) as u64;
    let origin_s = ctx.origin / 1000;
    let exp = exp();
    let ahead = out.ahead;
    let mut s = start;
    while s < end {
        if s + 128 > len {
            break;
        }
        if ahead != 0 {
            // (two cache lines per line: lines are mostly shorter than that)
            core::arch::x86_64::_mm_prefetch(base.wrapping_add(s + ahead) as *const i8, core::arch::x86_64::_MM_HINT_T0);
            core::arch::x86_64::_mm_prefetch(base.wrapping_add(s + ahead + 64) as *const i8, core::arch::x86_64::_MM_HINT_T0);
        }
        let c = fast::cut::<3, 3, 8>(base, s);
        if !c.shape {
            // An unusual line: the plain code decides.
            let Some(i) = memchr(b'\n', &file[s..end]) else { break };
            event_line(&file[s..s + i], ctx, out, tid);
            s += i + 1;
            continue;
        }
        let e = c.p[7];
        if e >= end {
            break;
        }
        s = e + 1;
        let (s0, n0) = c.field(0);
        if !c.quotes | (n0 == 0) | (exp == 1) {
            continue;
        }
        // Each part goes into the next pending slot as soon as it is known
        // (few values then have to be kept at hand across the rest); the
        // slot only counts if the event turns out valid.
        let n = out.npe;
        let pe = out.pend_e.as_mut_ptr().add(out.cur + n);
        let (h, hb) = hash_pair(fast::pack_at(base, s0, s0 + n0));
        let (s3, n3) = c.field(3);
        let ing = out.dates.f1(base.add(s3), n3);
        let x = (((hb >> 20) & H2_MASK) << H2_SHIFT) | ingest32_s(origin_s, ing) as u64;
        (*pe).h = h;
        (*pe).x = x;
        let (s1, n1) = c.field(1);
        let user = fast::pack_at(base, s1, s1 + n1);
        let first = ctx.ids.first(hash64(user));
        if exp != 4 {
            ctx.ids.prefetch_at(first);
        }
        (*pe).ulo = user as u64;
        (*pe).uhi = (user >> 64) as u64;
        (*pe).first = first;
        let (s4, n4) = c.field(4);
        let (s5, n5) = c.field(5);
        let tp = base.add(s4);
        let w = std::ptr::read_unaligned(tp as *const u32);
        let browse = (n4 == 4) & ((w == VIEW) | (w == CART));
        let (s2, n2) = c.field(2);
        let t = out.dates.ts(base.add(s2), n2);
        // A user, a product, a time in the window, and not ingested more than 24 hours late.
        let fresh = fast::good(ing) & (ing.wrapping_mul(1000).wrapping_sub(t) <= DAY);
        let ok = (n1 != 0) & (n5 != 0) & ((t.wrapping_sub(ctx.ws) as u64) < span) & fresh;
        let mut ev = (t.wrapping_sub(ctx.origin) as u64) << 33;
        let mut valid = ok & browse;
        if opaque((ok & !browse) as u64) != 0 && n4 == 8 && std::ptr::read_unaligned(tp as *const u64) == PURCHASE {
            let (s6, n6) = c.field(6);
            if let Some(p) = parse_price(std::slice::from_raw_parts(base.add(s6), n6)) {
                let prod = fast::pack_at(base, s5, s5 + n5);
                ctx.aux.products.prefetch(prod);
                out.pend_p[(out.cur != 0) as usize].push((n as u32, prod));
                valid = true;
                ev |= p as u64;
            }
        }
        (*pe).ev = ev;
        if exp == 2 {
            std::hint::black_box((valid, ev, x, first));
            continue;
        }
        if valid {
            out.npe = n + 1;
            if n + 1 == HALF {
                turn_events(ctx, out, tid);
            }
        } else {
            let p = (h >> (64 - ctx.pbits)) as usize;
            out.slim.push16(p, h, TAG | x, &mut out.arena_slim);
        }
    }
    s
}

/// `events_fast` taking the line in two parts: everything the first window
/// gives (event ID, user, time) is worked out and put away before the second
/// window is looked at, so fewer values are alive at once. What it puts in
/// the pending slot only counts once the whole line has proved sound.
#[inline(never)]
unsafe fn events_fast2(file: &[u8], start: usize, end: usize, ctx: &Ctx, out: &mut ThreadOut, tid: usize) -> usize {
    const VIEW: u32 = u32::from_le_bytes(*b"view");
    const CART: u32 = u32::from_le_bytes(*b"cart");
    const PURCHASE: u64 = u64::from_le_bytes(*b"purchase");
    let base = file.as_ptr();
    let len = file.len();
    let span = (ctx.we - ctx.ws) as u64;
    let origin_s = ctx.origin / 1000;
    let ahead = out.ahead;
    let mut s = start;
    while s < end {
        if s + 128 > len {
            break;
        }
        if ahead != 0 {
            core::arch::x86_64::_mm_prefetch(base.wrapping_add(s + ahead) as *const i8, core::arch::x86_64::_MM_HINT_T0);
            core::arch::x86_64::_mm_prefetch(base.wrapping_add(s + ahead + 64) as *const i8, core::arch::x86_64::_MM_HINT_T0);
        }
        let a = fast::cut_a::<3, 4>(base, s);
        if !a.shape {
            // An unusual line: the plain code decides.
            let Some(i) = memchr(b'\n', &file[s..end]) else { break };
            event_line(&file[s..s + i], ctx, out, tid);
            s += i + 1;
            continue;
        }
        let n = out.npe;
        let pe = out.pend_e.as_mut_ptr().add(out.cur + n);
        let (s0, n0) = a.field(0);
        let (h, hb) = hash_pair(fast::pack_at(base, s0, s0 + n0));
        (*pe).h = h;
        let (s1, n1) = a.field(1);
        let user = fast::pack_at(base, s1, s1 + n1);
        let first = ctx.ids.first(hash64(user));
        ctx.ids.prefetch_at(first);
        (*pe).ulo = user as u64;
        (*pe).uhi = (user >> 64) as u64;
        (*pe).first = first;
        let (s2, n2) = a.field(2);
        let t = out.dates.ts(base.add(s2), n2);
        let sound_a = a.quotes & (n0 != 0);
        let has_user = n1 != 0;

        let b = fast::cut_b::<3, 5>(base, a.p[3]);
        if !b.shape {
            let Some(i) = memchr(b'\n', &file[s..end]) else { break };
            event_line(&file[s..s + i], ctx, out, tid);
            s += i + 1;
            continue;
        }
        let e = b.p[4];
        if e >= end {
            break;
        }
        s = e + 1;
        if !(sound_a & b.quotes) {
            continue;
        }
        let (s3, n3) = b.field(0);
        let ing = out.dates.f1(base.add(s3), n3);
        let x = (((hb >> 20) & H2_MASK) << H2_SHIFT) | ingest32_s(origin_s, ing) as u64;
        (*pe).x = x;
        let (s4, n4) = b.field(1);
        let (s5, n5) = b.field(2);
        let tp = base.add(s4);
        let w = std::ptr::read_unaligned(tp as *const u32);
        let browse = (n4 == 4) & ((w == VIEW) | (w == CART));
        // A user, a product, a time in the window, and not ingested more than 24 hours late.
        let fresh = fast::good(ing) & (ing.wrapping_mul(1000).wrapping_sub(t) <= DAY);
        let ok = has_user & (n5 != 0) & ((t.wrapping_sub(ctx.ws) as u64) < span) & fresh;
        let mut ev = (t.wrapping_sub(ctx.origin) as u64) << 33;
        let mut valid = ok & browse;
        if opaque((ok & !browse) as u64) != 0 && n4 == 8 && std::ptr::read_unaligned(tp as *const u64) == PURCHASE {
            let (s6, n6) = b.field(3);
            if let Some(p) = parse_price(std::slice::from_raw_parts(base.add(s6), n6)) {
                let prod = fast::pack_at(base, s5, s5 + n5);
                ctx.aux.products.prefetch(prod);
                out.pend_p[(out.cur != 0) as usize].push((n as u32, prod));
                valid = true;
                ev |= p as u64;
            }
        }
        (*pe).ev = ev;
        if valid {
            out.npe = n + 1;
            if n + 1 == HALF {
                turn_events(ctx, out, tid);
            }
        } else {
            let p = (h >> (64 - ctx.pbits)) as usize;
            out.slim.push16(p, h, TAG | x, &mut out.arena_slim);
        }
    }
    s
}

fn parse_events(file: &[u8], start: usize, end: usize, ctx: &Ctx, out: &mut ThreadOut, tid: usize) {
    if exp() == 3 {
        return;
    }
    let rest = if slow_only() {
        start
    } else if out.two_part {
        // (SS_EV2=0 for the one-piece version)
        unsafe { events_fast2(file, start, end, ctx, out, tid) }
    } else {
        unsafe { events_fast(file, start, end, ctx, out, tid) }
    };
    for line in lines(&file[rest..end]) {
        event_line(line, ctx, out, tid);
    }
}

/// One click line by the plain rules.
fn click_line(line: &[u8], ctx: &Ctx, out: &mut ThreadOut) {
    let Some([cid, user, camp, ts]) = split::<4>(line) else { return };
    if cid.is_empty() || user.is_empty() || camp.is_empty() {
        return;
    }
    let Some(t) = parse_ts(ts) else { return };
    if t < ctx.cws || t >= ctx.we {
        return;
    }
    let Some(camp) = ctx.aux.campaigns.get(pack(camp)) else { return };
    emit_click(ctx, out, pack(user), pack(cid), (t - ctx.origin) as u32, camp);
}

/// `events_fast` for click lines.
#[inline(never)]
unsafe fn clicks_fast(file: &[u8], start: usize, end: usize, ctx: &Ctx, out: &mut ThreadOut) -> usize {
    let base = file.as_ptr();
    let len = file.len();
    let span = (ctx.we - ctx.cws) as u64;
    let ahead = out.ahead;
    let mut s = start;
    while s < end {
        if s + 128 > len {
            break;
        }
        if ahead != 0 {
            core::arch::x86_64::_mm_prefetch(base.wrapping_add(s + ahead) as *const i8, core::arch::x86_64::_MM_HINT_T0);
        }
        let c = fast::cut::<2, 1, 5>(base, s);
        if !c.shape {
            let Some(i) = memchr(b'\n', &file[s..end]) else { break };
            click_line(&file[s..s + i], ctx, out);
            s += i + 1;
            continue;
        }
        let e = c.p[4];
        if e >= end {
            break;
        }
        s = e + 1;
        let (s0, n0) = c.field(0);
        let (s1, n1) = c.field(1);
        let (s2, n2) = c.field(2);
        let (s3, n3) = c.field(3);
        if !c.quotes | (n0 == 0) | (n1 == 0) | (n2 == 0) {
            continue;
        }
        let t = out.dates.ts(base.add(s3), n3);
        // In the click window (a bad time is far outside it).
        if (t.wrapping_sub(ctx.cws) as u64) >= span {
            continue;
        }
        let Some(camp) = ctx.aux.campaigns.get(fast::pack_at(base, s2, s2 + n2)) else { continue };
        let user = fast::pack_at(base, s1, s1 + n1);
        let cid = fast::pack_at(base, s0, s0 + n0);
        emit_click(ctx, out, user, cid, (t - ctx.origin) as u32, camp);
    }
    s
}

fn parse_clicks(file: &[u8], start: usize, end: usize, ctx: &Ctx, out: &mut ThreadOut) {
    if exp() != 0 {
        return;
    }
    let rest = if slow_only() { start } else { unsafe { clicks_fast(file, start, end, ctx, out) } };
    for line in lines(&file[rest..end]) {
        click_line(line, ctx, out);
    }
}

// ====================================================================== dedup

/// One cache line of the dedup table: 16 entries.
#[repr(C, align(64))]
#[derive(Clone, Copy)]
struct Row([u32; 16]);

/// Marks the losing copies of every event ID in dedup bucket `p`.
///
/// The table holds one entry per event ID seen so far: some bits of its hash
/// and the number of the note of its kept copy (smallest ingest_ts so far),
/// counting the bucket's notes thread after thread. A note looks at one
/// cache line of 16 entries at a time: all 16 are compared at once, a new ID
/// takes the line's first free entry, and only a full line sends it on to
/// the next.
fn dedup_bucket_rows(p: usize, outs: &[&ThreadOut], table: &mut Vec<Row>, wide: &mut Vec<u64>) {
    use core::arch::x86_64::*;
    const EMPTY: u32 = u32::MAX;
    let n: usize = outs.iter().map(|o| o.slim.len(p)).sum();
    if n < 2 {
        return;
    }
    // Note numbers take `sb` bits (never all ones, so no entry looks empty); the hash gets the rest.
    let sb = usize::BITS - n.leading_zeros();
    if sb > 22 {
        return dedup_bucket(p, outs, wide);
    }
    let seq_mask = (1u32 << sb) - 1;
    let tag_mask = (1u32 << (32 - sb)) - 1;
    let rows = (n / 11 + 1).next_power_of_two();
    table.clear();
    table.resize(rows, Row([EMPTY; 16]));
    let rmask = rows - 1;
    // Where each thread's notes start in the numbering.
    let mut first = [0u32; MAX_THREADS + 2];
    for (t, o) in outs.iter().enumerate() {
        first[t + 1] = first[t] + o.slim.len(p) as u32;
    }
    let nt = outs.len();
    let note = |seq: u32| -> *mut Slim {
        let mut t = 0;
        while t + 1 < nt && first[t + 1] <= seq {
            t += 1;
        }
        outs[t].slim.at(p, (seq - first[t]) as usize)
    };
    let ev_at = |x: u64| -> *mut Ev {
        let t = (x >> (IDX_BITS + Q_BITS)) as usize & ((1 << T_BITS) - 1);
        let q = (x >> IDX_BITS) as usize & ((1 << Q_BITS) - 1);
        outs[t].ev.at(q, (x & ((1 << IDX_BITS) - 1)) as usize)
    };
    let ingest = |x: u64| -> u32 { if x & TAG != 0 { x as u32 } else { unsafe { (*ev_at(x)).ingest } } };
    // A copy that is not the kept one goes (invalid copies have no event to drop).
    let drop_copy = |x: u64| {
        if x & TAG == 0 {
            unsafe { (*ev_at(x)).ev = DEAD };
        }
    };
    // A repeat is settled a little later, in arrival order, so the events
    // it needs (far apart in memory) can be fetched meanwhile.
    const LATER: usize = 16;
    const AHEAD: usize = 12;
    let mut later = [(0usize, Slim { h: 0, x: 0 }, 0u32); LATER];
    let mut nl = 0;
    let entries = table.as_mut_ptr() as *mut u32;
    let settle = |later: &[(usize, Slim, u32)]| {
        for &(i, s, seq) in later {
            let v = unsafe { *entries.add(i) };
            let kept = unsafe { *note(v & seq_mask) };
            // The copy with the smallest ingest_ts is kept, valid or not;
            // copies with equal ingest_ts are identical.
            if ingest(s.x) < ingest(kept.x) {
                drop_copy(kept.x);
                unsafe { *entries.add(i) = (v & !seq_mask) | seq };
            } else {
                drop_copy(s.x);
            }
        }
    };
    let fetch = |x: u64| {
        if x & TAG == 0 {
            unsafe { _mm_prefetch(ev_at(x) as *const i8, _MM_HINT_T0) };
        }
    };
    unsafe {
        let shift = _mm_cvtsi32_si128(sb as i32);
        let ones = _mm256_set1_epi32(-1);
        let mut seq = 0u32;
        for o in outs {
            for (ptr, len) in o.slim.pieces(p) {
                for k in 0..len {
                    // The table may not fit in cache: ask for a later note's line now.
                    if k + AHEAD < len {
                        let h = (*ptr.add(k + AHEAD)).h;
                        _mm_prefetch(entries.add((h as usize & rmask) * 16) as *const i8, _MM_HINT_T0);
                    }
                    let s = *ptr.add(k);
                    let tag = (s.h >> 20) as u32 & tag_mask;
                    let want = _mm256_set1_epi32(tag as i32);
                    let mut r = s.h as usize & rmask;
                    'probe: loop {
                        let row = entries.add(r * 16);
                        let (a, b) = (_mm256_load_si256(row as *const __m256i), _mm256_load_si256(row.add(8) as *const __m256i));
                        let free = _mm256_movemask_ps(_mm256_castsi256_ps(_mm256_cmpeq_epi32(a, ones))) as u32
                            | (_mm256_movemask_ps(_mm256_castsi256_ps(_mm256_cmpeq_epi32(b, ones))) as u32) << 8;
                        let same = _mm256_movemask_ps(_mm256_castsi256_ps(_mm256_cmpeq_epi32(_mm256_srl_epi32(a, shift), want))) as u32
                            | (_mm256_movemask_ps(_mm256_castsi256_ps(_mm256_cmpeq_epi32(_mm256_srl_epi32(b, shift), want))) as u32) << 8;
                        let mut same = same & !free;
                        while same != 0 {
                            let j = same.trailing_zeros() as usize;
                            same &= same - 1;
                            let kept = *note(*row.add(j) & seq_mask);
                            // Copies of one event share the hash and the 20 extra hash bits.
                            if kept.h == s.h && ((kept.x ^ s.x) >> H2_SHIFT) & H2_MASK == 0 {
                                fetch(s.x);
                                fetch(kept.x);
                                later[nl] = (r * 16 + j, s, seq);
                                nl += 1;
                                if nl == LATER {
                                    settle(&later);
                                    nl = 0;
                                }
                                break 'probe;
                            }
                        }
                        if free != 0 {
                            *row.add(free.trailing_zeros() as usize) = (tag << sb) | seq;
                            break;
                        }
                        r = (r + 1) & rmask;
                    }
                    seq += 1;
                }
            }
        }
    }
    settle(&later[..nl]);
}

/// `dedup_bucket_rows` with a plain open-addressing table of 8-byte entries:
/// for buckets too large for the other, and for processors where the
/// branch-free line compare does not pay.
fn dedup_bucket(p: usize, outs: &[&ThreadOut], table: &mut Vec<u64>) {
    const EMPTY: u64 = u64::MAX;
    let n: usize = outs.iter().map(|o| o.slim.len(p)).sum();
    if n < 2 {
        return;
    }
    let cap = (n + n / 2).next_power_of_two().max(16);
    table.clear();
    table.resize(cap, EMPTY);
    let mask = cap - 1;
    // The table holds, per event ID seen so far, 32 bits of its hash and
    // where the note of its kept copy (smallest ingest_ts so far) is: the
    // thread, and the note's number in that thread's list in IDX_BITS bits.
    assert!(outs.iter().all(|o| o.slim.len(p) <= 1 << IDX_BITS), "dedup bucket too large");
    let note = |r: u32| -> *mut Slim { outs[(r >> IDX_BITS) as usize].slim.at(p, (r & ((1 << IDX_BITS) - 1)) as usize) };
    let ev_at = |x: u64| -> *mut Ev {
        let t = (x >> (IDX_BITS + Q_BITS)) as usize & ((1 << T_BITS) - 1);
        let q = (x >> IDX_BITS) as usize & ((1 << Q_BITS) - 1);
        outs[t].ev.at(q, (x & ((1 << IDX_BITS) - 1)) as usize)
    };
    let ingest = |x: u64| -> u32 { if x & TAG != 0 { x as u32 } else { unsafe { (*ev_at(x)).ingest } } };
    // A copy that is not the kept one goes (invalid copies have no event to drop).
    let drop_copy = |x: u64| {
        if x & TAG == 0 {
            unsafe { (*ev_at(x)).ev = DEAD };
        }
    };
    // A repeat is settled a little later, in arrival order, so the events
    // it needs (far apart in memory) can be fetched meanwhile.
    const LATER: usize = 16;
    const AHEAD: usize = 12;
    let mut later = [(0usize, Slim { h: 0, x: 0 }, 0u32); LATER];
    let mut nl = 0;
    let settle = |later: &[(usize, Slim, u32)], table: &mut Vec<u64>| {
        for &(i, s, idx) in later {
            let v = table[i];
            let kept = unsafe { *note(v as u32) };
            // The copy with the smallest ingest_ts is kept, valid or not;
            // copies with equal ingest_ts are identical.
            if ingest(s.x) < ingest(kept.x) {
                drop_copy(kept.x);
                table[i] = (v & !0xFFFF_FFFF) | idx as u64;
            } else {
                drop_copy(s.x);
            }
        }
    };
    let fetch = |x: u64| {
        if x & TAG == 0 {
            unsafe { core::arch::x86_64::_mm_prefetch(ev_at(x) as *const i8, core::arch::x86_64::_MM_HINT_T0) };
        }
    };
    for (t, o) in outs.iter().enumerate() {
        let mut idx = (t as u32) << IDX_BITS;
        for (ptr, len) in o.slim.pieces(p) {
            for k in 0..len {
                // The table may not fit in cache: ask for a later note's slot now.
                if k + AHEAD < len {
                    let h = unsafe { (*ptr.add(k + AHEAD)).h };
                    unsafe { core::arch::x86_64::_mm_prefetch(table.as_ptr().add(h as usize & mask) as *const i8, core::arch::x86_64::_MM_HINT_T0) };
                }
                let s = unsafe { *ptr.add(k) };
                let tag = (s.h >> 24) as u32;
                let mut i = (s.h as usize) & mask;
                loop {
                    let v = unsafe { *table.get_unchecked(i) };
                    if v == EMPTY {
                        unsafe { *table.get_unchecked_mut(i) = ((tag as u64) << 32) | idx as u64 };
                        break;
                    }
                    if (v >> 32) as u32 == tag {
                        let kept = unsafe { *note(v as u32) };
                        // Copies of one event share the hash and the 20 extra hash bits.
                        if kept.h == s.h && ((kept.x ^ s.x) >> H2_SHIFT) & H2_MASK == 0 {
                            fetch(s.x);
                            fetch(kept.x);
                            later[nl] = (i, s, idx);
                            nl += 1;
                            if nl == LATER {
                                settle(&later, table);
                                nl = 0;
                            }
                            break;
                        }
                    }
                    i = (i + 1) & mask;
                }
                idx += 1;
            }
        }
    }
    settle(&later[..nl], table);
}

// ====================================================================== persons

#[derive(Clone, Copy)]
struct Sess {
    start: u32,
    day: u32,
    purch: bool,
    rev: u64,
    events: u32,
}

/// One half-hour of one person's events (see `person_grid`).
#[derive(Clone, Copy, Default)]
#[repr(C)]
struct Bin {
    /// Which person last used this bin.
    stamp: u32,
    /// Events in it; later, the index of the session it belongs to.
    cnt: u32,
    min: u32,
    max: u32,
}

#[derive(Clone, Copy, Default)]
struct Ckl {
    cid: Key,
    ts: u32,
    camp: u32,
}

/// What attribution needs to know about a click.
trait Click: Copy {
    fn ts(&self) -> u32;
    fn camp(&self) -> u32;
    fn same_id(&self, other: &Self) -> bool;
    /// The byte-wise order of the two click IDs.
    fn cmp_id(a: &Self, b: &Self) -> std::cmp::Ordering;
}

impl Click for Ckl {
    #[inline(always)]
    fn ts(&self) -> u32 {
        self.ts
    }
    #[inline(always)]
    fn camp(&self) -> u32 {
        self.camp
    }
    #[inline(always)]
    fn same_id(&self, other: &Self) -> bool {
        self.cid == other.cid
    }
    #[inline(always)]
    fn cmp_id(a: &Self, b: &Self) -> std::cmp::Ordering {
        cmp_key(a.cid, b.cid)
    }
}

/// user_daily rows, already in Parquet's PLAIN encoding except `cat` and
/// `user`: that holds one PLAIN entry per run of rows of the same person,
/// and `runs` the number of rows of each.
#[derive(Default)]
struct Rows {
    n: usize,
    user: Vec<u8>,
    runs: Vec<u32>,
    /// The person of the last entry of `user`.
    last: Option<Key>,
    day: Vec<u8>,
    sessions: Vec<u8>,
    events: Vec<u8>,
    rev: Vec<u8>,
    cat: Vec<u16>,
}

impl Rows {
    /// With room for about `n` rows set aside on huge pages (it is only
    /// address space until written, and the lists still grow past it).
    fn with_room(n: usize) -> Rows {
        fn big<T>(n: usize) -> Vec<T> {
            let v: Vec<T> = Vec::with_capacity(n);
            let (a, b) = (v.as_ptr() as usize, v.as_ptr() as usize + n * std::mem::size_of::<T>());
            let (a, b) = ((a + 4095) & !4095, b & !4095);
            if b > a + (4 << 20) && std::env::var_os("SS_NOHUGE").is_none() {
                unsafe { sys::madvise(a as *mut std::ffi::c_void, b - a, sys::MADV_HUGEPAGE) };
            }
            v
        }
        Rows { n: 0, user: big(n * 6), runs: big(n / 2), last: None, day: big(n * 4), sessions: big(n * 4), events: big(n * 4), rev: big(n * 8), cat: big(n) }
    }
}

struct Cells {
    sessions: Vec<u32>,
    purch: Vec<u32>,
    assisted: Vec<u32>,
    rev: Vec<u64>,
}

struct Tl {
    cnt_e: Vec<u32>,
    cnt_c: Vec<u32>,
    evs: Vec<u64>,
    cks: Vec<Ckl>,
    sess: Vec<Sess>,
    catsum: Vec<u64>,
    touched: Vec<u16>,
    tmp: Vec<u64>,
    rows: Rows,
    cells: Cells,
    nd: usize,
    day0: i32,
    organic: usize,
    dayfirst: Vec<usize>,
    /// Per campaign: the last session (by serial number) it was counted for.
    stamp: Vec<u32>,
    serial: u32,
    /// `person_grid`: half-hour bins, the bitmap of bins in use, purchases
    /// (bin, category, price), revenue per (day, category) and the best
    /// category per day.
    grid: Vec<Bin>,
    bits: Vec<u64>,
    words: (usize, usize),
    pserial: u32,
    purchases: Vec<u64>,
    daycat: Vec<u64>,
    touched_dc: Vec<u32>,
    best: Vec<u16>,
    best_sum: Vec<u64>,
    ncat: usize,
    /// Persons with at least this many events use the grid.
    grid_min: usize,
    prof: [f64; 12],
}

impl Tl {
    fn new(ctx: &Ctx, ncell: usize, rows_room: usize) -> Tl {
        let bslots = 1usize << ctx.qshift;
        // Half-hour bins of event time, counted from the time origin.
        let bin_lo = ((ctx.ws - ctx.origin).max(0) / GAP as i64) as usize;
        let bin_hi = ((ctx.we - ctx.origin).max(0) / GAP as i64) as usize;
        let nbins = bin_hi + 1;
        Tl {
            cnt_e: vec![0; bslots + 1],
            cnt_c: vec![0; bslots + 1],
            evs: Vec::new(),
            cks: Vec::new(),
            sess: Vec::new(),
            catsum: vec![0; ctx.aux.cat_names.len()],
            touched: Vec::new(),
            tmp: Vec::new(),
            rows: if rows_room > 0 { Rows::with_room(rows_room) } else { Rows::default() },
            cells: Cells {
                sessions: vec![0; ncell],
                purch: vec![0; ncell],
                assisted: vec![0; ncell],
                rev: vec![0; ncell],
            },
            nd: ctx.nd,
            day0: ctx.day0 as i32,
            organic: ctx.aux.camp_names.len(),
            dayfirst: Vec::new(),
            stamp: vec![0; ctx.aux.camp_names.len() + 1],
            serial: 0,
            grid: vec![Bin::default(); nbins + 1],
            bits: vec![0; nbins / 64 + 2],
            words: (bin_lo / 64, bin_hi / 64 + 1),
            pserial: 0,
            purchases: Vec::new(),
            daycat: vec![0; ctx.nd * ctx.aux.cat_names.len()],
            touched_dc: Vec::new(),
            best: vec![NOCAT; ctx.nd],
            best_sum: vec![0; ctx.nd],
            ncat: ctx.aux.cat_names.len(),
            grid_min: if nbins <= 1 << 14 && std::env::var_os("SS_SORT").is_none() { env_usize("SS_GRID_MIN", 0) } else { usize::MAX },
            prof: [0.0; 12],
        }
    }
}

/// Ask for the memory a few cache lines past `x` (lists are read front to back).
#[inline(always)]
fn ahead<T>(x: &T) {
    unsafe {
        core::arch::x86_64::_mm_prefetch((x as *const T as *const i8).wrapping_add(640), core::arch::x86_64::_MM_HINT_T0);
    }
}

fn person_bucket(q: usize, outs: &[&ThreadOut], ctx: &Ctx, tl: &mut Tl) {
    let bslots = 1usize << ctx.qshift;
    let base = (q << ctx.qshift) as u32;
    let smask = (bslots - 1) as u32;

    // Pass 1: count each person's rows. A person's index is their slot in this bucket.
    let t_a = Instant::now();
    tl.cnt_e.iter_mut().for_each(|c| *c = 0);
    tl.cnt_c.iter_mut().for_each(|c| *c = 0);
    let (mut ne, mut ncl) = (0u32, 0u32);
    for o in outs {
        for (ptr, len) in o.ev.pieces(q) {
            for e in unsafe { std::slice::from_raw_parts(ptr, len) } {
                ahead(e);
                if e.ev != DEAD {
                    tl.cnt_e[(e.person & smask) as usize] += 1;
                    ne += 1;
                }
            }
        }
        for (ptr, len) in o.ck.pieces(q) {
            for c in unsafe { std::slice::from_raw_parts(ptr, len) } {
                ahead(c);
                tl.cnt_c[(c.person & smask) as usize] += 1;
                ncl += 1;
            }
        }
    }
    if ne == 0 {
        return;
    }
    // Counts become start offsets.
    let t_b = Instant::now();
    tl.prof[0] += (t_b - t_a).as_secs_f64();
    let (mut se, mut sc) = (0u32, 0u32);
    for i in 0..bslots {
        let (ce, cc) = (tl.cnt_e[i], tl.cnt_c[i]);
        tl.cnt_e[i] = se;
        tl.cnt_c[i] = sc;
        se += ce;
        sc += cc;
    }
    // Pass 2: counting sort by person.
    // Every element is written below.
    tl.evs.clear();
    tl.evs.reserve(ne as usize);
    tl.cks.clear();
    tl.cks.reserve(ncl as usize);
    unsafe {
        tl.evs.set_len(ne as usize);
        tl.cks.set_len(ncl as usize);
    }
    for o in outs {
        for (ptr, len) in o.ev.pieces(q) {
            for e in unsafe { std::slice::from_raw_parts(ptr, len) } {
                ahead(e);
                if e.ev != DEAD {
                    let slot = (e.person & smask) as usize;
                    let at = tl.cnt_e[slot];
                    tl.cnt_e[slot] = at + 1;
                    unsafe { *tl.evs.get_unchecked_mut(at as usize) = e.ev };
                }
            }
        }
        for (ptr, len) in o.ck.pieces(q) {
            for c in unsafe { std::slice::from_raw_parts(ptr, len) } {
                ahead(c);
                let slot = (c.person & smask) as usize;
                let at = tl.cnt_c[slot];
                tl.cnt_c[slot] = at + 1;
                unsafe { *tl.cks.get_unchecked_mut(at as usize) = Ckl { cid: c.cid, ts: c.ts, camp: c.camp } };
            }
        }
    }
    // cnt_* now hold each person's end offset.
    let t_c = Instant::now();
    tl.prof[1] += (t_c - t_b).as_secs_f64();
    let mut evs = std::mem::take(&mut tl.evs);
    let mut cks = std::mem::take(&mut tl.cks);
    let (mut pe, mut pc) = (0usize, 0usize);
    for i in 0..bslots {
        let (ee, ce) = (tl.cnt_e[i] as usize, tl.cnt_c[i] as usize);
        // A person's name is read from the ID table, which is cold by now:
        // ask for the names of the next few persons ahead of time.
        if i + 24 < bslots && tl.cnt_e[i + 24] > tl.cnt_e[i + 23] {
            ctx.ids.prefetch_at(base | (i + 24) as u32);
        }
        if ee > pe {
            // Slots are visited in table order, so these reads are sequential.
            let big = ee - pe > 20_000;
            let t_p = if big { Some(Instant::now()) } else { None };
            let who = ctx.ids.key(base | i as u32);
            if ee - pe >= tl.grid_min {
                person_grid(who, &evs[pe..ee], tl);
            } else {
                person_sorted(who, &mut evs[pe..ee], tl);
            }
            attribute(&mut cks[pc..ce], tl);
            if let Some(t_p) = t_p {
                let took = t_p.elapsed().as_secs_f64();
                if took > tl.prof[6] {
                    tl.prof[6] = took;
                    tl.prof[7] = (ee - pe) as f64;
                    tl.prof[8] = (ce - pc) as f64;
                }
            }
        }
        pe = ee;
        pc = ce;
    }
    tl.evs = evs;
    tl.cks = cks;
    tl.prof[2] += t_c.elapsed().as_secs_f64();
    let whole = t_a.elapsed().as_secs_f64();
    if whole > tl.prof[3] {
        tl.prof[3] = whole;
        tl.prof[4] = ne as f64;
        tl.prof[5] = ncl as f64;
    }
}

/// One person's sessions and user_daily rows without sorting their events.
///
/// Events go into half-hour bins. A bin is exactly as long as the session
/// gap, so the events of one bin always share a session; two neighbouring
/// bins share one when the last event of the first and the first event of the
/// second are at most the gap apart; and an empty bin between two events
/// always separates their sessions. So each bin needs only its count and its
/// earliest and latest event.
fn person_grid(who: Key, evs: &[u64], tl: &mut Tl) {
    let serial = tl.pserial + 1;
    tl.pserial = serial;
    let grid = tl.grid.as_mut_ptr();
    let bits = tl.bits.as_mut_ptr();
    tl.purchases.clear();
    for &e in evs {
        let ts = (e >> 33) as u32;
        let b = (ts / GAP) as usize;
        unsafe {
            let c = &mut *grid.add(b);
            let fresh = c.stamp != serial;
            c.stamp = serial;
            let mn = std::hint::select_unpredictable(fresh, ts, c.min);
            let mx = std::hint::select_unpredictable(fresh, ts, c.max);
            let cn = std::hint::select_unpredictable(fresh, 0, c.cnt);
            c.min = mn.min(ts);
            c.max = mx.max(ts);
            c.cnt = cn + 1;
            *bits.add(b >> 6) |= 1u64 << (b & 63);
        }
        if e & 0xFF_FFFF != 0 {
            tl.purchases.push(((b as u64) << 40) | (e & 0x1_FFFF_FFFF));
        }
    }

    // Sessions, in time order.
    tl.sess.clear();
    let (mut prev_b, mut prev_max) = (usize::MAX - 1, 0u32);
    for w in tl.words.0..tl.words.1 {
        let mut m = unsafe { *bits.add(w) };
        if m == 0 {
            continue;
        }
        unsafe { *bits.add(w) = 0 };
        while m != 0 {
            let b = w * 64 + m.trailing_zeros() as usize;
            m &= m - 1;
            let c = unsafe { &mut *grid.add(b) };
            if b == prev_b + 1 && c.min - prev_max <= GAP {
                let last = tl.sess.len() - 1;
                unsafe { tl.sess.get_unchecked_mut(last).events += c.cnt };
            } else {
                tl.sess.push(Sess { start: c.min, day: c.min / DAY_MS - 7, purch: false, rev: 0, events: c.cnt });
            }
            c.cnt = (tl.sess.len() - 1) as u32;
            prev_b = b;
            prev_max = c.max;
        }
    }

    // Purchases: revenue per session and per (day, category).
    let ncat = tl.ncat;
    for &p in &tl.purchases {
        let b = (p >> 40) as usize;
        let cat = ((p >> 24) & 0x1FF) as usize;
        let price = p & 0xFF_FFFF;
        let s = &mut tl.sess[unsafe { (*grid.add(b)).cnt } as usize];
        s.rev += price;
        s.purch = true;
        let k = s.day as usize * ncat + cat;
        if tl.daycat[k] == 0 {
            tl.touched_dc.push(k as u32);
        }
        tl.daycat[k] += price;
    }
    for &k in &tl.touched_dc {
        let (d, c) = (k as usize / ncat, (k as usize % ncat) as u16);
        let s = tl.daycat[k as usize];
        tl.daycat[k as usize] = 0;
        // Categories are ranked byte-wise, so the smaller index wins a tie.
        if s > tl.best_sum[d] || (s == tl.best_sum[d] && c < tl.best[d]) {
            tl.best[d] = c;
            tl.best_sum[d] = s;
        }
    }
    tl.touched_dc.clear();

    // One row per day with a session.
    let n = tl.sess.len();
    let mut i = 0;
    while i < n {
        let day = tl.sess[i].day;
        let (mut d_sessions, mut d_events, mut d_rev) = (0u32, 0u32, 0u64);
        while i < n && tl.sess[i].day == day {
            d_sessions += 1;
            d_events += tl.sess[i].events;
            d_rev += tl.sess[i].rev;
            i += 1;
        }
        let best = tl.best[day as usize];
        tl.best[day as usize] = NOCAT;
        tl.best_sum[day as usize] = 0;
        emit_row(who, day, d_sessions, d_events, d_rev, best, tl);
    }
}

/// One person's sessions and user_daily rows by sorting their events: the
/// plain reading of the rules, used for odd windows and to check `person_grid`.
fn person_sorted(who: Key, evs: &mut [u64], tl: &mut Tl) {
    evs.sort_unstable();
    let n = evs.len();

    tl.sess.clear();
    let mut cur_day = u32::MAX;
    let (mut d_sessions, mut d_events, mut d_rev) = (0u32, 0u32, 0u64);
    let mut i = 0;
    while i < n {
        let start = (evs[i] >> 33) as u32;
        let day = start / DAY_MS - 7;
        if day != cur_day {
            if cur_day != u32::MAX {
                flush_row(who, cur_day, d_sessions, d_events, d_rev, tl);
            }
            cur_day = day;
            d_sessions = 0;
            d_events = 0;
            d_rev = 0;
        }
        let mut prev = start;
        let (mut nev, mut rev, mut purch) = (0u32, 0u64, false);
        while i < n {
            let e = evs[i];
            let ts = (e >> 33) as u32;
            if ts - prev > GAP {
                break;
            }
            prev = ts;
            nev += 1;
            let price = e & 0xFF_FFFF;
            if price > 0 {
                purch = true;
                rev += price;
                let cat = ((e >> 24) & 0x1FF) as usize;
                if tl.catsum[cat] == 0 {
                    tl.touched.push(cat as u16);
                }
                tl.catsum[cat] += price;
            }
            i += 1;
        }
        tl.sess.push(Sess { start, day, purch, rev, events: nev });
        d_sessions += 1;
        d_events += nev;
        d_rev += rev;
    }
    flush_row(who, cur_day, d_sessions, d_events, d_rev, tl);
}

/// Credit and assists for the sessions in `tl.sess` (in time order).
fn attribute<C: Click>(cks: &mut [C], tl: &mut Tl) {
    let nd = tl.nd;
    let organic = tl.organic;
    if cks.is_empty() {
        for s in &tl.sess {
            let cell = organic * nd + s.day as usize;
            tl.cells.sessions[cell] += 1;
            tl.cells.purch[cell] += s.purch as u32;
            tl.cells.rev[cell] += s.rev;
        }
        return;
    }
    // Latest ts last; among equal ts the smallest click_id last.
    let mut nc = 1;
    if cks.len() > 1 {
        cks.sort_unstable_by(|a, b| a.ts().cmp(&b.ts()).then_with(|| C::cmp_id(b, a)));
        // Copies of one click are identical, so they are neighbours now.
        for k in 1..cks.len() {
            if !cks[k].same_id(&cks[nc - 1]) {
                cks[nc] = cks[k];
                nc += 1;
            }
        }
    }
    let cks = &cks[..nc];
    if tl.sess.len() * nc <= 16384 {
        // The common, small case: per session, walk the clicks in the 7 days
        // up to its start. The latest credits it; each campaign among them
        // assists it once (`stamp` remembers which were already counted).
        let (sess, stamp, cells) = (&tl.sess, &mut tl.stamp, &mut tl.cells);
        let mut serial = tl.serial;
        let (mut lo, mut hi) = (0usize, 0usize);
        for s in sess {
            while hi < nc && cks[hi].ts() <= s.start {
                hi += 1;
            }
            while lo < hi && s.start - cks[lo].ts() > WEEK {
                lo += 1;
            }
            let day = s.day as usize;
            let camp = if lo < hi { cks[hi - 1].camp() as usize } else { organic };
            let cell = camp * nd + day;
            cells.sessions[cell] += 1;
            cells.purch[cell] += s.purch as u32;
            cells.rev[cell] += s.rev;
            serial += 1;
            for c in &cks[lo..hi] {
                // (Counted without branching on whether the campaign is new here.)
                let camp = c.camp() as usize;
                let new = (stamp[camp] != serial) as u32;
                stamp[camp] = serial;
                cells.assisted[camp * nd + day] += new;
            }
        }
        tl.serial = serial;
        return;
    }
    let mut hi = 0;
    for s in &tl.sess {
        while hi < nc && cks[hi].ts() <= s.start {
            hi += 1;
        }
        let camp = if hi > 0 && s.start - cks[hi - 1].ts() <= WEEK { cks[hi - 1].camp() as usize } else { organic };
        let cell = camp * nd + s.day as usize;
        tl.cells.sessions[cell] += 1;
        tl.cells.purch[cell] += s.purch as u32;
        tl.cells.rev[cell] += s.rev;
    }

    // ---- assists for a person with many sessions and clicks: per campaign,
    // the union of [click, click + 7 days] over its clicks; every session
    // starting inside is assisted once.
    tl.tmp.clear();
    tl.tmp.extend(cks.iter().map(|c| ((c.camp() as u64) << 32) | c.ts() as u64));
    tl.tmp.sort_unstable();
    let sess = &tl.sess;
    // dayfirst[d]: the index of the first session on day d or later.
    tl.dayfirst.clear();
    tl.dayfirst.resize(nd + 1, sess.len());
    for (k, s) in sess.iter().enumerate().rev() {
        tl.dayfirst[s.day as usize] = k;
    }
    for d in (0..nd).rev() {
        tl.dayfirst[d] = tl.dayfirst[d].min(tl.dayfirst[d + 1]);
    }
    let dayfirst = &tl.dayfirst;
    let assisted = &mut tl.cells.assisted;
    let mut flush = |camp: usize, a: u32, b: u32| {
        let lo = sess.partition_point(|s| s.start < a);
        let hi = lo + sess[lo..].partition_point(|s| s.start <= b);
        if hi > lo {
            for d in sess[lo].day as usize..=sess[hi - 1].day as usize {
                let n = hi.min(dayfirst[d + 1]).saturating_sub(lo.max(dayfirst[d]));
                assisted[camp * nd + d] += n as u32;
            }
        }
    };
    let (mut cur_camp, mut a, mut b) = (u32::MAX, 0u32, 0u32);
    for &v in &tl.tmp {
        let (camp, ts) = ((v >> 32) as u32, v as u32);
        if camp != cur_camp || ts > b {
            if cur_camp != u32::MAX {
                flush(cur_camp as usize, a, b);
            }
            cur_camp = camp;
            a = ts;
        }
        b = ts + WEEK;
    }
    flush(cur_camp as usize, a, b);
}

#[inline(always)]
fn flush_row(who: Key, day: u32, sessions: u32, events: u32, rev: u64, tl: &mut Tl) {
    let mut best = NOCAT;
    let mut best_sum = 0u64;
    for &c in &tl.touched {
        let s = tl.catsum[c as usize];
        tl.catsum[c as usize] = 0;
        // Categories are ranked byte-wise, so the smaller index wins a tie.
        if s > best_sum || (s == best_sum && c < best) {
            best = c;
            best_sum = s;
        }
    }
    tl.touched.clear();
    emit_row(who, day, sessions, events, rev, best, tl);
}

#[inline(always)]
fn emit_row(who: Key, day: u32, sessions: u32, events: u32, rev: u64, best: u16, tl: &mut Tl) {
    let rows = &mut tl.rows;
    rows.n += 1;
    user_run(rows, who, 1);
    rows.day.extend_from_slice(&(tl.day0 + day as i32).to_le_bytes());
    rows.sessions.extend_from_slice(&sessions.to_le_bytes());
    rows.events.extend_from_slice(&events.to_le_bytes());
    rows.rev.extend_from_slice(&rev.to_le_bytes());
    rows.cat.push(best);
}

/// The person of the next `k` rows (pushed before or after this): extends
/// the last run when it is theirs, else starts one. (Equal keys are equal
/// IDs, so a run is always one ID.)
#[inline(always)]
fn user_run(rows: &mut Rows, who: Key, k: u32) {
    if rows.last == Some(who) {
        *rows.runs.last_mut().unwrap() += k;
        return;
    }
    rows.last = Some(who);
    rows.runs.push(k);
    if is_long(who) {
        let mut bytes = Vec::new();
        key_bytes(who, &mut bytes);
        rows.user.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        rows.user.extend_from_slice(&bytes);
    } else {
        // A 4-byte length, then the ID: write all 16 key bytes and keep `len`.
        let len = (who >> 120) as usize;
        rows.user.reserve(20);
        unsafe {
            let at = rows.user.len();
            let p = rows.user.as_mut_ptr().add(at);
            (p as *mut u32).write_unaligned(len as u32);
            (p.add(4) as *mut u128).write_unaligned(who);
            rows.user.set_len(at + 4 + len);
        }
    }
}

// ====================================================================== output

fn plain_bytes<'a>(out: &mut Vec<u8>, values: impl Iterator<Item = &'a [u8]>) {
    for v in values {
        out.extend_from_slice(&(v.len() as u32).to_le_bytes());
        out.extend_from_slice(v);
    }
}

/// user_id: a dictionary of the runs' entries and an RLE run per run of
/// rows, when that is smaller than PLAIN (one entry per row).
fn user_column(entries: Vec<u8>, runs: &[u32], n: usize) -> (Option<(Vec<u8>, usize)>, Vec<u8>) {
    let d = runs.len();
    debug_assert_eq!(runs.iter().map(|&r| r as usize).sum::<usize>(), n);
    if d == 0 {
        return (None, entries);
    }
    let width = parquet::bit_width(d as u32 - 1).max(1);
    let vb = (width as usize).div_ceil(8);
    let mut data: Vec<u8> = Vec::with_capacity(1 + d * (6 + vb) + 8);
    let mut plain = 0usize;
    let mut at = 0usize;
    unsafe {
        let (src, op) = (entries.as_ptr(), data.as_mut_ptr());
        *op = width as u8;
        let mut o = 1usize;
        for (i, &r) in runs.iter().enumerate() {
            let size = 4 + (src.add(at) as *const u32).read_unaligned() as usize;
            plain += size * r as usize;
            at += size;
            let mut h = (r as u64) << 1;
            while h >= 0x80 {
                *op.add(o) = (h as u8) | 0x80;
                o += 1;
                h >>= 7;
            }
            // The header's last byte, then the index (vb of its 4 bytes).
            (op.add(o) as *mut u64).write_unaligned(h | (i as u64) << 8);
            o += 1 + vb;
        }
        data.set_len(o);
    }
    debug_assert_eq!(at, entries.len());
    if entries.len() + data.len() < plain {
        return (Some((entries, d)), data);
    }
    // Every entry once per row.
    let mut out = Vec::with_capacity(plain);
    let mut at = 0usize;
    for &r in runs {
        let size = 4 + u32::from_le_bytes(entries[at..at + 4].try_into().unwrap()) as usize;
        for _ in 0..r {
            out.extend_from_slice(&entries[at..at + size]);
        }
        at += size;
    }
    (None, out)
}

fn write_user_daily(path: &Path, rows: &mut Rows, ctx: &Ctx) {
    let n = rows.n;
    if SUM_ON.load(Relaxed) {
        let (mut at, mut sum) = (0usize, 0u64);
        let (mut run, mut left, mut len) = (0usize, 0u32, 0usize);
        for i in 0..n {
            if left == 0 {
                if run > 0 {
                    at += 4 + len;
                }
                len = u32::from_le_bytes(rows.user[at..at + 4].try_into().unwrap()) as usize;
                left = rows.runs[run];
                run += 1;
            }
            left -= 1;
            let mut h = len as u64;
            for &b in &rows.user[at + 4..at + 4 + len] {
                h = mix(h, b as u64);
            }
            let w = |v: &Vec<u8>| u32::from_le_bytes(v[4 * i..4 * i + 4].try_into().unwrap()) as u64;
            for v in [w(&rows.day), w(&rows.sessions), w(&rows.events), u64::from_le_bytes(rows.rev[8 * i..8 * i + 8].try_into().unwrap()), rows.cat[i] as u64] {
                h = mix(h, v);
            }
            sum = sum.wrapping_add(h);
        }
        SUMS[0].fetch_add(sum, Relaxed);
        SUMS[1].fetch_add(n as u64, Relaxed);
    }
    let mut dict = Vec::new();
    plain_bytes(&mut dict, ctx.aux.cat_names.iter().map(|c| c.as_slice()));
    let width = parquet::bit_width(ctx.aux.cat_names.len() as u32 - 1).max(1);
    let mut cat = Vec::with_capacity(n);
    parquet::def_levels(&mut cat, n, |i| rows.cat[i] != NOCAT);
    let present: Vec<u32> = rows.cat.iter().filter(|&&c| c != NOCAT).map(|&c| c as u32).collect();
    if !present.is_empty() {
        parquet::dict_indices(&mut cat, present.len(), width, |i| present[i]);
    } else {
        cat.push(width as u8);
    }
    let col = |name, ptype, optional, logical, dict, data| Column { name, ptype, optional, logical, dict, data };
    // A column as a dictionary page and bit-packed indices when that is
    // smaller, else PLAIN as it was pushed.
    let enc = |dict: Option<(Vec<u8>, usize, Vec<u8>)>, plain: Vec<u8>| match dict {
        Some((d, nd, data)) => (Some((d, nd)), data),
        None => (None, plain),
    };
    let user = user_column(std::mem::take(&mut rows.user), &rows.runs, n);
    rows.runs.clear();
    rows.last = None;
    let i32col = |plain: Vec<u8>| enc(parquet::dict_i32(&plain), plain);
    let day = i32col(std::mem::take(&mut rows.day));
    let sessions = i32col(std::mem::take(&mut rows.sessions));
    let events = i32col(std::mem::take(&mut rows.events));
    // revenue_cents as INT32 when every value fits (it is never negative).
    let mut rev = std::mem::take(&mut rows.rev);
    let narrow = rev.chunks_exact(8).fold(0u64, |m, c| m.max(u64::from_le_bytes(c.try_into().unwrap()))) <= i32::MAX as u64;
    let (rev_type, rev) = if narrow {
        // In place: value i moves from 8i to 4i, never ahead of a value not read yet.
        unsafe {
            let p = rev.as_mut_ptr();
            for i in 0..n {
                let v = (p.add(8 * i) as *const u64).read_unaligned();
                (p.add(4 * i) as *mut u32).write_unaligned(v as u32);
            }
        }
        rev.truncate(4 * n);
        (parquet::T_INT32, i32col(rev))
    } else {
        (parquet::T_INT64, (None, rev))
    };
    let columns = [
        col("user_id", parquet::T_BYTE_ARRAY, false, Logical::String, user.0, user.1),
        col("day", parquet::T_INT32, false, Logical::Date, day.0, day.1),
        col("sessions", parquet::T_INT32, false, Logical::None, sessions.0, sessions.1),
        col("events", parquet::T_INT32, false, Logical::None, events.0, events.1),
        col("revenue_cents", rev_type, false, Logical::None, rev.0, rev.1),
        col("top_category", parquet::T_BYTE_ARRAY, true, Logical::String, Some((dict, ctx.aux.cat_names.len())), cat),
    ];
    parquet::write_file(path, &columns, n).unwrap();
}

fn write_campaign_daily(path: &Path, cells: &[Cells], ctx: &Ctx) {
    let nd = ctx.nd;
    let ncamp = ctx.aux.camp_names.len();
    let (mut id, mut channel, mut day) = (Vec::new(), Vec::new(), Vec::new());
    let (mut sessions, mut purch, mut rev, mut assisted) = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    let mut n = 0;
    for c in 0..=ncamp {
        for d in 0..nd {
            let cell = c * nd + d;
            let s: u64 = cells.iter().map(|x| x.sessions[cell] as u64).sum();
            let a: u64 = cells.iter().map(|x| x.assisted[cell] as u64).sum();
            if s == 0 && a == 0 {
                continue;
            }
            let p: u64 = cells.iter().map(|x| x.purch[cell] as u64).sum();
            let r: u64 = cells.iter().map(|x| x.rev[cell]).sum();
            let (name, chan): (&[u8], &[u8]) =
                if c == ncamp { (b"organic", b"organic") } else { (&ctx.aux.camp_names[c], &ctx.aux.camp_channel[c]) };
            if SUM_ON.load(Relaxed) {
                let mut h = 7u64;
                for v in [c as u64, d as u64, s, p, r, a] {
                    h = mix(h, v);
                }
                SUMS[2].fetch_add(h, Relaxed);
                SUMS[3].fetch_add(1, Relaxed);
            }
            plain_bytes(&mut id, std::iter::once(name));
            plain_bytes(&mut channel, std::iter::once(chan));
            day.extend_from_slice(&((ctx.day0 + d as i64) as i32).to_le_bytes());
            sessions.extend_from_slice(&(s as i64).to_le_bytes());
            purch.extend_from_slice(&(p as i64).to_le_bytes());
            rev.extend_from_slice(&(r as i64).to_le_bytes());
            assisted.extend_from_slice(&(a as i64).to_le_bytes());
            n += 1;
        }
    }
    let col = |name, ptype, logical, data| Column { name, ptype, optional: false, logical, dict: None, data };
    let columns = [
        col("campaign_id", parquet::T_BYTE_ARRAY, Logical::String, id),
        col("channel", parquet::T_BYTE_ARRAY, Logical::String, channel),
        col("day", parquet::T_INT32, Logical::Date, day),
        col("sessions", parquet::T_INT64, Logical::None, sessions),
        col("purchasing_sessions", parquet::T_INT64, Logical::None, purch),
        col("revenue_cents", parquet::T_INT64, Logical::None, rev),
        col("assisted_sessions", parquet::T_INT64, Logical::None, assisted),
    ];
    parquet::write_file(path, &columns, n).unwrap();
}
