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

mod fast;
mod input;
mod parquet;
#[cfg(feature = "sprof")]
mod sprof;
mod table;
mod util;

use std::cell::UnsafeCell;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use parquet::{Column, Logical};
use table::*;
use util::*;

const DEAD: u64 = u64::MAX;
const TAG: u64 = 1 << 63;
const H2_SHIFT: u32 = 42;
const H2_MASK: u64 = 0xF_FFFF;
const IDX_BITS: u32 = 26;
const Q_BITS: u32 = 10;
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

type EvStreams = Streams<Ev, 12>;
type SlimStreams = Streams<Slim, 12>;
type CkStreams = Streams<Ck, 10>;

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
    ev: EvStreams,
    ck: CkStreams,
    slim: SlimStreams,
    dates: fast::Dates,
    reader: input::Reader,
    pend_e: Vec<PendE>,
    pend_c: Vec<PendC>,
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
    // The ID table is sized from the input's size; if that guess is ever too
    // small, start over with a bigger one.
    #[cfg(feature = "sprof")]
    sprof::start();
    let mut scale = 1;
    while !run(&data_dir, &out_dir, threads, scale, t0) {
        scale *= 4;
    }
    #[cfg(feature = "sprof")]
    sprof::dump();
    // Skip destructors: the process is done.
    std::process::exit(0);
}

/// Run `f(tid)` on every thread.
fn par<F: Fn(usize) + Sync>(threads: usize, f: F) {
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
    let lap = |what: &str| {
        if log {
            eprintln!("{:>8.1} ms  {}", t0.elapsed().as_secs_f64() * 1e3, what);
        }
    };

    let event_files = input::list_dir(&data_dir.join("events"));
    let click_files = input::list_dir(&data_dir.join("clicks"));
    let link_files = input::list_dir(&data_dir.join("links"));
    let bytes = |files: &[input::Src]| files.iter().map(|s| s.len).sum::<usize>();
    let mut ctx = build_ctx(data_dir, bytes(&event_files), bytes(&click_files), bytes(&link_files), scale);
    let chunk = env_usize("SS_CHUNK", 128 << 10);
    lap("setup");

    let outs: Outs = (0..threads)
        .map(|_| {
            Shared(UnsafeCell::new(ThreadOut {
                arena: Arena::new(),
                ev: Streams::new(ctx.nq),
                ck: Streams::new(ctx.nq),
                slim: Streams::new(1 << ctx.pbits),
                dates: fast::Dates::new(),
                reader: input::Reader::new(chunk),
                pend_e: Vec::with_capacity(PEND),
                pend_c: Vec::with_capacity(PEND),
                pend_l: Vec::with_capacity(PEND),
            }))
        })
        .collect();

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
                let (start, end) = out.reader.load(&link_files[task.file], task.a, task.b);
                let buf = std::mem::take(&mut out.reader.buf);
                parse_links(&buf[..end + 64], start, end, &ctx, out);
                out.reader.buf = buf;
            }
            flush_links(&ctx, out);
        });
        let next = AtomicUsize::new(0);
        par(threads, |_| loop {
            let r = next.fetch_add(1, Ordering::Relaxed);
            if r >= ctx.ids.nregions {
                break;
            }
            ctx.ids.flatten_region(r);
        });
        aux.join().unwrap()
    });
    if ctx.ids.overflow.load(Ordering::Relaxed) {
        return false;
    }
    ctx.aux = aux;
    lap("links");

    // ------------------------------------------------------------ parse
    let mut tasks = Vec::new();
    grid_tasks(&event_files, true, chunk, &mut tasks);
    grid_tasks(&click_files, false, chunk, &mut tasks);
    let next = AtomicUsize::new(0);
    par(threads, |tid| {
        let out = out_mut(&outs, tid);
        loop {
            let i = next.fetch_add(1, Ordering::Relaxed);
            let Some(task) = tasks.get(i) else { break };
            let src = if task.events { &event_files[task.file] } else { &click_files[task.file] };
            let (start, end) = out.reader.load(src, task.a, task.b);
            let buf = std::mem::take(&mut out.reader.buf);
            if task.events {
                parse_events(&buf[..end + 64], start, end, &ctx, out, tid);
            } else {
                parse_clicks(&buf[..end + 64], start, end, &ctx, out);
            }
            out.reader.buf = buf;
        }
        flush_events(&ctx, out, tid);
        flush_clicks(&ctx, out);
        out.ev.finish(&mut out.arena);
        out.ck.finish(&mut out.arena);
        out.slim.finish(&mut out.arena);
    });
    if ctx.ids.overflow.load(Ordering::Relaxed) {
        return false;
    }
    if log {
        eprintln!("    ids {} in {} slots, nq {}, pbits {}", ctx.ids.len(), ctx.ids.nregions * RSIZE, ctx.nq, ctx.pbits);
    }
    lap("parse");
    // From here on the per-thread outputs are shared read-only (dedup marks
    // dead events through raw pointers, one owner per event).
    let outs_ref: Vec<&ThreadOut> = (0..threads).map(|t| &*out_mut(&outs, t)).collect();

    // ------------------------------------------------------------ dedup
    let next = AtomicUsize::new(0);
    par(threads, |_| {
        let mut table: Vec<u64> = Vec::new();
        loop {
            let p = next.fetch_add(1, Ordering::Relaxed);
            if p >= 1 << ctx.pbits {
                break;
            }
            dedup_bucket(p, &outs_ref, &mut table);
        }
    });
    lap("dedup");

    // ------------------------------------------------------------ persons
    let user_dir = out_dir.join("user_daily");
    let camp_dir = out_dir.join("campaign_daily");
    std::fs::create_dir_all(&user_dir).unwrap();
    std::fs::create_dir_all(&camp_dir).unwrap();

    let ncell = (ctx.aux.camp_names.len() + 1) * ctx.nd;
    let next = AtomicUsize::new(0);
    let cells: Vec<Shared<UnsafeCell<Option<Cells>>>> = (0..threads).map(|_| Shared(UnsafeCell::new(None))).collect();
    par(threads, |tid| {
        let mut tl = Tl::new(&ctx, ncell);
        loop {
            let q = next.fetch_add(1, Ordering::Relaxed);
            if q >= ctx.nq {
                break;
            }
            person_bucket(q, &outs_ref, &ctx, &mut tl);
        }
        let t_w = Instant::now();
        if tid == 0 || tl.rows.n > 0 {
            write_user_daily(&user_dir.join(format!("part-{tid:03}.parquet")), &mut tl.rows, &ctx);
        }
        if log {
            eprintln!("    t{tid}: count {:.0} place {:.0} persons {:.0} write {:.0} ms | Mcycles sort {:.0} sess {:.0} attr {:.0}", tl.prof[0] * 1e3, tl.prof[1] * 1e3, tl.prof[2] * 1e3, t_w.elapsed().as_secs_f64() * 1e3, tl.prof[3] / 1e6, tl.prof[4] / 1e6, tl.prof[5] / 1e6);
            eprintln!("       attr: cksort {:.0} credit {:.0} tmpsort {:.0} flush {:.0} Mcycles; persons_with_clicks {:.0} flushes {:.0}", tl.prof[6] / 1e6, tl.prof[7] / 1e6, tl.prof[8] / 1e6, tl.prof[9] / 1e6, tl.prof[10], tl.prof[11]);
        }
        unsafe { *cells[tid].0.get() = Some(tl.cells) };
    });
    lap("persons + user_daily");

    let cells: Vec<Cells> = cells.into_iter().map(|c| c.0.into_inner().unwrap()).collect();
    write_campaign_daily(&camp_dir.join("part-000.parquet"), &cells, &ctx);
    lap("campaign_daily");
    true
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
    let event_lines = event_bytes / 64 + 1;
    let ids_guess = event_lines / 8 + click_bytes / 512 + link_bytes / 64 + 1024;
    let nregions = ((ids_guess * scale).div_ceil(RSIZE)).max(4);
    let rpb = nregions.div_ceil(env_usize("SS_NQ", 400)).next_power_of_two();
    let qshift = (RBITS + rpb.trailing_zeros() as usize) as u32;
    let nq = nregions.div_ceil(rpb);
    assert!(nq <= 1 << Q_BITS);
    // Dedup buckets of about 32k lines each.
    let pbits = ((event_lines / 32_768).max(1).next_power_of_two().trailing_zeros()).clamp(4, 11);

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

fn exp() -> u32 {
    static EXP: std::sync::OnceLock<u32> = std::sync::OnceLock::new();
    *EXP.get_or_init(|| std::env::var("SS_EXP").ok().and_then(|v| v.parse().ok()).unwrap_or(0))
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
    for &(a, ha, b, hb) in &out.pend_l {
        let ia = ctx.ids.get_or_insert(a, ha);
        let ib = ctx.ids.get_or_insert(b, hb);
        ctx.ids.union(ia, ib);
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
#[derive(Clone, Copy)]
struct PendE {
    user: Key,
    ev: u64,
    h: u64,
    /// The extra hash bits in place, and the ingest time in the low 32 bits.
    x: u64,
}

#[derive(Clone, Copy)]
struct PendC {
    user: Key,
    uh: u64,
    cid: Key,
    ts: u32,
    camp: u32,
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

/// Record one parsed event line: `ev` is `Some` when the copy is valid.
#[cfg_attr(feature = "prof", inline(never))]
#[cfg_attr(not(feature = "prof"), inline(always))]
fn emit_event(ctx: &Ctx, out: &mut ThreadOut, tid: usize, ek: Key, user: Key, ev: Option<u64>, ing: Option<i64>) {
    let (h, hb) = hash_pair(ek);
    let h2 = (hb >> 20) & H2_MASK;
    let ingest = ingest32(ctx, ing);
    match ev {
        Some(ev) => {
            ctx.ids.prefetch(hash64(user));
            out.pend_e.push(PendE { user, ev, h, x: (h2 << H2_SHIFT) | ingest as u64 });
            if out.pend_e.len() == PEND {
                flush_events(ctx, out, tid);
            }
        }
        None => {
            let p = (h >> (64 - ctx.pbits)) as usize;
            out.slim.push(p, Slim { h, x: TAG | (h2 << H2_SHIFT) | ingest as u64 }, &mut out.arena);
        }
    }
}

#[cfg_attr(feature = "prof", inline(never))]
#[cfg_attr(not(feature = "prof"), inline(always))]
fn flush_events(ctx: &Ctx, out: &mut ThreadOut, tid: usize) {
    for i in 0..out.pend_e.len() {
        let pe = unsafe { *out.pend_e.get_unchecked(i) };
        let person = ctx.ids.root(ctx.ids.get_or_insert(pe.user, hash64(pe.user)));
        let q = (person >> ctx.qshift) as usize;
        let idx = out.ev.push(q, Ev { ev: pe.ev, person, ingest: pe.x as u32 }, &mut out.arena);
        assert!(idx < 1 << IDX_BITS, "bucket too large");
        let p = (pe.h >> (64 - ctx.pbits)) as usize;
        let x = (pe.x & (H2_MASK << H2_SHIFT)) | ((tid as u64) << (IDX_BITS + Q_BITS)) | ((q as u64) << IDX_BITS) | idx as u64;
        out.slim.push(p, Slim { h: pe.h, x }, &mut out.arena);
    }
    out.pend_e.clear();
}

#[cfg_attr(feature = "prof", inline(never))]
#[cfg_attr(not(feature = "prof"), inline(always))]
fn emit_click(ctx: &Ctx, out: &mut ThreadOut, user: Key, cid: Key, ts: u32, camp: u32) {
    let uh = hash64(user);
    ctx.ids.prefetch(uh);
    out.pend_c.push(PendC { user, uh, cid, ts, camp });
    if out.pend_c.len() == PEND {
        flush_clicks(ctx, out);
    }
}

#[cfg_attr(feature = "prof", inline(never))]
#[cfg_attr(not(feature = "prof"), inline(always))]
fn flush_clicks(ctx: &Ctx, out: &mut ThreadOut) {
    for i in 0..out.pend_c.len() {
        let c = out.pend_c[i];
        let person = ctx.ids.root(ctx.ids.get_or_insert(c.user, c.uh));
        let q = (person >> ctx.qshift) as usize;
        out.ck.push(q, Ck { cid: c.cid, person, ts: c.ts, camp: c.camp, _pad: 0 }, &mut out.arena);
    }
    out.pend_c.clear();
}

fn parse_events(file: &[u8], start: usize, end: usize, ctx: &Ctx, out: &mut ThreadOut, tid: usize) {
    let base = file.as_ptr();
    let exp = exp();
    if exp == 3 {
        return;
    }
    if exp == 4 || exp == 5 {
        let mut sum = 0u64;
        let mut i = start;
        while i < end {
            sum += file[i] as u64;
            i += if exp == 4 { 64 } else { 4096 };
        }
        std::hint::black_box(sum);
        return;
    }
    let rest = if slow_only() {
        start
    } else {
        unsafe {
            fast::scan(file, start, end, |ls, commas, nc, le, nq| {
                if nc != 6 || exp == 1 {
                    return;
                }
                let mut s = [ls, commas[0] + 1, commas[1] + 1, commas[2] + 1, commas[3] + 1, commas[4] + 1, commas[5] + 1];
                let mut e = [commas[0], commas[1], commas[2], commas[3], commas[4], commas[5], le];
                let mut k = 0;
                for i in 0..7 {
                    k += fast::strip(base, &mut s[i], &mut e[i]);
                }
                if nq != 2 * k || e[0] == s[0] {
                    return;
                }
                let ek = fast::pack_at(base, s[0], e[0]);
                let ing = out.dates.f1(base, s[3], e[3]);
                let mut ev = None;
                if e[1] > s[1] && e[5] > s[5] {
                    if let Some(t) = out.dates.ts(base, s[2], e[2]) {
                        if t >= ctx.ws && t < ctx.we && ing.is_some_and(|i| i - t <= DAY) {
                            let off = ((t - ctx.origin) as u64) << 33;
                            let tn = e[4] - s[4];
                            let tp = base.add(s[4]);
                            if tn == 4 {
                                let w = std::ptr::read_unaligned(tp as *const u32);
                                if w == u32::from_le_bytes(*b"view") || w == u32::from_le_bytes(*b"cart") {
                                    ev = Some(off);
                                }
                            } else if tn == 8
                                && std::ptr::read_unaligned(tp as *const u64) == u64::from_le_bytes(*b"purchase")
                            {
                                let price = std::slice::from_raw_parts(base.add(s[6]), e[6] - s[6]);
                                if let Some(p) = parse_price(price) {
                                    let cat = ctx.aux.products.get(fast::pack_at(base, s[5], e[5])).unwrap_or(ctx.aux.unknown);
                                    ev = Some(off | ((cat as u64) << 24) | p as u64);
                                }
                            }
                        }
                    }
                }
                let user = if ev.is_some() { fast::pack_at(base, s[1], e[1]) } else { 0 };
                if exp == 2 {
                    std::hint::black_box((ek, user, ev, ing));
                    return;
                }
                emit_event(ctx, out, tid, ek, user, ev, ing);
            })
        }
    };
    // The scalar reference path: the tail of a file, and everything under SS_SLOW.
    for line in lines(&file[rest..end]) {
        let Some([eid, user, ts, ingest, typ, prod, price]) = split::<7>(line) else { continue };
        if eid.is_empty() {
            continue;
        }
        let ek = pack(eid);
        let ing = parse_f1(ingest);
        let mut ev = None;
        if !user.is_empty() && !prod.is_empty() {
            if let Some(t) = parse_ts(ts) {
                // In the window, and not ingested more than 24 hours late.
                if t >= ctx.ws && t < ctx.we && ing.is_some_and(|i| i - t <= DAY) {
                    let off = ((t - ctx.origin) as u64) << 33;
                    if typ == b"view" || typ == b"cart" {
                        ev = Some(off);
                    } else if typ == b"purchase" {
                        if let Some(p) = parse_price(price) {
                            let cat = ctx.aux.products.get(pack(prod)).unwrap_or(ctx.aux.unknown);
                            ev = Some(off | ((cat as u64) << 24) | p as u64);
                        }
                    }
                }
            }
        }
        let user = if ev.is_some() { pack(user) } else { 0 };
        emit_event(ctx, out, tid, ek, user, ev, ing);
    }
}

fn parse_clicks(file: &[u8], start: usize, end: usize, ctx: &Ctx, out: &mut ThreadOut) {
    let base = file.as_ptr();
    let rest = if slow_only() {
        start
    } else {
        unsafe {
            fast::scan(file, start, end, |ls, commas, nc, le, nq| {
                if nc != 3 {
                    return;
                }
                let mut s = [ls, commas[0] + 1, commas[1] + 1, commas[2] + 1];
                let mut e = [commas[0], commas[1], commas[2], le];
                let mut k = 0;
                for i in 0..4 {
                    k += fast::strip(base, &mut s[i], &mut e[i]);
                }
                if nq != 2 * k || e[0] == s[0] || e[1] == s[1] || e[2] == s[2] {
                    return;
                }
                let Some(t) = out.dates.ts(base, s[3], e[3]) else { return };
                if t < ctx.cws || t >= ctx.we {
                    return;
                }
                let Some(camp) = ctx.aux.campaigns.get(fast::pack_at(base, s[2], e[2])) else { return };
                let user = fast::pack_at(base, s[1], e[1]);
                let cid = fast::pack_at(base, s[0], e[0]);
                emit_click(ctx, out, user, cid, (t - ctx.origin) as u32, camp);
            })
        }
    };
    for line in lines(&file[rest..end]) {
        let Some([cid, user, camp, ts]) = split::<4>(line) else { continue };
        if cid.is_empty() || user.is_empty() || camp.is_empty() {
            continue;
        }
        let Some(t) = parse_ts(ts) else { continue };
        if t < ctx.cws || t >= ctx.we {
            continue;
        }
        let Some(camp) = ctx.aux.campaigns.get(pack(camp)) else { continue };
        emit_click(ctx, out, pack(user), pack(cid), (t - ctx.origin) as u32, camp);
    }
}

// ====================================================================== dedup

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
    // where the note of its kept copy (smallest ingest_ts so far) is.
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
    for (t, o) in outs.iter().enumerate() {
        let mut idx = (t as u32) << IDX_BITS;
        for (ptr, len) in o.slim.pieces(p) {
            for k in 0..len {
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
                            // The copy with the smallest ingest_ts is kept, valid or
                            // not; copies with equal ingest_ts are identical.
                            if ingest(s.x) < ingest(kept.x) {
                                drop_copy(kept.x);
                                unsafe { *table.get_unchecked_mut(i) = ((tag as u64) << 32) | idx as u64 };
                            } else {
                                drop_copy(s.x);
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
}

// ====================================================================== persons

#[derive(Clone, Copy)]
struct Sess {
    start: u32,
    day: u32,
    purch: bool,
    rev: u64,
}

#[derive(Clone, Copy, Default)]
struct Ckl {
    cid: Key,
    ts: u32,
    camp: u32,
}

/// user_daily rows, already in Parquet's PLAIN encoding except `cat`.
#[derive(Default)]
struct Rows {
    n: usize,
    user: Vec<u8>,
    day: Vec<u8>,
    sessions: Vec<u8>,
    events: Vec<u8>,
    rev: Vec<u8>,
    cat: Vec<u16>,
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
    /// Per campaign: the last session (by serial number) it was counted for.
    stamp: Vec<u32>,
    serial: u32,
    prof: [f64; 12],
}

impl Tl {
    fn new(ctx: &Ctx, ncell: usize) -> Tl {
        let bslots = 1usize << ctx.qshift;
        Tl {
            cnt_e: vec![0; bslots + 1],
            cnt_c: vec![0; bslots + 1],
            evs: Vec::new(),
            cks: Vec::new(),
            sess: Vec::new(),
            catsum: vec![0; ctx.aux.cat_names.len()],
            touched: Vec::new(),
            tmp: Vec::new(),
            rows: Rows::default(),
            cells: Cells {
                sessions: vec![0; ncell],
                purch: vec![0; ncell],
                assisted: vec![0; ncell],
                rev: vec![0; ncell],
            },
            nd: ctx.nd,
            day0: ctx.day0 as i32,
            organic: ctx.aux.camp_names.len(),
            stamp: vec![0; ctx.aux.camp_names.len() + 1],
            serial: 0,
            prof: [0.0; 12],
        }
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
                if e.ev != DEAD {
                    tl.cnt_e[(e.person & smask) as usize] += 1;
                    ne += 1;
                }
            }
        }
        for (ptr, len) in o.ck.pieces(q) {
            for c in unsafe { std::slice::from_raw_parts(ptr, len) } {
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
    tl.evs.clear();
    tl.evs.resize(ne as usize, 0);
    tl.cks.clear();
    tl.cks.resize(ncl as usize, Ckl::default());
    for o in outs {
        for (ptr, len) in o.ev.pieces(q) {
            for e in unsafe { std::slice::from_raw_parts(ptr, len) } {
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
        if ee > pe {
            // Slots are visited in table order, so these reads are sequential.
            person(ctx.ids.key(base | i as u32), &mut evs[pe..ee], &mut cks[pc..ce], tl);
        }
        pe = ee;
        pc = ce;
    }
    tl.evs = evs;
    tl.cks = cks;
    tl.prof[2] += t_c.elapsed().as_secs_f64();
}

/// One person: sessions, their user_daily rows, attribution and assists.
fn person(who: Key, evs: &mut [u64], cks: &mut [Ckl], tl: &mut Tl) {
    evs.sort_unstable();
    let n = evs.len();
    let nd = tl.nd;

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
        tl.sess.push(Sess { start, day, purch, rev });
        d_sessions += 1;
        d_events += nev;
        d_rev += rev;
    }
    flush_row(who, cur_day, d_sessions, d_events, d_rev, tl);

    // ---- attribution
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
        cks.sort_unstable_by(|a, b| a.ts.cmp(&b.ts).then_with(|| cmp_key(b.cid, a.cid)));
        // Copies of one click are identical, so they are neighbours now.
        for k in 1..cks.len() {
            if cks[k].cid != cks[nc - 1].cid {
                cks[nc] = cks[k];
                nc += 1;
            }
        }
    }
    let cks = &cks[..nc];
    if tl.sess.len() * nc <= 4096 {
        // The common, small case: per session, walk the clicks in the 7 days
        // up to its start. The latest credits it; each campaign among them
        // assists it once (`stamp` remembers which were already counted).
        let (sess, stamp, cells) = (&tl.sess, &mut tl.stamp, &mut tl.cells);
        let mut serial = tl.serial;
        let (mut lo, mut hi) = (0usize, 0usize);
        for s in sess {
            while hi < nc && cks[hi].ts <= s.start {
                hi += 1;
            }
            while lo < hi && s.start - cks[lo].ts > WEEK {
                lo += 1;
            }
            let day = s.day as usize;
            let camp = if lo < hi { cks[hi - 1].camp as usize } else { organic };
            let cell = camp * nd + day;
            cells.sessions[cell] += 1;
            cells.purch[cell] += s.purch as u32;
            cells.rev[cell] += s.rev;
            serial += 1;
            for c in &cks[lo..hi] {
                let camp = c.camp as usize;
                if stamp[camp] != serial {
                    stamp[camp] = serial;
                    cells.assisted[camp * nd + day] += 1;
                }
            }
        }
        tl.serial = serial;
        return;
    }
    let mut hi = 0;
    for s in &tl.sess {
        while hi < nc && cks[hi].ts <= s.start {
            hi += 1;
        }
        let camp = if hi > 0 && s.start - cks[hi - 1].ts <= WEEK { cks[hi - 1].camp as usize } else { organic };
        let cell = camp * nd + s.day as usize;
        tl.cells.sessions[cell] += 1;
        tl.cells.purch[cell] += s.purch as u32;
        tl.cells.rev[cell] += s.rev;
    }

    // ---- assists for a person with many sessions and clicks: per campaign,
    // the union of [click, click + 7 days] over its clicks; every session
    // starting inside is assisted once.
    tl.tmp.clear();
    tl.tmp.extend(cks.iter().map(|c| ((c.camp as u64) << 32) | c.ts as u64));
    tl.tmp.sort_unstable();
    let sess = &tl.sess;
    let assisted = &mut tl.cells.assisted;
    let mut flush = |camp: usize, a: u32, b: u32| {
        let lo = sess.partition_point(|s| s.start < a);
        let hi = lo + sess[lo..].partition_point(|s| s.start <= b);
        if hi - lo <= 8 {
            for s in &sess[lo..hi] {
                assisted[camp * nd + s.day as usize] += 1;
            }
        } else {
            let mut k = lo;
            while k < hi {
                let d = sess[k].day;
                let e = k + sess[k..hi].partition_point(|s| s.day == d);
                assisted[camp * nd + d as usize] += (e - k) as u32;
                k = e;
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
    let rows = &mut tl.rows;
    rows.n += 1;
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
    rows.day.extend_from_slice(&(tl.day0 + day as i32).to_le_bytes());
    rows.sessions.extend_from_slice(&sessions.to_le_bytes());
    rows.events.extend_from_slice(&events.to_le_bytes());
    rows.rev.extend_from_slice(&rev.to_le_bytes());
    rows.cat.push(best);
}

// ====================================================================== output

fn plain_bytes<'a>(out: &mut Vec<u8>, values: impl Iterator<Item = &'a [u8]>) {
    for v in values {
        out.extend_from_slice(&(v.len() as u32).to_le_bytes());
        out.extend_from_slice(v);
    }
}

fn write_user_daily(path: &Path, rows: &mut Rows, ctx: &Ctx) {
    let n = rows.n;
    let mut dict = Vec::new();
    plain_bytes(&mut dict, ctx.aux.cat_names.iter().map(|c| c.as_slice()));
    let width = parquet::bit_width(ctx.aux.cat_names.len() as u32 - 1).max(1);
    let mut cat = Vec::with_capacity(n);
    parquet::def_levels(&mut cat, rows.cat.iter().map(|&c| c != NOCAT));
    cat.push(width as u8);
    let present: Vec<u32> = rows.cat.iter().filter(|&&c| c != NOCAT).map(|&c| c as u32).collect();
    if !present.is_empty() {
        parquet::bitpack_run(&mut cat, present.into_iter(), width);
    }
    let col = |name, ptype, optional, logical, dict, data| Column { name, ptype, optional, logical, dict, data };
    let take = std::mem::take::<Vec<u8>>;
    let columns = [
        col("user_id", parquet::T_BYTE_ARRAY, false, Logical::String, None, take(&mut rows.user)),
        col("day", parquet::T_INT32, false, Logical::Date, None, take(&mut rows.day)),
        col("sessions", parquet::T_INT32, false, Logical::None, None, take(&mut rows.sessions)),
        col("events", parquet::T_INT32, false, Logical::None, None, take(&mut rows.events)),
        col("revenue_cents", parquet::T_INT64, false, Logical::None, None, take(&mut rows.rev)),
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
