//! Shopping Sessions pipeline. See rules.md for the spec this implements.

mod parquet;
mod util;

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use parquet::{Column, Logical};
use util::*;

/// Person partitions (events and clicks are routed by the person's name).
const QB: u32 = 8;
const Q: usize = 1 << QB;
/// Dedup partitions (routed by the event ID's hash).
const PB: u32 = 8;
const P: usize = 1 << PB;

const DEAD: u64 = u64::MAX;
const TAG: u64 = 1 << 63;
const FLAG: u64 = 1 << 62;
const EMPTY: u64 = u64::MAX;
const H2_MASK: u32 = 0x3FFF_FFFF;

const GAP: u32 = 1_800_000;
const WEEK: u32 = 604_800_000;
const DAY_MS: u32 = 86_400_000;
const NOCAT: u16 = u16::MAX;

/// A surviving event candidate, routed to its person's partition.
#[repr(C)]
#[derive(Clone, Copy)]
struct Ev {
    name: Key,
    /// ts offset (31 bits) << 33 | category (9 bits) << 24 | price (24 bits; 0 unless a purchase)
    ev: u64,
    ingest: u32,
    h2: u32,
}

/// A valid click, routed to its person's partition.
#[repr(C)]
#[derive(Clone, Copy)]
struct Ck {
    name: Key,
    cid: Key,
    ts: u32,
    camp: u32,
}

/// One parsed event line, for dedup: the ID's hash and either a reference to
/// its `Ev` (valid copies) or TAG | h2 | ingest (invalid copies).
#[derive(Clone, Copy)]
struct Slim {
    h: u64,
    x: u64,
}

struct Ctx {
    ws: i64,
    we: i64,
    /// Click window start: ws - 7 days.
    cws: i64,
    /// Time origin for 32-bit offsets: midnight of the window's first day, minus 7 days.
    origin: i64,
    day0: i64,
    nd: usize,
    links: KMap<Key>,
    products: KMap<u16>,
    unknown: u16,
    cat_names: Vec<Vec<u8>>,
    campaigns: KMap<u32>,
    camp_names: Vec<Vec<u8>>,
    camp_channel: Vec<Vec<u8>>,
}

struct ParseOut {
    ev: Vec<Vec<Ev>>,
    ck: Vec<Vec<Ck>>,
    slim: Vec<Vec<Slim>>,
}

struct Task {
    events: bool,
    file: usize,
    start: usize,
    end: usize,
}

struct Shared<T>(T);
unsafe impl<T> Sync for Shared<T> {}
unsafe impl<T> Send for Shared<T> {}

fn timing() -> bool {
    std::env::var_os("SS_TIMING").is_some()
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
        .min(250);
    let log = timing();
    let lap = |what: &str| {
        if log {
            eprintln!("{:>8.1} ms  {}", t0.elapsed().as_secs_f64() * 1e3, what);
        }
    };

    let ctx = build_ctx(&data_dir);
    lap("ctx (manifest, products, campaigns, links)");

    // ------------------------------------------------------------ parse
    let event_files = map_dir(&data_dir.join("events"));
    let click_files = map_dir(&data_dir.join("clicks"));
    let mut tasks = Vec::new();
    for (events, files) in [(true, &event_files), (false, &click_files)] {
        for (fi, m) in files.iter().enumerate() {
            chunk_tasks(m.bytes(), events, fi, &mut tasks);
        }
    }
    // Biggest first, so the tail of the queue is small pieces.
    tasks.sort_by_key(|t| std::cmp::Reverse(t.end - t.start));
    let next = AtomicUsize::new(0);
    let mut outs: Vec<ParseOut> = Vec::new();
    std::thread::scope(|scope| {
        let handles: Vec<_> = (0..threads)
            .map(|tid| {
                let (ctx, tasks, next, event_files, click_files) = (&ctx, &tasks, &next, &event_files, &click_files);
                scope.spawn(move || {
                    let mut out = ParseOut {
                        ev: (0..Q).map(|_| Vec::new()).collect(),
                        ck: (0..Q).map(|_| Vec::new()).collect(),
                        slim: (0..P).map(|_| Vec::new()).collect(),
                    };
                    loop {
                        let i = next.fetch_add(1, Ordering::Relaxed);
                        let Some(task) = tasks.get(i) else { break };
                        if task.events {
                            parse_events(&event_files[task.file].bytes()[task.start..task.end], ctx, &mut out, tid);
                        } else {
                            parse_clicks(&click_files[task.file].bytes()[task.start..task.end], ctx, &mut out);
                        }
                    }
                    out
                })
            })
            .collect();
        for h in handles {
            outs.push(h.join().unwrap());
        }
    });
    lap("parse");

    // ------------------------------------------------------------ dedup
    let ev_ptrs: Shared<Vec<Vec<(*mut Ev, usize)>>> = Shared(
        outs.iter_mut()
            .map(|o| o.ev.iter_mut().map(|v| (v.as_mut_ptr(), v.len())).collect())
            .collect(),
    );
    let next = AtomicUsize::new(0);
    std::thread::scope(|scope| {
        for _ in 0..threads {
            let (outs, next, ev_ptrs) = (&outs, &next, &ev_ptrs);
            scope.spawn(move || {
                let mut table: Vec<Slim> = Vec::new();
                let mut dups: Vec<Slim> = Vec::new();
                let mut members: Vec<(u32, u32, u64)> = Vec::new();
                loop {
                    let p = next.fetch_add(1, Ordering::Relaxed);
                    if p >= P {
                        break;
                    }
                    dedup_bucket(p, outs, &ev_ptrs.0, &mut table, &mut dups, &mut members);
                }
            });
        }
    });
    lap("dedup");

    // ------------------------------------------------------------ persons
    let user_dir = out_dir.join("user_daily");
    let camp_dir = out_dir.join("campaign_daily");
    std::fs::create_dir_all(&user_dir).unwrap();
    std::fs::create_dir_all(&camp_dir).unwrap();

    let ncell = (ctx.camp_names.len() + 1) * ctx.nd;
    let next = AtomicUsize::new(0);
    let mut cells: Vec<Cells> = Vec::new();
    std::thread::scope(|scope| {
        let handles: Vec<_> = (0..threads)
            .map(|tid| {
                let (ctx, outs, next, user_dir) = (&ctx, &outs, &next, &user_dir);
                scope.spawn(move || {
                    let mut tl = Tl::new(ctx, ncell);
                    loop {
                        let q = next.fetch_add(1, Ordering::Relaxed);
                        if q >= Q {
                            break;
                        }
                        person_bucket(q, outs, ctx, &mut tl);
                    }
                    if tid == 0 || !tl.rows.name.is_empty() {
                        write_user_daily(&user_dir.join(format!("part-{tid:03}.parquet")), &tl.rows, ctx);
                    }
                    tl.cells
                })
            })
            .collect();
        for h in handles {
            cells.push(h.join().unwrap());
        }
    });
    lap("persons + user_daily");

    write_campaign_daily(&camp_dir.join("part-000.parquet"), &cells, &ctx);
    lap("campaign_daily");
    // Skip destructors: the process is done.
    std::process::exit(0);
}

// ====================================================================== setup

fn read_file(path: &Path) -> Vec<u8> {
    std::fs::read(path).unwrap_or_default()
}

/// The lines after the header row.
fn data_lines(bytes: &[u8]) -> impl Iterator<Item = &[u8]> {
    let body = match memchr::memchr(b'\n', bytes) {
        Some(i) => &bytes[i + 1..],
        None => &bytes[bytes.len()..],
    };
    let body = body.strip_suffix(b"\n").unwrap_or(body);
    let empty = body.is_empty();
    body.split(|&c| c == b'\n').filter(move |_| !empty)
}

fn csv_files(dir: &Path) -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = match std::fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|x| x == "csv") && p.is_file())
            .collect(),
        Err(_) => Vec::new(),
    };
    files.sort();
    files
}

fn map_dir(dir: &Path) -> Vec<Mmap> {
    csv_files(dir).iter().map(|p| Mmap::open(p).unwrap()).collect()
}

fn manifest_time(text: &[u8], key: &str) -> i64 {
    let at = memchr::memmem::find(text, key.as_bytes()).expect("manifest key");
    let rest = &text[at + key.len()..];
    let colon = memchr::memchr(b':', rest).expect("manifest colon");
    let rest = &rest[colon + 1..];
    let q1 = memchr::memchr(b'"', rest).expect("manifest value");
    let rest = &rest[q1 + 1..];
    let q2 = memchr::memchr(b'"', rest).expect("manifest value end");
    parse_ts(&rest[..q2]).expect("manifest timestamp")
}

fn build_ctx(data: &Path) -> Ctx {
    let manifest = read_file(&data.join("manifest.json"));
    let ws = manifest_time(&manifest, "\"window_start\"");
    let we = manifest_time(&manifest, "\"window_end\"");
    let day0 = ws.div_euclid(DAY);
    let origin = day0 * DAY - 7 * DAY;
    let nd = if we > ws { ((we - 1).div_euclid(DAY) - day0 + 1) as usize } else { 1 };
    assert!(we - origin < (1i64 << 31), "window too long for 31-bit offsets");

    // products -> category rank (categories sorted byte-wise, with `unknown`)
    let products_raw = read_file(&data.join("products.csv"));
    let mut pairs: Vec<(Key, &[u8])> = Vec::new();
    for line in data_lines(&products_raw) {
        if let Some([id, cat]) = split::<2>(line) {
            if !id.is_empty() {
                pairs.push((pack(id), cat));
            }
        }
    }
    let mut cat_names: Vec<Vec<u8>> = pairs.iter().map(|p| p.1.to_vec()).collect();
    cat_names.push(b"unknown".to_vec());
    cat_names.sort();
    cat_names.dedup();
    assert!(cat_names.len() <= 511, "too many categories for 9 bits");
    let unknown = cat_names.binary_search_by(|c| c.as_slice().cmp(b"unknown")).unwrap() as u16;
    let mut products = KMap::with_capacity(pairs.len(), 0u16);
    for (k, cat) in &pairs {
        let rank = cat_names.binary_search_by(|c| c.as_slice().cmp(cat)).unwrap() as u16;
        products.get_or_insert(*k, rank);
    }

    let campaigns_raw = read_file(&data.join("campaigns.csv"));
    let mut campaigns = KMap::with_capacity(4096, 0u32);
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

    let links = build_links(&data.join("links"));

    Ctx {
        ws,
        we,
        cws: ws - 7 * DAY,
        origin,
        day0,
        nd,
        links,
        products,
        unknown,
        cat_names,
        campaigns,
        camp_names,
        camp_channel,
    }
}

/// Union-find over every linked ID; the result maps each ID to its person's
/// name (the byte-wise smallest ID), for IDs that are not their own name.
fn build_links(dir: &Path) -> KMap<Key> {
    let files = map_dir(dir);
    let total: usize = files.iter().map(|m| m.bytes().len()).sum();
    let mut ids: KMap<u32> = KMap::with_capacity(total / 16 + 16, 0);
    let mut keys: Vec<Key> = Vec::new();
    let mut parent: Vec<u32> = Vec::new();

    fn find(parent: &mut [u32], mut i: u32) -> u32 {
        loop {
            let p = parent[i as usize];
            if p == i {
                return i;
            }
            let g = parent[p as usize];
            parent[i as usize] = g;
            i = g;
        }
    }

    for m in &files {
        for line in data_lines(m.bytes()) {
            let Some([a, b]) = split::<2>(line) else { continue };
            if a.is_empty() || b.is_empty() {
                continue;
            }
            let mut idx = |k: Key, keys: &mut Vec<Key>, parent: &mut Vec<u32>| {
                let n = keys.len() as u32;
                let i = ids.get_or_insert(k, n);
                if i == n {
                    keys.push(k);
                    parent.push(n);
                }
                i
            };
            let ia = idx(pack(a), &mut keys, &mut parent);
            let ib = idx(pack(b), &mut keys, &mut parent);
            let ra = find(&mut parent, ia);
            let rb = find(&mut parent, ib);
            if ra != rb {
                // The root is always the byte-wise smallest ID of its set.
                if cmp_key(keys[ra as usize], keys[rb as usize]).is_lt() {
                    parent[rb as usize] = ra;
                } else {
                    parent[ra as usize] = rb;
                }
            }
        }
    }
    let mut links = KMap::with_capacity(keys.len(), 0u128);
    for i in 0..keys.len() as u32 {
        let r = find(&mut parent, i);
        if r != i {
            links.set(keys[i as usize], keys[r as usize]);
        }
    }
    links
}

fn chunk_tasks(bytes: &[u8], events: bool, file: usize, tasks: &mut Vec<Task>) {
    const CHUNK: usize = 1 << 21;
    let Some(header) = memchr::memchr(b'\n', bytes) else { return };
    let mut start = header + 1;
    while start < bytes.len() {
        let mut end = (start + CHUNK).min(bytes.len());
        if end < bytes.len() {
            end = match memchr::memchr(b'\n', &bytes[end..]) {
                Some(i) => end + i + 1,
                None => bytes.len(),
            };
        }
        tasks.push(Task { events, file, start, end });
        start = end;
    }
}

// ====================================================================== parse

#[inline(always)]
fn person_of(ctx: &Ctx, user: Key) -> Key {
    ctx.links.get(user).unwrap_or(user)
}

fn parse_events(data: &[u8], ctx: &Ctx, out: &mut ParseOut, tid: usize) {
    let mut pos = 0;
    let n = data.len();
    while pos < n {
        let end = match memchr::memchr(b'\n', &data[pos..]) {
            Some(i) => pos + i,
            None => n,
        };
        let line = &data[pos..end];
        pos = end + 1;
        let Some([eid, user, ts, ingest, typ, prod, price]) = split::<7>(line) else { continue };
        if eid.is_empty() {
            continue;
        }
        let ek = pack(eid);
        let h = hash64(ek);
        let h2 = (hash64b(ek) >> 32) as u32;
        let ing = parse_f1(ingest).unwrap_or(i64::MAX);

        let mut ev: u64 = 0;
        let mut ok = !user.is_empty() && !prod.is_empty();
        if ok {
            match parse_ts(ts) {
                Some(t) if t >= ctx.ws && t < ctx.we => {
                    let off = ((t - ctx.origin) as u64) << 33;
                    if typ == b"view" || typ == b"cart" {
                        ev = off;
                    } else if typ == b"purchase" {
                        match parse_price(price) {
                            Some(p) => {
                                let cat = ctx.products.get(pack(prod)).unwrap_or(ctx.unknown);
                                ev = off | ((cat as u64) << 24) | p as u64;
                            }
                            None => ok = false,
                        }
                    } else {
                        ok = false;
                    }
                    // Late: ingested more than 24 hours after it happened.
                    if ok && ing.saturating_sub(t) > DAY {
                        ok = false;
                    }
                }
                _ => ok = false,
            }
        }
        // Ingest time in seconds as a biased u32 (only ever compared between copies of one event).
        let ing32 = (ing.saturating_sub(ctx.origin) / 1000)
            .saturating_add(1 << 31)
            .clamp(0, u32::MAX as i64 - 1) as u32;
        let p = (h >> (64 - PB)) as usize;
        if ok {
            let name = person_of(ctx, pack(user));
            let q = (hash64(name) >> (64 - QB)) as usize;
            let bucket = &mut out.ev[q];
            let idx = bucket.len() as u64;
            bucket.push(Ev { name, ev, ingest: ing32, h2 });
            out.slim[p].push(Slim { h, x: ((tid as u64) << 52) | ((q as u64) << 40) | idx });
        } else {
            out.slim[p].push(Slim { h, x: TAG | (((h2 & H2_MASK) as u64) << 32) | ing32 as u64 });
        }
    }
}

fn parse_clicks(data: &[u8], ctx: &Ctx, out: &mut ParseOut) {
    let mut pos = 0;
    let n = data.len();
    while pos < n {
        let end = match memchr::memchr(b'\n', &data[pos..]) {
            Some(i) => pos + i,
            None => n,
        };
        let line = &data[pos..end];
        pos = end + 1;
        let Some([cid, user, camp, ts]) = split::<4>(line) else { continue };
        if cid.is_empty() || user.is_empty() || camp.is_empty() {
            continue;
        }
        let Some(t) = parse_ts(ts) else { continue };
        if t < ctx.cws || t >= ctx.we {
            continue;
        }
        let Some(camp) = ctx.campaigns.get(pack(camp)) else { continue };
        let name = person_of(ctx, pack(user));
        let q = (hash64(name) >> (64 - QB)) as usize;
        out.ck[q].push(Ck { name, cid: pack(cid), ts: (t - ctx.origin) as u32, camp });
    }
}

// ====================================================================== dedup

fn dedup_bucket(
    p: usize,
    outs: &[ParseOut],
    ev_ptrs: &[Vec<(*mut Ev, usize)>],
    table: &mut Vec<Slim>,
    dups: &mut Vec<Slim>,
    members: &mut Vec<(u32, u32, u64)>,
) {
    let n: usize = outs.iter().map(|o| o.slim[p].len()).sum();
    if n < 2 {
        return;
    }
    let cap = (n * 2).next_power_of_two();
    table.clear();
    table.resize(cap, Slim { h: 0, x: EMPTY });
    let mask = cap - 1;
    dups.clear();
    for o in outs {
        for s in &o.slim[p] {
            let mut i = (s.h as usize) & mask;
            loop {
                let e = unsafe { table.get_unchecked_mut(i) };
                if e.x == EMPTY {
                    *e = *s;
                    break;
                }
                if e.h == s.h {
                    if e.x & FLAG == 0 {
                        dups.push(*e);
                        e.x |= FLAG;
                    }
                    dups.push(*s);
                    break;
                }
                i = (i + 1) & mask;
            }
        }
    }
    if dups.is_empty() {
        return;
    }
    dups.sort_unstable_by_key(|s| s.h);
    let ev_at = |x: u64| -> *mut Ev {
        let t = (x >> 52) as usize & 0xFF;
        let q = (x >> 40) as usize & 0xFFF;
        let idx = (x & ((1 << 40) - 1)) as usize;
        debug_assert!(idx < ev_ptrs[t][q].1);
        unsafe { ev_ptrs[t][q].0.add(idx) }
    };
    let mut i = 0;
    while i < dups.len() {
        let mut j = i + 1;
        while j < dups.len() && dups[j].h == dups[i].h {
            j += 1;
        }
        // (h2, ingest, x) for every copy with this hash.
        members.clear();
        for s in &dups[i..j] {
            if s.x & TAG != 0 {
                members.push((((s.x >> 32) as u32) & H2_MASK, s.x as u32, s.x));
            } else {
                let e = unsafe { &*ev_at(s.x) };
                members.push((e.h2 & H2_MASK, e.ingest, s.x));
            }
        }
        members.sort_unstable_by_key(|m| (m.0, m.1));
        let mut a = 0;
        while a < members.len() {
            let mut b = a + 1;
            while b < members.len() && members[b].0 == members[a].0 {
                b += 1;
            }
            // members[a] has the smallest ingest_ts: it is the kept copy.
            // Every other valid copy goes; if the kept copy is invalid, nothing survives.
            for m in &members[a + 1..b] {
                if m.2 & TAG == 0 {
                    unsafe { (*ev_at(m.2)).ev = DEAD };
                }
            }
            a = b;
        }
        i = j;
    }
}

// ====================================================================== persons

#[derive(Clone, Copy)]
struct Sess {
    start: u32,
    day: u32,
    nev: u32,
    purch: bool,
    rev: u64,
}

#[derive(Clone, Copy, Default)]
struct Ckl {
    cid: Key,
    ts: u32,
    camp: u32,
}

#[derive(Default)]
struct Rows {
    name: Vec<Key>,
    day: Vec<u16>,
    sessions: Vec<u32>,
    events: Vec<u32>,
    rev: Vec<u64>,
    cat: Vec<u16>,
}

struct Cells {
    sessions: Vec<u32>,
    purch: Vec<u32>,
    assisted: Vec<u32>,
    rev: Vec<u64>,
}

struct Tl {
    table: KMap<u32>,
    names: Vec<Key>,
    cnt_e: Vec<u32>,
    cnt_c: Vec<u32>,
    pe: Vec<u32>,
    pc: Vec<u32>,
    evs: Vec<u64>,
    cks: Vec<Ckl>,
    sess: Vec<Sess>,
    catsum: Vec<u64>,
    touched: Vec<u16>,
    tmp: Vec<u64>,
    rows: Rows,
    cells: Cells,
    nd: usize,
    organic: usize,
}

impl Tl {
    fn new(ctx: &Ctx, ncell: usize) -> Tl {
        Tl {
            table: KMap::with_capacity(2048, 0),
            names: Vec::new(),
            cnt_e: Vec::new(),
            cnt_c: Vec::new(),
            pe: Vec::new(),
            pc: Vec::new(),
            evs: Vec::new(),
            cks: Vec::new(),
            sess: Vec::new(),
            catsum: vec![0; ctx.cat_names.len()],
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
            organic: ctx.camp_names.len(),
        }
    }
}

fn person_bucket(q: usize, outs: &[ParseOut], _ctx: &Ctx, tl: &mut Tl) {
    tl.table.clear();
    tl.names.clear();
    tl.cnt_e.clear();
    tl.cnt_c.clear();
    tl.pe.clear();
    tl.pc.clear();

    // Pass 1: give every person in this bucket a dense index and count their rows.
    for o in outs {
        for e in &o.ev[q] {
            if e.ev == DEAD {
                continue;
            }
            let next = tl.names.len() as u32;
            let pi = tl.table.get_or_insert(e.name, next);
            if pi == next {
                tl.names.push(e.name);
                tl.cnt_e.push(0);
                tl.cnt_c.push(0);
            }
            tl.cnt_e[pi as usize] += 1;
            tl.pe.push(pi);
        }
    }
    for o in outs {
        for c in &o.ck[q] {
            let next = tl.names.len() as u32;
            let pi = tl.table.get_or_insert(c.name, next);
            if pi == next {
                tl.names.push(c.name);
                tl.cnt_e.push(0);
                tl.cnt_c.push(0);
            }
            tl.cnt_c[pi as usize] += 1;
            tl.pc.push(pi);
        }
    }
    let np = tl.names.len();
    if np == 0 {
        return;
    }

    // Pass 2: counting sort by person. cnt_* become start offsets.
    let (mut se, mut sc) = (0u32, 0u32);
    for i in 0..np {
        let (ce, cc) = (tl.cnt_e[i], tl.cnt_c[i]);
        tl.cnt_e[i] = se;
        tl.cnt_c[i] = sc;
        se += ce;
        sc += cc;
    }
    tl.evs.clear();
    tl.evs.resize(se as usize, 0);
    tl.cks.clear();
    tl.cks.resize(sc as usize, Ckl::default());
    let mut k = 0;
    for o in outs {
        for e in &o.ev[q] {
            if e.ev == DEAD {
                continue;
            }
            let pi = tl.pe[k] as usize;
            k += 1;
            let at = tl.cnt_e[pi];
            tl.cnt_e[pi] = at + 1;
            tl.evs[at as usize] = e.ev;
        }
    }
    let mut k = 0;
    for o in outs {
        for c in &o.ck[q] {
            let pi = tl.pc[k] as usize;
            k += 1;
            let at = tl.cnt_c[pi];
            tl.cnt_c[pi] = at + 1;
            tl.cks[at as usize] = Ckl { cid: c.cid, ts: c.ts, camp: c.camp };
        }
    }
    // cnt_* now hold each person's end offset.
    let mut evs = std::mem::take(&mut tl.evs);
    let mut cks = std::mem::take(&mut tl.cks);
    let (mut pe, mut pc) = (0usize, 0usize);
    for i in 0..np {
        let (ee, ce) = (tl.cnt_e[i] as usize, tl.cnt_c[i] as usize);
        if ee > pe {
            person(tl.names[i], &mut evs[pe..ee], &mut cks[pc..ce], tl);
        }
        pe = ee;
        pc = ce;
    }
    tl.evs = evs;
    tl.cks = cks;
}

/// One person: sessions, their user_daily rows, attribution and assists.
fn person(name: Key, evs: &mut [u64], cks: &mut [Ckl], tl: &mut Tl) {
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
                flush_row(name, cur_day, d_sessions, d_events, d_rev, tl);
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
        tl.sess.push(Sess { start, day, nev, purch, rev });
        d_sessions += 1;
        d_events += nev;
        d_rev += rev;
    }
    flush_row(name, cur_day, d_sessions, d_events, d_rev, tl);

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
    cks.sort_unstable_by(|a, b| a.ts.cmp(&b.ts).then_with(|| cmp_key(b.cid, a.cid)));
    // Copies of one click are identical, so they are neighbours now.
    let mut nc = 1;
    for k in 1..cks.len() {
        if cks[k].cid != cks[nc - 1].cid {
            cks[nc] = cks[k];
            nc += 1;
        }
    }
    let cks = &cks[..nc];
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

    // ---- assists: per campaign, the union of [click, click + 7 days] over
    // its clicks; every session starting inside is assisted once.
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
fn flush_row(name: Key, day: u32, sessions: u32, events: u32, rev: u64, tl: &mut Tl) {
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
    tl.rows.name.push(name);
    tl.rows.day.push(day as u16);
    tl.rows.sessions.push(sessions);
    tl.rows.events.push(events);
    tl.rows.rev.push(rev);
    tl.rows.cat.push(best);
}

// ====================================================================== output

fn plain_bytes<'a>(out: &mut Vec<u8>, values: impl Iterator<Item = &'a [u8]>) {
    for v in values {
        out.extend_from_slice(&(v.len() as u32).to_le_bytes());
        out.extend_from_slice(v);
    }
}

fn write_user_daily(path: &Path, rows: &Rows, ctx: &Ctx) {
    let n = rows.name.len();
    let mut user = Vec::with_capacity(n * 14);
    let mut scratch = Vec::new();
    for &k in &rows.name {
        if is_long(k) {
            scratch.clear();
            key_bytes(k, &mut scratch);
            user.extend_from_slice(&(scratch.len() as u32).to_le_bytes());
            user.extend_from_slice(&scratch);
        } else {
            let len = (k as u8) as usize;
            user.extend_from_slice(&(len as u32).to_le_bytes());
            user.extend_from_slice(&k.to_be_bytes()[..len]);
        }
    }
    let mut day = Vec::with_capacity(n * 4);
    for &d in &rows.day {
        day.extend_from_slice(&((ctx.day0 + d as i64) as i32).to_le_bytes());
    }
    let mut sessions = Vec::with_capacity(n * 4);
    for &v in &rows.sessions {
        sessions.extend_from_slice(&(v as i32).to_le_bytes());
    }
    let mut events = Vec::with_capacity(n * 4);
    for &v in &rows.events {
        events.extend_from_slice(&(v as i32).to_le_bytes());
    }
    let mut rev = Vec::with_capacity(n * 8);
    for &v in &rows.rev {
        rev.extend_from_slice(&(v as i64).to_le_bytes());
    }
    let mut dict = Vec::new();
    plain_bytes(&mut dict, ctx.cat_names.iter().map(|c| c.as_slice()));
    let width = parquet::bit_width(ctx.cat_names.len() as u32 - 1).max(1);
    let mut cat = Vec::with_capacity(n);
    parquet::def_levels(&mut cat, rows.cat.iter().map(|&c| c != NOCAT));
    cat.push(width as u8);
    let present: Vec<u32> = rows.cat.iter().filter(|&&c| c != NOCAT).map(|&c| c as u32).collect();
    if !present.is_empty() {
        parquet::bitpack_run(&mut cat, present.into_iter(), width);
    }
    let col = |name, ptype, optional, logical, dict, data| Column { name, ptype, optional, logical, dict, data };
    let columns = [
        col("user_id", parquet::T_BYTE_ARRAY, false, Logical::String, None, user),
        col("day", parquet::T_INT32, false, Logical::Date, None, day),
        col("sessions", parquet::T_INT32, false, Logical::None, None, sessions),
        col("events", parquet::T_INT32, false, Logical::None, None, events),
        col("revenue_cents", parquet::T_INT64, false, Logical::None, None, rev),
        col(
            "top_category",
            parquet::T_BYTE_ARRAY,
            true,
            Logical::String,
            Some((dict, ctx.cat_names.len())),
            cat,
        ),
    ];
    parquet::write_file(path, &columns, n).unwrap();
}

fn write_campaign_daily(path: &Path, cells: &[Cells], ctx: &Ctx) {
    let nd = ctx.nd;
    let ncamp = ctx.camp_names.len();
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
                if c == ncamp { (b"organic", b"organic") } else { (&ctx.camp_names[c], &ctx.camp_channel[c]) };
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
