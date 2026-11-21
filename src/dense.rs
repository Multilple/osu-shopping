//! Dense mode: a shorter road for inputs whose IDs are numbers in disguise.
//!
//! The data generator writes every event, user and click ID as a prefix and
//! `base36(scramble(number, key))`, where `scramble` is a fixed bijection on
//! 40-bit numbers and the numbers are small and dense: event `n` is the n-th
//! event, user `512 i + a` is the a-th ID of person i, click `n` the n-th
//! click. When a sample of the input shows that shape (the keys can be worked
//! out from it), IDs are turned back into their numbers as they are parsed,
//! and everything that the general pipeline does with hash tables becomes an
//! array index:
//!
//! - links: union-find over an array indexed by user number;
//! - dedup: events are spread over buckets of consecutive event numbers, and
//!   inside a bucket the number is the slot of a small table;
//! - people: the generator numbers events person after person, so reading a
//!   bucket's table in order gives each person's events together and in time
//!   order. Runs of one person's events are handed on as they are; a person
//!   whose events really do come as one sorted run needs no sorting at all.
//!
//! Nothing here assumes the input keeps to that shape. An ID that does not
//! fit it gets a number of its own from a side table, a person whose events
//! are not one sorted run is put together the general way, and if the side
//! tables overflow the run is abandoned before anything is written and the
//! general pipeline does the job.

use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU32, AtomicU64, AtomicU8, AtomicUsize, Ordering::{Acquire, Relaxed, Release}};
use std::sync::{Mutex, RwLock};
use std::time::Instant;

use crate::input::Src;
use crate::table::{map_anon, release, Arena};

/// The record lists of the parse stage. Measured on the scoring machine
/// (Cascade Lake, two threads to a core): records staged a cache line at a
/// time and moved out with non-temporal stores (`table::Streams`) cost 15%
/// less per record than plain stores (`table::Plain`) with 600 lists and
/// 27% less with 1800, and do not get dearer as the lists multiply.
pub(crate) type Lists = crate::table::Streams<[u64; 2], 10>;
/// The event streams (appended to from the assembly kernel too).
pub(crate) type EvLists = crate::table::Stage16<10>;
/// The click record lists: the same staging, kept by write pointers (what
/// the click kernel's assembly appends to).
pub(crate) type CkLists = crate::table::Stage16P<10>;
use crate::util::*;
use crate::{
    build_ctx, env_usize, fast, grid_tasks, input, lines, load_aux, par, person_sorted,
    slow_only, write_campaign_daily, write_user_daily, user_run, Cells, Click, Ctx, Rows, Sess, Shared, Tl, DAY_MS, GAP, NOCAT, WEEK,
};

// ---------------------------------------------------------------- the ID code

pub use crate::keys::{scramble, unscramble, Keys, NONE};
pub const M40: u64 = crate::keys::MASK;

/// The number behind the ID `prefix + digits` under `key` (see keys.rs), or
/// `NONE` when the ID is not a prefix and a canonical base-36 number below
/// 2^40. (The decoder reads 8 bytes from the first digit on: every buffer
/// an ID is read from has that much after it.)
#[inline(always)]
fn id_number(id: &[u8], prefix: u8, key: u64) -> u64 {
    if id.len() < 2 || id.len() > 9 || id[0] != prefix {
        return NONE;
    }
    unsafe { crate::keys::decode36_unscramble(id.as_ptr().add(1), id.len() - 1, key) }
}

/// Every two-digit base-36 number as text (first digit in the low byte).
static PAIRS: [u16; 1296] = {
    const DIGITS: &[u8; 36] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    let mut t = [0u16; 1296];
    let mut i = 0;
    while i < 1296 {
        t[i] = DIGITS[i / 36] as u16 | (DIGITS[i % 36] as u16) << 8;
        i += 1;
    }
    t
};

/// The ID `prefix + base36(s)` as a key, for `s` below 2^40.
#[inline(always)]
pub fn id_key(prefix: u8, s: u64) -> Key {
    // Eight digits, two at a time, then drop the leading zeros.
    let (hi, lo) = (s / 1_679_616, s % 1_679_616);
    let pair = |x: u64| unsafe { *PAIRS.get_unchecked(x as usize) } as u64;
    let w = pair(hi / 1296) | pair(hi % 1296) << 16 | pair(lo / 1296) << 32 | pair(lo % 1296) << 48;
    let zeros = ((w ^ 0x3030_3030_3030_3030).trailing_zeros() / 8).min(7);
    let (w, len) = (w >> (8 * zeros), 8 - zeros);
    prefix as u128 | (w as u128) << 8 | ((len + 1) as u128) << 120
}

const POW36: [u64; 9] = {
    let mut t = [1u64; 9];
    let mut i = 1;
    while i < 9 {
        t[i] = t[i - 1] * 36;
        i += 1;
    }
    t
};

/// The digit count in base 36 of the smallest number with this many
/// significant bits (one digit for none).
const DIGITS_OF_BITS: [u8; 41] = {
    let mut t = [1u8; 41];
    let mut b = 1;
    while b < 41 {
        let v = 1u64 << (b - 1);
        let mut l = 1;
        while v >= POW36[l] {
            l += 1;
        }
        t[b] = l as u8;
        b += 1;
    }
    t
};

/// Where the ID `prefix + base36(s)` stands among the IDs with its prefix,
/// byte-wise, for `s` below 2^40: `digit_order(s) < digit_order(t)` exactly
/// when `id_key(p, s)` is before `id_key(p, t)`. It is the digits as a
/// number with zeros behind them up to eight, then how many there are (a
/// string is after every one it starts with).
#[inline(always)]
pub fn digit_order(s: u64) -> u64 {
    let l = DIGITS_OF_BITS[(64 - s.leading_zeros()) as usize] as usize;
    // (Twice a number has at most one digit more.)
    let l = l + (s >= POW36[l]) as usize;
    (s * POW36[8 - l]) << 4 | l as u64
}

/// `id_key` the plain way (for checking it).
#[allow(dead_code)]
pub fn id_key_plain(prefix: u8, s: u64) -> Key {
    const DIGITS: &[u8; 36] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    let mut buf = [0u8; 14];
    let mut at = buf.len();
    let mut rest = s;
    loop {
        at -= 1;
        buf[at] = DIGITS[(rest % 36) as usize];
        rest /= 36;
        if rest == 0 {
            break;
        }
    }
    at -= 1;
    buf[at] = prefix;
    pack(&buf[at..])
}

// ---------------------------------------------------------------- shared state

/// Interned events: at most this many (their numbers follow the dense ones).
pub(crate) const EV_EXTRA: u64 = 1 << 20;
/// Interned users: at most this many.
pub(crate) const XCAP: usize = 1 << 20;
/// Links waiting for their parents' cache lines.
const PEND: usize = 32;
/// The low bits of an event record's second word: twice the slot.
pub(crate) const LOW21: u64 = 0x1F_FFFF;
/// In an event record's second word: a purchase (its price follows in a
/// record of its own, marked `EXT`).
pub(crate) const PURCHASE: u64 = 1 << 63;
pub(crate) const EXT: u64 = 3 << 62;
/// The largest ingest time an event record can hold (31 bits; all ones is
/// kept free so that no record is all ones).
pub(crate) const INGEST_TOP: u64 = (1 << 31) - 2;

struct Names {
    map: HashMap<Key, u32>,
    list: Vec<Key>,
}

/// One event bucket's survivors that belong to one person bucket: runs of
/// `[person | count << 32, event words...]`.
#[derive(Clone, Copy)]
struct Seg {
    q: u32,
    ptr: *mut u64,
    words: u32,
}

unsafe impl Sync for Seg {}
unsafe impl Send for Seg {}

pub struct Dn {
    pub(crate) keys: Keys,
    /// Event numbers below this come straight from the ID; interned ones follow.
    pub(crate) max_event: u64,
    pub(crate) max_click: u64,
    /// User numbers (512 x person + ID) below this are direct when the ID's
    /// number within its person is under 8.
    pub(crate) vbound: u64,
    /// An event bucket holds 2^kb consecutive event numbers.
    pub(crate) kb: u32,
    pub(crate) nb: usize,
    /// A person bucket holds 2^ku consecutive persons (2^(ku+3) nodes).
    pub(crate) ku: u32,
    pub(crate) nq: usize,
    /// Direct nodes are `0..d`: node = 8 x person + ID. Interned users follow.
    pub(crate) d: usize,
    /// Per direct node: xor this into the node to get its person's node
    /// (the one with the smallest name); 0xFF: a person that needs a lookup.
    pub(crate) r8: *mut u8,
    /// Per direct node that names a person: 1 once a run of its events was
    /// seen, 3 once a second one was.
    pub(crate) runs: *mut u8,
    /// Union-find parents, stored xor the node (so untouched memory says "itself").
    pub(crate) parent: *mut u32,
    pub(crate) users: RwLock<Names>,
    pub(crate) events: Mutex<HashMap<Key, u32>>,
    pub(crate) clicks: RwLock<Names>,
    pub(crate) overflow: AtomicBool,
    /// Per interned user known when the links were read: its person.
    pub(crate) ext_person: Vec<u32>,
    /// Direct nodes of persons that need a lookup.
    pub(crate) direct_special: HashMap<u32, u32>,
    /// Names of the persons numbered `d..` (a copy of `users.list`, taken
    /// when parsing is over).
    pub(crate) names: Vec<Key>,
    pub(crate) click_names: Vec<Key>,
    /// A click key is `ts << 32 | campaign << obits | number among its person's clicks`.
    pub(crate) obits: u32,
    /// The click kernel's tables (made once the campaigns are read, while
    /// the links are parsed; set before any click is parsed).
    pub(crate) ckx: std::sync::OnceLock<crate::dense_ck::Cx>,
    /// The assembly click kernel's, set with them (`None`: the run is one it was not made for).
    pub(crate) cka: std::sync::OnceLock<Option<&'static crate::dense_cka::KC>>,
}

unsafe impl Sync for Dn {}
unsafe impl Send for Dn {}

impl Dn {
    #[inline(always)]
    pub(crate) fn event_index(&self, id: &[u8]) -> u64 {
        let i = id_number(id, b'e', self.keys.event);
        if i < self.max_event {
            return i;
        }
        self.intern_event(id)
    }

    #[cold]
    #[inline(never)]
    pub(crate) fn intern_event(&self, id: &[u8]) -> u64 {
        let k = pack(id);
        let mut m = self.events.lock().unwrap();
        let n = m.len() as u32;
        let i = *m.entry(k).or_insert(n) as u64;
        if i >= EV_EXTRA {
            self.overflow.store(true, Relaxed);
            return self.max_event;
        }
        self.max_event + i
    }

    #[inline(always)]
    pub(crate) fn user_node(&self, id: &[u8]) -> u32 {
        let v = id_number(id, b'u', self.keys.user);
        if v < self.vbound && v & 0x1F8 == 0 {
            return (((v >> 9) << 3) | (v & 7)) as u32;
        }
        self.intern_user(id)
    }

    #[cold]
    #[inline(never)]
    pub(crate) fn intern_user(&self, id: &[u8]) -> u32 {
        let k = pack(id);
        if let Some(&i) = self.users.read().unwrap().map.get(&k) {
            return (self.d + i as usize) as u32;
        }
        let mut w = self.users.write().unwrap();
        let n = w.list.len() as u32;
        let i = *w.map.entry(k).or_insert(n);
        if i == n {
            if n as usize >= XCAP {
                self.overflow.store(true, Relaxed);
                w.map.remove(&k);
                return self.d as u32;
            }
            w.list.push(k);
        }
        (self.d + i as usize) as u32
    }

    #[inline(always)]
    pub(crate) fn click_index(&self, id: &[u8]) -> u32 {
        let i = id_number(id, b'k', self.keys.click);
        if i < self.max_click {
            return i as u32;
        }
        self.intern_click(id)
    }

    #[cold]
    #[inline(never)]
    pub(crate) fn intern_click(&self, id: &[u8]) -> u32 {
        let k = pack(id);
        if let Some(&i) = self.clicks.read().unwrap().map.get(&k) {
            return (self.max_click + i as u64) as u32;
        }
        let mut w = self.clicks.write().unwrap();
        let n = w.list.len() as u32;
        let i = *w.map.entry(k).or_insert(n);
        if i == n {
            if self.max_click + n as u64 >= u32::MAX as u64 {
                self.overflow.store(true, Relaxed);
                w.map.remove(&k);
                return self.max_click as u32;
            }
            w.list.push(k);
        }
        (self.max_click + i as u64) as u32
    }

    /// The name of a direct node.
    #[inline(always)]
    fn direct_key(&self, n: u32) -> Key {
        let v = ((n as u64 >> 3) << 9) | (n as u64 & 7);
        id_key(b'u', scramble(v, self.keys.user))
    }

    /// The name of any node (while the links are read).
    fn node_key(&self, n: u32) -> Key {
        if (n as usize) < self.d {
            self.direct_key(n)
        } else {
            self.users.read().unwrap().list[n as usize - self.d]
        }
    }

    /// The name of a person (after parsing).
    #[inline(always)]
    fn person_key(&self, p: u32) -> Key {
        if (p as usize) < self.d {
            self.direct_key(p)
        } else {
            self.names[p as usize - self.d]
        }
    }

    /// The person of a node whose `r8` entry says to look it up, or of an
    /// interned user.
    #[cold]
    #[inline(never)]
    fn special_person(&self, node: u32) -> u32 {
        if (node as usize) >= self.d {
            match self.ext_person.get(node as usize - self.d) {
                Some(&p) => p,
                None => node,
            }
        } else {
            self.direct_special[&node]
        }
    }

    #[inline(always)]
    fn parent_slot(&self, n: u32) -> &AtomicU32 {
        unsafe { &*(self.parent.add(n as usize) as *const AtomicU32) }
    }

    #[inline(always)]
    fn prefetch_parent(&self, n: u32) {
        unsafe { core::arch::x86_64::_mm_prefetch(self.parent.add(n as usize) as *const i8, core::arch::x86_64::_MM_HINT_T0) };
    }

    #[inline(always)]
    fn parent_of(&self, n: u32) -> u32 {
        self.parent_slot(n).load(Relaxed) ^ n
    }

    #[inline(always)]
    fn find(&self, mut x: u32) -> u32 {
        loop {
            // A root is its own parent, so this asks one thing of a root and
            // of a root's child alike: whether a node is a root is a coin,
            // whether its parent is one hardly ever is not.
            let p = self.parent_of(x);
            let gp = self.parent_of(p);
            if gp == p {
                return p;
            }
            let _ = self.parent_slot(x).compare_exchange(p ^ x, gp ^ x, Relaxed, Relaxed);
            x = p;
        }
    }

    /// Where a direct node's name stands among the direct nodes' names (see `digit_order`).
    #[inline(always)]
    fn direct_order(&self, n: u32) -> u64 {
        let v = ((n as u64 >> 3) << 9) | (n as u64 & 7);
        digit_order(scramble(v, self.keys.user))
    }

    /// Whether node `a`'s name is before node `b`'s, when one of them is interned.
    #[cold]
    #[inline(never)]
    fn name_before(&self, a: u32, b: u32) -> bool {
        cmp_key(self.node_key(a), self.node_key(b)).is_lt()
    }

    /// Join the persons of two nodes. A person's root is its node with the
    /// byte-wise smallest name.
    #[inline(always)]
    fn union(&self, a: u32, b: u32) {
        if a == b {
            return;
        }
        loop {
            let ra = self.find(a);
            let rb = self.find(b);
            if ra == rb {
                return;
            }
            // (Two direct nodes: their names compare as two numbers do.)
            let a_first = if (ra.max(rb) as usize) < self.d { self.direct_order(ra) < self.direct_order(rb) } else { self.name_before(ra, rb) };
            let (child, parent) = if a_first { (rb, ra) } else { (ra, rb) };
            if self.parent_slot(child).compare_exchange(0, parent ^ child, Relaxed, Relaxed).is_ok() {
                return;
            }
        }
    }

    /// Note that a run of person `p`'s events starts.
    #[inline(always)]
    fn mark_run(&self, p: u32) {
        let r = unsafe { &*(self.runs.add(p as usize) as *const AtomicU8) };
        if r.fetch_or(1, Relaxed) & 1 != 0 {
            r.fetch_or(2, Relaxed);
        }
    }

    /// Whether all of person `p`'s events came as one run.
    #[inline(always)]
    fn one_run(&self, p: u32) -> bool {
        unsafe { *self.runs.add(p as usize) == 1 }
    }
}

/// A click of a known person, for attribution: the ID is the click's number.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct Ckd {
    ts: u32,
    cid: u32,
    camp: u32,
}

/// The run in progress, for comparing click IDs by name (rarely needed:
/// only two different clicks of one person at the same millisecond).
static CURRENT: AtomicPtr<Dn> = AtomicPtr::new(std::ptr::null_mut());

#[cold]
#[inline(never)]
fn click_key(cid: u32) -> Key {
    let dn = unsafe { &*CURRENT.load(Relaxed) };
    if (cid as u64) < dn.max_click {
        id_key(b'k', scramble(cid as u64, dn.keys.click))
    } else {
        dn.click_names[(cid as u64 - dn.max_click) as usize]
    }
}

impl Click for Ckd {
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
        if a.cid == b.cid {
            std::cmp::Ordering::Equal
        } else {
            cmp_key(click_key(a.cid), click_key(b.cid))
        }
    }
}

/// What a thread produces while parsing.
pub(crate) struct DOut {
    pub(crate) arena: Arena,
    /// Event lines by event bucket: `[ingest << 33 | ts << 2 | purchase << 1
    /// | invalid, 2 x slot | node << 21]`. A purchase also has `PURCHASE` set
    /// in the second word and is followed by `[price | category << 24, EXT]`.
    /// (The first word orders the copies of an event: smallest ingest time
    /// first.)
    pub(crate) ev: EvLists,
    /// Valid clicks by their user's person bucket (interned users: bucket
    /// `nq`): `[ts << 32 | campaign, node | click << 32]`.
    pub(crate) ck: CkLists,
    pub(crate) reader: input::Reader,
    pub(crate) pend_l: Vec<(u32, u32)>,
}

/// What a thread produces while removing duplicates.
struct AOut {
    arena: Arena,
    segs: Vec<Seg>,
    /// Events of persons that need a lookup: `[person, event word]`.
    sp_ev: Vec<[u64; 2]>,
    /// The bucket's table: the kept copy of each event number so far.
    w: Vec<[u64; 2]>,
    purch: Vec<u64>,
    /// Scratch for the run in progress: its purchases (`session << 40 |
    /// category << 24 | price`).
    pscr: Vec<u64>,
}

/// A thread's state while putting persons together.
struct BOut {
    tl: Tl,
    cnt: Vec<u32>,
    /// The bucket's clicks, person after person: a key each (see `Dn::obits`)
    /// and, apart, the click's number.
    keys: Vec<u64>,
    cids: Vec<u32>,
    irr: Vec<(u32, *mut u64, u32)>,
    /// Clicks of persons that need a lookup: `[person | click << 32, ts << 32 | campaign]`.
    sp_ck: Vec<[u64; 2]>,
    /// Per campaign: the person it was last seen for, and how far into that
    /// person's sessions its clicks have assisted.
    cov: Vec<[u32; 2]>,
    serial: u32,
    dayfirst: Vec<u32>,
    big: Vec<Ckd>,
    /// Credit and assists per (campaign, day): what `Cells` holds, laid out
    /// so that crediting a session touches one cache line.
    credit: Vec<Credit>,
    assist: Vec<u32>,
    /// Scratch for one person: per session, how many clicks are at or
    /// before its start; per click, the sessions it can assist (from, to).
    hi: Vec<u32>,
    ca: Vec<u32>,
    cb: Vec<u32>,
    /// Scratch for one person: each session's `start << 32 | event count`,
    /// and the purchases (session << 40 | category << 24 | price).
    sstart: Vec<u64>,
    spurch: Vec<u64>,
    /// Scratch for a person with several runs: all their runs' sessions,
    /// their order by start, and the session of the whole each one is part of.
    sall: Vec<u64>,
    sorder: Vec<u32>,
    smap: Vec<u32>,
    /// Scratch for one person: each session's user_daily row, and each
    /// day's (the last row written for it).
    srow: Vec<u32>,
    dayrow: Vec<u32>,
    /// Seconds spent: sorting clicks by person, on persons whose events
    /// came as one run, on the others (CPU time, kept only when `timed`);
    /// then counts of events of each kind.
    prof: [f64; 8],
    timed: bool,
}

static STATS: [AtomicUsize; 12] = [const { AtomicUsize::new(0) }; 12];

/// Sessions credited to a campaign on a day.
#[repr(C)]
#[derive(Clone, Copy, Default)]
struct Credit {
    sessions: u32,
    purch: u32,
    rev: u64,
}

type Cell<T> = Shared<UnsafeCell<T>>;

#[allow(clippy::mut_from_ref)]
fn mine<T>(all: &[Cell<T>], tid: usize) -> &mut T {
    unsafe { &mut *all[tid].0.get() }
}

// ---------------------------------------------------------------- keys

/// Up to `max` bytes of whole data lines from the start of a file (and some
/// zero bytes after them, which the sampler may read).
fn head(src: &Src, max: usize) -> (Vec<u8>, usize) {
    let mut buf = vec![0u8; src.len.min(max) + 128];
    let want = buf.len() - 128;
    let mut have = 0;
    while have < want {
        match src.file.read_at(&mut buf[have..want], have as u64) {
            Ok(0) | Err(_) => break,
            Ok(n) => have += n,
        }
    }
    let Some(first) = memchr(b'\n', &buf[..have]) else { return (buf, 0) };
    let last = buf[..have].iter().rposition(|&c| c == b'\n').unwrap();
    buf.copy_within(first + 1..last + 1, 0);
    buf[last - first..].fill(0);
    (buf, last - first)
}

struct Plan {
    keys: Keys,
    max_event: u64,
    max_user: u64,
    max_click: u64,
}

/// Look at the start of the biggest event and click files: are the IDs the
/// generator's, and under which keys?
fn plan(event_files: &[Src], click_files: &[Src]) -> Option<Plan> {
    const LINES: usize = 4096;
    let biggest = |files: &[Src], max: usize| files.iter().max_by_key(|s| s.len).filter(|s| s.len > 0).map(|s| head(s, max));
    let (ev_text, ev_len) = biggest(event_files, 448 << 10)?;
    let (ck_text, ck_len) = biggest(click_files, 288 << 10)?;
    let (mut ev_ids, mut us_ids, mut ck_ids, mut none) = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    let ev_used = crate::keys::sample_ids(&ev_text[..ev_len + 96], b'e', b'u', LINES, &mut ev_ids, &mut us_ids).min(ev_len);
    let ck_used = crate::keys::sample_ids(&ck_text[..ck_len + 96], b'k', 0, LINES, &mut ck_ids, &mut none).min(ck_len);
    let count = |text: &[u8]| text.iter().filter(|&&c| c == b'\n').count();
    let (ev_n, ck_n) = (count(&ev_text[..ev_used]), count(&ck_text[..ck_used]));
    // Enough lines to judge, nearly all of them in the generator's form.
    if ev_n < 1000 || ck_n < 500 || ev_ids.len() * 100 < ev_n * 97 || us_ids.len() * 100 < ev_n * 97 || ck_ids.len() * 100 < ck_n * 97 {
        return None;
    }
    let total = |files: &[Src]| files.iter().map(|s| s.len).sum::<usize>();
    let ev_lines = (total(event_files) as f64 / (ev_used as f64 / ev_n as f64)) as u64;
    let ck_lines = (total(click_files) as f64 / (ck_used as f64 / ck_n as f64)) as u64;
    let (ev_guess, ck_guess) = (ev_lines + ev_lines / 8 + 4096, ck_lines + ck_lines / 8 + 4096);
    let us_guess = ev_guess / 24 + 4096;

    let keys = match std::env::var("SS_KEYS") {
        Ok(text) => {
            let k: Vec<u64> = text.split(',').filter_map(|x| u64::from_str_radix(x.trim().trim_start_matches("0x"), 16).ok()).collect();
            if k.len() != 3 {
                return None;
            }
            Keys { event: k[0], user: k[1], click: k[2] }
        }
        Err(_) => crate::keys::recover_keys(&ev_ids, &us_ids, &ck_ids, ev_guess, us_guess, ck_guess)?,
    };

    // The sample's largest numbers bound the rest (anything above is interned).
    let top = |ids: &[u64], key: u64, bound: u64| -> Option<u64> {
        let v: Vec<u64> = ids.iter().map(|&s| unscramble(s, key)).filter(|&v| v < bound).collect();
        if v.len() * 100 < ids.len() * 97 {
            return None;
        }
        v.into_iter().max()
    };
    let max_event = top(&ev_ids, keys.event, ev_guess)?;
    let max_click = top(&ck_ids, keys.click, ck_guess)?;
    let max_user = top(&us_ids, keys.user, us_guess * 512)? >> 9;
    let max_event = max_event + max_event / 64 + 4096;
    Some(Plan {
        keys,
        max_event,
        max_user: (max_user + max_user / 4).max(max_event / 30 + max_event / 1024) + 1024,
        max_click: max_click + max_click / 64 + 4096,
    })
}

// ---------------------------------------------------------------- links

#[inline(always)]
fn link(dn: &Dn, out: &mut DOut, a: &[u8], b: &[u8]) {
    link_nodes(dn, out, dn.user_node(a), dn.user_node(b));
}

/// Join two nodes' persons, once their parents' cache lines are here.
#[inline(always)]
pub(crate) fn link_nodes(dn: &Dn, out: &mut DOut, na: u32, nb: u32) {
    dn.prefetch_parent(na);
    dn.prefetch_parent(nb);
    out.pend_l.push((na, nb));
    if out.pend_l.len() == PEND {
        flush_links(dn, out);
    }
}

fn flush_links(dn: &Dn, out: &mut DOut) {
    // A join walks from parent to parent: ask for the parents' lines first.
    for &(a, b) in &out.pend_l {
        dn.prefetch_parent(dn.parent_of(a));
        dn.prefetch_parent(dn.parent_of(b));
    }
    for &(a, b) in &out.pend_l {
        dn.union(a, b);
    }
    out.pend_l.clear();
}

fn parse_links(file: &[u8], start: usize, end: usize, dn: &Dn, out: &mut DOut) {
    let base = file.as_ptr();
    let rest = if slow_only() {
        start
    } else {
        unsafe {
            // The kernel takes the lines it can prove sound; the scanner and the plain code the rest.
            let start = crate::dense_lk::links_dense(file, start, end, dn, out);
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
                link(dn, out, std::slice::from_raw_parts(base.add(s[0]), e[0] - s[0]), std::slice::from_raw_parts(base.add(s[1]), e[1] - s[1]));
            })
        }
    };
    for line in lines(&file[rest..end]) {
        let Some([a, b]) = split::<2>(line) else { continue };
        if !a.is_empty() && !b.is_empty() {
            link(dn, out, a, b);
        }
    }
}

#[repr(C, align(64))]
struct SetBits([[u8; 8]; 256]);

/// For every byte: the positions of its set bits, lowest first.
static SET_BITS: SetBits = {
    let mut t = [[0u8; 8]; 256];
    let mut m = 0;
    while m < 256 {
        let (mut k, mut b) = (0, 0);
        while b < 8 {
            if m >> b & 1 != 0 {
                t[m][k] = b as u8;
                k += 1;
            }
            b += 1;
        }
        m += 1;
    }
    SetBits(t)
};

/// Turn the joined sets into the `r8` table and the list of persons that
/// need a lookup: those with an interned ID, or IDs of more than one
/// generator person.
fn settle_links(dn: &mut Dn, threads: usize) {
    let d = dn.d;
    let found: Vec<Mutex<Vec<(u32, u32)>>> = (0..threads).map(|_| Mutex::new(Vec::new())).collect();
    {
        let dn = &*dn;
        par(threads, |tid| {
            // (`d` is a multiple of 8.)
            let (lo, hi) = (d / 8 * tid / threads * 8, d / 8 * (tid + 1) / threads * 8);
            let mut odd = Vec::new();
            // Most nodes have no link, and which do is a coin: list the
            // linked ones of a stretch first, eight slots at a step with no
            // branch, then settle those.
            const STRETCH: usize = 2048;
            let mut linked = [0u32; STRETCH + 8];
            let mut at = lo;
            while at < hi {
                let to = (at + STRETCH).min(hi);
                let mut k = 0;
                unsafe {
                    use core::arch::x86_64::*;
                    let mut g = at;
                    while g < to {
                        let slots = _mm256_loadu_si256(dn.parent.add(g) as *const __m256i);
                        let m = !_mm256_movemask_ps(_mm256_castsi256_ps(_mm256_cmpeq_epi32(slots, _mm256_setzero_si256()))) as usize & 0xFF;
                        // The numbers of the slots that hold something, packed to the front.
                        let idx = _mm256_cvtepu8_epi32(_mm_loadl_epi64(SET_BITS.0.as_ptr().add(m) as *const __m128i));
                        _mm256_storeu_si256(linked.as_mut_ptr().add(k) as *mut __m256i, _mm256_add_epi32(idx, _mm256_set1_epi32(g as i32)));
                        k += m.count_ones() as usize;
                        g += 8;
                    }
                }
                for &n in &linked[..k] {
                    let r = dn.find(n);
                    if r as usize >= d || (r >> 3) != (n >> 3) {
                        unsafe { *dn.r8.add(n as usize) = 0xFF };
                        odd.push((n, r));
                    } else {
                        unsafe { *dn.r8.add(n as usize) = (n ^ r) as u8 };
                    }
                }
                at = to;
            }
            *found[tid].lock().unwrap() = odd;
        });
    }
    let odd: Vec<(u32, u32)> = found.into_iter().flat_map(|m| m.into_inner().unwrap()).collect();
    let n_ext = dn.users.read().unwrap().list.len();
    let t_serial = std::time::Instant::now();
    // Every root of a set that holds an interned ID or spans persons.
    let mut person_of_root: HashMap<u32, u32> = HashMap::new();
    let mut roots: Vec<u32> = odd.iter().map(|&(_, r)| r).collect();
    roots.extend((0..n_ext).map(|k| dn.find((d + k) as u32)));
    roots.sort_unstable();
    roots.dedup();
    for &r in &roots {
        let p = if r as usize >= d {
            r
        } else {
            // A direct node names this person: give the person a number
            // among the interned ones, under that name.
            let key = dn.direct_key(r);
            let mut w = dn.users.write().unwrap();
            w.list.push(key);
            (d + w.list.len() - 1) as u32
        };
        person_of_root.insert(r, p);
    }
    let mut direct_special = HashMap::new();
    for &(n, r) in &odd {
        direct_special.insert(n, person_of_root[&r]);
    }
    for &r in &roots {
        if (r as usize) < d {
            // The root itself, and its own person's other IDs (which looked ordinary).
            for a in 0..8 {
                let n = (r & !7) | a;
                if n == r || (dn.parent_slot(n).load(Relaxed) != 0 && dn.find(n) == r) {
                    unsafe { *dn.r8.add(n as usize) = 0xFF };
                    direct_special.insert(n, person_of_root[&r]);
                }
            }
        }
    }
    let total = dn.users.read().unwrap().list.len();
    dn.ext_person = (0..total).map(|k| if k < n_ext { person_of_root[&dn.find((d + k) as u32)] } else { (d + k) as u32 }).collect();
    dn.direct_special = direct_special;
    if std::env::var_os("SS_TIMING").is_some() {
        eprintln!("    settle: {} odd nodes, {} interned users, {} roots; serial part {:.2} ms", odd.len(), n_ext, roots.len(), t_serial.elapsed().as_secs_f64() * 1e3);
    }
}

// ---------------------------------------------------------------- parse

/// An ingest time in seconds from 2^30 seconds before the time origin, in
/// 31 bits (only ever compared between copies of one event). Times out of
/// range, and unparsable ones, sit at the ends.
#[inline(always)]
pub(crate) fn ingest31(ctx: &Ctx, ing: Option<i64>) -> u64 {
    match ing {
        Some(ms) => {
            let rel = (ms - ctx.origin).div_euclid(1000) + (1 << 30);
            rel.clamp(0, INGEST_TOP as i64) as u64
        }
        None => INGEST_TOP,
    }
}

/// One event line by the plain rules (see `event_line` in main.rs), as
/// dense-mode records.
pub(crate) fn event_line(line: &[u8], ctx: &Ctx, dn: &Dn, out: &mut DOut) {
    let Some(fields) = split::<7>(line) else { return };
    event_fields(fields, ctx, dn, out);
}

/// `event_line` for a well-formed line, given its seven values.
#[inline(always)]
pub(crate) fn event_fields([eid, user, ts, ingest, typ, prod, price]: [&[u8]; 7], ctx: &Ctx, dn: &Dn, out: &mut DOut) {
    if eid.is_empty() {
        return;
    }
    let idx = dn.event_index(eid);
    let ing = parse_f1(ingest);
    // The event's time when it is valid, and price and category for a purchase.
    let mut ok = None;
    if !user.is_empty() && !prod.is_empty() {
        if let Some(t) = parse_ts(ts) {
            if t >= ctx.ws && t < ctx.we && ing.is_some_and(|i| i - t <= DAY) {
                if typ == b"view" || typ == b"cart" {
                    ok = Some((t, None));
                } else if typ == b"purchase" {
                    if let Some(p) = parse_price(price) {
                        let cat = ctx.aux.products.get(pack(prod)).unwrap_or(ctx.aux.unknown);
                        ok = Some((t, Some(p as u64 | (cat as u64) << 24)));
                    }
                }
            }
        }
    }
    let ing = ingest31(ctx, ing) << 33;
    let slot2 = (idx & ((1 << dn.kb) - 1)) << 1;
    let s = (idx >> dn.kb) as usize;
    match ok {
        Some((t, ext)) => {
            let node = dn.user_node(user) as u64;
            let w0 = ing | ((t - ctx.origin) as u64) << 2;
            match ext {
                Some(ext) => {
                    out.ev.push16(s, w0 | 2, slot2 | node << 21 | PURCHASE, &mut out.arena);
                    out.ev.push16(s, ext, EXT, &mut out.arena);
                }
                None => {
                    out.ev.push16(s, w0, slot2 | node << 21, &mut out.arena);
                }
            }
        }
        None => {
            out.ev.push16(s, ing | 1, slot2, &mut out.arena);
        }
    }
}

fn parse_events(file: &[u8], start: usize, end: usize, ctx: &Ctx, dn: &Dn, out: &mut DOut) {
    // SS_EVCHECK=1: also compare the kernel's records with the plain code's.
    static CHECK: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    if *CHECK.get_or_init(|| std::env::var_os("SS_EVCHECK").is_some()) {
        static BAD: AtomicUsize = AtomicUsize::new(0);
        static CHUNKS: AtomicUsize = AtomicUsize::new(0);
        let bad = crate::dense_ev::check(file, start, end, ctx, dn);
        let (b, c) = (BAD.fetch_add(bad, Relaxed) + bad, CHUNKS.fetch_add(1, Relaxed) + 1);
        if bad > 0 || c.is_power_of_two() {
            eprintln!("    evcheck: {c} chunks, {b} streams differ");
        }
    }
    // SS_EVTIME=1: the kernel's own time on its thread's processor clock,
    // summed over the chunks so far (on stderr after every chunk).
    static TIME: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    let timed = *TIME.get_or_init(|| std::env::var_os("SS_EVTIME").is_some());
    if timed {
        // SS_EVREP=n: first n passes of the kernel over the chunk into throwaway
        // output, so that the timed pass finds its input in the caches.
        static REP: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
        let rep = *REP.get_or_init(|| crate::env_usize("SS_EVREP", 0));
        if rep > 0 {
            thread_local! { static SCRATCH: std::cell::RefCell<Option<DOut>> = const { std::cell::RefCell::new(None) }; }
            SCRATCH.with(|sc| {
                let mut sc = sc.borrow_mut();
                let o = sc.get_or_insert_with(|| crate::dense_ev::scratch_out(dn));
                for _ in 0..rep {
                    unsafe { crate::dense_ev::events_dense(file, start, end, ctx, dn, o) };
                }
            });
        }
    }
    let t0 = if timed { sys::thread_cpu() } else { 0.0 };
    // The kernel takes the lines it can prove sound; the plain code the rest.
    let rest = if slow_only() { start } else { unsafe { crate::dense_ev::events_dense(file, start, end, ctx, dn, out) } };
    if timed {
        static NANOS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        static BYTES: AtomicUsize = AtomicUsize::new(0);
        let dt = ((sys::thread_cpu() - t0) * 1e9) as u64;
        let (t, b) = (NANOS.fetch_add(dt, Relaxed) + dt, BYTES.fetch_add(rest - start, Relaxed) + rest - start);
        eprintln!("    evtime: {:.1} ms for {} MB: {:.3} ns per byte", t as f64 / 1e6, b >> 20, t as f64 / b as f64);
    }
    for line in lines(&file[rest..end]) {
        event_line(line, ctx, dn, out);
    }
}

/// One click line by the plain rules (see `click_line` in main.rs).
pub(crate) fn click_line(line: &[u8], ctx: &Ctx, dn: &Dn, out: &mut DOut) {
    let Some([cid, user, camp, ts]) = split::<4>(line) else { return };
    if cid.is_empty() || user.is_empty() || camp.is_empty() {
        return;
    }
    let Some(t) = parse_ts(ts) else { return };
    if t < ctx.cws || t >= ctx.we {
        return;
    }
    let Some(camp) = ctx.aux.campaigns.get(pack(camp)) else { return };
    let cidx = dn.click_index(cid) as u64;
    let node = dn.user_node(user);
    let s = ((node >> (dn.ku + 3)) as usize).min(dn.nq);
    out.ck.push16(s, ((t - ctx.origin) as u64) << 32 | camp as u64, node as u64 | cidx << 32, &mut out.arena);
}

/// The click kernel: the assembly one unless SS_CKERNEL=rs (or the run is
/// one it was not made for), then the Rust one. Returns where the lines it
/// left start.
pub(crate) fn click_kernel(file: &[u8], start: usize, end: usize, ctx: &Ctx, dn: &Dn, out: &mut DOut) -> usize {
    static RS: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    let rs = *RS.get_or_init(|| std::env::var("SS_CKERNEL").is_ok_and(|v| v == "rs"));
    if !rs {
        if let Some(rest) = unsafe { crate::dense_cka::clicks(file, start, end, ctx, dn, out) } {
            return rest;
        }
    }
    unsafe { crate::dense_ck::clicks_dense(file, start, end, ctx, dn, out) }
}

fn parse_clicks(file: &[u8], start: usize, end: usize, ctx: &Ctx, dn: &Dn, out: &mut DOut) {
    // SS_CKCHECK=1: also compare the kernel's records with the plain code's.
    static CHECK: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    if *CHECK.get_or_init(|| std::env::var_os("SS_CKCHECK").is_some()) {
        static BAD: AtomicUsize = AtomicUsize::new(0);
        static CHUNKS: AtomicUsize = AtomicUsize::new(0);
        let bad = crate::dense_cka::check(file, start, end, ctx, dn);
        let (b, c) = (BAD.fetch_add(bad, Relaxed) + bad, CHUNKS.fetch_add(1, Relaxed) + 1);
        if bad > 0 || c.is_power_of_two() {
            eprintln!("    ckcheck: {c} chunks, {b} streams differ");
        }
    }
    // The kernel takes the lines it can prove sound; the plain code the rest.
    let rest = if slow_only() { start } else { click_kernel(file, start, end, ctx, dn, out) };
    for line in lines(&file[rest..end]) {
        click_line(line, ctx, dn, out);
    }
}

// ---------------------------------------------------------------- dedup

/// Stage A's output, per run of one person's events (in event order):
/// `[person | sessions << 32, (start << 32 | events, end) per session...,
/// purchases...]`, `end` being the time of the session's last event, the
/// first session's `end` word holding the number of purchases above bit 32,
/// and a purchase being `session << 40 | category << 24 | price` (the session
/// counted within the run).
///
/// Every session is a chain of events, each at most 30 minutes after the one
/// before (a step back in time starts a new session, as a gap does), so it
/// lies within one session of the person's whole time line whatever the
/// order of the events: the sessions of all of a person's runs put together
/// as intervals (`person_multi`) give the exact result. Only events in time
/// order, the usual case, give sessions that need no putting together.
struct Walk<'a> {
    dn: &'a Dn,
    me: &'a mut AOut,
    buf: *mut u64,
    cur_q: u32,
    /// Where the open segment starts.
    seg: *mut u64,
    /// The open run's header (null: none yet).
    hdr: *mut u64,
}

impl Walk<'_> {
    /// An event of a person that needs a lookup: on the side list.
    #[cold]
    #[inline(never)]
    unsafe fn special(&mut self, node: u32, w0: u64, w1: u64) {
        let mut ev = w0 << 31;
        if w0 & 2 != 0 {
            ev = (ev ^ (1 << 32)) | self.me.purch[(w1 & LOW21) as usize];
        }
        self.me.sp_ev.push([self.dn.special_person(node) as u64, ev]);
    }

    /// The open run ends, its last session
    /// at `sp`: write its sessions count and purchases (from the scratch up
    /// to `pp`: pairs of the purchase's session and where its price is).
    /// Returns where the next run's header goes.
    #[cold]
    #[inline(never)]
    unsafe fn close(&mut self, sp: *mut u32, pp: *const u64) -> *mut u64 {
        let hdr = self.hdr;
        if hdr.is_null() {
            return self.buf;
        }
        let pscr = self.me.pscr.as_ptr();
        let np = pp.offset_from(pscr) as usize / 2;
        let first = hdr.add(1) as usize;
        let ns = (sp as usize - first) / 16 + 1;
        *hdr |= (ns as u64) << 32;
        *hdr.add(2) |= (np as u64) << 32;
        let dst = hdr.add(1 + 2 * ns);
        let purch = &self.me.purch;
        for k in 0..np {
            let sid = (*pscr.add(2 * k) as usize - first) / 16;
            *dst.add(k) = (sid as u64) << 40 | purch[*pscr.add(2 * k + 1) as usize];
        }
        dst.add(np)
    }

    /// A run of person `p` starts with its header at `at`; returns where its
    /// first session goes.
    #[cold]
    #[inline(never)]
    unsafe fn open(&mut self, p: u32, at: *mut u64) -> *mut u32 {
        let q = p >> (self.dn.ku + 3);
        if q != self.cur_q {
            if self.cur_q != u32::MAX {
                self.me.segs.push(Seg { q: self.cur_q, ptr: self.seg, words: at.offset_from(self.seg) as u32 });
            }
            self.cur_q = q;
            self.seg = at;
        }
        self.dn.mark_run(p);
        *at = p as u64;
        self.hdr = at;
        at.add(1) as *mut u32
    }
}

/// Event bucket `b`: keep each event number's copy with the smallest
/// ingest time, then hand the valid ones on, in event order, as runs of one
/// person's events.
fn dedup_bucket(b: usize, outs: &[&DOut], dn: &Dn, me: &mut AOut) {
    let nrec: usize = outs.iter().map(|o| o.ev.len(b)).sum();
    if nrec == 0 {
        return;
    }
    let ns = 1usize << dn.kb;
    let sm2 = ((ns - 1) << 1) as u64;
    if me.w.len() != ns + 1 {
        // (Every later bucket finds the table as the walk below leaves it.)
        me.w.clear();
        me.w.resize(ns + 1, [u64::MAX; 2]);
    }
    me.purch.clear();
    let w = me.w.as_mut_ptr() as *mut u64;
    let prices = &mut me.purch;
    // A kept purchase: note where its price and category are.
    let mut attach = |at: usize, ext: u64| -> bool {
        if prices.len() > LOW21 as usize {
            return false;
        }
        unsafe { *w.add(at + 1) = (*w.add(at + 1) & !LOW21) | prices.len() as u64 };
        prices.push(ext);
        true
    };
    // (The entry after the table's last: see below.)
    let side = unsafe { w.add(2 * ns) };
    for o in outs {
        // Where the table holds a kept purchase whose price is in the next piece.
        let mut wants: Option<usize> = None;
        for (ptr, len) in o.ev.pieces(b) {
            unsafe {
                let mut p = ptr as *const u64;
                let end = p.add(2 * len);
                while p != end {
                    let (w0, w1) = (*p, *p.add(1));
                    p = p.add(2);
                    if (w1 as i64) < 0 {
                        if w1 >= EXT {
                            // The price of the purchase that ended the piece before.
                            if let Some(at) = wants.take() {
                                if !attach(at, w0) {
                                    dn.overflow.store(true, Relaxed);
                                    return;
                                }
                            }
                            continue;
                        }
                        let at = (w1 & sm2) as usize;
                        let keep = w0 < *w.add(at);
                        if keep {
                            *w.add(at) = w0;
                            *w.add(at + 1) = w1;
                        }
                        if p != end {
                            if keep && !attach(at, *p) {
                                dn.overflow.store(true, Relaxed);
                                return;
                            }
                            p = p.add(2);
                        } else {
                            wants = if keep { Some(at) } else { None };
                        }
                        continue;
                    }
                    let cur = w.add((w1 & sm2) as usize);
                    // (Copies with equal ingest times are identical.) A copy
                    // that is not kept is written to the side, not skipped
                    // by a branch.
                    let dst = std::hint::select_unpredictable(w0 < *cur, cur, side);
                    *dst = w0;
                    *dst.add(1) = w1;
                }
            }
        }
    }

    // Every valid kept event, in event order, turned into sessions run by run.
    // (Per run: a header word, at most a session (two words) and a purchase
    // per event.)
    let nmax = nrec.min(ns);
    let room = 4 * nmax + 8;
    let buf = me.arena.alloc(room * 8) as *mut u64;
    if me.pscr.len() < 2 * ns + 8 {
        me.pscr.resize(2 * ns + 8, 0);
    }
    let pscr = me.pscr.as_mut_ptr();
    let r8 = dn.r8;
    let purch = me.purch.as_ptr();
    let mut walk = Walk { dn, me, buf, cur_q: u32::MAX, seg: buf, hdr: std::ptr::null_mut() };
    unsafe {
        // The open run: its header, its session's words (as halves: count,
        // start, end), its start and count so far, the last event's time,
        // where its next purchase goes in the scratch.
        let mut sp: *mut u32 = std::ptr::null_mut();
        let (mut start, mut cnt) = (0u32, 0u32);
        let mut prev = 0u32;
        let mut pp = pscr;
        // (No person has this number.)
        let mut cur_p = 0x7FFF_FFFFu32;
        let mut e = w;
        let wend = e.add(2 * ns);
        // (Kept in a register rather than set for every event.)
        let ctl = std::hint::black_box(0x1F02u64);
        loop {
            // The open run's events (no calls here: the run's state stays in registers).
            let (mut w0, mut w1, mut p) = (0u64, 0u64, 0u32);
            let mut turn = false;
            while e != wend {
                let (a0, a1) = (*e, *e.add(1));
                // (Leave the table empty for the next bucket.)
                *e = u64::MAX;
                e = e.add(2);
                if a0 & 1 != 0 {
                    continue;
                }
                let node = (a1 >> 21) as u32;
                // A node's entry gives its person's node; an entry of -1 ("look
                // it up") gives a number no person has.
                let q = node ^ (*r8.add(node as usize) as i8 as i32 as u32);
                if q != cur_p {
                    (w0, w1, p, turn) = (a0, a1, q, true);
                    break;
                }
                // In plain code (written out by hand: the compiler's takes a
                // third more instructions):
                //   let ts = (a0 >> 2) as u32 & 0x7FFF_FFFF;
                //   let brk = ts.wrapping_sub(prev) > GAP;
                //   start = if brk { ts } else { start };
                //   cnt = if brk { 1 } else { cnt + 1 };
                //   sp = sp.add(4 * brk as usize);
                //   (*sp, *sp.add(1), *sp.add(2)) = (cnt, start, ts);
                //   prev = ts;
                // (A step back in time starts a session too: see `Walk`. The
                // end word's top half stays as the run's start left it.)
                std::arch::asm!(
                    "bextr {ts:r}, {a0}, {ctl}",
                    "mov {m:e}, {ts:e}",
                    "sub {m:e}, {prev:e}",
                    "cmp {m:e}, {gap1}",
                    "cmovae {start:e}, {ts:e}",
                    "sbb {m}, {m}",
                    "and {cnt:e}, {m:e}",
                    "inc {cnt:e}",
                    "and {m}, -16",
                    "lea {sp}, [{sp} + {m} + 16]",
                    "mov dword ptr [{sp}], {cnt:e}",
                    "mov dword ptr [{sp} + 4], {start:e}",
                    "mov dword ptr [{sp} + 8], {ts:e}",
                    "mov {prev:e}, {ts:e}",
                    a0 = in(reg) a0,
                    ctl = in(reg) ctl,
                    gap1 = const GAP + 1,
                    ts = out(reg) _,
                    m = out(reg) _,
                    prev = inout(reg) prev,
                    start = inout(reg) start,
                    cnt = inout(reg) cnt,
                    sp = inout(reg) sp,
                    options(nostack),
                );
                if a0 & 2 != 0 {
                    // (Its session and price are found when the run ends.)
                    *pp = sp as u64;
                    *pp.add(1) = a1 & LOW21;
                    pp = pp.add(2);
                }
            }
            if !turn {
                break;
            }
            if (p as i32) < 0 {
                walk.special((w1 >> 21) as u32, w0, w1);
                continue;
            }
            // A new run, and its first event.
            let at = walk.close(sp, pp);
            sp = walk.open(p, at);
            cur_p = p;
            let ts = (w0 >> 2) as u32 & 0x7FFF_FFFF;
            (start, cnt, prev) = (ts, 1, ts);
            *(sp as *mut u64) = (ts as u64) << 32 | 1;
            *(sp as *mut u64).add(1) = ts as u64;
            pp = walk.me.pscr.as_mut_ptr();
            if w0 & 2 != 0 {
                *pp = sp as u64;
                *pp.add(1) = w1 & LOW21;
                pp = pp.add(2);
            }
        }
        let out = walk.close(sp, pp);
        if walk.cur_q != u32::MAX {
            let seg = Seg { q: walk.cur_q, ptr: walk.seg, words: out.offset_from(walk.seg) as u32 };
            walk.me.segs.push(seg);
        }
        walk.me.arena.shrink_to(out as *mut u8);
    }
}

// ---------------------------------------------------------------- persons

/// Room in every column for `n` more rows (so that the row writers need not check).
#[inline(always)]
fn reserve_rows(rows: &mut Rows, n: usize) {
    #[cold]
    #[inline(never)]
    fn grow(rows: &mut Rows, n: usize) {
        rows.user.reserve(n * 20 + 16);
        rows.day.reserve(n * 4);
        rows.sessions.reserve(n * 4);
        rows.events.reserve(n * 4);
        rows.rev.reserve(n * 8);
        rows.cat.reserve(n);
    }
    if rows.user.capacity() - rows.user.len() < n * 20 + 16
        || rows.cat.capacity() - rows.cat.len() < n
        || rows.rev.capacity() - rows.rev.len() < n * 8
        || rows.day.capacity() - rows.day.len() < n * 4
        || rows.sessions.capacity() - rows.sessions.len() < n * 4
        || rows.events.capacity() - rows.events.len() < n * 4
    {
        grow(rows, n);
    }
}

/// One person's sessions and user_daily rows from events that should
/// already be in time order, for a person with a short name. Returns false,
/// having written nothing, when they turn out not to be in order.
///
/// The loop over the events has no branch that depends on them (but for
/// purchases): each event stores its session's start and running count at
/// the session's number, and the last store for a number is the one that
/// stays.
fn person_stream(who: Key, evs: &[u64], me: &mut BOut) -> bool {
    let n = evs.len();
    me.sstart.clear();
    me.sstart.reserve(2 * n + 2);
    me.spurch.clear();
    let start = me.sstart.as_mut_ptr();
    let p = evs.as_ptr();
    unsafe {
        let mut sid = 0usize;
        let mut prev = (*p >> 33) as u32;
        let mut cur = (prev as u64) << 32;
        let mut back = false;
        for i in 0..n {
            let e = *p.add(i);
            let ts = (e >> 33) as u32;
            back |= ts < prev;
            // (A step back in time looks like a gap here; `back` catches it.)
            let brk = ts.wrapping_sub(prev) > GAP;
            sid += brk as usize;
            cur = if brk { (ts as u64) << 32 | 1 } else { cur + 1 };
            *start.add(2 * sid) = cur;
            *start.add(2 * sid + 1) = ts as u64;
            prev = ts;
            if e & 0xFF_FFFF != 0 {
                me.spurch.push((sid as u64) << 40 | (e & 0x1_FFFF_FFFF));
            }
        }
        if back {
            return false;
        }
        let (np, pu) = (me.spurch.len(), me.spurch.as_ptr());
        sess_rows(who, start, sid + 1, pu, np, me)
    }
}

/// One person's sessions and user_daily rows from their sessions (two words
/// each, the first `start << 32 | events`; in time order) and purchases (`session << 40 | category
/// << 24 | price`). Neither list may be in `me.tl` or be `me.srow`/`me.dayrow`.
/// Returns false, having written nothing, if a session does not start more
/// than 30 minutes after the end (the second word's low half) of the one before.
#[must_use]
unsafe fn sess_rows(who: Key, sw: *const u64, ns: usize, pu: *const u64, np: usize, me: &mut BOut) -> bool {
    let tl = &mut me.tl;
    let purchases = std::slice::from_raw_parts(pu, np);
    {
        tl.sess.clear();
        tl.sess.reserve(ns);
        let sess = tl.sess.as_mut_ptr();
        me.srow.clear();
        me.srow.reserve(ns);
        if me.dayrow.len() < tl.nd {
            me.dayrow.resize(tl.nd, 0);
        }
        let (srow, dayrow) = (me.srow.as_mut_ptr(), me.dayrow.as_mut_ptr());

        // Sessions, and one row per day with a session: every session
        // writes its day's row as it stands so far at the row's number, and
        // the number moves on when the day changes (no branch on it).
        // (Sessions out of order may change days more often than there are days.)
        reserve_rows(&mut tl.rows, tl.nd.max(ns));
        let rows = &mut tl.rows;
        let r0 = rows.n;
        let dayp = rows.day.as_mut_ptr().add(r0 * 4) as *mut i32;
        let sesp = rows.sessions.as_mut_ptr().add(r0 * 4) as *mut u32;
        let evp = rows.events.as_mut_ptr().add(r0 * 4) as *mut u32;
        let revp = rows.rev.as_mut_ptr().add(r0 * 8) as *mut u64;
        let catp = rows.cat.as_mut_ptr().add(r0);
        let day0 = tl.day0;
        let mut rc = usize::MAX;
        let mut prevday = u32::MAX;
        let (mut ds, mut de) = (0u32, 0u32);
        // (The earliest start the next session may have.)
        let mut next = 0u32;
        let mut bad = false;
        for k in 0..ns {
            let w = *sw.add(2 * k);
            let (st, events) = ((w >> 32) as u32, w as u32);
            bad |= st < next;
            next = (*sw.add(2 * k + 1) as u32) + GAP + 1;
            let day = st / DAY_MS - 7;
            sess.add(k).write(Sess { start: st, day, purch: false, rev: 0, events });
            let newd = day != prevday;
            prevday = day;
            rc = rc.wrapping_add(newd as usize);
            ds = if newd { 1 } else { ds + 1 };
            de = if newd { events } else { de + events };
            dayp.add(rc).write_unaligned(day0 + day as i32);
            sesp.add(rc).write_unaligned(ds);
            evp.add(rc).write_unaligned(de);
            revp.add(rc).write_unaligned(0);
            catp.add(rc).write(NOCAT);
            *srow.add(k) = rc as u32;
            *dayrow.add(day as usize) = rc as u32;
        }
        if bad {
            return false;
        }
        tl.sess.set_len(ns);
        let nrows = rc + 1;
        rows.n = r0 + nrows;
        rows.day.set_len((r0 + nrows) * 4);
        rows.sessions.set_len((r0 + nrows) * 4);
        rows.events.set_len((r0 + nrows) * 4);
        rows.rev.set_len((r0 + nrows) * 8);
        rows.cat.set_len(r0 + nrows);
        // (The person's ID goes in once for all their rows.)
        user_run(rows, who, nrows as u32);

        // Purchases: revenue per session and row, and the best category per
        // day (as in `person_grid`). Sums only grow, so the best of every
        // sum on the way is the best of the final sums; categories are
        // ranked byte-wise, so the smaller index wins a tie.
        let ncat = tl.ncat;
        let (daycat, best, best_sum) = (tl.daycat.as_mut_ptr(), tl.best.as_mut_ptr(), tl.best_sum.as_mut_ptr());
        for &pp in purchases {
            let si = (pp >> 40) as usize;
            let s = &mut *sess.add(si);
            let cat = ((pp >> 24) & 0x1FF) as usize;
            let price = pp & 0xFF_FFFF;
            s.rev += price;
            s.purch = true;
            let r = revp.add(*srow.add(si) as usize);
            r.write_unaligned(r.read_unaligned() + price);
            let d = s.day as usize;
            let dc = daycat.add(d * ncat + cat);
            let sum = *dc + price;
            *dc = sum;
            let (bs, bc) = (*best_sum.add(d), *best.add(d));
            let better = (sum > bs) | ((sum == bs) & ((cat as u16) < bc));
            *best_sum.add(d) = std::hint::select_unpredictable(better, sum, bs);
            *best.add(d) = std::hint::select_unpredictable(better, cat as u16, bc);
        }
        // (A day may be here more than once: the first time leaves its best
        // in the row, later ones find NOCAT, which is larger.)
        for &pp in purchases {
            let si = (pp >> 40) as usize;
            let d = (*sess.add(si)).day as usize;
            let cat = ((pp >> 24) & 0x1FF) as usize;
            let c = catp.add(*srow.add(si) as usize);
            *c = (*c).min(*best.add(d));
            *best.add(d) = NOCAT;
            *best_sum.add(d) = 0;
            *daycat.add(d * ncat + cat) = 0;
        }
    }
    true
}

/// Among clicks of one person at one time for different campaigns, the one
/// with the byte-wise smallest ID decides which campaign gets the credit:
/// leave one key per campaign, that campaign's last.
#[cold]
#[inline(never)]
fn tie_group(keys: &mut [u64], i: usize, j: usize, n: &mut usize, cids: &[u32], dn: &Dn) {
    let ob = dn.obits;
    let id = |k: u64| click_key(cids[(k & ((1 << ob) - 1)) as usize]);
    let mut best = keys[i];
    for &k in &keys[i + 1..j] {
        if cmp_key(id(k), id(best)).is_lt() {
            best = k;
        }
    }
    let mut prev = u64::MAX;
    for k in i..j {
        let c = keys[k] >> ob;
        if c != prev && c != best >> ob {
            keys[*n] = keys[k];
            *n += 1;
        }
        prev = c;
    }
    keys[*n] = best;
    *n += 1;
}

/// Clicks of one person at one time for different campaigns: put the keys
/// in order for crediting (see `tie_group`), one per time and campaign.
/// Returns how many are left.
#[cold]
#[inline(never)]
fn settle_ties(keys: &mut [u64], cids: &[u32], dn: &Dn) -> usize {
    let ob = dn.obits;
    let n0 = keys.len();
    let mut n = 0;
    let mut i = 0;
    while i < n0 {
        let k = keys[i];
        let mut j = i + 1;
        while j < n0 && (keys[j] ^ k) >> 32 == 0 {
            j += 1;
        }
        if (keys[j - 1] ^ k) >> ob == 0 {
            keys[n] = k;
            n += 1;
        } else {
            tie_group(keys, i, j, &mut n, cids, dn);
        }
        i = j;
    }
    n
}

/// Sorts the first N (4, 8 or 16) of 16 keys with a fixed network
/// (Batcher's odd-even merge): no branch.
#[inline(always)]
fn sort_net<const N: usize>(k: &mut [u64; 16]) {
    macro_rules! ce {
        ($a:expr, $b:expr) => {{
            let (x, y) = (k[$a], k[$b]);
            k[$a] = x.min(y);
            k[$b] = x.max(y);
        }};
    }
    if N == 4 {
        ce!(0, 1);
        ce!(2, 3);
        ce!(0, 2);
        ce!(1, 3);
        ce!(1, 2);
    } else if N == 8 {
        ce!(0, 1);
        ce!(2, 3);
        ce!(0, 2);
        ce!(1, 3);
        ce!(1, 2);
        ce!(4, 5);
        ce!(6, 7);
        ce!(4, 6);
        ce!(5, 7);
        ce!(5, 6);
        ce!(0, 4);
        ce!(2, 6);
        ce!(2, 4);
        ce!(1, 5);
        ce!(3, 7);
        ce!(3, 5);
        ce!(1, 2);
        ce!(3, 4);
        ce!(5, 6);
    } else {
        ce!(0, 1);
        ce!(2, 3);
        ce!(0, 2);
        ce!(1, 3);
        ce!(1, 2);
        ce!(4, 5);
        ce!(6, 7);
        ce!(4, 6);
        ce!(5, 7);
        ce!(5, 6);
        ce!(0, 4);
        ce!(2, 6);
        ce!(2, 4);
        ce!(1, 5);
        ce!(3, 7);
        ce!(3, 5);
        ce!(1, 2);
        ce!(3, 4);
        ce!(5, 6);
        ce!(8, 9);
        ce!(10, 11);
        ce!(8, 10);
        ce!(9, 11);
        ce!(9, 10);
        ce!(12, 13);
        ce!(14, 15);
        ce!(12, 14);
        ce!(13, 15);
        ce!(13, 14);
        ce!(8, 12);
        ce!(10, 14);
        ce!(10, 12);
        ce!(9, 13);
        ce!(11, 15);
        ce!(11, 13);
        ce!(9, 10);
        ce!(11, 12);
        ce!(13, 14);
        ce!(0, 8);
        ce!(4, 12);
        ce!(4, 8);
        ce!(2, 10);
        ce!(6, 14);
        ce!(6, 10);
        ce!(2, 4);
        ce!(6, 8);
        ce!(10, 12);
        ce!(1, 9);
        ce!(5, 13);
        ce!(5, 9);
        ce!(3, 11);
        ce!(7, 15);
        ce!(7, 11);
        ce!(3, 5);
        ce!(7, 9);
        ce!(11, 13);
        ce!(1, 2);
        ce!(3, 4);
        ce!(5, 6);
        ce!(7, 8);
        ce!(9, 10);
        ce!(11, 12);
        ce!(13, 14);
    }
}

/// After `settle_ties`: the keys left, back into the padded array.
#[cold]
#[inline(never)]
fn small_ties(k: &mut [u64; 16], keys: &mut [u64], cids: &[u32], dn: &Dn) -> usize {
    let n0 = keys.len();
    keys.copy_from_slice(&k[..n0]);
    let n = settle_ties(keys, cids, dn);
    k[..n].copy_from_slice(&keys[..n]);
    for x in &mut k[n..] {
        *x = u64::MAX;
    }
    n
}

/// `attribute` for a person with 1 to 16 clicks, with as few branches that
/// depend on the data as can be: the keys are sorted by a network, and each
/// session finds its clicks by counting with vector compares (the clicks
/// at most 7 days before its start are a range [lo, hi) of the sorted
/// keys; the last of them credits it; of each campaign in the range the
/// last click assists it).
#[inline(always)]
unsafe fn attribute_small<const N: usize>(keys: &mut [u64], cids: &[u32], dn: &Dn, me: &mut BOut, sess: *const Sess, ns: usize) {
    use std::arch::x86_64::*;
    let (nd, organic) = (me.tl.nd, me.tl.organic);
    let (credit, assist) = (me.credit.as_mut_ptr(), me.assist.as_mut_ptr());
    let ob = dn.obits;
    let n0 = keys.len();
    let kp = keys.as_ptr();
    let mut k = [u64::MAX; 16];
    for (i, x) in k.iter_mut().enumerate().take(N) {
        let v = *kp.add(i.min(n0 - 1)) | i as u64;
        *x = if i < n0 { v } else { u64::MAX };
    }
    sort_net::<N>(&mut k);
    // Two clicks at one time for different campaigns differ by at least a
    // campaign step and by less than a time step (the padding never does).
    let (lo1, span) = (1u64 << ob, (1u64 << 32) - (1u64 << ob));
    let mut tie = false;
    for i in 1..N {
        tie |= (k[i] ^ k[i - 1]).wrapping_sub(lo1) < span;
    }
    if tie {
        // (This pads the keys left again.)
        small_ties(&mut k, keys, cids, dn);
    }
    let cmask = (1u64 << (32 - ob)) - 1;
    // Per click: its time and its time plus 7 days (as signed numbers for
    // the compares, the padding above every time), and its campaign's row.
    let mut ts = [0x7FFF_FFFFu32; 16];
    let mut tw = [0x7FFF_FFFFu32; 16];
    let mut camp = [organic; 16];
    for j in 0..N {
        let t = (k[j] >> 32) as u32;
        let real = k[j] != u64::MAX;
        ts[j] = t ^ 0x8000_0000;
        tw[j] = (if real { t + WEEK } else { u32::MAX }) ^ 0x8000_0000;
        camp[j] = if real { ((k[j] >> ob) & cmask) as usize } else { organic };
    }
    // nxt[j]: the next click of the same campaign (16: none).
    me.serial += 1;
    let serial = me.serial;
    let cov = me.cov.as_mut_ptr();
    let mut nxt = [0i32; 16];
    for j in (0..N).rev() {
        let e = &mut *cov.add(camp[j]);
        let m = 0u32.wrapping_sub((e[0] == serial) as u32);
        nxt[j] = ((e[1] & m) | (16 & !m)) as i32;
        *e = [serial, j as u32];
    }
    let mut row = [0usize; 16];
    for j in 0..N {
        row[j] = camp[j] * nd;
    }
    let ld = |a: &[u32; 16], h: usize| _mm256_loadu_si256(a.as_ptr().add(h) as *const __m256i);
    let (ts0, ts1, tw0, tw1) = (ld(&ts, 0), ld(&ts, 8), ld(&tw, 0), ld(&tw, 8));
    let nx0 = _mm256_loadu_si256(nxt.as_ptr() as *const __m256i);
    let nx1 = _mm256_loadu_si256(nxt.as_ptr().add(8) as *const __m256i);
    let mm = |a: __m256i, b: __m256i| {
        (_mm256_movemask_ps(_mm256_castsi256_ps(a)) as u32) | (_mm256_movemask_ps(_mm256_castsi256_ps(b)) as u32) << 8
    };
    for i in 0..ns {
        let s = &*sess.add(i);
        let st = _mm256_set1_epi32((s.start ^ 0x8000_0000) as i32);
        // Clicks at or before the start, and those more than 7 days before it.
        let hi = 16 - mm(_mm256_cmpgt_epi32(ts0, st), _mm256_cmpgt_epi32(ts1, st)).count_ones();
        let lo = mm(_mm256_cmpgt_epi32(st, tw0), _mm256_cmpgt_epi32(st, tw1)).count_ones();
        let c = if hi > lo { camp[(hi as usize).max(1) - 1] } else { organic };
        let day = s.day as usize;
        let cr = &mut *credit.add(c * nd + day);
        cr.sessions += 1;
        cr.purch += s.purch as u32;
        cr.rev += s.rev;
        // Of each campaign in [lo, hi), the click whose next one is not.
        let h1 = _mm256_set1_epi32(hi as i32 - 1);
        let last = mm(_mm256_cmpgt_epi32(nx0, h1), _mm256_cmpgt_epi32(nx1, h1));
        let cm = last & ((1u32 << hi) - 1) & !((1u32 << lo) - 1);
        // (All N, the padding's bits being 0: a count fixed at compile time.)
        for j in 0..N {
            *assist.add(row[j] + day) += (cm >> j) & 1;
        }
    }
}

/// Credit and assists for the sessions in `tl.sess` (in time order) from the
/// person's clicks: their keys, in any order, and their numbers.
///
/// What `attribute` in main.rs does, for clicks as sortable words. Clicks
/// at the same time for the same campaign need not be told apart: neither
/// credit nor assists can depend on which of them is which.
///
/// Sessions and clicks are both in time order, so two merges place every
/// click among the sessions: one gives, per session, the clicks at or
/// before its start (the last of them credits it, if within 7 days) and,
/// per click, the first session it could assist; the other gives the last.
/// The merges advance by arithmetic on a comparison, not by a branch on it.
fn attribute(keys: &mut [u64], cids: &[u32], dn: &Dn, me: &mut BOut) {
    let (nd, organic) = (me.tl.nd, me.tl.organic);
    let ns = me.tl.sess.len();
    let sess = me.tl.sess.as_ptr();
    let (credit, assist) = (me.credit.as_mut_ptr(), me.assist.as_mut_ptr());
    let n0 = keys.len();
    unsafe {
        if n0 == 0 {
            for i in 0..ns {
                let s = &*sess.add(i);
                let c = &mut *credit.add(organic * nd + s.day as usize);
                c.sessions += 1;
                c.purch += s.purch as u32;
                c.rev += s.rev;
            }
            return;
        }
        let ob = dn.obits;
        if n0 >> ob != 0 {
            // More clicks than a key can number: the general code.
            me.big.clear();
            me.big.extend(keys.iter().zip(cids).map(|(&k, &cid)| Ckd { ts: (k >> 32) as u32, cid, camp: (k as u32) >> ob }));
            crate::attribute(&mut me.big, &mut me.tl);
            return;
        }
        if n0 <= 16 {
            if n0 <= 4 {
                attribute_small::<4>(keys, cids, dn, me, sess, ns);
            } else if n0 <= 8 {
                attribute_small::<8>(keys, cids, dn, me, sess, ns);
            } else {
                attribute_small::<16>(keys, cids, dn, me, sess, ns);
            }
            return;
        }
        if me.timed {
            // (statistics: persons and click x session pairs by size class)
            let prod = n0 * ns;
            let class = if prod <= 64 { 0 } else if prod <= 256 { 1 } else if prod <= 1024 { 2 } else { 3 };
            STATS[class].fetch_add(1, Relaxed);
            STATS[4 + class].fetch_add(prod, Relaxed);
            STATS[8 + class].fetch_add(n0, Relaxed);
        }
        let kp = keys.as_mut_ptr();
        let mut n = n0;
        if n0 > 1 {
            for i in 0..n0 {
                *kp.add(i) |= i as u64;
            }
            keys.sort_unstable();
            // Two clicks at one time for different campaigns differ by at
            // least a campaign step and by less than a time step.
            let (lo, span) = (1u64 << ob, (1u64 << 32) - (1u64 << ob));
            let mut tie = false;
            let mut prev = *kp;
            for i in 1..n0 {
                let k = *kp.add(i);
                tie |= (k ^ prev).wrapping_sub(lo) < span;
                prev = k;
            }
            if tie {
                n = settle_ties(keys, cids, dn);
            }
        }
        let cmask = (1u64 << (32 - ob)) - 1;
        me.hi.clear();
        me.hi.reserve(ns);
        me.ca.clear();
        me.ca.reserve(n);
        me.cb.clear();
        me.cb.reserve(n);
        let (hi, ca, cb) = (me.hi.as_mut_ptr(), me.ca.as_mut_ptr(), me.cb.as_mut_ptr());

        // First merge: a click goes before the sessions that start at or after it.
        let (mut i, mut j) = (0usize, 0usize);
        while i < ns && j < n {
            let click = ((*kp.add(j) >> 32) as u32 <= (*sess.add(i)).start) as usize;
            *hi.add(i) = j as u32;
            *ca.add(j) = i as u32;
            j += click;
            i += 1 - click;
        }
        for k in i..ns {
            *hi.add(k) = n as u32;
        }
        // (Clicks after the last session's start assist nothing.)
        let live = j;

        // Credit: the latest click at or before the session's start, if within 7 days.
        for i in 0..ns {
            let s = &*sess.add(i);
            let h = *hi.add(i) as usize;
            // (With no click before it, the first one is looked at: it is later, so not within.)
            let k = *kp.add(h.max(1) - 1);
            let within = s.start.wrapping_sub((k >> 32) as u32) <= WEEK;
            let camp = if within { ((k >> ob) & cmask) as usize } else { organic };
            let c = &mut *credit.add(camp * nd + s.day as usize);
            c.sessions += 1;
            c.purch += s.purch as u32;
            c.rev += s.rev;
        }

        // Second merge: a session goes before the clicks made less than 7 days before it or later.
        let (mut i, mut j) = (0usize, 0usize);
        while j < live && i < ns {
            let first = ((*sess.add(i)).start <= (*kp.add(j) >> 32) as u32 + WEEK) as usize;
            *cb.add(j) = i as u32;
            i += first;
            j += 1 - first;
        }
        for k in j..live {
            *cb.add(k) = ns as u32;
        }

        // Assists: click j assists sessions ca[j]..cb[j]. A campaign's clicks
        // must count a session once, so each campaign remembers how far its
        // earlier clicks reached.
        me.serial += 1;
        let serial = me.serial;
        let cov = me.cov.as_mut_ptr();
        let nch = (nd + 7) / 8;
        let df = day_index(me, nch);
        for i in 0..ns {
            *df.add((*sess.add(i)).day as usize + 1) = i as i32 + 1;
        }
        day_prefix(df, nch);
        for j in 0..live {
            let c = ((*kp.add(j) >> ob) & cmask) as usize;
            let (a, b) = (*ca.add(j), *cb.add(j) as usize);
            let e = &mut *cov.add(c);
            // (Clicks are in time order, so the campaign's earlier reach is at most `b`.)
            let seen = 0u32.wrapping_sub((e[0] == serial) as u32);
            let from = a.max(e[1] & seen) as usize;
            *e = [serial, b as u32];
            let n = b - from;
            if me.timed {
                NHIST[n.min(15)].fetch_add(1, Relaxed);
            }
            // The first two sessions without a branch (see the two spare sessions above).
            add_days(assist.add(c * nd), df, from as i32, b as i32, nch);
        }
    }
}

/// The scratch for `add_days`, cleared: `nch` vectors of days and one more.
#[inline(always)]
unsafe fn day_index(me: &mut BOut, nch: usize) -> *mut i32 {
    use std::arch::x86_64::*;
    if me.dayfirst.len() < nch * 8 + 16 {
        me.dayfirst.resize(nch * 8 + 16, 0);
    }
    let df = me.dayfirst.as_mut_ptr() as *mut i32;
    for t in 0..=nch {
        _mm256_storeu_si256(df.add(t * 8) as *mut __m256i, _mm256_setzero_si256());
    }
    df
}

/// After each session `i` stored `i + 1` at its day plus one: `df[d]` is
/// the number of sessions before day `d` (for every `d` up to `nch * 8`).
#[inline(always)]
unsafe fn day_prefix(df: *mut i32, nch: usize) {
    let mut m = 0;
    for d in 1..=nch * 8 {
        m = m.max(*df.add(d));
        *df.add(d) = m;
    }
}

/// Adds to `row[d]`, for every day `d`, how many of the sessions `a..b` are
/// on day `d` (see `day_prefix`). No branch; the row may run up to `nch * 8`
/// days, adding 0 past the last (the table has room after its last row).
#[inline(always)]
unsafe fn add_days(row: *mut u32, df: *const i32, a: i32, b: i32, nch: usize) {
    use std::arch::x86_64::*;
    let (va, vb) = (_mm256_set1_epi32(a), _mm256_set1_epi32(b));
    for t in 0..nch {
        let lo = _mm256_loadu_si256(df.add(t * 8) as *const __m256i);
        let hi = _mm256_loadu_si256(df.add(t * 8 + 1) as *const __m256i);
        let c = _mm256_max_epi32(_mm256_sub_epi32(_mm256_min_epi32(vb, hi), _mm256_max_epi32(va, lo)), _mm256_setzero_si256());
        let r = row.add(t * 8) as *mut __m256i;
        _mm256_storeu_si256(r, _mm256_add_epi32(_mm256_loadu_si256(r), c));
    }
}

/// One person whose events are (believed to be) in time order.
#[inline(always)]
fn person_run(p: u32, evs: &mut [u64], keys: &mut [u64], cids: &[u32], dn: &Dn, me: &mut BOut) {
    let who = dn.person_key(p);
    // (A name too long for a key goes the general way.)
    if is_long(who) || !person_stream(who, evs, me) {
        if me.timed {
            UNORDERED.fetch_add(1, Relaxed);
        }
        person_sorted(who, evs, &mut me.tl);
    }
    attribute(keys, cids, dn, me);
}

/// (Statistics, kept when timed: persons with one run whose sessions had to
/// be put together (events out of time order); persons with several runs;
/// sessions joined to the one before when putting runs together; persons
/// whose sessions were not in order of start.)
static RUNSTAT: [AtomicUsize; 4] = [const { AtomicUsize::new(0) }; 4];

/// The sessions and purchases of a run at `hdr` (see `Walk`).
#[inline(always)]
unsafe fn run_parts(hdr: *const u64) -> (*const u64, usize, *const u64, usize) {
    let ns = (*hdr >> 32) as usize;
    let np = (*hdr.add(2) >> 32) as usize;
    (hdr.add(1), ns, hdr.add(1 + 2 * ns), np)
}

/// One person whose events all came as one run, at `hdr`.
#[inline(always)]
unsafe fn person_sess(p: u32, hdr: *mut u64, keys: &mut [u64], cids: &[u32], dn: &Dn, me: &mut BOut) {
    let who = dn.person_key(p);
    // (A direct person's name is always short.)
    debug_assert!(!is_long(who));
    let (sw, ns, pu, np) = run_parts(hdr);
    if !sess_rows(who, sw, ns, pu, np, me) {
        if me.timed {
            RUNSTAT[0].fetch_add(1, Relaxed);
        }
        return person_multi(p, &[(p, hdr, 0)], keys, cids, dn, me);
    }
    attribute(keys, cids, dn, me);
}

/// The time of a run's first event.
#[inline(always)]
unsafe fn run_first(hdr: *const u64) -> u32 {
    (*hdr.add(1) >> 32) as u32
}

/// One person whose sessions came as several runs (`runs`, best sorted by
/// their first times), or as one run whose sessions are not in order: put
/// together exactly. In order of start, a session joins the one before when
/// it starts at most 30 minutes after the latest end so far (see `Walk`: the
/// sessions are chains of events; a chain within 30 minutes of another, or
/// overlapping it, makes one chain with it).
unsafe fn person_multi(p: u32, runs: &[(u32, *mut u64, u32)], keys: &mut [u64], cids: &[u32], dn: &Dn, me: &mut BOut) {
    // Every run's sessions, one after the other, and their purchases
    // (numbered among these).
    let all = &mut me.sall;
    all.clear();
    me.spurch.clear();
    for r in runs {
        let (sw, ns, pu, np) = run_parts(r.1);
        let base = (all.len() / 2) as u64;
        all.extend_from_slice(std::slice::from_raw_parts(sw, 2 * ns));
        for &pw in std::slice::from_raw_parts(pu, np) {
            me.spurch.push(pw + (base << 40));
        }
    }
    let n = all.len() / 2;
    // In order of start (already so unless runs overlap or events were out of order).
    let order = &mut me.sorder;
    order.clear();
    order.extend(0..n as u32);
    let sorted = (1..n).all(|k| all[2 * k] >> 32 >= all[2 * k - 2] >> 32);
    if !sorted {
        order.sort_unstable_by_key(|&k| all[2 * k as usize] >> 32);
    }
    let (map, merged) = (&mut me.smap, &mut me.sstart);
    map.clear();
    map.resize(n, 0);
    merged.clear();
    let mut end = 0u32;
    let mut joined = 0usize;
    for &k in order.iter() {
        let (w, e) = (all[2 * k as usize], all[2 * k as usize + 1] as u32);
        let st = (w >> 32) as u32;
        if !merged.is_empty() && st.saturating_sub(end) <= GAP {
            let at = merged.len() - 2;
            merged[at] += w & 0xFFFF_FFFF;
            end = end.max(e);
            merged[at + 1] = end as u64;
            joined += 1;
        } else {
            merged.push(w);
            merged.push(e as u64);
            end = e;
        }
        map[k as usize] = (merged.len() / 2 - 1) as u32;
    }
    for pw in me.spurch.iter_mut() {
        *pw = (*pw & ((1 << 40) - 1)) | (map[(*pw >> 40) as usize] as u64) << 40;
    }
    if me.timed {
        RUNSTAT[1].fetch_add((runs.len() > 1) as usize, Relaxed);
        RUNSTAT[2].fetch_add(joined, Relaxed);
        RUNSTAT[3].fetch_add(!sorted as usize, Relaxed);
    }
    let who = dn.person_key(p);
    let (sw, ns, pu, np) = (merged.as_ptr(), merged.len() / 2, me.spurch.as_ptr(), me.spurch.len());
    let ok = sess_rows(who, sw, ns, pu, np, me);
    debug_assert!(ok);
    attribute(keys, cids, dn, me);
}

static NHIST: [AtomicUsize; 16] = [const { AtomicUsize::new(0) }; 16];

/// (Statistics: persons whose single run of events was not in time order.)
static UNORDERED: AtomicUsize = AtomicUsize::new(0);

/// Person bucket `q`: its clicks sorted by person, then every person.
fn person_bucket(q: usize, segs: &[Seg], outs: &[&DOut], dn: &Dn, me: &mut BOut) {
    let bslots = 1usize << (dn.ku + 3);
    let smask = (bslots - 1) as u32;
    let ob = dn.obits;
    let nck: usize = outs.iter().map(|o| o.ck.len(q)).sum();
    let timed = me.timed;
    let t_a = if timed { sys::thread_cpu() } else { 0.0 };
    me.keys.clear();
    me.cids.clear();
    if nck > 0 {
        me.cnt.clear();
        me.cnt.resize(bslots, 0);
        if timed { me.prof[5] += sys::thread_cpu() - t_a; }
        let mut n = 0usize;
        for o in outs {
            for (ptr, len) in o.ck.pieces(q) {
                for r in unsafe { std::slice::from_raw_parts(ptr, len) } {
                    let node = r[1] as u32;
                    let x = unsafe { *dn.r8.add(node as usize) };
                    if x >= 8 {
                        me.sp_ck.push([dn.special_person(node) as u64 | (r[1] >> 32) << 32, r[0]]);
                        continue;
                    }
                    me.cnt[((node ^ x as u32) & smask) as usize] += 1;
                    n += 1;
                }
            }
        }
        if timed { me.prof[6] -= sys::thread_cpu(); }
        let mut at = 0u32;
        for c in me.cnt.iter_mut() {
            let k = *c;
            *c = at;
            at += k;
        }
        if timed { me.prof[6] += sys::thread_cpu(); }
        me.keys.reserve(n);
        me.cids.reserve(n);
        // Every element is written below.
        unsafe {
            me.keys.set_len(n);
            me.cids.set_len(n);
        }
        for o in outs {
            for (ptr, len) in o.ck.pieces(q) {
                for r in unsafe { std::slice::from_raw_parts(ptr, len) } {
                    let node = r[1] as u32;
                    let x = unsafe { *dn.r8.add(node as usize) };
                    if x >= 8 {
                        continue;
                    }
                    let slot = ((node ^ x as u32) & smask) as usize;
                    let at = me.cnt[slot] as usize;
                    me.cnt[slot] = at as u32 + 1;
                    unsafe {
                        *me.keys.get_unchecked_mut(at) = (r[0] & !0xFFFF_FFFF) | (r[0] & 0xFFFF_FFFF) << ob;
                        *me.cids.get_unchecked_mut(at) = (r[1] >> 32) as u32;
                    }
                }
            }
        }
        // `cnt` now holds each slot's end.
    }
    let has_clicks = !me.keys.is_empty();
    let cnt = me.cnt.as_ptr();
    let (keys, cids) = (me.keys.as_mut_ptr(), me.cids.as_ptr());
    let clicks = |p: u32| -> (&mut [u64], &[u32]) {
        if !has_clicks {
            return (&mut [], &[]);
        }
        let slot = (p & smask) as usize;
        unsafe {
            let a = if slot == 0 { 0 } else { *cnt.add(slot - 1) } as usize;
            let b = *cnt.add(slot) as usize;
            (std::slice::from_raw_parts_mut(keys.add(a), b - a), std::slice::from_raw_parts(cids.add(a), b - a))
        }
    };

    let t_b = if timed { sys::thread_cpu() } else { 0.0 };
    me.prof[0] += t_b - t_a;
    me.irr.clear();
    for seg in segs {
        let mut p = seg.ptr;
        let end = unsafe { p.add(seg.words as usize) };
        while p < end {
            let hdr = p;
            let (person, ns, np) = unsafe { (*hdr as u32, (*hdr >> 32) as usize, (*hdr.add(2) >> 32) as usize) };
            p = unsafe { p.add(1 + 2 * ns + np) };
            if dn.one_run(person) {
                let (k, c) = clicks(person);
                me.prof[3] += ns as f64;
                unsafe { person_sess(person, hdr, k, c, dn, me) };
            } else {
                me.prof[4] += ns as f64;
                me.irr.push((person, hdr, unsafe { run_first(hdr) }));
            }
        }
    }
    let t_c = if timed { sys::thread_cpu() } else { 0.0 };
    me.prof[1] += t_c - t_b;
    if !me.irr.is_empty() {
        // Persons whose events came in several runs: in time order.
        let mut irr = std::mem::take(&mut me.irr);
        irr.sort_unstable_by_key(|r| (r.0, r.2));
        let mut i = 0;
        while i < irr.len() {
            let person = irr[i].0;
            let mut j = i + 1;
            while j < irr.len() && irr[j].0 == person {
                j += 1;
            }
            let (k, c) = clicks(person);
            unsafe { person_multi(person, &irr[i..j], k, c, dn, me) };
            i = j;
        }
        me.irr = irr;
    }
    if timed {
        me.prof[2] += sys::thread_cpu() - t_c;
    }
}

/// A click of a person that needs a lookup: (person, key (see `Dn::obits`), click number).
type SpCk = (u32, u64, u32);

/// The persons that need a lookup, put together once.
struct Special {
    /// The clicks of interned users, sorted by person.
    ext: Vec<SpCk>,
    /// Every person with events, biggest first (events and clicks of
    /// interned users).
    persons: Vec<u32>,
}

/// The special persons' event lists sorted (`runs`: per list, each person's
/// number of events): put the persons in order of size, and the clicks of
/// interned users by person.
fn prep_special(outs: &[&DOut], runs: &[&[(u32, u32)]], dn: &Dn) -> Special {
    let ob = dn.obits;
    let key = |w: u64| (w & !0xFFFF_FFFF) | (w & 0xFFFF_FFFF) << ob;
    let mut ext: Vec<SpCk> = Vec::new();
    for o in outs {
        for (ptr, len) in o.ck.pieces(dn.nq) {
            for r in unsafe { std::slice::from_raw_parts(ptr, len) } {
                ext.push((dn.special_person(r[1] as u32), key(r[0]), (r[1] >> 32) as u32));
            }
        }
    }
    ext.sort_unstable_by_key(|c| c.0);
    let mut all: Vec<(u32, u32)> = runs.iter().flat_map(|r| r.iter()).copied().collect();
    all.sort_unstable();
    let mut sized: Vec<(u32, u32)> = Vec::new();
    let mut c = 0;
    for &(p, n) in &all {
        match sized.last_mut() {
            Some(last) if last.0 == p => last.1 += n,
            _ => {
                while c < ext.len() && ext[c].0 < p {
                    c += 1;
                }
                let c0 = c;
                while c < ext.len() && ext[c].0 == p {
                    c += 1;
                }
                sized.push((p, n + (c - c0) as u32));
            }
        }
    }
    sized.sort_unstable_by_key(|&(p, n)| (std::cmp::Reverse(n), p));
    Special { ext, persons: sized.into_iter().map(|(p, _)| p).collect() }
}

/// The entries of `list` (sorted by person) that are person `p`'s.
#[inline]
fn of_person<T>(list: &[T], p: u32, person: impl Fn(&T) -> u32) -> &[T] {
    let lo = list.partition_point(|x| person(x) < p);
    let n = list[lo..].partition_point(|x| person(x) == p);
    &list[lo..lo + n]
}

// ---------------------------------------------------------------- the run

/// Do the whole job the dense way. False: not this input (nothing was
/// written); the general pipeline must run.
pub fn run(data_dir: &Path, out_dir: &Path, threads: usize, t0: Instant) -> bool {
    let log = std::env::var_os("SS_TIMING").is_some();
    let lap = |what: &str| {
        if log {
            let (user, sys, faults) = sys::usage();
            eprintln!("{:>8.1} ms  {:<28} user {:>7.1} sys {:>6.1} faults {}", t0.elapsed().as_secs_f64() * 1e3, what, user, sys, faults);
        }
    };
    let event_files = input::list_dir(&data_dir.join("events"));
    let click_files = input::list_dir(&data_dir.join("clicks"));
    let link_files = input::list_dir(&data_dir.join("links"));
    let Some(plan) = plan(&event_files, &click_files) else { return false };
    if log {
        eprintln!("    dense: keys {:x} {:x} {:x}, bounds {} events {} users {} clicks", plan.keys.event, plan.keys.user, plan.keys.click, plan.max_event, plan.max_user, plan.max_click);
    }
    let mut ctx = build_ctx(data_dir, 0, 0, 0, 1);

    // Events to a bucket: 2^15 up to about 33M events, twice that above (measured on
    // the scoring machine: at 60M events 2^16 is 2.5% faster, at 20M it makes no difference).
    let bits = 64 - plan.max_event.max(1).leading_zeros();
    let kb = env_usize("SS_KB", (bits as usize).saturating_sub(10).clamp(15, 17)) as u32;
    let ku = env_usize("SS_KU", 10) as u32;
    let nq = (plan.max_user as usize).div_ceil(1 << ku);
    let d = nq << (ku + 3);
    let nodes = d + XCAP;
    if nodes >= (1 << 31) - 2 || kb > 20 || plan.max_event + EV_EXTRA >= 1 << 40 {
        return false;
    }
    let nb = ((plan.max_event + EV_EXTRA) >> kb) as usize + 1;
    ctx.qshift = ku + 3;
    let mut dn = Dn {
        keys: plan.keys,
        max_event: plan.max_event,
        max_click: plan.max_click,
        vbound: plan.max_user * 512,
        kb,
        nb,
        ku,
        nq,
        d,
        r8: map_anon(d + XCAP),
        runs: map_anon(d),
        parent: map_anon(nodes * 4) as *mut u32,
        users: RwLock::new(Names { map: HashMap::new(), list: Vec::new() }),
        events: Mutex::new(HashMap::new()),
        clicks: RwLock::new(Names { map: HashMap::new(), list: Vec::new() }),
        overflow: AtomicBool::new(false),
        ext_person: Vec::new(),
        direct_special: HashMap::new(),
        names: Vec::new(),
        click_names: Vec::new(),
        obits: 0,
        ckx: std::sync::OnceLock::new(),
        cka: std::sync::OnceLock::new(),
    };
    // Interned users are always looked up.
    unsafe { std::ptr::write_bytes(dn.r8.add(d), 0xFF, XCAP) };
    let chunk = env_usize("SS_CHUNK", 64 << 10);
    let outs: Vec<Cell<DOut>> = (0..threads)
        .map(|_| {
            Shared(UnsafeCell::new(DOut {
                arena: Arena::new(),
                ev: EvLists::new(nb),
                ck: CkLists::new(nq + 1),
                reader: input::Reader::new(chunk),
                pend_l: Vec::with_capacity(PEND),
            }))
        })
        .collect();
    lap("setup");

    // ------------------------------------------------------------ links and parse
    // One queue: the link files' pieces first, then the event and click
    // files'. A thread that runs out of link pieces goes straight on to the
    // parse; the joined sets are settled once everything is read (the
    // parse needs nothing of them: the dedup is the first to read them).
    // Before its first piece each thread touches its share of the parents
    // and maps some of the event and click files; one thread first reads
    // the products and campaigns and makes the click kernel's tables.
    let mapped = env_usize("SS_MMAP", 1) != 0;
    let piece = env_usize("SS_PIECE", if mapped { 4 << 20 } else { chunk });
    let mut tasks = Vec::new();
    grid_tasks(&link_files, false, chunk, &mut tasks);
    let n_link = tasks.len();
    grid_tasks(&event_files, true, piece, &mut tasks);
    grid_tasks(&click_files, false, piece, &mut tasks);
    let to_map: Vec<&Src> = if mapped { event_files.iter().chain(click_files.iter()).collect() } else { Vec::new() };
    let next_map = AtomicUsize::new(0);
    let maps_done = AtomicUsize::new(0);
    let touched = AtomicUsize::new(0);
    let links_left = AtomicUsize::new(threads);
    let links_at = AtomicU64::new(0);
    let next = AtomicUsize::new(0);
    // (Asking for a piece's pages in one call costs more than their faults on the scoring machine.)
    let populate = env_usize("SS_POP", 0) != 0;
    // The context with the products and campaigns in it: set (once, with
    // `dn.ckx` before it) by thread `aux_tid`; no event or click piece is
    // parsed before it is.
    let ready: std::sync::OnceLock<Ctx> = std::sync::OnceLock::new();
    let ctx_slot = Mutex::new(Some(ctx));
    let aux_failed = AtomicBool::new(false);
    let aux_panic = Mutex::new(None);
    // (The last thread reads them: a pool worker, whose heap is grown ahead.)
    let aux_tid = threads - 1;
    lap("links");
    {
        let dn = &dn;
        par(threads, |tid| {
            let out = mine(&outs, tid);
            // The parents are read before they are written: touch them first,
            // so each 2 MB comes in as one huge page (see `IdTable::touch_part`).
            // (A write that changes nothing, so it is sound whatever runs.)
            let bytes = d * 4;
            let (a, b) = (bytes * tid / threads / 4096 * 4096, bytes * (tid + 1) / threads / 4096 * 4096);
            let mut o = a;
            while o < b {
                dn.parent_slot((o / 4) as u32).fetch_or(0, Relaxed);
                o += 4096;
            }
            touched.fetch_add(1, Release);
            if tid == aux_tid {
                // (Should this panic, nobody waits for it; the panic goes on
                // once all threads are back.)
                let mut ctx = ctx_slot.lock().unwrap().take().unwrap();
                match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| load_aux(data_dir))) {
                    Ok(aux) => {
                        ctx.aux = aux;
                        let _ = dn.ckx.set(crate::dense_ck::Cx::new(&ctx, dn));
                        let _ = dn.cka.set(crate::dense_cka::KC::new(&ctx, dn));
                        let _ = ready.set(ctx);
                    }
                    Err(e) => {
                        *aux_panic.lock().unwrap() = Some(e);
                        aux_failed.store(true, Release);
                    }
                }
            }
            // The event and click files are mapped now, before any of them is
            // parsed (no thread maps anything once the parse has started).
            loop {
                let i = next_map.fetch_add(1, Relaxed);
                let Some(src) = to_map.get(i) else { break };
                src.map_shared();
                maps_done.fetch_add(1, Release);
            }
            wait_until(|| touched.load(Acquire) == threads);
            let mut ctx: Option<&Ctx> = None;
            let mut linking = true;
            loop {
                let i = next.fetch_add(1, Relaxed);
                let Some(task) = tasks.get(i) else { break };
                if i < n_link {
                    let (start, end) = out.reader.load(&link_files[task.file], task.a, task.b);
                    let buf = std::mem::take(&mut out.reader.buf);
                    parse_links(&buf[..end + 128], start, end, dn, out);
                    out.reader.buf = buf;
                    continue;
                }
                if linking {
                    linking = false;
                    flush_links(dn, out);
                    if links_left.fetch_sub(1, Relaxed) == 1 {
                        links_at.store(t0.elapsed().as_micros() as u64, Relaxed);
                    }
                }
                let ctx = match ctx {
                    Some(c) => c,
                    None => {
                        // (The maps' and the side thread's stores are seen from here on.)
                        wait_until(|| (ready.get().is_some() && maps_done.load(Acquire) == to_map.len()) || aux_failed.load(Acquire));
                        let Some(c) = ready.get() else { break };
                        *ctx.insert(c)
                    }
                };
                let src = if task.events { &event_files[task.file] } else { &click_files[task.file] };
                if src.mapped() {
                    if populate {
                        src.populate(task.a, task.b);
                    }
                    let (buf, start, end) = src.view(task.a, task.b);
                    if task.events {
                        parse_events(buf, start, end, ctx, dn, out);
                    } else {
                        parse_clicks(buf, start, end, ctx, dn, out);
                    }
                    continue;
                }
                let (start, end) = out.reader.load(src, task.a, task.b);
                let buf = std::mem::take(&mut out.reader.buf);
                if task.events {
                    parse_events(&buf[..end + 128], start, end, ctx, dn, out);
                } else {
                    parse_clicks(&buf[..end + 128], start, end, ctx, dn, out);
                }
                out.reader.buf = buf;
            }
            if linking {
                flush_links(dn, out);
                if links_left.fetch_sub(1, Relaxed) == 1 {
                    links_at.store(t0.elapsed().as_micros() as u64, Relaxed);
                }
            }
            out.ev.finish(&mut out.arena);
            out.ck.finish(&mut out.arena);
        });
        // SS_KBENCH=N (measuring only): the event pieces N more times, then the
        // click pieces N more times, each pass timed (wall/user/kernel ms), with
        // every input page already in; then the run stops without an answer.
        if let Some(n) = std::env::var("SS_KBENCH").ok().and_then(|v| v.parse::<usize>().ok()) {
            if let Some(ctx) = ready.get() {
                for what in [true, false] {
                    let bytes: usize = tasks[n_link..].iter().filter(|t| t.events == what).map(|t| t.b - t.a).sum();
                    let mut line = format!("KB{threads}{} {}MB", if what { "e" } else { "c" }, bytes >> 20);
                    for _ in 0..n {
                        next.store(n_link, Relaxed);
                        let (u0, s0, _) = sys::usage();
                        let t = Instant::now();
                        par(threads, |tid| {
                            let out = mine(&outs, tid);
                            loop {
                                let i = next.fetch_add(1, Relaxed);
                                let Some(task) = tasks.get(i) else { break };
                                if task.events != what {
                                    continue;
                                }
                                let src = if task.events { &event_files[task.file] } else { &click_files[task.file] };
                                let (buf, start, end) = src.view(task.a, task.b);
                                if task.events {
                                    parse_events(buf, start, end, ctx, dn, out);
                                } else {
                                    parse_clicks(buf, start, end, ctx, dn, out);
                                }
                            }
                        });
                        let w = t.elapsed().as_secs_f64() * 1e3;
                        let (u1, s1, _) = sys::usage();
                        line += &format!(" {:.0}/{:.0}/{:.0}", w, u1 - u0, s1 - s0);
                    }
                    eprintln!("{line}");
                }
            }
            std::process::exit(0);
        }
    }
    drop(to_map);
    if let Some(e) = aux_panic.into_inner().unwrap() {
        std::panic::resume_unwind(e);
    }
    let ctx = ready.into_inner().expect("products and campaigns");
    lap("  parse: links, events, clicks");
    if log {
        eprintln!("    links: last link piece done at {:.1} ms", links_at.load(Relaxed) as f64 / 1e3);
    }
    // (Every interned user is known now, those of the events and clicks too.)
    settle_links(&mut dn, threads);
    // Campaign numbers take the bits they need; the rest of a click key's
    // low half numbers the click among its person's.
    dn.obits = (ctx.aux.camp_names.len() as u32).leading_zeros();
    lap("    links: settled");
    dn.names = dn.users.read().unwrap().list.clone();
    dn.click_names = dn.clicks.read().unwrap().list.clone();
    lap("parse");
    if dn.overflow.load(Relaxed) {
        return give_up(&dn, &outs, nodes);
    }
    let dn = dn;
    CURRENT.store(&dn as *const Dn as *mut Dn, Relaxed);
    let outs_ref: Vec<&DOut> = (0..threads).map(|t| &*mine(&outs, t)).collect();
    // The input text is done with (every name kept was copied): one more
    // thread gives its pages back while the stages below run, so that this
    // is neither left for the end nor for the exit.
    // (SS_UNMAP=final: every thread gives back its share at the end instead;
    // SS_UNMAP=none: left to the exit.)
    let unmap_mode = std::env::var("SS_UNMAP").unwrap_or_default();
    let in_files: std::sync::Arc<Vec<Src>> = std::sync::Arc::new(event_files.into_iter().chain(click_files).collect());
    // (The parents too: nothing reads them once the links are settled.)
    let parent = dn.parent as usize;
    if !unmap_mode.is_empty() {
        release(parent as *mut u8, nodes * 4);
    }
    let unmapper = unmap_mode.is_empty().then(|| {
        let files = in_files.clone();
        std::thread::spawn(move || {
            release(parent as *mut u8, nodes * 4);
            for f in files.iter() {
                f.unmap();
            }
        })
    });

    // ------------------------------------------------------------ dedup
    let aouts: Vec<Cell<AOut>> =
        (0..threads).map(|_| Shared(UnsafeCell::new(AOut { arena: Arena::new(), segs: Vec::new(), sp_ev: Vec::new(), w: Vec::new(), purch: Vec::new(), pscr: Vec::new() }))).collect();
    let next = AtomicUsize::new(0);
    par(threads, |tid| {
        let me = mine(&aouts, tid);
        loop {
            let b = next.fetch_add(1, Relaxed);
            if b >= nb || dn.overflow.load(Relaxed) {
                break;
            }
            dedup_bucket(b, &outs_ref, &dn, me);
        }
    });
    lap("dedup");
    if dn.overflow.load(Relaxed) {
        for a in &aouts {
            mine(std::slice::from_ref(a), 0).arena.release();
        }
        return give_up(&dn, &outs, nodes);
    }
    // (The special persons' events, apart: each thread sorts one list in the persons stage.)
    let sp_evs: Vec<Cell<Vec<[u64; 2]>>> = (0..threads).map(|t| Shared(UnsafeCell::new(std::mem::take(&mut mine(&aouts, t).sp_ev)))).collect();
    let aouts_ref: Vec<&AOut> = (0..threads).map(|t| &*mine(&aouts, t)).collect();

    // ------------------------------------------------------------ persons
    let user_dir = out_dir.join("user_daily");
    let camp_dir = out_dir.join("campaign_daily");
    std::fs::create_dir_all(&user_dir).unwrap();
    std::fs::create_dir_all(&camp_dir).unwrap();
    let mut by_q: Vec<Vec<Seg>> = (0..nq).map(|_| Vec::new()).collect();
    let mut kept = 0usize;
    for a in &aouts_ref {
        for s in &a.segs {
            by_q[s.q as usize].push(*s);
            kept += s.words as usize;
        }
    }
    // Biggest buckets first, so one huge person cannot be left for the end.
    let mut order: Vec<usize> = (0..nq).collect();
    order.sort_by_key(|&q| std::cmp::Reverse(by_q[q].iter().map(|s| s.words as usize).sum::<usize>() + outs_ref.iter().map(|o| o.ck.len(q)).sum::<usize>()));
    let ncell = (ctx.aux.camp_names.len() + 1) * ctx.nd;
    let rows_room = if env_usize("SS_ROOM", 1) != 0 { kept / 6 / threads + 4096 } else { 0 };
    let bouts: Vec<Cell<Option<BOut>>> = (0..threads).map(|_| Shared(UnsafeCell::new(None))).collect();
    // The persons that need a lookup: their events are all known now, their
    // clicks once every bucket is done. Each thread first sorts one of the
    // lists of their events (by person, then event), and once done with the
    // buckets the clicks it found; the first thread done with the buckets
    // puts the persons in order. Then all take persons from that order.
    let special: std::sync::OnceLock<Special> = std::sync::OnceLock::new();
    let sp_runs: Vec<Cell<Vec<(u32, u32)>>> = (0..threads).map(|_| Shared(UnsafeCell::new(Vec::new()))).collect();
    let sorted = AtomicUsize::new(0);
    let prep_claim = AtomicBool::new(false);
    let buckets_done = AtomicUsize::new(0);
    let buckets_at = AtomicU64::new(0);
    let next_sp = AtomicUsize::new(0);
    let sp_cks: Vec<Cell<Vec<SpCk>>> = (0..threads).map(|_| Shared(UnsafeCell::new(Vec::new()))).collect();
    let cells: Vec<Cell<Option<Cells>>> = (0..threads).map(|_| Shared(UnsafeCell::new(None))).collect();
    let next = AtomicUsize::new(0);
    par(threads, |tid| {
        let me = mine(&bouts, tid).insert(BOut {
            tl: Tl::new(&ctx, ncell, rows_room),
            cnt: Vec::new(),
            keys: Vec::new(),
            cids: Vec::new(),
            irr: Vec::new(),
            sp_ck: Vec::new(),
            cov: vec![[0; 2]; ctx.aux.camp_names.len() + 1],
            serial: 0,
            dayfirst: Vec::new(),
            big: Vec::new(),
            credit: vec![Credit::default(); ncell],
            // (Room past the last row: see `add_days`.)
            assist: vec![0; ncell + ctx.nd + 16],
            hi: Vec::new(),
            ca: Vec::new(),
            cb: Vec::new(),
            sstart: Vec::new(),
            sall: Vec::new(),
            sorder: Vec::new(),
            smap: Vec::new(),
            spurch: Vec::new(),
            srow: Vec::new(),
            dayrow: Vec::new(),
            prof: [0.0; 8],
            timed: log,
        });
        let cpu = || if log { sys::thread_cpu() } else { 0.0 };
        let t_a = cpu();
        {
            // (Written only here, read by the others only once `sorted` says so.)
            let evs = mine(&sp_evs, tid);
            evs.sort_unstable();
            let runs = mine(&sp_runs, tid);
            for r in evs.iter() {
                match runs.last_mut() {
                    Some(last) if last.0 == r[0] as u32 => last.1 += 1,
                    _ => runs.push((r[0] as u32, 1)),
                }
            }
        }
        sorted.fetch_add(1, Release);
        let t_q = cpu();
        loop {
            let i = next.fetch_add(1, Relaxed);
            let Some(&q) = order.get(i) else { break };
            person_bucket(q, &by_q[q], &outs_ref, &dn, me);
        }
        let t_b = cpu();
        {
            // (Written only here, read by the others only once every thread is past this.)
            let ob = dn.obits;
            let key = |w: u64| (w & !0xFFFF_FFFF) | (w & 0xFFFF_FFFF) << ob;
            let own = mine(&sp_cks, tid);
            own.extend(me.sp_ck.iter().map(|r| (r[0] as u32, key(r[1]), (r[0] >> 32) as u32)));
            own.sort_unstable_by_key(|c| c.0);
        }
        if buckets_done.fetch_add(1, Release) + 1 == threads {
            buckets_at.store(t0.elapsed().as_micros() as u64, Relaxed);
        }
        if !prep_claim.swap(true, Relaxed) {
            wait_until(|| sorted.load(Acquire) == threads);
            let runs: Vec<&[(u32, u32)]> = sp_runs.iter().map(|c| unsafe { &*c.0.get() }.as_slice()).collect();
            let _ = special.set(prep_special(&outs_ref, &runs, &dn));
        }
        let t_p = cpu();
        // This thread's rows so far are all of its buckets': write them while
        // the others finish theirs. Special persons' rows go to a file of
        // their own.
        if tid == 0 || me.tl.rows.n > 0 {
            write_user_daily(&user_dir.join(format!("part-{tid:03}.parquet")), &mut me.tl.rows, &ctx);
        }
        me.tl.rows = Rows::default();
        let t_w = cpu();
        wait_until(|| buckets_done.load(Acquire) == threads && special.get().is_some());
        let t_x = cpu();
        let sp = special.get().unwrap();
        // (No thread writes its list after the count above.)
        let cks: Vec<&[SpCk]> = sp_cks.iter().map(|c| unsafe { &*c.0.get() }.as_slice()).chain(std::iter::once(sp.ext.as_slice())).collect();
        let evs: Vec<&[[u64; 2]]> = sp_evs.iter().map(|c| unsafe { &*c.0.get() }.as_slice()).collect();
        let (mut own_k, mut own_c): (Vec<u64>, Vec<u32>) = (Vec::new(), Vec::new());
        let mut gather = Vec::new();
        loop {
            let i = next_sp.fetch_add(1, Relaxed);
            let Some(&person) = sp.persons.get(i) else { break };
            gather.clear();
            for list in &evs {
                gather.extend(of_person(list, person, |r| r[0] as u32).iter().map(|r| r[1]));
            }
            // (In event order, as one sorted list would give them.)
            gather.sort_unstable();
            own_k.clear();
            own_c.clear();
            for list in &cks {
                for c in of_person(list, person, |c| c.0) {
                    own_k.push(c.1);
                    own_c.push(c.2);
                }
            }
            person_run(person, &mut gather, &mut own_k, &own_c, &dn, me);
        }
        let t_s = cpu();
        if me.tl.rows.n > 0 {
            write_user_daily(&user_dir.join(format!("part-{tid:03}-s.parquet")), &mut me.tl.rows, &ctx);
        }
        if log {
            let t_e = sys::thread_cpu();
            eprintln!(
                "    t{tid}: CPU ms: clicks {:.1} (clear {:.1} prefix {:.1}) one-run {:.1} others {:.1} special {:.1} write {:.1} | events one-run {:.0} others {:.0} | wait {:.1} prep {:.1} special persons {:.1} write {:.1} + {:.1} | done at {:.1} ms",
                me.prof[0] * 1e3,
                me.prof[5] * 1e3,
                me.prof[6] * 1e3,
                me.prof[1] * 1e3,
                me.prof[2] * 1e3,
                (t_q - t_a + t_p - t_b + t_s - t_x) * 1e3,
                (t_w - t_p + t_e - t_s) * 1e3,
                me.prof[3],
                me.prof[4],
                (t_x - t_w) * 1e3,
                (t_q - t_a + t_p - t_b) * 1e3,
                (t_s - t_x) * 1e3,
                (t_w - t_p) * 1e3,
                (t_e - t_s) * 1e3,
                t0.elapsed().as_secs_f64() * 1e3,
            );
        }
        let done = mine(&bouts, tid).take().unwrap();
        let mut tl = done.tl;
        for (i, c) in done.credit.iter().enumerate() {
            tl.cells.sessions[i] += c.sessions;
            tl.cells.purch[i] += c.purch;
            tl.cells.rev[i] += c.rev;
            tl.cells.assisted[i] += done.assist[i];
        }
        *mine(&cells, tid) = Some(tl.cells);
    });
    lap("persons + user_daily");
    if log {
        let sp = special.get().unwrap();
        eprintln!("    persons: buckets done at {:.1} ms; {} special persons, {} of their events, {} clicks of interned users, {} other clicks", buckets_at.load(Relaxed) as f64 / 1e3, sp.persons.len(), sp_evs.iter().map(|c| unsafe { &*c.0.get() }.len()).sum::<usize>(), sp.ext.len(), sp_cks.iter().map(|c| unsafe { &*c.0.get() }.len()).sum::<usize>());
    }
    if log {
        let v: Vec<usize> = STATS.iter().map(|x| x.swap(0, Relaxed)).collect();
        eprintln!("    persons with clicks by clicks x sessions (<=64, <=256, <=1024, more): {:?}; pairs {:?}; clicks {:?}", &v[..4], &v[4..8], &v[8..]);
        eprintln!("    one-run persons not in time order (or with a long name): {}", UNORDERED.swap(0, Relaxed));
        eprintln!("    sessions newly assisted per click (0..14, 15+): {:?}", NHIST.iter().map(|x| x.swap(0, Relaxed)).collect::<Vec<_>>());
    }

    // The last table is small: write it while the other threads give the big
    // buffers back, which is quicker than leaving it all to process exit.
    let cells: Vec<Cells> = cells.into_iter().map(|c| c.0.into_inner().unwrap()).collect();
    par(threads, |tid| {
        if tid == 0 {
            write_campaign_daily(&camp_dir.join("part-000.parquet"), &cells, &ctx);
        }
        outs_ref[tid].arena.release();
        aouts_ref[tid].arena.release();
        if unmap_mode == "final" {
            for (i, f) in in_files.iter().enumerate() {
                if i % threads == tid {
                    f.unmap();
                }
            }
        }
    });
    if let Some(u) = unmapper {
        let _ = u.join();
    }
    std::mem::forget(in_files);
    release(dn.r8, d + XCAP);
    release(dn.runs, d);
    CURRENT.store(std::ptr::null_mut(), Relaxed);
    lap("campaign_daily");
    true
}

/// Spin (then yield) until `done()`.
fn wait_until(done: impl Fn() -> bool) {
    let mut n = 0u32;
    while !done() {
        if n < 2000 {
            std::hint::spin_loop();
            n += 1;
        } else {
            std::thread::yield_now();
        }
    }
}

/// Abandon the dense run: give its memory back and let the general pipeline run.
fn give_up(dn: &Dn, outs: &[Cell<DOut>], nodes: usize) -> bool {
    for o in outs {
        mine(std::slice::from_ref(o), 0).arena.release();
    }
    release(dn.r8, dn.d + XCAP);
    release(dn.runs, dn.d);
    release(dn.parent as *mut u8, nodes * 4);
    if std::env::var_os("SS_TIMING").is_some() {
        eprintln!("    dense: side tables overflowed, starting over the general way");
    }
    false
}
