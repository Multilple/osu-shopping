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
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU32, AtomicU8, AtomicUsize, Ordering::Relaxed};
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
use crate::util::*;
use crate::{
    build_ctx, env_usize, fast, grid_tasks, input, lines, load_aux, par, person_grid, person_sorted,
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
    /// The click kernel's tables.
    pub(crate) ckx: crate::dense_ck::Cx,
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
    pub(crate) ev: Lists,
    /// Valid clicks by their user's person bucket (interned users: bucket
    /// `nq`): `[ts << 32 | campaign, node | click << 32]`.
    pub(crate) ck: Lists,
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
    gather: Vec<u64>,
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
    /// Scratch for one person: each session's start and event count, and
    /// the purchases (session << 40 | category << 24 | price).
    sstart: Vec<u32>,
    scount: Vec<u32>,
    spurch: Vec<u64>,
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

fn parse_clicks(file: &[u8], start: usize, end: usize, ctx: &Ctx, dn: &Dn, out: &mut DOut) {
    // The kernel takes the lines it can prove sound; the plain code the rest.
    let rest = if slow_only() { start } else { unsafe { crate::dense_ck::clicks_dense(file, start, end, ctx, dn, out) } };
    for line in lines(&file[rest..end]) {
        click_line(line, ctx, dn, out);
    }
}

// ---------------------------------------------------------------- dedup

/// What the walk over a bucket's table keeps between runs.
struct Walk<'a> {
    dn: &'a Dn,
    me: &'a mut AOut,
    buf: *mut u64,
    /// The open run's header.
    hdr: *mut u64,
    cur_q: u32,
    /// Where the open segment starts.
    seg: *mut u64,
}

impl Walk<'_> {
    /// The run in progress ends before `out`.
    #[inline(always)]
    unsafe fn close(&mut self, out: *mut u64) {
        if !self.hdr.is_null() {
            *self.hdr |= (out.offset_from(self.hdr) as u64 - 1) << 32;
        }
    }

    /// An event whose person is not the open run's: start a run (the event
    /// follows its header at the place returned), or, for a person that
    /// needs a lookup, put the event on the side list (`None`).
    #[cold]
    #[inline(never)]
    unsafe fn turn(&mut self, p: u32, node: u32, ev: u64, out: *mut u64) -> Option<*mut u64> {
        if (p as i32) < 0 {
            self.me.sp_ev.push([self.dn.special_person(node) as u64, ev]);
            return None;
        }
        self.close(out);
        let q = p >> (self.dn.ku + 3);
        if q != self.cur_q {
            if self.cur_q != u32::MAX {
                self.me.segs.push(Seg { q: self.cur_q, ptr: self.seg, words: out.offset_from(self.seg) as u32 });
            }
            self.cur_q = q;
            self.seg = out;
        }
        self.dn.mark_run(p);
        self.hdr = out;
        *out = p as u64;
        Some(out.add(1))
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
    if me.w.len() != ns {
        // (Every later bucket finds the table as the walk below leaves it.)
        me.w.clear();
        me.w.resize(ns, [u64::MAX; 2]);
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
                    // (Copies with equal ingest times are identical.)
                    if w0 < *cur {
                        *cur = w0;
                        *cur.add(1) = w1;
                    }
                }
            }
        }
    }

    // Every valid kept event, in event order.
    let room = 2 * nrec.min(ns) + 2;
    let buf = me.arena.alloc(room * 8) as *mut u64;
    let r8 = dn.r8;
    let purch = me.purch.as_ptr();
    let mut walk = Walk { dn, me, buf, hdr: std::ptr::null_mut(), cur_q: u32::MAX, seg: buf };
    unsafe {
        let mut out = buf;
        // (No person has this number.)
        let mut cur_p = 0x7FFF_FFFFu32;
        let mut e = w;
        let wend = e.add(2 * ns);
        while e != wend {
            let (w0, w1) = (*e, *e.add(1));
            // (Leave the table empty for the next bucket.)
            *e = u64::MAX;
            e = e.add(2);
            // The time moves to the top; the ingest time above it falls off.
            let mut ev = w0 << 31;
            if w0 & 3 != 0 {
                if w0 & 1 != 0 {
                    continue;
                }
                ev = (ev ^ (1 << 32)) | *purch.add((w1 & LOW21) as usize);
            }
            let node = (w1 >> 21) as u32;
            // A node's entry gives its person's node; an entry of -1 ("look
            // it up") gives a number no person has.
            let p = node ^ (*r8.add(node as usize) as i8 as i32 as u32);
            if p != cur_p {
                match walk.turn(p, node, ev, out) {
                    Some(o) => {
                        out = o;
                        cur_p = p;
                    }
                    None => continue,
                }
            }
            *out = ev;
            out = out.add(1);
        }
        walk.close(out);
        if walk.cur_q != u32::MAX {
            let seg = Seg { q: walk.cur_q, ptr: walk.seg, words: out.offset_from(walk.seg) as u32 };
            walk.me.segs.push(seg);
        }
        walk.me.arena.shrink_to(out as *mut u8);
    }
}

// ---------------------------------------------------------------- persons

/// Room in every column for `n` more rows (so that `push_row` need not check).
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

/// One user_daily row, for a short name and columns with room (see `reserve_rows`).
#[inline(always)]
unsafe fn push_row(day: u32, sessions: u32, events: u32, rev: u64, best: u16, tl: &mut Tl) {
    let rows = &mut tl.rows;
    let n = rows.n;
    rows.n = n + 1;
    (rows.day.as_mut_ptr().add(n * 4) as *mut i32).write_unaligned(tl.day0 + day as i32);
    rows.day.set_len(n * 4 + 4);
    (rows.sessions.as_mut_ptr().add(n * 4) as *mut u32).write_unaligned(sessions);
    rows.sessions.set_len(n * 4 + 4);
    (rows.events.as_mut_ptr().add(n * 4) as *mut u32).write_unaligned(events);
    rows.events.set_len(n * 4 + 4);
    (rows.rev.as_mut_ptr().add(n * 8) as *mut u64).write_unaligned(rev);
    rows.rev.set_len(n * 8 + 8);
    rows.cat.as_mut_ptr().add(n).write(best);
    rows.cat.set_len(n + 1);
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
    me.sstart.reserve(n + 1);
    me.scount.clear();
    me.scount.reserve(n + 1);
    me.spurch.clear();
    let (start, count) = (me.sstart.as_mut_ptr(), me.scount.as_mut_ptr());
    let p = evs.as_ptr();
    let tl = &mut me.tl;
    unsafe {
        let mut sid = 0usize;
        let mut prev = (*p >> 33) as u32;
        let mut first = prev;
        let mut cnt = 0u32;
        let mut back = false;
        for i in 0..n {
            let e = *p.add(i);
            let ts = (e >> 33) as u32;
            back |= ts < prev;
            // (A step back in time looks like a gap here; `back` catches it.)
            let brk = ts.wrapping_sub(prev) > GAP;
            sid += brk as usize;
            first = if brk { ts } else { first };
            cnt = if brk { 1 } else { cnt + 1 };
            *start.add(sid) = first;
            *count.add(sid) = cnt;
            prev = ts;
            if e & 0xFF_FFFF != 0 {
                me.spurch.push((sid as u64) << 40 | (e & 0x1_FFFF_FFFF));
            }
        }
        if back {
            return false;
        }
        let ns = sid + 1;
        tl.sess.clear();
        tl.sess.reserve(ns);
        let sess = tl.sess.as_mut_ptr();
        for k in 0..ns {
            let st = *start.add(k);
            sess.add(k).write(Sess { start: st, day: st / DAY_MS - 7, purch: false, rev: 0, events: *count.add(k) });
        }
        tl.sess.set_len(ns);

        // Purchases: revenue per session, and the best category per day (as in `person_grid`).
        if !me.spurch.is_empty() {
            let ncat = tl.ncat;
            for &pp in &me.spurch {
                let s = &mut *sess.add((pp >> 40) as usize);
                let cat = ((pp >> 24) & 0x1FF) as usize;
                let price = pp & 0xFF_FFFF;
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
                let sum = tl.daycat[k as usize];
                tl.daycat[k as usize] = 0;
                // Categories are ranked byte-wise, so the smaller index wins a tie.
                if sum > tl.best_sum[d] || (sum == tl.best_sum[d] && c < tl.best[d]) {
                    tl.best[d] = c;
                    tl.best_sum[d] = sum;
                }
            }
            tl.touched_dc.clear();
        }

        // One row per day with a session.
        reserve_rows(&mut tl.rows, tl.nd);
        let mut i = 0;
        let n0 = tl.rows.n;
        while i < ns {
            let day = (*sess.add(i)).day;
            let (mut d_sessions, mut d_events, mut d_rev) = (0u32, 0u32, 0u64);
            while i < ns && (*sess.add(i)).day == day {
                d_sessions += 1;
                d_events += (*sess.add(i)).events;
                d_rev += (*sess.add(i)).rev;
                i += 1;
            }
            let best = tl.best[day as usize];
            tl.best[day as usize] = NOCAT;
            tl.best_sum[day as usize] = 0;
            push_row(day, d_sessions, d_events, d_rev, best, tl);
        }
        let k = (tl.rows.n - n0) as u32;
        if k > 0 {
            user_run(&mut tl.rows, who, k);
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
    // Two spare sessions after the last, on day 0, so that the assist loop
    // may look two sessions past a range it then adds nothing for.
    me.tl.sess.reserve(2);
    let sess = me.tl.sess.as_mut_ptr();
    let (credit, assist) = (me.credit.as_mut_ptr(), me.assist.as_mut_ptr());
    let n0 = keys.len();
    unsafe {
        let spare = Sess { start: 0, day: 0, purch: false, rev: 0, events: 0 };
        sess.add(ns).write(spare);
        sess.add(ns + 1).write(spare);
        let sess = sess as *const Sess;
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
        let mut have_days = false;
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
            let row = assist.add(c * nd);
            *row.add((*sess.add(from)).day as usize) += (n > 0) as u32;
            *row.add((*sess.add(from + 1)).day as usize) += (n > 1) as u32;
            if n <= 2 {
                continue;
            }
            let from = from + 2;
            if b - from <= 12 {
                for k in from..b {
                    *assist.add(c * nd + (*sess.add(k)).day as usize) += 1;
                }
            } else {
                // Many sessions at once (a very busy person): count them day by day.
                if !have_days {
                    have_days = true;
                    // dayfirst[d]: the index of the first session on day d or later.
                    me.dayfirst.clear();
                    me.dayfirst.resize(nd + 1, ns as u32);
                    for k in (0..ns).rev() {
                        me.dayfirst[(*sess.add(k)).day as usize] = k as u32;
                    }
                    for d in (0..nd).rev() {
                        me.dayfirst[d] = me.dayfirst[d].min(me.dayfirst[d + 1]);
                    }
                }
                for d in (*sess.add(from)).day as usize..=(*sess.add(b - 1)).day as usize {
                    let cnt = b.min(me.dayfirst[d + 1] as usize).saturating_sub(from.max(me.dayfirst[d] as usize));
                    *assist.add(c * nd + d) += cnt as u32;
                }
            }
        }
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

static NHIST: [AtomicUsize; 16] = [const { AtomicUsize::new(0) }; 16];

/// (Statistics: persons whose single run of events was not in time order.)
static UNORDERED: AtomicUsize = AtomicUsize::new(0);

/// One person whose events are in no particular order.
fn person_any(p: u32, evs: &mut [u64], keys: &mut [u64], cids: &[u32], dn: &Dn, me: &mut BOut) {
    let who = dn.person_key(p);
    if evs.len() >= me.tl.grid_min {
        person_grid(who, evs, &mut me.tl);
    } else {
        person_sorted(who, evs, &mut me.tl);
    }
    attribute(keys, cids, dn, me);
}

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
            let (person, n) = unsafe { (*p as u32, (*p >> 32) as usize) };
            let evs = unsafe { std::slice::from_raw_parts_mut(p.add(1), n) };
            p = unsafe { p.add(1 + n) };
            if dn.one_run(person) {
                let (k, c) = clicks(person);
                me.prof[3] += n as f64;
                person_run(person, evs, k, c, dn, me);
            } else {
                me.prof[4] += n as f64;
                me.irr.push((person, evs.as_mut_ptr(), n as u32));
            }
        }
    }
    let t_c = if timed { sys::thread_cpu() } else { 0.0 };
    me.prof[1] += t_c - t_b;
    if !me.irr.is_empty() {
        // Persons whose events came in several runs: put each one's together.
        me.irr.sort_unstable_by_key(|r| r.0);
        let mut gather = std::mem::take(&mut me.gather);
        let mut i = 0;
        while i < me.irr.len() {
            let person = me.irr[i].0;
            gather.clear();
            while i < me.irr.len() && me.irr[i].0 == person {
                gather.extend_from_slice(unsafe { std::slice::from_raw_parts(me.irr[i].1, me.irr[i].2 as usize) });
                i += 1;
            }
            let (k, c) = clicks(person);
            person_any(person, &mut gather, k, c, dn, me);
        }
        me.gather = gather;
    }
    if timed {
        me.prof[2] += sys::thread_cpu() - t_c;
    }
}

/// The persons that need a lookup, shared out by person number: thread
/// `tid` of `threads` takes its share of their events and clicks from every
/// thread's lists.
fn special_persons(tid: usize, threads: usize, outs: &[&DOut], aouts: &[&AOut], sp_ck: &[&Vec<[u64; 2]>], dn: &Dn, me: &mut BOut) {
    let take = |p: u64| (p as usize - dn.d) % threads == tid;
    let mut evs: Vec<[u64; 2]> = aouts.iter().flat_map(|a| a.sp_ev.iter()).filter(|r| take(r[0])).copied().collect();
    if evs.is_empty() {
        return;
    }
    evs.sort_unstable();
    // Clicks as (person, key, number): forwarded ones, and those of interned users.
    let ob = dn.obits;
    let key = |w: u64| (w & !0xFFFF_FFFF) | (w & 0xFFFF_FFFF) << ob;
    let mut cks: Vec<(u32, u64, u32)> = Vec::new();
    for list in sp_ck {
        for r in list.iter() {
            if take(r[0] & 0xFFFF_FFFF) {
                cks.push((r[0] as u32, key(r[1]), (r[0] >> 32) as u32));
            }
        }
    }
    for o in outs {
        for (ptr, len) in o.ck.pieces(dn.nq) {
            for r in unsafe { std::slice::from_raw_parts(ptr, len) } {
                let p = dn.special_person(r[1] as u32);
                if take(p as u64) {
                    cks.push((p, key(r[0]), (r[1] >> 32) as u32));
                }
            }
        }
    }
    cks.sort_unstable_by_key(|c| c.0);
    let (mut own_k, mut own_c): (Vec<u64>, Vec<u32>) = (Vec::new(), Vec::new());
    let mut gather = Vec::new();
    let (mut i, mut c) = (0, 0);
    while i < evs.len() {
        let person = evs[i][0] as u32;
        gather.clear();
        while i < evs.len() && evs[i][0] as u32 == person {
            gather.push(evs[i][1]);
            i += 1;
        }
        while c < cks.len() && cks[c].0 < person {
            c += 1;
        }
        own_k.clear();
        own_c.clear();
        while c < cks.len() && cks[c].0 == person {
            own_k.push(cks[c].1);
            own_c.push(cks[c].2);
            c += 1;
        }
        // (Sorted above: by person, then by time.)
        person_run(person, &mut gather, &mut own_k, &own_c, dn, me);
    }
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
    let mut event_files = input::list_dir(&data_dir.join("events"));
    let mut click_files = input::list_dir(&data_dir.join("clicks"));
    let link_files = input::list_dir(&data_dir.join("links"));
    let Some(plan) = plan(&event_files, &click_files) else { return false };
    if log {
        eprintln!("    dense: keys {:x} {:x} {:x}, bounds {} events {} users {} clicks", plan.keys.event, plan.keys.user, plan.keys.click, plan.max_event, plan.max_user, plan.max_click);
    }
    let mut ctx = build_ctx(data_dir, 0, 0, 0, 1);

    let kb = env_usize("SS_KB", 15) as u32;
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
        ckx: Default::default(),
    };
    // Interned users are always looked up.
    unsafe { std::ptr::write_bytes(dn.r8.add(d), 0xFF, XCAP) };
    let chunk = env_usize("SS_CHUNK", 64 << 10);
    let outs: Vec<Cell<DOut>> = (0..threads)
        .map(|_| {
            Shared(UnsafeCell::new(DOut {
                arena: Arena::new(),
                ev: Lists::new(nb),
                ck: Lists::new(nq + 1),
                reader: input::Reader::new(chunk),
                pend_l: Vec::with_capacity(PEND),
            }))
        })
        .collect();
    lap("setup");

    // ------------------------------------------------------------ links
    let mut tasks = Vec::new();
    grid_tasks(&link_files, false, chunk, &mut tasks);
    let aux = std::thread::scope(|scope| {
        let aux = scope.spawn(|| load_aux(data_dir));
        let dn = &dn;
        // The parents are read before they are written: touch them first, so
        // each 2 MB comes in as one huge page (see `IdTable::touch_part`).
        par(threads, |tid| {
            let bytes = d * 4;
            let (a, b) = (bytes * tid / threads / 4096 * 4096, bytes * (tid + 1) / threads / 4096 * 4096);
            let mut o = a;
            while o < b {
                unsafe { std::ptr::write_volatile((dn.parent as *mut u8).add(o), 0) };
                o += 4096;
            }
        });
        lap("    links: parents touched");
        let next = AtomicUsize::new(0);
        par(threads, |tid| {
            let out = mine(&outs, tid);
            loop {
                let i = next.fetch_add(1, Relaxed);
                let Some(task) = tasks.get(i) else { break };
                let (start, end) = out.reader.load(&link_files[task.file], task.a, task.b);
                let buf = std::mem::take(&mut out.reader.buf);
                parse_links(&buf[..end + 128], start, end, dn, out);
                out.reader.buf = buf;
            }
            flush_links(dn, out);
        });
        lap("    links: parsed");
        aux.join().unwrap()
    });
    lap("  links: parse and join");
    settle_links(&mut dn, threads);
    release(dn.parent as *mut u8, nodes * 4);
    ctx.aux = aux;
    // Campaign numbers take the bits they need; the rest of a click key's
    // low half numbers the click among its person's.
    dn.obits = (ctx.aux.camp_names.len() as u32).leading_zeros();
    dn.ckx = crate::dense_ck::Cx::new(&ctx, &dn);
    lap("links");
    if dn.overflow.load(Relaxed) {
        return give_up(&dn, &outs, nodes);
    }

    // ------------------------------------------------------------ parse
    let mapped = env_usize("SS_MMAP", 1) != 0;
    if mapped {
        for src in event_files.iter_mut().chain(click_files.iter_mut()) {
            src.map();
        }
    }
    let mut tasks = Vec::new();
    let piece = env_usize("SS_PIECE", if mapped { 4 << 20 } else { chunk });
    grid_tasks(&event_files, true, piece, &mut tasks);
    grid_tasks(&click_files, false, piece, &mut tasks);
    let next = AtomicUsize::new(0);
    // (Asking for a piece's pages in one call costs more than their faults on the scoring machine.)
    let populate = env_usize("SS_POP", 0) != 0;
    {
        let (dn, ctx) = (&dn, &ctx);
        par(threads, |tid| {
            let out = mine(&outs, tid);
            loop {
                let i = next.fetch_add(1, Relaxed);
                let Some(task) = tasks.get(i) else { break };
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
            out.ev.finish(&mut out.arena);
            out.ck.finish(&mut out.arena);
        });
    }
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
    let unmapper = {
        let files: Vec<Src> = event_files.into_iter().chain(click_files).collect();
        std::thread::spawn(move || {
            for f in &files {
                f.unmap();
            }
            std::mem::forget(files);
        })
    };

    // ------------------------------------------------------------ dedup
    let aouts: Vec<Cell<AOut>> =
        (0..threads).map(|_| Shared(UnsafeCell::new(AOut { arena: Arena::new(), segs: Vec::new(), sp_ev: Vec::new(), w: Vec::new(), purch: Vec::new() }))).collect();
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
    let next = AtomicUsize::new(0);
    par(threads, |tid| {
        let me = mine(&bouts, tid).insert(BOut {
            tl: Tl::new(&ctx, ncell, rows_room),
            cnt: Vec::new(),
            keys: Vec::new(),
            cids: Vec::new(),
            irr: Vec::new(),
            gather: Vec::new(),
            sp_ck: Vec::new(),
            cov: vec![[0; 2]; ctx.aux.camp_names.len() + 1],
            serial: 0,
            dayfirst: Vec::new(),
            big: Vec::new(),
            credit: vec![Credit::default(); ncell],
            assist: vec![0; ncell],
            hi: Vec::new(),
            ca: Vec::new(),
            cb: Vec::new(),
            sstart: Vec::new(),
            scount: Vec::new(),
            spurch: Vec::new(),
            prof: [0.0; 8],
            timed: log,
        });
        loop {
            let i = next.fetch_add(1, Relaxed);
            let Some(&q) = order.get(i) else { break };
            person_bucket(q, &by_q[q], &outs_ref, &dn, me);
        }
    });
    lap("  persons");
    let sp_ck: Vec<Vec<[u64; 2]>> = (0..threads).map(|t| std::mem::take(&mut mine(&bouts, t).as_mut().unwrap().sp_ck)).collect();
    let sp_ck: Vec<&Vec<[u64; 2]>> = sp_ck.iter().collect();
    let cells: Vec<Cell<Option<Cells>>> = (0..threads).map(|_| Shared(UnsafeCell::new(None))).collect();
    par(threads, |tid| {
        // (Each thread reads every thread's forwarded clicks and writes only its own rows.)
        let me = mine(&bouts, tid).as_mut().unwrap();
        let t_s = sys::thread_cpu();
        special_persons(tid, threads, &outs_ref, &aouts_ref, &sp_ck, &dn, me);
        let t_w = sys::thread_cpu();
        if tid == 0 || me.tl.rows.n > 0 {
            write_user_daily(&user_dir.join(format!("part-{tid:03}.parquet")), &mut me.tl.rows, &ctx);
        }
        if log {
            eprintln!(
                "    t{tid}: CPU ms: clicks {:.1} (clear {:.1} prefix {:.1}) one-run {:.1} others {:.1} special {:.1} write {:.1} | events one-run {:.0} others {:.0}",
                me.prof[0] * 1e3,
                me.prof[5] * 1e3,
                me.prof[6] * 1e3,
                me.prof[1] * 1e3,
                me.prof[2] * 1e3,
                (t_w - t_s) * 1e3,
                (sys::thread_cpu() - t_w) * 1e3,
                me.prof[3],
                me.prof[4]
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
    });
    let _ = unmapper.join();
    release(dn.r8, d + XCAP);
    release(dn.runs, d);
    CURRENT.store(std::ptr::null_mut(), Relaxed);
    lap("campaign_daily");
    true
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
