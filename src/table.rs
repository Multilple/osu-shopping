//! Shared structures: the concurrent ID table (which is also the union-find
//! over linked IDs), and arena-backed append-only buckets.

use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering::*};

use crate::util::*;

pub fn map_anon(bytes: usize) -> *mut u8 {
    unsafe {
        let p = sys::mmap(
            std::ptr::null_mut(),
            bytes,
            sys::PROT_READ | sys::PROT_WRITE,
            sys::MAP_PRIVATE | sys::MAP_ANONYMOUS | sys::MAP_NORESERVE,
            -1,
            0,
        );
        assert!(p != sys::MAP_FAILED, "mmap of {bytes} bytes failed");
        sys::madvise(p, bytes, sys::MADV_HUGEPAGE);
        p as *mut u8
    }
}

// ---------------------------------------------------------------- ID table

pub const RBITS: usize = 12;
pub const RSIZE: usize = 1 << RBITS;
const RMASK: usize = RSIZE - 1;
/// A region holding more IDs than this is treated as overflowing.
const RLIMIT: u16 = (RSIZE * 13 / 16) as u16;

#[repr(C, align(32))]
pub struct Slot {
    lo: AtomicU64,
    hi: AtomicU64,
    /// Union-find parent: a slot index. A root points at itself.
    root: AtomicU32,
}

/// Every user ID seen, each in a fixed slot. The table is split into regions
/// of 4096 slots; an ID hashes to a region and probes inside it. Lookups are
/// lock-free; inserts take the region's lock.
pub struct IdTable {
    slots: *mut Slot,
    pub nregions: usize,
    locks: Vec<AtomicBool>,
    counts: Vec<UnsafeCell<u16>>,
    pub overflow: AtomicBool,
}

unsafe impl Sync for IdTable {}
unsafe impl Send for IdTable {}

impl IdTable {
    pub fn new(nregions: usize) -> IdTable {
        let nregions = nregions.max(1);
        IdTable {
            slots: map_anon(nregions * RSIZE * std::mem::size_of::<Slot>()) as *mut Slot,
            nregions,
            locks: (0..nregions).map(|_| AtomicBool::new(false)).collect(),
            counts: (0..nregions).map(|_| UnsafeCell::new(0)).collect(),
            overflow: AtomicBool::new(false),
        }
    }

    pub fn nslots(&self) -> usize {
        self.nregions * RSIZE
    }

    #[inline(always)]
    fn slot(&self, i: usize) -> &Slot {
        unsafe { &*self.slots.add(i) }
    }

    #[inline(always)]
    fn home(&self, h: u64) -> (usize, usize) {
        let region = (((h >> 32) * self.nregions as u64) >> 32) as usize;
        (region << RBITS, (h as usize) & RMASK)
    }

    #[inline(always)]
    pub fn prefetch(&self, h: u64) {
        let (base, pos) = self.home(h);
        unsafe {
            core::arch::x86_64::_mm_prefetch(self.slots.add(base | pos) as *const i8, core::arch::x86_64::_MM_HINT_T0);
        }
    }

    /// The slot of `k` (whose hash is `h`), inserting it as its own root if new.
    #[cfg_attr(feature = "prof", inline(never))]
    #[cfg_attr(not(feature = "prof"), inline(always))]
    pub fn get_or_insert(&self, k: Key, h: u64) -> u32 {
        let (base, start) = self.home(h);
        let (klo, khi) = (k as u64, (k >> 64) as u64);
        let mut pos = start;
        loop {
            let s = self.slot(base | pos);
            let hi = s.hi.load(Acquire);
            if hi == khi && s.lo.load(Relaxed) == klo {
                return (base | pos) as u32;
            }
            if hi == 0 {
                break;
            }
            pos = (pos + 1) & RMASK;
        }
        self.insert(klo, khi, base, start)
    }

    #[cold]
    #[inline(never)]
    fn insert(&self, klo: u64, khi: u64, base: usize, start: usize) -> u32 {
        let region = base >> RBITS;
        let lock = &self.locks[region];
        while lock.compare_exchange_weak(false, true, Acquire, Relaxed).is_err() {
            std::hint::spin_loop();
        }
        let mut pos = start;
        let found = loop {
            let s = self.slot(base | pos);
            let hi = s.hi.load(Relaxed);
            if hi == khi && s.lo.load(Relaxed) == klo {
                break (base | pos) as u32;
            }
            if hi == 0 {
                let count = unsafe { &mut *self.counts[region].get() };
                if *count >= RLIMIT {
                    self.overflow.store(true, Relaxed);
                    break base as u32;
                }
                *count += 1;
                // The key's high word (never 0: it holds the length) goes
                // last: a reader that sees it sees the rest.
                s.root.store((base | pos) as u32, Relaxed);
                s.lo.store(klo, Relaxed);
                s.hi.store(khi, Release);
                break (base | pos) as u32;
            }
            pos = (pos + 1) & RMASK;
        };
        lock.store(false, Release);
        found
    }

    #[inline(always)]
    pub fn key(&self, i: u32) -> Key {
        let s = self.slot(i as usize);
        (s.lo.load(Relaxed) as u128) | ((s.hi.load(Relaxed) as u128) << 64)
    }

    #[inline(always)]
    pub fn root(&self, i: u32) -> u32 {
        self.slot(i as usize).root.load(Relaxed)
    }

    #[inline(always)]
    fn find(&self, mut x: u32) -> u32 {
        loop {
            let p = self.slot(x as usize).root.load(Relaxed);
            if p == x {
                return x;
            }
            let gp = self.slot(p as usize).root.load(Relaxed);
            if gp != p {
                let _ = self.slot(x as usize).root.compare_exchange(p, gp, Relaxed, Relaxed);
            }
            x = p;
        }
    }

    /// Join the sets of two slots. A set's root is its byte-wise smallest ID.
    #[inline(always)]
    pub fn union(&self, a: u32, b: u32) {
        if a == b {
            return;
        }
        loop {
            let ra = self.find(a);
            let rb = self.find(b);
            if ra == rb {
                return;
            }
            let (child, parent) = if cmp_key(self.key(ra), self.key(rb)).is_lt() { (rb, ra) } else { (ra, rb) };
            if self.slot(child as usize).root.compare_exchange(child, parent, Relaxed, Relaxed).is_ok() {
                return;
            }
        }
    }

    /// Point every ID in a region straight at its set's root.
    pub fn flatten_region(&self, region: usize) {
        let base = region << RBITS;
        for i in base..base + RSIZE {
            if self.slot(i).hi.load(Relaxed) != 0 {
                let r = self.find(i as u32);
                self.slot(i).root.store(r, Relaxed);
            }
        }
    }
}

// ---------------------------------------------------------------- arena

/// Bump allocator over large anonymous mappings (never freed).
pub struct Arena {
    cur: usize,
    end: usize,
}

impl Arena {
    pub fn new() -> Arena {
        Arena { cur: 0, end: 0 }
    }

    #[inline(always)]
    pub fn alloc(&mut self, bytes: usize) -> *mut u8 {
        if self.cur + bytes > self.end {
            let block = bytes.max(32 << 20);
            self.cur = map_anon(block) as usize;
            self.end = self.cur + block;
        }
        let p = self.cur;
        self.cur += bytes;
        p as *mut u8
    }
}

#[repr(C, align(64))]
#[derive(Clone, Copy)]
struct Line([u8; 64]);

/// Many append-only lists of `T` (a power-of-two size dividing 64), each
/// stored in chunks of `1 << CB` items. Items are staged one cache line at a
/// time and moved out with non-temporal stores, so appending to hundreds of
/// lists does not drag their tails through the cache.
pub struct Streams<T, const CB: usize> {
    lines: Vec<Line>,
    n: Vec<u32>,
    chunks: Vec<Vec<*mut T>>,
}

unsafe impl<T, const CB: usize> Sync for Streams<T, CB> {}
unsafe impl<T, const CB: usize> Send for Streams<T, CB> {}

impl<T: Copy, const CB: usize> Streams<T, CB> {
    const PER_LINE: usize = 64 / std::mem::size_of::<T>();

    pub fn new(streams: usize) -> Self {
        assert!(64 % std::mem::size_of::<T>() == 0 && (1usize << CB) % Self::PER_LINE == 0);
        Streams { lines: vec![Line([0; 64]); streams], n: vec![0; streams], chunks: (0..streams).map(|_| Vec::new()).collect() }
    }

    /// Where item `idx` of stream `s` lives, allocating its chunk if needed.
    #[inline(always)]
    fn dest(&mut self, s: usize, idx: usize, arena: &mut Arena) -> *mut T {
        let off = idx & ((1 << CB) - 1);
        if off == 0 && self.chunks[s].len() == idx >> CB {
            self.chunks[s].push(arena.alloc(std::mem::size_of::<T>() << CB) as *mut T);
        }
        unsafe { self.chunks.get_unchecked(s).get_unchecked(idx >> CB).add(off) }
    }

    /// Append to stream `s` and return the item's index.
    #[cfg_attr(feature = "prof", inline(never))]
    #[cfg_attr(not(feature = "prof"), inline(always))]
    pub fn push(&mut self, s: usize, v: T, arena: &mut Arena) -> usize {
        unsafe {
            let n = *self.n.get_unchecked(s) as usize;
            let r = n & (Self::PER_LINE - 1);
            let line = self.lines.as_mut_ptr().add(s);
            (line as *mut T).add(r).write(v);
            *self.n.get_unchecked_mut(s) = (n + 1) as u32;
            if r == Self::PER_LINE - 1 {
                use core::arch::x86_64::*;
                let d = self.dest(s, n + 1 - Self::PER_LINE, arena) as *mut __m256i;
                let src = line as *const __m256i;
                _mm256_stream_si256(d, _mm256_load_si256(src));
                _mm256_stream_si256(d.add(1), _mm256_load_si256(src.add(1)));
            }
            n
        }
    }

    /// Move every partly filled staging line out. Call once, when done appending.
    pub fn finish(&mut self, arena: &mut Arena) {
        for s in 0..self.n.len() {
            let n = self.n[s] as usize;
            let r = n & (Self::PER_LINE - 1);
            for k in 0..r {
                let v = unsafe { *(self.lines.as_ptr().add(s) as *const T).add(k) };
                let d = self.dest(s, n - r + k, arena);
                unsafe { d.write(v) };
            }
        }
        unsafe { core::arch::x86_64::_mm_sfence() };
    }

    pub fn len(&self, s: usize) -> usize {
        self.n[s] as usize
    }

    #[inline(always)]
    pub fn at(&self, s: usize, idx: usize) -> *mut T {
        unsafe { self.chunks.get_unchecked(s).get_unchecked(idx >> CB).add(idx & ((1 << CB) - 1)) }
    }

    /// Stream `s` as (pointer, length) pieces.
    pub fn pieces(&self, s: usize) -> impl Iterator<Item = (*mut T, usize)> + '_ {
        let n = self.n[s] as usize;
        self.chunks[s].iter().enumerate().map(move |(i, &p)| (p, (n - (i << CB)).min(1 << CB)))
    }
}
