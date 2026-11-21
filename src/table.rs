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
    #[inline(always)]
    pub fn get_or_insert(&self, k: Key, h: u64) -> u32 {
        let (base, start) = self.home(h);
        let (klo, khi) = (k as u64, (k >> 64) as u64);
        let mut pos = start;
        loop {
            let s = self.slot(base | pos);
            let lo = s.lo.load(Acquire);
            if lo == klo && s.hi.load(Relaxed) == khi {
                return (base | pos) as u32;
            }
            if lo == 0 {
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
            let lo = s.lo.load(Relaxed);
            if lo == klo && s.hi.load(Relaxed) == khi {
                break (base | pos) as u32;
            }
            if lo == 0 {
                let count = unsafe { &mut *self.counts[region].get() };
                if *count >= RLIMIT {
                    self.overflow.store(true, Relaxed);
                    break base as u32;
                }
                *count += 1;
                // The key's low word goes last: a reader that sees it sees the rest.
                s.root.store((base | pos) as u32, Relaxed);
                s.hi.store(khi, Relaxed);
                s.lo.store(klo, Release);
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
            if self.slot(i).lo.load(Relaxed) != 0 {
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

/// An append-only list of `T` stored in fixed-size chunks of `1 << CB` items.
pub struct Bucket<T, const CB: usize> {
    cur: *mut T,
    end: *mut T,
    pub chunks: Vec<*mut T>,
}

unsafe impl<T, const CB: usize> Sync for Bucket<T, CB> {}
unsafe impl<T, const CB: usize> Send for Bucket<T, CB> {}

impl<T: Copy, const CB: usize> Bucket<T, CB> {
    pub fn new() -> Self {
        Bucket { cur: std::ptr::null_mut(), end: std::ptr::null_mut(), chunks: Vec::new() }
    }

    #[cold]
    #[inline(never)]
    fn grow(&mut self, arena: &mut Arena) {
        let p = arena.alloc(std::mem::size_of::<T>() << CB) as *mut T;
        self.chunks.push(p);
        self.cur = p;
        self.end = unsafe { p.add(1 << CB) };
    }

    /// Append and return the item's index.
    #[inline(always)]
    pub fn push(&mut self, v: T, arena: &mut Arena) -> usize {
        if self.cur == self.end {
            self.grow(arena);
        }
        unsafe {
            self.cur.write(v);
            self.cur = self.cur.add(1);
            (self.chunks.len() << CB) - (self.end.offset_from(self.cur) as usize) - 1
        }
    }

    pub fn len(&self) -> usize {
        if self.chunks.is_empty() {
            0
        } else {
            (self.chunks.len() << CB) - unsafe { self.end.offset_from(self.cur) as usize }
        }
    }

    #[inline(always)]
    pub fn at(&self, idx: usize) -> *mut T {
        unsafe { self.chunks.get_unchecked(idx >> CB).add(idx & ((1 << CB) - 1)) }
    }

    /// The filled part of every chunk.
    pub fn slices(&self) -> impl Iterator<Item = &mut [T]> + '_ {
        let n = self.chunks.len();
        let last = if n == 0 { 0 } else { (1usize << CB) - unsafe { self.end.offset_from(self.cur) as usize } };
        self.chunks
            .iter()
            .enumerate()
            .map(move |(i, &p)| unsafe { std::slice::from_raw_parts_mut(p, if i + 1 == n { last } else { 1 << CB }) })
    }
}
