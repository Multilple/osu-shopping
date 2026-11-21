//! Shared structures: the concurrent ID table (which is also the union-find
//! over linked IDs), and arena-backed append-only buckets.

use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering::*};

use crate::util::*;

/// Fresh zeroed memory, starting on a 2 MB boundary so that all of it can
/// be backed by huge pages.
pub fn map_anon(bytes: usize) -> *mut u8 {
    const HUGE: usize = 2 << 20;
    unsafe {
        let p = sys::mmap(
            std::ptr::null_mut(),
            bytes + HUGE,
            sys::PROT_READ | sys::PROT_WRITE,
            sys::MAP_PRIVATE | sys::MAP_ANONYMOUS | sys::MAP_NORESERVE,
            -1,
            0,
        );
        assert!(p != sys::MAP_FAILED, "mmap of {bytes} bytes failed");
        let p = ((p as usize + HUGE - 1) & !(HUGE - 1)) as *mut std::ffi::c_void;
        if std::env::var_os("SS_NOHUGE").is_none() {
            sys::madvise(p, bytes, sys::MADV_HUGEPAGE);
        }
        p as *mut u8
    }
}

// ---------------------------------------------------------------- ID table

pub const RBITS: usize = 12;
pub const RSIZE: usize = 1 << RBITS;
const RMASK: usize = RSIZE - 1;
/// In a slot's high key word: the slot is being written.
const BUSY: u64 = u64::MAX;

#[repr(C, align(32))]
pub struct Slot {
    lo: AtomicU64,
    hi: AtomicU64,
    /// Union-find parent: a slot index. A root points at itself.
    root: AtomicU32,
}

/// Every user ID seen, each in a fixed slot. The table is split into regions
/// of 4096 slots; an ID hashes to a region and probes inside it. Lookups and
/// inserts are lock-free.
pub struct IdTable {
    slots: *mut Slot,
    pub nregions: usize,
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
            overflow: AtomicBool::new(false),
        }
    }

    /// Give back the pages of part `i` of `n` of the table.
    pub fn release_part(&self, i: usize, n: usize) {
        let bytes = self.nregions * RSIZE * std::mem::size_of::<Slot>();
        let page = 2 << 20;
        let (a, b) = (bytes * i / n / page * page, if i + 1 == n { bytes } else { bytes * (i + 1) / n / page * page });
        if b > a {
            release(unsafe { (self.slots as *mut u8).add(a) }, b - a);
        }
    }

    /// Write to every page of part `i` of `n` of the still empty table.
    ///
    /// A lookup reads a slot before anything is written near it, and memory
    /// that is first read gets the shared zero page: a later write then makes
    /// the kernel break the huge page up and fault its 4 KB pieces in one at
    /// a time. Written first, each 2 MB comes in as one huge page and stays
    /// one.
    pub fn touch_part(&self, i: usize, n: usize) {
        let bytes = self.bytes();
        let (a, b) = (bytes * i / n / 4096 * 4096, if i + 1 == n { bytes } else { bytes * (i + 1) / n / 4096 * 4096 });
        let mut o = a;
        while o < b {
            unsafe { std::ptr::write_volatile((self.slots as *mut u8).add(o), 0) };
            o += 4096;
        }
    }

    /// Where the table's memory starts, and its size.
    pub fn base(&self) -> *const u8 {
        self.slots as *const u8
    }

    pub fn bytes(&self) -> usize {
        self.nregions * RSIZE * std::mem::size_of::<Slot>()
    }

    /// IDs in the table (diagnostics; call between stages only).
    pub fn len(&self) -> usize {
        (0..self.nregions * RSIZE).filter(|&i| self.slot(i).hi.load(Relaxed) != 0).count()
    }

    #[inline(always)]
    fn slot(&self, i: usize) -> &Slot {
        unsafe { &*self.slots.add(i) }
    }

    /// Where a probe starts: always the first of the two slots of a cache
    /// line, so the first two slots tried cost one cache miss.
    #[inline(always)]
    fn home(&self, h: u64) -> (usize, usize) {
        let region = (((h >> 32) * self.nregions as u64) >> 32) as usize;
        (region << RBITS, (h as usize) & RMASK & !1)
    }

    #[inline(always)]
    pub fn prefetch(&self, h: u64) {
        let (base, pos) = self.home(h);
        self.prefetch_at((base | pos) as u32);
    }

    /// The first slot a probe for hash `h` tries.
    #[inline(always)]
    pub fn first(&self, h: u64) -> u32 {
        let (base, pos) = self.home(h);
        (base | pos) as u32
    }

    #[inline(always)]
    pub fn prefetch_at(&self, first: u32) {
        unsafe {
            core::arch::x86_64::_mm_prefetch(self.slots.add(first as usize) as *const i8, core::arch::x86_64::_MM_HINT_T0);
        }
    }

    /// The slot of `k` (whose hash is `h`), inserting it as its own root if new.
    #[inline(always)]
    pub fn get_or_insert(&self, k: Key, h: u64) -> u32 {
        self.get_or_insert_at(k as u64, (k >> 64) as u64, self.first(h))
    }

    /// The key's slot if it is one of the two at `first`. When it is not,
    /// `None`, having asked for the next cache line, where `get_or_insert_at`
    /// will most likely look next.
    #[inline(always)]
    pub fn peek(&self, klo: u64, khi: u64, first: u32) -> Option<u32> {
        let first = first as usize;
        let (s0, s1) = (self.slot(first), self.slot(first | 1));
        let d0 = (s0.hi.load(Acquire) ^ khi) | (s0.lo.load(Relaxed) ^ klo);
        let d1 = (s1.hi.load(Acquire) ^ khi) | (s1.lo.load(Relaxed) ^ klo);
        let at = opaque((first | (d0 != 0) as usize) as u64);
        if opaque(((d0 != 0) & (d1 != 0)) as u64) == 0 {
            return Some(at as u32);
        }
        self.prefetch_at(((first & !RMASK) | ((first + 2) & RMASK)) as u32);
        None
    }

    /// `get_or_insert` for a key given as two words and its `first` slot.
    #[cfg_attr(feature = "prof", inline(never))]
    #[cfg_attr(not(feature = "prof"), inline(always))]
    pub fn get_or_insert_at(&self, klo: u64, khi: u64, first: u32) -> u32 {
        let (base, start) = (first as usize & !RMASK, first as usize & RMASK);
        // Nearly always the key is in one of the first two slots: test both
        // without branching on which. (A slot's high word is its key's only
        // once the whole slot is written, so a match is never a half-written
        // slot; a missed one is found again by the careful loop.)
        let (s0, s1) = (self.slot(base | start), self.slot(base | start | 1));
        let d0 = (s0.hi.load(Acquire) ^ khi) | (s0.lo.load(Relaxed) ^ klo);
        let d1 = (s1.hi.load(Acquire) ^ khi) | (s1.lo.load(Relaxed) ^ klo);
        let at = opaque((base | start | (d0 != 0) as usize) as u64);
        if opaque(((d0 != 0) & (d1 != 0)) as u64) == 0 {
            return at as u32;
        }
        self.probe(klo, khi, base, start)
    }

    #[cold]
    #[inline(never)]
    fn probe(&self, klo: u64, khi: u64, base: usize, start: usize) -> u32 {
        let mut pos = start;
        loop {
            let s = self.slot(base | pos);
            let hi = s.hi.load(Acquire);
            if hi == khi && s.lo.load(Relaxed) == klo {
                return (base | pos) as u32;
            }
            if hi == 0 || hi == BUSY {
                return self.insert(klo, khi, base, pos, start);
            }
            pos = (pos + 1) & RMASK;
        }
    }

    /// Continue a probe at an empty or half-written slot. A slot is claimed by
    /// swapping BUSY into the key's high word (which is never 0 or BUSY for a
    /// real key); the real high word goes in last, so a reader that sees it
    /// sees the whole slot. No locks: a thread that stalls mid-insert holds up
    /// only readers of that one slot.
    #[cold]
    #[inline(never)]
    fn insert(&self, klo: u64, khi: u64, base: usize, mut pos: usize, start: usize) -> u32 {
        loop {
            let s = self.slot(base | pos);
            let mut hi = s.hi.load(Acquire);
            if hi == 0 {
                match s.hi.compare_exchange(0, BUSY, Acquire, Acquire) {
                    Ok(_) => {
                        s.root.store((base | pos) as u32, Relaxed);
                        s.lo.store(klo, Relaxed);
                        s.hi.store(khi, Release);
                        return (base | pos) as u32;
                    }
                    Err(now) => hi = now,
                }
            }
            while hi == BUSY {
                std::hint::spin_loop();
                hi = s.hi.load(Acquire);
            }
            if hi == khi && s.lo.load(Relaxed) == klo {
                return (base | pos) as u32;
            }
            pos = (pos + 1) & RMASK;
            // A probe this long means the table was sized far too small.
            if pos.wrapping_sub(start) & RMASK >= RSIZE / 4 {
                self.overflow.store(true, Relaxed);
                return base as u32;
            }
        }
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

/// Give the pages of a mapping back to the system (the mapping stays).
pub fn release(ptr: *mut u8, bytes: usize) {
    unsafe {
        sys::madvise(ptr as *mut std::ffi::c_void, bytes, sys::MADV_DONTNEED);
    }
}

/// Bump allocator over large anonymous mappings.
pub struct Arena {
    cur: usize,
    end: usize,
    blocks: Vec<(usize, usize)>,
}

impl Arena {
    pub fn new() -> Arena {
        Arena { cur: 0, end: 0, blocks: Vec::new() }
    }

    /// Give every page back. Nothing allocated here may be used afterwards.
    pub fn release(&self) {
        for &(ptr, bytes) in &self.blocks {
            release(ptr as *mut u8, bytes);
        }
    }

    #[inline(always)]
    pub fn alloc(&mut self, bytes: usize) -> *mut u8 {
        if self.cur + bytes > self.end {
            let block = bytes.max(32 << 20);
            self.cur = map_anon(block) as usize;
            self.end = self.cur + block;
            self.blocks.push((self.cur, block));
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
    /// Where each stream's staging line goes when it fills.
    dst: Vec<*mut T>,
    chunks: Vec<Vec<*mut T>>,
    /// Experiment (SS_QUEUE=1): appends without the branch on whether the
    /// staging line is full. Every append writes what the full line would be
    /// into the next slot of a short queue and only counts the slot when the
    /// line is in fact full; the queue is emptied each time it fills.
    qmode: bool,
    queue: Vec<Line>,
    owner: Vec<u64>,
    queued: usize,
    /// A line nobody writes, read in place of a staging line that is not
    /// full yet (reading that one right after writing part of it would wait
    /// on the store).
    idle: Box<Line>,
}

/// Lines the queue holds before it is emptied.
const QUEUE: usize = 32;

unsafe impl<T, const CB: usize> Sync for Streams<T, CB> {}
unsafe impl<T, const CB: usize> Send for Streams<T, CB> {}

impl<T: Copy, const CB: usize> Streams<T, CB> {
    const PER_LINE: usize = 64 / std::mem::size_of::<T>();

    pub fn new(streams: usize) -> Self {
        assert!(64 % std::mem::size_of::<T>() == 0 && (1usize << CB) % Self::PER_LINE == 0);
        Streams {
            lines: vec![Line([0; 64]); streams],
            n: vec![0; streams],
            dst: vec![std::ptr::null_mut(); streams],
            chunks: (0..streams).map(|_| Vec::new()).collect(),
            qmode: std::env::var_os("SS_QUEUE").is_some(),
            queue: vec![Line([0; 64]); QUEUE],
            owner: vec![0; QUEUE],
            queued: 0,
            idle: Box::new(Line([0; 64])),
        }
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

    #[cold]
    #[inline(never)]
    fn new_chunk(&mut self, s: usize, arena: &mut Arena) {
        let p = arena.alloc(std::mem::size_of::<T>() << CB) as *mut T;
        self.chunks[s].push(p);
        self.dst[s] = p;
    }

    /// Send the queued lines to their lists.
    #[inline(never)]
    fn drain(&mut self, arena: &mut Arena) {
        unsafe {
            use core::arch::x86_64::*;
            for i in 0..self.queued {
                let o = *self.owner.get_unchecked(i);
                let (s, last) = (o as u32 as usize, (o >> 32) as usize);
                // The line's first item starts a chunk?
                if (last + 1 - Self::PER_LINE) & ((1 << CB) - 1) == 0 {
                    self.new_chunk(s, arena);
                }
                let d = *self.dst.get_unchecked(s) as *mut __m256i;
                let q = self.queue.as_ptr().add(i) as *const __m256i;
                _mm256_stream_si256(d, _mm256_load_si256(q));
                _mm256_stream_si256(d.add(1), _mm256_load_si256(q.add(1)));
                *self.dst.get_unchecked_mut(s) = d.add(2) as *mut T;
            }
            self.queued = 0;
        }
    }

    #[inline(always)]
    unsafe fn push16_q(&mut self, s: usize, a: u64, b: u64, arena: &mut Arena) -> usize {
        use core::arch::x86_64::*;
        let n = *self.n.get_unchecked(s) as usize;
        let r = n & 3;
        let line = self.lines.as_mut_ptr().add(s) as *mut __m128i;
        *self.n.get_unchecked_mut(s) = (n + 1) as u32;
        let v = _mm_set_epi64x(b as i64, a as i64);
        _mm_store_si128(line.add(r), v);
        // The full line, if this is its last item: three staged items and
        // this one from the register.
        let full = r == 3;
        let src = std::hint::select_unpredictable(full, line as *const __m128i, &*self.idle as *const Line as *const __m128i);
        let k = self.queued;
        let q = self.queue.as_mut_ptr().add(k) as *mut __m256i;
        let lo = _mm256_loadu_si256(src as *const __m256i);
        let hi = _mm256_inserti128_si256(_mm256_castsi128_si256(_mm_load_si128(src.add(2))), v, 1);
        _mm256_store_si256(q, lo);
        _mm256_store_si256(q.add(1), hi);
        *self.owner.get_unchecked_mut(k) = ((n as u64) << 32) | s as u64;
        let k = k + full as usize;
        self.queued = k;
        if k == QUEUE {
            self.drain(arena);
        }
        n
    }

    #[inline(always)]
    unsafe fn push32_q(&mut self, s: usize, w: [u64; 4], arena: &mut Arena) -> usize {
        use core::arch::x86_64::*;
        let n = *self.n.get_unchecked(s) as usize;
        let r = n & 1;
        let line = self.lines.as_mut_ptr().add(s) as *mut __m256i;
        *self.n.get_unchecked_mut(s) = (n + 1) as u32;
        let v = _mm256_set_epi64x(w[3] as i64, w[2] as i64, w[1] as i64, w[0] as i64);
        _mm256_store_si256(line.add(r), v);
        let full = r == 1;
        let src = std::hint::select_unpredictable(full, line as *const __m256i, &*self.idle as *const Line as *const __m256i);
        let k = self.queued;
        let q = self.queue.as_mut_ptr().add(k) as *mut __m256i;
        _mm256_store_si256(q, _mm256_load_si256(src));
        _mm256_store_si256(q.add(1), v);
        *self.owner.get_unchecked_mut(k) = ((n as u64) << 32) | s as u64;
        let k = k + full as usize;
        self.queued = k;
        if k == QUEUE {
            self.drain(arena);
        }
        n
    }

    /// Append a 16-byte item, given as two words, to stream `s` and return
    /// its index. The item that fills a staging line goes out straight from
    /// the registers: reading it back from the line right after writing it
    /// would stall on the store.
    #[inline(always)]
    pub fn push16(&mut self, s: usize, a: u64, b: u64, arena: &mut Arena) -> usize {
        debug_assert!(std::mem::size_of::<T>() == 16);
        unsafe {
            use core::arch::x86_64::*;
            if self.qmode {
                return self.push16_q(s, a, b, arena);
            }
            let n = *self.n.get_unchecked(s) as usize;
            let r = n & 3;
            let line = self.lines.as_mut_ptr().add(s) as *mut u64;
            *self.n.get_unchecked_mut(s) = (n + 1) as u32;
            if r == 3 {
                if (n - 3) & ((1 << CB) - 1) == 0 {
                    self.new_chunk(s, arena);
                }
                let d = *self.dst.get_unchecked(s) as *mut __m256i;
                let lo = _mm256_load_si256(line as *const __m256i);
                let third = _mm_load_si128(line.add(4) as *const __m128i);
                let hi = _mm256_inserti128_si256(_mm256_castsi128_si256(third), _mm_set_epi64x(b as i64, a as i64), 1);
                _mm256_stream_si256(d, lo);
                _mm256_stream_si256(d.add(1), hi);
                *self.dst.get_unchecked_mut(s) = d.add(2) as *mut T;
            } else {
                *line.add(2 * r) = a;
                *line.add(2 * r + 1) = b;
            }
            n
        }
    }

    /// `push16` for 32-byte items given as four words.
    #[inline(always)]
    pub fn push32(&mut self, s: usize, w: [u64; 4], arena: &mut Arena) -> usize {
        debug_assert!(std::mem::size_of::<T>() == 32);
        unsafe {
            use core::arch::x86_64::*;
            if self.qmode {
                return self.push32_q(s, w, arena);
            }
            let n = *self.n.get_unchecked(s) as usize;
            let line = self.lines.as_mut_ptr().add(s) as *mut u64;
            *self.n.get_unchecked_mut(s) = (n + 1) as u32;
            if n & 1 == 1 {
                if (n - 1) & ((1 << CB) - 1) == 0 {
                    self.new_chunk(s, arena);
                }
                let d = *self.dst.get_unchecked(s) as *mut __m256i;
                let lo = _mm256_load_si256(line as *const __m256i);
                let hi = _mm256_set_epi64x(w[3] as i64, w[2] as i64, w[1] as i64, w[0] as i64);
                _mm256_stream_si256(d, lo);
                _mm256_stream_si256(d.add(1), hi);
                *self.dst.get_unchecked_mut(s) = d.add(2) as *mut T;
            } else {
                *line = w[0];
                *line.add(1) = w[1];
                *line.add(2) = w[2];
                *line.add(3) = w[3];
            }
            n
        }
    }

    /// Move every partly filled staging line out. Call once, when done appending.
    pub fn finish(&mut self, arena: &mut Arena) {
        self.drain(arena);
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
