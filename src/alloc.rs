//! Big blocks of memory, from huge pages and kept for reuse.
//!
//! The system allocator takes a big block from the kernel in 4 KB pages (a
//! fault for each on first use) and hands it back when it is freed, so that
//! the next one pays again. Here, blocks of 64 KB and more are cut from
//! 64 MB stretches the kernel is asked to back with 2 MB pages, in
//! power-of-two sizes; a freed block waits in its size's list for the next
//! request. The biggest ones (over 32 MB) are mapped on their own and given
//! back when freed. Everything smaller goes to the system allocator.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::UnsafeCell;
use std::ffi::c_void;
use std::sync::atomic::{AtomicBool, Ordering::*};

use crate::util::sys;

/// Requests of this size and more are served here.
const MIN: usize = 1 << 16;
const MIN_BITS: u32 = 16;
/// The sizes blocks come in: `MIN << c` for `c` below this.
const CLASSES: usize = 10;
/// The biggest of them.
const TOP: usize = MIN << (CLASSES - 1);
const HUGE: usize = 2 << 20;
/// What is asked of the kernel at a time for the blocks.
const STRETCH: usize = 64 << 20;

struct State {
    /// The unused rest of the current stretch.
    cur: usize,
    end: usize,
    /// By size class: the freed blocks, each holding the address of the next.
    free: [usize; CLASSES],
}

pub struct Big {
    busy: AtomicBool,
    st: UnsafeCell<State>,
}

unsafe impl Sync for Big {}

/// The size class of a request of `size` bytes (`MIN..=TOP`).
#[inline(always)]
fn class(size: usize) -> usize {
    ((usize::BITS - (size - 1).leading_zeros()).max(MIN_BITS) - MIN_BITS) as usize
}

/// `bytes` of fresh zeroed memory on a 2 MB boundary, huge pages asked for.
unsafe fn map(bytes: usize) -> usize {
    let p = sys::mmap(
        std::ptr::null_mut(),
        bytes + HUGE,
        sys::PROT_READ | sys::PROT_WRITE,
        sys::MAP_PRIVATE | sys::MAP_ANONYMOUS | sys::MAP_NORESERVE,
        -1,
        0,
    );
    if p == sys::MAP_FAILED {
        return 0;
    }
    let p = p as usize;
    let a = (p + HUGE - 1) & !(HUGE - 1);
    // (What sticks out on either side goes back: a block may be unmapped whole later.)
    if a > p {
        sys::munmap(p as *mut c_void, a - p);
    }
    let tail = p + bytes + HUGE - (a + bytes);
    if tail > 0 {
        sys::munmap((a + bytes) as *mut c_void, tail);
    }
    sys::madvise(a as *mut c_void, bytes, sys::MADV_HUGEPAGE);
    a
}

#[inline(always)]
fn whole(size: usize) -> usize {
    (size + HUGE - 1) & !(HUGE - 1)
}

impl Big {
    pub const fn new() -> Big {
        Big { busy: AtomicBool::new(false), st: UnsafeCell::new(State { cur: 0, end: 0, free: [0; CLASSES] }) }
    }

    #[inline(always)]
    fn lock(&self) -> &mut State {
        while self.busy.compare_exchange_weak(false, true, Acquire, Relaxed).is_err() {
            std::hint::spin_loop();
        }
        unsafe { &mut *self.st.get() }
    }

    #[inline(always)]
    fn unlock(&self) {
        self.busy.store(false, Release);
    }

    /// A block for `size` bytes (at least `MIN`), and whether it is known to be all zero.
    unsafe fn take(&self, size: usize) -> (*mut u8, bool) {
        if size > TOP {
            return (map(whole(size)) as *mut u8, true);
        }
        let c = class(size);
        let bytes = MIN << c;
        let st = self.lock();
        let p = st.free[c];
        if p != 0 {
            st.free[c] = *(p as *const usize);
            self.unlock();
            return (p as *mut u8, false);
        }
        if st.cur + bytes > st.end {
            let s = map(STRETCH);
            if s == 0 {
                self.unlock();
                return (std::ptr::null_mut(), true);
            }
            st.cur = s;
            st.end = s + STRETCH;
        }
        let p = st.cur;
        st.cur += bytes;
        self.unlock();
        (p as *mut u8, true)
    }

    unsafe fn give(&self, p: *mut u8, size: usize) {
        if size > TOP {
            sys::munmap(p as *mut c_void, whole(size));
            return;
        }
        let c = class(size);
        let st = self.lock();
        *(p as *mut usize) = st.free[c];
        st.free[c] = p as usize;
        self.unlock();
    }
}

/// Whether a request is one for the blocks here. (A block starts on a 64 KB
/// boundary; nothing asks for more than that.)
#[inline(always)]
fn big(l: &Layout) -> bool {
    l.size() >= MIN && l.align() <= MIN
}

unsafe impl GlobalAlloc for Big {
    #[inline]
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        if big(&l) {
            self.take(l.size()).0
        } else {
            System.alloc(l)
        }
    }

    #[inline]
    unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
        if big(&l) {
            let (p, zero) = self.take(l.size());
            if !zero && !p.is_null() {
                std::ptr::write_bytes(p, 0, l.size());
            }
            p
        } else {
            System.alloc_zeroed(l)
        }
    }

    #[inline]
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        if big(&l) {
            self.give(p, l.size())
        } else {
            System.dealloc(p, l)
        }
    }

    #[inline]
    unsafe fn realloc(&self, p: *mut u8, l: Layout, new: usize) -> *mut u8 {
        let old = l.size();
        let to = Layout::from_size_align_unchecked(new, l.align());
        match (big(&l), big(&to)) {
            (false, false) => return System.realloc(p, l, new),
            // The block it has is the block it would get.
            (true, true) if old <= TOP && new <= TOP && class(old) == class(new) => return p,
            (true, true) if old > TOP && new > TOP && whole(old) == whole(new) => return p,
            _ => {}
        }
        let q = self.alloc(to);
        if !q.is_null() {
            std::ptr::copy_nonoverlapping(p, q, old.min(new));
            self.dealloc(p, l);
        }
        q
    }
}
