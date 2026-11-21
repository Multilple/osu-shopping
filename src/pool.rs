//! A pool of worker threads that live for the whole process, so that `par`
//! does not create threads on every call (on the scoring machine each
//! creation costs tens of microseconds in the creating thread, serially).
//!
//! One job at a time, always handed out by the same (main) thread: the job
//! is a pointer to the caller's closure and a function that calls it. The
//! closure lives on the caller's stack; `run` returns only once every worker
//! has returned from it, and a worker touches nothing of a job after it has
//! said it is done, so the borrow never outlives the call (as with a scope).
//!
//! Waiting is a short spin (stages follow each other within microseconds),
//! then a futex sleep. `SS_POOL=0` turns the pool off (threads made per call
//! as before); `SS_SPIN=<us>` sets the spin (default 150 us).

use std::cell::UnsafeCell;
use std::hint::spin_loop;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering::{Acquire, Relaxed, SeqCst};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

extern "C" {
    fn syscall(n: i64, ...) -> i64;
    fn fcntl(fd: i32, cmd: i32, ...) -> i32;
    fn close(fd: i32) -> i32;
}
const SYS_FUTEX: i64 = 202;
const FUTEX_WAIT_PRIVATE: i64 = 128;
const FUTEX_WAKE_PRIVATE: i64 = 129;

fn futex_wait(w: &AtomicU32, val: u32) {
    unsafe { syscall(SYS_FUTEX, w.as_ptr(), FUTEX_WAIT_PRIVATE, val, std::ptr::null::<u8>(), std::ptr::null::<u8>(), 0i64) };
}

fn futex_wake(w: &AtomicU32, n: i32) {
    unsafe { syscall(SYS_FUTEX, w.as_ptr(), FUTEX_WAKE_PRIVATE, n, std::ptr::null::<u8>(), std::ptr::null::<u8>(), 0i64) };
}

/// Spin until `done()` or the spin budget is used up; true if done.
#[inline]
fn spin(done: impl Fn() -> bool, budget: Duration) -> bool {
    let t = Instant::now();
    loop {
        for _ in 0..32 {
            if done() {
                return true;
            }
            spin_loop();
        }
        if t.elapsed() >= budget {
            return done();
        }
    }
}

type Call = unsafe fn(*const (), usize);

unsafe fn call<F: Fn(usize) + Sync>(f: *const (), tid: usize) {
    (*(f as *const F))(tid)
}

pub struct Pool {
    /// Threads in all, the caller's included.
    n: usize,
    budget: Duration,
    /// Jobs handed out so far (the workers' futex word).
    gen: AtomicU32,
    /// Workers asleep on `gen`.
    sleeping: AtomicU32,
    /// Workers not yet done with the current job (the caller's futex word).
    left: AtomicU32,
    /// The caller is asleep on `left`.
    parked: AtomicU32,
    /// The current job; written only while no worker has a job.
    job: UnsafeCell<(*const (), Call)>,
}

unsafe impl Sync for Pool {}
unsafe impl Send for Pool {}

unsafe fn nothing(_: *const (), _: usize) {}

static POOL: OnceLock<Pool> = OnceLock::new();

fn enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| std::env::var("SS_POOL").map_or(true, |v| v != "0"))
}

/// The pool for `threads` threads, made (and its workers started) at the
/// first call; None when the pool is off or was made for another count.
pub fn get(threads: usize) -> Option<&'static Pool> {
    if threads <= 1 || !enabled() {
        return None;
    }
    let p = POOL.get_or_init(|| Pool {
        n: threads,
        budget: Duration::from_micros(crate::env_usize("SS_SPIN", 150) as u64),
        gen: AtomicU32::new(0),
        sleeping: AtomicU32::new(0),
        left: AtomicU32::new(0),
        parked: AtomicU32::new(0),
        job: UnsafeCell::new((std::ptr::null(), nothing as Call)),
    });
    static STARTED: OnceLock<()> = OnceLock::new();
    STARTED.get_or_init(|| p.start(1));
    (p.n == threads).then_some(p)
}

/// Start the pool's workers now (they start one another, two each, so the
/// caller makes only one thread), so that they are up by the first `par`.
pub fn warm(threads: usize) {
    if threads > 1 && enabled() {
        room_for_files();
    }
    let _ = get(threads);
}

/// Grow the table of open files now, while this is the only thread: once
/// other threads share it, each time it grows (at 64, 128, 256 ... files
/// open) the kernel waits for an RCU grace period, milliseconds on a VM.
/// (A file opened at 1023 or above and closed again leaves it that big.)
fn room_for_files() {
    const F_DUPFD_CLOEXEC: i32 = 1030;
    for at in [1023, 511, 255] {
        let fd = unsafe { fcntl(2, F_DUPFD_CLOEXEC, at) };
        if fd >= 0 {
            unsafe { close(fd) };
            return;
        }
    }
}

impl Pool {
    /// Start worker `w` (1-based); it starts workers 2w and 2w+1.
    fn start(&'static self, w: usize) {
        if w >= self.n {
            return;
        }
        std::thread::Builder::new()
            .name(format!("w{w}"))
            .spawn(move || {
                self.start(2 * w);
                self.start(2 * w + 1);
                self.work(w);
            })
            .expect("thread");
    }

    fn work(&self, tid: usize) -> ! {
        let mut seen = 0u32;
        loop {
            if !spin(|| self.gen.load(Acquire) != seen, self.budget) {
                loop {
                    self.sleeping.fetch_add(1, SeqCst);
                    if self.gen.load(SeqCst) == seen {
                        futex_wait(&self.gen, seen);
                    }
                    self.sleeping.fetch_sub(1, SeqCst);
                    if self.gen.load(Acquire) != seen {
                        break;
                    }
                }
            }
            // (The caller hands out no other job before this one is done
            // by every worker, so this is the next one.)
            seen = seen.wrapping_add(1);
            debug_assert_eq!(self.gen.load(Relaxed), seen);
            let (f, c) = unsafe { *self.job.get() };
            unsafe { c(f, tid) };
            // Done: nothing of the job is touched from here on.
            if self.left.fetch_sub(1, SeqCst) == 1 && self.parked.load(SeqCst) != 0 {
                futex_wake(&self.left, 1);
            }
        }
    }

    /// `f(tid)` for every tid, the caller taking 0; returns when all are done.
    pub fn run<F: Fn(usize) + Sync>(&self, f: &F) {
        debug_assert_eq!(self.left.load(Relaxed), 0);
        self.left.store((self.n - 1) as u32, Relaxed);
        unsafe { *self.job.get() = (f as *const F as *const (), call::<F> as Call) };
        // (Publishes the job and the count.)
        self.gen.fetch_add(1, SeqCst);
        if self.sleeping.load(SeqCst) != 0 {
            futex_wake(&self.gen, i32::MAX);
        }
        f(0);
        if !spin(|| self.left.load(Acquire) == 0, self.budget) {
            self.parked.store(1, SeqCst);
            loop {
                let l = self.left.load(SeqCst);
                if l == 0 {
                    break;
                }
                futex_wait(&self.left, l);
            }
            self.parked.store(0, Relaxed);
        }
    }
}
