//! A tiny sampling profiler for development (feature `sprof`): SIGPROF every
//! millisecond of CPU time, recording the interrupted instruction pointer.
use std::ffi::c_void;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering::Relaxed};

const CAP: usize = 1 << 17;
static N: AtomicUsize = AtomicUsize::new(0);
static BUF: [AtomicU64; CAP] = [const { AtomicU64::new(0) }; CAP];

#[repr(C)]
struct SigAction {
    handler: usize,
    mask: [u64; 16],
    flags: i32,
    restorer: usize,
}
#[repr(C)]
struct ITimerVal {
    interval: [i64; 2],
    value: [i64; 2],
}
extern "C" {
    fn sigaction(sig: i32, act: *const SigAction, old: *mut SigAction) -> i32;
    fn setitimer(which: i32, new: *const ITimerVal, old: *mut ITimerVal) -> i32;
}

extern "C" fn on_prof(_sig: i32, _info: *mut c_void, uctx: *mut c_void) {
    // ucontext_t.uc_mcontext.gregs[REG_RIP] on x86-64 Linux.
    let rip = unsafe { *((uctx as *const u8).add(168) as *const u64) };
    let i = N.fetch_add(1, Relaxed);
    if i < CAP {
        BUF[i].store(rip, Relaxed);
    }
}

#[inline(never)]
pub fn anchor() {}

pub fn start() {
    unsafe {
        let sa = SigAction { handler: on_prof as usize, mask: [0; 16], flags: 4 | 0x1000_0000, restorer: 0 };
        sigaction(27, &sa, std::ptr::null_mut());
        let tv = ITimerVal { interval: [0, 1000], value: [0, 1000] };
        setitimer(2, &tv, std::ptr::null_mut());
    }
}

pub fn dump() {
    use std::io::Write;
    unsafe {
        let tv = ITimerVal { interval: [0, 0], value: [0, 0] };
        setitimer(2, &tv, std::ptr::null_mut());
    }
    let n = N.load(Relaxed).min(CAP);
    let mut f = std::io::BufWriter::new(std::fs::File::create("/tmp/sprof.txt").unwrap());
    writeln!(f, "anchor {:x}", anchor as usize).unwrap();
    for i in 0..n {
        writeln!(f, "{:x}", BUF[i].load(Relaxed)).unwrap();
    }
}
