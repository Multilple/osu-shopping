//! Diagnostics for a run whose only visible result is its total time: facts
//! about the machine and the cost of two memory operations, each packed into
//! a number that `run` turns into a wait at the end (see `SS_DIAG`).

use std::time::Instant;

use crate::table::{map_anon, release, IdTable};

fn read(path: &str) -> String {
    std::fs::read_to_string(path).unwrap_or_default()
}

/// 0..=31: huge page mode (0 madvise, 1 always, 2 never, 3 unknown), plus 4
/// when this process really has huge pages, plus 8 times the CPU generation
/// (0 Skylake or Cascade Lake, 1 Ice Lake, 2 Sapphire Rapids, 3 other).
pub fn facts() -> u32 {
    let thp = read("/sys/kernel/mm/transparent_hugepage/enabled");
    let mode = if thp.contains("[madvise]") {
        0
    } else if thp.contains("[always]") {
        1
    } else if thp.contains("[never]") {
        2
    } else {
        3
    };
    let huge_kb = read("/proc/self/smaps_rollup")
        .lines()
        .find(|l| l.starts_with("AnonHugePages:"))
        .and_then(|l| l.split_whitespace().nth(1).and_then(|v| v.parse::<u64>().ok()))
        .unwrap_or(0);
    let model = read("/proc/cpuinfo")
        .lines()
        .find(|l| l.starts_with("model\t") || l.starts_with("model "))
        .and_then(|l| l.split(':').nth(1).and_then(|v| v.trim().parse::<u32>().ok()))
        .unwrap_or(0);
    let cpu = match model {
        85 => 0,
        106 | 108 => 1,
        143 | 207 => 2,
        _ => 3,
    };
    mode + 4 * (huge_kb >= 65_536) as u32 + 8 * cpu
}

/// Nanoseconds per random read of the ID table, each asked for 16 reads
/// ahead (0..=63), and the cost of touching fresh memory for the first time
/// in steps of 0.05 s per GB (0..=7), with every thread busy as in the run (`par` runs a closure on each).
pub fn costs(ids: &IdTable, par: &dyn Fn(&(dyn Fn(usize) + Sync))) -> (u32, u32) {
    const N: usize = 3_000_000;
    let (base, lines) = (ids.base() as usize, ids.bytes() / 64);
    let t = Instant::now();
    par(&|tid| {
        let mut x = 0x9E37_79B9_7F4A_7C15u64 ^ ((tid as u64 + 1) << 32);
        let mut ring = [0usize; 16];
        let mut sum = 0u64;
        for k in 0..N {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let at = base + ((x >> 11) as usize % lines) * 64;
            unsafe {
                core::arch::x86_64::_mm_prefetch(at as *const i8, core::arch::x86_64::_MM_HINT_T0);
                sum = sum.wrapping_add(std::ptr::read_volatile(ring[k & 15].max(base) as *const u64));
            }
            ring[k & 15] = at;
        }
        std::hint::black_box(sum);
    });
    let ns = t.elapsed().as_secs_f64() * 1e9 / N as f64;
    const BYTES: usize = 128 << 20;
    let t = Instant::now();
    par(&|_| {
        let p = map_anon(BYTES);
        let mut o = 0;
        while o < BYTES {
            unsafe { std::ptr::write_volatile(p.add(o), 1) };
            o += 4096;
        }
        release(p, BYTES);
    });
    let per_gb = t.elapsed().as_secs_f64() / (BYTES as f64 / (1u64 << 30) as f64);
    if std::env::var_os("SS_TIMING").is_some() {
        eprintln!("    diag: {ns:.1} ns per random read, {per_gb:.3} s per GB first touched");
    }
    ((ns.round() as u32).min(63), ((per_gb / 0.05) as u32).min(7))
}
