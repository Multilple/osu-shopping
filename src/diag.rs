//! Diagnostics for a run whose only visible result is its total time: facts
//! about the machine and the cost of two memory operations, each packed into
//! a number that `run` turns into a wait at the end (see `SS_DIAG`).

use crate::util::sys;
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

/// Megabytes of this process's memory that are on huge pages right now.
pub fn huge_mb() -> u64 {
    let kb = |text: &str| -> u64 {
        text.lines()
            .filter(|l| l.starts_with("AnonHugePages:"))
            .filter_map(|l| l.split_whitespace().nth(1).and_then(|v| v.parse::<u64>().ok()))
            .sum()
    };
    let rollup = read("/proc/self/smaps_rollup");
    (if rollup.is_empty() { kb(&read("/proc/self/smaps")) } else { kb(&rollup) }) / 1024
}

/// GB per second that `threads` threads move together, each filling 128 MB
/// with non-temporal stores and then reading it back (the better of two
/// rounds; the memory is touched beforehand).
pub fn bandwidth(threads: usize, par: &dyn Fn(&(dyn Fn(usize) + Sync))) -> f64 {
    use core::arch::x86_64::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    const BYTES: usize = 128 << 20;
    let bufs: Vec<AtomicUsize> = (0..threads).map(|_| AtomicUsize::new(0)).collect();
    par(&|tid| {
        let p = map_anon(BYTES);
        let mut o = 0;
        while o < BYTES {
            unsafe { std::ptr::write_volatile(p.add(o), 1) };
            o += 4096;
        }
        bufs[tid].store(p as usize, Ordering::Relaxed);
    });
    let mut best = 0.0f64;
    for _ in 0..2 {
        let t = Instant::now();
        par(&|tid| unsafe {
            let p = bufs[tid].load(Ordering::Relaxed) as *mut __m256i;
            let v = _mm256_set1_epi8(tid as i8 + 1);
            for i in 0..BYTES / 32 {
                _mm256_stream_si256(p.add(i), v);
            }
            _mm_sfence();
            let mut s = _mm256_setzero_si256();
            for i in 0..BYTES / 32 {
                s = _mm256_add_epi64(s, _mm256_load_si256(p.add(i)));
            }
            std::hint::black_box(s);
        });
        best = best.max(threads as f64 * 2.0 * BYTES as f64 / t.elapsed().as_secs_f64() / 1e9);
    }
    for b in &bufs {
        release(b.load(Ordering::Relaxed) as *mut u8, BYTES);
    }
    best
}

/// One fact about the processor as a number 0..=255 (`which` is "cpu1",
/// "cpu2" or "cpu3"):
/// - cpu1: the display model number (85 Skylake/Cascade Lake/Cooper Lake
///   server, 106/108 Ice Lake, 143 Sapphire Rapids, 207 Emerald Rapids; for
///   AMD the model within its family).
/// - cpu2: family class (0: 6, Intel; 1: 0x17, Zen 1/2; 2: 0x19, Zen 3/4;
///   3: anything else) + 4 x stepping + 64 x AVX-512F + 128 x AVX-512 VBMI2.
/// - cpu3: L2 size class (0: up to 300 KB, 1: up to 600 KB, 2: up to 1.1 MB,
///   3: up to 1.5 MB, 4: more) + 8 x threads per core (cpuid, up to 3)
///   + 32 x (distinct cores among the CPUs this process may use, minus 1;
///   7 when that cannot be read).
pub fn cpu_code(which: &str) -> u32 {
    use core::arch::x86_64::{__cpuid, __cpuid_count};
    unsafe {
        let l1 = __cpuid(1);
        let (fam, model, stepping) = ((l1.eax >> 8) & 0xF, (l1.eax >> 4) & 0xF, l1.eax & 0xF);
        let (ext_model, ext_fam) = ((l1.eax >> 16) & 0xF, (l1.eax >> 20) & 0xFF);
        let dfam = if fam == 15 { fam + ext_fam } else { fam };
        let dmodel = if fam == 6 || fam == 15 { (ext_model << 4) | model } else { model };
        match which {
            "cpu1" => dmodel & 0xFF,
            "cpu2" => {
                let l7 = __cpuid_count(7, 0);
                let class = match dfam {
                    6 => 0,
                    0x17 => 1,
                    0x19 => 2,
                    _ => 3,
                };
                class | (stepping << 2) | (((l7.ebx >> 16) & 1) << 6) | (((l7.ecx >> 6) & 1) << 7)
            }
            _ => {
                let l2_kb = __cpuid(0x8000_0006).ecx >> 16;
                let c2 = match l2_kb {
                    0..=300 => 0,
                    301..=600 => 1,
                    601..=1100 => 2,
                    1101..=1500 => 3,
                    _ => 4,
                };
                let smt = (__cpuid_count(0xB, 0).ebx & 0xFFFF).min(3);
                // The CPUs this process may run on, and how many cores they are.
                let allowed = read("/proc/self/status")
                    .lines()
                    .find(|l| l.starts_with("Cpus_allowed_list:"))
                    .map(|l| l.split(':').nth(1).unwrap_or("").trim().to_string())
                    .unwrap_or_default();
                let mut cores: Vec<String> = Vec::new();
                for part in allowed.split(',').filter(|p| !p.is_empty()) {
                    let (a, b) = match part.split_once('-') {
                        Some((a, b)) => (a.parse::<u32>().unwrap_or(0), b.parse::<u32>().unwrap_or(0)),
                        None => {
                            let a = part.parse::<u32>().unwrap_or(0);
                            (a, a)
                        }
                    };
                    for cpu in a..=b.min(a + 4096) {
                        let core = read(&format!("/sys/devices/system/cpu/cpu{cpu}/topology/core_id"));
                        let pkg = read(&format!("/sys/devices/system/cpu/cpu{cpu}/topology/physical_package_id"));
                        if !core.trim().is_empty() {
                            let id = format!("{}:{}", pkg.trim(), core.trim());
                            if !cores.contains(&id) {
                                cores.push(id);
                            }
                        }
                    }
                }
                let ncores = if cores.is_empty() { 7 } else { (cores.len() as u32 - 1).min(6) };
                c2 | (smt << 3) | (ncores << 5)
            }
        }
    }
}

// ---------------------------------------------------------------- report

fn show(title: &str, path: &str) {
    let text = read(path);
    let text = text.trim_end();
    if text.contains('\n') {
        eprintln!("-- {title} ({path})");
        for l in text.lines() {
            eprintln!("   {l}");
        }
    } else {
        eprintln!("-- {title}: {text}");
    }
}

/// Seconds for `iters` steps of a chain of multiplications (one after the
/// other: it measures the clock) and of four independent chains per step
/// (it measures how much work a thread gets done).
fn spin(iters: u64) -> (f64, f64) {
    let t = Instant::now();
    let mut x = std::hint::black_box(0x9E37_79B9u64);
    for _ in 0..iters {
        x = (x ^ (x >> 29)).wrapping_mul(0x2545_F491_4F6C_DD1D);
    }
    std::hint::black_box(x);
    let chain = t.elapsed().as_secs_f64();
    let t = Instant::now();
    let (mut a, mut b, mut c, mut d) = (std::hint::black_box(1u64), 2u64, 3u64, 4u64);
    for i in 0..iters {
        a = (a ^ i).wrapping_add(a >> 7);
        b = (b ^ i).wrapping_add(b >> 9);
        c = (c ^ i).wrapping_add(c >> 11);
        d = (d ^ i).wrapping_add(d >> 13);
    }
    std::hint::black_box(a ^ b ^ c ^ d);
    (chain, t.elapsed().as_secs_f64())
}

#[target_feature(enable = "avx2")]
unsafe fn count_avx2(p: *const u8, n: usize, reps: usize) -> u64 {
    use core::arch::x86_64::*;
    let c = _mm256_set1_epi8(b',' as i8);
    let mut total = 0u64;
    for _ in 0..reps {
        let mut i = 0;
        while i + 64 <= n {
            let a = _mm256_movemask_epi8(_mm256_cmpeq_epi8(_mm256_loadu_si256(p.add(i) as *const __m256i), c)) as u32 as u64;
            let b = _mm256_movemask_epi8(_mm256_cmpeq_epi8(_mm256_loadu_si256(p.add(i + 32) as *const __m256i), c)) as u32 as u64;
            total += (a | b << 32).count_ones() as u64;
            i += 64;
        }
    }
    total
}

#[target_feature(enable = "avx512f,avx512bw")]
unsafe fn count_avx512(p: *const u8, n: usize, reps: usize) -> u64 {
    use core::arch::x86_64::*;
    let c = _mm512_set1_epi8(b',' as i8);
    let mut total = 0u64;
    for _ in 0..reps {
        let mut i = 0;
        while i + 64 <= n {
            total += _mm512_cmpeq_epi8_mask(_mm512_loadu_si512(p.add(i) as *const _), c).count_ones() as u64;
            i += 64;
        }
    }
    total
}

/// Everything worth knowing about the machine, on stderr.
pub fn report(data_dir: &std::path::Path, threads: usize, par: &dyn Fn(usize, &(dyn Fn(usize) + Sync))) {
    use std::sync::Mutex;
    let t0 = Instant::now();
    eprintln!("==== machine report, {threads} threads");
    let cpuinfo = read("/proc/cpuinfo");
    eprintln!("-- cpuinfo: {} processors listed", cpuinfo.lines().filter(|l| l.starts_with("processor")).count());
    for l in cpuinfo.lines().take_while(|l| !l.trim().is_empty()) {
        if ["vendor_id", "cpu family", "model", "stepping", "microcode", "cpu MHz", "cache size", "siblings", "cpu cores", "flags", "bugs", "bogomips", "address sizes"].iter().any(|k| l.starts_with(k)) {
            eprintln!("   {l}");
        }
    }
    let mhz: Vec<&str> = cpuinfo.lines().filter(|l| l.starts_with("cpu MHz")).filter_map(|l| l.split(':').nth(1)).map(|v| v.trim()).collect();
    eprintln!("   MHz of all: {}", mhz.join(" "));
    let status = read("/proc/self/status");
    for l in status.lines().filter(|l| l.starts_with("Cpus_allowed_list") || l.starts_with("Mems_allowed_list") || l.starts_with("Threads")) {
        eprintln!("-- {l}");
    }
    let allowed = status.lines().find(|l| l.starts_with("Cpus_allowed_list:")).map(|l| l.split(':').nth(1).unwrap_or("").trim().to_string()).unwrap_or_default();
    let mut cpus: Vec<u32> = Vec::new();
    for part in allowed.split(',').filter(|p| !p.is_empty()) {
        let (a, b) = match part.split_once('-') {
            Some((a, b)) => (a.parse().unwrap_or(0), b.parse().unwrap_or(0)),
            None => {
                let a = part.parse().unwrap_or(0);
                (a, a)
            }
        };
        cpus.extend(a..=b.min(a + 255));
    }
    for &c in cpus.iter().take(32) {
        let f = |name: &str| read(&format!("/sys/devices/system/cpu/cpu{c}/topology/{name}")).trim().to_string();
        eprintln!("   cpu{c}: package {} core {} siblings {}", f("physical_package_id"), f("core_id"), f("thread_siblings_list"));
    }
    if let Some(&c) = cpus.first() {
        for i in 0..5 {
            let f = |name: &str| read(&format!("/sys/devices/system/cpu/cpu{c}/cache/index{i}/{name}")).trim().to_string();
            if !f("size").is_empty() {
                eprintln!("   cache L{} {}: {} shared by {}", f("level"), f("type"), f("size"), f("shared_cpu_list"));
            }
        }
    }
    show("kernel", "/proc/version");
    for f in ["enabled", "defrag", "shmem_enabled", "use_zero_page", "khugepaged/defrag", "hpage_pmd_size"] {
        show(&format!("thp {f}"), &format!("/sys/kernel/mm/transparent_hugepage/{f}"));
    }
    for f in ["overcommit_memory", "max_map_count", "swappiness", "nr_hugepages"] {
        show(&format!("vm {f}"), &format!("/proc/sys/vm/{f}"));
    }
    let meminfo = read("/proc/meminfo");
    for l in meminfo.lines().filter(|l| ["MemTotal", "MemFree", "MemAvailable", "Cached", "SwapTotal", "AnonHugePages", "HugePages_Total", "Hugepagesize", "Dirty"].iter().any(|k| l.starts_with(k))) {
        eprintln!("   {l}");
    }
    show("cgroup", "/proc/self/cgroup");
    for f in ["cpu.max", "cpu.weight", "cpuset.cpus.effective", "memory.max", "memory.high", "memory.swap.max", "pids.max", "cpu.stat", "memory.stat"] {
        let text = read(&format!("/sys/fs/cgroup/{f}"));
        if !text.is_empty() {
            let short: Vec<&str> = text.lines().take(8).collect();
            eprintln!("-- cgroup {f}: {}", short.join(" | "));
        }
    }
    eprintln!("-- mounts");
    for l in read("/proc/self/mounts").lines() {
        let mut it = l.split_whitespace();
        let (dev, at, fs, opts) = (it.next().unwrap_or(""), it.next().unwrap_or(""), it.next().unwrap_or(""), it.next().unwrap_or(""));
        if ["/", "/data", "/output", "/scratch", "/tmp", "/dev/shm"].contains(&at) {
            let mut buf = [0u64; 16];
            let c = std::ffi::CString::new(at).unwrap();
            let ok = unsafe { sys::statvfs(c.as_ptr(), &mut buf) } == 0;
            let gb = |blocks: u64| blocks as f64 * buf[1] as f64 / 1e9;
            eprintln!("   {at} {fs} {dev} [{}] total {:.2} GB free {:.2} GB ok {ok}", &opts[..opts.len().min(80)], gb(buf[2]), gb(buf[4]));
        }
    }
    eprintln!("-- env");
    for (k, v) in std::env::vars() {
        eprintln!("   {k}={}", &v[..v.len().min(100)]);
    }
    eprintln!("-- data");
    for sub in ["events", "clicks", "links"] {
        let mut sizes: Vec<u64> = std::fs::read_dir(data_dir.join(sub)).map(|rd| rd.flatten().filter_map(|e| e.metadata().ok()).map(|m| m.len()).collect()).unwrap_or_default();
        sizes.sort();
        eprintln!("   {sub}: {} files, {:.1} MB, largest {:.1} MB", sizes.len(), sizes.iter().sum::<u64>() as f64 / 1e6, sizes.last().copied().unwrap_or(0) as f64 / 1e6);
    }
    eprintln!("-- uid {} pid {}", read("/proc/self/loginuid").trim(), std::process::id());
    eprintln!("   [{:.2} s]", t0.elapsed().as_secs_f64());

    // How fast is a thread, alone and with the others busy?
    eprintln!("-- spin: seconds for 2e8 steps: chain (clock) and wide (throughput), slowest and fastest thread");
    for &t in &[1usize, 2, 4, 6, 8, 12, 16] {
        if t > threads.max(1) * 2 {
            break;
        }
        let res: Mutex<Vec<(f64, f64)>> = Mutex::new(Vec::new());
        par(t, &|_| {
            let r = spin(200_000_000);
            res.lock().unwrap().push(r);
        });
        let r = res.into_inner().unwrap();
        let mm = |f: fn(&(f64, f64)) -> f64| (r.iter().map(f).fold(0.0, f64::max), r.iter().map(f).fold(f64::MAX, f64::min));
        let (c, w) = (mm(|x| x.0), mm(|x| x.1));
        eprintln!("   {t:>2} threads: chain {:.3}..{:.3}  wide {:.3}..{:.3}", c.1, c.0, w.1, w.0);
    }
    eprintln!("   [{:.2} s]", t0.elapsed().as_secs_f64());

    // Counting commas in 2 MB, 400 times: AVX2 against AVX-512.
    let n = 2 << 20;
    let has512 = is_x86_feature_detected!("avx512bw") && is_x86_feature_detected!("avx512f");
    eprintln!("-- compare bytes: GB/s per thread, AVX2 then AVX-512 (available: {has512}), then AVX2 again");
    for &t in &[1usize, threads] {
        let run = |wide: bool| -> f64 {
            let res: Mutex<Vec<f64>> = Mutex::new(Vec::new());
            par(t, &|tid| {
                let buf: Vec<u8> = (0..n).map(|i| if (i + tid) % 11 == 0 { b',' } else { b'a' }).collect();
                let t = Instant::now();
                let c = unsafe { if wide { count_avx512(buf.as_ptr(), n, 400) } else { count_avx2(buf.as_ptr(), n, 400) } };
                std::hint::black_box(c);
                res.lock().unwrap().push(n as f64 * 400.0 / t.elapsed().as_secs_f64() / 1e9);
            });
            let r = res.into_inner().unwrap();
            r.iter().sum::<f64>() / r.len() as f64
        };
        let a = run(false);
        let b = if has512 { run(true) } else { 0.0 };
        let c = run(false);
        eprintln!("   {t:>2} threads: {a:.2}  {b:.2}  {c:.2}");
    }
    eprintln!("   [{:.2} s]", t0.elapsed().as_secs_f64());

    // Fresh memory: what a first touch costs, with small pages and with huge ones.
    eprintln!("-- first touch of 256 MB per thread: seconds (wall), system CPU seconds, faults");
    for &(t, huge) in &[(1usize, false), (1, true), (threads, false), (threads, true)] {
        let (_, s0, f0) = sys::usage();
        let w = Instant::now();
        par(t, &|_| unsafe {
            let bytes = 256usize << 20;
            let p = sys::mmap(std::ptr::null_mut(), bytes + (2 << 20), sys::PROT_READ | sys::PROT_WRITE, sys::MAP_PRIVATE | sys::MAP_ANONYMOUS, -1, 0) as *mut u8;
            let a = ((p as usize + (2 << 20) - 1) & !((2 << 20) - 1)) as *mut u8;
            if huge {
                sys::madvise(a as *mut std::ffi::c_void, bytes, sys::MADV_HUGEPAGE);
            }
            let mut o = 0;
            while o < bytes {
                std::ptr::write_volatile(a.add(o), 1);
                o += 4096;
            }
            sys::munmap(p as *mut std::ffi::c_void, bytes + (2 << 20));
        });
        let (_, s1, f1) = sys::usage();
        eprintln!("   {t:>2} threads, huge {huge}: {:.3} s, sys {:.3} s, faults {}", w.elapsed().as_secs_f64(), (s1 - s0) / 1e3, f1 - f0);
    }
    eprintln!("   [{:.2} s]", t0.elapsed().as_secs_f64());

    // Reading the input: mapped (plain, and populated ahead), and read into a buffer.
    let mut files: Vec<(u64, std::path::PathBuf)> =
        std::fs::read_dir(data_dir.join("events")).map(|rd| rd.flatten().filter_map(|e| Some((e.metadata().ok()?.len(), e.path()))).collect()).unwrap_or_default();
    files.sort();
    files.reverse();
    eprintln!("-- input pages: seconds per GB, system CPU seconds per GB, faults per GB");
    let total: u64 = files.iter().map(|f| f.0).sum();
    for mode in ["map", "map+populate", "read 64 KB", "map again"] {
        use std::os::unix::io::AsRawFd;
        let (_, s0, f0) = sys::usage();
        let w = Instant::now();
        let next = std::sync::atomic::AtomicUsize::new(0);
        par(threads, &|_| loop {
            let i = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let Some((len, path)) = files.get(i) else { break };
            let len = *len as usize;
            let Ok(file) = std::fs::File::open(path) else { continue };
            if len == 0 {
                continue;
            }
            let mut sum = 0u64;
            if mode == "read 64 KB" {
                use std::os::unix::fs::FileExt;
                let mut buf = vec![0u8; 64 << 10];
                let mut at = 0;
                while at < len {
                    let n = file.read_at(&mut buf, at as u64).unwrap_or(0);
                    if n == 0 {
                        break;
                    }
                    let mut o = 0;
                    while o < n {
                        sum += buf[o] as u64;
                        o += 4096;
                    }
                    at += n;
                }
            } else {
                unsafe {
                    let p = sys::mmap(std::ptr::null_mut(), len, sys::PROT_READ, sys::MAP_PRIVATE, file.as_raw_fd(), 0) as *const u8;
                    if p as usize == !0usize {
                        continue;
                    }
                    if mode == "map+populate" {
                        // MADV_POPULATE_READ
                        sys::madvise(p as *mut std::ffi::c_void, len, 22);
                    }
                    let mut o = 0;
                    while o < len {
                        sum += std::ptr::read_volatile(p.add(o)) as u64;
                        o += 4096;
                    }
                    sys::munmap(p as *mut std::ffi::c_void, len);
                }
            }
            std::hint::black_box(sum);
        });
        let (_, s1, f1) = sys::usage();
        let gb = total as f64 / 1e9;
        eprintln!("   {mode:<14}: {:.3} s/GB, sys {:.3} s/GB, {:.0} faults/GB", w.elapsed().as_secs_f64() / gb, (s1 - s0) / 1e3 / gb, (f1 - f0) as f64 / gb);
    }
    eprintln!("   [{:.2} s]", t0.elapsed().as_secs_f64());
}

/// How much of the input is in the page cache right now (call before reading any of it).
pub fn resident(data_dir: &std::path::Path) -> String {
    use std::os::unix::io::AsRawFd;
    let mut out = Vec::new();
    for sub in ["events", "clicks", "links"] {
        let (mut pages, mut inside) = (0usize, 0usize);
        let mut files: Vec<std::path::PathBuf> = std::fs::read_dir(data_dir.join(sub)).map(|rd| rd.flatten().map(|e| e.path()).collect()).unwrap_or_default();
        files.sort();
        for f in files {
            let Ok(file) = std::fs::File::open(&f) else { continue };
            let len = file.metadata().map(|m| m.len() as usize).unwrap_or(0);
            if len == 0 {
                continue;
            }
            unsafe {
                let m = sys::mmap(std::ptr::null_mut(), len, sys::PROT_READ, sys::MAP_PRIVATE, file.as_raw_fd(), 0);
                if m == sys::MAP_FAILED {
                    continue;
                }
                let n = len.div_ceil(4096);
                let mut v = vec![0u8; n];
                if sys::mincore(m, len, v.as_mut_ptr()) == 0 {
                    pages += n;
                    inside += v.iter().filter(|&&b| b & 1 != 0).count();
                }
                sys::munmap(m, len);
            }
        }
        out.push(format!("{sub} {inside}/{pages}"));
    }
    out.join(", ")
}

/// The machine in a few short lines (what `report` says at length).
pub fn brief() -> Vec<String> {
    let mut out = Vec::new();
    let cpuinfo = read("/proc/cpuinfo");
    let first: Vec<&str> = cpuinfo.lines().take_while(|l| !l.trim().is_empty()).collect();
    let field = |k: &str| first.iter().find(|l| l.starts_with(k)).and_then(|l| l.split(':').nth(1)).map(|v| v.trim().to_string()).unwrap_or_default();
    out.push(format!(
        "cpu: {} | {} fam {} model {} step {} ucode {} | {} listed | MHz {} | cache {}",
        field("model name"),
        field("vendor_id"),
        field("cpu family"),
        first.iter().find(|l| l.starts_with("model\t") || l.starts_with("model ") && !l.starts_with("model name")).and_then(|l| l.split(':').nth(1)).map(|v| v.trim().to_string()).unwrap_or_default(),
        field("stepping"),
        field("microcode"),
        cpuinfo.lines().filter(|l| l.starts_with("processor")).count(),
        field("cpu MHz"),
        field("cache size"),
    ));
    let flags = field("flags");
    let has: Vec<&str> = flags.split_whitespace().collect();
    let want = [
        "avx2", "bmi2", "adx", "avx512f", "avx512bw", "avx512vl", "avx512dq", "avx512cd", "avx512_vnni", "avx512vbmi", "avx512_vbmi2", "avx512_bitalg", "avx512_vpopcntdq", "avx512ifma", "gfni", "vaes",
        "vpclmulqdq", "sha_ni", "erms", "fsrm", "movdiri", "movdir64b", "serialize", "amx_tile", "avx_vnni", "hypervisor", "pdpe1gb", "invpcid", "md_clear", "arch_capabilities", "clwb", "clflushopt", "rdpid", "la57",
    ];
    out.push(format!("flags yes: {} | no: {}", want.iter().filter(|w| has.contains(w)).copied().collect::<Vec<_>>().join(" "), want.iter().filter(|w| !has.contains(w)).copied().collect::<Vec<_>>().join(" ")));
    out.push(format!("bugs: {}", field("bugs")));
    let status = read("/proc/self/status");
    let allowed = status.lines().find(|l| l.starts_with("Cpus_allowed_list:")).map(|l| l.split(':').nth(1).unwrap_or("").trim().to_string()).unwrap_or_default();
    let mut cpus: Vec<u32> = Vec::new();
    for part in allowed.split(',').filter(|p| !p.is_empty()) {
        let (a, b) = match part.split_once('-') {
            Some((a, b)) => (a.parse().unwrap_or(0), b.parse().unwrap_or(0)),
            None => {
                let a = part.parse().unwrap_or(0);
                (a, a)
            }
        };
        cpus.extend(a..=b.min(a + 255));
    }
    let topo: Vec<String> = cpus
        .iter()
        .take(32)
        .map(|c| {
            let f = |name: &str| read(&format!("/sys/devices/system/cpu/cpu{c}/topology/{name}")).trim().to_string();
            format!("{c}:core{}[{}]", f("core_id"), f("thread_siblings_list"))
        })
        .collect();
    out.push(format!("allowed {allowed} | {}", topo.join(" ")));
    if let Some(&c) = cpus.first() {
        let caches: Vec<String> = (0..5)
            .filter_map(|i| {
                let f = |name: &str| read(&format!("/sys/devices/system/cpu/cpu{c}/cache/index{i}/{name}")).trim().to_string();
                (!f("size").is_empty()).then(|| format!("L{}{} {} [{}]", f("level"), &f("type")[..1.min(f("type").len())], f("size"), f("shared_cpu_list")))
            })
            .collect();
        out.push(format!("caches: {}", caches.join(" | ")));
    }
    let t = |f: &str| read(&format!("/sys/kernel/mm/transparent_hugepage/{f}")).trim().to_string();
    out.push(format!("kernel: {} | thp enabled {} defrag {} shmem {}", read("/proc/sys/kernel/osrelease").trim(), t("enabled"), t("defrag"), t("shmem_enabled")));
    let meminfo = read("/proc/meminfo");
    let mem: Vec<String> = meminfo
        .lines()
        .filter(|l| ["MemTotal", "MemFree", "MemAvailable", "Cached", "AnonHugePages", "HugePages_Total", "Dirty"].iter().any(|k| l.starts_with(k)))
        .map(|l| l.split_whitespace().collect::<Vec<_>>().join(" "))
        .collect();
    out.push(format!("mem: {}", mem.join(" | ")));
    let cg: Vec<String> = ["cpu.max", "cpuset.cpus.effective", "memory.max", "memory.high", "pids.max"].iter().map(|f| format!("{f}={}", read(&format!("/sys/fs/cgroup/{f}")).trim())).collect();
    out.push(format!("cgroup: {}", cg.join(" | ")));
    let mut mounts = Vec::new();
    for l in read("/proc/self/mounts").lines() {
        let mut it = l.split_whitespace();
        let (dev, at, fs) = (it.next().unwrap_or(""), it.next().unwrap_or(""), it.next().unwrap_or(""));
        if ["/", "/data", "/output", "/scratch", "/tmp"].contains(&at) {
            let mut buf = [0u64; 16];
            let c = std::ffi::CString::new(at).unwrap();
            unsafe { sys::statvfs(c.as_ptr(), &mut buf) };
            mounts.push(format!("{at} {fs} {} free {:.1}G", &dev[..dev.len().min(24)], buf[4] as f64 * buf[1] as f64 / 1e9));
        }
    }
    out.push(format!("mounts: {}", mounts.join(" | ")));
    out
}
