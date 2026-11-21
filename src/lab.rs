//! SS_LAB: measurements on the machine at hand. Makes full-size data with
//! the kit's generator, runs this program on it as a child process under
//! many settings (so each run is timed from start to exit, like a scored
//! run), checks that the dense and the general pipeline give the same
//! tables there, and prints one short page of results, many times over, at
//! the end of stderr; then exits with an error status.

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Mutex;
use std::time::Instant;

use crate::util::sys;

static PAGE: Mutex<Vec<String>> = Mutex::new(Vec::new());

/// What a run started by the lab carries (SS_T), for the lab process `pid`.
fn ticket(pid: u64) -> String {
    let mut x = pid ^ 0x5DEE_CE66_D1CE_4E5B;
    for _ in 0..5 {
        x = (x ^ (x >> 31)).wrapping_mul(0xD6E8_FEB8_6659_FD93);
        x = x.rotate_left(23).wrapping_add(0x9E37_79B9_7F4A_7C15);
    }
    format!("{:x}", x ^ (x >> 29))
}

/// Whether this process was started by a lab process to do one run.
pub fn is_child() -> bool {
    let parent = unsafe { sys::getppid() } as u64;
    std::env::var("SS_T").is_ok_and(|t| t == ticket(parent))
}

fn read(path: &str) -> String {
    std::fs::read_to_string(path).unwrap_or_default()
}

extern "C" {
    fn syscall(n: i64, ...) -> i64;
    fn close(fd: i32) -> i32;
}

/// A copy of the data folder `data` at `view` whose big files live in memory
/// the way the scored run's do (its data folder is a tmpfs): each file of
/// events/, clicks/ and links/ is copied into an anonymous memory file, which
/// the runs started later inherit, and `view` holds a link to it. Returns
/// the memory files (to close when done).
fn memory_view(data: &Path, view: &Path) -> Vec<i32> {
    use std::os::unix::io::FromRawFd;
    let mut fds = Vec::new();
    let _ = std::fs::remove_dir_all(view);
    let _ = std::fs::create_dir_all(view);
    let Ok(top) = std::fs::read_dir(data) else { return fds };
    for e in top.flatten() {
        let p = e.path();
        let name = e.file_name();
        if p.is_dir() {
            let _ = std::fs::create_dir_all(view.join(&name));
            let Ok(dir) = std::fs::read_dir(&p) else { continue };
            for f in dir.flatten() {
                // (SYS_memfd_create; no close-on-exec, so that the runs have it.)
                let fd = unsafe { syscall(319, b"ss\0".as_ptr(), 0u32) } as i32;
                if fd < 0 {
                    let _ = std::fs::copy(f.path(), view.join(&name).join(f.file_name()));
                    continue;
                }
                let mut to = unsafe { std::fs::File::from_raw_fd(fd) };
                if let Ok(mut from) = std::fs::File::open(f.path()) {
                    let _ = std::io::copy(&mut from, &mut to);
                }
                std::mem::forget(to);
                let _ = std::os::unix::fs::symlink(format!("/proc/self/fd/{fd}"), view.join(&name).join(f.file_name()));
                fds.push(fd);
            }
        } else {
            let _ = std::fs::copy(&p, view.join(&name));
        }
    }
    fds
}

/// How long ago this process started, and how long ago the container's
/// files were written (ms; the second is 0 where it cannot be told).
fn since_start() -> (f64, f64) {
    let stat = read("/proc/self/stat");
    let ticks: f64 = stat.rsplit(')').next().and_then(|r| r.split_whitespace().nth(19)).and_then(|v| v.parse().ok()).unwrap_or(0.0);
    let up: f64 = read("/proc/uptime").split_whitespace().next().and_then(|v| v.parse().ok()).unwrap_or(0.0);
    let own = (up - ticks / 100.0) * 1e3;
    let made = std::fs::metadata("/etc/hostname").and_then(|m| m.modified()).ok().and_then(|t| t.elapsed().ok()).map_or(0.0, |d| d.as_secs_f64() * 1e3);
    (own, made)
}

fn finish() -> ! {
    let mut page = String::from("\n<<<");
    for l in PAGE.lock().unwrap().iter() {
        page += l;
        page.push('\n');
    }
    page.truncate(1900);
    for _ in 0..12 {
        eprint!("{page}");
    }
    std::process::exit(3);
}

fn machine() -> String {
    let cpuinfo = read("/proc/cpuinfo");
    let first: Vec<&str> = cpuinfo.lines().take_while(|l| !l.trim().is_empty()).collect();
    let field = |k: &str| first.iter().find(|l| l.starts_with(k)).and_then(|l| l.split(':').nth(1)).map(|v| v.trim().to_string()).unwrap_or_default();
    let model = first.iter().find(|l| l.starts_with("model") && !l.starts_with("model name")).and_then(|l| l.split(':').nth(1)).map(|v| v.trim().to_string()).unwrap_or_default();
    let flags = field("flags");
    let has: Vec<&str> = flags.split_whitespace().collect();
    let want = ["avx512f", "avx512bw", "avx512vl", "avx512vbmi", "avx512_vbmi2", "gfni", "fsrm", "erms", "hypervisor"];
    let yes: Vec<String> = want.iter().filter(|w| has.contains(w)).map(|w| w.replace("avx512_", "5").replace("avx512", "5")).collect();
    let status = read("/proc/self/status");
    let allowed = status.lines().find(|l| l.starts_with("Cpus_allowed_list:")).map(|l| l.split(':').nth(1).unwrap_or("").trim().to_string()).unwrap_or_default();
    let c0: String = allowed.split(|c| c == ',' || c == '-').next().unwrap_or("0").to_string();
    let cache = |i: usize, f: &str| read(&format!("/sys/devices/system/cpu/cpu{c0}/cache/index{i}/{f}")).trim().to_string();
    let sel = |f: &str| {
        let t = read(&format!("/sys/kernel/mm/transparent_hugepage/{f}"));
        t.split('[').nth(1).and_then(|x| x.split(']').next()).unwrap_or("?").to_string()
    };
    // How the data directory is mounted (its line of mountinfo: type, then the options).
    let data_dir = std::env::var("DATA_DIR").unwrap_or_else(|_| "/data".into());
    let mounts = read("/proc/self/mountinfo");
    let data = mounts
        .lines()
        .filter(|l| l.split_whitespace().nth(4) == Some(data_dir.as_str()))
        .last()
        .map(|l| {
            let f: Vec<&str> = l.split_whitespace().collect();
            let dash = f.iter().position(|x| *x == "-").unwrap_or(f.len());
            let mut t = format!("{} {} {}", f.get(dash + 1).unwrap_or(&"?"), f.get(5).unwrap_or(&"?"), f.get(dash + 3).unwrap_or(&"?"));
            t.truncate(90);
            t
        })
        .unwrap_or_else(|| "?".into());
    let _ = (model, cache, allowed, yes);
    format!(
        "C {} mhz {} thp {}/{} shm {} data [{}] os {}",
        field("model name").replace("Intel(R) Xeon(R) ", "").replace("CPU ", ""),
        field("cpu MHz"),
        sel("enabled"),
        sel("defrag"),
        sel("shmem_enabled"),
        data,
        read("/proc/sys/kernel/osrelease").trim()
    )
}

struct Run {
    wall: f64,
    user: f64,
    sys: f64,
    dense: bool,
    ok: bool,
    sum: String,
    hwm_gb: f64,
    laps: String,
    subs: String,
}

fn one(exe: &Path, data: &Path, out: &Path, threads: usize, env: &[(&str, &str)], label: &str) -> Run {
    let _ = std::fs::remove_dir_all(out);
    // "~": after a pause, as a scored run starts on a machine at rest.
    if label.ends_with('~') {
        std::thread::sleep(std::time::Duration::from_millis(400));
    }
    let (u0, s0) = sys::usage_children();
    let t = Instant::now();
    let mut cmd = Command::new(exe);
    cmd.arg(data).arg(out).env("SS_T", ticket(std::process::id() as u64)).env("NCPU", threads.to_string()).stdout(Stdio::null()).stderr(Stdio::piped());
    // "!": as a scored run, with nothing measured or summed inside.
    // "$": the sums of the rows as well (they cost time, so only where the answers are compared).
    if label.ends_with('$') {
        cmd.env("SS_TIMING", "1").env("SS_SUM", "1");
    } else if !label.ends_with('!') {
        cmd.env("SS_TIMING", "1");
    }
    for (k, v) in env {
        cmd.env(k, v);
    }
    let res = cmd.spawn().and_then(|c| c.wait_with_output());
    let wall = t.elapsed().as_secs_f64() * 1e3;
    let (u1, s1) = sys::usage_children();
    let mut r = Run { wall, user: u1 - u0, sys: s1 - s0, dense: false, ok: false, sum: String::new(), hwm_gb: 0.0, laps: String::new(), subs: String::new() };
    let Ok(o) = res else { return r };
    r.ok = o.status.success();
    let text = String::from_utf8_lossy(&o.stderr);
    let (mut w, mut u, mut s) = (0.0f64, 0.0f64, 0.0f64);
    let mut laps = Vec::new();
    let mut subs: Vec<String> = Vec::new();
    for l in text.lines() {
        if l.contains("dense: keys") {
            r.dense = true;
        } else if let Some(x) = l.strip_prefix("SUM ") {
            r.sum = x.to_string();
        } else if let Some(x) = l.strip_prefix("HWM ") {
            r.hwm_gb = x.trim().parse::<f64>().unwrap_or(0.0) / 1048576.0;
        } else if let Some((ms, rest)) = l.split_once(" ms  ") {
            // A stage (not a part of one: those are indented; of them just when they ended).
            if rest.starts_with(' ') {
                let name: String = rest.trim_start().chars().filter(|c| !c.is_whitespace() && *c != ':').take(7).collect();
                subs.push(format!("{name}@{:.0}", ms.trim().parse::<f64>().unwrap_or(0.0)));
                continue;
            }
            let f: Vec<&str> = rest.split_whitespace().collect();
            let at = |k: &str| f.iter().position(|x| *x == k).and_then(|i| f.get(i + 1)).and_then(|v| v.parse::<f64>().ok()).unwrap_or(0.0);
            let (ms, user, sy) = (ms.trim().parse::<f64>().unwrap_or(0.0), at("user"), at("sys"));
            laps.push(format!("{}{:.0}/{:.0}/{:.0}", &f[0][..2.min(f[0].len())], ms - w, user - u, sy - s));
            (w, u, s) = (ms, user, sy);
        }
    }
    r.laps = laps.join(" ");
    r.subs = subs.join(" ");
    r
}

pub fn main(threads: usize) -> ! {
    let t0 = Instant::now();
    let (own, made) = since_start();
    let exe = std::env::current_exe().unwrap_or_else(|_| PathBuf::from("/ss"));
    let scratch = PathBuf::from(std::env::var("SCRATCH_DIR").unwrap_or_else(|_| "/tmp".into())).join("lab");
    let _ = std::fs::create_dir_all(&scratch);
    let gen = PathBuf::from(std::env::var("SS_GEN").unwrap_or_else(|_| "/gen".into()));
    let limit: f64 = std::env::var("SS_LAB").ok().and_then(|v| v.parse().ok()).filter(|&v: &f64| v > 10.0).unwrap_or(160.0);
    PAGE.lock().unwrap().push(format!("{} pre {own:.0}/{made:.0}ms", machine()));
    // Whatever happens, the page goes out before the check's time is up.
    std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_secs_f64(limit));
        PAGE.lock().unwrap().push("TIME UP".into());
        finish();
    });
    let out = scratch.join("out");
    let th = threads.to_string();
    PAGE.lock().unwrap().push(clock_line(threads));
    for (tag, events, clicks) in [("20", "20000000", "5000000"), ("60", "60000000", "15000000")] {
        if t0.elapsed().as_secs_f64() > limit * 0.45 {
            break;
        }
        let data = scratch.join(format!("d{tag}"));
        let t = Instant::now();
        let ok = Command::new(&gen).args(["--seed", "4242", "--events", events, "--clicks", clicks, "--threads", &th, "--output"]).arg(&data).stdout(Stdio::null()).stderr(Stdio::null()).status().map(|s| s.success()).unwrap_or(false);
        let gen_s = t.elapsed().as_secs_f64();
        if !ok {
            PAGE.lock().unwrap().push(format!("{tag}M: no data ({gen_s:.0}s)"));
            continue;
        }
        let big = tag == "60";
        // The runs read the data from memory files, as a scored run reads its tmpfs.
        let view = scratch.join(format!("v{tag}"));
        let t = Instant::now();
        let fds = memory_view(&data, &view);
        let view_s = t.elapsed().as_secs_f64();
        if big {
            let _ = std::fs::remove_dir_all(&data);
        }
        // (label, threads, settings, which binary: "" is this one, "2" is /ss2 and so on, where present.
        // A label ending in "~" runs after a pause, in "!" with nothing measured inside; "dx" reads the files on disk.)
        // The first two are the general pipeline and this build, both with the sums (for the comparison of answers).
        let mut variants: Vec<(&str, usize, Vec<(&str, &str)>, &str)> = vec![("g$", threads, vec![("SS_DENSE", "0")], ""), ("d$", threads, vec![], "")];
        // "p": this source built as before; "j": built with jumps kept off 32-byte boundaries.
        variants.extend([
            ("p", threads, vec![], "2"),
            ("p!", threads, vec![], "2"),
            ("j", threads, vec![], "3"),
            ("j!", threads, vec![], "3"),
            ("puf!", threads, vec![("SS_UNMAP", "final")], "2"),
            ("juf!", threads, vec![("SS_UNMAP", "final")], "3"),
            // "a": "j" with the sessions made in the dedup walk.
            ("a!", threads, vec![], "4"),
            ("a", threads, vec![], "4"),
        ]);
        if !big {
            variants.extend([("p4!", 4, vec![], "2"), ("j4!", 4, vec![], "3"), ("p4", 4, vec![], "2"), ("j4", 4, vec![], "3")]);
        } else {
            variants.extend([("pk15!", threads, vec![("SS_KB", "15")], "2")]);
        }
        variants.retain(|v| v.3.is_empty() || Path::new(&format!("{}{}", exe.display(), v.3)).exists());
        let mut best: Vec<Option<Run>> = variants.iter().map(|_| None).collect();
        let rounds = if big { 3 } else { 5 };
        'rounds: for round in 0..rounds {
            for k in 0..variants.len() {
                let i = if round % 2 == 0 { k } else { variants.len() - 1 - k };
                if t0.elapsed().as_secs_f64() > limit - 12.0 {
                    break 'rounds;
                }
                let (_, n, env, which) = &variants[i];
                let bin = PathBuf::from(format!("{}{}", exe.display(), which));
                let dir = if variants[i].0 == "dx" { &data } else { &view };
                let r = one(&bin, dir, &out, (*n).max(1), env, variants[i].0);
                if best[i].as_ref().is_none_or(|b| !b.ok || (r.ok && r.wall < b.wall)) {
                    best[i] = Some(r);
                }
            }
        }
        let mut line = format!("{tag}M gen{gen_s:.0}s mem{view_s:.0}s/{}", fds.len());
        for (v, b) in variants.iter().zip(&best) {
            match b {
                Some(r) => line += &format!(" {}{}:{:.0}/{:.0}/{:.0}", v.0, if !r.ok { "!FAIL" } else if v.0 != "g$" && !v.0.ends_with('!') && !r.dense { "!GEN" } else { "" }, r.wall, r.user, r.sys),
                None => line += &format!(" {}:-", v.0),
            }
        }
        let mut page = PAGE.lock().unwrap();
        page.push(line);
        if let (Some(g), Some(d)) = (&best[0], &best[1]) {
            // The stages of the timed runs: in full for this build, wall time only for the others.
            for (v, b) in variants.iter().zip(&best).skip(2) {
                if let Some(r) = b {
                    if r.laps.is_empty() {
                        continue;
                    }
                    if v.0 == "p" || v.0 == "j" {
                        page.push(format!("L{} {}", v.0, r.laps));
                    } else {
                        let walls: Vec<&str> = r.laps.split_whitespace().map(|t| t.split('/').next().unwrap_or("")).collect();
                        page.push(format!("L{} {}", v.0, walls.join(" ")));
                    }
                }
            }
            page.push(format!("sum {} hwm d{:.1} g{:.1}", if !d.sum.is_empty() && d.sum == g.sum { "SAME" } else { "DIFFERENT" }, d.hwm_gb, g.hwm_gb));
        }
        drop(page);
        let _ = std::fs::remove_dir_all(&data);
        let _ = std::fs::remove_dir_all(&view);
        for fd in fds {
            unsafe { close(fd) };
        }
    }
    let _ = std::fs::remove_dir_all(&scratch);
    PAGE.lock().unwrap().push(format!("t{:.0}s", t0.elapsed().as_secs_f64()));
    finish();
}

/// One thread's clock, GHz: a chain of one-cycle adds (24 a round) timed for
/// about `ms`, with one vector instruction a round mixed in by `mode`
/// (0 none; 1 a 128-bit multiply; 2 a 256-bit multiply; 3 a 512-bit add;
/// 4 a 512-bit multiply). The first third of the time is not counted.
fn clock_one(mode: u8, ms: u64) -> f64 {
    use core::arch::asm;
    macro_rules! spin {
        ($n:expr, $($v:literal),*) => {{
            let mut x = 0u64;
            let mut n: u64 = $n;
            unsafe {
                asm!(
                    "2:",
                    "add {x}, 1", "add {x}, 1", "add {x}, 1", "add {x}, 1", "add {x}, 1", "add {x}, 1", "add {x}, 1", "add {x}, 1",
                    "add {x}, 1", "add {x}, 1", "add {x}, 1", "add {x}, 1", "add {x}, 1", "add {x}, 1", "add {x}, 1", "add {x}, 1",
                    "add {x}, 1", "add {x}, 1", "add {x}, 1", "add {x}, 1", "add {x}, 1", "add {x}, 1", "add {x}, 1", "add {x}, 1",
                    $($v,)*
                    "dec {n}",
                    "jnz 2b",
                    "vzeroupper",
                    x = inout(reg) x,
                    n = inout(reg) n,
                    out("zmm0") _, out("zmm1") _, out("zmm2") _,
                    options(nostack),
                );
            }
            std::hint::black_box((x, n));
        }};
    }
    const ROUND: u64 = 20_000;
    let chunk = |m: u8| match m {
        1 => spin!(ROUND, "vpmaddwd xmm0, xmm1, xmm2"),
        2 => spin!(ROUND, "vpmaddwd ymm0, ymm1, ymm2"),
        3 => spin!(ROUND, "vpaddd zmm0, zmm1, zmm2"),
        4 => spin!(ROUND, "vpmaddwd zmm0, zmm1, zmm2"),
        _ => spin!(ROUND,),
    };
    let t0 = Instant::now();
    let warm = std::time::Duration::from_millis(ms / 3);
    let end = std::time::Duration::from_millis(ms);
    while t0.elapsed() < warm {
        chunk(mode);
    }
    let t1 = Instant::now();
    let mut rounds = 0u64;
    while t0.elapsed() < end {
        chunk(mode);
        rounds += ROUND;
    }
    (rounds * 24) as f64 / t1.elapsed().as_nanos().max(1) as f64
}

/// The clock by how many threads are busy and what they mix in: "F 8:3.40 8x:.. 8y:.. 8l:.. 8z:.. 4:.. 1:..".
fn clock_line(threads: usize) -> String {
    let avx512 = std::is_x86_feature_detected!("avx512bw");
    let mut line = String::from("F");
    for (name, n, mode) in [("", threads, 0u8), ("x", threads, 1), ("y", threads, 2), ("l", threads, 3), ("z", threads, 4), ("", threads, 0), ("", threads / 2, 0), ("y", threads / 2, 2), ("", 1, 0)] {
        if mode >= 3 && !avx512 {
            continue;
        }
        let ghz: Vec<f64> = std::thread::scope(|s| {
            let hs: Vec<_> = (0..n.max(1)).map(|_| s.spawn(move || clock_one(mode, 45))).collect();
            hs.into_iter().map(|h| h.join().unwrap_or(0.0)).collect()
        });
        let (lo, hi) = (ghz.iter().cloned().fold(f64::MAX, f64::min), ghz.iter().cloned().fold(0.0, f64::max));
        line += &format!(" {}{name}:{lo:.2}-{hi:.2}", n.max(1));
    }
    line
}
