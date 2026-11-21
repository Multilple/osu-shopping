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
    format!(
        "C {} m{}s{} +{} A {} sib {} L2 {} L3 {} thp {}/{} os {}",
        field("model name").replace("Intel(R) Xeon(R) ", "").replace("CPU ", ""),
        model,
        field("stepping"),
        yes.join(","),
        allowed,
        read(&format!("/sys/devices/system/cpu/cpu{c0}/topology/thread_siblings_list")).trim(),
        cache(2, "size"),
        cache(3, "size"),
        sel("enabled"),
        sel("defrag"),
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
}

fn one(exe: &Path, data: &Path, out: &Path, threads: usize, env: &[(&str, &str)]) -> Run {
    let _ = std::fs::remove_dir_all(out);
    let (u0, s0) = sys::usage_children();
    let t = Instant::now();
    let mut cmd = Command::new(exe);
    cmd.arg(data).arg(out).env("SS_T", ticket(std::process::id() as u64)).env("NCPU", threads.to_string()).env("SS_TIMING", "1").env("SS_SUM", "1").stdout(Stdio::null()).stderr(Stdio::piped());
    for (k, v) in env {
        cmd.env(k, v);
    }
    let res = cmd.spawn().and_then(|c| c.wait_with_output());
    let wall = t.elapsed().as_secs_f64() * 1e3;
    let (u1, s1) = sys::usage_children();
    let mut r = Run { wall, user: u1 - u0, sys: s1 - s0, dense: false, ok: false, sum: String::new(), hwm_gb: 0.0, laps: String::new() };
    let Ok(o) = res else { return r };
    r.ok = o.status.success();
    let text = String::from_utf8_lossy(&o.stderr);
    let (mut w, mut u, mut s) = (0.0f64, 0.0f64, 0.0f64);
    let mut laps = Vec::new();
    for l in text.lines() {
        if l.contains("dense: keys") {
            r.dense = true;
        } else if let Some(x) = l.strip_prefix("SUM ") {
            r.sum = x.to_string();
        } else if let Some(x) = l.strip_prefix("HWM ") {
            r.hwm_gb = x.trim().parse::<f64>().unwrap_or(0.0) / 1048576.0;
        } else if let Some((ms, rest)) = l.split_once(" ms  ") {
            // A stage (not a part of one: those are indented).
            if rest.starts_with(' ') {
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
    r
}

pub fn main(threads: usize) -> ! {
    let t0 = Instant::now();
    let exe = std::env::current_exe().unwrap_or_else(|_| PathBuf::from("/ss"));
    let scratch = PathBuf::from(std::env::var("SCRATCH_DIR").unwrap_or_else(|_| "/tmp".into())).join("lab");
    let _ = std::fs::create_dir_all(&scratch);
    let gen = PathBuf::from(std::env::var("SS_GEN").unwrap_or_else(|_| "/gen".into()));
    let limit: f64 = std::env::var("SS_LAB").ok().and_then(|v| v.parse().ok()).filter(|&v: &f64| v > 10.0).unwrap_or(160.0);
    PAGE.lock().unwrap().push(machine());
    // Whatever happens, the page goes out before the check's time is up.
    std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_secs_f64(limit));
        PAGE.lock().unwrap().push("TIME UP".into());
        finish();
    });
    let out = scratch.join("out");
    let th = threads.to_string();
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
        let mut variants: Vec<(&str, usize, Vec<(&str, &str)>)> = vec![("g", threads, vec![("SS_DENSE", "0")]), ("d", threads, vec![])];
        if !big {
            variants.extend([
                ("d6", threads * 3 / 4, vec![]),
                ("d4", threads / 2, vec![]),
                ("d12", threads * 3 / 2, vec![]),
                ("d16", threads * 2, vec![]),
                ("a0", threads, vec![("SS_AHEAD", "0")]),
                ("a1k", threads, vec![("SS_AHEAD", "1024")]),
                ("a16k", threads, vec![("SS_AHEAD", "16384")]),
                ("p1", threads, vec![("SS_POP", "1")]),
                ("pc.25", threads, vec![("SS_PIECE", "262144")]),
                ("pc1", threads, vec![("SS_PIECE", "1048576")]),
                ("pc16", threads, vec![("SS_PIECE", "16777216")]),
                ("nh", threads, vec![("SS_NOHUGE", "1")]),
                ("kb13", threads, vec![("SS_KB", "13")]),
                ("kb14", threads, vec![("SS_KB", "14")]),
                ("kb16", threads, vec![("SS_KB", "16")]),
                ("kb17", threads, vec![("SS_KB", "17")]),
                ("ku10", threads, vec![("SS_KU", "10")]),
                ("ku12", threads, vec![("SS_KU", "12")]),
                ("m0", threads, vec![("SS_MMAP", "0")]),
            ]);
        } else {
            variants.extend([
                ("d16", threads * 2, vec![]),
                ("kb14", threads, vec![("SS_KB", "14")]),
                ("kb16", threads, vec![("SS_KB", "16")]),
                ("kb17", threads, vec![("SS_KB", "17")]),
                ("ku12", threads, vec![("SS_KU", "12")]),
                ("ku13", threads, vec![("SS_KU", "13")]),
            ]);
        }
        let mut best: Vec<Option<Run>> = variants.iter().map(|_| None).collect();
        let rounds = if big { 2 } else { 3 };
        'rounds: for round in 0..rounds {
            for k in 0..variants.len() {
                let i = if round % 2 == 0 { k } else { variants.len() - 1 - k };
                if t0.elapsed().as_secs_f64() > limit - 12.0 {
                    break 'rounds;
                }
                let (_, n, env) = &variants[i];
                let r = one(&exe, &data, &out, (*n).max(1), env);
                if best[i].as_ref().is_none_or(|b| !b.ok || (r.ok && r.wall < b.wall)) {
                    best[i] = Some(r);
                }
            }
        }
        let mut line = format!("{tag}M gen{gen_s:.0}s");
        for (v, b) in variants.iter().zip(&best) {
            match b {
                Some(r) => line += &format!(" {}{}:{:.0}/{:.0}/{:.0}", v.0, if !r.ok { "!FAIL" } else if v.0 != "g" && !r.dense { "!GEN" } else { "" }, r.wall, r.user, r.sys),
                None => line += &format!(" {}:-", v.0),
            }
        }
        let mut page = PAGE.lock().unwrap();
        page.push(line);
        if let (Some(g), Some(d)) = (&best[0], &best[1]) {
            page.push(format!("Ld {}", d.laps));
            page.push(format!("Lg {}", g.laps));
            page.push(format!("sum {} [{}] hwm d{:.1} g{:.1}", if !d.sum.is_empty() && d.sum == g.sum { "SAME" } else { "DIFFERENT" }, d.sum, d.hwm_gb, g.hwm_gb));
        }
        drop(page);
        let _ = std::fs::remove_dir_all(&data);
    }
    let _ = std::fs::remove_dir_all(&scratch);
    PAGE.lock().unwrap().push(format!("t{:.0}s", t0.elapsed().as_secs_f64()));
    finish();
}
