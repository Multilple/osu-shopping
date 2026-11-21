//! Measurement run (not an entry). Makes 20M and 60M event datasets with the
//! published generator, times each candidate binary on them in interleaved
//! rounds, prints one compact table on stderr and exits with code 3 so the
//! log comes back. Variants are listed in /v/list as: name binary [KEY=VALUE ...]
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Instant;

struct Variant {
    name: String,
    bin: String,
    env: Vec<(String, String)>,
}

fn files_under(dir: &Path, out: &mut Vec<PathBuf>) {
    if let Ok(rd) = fs::read_dir(dir) {
        for e in rd.flatten() {
            let p = e.path();
            if p.is_dir() {
                files_under(&p, out);
            } else {
                out.push(p);
            }
        }
    }
}

/// Forces the generated files to disk, so no writeback runs during the timings.
fn settle(dir: &Path) {
    let mut files = Vec::new();
    files_under(dir, &mut files);
    let files = &files;
    let next = std::sync::atomic::AtomicUsize::new(0);
    std::thread::scope(|s| {
        for _ in 0..8 {
            s.spawn(|| loop {
                let i = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if i >= files.len() {
                    break;
                }
                if let Ok(f) = fs::File::open(&files[i]) {
                    let _ = f.sync_all();
                }
            });
        }
    });
}

fn median(v: &mut Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    if v.is_empty() { 0.0 } else { v[v.len() / 2] }
}

fn main() {
    let t0 = Instant::now();
    let scratch = std::env::var("SCRATCH_DIR").unwrap_or_else(|_| "/scratch".into());
    let outdir = std::env::var("OUTPUT_DIR").unwrap_or_else(|_| "/output".into());
    let vdir = std::env::var("PROBE_V").unwrap_or_else(|_| "/v".into());
    let gen = std::env::var("PROBE_GEN").unwrap_or_else(|_| "/generate".into());
    let gen_threads = std::env::var("PROBE_GEN_THREADS").unwrap_or_else(|_| "8".into());
    // name of the variant whose own stage timings are printed (run once more, with XL_TIMING=1)
    let lap_of = std::env::var("PROBE_LAPS").unwrap_or_else(|_| "n,n4,n6".into());
    let settle_min: u64 = std::env::var("PROBE_SETTLE_MIN").ok().and_then(|s| s.parse().ok()).unwrap_or(40_000_000);
    let reps: usize = std::env::var("PROBE_REPS").ok().and_then(|s| s.parse().ok()).unwrap_or(5);
    let budget: f64 = std::env::var("PROBE_BUDGET").ok().and_then(|s| s.parse().ok()).unwrap_or(155.0);
    let sizes: Vec<(u64, u64)> = std::env::var("PROBE_SIZES")
        .unwrap_or_else(|_| "60000000:32,20000000:31".into())
        .split(',')
        .filter_map(|s| {
            let (a, b) = s.split_once(':')?;
            Some((a.parse().ok()?, b.parse().ok()?))
        })
        .collect();
    let list = fs::read_to_string(format!("{vdir}/list")).unwrap_or_default();
    let variants: Vec<Variant> = list
        .lines()
        .filter(|l| !l.trim().is_empty() && !l.starts_with('#'))
        .map(|l| {
            let mut it = l.split_whitespace();
            let name = it.next().unwrap().to_string();
            let bin = format!("{vdir}/{}", it.next().unwrap());
            let env = it.filter_map(|kv| kv.split_once('=').map(|(k, v)| (k.to_string(), v.to_string()))).collect();
            Variant { name, bin, env }
        })
        .collect();
    let mut report = String::new();
    for &(events, seed) in &sizes {
        let data = format!("{scratch}/d{}", events / 1_000_000);
        let g0 = Instant::now();
        let st = Command::new(&gen)
            .args(["--seed", &seed.to_string(), "--events", &events.to_string(), "--clicks", &(events / 4).to_string(), "--output", &data, "--threads", &gen_threads])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        let gen_s = g0.elapsed().as_secs_f64();
        if !matches!(&st, Ok(s) if s.success()) {
            report.push_str(&format!("{}M GENFAIL {:?}\n", events / 1_000_000, st.map(|s| s.code())));
            continue;
        }
        let s0 = Instant::now();
        // only the big set is forced to disk first: the small one is generated late and its
        // writeback is short
        if events >= settle_min {
            settle(Path::new(&data));
        }
        let settle_s = s0.elapsed().as_secs_f64();
        let out = format!("{outdir}/probe-out");
        let mut times: Vec<Vec<f64>> = vec![Vec::new(); variants.len()];
        let mut bad: Vec<String> = vec![String::new(); variants.len()];
        // round 0 is a warm-up and is not counted
        'rounds: for r in 0..=reps {
            for k in 0..variants.len() {
                let i = (k + r) % variants.len();
                if t0.elapsed().as_secs_f64() > budget {
                    break 'rounds;
                }
                let v = &variants[i];
                let _ = fs::remove_dir_all(&out);
                let _ = fs::create_dir_all(&out);
                let mut c = Command::new(&v.bin);
                c.arg(&data).arg(&out).stdout(Stdio::null()).stderr(Stdio::piped());
                for (k, val) in &v.env {
                    c.env(k, val);
                }
                let t = Instant::now();
                let res = c.output();
                let ms = t.elapsed().as_secs_f64() * 1e3;
                match res {
                    Ok(o) => {
                        let err = String::from_utf8_lossy(&o.stderr);
                        if !o.status.success() {
                            bad[i] = format!("rc{:?}", o.status.code());
                        } else if err.contains("FALLBACK") {
                            bad[i] = "FB".into();
                        }
                        if r > 0 {
                            times[i].push(ms);
                        }
                    }
                    Err(_) => bad[i] = "spawn".into(),
                }
            }
        }
        let mut laps = String::new();
        for name in lap_of.split(',') {
            let Some(v) = variants.iter().find(|v| v.name == name) else { continue };
            let _ = fs::remove_dir_all(&out);
            let _ = fs::create_dir_all(&out);
            let mut c = Command::new(&v.bin);
            c.arg(&data).arg(&out).env("XL_TIMING", "1").stdout(Stdio::null()).stderr(Stdio::piped());
            for (k, val) in &v.env {
                c.env(k, val);
            }
            let t = Instant::now();
            if let Ok(o) = c.output() {
                let ms = t.elapsed().as_secs_f64() * 1e3;
                let err = String::from_utf8_lossy(&o.stderr);
                for l in err.lines() {
                    if let Some(i) = l.find("ms: ") {
                        // only the big laps, to keep the line short
                        let kept: Vec<String> = l[i + 4..]
                            .split_whitespace()
                            .filter_map(|f| {
                                let (k, v) = f.split_once('=')?;
                                let short = match k {
                                    "parse" => "p",
                                    "st2sort" => "s2",
                                    "ev3a" => "e",
                                    "ev3b" => "e2",
                                    "write" => "w",
                                    "total" => "T",
                                    _ => return None,
                                };
                                let x: f64 = v.parse().ok()?;
                                Some(format!("{short}={x:.0}"))
                            })
                            .collect();
                        laps.push_str(&format!(" L[{}]W{:.0} {}", v.name, ms, kept.join(" ")));
                    }
                }
            }
        }
        let _ = fs::remove_dir_all(&out);
        report.push_str(&format!("{}M gen{:.0}s settle{:.0}s n{}", events / 1_000_000, gen_s, settle_s, times.iter().map(|t| t.len()).min().unwrap_or(0)));
        for (i, v) in variants.iter().enumerate() {
            let mn = times[i].iter().cloned().fold(f64::INFINITY, f64::min);
            let md = median(&mut times[i]);
            report.push_str(&format!(" {}:{:.0}/{:.0}{}", v.name, md, if mn.is_finite() { mn } else { 0.0 }, bad[i]));
        }
        report.push_str(&laps);
        report.push('\n');
        let _ = fs::remove_dir_all(&data);
    }
    eprintln!("PROBE med/min ms\n{report}t{:.0}s", t0.elapsed().as_secs_f64());
    std::process::exit(3);
}
