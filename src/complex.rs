use crate::core::Name;
use crate::events::{cid_cmp, cpush};
use crate::links::{name_of, resolve_normal, resolve_token};
use crate::*;
use std::sync::atomic::Ordering;

fn resolve_all(g: &G, t: &mut TState) {
    let raw = std::mem::take(&mut t.cx_raw);
    let mut last = u64::MAX;
    let mut cache = [u64::MAX; 16];
    let mut i = 0usize;
    while i < raw.len() {
        let r = &raw[i];
        if WX_GATHER && r.w1 & ODDU == 0 {
            // (Team Wix) A long stretch of one user (a whale that ends up here has hundreds
            // of thousands of records in a row): resolve each alias once, make room once,
            // then only copy. The records land in the same lists in the same order.
            let w1 = r.w1;
            let mut j = i + 1;
            while j < raw.len() && raw[j].w1 == w1 {
                j += 1;
            }
            if j - i >= 256 {
                let run = &raw[i..j];
                let mut cnt = [0usize; 16];
                for x in run {
                    cnt[(x.w0 & 15) as usize] += 1;
                }
                let mut pk = [0u64; 16];
                let mut sh = [0usize; 16];
                for a in 0..16 {
                    if cnt[a] > 0 {
                        pk[a] = resolve_normal(g, w1 as u32, a as u32);
                        sh[a] = shard(pk[a]);
                        t.cx_ev[sh[a]].reserve(cnt[a]);
                    }
                }
                for x in run {
                    let a = (x.w0 & 15) as usize;
                    t.cx_ev[sh[a]].push(Rec { w0: x.w0, w1: pk[a] });
                }
                // the plain path's per-user cache starts over at the next user
                last = u64::MAX;
                i = j;
                continue;
            }
        }
        let pk = if r.w1 & ODDU != 0 {
            let rf = (r.w1 & (ODDU - 1)) as usize;
            resolve_token(g, g.t(rf >> 22).odd_users[rf & ((1 << 22) - 1)])
        } else {
            if r.w1 != last {
                last = r.w1;
                cache = [u64::MAX; 16];
            }
            let a = (r.w0 & 15) as usize;
            if cache[a] == u64::MAX {
                cache[a] = resolve_normal(g, r.w1 as u32, a as u32);
            }
            cache[a]
        };
        cpush(t, pk, r.w0);
        i += 1;
    }
}

const RANKF: u64 = 1 << 61;

pub fn clicks_a(g: &G, tid: usize) {
    let t = g.t(tid);
    resolve_all(g, t);
    loop {
        let i0 = g.next.fetch_add(64, Ordering::Relaxed);
        if i0 >= g.cxu.len() {
            break;
        }
        for &u in &g.cxu[i0..(i0 + 64).min(g.cxu.len())] {
            let mut cache = [u64::MAX; 16];
            let cks = g.csr_range(u);
            if unsafe { *g.utab.add(u as usize) } & COMPLEX != 0 {
                for c in cks.chunks_exact(2) {
                    let a = (c[0] & 15) as usize;
                    if cache[a] == u64::MAX {
                        cache[a] = resolve_normal(g, u, a as u32);
                    }
                    t.cx_ck[shard(cache[a])].push([cache[a], c[0], c[1]]);
                }
            } else {
                let base = (cks.as_ptr() as usize - g.csr as usize) / 8;
                for (k, &c) in cks.iter().enumerate() {
                    let a = (c & 15) as usize;
                    if cache[a] == u64::MAX {
                        cache[a] = resolve_normal(g, u, a as u32);
                    }
                    t.cx_ck[shard(cache[a])].push([cache[a], c, RANKF | (base + k) as u64]);
                }
            }
        }
    }
    let odd = std::mem::take(&mut t.odd_clicks);
    for c in &odd {
        let pk = resolve_token(g, c[0]);
        t.cx_ck[shard(pk)].push([pk, c[1], c[2]]);
    }
    t.odd_clicks = odd;
}

#[derive(Default)]
pub struct PScratch {
    evs: Vec<Rec>,
    cks: Vec<[u64; 3]>,
    dec: Vec<(u128, u64, [u64; 3])>,
    name: Vec<u8>,
    blocks: Vec<(u64, u32, u32, u32)>,
    tmp: Vec<Rec>,
    runs: Vec<usize>,
}

// ---- (Team Wix) ordering a shard's events without a general sort
//
// A shard's events arrive almost in order already: each thread's list is a few hundred
// stretches of one person each, and inside a stretch the events are in time order, with
// only events of the same millisecond out of place and now and then a second run (late
// records). A whale that ends up here brings hundreds of thousands of events, and sorting
// them from scratch was the longest serial step of the last stage. So: put the stretches
// in person order (one copy, the copy the gathering made anyway), then repair each person
// in one pass. The result is the sequence the sort gave: by person key, then by `w0`
// (records equal in both are identical). `WX_CHECK=1` verifies exactly that at run time.

/// Build switch: `XL_WXG=0` keeps the general sort and the record-by-record hand-over.
const WX_GATHER: bool = crate::env_u32(option_env!("XL_WXG"), 1) == 1;

fn wx_check() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| std::env::var("WX_CHECK").is_ok_and(|v| v == "1"))
}

/// Merges the sorted halves `s[..mid]` and `s[mid..]` by `w0`, copying the shorter one out.
fn merge_halves(s: &mut [Rec], mid: usize, tmp: &mut Vec<Rec>) {
    let n = s.len();
    if mid == 0 || mid == n || s[mid - 1].w0 <= s[mid].w0 {
        return;
    }
    tmp.clear();
    if mid <= n - mid {
        // left half out, merge forwards
        tmp.extend_from_slice(&s[..mid]);
        let (mut i, mut j, mut k) = (0usize, mid, 0usize);
        while i < mid && j < n {
            if s[j].w0 < tmp[i].w0 {
                s[k] = s[j];
                j += 1;
            } else {
                s[k] = tmp[i];
                i += 1;
            }
            k += 1;
        }
        while i < mid {
            s[k] = tmp[i];
            i += 1;
            k += 1;
        }
    } else {
        // right half out, merge backwards
        tmp.extend_from_slice(&s[mid..]);
        let (mut i, mut j, mut k) = (mid, n - mid, n);
        while i > 0 && j > 0 {
            if s[i - 1].w0 > tmp[j - 1].w0 {
                s[k - 1] = s[i - 1];
                i -= 1;
            } else {
                s[k - 1] = tmp[j - 1];
                j -= 1;
            }
            k -= 1;
        }
        while j > 0 {
            s[k - 1] = tmp[j - 1];
            j -= 1;
            k -= 1;
        }
    }
}

/// Sorts one person's events by `w0`. Expects them nearly in order (see above) and does
/// one pass; anything else goes to the general sort.
fn sort_person(s: &mut [Rec], tmp: &mut Vec<Rec>, runs: &mut Vec<usize>) {
    const MAX_RUNS: usize = 8;
    let n = s.len();
    if n < 2 {
        return;
    }
    if n <= 32 {
        s.sort_unstable_by_key(|r| r.w0);
        return;
    }
    runs.clear();
    // One pass over the groups of consecutive events with the same time key: put each
    // group in `w0` order, and note where the time steps back (a new run starts there).
    let mut i = 0usize;
    let mut prev_tk = 0u64;
    while i < n {
        let tk = s[i].w0 >> 33;
        let mut j = i + 1;
        let mut in_order = true;
        while j < n && (s[j].w0 >> 33) == tk {
            in_order &= s[j].w0 >= s[j - 1].w0;
            j += 1;
        }
        if !in_order {
            s[i..j].sort_unstable_by_key(|r| r.w0);
        }
        if i > 0 && tk < prev_tk {
            if runs.len() == MAX_RUNS {
                s.sort_unstable_by_key(|r| r.w0);
                return;
            }
            runs.push(i);
        }
        prev_tk = tk;
        i = j;
    }
    if runs.is_empty() {
        return;
    }
    // merge the runs pairwise: bounds 0, runs.., n
    let mut bounds: Vec<usize> = Vec::with_capacity(runs.len() + 2);
    bounds.push(0);
    bounds.extend_from_slice(runs);
    bounds.push(n);
    while bounds.len() > 2 {
        let mut next: Vec<usize> = Vec::with_capacity(bounds.len() / 2 + 2);
        let mut k = 0;
        while k + 2 < bounds.len() {
            let (a, m, b) = (bounds[k], bounds[k + 1], bounds[k + 2]);
            merge_halves(&mut s[a..b], m - a, tmp);
            next.push(a);
            k += 2;
        }
        if k + 1 < bounds.len() {
            next.push(bounds[k]);
        }
        next.push(n);
        bounds = next;
    }
}

/// Gathers shard `s`'s events from every thread into `evs`, in the order the sort by
/// (person key, w0) gives. Returns false (leaving `evs` for the caller to fill) when the
/// lists are not stretches of one person each.
fn gather_sorted(g: &G, s: usize, evs: &mut Vec<Rec>, blocks: &mut Vec<(u64, u32, u32, u32)>, tmp: &mut Vec<Rec>, runs: &mut Vec<usize>) -> bool {
    let total: usize = (0..g.nt).map(|th| g.t(th).cx_ev[s].len()).sum();
    blocks.clear();
    for th in 0..g.nt {
        let v = &g.t(th).cx_ev[s];
        if v.len() > u32::MAX as usize {
            return false;
        }
        let mut i = 0usize;
        while i < v.len() {
            let pk = v[i].w1;
            let mut j = i + 1;
            while j < v.len() && v[j].w1 == pk {
                j += 1;
            }
            blocks.push((pk, th as u32, i as u32, (j - i) as u32));
            i = j;
        }
        if blocks.len() > total / 8 + 64 {
            return false;
        }
    }
    // by person key; a person's stretches stay in thread and arrival order
    blocks.sort_unstable();
    evs.clear();
    evs.reserve(total);
    for &(_, th, i, len) in blocks.iter() {
        evs.extend_from_slice(&g.t(th as usize).cx_ev[s][i as usize..i as usize + len as usize]);
    }
    let all = &mut evs[..];
    let mut a = 0usize;
    while a < all.len() {
        let pk = all[a].w1;
        let mut b = a + 1;
        while b < all.len() && all[b].w1 == pk {
            b += 1;
        }
        sort_person(&mut all[a..b], tmp, runs);
        a = b;
    }
    true
}

pub fn shard_order(g: &G) -> Vec<usize> {
    let mut w: Vec<(usize, usize)> = (0..NSHARD).map(|s| ((0..g.nt).map(|th| g.t(th).cx_ev[s].len() + g.t(th).cx_ck[s].len()).sum(), s)).collect();
    w.sort_unstable_by(|a, b| b.cmp(a));
    w.into_iter().map(|x| x.1).collect()
}

fn persons_b(g: &G, tid: usize, order: &[usize]) {
    let mut sc = PScratch::default();
    loop {
        let i = g.next.fetch_add(1, Ordering::Relaxed);
        if i >= order.len() {
            break;
        }
        person_shard(g, tid, order[i], &mut sc);
    }
}

pub fn person_shard(g: &G, tid: usize, s: usize, sc: &mut PScratch) {
    let t = g.t(tid);
    let PScratch { evs, cks, dec, name, blocks, tmp, runs } = sc;
    {
        cks.clear();
        for th in 0..g.nt {
            cks.extend_from_slice(&g.t(th).cx_ck[s]);
        }
        if !WX_GATHER || !gather_sorted(g, s, evs, blocks, tmp, runs) {
            evs.clear();
            for th in 0..g.nt {
                evs.extend_from_slice(&g.t(th).cx_ev[s]);
            }
            evs.sort_unstable_by_key(|r| (r.w1 as u128) << 64 | r.w0 as u128);
        } else if wx_check() {
            // self-check: the same sequence as the general sort
            let mut want: Vec<Rec> = Vec::new();
            for th in 0..g.nt {
                want.extend_from_slice(&g.t(th).cx_ev[s]);
            }
            want.sort_unstable_by_key(|r| (r.w1 as u128) << 64 | r.w0 as u128);
            let same = want.len() == evs.len() && want.iter().zip(evs.iter()).all(|(x, y)| x.w0 == y.w0 && x.w1 == y.w1);
            if !same {
                eprintln!("[wx] CHECK FAILED: shard {s} order differs from the general sort");
                unsafe { sys::_exit(9) }
            }
        }
        if evs.is_empty() {
            return;
        }
        if cks.iter().all(|c| c[2] & (RAW | RANKF) != RAW) {
            dec.clear();
            dec.extend(cks.iter().map(|c| ((c[0] as u128) << 64 | (c[1] >> 32) as u128, if c[2] & RANKF != 0 { c[2] } else { !enc36(c[2]).0 }, *c)));
            dec.sort_unstable_by(|x, y| (x.0, x.1).cmp(&(y.0, y.1)));
            for (d, x) in cks.iter_mut().zip(dec.iter()) {
                *d = x.2;
            }
        } else {
            cks.sort_unstable_by(|a, b| (a[0], a[1] >> 32).cmp(&(b[0], b[1] >> 32)).then_with(|| if (a[2] | b[2]) & RANKF != 0 { a[2].cmp(&b[2]) } else { cid_cmp(g, b[2], a[2]) }));
        }
        for w in cks.windows(2) {
            if w[0][0] == w[1][0] && (w[0][2] ^ w[1][2]) & RANKF != 0 {
                fallback("click order keys of one person");
            }
        }
        let (mut i, mut j) = (0usize, 0usize);
        while i < evs.len() {
            let pk = evs[i].w1;
            let mut i2 = i + 1;
            while i2 < evs.len() && evs[i2].w1 == pk {
                i2 += 1;
            }
            while j < cks.len() && cks[j][0] < pk {
                j += 1;
            }
            t.ck.clear();
            while j < cks.len() && cks[j][0] == pk {
                t.ck.push(cks[j][1]);
                j += 1;
            }
            name_of(g, pk, name);
            t.core.person(g, &evs[i..i2], &t.ck, Name::Bytes(name));
            i = i2;
        }
    }
}

pub fn stage4(g: &mut G) {
    stage4_pre(g);
    g.next.store(0, Ordering::Relaxed);
    {
        let gr: &G = g;
        sys::run_par(gr.nt, |tid| clicks_a(gr, tid));
    }
    crate::sys::lap("c.a");
    let order = shard_order(g);
    g.next.store(0, Ordering::Relaxed);
    {
        let gr: &G = g;
        let o = &order;
        sys::run_par(gr.nt, |tid| persons_b(gr, tid, o));
    }
}

pub fn stage4_pre(g: &mut G) {
    let users: Vec<u32> = g.extra.keys().copied().collect();
    for u in users {
        let tv = unsafe { *g.utab.add(u as usize) };
        let t0 = g.t(0);
        if tv & COMPLEX != 0 {
            for &w0 in &g.extra[&u] {
                t0.cx_raw.push(Rec { w0, w1: u as u64 });
                t0.n_odd += 1;
            }
        } else if unsafe { (*g.claim.add(u as usize >> 6)).load(Ordering::Relaxed) } >> (u & 63) & 1 == 0 {
            unsafe { crate::events::process_run(g, t0, u, &mut []) };
        }
    }
    for oe in &g.odd_kept {
        let t0 = g.t(0);
        t0.cx_raw.push(Rec { w0: oe.w0, w1: oe.user });
        t0.n_odd += 1;
    }
    let mut cxu = Vec::new();
    for tid in 0..g.nt {
        cxu.extend_from_slice(&g.t(tid).cx_users);
        cxu.extend_from_slice(&g.t(tid).mixed_users);
    }
    if crate::links::DBG.load(Ordering::Relaxed) {
        eprintln!("[dbg] stage 4 users: {} ({} with a run in two classes)", cxu.len(), (0..g.nt).map(|t| g.t(t).mixed_users.len()).sum::<usize>());
    }
    g.cxu = cxu;
    crate::sys::lap("c.pre");
}

#[cfg(test)]
mod wx_tests {
    use super::*;

    struct Lcg(u64);
    impl Lcg {
        fn next(&mut self) -> u64 {
            self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            self.0 >> 11
        }
        fn below(&mut self, n: u64) -> u64 {
            self.next() % n.max(1)
        }
    }

    fn check(mut v: Vec<Rec>, what: &str) {
        let mut want = v.clone();
        want.sort_unstable_by_key(|r| r.w0);
        let (mut tmp, mut runs) = (Vec::new(), Vec::new());
        sort_person(&mut v, &mut tmp, &mut runs);
        assert!(v.len() == want.len() && v.iter().zip(want.iter()).all(|(a, b)| a.w0 == b.w0 && a.w1 == b.w1), "{what}: n={}", v.len());
    }

    /// `n` records in time order: time keys step by 0..`step`, low bits are random.
    fn timed(r: &mut Lcg, n: usize, step: u64, t0: u64) -> Vec<Rec> {
        let mut t = t0;
        (0..n)
            .map(|_| {
                t += r.below(step + 1);
                Rec { w0: (t << 33) | r.below(1 << 33), w1: 7 }
            })
            .collect()
    }

    #[test]
    fn sort_person_matches_sort() {
        let mut r = Lcg(0x5eed);
        for round in 0..400 {
            let n = match round % 5 {
                0 => r.below(40) as usize,
                1 => r.below(300) as usize,
                _ => r.below(6000) as usize,
            };
            // random
            check((0..n).map(|_| Rec { w0: r.next() << 3 | r.below(8), w1: 7 }).collect(), "random");
            // already sorted
            let mut v: Vec<Rec> = (0..n).map(|_| Rec { w0: r.next(), w1: 7 }).collect();
            v.sort_unstable_by_key(|x| x.w0);
            check(v, "sorted");
            // time order, low bits out of order (groups of equal time of every size)
            for step in [0u64, 1, 2, 50] {
                check(timed(&mut r, n, step, 1000), "time order");
            }
            // several runs, each in time order (more than the merge limit too)
            for k in [2usize, 3, 5, 8, 9, 12, 40] {
                let mut v = Vec::new();
                for _ in 0..k {
                    let t0 = r.below(5000);
                    v.extend(timed(&mut r, n / k + 1, 3, t0));
                }
                check(v, "runs");
            }
            // one long run and a short late one, and the other way round
            let mut v = timed(&mut r, n, 2, 100);
            v.extend(timed(&mut r, n / 50 + 1, 2, 50));
            check(v, "long + short");
            let mut v = timed(&mut r, n / 50 + 1, 2, 5000);
            v.extend(timed(&mut r, n, 2, 100));
            check(v, "short + long");
            // every record the same time
            check((0..n).map(|_| Rec { w0: (9u64 << 33) | r.below(1 << 20), w1: 7 }).collect(), "one time");
            // descending
            let mut v: Vec<Rec> = (0..n).map(|_| Rec { w0: r.next(), w1: 7 }).collect();
            v.sort_unstable_by_key(|x| std::cmp::Reverse(x.w0));
            check(v, "descending");
        }
    }

    #[test]
    fn merge_halves_matches_sort() {
        let mut r = Lcg(77);
        for _ in 0..2000 {
            let (a, b) = (r.below(200) as usize, r.below(200) as usize);
            let mut left: Vec<Rec> = (0..a).map(|_| Rec { w0: r.below(500), w1: 1 }).collect();
            let mut right: Vec<Rec> = (0..b).map(|_| Rec { w0: r.below(500), w1: 1 }).collect();
            left.sort_unstable_by_key(|x| x.w0);
            right.sort_unstable_by_key(|x| x.w0);
            let mut v = left.clone();
            v.extend_from_slice(&right);
            let mut want = v.clone();
            want.sort_unstable_by_key(|x| x.w0);
            let mut tmp = Vec::new();
            merge_halves(&mut v, a, &mut tmp);
            assert!(v.iter().zip(want.iter()).all(|(x, y)| x.w0 == y.w0));
        }
    }
}
