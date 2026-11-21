//! Dense mode: the link parse kernel.
//!
//! A link line is two user IDs. Both are read at once, the way the click
//! kernel reads a click's two IDs (see `dense_ck`): the 16 bytes that end
//! with the comma and the 16 that end with the newline go through one table
//! row each, picked by the field's raw length, and come out as two numbers
//! or as "not a dense ID". A line is taken here only when every byte of it
//! has been checked; the scanner and the plain code in `dense::parse_links`
//! take the rest, so the pairs of nodes joined are the same either way.

#![allow(clippy::missing_safety_doc)]

use core::arch::x86_64::*;
use std::sync::OnceLock;

use crate::dense::{DOut, Dn, M40};
use crate::dense_ck::{both, id_row, ld128, ld256, tz64, unscramble_top, V32, ALIAS_HI_TOP, A_INV, B_INV, ID_ADJ, ID_LIM0, ID_LIM1, ID_W1, ID_W2, NODE_MASK_TOP};
use crate::lines;
use crate::util::*;

/// ID rows (see `dense_ck::id_row`) by `raw length + 65`: a length from -65
/// (no comma in sight, the newline at once) to 64 (no newline in sight).
#[repr(C, align(64))]
struct Rows([[u8; 16]; 130]);

static ROWS: Rows = {
    let mut t = [[15u8; 16]; 130];
    let mut r = 0;
    while r < 65 {
        t[r + 65] = id_row(r);
        r += 1;
    }
    Rows(t)
};

/// '0' off every byte; the comma that ends the first lane and the newline
/// that ends the second become what a quote becomes.
static LK_SUB: V32 = {
    let mut v = both([0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x3A]);
    v.0[31] = 0x18;
    v
};
/// What the slots that must hold one value hold (after `LK_SUB`): `u`, the
/// quotes, and slot 11 flipped so that only a first digit of '0' reads as 0xFF.
static LK_EXP: V32 = both([0, 0, 0, 0, 0, 0, 0, 0, b'u' - 0x30, 0xF2, 0xF2, 0xFF, 0, 0, 0, 0]);

/// Where a kernel's pairs go.
trait Sink {
    fn link(&mut self, dn: &Dn, a: u32, b: u32);
}

impl Sink for DOut {
    #[inline(always)]
    fn link(&mut self, dn: &Dn, a: u32, b: u32) {
        crate::dense::link_nodes(dn, self, a, b);
    }
}

#[derive(Default)]
struct Collect(Vec<(u32, u32)>);

impl Sink for Collect {
    fn link(&mut self, _: &Dn, a: u32, b: u32) {
        self.0.push((a, b));
    }
}

/// One link line by the plain rules: its two nodes, if it is a link.
fn link_record(line: &[u8], dn: &Dn) -> Option<(u32, u32)> {
    let [a, b] = split::<2>(line)?;
    if a.is_empty() || b.is_empty() {
        return None;
    }
    Some((dn.user_node(a), dn.user_node(b)))
}

/// What the code off the fast path needs, in one place: nothing of it is
/// held in a register while the loop runs.
struct Env<'a, S> {
    file: &'a [u8],
    end: usize,
    dn: &'a Dn,
    sink: *mut S,
    /// Where the loop stops.
    stop: *const u8,
}

/// The line starting at `file[s]` by the plain rules. Returns where the next
/// line starts, or `usize::MAX` when this one does not end before `end`.
#[cold]
#[inline(never)]
unsafe fn cold_line<S: Sink>(env: &Env<S>, s: usize) -> usize {
    match memchr(b'\n', &env.file[s..env.end]) {
        Some(i) => {
            if let Some((a, b)) = link_record(&env.file[s..s + i], env.dn) {
                (*env.sink).link(env.dn, a, b);
            }
            s + i + 1
        }
        None => usize::MAX,
    }
}

/// The node of an ID the fast path has checked, `raw` bytes that end before
/// `end`, when it is not a direct one.
#[cold]
#[inline(never)]
unsafe fn node_slow(end: *const u8, raw: usize, dn: &Dn) -> u64 {
    let f = std::slice::from_raw_parts(end.sub(raw), raw);
    let id = if f[0] == b'"' { &f[1..raw - 1] } else { f };
    dn.user_node(id) as u64
}

/// The link lines of `file[start..end]` that end before `end`, as far as
/// the fast path can go. Returns where the unhandled rest starts.
#[inline(always)]
unsafe fn kernel<S: Sink>(file: &[u8], start: usize, end: usize, dn: &Dn, sink: &mut S) -> usize {
    let base = file.as_ptr();
    // Past the last line that ends before `end`; the loop reads up to 128
    // bytes from a line's start.
    let mut lim = end;
    while lim > start && *base.add(lim - 1) != b'\n' {
        lim -= 1;
    }
    let stop = lim.min(file.len().saturating_sub(127));
    let env = Env { file, end, dn, sink: sink as *mut S, stop: base.add(stop) };
    let mut s = start;
    // The ID lanes look back 15 bytes from a line's start.
    while s < stop && s < 16 {
        match cold_line(&env, s) {
            usize::MAX => return s,
            n => s = n,
        }
    }
    let mut p = base.add(s);
    // The line by the plain rules, then the next one.
    macro_rules! cold {
        () => {{
            let n = cold_line(&env, p.offset_from(env.file.as_ptr()) as usize);
            if n == usize::MAX {
                break;
            }
            p = env.file.as_ptr().add(n);
            continue;
        }};
    }
    let rows = ROWS.0.as_ptr();
    let ukey = dn.keys.user & M40;
    let max_node = dn.vbound >> 6;
    let (binv24, ainv24) = (B_INV << 24, A_INV << 24);
    // (The end is read from memory each time round: it is one register more for the loop.)
    while p < std::ptr::read_volatile(&env.stop) {
        // The comma and the newline, in the line's first 32 bytes.
        let v0 = _mm256_loadu_si256(p as *const __m256i);
        let a1 = tz64(_mm256_movemask_epi8(_mm256_cmpeq_epi8(v0, _mm256_set1_epi8(b',' as i8))) as u32 as u64);
        let e = tz64(_mm256_movemask_epi8(_mm256_cmpeq_epi8(v0, _mm256_set1_epi8(b'\n' as i8))) as u32 as u64);

        // Both IDs at once: 16 bytes up to the comma and 16 up to the newline, moved to slots.
        let lanes = _mm256_inserti128_si256(_mm256_castsi128_si256(ld128(p.add(a1).sub(15))), ld128(p.add(e).sub(15)), 1);
        let ctrl = _mm256_inserti128_si256(_mm256_castsi128_si256(ld128(rows.add(a1 + 65))), ld128(rows.offset(e as isize - a1 as isize + 64)), 1);
        let a = _mm256_sub_epi8(lanes, ld256(&LK_SUB));
        let x = _mm256_xor_si256(_mm256_shuffle_epi8(a, ctrl), ld256(&LK_EXP));
        let g = _mm256_cmpgt_epi8(x, _mm256_set1_epi8(0x30));
        let over = _mm256_subs_epu8(x, _mm256_blendv_epi8(ld256(&ID_LIM0), ld256(&ID_LIM1), g));
        let v = _mm256_sub_epi8(x, _mm256_and_si256(g, ld256(&ID_ADJ)));
        let quads = _mm256_madd_epi16(_mm256_maddubs_epi16(v, ld256(&ID_W1)), ld256(&ID_W2));
        let ids = _mm256_add_epi64(_mm256_mul_epu32(quads, _mm256_set1_epi64x(1_679_616)), _mm256_srli_epi64(quads, 32));
        // Wrong byte somewhere, or a number of 2^40 or more.
        let bad = _mm256_or_si256(over, _mm256_srli_epi64(ids, 40));
        if _mm256_testz_si256(bad, bad) == 0 {
            cold!();
        }
        let ta = unscramble_top(_mm256_extract_epi64(ids, 0) as u64, ukey, binv24, ainv24);
        let tb = unscramble_top(_mm256_extract_epi64(ids, 2) as u64, ukey, binv24, ainv24);

        // The nodes: direct when the ID's number within its person is below
        // 8 and the person is in range.
        let mut na = _pext_u64(ta, NODE_MASK_TOP);
        if (ta & ALIAS_HI_TOP != 0) | (na >= max_node) {
            na = node_slow(p.add(a1), a1, env.dn);
        }
        let mut nb = _pext_u64(tb, NODE_MASK_TOP);
        if (tb & ALIAS_HI_TOP != 0) | (nb >= max_node) {
            nb = node_slow(p.add(e), e - a1 - 1, env.dn);
        }
        (*env.sink).link(env.dn, na as u32, nb as u32);
        p = p.add(e + 1);
    }
    p.offset_from(env.file.as_ptr()) as usize
}

/// SS_LKVERIFY: the kernel's pairs for `file[start..rest]` against the plain
/// code's, line by line when they differ.
#[cold]
#[inline(never)]
unsafe fn verified(file: &[u8], start: usize, end: usize, dn: &Dn, out: &mut DOut) -> usize {
    let mut got = Collect::default();
    let rest = kernel(file, start, end, dn, &mut got);
    let want: Vec<(u32, u32)> = lines(&file[start..rest]).filter_map(|l| link_record(l, dn)).collect();
    if got.0 != want {
        let mut at = start;
        while at < rest {
            let le = at + memchr(b'\n', &file[at..rest]).unwrap();
            let mut one = Collect::default();
            let r = kernel(file, at, le + 1, dn, &mut one);
            let plain = link_record(&file[at..le], dn);
            // (At the very end of the input the kernel takes no line.)
            if r != at && (r != le + 1 || one.0.first().copied() != plain || one.0.len() > 1) {
                panic!("link kernel differs on {:?}: {:x?} (rest {} of {}) against {:x?}", String::from_utf8_lossy(&file[at..le]), one.0, r - at, le + 1 - at, plain);
            }
            at = le + 1;
        }
        panic!("link kernel differs over {} lines, but not on any one of them", want.len());
    }
    for &(a, b) in &got.0 {
        out.link(dn, a, b);
    }
    rest
}

/// SS_LKVERIFY: `dense::digit_order` against the names themselves, on the
/// numbers where the digit count changes, on strings that start with one
/// another, and on numbers at random.
fn check_digit_order() {
    let mut vals: Vec<u64> = vec![0, 1, 35, (1 << 40) - 1, (1 << 40) - 2, 1 << 39];
    let mut p = 1u64;
    for _ in 0..8 {
        p *= 36;
        for v in [p - 1, p, p + 1, 2 * p, 35 * p, p + p / 36] {
            if v < 1 << 40 {
                vals.push(v);
            }
        }
    }
    let mut x = 0x9E37_79B9_7F4A_7C15u64;
    for i in 0..400 {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        // (Short numbers too, and ones that go on from a shorter one.)
        let v = (x >> 24) >> (i % 37);
        vals.push(v);
        if v != 0 && v * 36 < 1 << 40 {
            vals.push(v * 36);
            vals.push(v * 36 + i % 36);
        }
    }
    for &a in &vals {
        for &b in &vals {
            let want = cmp_key(crate::dense::id_key(b'u', a), crate::dense::id_key(b'u', b));
            let got = crate::dense::digit_order(a).cmp(&crate::dense::digit_order(b));
            assert!(want == got, "digit_order: {a} against {b}: {got:?}, the names say {want:?}");
        }
    }
}

/// The link lines of `file[start..end]` that end before `end`, as far as
/// the fast path takes them: their nodes go where `dense::link` puts them.
/// Returns where the rest starts.
#[inline(never)]
pub(crate) unsafe fn links_dense(file: &[u8], start: usize, end: usize, dn: &Dn, out: &mut DOut) -> usize {
    static VERIFY: OnceLock<bool> = OnceLock::new();
    if *VERIFY.get_or_init(|| {
        let on = std::env::var_os("SS_LKVERIFY").is_some();
        if on {
            check_digit_order();
        }
        on
    }) {
        return verified(file, start, end, dn, out);
    }
    kernel(file, start, end, dn, out)
}
