//! The dense event parse kernel: event lines straight to dense-mode records
//! (see `DOut::ev` in dense.rs for the record layout), a line at a time with
//! vector instructions and few branches.
//!
//! `dense::event_line` is the reference: the kernel pushes exactly its
//! records, in the same order, for every line it takes on itself, and hands
//! every line it cannot prove sound to it.

use core::arch::x86_64::*;
use std::cell::UnsafeCell;

use crate::dense::{event_fields, event_line, ingest31, DOut, Dn, EXT, INGEST_TOP, LOW21, M40, PURCHASE};
use crate::util::*;
use crate::Ctx;

const MUL_A: u64 = 0x9E37_79B9_7F4B;
const MUL_B: u64 = 0xD6E8_FEB8_6659;

/// The inverse of an odd number modulo 2^64.
const fn inverse(a: u64) -> u64 {
    let mut x = a;
    let mut i = 0;
    while i < 6 {
        x = x.wrapping_mul(2u64.wrapping_sub(a.wrapping_mul(x)));
        i += 1;
    }
    x
}

/// `(B_INV << 24)`: a 40-bit ID times this is `(ID * B_INV mod 2^40) << 24`.
pub(crate) const BINV24: u64 = (inverse(MUL_B) & M40) << 24;
pub(crate) const AINV24: u64 = (inverse(MUL_A) & M40) << 24;

// ------------------------------------------------------------------ times

/// Per-thread memos for the two things about a timestamp that take few
/// values: its calendar date, and its form with its UTC offset.
struct Memo {
    /// Dates seen: the 11 bytes `YYYY-MM-DDT` -> the time at midnight, in
    /// milliseconds and in seconds. Only valid dates are kept, so a hit
    /// needs no check.
    key8: [u64; 64],
    key4: [u32; 64],
    ms: [i64; 64],
    sec: [i64; 64],
    /// Forms seen (see `form_key`) -> what to take off the local time, ms.
    fkey: [u64; 512],
    fval: [i64; 512],
}

/// The bits of a form key that pick its slot: of the sign, the hours and the
/// tens of minutes, and one that tells the offset form from the others.
pub(crate) const FORM_BITS: u64 = 0x02 | (0x01 << 8) | (0x0F << 16) | (0x05 << 32) | (1 << 58);

#[repr(C, align(64))]
pub(crate) struct T64(pub(crate) [u64; 64]);

/// By ts length: which of the 8 bytes from its 20th on belong to a form's key
/// (`Z` of the 20-byte form, `+HH:MM` of the 25-byte one, nothing of the
/// 13-digit one) ...
static FORM_MASK: T64 = {
    let mut t = [0u64; 64];
    t[20] = 0xFF;
    t[25] = 0xFFFF_FFFF_FFFF;
    T64(t)
};

/// ... and the length itself in the key's top byte, so that no two lengths
/// share a key and a length that no form has gives a key that is never kept.
static FORM_TAG: T64 = {
    let mut t = [0u64; 64];
    let mut n = 0;
    while n < 64 {
        t[n] = (n as u64) << 56;
        n += 1;
    }
    T64(t)
};

impl Memo {
    fn new() -> Memo {
        // Every date slot starts as 1970-01-01, which is right for the one
        // slot that date maps to and can never match in any other. No form
        // key has all its bits set.
        let k = b"1970-01-01T";
        let k8 = u64::from_le_bytes(k[..8].try_into().unwrap());
        let k4 = u32::from_le_bytes(k[7..11].try_into().unwrap());
        Memo { key8: [k8; 64], key4: [k4; 64], ms: [0; 64], sec: [0; 64], fkey: [!0; 512], fval: [0; 512] }
    }

    /// A date not in its slot: checks it by the plain rules and keeps it.
    /// False when the 11 bytes at `p` are not a valid `YYYY-MM-DDT`.
    #[cold]
    #[inline(never)]
    unsafe fn date_miss(&mut self, p: *const u8, idx: usize) -> bool {
        match date_secs(p) {
            Some(v) => {
                self.key8[idx] = std::ptr::read_unaligned(p as *const u64);
                self.key4[idx] = std::ptr::read_unaligned(p.add(7) as *const u32);
                self.sec[idx] = v;
                self.ms[idx] = v * 1000;
                true
            }
            None => false,
        }
    }

    /// The slot of the date `YYYY-MM-DDT` at `p`, if it is one.
    #[inline(always)]
    unsafe fn date(&mut self, p: *const u8) -> Option<usize> {
        let k8 = std::ptr::read_unaligned(p as *const u64);
        let k4 = std::ptr::read_unaligned(p.add(7) as *const u32);
        let idx = _pext_u32(k4, 0x000F_0300) as usize;
        if (*self.key8.get_unchecked(idx) != k8 || *self.key4.get_unchecked(idx) != k4) && !self.date_miss(p, idx) {
            return None;
        }
        Some(idx)
    }

    /// A form key not in its slot: checks it by the plain rules and keeps it.
    /// False when a ts of `n` bytes with this key has none of the three forms
    /// (as far as its form bytes tell).
    #[cold]
    #[inline(never)]
    fn form_miss(&mut self, key: u64, idx: usize, n: usize) -> bool {
        match form_off(key, n) {
            Some(val) => {
                self.fkey[idx] = key;
                self.fval[idx] = val;
                true
            }
            None => false,
        }
    }
}

/// The time at midnight (seconds) of the date in the 11 bytes `YYYY-MM-DDT`
/// at `p`, by the plain rules; `None` when they are not a valid date.
pub(crate) unsafe fn date_secs(p: *const u8) -> Option<i64> {
    let b = std::slice::from_raw_parts(p, 11);
    let d = |i: usize| -> Option<i64> {
        let v = b[i].wrapping_sub(b'0');
        (v <= 9).then_some(v as i64)
    };
    if b[4] != b'-' || b[7] != b'-' || b[10] != b'T' {
        return None;
    }
    let year = d(0)? * 1000 + d(1)? * 100 + d(2)? * 10 + d(3)?;
    let month = d(5)? * 10 + d(6)?;
    let day = d(8)? * 10 + d(9)?;
    if !(1..=12).contains(&month) || day < 1 {
        return None;
    }
    let dim = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ => {
            if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) {
                29
            } else {
                28
            }
        }
    };
    if day > dim {
        return None;
    }
    Some(days_from_civil(year, month, day) * 86_400)
}

/// What a ts of `n` bytes takes off its local time (ms), given the form
/// bytes in the low 6 bytes of `key` (`Z`, or `+HH:MM`, or nothing for 13
/// digits); `None` when that is none of the three forms.
pub(crate) fn form_off(key: u64, n: usize) -> Option<i64> {
    let b = key.to_le_bytes();
    match n {
        13 => Some(0),
        20 if b[0] == b'Z' => Some(0),
        25 => {
            let d = |i: usize| (b[i].wrapping_sub(b'0') as i64, b[i].wrapping_sub(b'0') <= 9);
            let ((h1, a), (h0, c), (m1, e), (m0, f)) = (d(1), d(2), d(4), d(5));
            let (hh, mm) = (h1 * 10 + h0, m1 * 10 + m0);
            if !(a && c && e && f) || (b[0] != b'+' && b[0] != b'-') || b[3] != b':' || hh > 23 || mm > 59 {
                return None;
            }
            let off = (hh * 60 + mm) * MINUTE;
            Some(if b[0] == b'+' { off } else { -off })
        }
        _ => None,
    }
}

thread_local! {
    static MEMO: UnsafeCell<Memo> = UnsafeCell::new(Memo::new());
}

#[repr(C, align(32))]
pub(crate) struct Y32(pub(crate) [u8; 32]);
#[repr(C, align(32))]
pub(crate) struct W16(pub(crate) [i16; 16]);
#[repr(C, align(32))]
pub(crate) struct D8(pub(crate) [u32; 8]);

// The constants of `times`, low lane (two times of day) then high lane (13 digits).
pub(crate) static T_PAT: Y32 = Y32(*b"00:00:0000:00:000000000000000000");
pub(crate) static T_MAX: Y32 = Y32([2, 9, 0, 5, 9, 0, 5, 9, 2, 9, 0, 5, 9, 0, 5, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 255, 255, 255]);
pub(crate) static T_W1: Y32 = Y32([10, 1, 0, 10, 1, 0, 10, 1, 10, 1, 0, 10, 1, 0, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 1, 0, 0, 0]);
pub(crate) static T_MAXW: W16 = W16([23, 0x7FFF, 0x7FFF, 0x7FFF, 23, 0x7FFF, 0x7FFF, 0x7FFF, 0x7FFF, 0x7FFF, 0x7FFF, 0x7FFF, 0x7FFF, 0x7FFF, 0x7FFF, 0x7FFF]);
pub(crate) static T_W2: W16 = W16([60, 1, 60, 1, 60, 1, 60, 1, 100, 1, 100, 1, 100, 1, 1, 0]);
pub(crate) static T_SHUF: Y32 = Y32([0, 1, 4, 5, 128, 128, 128, 128, 8, 9, 12, 13, 128, 128, 128, 128, 0, 1, 4, 5, 8, 9, 12, 13, 128, 128, 128, 128, 128, 128, 128, 128]);
pub(crate) static T_W3: W16 = W16([60, 1, 0, 0, 60, 1, 0, 0, 10_000, 1, 10, 1, 0, 0, 0, 0]);
pub(crate) static T_MUL: D8 = D8([1000, 0, 1, 0, 100_000, 0, 0, 0]);

/// By ts length, 8 bytes apart: the parts of what `times` finds wrong that
/// count. The two times of day always do; the 13 digits when ts has that
/// form. (Lengths that no form has never get here.)
static T_COUNTS: [u8; 64 * 8 + 32] = {
    let mut t = [0u8; 64 * 8 + 32];
    let mut i = 0;
    while i < 32 {
        t[13 * 8 + i] = 255;
        if i < 16 {
            t[20 * 8 + i] = 255;
            t[25 * 8 + i] = 255;
        }
        i += 1;
    }
    t
};

/// Two times of day `HH:MM:SS` at `a` and `b` and 13 digits at `c`, all at
/// once: (the first in milliseconds since midnight, the second in seconds,
/// the digits' number, 0) and what is wrong with any of them (nonzero bytes;
/// in the low half for the times, in the high half for the digits).
#[inline(always)]
unsafe fn times(a: *const u8, b: *const u8, c: *const u8) -> (__m256i, __m256i) {
    let lo = _mm_set_epi64x(std::ptr::read_unaligned(b as *const i64), std::ptr::read_unaligned(a as *const i64));
    let x = _mm256_inserti128_si256(_mm256_castsi128_si256(lo), _mm_loadu_si128(c as *const __m128i), 1);
    let ld = |t: *const u8| _mm256_load_si256(t as *const __m256i);
    // After the xor, digits are 0..=9 and the colons are 0.
    let x = _mm256_xor_si256(x, ld(T_PAT.0.as_ptr()));
    // No digit above 2 9 : 5 9 : 5 9, or above 9.
    let over = _mm256_subs_epu8(x, ld(T_MAX.0.as_ptr()));
    // Pairs of bytes: hours, ten-minutes, minutes, seconds; or two digits.
    let w = _mm256_maddubs_epi16(x, ld(T_W1.0.as_ptr()));
    let late = _mm256_cmpgt_epi16(w, ld(T_MAXW.0.as_ptr() as *const u8));
    // Minutes of the day and seconds of ten minutes; or four digits.
    let f = _mm256_madd_epi16(w, ld(T_W2.0.as_ptr() as *const u8));
    let f = _mm256_shuffle_epi8(f, ld(T_SHUF.0.as_ptr()));
    // Seconds of the day (twice); or eight digits, then five.
    let g = _mm256_madd_epi16(f, ld(T_W3.0.as_ptr() as *const u8));
    let v = _mm256_add_epi64(_mm256_mul_epu32(g, ld(T_MUL.0.as_ptr() as *const u8)), _mm256_srli_epi64(g, 32));
    (v, _mm256_or_si256(over, late))
}

// ------------------------------------------------------------------ the fast path

/// By `digits + 2` (the length of an ID field less its quotes, plus 1): a
/// mask keeping the last `digits` bytes of 8, for 1..=8 digits; else 0.
pub(crate) static MASK8: T64 = {
    let mut t = [0u64; 64];
    let mut nd = 1;
    while nd <= 8 {
        t[nd + 2] = !0u64 << (8 * (8 - nd));
        nd += 1;
    }
    T64(t)
};

/// By `digits + 2`: the smallest number whose canonical base36 form has that
/// many digits ...
static LO36: T64 = {
    let mut t = [0u64; 64];
    let mut nd = 2;
    let mut p = 36u64;
    while nd <= 8 {
        t[nd + 2] = p;
        p *= 36;
        nd += 1;
    }
    T64(t)
};

/// ... and how many such numbers below 2^40 there are (0 for lengths that no ID has).
static SPAN36: T64 = {
    let mut t = [0u64; 64];
    let mut nd = 1;
    let mut lo = 0u64;
    let mut p = 36u64;
    while nd <= 8 {
        let hi = if p > (1 << 40) { 1 << 40 } else { p };
        t[nd + 2] = hi - lo;
        lo = p;
        p *= 36;
        nd += 1;
    }
    T64(t)
};

/// By whether the ingest field is quoted: what follows its first 19 bytes
/// (`Z,` or `Z",`), as a mask and a value for the 4 bytes there.
pub(crate) static ZMASK: [u32; 2] = [0x0000_FFFF, 0x00FF_FFFF];
pub(crate) static ZPAT: [u32; 2] = [u32::from_le_bytes([b'Z', b',', 0, 0]), u32::from_le_bytes([b'Z', b'"', b',', 0])];

const VIEW: u32 = u32::from_le_bytes(*b"view");
const CART: u32 = u32::from_le_bytes(*b"cart");

/// How far past a line's start the fast path may read.
pub(crate) const MARGIN: usize = 160;

/// Bit masks of the commas and quotes in the 64 bytes at `p`.
#[inline(always)]
unsafe fn masks_cq(p: *const u8) -> (u64, u64) {
    let a = _mm256_loadu_si256(p as *const __m256i);
    let b = _mm256_loadu_si256(p.add(32) as *const __m256i);
    let cm = _mm256_set1_epi8(b',' as i8);
    let qt = _mm256_set1_epi8(b'"' as i8);
    let c = (_mm256_movemask_epi8(_mm256_cmpeq_epi8(a, cm)) as u32 as u64) | ((_mm256_movemask_epi8(_mm256_cmpeq_epi8(b, cm)) as u32 as u64) << 32);
    let q = (_mm256_movemask_epi8(_mm256_cmpeq_epi8(a, qt)) as u32 as u64) | ((_mm256_movemask_epi8(_mm256_cmpeq_epi8(b, qt)) as u32 as u64) << 32);
    (c, q)
}

/// Bit masks of the commas, quotes and newlines in the 64 bytes at `p`.
#[inline(always)]
unsafe fn masks_cqn(p: *const u8) -> (u64, u64, u64) {
    let a = _mm256_loadu_si256(p as *const __m256i);
    let b = _mm256_loadu_si256(p.add(32) as *const __m256i);
    let both = |c: u8| {
        let v = _mm256_set1_epi8(c as i8);
        (_mm256_movemask_epi8(_mm256_cmpeq_epi8(a, v)) as u32 as u64) | ((_mm256_movemask_epi8(_mm256_cmpeq_epi8(b, v)) as u32 as u64) << 32)
    };
    (both(b','), both(b'"'), both(b'\n'))
}

/// A line the fast path does not take, by the plain rules; its fields are
/// found with the masks of its structure when it is at most 127 bytes long
/// and ends before `end - 128` (else by `slow_line`). Returns where the next
/// line starts, or `usize::MAX` when this one has no newline before `end`.
#[cold]
#[inline(never)]
pub(crate) unsafe fn odd_line(file: &[u8], s: usize, end: usize, ctx: &Ctx, dn: &Dn, out: &mut DOut) -> usize {
    if s + 128 > end {
        return slow_line(file, s, end, ctx, dn, out);
    }
    let p = file.as_ptr().add(s);
    let (c0, q0, n0) = masks_cqn(p);
    let (c1, q1, n1) = masks_cqn(p.add(64));
    let nl = n0 as u128 | (n1 as u128) << 64;
    if nl == 0 {
        return slow_line(file, s, end, ctx, dn, out);
    }
    let e = nl.trailing_zeros() as usize;
    let next = s + e + 1;
    let below = |n: usize| (1u128 << n) - 1;
    let mut commas = (c0 as u128 | (c1 as u128) << 64) & below(e);
    let quotes = (q0 as u128 | (q1 as u128) << 64) & below(e);
    // Seven fields, or the line is malformed and gives nothing.
    if commas.count_ones() != 6 {
        return next;
    }
    let mut fields: [&[u8]; 7] = [&[]; 7];
    let mut a = 0;
    for (i, field) in fields.iter_mut().enumerate() {
        let b = if i < 6 { commas.trailing_zeros() as usize } else { e };
        commas &= commas.wrapping_sub(1);
        // Quotes are syntax around a whole field, and malformed anywhere else.
        let inside = quotes & (below(b) ^ below(a));
        let (mut x, mut y) = (a, b);
        if inside != 0 {
            if b - a < 2 || inside != (1u128 << a) | (1u128 << (b - 1)) {
                return next;
            }
            x += 1;
            y -= 1;
        }
        *field = std::slice::from_raw_parts(p.add(x), y - x);
        a = b + 1;
    }
    event_fields(fields, ctx, dn, out);
    next
}

/// A line by the plain code, byte by byte. Returns where the next line
/// starts, or `usize::MAX` when this one has no newline before `end`.
#[cold]
#[inline(never)]
pub(crate) unsafe fn slow_line(file: &[u8], s: usize, end: usize, ctx: &Ctx, dn: &Dn, out: &mut DOut) -> usize {
    match memchr(b'\n', &file[s..end]) {
        Some(i) => {
            event_line(&file[s..s + i], ctx, dn, out);
            s + i + 1
        }
        None => usize::MAX,
    }
}

/// The end of a line whose shape is proven but which is not a plain valid
/// view or cart: a purchase, or an invalid line. `c4` is where comma 4 is;
/// `k5`, `k6`, `e` are comma 5, comma 6 and the newline relative to it; `qb`
/// tells which of type, product and price are quoted. `t` is the event's
/// time (ms), `ing` its ingest time (s); `w1` the record's second word.
#[cold]
#[inline(never)]
#[allow(clippy::too_many_arguments)]
unsafe fn odd_tail(file: &[u8], c4: usize, k5: usize, k6: usize, e: usize, qb: usize, t: i64, ing: i64, stream: usize, w1: u64, ctx: &Ctx, out: &mut DOut) {
    let (q4, q5, q6) = (qb & 1, (qb >> 1) & 1, (qb >> 2) & 1);
    let typ = &file[c4 + 1 + q4..c4 + k5 - q4];
    let prod = &file[c4 + k5 + 1 + q5..c4 + k6 - q5];
    let price = &file[c4 + k6 + 1 + q6..c4 + e - q6];
    let w0 = ingest31(ctx, Some(ing * 1000)) << 33;
    if !prod.is_empty() && t >= ctx.ws && t < ctx.we && ing * 1000 - t <= DAY {
        let w0 = w0 | ((t - ctx.origin) as u64) << 2;
        if typ == b"view" || typ == b"cart" {
            out.ev.push16(stream, w0, w1, &mut out.arena);
            return;
        } else if typ == b"purchase" {
            if let Some(p) = parse_price(price) {
                let cat = ctx.aux.products.get(pack(prod)).unwrap_or(ctx.aux.unknown) as u64;
                out.ev.push16(stream, w0 | 2, w1 | PURCHASE, &mut out.arena);
                out.ev.push16(stream, p as u64 | (cat << 24), EXT, &mut out.arena);
                return;
            }
        }
    }
    out.ev.push16(stream, w0 | 1, w1 & LOW21, &mut out.arena);
}

/// The event lines of `file[start..end]` as far as the fast path takes them;
/// returns where the rest starts (line by line for `event_line`).
/// `file[start - 1]` is a newline; `end` is a line boundary or the end of
/// the file.
pub(crate) unsafe fn events_dense(file: &[u8], start: usize, end: usize, ctx: &Ctx, dn: &Dn, out: &mut DOut) -> usize {
    // The fast path written out in assembly (dense_ka.rs), unless SS_KERNEL=rs
    // or the window is one it was not made for; else the one below.
    static ASM: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    if *ASM.get_or_init(|| std::env::var("SS_KERNEL").map_or(true, |v| v != "rs")) {
        if let Some(rest) = crate::dense_ka::events(file, start, end, ctx, dn, out) {
            return rest;
        }
    }
    events_rs(file, start, end, ctx, dn, out)
}

/// `events_dense` with the fast path in Rust.
#[inline(never)]
unsafe fn events_rs(file: &[u8], start: usize, end: usize, ctx: &Ctx, dn: &Dn, out: &mut DOut) -> usize {
    let base = file.as_ptr();
    let end = end.min(file.len());
    let mut s = start;
    if start == 0 || *base.add(start - 1) != b'\n' {
        return start;
    }
    // The fast path reads a few bytes before a line.
    while s < 16 && s < end {
        let next = slow_line(file, s, end, ctx, dn, out);
        if next == usize::MAX {
            return s;
        }
        s = next;
    }
    if end < MARGIN {
        return s;
    }
    let memo = &mut *MEMO.with(|d| d.get());
    let limit = end - MARGIN;
    let span = (ctx.we - ctx.ws).max(0) as u64;
    let origin_s = ctx.origin / 1000;
    let ekey = dn.keys.event & M40;
    let ukey = dn.keys.user & M40;
    let ev_top = dn.max_event << 24;
    let us_top = dn.vbound << 24;
    let kb = dn.kb;
    let slot_mask = ((1u64 << kb) - 1) << 1;
    let ahead = ahead();
    'lines: while s < limit {
        'fast: {
            if ahead != 0 {
                _mm_prefetch(base.wrapping_add(s + ahead) as *const i8, _MM_HINT_T0);
                _mm_prefetch(base.wrapping_add(s + ahead + 64) as *const i8, _MM_HINT_T0);
            }
            // ---- the first window: event ID, user, ts, and where ingest starts.
            let a0 = base.add(s - 1);
            let (cm, qt) = masks_cq(a0);
            let r1 = cm.trailing_zeros() as usize;
            let m1 = cm & cm.wrapping_sub(1);
            let r2 = m1.trailing_zeros() as usize;
            let m2 = m1 & m1.wrapping_sub(1);
            let r3 = m2.trailing_zeros() as usize;
            let m3 = m2 & m2.wrapping_sub(1);
            if r3 > 61 {
                break 'fast;
            }
            // Which fields have a quote right after their first separator and
            // one right before their second. (Any other quote ends up inside a
            // field's value, and every value here is checked byte by byte.)
            let seps = (cm ^ m3) | 1;
            let open = _pext_u64(qt >> 1, seps);
            let close = _pext_u64(qt << 1, seps);
            let qa = open & (close >> 1);
            let q0 = (qa & 1) as usize;
            let q1 = ((qa >> 1) & 1) as usize;
            let q2 = ((qa >> 2) & 1) as usize;
            let q3 = ((open >> 3) & 1) as usize;

            // ---- the two IDs: prefix, then 1..=8 base36 digits ending at the separator.
            let j0 = r1 - 2 * q0;
            let j1 = r2 - r1 - 2 * q1;
            if (*a0.add(1 + q0) != b'e') | (*a0.add(r1 + 1 + q1) != b'u') {
                break 'fast;
            }
            let x = _mm_set_epi64x(
                std::ptr::read_unaligned(a0.add(r2 - q1).sub(8) as *const i64),
                std::ptr::read_unaligned(a0.add(r1 - q0).sub(8) as *const i64),
            );
            let keep = _mm_set_epi64x(*MASK8.0.get_unchecked(j1) as i64, *MASK8.0.get_unchecked(j0) as i64);
            let t = _mm_sub_epi8(x, _mm_set1_epi8(0x30));
            let letter = _mm_cmpgt_epi8(x, _mm_set1_epi8(0x60));
            let d = _mm_sub_epi8(t, _mm_and_si128(letter, _mm_set1_epi8(39)));
            let lim = _mm_add_epi8(_mm_and_si128(letter, _mm_set1_epi8(26)), _mm_set1_epi8(9));
            let bad = _mm_subs_epu8(d, lim);
            if _mm_testz_si128(bad, keep) == 0 {
                break 'fast;
            }
            let d = _mm_and_si128(d, keep);
            let w = _mm_maddubs_epi16(d, _mm_set1_epi16(0x0124));
            let f = _mm_madd_epi16(w, _mm_set1_epi32(0x0001_0510));
            let v = _mm_add_epi64(_mm_mul_epu32(f, _mm_set1_epi64x(1_679_616)), _mm_srli_epi64(f, 32));
            let eid = _mm_cvtsi128_si64(v) as u64;
            let uid = _mm_extract_epi64(v, 1) as u64;
            // Canonical (no leading zero), and below 2^40.
            if (eid.wrapping_sub(*LO36.0.get_unchecked(j0)) >= *SPAN36.0.get_unchecked(j0)) | (uid.wrapping_sub(*LO36.0.get_unchecked(j1)) >= *SPAN36.0.get_unchecked(j1)) {
                break 'fast;
            }
            // The numbers behind them, kept in the top 40 bits.
            let ea = eid.wrapping_mul(BINV24);
            let eb = ea ^ ea.rotate_right(17) ^ (ea >> 34);
            let ew = ((eb >> 24) ^ ekey).wrapping_mul(AINV24);
            let ua = uid.wrapping_mul(BINV24);
            let ub = ua ^ ua.rotate_right(17) ^ (ua >> 34);
            let uw = ((ub >> 24) ^ ukey).wrapping_mul(AINV24);
            // A dense event; a user with an alias below 8.
            if (ew >= ev_top) | (uw >= us_top) | (uw & (0x1F8 << 24) != 0) {
                break 'fast;
            }
            let stream = (ew >> (24 + kb)) as usize;
            // Twice the slot, then the node from bit 21: the user number
            // without its alias bits 3..9, 21 zero bits below.
            let w1 = ((ew >> 23) & slot_mask) | _pext_u64(uw, !((0x1F8u64 << 24) | 0x7));

            // ---- ts and ingest, with no branch on the form of ts.
            let p2 = a0.add(r2 + 1 + q2);
            // (A lone quote for a field gives -1: 63, which no form has.)
            let n2 = (r3 - r2 - 1 - 2 * q2) & 63;
            let p3 = a0.add(r3 + 1 + q3);
            // The form: by length, with `Z` or the offset where they would be.
            let fk = (std::ptr::read_unaligned(p2.add(19) as *const u64) & *FORM_MASK.0.get_unchecked(n2)) | *FORM_TAG.0.get_unchecked(n2);
            let fi = _pext_u64(fk, FORM_BITS) as usize;
            if *memo.fkey.get_unchecked(fi) != fk && !memo.form_miss(fk, fi, n2) {
                break 'fast;
            }
            // The date and time code reads ts when it has them, and ingest
            // again (to no effect) when ts is 13 digits.
            let pt = std::hint::select_unpredictable(n2 == 13, p3, p2);
            let Some(dt) = memo.date(pt) else { break 'fast };
            let day = *memo.ms.get_unchecked(dt);
            // Ingest nearly always has the date of ts. (Its own date may take
            // the slot of the first: `day` is read before.)
            let mut di = dt;
            if std::ptr::read_unaligned(p3 as *const u64) != *memo.key8.get_unchecked(dt) || std::ptr::read_unaligned(p3.add(7) as *const u32) != *memo.key4.get_unchecked(dt) {
                let Some(d) = memo.date(p3) else { break 'fast };
                di = d;
            }
            let (tv, wrong) = times(pt.add(11), p3.add(11), p2);
            if _mm256_testz_si256(wrong, _mm256_loadu_si256(T_COUNTS.as_ptr().add(n2 * 8) as *const __m256i)) == 0 {
                break 'fast;
            }
            let iso = day + _mm_cvtsi128_si64(_mm256_castsi256_si128(tv)) - *memo.fval.get_unchecked(fi);
            let ing = *memo.sec.get_unchecked(di) + _mm_extract_epi64(_mm256_castsi256_si128(tv), 1);
            let t = std::hint::select_unpredictable(n2 == 13, _mm_cvtsi128_si64(_mm256_extracti128_si256(tv, 1)), iso);

            // ---- the rest of the line, from comma 4: type, product, price.
            // Ingest is 20 bytes; `Z,` or `Z",` must follow its first 19.
            let z = std::ptr::read_unaligned(p3.add(19) as *const u32);
            if z & *ZMASK.get_unchecked(q3) != *ZPAT.get_unchecked(q3) {
                break 'fast;
            }
            let c4 = p3.add(20 + q3);
            let vb = _mm256_loadu_si256(c4 as *const __m256i);
            let cmb = _mm256_movemask_epi8(_mm256_cmpeq_epi8(vb, _mm256_set1_epi8(b',' as i8))) as u32;
            let nlb = _mm256_movemask_epi8(_mm256_cmpeq_epi8(vb, _mm256_set1_epi8(b'\n' as i8))) as u32;
            let qtb = _mm256_movemask_epi8(_mm256_cmpeq_epi8(vb, _mm256_set1_epi8(b'"' as i8))) as u32;
            // (No newline in sight: e = 0, and the checks below fail.)
            let e = nlb.trailing_zeros() & 31;
            let m = _bzhi_u32(cmb, e);
            let ma = m & m.wrapping_sub(1);
            let k5 = ma.trailing_zeros();
            let mb = ma & ma.wrapping_sub(1);
            let k6 = mb.trailing_zeros();
            let mc = mb & mb.wrapping_sub(1);
            if (mc != 0) | (k6 >= e) {
                break 'fast;
            }
            // Every quote wraps a whole field.
            let sb = m | (1u32 << e);
            let open = _pext_u32(qtb >> 1, sb);
            let close = _pext_u32(qtb << 1, sb);
            let lone = _pext_u32(sb >> 2, sb);
            let qb = open & (close >> 1) & !lone & 7;
            if _bzhi_u32(qtb, e).count_ones() != 2 * qb.count_ones() {
                break 'fast;
            }
            let q4 = (qb & 1) as usize;
            let q5 = ((qb >> 1) & 1) as usize;
            let tw = std::ptr::read_unaligned(c4.add(1 + q4) as *const u32);
            let browse = ((tw == VIEW) | (tw == CART)) & (k5 as usize == 5 + 2 * q4);
            let has_product = k6 as usize != k5 as usize + 1 + 2 * q5;
            let c4o = c4.offset_from(base) as usize;
            s = c4o + e as usize + 1;

            // ---- a valid view or cart: in the window, not ingested over a day late.
            let in_window = (t.wrapping_sub(ctx.ws) as u64) < span;
            let fresh = ing.wrapping_mul(1000).wrapping_sub(t) <= DAY;
            let r = ing.wrapping_sub(origin_s).wrapping_add(1i64 << 30) as u64;
            if browse & has_product & in_window & fresh & (r < INGEST_TOP) {
                let w0 = (r << 33) | ((t.wrapping_sub(ctx.origin) as u64) << 2);
                out.ev.push16(stream, w0, w1, &mut out.arena);
            } else {
                odd_tail(file, c4o, k5 as usize, k6 as usize, e as usize, qb as usize, t, ing, stream, w1, ctx, out);
            }
            continue 'lines;
        }
        let next = odd_line(file, s, end, ctx, dn, out);
        if next == usize::MAX {
            return s;
        }
        s = next;
    }
    s
}

/// How far ahead of the line being parsed to ask for input bytes (SS_AHEAD; 0: never).
fn ahead() -> usize {
    static AHEAD: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *AHEAD.get_or_init(|| crate::env_usize("SS_AHEAD", 4096))
}

// ------------------------------------------------------------------ checking

/// Output to throw away (for the check and for timing).
pub(crate) fn scratch_out(dn: &Dn) -> DOut {
    use crate::dense::Lists;
    use crate::table::Arena;
    DOut { arena: Arena::new(), ev: Lists::new(dn.nb), ck: Lists::new(1), reader: crate::input::Reader::new(1 << 10), pend_l: Vec::new() }
}

/// SS_EVCHECK=1: for the lines of `file[start..end]`, the records the kernel
/// (with the plain code for what it leaves) gives against those of the plain
/// code alone, stream by stream and in order. Returns how many lines' worth
/// of streams differ (and says so on stderr).
pub(crate) fn check(file: &[u8], start: usize, end: usize, ctx: &Ctx, dn: &Dn) -> usize {
    let fresh = || scratch_out(dn);
    let (mut a, mut b) = (fresh(), fresh());
    let rest = unsafe { events_dense(file, start, end, ctx, dn, &mut a) };
    for line in crate::lines(&file[rest..end]) {
        event_line(line, ctx, dn, &mut a);
    }
    for line in crate::lines(&file[start..end]) {
        event_line(line, ctx, dn, &mut b);
    }
    a.ev.finish(&mut a.arena);
    b.ev.finish(&mut b.arena);
    let mut bad = 0;
    for s in 0..dn.nb {
        let get = |o: &DOut| -> Vec<[u64; 2]> { o.ev.pieces(s).flat_map(|(p, n)| unsafe { std::slice::from_raw_parts(p, n) }.iter().copied()).collect() };
        let (x, y) = (get(&a), get(&b));
        if x != y {
            if bad < 3 {
                let at = x.iter().zip(&y).position(|(p, q)| p != q).unwrap_or(x.len().min(y.len()));
                eprintln!("    evcheck: stream {s}: {} records against {}, first difference at {at}: {:x?} / {:x?}", x.len(), y.len(), x.get(at), y.get(at));
            }
            bad += 1;
        }
    }
    a.arena.release();
    b.arena.release();
    bad
}
