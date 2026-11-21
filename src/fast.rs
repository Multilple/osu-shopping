//! The hot path: a vectorised CSV scanner and branch-light field parsers.
//! Every line it handles gives exactly what the scalar code in `util` gives;
//! lines too close to the end of a file are left to the scalar code.

use core::arch::x86_64::*;

use crate::util::*;

/// Bit masks of newlines, commas and quotes in the 64 bytes at `p`.
#[inline(always)]
unsafe fn masks(p: *const u8) -> (u64, u64, u64) {
    let a = _mm256_loadu_si256(p as *const __m256i);
    let b = _mm256_loadu_si256(p.add(32) as *const __m256i);
    let nl = _mm256_set1_epi8(b'\n' as i8);
    let cm = _mm256_set1_epi8(b',' as i8);
    let qt = _mm256_set1_epi8(b'"' as i8);
    #[inline(always)]
    unsafe fn m(a: __m256i, b: __m256i, c: __m256i) -> u64 {
        let lo = _mm256_movemask_epi8(_mm256_cmpeq_epi8(a, c)) as u32 as u64;
        let hi = _mm256_movemask_epi8(_mm256_cmpeq_epi8(b, c)) as u32 as u64;
        lo | (hi << 32)
    }
    (m(a, b, nl), m(a, b, cm), m(a, b, qt))
}

/// Calls `line(start, commas, ncommas, end, nquotes)` for every line in
/// `file[start..end]` that ends in a newline at least 64 bytes before the end
/// of the file, and returns where the first unhandled line starts.
#[inline(always)]
pub unsafe fn scan<F: FnMut(usize, &[usize; 8], usize, usize, u32)>(
    file: &[u8],
    start: usize,
    end: usize,
    mut line: F,
) -> usize {
    let base = file.as_ptr();
    let len = file.len();
    let mut line_start = start;
    let mut commas = [0usize; 8];
    let mut nc = 0usize;
    let mut nq = 0u32;
    let mut pos = start;
    while pos < end {
        if pos + 64 > len {
            return line_start;
        }
        let (nl, cm, qt) = masks(base.add(pos));
        let valid = if end - pos >= 64 { !0u64 } else { (1u64 << (end - pos)) - 1 };
        let mut st = (nl | cm) & valid;
        let mut qrem = qt & valid;
        while st != 0 {
            let b = st.trailing_zeros();
            st &= st - 1;
            let abs = pos + b as usize;
            if (cm >> b) & 1 != 0 {
                *commas.get_unchecked_mut(nc & 7) = abs;
                nc += 1;
            } else {
                let below = (1u64 << b) - 1;
                nq += (qrem & below).count_ones();
                qrem &= !below;
                if abs + 64 > len {
                    return line_start;
                }
                line(line_start, &commas, nc, abs, nq);
                line_start = abs + 1;
                nc = 0;
                nq = 0;
            }
        }
        nq += qrem.count_ones();
        pos += 64;
    }
    line_start
}

/// Strip the quotes around a whole field. Returns 1 when it did.
#[inline(always)]
pub unsafe fn strip(base: *const u8, s: &mut usize, e: &mut usize) -> u32 {
    let q = (*e >= *s + 2) & (*base.add(*s) == b'"') & (*base.add((*e).wrapping_sub(1)) == b'"');
    *s += q as usize;
    *e -= q as usize;
    q as u32
}

/// A line cut into fields: `p` holds the separators around them (the newline
/// before the line, its commas, the newline that ends it) and `q[j]` is 1 when
/// field `j` is wrapped in quotes.
pub struct Cut<const N: usize> {
    pub p: [usize; N],
    pub q: [usize; N],
    /// The line has the expected commas where `cut` can see them.
    pub shape: bool,
    /// Every quote of the line wraps a whole field.
    pub quotes: bool,
}

impl<const N: usize> Cut<N> {
    /// Where field `j`'s value starts, and its length.
    #[inline(always)]
    pub fn field(&self, j: usize) -> (usize, usize) {
        let q = self.q[j];
        (self.p[j] + 1 + q, self.p[j + 1] - self.p[j] - 1 - 2 * q)
    }
}

/// Cuts the line starting at `base[s]` (the byte before it is a newline) into
/// `NA + NB + 1` fields without branching, by looking at two 64-byte windows:
/// one from the newline before the line, which must hold the first `NA`
/// commas, and one from comma `NA`, which must hold the rest of the line. A
/// line that does not fit this is reported with `shape` false and left to the
/// scalar code. Needs `base[s - 1..s + 127]` readable. `N` is `NA + NB + 2`.
#[inline(always)]
pub unsafe fn cut<const NA: usize, const NB: usize, const N: usize>(base: *const u8, s: usize) -> Cut<N> {
    debug_assert!(N == NA + NB + 2);
    let mut p = [0usize; N];
    let mut q = [0usize; N];
    let mut r = [0u32; N];

    // Window A: bit 0 is the newline before the line.
    let a0 = s - 1;
    let (nl, cm, qt) = masks(base.add(a0));
    let mut m = cm;
    for i in 1..=NA {
        r[i] = m.trailing_zeros();
        m &= m.wrapping_sub(1);
    }
    let last_a = r[NA];
    let shape_a = last_a < (nl & !1).trailing_zeros();
    // An opening quote sits right after a separator, a closing one right
    // before the next; a field that is one lone quote is not quoted.
    let seps = 1 | (cm ^ m);
    let qa = (qt >> 1) & !(seps >> 2);
    let qb = qt << 1;
    let mut k = 0;
    for j in 0..NA {
        q[j] = ((qa.wrapping_shr(r[j]) & qb.wrapping_shr(r[j + 1])) & 1) as usize;
        k += q[j];
        p[j] = a0 + r[j] as usize;
    }
    let nq_a = _bzhi_u64(qt, last_a).count_ones();

    // Window B: bit 0 is comma NA.
    let b0 = a0 + last_a as usize;
    let (nl, cm, qt) = masks(base.add(b0));
    let end = nl.trailing_zeros();
    let inside = _bzhi_u64(cm, end) & !1;
    let mut m = inside;
    let mut t = [0u32; N];
    for i in 1..=NB {
        t[i] = m.trailing_zeros();
        m &= m.wrapping_sub(1);
    }
    t[NB + 1] = end;
    let shape_b = (end < 64) & (t[NB] < 64) & (m == 0);
    let seps = 1 | inside | 1u64.wrapping_shl(end);
    let qa = (qt >> 1) & !(seps >> 2);
    let qb = qt << 1;
    for j in 0..=NB {
        q[NA + j] = ((qa.wrapping_shr(t[j]) & qb.wrapping_shr(t[j + 1])) & 1) as usize;
        k += q[NA + j];
        p[NA + j] = b0 + t[j] as usize;
    }
    p[NA + NB + 1] = b0 + end as usize;
    let nq_b = _bzhi_u64(qt, end).count_ones();

    Cut { p, q, shape: shape_a & shape_b, quotes: (nq_a + nq_b) as usize == 2 * k }
}

#[repr(C, align(32))]
struct PackRow {
    mask: [u8; 16],
    len: [u8; 16],
}

/// For each length 0..=15: a mask keeping that many bytes, and the length in the top byte.
static PACK: [PackRow; 16] = {
    let mut t = [const { PackRow { mask: [0; 16], len: [0; 16] } }; 16];
    let mut n = 0;
    while n < 16 {
        let mut i = 0;
        while i < n {
            t[n].mask[i] = 0xFF;
            i += 1;
        }
        t[n].len[15] = n as u8;
        n += 1;
    }
    t
};

/// `pack` for a non-empty field at `base[s..e]`, with 16 readable bytes at `s`.
#[inline(always)]
pub unsafe fn pack_at(base: *const u8, s: usize, e: usize) -> Key {
    let n = e - s;
    if n <= 15 {
        let row = PACK.get_unchecked(n);
        let v = _mm_loadu_si128(base.add(s) as *const __m128i);
        let v = _mm_or_si128(
            _mm_and_si128(v, _mm_load_si128(row.mask.as_ptr() as *const __m128i)),
            _mm_load_si128(row.len.as_ptr() as *const __m128i),
        );
        std::mem::transmute::<__m128i, u128>(v)
    } else {
        pack(std::slice::from_raw_parts(base.add(s), n))
    }
}

/// The timestamp was not valid.
pub const BAD: i64 = i64::MIN;

#[repr(C, align(16))]
struct V16([u8; 16]);

/// For each length 1..=16: moves that many leading bytes to the end of the
/// vector and zeroes the rest.
static RIGHT: [V16; 17] = {
    let mut t = [const { V16([0x80; 16]) }; 17];
    let mut n = 1;
    while n <= 16 {
        let mut i = 16 - n;
        while i < 16 {
            t[n].0[i] = (i - (16 - n)) as u8;
            i += 1;
        }
        n += 1;
    }
    t
};

/// 1..=16 ASCII digits at `p` (16 readable bytes) as a number, or `BAD`.
#[inline(always)]
unsafe fn digits16(p: *const u8, n: usize) -> i64 {
    let v = _mm_sub_epi8(_mm_loadu_si128(p as *const __m128i), _mm_set1_epi8(b'0' as i8));
    let d = _mm_shuffle_epi8(v, _mm_load_si128(RIGHT.get_unchecked(n).0.as_ptr() as *const __m128i));
    let over = _mm_subs_epu8(d, _mm_set1_epi8(9));
    if _mm_testz_si128(over, over) == 0 {
        return BAD;
    }
    // Pairs, then fours, then eights of digits.
    let w = _mm_maddubs_epi16(d, _mm_set1_epi16(0x010A));
    let f = _mm_madd_epi16(w, _mm_set1_epi32(0x0001_0064));
    let f = _mm_packus_epi32(f, f);
    let e = _mm_madd_epi16(f, _mm_set1_epi32(0x0001_2710));
    let both = _mm_cvtsi128_si64(e) as u64;
    ((both & 0xFFFF_FFFF) * 100_000_000 + (both >> 32)) as i64
}

/// Per-thread memo of calendar dates seen: the 11 bytes `YYYY-MM-DDT` ->
/// seconds at midnight, or `BAD` when they are not a valid date followed by `T`.
pub struct Dates {
    key8: [u64; 64],
    key4: [u32; 64],
    val: [i64; 64],
}

impl Dates {
    pub fn new() -> Dates {
        Dates { key8: [0; 64], key4: [0; 64], val: [BAD; 64] }
    }

    #[cold]
    #[inline(never)]
    unsafe fn miss(&mut self, p: *const u8, idx: usize, k8: u64, k4: u32) -> i64 {
        let b = std::slice::from_raw_parts(p, 11);
        let d = |i: usize| -> Option<i64> {
            let v = b[i].wrapping_sub(b'0');
            (v <= 9).then_some(v as i64)
        };
        let v = (|| {
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
        })()
        .unwrap_or(BAD);
        self.key8[idx] = k8;
        self.key4[idx] = k4;
        self.val[idx] = v;
        v
    }

    /// `YYYY-MM-DDTHH:MM:SS` at `p` (19+ readable bytes) to seconds since the
    /// epoch, as if UTC; `BAD` when it is not a valid date and time.
    #[cfg_attr(feature = "prof", inline(never))]
    #[cfg_attr(not(feature = "prof"), inline(always))]
    pub unsafe fn civil(&mut self, p: *const u8) -> i64 {
        // Bytes 0..8 and 7..11 (`-DDT`) identify the date; the day's two
        // digits pick the memo slot.
        let k8 = std::ptr::read_unaligned(p as *const u64);
        let k4 = std::ptr::read_unaligned(p.add(7) as *const u32);
        let idx = _pext_u32(k4, 0x000F_0300) as usize;
        let date = if (*self.key8.get_unchecked(idx) == k8) & (*self.key4.get_unchecked(idx) == k4) {
            *self.val.get_unchecked(idx)
        } else {
            self.miss(p, idx, k8, k4)
        };
        // `HH:MM:SS`: after the xor, digits are 0..=9 and the colons are 0.
        let x = _mm_xor_si128(
            _mm_loadl_epi64(p.add(11) as *const __m128i),
            _mm_set_epi64x(0, 0x3030_3A30_303A_3030),
        );
        // No digit above 2 9 : 5 9 : 5 9 (and the hour at most 23, below).
        let over = _mm_subs_epu8(x, _mm_set_epi64x(0, 0x0905_0009_0500_0902));
        // Pairs of bytes: hour, ten-minutes, minutes, seconds; then seconds
        // since midnight in two halves.
        let w = _mm_maddubs_epi16(x, _mm_set_epi64x(0, 0x010A_0001_0A00_010A));
        let hour = _mm_cvtsi128_si32(w) as u32 & 0xFFFF;
        let d = _mm_cvtsi128_si64(_mm_madd_epi16(w, _mm_set_epi64x(0, 0x0001_003C_003C_0E10))) as u64;
        let tod = (d & 0xFFFF_FFFF) + (d >> 32);
        if (date == BAD) | (_mm_testz_si128(over, over) == 0) | (hour > 23) {
            return BAD;
        }
        date + tod as i64
    }

    /// A `ts` field of `n` bytes at `p` in any form, in milliseconds, or
    /// `BAD`; 32 readable bytes at `p`.
    #[cfg_attr(feature = "prof", inline(never))]
    #[cfg_attr(not(feature = "prof"), inline(always))]
    pub unsafe fn ts(&mut self, p: *const u8, n: usize) -> i64 {
        if n == 20 && *p.add(19) == b'Z' {
            let s = self.civil(p);
            return if s == BAD { BAD } else { s * 1000 };
        }
        if n == 25 && (*p.add(19) == b'+' || *p.add(19) == b'-') {
            let local = self.civil(p);
            let d = |i: usize| (*p.add(i)).wrapping_sub(b'0') as i64;
            let (h1, h0, m1, m0) = (d(20), d(21), d(23), d(24));
            if local == BAD || h1 > 9 || h0 > 9 || m1 > 9 || m0 > 9 || *p.add(22) != b':' {
                return BAD;
            }
            let (hh, mm) = (h1 * 10 + h0, m1 * 10 + m0);
            if hh > 23 || mm > 59 {
                return BAD;
            }
            let off = (hh * 60 + mm) * 60;
            return (if *p.add(19) == b'+' { local - off } else { local + off }) * 1000;
        }
        if (1..=16).contains(&n) {
            return digits16(p, n);
        }
        parse_ts(std::slice::from_raw_parts(p, n)).unwrap_or(BAD)
    }

    /// An `ingest_ts` field (F1 only) in seconds, or `BAD`.
    #[inline(always)]
    pub unsafe fn f1(&mut self, p: *const u8, n: usize) -> i64 {
        if n == 20 && *p.add(19) == b'Z' {
            self.civil(p)
        } else {
            BAD
        }
    }
}
