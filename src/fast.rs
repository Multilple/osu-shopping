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

#[inline(always)]
fn parse8(x: u64) -> Option<u64> {
    let v = x ^ 0x3030_3030_3030_3030;
    if (v.wrapping_add(0x7676_7676_7676_7676) | v) & 0x8080_8080_8080_8080 != 0 {
        return None;
    }
    let v = v.wrapping_mul(2561) >> 8;
    let v = (v & 0x00FF_00FF_00FF_00FF).wrapping_mul(6_553_601) >> 16;
    Some((v & 0x0000_FFFF_0000_FFFF).wrapping_mul(42_949_672_960_001) >> 32)
}

/// 1..=16 ASCII digits at `p` (16 readable bytes) as a number.
#[inline(always)]
unsafe fn digits16(p: *const u8, n: usize) -> Option<u64> {
    let v = u128::from_be(std::ptr::read_unaligned(p as *const u128));
    let mut r = v >> ((16 - n) * 8);
    if n < 16 {
        r |= 0x3030_3030_3030_3030_3030_3030_3030_3030u128 << (n * 8);
    }
    let hi = parse8(((r >> 64) as u64).swap_bytes())?;
    let lo = parse8((r as u64).swap_bytes())?;
    Some(hi * 100_000_000 + lo)
}

/// Per-thread memo of calendar dates seen: the 11 bytes `YYYY-MM-DDT` -> ms
/// at midnight, or BAD_DATE when they are not a valid date followed by `T`.
pub struct Dates {
    key8: [u64; 64],
    key4: [u32; 64],
    val: [i64; 64],
}

const BAD_DATE: i64 = i64::MIN;

impl Dates {
    pub fn new() -> Dates {
        Dates { key8: [0; 64], key4: [0; 64], val: [BAD_DATE; 64] }
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
            Some(days_from_civil(year, month, day) * DAY)
        })()
        .unwrap_or(BAD_DATE);
        self.key8[idx] = k8;
        self.key4[idx] = k4;
        self.val[idx] = v;
        v
    }

    /// `YYYY-MM-DDTHH:MM:SS` at `p` (19+ readable bytes) to ms, as if UTC.
    #[cfg_attr(feature = "prof", inline(never))]
    #[cfg_attr(not(feature = "prof"), inline(always))]
    pub unsafe fn civil(&mut self, p: *const u8) -> Option<i64> {
        // Bytes 0..8 and 7..11 (`-DDT`) identify the date; the day's two
        // digits pick the memo slot.
        let k8 = std::ptr::read_unaligned(p as *const u64);
        let k4 = std::ptr::read_unaligned(p.add(7) as *const u32);
        let idx = (((k4 >> 4) & 0x30) | ((k4 >> 16) & 0xF)) as usize;
        let date = if *self.key8.get_unchecked(idx) == k8 && *self.key4.get_unchecked(idx) == k4 {
            *self.val.get_unchecked(idx)
        } else {
            self.miss(p, idx, k8, k4)
        };
        // `HH:MM:SS`: after the xor, digits are 0..=9 and the colons are 0.
        let x = std::ptr::read_unaligned(p.add(11) as *const u64) ^ 0x3030_3A30_303A_3030;
        let bad = ((x.wrapping_add(0x7676_7676_7676_7676) | x) & 0x8080_8080_8080_8080) | (x & 0x0000_FF00_00FF_0000);
        // Each byte becomes itself plus ten times the byte before it.
        let y = x.wrapping_mul(0x0A01);
        let (hh, mm, ss) = ((y >> 8) & 0xFF, (y >> 32) & 0xFF, y >> 56);
        let over = (23u64.wrapping_sub(hh) | 59u64.wrapping_sub(mm) | 59u64.wrapping_sub(ss)) >> 63;
        if (date == BAD_DATE) | (bad | over != 0) {
            return None;
        }
        Some(date + ((hh * 3600 + mm * 60 + ss) as i64) * 1000)
    }

    /// A `ts` field in any form; 32 readable bytes at `base + s`.
    #[cfg_attr(feature = "prof", inline(never))]
    #[cfg_attr(not(feature = "prof"), inline(always))]
    pub unsafe fn ts(&mut self, base: *const u8, s: usize, e: usize) -> Option<i64> {
        let n = e - s;
        let p = base.add(s);
        if n == 20 && *p.add(19) == b'Z' {
            return self.civil(p);
        }
        if n == 25 && (*p.add(19) == b'+' || *p.add(19) == b'-') {
            let local = self.civil(p)?;
            let d = |i: usize| (*p.add(i)).wrapping_sub(b'0') as i64;
            let (h1, h0, m1, m0) = (d(20), d(21), d(23), d(24));
            if h1 > 9 || h0 > 9 || m1 > 9 || m0 > 9 || *p.add(22) != b':' {
                return None;
            }
            let (hh, mm) = (h1 * 10 + h0, m1 * 10 + m0);
            if hh > 23 || mm > 59 {
                return None;
            }
            let off = (hh * 60 + mm) * MINUTE;
            return Some(if *p.add(19) == b'+' { local - off } else { local + off });
        }
        if (1..=16).contains(&n) {
            return digits16(p, n).map(|v| v as i64);
        }
        parse_ts(std::slice::from_raw_parts(p, n))
    }

    /// An `ingest_ts` field (F1 only).
    #[inline(always)]
    pub unsafe fn f1(&mut self, base: *const u8, s: usize, e: usize) -> Option<i64> {
        let p = base.add(s);
        if e - s == 20 && *p.add(19) == b'Z' {
            self.civil(p)
        } else {
            None
        }
    }
}
