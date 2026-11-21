//! Dense mode: keys + codec.
//!
//! The generator writes every id as a prefix letter plus the canonical lowercase base36 of
//! `scramble(number, key)`, a bijection on 40 bits (kit/generator/generator/src/text.rs):
//!
//!   event id  'e' + base36(scramble(event_index, event_key))          event_index in 0..E
//!   click id  'k' + base36(scramble(click_index, click_key))          click_index in 0..C
//!   user id   'u' + base36(scramble(user_index * 512 + alias, user_key))
//!
//! This module inverts the scramble, decodes/encodes the base36 form with a handful of
//! instructions, and recovers the three keys from a few thousand sampled ids in well under a
//! millisecond (no brute force over the index range; see `solve`).
//!
//! Build with `-C target-cpu=x86-64-v3` (SSSE3/AVX2/BMI2 are used; no AVX-512).

#![allow(clippy::missing_safety_doc, clippy::needless_range_loop)]

use core::arch::asm;
use core::arch::x86_64::_pdep_u64;

/// Returned by `decode36*` for anything that is not a canonical id number.
pub const NONE: u64 = u64::MAX;
pub const BITS: u32 = 40;
pub const MASK: u64 = (1 << BITS) - 1;
/// Multiplier applied before the key (only its low 40 bits matter).
pub const A: u64 = 0x9E37_79B9_7F4B;
/// Multiplier applied after the xorshift.
pub const B: u64 = 0xD6E8_FEB8_6659;
pub const A_INV: u64 = inv40(A);
pub const B_INV: u64 = inv40(B);
/// user number = user_index * ALIAS_SLOTS + alias.
pub const ALIAS_SLOTS: u64 = 512;
pub const ALIAS_BITS: u32 = 9;
/// Kiosk (shared device) user numbers are KIOSK_BASE + 1..=6; they only occur in links.
pub const KIOSK_BASE: u64 = 1 << 39;

/// 40-bit keys.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct Keys {
    pub event: u64,
    pub user: u64,
    pub click: u64,
}

const fn inv40(a: u64) -> u64 {
    // Newton iteration for the inverse of an odd number modulo 2^64, cut to 40 bits.
    let mut x = a;
    let mut i = 0;
    while i < 6 {
        x = x.wrapping_mul(2u64.wrapping_sub(a.wrapping_mul(x)));
        i += 1;
    }
    x & MASK
}

/// The generator's scramble: a bijection on 40-bit numbers.
#[inline(always)]
pub fn scramble(v: u64, key: u64) -> u64 {
    let mut m = ((v & MASK).wrapping_mul(A) ^ key) & MASK;
    m ^= m >> 17;
    m.wrapping_mul(B) & MASK
}

/// Inverse of `scramble`. Bits of `id` above bit 39 are ignored, so the value `decode36_raw`
/// returns can be passed directly.
#[inline(always)]
pub fn unscramble(id: u64, key: u64) -> u64 {
    let m = id.wrapping_mul(B_INV) & MASK;
    let m = m ^ ((m ^ (m >> 17)) >> 17); // m ^ m>>17 ^ m>>34
    (m ^ key).wrapping_mul(A_INV) & MASK
}

/// `unscramble` without the key: for a generator id this is `(A * number mod 2^40) ^ key`.
#[inline(always)]
pub fn pre_key(id: u64) -> u64 {
    let m = id.wrapping_mul(B_INV) & MASK;
    m ^ ((m ^ (m >> 17)) >> 17)
}

// ------------------------------------------------------------------------------------------
// base36 codec
// ------------------------------------------------------------------------------------------

const fn pow36(e: u32) -> u64 {
    let mut r = 1u64;
    let mut i = 0;
    while i < e {
        r *= 36;
        i += 1;
    }
    r
}

/// Smallest value of a canonical string of `len` chars (0 for len 1, 36^(len-1) otherwise).
const fn minv(len: usize) -> u64 {
    if len >= 2 && len <= 8 {
        pow36(len as u32 - 1)
    } else {
        0
    }
}

const fn words(w: [i16; 4]) -> u64 {
    (w[0] as u16 as u64) | (w[1] as u16 as u64) << 16 | (w[2] as u16 as u64) << 32 | (w[3] as u16 as u64) << 48
}

const fn quad_weights(n: usize) -> (i16, i16) {
    match n {
        4 => (1296, 1),
        3 => (36, 1),
        2 | 1 => (1, 0),
        _ => (0, 0),
    }
}

// One table block per variant, addressed as [base + 8*len + offset]: sixteen 8-byte entries per
// sub-table (len 0..15); the 16-byte loads read into the next entry, whose bytes only reach the
// unused upper half of the vectors.
//
// How the decoder works on the 8 bytes x:
//   letter = x > 0x60 (signed, so bytes >= 0x80 fall in the digit class and fail there)
//   d      = x - SUB - (letter & 39)            digit value: c - '0', or c - 'a' + 10
//   flags  = sat(sat(d + (!letter & 26)) + BIAS) >= 128  <=>  d > 9 (digits) or d > 35 (letters)
//   value  = pmaddubsw(d, W1) -> pmaddwd(W2) -> first * MUL + second
// SUB is one larger for the first character when len >= 2, so the decoder computes
// value - 36^(len-1): a leading '0' turns into 0xFF and is flagged like any bad character,
// which makes the canonical-form check free. Adding NEG = 36^(len-1) - 2^40 then carries out
// exactly when value >= 2^40 and leaves value - 2^40, whose low 40 bits are the value.
// EXTRA is 1 for len 0 and 9..15, which rejects those lengths without a branch.
const OFF_SUB: usize = 0;
const OFF_BIAS: usize = 128;
const OFF_W1: usize = 256;
const OFF_W2: usize = 384;
const OFF_MUL: usize = 512;
const OFF_NEG: usize = 640;
const OFF_FLAGS: usize = 768;
const OFF_EXTRA: usize = 896;
const OFF_K60: usize = 1024;
const OFF_K39: usize = 1040;
const OFF_K26: usize = 1056;
const OFF_BINV: usize = 1072;
const OFF_AINV: usize = 1080;
const OFF_MASK: usize = 1088;

#[repr(C, align(64))]
struct Codec([u64; 144]);

const fn build_codec(right: bool) -> Codec {
    let mut t = [0u64; 144];
    let mut len = 0;
    while len < 16 {
        let valid = len >= 1 && len <= 8;
        let mut sub = [0x30u8; 8];
        let mut bias = [92u8; 8];
        let mut w1 = [0u8; 8];
        let mut w2 = 0u64;
        let mut mul = 0u64;
        if valid {
            let first = if right { 8 - len } else { 0 };
            if len >= 2 {
                sub[first] = 0x31;
                bias[first] = 93;
            }
            if right {
                // field in bytes 8-len..8: fixed weights, zero for the bytes before the field
                let mut i = first;
                while i < 8 {
                    w1[i] = if i % 2 == 0 { 36 } else { 1 };
                    i += 1;
                }
                w2 = words([1296, 1, 1296, 1]);
                mul = 1_679_616;
            } else {
                // field in bytes 0..len: weights by position from the end
                let mut j = 0;
                while j < 4 {
                    if 2 * j + 1 < len {
                        w1[2 * j] = 36;
                        w1[2 * j + 1] = 1;
                    } else if 2 * j < len {
                        w1[2 * j] = 1;
                    }
                    j += 1;
                }
                let n0 = if len < 4 { len } else { 4 };
                let (a0, b0) = quad_weights(n0);
                let (a1, b1) = quad_weights(len - n0);
                w2 = words([a0, b0, a1, b1]);
                mul = pow36((len - n0) as u32);
            }
        }
        t[OFF_SUB / 8 + len] = u64::from_le_bytes(sub);
        t[OFF_BIAS / 8 + len] = u64::from_le_bytes(bias);
        t[OFF_W1 / 8 + len] = u64::from_le_bytes(w1);
        t[OFF_W2 / 8 + len] = w2;
        t[OFF_MUL / 8 + len] = mul;
        t[OFF_NEG / 8 + len] = minv(len).wrapping_sub(1 << 40);
        t[OFF_FLAGS / 8 + len] = if valid { (0xFFu64 << (8 - len)) & 0xFF } else { 0 };
        t[OFF_EXTRA / 8 + len] = !valid as u64;
        len += 1;
    }
    t[OFF_K60 / 8] = 0x6060_6060_6060_6060;
    t[OFF_K60 / 8 + 1] = 0x6060_6060_6060_6060;
    t[OFF_K39 / 8] = 0x2727_2727_2727_2727;
    t[OFF_K39 / 8 + 1] = 0x2727_2727_2727_2727;
    t[OFF_K26 / 8] = 0x1a1a_1a1a_1a1a_1a1a;
    t[OFF_K26 / 8 + 1] = 0x1a1a_1a1a_1a1a_1a1a;
    t[OFF_BINV / 8] = B_INV;
    t[OFF_AINV / 8] = A_INV;
    t[OFF_MASK / 8] = MASK;
    Codec(t)
}

static LEFT: Codec = build_codec(false);
static RIGHT: Codec = build_codec(true);

/// The decoder, in assembly so that the instruction count is what it says here: 16
/// instructions up to the masked flags, then a per-use tail. `$load` reads the 8 bytes,
/// `$field` keeps the flag bits of the field's bytes. The failure path is a two-instruction
/// stub outside the hot code.
macro_rules! decode_asm {
    ($tab:expr, $p:expr, $len:expr, $load:literal, $field:literal, [$($tail:literal,)*], $fail:literal; $($operands:tt)*) => {
        asm!(
            $load,
            "vpcmpgtb {l}, {x}, xmmword ptr [{t} + {K60}]",
            "vpsubb {x}, {x}, xmmword ptr [{t} + 8*{len} + {SUB}]",
            "vpand {b}, {l}, xmmword ptr [{t} + {K39}]",
            "vpsubb {x}, {x}, {b}",
            "vpandn {l}, {l}, xmmword ptr [{t} + {K26}]",
            "vpaddusb {l}, {l}, {x}",
            "vpaddusb {l}, {l}, xmmword ptr [{t} + 8*{len} + {BIAS}]",
            "vpmaddubsw {x}, {x}, xmmword ptr [{t} + 8*{len} + {W1}]",
            "vpmaddwd {x}, {x}, xmmword ptr [{t} + 8*{len} + {W2}]",
            "vpmuludq {b}, {x}, xmmword ptr [{t} + 8*{len} + {MUL}]",
            "vpsrlq {x}, {x}, 32",
            "vpaddq {x}, {x}, {b}",
            "vmovq {v}, {x}",
            "vpmovmskb {m:e}, {l}",
            $field,
            "add {v}, qword ptr [{t} + 8*{len} + {NEG}]",
            "adc {m:e}, dword ptr [{t} + 8*{len} + {EXTRA}]",
            "jnz 3f",
            $($tail,)*
            "2:",
            ".pushsection .text.unlikely,\"ax\",@progbits",
            $fail,
            "jmp 2b",
            ".popsection",
            t = in(reg) $tab.0.as_ptr(),
            p = in(reg) $p,
            len = in(reg) $len,
            x = out(xmm_reg) _,
            l = out(xmm_reg) _,
            b = out(xmm_reg) _,
            m = out(reg) _,
            K60 = const OFF_K60,
            K39 = const OFF_K39,
            K26 = const OFF_K26,
            SUB = const OFF_SUB,
            BIAS = const OFF_BIAS,
            W1 = const OFF_W1,
            W2 = const OFF_W2,
            MUL = const OFF_MUL,
            NEG = const OFF_NEG,
            EXTRA = const OFF_EXTRA,
            $($operands)*
            options(pure, readonly, nostack),
        )
    };
}

/// What `decode36_raw` / `decode36_end_raw` return for a non-canonical field. (A valid field
/// never gives 0: its raw value has all 24 top bits set.)
pub const RAW_NONE: u64 = 0;

/// Decode the field of `len` bytes at `p`. `len` must be below 16; reads 8 bytes at `p`.
///
/// Returns `RAW_NONE` (0) unless the field is canonical lowercase base36 of 1..=8 chars (no
/// leading '0' except "0" itself) with a value below 2^40. Otherwise returns the value minus
/// 2^40 as a wrapping u64: correct in its low 40 bits, which is all `unscramble` looks at.
/// 19 instructions.
#[inline(always)]
pub unsafe fn decode36_raw(p: *const u8, len: usize) -> u64 {
    let v: u64;
    decode_asm!(LEFT, p, len,
        "vmovq {x}, qword ptr [{p}]",
        "bzhi {m:e}, {m:e}, {len:e}",
        [],
        "3: xor {v:e}, {v:e}";
        v = out(reg) v,
    );
    v
}

/// `decode36_raw` for the field of `len` bytes ENDING at `end` (exclusive). `len` must be
/// below 16; reads the 8 bytes before `end`. 19 instructions.
#[inline(always)]
pub unsafe fn decode36_end_raw(end: *const u8, len: usize) -> u64 {
    let v: u64;
    decode_asm!(RIGHT, end, len,
        "vmovq {x}, qword ptr [{p} - 8]",
        "and {m:e}, dword ptr [{t} + 8*{len} + {FLAGS}]",
        [],
        "3: xor {v:e}, {v:e}";
        v = out(reg) v,
        FLAGS = const OFF_FLAGS,
    );
    v
}

/// Canonical lowercase base36 of 1..=8 chars at `p` (no leading '0' unless the string is
/// "0"), value < 2^40. Anything else, including any other `len`, returns `NONE`.
/// Reads 8 bytes at `p` (the caller guarantees they are readable). 20 instructions plus the
/// length guard.
#[inline(always)]
pub unsafe fn decode36(p: *const u8, len: usize) -> u64 {
    if len > 15 {
        return NONE;
    }
    let v: u64;
    decode_asm!(LEFT, p, len,
        "vmovq {x}, qword ptr [{p}]",
        "bzhi {m:e}, {m:e}, {len:e}",
        ["and {v}, qword ptr [{t} + {MASKC}]",],
        "3: mov {v}, -1";
        v = out(reg) v,
        MASKC = const OFF_MASK,
    );
    v
}

/// `decode36` for the field of `len` bytes that ends at `end` (exclusive). Reads the 8 bytes
/// before `end` (the caller guarantees they are readable).
#[inline(always)]
pub unsafe fn decode36_end(end: *const u8, len: usize) -> u64 {
    if len > 15 {
        return NONE;
    }
    let v: u64;
    decode_asm!(RIGHT, end, len,
        "vmovq {x}, qword ptr [{p} - 8]",
        "and {m:e}, dword ptr [{t} + 8*{len} + {FLAGS}]",
        ["and {v}, qword ptr [{t} + {MASKC}]",],
        "3: mov {v}, -1";
        v = out(reg) v,
        FLAGS = const OFF_FLAGS,
        MASKC = const OFF_MASK,
    );
    v
}

/// Decode and unscramble in one step: the dense number of the field at `p` (`len` < 16), or
/// `NONE` when the field is not a canonical id number. The caller still compares the result
/// with its bound (`NONE` fails any bound). 28 instructions, one never-taken branch.
#[inline(always)]
pub unsafe fn decode36_unscramble(p: *const u8, len: usize, key: u64) -> u64 {
    let v: u64;
    decode_asm!(LEFT, p, len,
        "vmovq {x}, qword ptr [{p}]",
        "bzhi {m:e}, {m:e}, {len:e}",
        [
            "imul {v}, qword ptr [{t} + {BINV}]", // low 40 bits: m = id * B^-1
            "rorx {a}, {v}, 40",
            "shr {a}, 41",                        // m >> 17
            "rorx {c}, {a}, 17",                  // m >> 34 (and junk above bit 46)
            "xor {v}, {key}",
            "xor {a}, {c}",
            "xor {v}, {a}",                       // m ^ m>>17 ^ m>>34 ^ key
            "imul {v}, qword ptr [{t} + {AINV}]",
            "and {v}, qword ptr [{t} + {MASKC}]",
        ],
        "3: mov {v}, -1";
        v = out(reg) v,
        key = in(reg) key,
        a = out(reg) _,
        c = out(reg) _,
        BINV = const OFF_BINV,
        AINV = const OFF_AINV,
        MASKC = const OFF_MASK,
    );
    v
}

/// `decode36_unscramble` for the field of `len` bytes ending at `end` (exclusive).
#[inline(always)]
pub unsafe fn decode36_end_unscramble(end: *const u8, len: usize, key: u64) -> u64 {
    let v: u64;
    decode_asm!(RIGHT, end, len,
        "vmovq {x}, qword ptr [{p} - 8]",
        "and {m:e}, dword ptr [{t} + 8*{len} + {FLAGS}]",
        [
            "imul {v}, qword ptr [{t} + {BINV}]",
            "rorx {a}, {v}, 40",
            "shr {a}, 41",
            "rorx {c}, {a}, 17",
            "xor {v}, {key}",
            "xor {a}, {c}",
            "xor {v}, {a}",
            "imul {v}, qword ptr [{t} + {AINV}]",
            "and {v}, qword ptr [{t} + {MASKC}]",
        ],
        "3: mov {v}, -1";
        v = out(reg) v,
        key = in(reg) key,
        a = out(reg) _,
        c = out(reg) _,
        FLAGS = const OFF_FLAGS,
        BINV = const OFF_BINV,
        AINV = const OFF_AINV,
        MASKC = const OFF_MASK,
    );
    v
}

/// Portable reference decoder (tests, cold paths).
pub fn decode36_ref(s: &[u8]) -> u64 {
    if s.is_empty() || s.len() > 8 || (s.len() > 1 && s[0] == b'0') {
        return NONE;
    }
    let mut v = 0u64;
    for &c in s {
        let d = match c {
            b'0'..=b'9' => c - b'0',
            b'a'..=b'z' => c - b'a' + 10,
            _ => return NONE,
        };
        v = v * 36 + d as u64;
    }
    if v > MASK {
        NONE
    } else {
        v
    }
}

static PAIRS: [u16; 1296] = {
    let digits = b"0123456789abcdefghijklmnopqrstuvwxyz";
    let mut t = [0u16; 1296];
    let mut i = 0;
    while i < 1296 {
        t[i] = digits[i / 36] as u16 | (digits[i % 36] as u16) << 8;
        i += 1;
    }
    t
};

/// Canonical base36 of `v` (`v < 36^8`, which covers every 40-bit number). Writes all 8 bytes
/// of `out` (the text, then zero bytes) and returns the text length.
#[inline]
pub fn encode36(v: u64, out: &mut [u8; 8]) -> usize {
    debug_assert!(v < pow36(8));
    let hi = (v / 1_679_616) as u32;
    let lo = (v % 1_679_616) as u32;
    let w = PAIRS[(hi / 1296) as usize] as u64
        | (PAIRS[(hi % 1296) as usize] as u64) << 16
        | (PAIRS[(lo / 1296) as usize] as u64) << 32
        | (PAIRS[(lo % 1296) as usize] as u64) << 48;
    // leading '0's sit in the low bytes
    let skip = ((w ^ 0x3030_3030_3030_3030).trailing_zeros() / 8).min(7);
    *out = (w >> (8 * skip)).to_le_bytes();
    8 - skip as usize
}

// ------------------------------------------------------------------------------------------
// sampling
// ------------------------------------------------------------------------------------------

/// Decodes the id columns of up to `max_lines` lines of CSV text for key recovery.
///
/// `text` must start at a line start (skip the header first). Column 0 ids with prefix
/// `prefix0` go to `first`; if `prefix1` is nonzero, column 1 ids with that prefix go to
/// `second` (the user column of events and clicks). Fields may be quoted. Anything that is not
/// prefix + canonical base36 is skipped, as are the last 96 bytes of `text` (the loads below
/// never leave it). Returns the number of bytes consumed. About 110 instructions per line
/// with both columns, 65 with one.
pub fn sample_ids(text: &[u8], prefix0: u8, prefix1: u8, max_lines: usize, first: &mut Vec<u64>, second: &mut Vec<u64>) -> usize {
    use core::arch::x86_64::*;
    let n = text.len();
    let base = text.as_ptr();
    let mut i = 0usize;
    let mut lines = 0usize;
    // SAFETY: every load is at an offset below i + 64 with i + 96 <= n, or inside the newline
    // loop which stops 32 bytes before the end.
    unsafe {
        let comma = _mm_set1_epi8(b',' as i8);
        let newline = _mm256_set1_epi8(b'\n' as i8);
        let newline16 = _mm_set1_epi8(b'\n' as i8);
        while lines < max_lines && i + 96 <= n {
            // one id field: [quote] prefix digits [quote] comma, all within 16 bytes
            let mut at = i;
            for (prefix, out) in [(prefix0, &mut *first), (prefix1, &mut *second)] {
                if prefix == 0 {
                    break;
                }
                let quoted = (*base.add(at) == b'"') as usize;
                let start = at + quoted;
                let x = _mm_loadu_si128(base.add(start) as *const __m128i);
                let ends = _mm_movemask_epi8(_mm_or_si128(_mm_cmpeq_epi8(x, comma), _mm_cmpeq_epi8(x, newline16))) as u32;
                if ends == 0 {
                    break; // not an id; the newline search starts from here
                }
                let c = ends.trailing_zeros() as usize;
                if *base.add(start + c) != b',' {
                    at = start + c; // the line ends inside this field
                    break;
                }
                at = start + c + 1;
                if *base.add(start) == prefix && (quoted == 0 || *base.add(start + c - 1) == b'"') && c > quoted {
                    let v = decode36(base.add(start + 1), c - 1 - quoted);
                    if v != NONE {
                        out.push(v);
                    }
                }
            }
            // next line
            loop {
                if at + 32 > n {
                    return i;
                }
                let m = _mm256_movemask_epi8(_mm256_cmpeq_epi8(_mm256_loadu_si256(base.add(at) as *const __m256i), newline)) as u32;
                if m != 0 {
                    at += m.trailing_zeros() as usize + 1;
                    break;
                }
                at += 32;
            }
            i = at;
            lines += 1;
        }
    }
    i
}

// ------------------------------------------------------------------------------------------
// key recovery
// ------------------------------------------------------------------------------------------
//
// With u = pre_key(id) every generator id gives  u = w ^ K,  w = A*n mod 2^D,  n < N  (D = 40
// for events and clicks; for users the rows with alias 0 give the same form in D = 31 after
// dropping nine low bits that equal the key's). K is found without any search over n:
//
// 1. Two samples whose u agree in the low j bits have indexes that differ by a multiple of
//    2^j (A is odd), d = k * 2^j with |k| <= (N-1) >> j. A hash on the low j bits finds such
//    pairs; j is chosen so a few dozen exist and k has a few hundred candidates at most.
// 2. For a pair, x = u_a ^ u_b = w_a ^ (w_a + c), c = A*k (taken above bit j). The carries
//    into each bit are x ^ c, so a candidate k is consistent only if, at every bit where x is
//    0, the carry out equals c's bit there (a wrong k survives with probability 2^-(zeros of
//    x)); and where x is 1 the carry out IS w_a's bit, which gives that bit of K. Which of the
//    two samples has the larger index is unknown (k and -k are always both consistent, and
//    they give complementary bits), so each pair yields the bits of K where its x is 1 up to
//    one common flip; overlapping pairs fix their flips relative to each other. That leaves
//    K above bit j known up to a complement of all its bits.
// 3. The bits below j are filled four at a time: with K known above bit j', a pair agreeing
//    in its low j' bits gives its k directly (A^-1 times the difference), and only the right
//    four bits keep every such k within range. (Starting from the complement this finds the
//    complement of the low bits too.)
// 4. Of K and its complement only one puts the indexes themselves below N, and bit D-1 of K,
//    which flips bit D-1 of every index, is set the same way.
// The result is accepted only if it puts at least 97% of the samples in range.

#[inline(always)]
fn low_mask(bits: u32) -> u64 {
    if bits >= 64 {
        !0
    } else {
        (1u64 << bits) - 1
    }
}

/// Samples used by the solver, verification included. More are ignored.
pub const SOLVE_SAMPLES: usize = 8192;
/// Samples the acceptance test looks at.
pub const VERIFY_SAMPLES: usize = 4096;
/// Below this bound a plain search over the index is used (it is cheaper than it is worth
/// avoiding, and works with a handful of samples).
const TINY: u64 = 1 << 11;
/// No key is reported from fewer distinct sampled ids than this.
pub const MIN_DISTINCT: usize = 8;
/// Automatic brute-force fallback when the solver fails is limited to this bound.
const FALLBACK: u64 = 1 << 16;

/// How many sampled ids to pass for a given index bound (any prefix of a shuffled file).
pub fn samples_needed(max_index: u64) -> usize {
    let mut s = 256usize;
    while (s as u64) * (s as u64) < max_index / 16 && s < SOLVE_SAMPLES {
        s *= 2;
    }
    s
}

/// Hash table for `find_pairs`: slots are (stamp << 16 | sample + 1), so a new search only
/// bumps the stamp instead of clearing.
struct Slots {
    table: Vec<u32>,
    stamp: u32,
}

impl Slots {
    fn new(samples: usize) -> Self {
        Slots { table: vec![0; (2 * samples).next_power_of_two().max(16)], stamp: 0 }
    }
}

/// Pairs of samples that agree in their low `level` bits but differ, up to `max_pairs`.
fn find_pairs(y: &[u64], level: u32, max_pairs: usize, slots: &mut Slots, out: &mut Vec<(u32, u32)>) {
    out.clear();
    let n = y.len();
    if n < 2 {
        return;
    }
    debug_assert!(2 * n <= slots.table.len() && n < 1 << 16);
    slots.stamp += 1 << 16;
    let stamp = slots.stamp;
    let size = (2 * n).next_power_of_two().max(16);
    let shift = 64 - size.trailing_zeros();
    let lm = low_mask(level);
    let table = &mut slots.table[..size];
    for (i, &v) in y.iter().enumerate() {
        let mut slot = ((v & lm).wrapping_mul(0x9E37_79B9_7F4A_7C15) >> shift) as usize;
        for _ in 0..8 {
            let e = unsafe { *table.get_unchecked(slot) };
            if e & 0xFFFF_0000 != stamp {
                unsafe { *table.get_unchecked_mut(slot) = stamp | (i as u32 + 1) };
                break;
            }
            let w = unsafe { *y.get_unchecked((e & 0xFFFF) as usize - 1) };
            if (w ^ v) & lm == 0 {
                if w != v {
                    out.push(((e & 0xFFFF) - 1, i as u32));
                    if out.len() == max_pairs {
                        return;
                    }
                }
                break;
            }
            slot = (slot + 1) & (size - 1);
        }
    }
}

/// Step 2 for one pair: `x` = (u_a ^ u_b) >> j in a domain of `dd` bits. Scans k upward from
/// 1 to `kmax` (-k always mirrors k) and returns the two smallest consistent k (0 = none) with
/// the c = A*k of the smallest.
#[inline]
fn scan_pair(x: u64, dd: u32, kmax: u64) -> (u64, u64, u64) {
    let md = low_mask(dd);
    let nx = !x & (md >> 1);
    let step = A.wrapping_mul(2) & md;
    // the carry into bit 0 is 0, so k has the parity of x
    let mut k = 2 - (x & 1);
    let mut c = A.wrapping_mul(k) & md;
    let (mut k1, mut c1) = (0u64, 0u64);
    while k <= kmax {
        // carries into each bit are x ^ c; where x is 0 the carry out must equal c's bit
        if (((x ^ c) >> 1) ^ c) & nx == 0 {
            if k1 != 0 {
                return (k1, c1, k);
            }
            k1 = k;
            c1 = c;
        }
        c = c.wrapping_add(step) & md;
        k += 2;
    }
    (k1, c1, 0)
}

/// Step 3's test: with the key bits from `j` up known in `kp`, is the index difference of the
/// pair (a multiple of 2^j) within range?
#[inline(always)]
fn pair_ok(ya: u64, yb: u64, kp: u64, j: u32, dbits: u32, kmax: u64) -> bool {
    let md = low_mask(dbits - 1 - j);
    let wa = ((ya ^ kp) >> j) & md;
    let wb = ((yb ^ kp) >> j) & md;
    let k = wb.wrapping_sub(wa).wrapping_mul(A_INV) & md;
    k <= kmax || (k.wrapping_neg() & md) <= kmax
}

/// Why a level gave no key bits.
enum Miss {
    /// Too little to go by at this level; a lower one has more pairs and more values of k.
    Lower,
    /// Most pairs have no consistent k at all: these are not the generator's ids.
    Hopeless,
}

/// Steps 1 and 2 at level `jt`: the key bits from `jt` to D-2 (up to a complement of all of
/// them), and a mask of the few bits no pair could tell (left to `refine`).
fn top_level(
    y: &[u64],
    dbits: u32,
    bound: u64,
    jt: u32,
    table: &mut Slots,
    pairs: &mut Vec<(u32, u32)>,
) -> Result<(u64, u64), Miss> {
    let dd = dbits - jt;
    let kmax = (bound - 1) >> jt;
    if kmax == 0 {
        return Err(Miss::Lower);
    }
    // bound the scan: at most ~2^18 candidates over all pairs
    let cap = ((1u64 << 19) / kmax).clamp(8, 48) as usize;
    find_pairs(y, jt, cap, table, pairs);
    if pairs.len() < 4 {
        return Err(Miss::Lower);
    }
    // the smallest consistent k of every pair (the true k is always consistent)
    let e = low_mask(dd) >> 1;
    let mut cands: Vec<(u64, u64, u64, u64)> = Vec::with_capacity(pairs.len());
    let mut smallest: Vec<u64> = Vec::with_capacity(pairs.len());
    for &(a, b) in pairs.iter() {
        let ya = y[a as usize];
        let x = (ya ^ y[b as usize]) >> jt;
        let (k1, c1, k2) = scan_pair(x, dd, kmax);
        if k1 != 0 {
            let m = x & e;
            cands.push((m, ((ya >> jt) ^ ((x ^ c1) >> 1)) & m, k1, k2));
            smallest.push(k1);
        }
    }
    // every pair of generator ids has its true k; a few percent of stray samples aside
    if pairs.len() >= 8 && cands.len() * 2 < pairs.len() {
        return Err(Miss::Hopeless);
    }
    if cands.len() < 3 {
        return Err(Miss::Lower);
    }
    // When the bound is several times too large, so is kmax, and wrong k slip through. The
    // true k of this level are bounded by what the pairs show: keep a pair if it has exactly
    // one consistent k up to four times the upper quartile of the smallest ones.
    smallest.sort_unstable();
    let kcap = (4 * smallest[smallest.len() * 3 / 4] + 4).min(kmax);
    // (bits of K where x is 1, their values up to one flip) for every pair with a single k
    let info: Vec<(u64, u64)> =
        cands.iter().filter(|c| c.2 <= kcap && (c.3 == 0 || c.3 > kcap)).map(|c| (c.0, c.1)).collect();
    if info.len() < 3 {
        return Err(Miss::Lower);
    }
    // orient the pairs against each other: three rounds of voting, each pair flipped to agree
    // with the majority so far (round 0 takes them as they link up with the first one)
    let bits = (dd - 1) as usize;
    let mut kp = info[0].1;
    let mut known = info[0].0;
    let mut placed = vec![false; info.len()];
    for round in 0..3 {
        let mut cnt = [0i32; 64];
        placed.iter_mut().for_each(|p| *p = false);
        let mut progress = true;
        let mut first = round == 0;
        while progress {
            progress = false;
            for (p, &(m, v)) in info.iter().enumerate() {
                if placed[p] {
                    continue;
                }
                let o = m & known;
                let differ = ((v ^ kp) & o).count_ones();
                let agree = o.count_ones() - differ;
                if agree == differ && !first {
                    continue; // nothing to go by (yet)
                }
                first = false;
                let v = if differ > agree { v ^ m } else { v };
                let mut mm = m;
                while mm != 0 {
                    let i = mm.trailing_zeros() as usize;
                    cnt[i] += if (v >> i) & 1 != 0 { 1 } else { -1 };
                    mm &= mm - 1;
                }
                placed[p] = true;
                progress = true;
                if round == 0 {
                    // provisional majority, so later pairs can link up
                    known |= m;
                    kp = 0;
                    for i in 0..bits {
                        if cnt[i] > 0 {
                            kp |= 1 << i;
                        }
                    }
                }
            }
        }
        kp = 0;
        known = 0;
        for i in 0..bits {
            if cnt[i] != 0 {
                known |= 1 << i;
            }
            if cnt[i] > 0 {
                kp |= 1 << i;
            }
        }
    }
    // bits with no vote or a tie (rare; A*k for the few small k of a level can leave a bit of
    // x almost always 0) are left for the levels below, which see other k
    let unknown = e & !known;
    if unknown.count_ones() > 4 {
        return Err(Miss::Lower);
    }
    // every pair must now agree exactly, one way or the other
    let exact = info.iter().filter(|&&(m, v)| (v ^ kp) & m & known == 0 || (v ^ kp) & m & known == m & known).count();
    if exact * 4 < info.len() * 3 {
        return Err(Miss::Lower);
    }
    Ok((kp << jt, unknown << jt))
}

/// Step 3: fills the key bits below `jt` four at a time, together with the `unknown` bits the
/// level above left open.
fn refine(
    y: &[u64],
    dbits: u32,
    bound: u64,
    jt: u32,
    mut kp: u64,
    mut unknown: u64,
    table: &mut Slots,
    pairs: &mut Vec<(u32, u32)>,
) -> Option<u64> {
    let mut j = jt;
    while j > 0 || unknown != 0 {
        let g = j.min(4);
        let j2 = j - g;
        let open = unknown | low_mask(g) << j2;
        let kmax = (bound - 1) >> j2;
        let mut want = 12usize;
        loop {
            // enough of a prefix for `want` pairs, with room to spare
            let need = (((2 * want) as u64) << j2.min(40)) as f64;
            let take = ((need.sqrt() * 4.0) as usize + 32).min(y.len());
            find_pairs(&y[..take], j2, want, table, pairs);
            let (mut best, mut best_n, mut second_n) = (0u64, 0usize, 0usize);
            for cand in 0..1u64 << open.count_ones() {
                let kc = kp & !open | unsafe { _pdep_u64(cand, open) };
                let n = pairs.iter().filter(|&&(a, b)| pair_ok(y[a as usize], y[b as usize], kc, j2, dbits, kmax)).count();
                if n > best_n {
                    second_n = best_n;
                    best_n = n;
                    best = kc;
                } else if n > second_n {
                    second_n = n;
                }
            }
            if pairs.len() >= 4 && best_n * 4 >= pairs.len() * 3 && second_n < best_n {
                kp = best;
                break;
            }
            if want >= 192 || take == y.len() && pairs.len() < want {
                return None;
            }
            want *= 4;
        }
        unknown = 0;
        j = j2;
    }
    Some(kp)
}

#[inline(always)]
fn index_of(y: u64, k: u64, md: u64) -> u64 {
    (y ^ k).wrapping_mul(A_INV) & md
}

fn accept(y: &[u64], k: u64, dbits: u32, bound: u64) -> bool {
    let md = low_mask(dbits);
    let s = y.len().min(VERIFY_SAMPLES);
    let ok = y[..s].iter().filter(|&&v| index_of(v, k, md) < bound).count();
    ok * 100 >= s * 97
}

/// Plain search over the index of a few probe samples. `threads` splits the range.
fn brute(y: &[u64], dbits: u32, bound: u64, threads: usize) -> Option<u64> {
    let md = low_mask(dbits);
    let s = y.len();
    if s < MIN_DISTINCT || bound == 0 {
        return None;
    }
    let threads = threads.max(1).min((bound / 4096).max(1) as usize);
    let chunk = bound.div_ceil(threads as u64);
    // a non-conforming probe or checker only costs another round
    for probe in 0..s.min(4) {
        let y0 = y[probe];
        let c1 = y[(probe + 1) % s] ^ y0;
        let c2 = y[(probe + 2) % s] ^ y0;
        let search = |from: u64, to: u64| -> Option<u64> {
            let mut c = A.wrapping_mul(from);
            for _ in from..to {
                // K = y0 ^ c; the next sample's index under K must be in range
                if (c1 ^ c).wrapping_mul(A_INV) & md < bound && (c2 ^ c).wrapping_mul(A_INV) & md < bound {
                    let k = (y0 ^ c) & md;
                    if accept(y, k, dbits, bound) {
                        return Some(k);
                    }
                }
                c = c.wrapping_add(A);
            }
            None
        };
        let found = if threads == 1 {
            search(0, bound)
        } else {
            std::thread::scope(|scope| {
                let handles: Vec<_> = (0..threads as u64)
                    .map(|t| {
                        let search = &search;
                        scope.spawn(move || search(t * chunk, ((t + 1) * chunk).min(bound)))
                    })
                    .collect();
                handles.into_iter().filter_map(|h| h.join().unwrap()).next()
            })
        };
        if found.is_some() {
            return found;
        }
    }
    None
}

/// Find K (all `dbits` bits) with `A^-1 * (y ^ K) mod 2^dbits < bound` for the samples.
fn solve(y: &[u64], dbits: u32, bound: u64) -> Option<u64> {
    let y = &y[..y.len().min(SOLVE_SAMPLES)];
    let s = y.len() as u64;
    if s < 2 || bound < 2 || bound >= 1u64 << (dbits - 2) {
        return None;
    }
    // a handful of distinct ids fit some key by accident (one id fits `bound` keys)
    let mut seen = [0u64; MIN_DISTINCT];
    let mut distinct = 0;
    for &v in y {
        if !seen[..distinct].contains(&v) {
            seen[distinct] = v;
            distinct += 1;
            if distinct == MIN_DISTINCT {
                break;
            }
        }
    }
    if distinct < MIN_DISTINCT {
        return None;
    }
    if bound <= TINY {
        return brute(y, dbits, bound, 1);
    }
    let bits = 64 - (bound - 1).leading_zeros(); // bound <= 2^bits
    let all = s * (s - 1) / 2;
    // highest level that still leaves ~24+ pairs, but with at least ~64 candidates of k, so
    // that the pairs see many different k even when the bound is several times too large
    let mut jt = if all >= 48 { 63 - (all / 24).leading_zeros() } else { 0 };
    jt = jt.min(bits.saturating_sub(6));
    let mut table = Slots::new(y.len());
    let mut pairs = Vec::with_capacity(256);
    for _ in 0..4 {
        let top = top_level(y, dbits, bound, jt, &mut table, &mut pairs);
        if let Err(Miss::Hopeless) = top {
            return None;
        }
        if let Ok((kp, unknown)) = top {
            if let Some(low) = refine(y, dbits, bound, jt, kp, unknown, &mut table, &mut pairs) {
                // K below bit D-1 is `low` or its complement; bit D-1 keeps the indexes below
                // 2^(D-1). Only the right one puts them below the bound.
                let md = low_mask(dbits);
                let n = y.len().min(31);
                for flip in [0, md >> 1] {
                    let mut k = low ^ flip;
                    let high = y[..n].iter().filter(|&&v| index_of(v, k, md) >> (dbits - 1) != 0).count();
                    if high * 2 > n {
                        k |= 1 << (dbits - 1);
                    }
                    let quick = y[..n].iter().filter(|&&v| index_of(v, k, md) < bound).count();
                    if quick * 4 >= n * 3 && accept(y, k, dbits, bound) {
                        return Some(k);
                    }
                }
            }
        }
        if jt == 0 {
            break;
        }
        // lower levels have more pairs and more values of k
        jt = jt.saturating_sub(2);
    }
    None
}

fn recover40(ids: &[u64], bound: u64, threads: usize) -> Option<u64> {
    let take = ids.len().min(SOLVE_SAMPLES);
    let y: Vec<u64> = ids[..take].iter().map(|&id| pre_key(id)).collect();
    if let Some(k) = solve(&y, BITS, bound) {
        return Some(k);
    }
    if bound > TINY && bound <= FALLBACK {
        return brute(&y, BITS, bound, threads);
    }
    None
}

/// Key of the event ids. `ids` are decoded 40-bit values of sampled event ids (pass
/// `samples_needed(max_index)` of them, 4096 is enough up to ~130M events); `max_index` is
/// any upper bound on the number of events (a bound a few times too large is fine).
/// Returns `None` when the ids are not the generator's (fewer than 97% of the samples land
/// below `max_index` under any key). `threads` is only used by the brute-force fallback,
/// which runs when the solver fails and `max_index <= 65536`.
#[inline(never)]
pub fn recover_event_key(ids: &[u64], max_index: u64, threads: usize) -> Option<u64> {
    recover40(ids, max_index, threads)
}

/// Key of the click ids; same contract as `recover_event_key`.
#[inline(never)]
pub fn recover_click_key(ids: &[u64], max_index: u64, threads: usize) -> Option<u64> {
    recover40(ids, max_index, threads)
}

/// Splits user samples into the key's nine low bits and the alias-0 rows in the 31-bit domain.
fn user_domain(ids: &[u64]) -> Option<(u64, Vec<u64>)> {
    let take = ids.len().min(SOLVE_SAMPLES);
    if take < 2 {
        return None;
    }
    // alias 0 => number = index * 512 => the low nine bits of A*number are zero => the low
    // nine bits of pre_key are the key's
    let u: Vec<u64> = ids[..take].iter().map(|&id| pre_key(id)).collect();
    let mut hist = [0u16; ALIAS_SLOTS as usize];
    for &v in &u {
        hist[(v & (ALIAS_SLOTS - 1)) as usize] += 1;
    }
    let (mut low, mut top) = (0usize, 0u16);
    for (i, &h) in hist.iter().enumerate() {
        if h > top {
            top = h;
            low = i;
        }
    }
    if (top as usize) * 2 < take {
        return None;
    }
    let y = u.iter().filter(|&&v| v & (ALIAS_SLOTS - 1) == low as u64).map(|&v| v >> ALIAS_BITS).collect();
    Some((low as u64, y))
}

fn accept_users(ids: &[u64], key: u64, max_user: u64) -> bool {
    let s = ids.len().min(VERIFY_SAMPLES);
    let ok = ids[..s].iter().filter(|&&id| unscramble(id, key) < max_user * ALIAS_SLOTS).count();
    ok * 100 >= s * 97
}

/// Key of the user ids. `ids` are decoded 40-bit values of sampled user ids as they appear in
/// event (or click) rows, where most rows use alias 0; `max_user` is any upper bound on the
/// number of users. `None` for non-generator ids.
#[inline(never)]
pub fn recover_user_key(ids: &[u64], max_user: u64) -> Option<u64> {
    let (low, y) = user_domain(ids)?;
    let mut k = solve(&y, BITS - ALIAS_BITS, max_user);
    if k.is_none() && max_user > TINY && max_user <= FALLBACK {
        k = brute(&y, BITS - ALIAS_BITS, max_user, 1);
    }
    let key = k? << ALIAS_BITS | low;
    if accept_users(ids, key, max_user) {
        Some(key)
    } else {
        None
    }
}

/// All three keys, or `None` if any of them is not recoverable.
pub fn recover_keys(
    event_ids: &[u64],
    user_ids: &[u64],
    click_ids: &[u64],
    max_event: u64,
    max_user: u64,
    max_click: u64,
) -> Option<Keys> {
    Some(Keys {
        event: recover_event_key(event_ids, max_event, 1)?,
        user: recover_user_key(user_ids, max_user)?,
        click: recover_click_key(click_ids, max_click, 1)?,
    })
}

/// Reference/oracle: exhaustive search over the index of the first samples (about 7
/// instructions per candidate index; `threads` split the range). Same acceptance rule.
pub fn brute_event_key(ids: &[u64], max_index: u64, threads: usize) -> Option<u64> {
    let y: Vec<u64> = ids.iter().take(SOLVE_SAMPLES).map(|&id| pre_key(id)).collect();
    brute(&y, BITS, max_index, threads)
}

/// Reference/oracle for the user key (alias trick + exhaustive search over the user index).
pub fn brute_user_key(ids: &[u64], max_user: u64, threads: usize) -> Option<u64> {
    let (low, y) = user_domain(ids)?;
    let key = brute(&y, BITS - ALIAS_BITS, max_user, threads)? << ALIAS_BITS | low;
    if accept_users(ids, key, max_user) {
        Some(key)
    } else {
        None
    }
}
