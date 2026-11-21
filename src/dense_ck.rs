//! Dense mode: the click parse kernel.
//!
//! One 16-byte record per valid click line, with no table lookup keyed by an
//! ID: `[ts offset << 32 | campaign, node | click number << 32]`, appended to
//! the stream of the user's person bucket (see `dense::click_line`, which is
//! the plain reading of the rules and what handles every line this kernel
//! leaves alone).
//!
//! A line is taken here only when every byte of it has been checked: both
//! IDs are a prefix and canonical base-36 digits (quoted or not), the
//! campaign is `c_` and up to four digits, the timestamp is one of the three
//! valid forms. Anything else, and every ID whose number is out of range, is
//! decided by the plain code, so the records are the same either way.

#![allow(clippy::missing_safety_doc, clippy::needless_range_loop)]

use core::arch::x86_64::*;

use crate::dense::{DOut, Dn, M40};
use crate::util::*;
use crate::{env_usize, lines, Ctx};

// ====================================================================== context

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
const A_INV: u64 = inverse(0x9E37_79B9_7F4B);
const B_INV: u64 = inverse(0xD6E8_FEB8_6659);

/// What the hot loop reads, in one place.
#[repr(C)]
#[derive(Default)]
struct Hot {
    /// `cws - origin`: the smallest valid offset.
    c0: i64,
    /// `we - cws`.
    span: u64,
    ckey: u64,
    ukey: u64,
    /// `max_click << 24`; the direct nodes are below `max_node`.
    click_top: u64,
    max_node: u64,
    binv24: u64,
    ainv24: u64,
    node_mask: u64,
    alias_hi: u64,
    /// `ku + 3`.
    sh: u64,
    last_stream: u64,
}

/// The kernel's tables for one run.
#[derive(Default)]
pub(crate) struct Cx {
    hot: Hot,
    /// Direct table for `c_` + 1..=4 digits: the campaign's number,
    /// `CAMP_NONE` when campaigns.csv has no such ID, `CAMP_ASK` for
    /// anything the table cannot answer.
    camp_tab: Box<[u16]>,
    /// How far ahead of the line being parsed to ask for input bytes (0: not at all).
    ahead: usize,
    /// SS_CKVERIFY=1: compare every line's record with the plain code's.
    verify: bool,
}

const CAMP_NONE: u16 = 0xFFFE;
const CAMP_ASK: u16 = 0xFFFF;
const CAMP_TAB: usize = 1 << 17;

/// In a user number shifted up by 24: the bits of the ID's number within
/// its person above 7, and everything but those.
const ALIAS_HI_TOP: u64 = 0x1F8 << 24;
const NODE_MASK_TOP: u64 = (M40 & !0x1F8) << 24;

impl Cx {
    pub(crate) fn new(ctx: &Ctx, dn: &Dn) -> Cx {
        let campaigns = &ctx.aux.campaigns;
        let mut camp_tab = vec![CAMP_ASK; CAMP_TAB].into_boxed_slice();
        // Every `c_` + 1..=4 digits has its own slot.
        for k in 1..=4usize {
            let mut name = [0u8; 6];
            name[0] = b'c';
            name[1] = b'_';
            for n in 0..10usize.pow(k as u32) {
                let mut rest = n;
                let mut bcd = 0usize;
                for j in (0..k).rev() {
                    name[2 + j] = b'0' + (rest % 10) as u8;
                    bcd |= (rest % 10) << (4 * (k - 1 - j));
                    rest /= 10;
                }
                let slot = CAMP_PREFIX[k] | bcd;
                camp_tab[slot] = match campaigns.get(pack(&name[..2 + k])) {
                    Some(i) if i < CAMP_NONE as u32 => i as u16,
                    Some(_) => CAMP_ASK,
                    None => CAMP_NONE,
                };
            }
        }
        let mapped = env_usize("SS_MMAP", 1) != 0;
        Cx {
            hot: Hot {
                c0: ctx.cws - ctx.origin,
                span: (ctx.we - ctx.cws).max(0) as u64,
                ckey: dn.keys.click & M40,
                ukey: dn.keys.user & M40,
                // (Click numbers must fit 32 bits; a larger bound sends every line to the plain code.)
                click_top: if dn.max_click >= 1 << 32 { 0 } else { dn.max_click << 24 },
                max_node: dn.vbound >> 6,
                binv24: B_INV << 24,
                ainv24: A_INV << 24,
                node_mask: NODE_MASK_TOP,
                alias_hi: ALIAS_HI_TOP,
                sh: dn.ku as u64 + 3,
                last_stream: dn.nq as u64,
            },
            camp_tab,
            ahead: if mapped { env_usize("SS_AHEAD", 4096) } else { 0 },
            verify: std::env::var_os("SS_CKVERIFY").is_some(),
        }
    }
}

// ====================================================================== dates

/// A date the thread has seen: `YYYY-MM-DDT`, bare and behind an opening
/// quote, and the milliseconds from the time origin to its midnight.
#[repr(C, align(32))]
#[derive(Clone, Copy)]
struct MemoEnt {
    key: [u8; 16],
    val: i64,
    _pad: u64,
}

const MEMO_BARE: usize = 1;
const MEMO_QUOTED: usize = 65;

/// Entry 0 serves the forms without a date (its value is `-origin`); then 64
/// entries for bare dates and 64 for quoted ones, by the day's two digits.
pub(crate) struct DateMemo {
    ent: [MemoEnt; 129],
    origin: i64,
}

impl DateMemo {
    pub(crate) fn new(origin: i64) -> Box<DateMemo> {
        let mut m = Box::new(DateMemo { ent: [MemoEnt { key: [0; 16], val: 0, _pad: 0 }; 129], origin });
        m.ent[0].val = -origin;
        for i in 0..64 {
            // An empty slot holds a key that belongs to another slot, so
            // nothing that lands here can be equal to it.
            let (tens, units) = ((i & 3) ^ 1, i >> 2);
            let mut bare = [0u8; 16];
            bare[8] = tens as u8;
            bare[9] = units as u8;
            m.ent[MEMO_BARE + i].key = bare;
            let mut quoted = [0u8; 16];
            quoted[9] = tens as u8;
            quoted[10] = units as u8;
            m.ent[MEMO_QUOTED + i].key = quoted;
        }
        m
    }

    /// Remember the date `f` starts with, if it is one (`YYYY-MM-DDT`).
    #[cold]
    #[inline(never)]
    fn install(&mut self, f: &[u8]) {
        if f.len() < 11 {
            return;
        }
        let b = &f[..11];
        let d = |i: usize| -> Option<i64> {
            let v = b[i].wrapping_sub(b'0');
            (v <= 9).then_some(v as i64)
        };
        let days = (|| {
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
            Some(days_from_civil(year, month, day))
        })();
        let Some(days) = days else { return };
        let idx = ((b[8] & 3) | (b[9] & 15) << 2) as usize;
        let val = days * DAY - self.origin;
        let mut bare = [0u8; 16];
        bare[..11].copy_from_slice(b);
        self.ent[MEMO_BARE + idx] = MemoEnt { key: bare, val, _pad: 0 };
        let mut quoted = [0u8; 16];
        quoted[0] = b'"';
        quoted[1..12].copy_from_slice(b);
        self.ent[MEMO_QUOTED + idx] = MemoEnt { key: quoted, val, _pad: 0 };
    }
}

// ====================================================================== reference

/// Where a kernel's records go.
trait Sink {
    fn put(&mut self, stream: usize, w0: u64, w1: u64);
}

impl Sink for DOut {
    #[inline(always)]
    fn put(&mut self, stream: usize, w0: u64, w1: u64) {
        self.ck.push16(stream, w0, w1, &mut self.arena);
    }
}

#[derive(Default)]
struct Collect(Vec<(usize, u64, u64)>);

impl Sink for Collect {
    fn put(&mut self, stream: usize, w0: u64, w1: u64) {
        self.0.push((stream, w0, w1));
    }
}

/// One click line by the plain rules: its stream and record, if it is a
/// valid click (what `dense::click_line` appends).
fn click_record(line: &[u8], ctx: &Ctx, dn: &Dn) -> Option<(usize, u64, u64)> {
    let [cid, user, camp, ts] = split::<4>(line)?;
    if cid.is_empty() || user.is_empty() || camp.is_empty() {
        return None;
    }
    let t = parse_ts(ts)?;
    if t < ctx.cws || t >= ctx.we {
        return None;
    }
    let camp = ctx.aux.campaigns.get(pack(camp))?;
    let cidx = dn.click_index(cid) as u64;
    let node = dn.user_node(user);
    let s = ((node >> (dn.ku + 3)) as usize).min(dn.nq);
    Some((s, ((t - ctx.origin) as u64) << 32 | camp as u64, node as u64 | cidx << 32))
}

/// The line starting at `file[s]` by the plain rules. Returns where the next
/// line starts, or `usize::MAX` when this one does not end before `end`.
#[cold]
#[inline(never)]
fn cold_line<S: Sink>(file: &[u8], s: usize, end: usize, ctx: &Ctx, dn: &Dn, sink: &mut S) -> usize {
    match memchr(b'\n', &file[s..end]) {
        Some(i) => {
            if let Some((stream, w0, w1)) = click_record(&file[s..s + i], ctx, dn) {
                sink.put(stream, w0, w1);
            }
            s + i + 1
        }
        None => usize::MAX,
    }
}

// ====================================================================== tables

#[repr(C, align(64))]
struct A64<T>(T);

/// Where each byte of an ID field goes. The field is read as the 16 bytes
/// that end with the comma after it, and the row is picked by `2 * raw
/// length + (last raw byte < 0x23)`:
///
///   slots 0..8   the digits, right-aligned ('0' in front)
///   slot 8       the prefix letter
///   slot 9, 10   the opening and closing quote (the comma stands in when
///                the field has none: both read as 0xF2 after `ID_SUB`)
///   slot 11      the first digit when there are two or more (the comma otherwise)
///
/// A row for a length no dense ID has routes the comma to the prefix slot,
/// which fails.
static ID_SHUF: A64<[[u8; 16]; 64]> = {
    let mut t = [[15u8; 16]; 64];
    let mut i = 0;
    while i < 64 {
        let (r, q) = (i >> 1, i & 1);
        if r >= 2 * q + 2 && r - 2 * q - 1 <= 8 {
            let n = r - 2 * q - 1;
            let f = 15 - r;
            let (prefix, last) = if q == 1 { (f + 1, 13) } else { (f, 14) };
            let mut row = [0x80u8; 16];
            let mut j = 0;
            while j < n {
                row[7 - j] = (last - j) as u8;
                j += 1;
            }
            row[8] = prefix as u8;
            row[9] = if q == 1 { f as u8 } else { 15 };
            row[10] = if q == 1 { 14 } else { 15 };
            row[11] = if n >= 2 { (prefix + 1) as u8 } else { 15 };
            t[i] = row;
        }
        i += 1;
    }
    A64(t)
};

/// Index bits of the direct campaign table above the digits, by digit count:
/// they come from the `c_` bytes themselves, so the four tables live apart.
const CAMP_PREFIX: [usize; 5] = [0, 0x135F0, 0x15F00, 0x1F000, 0];

/// Campaign field rows, picked by `2 * (raw length + 1) + (last raw byte <
/// 0x23)`; the field is read big-endian from its first raw byte. `cm`/`cv`:
/// the bits that must match and their values (quotes, `c_`, the digits' high
/// nibbles). `pm`: the bits that make the table index.
struct CampRows {
    pm: [u64; 64],
    cm: [u64; 64],
    cv: [u64; 64],
}

/// Where raw byte `j` of a field read big-endian sits.
const fn sh(j: usize) -> u32 {
    56 - 8 * j as u32
}

static CAMP: A64<CampRows> = {
    // A row for a shape no direct ID has: it asks for a value whose index is
    // a slot that is never direct.
    let mut t = CampRows { pm: [0xF; 64], cm: [!0; 64], cv: [0x0F0F_0F0F_0F0F_0F0F; 64] };
    let mut i = 2;
    while i < 64 {
        let (r, q) = ((i >> 1) - 1, i & 1);
        if r >= 2 * q + 3 && r - 2 * q - 2 <= 4 {
            let k = r - 2 * q - 2;
            let (mut pm, mut cm, mut cv) = (0u64, 0u64, 0u64);
            if q == 1 {
                cm |= 0xFF << sh(0);
                cv |= 0x22 << sh(0);
                cm |= 0xFF << sh(r - 1);
                cv |= 0x22 << sh(r - 1);
            }
            let (c, u) = (q, q + 1);
            cm |= 0xFF << sh(c) | 0xFF << sh(u);
            cv |= (b'c' as u64) << sh(c) | (b'_' as u64) << sh(u);
            let mut j = 0;
            while j < k {
                cm |= 0xF0 << sh(u + 1 + j);
                cv |= 0x30 << sh(u + 1 + j);
                pm |= 0x0F << sh(u + 1 + j);
                j += 1;
            }
            match k {
                3 => pm |= 0x1F << sh(u),
                2 => pm |= 0xFF << sh(u) | 0x01 << sh(c),
                1 => pm |= 0xFF << sh(u) | 0x2F << sh(c),
                _ => {}
            }
            t.pm[i] = pm;
            t.cm[i] = cm;
            t.cv[i] = cv;
        }
        i += 1;
    }
    A64(t)
};

/// How a `ts` field of one raw length is read. `W` is the 16 bytes that end
/// the field; `shuf` moves them to 16 slots, `lo` is taken off and nothing may
/// then be above `range`; pairs of slots are combined with `w1` and may not
/// be above `plim`; `w2`, `w3` and `mul` combine the pairs into milliseconds.
/// The date (the forms that have one) is looked up in the thread's memo by
/// the bits `pmask` of the 8 bytes at raw offset 7, in the block at `moff`,
/// and must match the field's first bytes under `dmask`.
#[repr(C, align(64))]
#[derive(Clone, Copy)]
struct TsRow {
    shuf: [u8; 16],
    lo: [u8; 16],
    range: [u8; 16],
    w1: [i8; 16],
    plim: [i16; 8],
    w2: [i16; 8],
    w3: [i16; 8],
    mul: [i64; 2],
    dmask: [u8; 16],
    pmask: u64,
    moff: u64,
    _pad: [u64; 12],
}

const TS_ROW_SHIFT: u32 = 8;
const _: () = assert!(std::mem::size_of::<TsRow>() == 1 << TS_ROW_SHIFT);

const fn ts_blank() -> TsRow {
    TsRow {
        shuf: [0x80; 16],
        lo: [0; 16],
        range: [0; 16],
        w1: [10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1],
        plim: [i16::MAX; 8],
        w2: [100, 1, 100, 1, 100, 1, 100, 1],
        w3: [0; 8],
        mul: [0; 2],
        dmask: [0; 16],
        pmask: 0,
        moff: 0,
        _pad: [0; 12],
    }
}

/// A row nothing passes: one byte would have to be both 0 and 1.
const fn ts_poison() -> TsRow {
    let mut r = ts_blank();
    r.shuf[0] = 15;
    r.shuf[1] = 15;
    r.lo[1] = 1;
    r
}

/// Epoch milliseconds, 13 digits, `q` quotes around them.
const fn ts_epoch(q: usize) -> TsRow {
    let mut r = ts_blank();
    let mut i = 3;
    while i < 16 {
        r.shuf[i] = (i - q) as u8;
        r.lo[i] = b'0';
        r.range[i] = 9;
        i += 1;
    }
    if q == 1 {
        r.shuf[0] = 1;
        r.lo[0] = b'"';
        r.shuf[1] = 15;
        r.lo[1] = b'"';
    }
    r.w3 = [10_000, 1, 10_000, 1, 0, 0, 0, 0];
    r.mul = [100_000_000, 1];
    r
}

/// `YYYY-MM-DDTHH:MM:SS` then `Z` (`sign` 0) or an offset with that sign,
/// `q` quotes around it.
const fn ts_civil(sign: u8, q: usize) -> TsRow {
    let mut r = ts_blank();
    let n = if sign == 0 { 20 } else { 25 };
    // Content byte c is W[c - d].
    let d = n + q - 16;
    let mut exact = 0;
    // (content offset, expected byte) of the separators W holds.
    let seps: [(usize, u8); 4] = [(13, b':'), (16, b':'), (19, if sign == 0 { b'Z' } else { sign }), (22, b':')];
    let mut i = 0;
    while i < 4 {
        if seps[i].0 < n {
            r.shuf[exact] = (seps[i].0 - d) as u8;
            r.lo[exact] = seps[i].1;
            exact += 1;
        }
        i += 1;
    }
    if q == 1 {
        // The closing quote. (The opening one is part of the date key.)
        let slot = if exact < 4 { exact } else { 8 };
        r.shuf[slot] = 15;
        r.lo[slot] = b'"';
    }
    // Hour, minute, second in slots 10..16; the offset's in slots 4..8.
    let digits: [(usize, usize); 10] =
        [(10, 11), (11, 12), (12, 14), (13, 15), (14, 17), (15, 18), (4, 20), (5, 21), (6, 23), (7, 24)];
    let mut i = 0;
    while i < 10 {
        if digits[i].1 < n {
            r.shuf[digits[i].0] = (digits[i].1 - d) as u8;
            r.lo[digits[i].0] = b'0';
            r.range[digits[i].0] = 9;
        }
        i += 1;
    }
    r.plim[5] = 23;
    r.plim[6] = 59;
    r.plim[7] = 59;
    r.w2 = [100, 1, 100, 1, 100, 1, 60, 1];
    r.w3 = [0, 0, 3600, 1, 0, 0, 0, 0];
    r.mul = [0, 1000];
    if sign != 0 {
        r.plim[2] = 23;
        r.plim[3] = 59;
        r.w2 = [100, 1, 60, 1, 100, 1, 60, 1];
        r.w3 = [0, 1, 3600, 1, 0, 0, 0, 0];
        // Local time ahead of UTC: take the offset off.
        r.mul = [if sign == b'+' { -60_000 } else { 60_000 }, 1000];
    }
    let mut i = 0;
    while i < 11 + q {
        r.dmask[i] = 0xFF;
        i += 1;
    }
    // The day's digits are raw bytes 8 + q and 9 + q: bytes 1 + q and 2 + q
    // of the 8 read at raw offset 7. Two bits of the tens, four of the units.
    r.pmask = 0x03 << (8 * (1 + q)) | 0x0F << (8 * (2 + q));
    r.moff = (32 * if q == 1 { MEMO_QUOTED } else { MEMO_BARE }) as u64;
    r
}

/// `ts` rows by `2 * raw length + (byte 6 before the end < 0x2D)`. That byte
/// is the sign of an unquoted offset form (`+` is below 0x2D, `-` is not) and
/// a digit in every other form. Row `TS_MINUS_QUOTED` is for the quoted
/// offset form with `-`, which the cold path picks.
static TS_ROWS: A64<[TsRow; 68]> = {
    let mut t = [ts_poison(); 68];
    t[2 * 13] = ts_epoch(0);
    t[2 * 15] = ts_epoch(1);
    t[2 * 20] = ts_civil(0, 0);
    t[2 * 22] = ts_civil(0, 1);
    t[2 * 25] = ts_civil(b'-', 0);
    t[2 * 25 + 1] = ts_civil(b'+', 0);
    t[2 * 27] = ts_civil(b'+', 1);
    t[TS_MINUS_QUOTED] = ts_civil(b'-', 1);
    A64(t)
};
const TS_MINUS_QUOTED: usize = 66;

#[repr(C, align(32))]
struct V32([u8; 32]);

const fn both(lane: [u8; 16]) -> V32 {
    let mut v = [0u8; 32];
    let mut i = 0;
    while i < 16 {
        v[i] = lane[i];
        v[16 + i] = lane[i];
        i += 1;
    }
    V32(v)
}

/// '0' off every byte; the comma that ends the lane becomes what a quote becomes.
static ID_SUB: V32 = both([0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x3A]);
/// What the slots that must hold one value hold (after `ID_SUB`); slot 11 is
/// flipped so that only a first digit of '0' reads as 0xFF.
static ID_EXP: V32 = {
    let mut v = both([0, 0, 0, 0, 0, 0, 0, 0, b'k' - 0x30, 0xF2, 0xF2, 0xFF, 0, 0, 0, 0]);
    v.0[16 + 8] = b'u' - 0x30;
    v
};
/// The most a slot may hold when it is not above 0x30 (a digit), and when it is (a letter).
static ID_LIM0: V32 = both([9, 9, 9, 9, 9, 9, 9, 9, 0, 0, 0, 0xFE, 0xFF, 0xFF, 0xFF, 0xFF]);
static ID_LIM1: V32 = both([0x4A, 0x4A, 0x4A, 0x4A, 0x4A, 0x4A, 0x4A, 0x4A, 0, 0, 0, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]);
/// What turns a letter into its digit value.
static ID_ADJ: V32 = both([0x27, 0x27, 0x27, 0x27, 0x27, 0x27, 0x27, 0x27, 0, 0, 0, 0, 0, 0, 0, 0]);
static ID_W1: V32 = both([36, 1, 36, 1, 36, 1, 36, 1, 0, 0, 0, 0, 0, 0, 0, 0]);
static ID_W2: V32 = both([0x10, 0x05, 1, 0, 0x10, 0x05, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0]);

// ====================================================================== fast path

const TS_COLD: i64 = i64::MIN;
const TS_DROP: i64 = i64::MIN + 1;

/// A `ts` field the fast rows did not take: `raw` bytes at `q`. Returns the
/// offset from `origin`, `TS_DROP` when it is not a valid timestamp, or
/// `TS_COLD` when the line needs the plain rules (no newline in sight).
#[cold]
#[inline(never)]
unsafe fn ts_slow(q: *const u8, raw: usize, origin: i64, dates: *mut DateMemo) -> i64 {
    if raw >= 32 {
        return TS_COLD;
    }
    let Some(f) = unquote(std::slice::from_raw_parts(q, raw)) else { return TS_DROP };
    (*dates).install(f);
    match parse_ts(f) {
        Some(t) => t - origin,
        None => TS_DROP,
    }
}

/// The user field between commas `p1` and `p2` of the line at `p`, which the
/// fast path has checked, as a node.
#[cold]
#[inline(never)]
unsafe fn user_slow(p: *const u8, p1: usize, p2: usize, dn: &Dn) -> u64 {
    let raw = std::slice::from_raw_parts(p.add(p1 + 1), p2 - p1 - 1);
    dn.user_node(unquote(raw).unwrap()) as u64
}

#[inline(always)]
unsafe fn ld128<T>(p: *const T) -> __m128i {
    _mm_loadu_si128(p as *const __m128i)
}

#[inline(always)]
unsafe fn ld256(v: &V32) -> __m256i {
    _mm256_load_si256(v.0.as_ptr() as *const __m256i)
}

/// `unscramble(id, key) << 24`.
#[inline(always)]
fn unscramble_top(id: u64, key: u64, binv24: u64, ainv24: u64) -> u64 {
    let x = id.wrapping_mul(binv24);
    let t = x ^ x.rotate_right(17) ^ (x >> 34);
    ((t >> 24) ^ key).wrapping_mul(ainv24)
}

/// The click lines of `file[start..end]` that end before `end`, as far as
/// the fast path can go: `file[..end + 128]` is readable, the byte before
/// `start` is a newline. Returns where the unhandled rest starts.
#[inline(always)]
unsafe fn kernel<S: Sink>(file: &[u8], start: usize, end: usize, ctx: &Ctx, dn: &Dn, dates: *mut DateMemo, sink: &mut S) -> usize {
    let cx = &dn.ckx;
    let base = file.as_ptr();
    let len = file.len();
    let mut s = start;
    // The ID lanes look back 15 bytes from a line's first comma.
    while s < end && s < 16 {
        match cold_line(file, s, end, ctx, dn, sink) {
            usize::MAX => return s,
            n => s = n,
        }
    }
    let hot = &cx.hot;
    let memo = (*dates).ent.as_ptr() as *const u8;
    let camp_tab = cx.camp_tab.as_ptr();
    let ahead = cx.ahead;
    let rows = TS_ROWS.0.as_ptr() as *const u8;
    macro_rules! cold {
        () => {{
            let n = cold_line(file, s, end, ctx, dn, sink);
            if n == usize::MAX {
                break;
            }
            s = n;
            continue;
        }};
    }
    while s < end {
        if s + 128 > len {
            break;
        }
        if ahead != 0 {
            _mm_prefetch(base.wrapping_add(s + ahead) as *const i8, _MM_HINT_T0);
        }
        let p = base.add(s);

        // The first three commas, in the line's first 32 bytes.
        let v0 = _mm256_loadu_si256(p as *const __m256i);
        let cm = _mm256_movemask_epi8(_mm256_cmpeq_epi8(v0, _mm256_set1_epi8(b',' as i8))) as u32;
        let p1 = cm.trailing_zeros() as usize;
        let m2 = _blsr_u32(cm);
        let p2 = m2.trailing_zeros() as usize;
        let m3 = _blsr_u32(m2);
        let p3 = m3.trailing_zeros() as usize;
        // The newline, in the 32 bytes after the third comma.
        let q = p.add(p3 + 1);
        let v1 = _mm256_loadu_si256(q as *const __m256i);
        let nl = _mm256_movemask_epi8(_mm256_cmpeq_epi8(v1, _mm256_set1_epi8(b'\n' as i8))) as u32;
        if (m3 == 0) | (nl == 0) {
            cold!();
        }
        let r3 = nl.trailing_zeros() as usize;
        let pe = q.add(r3);
        let e = s + p3 + 1 + r3;
        if e >= end {
            break;
        }

        // Both IDs at once: 16 bytes up to each one's comma, moved to slots.
        let i0 = 2 * p1 + (*p.add(p1).sub(1) < 0x23) as usize;
        let i1 = 2 * (p2 - p1 - 1) + (*p.add(p2).sub(1) < 0x23) as usize;
        let lanes = _mm256_inserti128_si256(_mm256_castsi128_si256(ld128(p.add(p1).sub(15))), ld128(p.add(p2).sub(15)), 1);
        let ctrl = _mm256_inserti128_si256(
            _mm256_castsi128_si256(ld128(ID_SHUF.0.as_ptr().add(i0))),
            ld128(ID_SHUF.0.as_ptr().add(i1)),
            1,
        );
        let a = _mm256_sub_epi8(lanes, ld256(&ID_SUB));
        let x = _mm256_xor_si256(_mm256_shuffle_epi8(a, ctrl), ld256(&ID_EXP));
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
        let tc = unscramble_top(_mm256_extract_epi64(ids, 0) as u64, hot.ckey, hot.binv24, hot.ainv24);
        let tu = unscramble_top(_mm256_extract_epi64(ids, 2) as u64, hot.ukey, hot.binv24, hot.ainv24);
        if tc >= hot.click_top {
            cold!();
        }

        // Campaign: checked against its row, then one table read.
        let i2 = 2 * (p3 - p2) + (*p.add(p3).sub(1) < 0x23) as usize;
        let xc = u64::from_be(std::ptr::read_unaligned(p.add(p2 + 1) as *const u64));
        if (xc ^ *CAMP.0.cv.get_unchecked(i2)) & *CAMP.0.cm.get_unchecked(i2) != 0 {
            cold!();
        }
        let camp = *camp_tab.add(_pext_u64(xc, *CAMP.0.pm.get_unchecked(i2)) as usize);
        if camp >= CAMP_NONE {
            if camp == CAMP_NONE {
                // A well-formed ID that campaigns.csv does not have. The
                // rest of the line may still be malformed, but the line is
                // dropped either way.
                s = e + 1;
                continue;
            }
            cold!();
        }

        // Timestamp: every form through the same steps.
        let i3 = 2 * r3 + (*pe.sub(6) < 0x2D) as usize;
        let row = rows.add(i3 << TS_ROW_SHIFT) as *const TsRow;
        let w = ld128(pe.sub(16));
        let t16 = _mm_sub_epi8(_mm_shuffle_epi8(w, ld128((*row).shuf.as_ptr())), ld128((*row).lo.as_ptr()));
        let pairs = _mm_maddubs_epi16(t16, ld128((*row).w1.as_ptr()));
        let tbad = _mm_or_si128(
            _mm_subs_epu8(t16, ld128((*row).range.as_ptr())),
            _mm_cmpgt_epi16(pairs, ld128((*row).plim.as_ptr())),
        );
        let q4 = _mm_madd_epi16(pairs, ld128((*row).w2.as_ptr()));
        let e2 = _mm_madd_epi16(_mm_packus_epi32(q4, q4), ld128((*row).w3.as_ptr()));
        let prod = _mm_mul_epi32(_mm_cvtepu32_epi64(e2), ld128((*row).mul.as_ptr()));
        let part = _mm_cvtsi128_si64(_mm_add_epi64(prod, _mm_unpackhi_epi64(prod, prod)));
        let k = std::ptr::read_unaligned(q.add(7) as *const u64);
        let ent = memo.add(((_pext_u64(k, (*row).pmask) << 5) + (*row).moff) as usize) as *const MemoEnt;
        let dbad = _mm_and_si128(_mm_xor_si128(_mm256_castsi256_si128(v1), ld128((*ent).key.as_ptr())), ld128((*row).dmask.as_ptr()));
        let tbad = _mm_or_si128(tbad, dbad);
        let mut t = part + (*ent).val;
        if _mm_testz_si128(tbad, tbad) == 0 {
            t = ts_slow(q, r3, ctx.origin, dates);
            if t == TS_COLD {
                cold!();
            }
            if t == TS_DROP {
                s = e + 1;
                continue;
            }
        }
        // In the click window.
        if (t - hot.c0) as u64 >= hot.span {
            s = e + 1;
            continue;
        }

        // The node: direct when the ID's number within its person is below 8
        // and the person is in range.
        let mut node = _pext_u64(tu, hot.node_mask);
        if (tu & hot.alias_hi != 0) | (node >= hot.max_node) {
            node = user_slow(p, p1, p2, dn);
        }
        let stream = (node >> hot.sh).min(hot.last_stream);
        sink.put(stream as usize, (t as u64) << 32 | camp as u64, node | tc << 8);
        s = e + 1;
    }
    s
}

/// SS_CKVERIFY: the kernel's records for `file[start..rest]` against the
/// plain code's, line by line when they differ.
#[cold]
#[inline(never)]
unsafe fn verified(file: &[u8], start: usize, end: usize, ctx: &Ctx, dn: &Dn, out: &mut DOut) -> usize {
    let dates: *mut DateMemo = &mut *out.ckd;
    let mut got = Collect::default();
    let rest = kernel(file, start, end, ctx, dn, dates, &mut got);
    let want: Vec<(usize, u64, u64)> = lines(&file[start..rest]).filter_map(|l| click_record(l, ctx, dn)).collect();
    if got.0 != want {
        let mut at = start;
        while at < rest {
            let le = at + memchr(b'\n', &file[at..rest]).unwrap();
            let mut one = Collect::default();
            let r = kernel(file, at, le + 1, ctx, dn, dates, &mut one);
            let plain = click_record(&file[at..le], ctx, dn);
            if r != le + 1 || one.0.first().copied() != plain || one.0.len() > 1 {
                panic!("click kernel differs on {:?}: {:x?} (rest {} of {}) against {:x?}", String::from_utf8_lossy(&file[at..le]), one.0, r - at, le + 1 - at, plain);
            }
            at = le + 1;
        }
        panic!("click kernel differs over {} lines, but not on any one of them", want.len());
    }
    for &(s, w0, w1) in &got.0 {
        out.put(s, w0, w1);
    }
    rest
}

/// `dense::click_line` for the click lines of `file[start..end]` that end
/// before `end`, as far as the fast path takes them; returns where the rest
/// starts (see `kernel`).
#[inline(never)]
pub(crate) unsafe fn clicks_dense(file: &[u8], start: usize, end: usize, ctx: &Ctx, dn: &Dn, out: &mut DOut) -> usize {
    if dn.ckx.verify {
        return verified(file, start, end, ctx, dn, out);
    }
    let dates: *mut DateMemo = &mut *out.ckd;
    kernel(file, start, end, ctx, dn, dates, out)
}
