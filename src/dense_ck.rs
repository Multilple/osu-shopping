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
//! campaign field is, byte for byte, a name of campaigns.csv (quoted or not),
//! the timestamp is one of the three valid forms on a day of the click
//! window. A line is dropped here only when the plain rules drop it whatever
//! its other fields hold. Everything else is decided by the plain code, so the
//! records are the same either way.
//!
//! The loop has no branch that depends on the shape of a valid line: the
//! field lengths, the quotes and the timestamp form pick table rows, and the
//! branches left are the ones that leave the fast path.

#![allow(clippy::missing_safety_doc, clippy::needless_range_loop)]

use core::arch::x86_64::*;
use std::cell::Cell;

use crate::dense::{DOut, Dn, M40};
use crate::util::*;
use crate::{lines, Ctx};

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
pub(crate) const A_INV: u64 = inverse(0x9E37_79B9_7F4B);
pub(crate) const B_INV: u64 = inverse(0xD6E8_FEB8_6659);

/// In a user number shifted up by 24: the bits of the ID's number within
/// its person above 7, and everything but those.
pub(crate) const ALIAS_HI_TOP: u64 = 0x1F8 << 24;
pub(crate) const NODE_MASK_TOP: u64 = (M40 & !0x1F8) << 24;

/// How far ahead of the line being parsed the loop asks for input bytes. (A
/// constant, mapped input or not: as a variable it costs the loop a register.)
const AHEAD: usize = 4096;

/// Stands in for a comma the line's first 33 bytes do not have: far enough
/// that every row picked with it is one nothing passes.
const NO_COMMA: u64 = 1 << 44;

/// A day of the click window (or, for epoch times, 10^8 ms of it): the bytes
/// a `ts` on it starts with and the milliseconds from the window's start to
/// the day's (see `TsRow`).
#[repr(C)]
#[derive(Clone, Copy)]
struct DayEnt {
    key: [u8; 16],
    val: i64,
    _pad: u64,
}

/// The four blocks of the day table: dates, quoted dates, epoch times, quoted epoch times.
const DAYS_CIVIL: usize = 1 << 11;
const DAYS_EPOCH: usize = 1 << 8;
const DAYS_LEN: usize = 2 * DAYS_CIVIL + 2 * DAYS_EPOCH;

/// How a `ts` field of one raw length (and sign) is read. `W` is the 16
/// bytes that end the field; `shuf` moves them to 16 slots, `lo` is taken off
/// and no slot may then be above `range` (a digit: `'0'` and 9; a separator:
/// itself and 0). Slot pairs are combined with `w1` and may not be above
/// `plim`, and pairs of those with `w2`: four sums. With `lo` the first two
/// added and `hi` the other two, the count in milliseconds is the upper half
/// of `(hi << 32 | lo) * mm`: `(lo + hi) * 1000` for the date forms,
/// `lo * 10^4 + hi` for epoch digits.
///
/// The field's first bytes (the date; an epoch time's first five digits) are
/// looked up instead: bits `pmask` of the 8 bytes at raw offset 3, read
/// big-endian, index the block `tbl` of the day table, and the entry's key
/// must equal the field's start under `dmask`. Its value is what the count
/// is added to.
#[repr(C, align(64))]
#[derive(Clone, Copy)]
struct TsRow {
    shuf: [u8; 16],
    lo: [u8; 16],
    range: [u8; 16],
    w1: [i8; 16],
    plim: [i16; 8],
    w2: [i16; 8],
    dmask: [u8; 16],
    pmask: u64,
    tbl: *const DayEnt,
    mm: u64,
    _pad: [u64; 7],
}

const ROW_POISON: usize = 0;
const ROW_EPOCH: usize = 1;
const ROW_F1: usize = 3;
const ROW_F2_PLUS: usize = 5;
const ROW_F2_MINUS: usize = 7;
const ROWS: usize = 9;

/// Room for the direct campaign table: keys in the first half, values in the second.
const CAMP_CAP: usize = 1 << 19;
/// The most bits of a campaign field that index its block.
const CAMP_BITS: u32 = 17;

/// Everything the loop reads, behind one pointer.
#[repr(C, align(64))]
struct Tables {
    /// By `raw length + 1`: where each byte of an ID field goes (see `id_row`).
    /// (First, so that a row's address is the tables' plus an index.)
    id_rows: [[u8; 16]; 67],
    /// `we - cws`: times are counted from the click window's start.
    span: u64,
    ckey: u64,
    ukey: u64,
    /// `max_click << 24`; the direct nodes are below `max_node`.
    click_top: u64,
    max_node: u64,
    /// The inverses of the scramble's multipliers, shifted up by 24. One
    /// copy for each ID, so that each is read where it is used and neither
    /// takes a register.
    binv24: [u64; 2],
    ainv24: [u64; 2],
    node_mask: u64,
    alias_hi: u64,
    /// The bits of a user number (shifted up by 24) that are its stream.
    stream_mask: u64,
    cws: i64,
    /// `NO_COMMA`, kept where the compiler cannot fold it into the mask arithmetic.
    no_comma: u64,
    /// Every day a valid `ts` can name is in the day table.
    complete: bool,
    /// Campaign field rows by `raw length + 1`: the bytes that are the
    /// field, the bits that index its block, and the block.
    cmask: [u64; 65],
    cpm: [u64; 65],
    cbase: [*const u64; 65],
    /// By `ts` raw length: the bit of the two bytes at 7 before the end that
    /// tells `-` from `+`.
    signmask: [u32; 33],
    /// By `2 * raw length + that bit`.
    rowptr: [*const TsRow; 66],
    rows: [TsRow; ROWS],
}

/// The kernel's tables for one run.
#[derive(Default)]
pub(crate) struct Cx {
    t: Option<Box<Tables>>,
    /// (What the tables point into.)
    _days: Vec<[u64; 4]>,
    _camp: Vec<u64>,
    /// SS_CKVERIFY=1: compare every line's record with the plain code's.
    verify: bool,
}

unsafe impl Sync for Cx {}
unsafe impl Send for Cx {}

/// Where each byte of an ID field of raw length `r` goes. The field is read
/// as the 16 bytes that end with the comma after it:
///
///   slots 0..8   the digits, right-aligned ('0' in front)
///   slot 8       the prefix letter
///   slot 9, 10   the opening and closing quote (the comma stands in when
///                the field has none: both read as 0xF2 after `ID_SUB`)
///   slot 11      the first digit when there are two or more (the comma otherwise)
///
/// Ten or eleven raw bytes are a quoted ID of seven or eight digits, nine or
/// fewer an ID without quotes. (A quoted ID of six digits or fewer, one field
/// in 2500, fails its row and goes to the plain code.) A row for a length no
/// dense ID has routes the lane's last byte to the prefix and the quote
/// slots, which cannot both hold.
pub(crate) const fn id_row(r: usize) -> [u8; 16] {
    if r < 2 || r > 11 {
        return [15; 16];
    }
    let q = (r >= 10) as usize;
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
    row
}

fn ts_blank(tbl: *const DayEnt) -> TsRow {
    TsRow {
        shuf: [0x80; 16],
        lo: [0; 16],
        range: [0; 16],
        w1: [0; 16],
        plim: [i16::MAX; 8],
        w2: [0; 8],
        dmask: [0; 16],
        pmask: 0,
        tbl,
        mm: 0,
        _pad: [0; 7],
    }
}

/// A row nothing passes: one byte would have to be both 0 and 1.
fn ts_poison(tbl: *const DayEnt) -> TsRow {
    let mut r = ts_blank(tbl);
    r.shuf[0] = 15;
    r.shuf[1] = 15;
    r.lo[1] = 1;
    r
}

/// Epoch milliseconds, 13 digits, `q` quotes around them: the first five
/// digits are the day table's, the other eight the count.
fn ts_epoch(q: usize, tbl: *const DayEnt) -> TsRow {
    let mut r = ts_blank(tbl);
    // Four digits in slots 0..4, the last four in slots 8..12.
    for i in 0..8 {
        let slot = i + 4 * (i / 4);
        r.shuf[slot] = (8 - q + i) as u8;
        r.lo[slot] = b'0';
        r.range[slot] = 9;
    }
    if q == 1 {
        r.shuf[12] = 15;
        r.lo[12] = b'"';
    }
    r.w1 = [10, 1, 10, 1, 0, 0, 0, 0, 10, 1, 10, 1, 0, 0, 0, 0];
    r.w2 = [100, 1, 0, 0, 100, 1, 0, 0];
    r.mm = 10_000 << 32 | 1;
    for i in 0..5 + q {
        r.dmask[i] = 0xFF;
    }
    // The fourth and fifth digit: raw bytes 3 + q and 4 + q.
    r.pmask = 0xF << (8 * (7 - q)) | 0xF << (8 * (6 - q));
    r
}

/// `YYYY-MM-DDTHH:MM:SS` then `Z` (`sign` 0) or an offset with that sign,
/// `q` quotes around it: the date is the day table's, the count is seconds
/// (plus a day, so that an offset cannot take it below zero).
fn ts_civil(sign: u8, q: usize, tbl: *const DayEnt) -> TsRow {
    let mut r = ts_blank(tbl);
    let n = if sign == 0 { 20 } else { 25 };
    // Content byte c is W[c + q - d].
    let d = n + 2 * q - 16;
    let at = |c: usize| (c + q - d) as u8;
    // Hour, minute, second in slots 0..6; the offset's hour and minute in 8..12.
    let digits: [(usize, usize); 10] = [(0, 11), (1, 12), (2, 14), (3, 15), (4, 17), (5, 18), (8, 20), (9, 21), (10, 23), (11, 24)];
    for (slot, c) in digits {
        if c < n {
            r.shuf[slot] = at(c);
            r.lo[slot] = b'0';
            r.range[slot] = 9;
        }
    }
    // Slot 6 reads as 4 whatever the field holds: 4 * 21600 is the day added.
    r.lo[6] = 0xFC;
    r.range[6] = 4;
    // The separators, each in a slot of its own.
    let seps: [(usize, u8); 4] = [(13, b':'), (16, b':'), (19, if sign == 0 { b'Z' } else { sign }), (22, b':')];
    let slots = [12, 13, 14, 15, 7];
    let mut k = 0;
    for (c, ch) in seps {
        if c < n {
            r.shuf[slots[k]] = at(c);
            r.lo[slots[k]] = ch;
            k += 1;
        }
    }
    if q == 1 {
        // The closing quote. (The opening one is part of the day's key.)
        r.shuf[slots[k]] = 15;
        r.lo[slots[k]] = b'"';
    }
    r.w1 = [10, 1, 10, 1, 10, 1, 1, 0, 10, 1, 10, 1, 0, 0, 0, 0];
    r.plim = [23, 59, 59, i16::MAX, 23, 59, i16::MAX, i16::MAX];
    // Local time ahead of UTC: take the offset off.
    let sg: i16 = match sign {
        b'+' => -1,
        b'-' => 1,
        _ => 0,
    };
    r.w2 = [3600, 60, 1, 21_600, 3600 * sg, 60 * sg, 0, 0];
    r.mm = 1000 << 32 | 1000;
    for i in 0..11 + q {
        r.dmask[i] = 0xFF;
    }
    // Month and day digits: content bytes 5, 6, 8, 9 (one bit of the
    // month's tens, two of the day's).
    let sh = |c: usize| 8 * (10 - c - q);
    r.pmask = 0x1 << sh(5) | 0xF << sh(6) | 0x3 << sh(8) | 0xF << sh(9);
    r
}

/// The date of day number `z` (days since 1970-01-01).
fn civil_of_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    (yoe + era * 400 + (month <= 2) as i64, month, day)
}

impl Cx {
    pub(crate) fn new(ctx: &Ctx, dn: &Dn) -> Cx {
        let mut t: Box<Tables> = unsafe { Box::<Tables>::new_zeroed().assume_init() };
        let cws = ctx.cws;
        t.span = (ctx.we - cws).max(0) as u64;
        t.ckey = dn.keys.click & M40;
        t.ukey = dn.keys.user & M40;
        // (Click numbers must fit 32 bits; a larger bound sends every line to the plain code.)
        t.click_top = if dn.max_click >= 1 << 32 { 0 } else { dn.max_click << 24 };
        t.max_node = dn.vbound >> 6;
        t.binv24 = [B_INV << 24; 2];
        t.ainv24 = [A_INV << 24; 2];
        t.node_mask = NODE_MASK_TOP;
        t.alias_hi = ALIAS_HI_TOP;
        t.stream_mask = !0u64 << (33 + dn.ku);
        t.cws = cws;
        t.no_comma = NO_COMMA;
        for (j, row) in t.id_rows.iter_mut().enumerate() {
            *row = if j == 0 { [15; 16] } else { id_row(j - 1) };
        }

        // ---- the days of the click window, a day to spare on both sides
        // for the offset forms.
        let mut days = vec![[0u64; 4]; DAYS_LEN + 1];
        let ents = days.as_mut_ptr() as *mut DayEnt;
        let blocks = [0, DAYS_CIVIL, 2 * DAYS_CIVIL, 2 * DAYS_CIVIL + DAYS_EPOCH];
        let complete = Cell::new(true);
        let put = |block: usize, pmask: u64, start: &[u8], val: i64| unsafe {
            let mut key = [0u8; 16];
            key[..start.len()].copy_from_slice(start);
            let idx = _pext_u64(u64::from_be_bytes(key[3..11].try_into().unwrap()), pmask) as usize;
            let e = &mut *ents.add(block + idx);
            if e.key != [0u8; 16] {
                complete.set(false);
            } else {
                *e = DayEnt { key, val, _pad: 0 };
            }
        };
        let civil = [ts_civil(0, 0, ents).pmask, ts_civil(0, 1, ents).pmask];
        let epoch = [ts_epoch(0, ents).pmask, ts_epoch(1, ents).pmask];
        if ctx.we > cws {
            for z in cws.div_euclid(DAY) - 1..=(ctx.we - 1).div_euclid(DAY) + 1 {
                let (y, m, d) = civil_of_days(z);
                let date = format!("\"{y:04}-{m:02}-{d:02}T");
                // The plain parser has the last word on what the day is.
                match parse_ts(format!("{}00:00:00Z", &date[1..]).as_bytes()) {
                    Some(ms) if date.len() == 12 && ms == z * DAY => {
                        // The count carries a day too many (see `ts_civil`).
                        put(blocks[0], civil[0], &date.as_bytes()[1..], ms - DAY - cws);
                        put(blocks[1], civil[1], date.as_bytes(), ms - DAY - cws);
                    }
                    _ => complete.set(false),
                }
            }
            for p in cws.div_euclid(100_000_000)..=(ctx.we - 1).div_euclid(100_000_000) {
                if !(10_000..100_000).contains(&p) {
                    // Not 13 digits: no row reads these, the plain parser does.
                    continue;
                }
                let digits = format!("\"{p}");
                put(blocks[2], epoch[0], &digits.as_bytes()[1..], p * 100_000_000 - cws);
                put(blocks[3], epoch[1], digits.as_bytes(), p * 100_000_000 - cws);
            }
        }
        // An unused entry is all zeros, which no field that lands on it
        // equals, as the index bits are among the bytes compared, except
        // on entry 0: there it gets a key that belongs to entry 1.
        for (b, &block) in blocks.iter().enumerate() {
            let e = unsafe { &mut *ents.add(block) };
            if e.key == [0u8; 16] {
                // The byte with the lowest index bits: the day's units, the fifth digit.
                e.key[if b < 2 { 9 + b } else { 2 + b }] = 1;
            }
        }
        t.complete = complete.get();

        // ---- timestamp rows
        let blk = |b: usize| unsafe { ents.add(blocks[b]) as *const DayEnt };
        t.rows[ROW_POISON] = ts_poison(blk(0));
        for q in 0..2 {
            t.rows[ROW_EPOCH + q] = ts_epoch(q, blk(2 + q));
            t.rows[ROW_F1 + q] = ts_civil(0, q, blk(q));
            t.rows[ROW_F2_PLUS + q] = ts_civil(b'+', q, blk(q));
            t.rows[ROW_F2_MINUS + q] = ts_civil(b'-', q, blk(q));
        }
        let rows = t.rows.as_ptr();
        let row = |i: usize| unsafe { rows.add(i) };
        t.rowptr = [row(ROW_POISON); 66];
        for q in 0..2 {
            for bit in 0..2 {
                t.rowptr[2 * (13 + 2 * q) + bit] = row(ROW_EPOCH + q);
                t.rowptr[2 * (20 + 2 * q) + bit] = row(ROW_F1 + q);
            }
            // `+` is 0x2B, `-` is 0x2D: bit 2 tells them apart.
            t.rowptr[2 * (25 + 2 * q)] = row(ROW_F2_PLUS + q);
            t.rowptr[2 * (25 + 2 * q) + 1] = row(ROW_F2_MINUS + q);
        }
        // The sign is 6 bytes before the end without quotes, 7 with: the
        // high and the low byte of the two read at 7 before the end.
        t.signmask[25] = 0x0400;
        t.signmask[27] = 0x0004;

        // ---- campaigns: every name, bare and quoted, that is at most 8 raw
        // bytes, in a block for its raw length, at the index its own bits
        // make. The value carries the time origin's offset as well.
        let mut camp = vec![0u64; 2 * CAMP_CAP];
        let c0sh = ((cws - ctx.origin) as u64) << 32;
        // The block of a length with no names: two entries, each with a key
        // of the other's index.
        camp[0] = 1;
        let mut next = 2usize;
        let mut keys: [Vec<(u64, u32)>; 9] = Default::default();
        let be_key = |raw: &[u8]| {
            let mut b = [0u8; 8];
            b[..raw.len()].copy_from_slice(raw);
            u64::from_be_bytes(b)
        };
        for (i, name) in ctx.aux.camp_names.iter().enumerate() {
            let n = name.len();
            if (1..=8).contains(&n) {
                keys[n].push((be_key(name), i as u32));
            }
            if (1..=6).contains(&n) {
                let mut quoted = vec![b'"'];
                quoted.extend_from_slice(name);
                quoted.push(b'"');
                keys[n + 2].push((be_key(&quoted), i as u32));
            }
        }
        for l in 0..65usize {
            t.cmask[l] = if (2..=9).contains(&l) { !0u64 << (8 * (9 - l)) } else { 0 };
            t.cpm[l] = 1;
            t.cbase[l] = camp.as_ptr();
            if !(2..=9).contains(&l) || keys[l - 1].is_empty() {
                continue;
            }
            let ks = &keys[l - 1];
            // The lowest bits that tell the names apart.
            let mut differ = ks.iter().fold(0u64, |v, k| v | (k.0 ^ ks[0].0));
            let mut pm = 0u64;
            while differ != 0 && pm.count_ones() < CAMP_BITS {
                pm |= differ & differ.wrapping_neg();
                differ &= differ - 1;
            }
            if pm == 0 {
                // One name: any bit of the field will do.
                pm = 1 << (8 * (9 - l));
            }
            let size = 1usize << pm.count_ones();
            if next + size > CAMP_CAP {
                // No room: the plain lookup takes this length.
                continue;
            }
            let base = next;
            next += size;
            for &(k, i) in ks {
                let at = base + unsafe { _pext_u64(k, pm) } as usize;
                // (A name of zeros, or a second name on one index, is left to the plain lookup.)
                if k != 0 && camp[at] == 0 {
                    camp[at] = k;
                    camp[CAMP_CAP + at] = c0sh | i as u64;
                }
            }
            if camp[base] == 0 {
                // A key that belongs to entry 1.
                camp[base] = pm & pm.wrapping_neg();
            }
            t.cpm[l] = pm;
            t.cbase[l] = unsafe { camp.as_ptr().add(base) };
        }

        Cx {
            t: Some(t),
            _days: days,
            _camp: camp,
            verify: std::env::var_os("SS_CKVERIFY").is_some(),
        }
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

/// A line the plain rules drop for a reason seen at a glance: fewer than
/// four fields, or an empty click ID, user or campaign.
fn dropped_at_a_glance(line: &[u8]) -> bool {
    let mut at = 0;
    for _ in 0..3 {
        match memchr(b',', &line[at..]) {
            // (An empty field, or no comma left.)
            Some(0) | None => return true,
            Some(i) => at += i + 1,
        }
    }
    false
}

/// What the code off the fast path needs, in one place: nothing of it is
/// held in a register while the loop runs.
struct Env<'a, S> {
    file: &'a [u8],
    end: usize,
    ctx: &'a Ctx,
    dn: &'a Dn,
    t: &'a Tables,
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
            let line = &env.file[s..s + i];
            if !dropped_at_a_glance(line) {
                if let Some((stream, w0, w1)) = click_record(line, env.ctx, env.dn) {
                    (*env.sink).put(stream, w0, w1);
                }
            }
            s + i + 1
        }
        None => usize::MAX,
    }
}

// ====================================================================== constants

#[repr(C, align(32))]
pub(crate) struct V32(pub(crate) [u8; 32]);

pub(crate) const fn both(lane: [u8; 16]) -> V32 {
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
pub(crate) static ID_LIM0: V32 = both([9, 9, 9, 9, 9, 9, 9, 9, 0, 0, 0, 0xFE, 0xFF, 0xFF, 0xFF, 0xFF]);
pub(crate) static ID_LIM1: V32 = both([0x4A, 0x4A, 0x4A, 0x4A, 0x4A, 0x4A, 0x4A, 0x4A, 0, 0, 0, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]);
/// What turns a letter into its digit value.
pub(crate) static ID_ADJ: V32 = both([0x27, 0x27, 0x27, 0x27, 0x27, 0x27, 0x27, 0x27, 0, 0, 0, 0, 0, 0, 0, 0]);
pub(crate) static ID_W1: V32 = both([36, 1, 36, 1, 36, 1, 36, 1, 0, 0, 0, 0, 0, 0, 0, 0]);
pub(crate) static ID_W2: V32 = both([0x10, 0x05, 1, 0, 0x10, 0x05, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0]);

// ====================================================================== off the fast path

const TS_COLD: i64 = i64::MIN;
const TS_DROP: i64 = i64::MIN + 1;

/// A `ts` field the rows did not take: `raw` bytes at `q`. `date_only`: its
/// end was fine, its start is no day of the table. Returns the time counted
/// from the click window's start, `TS_DROP` when the line is dropped
/// whatever else it holds, or `TS_COLD` when the line needs the plain rules
/// (no newline in sight).
#[cold]
#[inline(never)]
unsafe fn ts_cold(q: *const u8, raw: usize, t: &Tables, date_only: bool) -> i64 {
    if date_only && t.complete {
        // Not all digits (its end has a separator, or its start is not the
        // window's digits), so a date form or nothing: on no day that
        // reaches the window, or not a date.
        return TS_DROP;
    }
    if raw >= 32 {
        return TS_COLD;
    }
    let Some(f) = unquote(std::slice::from_raw_parts(q, raw)) else { return TS_DROP };
    match parse_ts(f) {
        Some(ms) => ms - t.cws,
        None => TS_DROP,
    }
}

/// (A campaign's value is below 2^63: the origin's offset fits 31 bits.)
const CAMP_DROP: u64 = u64::MAX - 1;
const CAMP_COLD: u64 = u64::MAX;

/// A campaign field the direct table does not have: the bytes between the
/// second and third comma (`a2`, `a3`, counted from the line's second byte)
/// of the line at `p`, whose IDs have been checked. Returns `CAMP_COLD` when
/// the line needs the plain rules, `CAMP_DROP` when it is dropped whatever
/// else it holds, and otherwise the campaign's value as the table has it.
#[cold]
#[inline(never)]
unsafe fn camp_miss(p: *const u8, a2: usize, a3: usize, r3: usize, ctx: &Ctx, t: &Tables) -> u64 {
    // No third comma, or no newline, where the loop looks for them.
    if a3 >= 32 || r3 >= 32 {
        return CAMP_COLD;
    }
    let raw = std::slice::from_raw_parts(p.add(a2 + 2), a3 - a2 - 1);
    if memchr(b'\n', raw).is_some() {
        // The line ends before its third comma.
        return CAMP_COLD;
    }
    let Some(name) = unquote(raw) else { return CAMP_DROP };
    if name.is_empty() {
        return CAMP_DROP;
    }
    if name.len() > 15 {
        // (`pack` would intern it: the plain code does that in its own order.)
        return CAMP_COLD;
    }
    match ctx.aux.campaigns.get(pack(name)) {
        Some(i) => ((t.cws - ctx.origin) as u64) << 32 | i as u64,
        None => CAMP_DROP,
    }
}

/// The user of the line at `p`, whose ID fields the fast path has checked
/// (so the first two commas are the ones around it), as a node and its stream.
#[cold]
#[inline(never)]
unsafe fn user_slow(p: *const u8, dn: &Dn) -> (u64, u64) {
    let head = std::slice::from_raw_parts(p, 26);
    let c1 = memchr(b',', head).unwrap();
    let c2 = c1 + 1 + memchr(b',', &head[c1 + 1..]).unwrap();
    let node = dn.user_node(unquote(&head[c1 + 1..c2]).unwrap()) as u64;
    (node, (node >> (dn.ku + 3)).min(dn.nq as u64))
}

#[inline(always)]
pub(crate) unsafe fn ld128<T>(p: *const T) -> __m128i {
    _mm_loadu_si128(p as *const __m128i)
}

#[inline(always)]
pub(crate) unsafe fn ld256(v: &V32) -> __m256i {
    _mm256_load_si256(v.0.as_ptr() as *const __m256i)
}

/// The index of the lowest set bit (64 when there is none), written so that
/// the count replaces its operand: the compiler's own form first clears the
/// register the count goes to.
#[inline(always)]
pub(crate) fn tz64(x: u64) -> usize {
    let mut y = x;
    unsafe {
        core::arch::asm!("tzcnt {0}, {0}", inout(reg) y, options(pure, nomem, nostack));
    }
    y as usize
}

/// `tz64` for 32 bits (32 when there is none).
#[inline(always)]
fn tz32(x: u32) -> usize {
    let mut y = x as u64;
    unsafe {
        core::arch::asm!("tzcnt {0:e}, {0:e}", inout(reg) y, options(pure, nomem, nostack));
    }
    y as usize
}

/// The 8 bytes at `p + 5`, read big-endian. (Read by hand: these bytes are
/// also in a vector register by then, and the compiler would shift them
/// out of it, at four instructions for one.)
#[inline(always)]
unsafe fn be_at_5(p: *const u8) -> u64 {
    let k: u64;
    core::arch::asm!("movbe {k}, qword ptr [{p} + 5]", k = out(reg) k, p = in(reg) p, options(pure, readonly, nostack, preserves_flags));
    k
}

/// `unscramble(id, key) << 24`.
#[inline(always)]
pub(crate) fn unscramble_top(id: u64, key: u64, binv24: u64, ainv24: u64) -> u64 {
    // With m the 40 bits at the top of x: m ^ m >> 17 ^ m >> 34, in two
    // steps. (The low 24 bits of x are zero, so the rotation is a shift
    // that leaves x in place.)
    let x = id.wrapping_mul(binv24);
    let u = x ^ x.rotate_right(17);
    let m = x ^ (u >> 17);
    ((m >> 24) ^ key).wrapping_mul(ainv24)
}

// ====================================================================== fast path

/// The click lines of `file[start..end]` that end before `end`, as far as
/// the fast path can go. Returns where the unhandled rest starts.
#[inline(always)]
unsafe fn kernel<S: Sink>(file: &[u8], start: usize, end: usize, ctx: &Ctx, dn: &Dn, sink: &mut S) -> usize {
    let cx = &dn.ckx;
    let t: &Tables = cx.t.as_deref().unwrap_unchecked();
    let base = file.as_ptr();
    // Past the last line that ends before `end`; the loop reads up to 128
    // bytes from a line's start.
    let mut lim = end;
    while lim > start && *base.add(lim - 1) != b'\n' {
        lim -= 1;
    }
    let stop = lim.min(file.len().saturating_sub(127));
    let env = Env { file, end, ctx, dn, t, sink: sink as *mut S, stop: base.add(stop) };
    let mut s = start;
    // The ID lanes look back 14 bytes from a line's start.
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
    // (The end is read from memory each time round: it is one register more for the loop.)
    while p < std::ptr::read_volatile(&env.stop) {
        // Ask for the input a page ahead.
        _mm_prefetch(p.wrapping_add(AHEAD) as *const i8, _MM_HINT_T0);

        // The three commas, in the 32 bytes after the line's first; the
        // newline, in the 32 bytes after the third comma.
        let v0 = _mm256_loadu_si256(p.add(1) as *const __m256i);
        let cm = _mm256_movemask_epi8(_mm256_cmpeq_epi8(v0, _mm256_set1_epi8(b',' as i8))) as u32 as u64 | t.no_comma;
        let m2 = _blsr_u64(cm);
        let a1 = tz64(cm);
        let m3 = _blsr_u64(m2);
        let a2 = tz64(m2);
        let a3 = tz64(m3);
        // The third comma is the byte after `z`; `ts` starts at `z + 2`.
        let z = opaque(p.add(a3) as u64) as *const u8;
        let v1 = _mm256_loadu_si256(z.add(2) as *const __m256i);
        let r3 = tz32(_mm256_movemask_epi8(_mm256_cmpeq_epi8(v1, _mm256_set1_epi8(b'\n' as i8))) as u32);
        // The newline is at `z + r3 + 2`; the next line starts after it,
        // once every field has been checked.

        // Both IDs at once: 16 bytes up to each one's comma, moved to slots.
        let lanes = _mm256_inserti128_si256(_mm256_castsi128_si256(ld128(p.add(a1).sub(14))), ld128(p.add(a2).sub(14)), 1);
        let ctrl = _mm256_inserti128_si256(
            _mm256_castsi128_si256(ld128(t.id_rows.as_ptr().add(a1 + 2))),
            ld128(t.id_rows.as_ptr().add(a2 - a1)),
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
        let tc = unscramble_top(_mm256_extract_epi64(ids, 0) as u64, t.ckey, t.binv24[0], t.ainv24[0]);
        let tu = opaque(unscramble_top(_mm256_extract_epi64(ids, 2) as u64, t.ukey, t.binv24[1], t.ainv24[1]));
        if tc >= t.click_top {
            cold!();
        }

        // Campaign: the field's bytes, at the index their own bits make, are a name or not.
        let l = a3 - a2;
        let xc = u64::from_be(std::ptr::read_unaligned(p.add(a2 + 2) as *const u64)) & *t.cmask.get_unchecked(l);
        let slot = (*t.cbase.get_unchecked(l)).add(_pext_u64(xc, *t.cpm.get_unchecked(l)) as usize);
        let mut camp = *slot.add(CAMP_CAP);
        if *slot != xc {
            camp = camp_miss(p, a2, a3, r3, env.ctx, env.t);
            if camp >= CAMP_DROP {
                if camp == CAMP_COLD {
                    cold!();
                }
                p = z.add(r3 + 3);
                continue;
            }
        }

        // Timestamp: every form through the same steps.
        let sign = _pext_u32(std::ptr::read_unaligned(z.add(r3).sub(5) as *const u16) as u32, *t.signmask.get_unchecked(r3));
        let row = *t.rowptr.get_unchecked(2 * r3 + sign as usize);
        let w = ld128(z.add(r3).sub(14));
        let t16 = _mm_sub_epi8(_mm_shuffle_epi8(w, ld128((*row).shuf.as_ptr())), ld128((*row).lo.as_ptr()));
        let pairs = _mm_maddubs_epi16(t16, ld128((*row).w1.as_ptr()));
        let tbad = _mm_or_si128(
            _mm_subs_epu8(t16, ld128((*row).range.as_ptr())),
            _mm_cmpgt_epi16(pairs, ld128((*row).plim.as_ptr())),
        );
        let day = (*row).tbl.add(_pext_u64(be_at_5(z), (*row).pmask) as usize);
        let dbad = _mm_xor_si128(_mm256_castsi256_si128(v1), ld128((*day).key.as_ptr()));
        let sums = _mm_madd_epi16(pairs, ld128((*row).w2.as_ptr()));
        let halves = _mm_cvtsi128_si64(_mm_hadd_epi32(sums, sums)) as u64;
        // Counted from the click window's start.
        let mut ms = (halves.wrapping_mul((*row).mm) >> 32).wrapping_add((*day).val as u64);
        macro_rules! ts_off {
            ($date_only:expr) => {{
                let r = ts_cold(z.add(2), r3, env.t, $date_only);
                if r == TS_COLD {
                    cold!();
                }
                if r == TS_DROP {
                    p = z.add(r3 + 3);
                    continue;
                }
                ms = r as u64;
            }};
        }
        if _mm_testz_si128(tbad, tbad) == 0 {
            ts_off!(false);
        } else if _mm_testz_si128(dbad, ld128((*row).dmask.as_ptr())) == 0 {
            ts_off!(true);
        }
        // In the click window.
        if ms >= t.span {
            p = z.add(r3 + 3);
            continue;
        }

        // The node: direct when the ID's number within its person is below 8
        // and the person is in range.
        let mut node = _pext_u64(tu, t.node_mask);
        let mut stream = _pext_u64(tu, t.stream_mask);
        if (tu & t.alias_hi != 0) | (node >= t.max_node) {
            (node, stream) = user_slow(p, env.dn);
        }
        (*env.sink).put(stream as usize, (ms << 32).wrapping_add(camp), node | tc << 8);
        p = z.add(r3 + 3);
    }
    p.offset_from(env.file.as_ptr()) as usize
}

/// SS_CKVERIFY: the kernel's records for `file[start..rest]` against the
/// plain code's, line by line when they differ.
#[cold]
#[inline(never)]
unsafe fn verified(file: &[u8], start: usize, end: usize, ctx: &Ctx, dn: &Dn, out: &mut DOut) -> usize {
    let mut got = Collect::default();
    let rest = kernel(file, start, end, ctx, dn, &mut got);
    let want: Vec<(usize, u64, u64)> = lines(&file[start..rest]).filter_map(|l| click_record(l, ctx, dn)).collect();
    if got.0 != want {
        let mut at = start;
        while at < rest {
            let le = at + memchr(b'\n', &file[at..rest]).unwrap();
            let mut one = Collect::default();
            let r = kernel(file, at, le + 1, ctx, dn, &mut one);
            let plain = click_record(&file[at..le], ctx, dn);
            // (At the very end of the input the kernel takes no line.)
            if r != at && (r != le + 1 || one.0.first().copied() != plain || one.0.len() > 1) {
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
    kernel(file, start, end, ctx, dn, out)
}
