//! The dense click parse kernel's fast path, written out in assembly: the
//! pass for a line and the append of its record in one `asm!` block that
//! goes from line to line until a line needs something else; every table
//! and constant behind one pointer (`KC`).
//!
//! The contract is `dense_ck`'s: exactly `dense::click_line`'s records, in
//! order. A line the pass cannot prove sound leaves the block whole: to the
//! plain code when its IDs are not both dense (or the click is not one of
//! the window's numbers), to the Rust kernel (`dense_ck::one_line`, which
//! sorts out odd campaign fields and timestamps the same way as ever)
//! otherwise; a user the tables do not hold directly is looked up in Rust.
//!
//! What a pass checks, so that a line it takes is the plain code's line (the
//! same checks as `dense_ck`'s, made in other ways):
//! * three commas in the 32 bytes after the line's first; the first two
//!   fields are IDs: by raw length (row of `idt` by both lengths), a prefix
//!   `k` / `u`, quotes around both or neither (ten or eleven raw bytes are
//!   quoted), 1..=8 lowercase base-36 digits with no leading `0`, below 2^40;
//!   the click's number below the bound;
//! * the third field, byte for byte, a name of campaigns.csv (`dense_ck`'s
//!   direct table);
//! * a newline in the 32 bytes after the third comma, and the field before
//!   it one of the three `ts` forms (row by raw length; every byte of it is
//!   checked: the date (or an epoch time's first five digits) is a key of the
//!   day table, the rest digits, separators, the sign, the closing quote, by
//!   value range; hours, minutes and seconds in range);
//! * then the time in the click window (else the line is dropped), and the
//!   user direct (else its node is looked up).
//!
//! The time: the 16 bytes that end the field are moved to slots; pairs of
//! digits become numbers, then two sums `A` (time of day in seconds, or an
//! epoch time's digits 6..9) and `B` (the offset in seconds, or digits
//! 10..13), and `(A << 32 | B) * mm` has `A * m0 + B * m1` in its upper half,
//! with `m1` picked by the sign (the one byte of the slots that can be
//! above 0x7F in a sound field: the sign less 0x2C, 0xFF for `+`). Counted
//! modulo 2^32 in place, it gets the day's value (also shifted up by 32), is
//! compared with the window's length and gets the campaign's value: the
//! record's first word.

use core::arch::asm;
use core::arch::x86_64::*;
use core::mem::offset_of;
use std::cell::Cell;

use crate::dense::{DOut, Dn, M40};
use crate::dense_ck::{both, civil_of_days, A_INV, ALIAS_HI_TOP, B_INV, ID_ADJ, ID_LIM0, ID_LIM1, ID_W1, ID_W2, NODE_MASK_TOP};
use crate::table::STAGE_MAX;
use crate::util::*;
use crate::{lines, Ctx};

/// A day of the click window (or, for epoch times, 10^8 ms of it): the bytes
/// a `ts` on it starts with, and the milliseconds from the window's start to
/// the day's start, shifted up by 32.
#[repr(C)]
#[derive(Clone, Copy)]
struct DayEnt {
    key: [u8; 16],
    val: u64,
    _pad: u64,
}

const DAYS_CIVIL: usize = 1 << 11;
const DAYS_EPOCH: usize = 1 << 8;
const DAYS_LEN: usize = 2 * DAYS_CIVIL + 2 * DAYS_EPOCH;

/// How a `ts` field of one raw length is read (offsets fixed by the assembly).
#[repr(C, align(64))]
#[derive(Clone, Copy)]
struct Row {
    shuf: [u8; 16],
    lo: [u8; 16],
    range: [u8; 16],
    w1: [i8; 16],
    plim: [i16; 8],
    w2: [i16; 8],
    dmask: [u8; 16],
    pmask: u64,
    tbl: *const DayEnt,
    /// `mm[0]` for a sign byte of `-` (or none), `mm[8]` for `+`: 64 bytes apart.
    mm: [u64; 16],
}

const ROWS: usize = 7;
const R_POISON: usize = 0;

/// ID rows by both comma positions (`a1 * 32 + a2`, each at most 32).
const IDT: usize = 33 * 32 + 1;

#[repr(C, align(64))]
#[derive(Clone, Copy)]
struct IdEnt {
    ctrl: [u8; 32],
    sub: [u8; 32],
}

/// Everything a pass reads besides the line and the write pointers.
#[repr(C, align(64))]
pub(crate) struct KC {
    c_30: [u8; 32],
    c_lim1: [u8; 32],
    c_adj: [u8; 32],
    c_w1: [u8; 32],
    c_w2: [u8; 32],
    c_36p4: [u64; 4],
    binv: u64,
    ainv: u64,
    ckey: u64,
    ukey: u64,
    click_top: u64,
    alias_hi: u64,
    node_mask: u64,
    max_node: u64,
    stream_mask: u64,
    c_hi: u64,
    span32: u64,
    /// Campaign rows by `a3 - a2` (`dense_ck`'s direct table).
    cmask: [u64; 33],
    cbase: [*const u64; 33],
    cpm: [u64; 33],
    rowptr: [*const Row; 33],
    rows: [Row; ROWS],
    idt: [IdEnt; IDT],
    days: [DayEnt; DAYS_LEN + 1],
}

unsafe impl Sync for KC {}
unsafe impl Send for KC {}

/// Where each byte of an ID field of raw length `r` goes, and what is taken
/// off it: the field is read as the 16 bytes that end with the comma after
/// it. Slots 0..8 the digits, right-aligned (less '0'); 8 the prefix (less
/// itself), 9 and 10 the quotes (less '"'; nothing without quotes), 11 the
/// first digit when there are two or more (less '1': 0xFF only for '0').
/// A length no dense ID has reads 0xFF in slot 8.
fn id_lane(r: usize, prefix: u8) -> ([u8; 16], [u8; 16]) {
    let mut c = [0x80u8; 16];
    let mut s = [0u8; 16];
    if !(2..=11).contains(&r) {
        s[8] = 1;
        return (c, s);
    }
    let q = (r >= 10) as usize;
    let n = r - 2 * q - 1;
    let f = 15 - r;
    let (pre, last) = if q == 1 { (f + 1, 13) } else { (f, 14) };
    for j in 0..n {
        c[7 - j] = (last - j) as u8;
        s[7 - j] = 0x30;
    }
    c[8] = pre as u8;
    s[8] = prefix;
    if q == 1 {
        c[9] = f as u8;
        s[9] = b'"';
        c[10] = 14;
        s[10] = b'"';
    }
    if n >= 2 {
        c[11] = (pre + 1) as u8;
        s[11] = 0x31;
    }
    (c, s)
}

fn blank_row(tbl: *const DayEnt) -> Row {
    Row {
        shuf: [0x80; 16],
        lo: [0; 16],
        range: [0; 16],
        w1: [0; 16],
        plim: [i16::MAX; 8],
        w2: [0; 8],
        dmask: [0; 16],
        pmask: 0,
        tbl,
        mm: [0; 16],
    }
}

/// A row nothing passes: one byte would have to be both 0 and 1.
fn poison_row(tbl: *const DayEnt) -> Row {
    let mut r = blank_row(tbl);
    r.shuf[0] = 15;
    r.shuf[1] = 15;
    r.lo[1] = 1;
    r
}

/// A row for content of `n` bytes with `q` quotes around it: content byte
/// `c` to `slot` (less `lo`, at most `range` after).
fn put(r: &mut Row, n: usize, q: usize, slot: usize, c: usize, lo: u8, range: u8) {
    r.shuf[slot] = (c + 16 - n - q) as u8;
    r.lo[slot] = lo;
    r.range[slot] = range;
}

fn close_quote(r: &mut Row, q: usize) {
    if q == 1 {
        r.shuf[15] = 15;
        r.lo[15] = b'"';
    }
}

/// Epoch milliseconds, 13 digits: the first five are the day table's, then
/// `A` = digits 6..9 (slots 8..12), `B` = digits 10..13 (slots 0..4).
fn epoch_row(q: usize, tbl: *const DayEnt) -> Row {
    let mut r = blank_row(tbl);
    for i in 0..4 {
        put(&mut r, 13, q, 8 + i, 5 + i, b'0', 9);
        put(&mut r, 13, q, i, 9 + i, b'0', 9);
    }
    close_quote(&mut r, q);
    r.w1 = [10, 1, 10, 1, 0, 0, 0, 0, 10, 1, 10, 1, 0, 0, 0, 0];
    r.w2 = [100, 1, 0, 0, 100, 1, 0, 0];
    // A * 10^4 + B.
    r.mm[0] = 1 << 32 | 10_000;
    r.mm[8] = r.mm[0];
    for i in 0..5 + q {
        r.dmask[i] = 0xFF;
    }
    // The fourth and fifth digit: raw bytes 3 + q and 4 + q (as in dense_ck).
    r.pmask = 0xF << (8 * (7 - q)) | 0xF << (8 * (6 - q));
    r
}

/// `YYYY-MM-DDTHH:MM:SS` then `Z` (`off` false) or `+HH:MM` / `-HH:MM`:
/// the date is the day table's; `A` is the time of day in seconds, `B` the
/// offset's.
fn civil_row(off: bool, q: usize, tbl: *const DayEnt) -> Row {
    let mut r = blank_row(tbl);
    let n = if off { 25 } else { 20 };
    // Hours, minutes, seconds in slots 8..14.
    for (slot, c) in [(8, 11), (9, 12), (10, 14), (11, 15), (12, 17), (13, 18)] {
        put(&mut r, n, q, slot, c, b'0', 9);
    }
    put(&mut r, n, q, 4, 13, b':', 0);
    put(&mut r, n, q, 5, 16, b':', 0);
    if off {
        // The offset's hour and minute in slots 0..4.
        for (slot, c) in [(0, 20), (1, 21), (2, 23), (3, 24)] {
            put(&mut r, n, q, slot, c, b'0', 9);
        }
        put(&mut r, n, q, 7, 22, b':', 0);
        // The sign: 0x2B..=0x2D in slot 14, not 0x2C in slot 6 (its pair is
        // the negated byte, which may not be above -1).
        put(&mut r, n, q, 14, 19, 0x2B, 2);
        put(&mut r, n, q, 6, 19, 0x2C, 0xFF);
        r.w1 = [10, 1, 10, 1, 0, 0, -1, 0, 10, 1, 10, 1, 10, 1, 0, 0];
        r.plim = [23, 59, i16::MAX, -1, 23, 59, 59, i16::MAX];
        r.w2 = [3600, 60, 0, 0, 3600, 60, 1, 0];
        // `-`: local time behind UTC, add the offset; `+`: take it off.
        r.mm[0] = 1000 << 32 | 1000;
        r.mm[8] = (1u64 << 32).wrapping_sub(1000) << 32 | 1000;
    } else {
        put(&mut r, n, q, 7, 19, b'Z', 0);
        r.w1 = [0, 0, 0, 0, 0, 0, 0, 0, 10, 1, 10, 1, 10, 1, 0, 0];
        r.plim = [i16::MAX, i16::MAX, i16::MAX, i16::MAX, 23, 59, 59, i16::MAX];
        r.w2 = [0, 0, 0, 0, 3600, 60, 1, 0];
        r.mm[0] = 1000;
        r.mm[8] = 1000;
    }
    close_quote(&mut r, q);
    for i in 0..11 + q {
        r.dmask[i] = 0xFF;
    }
    // Month and day digits: content bytes 5, 6, 8, 9 (as in dense_ck).
    let sh = |c: usize| 8 * (10 - c - q);
    r.pmask = 0x1 << sh(5) | 0xF << sh(6) | 0x3 << sh(8) | 0xF << sh(9);
    r
}

impl KC {
    /// The tables for a run, or `None` when the run is one the pass was not
    /// made for (then the Rust kernel takes it).
    pub(crate) fn new(ctx: &Ctx, dn: &Dn) -> Option<Box<KC>> {
        let (cmask, cbase, cpm) = dn.ckx.camp_rows()?;
        let cws = ctx.cws;
        let span = (ctx.we - cws).max(0);
        // Times are compared modulo 2^32 (and may be up to three days off the window).
        if span + 3 * DAY >= 1 << 31 || dn.nq + 1 > STAGE_MAX {
            return None;
        }
        let mut k: Box<KC> = unsafe { Box::<KC>::new_zeroed().assume_init() };
        k.c_30 = [0x30; 32];
        k.c_lim1 = ID_LIM1.0;
        k.c_adj = ID_ADJ.0;
        k.c_w1 = ID_W1.0;
        k.c_w2 = ID_W2.0;
        k.c_36p4 = [1_679_616; 4];
        k.binv = B_INV << 24;
        k.ainv = A_INV << 24;
        k.ckey = dn.keys.click & M40;
        k.ukey = dn.keys.user & M40;
        k.click_top = if dn.max_click >= 1 << 32 { 0 } else { dn.max_click << 24 };
        k.alias_hi = ALIAS_HI_TOP;
        k.node_mask = NODE_MASK_TOP;
        k.max_node = dn.vbound >> 6;
        k.stream_mask = !0u64 << (33 + dn.ku);
        k.c_hi = !0u64 << 32;
        k.span32 = (span as u64) << 32;
        k.cmask.copy_from_slice(&cmask[..33]);
        k.cbase.copy_from_slice(&cbase[..33]);
        k.cpm.copy_from_slice(&cpm[..33]);

        // ---- IDs
        for e in k.idt.iter_mut() {
            let (c, s) = id_lane(0, b'k');
            *e = IdEnt { ctrl: both(c).0, sub: both(s).0 };
        }
        let pairs = (0..32usize).flat_map(|a1| (a1 + 1..=32).map(move |a2| (a1, a2))).chain([(32, 32)]);
        for (a1, a2) in pairs {
            let (c0, s0) = id_lane(a1 + 1, b'k');
            let (c1, s1) = id_lane((a2 - a1).wrapping_sub(1), b'u');
            let e = &mut k.idt[a1 * 32 + a2];
            e.ctrl[..16].copy_from_slice(&c0);
            e.ctrl[16..].copy_from_slice(&c1);
            e.sub[..16].copy_from_slice(&s0);
            e.sub[16..].copy_from_slice(&s1);
        }

        // ---- the days of the click window, a day to spare on both sides
        // for the offset forms (the same keys as dense_ck's).
        let ents = k.days.as_mut_ptr();
        let blocks = [0, DAYS_CIVIL, 2 * DAYS_CIVIL, 2 * DAYS_CIVIL + DAYS_EPOCH];
        let civil = [civil_row(false, 0, ents).pmask, civil_row(false, 1, ents).pmask];
        let epoch = [epoch_row(0, ents).pmask, epoch_row(1, ents).pmask];
        let taken = Cell::new(0usize);
        let put_day = |block: usize, pmask: u64, start: &[u8], val: i64| unsafe {
            let mut key = [0u8; 16];
            key[..start.len()].copy_from_slice(start);
            let idx = _pext_u64(u64::from_be_bytes(key[3..11].try_into().unwrap()), pmask) as usize;
            let e = &mut *ents.add(block + idx);
            if e.key == [0u8; 16] {
                *e = DayEnt { key, val: (val as u64) << 32, _pad: 0 };
                taken.set(taken.get() + 1);
            }
        };
        if ctx.we > cws {
            for z in cws.div_euclid(DAY) - 1..=(ctx.we - 1).div_euclid(DAY) + 1 {
                let (y, m, d) = civil_of_days(z);
                let date = format!("\"{y:04}-{m:02}-{d:02}T");
                // The plain parser has the last word on what the day is.
                if let Some(ms) = parse_ts(format!("{}00:00:00Z", &date[1..]).as_bytes()) {
                    if date.len() == 12 && ms == z * DAY {
                        put_day(blocks[0], civil[0], &date.as_bytes()[1..], ms - cws);
                        put_day(blocks[1], civil[1], date.as_bytes(), ms - cws);
                    }
                }
            }
            for p in cws.div_euclid(100_000_000)..=(ctx.we - 1).div_euclid(100_000_000) {
                if !(10_000..100_000).contains(&p) {
                    continue;
                }
                let digits = format!("\"{p}");
                put_day(blocks[2], epoch[0], &digits.as_bytes()[1..], p * 100_000_000 - cws);
                put_day(blocks[3], epoch[1], digits.as_bytes(), p * 100_000_000 - cws);
            }
        }
        // An unused entry is all zeros, which no field that lands on it
        // equals, as the index bits are among the bytes compared, except on
        // entry 0: there it gets a key that belongs to entry 1.
        for (b, &block) in blocks.iter().enumerate() {
            let e = &mut k.days[block];
            if e.key == [0u8; 16] {
                e.key[if b < 2 { 9 + b } else { 2 + b }] = 1;
            }
        }

        // ---- timestamp rows
        let blk = |b: usize| unsafe { ents.add(blocks[b]) as *const DayEnt };
        k.rows[R_POISON] = poison_row(blk(0));
        for q in 0..2 {
            k.rows[1 + q] = epoch_row(q, blk(2 + q));
            k.rows[3 + q] = civil_row(false, q, blk(q));
            k.rows[5 + q] = civil_row(true, q, blk(q));
        }
        let rows = k.rows.as_ptr();
        k.rowptr = [rows; 33];
        for q in 0..2 {
            k.rowptr[13 + 2 * q] = unsafe { rows.add(1 + q) };
            k.rowptr[20 + 2 * q] = unsafe { rows.add(3 + q) };
            k.rowptr[25 + 2 * q] = unsafe { rows.add(5 + q) };
        }
        Some(k)
    }
}

/// How a pass ended.
const ST_END: u64 = 0;
const ST_COLD: u64 = 1;
const ST_CAMP: u64 = 2;
const ST_USER: u64 = 3;
const ST_CHUNK: u64 = 4;
const ST_TS: u64 = 5;
const ST_DATE: u64 = 6;

/// How far ahead of the line being parsed to ask for input bytes.
const AHEAD: usize = 4096;

/// A line the pass took but whose user it does not hold directly.
#[cold]
#[inline(never)]
unsafe fn user_line(p: *const u8, w0: u64, tc: u64, dn: &Dn, out: &mut DOut) {
    let (node, stream) = crate::dense_ck::user_slow(p, dn);
    out.ck.push16(stream as usize, w0, node | tc << 8, &mut out.arena);
}

/// The click lines of `file[start..end]` that end before `end`, as far as
/// the fast path takes them (`dense_ck::clicks_dense`'s contract); returns
/// where the rest starts, or `None` when this kernel cannot take the run.
#[inline(never)]
pub(crate) unsafe fn clicks(file: &[u8], start: usize, end: usize, ctx: &Ctx, dn: &Dn, out: &mut DOut) -> Option<usize> {
    let kc: &KC = dn.cka.as_deref()?;
    let pos = out.ck.pointers()?;
    let base = file.as_ptr();
    // Past the last line that ends before `end`; a pass reads up to 100
    // bytes from a line's start, and 14 before it.
    let mut lim = end;
    while lim > start && *base.add(lim - 1) != b'\n' {
        lim -= 1;
    }
    let stop = lim.min(file.len().saturating_sub(127));
    let mut s = start;
    while s < stop && s < 16 {
        match crate::dense_ck::plain_line(file, s, end, ctx, dn, out) {
            usize::MAX => return Some(s),
            n => s = n,
        }
    }
    if s >= stop {
        return Some(s);
    }
    // `a0` is the newline before the line; a line may start below `stop`.
    let limit = base.add(stop - 1);
    let mut a0 = base.add(s - 1);
    let kp: *const KC = kc;
    let commas = _mm256_set1_epi8(b',' as i8);
    let nls = _mm256_set1_epi8(b'\n' as i8);
    let lim0 = _mm256_load_si256(ID_LIM0.0.as_ptr() as *const __m256i);
    while a0 < limit {
        let (st, w0, w1, stream, next, tu, r3): (u64, u64, u64, u64, u64, u64, u64);
        asm!(
            "2:",
            "prefetcht0 byte ptr [rsi + {ahead}]",
            // ---- The first three commas (32 when there is none).
            "vpcmpeqb ymm0, ymm15, ymmword ptr [rsi + 2]",
            "vpmovmskb ecx, ymm0",
            "blsr edx, ecx",
            "tzcnt ecx, ecx",           // a1: the first comma is at rsi + 2 + a1
            "blsr r8d, edx",
            "tzcnt edx, edx",           // a2
            "tzcnt r8d, r8d",           // a3
            // ---- Both IDs: the 16 bytes up to each one's comma, to slots.
            "vmovdqu xmm2, xmmword ptr [rsi + rcx - 13]",
            "vinserti128 ymm2, ymm2, xmmword ptr [rsi + rdx - 13], 1",
            "shl ecx, 8",
            "lea rcx, [rcx + rdx*8]",   // (a1 * 32 + a2) * 8
            "vpshufb ymm2, ymm2, ymmword ptr [rdi + rcx*8 + {idt}]",
            "vpsubb ymm2, ymm2, ymmword ptr [rdi + rcx*8 + {idt} + 32]",
            "vpcmpgtb ymm3, ymm2, ymmword ptr [rdi + {c_30}]",      // letters
            "vpblendvb ymm4, ymm13, ymmword ptr [rdi + {c_lim1}], ymm3",
            "vpsubusb ymm4, ymm2, ymm4",                            // ymm4: what is wrong with them
            "vpand ymm3, ymm3, ymmword ptr [rdi + {c_adj}]",
            "vpsubb ymm2, ymm2, ymm3",
            "vpmaddubsw ymm2, ymm2, ymmword ptr [rdi + {c_w1}]",
            "vpmaddwd ymm2, ymm2, ymmword ptr [rdi + {c_w2}]",
            "vpmuludq ymm3, ymm2, ymmword ptr [rdi + {c_36p4}]",
            "vpsrlq ymm2, ymm2, 32",
            "vpaddq ymm2, ymm2, ymm3",                              // the two numbers
            "vpsrlq ymm3, ymm2, 40",
            "vpor ymm4, ymm4, ymm3",
            // ---- ts: the field after the third comma, up to the newline (32 when there is none in sight).
            "lea rcx, [rsi + r8 + 3]",
            "vmovdqu ymm1, ymmword ptr [rcx]",
            "vpcmpeqb ymm0, ymm14, ymm1",
            "vpmovmskb r12d, ymm0",
            "tzcnt r12d, r12d",         // r3
            // ---- The IDs' numbers.
            "vmovq r9, xmm2",
            "imul r9, qword ptr [rdi + {binv}]",
            "rorx r10, r9, 17",
            "xor r10, r9",
            "shr r10, 17",
            "xor r9, r10",
            "shr r9, 24",
            "xor r9, qword ptr [rdi + {ckey}]",
            "imul r9, qword ptr [rdi + {ainv}]",                   // r9: the click's number, top 40 bits
            "cmp r9, qword ptr [rdi + {click_top}]",
            "jae 81f",
            "vextracti128 xmm2, ymm2, 1",
            "vmovq r10, xmm2",
            "imul r10, qword ptr [rdi + {binv}]",
            "rorx r11, r10, 17",
            "xor r11, r10",
            "shr r11, 17",
            "xor r10, r11",
            "shr r10, 24",
            "xor r10, qword ptr [rdi + {ukey}]",
            "imul r10, qword ptr [rdi + {ainv}]",                  // r10: the user's
            // ---- Campaign: the field's bytes, at the index their own bits make, are a name or not.
            "mov r11, r8",
            "sub r11, rdx",
            "movbe rax, qword ptr [rsi + rdx + 3]",
            "and rax, qword ptr [rdi + r11*8 + {cmask}]",
            "mov r13, qword ptr [rdi + r11*8 + {cbase}]",
            "pext r11, rax, qword ptr [rdi + r11*8 + {cpm}]",
            "cmp rax, qword ptr [r13 + r11*8]",
            "jne 83f",
            "mov r13, qword ptr [r13 + r11*8 + {cap8}]",          // r13: the campaign's value
            // ---- ts: its end to slots, its start against the day table.
            "movbe rdx, qword ptr [rcx + 3]",
            "mov r11, qword ptr [rdi + r12*8 + {rowptr}]",
            "lea rsi, [rcx + r12]",     // rsi: the newline, where the next pass starts
            "vmovdqu xmm2, xmmword ptr [rsi - 16]",
            "vpshufb xmm2, xmm2, xmmword ptr [r11]",
            "vpsubb xmm2, xmm2, xmmword ptr [r11 + 16]",
            "vpsubusb xmm3, xmm2, xmmword ptr [r11 + 32]",
            "vpmaddubsw xmm5, xmm2, xmmword ptr [r11 + 48]",
            "vpcmpgtw xmm6, xmm5, xmmword ptr [r11 + 64]",
            "vpor xmm3, xmm3, xmm6",    // xmm3: what is wrong with its end
            "pext rdx, rdx, qword ptr [r11 + 112]",
            "shl rdx, 5",
            "add rdx, qword ptr [r11 + 120]",                       // the day's entry
            "vpxor xmm6, xmm1, xmmword ptr [rdx]",
            "vpand xmm6, xmm6, xmmword ptr [r11 + 96]",
            "vpor xmm6, xmm6, xmm3",
            "vpor ymm6, ymm6, ymm4",
            "vptest ymm6, ymm6",
            "jnz 84f",
            "vpmaddwd xmm5, xmm5, xmmword ptr [r11 + 80]",
            "vphaddd xmm5, xmm5, xmm5",
            "vmovq r12, xmm5",          // A << 32 | B
            "vpmovmskb eax, xmm2",      // 64 for `+`, else 0
            "imul r12, qword ptr [r11 + rax + 128]",
            "and r12, qword ptr [rdi + {c_hi}]",
            "add r12, qword ptr [rdx + 16]",                        // ms from the window's start, << 32
            "cmp r12, qword ptr [rdi + {span32}]",
            "jae 3f",                   // outside the window: no record
            "add r12, r13",             // r12: the record's first word
            // ---- The node: direct when the ID's number within its person is below 8 and the person in range.
            "test r10, qword ptr [rdi + {alias_hi}]",
            "jnz 85f",
            "pext rdx, r10, qword ptr [rdi + {node_mask}]",
            "cmp rdx, qword ptr [rdi + {max_node}]",
            "jae 85f",
            "pext r13, r10, qword ptr [rdi + {stream_mask}]",
            "shl r9, 8",
            "or r9, rdx",               // r9: the second word
            // ---- Append.
            "mov rax, qword ptr [r14 + r13*8]",
            "lea r11, [rax + 16]",
            "test r11b, 127",
            "jz 86f",                   // the staging line's last slot
            "mov qword ptr [rax], r12",
            "mov qword ptr [rax + 8], r9",
            "mov qword ptr [r14 + r13*8], r11",
            "3:",
            "cmp rsi, r15",
            "jb 2b",
            "xor eax, eax",
            "99:",
            // ---- Out of line.
            ".pushsection .text.unlikely.ss_cka,\"ax\",@progbits",
            // The staging line is full: moved out with the item from the registers.
            "86:",
            "mov rdx, qword ptr [r14 + r13*8 + {dst}]",
            "test edx, {chunk_mask}",
            "jz 87f",                   // a new chunk first
            "sub rax, 112",
            "mov qword ptr [r14 + r13*8], rax",
            "vmovdqa ymm0, ymmword ptr [rax]",
            "vmovntdq ymmword ptr [rdx], ymm0",
            "vmovdqa ymm0, ymmword ptr [rax + 32]",
            "vmovntdq ymmword ptr [rdx + 32], ymm0",
            "vmovdqa ymm0, ymmword ptr [rax + 64]",
            "vmovntdq ymmword ptr [rdx + 64], ymm0",
            "vmovq xmm0, r12",
            "vpinsrq xmm0, xmm0, r9, 1",
            "vmovdqa xmm1, xmmword ptr [rax + 96]",
            "vinserti128 ymm1, ymm1, xmm0, 1",
            "vmovntdq ymmword ptr [rdx + 96], ymm1",
            "add rdx, 128",
            "mov qword ptr [r14 + r13*8 + {dst}], rdx",
            "jmp 3b",
            "87:",
            "mov rdx, r12",
            "mov r8, rsi",
            "mov eax, 4",
            "jmp 99b",
            // The IDs are not both dense, or the click is not a direct one.
            "81:",
            "mov eax, 1",
            "jmp 99b",
            // The campaign is not in the direct table (rdx: a2, r8: a3, r12: r3).
            "83:",
            "vptest ymm4, ymm4",
            "jnz 81b",
            "mov eax, 2",
            "jmp 99b",
            // Something wrong in the IDs or ts: back to the line's start.
            "84:",
            "lea rsi, [rcx - 3]",
            "sub rsi, r8",
            "vptest ymm4, ymm4",
            "jnz 81b",
            "vptest xmm3, xmm3",
            "setz al",
            "movzx eax, al",
            "add eax, 5",               // 5: ts not in its row; 6: its end was fine
            "jmp 99b",
            // A user to look up.
            "85:",
            "mov rdx, r12",
            "lea rax, [rcx - 3]",
            "sub rax, r8",
            "mov r8, rsi",
            "mov rsi, rax",
            "mov eax, 3",
            "jmp 99b",
            ".popsection",
            ahead = const AHEAD,
            idt = const offset_of!(KC, idt),
            c_30 = const offset_of!(KC, c_30),
            c_lim1 = const offset_of!(KC, c_lim1),
            c_adj = const offset_of!(KC, c_adj),
            c_w1 = const offset_of!(KC, c_w1),
            c_w2 = const offset_of!(KC, c_w2),
            c_36p4 = const offset_of!(KC, c_36p4),
            binv = const offset_of!(KC, binv),
            ainv = const offset_of!(KC, ainv),
            ckey = const offset_of!(KC, ckey),
            ukey = const offset_of!(KC, ukey),
            click_top = const offset_of!(KC, click_top),
            cmask = const offset_of!(KC, cmask),
            cbase = const offset_of!(KC, cbase),
            cpm = const offset_of!(KC, cpm),
            cap8 = const 8 * crate::dense_ck::CAMP_CAP,
            rowptr = const offset_of!(KC, rowptr),
            c_hi = const offset_of!(KC, c_hi),
            span32 = const offset_of!(KC, span32),
            alias_hi = const offset_of!(KC, alias_hi),
            node_mask = const offset_of!(KC, node_mask),
            max_node = const offset_of!(KC, max_node),
            stream_mask = const offset_of!(KC, stream_mask),
            dst = const 8 * STAGE_MAX,
            chunk_mask = const (16 << 10) - 1,
            inout("rsi") a0,
            in("rdi") kp,
            in("r14") pos,
            in("r15") limit,
            in("ymm15") commas,
            in("ymm14") nls,
            in("ymm13") lim0,
            out("rax") st,
            out("rdx") w0,
            out("r9") w1,
            out("r13") stream,
            out("r8") next,
            out("r10") tu,
            out("r12") r3,
            out("rcx") _, out("r11") _,
            out("ymm0") _, out("ymm1") _, out("ymm2") _, out("ymm3") _, out("ymm4") _, out("ymm5") _, out("ymm6") _,
            options(nostack),
        );
        let s = a0.offset_from(base) as usize + 1;
        // (Tests rather than a jump table: these are rare, one way or the other.)
        let n = if st == ST_END {
            break;
        } else if st == ST_COLD {
            crate::dense_ck::plain_line(file, s, end, ctx, dn, out)
        } else if st == ST_CAMP {
            crate::dense_ck::slow_line(file, s, end, w0 as usize, next as usize, r3 as usize, None, false, w1, tu, ctx, dn, out)
        } else if st >= ST_TS {
            crate::dense_ck::slow_line(file, s, end, 0, next as usize, r3 as usize, Some(stream), st == ST_DATE, w1, tu, ctx, dn, out)
        } else if st == ST_USER {
            user_line(a0.add(1), w0, w1, dn, out);
            next as usize - base as usize + 1
        } else {
            debug_assert_eq!(st, ST_CHUNK);
            out.ck.flush(stream as usize, w0, w1, &mut out.arena);
            next as usize - base as usize + 1
        };
        if n == usize::MAX {
            return Some(s);
        }
        a0 = base.add(n - 1);
    }
    Some(a0.offset_from(base) as usize + 1)
}

/// SS_CKCHECK=1: for the lines of `file[start..end]`, the records the kernel
/// (with the plain code for what it leaves) gives against those of the plain
/// code alone, stream by stream and in order. Returns how many streams differ
/// (and says so on stderr).
pub(crate) fn check(file: &[u8], start: usize, end: usize, ctx: &Ctx, dn: &Dn) -> usize {
    let fresh = || {
        let mut o = crate::dense_ev::scratch_out(dn);
        o.ck = crate::dense::CkLists::new(dn.nq + 1);
        o
    };
    let (mut a, mut b) = (fresh(), fresh());
    let rest = crate::dense::click_kernel(file, start, end, ctx, dn, &mut a);
    for line in lines(&file[rest..end]) {
        crate::dense::click_line(line, ctx, dn, &mut a);
    }
    for line in lines(&file[start..end]) {
        crate::dense::click_line(line, ctx, dn, &mut b);
    }
    a.ck.finish(&mut a.arena);
    b.ck.finish(&mut b.arena);
    let mut bad = 0;
    for s in 0..=dn.nq {
        let get = |o: &DOut| -> Vec<[u64; 2]> { o.ck.pieces(s).flat_map(|(p, n)| unsafe { std::slice::from_raw_parts(p, n) }.iter().copied()).collect() };
        let (x, y) = (get(&a), get(&b));
        if x != y {
            if bad < 3 {
                let at = x.iter().zip(&y).position(|(p, q)| p != q).unwrap_or(x.len().min(y.len()));
                eprintln!("    ckcheck: stream {s}: {} records against {}, first difference at {at}: {:x?} / {:x?}", x.len(), y.len(), x.get(at), y.get(at));
            }
            bad += 1;
        }
    }
    a.arena.release();
    b.arena.release();
    bad
}
