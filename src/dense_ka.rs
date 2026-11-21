//! The dense event parse kernel's fast path, written out in assembly: one
//! line per pass, about 220 instructions, every table and constant behind one
//! pointer, so that nothing lives on the stack.
//!
//! The contract is `dense_ev`'s: exactly `dense::event_line`'s records, in
//! order. A pass ends in one of a few ways (`ST_*`): a plain valid view or
//! cart of a dense event and a direct user, record ready; a line that parsed
//! but is something else (a purchase, an invalid line, a user to intern),
//! which `special` finishes from the parsed values; or a line the pass could
//! not prove sound, which goes to `dense_ev::odd_line` whole.
//!
//! What a pass checks, so that a taken line is the plain code's line:
//! * three commas in the first 64 bytes; around each of the first three
//!   fields either both quotes or none (any other quote lands in a value);
//! * event ID and user: prefix, then 1..=8 base-36 digits, no leading zero,
//!   below 2^40 (`t_keep`, `t_range` by length); the event dense;
//! * ts: by length 13 digits, or date and time with `Z`, or with `+HH:MM`
//!   (the form memo holds only keys the plain rules accept, the date memo
//!   only valid dates; times of day and digits are checked in one vector);
//! * ingest: date, time, `Z`, the closing quote if it opened with one, comma;
//! * then exactly two more commas before the newline, within 32 bytes, and
//!   quotes only as the first and last byte of a field of two bytes or more.

use core::arch::asm;
use core::arch::x86_64::*;
use core::mem::offset_of;
use std::cell::UnsafeCell;

use crate::dense::{ingest31, DOut, Dn, EXT, INGEST_TOP, LOW21, M40, PURCHASE};
use crate::dense_ev::{date_secs, form_off, odd_line, slow_line, AINV24, BINV24, FORM_BITS, MARGIN, MASK8, T_MAX, T_MAXW, T_MUL, T_PAT, T_SHUF, T_W1, T_W2, T_W3, ZMASK, ZPAT};
use crate::util::*;
use crate::Ctx;

/// Everything a pass reads besides the line, and what it leaves for the
/// cold code: one per thread.
#[repr(C, align(64))]
pub(crate) struct K {
    // Vector constants.
    c_comma: [u8; 32],
    c_quote: [u8; 32],
    c_nl: [u8; 32],
    c_60: [u8; 32],
    c_39: [u8; 32],
    c_30: [u8; 32],
    c_26: [u8; 32],
    c_9: [u8; 32],
    c_36_1: [u8; 32],
    c_1296_1: [u16; 16],
    c_36p4: [u64; 4],
    c_range_ok: [u64; 4],
    c_tpat: [u8; 32],
    c_tmax: [u8; 32],
    c_tw1: [u8; 32],
    c_tmaxw: [i16; 16],
    c_tw2: [i16; 16],
    c_tshuf: [u8; 32],
    c_tw3: [i16; 16],
    c_tmul: [u32; 8],
    c_typemask: [u64; 4],
    c_typepat: [u64; 4],
    /// (-window start, the record ingest field's bias), for (ts, ingest).
    c_bias: [i64; 4],
    // The run's numbers (see `setup`).
    binv: u64,
    ainv: u64,
    ekey: u64,
    ukey: u64,
    ev_top: u64,
    us_top: u64,
    m_stream: u64,
    m_slot2: u64,
    m_node: u64,
    m_alias: u64,
    form_bits: u64,
    date_bits: u64,
    ws: i64,
    span: u64,
    xlo: i64,
    xspan: u64,
    wso: i64,
    rbias: i64,
    /// What `setup` was last given, to do its work once.
    seen: [u64; 6],
    // What a pass leaves when it does not end with a record.
    s_t: i64,
    s_ing: i64,
    s_w1: u64,
    s_stream: u64,
    s_next: u64,
    s_indirect: u64,
    s_ptr: u64,
    s_idx: u64,
    s_key: u64,
    s_n: u64,
    /// The newline before the line the pass that ended was on.
    s_line: u64,
    /// (A register the loop borrows.)
    s_rbx: u64,
    /// The next line's masks: commas, quotes, where its newline is.
    n_c: u64,
    n_q: u64,
    n_nl: u64,
    // Where a run of passes stops, and the event streams' states (`table::Slot16`).
    limit: u64,
    streams: u64,
    // Tables.
    /// By an ID's length plus 1: a mask keeping its digits in the 8 bytes
    /// that end where it ends (0 for lengths no ID has).
    t_keep: [u64; 64],
    /// At the same index: the smallest number an ID of that length may be,
    /// and at the next, the first it may not (an empty range for lengths no
    /// ID has).
    t_range: [u64; 66],
    /// By ts length plus 1: which of the 8 bytes from its 20th on are the
    /// form's key, and the length itself for the key's top byte.
    t_fmask: [u64; 64],
    t_ftag: [u64; 64],
    /// By ts length plus 1, 8 bytes apart: the bytes of the time vector
    /// that count (the times of day always; the digits for 13 digits).
    t_counts: [u8; 64 * 8 + 32],
    /// The same way: which of (ts by date and time, ingest, ts by digits, -)
    /// the time vector's result keeps.
    t_sel: [u8; 64 * 8 + 32],
    t_zmask: [u32; 2],
    t_zpat: [u32; 2],
    // Memos: dates (`YYYY-MM-DDT` -> midnight in ms and s) and ts forms
    // (key -> what to take off the local time, ms).
    key8: [u64; 64],
    key4: [u32; 64],
    ms: [i64; 64],
    sec: [i64; 64],
    fkey: [u64; 512],
    fval: [i64; 512],
}

thread_local! {
    static KS: UnsafeCell<Box<K>> = UnsafeCell::new(K::new());
}

/// How a pass ended.
const ST_ODD: u64 = 1;
const ST_FORM: u64 = 2;
const ST_DATE: u64 = 3;
const ST_SPECIAL: u64 = 5;
/// No line may start where the next pass would: the run is done.
const ST_END: u64 = 7;
/// The record is ready, but its stream needs a new chunk first.
const ST_CHUNK: u64 = 9;

/// How far ahead of the line being parsed to ask for input bytes.
const AHEAD: usize = 4096;

impl K {
    fn new() -> Box<K> {
        // (Zeroed is a valid K: plain numbers throughout.)
        let mut k: Box<K> = unsafe { Box::new(std::mem::zeroed()) };
        k.c_comma = [b','; 32];
        k.c_quote = [b'"'; 32];
        k.c_nl = [b'\n'; 32];
        k.c_60 = [0x60; 32];
        k.c_39 = [39; 32];
        k.c_30 = [0x30; 32];
        k.c_26 = [26; 32];
        k.c_9 = [9; 32];
        for i in 0..16 {
            k.c_36_1[2 * i] = 36;
            k.c_36_1[2 * i + 1] = 1;
        }
        for i in 0..8 {
            k.c_1296_1[2 * i] = 1296;
            k.c_1296_1[2 * i + 1] = 1;
        }
        k.c_36p4 = [1_679_616; 4];
        k.c_range_ok = [0, !0, 0, !0];
        k.c_tpat = T_PAT.0;
        k.c_tmax = T_MAX.0;
        k.c_tw1 = T_W1.0;
        k.c_tmaxw = T_MAXW.0;
        k.c_tw2 = T_W2.0;
        k.c_tshuf = T_SHUF.0;
        k.c_tw3 = T_W3.0;
        k.c_tmul = T_MUL.0;
        k.binv = BINV24;
        k.ainv = AINV24;
        k.form_bits = FORM_BITS;
        k.date_bits = 0x000F_0300;
        k.m_node = !((0x1F8u64 << 24) | 0x7);
        k.m_alias = 0x1F8 << 24;
        k.t_keep = MASK8.0;
        // 1 digit: 0..36; d digits: 36^(d-1)..36^d; 8 digits: up to 2^40.
        let mut p = 36u64;
        k.t_range[3] = 0;
        for d in 1..=8 {
            k.t_range[d + 3] = p.min(1 << 40);
            p *= 36;
        }
        for n in 0..64 {
            k.t_ftag[n] = (n as u64) << 56;
        }
        k.t_fmask[21] = 0xFF;
        k.t_fmask[26] = 0xFFFF_FFFF_FFFF;
        for i in 0..32 {
            k.t_counts[14 * 8 + i] = 255;
            if i < 16 {
                k.t_counts[21 * 8 + i] = 255;
                k.t_counts[26 * 8 + i] = 255;
            }
        }
        for i in 0..16 {
            k.t_sel[14 * 8 + 8 + i] = 255;
            k.t_sel[21 * 8 + i] = 255;
            k.t_sel[26 * 8 + i] = 255;
        }
        k.c_typemask = [0xFFFF_FFFF_FFFF, 0xFFFF_FFFF_FFFF, !0, !0];
        k.c_typepat = [u64::from_le_bytes(*b",view,\0\0"), u64::from_le_bytes(*b",cart,\0\0"), u64::from_le_bytes(*b",\"view\","), u64::from_le_bytes(*b",\"cart\",")];
        k.t_zmask = ZMASK;
        k.t_zpat = ZPAT;
        // Every date slot starts as 1970-01-01, which is right for the one
        // slot that date maps to and can never match in any other. No form
        // key has all its bits set.
        let d = b"1970-01-01T";
        k.key8 = [u64::from_le_bytes(d[..8].try_into().unwrap()); 64];
        k.key4 = [u32::from_le_bytes(d[7..11].try_into().unwrap()); 64];
        k.fkey = [!0; 512];
        k
    }

    /// The run's numbers. False when the window is one the fast path's
    /// range checks were not made for (the caller then uses the other kernel).
    fn setup(&mut self, ctx: &Ctx, dn: &Dn) -> bool {
        let seen = [dn.keys.event, dn.keys.user, dn.max_event, dn.vbound, dn.kb as u64, (ctx.ws ^ ctx.we.rotate_left(21) ^ ctx.origin.rotate_left(42)) as u64];
        if self.seen == seen && self.span != 0 {
            return true;
        }
        self.span = 0;
        // A record's ingest field: seconds from 2^30 s before the origin.
        let rbias = (1i64 << 30) + (-ctx.origin).div_euclid(1000);
        let wso = ctx.ws - ctx.origin;
        // The fast path takes a line whose ts is in the window and whose
        // ingest time is at most a day after it and no earlier than the
        // record field's zero: then the field needs no clamping, if the
        // window ends this far below its top.
        if ctx.we <= ctx.ws || wso < 0 || ctx.we - ctx.origin >= 1 << 31 || (DAY + ctx.we).div_euclid(1000) + rbias >= INGEST_TOP as i64 || 24 + dn.kb >= 63 {
            return false;
        }
        self.ekey = dn.keys.event & M40;
        self.ukey = dn.keys.user & M40;
        self.ev_top = dn.max_event << 24;
        self.us_top = dn.vbound << 24;
        self.m_stream = !0u64 << (24 + dn.kb);
        self.m_slot2 = ((1u64 << (dn.kb + 1)) - 1) << 23;
        self.ws = ctx.ws;
        self.wso = wso;
        self.rbias = rbias;
        self.c_bias = [-ctx.ws, rbias, 0, 0];
        // With x = ingest (ms) - (ts - window start): x from the field's
        // zero up to a day past the window start.
        self.xlo = -rbias * 1000;
        self.xspan = (DAY + ctx.ws - self.xlo) as u64;
        self.seen = seen;
        self.span = (ctx.we - ctx.ws) as u64;
        true
    }
}

/// One field's value given its bytes `[a, b)`, known to be well formed.
#[inline(always)]
unsafe fn value<'a>(a: *const u8, b: *const u8) -> &'a [u8] {
    let n = b.offset_from(a) as usize;
    if n >= 2 && *a == b'"' {
        std::slice::from_raw_parts(a.add(1), n - 2)
    } else {
        std::slice::from_raw_parts(a, n)
    }
}

/// A line that parsed (structure sound, event dense, user in generator
/// form, ts and ingest read) but is not a plain valid view or cart of a
/// direct user: the plain rules from the parsed values.
#[cold]
#[inline(never)]
unsafe fn special(k: &K, a0: *const u8, ctx: &Ctx, dn: &Dn, out: &mut DOut) {
    let (t, ing, w1, stream) = (k.s_t, k.s_ing * 1000, k.s_w1, k.s_stream as usize);
    // Type, product and price lie between the last three commas, all of
    // them in the 32 bytes before the newline.
    let nl = k.s_next as *const u8;
    let from = nl.sub(32);
    let comma = _mm256_set1_epi8(b',' as i8);
    let mut cm = _mm256_movemask_epi8(_mm256_cmpeq_epi8(_mm256_loadu_si256(from as *const __m256i), comma)) as u32;
    let mut last = || {
        let i = 31 - cm.leading_zeros();
        cm ^= 1 << i;
        from.add(i as usize)
    };
    let (c6, c5, c4) = (last(), last(), last());
    let (typ, prod, price) = (value(c4.add(1), c5), value(c5.add(1), c6), value(c6.add(1), nl));
    let w0 = ingest31(ctx, Some(ing)) << 33;
    let mut ext = None;
    let mut ok = false;
    if !prod.is_empty() && t >= ctx.ws && t < ctx.we && ing - t <= DAY {
        if typ == b"view" || typ == b"cart" {
            ok = true;
        } else if typ == b"purchase" {
            if let Some(p) = parse_price(price) {
                let cat = ctx.aux.products.get(pack(prod)).unwrap_or(ctx.aux.unknown);
                ext = Some(p as u64 | (cat as u64) << 24);
                ok = true;
            }
        }
    }
    if !ok {
        out.ev.push16(stream, w0 | 1, w1 & LOW21, &mut out.arena);
        return;
    }
    let mut w1 = w1;
    if k.s_indirect != 0 {
        // A user the tables do not hold directly: the second field.
        let cm = _mm256_movemask_epi8(_mm256_cmpeq_epi8(_mm256_loadu_si256(a0 as *const __m256i), comma)) as u32;
        let r1 = cm.trailing_zeros() as usize;
        let r2 = (cm & cm.wrapping_sub(1)).trailing_zeros() as usize;
        let user = value(a0.add(r1 + 1), a0.add(r2));
        w1 = (w1 & LOW21) | (dn.intern_user(user) as u64) << 21;
    }
    let w0 = w0 | ((t - ctx.origin) as u64) << 2;
    match ext {
        Some(ext) => {
            out.ev.push16(stream, w0 | 2, w1 | PURCHASE, &mut out.arena);
            out.ev.push16(stream, ext, EXT, &mut out.arena);
        }
        None => {
            out.ev.push16(stream, w0, w1, &mut out.arena);
        }
    }
}

/// A pass that ended without a record. Returns where the next line starts,
/// or `usize::MAX` when this one has no newline before `end`.
#[cold]
#[inline(never)]
unsafe fn cold(k: &mut K, st: u64, a0: *const u8, file: &[u8], end: usize, ctx: &Ctx, dn: &Dn, out: &mut DOut) -> usize {
    let base = file.as_ptr();
    match st {
        ST_SPECIAL => {
            special(k, a0, ctx, dn, out);
            return (k.s_next as *const u8).offset_from(base) as usize + 1;
        }
        // A form or a date the memos do not have yet: kept if the plain
        // rules know it; this line goes the long way all the same.
        ST_FORM => {
            if let Some(v) = form_off(k.s_key, (k.s_n as usize).wrapping_sub(1)) {
                k.fkey[k.s_idx as usize] = k.s_key;
                k.fval[k.s_idx as usize] = v;
            }
        }
        ST_DATE => {
            let p = k.s_ptr as *const u8;
            if let Some(v) = date_secs(p) {
                let i = k.s_idx as usize;
                k.key8[i] = std::ptr::read_unaligned(p as *const u64);
                k.key4[i] = std::ptr::read_unaligned(p.add(7) as *const u32);
                k.sec[i] = v;
                k.ms[i] = v * 1000;
            }
        }
        _ => {}
    }
    odd_line(file, a0.offset_from(base) as usize + 1, end, ctx, dn, out)
}

/// The event lines of `file[start..end]` as far as the fast path takes
/// them; returns where the rest starts, or `None` when this kernel cannot
/// take the run at all. `file[start - 1]` is a newline; `end` is a line
/// boundary or the end of the file; 128 bytes past `end` can be read.
/// The kernel, once per way of finding a line's commas, quotes and end.
macro_rules! define_events {
    ($(#[$attr:meta])* $name:ident, $set1:ident, [$c:ident $q:ident $n:ident], [$($head:literal),* $(,)?], [$($nhead:literal),* $(,)?], [$($regs:tt)*]) => {
#[inline(never)]
        $(#[$attr])*
        pub(crate) unsafe fn $name(file: &[u8], start: usize, end: usize, ctx: &Ctx, dn: &Dn, out: &mut DOut) -> Option<usize> {
    let base = file.as_ptr();
    let end = end.min(file.len());
    let k: &mut K = &mut *KS.with(|k| k.get());
    if !k.setup(ctx, dn) {
        return None;
    }
    let mut s = start;
    if start == 0 || *base.add(start - 1) != b'\n' {
        return Some(start);
    }
    // A pass reads a few bytes before its line.
    while s < 16 && s < end {
        let next = slow_line(file, s, end, ctx, dn, out);
        if next == usize::MAX {
            return Some(s);
        }
        s = next;
    }
    if end < MARGIN + 16 {
        return Some(s);
    }
    // `a0` is the newline before the line; a line may start below `limit`.
    let limit = base.add(end - MARGIN - 1);
    let mut a0 = base.add(s - 1);
    let kp: *mut K = k;
    let ($c, $q, $n) = ($set1(b',' as i8), $set1(b'"' as i8), $set1(b'\n' as i8));
    (*kp).limit = limit as u64;
    (*kp).streams = out.ev.base() as u64;
    while a0 < limit {
        let (st, w0, w1, stream): (u64, u64, u64, u64);
        // Line after line, until one needs the code outside (or none is left).
        asm!(
            "mov qword ptr [rdi + {s_rbx}], rbx",
            // ---- The masks: where the first newline 41..104 bytes on is (r15, 64 for none); commas (rax) and quotes
            // (rdx) of the first 64. A pass finds them for the next line (`nhead`), this for the first.
            $($head,)*
            "tzcnt r15, r15",
            "mov qword ptr [rdi + {n_c}], rax",
            "mov qword ptr [rdi + {n_q}], rdx",
            "mov qword ptr [rdi + {n_nl}], r15",
            "2:",
            "mov rax, qword ptr [rdi + {n_c}]",
            "mov rdx, qword ptr [rdi + {n_q}]",
            "mov r15, qword ptr [rdi + {n_nl}]",
            "mov rbx, rsi",             // rbx: the newline before the line
            "cmp r15d, 63",
            "ja 88f",                   // no newline in sight
            "prefetcht0 byte ptr [rsi + {ahead}]",
            "prefetcht0 byte ptr [rsi + {ahead} + 64]",
            "add r15, rsi",             // r15: the newline that ends the line, less 41
            "vpcmpeqb ymm4, ymm12, ymmword ptr [r15 + 9]",           // commas and quotes of the 32 bytes before it
            "vpcmpeqb ymm5, ymm13, ymmword ptr [r15 + 9]",
            // ---- Where values end and start.
            "lea r10, [rax + 1]",       // separators: the line's start and the commas
            "rorx r12, rdx, 1",
            "and r12, r10",             // separators a quote follows
            "lea r11, [r12 + r12]",     // opening quotes
            "add r12, r10",             // r12: one before where each value starts (each such separator moved on to its quote)
            "rorx r8, rax, 1",
            "and r8, rdx",              // closing quotes: a comma follows
            "sub rax, r8",              // rax: where each value ends (each such comma moved back to its quote)
            "pext r11, r11, r12",       // by field: opens with a quote
            "pext r8, r8, rax",         // by field: closes with one
            "xor r11, r8",
            "test r11b, 7",
            "jnz 88f",                  // one quote without the other, in the first three fields
            // ---- The fields.
            "tzcnt r10, rax",
            "tzcnt r11, r12",
            "blsr rax, rax",
            "blsr r12, r12",
            "cmp byte ptr [rsi + r11 + 1], 0x65",
            "jne 88f",
            "vmovq xmm0, qword ptr [rsi + r10 - 8]",
            "sub r10, r11",             // the event ID's length plus 1
            "vmovq xmm2, qword ptr [rdi + r10*8 + {t_keep}]",
            "vmovdqu xmm8, xmmword ptr [rdi + r10*8 + {t_range}]",
            "tzcnt r10, rax",
            "tzcnt r11, r12",
            "blsr rax, rax",
            "blsr r12, r12",
            "cmp byte ptr [rsi + r11 + 1], 0x75",
            "jne 88f",
            "vpinsrq xmm0, xmm0, qword ptr [rsi + r10 - 8], 1",
            "sub r10, r11",             // the user's
            "vpinsrq xmm2, xmm2, qword ptr [rdi + r10*8 + {t_keep}], 1",
            "vinserti128 ymm8, ymm8, xmmword ptr [rdi + r10*8 + {t_range}], 1",
            "tzcnt r10, rax",
            "jc 88f",                   // fewer than three commas in sight
            "tzcnt r11, r12",
            "blsr r12, r12",
            "tzcnt r13, r12",
            "sub r10, r11",             // N: the length of ts plus 1
            "shrx r12, rdx, r13",
            "and r12d, 1",              // ingest opens with a quote?
            "add r11, rsi",             // ts, less 1
            "add r13, rsi",             // ingest, less 1
            "lea rsi, [r15 + 41]",      // rsi: the newline, where the next pass starts
            // ---- ts: the form the memo knows.
            "mov rax, qword ptr [r11 + 20]",
            "and rax, qword ptr [rdi + r10*8 + {t_fmask}]",
            "or rax, qword ptr [rdi + r10*8 + {t_ftag}]",           // the form's key
            "pext rcx, rax, qword ptr [rdi + {form_bits}]",
            "cmp rax, qword ptr [rdi + rcx*8 + {fkey}]",
            "jne 82f",
            "vmovq xmm9, qword ptr [rdi + rcx*8 + {fval}]",          // the form's offset
            // ---- The times (as early as their bytes are known: the longest chain).
            "vbroadcasti128 ymm1, xmmword ptr [r11 + 1]",            // the 13 digits, if that is what ts is
            "vmovdqu ymm10, ymmword ptr [rdi + r10*8 + {t_counts}]",
            "vmovdqu ymm11, ymmword ptr [rdi + r10*8 + {t_sel}]",
            "cmp r10d, 14",
            "cmove r11, r13",           // date and time: of ts, or of ingest again when ts is digits (less 1)
            "vmovq xmm3, qword ptr [r11 + 12]",
            "vpbroadcastq xmm6, qword ptr [r13 + 12]",
            "vpblendd xmm3, xmm3, xmm6, 0x0C",
            "vpblendd ymm1, ymm1, ymm3, 0x0F",                       // two times of day | 13 digits
            "vpxor ymm1, ymm1, ymmword ptr [rdi + {c_tpat}]",
            "vpsubusb ymm3, ymm1, ymmword ptr [rdi + {c_tmax}]",
            "vpmaddubsw ymm1, ymm1, ymmword ptr [rdi + {c_tw1}]",
            "vpcmpgtw ymm6, ymm1, ymmword ptr [rdi + {c_tmaxw}]",
            "vpor ymm3, ymm3, ymm6",
            "vpand ymm3, ymm3, ymm10",                               // what is wrong with the times
            "vpmaddwd ymm1, ymm1, ymmword ptr [rdi + {c_tw2}]",
            "vpshufb ymm1, ymm1, ymmword ptr [rdi + {c_tshuf}]",
            "vpmaddwd ymm1, ymm1, ymmword ptr [rdi + {c_tw3}]",
            "vpmuludq ymm6, ymm1, ymmword ptr [rdi + {c_tmul}]",
            "vpsrlq ymm1, ymm1, 32",
            "vpaddq ymm1, ymm1, ymm6",                               // ts ms of day, ingest s of day | the digits' number
            // ---- The IDs: digits, numbers, ranges.
            "vpcmpgtb xmm7, xmm0, xmmword ptr [rdi + {c_60}]",     // letters
            "vpand xmm6, xmm7, xmmword ptr [rdi + {c_39}]",
            "vpsubb xmm0, xmm0, xmmword ptr [rdi + {c_30}]",
            "vpsubb xmm0, xmm0, xmm6",                               // digit values
            "vpand xmm7, xmm7, xmmword ptr [rdi + {c_26}]",
            "vpaddb xmm7, xmm7, xmmword ptr [rdi + {c_9}]",       // the most each may be
            "vpsubusb xmm7, xmm0, xmm7",
            "vpand xmm15, xmm7, xmm2",                               // ymm15: what is wrong
            "vpand xmm0, xmm0, xmm2",
            "vpmaddubsw xmm0, xmm0, xmmword ptr [rdi + {c_36_1}]",
            "vpmaddwd xmm0, xmm0, xmmword ptr [rdi + {c_1296_1}]",
            "vpmuludq xmm6, xmm0, xmmword ptr [rdi + {c_36p4}]",
            "vpsrlq xmm0, xmm0, 32",
            "vpaddq xmm0, xmm0, xmm6",                               // the two numbers
            "vpermq ymm6, ymm0, 0x50",
            "vpcmpgtq ymm8, ymm8, ymm6",                             // (least > it, bound > it) for each: (no, yes)
            "vpxor ymm8, ymm8, ymmword ptr [rdi + {c_range_ok}]",
            "vpor ymm15, ymm15, ymm8",
            "vpor ymm15, ymm15, ymm3",
            // ---- The next line's masks (into n_*), early, so that its pass need not wait for them.
            $($nhead,)*
            "vmovq rcx, xmm0",
            "vpextrq r8, xmm0, 1",
            "imul rcx, qword ptr [rdi + {binv}]",
            "rorx r9, rcx, 17",
            "xor r9, rcx",
            "shr rcx, 34",
            "xor r9, rcx",
            "shr r9, 24",
            "xor r9, qword ptr [rdi + {ekey}]",
            "imul r9, qword ptr [rdi + {ainv}]",                    // the event's number, top 40 bits
            "imul r8, qword ptr [rdi + {binv}]",
            "rorx r14, r8, 17",
            "xor r14, r8",
            "shr r8, 34",
            "xor r14, r8",
            "shr r14, 24",
            "xor r14, qword ptr [rdi + {ukey}]",
            "imul r14, qword ptr [rdi + {ainv}]",                   // the user's
            "cmp r9, qword ptr [rdi + {ev_top}]",
            "jae 88f",                  // not a dense event
            "pext r8, r9, qword ptr [rdi + {m_stream}]",            // r8: stream
            "pext rcx, r9, qword ptr [rdi + {m_slot2}]",
            "pext r9, r14, qword ptr [rdi + {m_node}]",
            "or rcx, r9",               // rcx: twice the slot | node << 21
            "pext r9, r14, qword ptr [rdi + {m_alias}]",
            "cmp r14, qword ptr [rdi + {us_top}]",
            "sbb r9, -1",               // r9: 0 for a direct user (in bound, alias 0)
            // ---- The dates: what the memo knows.
            "mov rdx, qword ptr [r11 + 1]",
            "mov eax, dword ptr [r11 + 8]",
            "pext r10d, eax, dword ptr [rdi + {date_bits}]",         // the date's slot
            "cmp rdx, qword ptr [rdi + r10*8 + {key8}]",
            "jne 83f",
            "cmp eax, dword ptr [rdi + r10*4 + {key4}]",
            "jne 83f",
            "vmovq xmm6, qword ptr [rdi + r10*8 + {ms}]",
            "vpsubq xmm9, xmm6, xmm9",
            "mov r14, r10",             // ingest's date: nearly always the same bytes
            "cmp rdx, qword ptr [r13 + 1]",
            "jne 62f",
            "cmp eax, dword ptr [r13 + 8]",
            "jne 62f",
            "63:",
            "vpinsrq xmm9, xmm9, qword ptr [rdi + r14*8 + {sec}], 1", // (date of ts - form's offset, ingest's date)
            "vpand xmm9, xmm9, xmm11",                               // (ts by date and time only)
            "vpaddq xmm9, xmm9, xmmword ptr [rdi + {c_bias}]",       // (less the window's start, plus the record field's bias)
            "mov eax, dword ptr [r13 + 20]",
            "and eax, dword ptr [rdi + r12*4 + {t_zmask}]",
            "cmp eax, dword ptr [rdi + r12*4 + {t_zpat}]",          // `Z,` or `Z\",`
            "jne 88f",
            "add r13, r12",             // comma 4, less 21
            // ---- The type.
            "vpbroadcastq ymm6, qword ptr [r13 + 21]",
            "vpand ymm6, ymm6, ymmword ptr [rdi + {c_typemask}]",
            "vpcmpeqq ymm6, ymm6, ymmword ptr [rdi + {c_typepat}]", // `,view,` `,cart,` `,\"view\",` `,\"cart\",`
            "vmovmskpd r12d, ymm6",
            "cmp r12d, 1",
            "sbb r12, r12",             // r12: 0 for a view or cart, else all ones
            // ---- The rest of the line: from comma 4 to the newline.
            "vpmovmskb eax, ymm4",
            "vpmovmskb edx, ymm5",
            "lea r10, [r13 + 53]",
            "sub r10, rsi",             // how far into the 32 bytes comma 4 is
            "cmp r10, 31",
            "ja 88f",                   // more than 32 bytes from comma 4 to the newline (or less than none)
            "shrx r11, rax, r10",       // C: the commas from comma 4 to the newline (bit 0: comma 4)
            "bts rax, 32",
            "shrx rax, rax, r10",       // S: and the newline
            "shrx rdx, rdx, r10",       // Q: the quotes
            "popcnt r10, r11",
            "cmp r10, 3",
            "jne 88f",                  // not exactly three commas
            "lea r10, [rax + rax]",
            "and r10, rdx",             // O: the quotes right after a separator (a field opens with them)
            "andn r14, r10, rdx",       // the other quotes
            "add r14, r14",
            "mov r15, rax",
            "sub r15, r10",
            "andn r15, r15, rax",       // the separators after a field that opened (each opening carried up to the next one)
            "cmp r15, r14",
            "jne 88f",                  // the other quotes are not exactly those right before them
            "lea r10, [rdx + rdx]",
            "and r10, rdx",
            "or r10, rax",
            "add r10, r10",
            "and r10, r11",             // commas that end an empty value (none or `""`)
            "or r12, r10",              // (an empty type or product: not a plain view or cart)
            // ---- All checked?
            "vptest ymm15, ymm15",
            "jnz 88f",
            "vpand ymm1, ymm1, ymm11",                               // the one ts has: by date and time, or the digits
            "vextracti128 xmm3, ymm1, 1",
            "vpaddq xmm1, xmm1, xmm9",                               // with the dates, less the form's offset
            "vpaddq xmm1, xmm1, xmm3",                               // (ts in ms - window start, ingest in s + bias)
            "or r12, r9",
            // ---- A valid view or cart?
            "vmovq rax, xmm1",          // rax: ts less the window's start, ms
            "vpextrq rdx, xmm1, 1",     // rdx: ingest, s, plus the record field's bias
            "test r12, r12",
            "jnz 85f",
            "cmp rax, qword ptr [rdi + {span}]",
            "jae 86f",                  // outside the window
            "imul r11, rdx, 1000",
            "sub r11, rax",
            "cmp r11, qword ptr [rdi + {xspan}]",
            "ja 86f",                   // ingested over a day late (or ages before)
            "add rax, qword ptr [rdi + {wso}]",
            "shl rdx, 33",
            "lea rdx, [rdx + rax*4]",   // rdx: ingest31 << 33 | ts offset << 2
            // ---- The record goes to its stream, whose count, write pointer and waiting items are one 128-byte block.
            "shl r8, 7",
            "add r8, qword ptr [rdi + {streams}]",
            "mov eax, dword ptr [r8]",
            "lea r9d, [rax + 1]",
            "mov dword ptr [r8], r9d",
            "and eax, 7",
            "cmp eax, 7",
            "je 70f",                   // the eighth: all eight go out
            "shl eax, 4",
            "mov qword ptr [r8 + rax + 16], rdx",
            "mov qword ptr [r8 + rax + 24], rcx",
            "71:",
            "cmp rsi, qword ptr [rdi + {limit}]",
            "jb 2b",
            "mov eax, 7",               // no line may start here
            "99:",
            "mov qword ptr [rdi + {s_line}], rbx",
            "mov rbx, qword ptr [rdi + {s_rbx}]",
            // ---- Out of line.
            ".pushsection .text.unlikely.ss_ka,\"ax\",@progbits",
            // Ingest's date is not the date of ts: its own slot.
            "62:",
            "mov rdx, qword ptr [r13 + 1]",
            "mov eax, dword ptr [r13 + 8]",
            "pext r14d, eax, dword ptr [rdi + {date_bits}]",
            "cmp rdx, qword ptr [rdi + r14*8 + {key8}]",
            "jne 84f",
            "cmp eax, dword ptr [rdi + r14*4 + {key4}]",
            "jne 84f",
            "jmp 63b",
            "82:",
            "mov qword ptr [rdi + {s_key}], rax",
            "mov qword ptr [rdi + {s_idx}], rcx",
            "mov qword ptr [rdi + {s_n}], r10",
            "mov eax, 2",
            "jmp 99b",
            "83:",
            "inc r11",
            "mov qword ptr [rdi + {s_ptr}], r11",
            "mov qword ptr [rdi + {s_idx}], r10",
            "mov eax, 3",
            "jmp 99b",
            "84:",
            "inc r13",
            "mov qword ptr [rdi + {s_ptr}], r13",
            "mov qword ptr [rdi + {s_idx}], r14",
            "mov eax, 3",
            "jmp 99b",
            "85:",
            "86:",
            "add rax, qword ptr [rdi + {ws}]",
            "sub rdx, qword ptr [rdi + {rbias}]",
            "mov qword ptr [rdi + {s_t}], rax",
            "mov qword ptr [rdi + {s_ing}], rdx",
            "mov qword ptr [rdi + {s_w1}], rcx",
            "mov qword ptr [rdi + {s_stream}], r8",
            "mov qword ptr [rdi + {s_next}], rsi",
            "mov qword ptr [rdi + {s_indirect}], r9",
            "mov eax, 5",
            "jmp 99b",
            "88:",
            "mov eax, 1",
            "jmp 99b",
            // The stream's seven waiting items and this one, out to where its items are kept.
            "70:",
            "lea eax, [r9 - 8]",        // the first one's index
            "test eax, {chunk_mask}",
            "jz 74f",                   // they start a chunk, which has to be made first
            "mov r9, qword ptr [r8 + 8]",
            "vmovdqu ymm0, ymmword ptr [r8 + 16]",
            "vmovdqu ymm1, ymmword ptr [r8 + 48]",
            "vmovdqu ymm2, ymmword ptr [r8 + 80]",
            "vmovdqu xmm3, xmmword ptr [r8 + 112]",
            "vmovq xmm4, rdx",
            "vpinsrq xmm4, xmm4, rcx, 1",
            "vinserti128 ymm3, ymm3, xmm4, 1",
            "vmovntdq ymmword ptr [r9], ymm0",
            "vmovntdq ymmword ptr [r9 + 32], ymm1",
            "vmovntdq ymmword ptr [r9 + 64], ymm2",
            "vmovntdq ymmword ptr [r9 + 96], ymm3",
            "add r9, 128",
            "mov qword ptr [r8 + 8], r9",
            "jmp 71b",
            "74:",
            "sub dword ptr [r8], 1",    // (not appended after all: the code outside does it)
            "sub r8, qword ptr [rdi + {streams}]",
            "shr r8, 7",                // the stream's number again
            "mov eax, 9",
            "jmp 99b",
            ".popsection",
            ahead = const AHEAD,
            chunk_mask = const crate::dense::EvLists::CHUNK_MASK,
            streams = const offset_of!(K, streams),
            limit = const offset_of!(K, limit),
            s_line = const offset_of!(K, s_line),
            s_rbx = const offset_of!(K, s_rbx),
            t_keep = const offset_of!(K, t_keep),
            t_range = const offset_of!(K, t_range),
            t_fmask = const offset_of!(K, t_fmask),
            t_ftag = const offset_of!(K, t_ftag),
            form_bits = const offset_of!(K, form_bits),
            fkey = const offset_of!(K, fkey),
            t_counts = const offset_of!(K, t_counts),
            t_sel = const offset_of!(K, t_sel),
            date_bits = const offset_of!(K, date_bits),
            key8 = const offset_of!(K, key8),
            key4 = const offset_of!(K, key4),
            ms = const offset_of!(K, ms),
            fval = const offset_of!(K, fval),
            sec = const offset_of!(K, sec),
            t_zmask = const offset_of!(K, t_zmask),
            t_zpat = const offset_of!(K, t_zpat),
            c_60 = const offset_of!(K, c_60),
            c_39 = const offset_of!(K, c_39),
            c_30 = const offset_of!(K, c_30),
            c_26 = const offset_of!(K, c_26),
            c_9 = const offset_of!(K, c_9),
            c_tpat = const offset_of!(K, c_tpat),
            c_tmax = const offset_of!(K, c_tmax),
            c_tw1 = const offset_of!(K, c_tw1),
            c_tmaxw = const offset_of!(K, c_tmaxw),
            c_typemask = const offset_of!(K, c_typemask),
            c_typepat = const offset_of!(K, c_typepat),
            c_36_1 = const offset_of!(K, c_36_1),
            c_1296_1 = const offset_of!(K, c_1296_1),
            c_36p4 = const offset_of!(K, c_36p4),
            c_tw2 = const offset_of!(K, c_tw2),
            c_tshuf = const offset_of!(K, c_tshuf),
            c_tw3 = const offset_of!(K, c_tw3),
            c_tmul = const offset_of!(K, c_tmul),
            c_range_ok = const offset_of!(K, c_range_ok),
            binv = const offset_of!(K, binv),
            ekey = const offset_of!(K, ekey),
            ainv = const offset_of!(K, ainv),
            ukey = const offset_of!(K, ukey),
            ev_top = const offset_of!(K, ev_top),
            m_stream = const offset_of!(K, m_stream),
            m_slot2 = const offset_of!(K, m_slot2),
            m_node = const offset_of!(K, m_node),
            m_alias = const offset_of!(K, m_alias),
            us_top = const offset_of!(K, us_top),
            ws = const offset_of!(K, ws),
            span = const offset_of!(K, span),
            c_bias = const offset_of!(K, c_bias),
            n_c = const offset_of!(K, n_c),
            n_q = const offset_of!(K, n_q),
            n_nl = const offset_of!(K, n_nl),
            xspan = const offset_of!(K, xspan),
            wso = const offset_of!(K, wso),
            rbias = const offset_of!(K, rbias),
            s_key = const offset_of!(K, s_key),
            s_idx = const offset_of!(K, s_idx),
            s_n = const offset_of!(K, s_n),
            s_ptr = const offset_of!(K, s_ptr),
            s_t = const offset_of!(K, s_t),
            s_ing = const offset_of!(K, s_ing),
            s_w1 = const offset_of!(K, s_w1),
            s_stream = const offset_of!(K, s_stream),
            s_next = const offset_of!(K, s_next),
            s_indirect = const offset_of!(K, s_indirect),
            inout("rsi") a0,
            in("rdi") kp,
            $($regs)*
            out("rax") st,
            out("rdx") w0,
            out("rcx") w1,
            out("r8") stream,
            out("r9") _, out("r10") _, out("r11") _, out("r12") _, out("r13") _, out("r14") _, out("r15") _,
            out("ymm0") _, out("ymm1") _, out("ymm2") _, out("ymm3") _, out("ymm4") _, out("ymm5") _, out("ymm6") _, out("ymm7") _,
            out("ymm8") _, out("ymm9") _, out("ymm10") _, out("ymm11") _, out("ymm15") _,
            options(nostack),
        );
        if st == ST_END {
            break;
        }
        if st == ST_CHUNK {
            out.ev.push16(stream as usize, w0, w1, &mut out.arena);
            continue;
        }
        let line = (*kp).s_line as *const u8;
        let next = cold(&mut *kp, st, line, file, end, ctx, dn, out);
        if next == usize::MAX {
            a0 = line;
            break;
        }
        a0 = base.add(next - 1);
    }
    Some(a0.offset_from(base) as usize + 1)
}
    };
}

define_events!(
    events, _mm256_set1_epi8, [commas quotes newlines],
    [
        "vpcmpeqb ymm6, ymm14, ymmword ptr [rsi + 41]",
        "vpcmpeqb ymm7, ymm14, ymmword ptr [rsi + 73]",
        "vpcmpeqb ymm2, ymm12, ymmword ptr [rsi]",
        "vpcmpeqb ymm3, ymm12, ymmword ptr [rsi + 32]",
        "vpcmpeqb ymm0, ymm13, ymmword ptr [rsi]",
        "vpcmpeqb ymm1, ymm13, ymmword ptr [rsi + 32]",
        "vpmovmskb r15d, ymm6",
        "vpmovmskb r14d, ymm7",
        "vpmovmskb eax, ymm2",
        "vpmovmskb ecx, ymm3",
        "vpmovmskb edx, ymm0",
        "vpmovmskb r8d, ymm1",
        "shl r14, 32",
        "or r15, r14",
        "shl rcx, 32",
        "or rax, rcx",
        "shl r8, 32",
        "or rdx, r8",
    ],
    [
        "vpcmpeqb ymm6, ymm14, ymmword ptr [rsi + 41]",
        "vpcmpeqb ymm7, ymm14, ymmword ptr [rsi + 73]",
        "vpcmpeqb ymm10, ymm12, ymmword ptr [rsi]",
        "vpcmpeqb ymm3, ymm12, ymmword ptr [rsi + 32]",
        "vpmovmskb eax, ymm6",
        "vpmovmskb r10d, ymm7",
        "vpmovmskb edx, ymm10",
        "vpmovmskb r14d, ymm3",
        "vpcmpeqb ymm6, ymm13, ymmword ptr [rsi]",
        "vpcmpeqb ymm7, ymm13, ymmword ptr [rsi + 32]",
        "shl r10, 32",
        "or rax, r10",
        "tzcnt rax, rax",
        "mov qword ptr [rdi + {n_nl}], rax",
        "shl r14, 32",
        "or rdx, r14",
        "mov qword ptr [rdi + {n_c}], rdx",
        "vpmovmskb eax, ymm6",
        "vpmovmskb r10d, ymm7",
        "shl r10, 32",
        "or rax, r10",
        "mov qword ptr [rdi + {n_q}], rax",
    ],
    [in("ymm12") commas, in("ymm13") quotes, in("ymm14") newlines,]
);

// The same with 64-byte compares into mask registers (AVX-512BW): a third
// of the instructions for the masks.
define_events!(
    #[target_feature(enable = "avx512f,avx512bw")]
    events512, _mm512_set1_epi8, [commas quotes newlines],
    [
        "vpcmpeqb k1, zmm14, zmmword ptr [rsi + 41]",
        "vpcmpeqb k2, zmm12, zmmword ptr [rsi]",
        "vpcmpeqb k3, zmm13, zmmword ptr [rsi]",
        "kmovq r15, k1",
        "kmovq rax, k2",
        "kmovq rdx, k3",
    ],
    [
        "vpcmpeqb k1, zmm14, zmmword ptr [rsi + 41]",
        "vpcmpeqb k2, zmm12, zmmword ptr [rsi]",
        "vpcmpeqb k3, zmm13, zmmword ptr [rsi]",
        "kmovq rax, k1",
        "tzcnt rax, rax",
        "mov qword ptr [rdi + {n_nl}], rax",
        "kmovq rax, k2",
        "mov qword ptr [rdi + {n_c}], rax",
        "kmovq rax, k3",
        "mov qword ptr [rdi + {n_q}], rax",
    ],
    [in("zmm12") commas, in("zmm13") quotes, in("zmm14") newlines, out("k1") _, out("k2") _, out("k3") _,]
);
