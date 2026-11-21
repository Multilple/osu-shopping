//! A minimal Parquet writer: one row group, uncompressed, data page v1,
//! PLAIN and RLE_DICTIONARY encodings.

use std::io::Write;

pub const T_INT32: i32 = 1;
pub const T_INT64: i32 = 2;
pub const T_BYTE_ARRAY: i32 = 6;

const ENC_PLAIN: i32 = 0;
const ENC_RLE: i32 = 3;
const ENC_RLE_DICT: i32 = 8;

#[derive(Clone, Copy, PartialEq)]
pub enum Logical {
    None,
    String,
    Date,
}

pub struct Column {
    pub name: &'static str,
    pub ptype: i32,
    pub optional: bool,
    pub logical: Logical,
    /// Dictionary page body (PLAIN values) and its value count.
    pub dict: Option<(Vec<u8>, usize)>,
    /// Data page body: definition levels (when optional), then values.
    pub data: Vec<u8>,
}

// ------------------------------------------------------------ thrift compact

struct Thrift {
    buf: Vec<u8>,
    last: Vec<i16>,
    cur: i16,
}

const TC_I32: u8 = 5;
const TC_I64: u8 = 6;
const TC_BINARY: u8 = 8;
const TC_LIST: u8 = 9;
const TC_STRUCT: u8 = 12;

impl Thrift {
    fn new() -> Self {
        Thrift {
            buf: Vec::with_capacity(1024),
            last: Vec::new(),
            cur: 0,
        }
    }
    fn varint(&mut self, mut v: u64) {
        while v >= 0x80 {
            self.buf.push((v as u8) | 0x80);
            v >>= 7;
        }
        self.buf.push(v as u8);
    }
    fn zigzag(&mut self, v: i64) {
        self.varint(((v << 1) ^ (v >> 63)) as u64);
    }
    fn field(&mut self, id: i16, ty: u8) {
        let delta = id - self.cur;
        if delta > 0 && delta <= 15 {
            self.buf.push(((delta as u8) << 4) | ty);
        } else {
            self.buf.push(ty);
            self.zigzag(id as i64);
        }
        self.cur = id;
    }
    fn i32(&mut self, id: i16, v: i32) {
        self.field(id, TC_I32);
        self.zigzag(v as i64);
    }
    fn i64(&mut self, id: i16, v: i64) {
        self.field(id, TC_I64);
        self.zigzag(v);
    }
    fn string(&mut self, id: i16, s: &[u8]) {
        self.field(id, TC_BINARY);
        self.varint(s.len() as u64);
        self.buf.extend_from_slice(s);
    }
    fn begin(&mut self, id: i16) {
        self.field(id, TC_STRUCT);
        self.push();
    }
    /// Start a struct that is a list element (no field header).
    fn push(&mut self) {
        self.last.push(self.cur);
        self.cur = 0;
    }
    fn end(&mut self) {
        self.buf.push(0);
        self.cur = self.last.pop().unwrap();
    }
    fn list(&mut self, id: i16, elem: u8, n: usize) {
        self.field(id, TC_LIST);
        if n < 15 {
            self.buf.push(((n as u8) << 4) | elem);
        } else {
            self.buf.push(0xF0 | elem);
            self.varint(n as u64);
        }
    }
}

fn page_header(kind_dict: bool, size: usize, num_values: usize, encoding: i32) -> Vec<u8> {
    let mut t = Thrift::new();
    t.push();
    t.i32(1, if kind_dict { 2 } else { 0 });
    t.i32(2, size as i32);
    t.i32(3, size as i32);
    if kind_dict {
        t.begin(7);
        t.i32(1, num_values as i32);
        t.i32(2, ENC_PLAIN);
        t.end();
    } else {
        t.begin(5);
        t.i32(1, num_values as i32);
        t.i32(2, encoding);
        t.i32(3, ENC_RLE);
        t.i32(4, ENC_RLE);
        t.end();
    }
    t.end();
    t.buf
}

/// Write `columns` (each holding `num_rows` values) as one Parquet file.
pub fn write_file(path: &std::path::Path, columns: &[Column], num_rows: usize) -> std::io::Result<()> {
    let mut file = std::fs::File::create(path)?;
    let mut pos: u64 = 4;
    file.write_all(b"PAR1")?;

    struct Chunk {
        dict_offset: Option<u64>,
        data_offset: u64,
        total: u64,
    }
    let mut chunks = Vec::new();
    if num_rows > 0 {
        for c in columns {
            let start = pos;
            let mut dict_offset = None;
            if let Some((body, n)) = &c.dict {
                dict_offset = Some(pos);
                let h = page_header(true, body.len(), *n, ENC_PLAIN);
                file.write_all(&h)?;
                file.write_all(body)?;
                pos += (h.len() + body.len()) as u64;
            }
            let data_offset = pos;
            let enc = if c.dict.is_some() { ENC_RLE_DICT } else { ENC_PLAIN };
            let h = page_header(false, c.data.len(), num_rows, enc);
            file.write_all(&h)?;
            file.write_all(&c.data)?;
            pos += (h.len() + c.data.len()) as u64;
            chunks.push(Chunk {
                dict_offset,
                data_offset,
                total: pos - start,
            });
        }
    }

    let mut t = Thrift::new();
    t.push();
    t.i32(1, 1);
    t.list(2, TC_STRUCT, columns.len() + 1);
    // root
    t.push();
    t.string(4, b"schema");
    t.i32(5, columns.len() as i32);
    t.end();
    for c in columns {
        t.push();
        t.i32(1, c.ptype);
        t.i32(3, if c.optional { 1 } else { 0 });
        t.string(4, c.name.as_bytes());
        match c.logical {
            Logical::String => {
                t.i32(6, 0);
                t.begin(10);
                t.begin(1);
                t.end();
                t.end();
            }
            Logical::Date => {
                t.i32(6, 6);
                t.begin(10);
                t.begin(6);
                t.end();
                t.end();
            }
            Logical::None => {}
        }
        t.end();
    }
    t.i64(3, num_rows as i64);
    if num_rows > 0 {
        t.list(4, TC_STRUCT, 1);
        t.push();
        t.list(1, TC_STRUCT, columns.len());
        let mut total: i64 = 0;
        for (c, ch) in columns.iter().zip(&chunks) {
            total += ch.total as i64;
            t.push();
            t.i64(2, ch.dict_offset.unwrap_or(ch.data_offset) as i64);
            t.begin(3);
            t.i32(1, c.ptype);
            if c.dict.is_some() {
                t.list(2, TC_I32, 3);
                t.zigzag(ENC_PLAIN as i64);
                t.zigzag(ENC_RLE as i64);
                t.zigzag(ENC_RLE_DICT as i64);
            } else {
                t.list(2, TC_I32, 2);
                t.zigzag(ENC_PLAIN as i64);
                t.zigzag(ENC_RLE as i64);
            }
            t.list(3, TC_BINARY, 1);
            t.varint(c.name.len() as u64);
            t.buf.extend_from_slice(c.name.as_bytes());
            t.i32(4, 0);
            t.i64(5, num_rows as i64);
            t.i64(6, ch.total as i64);
            t.i64(7, ch.total as i64);
            t.i64(9, ch.data_offset as i64);
            if let Some(d) = ch.dict_offset {
                t.i64(11, d as i64);
            }
            t.end();
            t.end();
        }
        t.i64(2, total);
        t.i64(3, num_rows as i64);
        t.end();
    } else {
        t.list(4, TC_STRUCT, 0);
    }
    t.string(6, b"ss");
    t.end();

    file.write_all(&t.buf)?;
    file.write_all(&(t.buf.len() as u32).to_le_bytes())?;
    file.write_all(b"PAR1")?;
    Ok(())
}

// ------------------------------------------------------------ encoders

/// Definition levels for a data page v1: a 4-byte length, then the levels
/// (`present(i)` for i in 0..n) as one bit-packed run.
#[inline(always)]
pub fn def_levels(out: &mut Vec<u8>, n: usize, present: impl Fn(usize) -> bool) {
    let at = out.len();
    out.extend_from_slice(&[0; 4]);
    pack_run(out, n, 1, |i| present(i) as u32);
    let len = (out.len() - at - 4) as u32;
    out[at..at + 4].copy_from_slice(&len.to_le_bytes());
}

pub fn bit_width(max: u32) -> u32 {
    32 - max.leading_zeros()
}


/// One RLE/bit-packed hybrid bit-packed run of `n` values `idx(0..n)`, each
/// below 2^width (width 1..=16): the run header (number of groups of 8),
/// then the groups, the last one padded with zeros.
#[inline(always)]
pub fn pack_run(out: &mut Vec<u8>, n: usize, width: u32, idx: impl Fn(usize) -> u32) {
    debug_assert!((1..=16).contains(&width));
    let groups = n.div_ceil(8);
    let w = width as usize;
    out.reserve(16 + groups * w + 16);
    let mut header = ((groups as u64) << 1) | 1;
    while header >= 0x80 {
        out.push((header as u8) | 0x80);
        header >>= 7;
    }
    out.push(header as u8);
    let full = n / 8;
    let rest = n - full * 8;
    // Values k of a group as one integer (k in 0..m, at most 4 when w > 8).
    let word = |at: usize, m: usize| {
        let mut acc = 0u64;
        for k in 0..m {
            acc |= (idx(at + k) as u64) << (k * w);
        }
        acc
    };
    unsafe {
        let base = out.as_mut_ptr().add(out.len());
        let mut p = base;
        if w <= 8 {
            for g in 0..full {
                (p as *mut u64).write_unaligned(word(g * 8, 8));
                p = p.add(w);
            }
            if rest > 0 {
                (p as *mut u64).write_unaligned(word(full * 8, rest));
                p = p.add(w);
            }
        } else {
            // Two halves of 4 values (4w bits each); the second starts at
            // byte w/2, 4 bits in when w is odd.
            let (q, r) = (w / 2, 4 * (w & 1));
            let put = |p: *mut u8, lo: u64, hi: u64| {
                (p as *mut u64).write_unaligned(lo);
                let keep = (lo >> ((8 * q) & 63)) & ((1u64 << r) - 1);
                (p.add(q) as *mut u64).write_unaligned((hi << r) | keep);
            };
            for g in 0..full {
                put(p, word(g * 8, 4), word(g * 8 + 4, 4));
                p = p.add(w);
            }
            if rest > 0 {
                let lo = word(full * 8, rest.min(4));
                let hi = if rest > 4 { word(full * 8 + 4, rest - 4) } else { 0 };
                put(p, lo, hi);
                p = p.add(w);
            }
        }
        let len = out.len() + (p as usize - base as usize);
        out.set_len(len);
    }
}

/// The data page body of a required dictionary-encoded column: the bit
/// width, then the indices as one bit-packed run.
#[inline(always)]
pub fn dict_indices(out: &mut Vec<u8>, n: usize, width: u32, idx: impl Fn(usize) -> u32) {
    out.push(width as u8);
    pack_run(out, n, width, idx);
}

/// Dictionary encoding of an INT32 column given in PLAIN layout, when its
/// values span a range below 2^20 and it comes out smaller: (dictionary
/// page body, dictionary size, data page body). The dictionary holds the
/// values in ascending order: every value of the range when it is short,
/// else the distinct ones.
pub fn dict_i32(plain: &[u8]) -> Option<(Vec<u8>, usize, Vec<u8>)> {
    let n = plain.len() / 4;
    if n == 0 {
        return None;
    }
    let get = |i: usize| unsafe { (plain.as_ptr().add(4 * i) as *const i32).read_unaligned() };
    let (mut lo, mut hi) = (i32::MAX, i32::MIN);
    for i in 0..n {
        let v = get(i);
        lo = lo.min(v);
        hi = hi.max(v);
    }
    let range = (hi as i64 - lo as i64) as usize;
    if range >= 1 << 20 {
        return None;
    }
    let off = |v: i32| v.wrapping_sub(lo) as u32;
    let mut dict = Vec::new();
    let mut data = Vec::new();
    let d;
    if range < 4096 {
        d = range as u32 + 1;
        for k in 0..d {
            dict.extend_from_slice(&lo.wrapping_add(k as i32).to_le_bytes());
        }
        let width = bit_width(d - 1).max(1);
        dict_indices(&mut data, n, width, |i| off(get(i)));
    } else {
        let mut map = vec![0u32; range + 1];
        for i in 0..n {
            unsafe { *map.get_unchecked_mut(off(get(i)) as usize) = 1 };
        }
        let mut m = 0u32;
        for (k, x) in map.iter_mut().enumerate() {
            if *x != 0 {
                *x = m;
                m += 1;
                dict.extend_from_slice(&lo.wrapping_add(k as i32).to_le_bytes());
            }
        }
        d = m;
        if d > 1 << 16 {
            return None;
        }
        let width = bit_width(d - 1).max(1);
        if dict.len() + 16 + n * width as usize / 8 >= plain.len() {
            return None;
        }
        dict_indices(&mut data, n, width, |i| unsafe { *map.get_unchecked(off(get(i)) as usize) });
    }
    if dict.len() + data.len() >= plain.len() {
        return None;
    }
    Some((dict, d as usize, data))
}
