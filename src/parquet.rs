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

/// Bit-packs `values` (each below 2^width) as one RLE-hybrid bit-packed run.
pub fn bitpack_run(out: &mut Vec<u8>, values: impl ExactSizeIterator<Item = u32>, width: u32) {
    let n = values.len();
    let groups = n.div_ceil(8);
    let mut header = ((groups as u64) << 1) | 1;
    while header >= 0x80 {
        out.push((header as u8) | 0x80);
        header >>= 7;
    }
    out.push(header as u8);
    if width == 0 {
        return;
    }
    let start = out.len();
    let mut acc: u64 = 0;
    let mut bits: u32 = 0;
    for v in values {
        acc |= (v as u64) << bits;
        bits += width;
        while bits >= 8 {
            out.push(acc as u8);
            acc >>= 8;
            bits -= 8;
        }
    }
    // Pad the last group of 8 values with zeros.
    let want = groups * width as usize;
    while out.len() - start < want {
        out.push(acc as u8);
        acc >>= 8;
    }
}

/// Definition levels for a data page v1: a 4-byte length, then the levels.
pub fn def_levels(out: &mut Vec<u8>, present: impl ExactSizeIterator<Item = bool>) {
    let at = out.len();
    out.extend_from_slice(&[0; 4]);
    bitpack_run(out, present.map(|p| p as u32), 1);
    let len = (out.len() - at - 4) as u32;
    out[at..at + 4].copy_from_slice(&len.to_le_bytes());
}

pub fn bit_width(max: u32) -> u32 {
    32 - max.leading_zeros()
}
