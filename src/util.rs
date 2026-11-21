//! Keys, hashing, a small open-addressing map, mmap, and field parsers.

use std::collections::HashMap;
use std::sync::Mutex;

pub const SECOND: i64 = 1_000;
pub const MINUTE: i64 = 60 * SECOND;
pub const HOUR: i64 = 60 * MINUTE;
pub const DAY: i64 = 24 * HOUR;

// ---------------------------------------------------------------- keys

/// An ID as a 128-bit key. IDs of 1..=15 bytes are stored exactly: the bytes
/// left-aligned (big-endian) in the top 15 bytes and the length in the low
/// byte, so comparing two such keys as integers is a byte-wise comparison of
/// the IDs. Longer IDs are interned and carry 0xFF in the low byte.
pub type Key = u128;

static LONG: Mutex<Option<(HashMap<Vec<u8>, u32>, Vec<Vec<u8>>)>> = Mutex::new(None);

#[cold]
#[inline(never)]
fn intern_long(b: &[u8]) -> Key {
    let mut guard = LONG.lock().unwrap();
    let (map, list) = guard.get_or_insert_with(|| (HashMap::new(), Vec::new()));
    let idx = match map.get(b) {
        Some(&i) => i,
        None => {
            let i = list.len() as u32;
            list.push(b.to_vec());
            map.insert(b.to_vec(), i);
            i
        }
    };
    ((idx as u128) << 8) | 0xFF
}

/// Pack a non-empty ID.
#[inline(always)]
pub fn pack(b: &[u8]) -> Key {
    let n = b.len();
    debug_assert!(n > 0);
    if n <= 15 {
        let mut buf = [0u8; 16];
        buf[..n].copy_from_slice(b);
        u128::from_be_bytes(buf) | n as u128
    } else {
        intern_long(b)
    }
}

#[inline(always)]
pub fn is_long(k: Key) -> bool {
    (k as u8) == 0xFF
}

pub fn key_bytes(k: Key, out: &mut Vec<u8>) {
    if is_long(k) {
        let guard = LONG.lock().unwrap();
        let (_, list) = guard.as_ref().unwrap();
        out.extend_from_slice(&list[(k >> 8) as u32 as usize]);
    } else {
        let n = (k as u8) as usize;
        out.extend_from_slice(&k.to_be_bytes()[..n]);
    }
}

/// Byte-wise comparison of the IDs behind two keys.
#[inline(always)]
pub fn cmp_key(a: Key, b: Key) -> std::cmp::Ordering {
    if !is_long(a) && !is_long(b) {
        a.cmp(&b)
    } else {
        cmp_key_slow(a, b)
    }
}

#[cold]
#[inline(never)]
fn cmp_key_slow(a: Key, b: Key) -> std::cmp::Ordering {
    let (mut x, mut y) = (Vec::new(), Vec::new());
    key_bytes(a, &mut x);
    key_bytes(b, &mut y);
    x.cmp(&y)
}

#[inline(always)]
fn wymix(a: u64, b: u64) -> u64 {
    let r = (a as u128).wrapping_mul(b as u128);
    (r as u64) ^ ((r >> 64) as u64)
}

#[inline(always)]
pub fn hash64(k: Key) -> u64 {
    wymix(
        (k as u64) ^ 0x9E37_79B9_7F4A_7C15,
        ((k >> 64) as u64) ^ 0xD6E8_FEB8_6659_FD93,
    )
}

/// A second, independent hash, used to tell apart event IDs whose first
/// hashes collide.
#[inline(always)]
pub fn hash64b(k: Key) -> u64 {
    wymix(
        (k as u64) ^ 0xA076_1D64_78BD_642F,
        ((k >> 64) as u64) ^ 0xE703_7ED1_A0B4_28DB,
    )
}

// ---------------------------------------------------------------- map

/// Open-addressing map from a key (never 0) to a small copyable value.
pub struct KMap<V: Copy> {
    keys: Vec<Key>,
    vals: Vec<V>,
    mask: usize,
    len: usize,
    dflt: V,
}

impl<V: Copy> KMap<V> {
    pub fn with_capacity(n: usize, dflt: V) -> Self {
        let cap = (n.max(4) * 2).next_power_of_two();
        KMap {
            keys: vec![0; cap],
            vals: vec![dflt; cap],
            mask: cap - 1,
            len: 0,
            dflt,
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn clear(&mut self) {
        if self.len > 0 {
            self.keys.iter_mut().for_each(|k| *k = 0);
            self.len = 0;
        }
    }

    /// Clear and make sure `n` entries fit without growing.
    pub fn reset(&mut self, n: usize) {
        let cap = (n.max(4) * 2).next_power_of_two();
        if cap != self.keys.len() {
            self.keys = vec![0; cap];
            self.vals = vec![self.dflt; cap];
            self.mask = cap - 1;
            self.len = 0;
        } else {
            self.clear();
        }
    }

    #[inline(always)]
    pub fn get(&self, k: Key) -> Option<V> {
        let mut i = (hash64(k) as usize) & self.mask;
        loop {
            let kk = unsafe { *self.keys.get_unchecked(i) };
            if kk == k {
                return Some(unsafe { *self.vals.get_unchecked(i) });
            }
            if kk == 0 {
                return None;
            }
            i = (i + 1) & self.mask;
        }
    }

    fn grow(&mut self) {
        let cap = self.keys.len() * 2;
        let old_keys = std::mem::replace(&mut self.keys, vec![0; cap]);
        let old_vals = std::mem::replace(&mut self.vals, vec![self.dflt; cap]);
        self.mask = cap - 1;
        for (k, v) in old_keys.into_iter().zip(old_vals) {
            if k != 0 {
                let mut i = (hash64(k) as usize) & self.mask;
                while self.keys[i] != 0 {
                    i = (i + 1) & self.mask;
                }
                self.keys[i] = k;
                self.vals[i] = v;
            }
        }
    }

    /// Returns the value already stored for `k`, or stores and returns `v`.
    #[inline(always)]
    pub fn get_or_insert(&mut self, k: Key, v: V) -> V {
        if (self.len + 1) * 2 > self.keys.len() {
            self.grow();
        }
        let mut i = (hash64(k) as usize) & self.mask;
        loop {
            let kk = unsafe { *self.keys.get_unchecked(i) };
            if kk == k {
                return unsafe { *self.vals.get_unchecked(i) };
            }
            if kk == 0 {
                self.keys[i] = k;
                self.vals[i] = v;
                self.len += 1;
                return v;
            }
            i = (i + 1) & self.mask;
        }
    }

    pub fn set(&mut self, k: Key, v: V) {
        if (self.len + 1) * 2 > self.keys.len() {
            self.grow();
        }
        let mut i = (hash64(k) as usize) & self.mask;
        loop {
            let kk = self.keys[i];
            if kk == k {
                self.vals[i] = v;
                return;
            }
            if kk == 0 {
                self.keys[i] = k;
                self.vals[i] = v;
                self.len += 1;
                return;
            }
            i = (i + 1) & self.mask;
        }
    }
}

// ---------------------------------------------------------------- mmap

pub struct Mmap {
    ptr: *const u8,
    len: usize,
}

unsafe impl Send for Mmap {}
unsafe impl Sync for Mmap {}

impl Mmap {
    pub fn open(path: &std::path::Path) -> std::io::Result<Mmap> {
        use std::os::unix::io::AsRawFd;
        let file = std::fs::File::open(path)?;
        let len = file.metadata()?.len() as usize;
        if len == 0 {
            return Ok(Mmap {
                ptr: std::ptr::NonNull::dangling().as_ptr(),
                len: 0,
            });
        }
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ,
                libc::MAP_PRIVATE,
                file.as_raw_fd(),
                0,
            )
        };
        if ptr == libc::MAP_FAILED {
            return Err(std::io::Error::last_os_error());
        }
        unsafe {
            libc::madvise(ptr, len, libc::MADV_SEQUENTIAL | libc::MADV_WILLNEED);
        }
        Ok(Mmap {
            ptr: ptr as *const u8,
            len,
        })
    }

    #[inline(always)]
    pub fn bytes(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}

// ---------------------------------------------------------------- CSV

/// A field's value: the quotes around a whole field are syntax. `None` when a
/// quote appears anywhere else.
#[inline(always)]
pub fn unquote(f: &[u8]) -> Option<&[u8]> {
    if memchr::memchr(b'"', f).is_none() {
        return Some(f);
    }
    let n = f.len();
    if n >= 2 && f[0] == b'"' && f[n - 1] == b'"' && memchr::memchr(b'"', &f[1..n - 1]).is_none() {
        Some(&f[1..n - 1])
    } else {
        None
    }
}

/// Split a line into exactly N values, or `None` when it is malformed.
#[inline(always)]
pub fn split<const N: usize>(line: &[u8]) -> Option<[&[u8]; N]> {
    let mut out: [&[u8]; N] = [&[]; N];
    let mut k = 0;
    let mut start = 0;
    for i in memchr::memchr_iter(b',', line) {
        if k == N - 1 {
            return None;
        }
        out[k] = unquote(&line[start..i])?;
        k += 1;
        start = i + 1;
    }
    if k != N - 1 {
        return None;
    }
    out[k] = unquote(&line[start..])?;
    Some(out)
}

// ---------------------------------------------------------------- time

#[inline(always)]
pub fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let yoe = year - era * 400;
    let doy = (153 * ((month + 9) % 12) + 2) / 5 + day - 1;
    era * 146_097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719_468
}

#[inline(always)]
fn d2(b: &[u8], i: usize) -> Option<i64> {
    let a = b[i].wrapping_sub(b'0');
    let c = b[i + 1].wrapping_sub(b'0');
    if a > 9 || c > 9 {
        None
    } else {
        Some((a * 10 + c) as i64)
    }
}

/// `YYYY-MM-DDTHH:MM:SS` (19 bytes) to milliseconds since the epoch, as if UTC.
#[inline(always)]
fn parse_civil(b: &[u8]) -> Option<i64> {
    if b[4] != b'-' || b[7] != b'-' || b[10] != b'T' || b[13] != b':' || b[16] != b':' {
        return None;
    }
    let year = d2(b, 0)? * 100 + d2(b, 2)?;
    let month = d2(b, 5)?;
    let day = d2(b, 8)?;
    let hour = d2(b, 11)?;
    let minute = d2(b, 14)?;
    let second = d2(b, 17)?;
    if month < 1 || month > 12 || day < 1 || hour > 23 || minute > 59 || second > 59 {
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
    Some((days_from_civil(year, month, day) * 86_400 + hour * 3_600 + minute * 60 + second) * 1_000)
}

/// F1 only: `2026-10-03T23:58:10Z`.
#[inline(always)]
pub fn parse_f1(b: &[u8]) -> Option<i64> {
    if b.len() == 20 && b[19] == b'Z' {
        parse_civil(&b[..19])
    } else {
        None
    }
}

/// A `ts` in any of the three forms, in milliseconds since the epoch.
#[inline(always)]
pub fn parse_ts(b: &[u8]) -> Option<i64> {
    let n = b.len();
    if n == 20 && b[19] == b'Z' {
        return parse_civil(&b[..19]);
    }
    if n == 25 && (b[19] == b'+' || b[19] == b'-') {
        let local = parse_civil(&b[..19])?;
        if b[22] != b':' {
            return None;
        }
        let hh = d2(b, 20)?;
        let mm = d2(b, 23)?;
        if hh > 23 || mm > 59 {
            return None;
        }
        let off = (hh * 60 + mm) * MINUTE;
        return Some(if b[19] == b'+' { local - off } else { local + off });
    }
    parse_digits(b, 18).map(|v| v as i64)
}

/// ASCII digits only, leading zeros allowed; `None` when empty, when any byte
/// is not a digit, or when the value has more than `max_digits` significant
/// digits (callers treat that as out of range).
#[inline(always)]
pub fn parse_digits(b: &[u8], max_digits: usize) -> Option<u64> {
    if b.is_empty() {
        return None;
    }
    let mut v: u64 = 0;
    let mut sig = 0usize;
    for &c in b {
        let d = c.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        if sig > 0 || d != 0 {
            sig += 1;
            if sig > max_digits {
                // Keep scanning: a later non-digit still makes it malformed,
                // but either way the value is unusable.
                return None;
            }
            v = v * 10 + d as u64;
        }
    }
    Some(v)
}

/// A purchase price: digits only, value 1..=10,000,000.
#[inline(always)]
pub fn parse_price(b: &[u8]) -> Option<u32> {
    match parse_digits(b, 8) {
        Some(v) if (1..=10_000_000).contains(&v) => Some(v as u32),
        _ => None,
    }
}

// ---------------------------------------------------------------- link map

/// user ID -> person name, for IDs that are not their own name. Key and value
/// share a slot so a lookup touches one cache line.
pub struct LinkMap {
    slots: Vec<[Key; 2]>,
    shift: u32,
    mask: usize,
}

impl LinkMap {
    pub fn with_capacity(n: usize) -> LinkMap {
        let cap = (n.max(8) * 2).next_power_of_two();
        LinkMap { slots: vec![[0, 0]; cap], shift: 64 - cap.trailing_zeros(), mask: cap - 1 }
    }

    pub fn set(&mut self, k: Key, v: Key) {
        let mut i = (hash64(k) >> self.shift) as usize;
        loop {
            let s = &mut self.slots[i];
            if s[0] == 0 || s[0] == k {
                *s = [k, v];
                return;
            }
            i = (i + 1) & self.mask;
        }
    }

    #[inline(always)]
    pub fn prefetch(&self, k: Key) {
        let i = (hash64(k) >> self.shift) as usize;
        unsafe {
            core::arch::x86_64::_mm_prefetch(
                self.slots.as_ptr().add(i) as *const i8,
                core::arch::x86_64::_MM_HINT_T0,
            );
        }
    }

    /// The person an ID belongs to.
    #[inline(always)]
    pub fn person(&self, k: Key) -> Key {
        let mut i = (hash64(k) >> self.shift) as usize;
        loop {
            let s = unsafe { self.slots.get_unchecked(i) };
            if s[0] == k {
                return s[1];
            }
            if s[0] == 0 {
                return k;
            }
            i = (i + 1) & self.mask;
        }
    }
}
