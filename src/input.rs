//! Input files: read a chunk at a time into a small per-thread buffer that
//! stays in cache, or mapped whole and parsed in place.

use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;

use crate::util::{memchr, sys};

pub struct Src {
    pub file: std::fs::File,
    pub len: usize,
    /// Where file offset 0 is mapped (0: not mapped). A readable page of
    /// zeros comes before the file's pages and another after them. (Atomic
    /// so that any thread can map a shared file: see `map_shared`; whoever
    /// reads the file after that must have synchronized with that thread.)
    map: AtomicUsize,
}

impl Src {
    /// Map the whole file read-only. On failure it simply stays unmapped.
    pub fn map(&mut self) {
        let at = self.map_addr();
        self.set_map(at);
    }

    /// Where `map` would put the file: maps it and gives the address of
    /// file offset 0 (0: not mapped, or already mapped), without taking it
    /// up yet (see `set_map`), so that any thread can do the mapping.
    pub fn map_addr(&self) -> usize {
        use std::os::unix::io::AsRawFd;
        const PAGE: usize = 4096;
        if self.len == 0 || self.m() != 0 {
            return 0;
        }
        let pages = (self.len + PAGE - 1) & !(PAGE - 1);
        unsafe {
            let all = sys::mmap(
                std::ptr::null_mut(),
                pages + 2 * PAGE,
                sys::PROT_READ,
                sys::MAP_PRIVATE | sys::MAP_ANONYMOUS | sys::MAP_NORESERVE,
                -1,
                0,
            );
            if all == sys::MAP_FAILED {
                return 0;
            }
            let at = (all as usize + PAGE) as *mut std::ffi::c_void;
            let m = sys::mmap(at, pages, sys::PROT_READ, sys::MAP_PRIVATE | sys::MAP_FIXED, self.file.as_raw_fd(), 0);
            if m == sys::MAP_FAILED {
                return 0;
            }
            at as usize
        }
    }

    /// Take up a mapping that `map_addr` made for this file (0: none).
    pub fn set_map(&mut self, at: usize) {
        if at != 0 && self.m() == 0 {
            *self.map.get_mut() = at;
        }
    }

    /// `map` through a shared reference (one thread per file at a time).
    pub fn map_shared(&self) {
        let at = self.map_addr();
        if at != 0 {
            self.map.store(at, Relaxed);
        }
    }

    #[inline(always)]
    fn m(&self) -> usize {
        self.map.load(Relaxed)
    }

    /// Ask the system to map the pages holding file offsets `a..b` now, in
    /// one call, rather than fault by fault as the parser reaches them.
    pub fn populate(&self, a: usize, b: usize) {
        const MADV_POPULATE_READ: i32 = 22;
        let map = self.m();
        if map != 0 {
            let lo = (map + a) & !4095;
            let hi = (map + b.min(self.len) + 4095) & !4095;
            unsafe { sys::madvise(lo as *mut std::ffi::c_void, hi - lo, MADV_POPULATE_READ) };
        }
    }

    pub fn mapped(&self) -> bool {
        self.m() != 0
    }

    /// Let go of a mapped file's pages (cheaper done by several threads
    /// before the end than by process exit alone). It must not be read after.
    pub fn unmap(&self) {
        let map = self.m();
        if map != 0 {
            unsafe { sys::madvise(map as *mut std::ffi::c_void, (self.len + 4095) & !4095, sys::MADV_DONTNEED) };
        }
    }

    /// For a mapped file, what `Reader::load` gives: the data lines that
    /// start in file offsets `a..b` span `buf[start..end]`, the byte before
    /// `start` is a newline, and all of `buf` (which reaches 128 bytes past
    /// `end`) is readable.
    pub fn view(&self, a: usize, b: usize) -> (&[u8], usize, usize) {
        let r0 = a.saturating_sub(1);
        // File offset x is at v[FRONT + x - r0]; the page of zeros before the
        // file covers the first FRONT bytes when r0 is 0, and the one after
        // it covers what is read past the end.
        let data_end = FRONT + self.len - r0;
        let v = unsafe { std::slice::from_raw_parts((self.m() + r0 - FRONT) as *const u8, data_end + 1024) };
        let ib = FRONT + b - r0;
        let lim = (ib - 1).min(data_end);
        let Some(i) = memchr(b'\n', &v[FRONT..lim.max(FRONT)]) else { return (&v[..FRONT + 128], FRONT, FRONT) };
        let start = FRONT + i + 1;
        let end = if ib - 1 >= data_end {
            data_end
        } else {
            match memchr(b'\n', &v[ib - 1..data_end]) {
                Some(j) => ib + j,
                None => data_end,
            }
        };
        let end = end.max(start);
        (&v[..end + 128], start, end)
    }
}

/// Every `*.csv` file directly in `dir`, in name order.
pub fn list_dir(dir: &Path) -> Vec<Src> {
    let mut files: Vec<PathBuf> = match std::fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|x| x == "csv") && p.is_file())
            .collect(),
        Err(_) => Vec::new(),
    };
    files.sort();
    files
        .iter()
        .map(|p| {
            let file = std::fs::File::open(p).unwrap();
            let len = file.metadata().unwrap().len() as usize;
            Src { file, len, map: AtomicUsize::new(0) }
        })
        .collect()
}

/// Bytes kept readable before the data and after it.
const FRONT: usize = 64;
const BACK: usize = 192;

pub struct Reader {
    pub buf: Vec<u8>,
}

impl Reader {
    pub fn new(chunk: usize) -> Reader {
        Reader { buf: vec![0; FRONT + chunk + 8192 + BACK] }
    }

    /// Loads the data lines of `src` that start in file offsets `a..b` (the
    /// header line never counts). They span `buf[start..end]`; the byte before
    /// `start` is a newline, and `buf[..end + 128]` is readable.
    pub fn load(&mut self, src: &Src, a: usize, b: usize) -> (usize, usize) {
        let r0 = a.saturating_sub(1);
        let mut extra = 4096;
        let mut have = 0usize;
        loop {
            // File offset x lives at buf[FRONT + x - r0].
            let need = (b + extra).min(src.len) - r0;
            if self.buf.len() < FRONT + need + BACK {
                self.buf.resize(FRONT + need + BACK, 0);
            }
            while have < need {
                let n = src.file.read_at(&mut self.buf[FRONT + have..FRONT + need], (r0 + have) as u64).expect("read");
                if n == 0 {
                    break;
                }
                have += n;
            }
            let eof = have < need || r0 + have >= src.len;
            let data_end = FRONT + have;
            let ib = FRONT + b - r0;
            // A line starts here if the byte before it, at offsets a-1..b-1,
            // is a newline. buf[FRONT] is offset a-1; for a == 0 it is offset
            // 0, and the first newline found ends the header.
            let lim = (ib - 1).min(data_end);
            let Some(i) = memchr(b'\n', &self.buf[FRONT..lim.max(FRONT)]) else { return (FRONT, FRONT) };
            let start = FRONT + i + 1;
            // The last line starting before b ends at the first newline at
            // offset b-1 or later.
            let end = if ib - 1 >= data_end {
                if !eof {
                    extra *= 4;
                    continue;
                }
                data_end
            } else {
                match memchr(b'\n', &self.buf[ib - 1..data_end]) {
                    Some(j) => ib + j,
                    None if eof => data_end,
                    None => {
                        extra *= 4;
                        continue;
                    }
                }
            };
            return (start, end.max(start));
        }
    }
}
