//! Reading input files a chunk at a time into a small per-thread buffer that
//! stays in cache, instead of mapping whole files.

use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

use crate::util::memchr;

pub struct Src {
    pub file: std::fs::File,
    pub len: usize,
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
            Src { file, len }
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
