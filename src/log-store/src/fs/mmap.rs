use bytes::Buf;
use common_base::buffer::Buffer;
use memmap2::Mmap;
use snafu::ensure;

use crate::error::{EofSnafu, Error};

pub struct MmappedBuffer {
    mmap: Mmap,
    offset: usize,
}

impl MmappedBuffer {
    pub fn new(mmap: Mmap) -> Self {
        Self { mmap, offset: 0 }
    }
}

impl AsRef<[u8]> for MmappedBuffer {
    fn as_ref(&self) -> &[u8] {
        &self.mmap.as_ref()[self.offset..]
    }
}

impl Buffer for MmappedBuffer {
    type Error = Error;

    fn remaining_slice(&self) -> &[u8] {
        &self.mmap.as_ref()[self.offset..]
    }

    fn read_to_slice(&mut self, dst: &mut [u8]) -> Result<(), Self::Error> {
        ensure!(self.remaining_size() >= dst.len(), EofSnafu);
        self.remaining_slice().copy_to_slice(dst);
        self.advance_by(dst.len());
        Ok(())
    }

    fn advance_by(&mut self, by: usize) {
        self.offset += by;
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use tempdir::TempDir;

    use super::*;

    fn read_char_from_buffer_unchecked(buf: &mut dyn Buffer<Error = Error>) -> char {
        char::try_from(buf.read_u8_le().unwrap() as u32).unwrap()
    }

    #[test]
    pub fn test_mmap_buffer() {
        let dir = TempDir::new("greptime-test").unwrap();
        let file_path = dir.path().join("test.txt");
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(file_path)
            .unwrap();

        let _ = file.write("hello".as_bytes()).unwrap();
        file.sync_all().unwrap();
        let result = unsafe { Mmap::map(&file).unwrap() };

        let mut buffer = MmappedBuffer::new(result);

        assert_eq!('h', read_char_from_buffer_unchecked(&mut buffer));
        assert_eq!('e', read_char_from_buffer_unchecked(&mut buffer));
        assert_eq!('l', read_char_from_buffer_unchecked(&mut buffer));
        assert_eq!('l', read_char_from_buffer_unchecked(&mut buffer));
        assert_eq!('o', read_char_from_buffer_unchecked(&mut buffer));
    }
}
