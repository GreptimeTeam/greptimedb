use std::collections::LinkedList;

use common_base::buffer::Buffer;
use common_base::buffer::UnderflowSnafu;
use snafu::ensure;

pub const DEFAULT_CHUNK_SIZE: usize = 4096;

#[derive(Debug)]
pub(crate) struct Chunk {
    // internal data
    pub data: Box<[u8]>,
    // read offset
    pub read_offset: usize,
    // write offset
    pub write_offset: usize,
}

impl Default for Chunk {
    fn default() -> Self {
        let data = vec![0u8; DEFAULT_CHUNK_SIZE].into_boxed_slice();
        Self {
            write_offset: 0,
            read_offset: 0,
            data,
        }
    }
}

impl Chunk {
    #[cfg(test)]
    pub fn copy_from_slice(s: &[u8]) -> Self {
        let src_len = s.len();
        // before [box syntax](https://github.com/rust-lang/rust/issues/49733) becomes stable,
        // we can only initialize an array on heap like this.
        let mut data = vec![0u8; src_len].into_boxed_slice();
        data[0..src_len].copy_from_slice(s);
        Self {
            read_offset: 0,
            write_offset: src_len,
            data,
        }
    }

    pub fn new(data: Box<[u8]>, write: usize) -> Self {
        Self {
            write_offset: write,
            read_offset: 0,
            data,
        }
    }

    pub fn len(&self) -> usize {
        self.write_offset - self.read_offset
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// allows short read.
    /// Calling read **will not** advance read cursor, must call `advance` manually.
    pub fn read(&self, dst: &mut [u8]) -> usize {
        let size = self.len().min(dst.len());
        let range = self.read_offset..(self.read_offset + size);
        (&mut dst[0..size]).copy_from_slice(&self.data[range]);
        size
    }

    pub fn advance(&mut self, by: usize) -> usize {
        assert!(
            self.write_offset >= self.read_offset,
            "Illegal chunk state, read: {}, write: {}",
            self.read_offset,
            self.write_offset
        );
        let step = by.min(self.write_offset - self.read_offset);
        self.read_offset += step;
        step
    }
}

pub struct ChunkList {
    chunks: LinkedList<Chunk>,
}

impl ChunkList {
    pub fn new() -> Self {
        Self {
            chunks: LinkedList::new(),
        }
    }

    pub(crate) fn push(&mut self, chunk: Chunk) {
        self.chunks.push_back(chunk);
    }
}

impl Buffer for ChunkList {
    fn remaining_size(&self) -> usize {
        self.chunks.iter().map(|c| c.len()).sum()
    }

    fn peek_to_slice(&self, mut dst: &mut [u8]) -> common_base::buffer::Result<()> {
        ensure!(self.remaining_size() >= dst.len(), UnderflowSnafu);

        for c in &self.chunks {
            if dst.is_empty() {
                break;
            }
            let read = c.read(dst);
            dst = &mut dst[read..];
        }

        ensure!(dst.is_empty(), UnderflowSnafu);
        Ok(())
    }

    fn read_to_slice(&mut self, dst: &mut [u8]) -> common_base::buffer::Result<()> {
        self.peek_to_slice(dst)?;
        self.advance_by(dst.len());
        Ok(())
    }

    fn advance_by(&mut self, by: usize) {
        let mut left = by;
        while left > 0 {
            if let Some(c) = self.chunks.front_mut() {
                let actual = c.advance(left);
                if c.is_empty() {
                    self.chunks.pop_front(); // remove first chunk
                }
                left -= actual;
            } else {
                panic!("Advance step [{}] exceeds max readable bytes", by);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub fn test_chunk() {
        let chunk: Chunk = Chunk::copy_from_slice("hello".as_bytes());
        assert_eq!(5, chunk.write_offset);
        assert_eq!(0, chunk.read_offset);
        assert_eq!(5, chunk.len());

        let mut dst = [0u8; 3];
        assert_eq!(3, chunk.read(&mut dst));
        assert_eq!(5, chunk.write_offset);
        assert_eq!(0, chunk.read_offset);
        assert_eq!(5, chunk.len());
    }

    #[test]
    pub fn test_chunk_short_read() {
        let chunk: Chunk = Chunk::copy_from_slice("hello".as_bytes());

        let mut dst = vec![0u8; 8];
        let read = chunk.read(&mut dst);
        assert_eq!(5, read);
        assert_eq!(vec![b'h', b'e', b'l', b'l', b'o', 0x0, 0x0, 0x0], dst);
    }

    #[test]
    pub fn test_chunk_advance() {
        let mut chunk: Chunk = Chunk::copy_from_slice("hello".as_bytes());
        let mut dst = vec![0u8; 8];
        assert_eq!(5, chunk.read(&mut dst));
        assert_eq!(0, chunk.read_offset);
        assert_eq!(5, chunk.write_offset);

        assert_eq!(1, chunk.advance(1));
        assert_eq!(1, chunk.read_offset);
        assert_eq!(5, chunk.write_offset);

        assert_eq!(4, chunk.advance(5));
        assert_eq!(5, chunk.read_offset);
        assert_eq!(5, chunk.write_offset);
    }

    #[test]
    pub fn test_composite_chunk_read() {
        let mut chunks = ChunkList {
            chunks: LinkedList::new(),
        };

        chunks.push(Chunk::copy_from_slice("abcd".as_bytes()));
        chunks.push(Chunk::copy_from_slice("12345".as_bytes()));
        assert_eq!(9, chunks.remaining_size());

        let mut dst = [0u8; 2];
        chunks.peek_to_slice(&mut dst).unwrap();
        chunks.advance_by(2);
        assert_eq!([b'a', b'b'], dst);
        assert_eq!(2, chunks.chunks.len());

        let mut dst = [0u8; 3];
        chunks.peek_to_slice(&mut dst).unwrap();
        chunks.advance_by(3);
        assert_eq!([b'c', b'd', b'1'], dst);
        assert_eq!(4, chunks.remaining_size());
        assert_eq!(1, chunks.chunks.len());

        let mut dst = [0u8; 4];
        chunks.peek_to_slice(&mut dst).unwrap();
        chunks.advance_by(4);
        assert_eq!([b'2', b'3', b'4', b'5'], dst);
        assert_eq!(0, chunks.remaining_size());
        assert_eq!(0, chunks.chunks.len());

        chunks.push(Chunk::copy_from_slice("uvwxyz".as_bytes()));
        assert_eq!(6, chunks.remaining_size());
        assert_eq!(1, chunks.chunks.len());
    }
}
