use std::collections::LinkedList;

use common_base::buffer::Buffer;
use common_base::buffer::Error::Underflow;

#[derive(Debug)]
pub(crate) struct Chunk<const N: usize> {
    pub data: [u8; N],
    pub read: usize,
    pub write: usize,
}

impl<const N: usize> Default for Chunk<N> {
    fn default() -> Self {
        let data = [0u8; N];
        Self {
            write: 0,
            read: 0,
            data,
        }
    }
}

impl<const N: usize> Chunk<N> {
    pub fn new(data: [u8; N], read: usize) -> Self {
        Self {
            write: 0,
            read,
            data,
        }
    }

    pub fn len(&self) -> usize {
        self.write - self.read
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Calling write will advance write cursor.
    pub fn write(&mut self, src: &[u8]) -> usize {
        let size = src.len().min(N - self.write);
        (&mut self.data[self.write..(self.write + size)]).copy_from_slice(&src[0..size]);
        self.write += size;
        size
    }

    /// allows short read.
    /// Calling read **will not** advance read cursor, must call `advance` manually.
    pub fn read(&self, dst: &mut [u8]) -> usize {
        let size = self.len().min(dst.len());
        let range = self.read..(self.read + size);
        (&mut dst[0..size]).copy_from_slice(&self.data[range]);
        size
    }

    pub fn advance(&mut self, by: usize) -> usize {
        assert!(
            self.write >= self.read,
            "Illegal chunk state, read: {}, write: {}",
            self.read,
            self.write
        );
        let step = by.min(self.write - self.read);
        self.read += step;
        step
    }
}

pub struct CompositeChunk {
    chunks: LinkedList<Chunk<4096>>,
}

impl CompositeChunk {
    pub fn new() -> Self {
        Self {
            chunks: LinkedList::new(),
        }
    }
}

impl Buffer for CompositeChunk {
    fn remaining_size(&self) -> usize {
        self.chunks.iter().map(|c| c.len()).sum()
    }

    fn read_to_slice(&mut self, mut dst: &mut [u8]) -> common_base::buffer::Result<()> {
        if self.remaining_size() < dst.len() {
            return Err(Underflow {});
        }

        for c in &self.chunks {
            if dst.is_empty() {
                break;
            }
            let read = c.read(dst);
            dst = &mut dst[read..];
        }

        if !dst.is_empty() {
            return Err(Underflow {});
        }
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

impl CompositeChunk {
    pub(crate) fn add(&mut self, chunk: Chunk<4096>) {
        self.chunks.push_back(chunk);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub fn test_chunk() {
        let mut chunk = Chunk {
            data: [0u8; 4096],
            read: 0,
            write: 0,
        };
        assert_eq!(0, chunk.len());
        assert_eq!(5, chunk.write("hello".as_bytes()));
        assert_eq!(5, chunk.write);
        assert_eq!(0, chunk.read);
        assert_eq!(5, chunk.len());

        let mut dst = [0u8; 3];
        assert_eq!(3, chunk.read(&mut dst));
        assert_eq!(5, chunk.write);
        assert_eq!(0, chunk.read);
        assert_eq!(5, chunk.len());
    }

    #[test]
    pub fn test_chunk_short_read() {
        let mut chunk = Chunk {
            data: [0u8; 4096],
            read: 0,
            write: 0,
        };
        assert_eq!(5, chunk.write("hello".as_bytes()));

        let mut dst = vec![0u8; 8];
        let read = chunk.read(&mut dst);
        assert_eq!(5, read);
        assert_eq!(vec![b'h', b'e', b'l', b'l', b'o', 0x0, 0x0, 0x0], dst);
    }

    #[test]
    pub fn test_chunk_advance() {
        let mut chunk = Chunk {
            data: [0u8; 4096],
            read: 0,
            write: 0,
        };
        assert_eq!(5, chunk.write("hello".as_bytes()));
        let mut dst = vec![0u8; 8];
        assert_eq!(5, chunk.read(&mut dst));
        assert_eq!(0, chunk.read);
        assert_eq!(5, chunk.write);

        assert_eq!(1, chunk.advance(1));
        assert_eq!(1, chunk.read);
        assert_eq!(5, chunk.write);

        assert_eq!(4, chunk.advance(5));
        assert_eq!(5, chunk.read);
        assert_eq!(5, chunk.write);
    }

    #[test]
    pub fn test_composite_chunk_read() {
        let mut chunks = CompositeChunk {
            chunks: LinkedList::new(),
        };

        let mut c1 = Chunk::default();
        assert_eq!(4, c1.write("abcd".as_bytes()));

        let mut c2 = Chunk::default();
        assert_eq!(5, c2.write("12345".as_bytes()));

        chunks.add(c1);
        chunks.add(c2);

        let mut dst = [0u8; 2];
        chunks.read_to_slice(&mut dst).unwrap();
        chunks.advance_by(2);
        assert_eq!([b'a', b'b'], dst);
        assert_eq!(2, chunks.chunks.len());

        let mut dst = [0u8; 3];
        chunks.read_to_slice(&mut dst).unwrap();
        chunks.advance_by(3);
        assert_eq!([b'c', b'd', b'1'], dst);
        assert_eq!(4, chunks.remaining_size());
        assert_eq!(1, chunks.chunks.len());

        let mut dst = [0u8; 4];
        chunks.read_to_slice(&mut dst).unwrap();
        chunks.advance_by(4);
        assert_eq!([b'2', b'3', b'4', b'5'], dst);
        assert_eq!(0, chunks.remaining_size());
        assert_eq!(0, chunks.chunks.len());

        let mut c3 = Chunk::default();
        c3.write("uvwxyz".as_bytes());
        chunks.add(c3);
        assert_eq!(6, chunks.remaining_size());
        assert_eq!(1, chunks.chunks.len());
    }
}
