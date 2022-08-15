// TODO(fys): Use bitset crate to replace it
pub struct BitSet {
    // TODO(fys): Is SmallVec or TinyVec better?
    buffer: Vec<u8>,
    nbits: usize,
}

impl BitSet {
    pub fn from_bytes(data: Vec<u8>, nbits: usize) -> Self {
        debug_assert!(data.len() << 3 >= nbits);
        Self {
            buffer: data,
            nbits,
        }
    }

    pub fn with_capacity(size: usize) -> Self {
        let buffer_len = (size + 7) >> 3;
        Self {
            buffer: vec![0; buffer_len],
            nbits: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.nbits
    }

    pub fn is_empty(&self) -> bool {
        self.nbits == 0
    }

    pub fn ones_count(&self) -> usize {
        (0..self.nbits)
            .into_iter()
            .filter(|&i| matches!(self.get(i), Some(true)))
            .count()
    }

    pub fn get(&self, idx: usize) -> Option<bool> {
        if idx >= self.nbits {
            return None;
        }

        let byte_idx = idx >> 3;
        let bit_idx = idx & 7;
        Some((self.buffer[byte_idx] >> bit_idx) & 1 != 0)
    }

    pub fn extend(&mut self, other: &BitSet) {
        let nbits = self.len() + other.len();

        if self.buffer.len() << 3 < nbits {
            let buffer_len = (nbits + 7) >> 3;
            self.buffer.resize(buffer_len, 0);
        }

        for idx in 0..other.len() {
            if let Some(true) = other.get(idx) {
                self.set_bit_uncheck(idx + self.nbits);
            }
        }

        self.nbits = nbits;
    }

    pub fn append(&mut self, to_set: &[bool]) {
        let nbits = self.nbits + to_set.len();

        if self.buffer.len() << 3 < nbits {
            let buffer_len = (nbits + 7) >> 3;
            self.buffer.resize(buffer_len, 0);
        }

        for (idx, is_set) in to_set.iter().enumerate() {
            if *is_set {
                self.set_bit_uncheck(self.nbits + idx);
            }
        }

        self.nbits = nbits;
    }

    pub fn set(&mut self, idx: usize) {
        debug_assert!(idx < self.nbits, "idx should be less than nbits");

        self.set_bit_uncheck(idx);
    }

    pub fn buffer(self) -> Vec<u8> {
        self.buffer
    }

    fn set_bit_uncheck(&mut self, idx: usize) {
        let byte_idx = idx >> 3;
        let bit_idx = idx & 7;
        self.buffer[byte_idx] |= 1 << bit_idx;
    }
}

impl From<Vec<u8>> for BitSet {
    fn from(data: Vec<u8>) -> Self {
        BitSet {
            nbits: data.len() << 3,
            buffer: data,
        }
    }
}

impl From<&[u8]> for BitSet {
    fn from(data: &[u8]) -> Self {
        BitSet {
            buffer: data.into(),
            nbits: data.len() << 3,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::usize;

    use crate::bitset::BitSet;

    #[test]
    fn test_bit_set_get_and_set() {
        let mut bit_set: BitSet = vec![0b0000_0000, 0b0000_0000].into();

        check_bit_set(&bit_set, &[]);
        assert_eq!(16, bit_set.len());
        assert_eq!(0, bit_set.ones_count());

        bit_set.set(0);
        bit_set.set(7);
        bit_set.set(15);

        check_bit_set(&bit_set, &[0, 7, 15]);
        assert_eq!(None, bit_set.get(16));
        assert_eq!(16, bit_set.len());
        assert_eq!(3, bit_set.ones_count());
    }

    #[test]
    fn test_bit_set_extend() {
        let mut bit_set = BitSet::with_capacity(10);

        bit_set.extend(&vec![0b1000_0111].into());

        check_bit_set(&bit_set, &[0, 1, 2, 7]);
        assert_eq!(None, bit_set.get(9));
        assert_eq!(8, bit_set.len());
        assert_eq!(4, bit_set.ones_count());

        let other = BitSet::from_bytes(vec![0b0000_0111], 3);
        bit_set.extend(&other);

        check_bit_set(&bit_set, &[0, 1, 2, 7, 8, 9, 10]);
        assert_eq!(None, bit_set.get(11));
        assert_eq!(11, bit_set.len());
        assert_eq!(7, bit_set.ones_count());
    }

    #[test]
    fn test_ones_count() {
        let bit_set = BitSet::from_bytes(vec![0b1111_1111], 4);

        let ones_count = bit_set.ones_count();

        assert_eq!(4, ones_count);
    }

    #[test]
    fn test_bit_set_append() {
        let mut bit_set: BitSet = vec![0b0000_0001, 0b0000_1000].into();

        bit_set.append(&[true, false]);

        check_bit_set(&bit_set, &[0, 11, 16]);
        assert_eq!(None, bit_set.get(18));
        assert_eq!(18, bit_set.len());
        assert_eq!(3, bit_set.ones_count());
    }

    #[test]
    fn test_bit_set_buffer() {
        let empty_bit_set = BitSet::with_capacity(10);

        assert!(empty_bit_set.is_empty());
        assert_eq!(vec![0, 0], empty_bit_set.buffer());

        let mut bit_set = BitSet::with_capacity(10);

        bit_set.append(&[true, false]);

        let buffer = bit_set.buffer();
        assert_eq!(vec![1, 0], buffer);
    }

    fn check_bit_set(bit_set: &BitSet, set_positions: &[usize]) {
        (0..bit_set.len()).for_each(|idx| {
            let mut is_hit = false;
            for position in set_positions {
                if idx == *position {
                    assert_eq!(Some(true), bit_set.get(idx));
                    is_hit = true;
                    break;
                }
            }
            if !is_hit {
                assert_eq!(Some(false), bit_set.get(idx));
            }
        });
    }
}
