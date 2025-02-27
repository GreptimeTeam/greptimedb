// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::io;
use std::ops::RangeInclusive;

use common_base::BitVec;
/// `BitmapType` enumerates how bitmaps are encoded within the inverted index.
pub use greptime_proto::v1::index::BitmapType;
use roaring::RoaringBitmap;

/// A bitmap representation supporting both BitVec and RoaringBitmap formats.
///
/// This enum provides unified bitmap operations while allowing efficient storage
/// in different formats. The implementation automatically handles type conversions
/// when performing operations between different formats.
///
/// # Examples
///
/// Creating a new Roaring bitmap:
/// ```
/// use bitmap::Bitmap;
/// let bitmap = Bitmap::new_roaring();
/// assert!(bitmap.is_empty());
/// ```
///
/// Creating a full BitVec bitmap:
/// ```
/// use bitmap::Bitmap;
/// let bitmap = Bitmap::full_bitvec(10);
/// assert_eq!(bitmap.count_ones(), 10);
/// ```
#[derive(Debug, Clone, PartialEq)]
pub enum Bitmap {
    Roaring(RoaringBitmap),
    BitVec(BitVec),
}

impl Bitmap {
    /// Creates a new empty BitVec-based bitmap.
    pub fn new_bitvec() -> Self {
        Bitmap::BitVec(BitVec::EMPTY)
    }

    /// Creates a new empty RoaringBitmap-based bitmap.
    pub fn new_roaring() -> Self {
        Bitmap::Roaring(RoaringBitmap::new())
    }

    /// Creates a full BitVec-based bitmap with all bits set to 1.
    ///
    /// # Arguments
    /// * `size` - The number of bits to allocate and set
    pub fn full_bitvec(size: usize) -> Self {
        Bitmap::BitVec(BitVec::repeat(true, size))
    }

    /// Creates a full RoaringBitmap-based bitmap with bits 0..size set to 1.
    ///
    /// # Arguments
    /// * `size` - The exclusive upper bound for the bit range
    pub fn full_roaring(size: usize) -> Self {
        let mut roaring = RoaringBitmap::new();
        roaring.insert_range(0..size as u32);
        Bitmap::Roaring(roaring)
    }

    /// Returns the number of bits set to 1 in the bitmap.
    pub fn count_ones(&self) -> usize {
        match self {
            Bitmap::BitVec(bitvec) => bitvec.count_ones(),
            Bitmap::Roaring(roaring) => roaring.len() as _,
        }
    }

    /// Checks if the bitmap contains no set bits.
    pub fn is_empty(&self) -> bool {
        match self {
            Bitmap::BitVec(bitvec) => bitvec.is_empty(),
            Bitmap::Roaring(roaring) => roaring.is_empty(),
        }
    }

    pub fn insert_range(&mut self, range: RangeInclusive<usize>) {
        match self {
            Bitmap::BitVec(bitvec) => {
                if *range.end() >= bitvec.len() {
                    bitvec.resize(range.end() + 1, false);
                }
                for i in range {
                    bitvec.set(i, true);
                }
            }
            Bitmap::Roaring(roaring) => {
                let range = *range.start() as u32..=*range.end() as u32;
                roaring.insert_range(range);
            }
        }
    }

    /// Serializes the bitmap into a byte buffer using the specified format.
    ///
    /// # Arguments
    /// * `serialize_type` - Target format for serialization
    /// * `writer` - Output writer to write the serialized data
    pub fn serialize_into(
        &self,
        serialize_type: BitmapType,
        mut writer: impl io::Write,
    ) -> io::Result<()> {
        match (self, serialize_type) {
            (Bitmap::BitVec(bitvec), BitmapType::BitVec) => {
                writer.write_all(bitvec.as_raw_slice())?;
            }
            (Bitmap::Roaring(roaring), BitmapType::Roaring) => {
                roaring.serialize_into(writer)?;
            }
            (Bitmap::BitVec(bitvec), BitmapType::Roaring) => {
                let bitmap = Bitmap::bitvec_to_roaring(bitvec.clone());
                bitmap.serialize_into(writer)?;
            }
            (Bitmap::Roaring(roaring), BitmapType::BitVec) => {
                let bitvec = Bitmap::roaring_to_bitvec(roaring);
                writer.write_all(bitvec.as_raw_slice())?;
            }
        }

        Ok(())
    }

    /// Computes the size of the serialized bitmap in bytes.
    pub fn serialized_size(&self, bitmap_type: BitmapType) -> usize {
        match (self, bitmap_type) {
            (Bitmap::BitVec(bitvec), BitmapType::BitVec) => bitvec.as_raw_slice().len(),
            (Bitmap::Roaring(roaring), BitmapType::Roaring) => roaring.serialized_size(),
            (Bitmap::BitVec(bitvec), BitmapType::Roaring) => {
                let bitmap = Bitmap::bitvec_to_roaring(bitvec.clone());
                bitmap.serialized_size()
            }
            (Bitmap::Roaring(roaring), BitmapType::BitVec) => {
                let bitvec = Bitmap::roaring_to_bitvec(roaring);
                bitvec.as_raw_slice().len()
            }
        }
    }

    /// Deserializes a bitmap from a byte buffer.
    ///
    /// # Arguments
    /// * `buf` - Input buffer containing serialized data
    /// * `bitmap_type` - Format of the serialized data
    pub fn deserialize_from(buf: &[u8], bitmap_type: BitmapType) -> std::io::Result<Self> {
        match bitmap_type {
            BitmapType::BitVec => {
                let bitvec = BitVec::from_slice(buf);
                Ok(Bitmap::BitVec(bitvec))
            }
            BitmapType::Roaring => {
                let roaring = RoaringBitmap::deserialize_from(buf)?;
                Ok(Bitmap::Roaring(roaring))
            }
        }
    }

    /// Computes the union with another bitmap (in-place).
    ///
    /// If the other bitmap is a different type, it will be converted to match
    /// the current bitmap's type.
    pub fn union(&mut self, other: Self) {
        if self.is_empty() {
            *self = other;
            return;
        }

        match (self, other) {
            (Bitmap::BitVec(bitvec1), Bitmap::BitVec(bitvec2)) => {
                if bitvec1.len() > bitvec2.len() {
                    *bitvec1 |= bitvec2
                } else {
                    *bitvec1 = bitvec2 | &*bitvec1;
                }
            }
            (Bitmap::Roaring(roaring1), Bitmap::Roaring(roaring2)) => {
                *roaring1 |= roaring2;
            }
            (Bitmap::BitVec(bitvec1), Bitmap::Roaring(roaring)) => {
                let bitvec2 = Self::roaring_to_bitvec(&roaring);
                if bitvec1.len() > bitvec2.len() {
                    *bitvec1 |= bitvec2
                } else {
                    *bitvec1 = bitvec2 | &*bitvec1;
                }
            }
            (Bitmap::Roaring(roaring1), Bitmap::BitVec(bitvec)) => {
                let roaring2 = Self::bitvec_to_roaring(bitvec);
                *roaring1 |= roaring2;
            }
        }
    }

    /// Computes the intersection with another bitmap (in-place).
    ///
    /// If the other bitmap is a different type, it will be converted to match
    /// the current bitmap's type.
    pub fn intersect(&mut self, other: Self) {
        match (self, other) {
            (Bitmap::BitVec(bitvec1), Bitmap::BitVec(mut bitvec2)) => {
                let len = (bitvec1.len() - bitvec1.trailing_zeros())
                    .min(bitvec2.len() - bitvec2.trailing_zeros());
                bitvec1.truncate(len);
                bitvec2.truncate(len);
                *bitvec1 &= bitvec2;
            }
            (Bitmap::Roaring(roaring1), Bitmap::Roaring(roaring2)) => {
                *roaring1 &= roaring2;
            }
            (Bitmap::BitVec(bitvec1), Bitmap::Roaring(roaring)) => {
                let mut bitvec2 = Self::roaring_to_bitvec(&roaring);
                let len = (bitvec1.len() - bitvec1.trailing_zeros())
                    .min(bitvec2.len() - bitvec2.trailing_zeros());
                bitvec1.truncate(len);
                bitvec2.truncate(len);
                *bitvec1 &= bitvec2;
            }
            (Bitmap::Roaring(roaring), Bitmap::BitVec(bitvec)) => {
                let roaring2 = Self::bitvec_to_roaring(bitvec);
                *roaring &= roaring2;
            }
        }
    }

    /// Returns an iterator over the indices of set bits.
    pub fn iter_ones(&self) -> Box<dyn Iterator<Item = usize> + '_> {
        match self {
            Bitmap::BitVec(bitvec) => Box::new(bitvec.iter_ones()),
            Bitmap::Roaring(roaring) => Box::new(roaring.iter().map(|x| x as usize)),
        }
    }

    /// Creates a bitmap from bytes in LSB0 (least significant bit first) order.
    ///
    /// # Arguments
    /// * `bytes` - Input bytes in LSB0 order
    /// * `bitmap_type` - Type of bitmap to create
    pub fn from_lsb0_bytes(bytes: &[u8], bitmap_type: BitmapType) -> Self {
        match bitmap_type {
            BitmapType::BitVec => {
                let bitvec = BitVec::from_slice(bytes);
                Bitmap::BitVec(bitvec)
            }
            BitmapType::Roaring => {
                let roaring = RoaringBitmap::from_lsb0_bytes(0, bytes);
                Bitmap::Roaring(roaring)
            }
        }
    }

    /// Computes memory usage of the bitmap in bytes.
    pub fn memory_usage(&self) -> usize {
        match self {
            Bitmap::BitVec(bitvec) => bitvec.capacity(),
            Bitmap::Roaring(roaring) => {
                let stat = roaring.statistics();
                (stat.n_bytes_array_containers
                    + stat.n_bytes_bitset_containers
                    + stat.n_bytes_run_containers) as usize
            }
        }
    }

    fn roaring_to_bitvec(roaring: &RoaringBitmap) -> BitVec {
        let max_value = roaring.max().unwrap_or(0);
        let mut bitvec = BitVec::repeat(false, max_value as usize + 1);
        for i in roaring {
            bitvec.set(i as usize, true);
        }
        bitvec
    }

    fn bitvec_to_roaring(mut bitvec: BitVec) -> RoaringBitmap {
        bitvec.resize(bitvec.capacity(), false);
        RoaringBitmap::from_lsb0_bytes(0, bitvec.as_raw_slice())
    }
}

impl Default for Bitmap {
    fn default() -> Self {
        Bitmap::new_roaring()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_full_bitmaps() {
        let bv = Bitmap::full_bitvec(10);
        assert_eq!(bv.count_ones(), 10);

        let rb = Bitmap::full_roaring(10);
        assert_eq!(rb.count_ones(), 10);
    }

    #[test]
    fn test_serialization_roundtrip() {
        let original = Bitmap::full_roaring(100);
        let mut buf = Vec::new();

        // Serialize as Roaring
        original
            .serialize_into(BitmapType::Roaring, &mut buf)
            .unwrap();
        let deserialized = Bitmap::deserialize_from(&buf, BitmapType::Roaring).unwrap();
        assert_eq!(original, deserialized);

        // Serialize as BitVec
        buf.clear();
        original
            .serialize_into(BitmapType::BitVec, &mut buf)
            .unwrap();
        let deserialized = Bitmap::deserialize_from(&buf, BitmapType::BitVec).unwrap();
        assert_eq!(original.count_ones(), deserialized.count_ones());
    }

    #[test]
    fn test_union_fulls() {
        // Test BitVec union
        let mut bv1 = Bitmap::full_bitvec(3); // 0-2: 111
        let bv2 = Bitmap::full_bitvec(5); // 0-4: 11111
        bv1.union(bv2);
        assert_eq!(bv1.count_ones(), 5);

        let mut bv1 = Bitmap::full_bitvec(5); // 0-4: 11111
        let bv2 = Bitmap::full_bitvec(3); // 0-2: 111
        bv1.union(bv2);
        assert_eq!(bv1.count_ones(), 5);

        // Test Roaring union
        let mut rb1 = Bitmap::full_roaring(3); // 0-2: 111
        let rb2 = Bitmap::full_roaring(5); // 0-4: 11111
        rb1.union(rb2);
        assert_eq!(rb1.count_ones(), 5);

        let mut rb1 = Bitmap::full_roaring(5); // 0-4: 11111
        let rb2 = Bitmap::full_roaring(3); // 0-2: 111
        rb1.union(rb2);
        assert_eq!(rb1.count_ones(), 5);

        // Test cross-type union
        let mut rb = Bitmap::full_roaring(5); // 0-4: 11111
        let bv = Bitmap::full_bitvec(3); // 0-2: 111
        rb.union(bv);
        assert_eq!(rb.count_ones(), 5);

        let mut bv = Bitmap::full_bitvec(5); // 0-4: 11111
        let rb = Bitmap::full_roaring(3); // 0-2: 111
        bv.union(rb);
        assert_eq!(bv.count_ones(), 5);

        let mut rb = Bitmap::full_roaring(3); // 0-2: 111
        let bv = Bitmap::full_bitvec(5); // 0-4: 11111
        rb.union(bv);
        assert_eq!(rb.count_ones(), 5);

        let mut bv = Bitmap::full_bitvec(3); // 0-2: 111
        let rb = Bitmap::full_roaring(5); // 0-4: 11111
        bv.union(rb);
        assert_eq!(bv.count_ones(), 5);
    }

    #[test]
    fn test_union_bitvec() {
        let mut bv1 = Bitmap::from_lsb0_bytes(&[0b10101010], BitmapType::BitVec);
        let bv2 = Bitmap::from_lsb0_bytes(&[0b01010101], BitmapType::BitVec);
        bv1.union(bv2);
        assert_eq!(
            bv1,
            Bitmap::from_lsb0_bytes(&[0b11111111], BitmapType::BitVec)
        );

        // Test different lengths
        let mut bv1 = Bitmap::from_lsb0_bytes(&[0b10101010], BitmapType::BitVec);
        let bv2 = Bitmap::from_lsb0_bytes(&[0b01010101, 0b00000001], BitmapType::BitVec);
        bv1.union(bv2);
        assert_eq!(
            bv1,
            Bitmap::from_lsb0_bytes(&[0b11111111, 0b00000001], BitmapType::BitVec)
        );

        let mut bv1 = Bitmap::from_lsb0_bytes(&[0b10101010, 0b00000001], BitmapType::BitVec);
        let bv2 = Bitmap::from_lsb0_bytes(&[0b01010101], BitmapType::BitVec);
        bv1.union(bv2);
        assert_eq!(
            bv1,
            Bitmap::from_lsb0_bytes(&[0b11111111, 0b00000001], BitmapType::BitVec)
        );

        // Test empty bitmaps
        let mut bv1 = Bitmap::new_bitvec();
        let bv2 = Bitmap::new_bitvec();
        bv1.union(bv2);
        assert!(bv1.is_empty());

        let mut bv1 = Bitmap::new_bitvec();
        let bv2 = Bitmap::from_lsb0_bytes(&[0b01010101], BitmapType::BitVec);
        bv1.union(bv2);
        assert_eq!(
            bv1,
            Bitmap::from_lsb0_bytes(&[0b01010101], BitmapType::BitVec)
        );

        let mut bv1 = Bitmap::from_lsb0_bytes(&[0b01010101], BitmapType::BitVec);
        let bv2 = Bitmap::new_bitvec();
        bv1.union(bv2);
        assert_eq!(
            bv1,
            Bitmap::from_lsb0_bytes(&[0b01010101], BitmapType::BitVec)
        );

        // Test empty and full bitmaps
        let mut bv1 = Bitmap::new_bitvec();
        let bv2 = Bitmap::full_bitvec(8);
        bv1.union(bv2);
        assert_eq!(bv1, Bitmap::full_bitvec(8));

        let mut bv1 = Bitmap::full_bitvec(8);
        let bv2 = Bitmap::new_bitvec();
        bv1.union(bv2);
        assert_eq!(bv1, Bitmap::full_bitvec(8));
    }

    #[test]
    fn test_union_roaring() {
        let mut rb1 = Bitmap::from_lsb0_bytes(&[0b10101010], BitmapType::Roaring);
        let rb2 = Bitmap::from_lsb0_bytes(&[0b01010101], BitmapType::Roaring);
        rb1.union(rb2);
        assert_eq!(
            rb1,
            Bitmap::from_lsb0_bytes(&[0b11111111], BitmapType::Roaring)
        );

        // Test different lengths
        let mut rb1 = Bitmap::from_lsb0_bytes(&[0b10101010], BitmapType::Roaring);
        let rb2 = Bitmap::from_lsb0_bytes(&[0b01010101, 0b00000001], BitmapType::Roaring);
        rb1.union(rb2);
        assert_eq!(
            rb1,
            Bitmap::from_lsb0_bytes(&[0b11111111, 0b00000001], BitmapType::Roaring)
        );

        let mut rb1 = Bitmap::from_lsb0_bytes(&[0b10101010, 0b00000001], BitmapType::Roaring);
        let rb2 = Bitmap::from_lsb0_bytes(&[0b01010101], BitmapType::Roaring);
        rb1.union(rb2);
        assert_eq!(
            rb1,
            Bitmap::from_lsb0_bytes(&[0b11111111, 0b00000001], BitmapType::Roaring)
        );

        // Test empty bitmaps
        let mut rb1 = Bitmap::new_roaring();
        let rb2 = Bitmap::new_roaring();
        rb1.union(rb2);
        assert!(rb1.is_empty());

        let mut rb1 = Bitmap::new_roaring();
        let rb2 = Bitmap::from_lsb0_bytes(&[0b01010101], BitmapType::Roaring);
        rb1.union(rb2);
        assert_eq!(
            rb1,
            Bitmap::from_lsb0_bytes(&[0b01010101], BitmapType::Roaring)
        );

        let mut rb1 = Bitmap::from_lsb0_bytes(&[0b01010101], BitmapType::Roaring);
        let rb2 = Bitmap::new_roaring();
        rb1.union(rb2);
        assert_eq!(
            rb1,
            Bitmap::from_lsb0_bytes(&[0b01010101], BitmapType::Roaring)
        );

        // Test empty and full bit
        let mut rb1 = Bitmap::new_roaring();
        let rb2 = Bitmap::full_roaring(8);
        rb1.union(rb2);
        assert_eq!(rb1, Bitmap::full_roaring(8));

        let mut rb1 = Bitmap::full_roaring(8);
        let rb2 = Bitmap::new_roaring();
        rb1.union(rb2);
        assert_eq!(rb1, Bitmap::full_roaring(8));
    }

    #[test]
    fn test_union_mixed() {
        let mut rb = Bitmap::from_lsb0_bytes(&[0b10101010], BitmapType::Roaring);
        let bv = Bitmap::from_lsb0_bytes(&[0b01010101], BitmapType::BitVec);
        rb.union(bv);
        assert_eq!(
            rb,
            Bitmap::from_lsb0_bytes(&[0b11111111], BitmapType::Roaring)
        );

        let mut bv = Bitmap::from_lsb0_bytes(&[0b10101010], BitmapType::BitVec);
        let rb = Bitmap::from_lsb0_bytes(&[0b01010101], BitmapType::Roaring);
        bv.union(rb);
        assert_eq!(
            bv,
            Bitmap::from_lsb0_bytes(&[0b11111111], BitmapType::BitVec)
        );

        let mut rb = Bitmap::from_lsb0_bytes(&[0b10101010], BitmapType::Roaring);
        let bv = Bitmap::full_bitvec(8);
        rb.union(bv);
        assert_eq!(rb, Bitmap::full_roaring(8));

        let mut bv = Bitmap::full_bitvec(8);
        let rb = Bitmap::from_lsb0_bytes(&[0b10101010], BitmapType::Roaring);
        bv.union(rb);
        assert_eq!(bv, Bitmap::full_bitvec(8));

        let mut rb = Bitmap::new_roaring();
        let bv = Bitmap::full_bitvec(8);
        rb.union(bv);
        assert_eq!(rb, Bitmap::full_bitvec(8));

        let mut bv = Bitmap::full_bitvec(8);
        let rb = Bitmap::new_roaring();
        bv.union(rb);
        assert_eq!(bv, Bitmap::full_bitvec(8));

        let mut rb = Bitmap::new_roaring();
        let bv = Bitmap::new_bitvec();
        rb.union(bv);
        assert!(rb.is_empty());

        let mut bv = Bitmap::new_bitvec();
        let rb = Bitmap::new_roaring();
        bv.union(rb);
        assert!(bv.is_empty());

        let mut rb = Bitmap::new_roaring();
        let bv = Bitmap::from_lsb0_bytes(&[0b01010101], BitmapType::BitVec);
        rb.union(bv);
        assert_eq!(
            rb,
            Bitmap::from_lsb0_bytes(&[0b01010101], BitmapType::BitVec)
        );

        let mut bv = Bitmap::from_lsb0_bytes(&[0b01010101], BitmapType::BitVec);
        let rb = Bitmap::new_roaring();
        bv.union(rb);
        assert_eq!(
            bv,
            Bitmap::from_lsb0_bytes(&[0b01010101], BitmapType::BitVec)
        );

        let mut rb = Bitmap::from_lsb0_bytes(&[0b01010101], BitmapType::Roaring);
        let bv = Bitmap::new_bitvec();
        rb.union(bv);
        assert_eq!(
            rb,
            Bitmap::from_lsb0_bytes(&[0b01010101], BitmapType::Roaring)
        );

        let mut bv = Bitmap::new_bitvec();
        let rb = Bitmap::from_lsb0_bytes(&[0b01010101], BitmapType::Roaring);
        bv.union(rb);
        assert_eq!(
            bv,
            Bitmap::from_lsb0_bytes(&[0b01010101], BitmapType::Roaring)
        );
    }

    #[test]
    fn test_intersect_fulls() {
        // Test BitVec intersect
        let mut bv1 = Bitmap::full_bitvec(3); // 0-2: 111
        let bv2 = Bitmap::full_bitvec(5); // 0-4: 11111
        bv1.intersect(bv2);
        assert_eq!(bv1.count_ones(), 3);

        let mut bv1 = Bitmap::full_bitvec(5); // 0-4: 11111
        let bv2 = Bitmap::full_bitvec(3); // 0-2: 111
        bv1.intersect(bv2);
        assert_eq!(bv1.count_ones(), 3);

        // Test Roaring intersect
        let mut rb1 = Bitmap::full_roaring(3); // 0-2: 111
        let rb2 = Bitmap::full_roaring(5); // 0-4: 11111
        rb1.intersect(rb2);
        assert_eq!(rb1.count_ones(), 3);

        let mut rb1 = Bitmap::full_roaring(5); // 0-4: 11111
        let rb2 = Bitmap::full_roaring(3); // 0-2: 111
        rb1.intersect(rb2);
        assert_eq!(rb1.count_ones(), 3);

        // Test cross-type intersect
        let mut rb = Bitmap::full_roaring(5); // 0-4: 11111
        let bv = Bitmap::full_bitvec(3); // 0-2: 111
        rb.intersect(bv);
        assert_eq!(rb.count_ones(), 3);

        let mut bv = Bitmap::full_bitvec(5); // 0-4: 11111
        let rb = Bitmap::full_roaring(3); // 0-2: 111
        bv.intersect(rb);
        assert_eq!(bv.count_ones(), 3);

        let mut rb = Bitmap::full_roaring(3); // 0-2: 111
        let bv = Bitmap::full_bitvec(5); // 0-4: 11111
        rb.intersect(bv);
        assert_eq!(rb.count_ones(), 3);

        let mut bv = Bitmap::full_bitvec(3); // 0-2: 111
        let rb = Bitmap::full_roaring(5); // 0-4: 11111
        bv.intersect(rb);
        assert_eq!(bv.count_ones(), 3);
    }

    #[test]
    fn test_intersect_bitvec() {
        let mut bv1 = Bitmap::from_lsb0_bytes(&[0b11110000], BitmapType::BitVec);
        let bv2 = Bitmap::from_lsb0_bytes(&[0b10101010], BitmapType::BitVec);
        bv1.intersect(bv2);
        assert_eq!(
            bv1,
            Bitmap::from_lsb0_bytes(&[0b10100000], BitmapType::BitVec)
        );

        // Test different lengths
        let mut bv1 = Bitmap::from_lsb0_bytes(&[0b11110000], BitmapType::BitVec);
        let bv2 = Bitmap::from_lsb0_bytes(&[0b10101010, 0b00000001], BitmapType::BitVec);
        bv1.intersect(bv2);
        assert_eq!(
            bv1,
            Bitmap::from_lsb0_bytes(&[0b10100000], BitmapType::BitVec)
        );

        let mut bv1 = Bitmap::from_lsb0_bytes(&[0b11110000, 0b00000001], BitmapType::BitVec);
        let bv2 = Bitmap::from_lsb0_bytes(&[0b10101010], BitmapType::BitVec);
        bv1.intersect(bv2);
        assert_eq!(
            bv1,
            Bitmap::from_lsb0_bytes(&[0b10100000], BitmapType::BitVec)
        );

        // Test empty bitmaps
        let mut bv1 = Bitmap::new_bitvec();
        let bv2 = Bitmap::new_bitvec();
        bv1.intersect(bv2);
        assert!(bv1.is_empty());

        let mut bv1 = Bitmap::new_bitvec();
        let bv2 = Bitmap::from_lsb0_bytes(&[0b10101010], BitmapType::BitVec);
        bv1.intersect(bv2);
        assert!(bv1.is_empty());

        let mut bv1 = Bitmap::from_lsb0_bytes(&[0b10101010], BitmapType::BitVec);
        let bv2 = Bitmap::new_bitvec();
        bv1.intersect(bv2);
        assert!(bv1.is_empty());

        // Test empty and full bitmaps
        let mut bv1 = Bitmap::new_bitvec();
        let bv2 = Bitmap::full_bitvec(8);
        bv1.intersect(bv2);
        assert!(bv1.is_empty());

        let mut bv1 = Bitmap::full_bitvec(8);
        let bv2 = Bitmap::new_bitvec();
        bv1.intersect(bv2);
        assert!(bv1.is_empty());
    }

    #[test]
    fn test_intersect_roaring() {
        let mut rb1 = Bitmap::from_lsb0_bytes(&[0b11110000], BitmapType::Roaring);
        let rb2 = Bitmap::from_lsb0_bytes(&[0b10101010], BitmapType::Roaring);
        rb1.intersect(rb2);
        assert_eq!(
            rb1,
            Bitmap::from_lsb0_bytes(&[0b10100000], BitmapType::Roaring)
        );

        // Test different lengths
        let mut rb1 = Bitmap::from_lsb0_bytes(&[0b11110000], BitmapType::Roaring);
        let rb2 = Bitmap::from_lsb0_bytes(&[0b10101010, 0b00000001], BitmapType::Roaring);
        rb1.intersect(rb2);
        assert_eq!(
            rb1,
            Bitmap::from_lsb0_bytes(&[0b10100000], BitmapType::Roaring)
        );

        let mut rb1 = Bitmap::from_lsb0_bytes(&[0b11110000, 0b00000001], BitmapType::Roaring);
        let rb2 = Bitmap::from_lsb0_bytes(&[0b10101010], BitmapType::Roaring);
        rb1.intersect(rb2);
        assert_eq!(
            rb1,
            Bitmap::from_lsb0_bytes(&[0b10100000], BitmapType::Roaring)
        );

        // Test empty bitmaps
        let mut rb1 = Bitmap::new_roaring();
        let rb2 = Bitmap::new_roaring();
        rb1.intersect(rb2);
        assert!(rb1.is_empty());

        let mut rb1 = Bitmap::new_roaring();
        let rb2 = Bitmap::from_lsb0_bytes(&[0b10101010], BitmapType::Roaring);
        rb1.intersect(rb2);
        assert!(rb1.is_empty());

        let mut rb1 = Bitmap::from_lsb0_bytes(&[0b10101010], BitmapType::Roaring);
        let rb2 = Bitmap::new_roaring();
        rb1.intersect(rb2);
        assert!(rb1.is_empty());

        // Test empty and full bitmaps
        let mut rb1 = Bitmap::new_roaring();
        let rb2 = Bitmap::full_roaring(8);
        rb1.intersect(rb2);
        assert!(rb1.is_empty());

        let mut rb1 = Bitmap::full_roaring(8);
        let rb2 = Bitmap::new_roaring();
        rb1.intersect(rb2);
        assert!(rb1.is_empty());
    }

    #[test]
    fn test_intersect_mixed() {
        let mut rb = Bitmap::from_lsb0_bytes(&[0b11110000], BitmapType::Roaring);
        let bv = Bitmap::from_lsb0_bytes(&[0b10101010], BitmapType::BitVec);
        rb.intersect(bv);
        assert_eq!(
            rb,
            Bitmap::from_lsb0_bytes(&[0b10100000], BitmapType::Roaring)
        );

        let mut bv = Bitmap::from_lsb0_bytes(&[0b11110000], BitmapType::BitVec);
        let rb = Bitmap::from_lsb0_bytes(&[0b10101010], BitmapType::Roaring);
        bv.intersect(rb);
        assert_eq!(
            bv,
            Bitmap::from_lsb0_bytes(&[0b10100000], BitmapType::BitVec)
        );

        let mut rb = Bitmap::from_lsb0_bytes(&[0b11110000], BitmapType::Roaring);
        let bv = Bitmap::full_bitvec(8);
        rb.intersect(bv);
        assert_eq!(
            rb,
            Bitmap::from_lsb0_bytes(&[0b11110000], BitmapType::Roaring)
        );

        let mut bv = Bitmap::full_bitvec(8);
        let rb = Bitmap::from_lsb0_bytes(&[0b11110000], BitmapType::Roaring);
        bv.intersect(rb);
        assert_eq!(
            bv,
            Bitmap::from_lsb0_bytes(&[0b11110000], BitmapType::BitVec)
        );

        let mut rb = Bitmap::from_lsb0_bytes(&[0b11110000], BitmapType::Roaring);
        let bv = Bitmap::from_lsb0_bytes(&[0b10101010, 0b00000001], BitmapType::BitVec);
        rb.intersect(bv);
        assert_eq!(
            rb,
            Bitmap::from_lsb0_bytes(&[0b10100000], BitmapType::Roaring)
        );

        let mut bv = Bitmap::from_lsb0_bytes(&[0b11110000, 0b00000001], BitmapType::BitVec);
        let rb = Bitmap::from_lsb0_bytes(&[0b10101010], BitmapType::Roaring);
        bv.intersect(rb);
        assert_eq!(
            bv,
            Bitmap::from_lsb0_bytes(&[0b10100000], BitmapType::BitVec)
        );

        let mut rb = Bitmap::from_lsb0_bytes(&[0b11110000, 0b00000001], BitmapType::Roaring);
        let bv = Bitmap::from_lsb0_bytes(&[0b10101010], BitmapType::BitVec);
        rb.intersect(bv);
        assert_eq!(
            rb,
            Bitmap::from_lsb0_bytes(&[0b10100000], BitmapType::Roaring)
        );

        let mut bv = Bitmap::from_lsb0_bytes(&[0b11110000], BitmapType::BitVec);
        let rb = Bitmap::from_lsb0_bytes(&[0b10101010, 0b00000001], BitmapType::Roaring);
        bv.intersect(rb);
        assert_eq!(
            bv,
            Bitmap::from_lsb0_bytes(&[0b10100000], BitmapType::BitVec)
        );

        let mut rb = Bitmap::new_roaring();
        let bv = Bitmap::full_bitvec(8);
        rb.intersect(bv);
        assert!(rb.is_empty());

        let mut bv = Bitmap::full_bitvec(8);
        let rb = Bitmap::new_roaring();
        bv.intersect(rb);
        assert!(bv.is_empty());

        let mut bv = Bitmap::new_bitvec();
        let rb = Bitmap::full_roaring(8);
        bv.intersect(rb);
        assert!(bv.is_empty());

        let mut rb = Bitmap::full_roaring(8);
        let bv = Bitmap::new_bitvec();
        rb.intersect(bv);
        assert!(rb.is_empty());

        let mut rb = Bitmap::new_roaring();
        let bv = Bitmap::from_lsb0_bytes(&[0b10101010], BitmapType::BitVec);
        rb.intersect(bv);
        assert!(rb.is_empty());

        let mut bv = Bitmap::from_lsb0_bytes(&[0b10101010], BitmapType::BitVec);
        let rb = Bitmap::new_roaring();
        bv.intersect(rb);
        assert!(bv.is_empty());

        let mut bv = Bitmap::new_bitvec();
        let rb = Bitmap::from_lsb0_bytes(&[0b10101010], BitmapType::Roaring);
        bv.intersect(rb);
        assert!(bv.is_empty());

        let mut rb = Bitmap::from_lsb0_bytes(&[0b10101010], BitmapType::Roaring);
        let bv = Bitmap::new_bitvec();
        rb.intersect(bv);
        assert!(rb.is_empty());
    }

    #[test]
    fn test_insert_range() {
        let mut bv = Bitmap::new_bitvec();
        bv.insert_range(0..=5);
        assert_eq!(bv.iter_ones().collect::<Vec<_>>(), vec![0, 1, 2, 3, 4, 5]);

        let mut rb = Bitmap::new_roaring();
        rb.insert_range(0..=5);
        assert_eq!(bv.iter_ones().collect::<Vec<_>>(), vec![0, 1, 2, 3, 4, 5]);

        let mut bv = Bitmap::new_bitvec();
        bv.insert_range(10..=10);
        assert_eq!(bv.iter_ones().collect::<Vec<_>>(), vec![10]);

        let mut rb = Bitmap::new_roaring();
        rb.insert_range(10..=10);
        assert_eq!(bv.iter_ones().collect::<Vec<_>>(), vec![10]);
    }
}
