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

use common_base::BitVec;
use greptime_proto::v1::index::InvertedIndexMeta;

use crate::inverted_index::error::Result;
use crate::inverted_index::format::reader::InvertedIndexReader;

/// `FstValuesMapper` maps FST-encoded u64 values to their corresponding bitmaps
/// within an inverted index. The higher 32 bits of each u64 value represent the
/// bitmap offset and the lower 32 bits represent its size. This mapper uses these
/// combined offset-size pairs to fetch and union multiple bitmaps into a single `BitVec`.
pub struct FstValuesMapper<'a> {
    /// `reader` retrieves bitmap data using offsets and sizes from FST values.
    reader: &'a mut dyn InvertedIndexReader,

    /// `metadata` provides context for interpreting the index structures.
    metadata: &'a InvertedIndexMeta,
}

impl<'a> FstValuesMapper<'a> {
    pub fn new(
        reader: &'a mut dyn InvertedIndexReader,
        metadata: &'a InvertedIndexMeta,
    ) -> FstValuesMapper<'a> {
        FstValuesMapper { reader, metadata }
    }

    /// Maps an array of FST values to a `BitVec` by retrieving and combining bitmaps.
    pub async fn map_values(&mut self, values: &[u64]) -> Result<BitVec> {
        let mut bitmap = BitVec::new();

        for value in values {
            // relative_offset (higher 32 bits), size (lower 32 bits)
            let [relative_offset, size] = bytemuck::cast::<u64, [u32; 2]>(*value);

            let bm = self
                .reader
                .bitmap(self.metadata, relative_offset, size)
                .await?;

            // Ensure the longest BitVec is the left operand to prevent truncation during OR.
            if bm.len() > bitmap.len() {
                bitmap = bm | bitmap
            } else {
                bitmap |= bm
            }
        }

        Ok(bitmap)
    }
}

#[cfg(test)]
mod tests {
    use common_base::bit_vec::prelude::*;

    use super::*;
    use crate::inverted_index::format::reader::MockInvertedIndexReader;

    fn value(offset: u32, size: u32) -> u64 {
        bytemuck::cast::<[u32; 2], u64>([offset, size])
    }

    #[tokio::test]
    async fn test_map_values() {
        let mut mock_reader = MockInvertedIndexReader::new();
        mock_reader
            .expect_bitmap()
            .returning(|_, offset, size| match (offset, size) {
                (1, 1) => Ok(bitvec![u8, Lsb0; 1, 0, 1, 0, 1, 0, 1]),
                (2, 1) => Ok(bitvec![u8, Lsb0; 0, 1, 0, 1, 0, 1, 0, 1]),
                _ => unreachable!(),
            });

        let meta = InvertedIndexMeta::default();
        let mut values_mapper = FstValuesMapper::new(&mut mock_reader, &meta);

        let result = values_mapper.map_values(&[]).await.unwrap();
        assert_eq!(result.count_ones(), 0);

        let result = values_mapper.map_values(&[value(1, 1)]).await.unwrap();
        assert_eq!(result, bitvec![u8, Lsb0; 1, 0, 1, 0, 1, 0, 1]);

        let result = values_mapper.map_values(&[value(2, 1)]).await.unwrap();
        assert_eq!(result, bitvec![u8, Lsb0; 0, 1, 0, 1, 0, 1, 0, 1]);

        let result = values_mapper
            .map_values(&[value(1, 1), value(2, 1)])
            .await
            .unwrap();
        assert_eq!(result, bitvec![u8, Lsb0; 1, 1, 1, 1, 1, 1, 1, 1]);

        let result = values_mapper
            .map_values(&[value(2, 1), value(1, 1)])
            .await
            .unwrap();
        assert_eq!(result, bitvec![u8, Lsb0; 1, 1, 1, 1, 1, 1, 1, 1]);
    }
}
