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

/// `ParallelFstValuesMapper` enables parallel mapping of multiple FST value groups to their
/// corresponding bitmaps within an inverted index.
///
/// This mapper processes multiple groups of FST values in parallel, where each group is associated
/// with its own metadata. It optimizes bitmap retrieval by batching requests across all groups
/// before combining them into separate result bitmaps.
pub struct ParallelFstValuesMapper<'a> {
    reader: &'a mut dyn InvertedIndexReader,
}

impl<'a> ParallelFstValuesMapper<'a> {
    pub fn new(reader: &'a mut dyn InvertedIndexReader) -> Self {
        Self { reader }
    }

    pub async fn map_values_vec(
        &mut self,
        value_and_meta_vec: &[(Vec<u64>, &'a InvertedIndexMeta)],
    ) -> Result<Vec<BitVec>> {
        let groups = value_and_meta_vec
            .iter()
            .map(|(values, _)| values.len())
            .collect::<Vec<_>>();
        let len = groups.iter().sum::<usize>();
        let mut fetch_ranges = Vec::with_capacity(len);

        for (values, meta) in value_and_meta_vec {
            for value in values {
                // The higher 32 bits of each u64 value represent the
                // bitmap offset and the lower 32 bits represent its size. This mapper uses these
                // combined offset-size pairs to fetch and union multiple bitmaps into a single `BitVec`.
                let [relative_offset, size] = bytemuck::cast::<u64, [u32; 2]>(*value);
                fetch_ranges.push(
                    meta.base_offset + relative_offset as u64
                        ..meta.base_offset + relative_offset as u64 + size as u64,
                );
            }
        }

        if fetch_ranges.is_empty() {
            return Ok(vec![BitVec::new()]);
        }

        common_telemetry::debug!("fetch ranges: {:?}", fetch_ranges);
        let mut bitmaps = self.reader.bitmap_vec(&fetch_ranges).await?;
        let mut output = Vec::with_capacity(groups.len());

        for counter in groups {
            let mut bitmap = BitVec::new();
            for _ in 0..counter {
                let bm = bitmaps.pop_front().unwrap();
                if bm.len() > bitmap.len() {
                    bitmap = bm | bitmap
                } else {
                    bitmap |= bm
                }
            }

            output.push(bitmap);
        }

        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use common_base::bit_vec::prelude::*;

    use super::*;
    use crate::inverted_index::format::reader::MockInvertedIndexReader;

    fn value(offset: u32, size: u32) -> u64 {
        bytemuck::cast::<[u32; 2], u64>([offset, size])
    }

    #[tokio::test]
    async fn test_map_values_vec() {
        let mut mock_reader = MockInvertedIndexReader::new();
        mock_reader.expect_bitmap_vec().returning(|ranges| {
            let mut output = VecDeque::new();
            for range in ranges {
                let offset = range.start;
                let size = range.end - range.start;
                match (offset, size) {
                    (1, 1) => output.push_back(bitvec![u8, Lsb0; 1, 0, 1, 0, 1, 0, 1]),
                    (2, 1) => output.push_back(bitvec![u8, Lsb0; 0, 1, 0, 1, 0, 1, 0, 1]),
                    _ => unreachable!(),
                }
            }
            Ok(output)
        });

        let meta = InvertedIndexMeta::default();
        let mut values_mapper = ParallelFstValuesMapper::new(&mut mock_reader);

        let result = values_mapper
            .map_values_vec(&[(vec![], &meta)])
            .await
            .unwrap();
        assert_eq!(result[0].count_ones(), 0);

        let result = values_mapper
            .map_values_vec(&[(vec![value(1, 1)], &meta)])
            .await
            .unwrap();
        assert_eq!(result[0], bitvec![u8, Lsb0; 1, 0, 1, 0, 1, 0, 1]);

        let result = values_mapper
            .map_values_vec(&[(vec![value(2, 1)], &meta)])
            .await
            .unwrap();
        assert_eq!(result[0], bitvec![u8, Lsb0; 0, 1, 0, 1, 0, 1, 0, 1]);

        let result = values_mapper
            .map_values_vec(&[(vec![value(1, 1), value(2, 1)], &meta)])
            .await
            .unwrap();
        assert_eq!(result[0], bitvec![u8, Lsb0; 1, 1, 1, 1, 1, 1, 1, 1]);

        let result = values_mapper
            .map_values_vec(&[(vec![value(2, 1), value(1, 1)], &meta)])
            .await
            .unwrap();
        assert_eq!(result[0], bitvec![u8, Lsb0; 1, 1, 1, 1, 1, 1, 1, 1]);
    }
}
