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

use greptime_proto::v1::index::{BitmapType, InvertedIndexMeta};

use crate::bitmap::Bitmap;
use crate::inverted_index::error::Result;
use crate::inverted_index::format::reader::{InvertedIndexReadMetrics, InvertedIndexReader};

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
        value_and_meta_vec: &[(Vec<u64>, &InvertedIndexMeta)],
        metrics: Option<&mut InvertedIndexReadMetrics>,
    ) -> Result<Vec<Bitmap>> {
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
                let range = meta.base_offset + relative_offset as u64
                    ..meta.base_offset + relative_offset as u64 + size as u64;
                fetch_ranges.push((
                    range,
                    BitmapType::try_from(meta.bitmap_type).unwrap_or(BitmapType::BitVec),
                ));
            }
        }

        if fetch_ranges.is_empty() {
            return Ok(vec![Bitmap::new_bitvec()]);
        }

        common_telemetry::debug!("fetch ranges: {:?}", fetch_ranges);
        let mut bitmaps = self.reader.bitmap_deque(&fetch_ranges, metrics).await?;
        let mut output = Vec::with_capacity(groups.len());

        for counter in groups {
            let mut bitmap = Bitmap::new_roaring();
            for _ in 0..counter {
                let bm = bitmaps.pop_front().unwrap();
                bitmap.union(bm);
            }

            output.push(bitmap);
        }

        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use super::*;
    use crate::inverted_index::format::reader::MockInvertedIndexReader;

    fn value(offset: u32, size: u32) -> u64 {
        bytemuck::cast::<[u32; 2], u64>([offset, size])
    }

    #[tokio::test]
    async fn test_map_values_vec() {
        let mut mock_reader = MockInvertedIndexReader::new();
        mock_reader
            .expect_bitmap_deque()
            .returning(|ranges, _metrics| {
                let mut output = VecDeque::new();
                for (range, bitmap_type) in ranges {
                    let offset = range.start;
                    let size = range.end - range.start;
                    match (offset, size, bitmap_type) {
                        (1, 1, BitmapType::Roaring) => {
                            output.push_back(Bitmap::from_lsb0_bytes(&[0b10101010], *bitmap_type))
                        }
                        (2, 1, BitmapType::Roaring) => {
                            output.push_back(Bitmap::from_lsb0_bytes(&[0b01010101], *bitmap_type))
                        }
                        _ => unreachable!(),
                    }
                }
                Ok(output)
            });

        let meta = InvertedIndexMeta {
            bitmap_type: BitmapType::Roaring.into(),
            ..Default::default()
        };
        let mut values_mapper = ParallelFstValuesMapper::new(&mut mock_reader);

        let result = values_mapper
            .map_values_vec(&[(vec![], &meta)], None)
            .await
            .unwrap();
        assert_eq!(result[0].count_ones(), 0);

        let result = values_mapper
            .map_values_vec(&[(vec![value(1, 1)], &meta)], None)
            .await
            .unwrap();
        assert_eq!(
            result[0],
            Bitmap::from_lsb0_bytes(&[0b10101010], BitmapType::Roaring)
        );

        let result = values_mapper
            .map_values_vec(&[(vec![value(2, 1)], &meta)], None)
            .await
            .unwrap();
        assert_eq!(
            result[0],
            Bitmap::from_lsb0_bytes(&[0b01010101], BitmapType::Roaring)
        );

        let result = values_mapper
            .map_values_vec(&[(vec![value(1, 1), value(2, 1)], &meta)], None)
            .await
            .unwrap();
        assert_eq!(
            result[0],
            Bitmap::from_lsb0_bytes(&[0b11111111], BitmapType::Roaring)
        );

        let result = values_mapper
            .map_values_vec(&[(vec![value(2, 1), value(1, 1)], &meta)], None)
            .await
            .unwrap();
        assert_eq!(
            result[0],
            Bitmap::from_lsb0_bytes(&[0b11111111], BitmapType::Roaring)
        );

        let result = values_mapper
            .map_values_vec(
                &[(vec![value(2, 1)], &meta), (vec![value(1, 1)], &meta)],
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            result[0],
            Bitmap::from_lsb0_bytes(&[0b01010101], BitmapType::Roaring)
        );
        assert_eq!(
            result[1],
            Bitmap::from_lsb0_bytes(&[0b10101010], BitmapType::Roaring)
        );
        let result = values_mapper
            .map_values_vec(
                &[
                    (vec![value(2, 1), value(1, 1)], &meta),
                    (vec![value(1, 1)], &meta),
                ],
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            result[0],
            Bitmap::from_lsb0_bytes(&[0b11111111], BitmapType::Roaring)
        );
        assert_eq!(
            result[1],
            Bitmap::from_lsb0_bytes(&[0b10101010], BitmapType::Roaring)
        );
    }
}
