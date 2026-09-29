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

use greptime_proto::v1::index::{BitmapType, InvertedIndexMeta};
use snafu::{IntoError, ResultExt};

use crate::bitmap::Bitmap;
use crate::inverted_index::error::{CommonIoSnafu, DecodeBitmapSnafu, Result};
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

    /// Maps each group of FST values to the union of the bitmaps its values point to, preserving the
    /// group order. All bitmaps are fetched with a single batched read and decoded one at a time;
    /// any decode failure returns an error instead of a partial result.
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
        let mut bitmap_types = Vec::with_capacity(len);

        for (values, meta) in value_and_meta_vec {
            for value in values {
                // The higher 32 bits of each u64 value represent the
                // bitmap offset and the lower 32 bits represent its size. This mapper uses these
                // combined offset-size pairs to fetch and union multiple bitmaps into a single `BitVec`.
                let [relative_offset, size] = bytemuck::cast::<u64, [u32; 2]>(*value);
                let start = meta.base_offset + relative_offset as u64;
                fetch_ranges.push(start..start + size as u64);
                bitmap_types
                    .push(BitmapType::try_from(meta.bitmap_type).unwrap_or(BitmapType::BitVec));
            }
        }

        if fetch_ranges.is_empty() {
            return Ok(vec![Bitmap::new_bitvec()]);
        }

        common_telemetry::debug!("fetch ranges: {:?}", fetch_ranges);
        let bytes_vec = self.reader.read_vec(&fetch_ranges, metrics).await?;
        // A conforming reader returns exactly one buffer per range. Bail out instead of silently
        // unioning a group from fewer bitmaps, which would drop matching rows.
        if bytes_vec.len() != fetch_ranges.len() {
            let error = io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Expected {} bitmap buffers but got {}",
                    fetch_ranges.len(),
                    bytes_vec.len()
                ),
            );
            return Err(CommonIoSnafu.into_error(error));
        }

        // `bitmap_types` is built alongside `fetch_ranges`, so every buffer keeps its bitmap type.
        let mut fetched = bytes_vec.into_iter().zip(bitmap_types);
        let mut output = Vec::with_capacity(groups.len());

        for counter in groups {
            let mut bitmap = Bitmap::new_roaring();
            for (bytes, bitmap_type) in fetched.by_ref().take(counter) {
                let decoded =
                    Bitmap::deserialize_from(&bytes, bitmap_type).context(DecodeBitmapSnafu)?;
                bitmap.union(decoded);
            }

            output.push(bitmap);
        }

        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::io;
    use std::ops::Range;

    use bytes::Bytes;
    use snafu::Location;

    use super::*;
    use crate::inverted_index::error::Error;
    use crate::inverted_index::format::reader::MockInvertedIndexReader;

    fn value(offset: u32, size: u32) -> u64 {
        bytemuck::cast::<[u32; 2], u64>([offset, size])
    }

    fn meta(bitmap_type: BitmapType) -> InvertedIndexMeta {
        InvertedIndexMeta {
            bitmap_type: bitmap_type.into(),
            ..Default::default()
        }
    }

    fn roaring(lsb0_bytes: &[u8]) -> Bitmap {
        Bitmap::from_lsb0_bytes(lsb0_bytes, BitmapType::Roaring)
    }

    fn serialized(bitmap: &Bitmap, bitmap_type: BitmapType) -> Bytes {
        let mut buf = Vec::new();
        bitmap.serialize_into(bitmap_type, &mut buf).unwrap();
        Bytes::from(buf)
    }

    /// Returns a mock reader that answers exactly one `read_vec` call with the serialized
    /// `responses`, asserting that the requested ranges are exactly `expected_ranges` in order.
    fn mock_reader_returning(
        expected_ranges: Vec<Range<u64>>,
        responses: Vec<(Bitmap, BitmapType)>,
    ) -> MockInvertedIndexReader {
        let mut mock_reader = MockInvertedIndexReader::new();
        mock_reader
            .expect_read_vec()
            .times(1)
            .returning(move |ranges, _metrics| {
                assert_eq!(ranges, expected_ranges.as_slice());
                Ok(responses
                    .iter()
                    .map(|(bitmap, bitmap_type)| serialized(bitmap, *bitmap_type))
                    .collect())
            });
        mock_reader
    }

    #[tokio::test]
    async fn test_map_values_vec_with_empty_values() {
        let roaring_meta = meta(BitmapType::Roaring);
        let bitvec_meta = meta(BitmapType::BitVec);
        // No `read_vec` expectation: no read is issued when every group is empty.
        let mut mock_reader = MockInvertedIndexReader::new();
        let mut values_mapper = ParallelFstValuesMapper::new(&mut mock_reader);

        let output = values_mapper.map_values_vec(&[], None).await.unwrap();
        assert_eq!(output, vec![Bitmap::new_bitvec()]);

        let output = values_mapper
            .map_values_vec(&[(vec![], &roaring_meta)], None)
            .await
            .unwrap();
        assert_eq!(output, vec![Bitmap::new_bitvec()]);

        // All groups empty keeps the existing single empty `BitVec` shape.
        let output = values_mapper
            .map_values_vec(&[(vec![], &roaring_meta), (vec![], &bitvec_meta)], None)
            .await
            .unwrap();
        assert_eq!(output, vec![Bitmap::new_bitvec()]);
    }

    #[tokio::test]
    async fn test_map_values_vec_single_group_in_value_order() {
        let roaring_meta = meta(BitmapType::Roaring);
        // Values are fetched in the order they are given, duplicates included.
        let mut mock_reader = mock_reader_returning(
            vec![2..3, 1..2, 1..2],
            vec![
                (roaring(&[0b01010101]), BitmapType::Roaring),
                (roaring(&[0b10101010]), BitmapType::Roaring),
                (roaring(&[0b10101010]), BitmapType::Roaring),
            ],
        );
        let mut values_mapper = ParallelFstValuesMapper::new(&mut mock_reader);

        let output = values_mapper
            .map_values_vec(
                &[(vec![value(2, 1), value(1, 1), value(1, 1)], &roaring_meta)],
                None,
            )
            .await
            .unwrap();

        assert_eq!(output, vec![roaring(&[0b11111111])]);
    }

    #[tokio::test]
    async fn test_map_values_vec_multiple_groups_with_empty_middle() {
        let roaring_meta = meta(BitmapType::Roaring);
        let mut mock_reader = mock_reader_returning(
            vec![2..3, 1..2, 4..5],
            vec![
                (roaring(&[0b01010101]), BitmapType::Roaring),
                (roaring(&[0b10101010]), BitmapType::Roaring),
                (roaring(&[0b00001111]), BitmapType::Roaring),
            ],
        );
        let mut values_mapper = ParallelFstValuesMapper::new(&mut mock_reader);

        let output = values_mapper
            .map_values_vec(
                &[
                    (vec![value(2, 1), value(1, 1)], &roaring_meta),
                    (vec![], &roaring_meta),
                    (vec![value(4, 1)], &roaring_meta),
                ],
                None,
            )
            .await
            .unwrap();

        assert_eq!(
            output,
            vec![
                roaring(&[0b11111111]),
                Bitmap::new_roaring(),
                roaring(&[0b00001111]),
            ]
        );
    }

    #[tokio::test]
    async fn test_map_values_vec_duplicate_values_and_both_encodings() {
        let roaring_meta = meta(BitmapType::Roaring);
        let bitvec_meta = meta(BitmapType::BitVec);
        let mut mock_reader = mock_reader_returning(
            vec![1..2, 1..2, 2..3],
            vec![
                (roaring(&[0b10101010]), BitmapType::Roaring),
                (roaring(&[0b10101010]), BitmapType::Roaring),
                (
                    Bitmap::from_lsb0_bytes(&[0b01010101], BitmapType::BitVec),
                    BitmapType::BitVec,
                ),
            ],
        );
        let mut values_mapper = ParallelFstValuesMapper::new(&mut mock_reader);

        let output = values_mapper
            .map_values_vec(
                &[
                    (vec![value(1, 1), value(1, 1)], &roaring_meta),
                    (vec![value(2, 1)], &bitvec_meta),
                ],
                None,
            )
            .await
            .unwrap();

        assert_eq!(
            output,
            vec![
                roaring(&[0b10101010]),
                Bitmap::from_lsb0_bytes(&[0b01010101], BitmapType::BitVec),
            ]
        );
    }

    #[tokio::test]
    async fn test_map_values_vec_late_malformed_bitmap_returns_error() {
        let roaring_meta = meta(BitmapType::Roaring);
        let mut mock_reader = MockInvertedIndexReader::new();
        mock_reader
            .expect_read_vec()
            .times(1)
            .returning(|ranges, _metrics| {
                assert_eq!(ranges, [1..2, 2..3].as_slice());
                Ok(vec![
                    serialized(&roaring(&[0b10101010]), BitmapType::Roaring),
                    Bytes::from_static(b"not a roaring bitmap"),
                ])
            });
        let mut values_mapper = ParallelFstValuesMapper::new(&mut mock_reader);

        let output = values_mapper
            .map_values_vec(&[(vec![value(1, 1), value(2, 1)], &roaring_meta)], None)
            .await;

        assert!(matches!(output, Err(Error::DecodeBitmap { .. })));
    }

    #[tokio::test]
    async fn test_map_values_vec_reader_failure_returns_error() {
        let roaring_meta = meta(BitmapType::Roaring);
        let mut mock_reader = MockInvertedIndexReader::new();
        mock_reader
            .expect_read_vec()
            .times(1)
            .returning(|_ranges, _metrics| {
                Err(Error::Read {
                    error: io::Error::other("read failed"),
                    location: Location::default(),
                })
            });
        let mut values_mapper = ParallelFstValuesMapper::new(&mut mock_reader);

        let output = values_mapper
            .map_values_vec(&[(vec![value(1, 1)], &roaring_meta)], None)
            .await;

        assert!(matches!(output, Err(Error::Read { .. })));
    }

    #[tokio::test]
    async fn test_map_values_vec_short_read_response_returns_error() {
        let roaring_meta = meta(BitmapType::Roaring);
        let mut mock_reader = MockInvertedIndexReader::new();
        mock_reader
            .expect_read_vec()
            .times(1)
            .returning(|ranges, _metrics| {
                assert_eq!(ranges, [1..2, 2..3].as_slice());
                // Fewer buffers than requested must not silently produce a partial group.
                Ok(vec![serialized(
                    &roaring(&[0b10101010]),
                    BitmapType::Roaring,
                )])
            });
        let mut values_mapper = ParallelFstValuesMapper::new(&mut mock_reader);

        let output = values_mapper
            .map_values_vec(&[(vec![value(1, 1), value(2, 1)], &roaring_meta)], None)
            .await;

        assert!(matches!(output, Err(Error::CommonIo { .. })));
    }

    #[tokio::test]
    async fn test_map_values_vec_matches_eager_decode_reference() {
        let roaring_meta = meta(BitmapType::Roaring);
        let bitvec_meta = meta(BitmapType::BitVec);
        let responses = vec![
            (roaring(&[0b10101010]), BitmapType::Roaring),
            (roaring(&[0b01010101]), BitmapType::Roaring),
            (
                Bitmap::from_lsb0_bytes(&[0b00001111], BitmapType::BitVec),
                BitmapType::BitVec,
            ),
        ];
        let mut mock_reader = mock_reader_returning(vec![1..2, 2..3, 3..4], responses.clone());
        let mut values_mapper = ParallelFstValuesMapper::new(&mut mock_reader);

        let output = values_mapper
            .map_values_vec(
                &[
                    (vec![value(1, 1), value(2, 1)], &roaring_meta),
                    (vec![], &roaring_meta),
                    (vec![value(3, 1)], &bitvec_meta),
                ],
                None,
            )
            .await
            .unwrap();

        // The previous eager behavior: decode every fetched bitmap first, then pop and union
        // them into each group in order.
        let mut decoded = VecDeque::new();
        for (bitmap, bitmap_type) in &responses {
            let bytes = serialized(bitmap, *bitmap_type);
            decoded.push_back(Bitmap::deserialize_from(&bytes, *bitmap_type).unwrap());
        }
        let mut eager = Vec::new();
        for counter in [2usize, 0, 1] {
            let mut bitmap = Bitmap::new_roaring();
            for _ in 0..counter {
                bitmap.union(decoded.pop_front().unwrap());
            }
            eager.push(bitmap);
        }

        assert_eq!(output, eager);
        assert_eq!(
            output,
            vec![
                roaring(&[0b11111111]),
                Bitmap::new_roaring(),
                Bitmap::from_lsb0_bytes(&[0b00001111], BitmapType::BitVec),
            ]
        );
    }
}
