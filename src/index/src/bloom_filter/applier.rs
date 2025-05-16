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

use std::collections::BTreeSet;
use std::ops::Range;

use fastbloom::BloomFilter;
use greptime_proto::v1::index::BloomFilterMeta;
use itertools::Itertools;

use crate::bloom_filter::error::Result;
use crate::bloom_filter::reader::BloomFilterReader;
use crate::Bytes;

/// `InListPredicate` contains a list of acceptable values. A value needs to match at least
/// one of the elements (logical OR semantic) for the predicate to be satisfied.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct InListPredicate {
    /// List of acceptable values.
    pub list: BTreeSet<Bytes>,
}

pub struct BloomFilterApplier {
    reader: Box<dyn BloomFilterReader + Send>,
    meta: BloomFilterMeta,
}

impl BloomFilterApplier {
    pub async fn new(reader: Box<dyn BloomFilterReader + Send>) -> Result<Self> {
        let meta = reader.metadata().await?;

        Ok(Self { reader, meta })
    }

    /// Searches ranges of rows that match all the given predicates in the search ranges.
    /// Each predicate represents an OR condition of probes, and all predicates must match (AND semantics).
    /// The logic is: (probe1 OR probe2 OR ...) AND (probe3 OR probe4 OR ...)
    pub async fn search(
        &mut self,
        predicates: &[InListPredicate],
        search_ranges: &[Range<usize>],
    ) -> Result<Vec<Range<usize>>> {
        if predicates.is_empty() {
            // If no predicates, return empty result
            return Ok(Vec::new());
        }

        let segments = self.row_ranges_to_segments(search_ranges);
        let (seg_locations, bloom_filters) = self.load_bloom_filters(&segments).await?;
        let matching_row_ranges = self.find_matching_rows(seg_locations, bloom_filters, predicates);
        Ok(intersect_ranges(search_ranges, &matching_row_ranges))
    }

    /// Converts row ranges to segment ranges and returns unique segments
    fn row_ranges_to_segments(&self, row_ranges: &[Range<usize>]) -> Vec<usize> {
        let rows_per_segment = self.meta.rows_per_segment as usize;

        let mut segments = vec![];
        for range in row_ranges {
            let start_seg = range.start / rows_per_segment;
            let mut end_seg = range.end.div_ceil(rows_per_segment);

            if end_seg == self.meta.segment_loc_indices.len() + 1 {
                // Handle legacy bug with missing last segment
                //
                // In a previous version, there was a bug where if the last segment was all null,
                // this segment would not be written into the index. This caused the slice
                // `self.meta.segment_loc_indices[start_seg..end_seg]` to go out of bounds due to
                // the missing segment. Since the `search` function does not search for nulls,
                // we can simply ignore the last segment in this buggy scenario.
                end_seg -= 1;
            }
            segments.extend(start_seg..end_seg);
        }

        // Ensure segments are unique and sorted
        segments.sort_unstable();
        segments.dedup();

        segments
    }

    /// Loads bloom filters for the given segments and returns the segment locations and bloom filters
    async fn load_bloom_filters(
        &mut self,
        segments: &[usize],
    ) -> Result<(Vec<(u64, usize)>, Vec<BloomFilter>)> {
        let segment_locations = segments
            .iter()
            .map(|&seg| (self.meta.segment_loc_indices[seg], seg))
            .collect::<Vec<_>>();

        let bloom_filter_locs = segment_locations
            .iter()
            .map(|(loc, _)| *loc)
            .dedup()
            .map(|i| self.meta.bloom_filter_locs[i as usize])
            .collect::<Vec<_>>();

        let bloom_filters = self.reader.bloom_filter_vec(&bloom_filter_locs).await?;

        Ok((segment_locations, bloom_filters))
    }

    /// Finds segments that match all predicates and converts them to row ranges
    fn find_matching_rows(
        &self,
        segment_locations: Vec<(u64, usize)>,
        bloom_filters: Vec<BloomFilter>,
        predicates: &[InListPredicate],
    ) -> Vec<Range<usize>> {
        let rows_per_segment = self.meta.rows_per_segment as usize;
        let mut matching_row_ranges = Vec::with_capacity(bloom_filters.len());

        // Group segments by their location index (since they have the same bloom filter) and check if they match all predicates
        for ((_loc_index, group), bloom_filter) in segment_locations
            .into_iter()
            .chunk_by(|(loc, _)| *loc)
            .into_iter()
            .zip(bloom_filters.iter())
        {
            // Check if this bloom filter matches each predicate (AND semantics)
            let matches_all_predicates = predicates.iter().all(|predicate| {
                // For each predicate, at least one probe must match (OR semantics)
                predicate
                    .list
                    .iter()
                    .any(|probe| bloom_filter.contains(probe))
            });

            if !matches_all_predicates {
                continue;
            }

            // For each matching segment, convert to row range
            for (_, segment) in group {
                let start_row = segment * rows_per_segment;
                let end_row = (segment + 1) * rows_per_segment;
                matching_row_ranges.push(start_row..end_row);
            }
        }

        self.merge_adjacent_ranges(matching_row_ranges)
    }

    /// Merges adjacent row ranges to reduce the number of ranges
    fn merge_adjacent_ranges(&self, ranges: Vec<Range<usize>>) -> Vec<Range<usize>> {
        ranges
            .into_iter()
            .coalesce(|prev, next| {
                if prev.end == next.start {
                    Ok(prev.start..next.end)
                } else {
                    Err((prev, next))
                }
            })
            .collect::<Vec<_>>()
    }
}

/// Intersects two lists of ranges and returns the intersection.
///
/// The input lists are assumed to be sorted and non-overlapping.
fn intersect_ranges(lhs: &[Range<usize>], rhs: &[Range<usize>]) -> Vec<Range<usize>> {
    let mut i = 0;
    let mut j = 0;

    let mut output = Vec::new();
    while i < lhs.len() && j < rhs.len() {
        let r1 = &lhs[i];
        let r2 = &rhs[j];

        // Find intersection if exists
        let start = r1.start.max(r2.start);
        let end = r1.end.min(r2.end);

        if start < end {
            output.push(start..end);
        }

        // Move forward the range that ends first
        if r1.end < r2.end {
            i += 1;
        } else {
            j += 1;
        }
    }

    output
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::Arc;

    use futures::io::Cursor;

    use super::*;
    use crate::bloom_filter::creator::BloomFilterCreator;
    use crate::bloom_filter::reader::BloomFilterReaderImpl;
    use crate::external_provider::MockExternalTempFileProvider;

    #[tokio::test]
    #[allow(clippy::single_range_in_vec_init)]
    async fn test_appliter() {
        let mut writer = Cursor::new(Vec::new());
        let mut creator = BloomFilterCreator::new(
            4,
            Arc::new(MockExternalTempFileProvider::new()),
            Arc::new(AtomicUsize::new(0)),
            None,
        );

        let rows = vec![
            // seg 0
            vec![b"row00".to_vec(), b"seg00".to_vec(), b"overl".to_vec()],
            vec![b"row01".to_vec(), b"seg00".to_vec(), b"overl".to_vec()],
            vec![b"row02".to_vec(), b"seg00".to_vec(), b"overl".to_vec()],
            vec![b"row03".to_vec(), b"seg00".to_vec(), b"overl".to_vec()],
            // seg 1
            vec![b"row04".to_vec(), b"seg01".to_vec(), b"overl".to_vec()],
            vec![b"row05".to_vec(), b"seg01".to_vec(), b"overl".to_vec()],
            vec![b"row06".to_vec(), b"seg01".to_vec(), b"overp".to_vec()],
            vec![b"row07".to_vec(), b"seg01".to_vec(), b"overp".to_vec()],
            // seg 2
            vec![b"row08".to_vec(), b"seg02".to_vec(), b"overp".to_vec()],
            vec![b"row09".to_vec(), b"seg02".to_vec(), b"overp".to_vec()],
            vec![b"row10".to_vec(), b"seg02".to_vec(), b"overp".to_vec()],
            vec![b"row11".to_vec(), b"seg02".to_vec(), b"overp".to_vec()],
            // duplicate rows
            // seg 3
            vec![b"dup".to_vec()],
            vec![b"dup".to_vec()],
            vec![b"dup".to_vec()],
            vec![b"dup".to_vec()],
            // seg 4
            vec![b"dup".to_vec()],
            vec![b"dup".to_vec()],
            vec![b"dup".to_vec()],
            vec![b"dup".to_vec()],
            // seg 5
            vec![b"dup".to_vec()],
            vec![b"dup".to_vec()],
            vec![b"dup".to_vec()],
            vec![b"dup".to_vec()],
            // seg 6
            vec![b"dup".to_vec()],
            vec![b"dup".to_vec()],
            vec![b"dup".to_vec()],
            vec![b"dup".to_vec()],
        ];

        for row in rows {
            creator.push_row_elems(row).await.unwrap();
        }

        creator.finish(&mut writer).await.unwrap();

        let bytes = writer.into_inner();
        let reader = BloomFilterReaderImpl::new(bytes);
        let mut applier = BloomFilterApplier::new(Box::new(reader)).await.unwrap();

        // Test cases for predicates
        let cases = vec![
            // Single value predicates
            (
                vec![InListPredicate {
                    list: BTreeSet::from_iter([b"row00".to_vec()]),
                }],
                0..28,
                vec![0..4],
            ),
            (
                vec![InListPredicate {
                    list: BTreeSet::from_iter([b"row05".to_vec()]),
                }],
                4..8,
                vec![4..8],
            ),
            (
                vec![InListPredicate {
                    list: BTreeSet::from_iter([b"row03".to_vec()]),
                }],
                4..8,
                vec![],
            ),
            // Multiple values in a single predicate (OR logic)
            (
                vec![InListPredicate {
                    list: BTreeSet::from_iter([b"overl".to_vec(), b"row06".to_vec()]),
                }],
                0..28,
                vec![0..8],
            ),
            (
                vec![InListPredicate {
                    list: BTreeSet::from_iter([b"seg01".to_vec(), b"overp".to_vec()]),
                }],
                0..28,
                vec![4..12],
            ),
            // Non-existent values
            (
                vec![InListPredicate {
                    list: BTreeSet::from_iter([b"row99".to_vec()]),
                }],
                0..28,
                vec![],
            ),
            // Empty range
            (
                vec![InListPredicate {
                    list: BTreeSet::from_iter([b"row00".to_vec()]),
                }],
                12..12,
                vec![],
            ),
            // Multiple values in a single predicate within specific ranges
            (
                vec![InListPredicate {
                    list: BTreeSet::from_iter([b"row04".to_vec(), b"row05".to_vec()]),
                }],
                0..12,
                vec![4..8],
            ),
            (
                vec![InListPredicate {
                    list: BTreeSet::from_iter([b"seg01".to_vec()]),
                }],
                0..28,
                vec![4..8],
            ),
            (
                vec![InListPredicate {
                    list: BTreeSet::from_iter([b"seg01".to_vec()]),
                }],
                6..28,
                vec![6..8],
            ),
            // Values spanning multiple segments
            (
                vec![InListPredicate {
                    list: BTreeSet::from_iter([b"overl".to_vec()]),
                }],
                0..28,
                vec![0..8],
            ),
            (
                vec![InListPredicate {
                    list: BTreeSet::from_iter([b"overl".to_vec()]),
                }],
                2..28,
                vec![2..8],
            ),
            (
                vec![InListPredicate {
                    list: BTreeSet::from_iter([b"overp".to_vec()]),
                }],
                0..10,
                vec![4..10],
            ),
            // Duplicate values
            (
                vec![InListPredicate {
                    list: BTreeSet::from_iter([b"dup".to_vec()]),
                }],
                0..12,
                vec![],
            ),
            (
                vec![InListPredicate {
                    list: BTreeSet::from_iter([b"dup".to_vec()]),
                }],
                0..16,
                vec![12..16],
            ),
            (
                vec![InListPredicate {
                    list: BTreeSet::from_iter([b"dup".to_vec()]),
                }],
                0..28,
                vec![12..28],
            ),
            // Multiple predicates (AND logic)
            (
                vec![
                    InListPredicate {
                        list: BTreeSet::from_iter([b"row00".to_vec(), b"row01".to_vec()]),
                    },
                    InListPredicate {
                        list: BTreeSet::from_iter([b"seg00".to_vec()]),
                    },
                ],
                0..28,
                vec![0..4],
            ),
            (
                vec![
                    InListPredicate {
                        list: BTreeSet::from_iter([b"overl".to_vec()]),
                    },
                    InListPredicate {
                        list: BTreeSet::from_iter([b"seg01".to_vec()]),
                    },
                ],
                0..28,
                vec![4..8],
            ),
        ];

        for (predicates, search_range, expected) in cases {
            let result = applier.search(&predicates, &[search_range]).await.unwrap();
            assert_eq!(
                result, expected,
                "Expected {:?}, got {:?}",
                expected, result
            );
        }
    }

    #[test]
    #[allow(clippy::single_range_in_vec_init)]
    fn test_intersect_ranges() {
        // empty inputs
        assert_eq!(intersect_ranges(&[], &[]), Vec::<Range<usize>>::new());
        assert_eq!(intersect_ranges(&[1..5], &[]), Vec::<Range<usize>>::new());
        assert_eq!(intersect_ranges(&[], &[1..5]), Vec::<Range<usize>>::new());

        // no overlap
        assert_eq!(
            intersect_ranges(&[1..3, 5..7], &[3..5, 7..9]),
            Vec::<Range<usize>>::new()
        );

        // single overlap
        assert_eq!(intersect_ranges(&[1..5], &[3..7]), vec![3..5]);

        // multiple overlaps
        assert_eq!(
            intersect_ranges(&[1..5, 7..10, 12..15], &[2..6, 8..13]),
            vec![2..5, 8..10, 12..13]
        );

        // exact overlap
        assert_eq!(
            intersect_ranges(&[1..3, 5..7], &[1..3, 5..7]),
            vec![1..3, 5..7]
        );

        // contained ranges
        assert_eq!(
            intersect_ranges(&[1..10], &[2..4, 5..7, 8..9]),
            vec![2..4, 5..7, 8..9]
        );

        // partial overlaps
        assert_eq!(
            intersect_ranges(&[1..4, 6..9], &[2..7, 8..10]),
            vec![2..4, 6..7, 8..9]
        );

        // single point overlap
        assert_eq!(
            intersect_ranges(&[1..3], &[3..5]),
            Vec::<Range<usize>>::new()
        );

        // large ranges
        assert_eq!(intersect_ranges(&[0..100], &[50..150]), vec![50..100]);
    }
}
