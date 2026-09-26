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

use crate::Bytes;
use crate::bloom_filter::error::Result;
use crate::bloom_filter::reader::{BloomFilterReadMetrics, BloomFilterReader};

/// Filter bytes one batch of [`BloomFilterApplier::search_groups`] reads. A single row
/// group larger than this is still read as one batch, so this is not a memory limit; a
/// batch holds both its raw bytes and the decoded filters.
const MAX_BATCH_FILTER_BYTES: u64 = 8 * 1024 * 1024;

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
        let meta = reader.metadata(None).await?;

        Ok(Self { reader, meta })
    }

    /// Runs [`Self::search`] over several groups of ranges (e.g. row groups) and keeps in
    /// each group only its matching ranges.
    ///
    /// Consecutive groups are searched together, one read per batch, as long as the
    /// filters of a batch stay within [`MAX_BATCH_FILTER_BYTES`]. This saves a round trip
    /// per group on object storage without holding the filters of a whole file at once.
    /// Groups must be ordered and their ranges sorted and disjoint, as for `search`.
    pub async fn search_groups(
        &mut self,
        predicates: &[InListPredicate],
        groups: &mut [&mut Vec<Range<usize>>],
        metrics: Option<&mut BloomFilterReadMetrics>,
    ) -> Result<()> {
        self.search_groups_in_batches(predicates, groups, metrics, MAX_BATCH_FILTER_BYTES)
            .await
    }

    async fn search_groups_in_batches(
        &mut self,
        predicates: &[InListPredicate],
        groups: &mut [&mut Vec<Range<usize>>],
        mut metrics: Option<&mut BloomFilterReadMetrics>,
        max_batch_bytes: u64,
    ) -> Result<()> {
        let mut start = 0;
        while start < groups.len() {
            // Mirrors `load_bloom_filters`: segments map to filter locations in order and
            // only consecutive equal locations share a read, so this is the exact number of
            // bytes the batch requests.
            let mut last_loc = None;
            let mut batch_bytes = 0;
            let mut end = start;
            while end < groups.len() {
                let mut group_last = last_loc;
                let mut bytes = 0;
                for seg in self.row_ranges_to_segments(groups[end]) {
                    let loc = self.meta.segment_loc_indices[seg];
                    if group_last != Some(loc) {
                        bytes += self.meta.bloom_filter_locs[loc as usize].size;
                        group_last = Some(loc);
                    }
                }
                // A group whose filters alone exceed the budget still forms its own batch.
                if end > start && batch_bytes + bytes > max_batch_bytes {
                    break;
                }
                last_loc = group_last;
                batch_bytes += bytes;
                end += 1;
            }
            self.search_batch(predicates, &mut groups[start..end], metrics.as_deref_mut())
                .await?;
            start = end;
        }
        Ok(())
    }

    /// Searches `groups` with one `search`, i.e. one read for all their filters.
    async fn search_batch(
        &mut self,
        predicates: &[InListPredicate],
        groups: &mut [&mut Vec<Range<usize>>],
        metrics: Option<&mut BloomFilterReadMetrics>,
    ) -> Result<()> {
        let all = groups
            .iter()
            .flat_map(|g| g.iter().cloned())
            .collect::<Vec<_>>();
        if all.is_empty() {
            return Ok(());
        }
        // Each matched range lies within one input range, so it belongs to one group.
        let mut matched = self
            .search(predicates, &all, metrics)
            .await?
            .into_iter()
            .peekable();
        for group in groups.iter_mut() {
            for range in std::mem::take(*group) {
                while let Some(m) = matched.next_if(|m| m.start < range.end) {
                    group.push(m);
                }
            }
        }
        Ok(())
    }

    /// Searches ranges of rows that match all the given predicates in the search ranges.
    /// Each predicate represents an OR condition of probes, and all predicates must match (AND semantics).
    /// The logic is: (probe1 OR probe2 OR ...) AND (probe3 OR probe4 OR ...)
    pub async fn search(
        &mut self,
        predicates: &[InListPredicate],
        search_ranges: &[Range<usize>],
        metrics: Option<&mut BloomFilterReadMetrics>,
    ) -> Result<Vec<Range<usize>>> {
        if predicates.is_empty() {
            // If no predicates, return empty result
            return Ok(Vec::new());
        }

        let segments = self.row_ranges_to_segments(search_ranges);
        let (seg_locations, bloom_filters) = self.load_bloom_filters(&segments, metrics).await?;
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
        metrics: Option<&mut BloomFilterReadMetrics>,
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

        let bloom_filters = self
            .reader
            .bloom_filter_vec(&bloom_filter_locs, metrics)
            .await?;

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
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;

    use futures::io::Cursor;

    use super::*;
    use crate::bloom_filter::creator::BloomFilterCreator;
    use crate::bloom_filter::reader::BloomFilterReaderImpl;
    use crate::external_provider::MockExternalTempFileProvider;

    #[tokio::test]
    async fn test_search_groups_matches_per_group_search() {
        let mut creator = BloomFilterCreator::new(
            4,
            0.01,
            Arc::new(MockExternalTempFileProvider::new()),
            Arc::new(AtomicUsize::new(0)),
            None,
        );
        // Row i holds "v{i / 3}", so values straddle segment boundaries.
        for i in 0..40 {
            creator
                .push_row_elems([format!("v{}", i / 3).into_bytes()])
                .await
                .unwrap();
        }
        let mut writer = Cursor::new(Vec::new());
        creator.finish(&mut writer).await.unwrap();
        let bytes = writer.into_inner();

        // Row groups of 10 rows, some already narrowed by other predicates.
        let groups = vec![
            vec![0..10],
            vec![10..13, 15..20],
            vec![],
            vec![30..33, 37..40],
        ];
        for values in [
            vec!["v1"],
            vec!["v4", "v5"],
            vec!["v3", "v10", "v12"],
            vec!["x"],
        ] {
            let predicates = vec![InListPredicate {
                list: values.iter().map(|v| v.as_bytes().to_vec()).collect(),
            }];
            let mut applier =
                BloomFilterApplier::new(Box::new(BloomFilterReaderImpl::new(bytes.clone())))
                    .await
                    .unwrap();
            let filter_size = applier.meta.bloom_filter_locs[0].size;
            let mut expected = Vec::new();
            for group in &groups {
                expected.push(if group.is_empty() {
                    vec![]
                } else {
                    applier.search(&predicates, group, None).await.unwrap()
                });
            }
            // Budgets of zero (a batch per group), one filter and unlimited (one batch).
            for budget in [0, filter_size, u64::MAX] {
                let mut actual = groups.clone();
                let mut refs = actual.iter_mut().collect::<Vec<_>>();
                applier
                    .search_groups_in_batches(&predicates, &mut refs, None, budget)
                    .await
                    .unwrap();
                assert_eq!(actual, expected, "values: {values:?}, budget: {budget}");
            }
        }
    }

    /// Records the bytes of every `read_vec`, i.e. of every batch.
    struct RecordingReader {
        inner: BloomFilterReaderImpl<Vec<u8>>,
        reads: Arc<std::sync::Mutex<Vec<u64>>>,
    }

    #[async_trait::async_trait]
    impl BloomFilterReader for RecordingReader {
        async fn range_read(
            &self,
            offset: u64,
            size: u32,
            metrics: Option<&mut BloomFilterReadMetrics>,
        ) -> Result<bytes::Bytes> {
            self.inner.range_read(offset, size, metrics).await
        }

        async fn read_vec(
            &self,
            ranges: &[Range<u64>],
            metrics: Option<&mut BloomFilterReadMetrics>,
        ) -> Result<Vec<bytes::Bytes>> {
            let bytes = ranges.iter().map(|r| r.end - r.start).sum();
            self.reads.lock().unwrap().push(bytes);
            self.inner.read_vec(ranges, metrics).await
        }

        async fn metadata(
            &self,
            metrics: Option<&mut BloomFilterReadMetrics>,
        ) -> Result<BloomFilterMeta> {
            self.inner.metadata(metrics).await
        }
    }

    #[tokio::test]
    #[allow(clippy::single_range_in_vec_init)]
    async fn test_search_groups_respects_budget() {
        let mut creator = BloomFilterCreator::new(
            4,
            0.01,
            Arc::new(MockExternalTempFileProvider::new()),
            Arc::new(AtomicUsize::new(0)),
            None,
        );
        // Distinct values everywhere: 10 segments with filters of the same size.
        for i in 0..40 {
            creator
                .push_row_elems([format!("v{i}").into_bytes()])
                .await
                .unwrap();
        }
        let mut writer = Cursor::new(Vec::new());
        creator.finish(&mut writer).await.unwrap();
        let bytes = writer.into_inner();
        let predicates = vec![InListPredicate {
            list: BTreeSet::from([b"v1".to_vec()]),
        }];

        let reads = Arc::new(std::sync::Mutex::new(Vec::new()));
        let reader = RecordingReader {
            inner: BloomFilterReaderImpl::new(bytes),
            reads: reads.clone(),
        };
        let mut applier = BloomFilterApplier::new(Box::new(reader)).await.unwrap();
        let filter_size = applier.meta.bloom_filter_locs[0].size;

        // Row groups of 6 rows: most share a boundary segment with their neighbor, which
        // a batch reads only once.
        let groups = (0..40)
            .step_by(6)
            .map(|s| vec![s..(s + 6).min(40)])
            .collect::<Vec<_>>();
        for (budget, expected_reads) in [
            (u64::MAX, vec![10 * filter_size]),
            // Batches close before exceeding the budget; shared boundary filters count once.
            (
                5 * filter_size,
                vec![5 * filter_size, 5 * filter_size, filter_size],
            ),
            // Smaller than any row group: one row group per batch, still above the budget.
            (
                filter_size,
                vec![
                    2 * filter_size,
                    2 * filter_size,
                    2 * filter_size,
                    2 * filter_size,
                    2 * filter_size,
                    2 * filter_size,
                    filter_size,
                ],
            ),
        ] {
            reads.lock().unwrap().clear();
            let mut actual = groups.clone();
            let mut refs = actual.iter_mut().collect::<Vec<_>>();
            applier
                .search_groups_in_batches(&predicates, &mut refs, None, budget)
                .await
                .unwrap();
            assert_eq!(*reads.lock().unwrap(), expected_reads, "budget: {budget}");
        }

        // Segments 0..3 hold the same value and share one filter, which a batch counts
        // and reads once even across row groups.
        let mut creator = BloomFilterCreator::new(
            4,
            0.01,
            Arc::new(MockExternalTempFileProvider::new()),
            Arc::new(AtomicUsize::new(0)),
            None,
        );
        for i in 0..24 {
            let value = if i < 12 {
                "a".to_string()
            } else {
                format!("v{i}")
            };
            creator.push_row_elems([value.into_bytes()]).await.unwrap();
        }
        let mut writer = Cursor::new(Vec::new());
        creator.finish(&mut writer).await.unwrap();
        let reader = RecordingReader {
            inner: BloomFilterReaderImpl::new(writer.into_inner()),
            reads: reads.clone(),
        };
        let mut applier = BloomFilterApplier::new(Box::new(reader)).await.unwrap();
        assert_eq!(applier.meta.bloom_filter_locs.len(), 4);
        let filter_size = applier.meta.bloom_filter_locs[0].size;
        assert!(
            applier
                .meta
                .bloom_filter_locs
                .iter()
                .all(|l| l.size == filter_size)
        );
        reads.lock().unwrap().clear();
        let mut groups = (0..24)
            .step_by(6)
            .map(|s| vec![s..s + 6])
            .collect::<Vec<_>>();
        let mut refs = groups.iter_mut().collect::<Vec<_>>();
        applier
            .search_groups_in_batches(&predicates, &mut refs, None, 2 * filter_size)
            .await
            .unwrap();
        // Row groups 0 and 1 need only the shared filter; 2 and 3 need two each.
        assert_eq!(
            *reads.lock().unwrap(),
            vec![filter_size, 2 * filter_size, 2 * filter_size]
        );
    }

    #[tokio::test]
    #[allow(clippy::single_range_in_vec_init)]
    async fn test_appliter() {
        let mut writer = Cursor::new(Vec::new());
        let mut creator = BloomFilterCreator::new(
            4,
            0.01,
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
            let result = applier
                .search(&predicates, &[search_range], None)
                .await
                .unwrap();
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
