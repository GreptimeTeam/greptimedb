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

//! Structs for partition ranges.

use std::collections::BTreeMap;

use bytes::Bytes;
use common_time::Timestamp;
use futures::future::BoxFuture;
use smallvec::{SmallVec, smallvec};
use store_api::region_engine::PartitionRange;
use store_api::storage::TimeSeriesDistribution;

use crate::cache::CacheStrategy;
use crate::compaction::run::merge_primary_key_ranges;
use crate::memtable::{MemtableRange, MemtableStats};
use crate::read::scan_region::ScanInput;
use crate::sst::file::{FileHandle, FileTimeRange, overlaps};
use crate::sst::parquet::DEFAULT_ROW_GROUP_SIZE;
use crate::sst::parquet::file_range::{FileRange, FileRangeContextRef};
use crate::sst::parquet::format::parquet_row_group_time_range;
use crate::sst::parquet::row_selection::RowGroupSelection;

const ALL_ROW_GROUPS: i64 = -1;

/// Index and metadata for a memtable or file.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct SourceIndex {
    /// Index of the memtable and file.
    pub(crate) index: usize,
    /// Total number of row groups in this source. 0 if the metadata
    /// is unavailable. We use this to split files.
    pub(crate) num_row_groups: u64,
}

/// Index to access a row group.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RowGroupIndex {
    /// Index to the memtable/file.
    pub(crate) index: usize,
    /// Row group index in the file.
    /// Negative index indicates all row groups.
    pub row_group_index: i64,
}

/// Meta data of a partition range.
/// If the scanner is [UnorderedScan], each meta only has one row group or memtable.
/// If the scanner is [SeqScan], each meta may have multiple row groups and memtables.
#[derive(Debug, PartialEq, Clone)]
pub(crate) struct RangeMeta {
    /// The time range of the range.
    pub(crate) time_range: FileTimeRange,
    /// Indices to memtables or files.
    pub(crate) indices: SmallVec<[SourceIndex; 2]>,
    /// Indices to memtable/file row groups that this range scans.
    pub(crate) row_group_indices: SmallVec<[RowGroupIndex; 2]>,
    /// Estimated number of rows in the range. This can be 0 if the statistics are not available.
    pub(crate) num_rows: usize,
}

impl RangeMeta {
    /// Creates a [PartitionRange] with specific identifier.
    /// It converts the inclusive max timestamp to exclusive end timestamp.
    pub(crate) fn new_partition_range(&self, identifier: usize) -> PartitionRange {
        PartitionRange {
            start: self.time_range.0,
            end: Timestamp::new(
                // The i64::MAX timestamp may be invisible but we don't guarantee to support this
                // value now.
                self.time_range
                    .1
                    .value()
                    .checked_add(1)
                    .unwrap_or(self.time_range.1.value()),
                self.time_range.1.unit(),
            ),
            num_rows: self.num_rows,
            identifier,
        }
    }

    /// Creates a list of ranges from the `input` for seq scan.
    /// If `input.compaction` is true, it doesn't split the ranges.
    pub(crate) fn seq_scan_ranges(input: &ScanInput) -> Vec<RangeMeta> {
        let mut ranges = Vec::with_capacity(input.memtables.len() + input.files.len());
        Self::push_seq_mem_ranges(&input.memtables, &mut ranges);
        Self::push_seq_file_ranges(input.memtables.len(), &input.files, &mut ranges);

        #[cfg(feature = "enterprise")]
        Self::push_extension_ranges(input, &mut ranges);

        let per_series = input.distribution == Some(TimeSeriesDistribution::PerSeries);
        // Scans with a distribution keep grouping ranges by time only. `RegionScanExec`
        // declares `(ts, primary key)` ordering for `TimeWindowed`; SeqScan already breaks it
        // for files that overlap in keys, and splitting by key would break it for disjoint ones.
        let split_by_key = input.distribution.is_none();
        let num_memtables = input.memtables.len();
        let mapper = input.primary_key_mapper();
        let ranges = ranges
            .into_iter()
            .map(|range| {
                let primary_keys = range.indices[0]
                    .index
                    .checked_sub(num_memtables)
                    .and_then(|i| input.files.get(i))
                    .filter(|_| split_by_key)
                    .and_then(|file| file.primary_key_range(mapper));
                (range, primary_keys)
            })
            .collect();
        let ranges = group_ranges_for_seq_scan(ranges);
        if input.compaction || per_series {
            // We don't split ranges in compaction or TimeSeriesDistribution::PerSeries.
            return ranges;
        }
        maybe_split_ranges_for_seq_scan(ranges)
    }

    /// Creates a list of ranges from the `input` for unordered scan.
    pub(crate) fn unordered_scan_ranges(input: &ScanInput) -> BoxFuture<'_, Vec<RangeMeta>> {
        // Keep Moka's async lookup type out of callers' nested scanner futures.
        Box::pin(async move {
            let mut ranges = Vec::with_capacity(input.memtables.len() + input.files.len());
            Self::push_unordered_mem_ranges(&input.memtables, &mut ranges);
            Self::push_unordered_file_ranges(
                input.memtables.len(),
                &input.files,
                &input.cache_strategy,
                &mut ranges,
            )
            .await;

            #[cfg(feature = "enterprise")]
            Self::push_extension_ranges(input, &mut ranges);

            ranges
        })
    }

    /// Returns true if the time range of given `meta` overlaps with the time range of this meta.
    fn overlaps(&self, meta: &RangeMeta) -> bool {
        overlaps(&self.time_range, &meta.time_range)
    }

    /// Merges given `meta` to this meta.
    /// It assumes that the time ranges overlap and they don't have the same file or memtable index.
    fn merge(&mut self, mut other: RangeMeta) {
        debug_assert!(self.overlaps(&other));
        debug_assert!(self.indices.iter().all(|idx| !other.indices.contains(idx)));
        debug_assert!(
            self.row_group_indices
                .iter()
                .all(|idx| !other.row_group_indices.contains(idx))
        );

        self.time_range = (
            self.time_range.0.min(other.time_range.0),
            self.time_range.1.max(other.time_range.1),
        );
        self.indices.append(&mut other.indices);
        self.row_group_indices.append(&mut other.row_group_indices);
        self.num_rows += other.num_rows;
    }

    /// Returns true if we can split the range into multiple smaller ranges and
    /// still preserve the order for [SeqScan].
    fn can_split_preserve_order(&self) -> bool {
        self.indices.len() == 1 && self.indices[0].num_row_groups > 1
    }

    /// Splits the range if it can preserve the order.
    fn maybe_split(self, output: &mut Vec<RangeMeta>) {
        if self.can_split_preserve_order() {
            let num_row_groups = self.indices[0].num_row_groups;
            debug_assert_eq!(1, self.row_group_indices.len());
            debug_assert_eq!(ALL_ROW_GROUPS, self.row_group_indices[0].row_group_index);

            output.reserve(self.row_group_indices.len());
            let num_rows = self.num_rows / num_row_groups as usize;
            // Splits by row group.
            for row_group_index in 0..num_row_groups {
                output.push(RangeMeta {
                    time_range: self.time_range,
                    indices: self.indices.clone(),
                    row_group_indices: smallvec![RowGroupIndex {
                        index: self.indices[0].index,
                        row_group_index: row_group_index as i64,
                    }],
                    num_rows,
                });
            }
        } else {
            output.push(self);
        }
    }

    fn push_unordered_mem_ranges(memtables: &[MemRangeBuilder], ranges: &mut Vec<RangeMeta>) {
        // For append mode, we can parallelize reading memtables.
        for (memtable_index, memtable) in memtables.iter().enumerate() {
            let stats = memtable.stats();
            let Some(time_range) = stats.time_range() else {
                continue;
            };
            for row_group_index in 0..stats.num_ranges() {
                let num_rows = stats.num_rows() / stats.num_ranges();
                ranges.push(RangeMeta {
                    time_range,
                    indices: smallvec![SourceIndex {
                        index: memtable_index,
                        num_row_groups: stats.num_ranges() as u64,
                    }],
                    row_group_indices: smallvec![RowGroupIndex {
                        index: memtable_index,
                        row_group_index: row_group_index as i64,
                    }],
                    num_rows,
                });
            }
        }
    }

    async fn push_unordered_file_ranges(
        num_memtables: usize,
        files: &[FileHandle],
        cache: &CacheStrategy,
        ranges: &mut Vec<RangeMeta>,
    ) {
        // For append mode, we can parallelize reading row groups.
        for (i, file) in files.iter().enumerate() {
            let file_index = num_memtables + i;
            // Get parquet meta from the cache.
            let parquet_meta = cache
                .get_parquet_meta_data_from_mem_cache(file.file_id())
                .await;
            if let Some(parquet_meta) = parquet_meta {
                // Scans each row group.
                for row_group_index in 0..file.meta_ref().num_row_groups {
                    let time_range = parquet_row_group_time_range(
                        file.meta_ref(),
                        &parquet_meta,
                        row_group_index as usize,
                    );
                    let num_rows = parquet_meta.row_group(row_group_index as usize).num_rows();
                    ranges.push(RangeMeta {
                        time_range: time_range.unwrap_or_else(|| file.time_range()),
                        indices: smallvec![SourceIndex {
                            index: file_index,
                            num_row_groups: file.meta_ref().num_row_groups,
                        }],
                        row_group_indices: smallvec![RowGroupIndex {
                            index: file_index,
                            row_group_index: row_group_index as i64,
                        }],
                        num_rows: num_rows as usize,
                    });
                }
            } else if file.meta_ref().num_row_groups > 0 {
                // Scans each row group.
                for row_group_index in 0..file.meta_ref().num_row_groups {
                    ranges.push(RangeMeta {
                        time_range: file.time_range(),
                        indices: smallvec![SourceIndex {
                            index: file_index,
                            num_row_groups: file.meta_ref().num_row_groups,
                        }],
                        row_group_indices: smallvec![RowGroupIndex {
                            index: file_index,
                            row_group_index: row_group_index as i64,
                        }],
                        num_rows: DEFAULT_ROW_GROUP_SIZE,
                    });
                }
            } else {
                // If we don't known the number of row groups in advance, scan all row groups.
                ranges.push(RangeMeta {
                    time_range: file.time_range(),
                    indices: smallvec![SourceIndex {
                        index: file_index,
                        num_row_groups: 0,
                    }],
                    row_group_indices: smallvec![RowGroupIndex {
                        index: file_index,
                        row_group_index: ALL_ROW_GROUPS,
                    }],
                    // This may be 0.
                    num_rows: file.meta_ref().num_rows as usize,
                });
            }
        }
    }

    fn push_seq_mem_ranges(memtables: &[MemRangeBuilder], ranges: &mut Vec<RangeMeta>) {
        // For non append-only mode, each range only contains one memtable by default.
        for (i, memtable) in memtables.iter().enumerate() {
            let stats = memtable.stats();
            let Some(time_range) = stats.time_range() else {
                continue;
            };
            ranges.push(RangeMeta {
                time_range,
                indices: smallvec![SourceIndex {
                    index: i,
                    num_row_groups: stats.num_ranges() as u64,
                }],
                row_group_indices: smallvec![RowGroupIndex {
                    index: i,
                    row_group_index: ALL_ROW_GROUPS,
                }],
                num_rows: stats.num_rows(),
            });
        }
    }

    fn push_seq_file_ranges(
        num_memtables: usize,
        files: &[FileHandle],
        ranges: &mut Vec<RangeMeta>,
    ) {
        // For non append-only mode, each range only contains one file.
        for (i, file) in files.iter().enumerate() {
            let file_index = num_memtables + i;
            ranges.push(RangeMeta {
                time_range: file.time_range(),
                indices: smallvec![SourceIndex {
                    index: file_index,
                    num_row_groups: file.meta_ref().num_row_groups,
                }],
                row_group_indices: smallvec![RowGroupIndex {
                    index: file_index,
                    row_group_index: ALL_ROW_GROUPS,
                }],
                num_rows: file.meta_ref().num_rows as usize,
            });
        }
    }

    #[cfg(feature = "enterprise")]
    fn push_extension_ranges(input: &ScanInput, metas: &mut Vec<RangeMeta>) {
        for (i, range) in input.extension_ranges().iter().enumerate() {
            let index = input.num_memtables() + input.num_files() + i;
            metas.push(RangeMeta {
                time_range: range.time_range(),
                indices: smallvec![SourceIndex {
                    index,
                    num_row_groups: range.num_row_groups(),
                }],
                row_group_indices: smallvec![RowGroupIndex {
                    index,
                    row_group_index: ALL_ROW_GROUPS,
                }],
                num_rows: range.num_rows() as usize,
            });
        }
    }
}

/// Groups ranges so that all versions of a (primary key, timestamp) pair stay
/// together for deduplication. Ranges with disjoint time or primary key ranges
/// can be scanned independently. An unknown primary key range can match any key.
/// It assumes each input range only contains a file or a memtable.
fn group_ranges_for_seq_scan(
    mut ranges: Vec<(RangeMeta, Option<(Bytes, Bytes)>)>,
) -> Vec<RangeMeta> {
    // Sorts ranges by time range (start asc, end desc).
    ranges.sort_unstable_by(|(a, _), (b, _)| compare_time_ranges(a, b));
    let mut groups = Vec::with_capacity(ranges.len());
    // Groups with known keys, by min key to (max key, group). Their key ranges are disjoint.
    // A group that ended before the current range starts is closed when a lookup reaches it.
    let mut keyed: BTreeMap<Bytes, (Bytes, RangeMeta)> = BTreeMap::new();
    // A group with unknown keys. While it is open, `keyed` is empty.
    let mut unknown: Option<RangeMeta> = None;
    for (range, primary_keys) in ranges {
        // Later ranges start no earlier than this one, so groups ending before it are complete.
        let start = range.time_range.0;
        let mut current = range;
        let mut current_keys = primary_keys;
        if let Some(group) = unknown.take() {
            if group.time_range.1 < start {
                groups.push(group);
            } else {
                current = merge_ranges(group, current);
                current_keys = None;
            }
        }

        // The key ranges are disjoint, so their upper bounds have the same order as
        // their lower bounds. Walking backward, stop at the first range ending below `min`.
        let candidates: Vec<_> = match &current_keys {
            Some((min, max)) => keyed
                .range(..=max.clone())
                .rev()
                .take_while(|(_, (group_max, _))| group_max >= min)
                .map(|(group_min, _)| group_min.clone())
                .collect(),
            None => keyed.keys().cloned().collect(),
        };
        for group_min in candidates {
            let (group_max, group) = keyed.remove(&group_min).unwrap();
            if group.time_range.1 < start {
                groups.push(group);
            } else {
                current = merge_ranges(group, current);
                current_keys = merge_primary_key_ranges(current_keys, Some((group_min, group_max)));
            }
        }

        match current_keys {
            Some((min, max)) => {
                keyed.insert(min, (max, current));
            }
            None => {
                // Unknown keys overlap every group, so the lookup above emptied `keyed`.
                debug_assert!(keyed.is_empty());
                unknown = Some(current);
            }
        }
    }
    groups.extend(unknown);
    groups.extend(keyed.into_values().map(|(_, group)| group));
    groups.sort_unstable_by(compare_time_ranges);
    groups
}

/// Appends the smaller range to the larger one, so each index is copied O(log n) times over
/// all merges of a scan.
fn merge_ranges(a: RangeMeta, b: RangeMeta) -> RangeMeta {
    let (mut larger, smaller) = if a.row_group_indices.len() >= b.row_group_indices.len() {
        (a, b)
    } else {
        (b, a)
    };
    larger.merge(smaller);
    larger
}

fn compare_time_ranges(a: &RangeMeta, b: &RangeMeta) -> std::cmp::Ordering {
    let (l, r) = (a.time_range, b.time_range);
    l.0.cmp(&r.0).then_with(|| r.1.cmp(&l.1))
}

/// Splits the range into multiple smaller ranges.
/// It assumes the input `ranges` list is created by [group_ranges_for_seq_scan()].
fn maybe_split_ranges_for_seq_scan(ranges: Vec<RangeMeta>) -> Vec<RangeMeta> {
    let mut new_ranges = Vec::with_capacity(ranges.len());
    for range in ranges {
        range.maybe_split(&mut new_ranges);
    }

    new_ranges
}

/// Builder to create file ranges.
#[derive(Default)]
pub struct FileRangeBuilder {
    /// Context for the file.
    /// None indicates nothing to read.
    context: Option<FileRangeContextRef>,
    /// Row group selection for the file to read.
    selection: RowGroupSelection,
}

impl FileRangeBuilder {
    /// Builds a file range builder from context and row groups.
    pub fn new(context: FileRangeContextRef, selection: RowGroupSelection) -> Self {
        Self {
            context: Some(context),
            selection,
        }
    }

    /// Builds file ranges to read.
    /// Negative `row_group_index` indicates all row groups.
    pub fn build_ranges(&self, row_group_index: i64, ranges: &mut SmallVec<[FileRange; 2]>) {
        let Some(context) = self.context.clone() else {
            return;
        };
        if row_group_index >= 0 {
            let row_group_index = row_group_index as usize;
            // Scans one row group.
            let Some(row_selection) = self.selection.get(row_group_index) else {
                return;
            };
            ranges.push(FileRange::new(
                context,
                row_group_index,
                Some(row_selection.clone()),
            ));
        } else {
            // Scans all row groups.
            ranges.extend(
                self.selection
                    .iter()
                    .map(|(row_group_index, row_selection)| {
                        FileRange::new(
                            context.clone(),
                            *row_group_index,
                            Some(row_selection.clone()),
                        )
                    }),
            );
        }
    }

    /// Returns the estimated memory size of this builder.
    pub(crate) fn memory_size(&self) -> usize {
        let context_size = self
            .context
            .as_ref()
            .map(|ctx| ctx.memory_size())
            .unwrap_or(0);
        let selection_size = self.selection.mem_usage();
        context_size + selection_size
    }
}

/// Builder to create mem ranges.
#[derive(Clone)]
pub(crate) struct MemRangeBuilder {
    /// Ranges of a memtable.
    range: MemtableRange,
    /// Stats of a memtable.
    stats: MemtableStats,
}

impl MemRangeBuilder {
    /// Builds a mem range builder from row groups.
    pub(crate) fn new(range: MemtableRange, stats: MemtableStats) -> Self {
        Self { range, stats }
    }

    /// Builds mem ranges to read in the memtable.
    /// Negative `row_group_index` indicates all row groups.
    pub(crate) fn build_ranges(
        &self,
        _row_group_index: i64,
        ranges: &mut SmallVec<[MemtableRange; 2]>,
    ) {
        ranges.push(self.range.clone())
    }

    /// Returns the statistics of the memtable.
    pub(crate) fn stats(&self) -> &MemtableStats {
        &self.stats
    }
}

#[cfg(test)]
mod tests {
    use common_time::Timestamp;
    use common_time::timestamp::TimeUnit;

    use super::*;

    type Output = (Vec<usize>, i64, i64);

    fn run_group_ranges_test(input: &[(usize, i64, i64)], expect: &[Output]) {
        let ranges = input
            .iter()
            .map(|(idx, start, end)| {
                let time_range = (
                    Timestamp::new(*start, TimeUnit::Second),
                    Timestamp::new(*end, TimeUnit::Second),
                );
                let range = RangeMeta {
                    time_range,
                    indices: smallvec![SourceIndex {
                        index: *idx,
                        num_row_groups: 0,
                    }],
                    row_group_indices: smallvec![RowGroupIndex {
                        index: *idx,
                        row_group_index: 0
                    }],
                    num_rows: 1,
                };
                (range, None)
            })
            .collect();
        let output = group_ranges_for_seq_scan(ranges);
        let actual: Vec<_> = output
            .iter()
            .map(|range| {
                let indices = range.indices.iter().map(|index| index.index).collect();
                let group_indices: Vec<_> = range
                    .row_group_indices
                    .iter()
                    .map(|idx| idx.index)
                    .collect();
                assert_eq!(indices, group_indices);
                let range = range.time_range;
                (indices, range.0.value(), range.1.value())
            })
            .collect();
        assert_eq!(expect, actual);
    }

    #[test]
    fn test_group_ranges() {
        // Group 1 part.
        run_group_ranges_test(&[(1, 0, 2000)], &[(vec![1], 0, 2000)]);

        // 1, 2, 3, 4 => [3, 1, 4], [2]
        run_group_ranges_test(
            &[
                (1, 1000, 2000),
                (2, 6000, 7000),
                (3, 0, 1500),
                (4, 1500, 3000),
            ],
            &[(vec![3, 1, 4], 0, 3000), (vec![2], 6000, 7000)],
        );

        // 1, 2, 3 => [3], [1], [2],
        run_group_ranges_test(
            &[(1, 3000, 4000), (2, 4001, 6000), (3, 0, 1000)],
            &[
                (vec![3], 0, 1000),
                (vec![1], 3000, 4000),
                (vec![2], 4001, 6000),
            ],
        );

        // 1, 2, 3 => [3], [1, 2]
        run_group_ranges_test(
            &[(1, 3000, 4000), (2, 4000, 6000), (3, 0, 1000)],
            &[(vec![3], 0, 1000), (vec![1, 2], 3000, 6000)],
        );
    }

    /// `(index, start, end, primary key range)` of a range.
    type KeyedRange = (usize, i64, i64, Option<(&'static str, &'static str)>);

    /// Groups the ranges and returns the sorted indices of each group, in sorted order.
    fn group_with_keys(input: &[KeyedRange]) -> Vec<Vec<usize>> {
        let ranges = input
            .iter()
            .map(|(idx, start, end, keys)| {
                let range = RangeMeta {
                    time_range: (Timestamp::new_second(*start), Timestamp::new_second(*end)),
                    indices: smallvec![SourceIndex {
                        index: *idx,
                        num_row_groups: 0,
                    }],
                    row_group_indices: smallvec![RowGroupIndex {
                        index: *idx,
                        row_group_index: 0
                    }],
                    num_rows: 1,
                };
                let keys = keys.map(|(min, max)| {
                    (
                        Bytes::copy_from_slice(min.as_bytes()),
                        Bytes::copy_from_slice(max.as_bytes()),
                    )
                });
                (range, keys)
            })
            .collect();
        let mut groups: Vec<_> = group_ranges_for_seq_scan(ranges)
            .iter()
            .map(|range| {
                let mut indices: Vec<_> = range.indices.iter().map(|i| i.index).collect();
                indices.sort_unstable();
                indices
            })
            .collect();
        groups.sort_unstable();
        groups
    }

    #[test]
    fn test_group_ranges_by_primary_keys() {
        // Outputs of one compaction cover the same time range with disjoint keys.
        let outputs = [
            (1, 0, 100, Some(("a", "b"))),
            (2, 0, 100, Some(("c", "d"))),
            (3, 0, 100, Some(("e", "f"))),
        ];
        assert_eq!(vec![vec![1], vec![2], vec![3]], group_with_keys(&outputs));

        // A range whose keys span two groups joins them; ranges with shared boundary keys overlap.
        let mut input = outputs.to_vec();
        input.push((4, 50, 150, Some(("b", "c"))));
        assert_eq!(vec![vec![1, 2, 4], vec![3]], group_with_keys(&input));

        // Unknown keys may overlap anything in the same time range.
        let mut input = outputs.to_vec();
        input.push((4, 90, 200, None));
        assert_eq!(vec![vec![1, 2, 3, 4]], group_with_keys(&input));

        // Ranges sharing keys still chain through time; a range with other keys stays apart.
        let input = [
            (1, 0, 10, Some(("a", "a"))),
            (2, 5, 20, Some(("a", "b"))),
            (3, 15, 30, Some(("b", "b"))),
            (4, 15, 30, Some(("c", "c"))),
        ];
        assert_eq!(vec![vec![1, 2, 3], vec![4]], group_with_keys(&input));

        // A group that ended before a range starts stays apart even if their keys overlap.
        let input = [
            (1, 0, 10, Some(("a", "b"))),
            (2, 20, 30, Some(("a", "b"))),
            (3, 20, 30, Some(("c", "c"))),
        ];
        assert_eq!(vec![vec![1], vec![2], vec![3]], group_with_keys(&input));

        // An open group with unknown keys takes every range it overlaps in time.
        let input = [
            (1, 0, 50, None),
            (2, 10, 20, Some(("a", "a"))),
            (3, 40, 60, Some(("b", "b"))),
            (4, 70, 80, Some(("a", "a"))),
            (5, 70, 80, Some(("b", "b"))),
        ];
        assert_eq!(
            vec![vec![1, 2, 3], vec![4], vec![5]],
            group_with_keys(&input)
        );
    }

    #[test]
    fn test_group_ranges_never_split_overlapping_ranges() {
        use rand::rngs::StdRng;
        use rand::{Rng, SeedableRng};

        const KEYS: [&str; 8] = ["a", "b", "c", "d", "e", "f", "g", "h"];
        let mut rng = StdRng::seed_from_u64(42);
        for _ in 0..200 {
            let input: Vec<KeyedRange> = (0..rng.random_range(1..40))
                .map(|idx| {
                    let start = rng.random_range(0..100);
                    let end = start + rng.random_range(0..20);
                    let keys = rng.random_bool(0.9).then(|| {
                        let lo = rng.random_range(0..KEYS.len());
                        let hi = rng.random_range(lo..KEYS.len());
                        (KEYS[lo], KEYS[hi])
                    });
                    (idx, start, end, keys)
                })
                .collect();
            let groups = group_with_keys(&input);
            let group_of = |idx: usize| groups.iter().position(|g| g.contains(&idx)).unwrap();
            assert_eq!(input.len(), groups.iter().map(Vec::len).sum::<usize>());
            for (i, (_, a_start, a_end, a_keys)) in input.iter().enumerate() {
                for (j, (_, b_start, b_end, b_keys)) in input.iter().enumerate().skip(i + 1) {
                    let times = a_start.max(b_start) <= a_end.min(b_end);
                    let keys = match (a_keys, b_keys) {
                        (Some(a), Some(b)) => a.0.max(b.0) <= a.1.min(b.1),
                        _ => true,
                    };
                    if times && keys {
                        assert_eq!(group_of(i), group_of(j), "{input:?}");
                    }
                }
            }
        }
    }

    #[test]
    fn test_merge_range() {
        let mut left = RangeMeta {
            time_range: (Timestamp::new_second(1000), Timestamp::new_second(2000)),
            indices: smallvec![SourceIndex {
                index: 1,
                num_row_groups: 2,
            }],
            row_group_indices: smallvec![
                RowGroupIndex {
                    index: 1,
                    row_group_index: 1
                },
                RowGroupIndex {
                    index: 1,
                    row_group_index: 2
                }
            ],
            num_rows: 5,
        };
        let right = RangeMeta {
            time_range: (Timestamp::new_second(800), Timestamp::new_second(1200)),
            indices: smallvec![SourceIndex {
                index: 2,
                num_row_groups: 2,
            }],
            row_group_indices: smallvec![
                RowGroupIndex {
                    index: 2,
                    row_group_index: 1
                },
                RowGroupIndex {
                    index: 2,
                    row_group_index: 2
                }
            ],
            num_rows: 4,
        };
        left.merge(right);

        assert_eq!(
            left,
            RangeMeta {
                time_range: (Timestamp::new_second(800), Timestamp::new_second(2000)),
                indices: smallvec![
                    SourceIndex {
                        index: 1,
                        num_row_groups: 2
                    },
                    SourceIndex {
                        index: 2,
                        num_row_groups: 2
                    }
                ],
                row_group_indices: smallvec![
                    RowGroupIndex {
                        index: 1,
                        row_group_index: 1
                    },
                    RowGroupIndex {
                        index: 1,
                        row_group_index: 2
                    },
                    RowGroupIndex {
                        index: 2,
                        row_group_index: 1
                    },
                    RowGroupIndex {
                        index: 2,
                        row_group_index: 2
                    },
                ],
                num_rows: 9,
            }
        );
    }

    #[test]
    fn test_split_range() {
        let range = RangeMeta {
            time_range: (Timestamp::new_second(1000), Timestamp::new_second(2000)),
            indices: smallvec![SourceIndex {
                index: 1,
                num_row_groups: 2,
            }],
            row_group_indices: smallvec![RowGroupIndex {
                index: 1,
                row_group_index: ALL_ROW_GROUPS,
            }],
            num_rows: 5,
        };

        assert!(range.can_split_preserve_order());
        let mut output = Vec::new();
        range.maybe_split(&mut output);

        assert_eq!(
            output,
            &[
                RangeMeta {
                    time_range: (Timestamp::new_second(1000), Timestamp::new_second(2000)),
                    indices: smallvec![SourceIndex {
                        index: 1,
                        num_row_groups: 2,
                    }],
                    row_group_indices: smallvec![RowGroupIndex {
                        index: 1,
                        row_group_index: 0
                    },],
                    num_rows: 2,
                },
                RangeMeta {
                    time_range: (Timestamp::new_second(1000), Timestamp::new_second(2000)),
                    indices: smallvec![SourceIndex {
                        index: 1,
                        num_row_groups: 2,
                    }],
                    row_group_indices: smallvec![RowGroupIndex {
                        index: 1,
                        row_group_index: 1
                    }],
                    num_rows: 2,
                }
            ]
        );
    }

    #[test]
    fn test_not_split_range() {
        let range = RangeMeta {
            time_range: (Timestamp::new_second(1000), Timestamp::new_second(2000)),
            indices: smallvec![
                SourceIndex {
                    index: 1,
                    num_row_groups: 1,
                },
                SourceIndex {
                    index: 2,
                    num_row_groups: 1,
                }
            ],
            row_group_indices: smallvec![
                RowGroupIndex {
                    index: 1,
                    row_group_index: 1
                },
                RowGroupIndex {
                    index: 2,
                    row_group_index: 1
                }
            ],
            num_rows: 5,
        };

        assert!(!range.can_split_preserve_order());
        let mut output = Vec::new();
        range.maybe_split(&mut output);
        assert_eq!(1, output.len());
    }

    #[test]
    fn test_maybe_split_ranges() {
        let ranges = vec![
            RangeMeta {
                time_range: (Timestamp::new_second(0), Timestamp::new_second(500)),
                indices: smallvec![SourceIndex {
                    index: 0,
                    num_row_groups: 1,
                }],
                row_group_indices: smallvec![RowGroupIndex {
                    index: 0,
                    row_group_index: 0,
                },],
                num_rows: 4,
            },
            RangeMeta {
                time_range: (Timestamp::new_second(1000), Timestamp::new_second(2000)),
                indices: smallvec![SourceIndex {
                    index: 1,
                    num_row_groups: 2,
                }],
                row_group_indices: smallvec![RowGroupIndex {
                    index: 1,
                    row_group_index: ALL_ROW_GROUPS,
                },],
                num_rows: 4,
            },
            RangeMeta {
                time_range: (Timestamp::new_second(3000), Timestamp::new_second(4000)),
                indices: smallvec![
                    SourceIndex {
                        index: 2,
                        num_row_groups: 2,
                    },
                    SourceIndex {
                        index: 3,
                        num_row_groups: 0,
                    }
                ],
                row_group_indices: smallvec![
                    RowGroupIndex {
                        index: 2,
                        row_group_index: ALL_ROW_GROUPS,
                    },
                    RowGroupIndex {
                        index: 3,
                        row_group_index: ALL_ROW_GROUPS,
                    }
                ],
                num_rows: 5,
            },
        ];
        let output = maybe_split_ranges_for_seq_scan(ranges);
        assert_eq!(
            output,
            vec![
                RangeMeta {
                    time_range: (Timestamp::new_second(0), Timestamp::new_second(500)),
                    indices: smallvec![SourceIndex {
                        index: 0,
                        num_row_groups: 1,
                    }],
                    row_group_indices: smallvec![RowGroupIndex {
                        index: 0,
                        row_group_index: 0
                    },],
                    num_rows: 4,
                },
                RangeMeta {
                    time_range: (Timestamp::new_second(1000), Timestamp::new_second(2000)),
                    indices: smallvec![SourceIndex {
                        index: 1,
                        num_row_groups: 2,
                    }],
                    row_group_indices: smallvec![RowGroupIndex {
                        index: 1,
                        row_group_index: 0
                    },],
                    num_rows: 2,
                },
                RangeMeta {
                    time_range: (Timestamp::new_second(1000), Timestamp::new_second(2000)),
                    indices: smallvec![SourceIndex {
                        index: 1,
                        num_row_groups: 2,
                    }],
                    row_group_indices: smallvec![RowGroupIndex {
                        index: 1,
                        row_group_index: 1
                    }],
                    num_rows: 2,
                },
                RangeMeta {
                    time_range: (Timestamp::new_second(3000), Timestamp::new_second(4000)),
                    indices: smallvec![
                        SourceIndex {
                            index: 2,
                            num_row_groups: 2
                        },
                        SourceIndex {
                            index: 3,
                            num_row_groups: 0,
                        }
                    ],
                    row_group_indices: smallvec![
                        RowGroupIndex {
                            index: 2,
                            row_group_index: ALL_ROW_GROUPS,
                        },
                        RowGroupIndex {
                            index: 3,
                            row_group_index: ALL_ROW_GROUPS,
                        }
                    ],
                    num_rows: 5,
                },
            ]
        )
    }
}
