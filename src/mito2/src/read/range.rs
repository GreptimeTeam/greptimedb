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

use std::sync::{Arc, Mutex};

use common_time::Timestamp;
use smallvec::{smallvec, SmallVec};
use store_api::region_engine::PartitionRange;
use store_api::storage::TimeSeriesDistribution;

use crate::cache::CacheStrategy;
use crate::error::Result;
use crate::memtable::{MemtableRange, MemtableRanges, MemtableStats};
use crate::read::scan_region::ScanInput;
use crate::sst::file::{overlaps, FileHandle, FileTimeRange};
use crate::sst::parquet::file_range::{FileRange, FileRangeContextRef};
use crate::sst::parquet::format::parquet_row_group_time_range;
use crate::sst::parquet::reader::ReaderMetrics;
use crate::sst::parquet::row_selection::RowGroupSelection;
use crate::sst::parquet::DEFAULT_ROW_GROUP_SIZE;

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
pub(crate) struct RowGroupIndex {
    /// Index to the memtable/file.
    pub(crate) index: usize,
    /// Row group index in the file.
    /// Negative index indicates all row groups.
    pub(crate) row_group_index: i64,
}

/// Meta data of a partition range.
/// If the scanner is [UnorderedScan], each meta only has one row group or memtable.
/// If the scanner is [SeqScan], each meta may have multiple row groups and memtables.
#[derive(Debug, PartialEq)]
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
    /// If `compaction` is true, it doesn't split the ranges.
    pub(crate) fn seq_scan_ranges(input: &ScanInput, compaction: bool) -> Vec<RangeMeta> {
        let mut ranges = Vec::with_capacity(input.memtables.len() + input.files.len());
        Self::push_seq_mem_ranges(&input.memtables, &mut ranges);
        Self::push_seq_file_ranges(input.memtables.len(), &input.files, &mut ranges);

        let ranges = group_ranges_for_seq_scan(ranges);
        if compaction || input.distribution == Some(TimeSeriesDistribution::PerSeries) {
            // We don't split ranges in compaction or TimeSeriesDistribution::PerSeries.
            return ranges;
        }
        maybe_split_ranges_for_seq_scan(ranges)
    }

    /// Creates a list of ranges from the `input` for unordered scan.
    pub(crate) fn unordered_scan_ranges(input: &ScanInput) -> Vec<RangeMeta> {
        let mut ranges = Vec::with_capacity(input.memtables.len() + input.files.len());
        Self::push_unordered_mem_ranges(&input.memtables, &mut ranges);
        Self::push_unordered_file_ranges(
            input.memtables.len(),
            &input.files,
            &input.cache_strategy,
            &mut ranges,
        );

        ranges
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
        debug_assert!(self
            .row_group_indices
            .iter()
            .all(|idx| !other.row_group_indices.contains(idx)));

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

    fn push_unordered_file_ranges(
        num_memtables: usize,
        files: &[FileHandle],
        cache: &CacheStrategy,
        ranges: &mut Vec<RangeMeta>,
    ) {
        // For append mode, we can parallelize reading row groups.
        for (i, file) in files.iter().enumerate() {
            let file_index = num_memtables + i;
            // Get parquet meta from the cache.
            let parquet_meta =
                cache.get_parquet_meta_data_from_mem_cache(file.region_id(), file.file_id());
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
}

/// Groups ranges by time range.
/// It assumes each input range only contains a file or a memtable.
fn group_ranges_for_seq_scan(mut ranges: Vec<RangeMeta>) -> Vec<RangeMeta> {
    if ranges.is_empty() {
        return ranges;
    }

    // Sorts ranges by time range (start asc, end desc).
    ranges.sort_unstable_by(|a, b| {
        let l = a.time_range;
        let r = b.time_range;
        l.0.cmp(&r.0).then_with(|| r.1.cmp(&l.1))
    });
    let mut range_in_progress = None;
    // Parts with exclusive time ranges.
    let mut exclusive_ranges = Vec::with_capacity(ranges.len());
    for range in ranges {
        let Some(mut prev_range) = range_in_progress.take() else {
            // This is the new range to process.
            range_in_progress = Some(range);
            continue;
        };

        if prev_range.overlaps(&range) {
            prev_range.merge(range);
            range_in_progress = Some(prev_range);
        } else {
            exclusive_ranges.push(prev_range);
            range_in_progress = Some(range);
        }
    }
    if let Some(range) = range_in_progress {
        exclusive_ranges.push(range);
    }

    exclusive_ranges
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
pub(crate) struct FileRangeBuilder {
    /// Context for the file.
    /// None indicates nothing to read.
    context: Option<FileRangeContextRef>,
    /// Row group selection for the file to read.
    selection: RowGroupSelection,
}

impl FileRangeBuilder {
    /// Builds a file range builder from context and row groups.
    pub(crate) fn new(context: FileRangeContextRef, selection: RowGroupSelection) -> Self {
        Self {
            context: Some(context),
            selection,
        }
    }

    /// Builds file ranges to read.
    /// Negative `row_group_index` indicates all row groups.
    pub(crate) fn build_ranges(&self, row_group_index: i64, ranges: &mut SmallVec<[FileRange; 2]>) {
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
}

/// Builder to create mem ranges.
pub(crate) struct MemRangeBuilder {
    /// Ranges of a memtable.
    ranges: MemtableRanges,
}

impl MemRangeBuilder {
    /// Builds a mem range builder from row groups.
    pub(crate) fn new(ranges: MemtableRanges) -> Self {
        Self { ranges }
    }

    /// Builds mem ranges to read in the memtable.
    /// Negative `row_group_index` indicates all row groups.
    pub(crate) fn build_ranges(
        &self,
        row_group_index: i64,
        ranges: &mut SmallVec<[MemtableRange; 2]>,
    ) {
        if row_group_index >= 0 {
            let row_group_index = row_group_index as usize;
            // Scans one row group.
            let Some(range) = self.ranges.ranges.get(&row_group_index) else {
                return;
            };
            ranges.push(range.clone());
        } else {
            ranges.extend(self.ranges.ranges.values().cloned());
        }
    }

    /// Returns the statistics of the memtable.
    pub(crate) fn stats(&self) -> &MemtableStats {
        &self.ranges.stats
    }
}

/// List to manages the builders to create file ranges.
/// Each scan partition should have its own list. Mutex inside this list is used to allow moving
/// the list to different streams in the same partition.
pub(crate) struct RangeBuilderList {
    num_memtables: usize,
    file_builders: Mutex<Vec<Option<Arc<FileRangeBuilder>>>>,
}

impl RangeBuilderList {
    /// Creates a new [ReaderBuilderList] with the given number of memtables and files.
    pub(crate) fn new(num_memtables: usize, num_files: usize) -> Self {
        let file_builders = (0..num_files).map(|_| None).collect();
        Self {
            num_memtables,
            file_builders: Mutex::new(file_builders),
        }
    }

    /// Builds file ranges to read the row group at `index`.
    pub(crate) async fn build_file_ranges(
        &self,
        input: &ScanInput,
        index: RowGroupIndex,
        reader_metrics: &mut ReaderMetrics,
    ) -> Result<SmallVec<[FileRange; 2]>> {
        let mut ranges = SmallVec::new();
        let file_index = index.index - self.num_memtables;
        let builder_opt = self.get_file_builder(file_index);
        match builder_opt {
            Some(builder) => builder.build_ranges(index.row_group_index, &mut ranges),
            None => {
                let builder = input.prune_file(file_index, reader_metrics).await?;
                builder.build_ranges(index.row_group_index, &mut ranges);
                self.set_file_builder(file_index, Arc::new(builder));
            }
        }
        Ok(ranges)
    }

    fn get_file_builder(&self, index: usize) -> Option<Arc<FileRangeBuilder>> {
        let file_builders = self.file_builders.lock().unwrap();
        file_builders[index].clone()
    }

    fn set_file_builder(&self, index: usize, builder: Arc<FileRangeBuilder>) {
        let mut file_builders = self.file_builders.lock().unwrap();
        file_builders[index] = Some(builder);
    }
}

#[cfg(test)]
mod tests {
    use common_time::timestamp::TimeUnit;
    use common_time::Timestamp;

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
                RangeMeta {
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
                }
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
