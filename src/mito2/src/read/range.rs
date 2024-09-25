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

use smallvec::{smallvec, SmallVec};

use crate::memtable::MemtableRef;
use crate::read::scan_region::ScanInput;
use crate::sst::file::{overlaps, FileHandle, FileTimeRange};
use crate::sst::parquet::DEFAULT_ROW_GROUP_SIZE;

const ALL_ROW_GROUPS: i64 = -1;

/// Index to access a row group.
#[derive(Clone, Copy)]
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
pub(crate) struct RangeMeta {
    /// The time range of the range.
    pub(crate) time_range: FileTimeRange,
    /// Indices to memtables or files.
    indices: SmallVec<[usize; 2]>,
    /// Indices to memtable/file row groups that this range scans.
    pub(crate) row_group_indices: SmallVec<[RowGroupIndex; 2]>,
    /// Estimated number of rows in the range. This can be 0 if the statistics are not available.
    pub(crate) num_rows: usize,
}

impl RangeMeta {
    /// Creates a list of ranges from the `input` for seq scan.
    pub(crate) fn seq_scan_ranges(input: &ScanInput) -> Vec<RangeMeta> {
        let mut ranges = Vec::with_capacity(input.memtables.len() + input.files.len());
        Self::push_seq_mem_ranges(&input.memtables, &mut ranges);
        Self::push_seq_file_ranges(input.memtables.len(), &input.files, &mut ranges);

        let ranges = group_ranges_for_seq_scan(ranges);
        maybe_split_ranges_for_seq_scan(ranges)
    }

    /// Creates a list of ranges from the `input` for unordered scan.
    pub(crate) fn unordered_scan_ranges(input: &ScanInput) -> Vec<RangeMeta> {
        let mut ranges = Vec::with_capacity(input.memtables.len() + input.files.len());
        Self::push_unordered_mem_ranges(&input.memtables, &mut ranges);
        Self::push_unordered_file_ranges(input.memtables.len(), &input.files, &mut ranges);

        ranges
    }

    /// Returns true if the time range of given `meta` overlaps with the time range of this meta.
    pub(crate) fn overlaps(&self, meta: &RangeMeta) -> bool {
        overlaps(&self.time_range, &meta.time_range)
    }

    /// Merges given `meta` to this meta.
    /// It assumes that the time ranges overlap and they don't have the same file or memtable index.
    pub(crate) fn merge(&mut self, mut other: RangeMeta) {
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
    pub(crate) fn can_split_preserve_order(&self) -> bool {
        // Only one source and multiple row groups.
        self.indices.len() == 1 && self.row_group_indices.len() > 1
    }

    /// Splits the range if it can preserve the order.
    pub(crate) fn maybe_split(self, output: &mut Vec<RangeMeta>) {
        if self.can_split_preserve_order() {
            output.reserve(self.row_group_indices.len());
            let num_rows = self.num_rows / self.row_group_indices.len();
            // Splits by row group.
            for index in self.row_group_indices {
                output.push(RangeMeta {
                    time_range: self.time_range,
                    indices: self.indices.clone(),
                    row_group_indices: smallvec![index],
                    num_rows,
                });
            }
        } else {
            output.push(self);
        }
    }

    fn push_unordered_mem_ranges(memtables: &[MemtableRef], ranges: &mut Vec<RangeMeta>) {
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
                    indices: smallvec![memtable_index],
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
        ranges: &mut Vec<RangeMeta>,
    ) {
        // For append mode, we can parallelize reading row groups.
        for (i, file) in files.iter().enumerate() {
            let file_index = num_memtables + i;
            if file.meta_ref().num_row_groups > 0 {
                // Scans each row group.
                for row_group_index in 0..file.meta_ref().num_row_groups {
                    ranges.push(RangeMeta {
                        time_range: file.time_range(),
                        indices: smallvec![file_index],
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
                    indices: smallvec![file_index],
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

    fn push_seq_mem_ranges(memtables: &[MemtableRef], ranges: &mut Vec<RangeMeta>) {
        // For non append-only mode, each range only contains one memtable.
        for (i, memtable) in memtables.iter().enumerate() {
            let stats = memtable.stats();
            let Some(time_range) = stats.time_range() else {
                continue;
            };
            ranges.push(RangeMeta {
                time_range,
                indices: smallvec![i],
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
                indices: smallvec![file_index],
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
