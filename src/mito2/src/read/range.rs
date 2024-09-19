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

use smallvec::SmallVec;

use crate::sst::file::{overlaps, FileTimeRange};

/// Index to access a row group.
#[derive(Clone)]
struct RowGroupIndex {
    /// Index to the file.
    file_index: usize,
    /// Row group index in the file.
    row_group_index: usize,
}

/// Meta data of a partition range.
/// If the scanner is [UnorderedScan], each meta only has one row group or memtable.
/// If the scanner is [SeqScan], each meta may have multiple row groups and memtables.
pub(crate) struct RangeMeta {
    /// The time range of the range.
    pub(crate) time_range: FileTimeRange,
    /// Indices to memtables.
    mem_indices: SmallVec<[usize; 2]>,
    /// Indices to files. It contains the indices to all the files in `row_group_indices`.
    file_indices: SmallVec<[usize; 4]>,
    /// Indices to file row groups that this range scans.
    /// An empty vec indicates all row groups. (Some file metas don't have row group number).
    row_group_indices: SmallVec<[RowGroupIndex; 2]>,
}

impl RangeMeta {
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
        self.mem_indices.append(&mut other.mem_indices);
        self.file_indices.append(&mut other.file_indices);
        self.row_group_indices.append(&mut other.row_group_indices);
    }

    /// Returns true if we can split the range into multiple smaller ranges and
    /// still preserve the order for [SeqScan].
    pub(crate) fn can_split_preserve_order(&self) -> bool {
        // No memtable, only one file, and multiple row groups.
        self.mem_indices.is_empty()
            && self.file_indices.len() == 1
            && self.row_group_indices.len() > 1
    }

    /// Splits the range if it can preserve the order.
    pub(crate) fn maybe_split(self, output: &mut Vec<RangeMeta>) {
        if self.can_split_preserve_order() {
            output.reserve(self.row_group_indices.len());
            for index in self.row_group_indices {
                output.push(RangeMeta {
                    time_range: self.time_range,
                    mem_indices: SmallVec::new(),
                    file_indices: self.file_indices.clone(),
                    row_group_indices: SmallVec::from_elem(index, 1),
                });
            }
        } else {
            output.push(self);
        }
    }
}

/// Groups ranges by time range.
/// It assumes each input range only contains a file or a memtable.
fn group_ranges_for_merge_mode(mut ranges: Vec<RangeMeta>) -> Vec<RangeMeta> {
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
/// It assumes the input `ranges` list is created by [group_ranges_for_merge_mode()].
fn maybe_split_ranges_for_merge_mode(ranges: Vec<RangeMeta>) -> Vec<RangeMeta> {
    let mut new_ranges = Vec::with_capacity(ranges.len());
    for range in ranges {
        range.maybe_split(&mut new_ranges);
    }

    new_ranges
}
