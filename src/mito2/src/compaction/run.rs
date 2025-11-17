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

//! This file contains code to find sorted runs in a set if ranged items and
//! along with the best way to merge these items to satisfy the desired run count.

use common_base::BitVec;
use common_base::readable_size::ReadableSize;
use common_time::Timestamp;
use smallvec::{SmallVec, smallvec};

use crate::sst::file::{FileHandle, RegionFileId};

/// Default max compaction output file size when not specified.
const DEFAULT_MAX_OUTPUT_SIZE: u64 = ReadableSize::gb(2).as_bytes();

/// Trait for any items with specific range (both boundaries are inclusive).
pub trait Ranged {
    type BoundType: Ord + Copy;

    /// Returns the inclusive range of item.
    fn range(&self) -> (Self::BoundType, Self::BoundType);

    fn overlap<T>(&self, other: &T) -> bool
    where
        T: Ranged<BoundType = Self::BoundType>,
    {
        let (lhs_start, lhs_end) = self.range();
        let (rhs_start, rhs_end) = other.range();

        lhs_start.max(rhs_start) < lhs_end.min(rhs_end)
    }
}

pub fn find_overlapping_items<T: Item + Clone>(
    l: &mut SortedRun<T>,
    r: &mut SortedRun<T>,
    result: &mut Vec<T>,
) {
    if l.items.is_empty() || r.items.is_empty() {
        return;
    }

    result.clear();
    result.reserve(l.items.len() + r.items.len());

    // Sort both arrays by start boundary for more efficient overlap detection
    if !l.sorted {
        sort_ranged_items(&mut l.items);
        l.sorted = true;
    }
    if !r.sorted {
        sort_ranged_items(&mut r.items);
        r.sorted = true;
    }

    let mut r_idx = 0;

    let mut selected = BitVec::repeat(false, r.items().len() + l.items.len());

    for (lhs_idx, lhs) in l.items.iter().enumerate() {
        let (lhs_start, lhs_end) = lhs.range();

        // Skip right elements that end before current left element starts
        while r_idx < r.items.len() {
            let (_, rhs_end) = r.items[r_idx].range();
            if rhs_end < lhs_start {
                r_idx += 1;
            } else {
                break;
            }
        }

        // Check for overlaps with remaining right elements
        let mut j = r_idx;
        while j < r.items.len() {
            let (rhs_start, rhs_end) = r.items[j].range();

            // If right element starts after left element ends, no more overlaps possible
            if rhs_start > lhs_end {
                break;
            }

            // We have an overlap
            if lhs_start.max(rhs_start) <= lhs_end.min(rhs_end) {
                if !selected[lhs_idx] {
                    result.push(lhs.clone());
                    selected.set(lhs_idx, true);
                }

                let rhs_selected_idx = l.items.len() + j;
                if !selected[rhs_selected_idx] {
                    result.push(r.items[j].clone());
                    selected.set(rhs_selected_idx, true);
                }
            }

            j += 1;
        }
    }
}

// Sorts ranges by start asc and end desc.
fn sort_ranged_items<T: Ranged>(values: &mut [T]) {
    values.sort_unstable_by(|l, r| {
        let (l_start, l_end) = l.range();
        let (r_start, r_end) = r.range();
        l_start.cmp(&r_start).then(r_end.cmp(&l_end))
    });
}

/// Trait for items to merge.
pub trait Item: Ranged + Clone {
    /// Size is used to calculate the cost of merging items.
    fn size(&self) -> usize;
}

/// A group of files that are created by the same compaction task.
#[derive(Debug, Clone)]
pub struct FileGroup {
    files: SmallVec<[FileHandle; 2]>,
    size: usize,
    num_rows: usize,
    min_timestamp: Timestamp,
    max_timestamp: Timestamp,
}

impl FileGroup {
    pub(crate) fn new_with_file(file: FileHandle) -> Self {
        let size = file.size() as usize;
        let (min_timestamp, max_timestamp) = file.time_range();
        let num_rows = file.num_rows();
        Self {
            files: smallvec![file],
            size,
            num_rows,
            min_timestamp,
            max_timestamp,
        }
    }

    pub(crate) fn num_rows(&self) -> usize {
        self.num_rows
    }

    pub(crate) fn add_file(&mut self, file: FileHandle) {
        self.size += file.size() as usize;
        self.num_rows += file.num_rows();
        let (min_timestamp, max_timestamp) = file.time_range();
        self.min_timestamp = self.min_timestamp.min(min_timestamp);
        self.max_timestamp = self.max_timestamp.max(max_timestamp);
        self.files.push(file);
    }

    pub(crate) fn num_files(&self) -> usize {
        self.files.len()
    }

    #[cfg(test)]
    pub(crate) fn files(&self) -> &[FileHandle] {
        &self.files[..]
    }

    pub(crate) fn file_ids(&self) -> SmallVec<[RegionFileId; 2]> {
        SmallVec::from_iter(self.files.iter().map(|f| f.file_id()))
    }

    pub(crate) fn into_files(self) -> impl Iterator<Item = FileHandle> {
        self.files.into_iter()
    }
}

impl Ranged for FileGroup {
    type BoundType = Timestamp;

    fn range(&self) -> (Self::BoundType, Self::BoundType) {
        (self.min_timestamp, self.max_timestamp)
    }
}

impl Item for FileGroup {
    fn size(&self) -> usize {
        self.size
    }
}

/// A set of files with non-overlapping time ranges.
#[derive(Debug, Clone)]
pub struct SortedRun<T: Item> {
    /// Items to merge
    items: Vec<T>,
    /// The total size of all items.
    size: usize,
    /// The lower bound of all items.
    start: Option<T::BoundType>,
    /// The upper bound of all items.
    end: Option<T::BoundType>,
    /// Whether items are sorted.
    sorted: bool,
}

impl<T: Item> From<Vec<T>> for SortedRun<T> {
    fn from(items: Vec<T>) -> Self {
        let mut r = Self {
            items: Vec::with_capacity(items.len()),
            size: 0,
            start: None,
            end: None,
            sorted: false,
        };
        for item in items {
            r.push_item(item);
        }

        r
    }
}

impl<T> Default for SortedRun<T>
where
    T: Item,
{
    fn default() -> Self {
        Self {
            items: vec![],
            size: 0,
            start: None,
            end: None,
            sorted: false,
        }
    }
}

impl<T> SortedRun<T>
where
    T: Item,
{
    pub fn items(&self) -> &[T] {
        &self.items
    }

    fn push_item(&mut self, t: T) {
        let (file_start, file_end) = t.range();
        self.size += t.size();
        self.items.push(t);
        self.start = Some(self.start.map_or(file_start, |v| v.min(file_start)));
        self.end = Some(self.end.map_or(file_end, |v| v.max(file_end)));
    }
}

/// Finds sorted runs in given items.
pub fn find_sorted_runs<T>(items: &mut [T]) -> Vec<SortedRun<T>>
where
    T: Item,
{
    if items.is_empty() {
        return vec![];
    }
    // sort files
    sort_ranged_items(items);

    let mut current_run = SortedRun::default();
    let mut runs = vec![];

    let mut selection = BitVec::repeat(false, items.len());
    while !selection.all() {
        // until all items are assigned to some sorted run.
        for (item, mut selected) in items.iter().zip(selection.iter_mut()) {
            if *selected {
                // item is already assigned.
                continue;
            }
            match current_run.items.last() {
                None => {
                    // current run is empty, just add current_item
                    selected.set(true);
                    current_run.push_item(item.clone());
                }
                Some(last) => {
                    // the current item does not overlap with the last item in current run,
                    // then it belongs to current run.
                    if !last.overlap(item) {
                        // does not overlap, push to current run
                        selected.set(true);
                        current_run.push_item(item.clone());
                    }
                }
            }
        }
        // finished an iteration, we've found a new run.
        runs.push(std::mem::take(&mut current_run));
    }
    runs
}

/// Finds a set of files with minimum penalty to merge that can reduce the total num of runs.
/// The penalty of merging is defined as the size of all overlapping files between two runs.
pub fn reduce_runs<T: Item>(mut runs: Vec<SortedRun<T>>) -> Vec<T> {
    assert!(runs.len() > 1);
    // sort runs by size
    runs.sort_unstable_by(|a, b| a.size.cmp(&b.size));
    // limit max probe runs to 100
    let probe_end = runs.len().min(100);
    let mut min_penalty = usize::MAX;
    let mut files = vec![];
    let mut temp_files = vec![];
    for i in 0..probe_end {
        for j in i + 1..probe_end {
            let (a, b) = runs.split_at_mut(j);
            find_overlapping_items(&mut a[i], &mut b[0], &mut temp_files);
            let penalty = temp_files.iter().map(|e| e.size()).sum();
            if penalty < min_penalty {
                min_penalty = penalty;
                files.clear();
                files.extend_from_slice(&temp_files);
            }
        }
    }
    files
}

/// Finds the optimal set of adjacent files to merge based on a scoring system.
///
/// This function evaluates all possible contiguous subsets of files to find the best
/// candidates for merging, considering:
///
/// 1. File reduction - prioritizes merging more files to reduce the total count
/// 2. Write amplification - minimizes the ratio of largest file to total size
/// 3. Size efficiency - prefers merges that utilize available space effectively
///
/// When multiple merge candidates have the same score, older files (those with lower indices)
/// are preferred.
///
/// # Arguments
/// * `input_files` - Slice of files to consider for merging
/// * `max_file_size` - Optional maximum size constraint for the merged file.
///   If None, uses 1.5 times the average file size.
///
/// # Returns
/// A vector containing the best set of adjacent files to merge.
/// Returns an empty vector if input is empty or contains only one file.
pub fn merge_seq_files<T: Item>(input_files: &[T], max_file_size: Option<u64>) -> Vec<T> {
    if input_files.is_empty() || input_files.len() == 1 {
        return vec![];
    }

    // Limit the number of files to process to 100 to control time complexity
    let files_to_process = if input_files.len() > 100 {
        &input_files[0..100]
    } else {
        input_files
    };

    // Calculate target size based on max_file_size or average file size
    let target_size = match max_file_size {
        Some(size) => size as usize,
        None => {
            // Calculate 1.5*average_file_size if max_file_size is not provided and clamp to 2GB
            let total_size: usize = files_to_process.iter().map(|f| f.size()).sum();
            ((((total_size as f64) / (files_to_process.len() as f64)) * 1.5) as usize)
                .min(DEFAULT_MAX_OUTPUT_SIZE as usize)
        }
    };

    // Find the best group of adjacent files to merge
    let mut best_group = Vec::new();
    let mut best_score = f64::NEG_INFINITY;

    // Try different starting positions - iterate from end to start to prefer older files
    for start_idx in (0..files_to_process.len()).rev() {
        // Try different ending positions - also iterate from end to start
        for end_idx in (start_idx + 1..files_to_process.len()).rev() {
            let group = &files_to_process[start_idx..=end_idx];
            let total_size: usize = group.iter().map(|f| f.size()).sum();

            // Skip if total size exceeds target size
            if total_size > target_size {
                continue; // Use continue instead of break to check smaller ranges
            }

            // Calculate amplification factor (largest file size / total size)
            let largest_file_size = group.iter().map(|f| f.size()).max().unwrap_or(0);
            let amplification_factor = largest_file_size as f64 / total_size as f64;

            // Calculate file reduction (number of files that will be reduced)
            let file_reduction = group.len() - 1;

            // Calculate score based on multiple factors:
            // 1. File reduction (higher is better)
            // 2. Amplification factor (lower is better)
            // 3. Size efficiency (how close to target size)
            let file_reduction_score = file_reduction as f64 / files_to_process.len() as f64;
            let amp_factor_score = (1.0 - amplification_factor) * 1.5; // Lower amplification is better
            let size_efficiency = (total_size as f64 / target_size as f64).min(1.0); // Reward using available space

            let score = file_reduction_score + amp_factor_score + size_efficiency;

            // Check if this group is better than our current best
            // Use >= instead of > to prefer older files (which we encounter first due to reverse iteration)
            if score >= best_score {
                best_score = score;
                best_group = group.to_vec();
            }
        }
    }

    best_group
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    #[derive(Clone, Debug, PartialEq)]
    struct MockFile {
        start: i64,
        end: i64,
        size: usize,
    }

    impl Ranged for MockFile {
        type BoundType = i64;

        fn range(&self) -> (Self::BoundType, Self::BoundType) {
            (self.start, self.end)
        }
    }

    impl Item for MockFile {
        fn size(&self) -> usize {
            self.size
        }
    }

    fn build_items(ranges: &[(i64, i64)]) -> Vec<MockFile> {
        ranges
            .iter()
            .map(|(start, end)| MockFile {
                start: *start,
                end: *end,
                size: (*end - *start) as usize,
            })
            .collect()
    }

    fn build_items_with_size(items: &[(i64, i64, usize)]) -> Vec<MockFile> {
        items
            .iter()
            .map(|(start, end, size)| MockFile {
                start: *start,
                end: *end,
                size: *size,
            })
            .collect()
    }

    fn check_sorted_runs(
        ranges: &[(i64, i64)],
        expected_runs: &[Vec<(i64, i64)>],
    ) -> Vec<SortedRun<MockFile>> {
        let mut files = build_items(ranges);
        let runs = find_sorted_runs(&mut files);

        let result_file_ranges: Vec<Vec<_>> = runs
            .iter()
            .map(|r| r.items.iter().map(|f| f.range()).collect())
            .collect();
        assert_eq!(&expected_runs, &result_file_ranges);
        runs
    }

    #[test]
    fn test_find_sorted_runs() {
        check_sorted_runs(&[], &[]);
        check_sorted_runs(&[(1, 1), (2, 2)], &[vec![(1, 1), (2, 2)]]);
        check_sorted_runs(&[(1, 2)], &[vec![(1, 2)]]);
        check_sorted_runs(&[(1, 2), (2, 3)], &[vec![(1, 2), (2, 3)]]);
        check_sorted_runs(&[(1, 2), (3, 4)], &[vec![(1, 2), (3, 4)]]);
        check_sorted_runs(&[(2, 4), (1, 3)], &[vec![(1, 3)], vec![(2, 4)]]);
        check_sorted_runs(
            &[(1, 3), (2, 4), (4, 5)],
            &[vec![(1, 3), (4, 5)], vec![(2, 4)]],
        );

        check_sorted_runs(
            &[(1, 2), (3, 4), (3, 5)],
            &[vec![(1, 2), (3, 5)], vec![(3, 4)]],
        );

        check_sorted_runs(
            &[(1, 3), (2, 4), (5, 6)],
            &[vec![(1, 3), (5, 6)], vec![(2, 4)]],
        );

        check_sorted_runs(
            &[(1, 2), (3, 5), (4, 6)],
            &[vec![(1, 2), (3, 5)], vec![(4, 6)]],
        );

        check_sorted_runs(
            &[(1, 2), (3, 4), (4, 6), (7, 8)],
            &[vec![(1, 2), (3, 4), (4, 6), (7, 8)]],
        );
        check_sorted_runs(
            &[(1, 2), (3, 4), (5, 6), (3, 6), (7, 8), (8, 9)],
            &[vec![(1, 2), (3, 6), (7, 8), (8, 9)], vec![(3, 4), (5, 6)]],
        );

        check_sorted_runs(
            &[(10, 19), (20, 21), (20, 29), (30, 39)],
            &[vec![(10, 19), (20, 29), (30, 39)], vec![(20, 21)]],
        );

        check_sorted_runs(
            &[(10, 19), (20, 29), (21, 22), (30, 39), (31, 32), (32, 42)],
            &[
                vec![(10, 19), (20, 29), (30, 39)],
                vec![(21, 22), (31, 32), (32, 42)],
            ],
        );
    }

    fn check_reduce_runs(
        files: &[(i64, i64)],
        expected_runs: &[Vec<(i64, i64)>],
        expected: &[(i64, i64)],
    ) {
        let runs = check_sorted_runs(files, expected_runs);
        if runs.len() <= 1 {
            assert!(expected.is_empty());
            return;
        }
        let files_to_merge = reduce_runs(runs);
        let file_to_merge_timestamps = files_to_merge
            .into_iter()
            .map(|f| (f.start, f.end))
            .collect::<HashSet<_>>();

        let expected = expected.iter().cloned().collect::<HashSet<_>>();
        assert_eq!(&expected, &file_to_merge_timestamps);
    }

    #[test]
    fn test_reduce_runs() {
        // [1..3]   [5..6]
        //   [2..4]
        check_reduce_runs(
            &[(1, 3), (2, 4), (5, 6)],
            &[vec![(1, 3), (5, 6)], vec![(2, 4)]],
            &[(1, 3), (2, 4)],
        );

        // [1..2][3..5]
        //         [4..6]
        check_reduce_runs(
            &[(1, 2), (3, 5), (4, 6)],
            &[vec![(1, 2), (3, 5)], vec![(4, 6)]],
            &[(3, 5), (4, 6)],
        );

        // [1..2][3..4]    [7..8]
        //          [4..6]
        check_reduce_runs(
            &[(1, 2), (3, 4), (4, 6), (7, 8)],
            &[vec![(1, 2), (3, 4), (4, 6), (7, 8)]],
            &[],
        );

        // [1..2][3........6][7..8][8..9]
        //       [3..4][5..6]
        check_reduce_runs(
            &[(1, 2), (3, 4), (5, 6), (3, 6), (7, 8), (8, 9)],
            &[vec![(1, 2), (3, 6), (7, 8), (8, 9)], vec![(3, 4), (5, 6)]],
            &[(5, 6), (3, 4), (3, 6)], // already satisfied
        );

        // [1..2][3........6][7..8][8..9]
        //       [3..4][5..6]
        check_reduce_runs(
            &[(1, 2), (3, 4), (5, 6), (3, 6), (7, 8), (8, 9)],
            &[vec![(1, 2), (3, 6), (7, 8), (8, 9)], vec![(3, 4), (5, 6)]],
            &[(3, 4), (3, 6), (5, 6)],
        );

        // [10..20] [30..40] [50........80][80...100][100..110]
        //                   [50..60]  [80..90]
        //
        check_reduce_runs(
            &[
                (10, 20),
                (30, 40),
                (50, 60),
                (50, 80),
                (80, 90),
                (80, 100),
                (100, 110),
            ],
            &[
                vec![(10, 20), (30, 40), (50, 80), (80, 100), (100, 110)],
                vec![(50, 60), (80, 90)],
            ],
            &[(50, 80), (80, 100), (50, 60), (80, 90)],
        );

        // [0..10]
        // [0...11]
        // [0....12]
        // [0.....13]
        check_reduce_runs(
            &[(0, 10), (0, 11), (0, 12), (0, 13)],
            &[vec![(0, 13)], vec![(0, 12)], vec![(0, 11)], vec![(0, 10)]],
            &[(0, 10), (0, 11)],
        );
    }

    #[test]
    fn test_find_overlapping_items() {
        let mut result = Vec::new();

        // Test empty inputs
        find_overlapping_items(
            &mut SortedRun::from(Vec::<MockFile>::new()),
            &mut SortedRun::from(Vec::<MockFile>::new()),
            &mut result,
        );
        assert_eq!(result, Vec::<MockFile>::new());

        let files1 = build_items(&[(1, 3)]);
        find_overlapping_items(
            &mut SortedRun::from(files1.clone()),
            &mut SortedRun::from(Vec::<MockFile>::new()),
            &mut result,
        );
        assert_eq!(result, Vec::<MockFile>::new());

        find_overlapping_items(
            &mut SortedRun::from(Vec::<MockFile>::new()),
            &mut SortedRun::from(files1.clone()),
            &mut result,
        );
        assert_eq!(result, Vec::<MockFile>::new());

        // Test non-overlapping ranges
        let files1 = build_items(&[(1, 3), (5, 7)]);
        let files2 = build_items(&[(10, 12), (15, 20)]);
        find_overlapping_items(
            &mut SortedRun::from(files1),
            &mut SortedRun::from(files2),
            &mut result,
        );
        assert_eq!(result, Vec::<MockFile>::new());

        // Test simple overlap
        let files1 = build_items(&[(1, 5)]);
        let files2 = build_items(&[(3, 7)]);
        find_overlapping_items(
            &mut SortedRun::from(files1),
            &mut SortedRun::from(files2),
            &mut result,
        );
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].range(), (1, 5));
        assert_eq!(result[1].range(), (3, 7));

        // Test multiple overlaps
        let files1 = build_items(&[(1, 5), (8, 12), (15, 20)]);
        let files2 = build_items(&[(3, 6), (7, 10), (18, 25)]);
        find_overlapping_items(
            &mut SortedRun::from(files1),
            &mut SortedRun::from(files2),
            &mut result,
        );
        assert_eq!(result.len(), 6);

        // Test boundary cases (touching but not overlapping)
        let files1 = build_items(&[(1, 5)]);
        let files2 = build_items(&[(5, 10)]); // Touching at 5
        find_overlapping_items(
            &mut SortedRun::from(files1),
            &mut SortedRun::from(files2),
            &mut result,
        );
        assert_eq!(result.len(), 2); // Should overlap since ranges are inclusive

        // Test completely contained ranges
        let files1 = build_items(&[(1, 10)]);
        let files2 = build_items(&[(3, 7)]);
        find_overlapping_items(
            &mut SortedRun::from(files1),
            &mut SortedRun::from(files2),
            &mut result,
        );
        assert_eq!(result.len(), 2);

        // Test identical ranges
        let files1 = build_items(&[(1, 5)]);
        let files2 = build_items(&[(1, 5)]);
        find_overlapping_items(
            &mut SortedRun::from(files1),
            &mut SortedRun::from(files2),
            &mut result,
        );
        assert_eq!(result.len(), 2);

        // Test unsorted input handling
        let files1 = build_items(&[(5, 10), (1, 3)]); // Unsorted
        let files2 = build_items(&[(2, 7), (8, 12)]); // Unsorted
        find_overlapping_items(
            &mut SortedRun::from(files1),
            &mut SortedRun::from(files2),
            &mut result,
        );
        assert_eq!(result.len(), 4); // Should find both overlaps
    }

    #[test]
    fn test_merge_seq_files() {
        // Test empty input
        let files = Vec::<MockFile>::new();
        assert_eq!(merge_seq_files(&files, None), Vec::<MockFile>::new());

        // Test single file input (should return empty vec as no merge needed)
        let files = build_items(&[(1, 5)]);
        assert_eq!(merge_seq_files(&files, None), Vec::<MockFile>::new());

        // Test the example case: [10, 1, 1, 1] - should merge the last three files
        let files = build_items_with_size(&[(1, 2, 10), (3, 4, 1), (5, 6, 1), (7, 8, 1)]);
        let result = merge_seq_files(&files, None);
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].size, 1);
        assert_eq!(result[1].size, 1);
        assert_eq!(result[2].size, 1);

        // Test with files of equal size - should merge as many as possible
        let files = build_items_with_size(&[(1, 2, 5), (3, 4, 5), (5, 6, 5), (7, 8, 5)]);
        let result = merge_seq_files(&files, Some(20));
        assert_eq!(result.len(), 4); // Should merge all 4 files as total size is 20

        // Test with max_file_size constraint
        let files = build_items_with_size(&[(1, 2, 5), (3, 4, 5), (5, 6, 5), (7, 8, 5)]);
        let result = merge_seq_files(&files, Some(10));
        assert_eq!(result.len(), 2); // Should merge only 2 files as max size is 10

        // Test with uneven file sizes - should prioritize reducing file count
        let files = build_items_with_size(&[(1, 2, 2), (3, 4, 3), (5, 6, 4), (7, 8, 10)]);
        let result = merge_seq_files(&files, Some(10));
        assert_eq!(result.len(), 3); // Should merge the first 3 files (total size 9)

        // Test amplification factor prioritization
        // Two possible merges: [5, 5] (amp factor 0.5) vs [10, 1, 1] (amp factor 0.83)
        let files =
            build_items_with_size(&[(1, 2, 5), (3, 4, 5), (5, 6, 10), (7, 8, 1), (9, 10, 1)]);
        let result = merge_seq_files(&files, Some(12));
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].size, 5);
        assert_eq!(result[1].size, 5);

        // Test with large file preventing merges
        let files = build_items_with_size(&[(1, 2, 100), (3, 4, 1), (5, 6, 1), (7, 8, 1)]);
        let result = merge_seq_files(&files, Some(10));
        assert_eq!(result.len(), 3); // Should merge the last 3 small files
        assert_eq!(result[0].size, 1);
        assert_eq!(result[1].size, 1);
        assert_eq!(result[2].size, 1);

        let files = build_items_with_size(&[(1, 2, 100), (3, 4, 20), (5, 6, 20), (7, 8, 20)]);
        let result = merge_seq_files(&files, Some(200));
        assert_eq!(result.len(), 4);

        let files = build_items_with_size(&[(1, 2, 160), (3, 4, 20), (5, 6, 20), (7, 8, 20)]);
        let result = merge_seq_files(&files, None);
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].size, 20);
        assert_eq!(result[1].size, 20);
        assert_eq!(result[2].size, 20);

        let files = build_items_with_size(&[(1, 2, 100), (3, 4, 1)]);
        let result = merge_seq_files(&files, Some(200));
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].size, 100);
        assert_eq!(result[1].size, 1);

        let files = build_items_with_size(&[(1, 2, 20), (3, 4, 20), (5, 6, 20), (7, 8, 20)]);
        let result = merge_seq_files(&files, Some(40));
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].start, 1);
        assert_eq!(result[1].start, 3);
    }
}
