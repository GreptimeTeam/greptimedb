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
use common_time::Timestamp;

use crate::sst::file::FileHandle;

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
        lhs_start.max(rhs_start) <= lhs_end.min(rhs_end)
    }
}

pub fn find_overlapping_items<T: Ranged + Clone>(l: &mut Vec<T>, r: &mut Vec<T>) -> Vec<T> {
    if l.is_empty() || r.is_empty() {
        return vec![];
    }

    let mut res = Vec::with_capacity(l.len() + r.len());

    // Sort both arrays by start boundary for more efficient overlap detection
    sort_ranged_items(l);
    sort_ranged_items(r);

    let mut r_idx = 0;

    let mut selected = BitVec::repeat(false, r.len() + l.len());

    for (lhs_idx, lhs) in l.iter().enumerate() {
        let (lhs_start, lhs_end) = lhs.range();

        // Skip right elements that end before current left element starts
        while r_idx < r.len() {
            let (_, rhs_end) = r[r_idx].range();
            if rhs_end < lhs_start {
                r_idx += 1;
            } else {
                break;
            }
        }

        // Check for overlaps with remaining right elements
        let mut j = r_idx;
        while j < r.len() {
            let (rhs_start, rhs_end) = r[j].range();

            // If right element starts after left element ends, no more overlaps possible
            if rhs_start > lhs_end {
                break;
            }

            // We have an overlap
            if lhs_start.max(rhs_start) <= lhs_end.min(rhs_end) {
                if !selected[lhs_idx] {
                    res.push(lhs.clone());
                    selected.set(lhs_idx, true);
                }

                let rhs_selected_idx = l.len() + j;
                if !selected[rhs_selected_idx] {
                    res.push(r[j].clone());
                    selected.set(rhs_selected_idx, true);
                }
            }

            j += 1;
        }
    }

    res
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

impl Ranged for FileHandle {
    type BoundType = Timestamp;

    fn range(&self) -> (Self::BoundType, Self::BoundType) {
        self.time_range()
    }
}

impl Item for FileHandle {
    fn size(&self) -> usize {
        self.size() as usize
    }
}

/// A set of files with non-overlapping time ranges.
#[derive(Debug, Clone)]
pub struct SortedRun<T: Item> {
    /// Items to merge
    items: Vec<T>,
    /// penalty is defined as the total size of merged items.
    penalty: usize,
    /// The lower bound of all items.
    start: Option<T::BoundType>,
    // The upper bound of all items.
    end: Option<T::BoundType>,
}

impl<T> Default for SortedRun<T>
where
    T: Item,
{
    fn default() -> Self {
        Self {
            items: vec![],
            penalty: 0,
            start: None,
            end: None,
        }
    }
}

impl<T> SortedRun<T>
where
    T: Item,
{
    fn push_item(&mut self, t: T) {
        let (file_start, file_end) = t.range();
        self.penalty += t.size();
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

/// Reduces the num of runs to given target and returns items to merge.
pub fn reduce_runs<T: Item>(runs: Vec<SortedRun<T>>) -> Vec<T> {
    if runs.len() <= 1 {
        // already satisfied.
        return vec![];
    }

    let mut min_penalty = usize::MAX;
    let mut files = vec![];
    for i in 0..runs.len() {
        for j in i + 1..runs.len() {
            let mut l = runs[i].items.clone();
            let mut r = runs[j].items.clone();
            let files_to_merge = find_overlapping_items(&mut l, &mut r);
            let penalty = files_to_merge.iter().map(|e| e.size()).sum();
            if penalty < min_penalty {
                min_penalty = penalty;
                files = files_to_merge;
            }
        }
    }
    files
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
        check_sorted_runs(&[(1, 2), (2, 3)], &[vec![(1, 2)], vec![(2, 3)]]);
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
            &[vec![(1, 2), (3, 4), (7, 8)], vec![(4, 6)]],
        );
        check_sorted_runs(
            &[(1, 2), (3, 4), (5, 6), (3, 6), (7, 8), (8, 9)],
            &[vec![(1, 2), (3, 6), (7, 8)], vec![(3, 4), (5, 6), (8, 9)]],
        );

        check_sorted_runs(
            &[(10, 19), (20, 21), (20, 29), (30, 39)],
            &[vec![(10, 19), (20, 29), (30, 39)], vec![(20, 21)]],
        );

        check_sorted_runs(
            &[(10, 19), (20, 29), (21, 22), (30, 39), (31, 32), (32, 42)],
            &[
                vec![(10, 19), (20, 29), (30, 39)],
                vec![(21, 22), (31, 32)],
                vec![(32, 42)],
            ],
        );
    }

    #[test]
    fn test_sorted_runs_time_range() {
        let mut files = build_items(&[(1, 2), (3, 4), (4, 10)]);
        let runs = find_sorted_runs(&mut files);
        assert_eq!(2, runs.len());
        let SortedRun { start, end, .. } = &runs[0];
        assert_eq!(Some(1), *start);
        assert_eq!(Some(4), *end);

        let SortedRun { start, end, .. } = &runs[1];
        assert_eq!(Some(4), *start);
        assert_eq!(Some(10), *end);
    }

    fn check_reduce_runs(
        files: &[(i64, i64)],
        expected_runs: &[Vec<(i64, i64)>],
        expected: &[(i64, i64)],
    ) {
        let runs = check_sorted_runs(files, expected_runs);
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
            &[vec![(1, 2), (3, 4), (7, 8)], vec![(4, 6)]],
            &[(3, 4), (4, 6)],
        );

        // [1..2][3........6][7..8]
        //       [3..4][5..6]   [8..9]
        check_reduce_runs(
            &[(1, 2), (3, 4), (5, 6), (3, 6), (7, 8), (8, 9)],
            &[vec![(1, 2), (3, 6), (7, 8)], vec![(3, 4), (5, 6), (8, 9)]],
            &[(5, 6), (3, 4), (3, 6), (7, 8), (8, 9)], // already satisfied
        );

        // [1..2][3........6][7..8]
        //       [3..4][5..6]   [8..9]
        check_reduce_runs(
            &[(1, 2), (3, 4), (5, 6), (3, 6), (7, 8), (8, 9)],
            &[vec![(1, 2), (3, 6), (7, 8)], vec![(3, 4), (5, 6), (8, 9)]],
            &[(3, 4), (3, 6), (5, 6), (7, 8), (8, 9)],
        );

        // [10..20] [30..40] [50........80]  [100..110]
        //                   [50..60]  [80...100]
        //                             [80..90]
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
                vec![(10, 20), (30, 40), (50, 80), (100, 110)],
                vec![(50, 60), (80, 100)],
                vec![(80, 90)],
            ],
            &[(80, 90), (80, 100)],
        );

        // [10..20] [30..40] [50........80]     [100..110]
        //                   [50..60]  [80.......100]
        //                             [80..90]
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
                vec![(10, 20), (30, 40), (50, 80), (100, 110)],
                vec![(50, 60), (80, 100)],
                vec![(80, 90)],
            ],
            &[(80, 90), (80, 100)],
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
        // Test empty inputs
        assert_eq!(
            find_overlapping_items(&mut Vec::<MockFile>::new(), &mut Vec::<MockFile>::new()),
            Vec::<MockFile>::new()
        );

        let mut files1 = build_items(&[(1, 3)]);
        assert_eq!(
            find_overlapping_items(&mut files1.clone(), &mut Vec::<MockFile>::new()),
            Vec::<MockFile>::new()
        );
        assert_eq!(
            find_overlapping_items(&mut Vec::<MockFile>::new(), &mut files1),
            Vec::<MockFile>::new()
        );

        // Test non-overlapping ranges
        let mut files1 = build_items(&[(1, 3), (5, 7)]);
        let mut files2 = build_items(&[(10, 12), (15, 20)]);
        assert_eq!(
            find_overlapping_items(&mut files1, &mut files2),
            Vec::<MockFile>::new()
        );

        // Test simple overlap
        let mut files1 = build_items(&[(1, 5)]);
        let mut files2 = build_items(&[(3, 7)]);
        let result = find_overlapping_items(&mut files1, &mut files2);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].range(), (1, 5));
        assert_eq!(result[1].range(), (3, 7));

        // Test multiple overlaps
        let mut files1 = build_items(&[(1, 5), (8, 12), (15, 20)]);
        let mut files2 = build_items(&[(3, 6), (7, 10), (18, 25)]);
        let result = find_overlapping_items(&mut files1, &mut files2);
        assert_eq!(result.len(), 6);

        // Test boundary cases (touching but not overlapping)
        let mut files1 = build_items(&[(1, 5)]);
        let mut files2 = build_items(&[(5, 10)]); // Touching at 5
        let result = find_overlapping_items(&mut files1, &mut files2);
        assert_eq!(result.len(), 2); // Should overlap since ranges are inclusive

        // Test completely contained ranges
        let mut files1 = build_items(&[(1, 10)]);
        let mut files2 = build_items(&[(3, 7)]);
        let result = find_overlapping_items(&mut files1, &mut files2);
        assert_eq!(result.len(), 2);

        // Test identical ranges
        let mut files1 = build_items(&[(1, 5)]);
        let mut files2 = build_items(&[(1, 5)]);
        let result = find_overlapping_items(&mut files1, &mut files2);
        assert_eq!(result.len(), 2);

        // Test unsorted input handling
        let mut files1 = build_items(&[(5, 10), (1, 3)]); // Unsorted
        let mut files2 = build_items(&[(2, 7), (8, 12)]); // Unsorted
        let result = find_overlapping_items(&mut files1, &mut files2);
        assert_eq!(result.len(), 4); // Should find both overlaps
    }
}
