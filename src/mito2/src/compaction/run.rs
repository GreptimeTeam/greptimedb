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

use std::cmp::Ordering;
use std::marker::PhantomData;

use common_base::BitVec;
use common_time::Timestamp;
use itertools::Itertools;

use crate::sst::file::FileHandle;

/// Trait for any items with specific range.
pub(crate) trait Ranged {
    type BoundType: Ord + Copy;

    fn range(&self) -> (Self::BoundType, Self::BoundType);

    fn overlap<T>(&self, other: &T) -> bool
    where
        T: Ranged<BoundType = Self::BoundType>,
    {
        let (lhs_start, lhs_end) = self.range();
        let (rhs_start, rhs_end) = other.range();
        match lhs_start.cmp(&rhs_start) {
            Ordering::Less => lhs_end >= rhs_start,
            Ordering::Equal => true,
            Ordering::Greater => lhs_start <= rhs_end,
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
pub(crate) trait Item: Ranged + Clone {
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

#[derive(Debug, Clone)]
struct MergeItems<T: Item> {
    items: Vec<T>,
    start: T::BoundType,
    end: T::BoundType,
    size: usize,
}

impl<T: Item> Ranged for MergeItems<T> {
    type BoundType = T::BoundType;

    fn range(&self) -> (Self::BoundType, Self::BoundType) {
        (self.start, self.end)
    }
}

impl<T: Item> MergeItems<T> {
    /// Creates unmerged item from given value.
    pub fn new_unmerged(val: T) -> Self {
        let (start, end) = val.range();
        let size = val.size();
        Self {
            items: vec![val],
            start,
            end,
            size,
        }
    }

    /// The range of current merge item
    pub(crate) fn range(&self) -> (T::BoundType, T::BoundType) {
        (self.start, self.end)
    }

    /// Merges current item with other item.
    pub(crate) fn merge(self, other: Self) -> Self {
        let start = self.start.min(other.start);
        let end = self.end.max(other.end);
        let size = self.size + other.size;

        let mut items = Vec::with_capacity(self.items.len() + other.items.len());
        items.extend(self.items);
        items.extend(other.items);
        Self {
            start,
            end,
            size,
            items,
        }
    }

    /// Returns the size of current item.
    // If current item is merged from two or more items, then the size will be the sum of all
    // items merged.
    fn size(&self) -> usize {
        self.size
    }

    /// Returns true if current item is merged from two items.
    pub fn merged(&self) -> bool {
        self.items.len() > 1
    }
}

/// A set of files with non-overlapping time ranges.
#[derive(Debug, Clone)]
pub(crate) struct SortedRun<T: Item> {
    /// Items to merge
    items: Vec<MergeItems<T>>,
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
    fn push_item(&mut self, t: MergeItems<T>) {
        let (file_start, file_end) = t.range();
        self.items.push(t);
        self.start = Some(self.start.map_or(file_start, |v| v.min(file_start)));
        self.end = Some(self.end.map_or(file_end, |v| v.max(file_end)));
    }

    fn merge(self, other: Self) -> Self {
        let (lhs, rhs) = if self.start < other.start {
            (self, other)
        } else {
            (other, self)
        };

        #[derive(Default)]
        struct Selection<T: Ranged> {
            lhs_selection: BitVec,
            rhs_selection: BitVec,
            start: Option<T::BoundType>,
            end: Option<T::BoundType>,
            _phantom_data: PhantomData<T>,
        }
        impl<T: Ranged> Ranged for Selection<T> {
            type BoundType = T::BoundType;

            fn range(&self) -> (Self::BoundType, Self::BoundType) {
                (self.start.unwrap(), self.end.unwrap())
            }
        }

        impl<T: Ranged> Selection<T> {
            fn new(lhs_size: usize, rhs_size: usize) -> Self {
                Self {
                    lhs_selection: BitVec::repeat(false, lhs_size),
                    rhs_selection: BitVec::repeat(false, rhs_size),
                    start: None,
                    end: None,
                    _phantom_data: Default::default(),
                }
            }

            fn select_item(&mut self, lhs: bool, idx: usize, item: &T) {
                let selection = if lhs {
                    &mut self.lhs_selection
                } else {
                    &mut self.rhs_selection
                };

                selection.set(idx, true);
                let (start, end) = item.range();
                self.start = Some(self.start.map_or(start, |e| e.min(start)));
                self.end = Some(self.end.map_or(end, |e| e.max(end)));
            }
        }

        let mut overlapping_item: Vec<Selection<MergeItems<T>>> = vec![];
        let mut current_overlapping: Option<Selection<MergeItems<T>>> = None;

        let mut lhs_start_offset = None;
        let mut lhs_remain = BitVec::repeat(true, lhs.items.len());

        for (rhs_idx, rhs_item) in rhs.items.iter().enumerate() {
            if let Some(current) = &current_overlapping {
                // it's a new round
                if !rhs_item.overlap(current) {
                    overlapping_item.push(std::mem::take(&mut current_overlapping).unwrap())
                }
            }

            for lhs_idx in lhs_start_offset.unwrap_or(0)..lhs.items.len() {
                let lhs_item = &lhs.items[lhs_idx];
                if !lhs_item.overlap(rhs_item) {
                    continue;
                }

                let overlapping = current_overlapping
                    .get_or_insert_with(|| Selection::new(lhs.items.len(), rhs.items.len()));
                overlapping.select_item(true, lhs_idx, lhs_item);
                overlapping.select_item(false, rhs_idx, rhs_item);
                // lhs_item is selected in current overlapping, then it won't remain
                lhs_remain.set(lhs_idx, false);
                lhs_start_offset.get_or_insert(lhs_idx);
            }
        }

        if let Some(o) = std::mem::take(&mut current_overlapping) {
            overlapping_item.push(o);
        }

        let mut penalty = 0;
        let mut result = SortedRun::default();

        for overlapping in overlapping_item {
            let mut item: Option<MergeItems<T>> = None;
            for (selected, lhs_item) in overlapping
                .lhs_selection
                .iter()
                .by_vals()
                .zip(lhs.items.iter())
            {
                if selected {
                    // lhs_item in current overlapping.
                    penalty += lhs_item.size();
                    item = Some(match item {
                        None => lhs_item.clone(),
                        Some(e) => e.merge(lhs_item.clone()),
                    });
                }
            }

            for (selected, rhs_item) in overlapping
                .rhs_selection
                .iter()
                .by_vals()
                .zip(rhs.items.iter())
            {
                if selected {
                    penalty += rhs_item.size();
                    item = Some(match item {
                        None => rhs_item.clone(),
                        Some(e) => e.merge(rhs_item.clone()),
                    });
                }
            }
            // safety: for each overlapping there must be at least one item.
            result.push_item(item.unwrap());
        }

        for (remain, lhs_item) in lhs_remain.iter().by_vals().zip(lhs.items.into_iter()) {
            if remain {
                // lhs item remains unmerged
                result.push_item(lhs_item);
            }
        }

        sort_ranged_items(&mut result.items);
        result.penalty = penalty;
        result
    }
}

/// Finds sorted runs in given items.
pub(crate) fn find_sorted_runs<T>(mut items: Vec<T>) -> Vec<SortedRun<T>>
where
    T: Item,
{
    if items.is_empty() {
        return vec![];
    }
    // sort files
    sort_ranged_items(&mut items);

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
            let current_item = MergeItems::new_unmerged(item.clone());
            match current_run.items.last() {
                None => {
                    // current run is empty, just add current_item
                    selected.set(true);
                    current_run.push_item(current_item);
                }
                Some(last) => {
                    // the current item does not overlap with the last item in current run,
                    // then it belongs to current run.
                    if !last.overlap(&current_item) {
                        // does not overlap, push to current run
                        selected.set(true);
                        current_run.push_item(current_item);
                    }
                }
            }
        }
        // finished an iteration, we've found a new run.
        runs.push(std::mem::take(&mut current_run));
    }
    runs
}

fn merge_all_runs<T: Item>(mut runs: Vec<SortedRun<T>>) -> SortedRun<T> {
    assert!(!runs.is_empty());
    if runs.len() == 1 {
        return runs.pop().unwrap();
    }
    let mut res = runs.pop().unwrap();
    while let Some(next) = runs.pop() {
        res = res.merge(next);
    }
    res
}

/// Reduces the num of runs to given target and returns items to merge.
/// The time complexity of this function is `C_{k}_{runs.len()}` where k=`runs.len()`-target+1.
pub(crate) fn reduce_runs<T: Item>(runs: Vec<SortedRun<T>>, target: usize) -> Vec<Vec<T>> {
    assert_ne!(target, 0);
    if target >= runs.len() {
        // already satisfied.
        return vec![];
    }

    let k = runs.len() + 1 - target;
    runs.into_iter()
        .combinations(k) // find all possible solutions
        .map(|runs_to_merge| merge_all_runs(runs_to_merge)) // calculate merge penalty
        .min_by(|p, r| p.penalty.cmp(&r.penalty)) // find solution with the min penalty
        .unwrap() // safety: their must be at least one solution.
        .items
        .into_iter()
        .filter(|m| m.merged()) // find all files to merge in that solution
        .map(|m| m.items)
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    #[derive(Clone, Debug)]
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
        let files = build_items(ranges);
        let runs = find_sorted_runs(files);

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

    fn check_merge_sorted_runs(
        items: &[(i64, i64)],
        expected_penalty: usize,
        expected: &[Vec<(i64, i64)>],
    ) {
        let mut runs = find_sorted_runs(build_items(items));
        assert_eq!(2, runs.len());
        let lhs = runs.pop().unwrap();
        let rhs = runs.pop().unwrap();
        let res = lhs.merge(rhs);
        let penalty = res.penalty;
        let ranges = res
            .items
            .into_iter()
            .map(|i| {
                i.items
                    .into_iter()
                    .map(|f| (f.start, f.end))
                    .sorted_by(|l, r| l.0.cmp(&r.0).then(l.1.cmp(&r.1)))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(expected, &ranges);

        assert_eq!(expected_penalty, penalty);
    }

    #[test]
    fn test_merge_sorted_runs() {
        // [1..2]
        // [1...3]
        check_merge_sorted_runs(&[(1, 2), (1, 3)], 3, &[vec![(1, 2), (1, 3)]]);

        // [1..2][3..4]
        //    [2..3]
        check_merge_sorted_runs(
            &[(1, 2), (2, 3), (3, 4)],
            3,
            &[vec![(1, 2), (2, 3), (3, 4)]],
        );

        // [1..10][11..20][21...30]
        //          [18]
        check_merge_sorted_runs(
            &[(1, 10), (11, 20), (21, 30), (18, 18)],
            9,
            &[vec![(1, 10)], vec![(11, 20), (18, 18)], vec![(21, 30)]],
        );

        // [1..3][4..5]
        //   [2...4]
        check_merge_sorted_runs(
            &[(1, 3), (2, 4), (4, 5)],
            5,
            &[vec![(1, 3), (2, 4), (4, 5)]],
        );

        // [1..2][3..4]    [7..8]
        //          [4..6]
        check_merge_sorted_runs(
            &[(1, 2), (3, 4), (4, 6), (7, 8)],
            3,
            &[vec![(1, 2)], vec![(3, 4), (4, 6)], vec![(7, 8)]],
        );

        // [1..2][3..4][5..6][7..8]
        //       [3........6]   [8..9]
        //
        check_merge_sorted_runs(
            &[(1, 2), (3, 4), (5, 6), (3, 6), (7, 8), (8, 9)],
            7,
            &[
                vec![(1, 2)],
                vec![(3, 4), (3, 6), (5, 6)],
                vec![(7, 8), (8, 9)],
            ],
        );

        // [10.....19][20........29][30........39]
        //              [21..22]     [31..32]
        check_merge_sorted_runs(
            &[(10, 19), (20, 29), (21, 22), (30, 39), (31, 32)],
            20,
            &[
                vec![(10, 19)],
                vec![(20, 29), (21, 22)],
                vec![(30, 39), (31, 32)],
            ],
        );

        // [1..10][11..20][21..30]
        // [1..10]        [21..30]
        check_merge_sorted_runs(
            &[(1, 10), (1, 10), (11, 20), (21, 30), (21, 30)],
            36,
            &[
                vec![(1, 10), (1, 10)],
                vec![(11, 20)],
                vec![(21, 30), (21, 30)],
            ],
        );

        // [1..10][11..20][21...30]
        //                 [22..30]
        check_merge_sorted_runs(
            &[(1, 10), (11, 20), (21, 30), (22, 30)],
            17,
            &[vec![(1, 10)], vec![(11, 20)], vec![(21, 30), (22, 30)]],
        );
    }

    /// files: file arrangement with two sorted runs.
    fn check_merge_all_sorted_runs(
        files: &[(i64, i64)],
        expected_penalty: usize,
        expected: &[Vec<(i64, i64)>],
    ) {
        let files = build_items(files);
        let runs = find_sorted_runs(files);
        let result = merge_all_runs(runs);
        assert_eq!(expected_penalty, result.penalty);
        assert_eq!(expected.len(), result.items.len());
        let res = result
            .items
            .iter()
            .map(|i| {
                let mut res = i.items.iter().map(|f| (f.start, f.end)).collect::<Vec<_>>();
                res.sort_unstable_by(|l, r| l.0.cmp(&r.0));
                res
            })
            .collect::<Vec<_>>();
        assert_eq!(expected, &res);
    }

    #[test]
    fn test_merge_all_sorted_runs() {
        // [1..2][3..4]
        //          [4..10]
        check_merge_all_sorted_runs(
            &[(1, 2), (3, 4), (4, 10)],
            7, // 1+6
            &[vec![(1, 2)], vec![(3, 4), (4, 10)]],
        );

        // [1..2] [3..4] [5..6]
        //           [4..........10]
        check_merge_all_sorted_runs(
            &[(1, 2), (3, 4), (5, 6), (4, 10)],
            8, // 1+1+6
            &[vec![(1, 2)], vec![(3, 4), (4, 10), (5, 6)]],
        );

        // [10..20] [30..40] [50....60]
        //             [35........55]
        //                     [51..61]
        check_merge_all_sorted_runs(
            &[(10, 20), (30, 40), (50, 60), (35, 55), (51, 61)],
            50,
            &[vec![(10, 20)], vec![(30, 40), (35, 55), (50, 60), (51, 61)]],
        );
    }

    #[test]
    fn test_sorted_runs_time_range() {
        let files = build_items(&[(1, 2), (3, 4), (4, 10)]);
        let runs = find_sorted_runs(files);
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
        target: usize,
        expected: &[Vec<(i64, i64)>],
    ) {
        let runs = check_sorted_runs(files, expected_runs);
        let files_to_merge = reduce_runs(runs, target);
        let file_timestamps = files_to_merge
            .into_iter()
            .map(|f| {
                let mut overlapping = f.into_iter().map(|f| (f.start, f.end)).collect::<Vec<_>>();
                overlapping.sort_unstable_by(|l, r| l.0.cmp(&r.0).then(l.1.cmp(&r.1)));
                overlapping
            })
            .collect::<HashSet<_>>();

        let expected = expected.iter().cloned().collect::<HashSet<_>>();
        assert_eq!(&expected, &file_timestamps);
    }

    #[test]
    fn test_reduce_runs() {
        // [1..3]   [5..6]
        //   [2..4]
        check_reduce_runs(
            &[(1, 3), (2, 4), (5, 6)],
            &[vec![(1, 3), (5, 6)], vec![(2, 4)]],
            1,
            &[vec![(1, 3), (2, 4)]],
        );

        // [1..2][3..5]
        //         [4..6]
        check_reduce_runs(
            &[(1, 2), (3, 5), (4, 6)],
            &[vec![(1, 2), (3, 5)], vec![(4, 6)]],
            1,
            &[vec![(3, 5), (4, 6)]],
        );

        // [1..4]
        //  [2..5]
        //   [3..6]
        check_reduce_runs(
            &[(1, 4), (2, 5), (3, 6)],
            &[vec![(1, 4)], vec![(2, 5)], vec![(3, 6)]],
            1,
            &[vec![(1, 4), (2, 5), (3, 6)]],
        );
        check_reduce_runs(
            &[(1, 4), (2, 5), (3, 6)],
            &[vec![(1, 4)], vec![(2, 5)], vec![(3, 6)]],
            2,
            &[vec![(1, 4), (2, 5)]],
        );

        // [1..2][3..4]    [7..8]
        //          [4..6]
        check_reduce_runs(
            &[(1, 2), (3, 4), (4, 6), (7, 8)],
            &[vec![(1, 2), (3, 4), (7, 8)], vec![(4, 6)]],
            1,
            &[vec![(3, 4), (4, 6)]],
        );

        // [1..2][3........6][7..8]
        //       [3..4][5..6]   [8..9]
        check_reduce_runs(
            &[(1, 2), (3, 4), (5, 6), (3, 6), (7, 8), (8, 9)],
            &[vec![(1, 2), (3, 6), (7, 8)], vec![(3, 4), (5, 6), (8, 9)]],
            2,
            &[], // already satisfied
        );

        // [1..2][3........6][7..8]
        //       [3..4][5..6]   [8..9]
        check_reduce_runs(
            &[(1, 2), (3, 4), (5, 6), (3, 6), (7, 8), (8, 9)],
            &[vec![(1, 2), (3, 6), (7, 8)], vec![(3, 4), (5, 6), (8, 9)]],
            1,
            &[vec![(3, 4), (3, 6), (5, 6)], vec![(7, 8), (8, 9)]],
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
            2,
            &[vec![(80, 90), (80, 100)]],
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
            1,
            &[vec![(50, 60), (50, 80), (80, 90), (80, 100), (100, 110)]],
        );

        // [0..10]
        // [0...11]
        // [0....12]
        // [0.....13]
        check_reduce_runs(
            &[(0, 10), (0, 11), (0, 12), (0, 13)],
            &[vec![(0, 13)], vec![(0, 12)], vec![(0, 11)], vec![(0, 10)]],
            4,
            &[],
        );
        // enforce 3 runs
        check_reduce_runs(
            &[(0, 10), (0, 11), (0, 12), (0, 13)],
            &[vec![(0, 13)], vec![(0, 12)], vec![(0, 11)], vec![(0, 10)]],
            3,
            &[vec![(0, 10), (0, 11)]],
        );
        // enforce 2 runs
        check_reduce_runs(
            &[(0, 10), (0, 11), (0, 12), (0, 13)],
            &[vec![(0, 13)], vec![(0, 12)], vec![(0, 11)], vec![(0, 10)]],
            2,
            &[vec![(0, 10), (0, 11), (0, 12)]],
        );
        // enforce 1 run
        check_reduce_runs(
            &[(0, 10), (0, 11), (0, 12), (0, 13)],
            &[vec![(0, 13)], vec![(0, 12)], vec![(0, 11)], vec![(0, 10)]],
            1,
            &[vec![(0, 10), (0, 11), (0, 12), (0, 13)]],
        );
    }
}
