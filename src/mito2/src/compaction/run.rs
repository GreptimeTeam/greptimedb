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

use common_base::BitVec;

use crate::sst::file::FileHandle;

pub(crate) trait Item: Clone {
    type BoundType: Ord + Copy;

    fn range(&self) -> (Self::BoundType, Self::BoundType);

    fn size(&self) -> usize;

    fn merge(&self, other: &Self) -> Self;

    fn overlap(&self, other: &Self) -> bool {
        let (lhs_start, lhs_end) = self.range();
        let (rhs_start, rhs_end) = other.range();
        if lhs_start < rhs_start {
            lhs_end >= rhs_start
        } else if lhs_start > rhs_start {
            lhs_start <= rhs_end
        } else {
            true
        }
    }
}

struct FileHandlesItem {
    files: Vec<FileHandle>,
}

/// A set of files with non-overlapping time ranges.
#[derive(Debug, Clone)]
pub(crate) struct SortedRun<T: Item> {
    pub(crate) items: Vec<T>,
    penalty: usize,
    start: Option<T::BoundType>,
    end: Option<T::BoundType>,
}

impl<T: Item> SortedRun<T> {
    pub(crate) fn new(items: &[T]) -> Self {
        let mut res = Self {
            items: Vec::with_capacity(items.len()),
            penalty: 0,
            start: None,
            end: None,
        };
        for i in items {
            res.push_item(i);
        }
        res
    }
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
    /// Adds a file to current run and updates timestamps.
    fn push_item(&mut self, t: &T) {
        let (file_start, file_end) = t.range();
        self.items.push(t.clone());
        self.start = Some(self.start.map_or(file_start, |v| v.min(file_start)));
        self.end = Some(self.end.map_or(file_end, |v| v.max(file_end)));
    }

    fn merge(self, other: Self) -> Self {
        let (lhs, rhs) = if self.start < other.start {
            (self, other)
        } else {
            (other, self)
        };

        let mut overlapping_item: Option<T> = None;

        let mut lhs_selection = BitVec::repeat(false, lhs.items.len());
        let mut lhs_start_offset = None;

        for rhs_idx in 0..rhs.items.len() {
            for lhs_idx in lhs_start_offset.unwrap_or(0)..lhs.items.len() {
                let rhs_item = &rhs.items[rhs_idx];
                let lhs_item = &lhs.items[lhs_idx];

                if !lhs_item.overlap(rhs_item) {
                    continue;
                }
                lhs_start_offset.get_or_insert(lhs_idx);
                lhs_selection.set(lhs_idx, true);
            }
        }

        let mut lhs_remain = SortedRun::default();
        let mut penalty = 0;
        for (f, selected) in lhs.items.iter().zip(lhs_selection.iter().by_vals()) {
            if selected {
                penalty += f.size();
                overlapping_item =
                    Some(overlapping_item.map_or(f.clone(), |existing| existing.merge(f)));
            } else {
                lhs_remain.push_item(f);
            }
        }

        for rhs in &rhs.items {
            penalty += rhs.size();
            overlapping_item =
                Some(overlapping_item.map_or(rhs.clone(), |existing| existing.merge(rhs)));
        }
        lhs_remain.push_item(&overlapping_item.unwrap());
        lhs_remain.items.sort_unstable_by(|l, r| {
            let (l_start, l_end) = l.range();
            let (r_start, r_end) = r.range();
            l_start.cmp(&r_start).then(r_end.cmp(&l_end))
        });
        lhs_remain.penalty = penalty;
        lhs_remain
    }
}

pub(crate) fn find_sorted_runs<T>(mut files: Vec<T>) -> Vec<SortedRun<T>>
where
    T: Item,
{
    if files.is_empty() {
        return vec![];
    }
    // sort files
    files.sort_unstable_by(|l, r| {
        let (l_start, l_end) = l.range();
        let (r_start, r_end) = r.range();
        l_start.cmp(&r_start).then(r_end.cmp(&l_end))
    });

    let mut current_run = SortedRun::default();
    let mut runs = vec![];

    while !files.is_empty() {
        let mut selection = BitVec::repeat(false, files.len());
        for idx in 0..files.len() {
            let current = &files[idx];
            match current_run.items.last() {
                None => {
                    selection.set(idx, true);
                    current_run.push_item(current);
                }
                Some(last) => {
                    if !last.overlap(current) {
                        // does not overlap, push to current run
                        selection.set(idx, true);
                        current_run.push_item(current);
                    } else {
                        selection.set(idx, false);
                    }
                }
            }
        }
        runs.push(std::mem::take(&mut current_run));
        files = files
            .into_iter()
            .zip(selection.iter().by_vals())
            .filter_map(|(f, selected)| if selected { None } else { Some(f) })
            .collect::<Vec<_>>();
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

/// Finds out the overlapping items between two adjacent runs.
/// For two adjacent runs, all items in the latter (with larger start bound) should overlap
/// with some item in previous run.
/// # Return
/// Returns the remaining lhs run and overlapping files.
fn find_overlapping_items<T: Item>(
    lhs: &SortedRun<T>,
    rhs: &SortedRun<T>,
) -> (SortedRun<T>, Vec<T>) {
    assert!(lhs.start <= rhs.start);
    assert!(!lhs.items.is_empty());
    assert!(!rhs.items.is_empty());
    // todo: empty handling
    let mut overlapping_items = vec![];

    let mut lhs_selection = BitVec::repeat(false, lhs.items.len());

    let mut lhs_start_offset = None;
    for rhs_idx in 0..rhs.items.len() {
        for lhs_idx in lhs_start_offset.unwrap_or(0)..lhs.items.len() {
            let rhs_item = &rhs.items[rhs_idx];
            let lhs_item = &lhs.items[lhs_idx];

            if !lhs_item.overlap(rhs_item) {
                continue;
            }
            lhs_start_offset.get_or_insert(lhs_idx);
            lhs_selection.set(lhs_idx, true);
        }
    }

    let mut lhs_remain = SortedRun::default();
    for (f, selected) in lhs.items.iter().zip(lhs_selection.iter().by_vals()) {
        if selected {
            overlapping_items.push(f.clone());
        } else {
            lhs_remain.push_item(f);
        }
    }
    overlapping_items.extend(rhs.items.iter().cloned());

    (lhs_remain, overlapping_items)
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use itertools::Itertools;

    use super::*;

    #[derive(Clone, Debug)]
    struct MockFile {
        start: i64,
        end: i64,
        size: usize,
    }

    #[derive(Clone, Debug)]
    struct MockFileItem {
        files: Vec<MockFile>,
        start: i64,
        end: i64,
        size: usize,
    }

    impl MockFileItem {
        fn new(file: MockFile) -> Self {
            let start = file.start;
            let end = file.end;
            let size = file.size;
            let files = vec![file];
            Self {
                files,
                start,
                end,
                size,
            }
        }
    }

    impl Item for MockFileItem {
        type BoundType = i64;

        fn range(&self) -> (Self::BoundType, Self::BoundType) {
            (self.start, self.end)
        }

        fn size(&self) -> usize {
            self.size
        }

        fn merge(&self, other: &Self) -> Self {
            let start = self.start.min(other.start);
            let end = self.end.max(other.end);
            let size = self.size + other.size;
            let mut files = Vec::with_capacity(self.files.len() + other.files.len());
            files.extend(self.files.iter().cloned());
            files.extend(other.files.iter().cloned());

            Self {
                start,
                end,
                size,
                files,
            }
        }
    }

    impl MockFile {
        fn new(start: i64, end: i64) -> Self {
            Self {
                start,
                end,
                size: 1,
            }
        }
    }

    fn build_items(ranges: &[(i64, i64)]) -> Vec<MockFileItem> {
        ranges
            .iter()
            .map(|(start, end)| {
                MockFileItem::new(MockFile {
                    start: *start,
                    end: *end,
                    size: 1,
                })
            })
            .collect()
    }

    fn check_sorted_runs(
        ranges: &[(i64, i64)],
        expected_runs: &[Vec<(i64, i64)>],
    ) -> Vec<SortedRun<MockFileItem>> {
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

    fn check_find_overlapping(ranges: &[(i64, i64)], expected: &[Vec<(i64, i64)>]) {
        let runs = find_sorted_runs(build_items(ranges));
        for run_idx in 0..runs.len() - 1 {
            let (_, files) = find_overlapping_items(&runs[run_idx], &runs[run_idx + 1]);
            let mut range = files.iter().map(|f| f.range()).collect::<Vec<_>>();
            range.sort_unstable_by(|(l_start, l_end), (r_start, r_end)| {
                l_start.cmp(&r_start).then(l_end.cmp(&r_end))
            });
            assert_eq!(expected[run_idx], range);
        }
    }

    #[test]
    fn test_find_overlapping() {
        check_find_overlapping(&[(1, 2), (2, 3)], &[vec![(1, 2), (2, 3)]]);
        check_find_overlapping(&[(1, 3), (2, 4), (4, 5)], &[vec![(1, 3), (2, 4), (4, 5)]]);
        check_find_overlapping(&[(1, 2), (3, 4), (4, 6), (7, 8)], &[vec![(3, 4), (4, 6)]]);
        check_find_overlapping(
            &[(1, 2), (3, 4), (5, 6), (3, 6), (7, 8), (8, 9)],
            &[vec![(3, 4), (3, 6), (5, 6), (7, 8), (8, 9)]],
        );

        check_find_overlapping(
            &[(10, 19), (20, 29), (21, 22), (30, 39), (31, 32), (32, 42)],
            &[
                vec![(20, 29), (21, 22), (30, 39), (31, 32)],
                vec![(31, 32), (32, 42)],
            ],
        );

        check_find_overlapping(
            &[
                (0, 9),
                (10, 19),
                (20, 29),
                (30, 39), // run1
                (11, 18),
                (35, 40), // run2
                (15, 19), // run3
            ],
            &[
                vec![(10, 19), (11, 18), (30, 39), (35, 40)],
                vec![(11, 18), (15, 19)],
            ],
        );
    }

    /// files: file arrangement with two sorted runs.
    fn check_merge_sorted_runs(
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
                let mut res = i.files.iter().map(|f| (f.start, f.end)).collect::<Vec<_>>();
                res.sort_unstable_by(|l, r| l.0.cmp(&r.0));
                res
            })
            .collect::<Vec<_>>();
        assert_eq!(expected, &res);
    }

    #[test]
    fn test_merge_sorted_runs() {
        // [1..2][3..4]
        //          [4..10]
        check_merge_sorted_runs(
            &[(1, 2), (3, 4), (4, 10)],
            2,
            &[vec![(1, 2)], vec![(3, 4), (4, 10)]],
        );

        // [1..2] [3..4] [5..6]
        //           [4..........10]
        check_merge_sorted_runs(
            &[(1, 2), (3, 4), (5, 6), (4, 10)],
            3,
            &[vec![(1, 2)], vec![(3, 4), (4, 10), (5, 6)]],
        );

        // [10..20] [30..40] [50....60]
        //             [35........55]
        //                     [51..61]
        check_merge_sorted_runs(
            &[(10, 20), (30, 40), (50, 60), (35, 55), (51, 61)],
            4,
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

    fn reduce_runs(runs: Vec<SortedRun<MockFileItem>>, target: usize) -> Vec<MockFile> {
        assert_ne!(target, 0);
        //todo: check run length
        let k = runs.len() + 1 - target;

        let run = runs
            .into_iter()
            .combinations(k)
            .into_iter()
            .map(|runs_to_merge| merge_all_runs(runs_to_merge))
            .min_by(|p, r| p.penalty.cmp(&r.penalty))
            .unwrap();

        let mut files_to_merge = vec![];

        for file_item in run.items {
            if file_item.files.len() > 1 {
                // todo: do we have better way to find merged runs.
                files_to_merge.extend(file_item.files);
            }
        }
        files_to_merge
    }

    fn check_reduce_runs(
        files: &[(i64, i64)],
        expected_runs: &[Vec<(i64, i64)>],
        target: usize,
        expected: &[(i64, i64)],
    ) {
        let runs = check_sorted_runs(files, expected_runs);
        let mut files_to_merge = reduce_runs(runs, target);
        files_to_merge.sort_unstable_by(|l, r| l.start.cmp(&r.start));
        let file_timestamps = files_to_merge
            .into_iter()
            .map(|f| (f.start, f.end))
            .collect::<Vec<_>>();
        assert_eq!(expected, &file_timestamps);
    }

    #[test]
    fn test_minimize_runs() {
        check_reduce_runs(
            &[(1, 3), (2, 4), (5, 6)],
            &[vec![(1, 3), (5, 6)], vec![(2, 4)]],
            1,
            &[(1, 3), (2, 4)],
        );

        check_reduce_runs(
            &[(1, 2), (3, 5), (4, 6)],
            &[vec![(1, 2), (3, 5)], vec![(4, 6)]],
            1,
            &[(3, 5), (4, 6)],
        );

        check_reduce_runs(
            &[(1, 4), (2, 5), (3, 6)],
            &[vec![(1, 4)], vec![(2, 5)], vec![(3, 6)]],
            1,
            &[(1, 4), (2, 5), (3, 6)],
        );

        check_reduce_runs(
            &[(1, 4), (2, 5), (3, 6)],
            &[vec![(1, 4)], vec![(2, 5)], vec![(3, 6)]],
            2,
            &[(1, 4), (2, 5)],
        );

        // [1..2][3..4]    [7..8]
        //          [4..6]
        check_reduce_runs(
            &[(1, 2), (3, 4), (4, 6), (7, 8)],
            &[vec![(1, 2), (3, 4), (7, 8)], vec![(4, 6)]],
            1,
            &[(3, 4), (4, 6)],
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
            &[(3, 6), (3, 4), (5, 6), (7, 8), (8, 9)], // already satisfied
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
            2,
            &[(50, 80), (80, 90)],
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
            &[(50, 80), (50, 60), (80, 100), (80, 90), (100, 110)],
        );

        // corner case
        check_reduce_runs(
            &[(0, 10), (0, 11), (0, 12), (0, 13)],
            &[vec![(0, 13)], vec![(0, 12)], vec![(0, 11)], vec![(0, 10)]],
            4,
            &[],
        );

        check_reduce_runs(
            &[(0, 10), (0, 11), (0, 12), (0, 13)],
            &[vec![(0, 13)], vec![(0, 12)], vec![(0, 11)], vec![(0, 10)]],
            3,
            &[(0, 13), (0, 12)],
        );

        check_reduce_runs(
            &[(0, 10), (0, 11), (0, 12), (0, 13)],
            &[vec![(0, 13)], vec![(0, 12)], vec![(0, 11)], vec![(0, 10)]],
            2,
            &[(0, 13), (0, 12), (0, 11)],
        );

        check_reduce_runs(
            &[(0, 10), (0, 11), (0, 12), (0, 13)],
            &[vec![(0, 13)], vec![(0, 12)], vec![(0, 11)], vec![(0, 10)]],
            1,
            &[(0, 13), (0, 12), (0, 11), (0, 10)],
        );
    }
}
