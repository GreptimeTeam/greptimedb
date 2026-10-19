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

use bytes::{Buf, Bytes};
use common_base::BitVec;
use common_time::Timestamp;

use crate::sst::file::FileHandle;
use crate::sst::primary_key::PrimaryKeyRangeMapper;

/// Trait for any items with specific range (both boundaries are inclusive).
pub trait Ranged {
    type BoundType: Ord + Copy;

    /// Returns the inclusive range of item.
    fn range(&self) -> (Self::BoundType, Self::BoundType);

    fn overlap(&self, other: &Self) -> bool {
        let (lhs_start, lhs_end) = self.range();
        let (rhs_start, rhs_end) = other.range();

        lhs_start.max(rhs_start) < lhs_end.min(rhs_end)
    }

    /// Like `overlap`, but treats touching boundaries as overlapping (inclusive).
    /// Used by `find_overlapping_items` where shared boundaries count as overlap.
    fn overlap_inclusive(&self, other: &Self) -> bool {
        let (lhs_start, lhs_end) = self.range();
        let (rhs_start, rhs_end) = other.range();

        lhs_start.max(rhs_start) <= lhs_end.min(rhs_end)
    }
}

pub(crate) fn primary_key_ranges_overlap(lhs: &(Bytes, Bytes), rhs: &(Bytes, Bytes)) -> bool {
    lhs.0.chunk().max(rhs.0.chunk()) <= lhs.1.chunk().min(rhs.1.chunk())
}

pub(crate) fn merge_primary_key_ranges(
    lhs: Option<(Bytes, Bytes)>,
    rhs: Option<(Bytes, Bytes)>,
) -> Option<(Bytes, Bytes)> {
    match (lhs, rhs) {
        (Some((lhs_min, lhs_max)), Some((rhs_min, rhs_max))) => {
            Some((lhs_min.min(rhs_min), lhs_max.max(rhs_max)))
        }
        _ => None,
    }
}

/// Uses the caller's inclusive overlap test after pruning disjoint time ranges.
pub fn find_overlapping_items<T: Item + Clone>(
    l: &mut SortedRun<T>,
    r: &mut SortedRun<T>,
    result: &mut Vec<T>,
    overlaps: impl Fn(&T, &T) -> bool,
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
            let (rhs_start, _rhs_end) = r.items[j].range();

            // If right element starts after left element ends, no more overlaps possible
            if rhs_start > lhs_end {
                break;
            }

            // We have an overlap (inclusive: touching boundaries count)
            if overlaps(lhs, &r.items[j]) {
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

// Physical handles only supply time ranges. PK-aware comparisons need a target schema.
impl Ranged for FileHandle {
    type BoundType = Timestamp;

    fn range(&self) -> (Self::BoundType, Self::BoundType) {
        self.time_range()
    }
}

/// Tests exclusive time overlap and inclusive PK overlap in one pinned schema.
pub(crate) fn files_overlap(
    lhs: &FileHandle,
    rhs: &FileHandle,
    mapper: &PrimaryKeyRangeMapper,
) -> bool {
    lhs.overlap(rhs) && file_primary_keys_overlap(lhs, rhs, mapper)
}

/// Includes touching time and PK boundaries when checking file dependencies.
pub(crate) fn files_overlap_inclusive(
    lhs: &FileHandle,
    rhs: &FileHandle,
    mapper: &PrimaryKeyRangeMapper,
) -> bool {
    lhs.overlap_inclusive(rhs) && file_primary_keys_overlap(lhs, rhs, mapper)
}

fn file_primary_keys_overlap(
    lhs: &FileHandle,
    rhs: &FileHandle,
    mapper: &PrimaryKeyRangeMapper,
) -> bool {
    match (lhs.primary_key_range(mapper), rhs.primary_key_range(mapper)) {
        (Some(lhs), Some(rhs)) => primary_key_ranges_overlap(&lhs, &rhs),
        _ => true,
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

/// Finds sorted runs using the caller's overlap test.
///
/// Items are visited in (start asc, end desc) order and each joins the first run
/// with no overlapping item, which yields the same runs as repeatedly extracting
/// a greedy run from the remaining items. Each run only compares an item with
/// its items whose time ranges are still open: O(N log N + R · N + C) for R runs
/// and C compared pairs.
pub fn find_sorted_runs<T>(items: &mut [T], overlaps: impl Fn(&T, &T) -> bool) -> Vec<SortedRun<T>>
where
    T: Item,
{
    sort_ranged_items(items);

    let mut runs: Vec<OpenRun<T>> = Vec::new();
    for item in items.iter() {
        let (start, _) = item.range();
        let fit = runs
            .iter_mut()
            .position(|run| !run.overlaps_any(item, start, &overlaps));
        let idx = fit.unwrap_or_else(|| {
            runs.push(OpenRun::default());
            runs.len() - 1
        });
        runs[idx].push(item);
    }
    runs.into_iter().map(|run| run.run).collect()
}

/// A sorted run with the indices of its items whose time ranges may still be
/// open.
struct OpenRun<T: Item> {
    run: SortedRun<T>,
    open: Vec<usize>,
    /// The start that `open` was last pruned at. Items arrive in start order,
    /// so pruning once per distinct start suffices.
    pruned_at: Option<T::BoundType>,
}

impl<T: Item> Default for OpenRun<T> {
    fn default() -> Self {
        Self {
            run: SortedRun::default(),
            open: Vec::new(),
            pruned_at: None,
        }
    }
}

impl<T: Item> OpenRun<T> {
    fn overlaps_any(
        &mut self,
        item: &T,
        start: T::BoundType,
        overlaps: impl Fn(&T, &T) -> bool,
    ) -> bool {
        if self.pruned_at != Some(start) {
            let items = &self.run.items;
            self.open.retain(|&idx| items[idx].range().1 > start);
            self.pruned_at = Some(start);
        }
        self.open
            .iter()
            .any(|&idx| overlaps(&self.run.items[idx], item))
    }

    fn push(&mut self, item: &T) {
        self.open.push(self.run.items.len());
        self.run.push_item(item.clone());
    }
}

#[cfg(any(test, feature = "test", feature = "testing"))]
pub fn find_sorted_runs_original<T>(items: &mut [T]) -> Vec<SortedRun<T>>
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
            if current_run.items.is_empty() {
                // current run is empty, just add current_item
                selected.set(true);
                current_run.push_item(item.clone());
            } else {
                // the current item does not overlap with any item in current run,
                // then it belongs to current run. Because now we introduced primary
                // key range, we cannot simply use timestamps to check overlapping.
                let overlaps_any = current_run.items.iter().any(|i| i.overlap(item));
                if !overlaps_any {
                    // does not overlap, push to current run
                    selected.set(true);
                    current_run.push_item(item.clone());
                }
            }
        }
        // finished an iteration, we've found a new run.
        runs.push(std::mem::take(&mut current_run));
    }
    runs
}

#[cfg(test)]
mod tests {
    use store_api::storage::FileId;

    use super::*;
    use crate::compaction::test_util::{
        new_file_handle_with_size_sequence_and_primary_key_range, pk_range,
        primary_key_mapper_for_test,
    };

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

        let runs = find_sorted_runs(&mut files, Ranged::overlap);

        let result_file_ranges: Vec<Vec<_>> = runs
            .iter()
            .map(|r| r.items.iter().map(|f| f.range()).collect())
            .collect();
        assert_eq!(&expected_runs, &result_file_ranges);
        runs
    }

    fn sorted_run_ranges<T: Item>(runs: &[SortedRun<T>]) -> Vec<Vec<T::BoundType>> {
        runs.iter()
            .map(|r| {
                r.items
                    .iter()
                    .flat_map(|f| {
                        let (start, end) = f.range();
                        [start, end]
                    })
                    .collect()
            })
            .collect()
    }

    fn check_find_sorted_runs_consistency(ranges: &[(i64, i64)]) {
        let mut files = build_items(ranges);
        let mut files_for_original = files.clone();

        let runs = find_sorted_runs(&mut files, Ranged::overlap);
        let original_runs = find_sorted_runs_original(&mut files_for_original);

        assert_eq!(sorted_run_ranges(&original_runs), sorted_run_ranges(&runs));
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

    /// Repeatedly extracts a greedy run from the remaining items.
    fn find_sorted_runs_by_passes<T: Item>(
        items: &mut [T],
        overlaps: impl Fn(&T, &T) -> bool,
    ) -> Vec<SortedRun<T>> {
        sort_ranged_items(items);
        let mut remaining = items.to_vec();
        let mut runs = Vec::new();
        while !remaining.is_empty() {
            let mut run = SortedRun::default();
            let mut rest = Vec::new();
            for item in remaining {
                if run.items.iter().any(|other| overlaps(other, &item)) {
                    rest.push(item);
                } else {
                    run.push_item(item);
                }
            }
            runs.push(run);
            remaining = rest;
        }
        runs
    }

    #[test]
    fn test_find_sorted_runs_matches_greedy_passes_with_key_ranges() {
        // Time ranges paired with key ranges, overlapping only in both dimensions.
        #[derive(Clone, Debug, PartialEq)]
        struct Rect {
            time: (i64, i64),
            key: (i64, i64),
        }
        impl Ranged for Rect {
            type BoundType = i64;

            fn range(&self) -> (i64, i64) {
                self.time
            }
        }
        impl Item for Rect {
            fn size(&self) -> usize {
                1
            }
        }
        let overlaps = |lhs: &Rect, rhs: &Rect| {
            lhs.time.0 < rhs.time.1
                && rhs.time.0 < lhs.time.1
                && lhs.key.0 <= rhs.key.1
                && rhs.key.0 <= lhs.key.1
        };
        let mut seed = 42u64;
        let mut next = |bound: i64| {
            seed = seed
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            ((seed >> 33) % bound as u64) as i64
        };
        for _ in 0..200 {
            let len = next(40) as usize;
            let mut rects = (0..len)
                .map(|_| {
                    let (start, key) = (next(100), next(100));
                    Rect {
                        time: (start, start + 1 + next(30)),
                        key: (key, key + next(30)),
                    }
                })
                .collect::<Vec<_>>();
            let mut expected = rects.clone();
            let runs = find_sorted_runs(&mut rects, overlaps);
            let expected = find_sorted_runs_by_passes(&mut expected, overlaps);
            let items = |runs: &[SortedRun<Rect>]| {
                runs.iter().map(|run| run.items.clone()).collect::<Vec<_>>()
            };
            assert_eq!(items(&expected), items(&runs));
        }
    }

    #[test]
    fn test_find_sorted_runs_matches_original_impl() {
        for ranges in [
            &[][..],
            &[(1, 1), (2, 2)],
            &[(1, 2), (2, 3)],
            &[(2, 4), (1, 3)],
            &[(1, 3), (2, 4), (4, 5)],
            &[(1, 2), (3, 4), (3, 5)],
            &[(1, 3), (2, 4), (5, 6)],
            &[(1, 2), (3, 5), (4, 6)],
            &[(1, 2), (3, 4), (4, 6), (7, 8)],
            &[(1, 2), (3, 4), (5, 6), (3, 6), (7, 8), (8, 9)],
            &[(10, 19), (20, 21), (20, 29), (30, 39)],
            &[(10, 19), (20, 29), (21, 22), (30, 39), (31, 32), (32, 42)],
            &[(32, 42), (10, 19), (31, 32), (20, 29), (21, 22), (30, 39)],
        ] {
            check_find_sorted_runs_consistency(ranges);
        }
    }

    #[test]
    fn test_find_overlapping_items() {
        let mut result = Vec::new();

        // Test empty inputs
        find_overlapping_items(
            &mut SortedRun::from(Vec::<MockFile>::new()),
            &mut SortedRun::from(Vec::<MockFile>::new()),
            &mut result,
            Ranged::overlap_inclusive,
        );
        assert_eq!(result, Vec::<MockFile>::new());

        let files1 = build_items(&[(1, 3)]);
        find_overlapping_items(
            &mut SortedRun::from(files1.clone()),
            &mut SortedRun::from(Vec::<MockFile>::new()),
            &mut result,
            Ranged::overlap_inclusive,
        );
        assert_eq!(result, Vec::<MockFile>::new());

        find_overlapping_items(
            &mut SortedRun::from(Vec::<MockFile>::new()),
            &mut SortedRun::from(files1.clone()),
            &mut result,
            Ranged::overlap_inclusive,
        );
        assert_eq!(result, Vec::<MockFile>::new());

        // Test non-overlapping ranges
        let files1 = build_items(&[(1, 3), (5, 7)]);
        let files2 = build_items(&[(10, 12), (15, 20)]);
        find_overlapping_items(
            &mut SortedRun::from(files1),
            &mut SortedRun::from(files2),
            &mut result,
            Ranged::overlap_inclusive,
        );
        assert_eq!(result, Vec::<MockFile>::new());

        // Test simple overlap
        let files1 = build_items(&[(1, 5)]);
        let files2 = build_items(&[(3, 7)]);
        find_overlapping_items(
            &mut SortedRun::from(files1),
            &mut SortedRun::from(files2),
            &mut result,
            Ranged::overlap_inclusive,
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
            Ranged::overlap_inclusive,
        );
        assert_eq!(result.len(), 6);

        // Test boundary cases (touching but not overlapping)
        let files1 = build_items(&[(1, 5)]);
        let files2 = build_items(&[(5, 10)]); // Touching at 5
        find_overlapping_items(
            &mut SortedRun::from(files1),
            &mut SortedRun::from(files2),
            &mut result,
            Ranged::overlap_inclusive,
        );
        assert_eq!(result.len(), 2); // Should overlap since ranges are inclusive

        // Test completely contained ranges
        let files1 = build_items(&[(1, 10)]);
        let files2 = build_items(&[(3, 7)]);
        find_overlapping_items(
            &mut SortedRun::from(files1),
            &mut SortedRun::from(files2),
            &mut result,
            Ranged::overlap_inclusive,
        );
        assert_eq!(result.len(), 2);

        // Test identical ranges
        let files1 = build_items(&[(1, 5)]);
        let files2 = build_items(&[(1, 5)]);
        find_overlapping_items(
            &mut SortedRun::from(files1),
            &mut SortedRun::from(files2),
            &mut result,
            Ranged::overlap_inclusive,
        );
        assert_eq!(result.len(), 2);

        // Test unsorted input handling
        let files1 = build_items(&[(5, 10), (1, 3)]); // Unsorted
        let files2 = build_items(&[(2, 7), (8, 12)]); // Unsorted
        find_overlapping_items(
            &mut SortedRun::from(files1),
            &mut SortedRun::from(files2),
            &mut result,
            Ranged::overlap_inclusive,
        );
        assert_eq!(result.len(), 4); // Should find both overlaps
    }

    #[test]
    fn test_file_overlap_time_overlap_pk_disjoint() {
        let lhs = new_file_handle_with_size_sequence_and_primary_key_range(
            FileId::random(),
            0,
            100,
            0,
            1,
            10,
            pk_range(b"a", b"f"),
        );
        let rhs = new_file_handle_with_size_sequence_and_primary_key_range(
            FileId::random(),
            50,
            150,
            0,
            2,
            10,
            pk_range(b"x", b"z"),
        );

        assert!(!files_overlap(&lhs, &rhs, &primary_key_mapper_for_test()));
    }

    #[test]
    fn test_find_sorted_runs_collapses_pk_disjoint_files_into_one_run() {
        let mut files = vec![
            new_file_handle_with_size_sequence_and_primary_key_range(
                FileId::random(),
                0,
                100,
                0,
                1,
                10,
                pk_range(b"a", b"f"),
            ),
            new_file_handle_with_size_sequence_and_primary_key_range(
                FileId::random(),
                50,
                150,
                0,
                2,
                10,
                pk_range(b"x", b"z"),
            ),
        ];

        let ranges = primary_key_mapper_for_test();
        let runs = find_sorted_runs(&mut files, |lhs, rhs| files_overlap(lhs, rhs, &ranges));

        assert_eq!(1, runs.len());
        assert_eq!(2, runs[0].items().len());
    }

    #[test]
    fn test_find_sorted_runs_handles_2d_transitivity_break() {
        let mut files = vec![
            new_file_handle_with_size_sequence_and_primary_key_range(
                FileId::random(),
                0,
                100,
                0,
                1,
                10,
                pk_range(b"a", b"f"),
            ),
            new_file_handle_with_size_sequence_and_primary_key_range(
                FileId::random(),
                50,
                150,
                0,
                2,
                10,
                pk_range(b"x", b"z"),
            ),
            new_file_handle_with_size_sequence_and_primary_key_range(
                FileId::random(),
                50,
                150,
                0,
                3,
                10,
                pk_range(b"a", b"f"),
            ),
        ];

        let ranges = primary_key_mapper_for_test();
        let runs = find_sorted_runs(&mut files, |lhs, rhs| files_overlap(lhs, rhs, &ranges));

        assert_eq!(2, runs.len());
        assert_eq!(2, runs[0].items().len());
        assert_eq!(1, runs[1].items().len());
    }

    #[test]
    fn test_find_overlapping_items_skips_pk_disjoint_pairs() {
        let mut left = SortedRun::from(vec![
            new_file_handle_with_size_sequence_and_primary_key_range(
                FileId::random(),
                0,
                100,
                0,
                1,
                10,
                pk_range(b"a", b"f"),
            ),
        ]);
        let mut right = SortedRun::from(vec![
            new_file_handle_with_size_sequence_and_primary_key_range(
                FileId::random(),
                50,
                150,
                0,
                2,
                10,
                pk_range(b"x", b"z"),
            ),
        ]);
        let mut result = Vec::new();

        let ranges = primary_key_mapper_for_test();
        find_overlapping_items(&mut left, &mut right, &mut result, |lhs, rhs| {
            files_overlap_inclusive(lhs, rhs, &ranges)
        });

        assert!(result.is_empty());
    }

    #[test]
    fn test_file_touching_time_boundary_with_same_pk_is_not_overlap() {
        let lhs = new_file_handle_with_size_sequence_and_primary_key_range(
            FileId::random(),
            0,
            100,
            0,
            1,
            10,
            pk_range(b"a", b"f"),
        );
        let rhs = new_file_handle_with_size_sequence_and_primary_key_range(
            FileId::random(),
            100,
            150,
            0,
            2,
            10,
            pk_range(b"a", b"f"),
        );

        assert!(!files_overlap(&lhs, &rhs, &primary_key_mapper_for_test()));
    }
}
