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
use std::collections::BinaryHeap;

use bytes::{Buf, Bytes};
use common_base::BitVec;
use common_time::Timestamp;
use smallvec::{SmallVec, smallvec};

use crate::sst::file::{FileHandle, RegionFileId};

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
            let (rhs_start, _rhs_end) = r.items[j].range();

            // If right element starts after left element ends, no more overlaps possible
            if rhs_start > lhs_end {
                break;
            }

            // We have an overlap (inclusive: touching boundaries count)
            if lhs.overlap_inclusive(&r.items[j]) {
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
    primary_key_range: Option<(Bytes, Bytes)>,
}

impl FileGroup {
    pub(crate) fn new_with_file(file: FileHandle) -> Self {
        let size = file.size() as usize;
        let (min_timestamp, max_timestamp) = file.time_range();
        let num_rows = file.num_rows();
        let primary_key_range = file.primary_key_range();
        Self {
            files: smallvec![file],
            size,
            num_rows,
            min_timestamp,
            max_timestamp,
            primary_key_range,
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
        self.primary_key_range =
            merge_primary_key_ranges(self.primary_key_range.take(), file.primary_key_range());
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

    fn overlap(&self, other: &Self) -> bool {
        let (lhs_start, lhs_end) = self.range();
        let (rhs_start, rhs_end) = other.range();
        if lhs_start.max(rhs_start) >= lhs_end.min(rhs_end) {
            return false;
        }

        match (&self.primary_key_range, &other.primary_key_range) {
            (Some(lhs), Some(rhs)) => primary_key_ranges_overlap(lhs, rhs),
            _ => true,
        }
    }

    fn overlap_inclusive(&self, other: &Self) -> bool {
        let (lhs_start, lhs_end) = self.range();
        let (rhs_start, rhs_end) = other.range();
        if lhs_start.max(rhs_start) > lhs_end.min(rhs_end) {
            return false;
        }

        match (&self.primary_key_range, &other.primary_key_range) {
            (Some(lhs), Some(rhs)) => primary_key_ranges_overlap(lhs, rhs),
            _ => true,
        }
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
    let mut active_run_item_indices = Vec::new();

    let mut selection = BitVec::repeat(false, items.len());
    while !selection.all() {
        // until all items are assigned to some sorted run.
        let mut last_pruned_start = None;
        for (item, mut selected) in items.iter().zip(selection.iter_mut()) {
            if *selected {
                // item is already assigned.
                continue;
            }
            if current_run.items.is_empty() {
                // current run is empty, just add current_item
                selected.set(true);
                current_run.push_item(item.clone());
                active_run_item_indices.push(current_run.items.len() - 1);
            } else {
                // the current item does not overlap with any item in current run,
                // then it belongs to current run. Because now we introduced primary
                // key range, we cannot simply use timestamps to check overlapping.
                let (item_start, _) = item.range();
                if last_pruned_start != Some(item_start) {
                    active_run_item_indices.retain(|idx| {
                        let (_, run_item_end) = current_run.items[*idx].range();
                        run_item_end > item_start
                    });
                    last_pruned_start = Some(item_start);
                }

                let mut overlaps_any = false;
                for idx in &active_run_item_indices {
                    let run_item = &current_run.items[*idx];
                    if run_item.overlap(item) {
                        overlaps_any = true;
                        break;
                    }
                }
                if !overlaps_any {
                    // does not overlap, push to current run
                    selected.set(true);
                    let item_idx = current_run.items.len();
                    current_run.push_item(item.clone());
                    active_run_item_indices.push(item_idx);
                }
            }
        }
        // finished an iteration, we've found a new run.
        runs.push(std::mem::take(&mut current_run));
        active_run_item_indices.clear();
    }
    runs
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

pub(crate) fn find_sorted_runs_by_time_range<T>(items: &mut [T]) -> Vec<SortedRun<T>>
where
    T: Item,
{
    if items.is_empty() {
        return vec![];
    }
    sort_ranged_items(items);

    use derive_more::{Eq, PartialEq};

    /// `SortedRun` with a creation sequence `i`.
    #[derive(PartialEq, Eq)]
    struct Run<T: Item> {
        i: usize,
        #[partial_eq(skip)]
        run: SortedRun<T>,
    }

    impl<T: Item> Run<T> {
        fn new(i: usize, item: &T) -> Run<T> {
            let mut run = SortedRun::default();
            run.push_item(item.clone());
            Run { i, run }
        }

        fn push_item(&mut self, item: &T) {
            self.run.push_item(item.clone());
        }
    }

    impl<T: Item> PartialOrd for Run<T> {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            Some(self.cmp(other))
        }
    }

    /// Sort by run's `end` desc then `start` asc.
    impl<T: Item> Ord for Run<T> {
        fn cmp(&self, other: &Self) -> Ordering {
            let l_run = &self.run;
            let r_run = &other.run;

            // Safety: `start` and `end` must both exist because it's guaranteed that whenever a
            // `Run` is created, an item is pushed into it immediately (see its `new` method above).
            // And there are no other ways to create a `Run` beyond its `new` method in this
            // function's scope.
            let l_end = l_run.end.unwrap();
            let r_end = r_run.end.unwrap();
            r_end
                .cmp(&l_end)
                .then_with(|| {
                    let l_start = l_run.start.unwrap();
                    let r_start = r_run.start.unwrap();
                    l_start.cmp(&r_start)
                })
                .then_with(|| self.i.cmp(&other.i))
        }
    }

    /// Wrapper around the `Run` above, to support sorting them by their creation sequence `i`.
    #[derive(PartialEq, Eq)]
    struct Wrapper<T: Item>(Run<T>);

    impl<T: Item> PartialOrd for Wrapper<T> {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            Some(self.cmp(other))
        }
    }

    impl<T: Item> Ord for Wrapper<T> {
        fn cmp(&self, other: &Self) -> Ordering {
            other.0.i.cmp(&self.0.i)
        }
    }

    // Two heaps for finding a run that is both:
    // 1. not overlapping with item's range,
    // 2. and is created earliest,
    // when iterating the items.
    //
    // Heap 1 (`runs_sorted_by_end`) is for storing the runs of which top has the minimal "end"
    // just about to overlap with the current selected item.
    //
    // Heap 2 (`runs_sort_by_index`) is for storing the runs that all have "end"s non-overlap with
    // the current selected item, and of which top is the earliest created run.
    //
    // The finding of a suitable run basically works like this:
    // 1. moves the runs in heap 1 to heap 2, until the top is overlapping with the current item;
    // 2. now heap 2 has all the runs that can accept the current item, pop its top;
    // 3. the top is the earliest created run, push the current item;
    // 4. because the run has changed, push it back to heap 1;
    // 5. check the next item. Important: we don't need to push the runs in heap 2 to 1, because
    //    the items are sorted by "start". When checking the next item, heap 2's runs must all have
    //    "end"s smaller than next item's "start".
    //
    // Actually the heap 2 is only for aligning with the runs selection outcomes in the original
    // `find_sorted_runs` implementation. If we just need the invariant that each run has the
    // non-overlapping items, we can get rid of heap 2 and make the codes simpler.

    let mut runs_sort_by_end = BinaryHeap::<Run<T>>::new();
    let mut runs_sort_by_index = BinaryHeap::<Wrapper<T>>::new();
    let mut i = 0;

    for item in items {
        let (start, _) = item.range();

        while let Some(run) = runs_sort_by_end.pop_if(|x| x.run.end.unwrap() <= start) {
            runs_sort_by_index.push(Wrapper(run));
        }

        let Some(mut run) = runs_sort_by_index.pop() else {
            i += 1;
            runs_sort_by_end.push(Run::new(i, item));
            continue;
        };

        run.0.push_item(item);
        runs_sort_by_end.push(run.0);
    }

    let mut runs = runs_sort_by_end.into_vec();
    runs.extend(runs_sort_by_index.into_vec().into_iter().map(|x| x.0));
    runs.sort_unstable_by_key(|run| run.i);
    runs.into_iter().map(|x| x.run).collect()
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use store_api::storage::FileId;

    use super::*;
    use crate::compaction::test_util::new_file_handle_with_size_sequence_and_primary_key_range;

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

    fn pk_range(min: &'static [u8], max: &'static [u8]) -> Option<(Bytes, Bytes)> {
        Some((Bytes::from_static(min), Bytes::from_static(max)))
    }

    fn check_sorted_runs(
        ranges: &[(i64, i64)],
        expected_runs: &[Vec<(i64, i64)>],
    ) -> Vec<SortedRun<MockFile>> {
        let mut files = build_items(ranges);
        let mut files_clone = files.clone();

        let runs = find_sorted_runs(&mut files);

        let result_file_ranges: Vec<Vec<_>> = runs
            .iter()
            .map(|r| r.items.iter().map(|f| f.range()).collect())
            .collect();
        assert_eq!(&expected_runs, &result_file_ranges);

        let runs_by_time_range = find_sorted_runs_by_time_range(&mut files_clone);
        let results: Vec<Vec<_>> = runs_by_time_range
            .iter()
            .map(|r| r.items.iter().map(|f| f.range()).collect())
            .collect();
        assert_eq!(&expected_runs, &results);
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

        let runs = find_sorted_runs(&mut files);
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
    fn test_file_group_overlap_time_overlap_pk_disjoint() {
        let lhs =
            FileGroup::new_with_file(new_file_handle_with_size_sequence_and_primary_key_range(
                FileId::random(),
                0,
                100,
                0,
                1,
                10,
                pk_range(b"a", b"f"),
            ));
        let rhs =
            FileGroup::new_with_file(new_file_handle_with_size_sequence_and_primary_key_range(
                FileId::random(),
                50,
                150,
                0,
                2,
                10,
                pk_range(b"x", b"z"),
            ));

        assert!(!lhs.overlap(&rhs));
    }

    #[test]
    fn test_find_sorted_runs_collapses_pk_disjoint_files_into_one_run() {
        let mut files = vec![
            FileGroup::new_with_file(new_file_handle_with_size_sequence_and_primary_key_range(
                FileId::random(),
                0,
                100,
                0,
                1,
                10,
                pk_range(b"a", b"f"),
            )),
            FileGroup::new_with_file(new_file_handle_with_size_sequence_and_primary_key_range(
                FileId::random(),
                50,
                150,
                0,
                2,
                10,
                pk_range(b"x", b"z"),
            )),
        ];

        let runs = find_sorted_runs(&mut files);

        assert_eq!(1, runs.len());
        assert_eq!(2, runs[0].items().len());
    }

    #[test]
    fn test_find_sorted_runs_handles_2d_transitivity_break() {
        let mut files = vec![
            FileGroup::new_with_file(new_file_handle_with_size_sequence_and_primary_key_range(
                FileId::random(),
                0,
                100,
                0,
                1,
                10,
                pk_range(b"a", b"f"),
            )),
            FileGroup::new_with_file(new_file_handle_with_size_sequence_and_primary_key_range(
                FileId::random(),
                50,
                150,
                0,
                2,
                10,
                pk_range(b"x", b"z"),
            )),
            FileGroup::new_with_file(new_file_handle_with_size_sequence_and_primary_key_range(
                FileId::random(),
                50,
                150,
                0,
                3,
                10,
                pk_range(b"a", b"f"),
            )),
        ];

        let runs = find_sorted_runs(&mut files);

        assert_eq!(2, runs.len());
        assert_eq!(2, runs[0].items().len());
        assert_eq!(1, runs[1].items().len());
    }

    #[test]
    fn test_find_overlapping_items_skips_pk_disjoint_pairs() {
        let mut left = SortedRun::from(vec![FileGroup::new_with_file(
            new_file_handle_with_size_sequence_and_primary_key_range(
                FileId::random(),
                0,
                100,
                0,
                1,
                10,
                pk_range(b"a", b"f"),
            ),
        )]);
        let mut right = SortedRun::from(vec![FileGroup::new_with_file(
            new_file_handle_with_size_sequence_and_primary_key_range(
                FileId::random(),
                50,
                150,
                0,
                2,
                10,
                pk_range(b"x", b"z"),
            ),
        )]);
        let mut result = Vec::new();

        find_overlapping_items(&mut left, &mut right, &mut result);

        assert!(result.is_empty());
    }

    #[test]
    fn test_file_group_touching_time_boundary_with_same_pk_is_not_overlap() {
        let lhs =
            FileGroup::new_with_file(new_file_handle_with_size_sequence_and_primary_key_range(
                FileId::random(),
                0,
                100,
                0,
                1,
                10,
                pk_range(b"a", b"f"),
            ));
        let rhs =
            FileGroup::new_with_file(new_file_handle_with_size_sequence_and_primary_key_range(
                FileId::random(),
                100,
                150,
                0,
                2,
                10,
                pk_range(b"a", b"f"),
            ));

        assert!(!lhs.overlap(&rhs));
    }
}
