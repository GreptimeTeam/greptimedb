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

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;

use index::inverted_index::search::index_apply::ApplyOutput;
use itertools::Itertools;
use parquet::arrow::arrow_reader::{RowSelection, RowSelector};

/// A selection of row groups.
#[derive(Debug, Clone, Default)]
pub struct RowGroupSelection {
    /// Row group id to row selection.
    selection_in_rg: BTreeMap<usize, RowSelectionWithCount>,
    /// Total number of rows in the selection.
    row_count: usize,
    /// Total length of the selectors.
    selector_len: usize,
}

/// A row selection with its count.
#[derive(Debug, Clone)]
struct RowSelectionWithCount {
    /// Row selection.
    selection: RowSelection,
    /// Number of rows in the selection.
    row_count: usize,
    /// Length of the selectors.
    selector_len: usize,
}

impl RowGroupSelection {
    /// Creates a new `RowGroupSelection` with all row groups selected.
    ///
    /// # Arguments
    /// * `row_group_size` - The number of rows in each row group (except possibly the last one)
    /// * `total_row_count` - Total number of rows
    pub fn new(row_group_size: usize, total_row_count: usize) -> Self {
        let mut selection_in_rg = BTreeMap::new();

        let row_group_count = total_row_count.div_ceil(row_group_size);
        for rg_id in 0..row_group_count {
            // The last row group may have fewer rows than `row_group_size`
            let row_group_size = if rg_id == row_group_count - 1 {
                total_row_count - (row_group_count - 1) * row_group_size
            } else {
                row_group_size
            };

            let selection = RowSelection::from(vec![RowSelector::select(row_group_size)]);
            selection_in_rg.insert(
                rg_id,
                RowSelectionWithCount {
                    selection,
                    row_count: row_group_size,
                    selector_len: 1,
                },
            );
        }

        Self {
            selection_in_rg,
            row_count: total_row_count,
            selector_len: row_group_count,
        }
    }

    /// Returns the row selection for a given row group.
    ///
    /// `None` indicates not selected.
    pub fn get(&self, rg_id: usize) -> Option<&RowSelection> {
        self.selection_in_rg.get(&rg_id).map(|x| &x.selection)
    }

    /// Creates a new `RowGroupSelection` from the output of inverted index application.
    ///
    /// # Arguments
    /// * `row_group_size` - The number of rows in each row group (except possibly the last one)
    /// * `apply_output` - The output from applying the inverted index
    ///
    /// # Assumptions
    /// * All row groups (except possibly the last one) have the same number of rows
    /// * The last row group may have fewer rows than `row_group_size`
    pub fn from_inverted_index_apply_output(
        row_group_size: usize,
        apply_output: ApplyOutput,
    ) -> Self {
        // Step 1: Convert segment IDs to row ranges within row groups
        // For each segment ID, calculate its corresponding row range in the row group
        let segment_row_count = apply_output.segment_row_count;
        let row_group_ranges = apply_output.matched_segment_ids.iter_ones().map(|seg_id| {
            // Calculate the global row ID where this segment starts
            let begin_row_id = seg_id * segment_row_count;
            // Determine which row group this segment belongs to
            let row_group_id = begin_row_id / row_group_size;
            // Calculate the offset within the row group
            let rg_begin_row_id = begin_row_id % row_group_size;
            // Ensure the end row ID doesn't exceed the row group size
            let rg_end_row_id = (rg_begin_row_id + segment_row_count).min(row_group_size);

            (row_group_id, rg_begin_row_id..rg_end_row_id)
        });

        // Step 2: Group ranges by row group ID and create row selections
        let mut total_row_count = 0;
        let mut total_selector_len = 0;
        let selection_in_rg = row_group_ranges
            .chunk_by(|(row_group_id, _)| *row_group_id)
            .into_iter()
            .map(|(row_group_id, group)| {
                // Extract just the ranges from the group
                let ranges = group.map(|(_, ranges)| ranges);
                // Create row selection from the ranges
                // Note: We use `row_group_size` here, which is safe because:
                // 1. For non-last row groups, it's the actual size
                // 2. For the last row group, any ranges beyond the actual size will be clipped
                //    by the min() operation above
                let selection = row_selection_from_row_ranges(ranges, row_group_size);
                let row_count = selection.row_count();
                let selector_len = selector_len(&selection);
                total_row_count += row_count;
                total_selector_len += selector_len;
                (
                    row_group_id,
                    RowSelectionWithCount {
                        selection,
                        row_count,
                        selector_len,
                    },
                )
            })
            .collect();

        Self {
            selection_in_rg,
            row_count: total_row_count,
            selector_len: total_selector_len,
        }
    }

    /// Creates a new `RowGroupSelection` from a set of row IDs.
    ///
    /// # Arguments
    /// * `row_ids` - Set of row IDs to select
    /// * `row_group_size` - The number of rows in each row group (except possibly the last one)
    /// * `num_row_groups` - Total number of row groups
    ///
    /// # Assumptions
    /// * All row groups (except possibly the last one) have the same number of rows
    /// * The last row group may have fewer rows than `row_group_size`
    /// * All row IDs must within the range of [0, num_row_groups * row_group_size)
    pub fn from_row_ids(
        row_ids: BTreeSet<u32>,
        row_group_size: usize,
        num_row_groups: usize,
    ) -> Self {
        // Step 1: Group row IDs by their row group
        let row_group_to_row_ids =
            Self::group_row_ids_by_row_group(row_ids, row_group_size, num_row_groups);

        // Step 2: Create row selections for each row group
        let mut total_row_count = 0;
        let mut total_selector_len = 0;
        let selection_in_rg = row_group_to_row_ids
            .into_iter()
            .map(|(row_group_id, row_ids)| {
                let selection =
                    row_selection_from_sorted_row_ids(row_ids.into_iter(), row_group_size);
                let row_count = selection.row_count();
                let selector_len = selector_len(&selection);
                total_row_count += row_count;
                total_selector_len += selector_len;
                (
                    row_group_id,
                    RowSelectionWithCount {
                        selection,
                        row_count,
                        selector_len,
                    },
                )
            })
            .collect();

        Self {
            selection_in_rg,
            row_count: total_row_count,
            selector_len: total_selector_len,
        }
    }

    /// Creates a new `RowGroupSelection` from a set of row ranges.
    ///
    /// # Arguments
    /// * `row_ranges` - A vector of (row_group_id, row_ranges) pairs
    /// * `row_group_size` - The number of rows in each row group (except possibly the last one)
    ///
    /// # Assumptions
    /// * All row groups (except possibly the last one) have the same number of rows
    /// * The last row group may have fewer rows than `row_group_size`
    /// * All ranges in `row_ranges` must be within the bounds of their respective row groups
    ///   (i.e., for row group i, all ranges must be within [0, row_group_size) or [0, remaining_rows) for the last row group)
    /// * Ranges within the same row group must not overlap. Overlapping ranges will result in undefined behavior.
    pub fn from_row_ranges(
        row_ranges: Vec<(usize, Vec<Range<usize>>)>,
        row_group_size: usize,
    ) -> Self {
        let mut total_row_count = 0;
        let mut total_selector_len = 0;
        let selection_in_rg = row_ranges
            .into_iter()
            .map(|(row_group_id, ranges)| {
                let selection = row_selection_from_row_ranges(ranges.into_iter(), row_group_size);
                let row_count = selection.row_count();
                let selector_len = selector_len(&selection);
                total_row_count += row_count;
                total_selector_len += selector_len;
                (
                    row_group_id,
                    RowSelectionWithCount {
                        selection,
                        row_count,
                        selector_len,
                    },
                )
            })
            .collect();

        Self {
            selection_in_rg,
            row_count: total_row_count,
            selector_len: total_selector_len,
        }
    }

    /// Groups row IDs by their row group.
    ///
    /// # Arguments
    /// * `row_ids` - Set of row IDs to group
    /// * `row_group_size` - Size of each row group
    /// * `num_row_groups` - Total number of row groups
    ///
    /// # Returns
    /// A vector of (row_group_id, row_ids) pairs, where row_ids are the IDs within that row group.
    fn group_row_ids_by_row_group(
        row_ids: BTreeSet<u32>,
        row_group_size: usize,
        num_row_groups: usize,
    ) -> Vec<(usize, Vec<usize>)> {
        let est_rows_per_group = row_ids.len() / num_row_groups;
        let mut row_group_to_row_ids: Vec<(usize, Vec<usize>)> = Vec::with_capacity(num_row_groups);

        for row_id in row_ids {
            let row_group_id = row_id as usize / row_group_size;
            let row_id_in_group = row_id as usize % row_group_size;

            if let Some((rg_id, row_ids)) = row_group_to_row_ids.last_mut()
                && *rg_id == row_group_id
            {
                row_ids.push(row_id_in_group);
            } else {
                let mut row_ids = Vec::with_capacity(est_rows_per_group);
                row_ids.push(row_id_in_group);
                row_group_to_row_ids.push((row_group_id, row_ids));
            }
        }

        row_group_to_row_ids
    }

    /// Intersects two `RowGroupSelection`s.
    pub fn intersect(&self, other: &Self) -> Self {
        let mut res = BTreeMap::new();
        let mut total_row_count = 0;
        let mut total_selector_len = 0;

        for (rg_id, x) in other.selection_in_rg.iter() {
            let Some(y) = self.selection_in_rg.get(rg_id) else {
                continue;
            };
            let selection = x.selection.intersection(&y.selection);
            let row_count = selection.row_count();
            let selector_len = selector_len(&selection);
            if row_count > 0 {
                total_row_count += row_count;
                total_selector_len += selector_len;
                res.insert(
                    *rg_id,
                    RowSelectionWithCount {
                        selection,
                        row_count,
                        selector_len,
                    },
                );
            }
        }

        Self {
            selection_in_rg: res,
            row_count: total_row_count,
            selector_len: total_selector_len,
        }
    }

    /// Returns the number of row groups in the selection.
    pub fn row_group_count(&self) -> usize {
        self.selection_in_rg.len()
    }

    /// Returns the number of rows in the selection.
    pub fn row_count(&self) -> usize {
        self.row_count
    }

    /// Returns the first row group in the selection.
    pub fn pop_first(&mut self) -> Option<(usize, RowSelection)> {
        let (
            row_group_id,
            RowSelectionWithCount {
                selection,
                row_count,
                selector_len,
            },
        ) = self.selection_in_rg.pop_first()?;

        self.row_count -= row_count;
        self.selector_len -= selector_len;
        Some((row_group_id, selection))
    }

    /// Removes a row group from the selection.
    pub fn remove_row_group(&mut self, row_group_id: usize) {
        let Some(RowSelectionWithCount {
            row_count,
            selector_len,
            ..
        }) = self.selection_in_rg.remove(&row_group_id)
        else {
            return;
        };
        self.row_count -= row_count;
        self.selector_len -= selector_len;
    }

    /// Returns true if the selection is empty.
    pub fn is_empty(&self) -> bool {
        self.selection_in_rg.is_empty()
    }

    /// Returns true if the selection contains a row group with the given ID.
    pub fn contains_row_group(&self, row_group_id: usize) -> bool {
        self.selection_in_rg.contains_key(&row_group_id)
    }

    /// Returns an iterator over the row groups in the selection.
    pub fn iter(&self) -> impl Iterator<Item = (&usize, &RowSelection)> {
        self.selection_in_rg
            .iter()
            .map(|(row_group_id, x)| (row_group_id, &x.selection))
    }

    /// Returns the memory usage of the selection.
    pub fn mem_usage(&self) -> usize {
        self.selector_len * size_of::<RowSelector>()
            + self.selection_in_rg.len() * size_of::<RowSelectionWithCount>()
    }
}

/// Converts an iterator of row ranges into a `RowSelection` by creating a sequence of `RowSelector`s.
///
/// This function processes each range in the input and either creates a new selector or merges
/// with the existing one, depending on whether the current range is contiguous with the preceding one
/// or if there's a gap that requires skipping rows. It handles both "select" and "skip" actions,
/// optimizing the list of selectors by merging contiguous actions of the same type.
///
/// Note: overlapping ranges are not supported and will result in an incorrect selection.
pub(crate) fn row_selection_from_row_ranges(
    row_ranges: impl Iterator<Item = Range<usize>>,
    total_row_count: usize,
) -> RowSelection {
    let mut selectors: Vec<RowSelector> = Vec::new();
    let mut last_processed_end = 0;

    for Range { start, end } in row_ranges {
        let end = end.min(total_row_count);
        if start > last_processed_end {
            add_or_merge_selector(&mut selectors, start - last_processed_end, true);
        }

        add_or_merge_selector(&mut selectors, end - start, false);
        last_processed_end = end;
    }

    if last_processed_end < total_row_count {
        add_or_merge_selector(&mut selectors, total_row_count - last_processed_end, true);
    }

    RowSelection::from(selectors)
}

/// Converts an iterator of sorted row IDs into a `RowSelection`.
///
/// Note: the input iterator must be sorted in ascending order and
///       contain unique row IDs in the range [0, total_row_count).
pub(crate) fn row_selection_from_sorted_row_ids(
    row_ids: impl Iterator<Item = usize>,
    total_row_count: usize,
) -> RowSelection {
    let mut selectors: Vec<RowSelector> = Vec::new();
    let mut last_processed_end = 0;

    for row_id in row_ids {
        let start = row_id;
        let end = start + 1;

        if start > last_processed_end {
            add_or_merge_selector(&mut selectors, start - last_processed_end, true);
        }

        add_or_merge_selector(&mut selectors, end - start, false);
        last_processed_end = end;
    }

    if last_processed_end < total_row_count {
        add_or_merge_selector(&mut selectors, total_row_count - last_processed_end, true);
    }

    RowSelection::from(selectors)
}

/// Helper function to either add a new `RowSelector` to `selectors` or merge it with the last one
/// if they are of the same type (both skip or both select).
fn add_or_merge_selector(selectors: &mut Vec<RowSelector>, count: usize, is_skip: bool) {
    if let Some(last) = selectors.last_mut() {
        // Merge with last if both actions are same
        if last.skip == is_skip {
            last.row_count += count;
            return;
        }
    }
    // Add new selector otherwise
    let new_selector = if is_skip {
        RowSelector::skip(count)
    } else {
        RowSelector::select(count)
    };
    selectors.push(new_selector);
}

/// Returns the length of the selectors in the selection.
fn selector_len(selection: &RowSelection) -> usize {
    selection.iter().size_hint().0
}

#[cfg(test)]
#[allow(clippy::single_range_in_vec_init)]
mod tests {
    use super::*;

    #[test]
    fn test_selector_len() {
        let selection = RowSelection::from(vec![RowSelector::skip(5), RowSelector::select(5)]);
        assert_eq!(selector_len(&selection), 2);

        let selection = RowSelection::from(vec![
            RowSelector::select(5),
            RowSelector::skip(5),
            RowSelector::select(5),
        ]);
        assert_eq!(selector_len(&selection), 3);

        let selection = RowSelection::from(vec![]);
        assert_eq!(selector_len(&selection), 0);
    }

    #[test]
    fn test_single_contiguous_range() {
        let selection = row_selection_from_row_ranges(Some(5..10).into_iter(), 10);
        let expected = RowSelection::from(vec![RowSelector::skip(5), RowSelector::select(5)]);
        assert_eq!(selection, expected);
    }

    #[test]
    fn test_non_contiguous_ranges() {
        let ranges = [1..3, 5..8];
        let selection = row_selection_from_row_ranges(ranges.iter().cloned(), 10);
        let expected = RowSelection::from(vec![
            RowSelector::skip(1),
            RowSelector::select(2),
            RowSelector::skip(2),
            RowSelector::select(3),
            RowSelector::skip(2),
        ]);
        assert_eq!(selection, expected);
    }

    #[test]
    fn test_empty_range() {
        let ranges = [];
        let selection = row_selection_from_row_ranges(ranges.iter().cloned(), 10);
        let expected = RowSelection::from(vec![RowSelector::skip(10)]);
        assert_eq!(selection, expected);
    }

    #[test]
    fn test_adjacent_ranges() {
        let ranges = [1..2, 2..3];
        let selection = row_selection_from_row_ranges(ranges.iter().cloned(), 10);
        let expected = RowSelection::from(vec![
            RowSelector::skip(1),
            RowSelector::select(2),
            RowSelector::skip(7),
        ]);
        assert_eq!(selection, expected);
    }

    #[test]
    fn test_large_gap_between_ranges() {
        let ranges = [1..2, 100..101];
        let selection = row_selection_from_row_ranges(ranges.iter().cloned(), 10240);
        let expected = RowSelection::from(vec![
            RowSelector::skip(1),
            RowSelector::select(1),
            RowSelector::skip(98),
            RowSelector::select(1),
            RowSelector::skip(10139),
        ]);
        assert_eq!(selection, expected);
    }

    #[test]
    fn test_range_end_over_total_row_count() {
        let ranges = Some(1..10);
        let selection = row_selection_from_row_ranges(ranges.into_iter(), 5);
        let expected = RowSelection::from(vec![RowSelector::skip(1), RowSelector::select(4)]);
        assert_eq!(selection, expected);
    }

    #[test]
    fn test_row_ids_to_selection() {
        let row_ids = [1, 3, 5, 7, 9].into_iter();
        let selection = row_selection_from_sorted_row_ids(row_ids, 10);
        let expected = RowSelection::from(vec![
            RowSelector::skip(1),
            RowSelector::select(1),
            RowSelector::skip(1),
            RowSelector::select(1),
            RowSelector::skip(1),
            RowSelector::select(1),
            RowSelector::skip(1),
            RowSelector::select(1),
            RowSelector::skip(1),
            RowSelector::select(1),
        ]);
        assert_eq!(selection, expected);
    }

    #[test]
    fn test_row_ids_to_selection_full() {
        let row_ids = 0..10;
        let selection = row_selection_from_sorted_row_ids(row_ids, 10);
        let expected = RowSelection::from(vec![RowSelector::select(10)]);
        assert_eq!(selection, expected);
    }

    #[test]
    fn test_row_ids_to_selection_empty() {
        let selection = row_selection_from_sorted_row_ids(None.into_iter(), 10);
        let expected = RowSelection::from(vec![RowSelector::skip(10)]);
        assert_eq!(selection, expected);
    }

    #[test]
    fn test_group_row_ids() {
        let row_ids = [0, 1, 2, 5, 6, 7, 8, 12].into_iter().collect();
        let row_group_size = 5;
        let num_row_groups = 3;

        let row_group_to_row_ids =
            RowGroupSelection::group_row_ids_by_row_group(row_ids, row_group_size, num_row_groups);

        assert_eq!(
            row_group_to_row_ids,
            vec![(0, vec![0, 1, 2]), (1, vec![0, 1, 2, 3]), (2, vec![2])]
        );
    }

    #[test]
    fn test_row_group_selection_new() {
        // Test with regular case
        let selection = RowGroupSelection::new(100, 250);
        assert_eq!(selection.row_count(), 250);
        assert_eq!(selection.row_group_count(), 3);

        // Check content of each row group
        let row_selection = selection.get(0).unwrap();
        assert_eq!(row_selection.row_count(), 100);

        let row_selection = selection.get(1).unwrap();
        assert_eq!(row_selection.row_count(), 100);

        let row_selection = selection.get(2).unwrap();
        assert_eq!(row_selection.row_count(), 50);

        // Test with empty selection
        let selection = RowGroupSelection::new(100, 0);
        assert_eq!(selection.row_count(), 0);
        assert_eq!(selection.row_group_count(), 0);
        assert!(selection.get(0).is_none());

        // Test with single row group
        let selection = RowGroupSelection::new(100, 50);
        assert_eq!(selection.row_count(), 50);
        assert_eq!(selection.row_group_count(), 1);

        let row_selection = selection.get(0).unwrap();
        assert_eq!(row_selection.row_count(), 50);

        // Test with row count that doesn't divide evenly
        let selection = RowGroupSelection::new(100, 150);
        assert_eq!(selection.row_count(), 150);
        assert_eq!(selection.row_group_count(), 2);

        let row_selection = selection.get(0).unwrap();
        assert_eq!(row_selection.row_count(), 100);

        let row_selection = selection.get(1).unwrap();
        assert_eq!(row_selection.row_count(), 50);

        // Test with row count that's just over a multiple of row_group_size
        let selection = RowGroupSelection::new(100, 101);
        assert_eq!(selection.row_count(), 101);
        assert_eq!(selection.row_group_count(), 2);

        let row_selection = selection.get(0).unwrap();
        assert_eq!(row_selection.row_count(), 100);

        let row_selection = selection.get(1).unwrap();
        assert_eq!(row_selection.row_count(), 1);
    }

    #[test]
    fn test_from_row_ids() {
        let row_group_size = 100;
        let num_row_groups = 3;

        // Test with regular case
        let row_ids: BTreeSet<u32> = vec![5, 15, 25, 35, 105, 115, 205, 215]
            .into_iter()
            .collect();
        let selection = RowGroupSelection::from_row_ids(row_ids, row_group_size, num_row_groups);
        assert_eq!(selection.row_count(), 8);
        assert_eq!(selection.row_group_count(), 3);

        // Check content of each row group
        let row_selection = selection.get(0).unwrap();
        assert_eq!(row_selection.row_count(), 4); // 5, 15, 25, 35

        let row_selection = selection.get(1).unwrap();
        assert_eq!(row_selection.row_count(), 2); // 105, 115

        let row_selection = selection.get(2).unwrap();
        assert_eq!(row_selection.row_count(), 2); // 205, 215

        // Test with empty row IDs
        let empty_row_ids: BTreeSet<u32> = BTreeSet::new();
        let selection =
            RowGroupSelection::from_row_ids(empty_row_ids, row_group_size, num_row_groups);
        assert_eq!(selection.row_count(), 0);
        assert_eq!(selection.row_group_count(), 0);
        assert!(selection.get(0).is_none());

        // Test with consecutive row IDs
        let consecutive_row_ids: BTreeSet<u32> = vec![5, 6, 7, 8, 9].into_iter().collect();
        let selection =
            RowGroupSelection::from_row_ids(consecutive_row_ids, row_group_size, num_row_groups);
        assert_eq!(selection.row_count(), 5);
        assert_eq!(selection.row_group_count(), 1);

        let row_selection = selection.get(0).unwrap();
        assert_eq!(row_selection.row_count(), 5); // 5, 6, 7, 8, 9

        // Test with row IDs at row group boundaries
        let boundary_row_ids: BTreeSet<u32> = vec![0, 99, 100, 199, 200, 249].into_iter().collect();
        let selection =
            RowGroupSelection::from_row_ids(boundary_row_ids, row_group_size, num_row_groups);
        assert_eq!(selection.row_count(), 6);
        assert_eq!(selection.row_group_count(), 3);

        let row_selection = selection.get(0).unwrap();
        assert_eq!(row_selection.row_count(), 2); // 0, 99

        let row_selection = selection.get(1).unwrap();
        assert_eq!(row_selection.row_count(), 2); // 100, 199

        let row_selection = selection.get(2).unwrap();
        assert_eq!(row_selection.row_count(), 2); // 200, 249

        // Test with single row group
        let single_group_row_ids: BTreeSet<u32> = vec![5, 10, 15].into_iter().collect();
        let selection = RowGroupSelection::from_row_ids(single_group_row_ids, row_group_size, 1);
        assert_eq!(selection.row_count(), 3);
        assert_eq!(selection.row_group_count(), 1);

        let row_selection = selection.get(0).unwrap();
        assert_eq!(row_selection.row_count(), 3); // 5, 10, 15
    }

    #[test]
    fn test_from_row_ranges() {
        let row_group_size = 100;

        // Test with regular case
        let ranges = vec![
            (0, vec![5..15, 25..35]), // Within [0, 100)
            (1, vec![5..15]),         // Within [0, 100)
            (2, vec![0..5, 10..15]),  // Within [0, 50) for last row group
        ];
        let selection = RowGroupSelection::from_row_ranges(ranges, row_group_size);
        assert_eq!(selection.row_count(), 40);
        assert_eq!(selection.row_group_count(), 3);

        // Check content of each row group
        let row_selection = selection.get(0).unwrap();
        assert_eq!(row_selection.row_count(), 20); // 5..15 (10) + 25..35 (10)

        let row_selection = selection.get(1).unwrap();
        assert_eq!(row_selection.row_count(), 10); // 5..15 (10)

        let row_selection = selection.get(2).unwrap();
        assert_eq!(row_selection.row_count(), 10); // 0..5 (5) + 10..15 (5)

        // Test with empty ranges
        let empty_ranges: Vec<(usize, Vec<Range<usize>>)> = vec![];
        let selection = RowGroupSelection::from_row_ranges(empty_ranges, row_group_size);
        assert_eq!(selection.row_count(), 0);
        assert_eq!(selection.row_group_count(), 0);
        assert!(selection.get(0).is_none());

        // Test with adjacent ranges within same row group
        let adjacent_ranges = vec![
            (0, vec![5..15, 15..25]), // Adjacent ranges within [0, 100)
        ];
        let selection = RowGroupSelection::from_row_ranges(adjacent_ranges, row_group_size);
        assert_eq!(selection.row_count(), 20);
        assert_eq!(selection.row_group_count(), 1);

        let row_selection = selection.get(0).unwrap();
        assert_eq!(row_selection.row_count(), 20); // 5..15 (10) + 15..25 (10)

        // Test with ranges at row group boundaries
        let boundary_ranges = vec![
            (0, vec![0..10, 90..100]), // Ranges at start and end of first row group
            (1, vec![0..100]),         // Full range of second row group
            (2, vec![0..50]),          // Full range of last row group
        ];
        let selection = RowGroupSelection::from_row_ranges(boundary_ranges, row_group_size);
        assert_eq!(selection.row_count(), 170);
        assert_eq!(selection.row_group_count(), 3);

        let row_selection = selection.get(0).unwrap();
        assert_eq!(row_selection.row_count(), 20); // 0..10 (10) + 90..100 (10)

        let row_selection = selection.get(1).unwrap();
        assert_eq!(row_selection.row_count(), 100); // 0..100 (100)

        let row_selection = selection.get(2).unwrap();
        assert_eq!(row_selection.row_count(), 50); // 0..50 (50)

        // Test with single row group
        let single_group_ranges = vec![
            (0, vec![0..50]), // Half of first row group
        ];
        let selection = RowGroupSelection::from_row_ranges(single_group_ranges, row_group_size);
        assert_eq!(selection.row_count(), 50);
        assert_eq!(selection.row_group_count(), 1);

        let row_selection = selection.get(0).unwrap();
        assert_eq!(row_selection.row_count(), 50); // 0..50 (50)
    }

    #[test]
    fn test_intersect() {
        let row_group_size = 100;

        // Test case 1: Regular intersection with partial overlap
        let ranges1 = vec![
            (0, vec![5..15, 25..35]), // Within [0, 100)
            (1, vec![5..15]),         // Within [0, 100)
        ];
        let selection1 = RowGroupSelection::from_row_ranges(ranges1, row_group_size);

        let ranges2 = vec![
            (0, vec![10..20]), // Within [0, 100)
            (1, vec![10..20]), // Within [0, 100)
            (2, vec![0..10]),  // Within [0, 50) for last row group
        ];
        let selection2 = RowGroupSelection::from_row_ranges(ranges2, row_group_size);

        let intersection = selection1.intersect(&selection2);
        assert_eq!(intersection.row_count(), 10);
        assert_eq!(intersection.row_group_count(), 2);

        let row_selection = intersection.get(0).unwrap();
        assert_eq!(row_selection.row_count(), 5); // 10..15 (5)

        let row_selection = intersection.get(1).unwrap();
        assert_eq!(row_selection.row_count(), 5); // 10..15 (5)

        // Test case 2: Empty intersection with empty selection
        let empty_ranges: Vec<(usize, Vec<Range<usize>>)> = vec![];
        let empty_selection = RowGroupSelection::from_row_ranges(empty_ranges, row_group_size);
        let intersection = selection1.intersect(&empty_selection);
        assert_eq!(intersection.row_count(), 0);
        assert_eq!(intersection.row_group_count(), 0);
        assert!(intersection.get(0).is_none());

        // Test case 3: No overlapping row groups
        let non_overlapping_ranges = vec![
            (3, vec![0..10]), // Within [0, 50) for last row group
        ];
        let non_overlapping_selection =
            RowGroupSelection::from_row_ranges(non_overlapping_ranges, row_group_size);
        let intersection = selection1.intersect(&non_overlapping_selection);
        assert_eq!(intersection.row_count(), 0);
        assert_eq!(intersection.row_group_count(), 0);
        assert!(intersection.get(0).is_none());

        // Test case 4: Full overlap within same row group
        let full_overlap_ranges1 = vec![
            (0, vec![0..50]), // Within [0, 100)
        ];
        let full_overlap_ranges2 = vec![
            (0, vec![0..50]), // Within [0, 100)
        ];
        let selection1 = RowGroupSelection::from_row_ranges(full_overlap_ranges1, row_group_size);
        let selection2 = RowGroupSelection::from_row_ranges(full_overlap_ranges2, row_group_size);
        let intersection = selection1.intersect(&selection2);
        assert_eq!(intersection.row_count(), 50);
        assert_eq!(intersection.row_group_count(), 1);

        let row_selection = intersection.get(0).unwrap();
        assert_eq!(row_selection.row_count(), 50); // 0..50 (50)

        // Test case 5: Partial overlap at row group boundaries
        let boundary_ranges1 = vec![
            (0, vec![0..10, 90..100]), // Within [0, 100)
            (1, vec![0..100]),         // Within [0, 100)
        ];
        let boundary_ranges2 = vec![
            (0, vec![5..15, 95..100]), // Within [0, 100)
            (1, vec![50..100]),        // Within [0, 100)
        ];
        let selection1 = RowGroupSelection::from_row_ranges(boundary_ranges1, row_group_size);
        let selection2 = RowGroupSelection::from_row_ranges(boundary_ranges2, row_group_size);
        let intersection = selection1.intersect(&selection2);
        assert_eq!(intersection.row_count(), 60);
        assert_eq!(intersection.row_group_count(), 2);

        let row_selection = intersection.get(0).unwrap();
        assert_eq!(row_selection.row_count(), 10); // 5..10 (5) + 95..100 (5)

        let row_selection = intersection.get(1).unwrap();
        assert_eq!(row_selection.row_count(), 50); // 50..100 (50)

        // Test case 6: Multiple ranges with complex overlap
        let complex_ranges1 = vec![
            (0, vec![5..15, 25..35, 45..55]), // Within [0, 100)
            (1, vec![10..20, 30..40]),        // Within [0, 100)
        ];
        let complex_ranges2 = vec![
            (0, vec![10..20, 30..40, 50..60]), // Within [0, 100)
            (1, vec![15..25, 35..45]),         // Within [0, 100)
        ];
        let selection1 = RowGroupSelection::from_row_ranges(complex_ranges1, row_group_size);
        let selection2 = RowGroupSelection::from_row_ranges(complex_ranges2, row_group_size);
        let intersection = selection1.intersect(&selection2);
        assert_eq!(intersection.row_count(), 25);
        assert_eq!(intersection.row_group_count(), 2);

        let row_selection = intersection.get(0).unwrap();
        assert_eq!(row_selection.row_count(), 15); // 10..15 (5) + 30..35 (5) + 50..55 (5)

        let row_selection = intersection.get(1).unwrap();
        assert_eq!(row_selection.row_count(), 10); // 15..20 (5) + 35..40 (5)

        // Test case 7: Intersection with last row group (smaller size)
        let last_rg_ranges1 = vec![
            (2, vec![0..25, 30..40]), // Within [0, 50) for last row group
        ];
        let last_rg_ranges2 = vec![
            (2, vec![20..30, 35..45]), // Within [0, 50) for last row group
        ];
        let selection1 = RowGroupSelection::from_row_ranges(last_rg_ranges1, row_group_size);
        let selection2 = RowGroupSelection::from_row_ranges(last_rg_ranges2, row_group_size);
        let intersection = selection1.intersect(&selection2);
        assert_eq!(intersection.row_count(), 10);
        assert_eq!(intersection.row_group_count(), 1);

        let row_selection = intersection.get(2).unwrap();
        assert_eq!(row_selection.row_count(), 10); // 20..25 (5) + 35..40 (5)

        // Test case 8: Intersection with empty ranges in one selection
        let empty_ranges = vec![
            (0, vec![]),      // Empty ranges
            (1, vec![5..15]), // Within [0, 100)
        ];
        let selection1 = RowGroupSelection::from_row_ranges(empty_ranges, row_group_size);
        let ranges2 = vec![
            (0, vec![5..15, 25..35]), // Within [0, 100)
            (1, vec![5..15]),         // Within [0, 100)
        ];
        let selection2 = RowGroupSelection::from_row_ranges(ranges2, row_group_size);
        let intersection = selection1.intersect(&selection2);
        assert_eq!(intersection.row_count(), 10);
        assert_eq!(intersection.row_group_count(), 1);

        let row_selection = intersection.get(1).unwrap();
        assert_eq!(row_selection.row_count(), 10); // 5..15 (10)
    }

    #[test]
    fn test_pop_first() {
        let row_group_size = 100;
        let ranges = vec![
            (0, vec![5..15]), // Within [0, 100)
            (1, vec![5..15]), // Within [0, 100)
            (2, vec![0..5]),  // Within [0, 50) for last row group
        ];
        let mut selection = RowGroupSelection::from_row_ranges(ranges, row_group_size);

        // Test popping first row group
        let (rg_id, row_selection) = selection.pop_first().unwrap();
        assert_eq!(rg_id, 0);
        assert_eq!(row_selection.row_count(), 10); // 5..15 (10)
        assert_eq!(selection.row_count(), 15);
        assert_eq!(selection.row_group_count(), 2);

        // Verify remaining row groups' content
        let row_selection = selection.get(1).unwrap();
        assert_eq!(row_selection.row_count(), 10); // 5..15 (10)

        let row_selection = selection.get(2).unwrap();
        assert_eq!(row_selection.row_count(), 5); // 0..5 (5)

        // Test popping second row group
        let (rg_id, row_selection) = selection.pop_first().unwrap();
        assert_eq!(rg_id, 1);
        assert_eq!(row_selection.row_count(), 10); // 5..15 (10)
        assert_eq!(selection.row_count(), 5);
        assert_eq!(selection.row_group_count(), 1);

        // Verify remaining row group's content
        let row_selection = selection.get(2).unwrap();
        assert_eq!(row_selection.row_count(), 5); // 0..5 (5)

        // Test popping last row group
        let (rg_id, row_selection) = selection.pop_first().unwrap();
        assert_eq!(rg_id, 2);
        assert_eq!(row_selection.row_count(), 5); // 0..5 (5)
        assert_eq!(selection.row_count(), 0);
        assert_eq!(selection.row_group_count(), 0);
        assert!(selection.is_empty());

        // Test popping from empty selection
        let mut empty_selection = RowGroupSelection::from_row_ranges(vec![], row_group_size);
        assert!(empty_selection.pop_first().is_none());
        assert_eq!(empty_selection.row_count(), 0);
        assert_eq!(empty_selection.row_group_count(), 0);
        assert!(empty_selection.is_empty());
    }

    #[test]
    fn test_remove_row_group() {
        let row_group_size = 100;
        let ranges = vec![
            (0, vec![5..15]), // Within [0, 100)
            (1, vec![5..15]), // Within [0, 100)
            (2, vec![0..5]),  // Within [0, 50) for last row group
        ];
        let mut selection = RowGroupSelection::from_row_ranges(ranges, row_group_size);

        // Test removing existing row group
        selection.remove_row_group(1);
        assert_eq!(selection.row_count(), 15);
        assert_eq!(selection.row_group_count(), 2);
        assert!(!selection.contains_row_group(1));

        // Verify remaining row groups' content
        let row_selection = selection.get(0).unwrap();
        assert_eq!(row_selection.row_count(), 10); // 5..15 (10)

        let row_selection = selection.get(2).unwrap();
        assert_eq!(row_selection.row_count(), 5); // 0..5 (5)

        // Test removing non-existent row group
        selection.remove_row_group(5);
        assert_eq!(selection.row_count(), 15);
        assert_eq!(selection.row_group_count(), 2);

        // Test removing all row groups
        selection.remove_row_group(0);
        assert_eq!(selection.row_count(), 5);
        assert_eq!(selection.row_group_count(), 1);

        let row_selection = selection.get(2).unwrap();
        assert_eq!(row_selection.row_count(), 5); // 0..5 (5)

        selection.remove_row_group(2);
        assert_eq!(selection.row_count(), 0);
        assert_eq!(selection.row_group_count(), 0);
        assert!(selection.is_empty());

        // Test removing from empty selection
        let mut empty_selection = RowGroupSelection::from_row_ranges(vec![], row_group_size);
        empty_selection.remove_row_group(0);
        assert_eq!(empty_selection.row_count(), 0);
        assert_eq!(empty_selection.row_group_count(), 0);
        assert!(empty_selection.is_empty());
    }

    #[test]
    fn test_contains_row_group() {
        let row_group_size = 100;
        let ranges = vec![
            (0, vec![5..15]), // Within [0, 100)
            (1, vec![5..15]), // Within [0, 100)
        ];
        let selection = RowGroupSelection::from_row_ranges(ranges, row_group_size);

        assert!(selection.contains_row_group(0));
        assert!(selection.contains_row_group(1));
        assert!(!selection.contains_row_group(2));

        // Test empty selection
        let empty_selection = RowGroupSelection::from_row_ranges(vec![], row_group_size);
        assert!(!empty_selection.contains_row_group(0));
    }
}
