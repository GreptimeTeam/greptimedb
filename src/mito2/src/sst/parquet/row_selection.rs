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

use std::ops::Range;

use parquet::arrow::arrow_reader::{RowSelection, RowSelector};

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

/// Intersects two `RowSelection`s.
pub(crate) fn intersect_row_selections(
    a: Option<RowSelection>,
    b: Option<RowSelection>,
) -> Option<RowSelection> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.intersection(&b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    }
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
