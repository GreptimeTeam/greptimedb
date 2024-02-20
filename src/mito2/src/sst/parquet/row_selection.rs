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

type SkipRowCount = usize;

/// Converts an iterator of row ranges into a `RowSelection` by creating a sequence of `RowSelector`s.
/// Returns the `RowSelection` and the number of rows that were skipped.
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
) -> (RowSelection, SkipRowCount) {
    let mut selectors: Vec<RowSelector> = Vec::new();
    let mut last_processed_end = 0;
    let mut skip_row_count = 0;

    for Range { start, end } in row_ranges {
        if start > last_processed_end {
            add_or_merge_selector(&mut selectors, start - last_processed_end, true);
            skip_row_count += start - last_processed_end;
        }

        add_or_merge_selector(&mut selectors, end - start, false);
        last_processed_end = end;
    }

    skip_row_count += total_row_count.saturating_sub(last_processed_end);
    (RowSelection::from(selectors), skip_row_count)
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
        let (selection, skipped) = row_selection_from_row_ranges(Some(5..10).into_iter(), 10);
        let expected = RowSelection::from(vec![RowSelector::skip(5), RowSelector::select(5)]);
        assert_eq!(selection, expected);
        assert_eq!(skipped, 5);
    }

    #[test]
    fn test_non_contiguous_ranges() {
        let ranges = vec![1..3, 5..8];
        let (selection, skipped) = row_selection_from_row_ranges(ranges.iter().cloned(), 10);
        let expected = RowSelection::from(vec![
            RowSelector::skip(1),
            RowSelector::select(2),
            RowSelector::skip(2),
            RowSelector::select(3),
        ]);
        assert_eq!(selection, expected);
        assert_eq!(skipped, 5);
    }

    #[test]
    fn test_empty_range() {
        let ranges = vec![];
        let (selection, skipped) = row_selection_from_row_ranges(ranges.iter().cloned(), 10);
        let expected = RowSelection::from(vec![]);
        assert_eq!(selection, expected);
        assert_eq!(skipped, 10);
    }

    #[test]
    fn test_adjacent_ranges() {
        let ranges = vec![1..2, 2..3];
        let (selection, skipped) = row_selection_from_row_ranges(ranges.iter().cloned(), 10);
        let expected = RowSelection::from(vec![RowSelector::skip(1), RowSelector::select(2)]);
        assert_eq!(selection, expected);
        assert_eq!(skipped, 8);
    }

    #[test]
    fn test_large_gap_between_ranges() {
        let ranges = vec![1..2, 100..101];
        let (selection, skipped) = row_selection_from_row_ranges(ranges.iter().cloned(), 10240);
        let expected = RowSelection::from(vec![
            RowSelector::skip(1),
            RowSelector::select(1),
            RowSelector::skip(98),
            RowSelector::select(1),
        ]);
        assert_eq!(selection, expected);
        assert_eq!(skipped, 10238);
    }
}
