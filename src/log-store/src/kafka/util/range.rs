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

use std::cmp::{max, min};
use std::iter::Peekable;
use std::ops::Range;

use store_api::logstore::EntryId;

/// Convert a sequence of [`EntryId`]s into size ranges.
pub(crate) struct ConvertIndexToRange<'a, I: Iterator<Item = &'a EntryId>> {
    base: Option<EntryId>,
    iter: Peekable<I>,
    avg_size: usize,
}

impl<'a, I: Iterator<Item = &'a EntryId>> ConvertIndexToRange<'a, I> {
    pub fn new(mut iter: Peekable<I>, avg_size: usize) -> Self {
        let base = iter.peek().cloned().cloned();

        Self {
            base,
            iter,
            avg_size,
        }
    }
}

impl<'a, I: Iterator<Item = &'a EntryId>> Iterator for ConvertIndexToRange<'a, I> {
    type Item = Range<usize>;

    fn next(&mut self) -> Option<Self::Item> {
        let (base, val) = (&self.base?, self.iter.next()?);
        let start = (*val - *base) as usize * self.avg_size;
        let end = start + self.avg_size + 1;
        Some(start..end)
    }
}

/// Merge all ranges smaller than the `window_size`.
///
/// e.g.,
///
/// Case 1
/// - input: range: [(0..3), (5..6), (5..8)], window_size: 6
/// - output: range: (0..6)
///
/// Case 2
/// - input: range: [(0..3)], window_size: 6
/// - output: range: (0..3)
pub(crate) struct MergeRange<I: Iterator<Item = Range<usize>>> {
    iter: I,
    window_size: usize,
}

impl<I: Iterator<Item = Range<usize>>> MergeRange<I> {
    pub fn new(iter: I, window_size: usize) -> Self {
        Self { iter, window_size }
    }
}

/// Merges ranges.
fn merge(this: &mut Range<usize>, other: &Range<usize>) {
    this.start = min(this.start, other.start);
    this.end = max(this.end, other.end);
}

impl<I: Iterator<Item = Range<usize>>> MergeRange<I> {
    /// Calculates the size of the next merged range.
    pub(crate) fn merge(mut self) -> Option<(Range<usize>, usize)> {
        let mut merged_range = self.iter.next();
        let this = merged_range.as_mut()?;
        let mut merged = 1;
        for next in self.iter {
            let window = next.start - this.start;
            if window > self.window_size {
                break;
            } else {
                merge(this, &next);
                merged += 1;
            }
        }
        merged_range.map(|range| (range, merged))
    }
}

#[cfg(test)]
#[allow(clippy::single_range_in_vec_init)]
mod tests {
    use super::*;

    #[test]
    fn test_convert_index_to_range() {
        let avg_size = 1024;
        let index = [1u64, 4, 10, 15];
        let mut converter = ConvertIndexToRange::new(index.iter().peekable(), avg_size);

        assert_eq!(converter.next(), Some(0..avg_size + 1));
        assert_eq!(converter.next(), Some(3 * avg_size..4 * avg_size + 1));
        assert_eq!(converter.next(), Some(9 * avg_size..10 * avg_size + 1));
        assert_eq!(converter.next(), Some(14 * avg_size..15 * avg_size + 1));
        assert_eq!(converter.next(), None);

        let index = [];
        let mut converter = ConvertIndexToRange::new(index.iter().peekable(), avg_size);
        assert_eq!(converter.next(), None);
    }

    #[test]
    fn test_merge_range() {
        let size_range = [(10usize..13), (12..14), (16..18), (19..29)];
        let merger = MergeRange::new(size_range.into_iter(), 9);
        assert_eq!(merger.merge(), Some((10..29, 4)));

        let size_range = [(10usize..13), (12..14), (16..18)];
        let merger = MergeRange::new(size_range.into_iter(), 5);
        assert_eq!(merger.merge(), Some((10..14, 2)));

        let size_range = [(10usize..13), (15..17), (16..18)];
        let merger = MergeRange::new(size_range.into_iter(), 5);
        assert_eq!(merger.merge(), Some((10..17, 2)));

        let size_range = [(10usize..13)];
        let merger = MergeRange::new(size_range.into_iter(), 4);
        assert_eq!(merger.merge(), Some((10..13, 1)));

        let size_range = [(10usize..13)];
        let merger = MergeRange::new(size_range.into_iter(), 2);
        assert_eq!(merger.merge(), Some((10..13, 1)));

        let size_range = [];
        let merger = MergeRange::new(size_range.into_iter(), 2);
        assert_eq!(merger.merge(), None);
    }
}
