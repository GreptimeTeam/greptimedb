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
use std::collections::{BTreeSet, VecDeque};
use std::fmt::Debug;
use std::ops::Range;

use store_api::logstore::EntryId;

use crate::kafka::util::range::{ConvertIndexToRange, MergeRange};

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct NextBatchHint {
    pub(crate) bytes: usize,
    pub(crate) len: usize,
}

/// An iterator over WAL (Write-Ahead Log) entries index for a region.
pub trait RegionWalIndexIterator: Send + Sync + Debug {
    /// Returns next batch hint.
    fn next_batch_hint(&self, avg_size: usize) -> Option<NextBatchHint>;

    // Peeks the next EntryId without advancing the iterator.
    fn peek(&self) -> Option<EntryId>;

    // Advances the iterator and returns the next EntryId.
    fn next(&mut self) -> Option<EntryId>;

    #[cfg(test)]
    fn as_any(&self) -> &dyn std::any::Any;
}

/// Represents a range [next_entry_id, end_entry_id) of WAL entries for a region.
#[derive(Debug)]
pub struct RegionWalRange {
    current_entry_id: EntryId,
    end_entry_id: EntryId,
    max_batch_size: usize,
}

impl RegionWalRange {
    pub fn new(range: Range<EntryId>, max_batch_size: usize) -> Self {
        Self {
            current_entry_id: range.start,
            end_entry_id: range.end,
            max_batch_size,
        }
    }

    fn next_batch_size(&self) -> Option<u64> {
        if self.current_entry_id < self.end_entry_id {
            Some(
                self.end_entry_id
                    .checked_sub(self.current_entry_id)
                    .unwrap_or_default(),
            )
        } else {
            None
        }
    }
}

impl RegionWalIndexIterator for RegionWalRange {
    fn next_batch_hint(&self, avg_size: usize) -> Option<NextBatchHint> {
        if let Some(size) = self.next_batch_size() {
            let bytes = min(size as usize * avg_size, self.max_batch_size);
            let len = bytes / avg_size;

            return Some(NextBatchHint { bytes, len });
        }

        None
    }

    fn peek(&self) -> Option<EntryId> {
        if self.current_entry_id < self.end_entry_id {
            Some(self.current_entry_id)
        } else {
            None
        }
    }

    fn next(&mut self) -> Option<EntryId> {
        if self.current_entry_id < self.end_entry_id {
            let next = self.current_entry_id;
            self.current_entry_id += 1;
            Some(next)
        } else {
            None
        }
    }

    #[cfg(test)]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

pub const MIN_BATCH_WINDOW_SIZE: usize = 4 * 1024 * 1024;

/// Represents an index of Write-Ahead Log entries for a region,
/// stored as a vector of [EntryId]s.
#[derive(Debug)]
pub struct RegionWalVecIndex {
    index: VecDeque<EntryId>,
    min_batch_window_size: usize,
}

impl RegionWalVecIndex {
    pub fn new<I: IntoIterator<Item = EntryId>>(index: I, min_batch_window_size: usize) -> Self {
        Self {
            index: index.into_iter().collect::<VecDeque<_>>(),
            min_batch_window_size,
        }
    }
}

impl RegionWalIndexIterator for RegionWalVecIndex {
    fn next_batch_hint(&self, avg_size: usize) -> Option<NextBatchHint> {
        let merger = MergeRange::new(
            ConvertIndexToRange::new(self.index.iter().peekable(), avg_size),
            self.min_batch_window_size,
        );

        merger.merge().map(|(range, size)| NextBatchHint {
            bytes: range.end - range.start - 1,
            len: size,
        })
    }

    fn peek(&self) -> Option<EntryId> {
        self.index.front().cloned()
    }

    fn next(&mut self) -> Option<EntryId> {
        self.index.pop_front()
    }

    #[cfg(test)]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Represents an iterator over multiple region WAL indexes.
///
/// Allowing iteration through multiple WAL indexes.
#[derive(Debug)]
pub struct MultipleRegionWalIndexIterator {
    iterator: VecDeque<Box<dyn RegionWalIndexIterator>>,
}

impl MultipleRegionWalIndexIterator {
    pub fn new<I: IntoIterator<Item = Box<dyn RegionWalIndexIterator>>>(iterator: I) -> Self {
        Self {
            iterator: iterator.into_iter().collect::<VecDeque<_>>(),
        }
    }
}

impl RegionWalIndexIterator for MultipleRegionWalIndexIterator {
    fn next_batch_hint(&self, avg_size: usize) -> Option<NextBatchHint> {
        for iter in &self.iterator {
            if let Some(batch) = iter.next_batch_hint(avg_size) {
                return Some(batch);
            }
        }

        None
    }

    fn peek(&self) -> Option<EntryId> {
        for iter in &self.iterator {
            let peek = iter.peek();
            if peek.is_some() {
                return peek;
            }
        }

        None
    }

    fn next(&mut self) -> Option<EntryId> {
        while !self.iterator.is_empty() {
            let remove = self.iterator.front().and_then(|iter| iter.peek()).is_none();
            if remove {
                self.iterator.pop_front();
            } else {
                break;
            }
        }

        self.iterator.front_mut().and_then(|iter| iter.next())
    }

    #[cfg(test)]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Builds [`RegionWalIndexIterator`].
///
/// Returns None means there are no entries to replay.
pub fn build_region_wal_index_iterator(
    start_entry_id: EntryId,
    end_entry_id: EntryId,
    region_indexes: Option<(BTreeSet<EntryId>, EntryId)>,
    max_batch_bytes: usize,
    min_window_size: usize,
) -> Option<Box<dyn RegionWalIndexIterator>> {
    if (start_entry_id..end_entry_id).is_empty() {
        return None;
    }

    match region_indexes {
        Some((region_indexes, last_index)) => {
            if region_indexes.is_empty() && last_index >= end_entry_id {
                return None;
            }

            let mut iterator: Vec<Box<dyn RegionWalIndexIterator>> = Vec::with_capacity(2);
            if !region_indexes.is_empty() {
                let index = RegionWalVecIndex::new(region_indexes, min_window_size);
                iterator.push(Box::new(index));
            }
            let known_last_index = max(last_index, start_entry_id);
            if known_last_index < end_entry_id {
                let range = known_last_index..end_entry_id;
                let index = RegionWalRange::new(range, max_batch_bytes);
                iterator.push(Box::new(index));
            }

            Some(Box::new(MultipleRegionWalIndexIterator::new(iterator)))
        }
        None => {
            let range = start_entry_id..end_entry_id;

            Some(Box::new(RegionWalRange::new(range, max_batch_bytes)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_region_wal_range() {
        let range = RegionWalRange::new(0..1024, 1024);
        assert_eq!(
            range.next_batch_hint(10),
            Some(NextBatchHint {
                bytes: 1024,
                len: 102
            })
        );

        let mut range = RegionWalRange::new(0..1, 1024);

        assert_eq!(range.next_batch_size(), Some(1));
        assert_eq!(range.peek(), Some(0));

        // Advance 1 step
        assert_eq!(range.next(), Some(0));
        assert_eq!(range.next_batch_size(), None);

        // Advance 1 step
        assert_eq!(range.next(), None);
        assert_eq!(range.next_batch_size(), None);
        // No effect
        assert_eq!(range.next(), None);
        assert_eq!(range.next_batch_size(), None);

        let mut range = RegionWalRange::new(0..0, 1024);
        assert_eq!(range.next_batch_size(), None);
        // No effect
        assert_eq!(range.next(), None);
        assert_eq!(range.next_batch_size(), None);
    }

    #[test]
    fn test_region_wal_vec_index() {
        let mut index = RegionWalVecIndex::new([0, 1, 2, 7, 8, 11], 30);
        assert_eq!(
            index.next_batch_hint(10),
            Some(NextBatchHint { bytes: 30, len: 3 })
        );
        assert_eq!(index.peek(), Some(0));
        // Advance 1 step
        assert_eq!(index.next(), Some(0));
        assert_eq!(
            index.next_batch_hint(10),
            Some(NextBatchHint { bytes: 20, len: 2 })
        );
        // Advance 1 step
        assert_eq!(index.next(), Some(1));
        assert_eq!(
            index.next_batch_hint(10),
            Some(NextBatchHint { bytes: 10, len: 1 })
        );
        // Advance 1 step
        assert_eq!(index.next(), Some(2));
        assert_eq!(
            index.next_batch_hint(10),
            Some(NextBatchHint { bytes: 20, len: 2 })
        );
        // Advance 1 step
        assert_eq!(index.next(), Some(7));
        assert_eq!(
            index.next_batch_hint(10),
            Some(NextBatchHint { bytes: 40, len: 2 })
        );
        // Advance 1 step
        assert_eq!(index.next(), Some(8));
        assert_eq!(
            index.next_batch_hint(10),
            Some(NextBatchHint { bytes: 10, len: 1 })
        );
        // Advance 1 step
        assert_eq!(index.next(), Some(11));
        assert_eq!(index.next_batch_hint(10), None);

        // No effect
        assert_eq!(index.next(), None);
        assert_eq!(index.next_batch_hint(10), None);

        let mut index = RegionWalVecIndex::new([], 1024);
        assert_eq!(index.next_batch_hint(10), None);
        assert_eq!(index.peek(), None);
        // No effect
        assert_eq!(index.peek(), None);
        assert_eq!(index.next(), None);
        assert_eq!(index.next_batch_hint(10), None);
    }

    #[test]
    fn test_multiple_region_wal_iterator() {
        let iter0 = Box::new(RegionWalRange::new(0..0, 1024)) as _;
        let iter1 = Box::new(RegionWalVecIndex::new([0, 1, 2, 7, 8, 11], 40)) as _;
        let iter2 = Box::new(RegionWalRange::new(1024..1024, 1024)) as _;
        let mut iter = MultipleRegionWalIndexIterator::new([iter0, iter1, iter2]);

        // The next batch is 0, 1, 2
        assert_eq!(
            iter.next_batch_hint(10),
            Some(NextBatchHint { bytes: 30, len: 3 })
        );
        assert_eq!(iter.peek(), Some(0));
        // Advance 1 step
        assert_eq!(iter.next(), Some(0));

        // The next batch is 1, 2
        assert_eq!(
            iter.next_batch_hint(10),
            Some(NextBatchHint { bytes: 20, len: 2 })
        );
        assert_eq!(iter.peek(), Some(1));
        // Advance 1 step
        assert_eq!(iter.next(), Some(1));

        // The next batch is 2
        assert_eq!(
            iter.next_batch_hint(10),
            Some(NextBatchHint { bytes: 10, len: 1 })
        );
        assert_eq!(iter.peek(), Some(2));

        // Advance 1 step
        assert_eq!(iter.next(), Some(2));
        // The next batch is 7, 8, 11
        assert_eq!(
            iter.next_batch_hint(10),
            Some(NextBatchHint { bytes: 50, len: 3 })
        );
        assert_eq!(iter.peek(), Some(7));

        // Advance 1 step
        assert_eq!(iter.next(), Some(7));
        // The next batch is 8, 11
        assert_eq!(
            iter.next_batch_hint(10),
            Some(NextBatchHint { bytes: 40, len: 2 })
        );
        assert_eq!(iter.peek(), Some(8));

        // Advance 1 step
        assert_eq!(iter.next(), Some(8));
        // The next batch is 11
        assert_eq!(
            iter.next_batch_hint(10),
            Some(NextBatchHint { bytes: 10, len: 1 })
        );
        assert_eq!(iter.peek(), Some(11));
        // Advance 1 step
        assert_eq!(iter.next(), Some(11));

        assert_eq!(iter.next_batch_hint(10), None,);
        assert_eq!(iter.peek(), None);
        assert!(!iter.iterator.is_empty());
        assert_eq!(iter.next(), None);
        assert!(iter.iterator.is_empty());

        // No effect
        assert_eq!(iter.next(), None);
        assert_eq!(iter.next_batch_hint(10), None,);
        assert_eq!(iter.peek(), None);
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_build_region_wal_index_iterator() {
        let iterator = build_region_wal_index_iterator(1024, 1024, None, 5, 5);
        assert!(iterator.is_none());

        let iterator = build_region_wal_index_iterator(1024, 1023, None, 5, 5);
        assert!(iterator.is_none());

        let iterator =
            build_region_wal_index_iterator(1024, 1024, Some((BTreeSet::new(), 1024)), 5, 5);
        assert!(iterator.is_none());

        let iterator =
            build_region_wal_index_iterator(1, 1024, Some((BTreeSet::new(), 1024)), 5, 5);
        assert!(iterator.is_none());

        let iterator =
            build_region_wal_index_iterator(1, 1024, Some((BTreeSet::new(), 1025)), 5, 5);
        assert!(iterator.is_none());

        let iterator = build_region_wal_index_iterator(
            1,
            1024,
            Some((BTreeSet::from([512, 756]), 1024)),
            5,
            5,
        )
        .unwrap();
        let iter = iterator
            .as_any()
            .downcast_ref::<MultipleRegionWalIndexIterator>()
            .unwrap();
        assert_eq!(iter.iterator.len(), 1);
        let vec_index = iter.iterator[0]
            .as_any()
            .downcast_ref::<RegionWalVecIndex>()
            .unwrap();
        assert_eq!(vec_index.index, VecDeque::from([512, 756]));

        let iterator = build_region_wal_index_iterator(
            1,
            1024,
            Some((BTreeSet::from([512, 756]), 1023)),
            5,
            5,
        )
        .unwrap();
        let iter = iterator
            .as_any()
            .downcast_ref::<MultipleRegionWalIndexIterator>()
            .unwrap();
        assert_eq!(iter.iterator.len(), 2);
        let vec_index = iter.iterator[0]
            .as_any()
            .downcast_ref::<RegionWalVecIndex>()
            .unwrap();
        assert_eq!(vec_index.index, VecDeque::from([512, 756]));
        let wal_range = iter.iterator[1]
            .as_any()
            .downcast_ref::<RegionWalRange>()
            .unwrap();
        assert_eq!(wal_range.current_entry_id, 1023);
        assert_eq!(wal_range.end_entry_id, 1024);
    }
}
