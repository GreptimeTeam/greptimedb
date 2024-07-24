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

use std::collections::VecDeque;
use std::ops::Range;

use itertools::Itertools;
use store_api::logstore::EntryId;

/// An iterator over WAL (Write-Ahead Log) entries index for a region.
pub trait RegionWalIndexIterator: Send + Sync {
    // Returns the size of the next batch.
    fn next_batch_size(&self) -> u64;

    // Peeks the next EntryId without advancing the iterator.
    fn peek(&self) -> Option<EntryId>;

    // Advances the iterator and returns the next EntryId.
    fn next(&mut self) -> Option<EntryId>;
}

pub struct RegionWalRange {
    next_entry_id: EntryId,
    end_entry_id: EntryId,
}

impl RegionWalRange {
    pub fn new(range: Range<EntryId>) -> Self {
        Self {
            next_entry_id: range.start,
            end_entry_id: range.end,
        }
    }
}

impl RegionWalIndexIterator for RegionWalRange {
    fn next_batch_size(&self) -> u64 {
        if self.next_entry_id < self.end_entry_id {
            self.end_entry_id
                .checked_sub(self.next_entry_id)
                .unwrap_or_default()
        } else {
            0
        }
    }

    fn peek(&self) -> Option<EntryId> {
        if self.next_entry_id < self.end_entry_id {
            Some(self.next_entry_id)
        } else {
            None
        }
    }

    fn next(&mut self) -> Option<EntryId> {
        if self.next_entry_id < self.end_entry_id {
            let next = self.next_entry_id;
            self.next_entry_id += 1;
            Some(next)
        } else {
            None
        }
    }
}

pub struct RegionWalVecIndex {
    index: VecDeque<EntryId>,
}

impl RegionWalVecIndex {
    pub fn new<I: IntoIterator<Item = EntryId>>(index: I) -> Self {
        Self {
            index: index.into_iter().collect::<VecDeque<_>>(),
        }
    }
}

impl RegionWalIndexIterator for RegionWalVecIndex {
    fn next_batch_size(&self) -> u64 {
        if self.index.is_empty() {
            0
        } else {
            let mut count = 1;
            for (prev, next) in self.index.iter().tuple_windows() {
                if prev + 1 == *next {
                    count += 1;
                } else {
                    break;
                }
            }

            count
        }
    }

    fn peek(&self) -> Option<EntryId> {
        self.index.front().cloned()
    }

    fn next(&mut self) -> Option<EntryId> {
        self.index.pop_front()
    }
}

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
    fn next_batch_size(&self) -> u64 {
        for iter in &self.iterator {
            let next_batch_size = iter.next_batch_size();
            if next_batch_size != 0 {
                return next_batch_size;
            }
        }

        0
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_region_wal_range() {
        let mut range = RegionWalRange::new(0..1);

        assert_eq!(range.next_batch_size(), 1);
        assert_eq!(range.peek(), Some(0));

        // Advance 1 step
        assert_eq!(range.next(), Some(0));
        assert_eq!(range.next_batch_size(), 0);

        // Advance 1 step
        assert_eq!(range.next(), None);
        assert_eq!(range.next_batch_size(), 0);
        // No effect
        assert_eq!(range.next(), None);
        assert_eq!(range.next_batch_size(), 0);

        let mut range = RegionWalRange::new(0..0);
        assert_eq!(range.next_batch_size(), 0);
        // No effect
        assert_eq!(range.next(), None);
        assert_eq!(range.next_batch_size(), 0);
    }

    #[test]
    fn test_region_wal_vec_index() {
        let mut index = RegionWalVecIndex::new([0, 1, 2, 7, 8, 11]);
        assert_eq!(index.next_batch_size(), 3);
        assert_eq!(index.peek(), Some(0));
        // Advance 1 step
        assert_eq!(index.next(), Some(0));
        assert_eq!(index.next_batch_size(), 2);
        // Advance 1 step
        assert_eq!(index.next(), Some(1));
        assert_eq!(index.next_batch_size(), 1);
        // Advance 1 step
        assert_eq!(index.next(), Some(2));
        assert_eq!(index.next_batch_size(), 2);
        // Advance 1 step
        assert_eq!(index.next(), Some(7));
        assert_eq!(index.next_batch_size(), 1);
        // Advance 1 step
        assert_eq!(index.next(), Some(8));
        assert_eq!(index.next_batch_size(), 1);
        // Advance 1 step
        assert_eq!(index.next(), Some(11));
        assert_eq!(index.next_batch_size(), 0);

        // No effect
        assert_eq!(index.next(), None);
        assert_eq!(index.next_batch_size(), 0);

        let mut index = RegionWalVecIndex::new([]);
        assert_eq!(index.next_batch_size(), 0);
        assert_eq!(index.peek(), None);
        // No effect
        assert_eq!(index.peek(), None);
        assert_eq!(index.next(), None);
        assert_eq!(index.next_batch_size(), 0);
    }

    #[test]
    fn test_multiple_region_wal_iterator() {
        let iter0 = Box::new(RegionWalRange::new(0..0)) as _;
        let iter1 = Box::new(RegionWalVecIndex::new([0, 1, 2, 7, 8, 11])) as _;
        let iter2 = Box::new(RegionWalRange::new(1024..1024)) as _;
        let mut iter = MultipleRegionWalIndexIterator::new([iter0, iter1, iter2]);

        // The next batch is 0, 1, 2
        assert_eq!(iter.next_batch_size(), 3);
        assert_eq!(iter.peek(), Some(0));
        // Advance 1 step
        assert_eq!(iter.next(), Some(0));

        // The next batch is 1,2
        assert_eq!(iter.next_batch_size(), 2);
        assert_eq!(iter.peek(), Some(1));
        // Advance 1 step
        assert_eq!(iter.next(), Some(1));

        // The next batch is 2
        assert_eq!(iter.next_batch_size(), 1);
        assert_eq!(iter.peek(), Some(2));

        // Advance 1 step
        assert_eq!(iter.next(), Some(2));
        // The next batch is 7, 8
        assert_eq!(iter.next_batch_size(), 2);
        assert_eq!(iter.peek(), Some(7));

        // Advance 1 step
        assert_eq!(iter.next(), Some(7));
        // The next batch is 8
        assert_eq!(iter.next_batch_size(), 1);
        assert_eq!(iter.peek(), Some(8));

        // Advance 1 step
        assert_eq!(iter.next(), Some(8));
        // The next batch is 11
        assert_eq!(iter.next_batch_size(), 1);
        assert_eq!(iter.peek(), Some(11));
        // Advance 1 step
        assert_eq!(iter.next(), Some(11));

        assert_eq!(iter.next_batch_size(), 0);
        assert_eq!(iter.peek(), None);
        assert!(!iter.iterator.is_empty());
        assert_eq!(iter.next(), None);
        assert!(iter.iterator.is_empty());

        // No effect
        assert_eq!(iter.next(), None);
        assert_eq!(iter.next_batch_size(), 0);
        assert_eq!(iter.peek(), None);
        assert_eq!(iter.next(), None);
    }
}
