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

use std::collections::{BTreeMap, btree_map};

/// Groups items by key while bounding both batch size and total buffered items.
/// Full groups are emitted immediately; at the buffer limit the largest partial
/// group is emitted. Equal-sized groups are ordered by key.
pub(crate) struct BatchAggregator<K, V> {
    groups: BTreeMap<K, Vec<V>>,
    batch_size: usize,
    buffer_limit: usize,
    buffered: usize,
}

impl<K: Ord + Clone, V> BatchAggregator<K, V> {
    /// Creates an aggregator. Zero limits behave as one, emitting each item immediately.
    pub(crate) fn new(batch_size: usize, buffer_limit: usize) -> Self {
        Self {
            groups: BTreeMap::new(),
            batch_size: batch_size.max(1),
            buffer_limit: buffer_limit.max(1),
            buffered: 0,
        }
    }

    /// Adds an item and returns a full group, or the largest group at the buffer limit.
    /// The returned key can differ from the input key when a partial group is emitted.
    pub(crate) fn push(&mut self, key: K, item: V) -> Option<(K, Vec<V>)> {
        let mut entry = match self.groups.entry(key) {
            btree_map::Entry::Occupied(entry) => entry,
            btree_map::Entry::Vacant(entry) => entry.insert_entry(Vec::new()),
        };
        entry.get_mut().push(item);
        self.buffered += 1;
        if entry.get().len() >= self.batch_size {
            let batch = entry.remove_entry();
            self.buffered -= batch.1.len();
            return Some(batch);
        }
        if self.buffered >= self.buffer_limit {
            let key = self
                .groups
                .iter()
                .max_by_key(|(_, items)| items.len())?
                .0
                .clone();
            let batch = self.groups.remove_entry(&key)?;
            self.buffered -= batch.1.len();
            return Some(batch);
        }
        None
    }

    /// Removes one remaining group, including its key, until the buffer is empty.
    pub(crate) fn flush(&mut self) -> Option<(K, Vec<V>)> {
        let batch = self.groups.pop_first()?;
        self.buffered -= batch.1.len();
        Some(batch)
    }

    /// Groups a finite input without splitting groups or imposing a streaming buffer limit.
    pub(crate) fn group(items: impl IntoIterator<Item = (K, V)>) -> Vec<(K, Vec<V>)> {
        let mut groups = Self::new(usize::MAX, usize::MAX);
        let mut batches = Vec::new();
        for (key, item) in items {
            if let Some(batch) = groups.push(key, item) {
                batches.push(batch);
            }
        }
        batches.extend(std::iter::from_fn(|| groups.flush()));
        batches
    }
}

#[cfg(test)]
mod tests {
    use super::BatchAggregator;

    #[test]
    fn test_interleaved_groups_and_tails() {
        let mut batches = BatchAggregator::new(2, 4);
        assert_eq!(None, batches.push(1, 10));
        assert_eq!(None, batches.push(2, 20));
        assert_eq!(Some((1, vec![10, 11])), batches.push(1, 11));
        assert_eq!(None, batches.push(1, 12));
        assert_eq!(Some((1, vec![12])), batches.flush());
        assert_eq!(Some((2, vec![20])), batches.flush());
        assert_eq!(None, batches.flush());
        assert_eq!(0, batches.buffered);
    }

    #[test]
    fn test_bounds_buffer_with_many_groups() {
        let mut batches = BatchAggregator::new(8, 3);
        let mut emitted = Vec::new();
        for i in 0..100 {
            if let Some((_, batch)) = batches.push(i, i) {
                assert!(batch.len() <= 8);
                emitted.extend(batch);
            }
            assert!(batches.buffered < 3);
        }
        while let Some((_, batch)) = batches.flush() {
            emitted.extend(batch);
        }
        emitted.sort_unstable();
        assert_eq!((0..100).collect::<Vec<_>>(), emitted);
    }

    #[test]
    fn test_evicts_largest_partial_group() {
        let mut batches = BatchAggregator::new(10, 4);
        assert_eq!(None, batches.push(1, 10));
        assert_eq!(None, batches.push(2, 20));
        assert_eq!(None, batches.push(1, 11));
        assert_eq!(Some((1, vec![10, 11, 12])), batches.push(1, 12));
        assert_eq!(Some((2, vec![20])), batches.flush());
    }

    #[test]
    fn test_zero_and_singleton_limits() {
        for (batch, buffer) in [(0, 4), (4, 0), (1, 1)] {
            let mut batches = BatchAggregator::new(batch, buffer);
            assert_eq!(Some((1, vec![42])), batches.push(1, 42));
            assert_eq!(None, batches.flush());
        }
    }

    #[test]
    fn test_group_preserves_complete_groups() {
        let input = (0..1200).map(|i| (i % 2, i));
        let groups = BatchAggregator::group(input);
        assert_eq!(2, groups.len());
        for (key, values) in groups {
            assert_eq!(
                (0..1200).filter(|i| i % 2 == key).collect::<Vec<_>>(),
                values
            );
        }
    }
}
