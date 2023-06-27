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

use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};
use std::fmt::{Debug, Formatter};
use std::hash::Hash;

/// Deque with key deduplication.
pub struct DedupDeque<K, V> {
    deque: VecDeque<K>,
    existing: HashMap<K, V>,
}

impl<K, V> Default for DedupDeque<K, V> {
    fn default() -> Self {
        Self {
            deque: VecDeque::new(),
            existing: HashMap::new(),
        }
    }
}

impl<K: Eq + Hash + Clone, V> DedupDeque<K, V> {
    /// Pushes a key value to the back of deque.
    /// Returns true if the deque does not already contain value with the same key, otherwise
    /// returns false.
    pub fn push_back(&mut self, key: K, value: V) -> bool {
        debug_assert_eq!(self.deque.len(), self.existing.len());
        if let Entry::Vacant(entry) = self.existing.entry(key.clone()) {
            let _ = entry.insert(value);
            self.deque.push_back(key);
            return true;
        }
        false
    }

    /// Pushes a key value to the front of deque.
    /// Returns true if the deque does not already contain value with the same key, otherwise
    /// returns false.
    pub fn push_front(&mut self, key: K, value: V) -> bool {
        if let Entry::Vacant(entry) = self.existing.entry(key.clone()) {
            let _ = entry.insert(value);
            self.deque.push_front(key);
            return true;
        }
        false
    }

    /// Pops a pair from the back of deque. Returns [None] if the deque is empty.
    pub fn pop_front(&mut self) -> Option<(K, V)> {
        debug_assert_eq!(self.deque.len(), self.existing.len());
        let key = self.deque.pop_front()?;
        let value = self.existing.remove(&key)?;
        Some((key, value))
    }

    #[inline]
    pub fn len(&self) -> usize {
        debug_assert_eq!(self.deque.len(), self.existing.len());
        self.deque.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.deque.is_empty()
    }

    #[inline]
    pub fn clear(&mut self) {
        self.deque.clear();
        self.existing.clear();
    }
}

impl<K, V> Debug for DedupDeque<K, V>
where
    K: Debug,
    V: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DedupDeque")
            .field("deque", &self.deque)
            .field("existing", &self.existing)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dedup_deque() {
        let mut deque = DedupDeque::default();
        assert!(deque.push_back(1, "hello".to_string()));
        assert_eq!(1, deque.len());
        assert!(deque.push_back(2, "world".to_string()));
        assert_eq!(2, deque.len());
        assert_eq!((1, "hello".to_string()), deque.pop_front().unwrap());
        assert_eq!(1, deque.len());
        assert_eq!((2, "world".to_string()), deque.pop_front().unwrap());
        assert_eq!(0, deque.len());

        // insert duplicated item
        assert!(deque.push_back(1, "hello".to_string()));
        assert!(!deque.push_back(1, "world".to_string()));
        assert_eq!((1, "hello".to_string()), deque.pop_front().unwrap());

        deque.clear();
        assert!(deque.is_empty());
    }
}
