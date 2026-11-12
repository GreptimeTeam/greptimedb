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

use std::collections::{HashMap, VecDeque};
use std::hash::{BuildHasher, Hash, RandomState};
use std::sync::{Arc, Mutex, Weak};

use smallvec::SmallVec;

const SHARD_COUNT: usize = 32;

/// Shares equal immutable values without keeping their payloads alive.
///
/// Keys select candidate buckets; equality of values determines sharing. Keys must
/// not own the payload being interned. Live entries are never evicted. Each call
/// sweeps one bucket in its shard, so inactive shards retain only weak references
/// until used again or dropped. No guards or callbacks escape this abstraction.
#[derive(Debug)]
pub struct WeakInterner<K, V> {
    shards: [Mutex<Entries<K, V>>; SHARD_COUNT],
    hasher: RandomState,
}

impl<K, V> Default for WeakInterner<K, V> {
    fn default() -> Self {
        Self {
            shards: std::array::from_fn(|_| {
                Mutex::new(Entries {
                    values: HashMap::new(),
                    sweep: VecDeque::new(),
                })
            }),
            hasher: RandomState::new(),
        }
    }
}

impl<K: Clone + Eq + Hash, V: Eq> WeakInterner<K, V> {
    /// Returns the live equal value, or registers the supplied allocation.
    /// Publication and lookup are atomic within the key's shard. Implementations
    /// of Hash, Eq and Drop must not reenter this interner or acquire external locks.
    pub fn intern(&self, key: K, value: Arc<V>) -> Arc<V> {
        let shard = self.hasher.hash_one(&key) as usize % SHARD_COUNT;
        // A panicking Eq/Hash may poison this optimization, but cannot compromise
        // ownership: the map contains only weak references and live entries remain valid.
        let mut entries = self.shards[shard]
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        entries.sweep_one();
        if !entries.values.contains_key(&key) {
            entries.sweep.push_back(key.clone());
        }
        let candidates = entries.values.entry(key).or_default();
        candidates.retain(|candidate| candidate.strong_count() > 0);
        for existing in candidates.iter().filter_map(Weak::upgrade) {
            if Arc::ptr_eq(&existing, &value) || existing == value {
                return existing;
            }
        }
        candidates.push(Arc::downgrade(&value));
        value
    }
}

#[derive(Debug)]
struct Entries<K, V> {
    values: HashMap<K, SmallVec<[Weak<V>; 1]>>,
    sweep: VecDeque<K>,
}

impl<K: Eq + Hash, V> Entries<K, V> {
    fn sweep_one(&mut self) {
        let Some(key) = self.sweep.pop_front() else {
            return;
        };
        if let Some(values) = self.values.get_mut(&key) {
            values.retain(|value| value.strong_count() > 0);
            if values.is_empty() {
                self.values.remove(&key);
            } else {
                self.sweep.push_back(key);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Barrier;
    use std::thread;

    use super::*;

    #[test]
    fn test_concurrent_publication() {
        let interner = WeakInterner::default();
        let barrier = Barrier::new(8);
        let values = thread::scope(|scope| {
            let workers: Vec<_> = (0..8)
                .map(|_| {
                    scope.spawn(|| {
                        barrier.wait();
                        interner.intern(1, Arc::new(String::from("same")))
                    })
                })
                .collect();
            workers
                .into_iter()
                .map(|worker| worker.join().unwrap())
                .collect::<Vec<_>>()
        });
        assert!(values.iter().all(|value| Arc::ptr_eq(value, &values[0])));
        let different = interner.intern(1, Arc::new(String::from("different")));
        assert!(!Arc::ptr_eq(&different, &values[0]));
        assert!(Arc::ptr_eq(
            &values[0],
            &interner.intern(1, Arc::new(String::from("same")))
        ));
    }

    #[test]
    fn test_weak_ownership_and_churn() {
        let interner = WeakInterner::default();
        let value = interner.intern(1, Arc::new(10));
        let weak = Arc::downgrade(&value);
        drop(value);
        assert!(weak.upgrade().is_none());
        for version in 0..1000 {
            interner.intern(1, Arc::new(version));
        }
        let shard = interner.hasher.hash_one(1) as usize % SHARD_COUNT;
        let entries = interner.shards[shard].lock().unwrap();
        assert_eq!(entries.values.len(), 1);
        assert_eq!(entries.values[&1].len(), 1);
        assert_eq!(entries.sweep.len(), 1);
    }
}
