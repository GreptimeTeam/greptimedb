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

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use store_api::storage::FileId;

/// A cached value and the generation of the insert that stored it.
pub(crate) type Tracked<V> = (u64, V);

/// Generations are unique across caches, so they only need to be compared per key.
static NEXT_GENERATION: AtomicU64 = AtomicU64::new(0);

/// Keys of a cache grouped by the SST file they belong to.
///
/// Dropping a purged file's entries through `invalidate_entries_if` is not an option
/// for hot caches: moka evaluates every pending predicate on each `get` until its
/// housekeeper has scanned the whole cache, which falls behind under a steady purge rate.
///
/// Each key is registered with the generation of its latest insert, see [insert_tracked].
/// The eviction listener of an older insert may run after the key was inserted again, so
/// it only removes the registration of the generation it evicts.
#[derive(Debug)]
pub(crate) struct FileKeys<K> {
    keys: Mutex<HashMap<FileId, HashMap<K, u64>>>,
}

impl<K> Default for FileKeys<K> {
    fn default() -> Self {
        Self {
            keys: Mutex::new(HashMap::new()),
        }
    }
}

impl<K: Hash + Eq> FileKeys<K> {
    pub(crate) fn add(&self, file_id: FileId, key: K, generation: u64) {
        self.keys
            .lock()
            .unwrap()
            .entry(file_id)
            .or_default()
            .insert(key, generation);
    }

    /// Forgets a key whose insert `generation` was removed from the cache.
    pub(crate) fn remove(&self, file_id: FileId, key: &K, generation: u64) {
        let mut keys = self.keys.lock().unwrap();
        let Some(file_keys) = keys.get_mut(&file_id) else {
            return;
        };
        if file_keys.get(key) == Some(&generation) {
            file_keys.remove(key);
        }
        if file_keys.is_empty() {
            keys.remove(&file_id);
        }
    }

    /// Removes and returns all keys of `file_id`.
    pub(crate) fn take(&self, file_id: FileId) -> Vec<K> {
        self.keys
            .lock()
            .unwrap()
            .remove(&file_id)
            .map(|file_keys| file_keys.into_keys().collect())
            .unwrap_or_default()
    }
}

/// Inserts `value` under `key` with a new generation, and calls `register` with that
/// generation while moka holds the key's entry lock.
///
/// Registrations of a key then happen in the same order as its inserts, so the latest
/// registration always belongs to the value in the cache.
pub(crate) fn insert_tracked<K, V>(
    cache: &moka::sync::Cache<K, Tracked<V>>,
    key: K,
    value: V,
    register: impl FnOnce(u64),
) where
    K: Hash + Eq + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    let generation = NEXT_GENERATION.fetch_add(1, Ordering::Relaxed);
    cache.entry(key).and_upsert_with(|_| {
        register(generation);
        (generation, value)
    });
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::time::Duration;

    use super::*;

    #[test]
    fn test_stale_removal_keeps_reinserted_key() {
        let keys = FileKeys::default();
        let file_id = FileId::random();
        keys.add(file_id, "k", 1);
        // The key is inserted again before the listener of the evicted insert runs.
        keys.add(file_id, "k", 2);
        keys.remove(file_id, &"k", 1);
        assert_eq!(vec!["k"], keys.take(file_id));

        keys.add(file_id, "k", 2);
        keys.remove(file_id, &"k", 2);
        assert!(keys.take(file_id).is_empty());
    }

    #[test]
    fn test_concurrent_inserts_register_in_insert_order() {
        let file_id = FileId::random();
        let keys = Arc::new(FileKeys::default());
        let cache: moka::sync::Cache<&'static str, Tracked<u32>> = moka::sync::Cache::builder()
            .eviction_listener({
                let keys = keys.clone();
                move |k, v: Tracked<u32>, _cause| keys.remove(file_id, &*k, v.0)
            })
            .build();
        let second_registered = Arc::new(AtomicBool::new(false));

        insert_tracked(&cache, "k", 1, |generation| {
            keys.add(file_id, "k", generation);
            // Another insert of the key can't register until this one is stored.
            let cache = cache.clone();
            let keys = keys.clone();
            let registered = second_registered.clone();
            std::thread::spawn(move || {
                insert_tracked(&cache, "k", 2, |generation| {
                    registered.store(true, Ordering::SeqCst);
                    keys.add(file_id, "k", generation);
                })
            });
            std::thread::sleep(Duration::from_millis(100));
            assert!(!second_registered.load(Ordering::SeqCst));
        });
        // Wait for the second insert and the replacement callback of the first value.
        while cache.get(&"k").map(|(_, value)| value) != Some(2) {
            std::thread::yield_now();
        }
        cache.run_pending_tasks();

        assert_eq!(vec!["k"], keys.take(file_id));
    }
}
