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
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use store_api::storage::FileId;

/// Keys of a cache grouped by the SST file they belong to.
///
/// Dropping a purged file's entries through `invalidate_entries_if` is not an option
/// for hot caches: moka evaluates every pending predicate on each `get` until its
/// housekeeper has scanned the whole cache, which falls behind under a steady purge rate.
///
/// Each key is registered with the id of the entry it belongs to, see [arc_entry_id]. The
/// eviction listener of an old entry may run after the same key was inserted again, so it
/// only removes the registration if the ids match.
#[derive(Debug)]
pub(crate) struct FileKeys<K> {
    keys: Mutex<HashMap<FileId, HashMap<K, usize>>>,
}

impl<K> Default for FileKeys<K> {
    fn default() -> Self {
        Self {
            keys: Mutex::new(HashMap::new()),
        }
    }
}

impl<K: Hash + Eq> FileKeys<K> {
    pub(crate) fn add(&self, file_id: FileId, key: K, entry_id: usize) {
        self.keys
            .lock()
            .unwrap()
            .entry(file_id)
            .or_default()
            .insert(key, entry_id);
    }

    /// Forgets a key whose entry `entry_id` was removed from the cache.
    pub(crate) fn remove(&self, file_id: FileId, key: &K, entry_id: usize) {
        let mut keys = self.keys.lock().unwrap();
        let Some(file_keys) = keys.get_mut(&file_id) else {
            return;
        };
        if file_keys.get(key) == Some(&entry_id) {
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

/// Returns the id of a cache entry whose value is `value`.
///
/// Values are allocated for each insert and the eviction listener holds the old value, so
/// an evicted entry and an entry inserted again under the same key never share an id.
pub(crate) fn arc_entry_id<T: ?Sized>(value: &Arc<T>) -> usize {
    Arc::as_ptr(value).cast::<()>() as usize
}

/// Same as [arc_entry_id] for values stored as [Bytes] copied on insert.
pub(crate) fn bytes_entry_id(value: &Bytes) -> usize {
    value.as_ptr() as usize
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stale_removal_keeps_reinserted_key() {
        let keys = FileKeys::default();
        let file_id = FileId::random();
        keys.add(file_id, "k", 1);
        // The key is inserted again before the listener of the evicted entry runs.
        keys.add(file_id, "k", 2);
        keys.remove(file_id, &"k", 1);
        assert_eq!(vec!["k"], keys.take(file_id));

        keys.add(file_id, "k", 2);
        keys.remove(file_id, &"k", 2);
        assert!(keys.take(file_id).is_empty());
    }
}
