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

use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::sync::Mutex;

use store_api::storage::FileId;

/// Keys of a cache grouped by the SST file they belong to.
///
/// Dropping a purged file's entries through `invalidate_entries_if` is not an option
/// for hot caches: moka evaluates every pending predicate on each `get` until its
/// housekeeper has scanned the whole cache, which falls behind under a steady purge rate.
#[derive(Debug)]
pub(crate) struct FileKeys<K> {
    keys: Mutex<HashMap<FileId, HashSet<K>>>,
}

impl<K> Default for FileKeys<K> {
    fn default() -> Self {
        Self {
            keys: Mutex::new(HashMap::new()),
        }
    }
}

impl<K: Hash + Eq + Clone> FileKeys<K> {
    pub(crate) fn add(&self, file_id: FileId, key: K) {
        self.keys
            .lock()
            .unwrap()
            .entry(file_id)
            .or_default()
            .insert(key);
    }

    /// Forgets a key removed from the cache.
    pub(crate) fn remove(&self, file_id: FileId, key: &K) {
        let mut keys = self.keys.lock().unwrap();
        if let Some(file_keys) = keys.get_mut(&file_id) {
            file_keys.remove(key);
            if file_keys.is_empty() {
                keys.remove(&file_id);
            }
        }
    }

    /// Removes and returns all keys of `file_id`.
    pub(crate) fn take(&self, file_id: FileId) -> HashSet<K> {
        self.keys
            .lock()
            .unwrap()
            .remove(&file_id)
            .unwrap_or_default()
    }
}
