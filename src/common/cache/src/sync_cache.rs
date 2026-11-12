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

use std::borrow::Borrow;
use std::hash::Hash;
use std::sync::Arc;

use crate::RemovalCause;

/// A weighted completed-value cache for synchronous callers.
///
/// Uses the same backend and removal causes as [`crate::Cache`]. Callers can
/// serialize publication and invalidation with their own lock without awaiting
/// under that lock. Values should be inexpensive to clone, such as `Arc<T>`.
#[derive(Clone)]
pub struct SyncCache<K, V> {
    inner: moka::sync::Cache<K, V>,
}

impl<K, V> SyncCache<K, V>
where
    K: Eq + Hash + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    /// Creates a cache with the caller's weight and eviction accounting.
    pub fn new<W, L>(name: &str, capacity: u64, weigher: W, listener: L) -> Self
    where
        W: Fn(&K, &V) -> u32 + Send + Sync + 'static,
        L: Fn(Arc<K>, V, RemovalCause) + Send + Sync + 'static,
    {
        Self {
            inner: moka::sync::Cache::builder()
                .name(name)
                .max_capacity(capacity)
                .weigher(weigher)
                .eviction_listener(move |key, value, cause| listener(key, value, cause.into()))
                .build(),
        }
    }

    /// Gets a completed value without starting a load.
    pub fn get<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.inner.get(key)
    }

    /// Submits a completed value to the cache's admission and eviction policy.
    /// Insertion does not guarantee retention.
    pub fn insert(&self, key: K, value: V) {
        self.inner.insert(key, value);
    }

    /// Applies pending admission, eviction, and accounting work.
    pub fn run_pending_tasks(&self) {
        self.inner.run_pending_tasks();
    }

    /// Invalidates a completed entry. Concurrent producers are fenced by the caller.
    pub fn invalidate<Q>(&self, key: &Q)
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.inner.invalidate(key);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    #[test]
    fn test_borrowed_lookup_and_invalidation() {
        let removed = Arc::new(Mutex::new(Vec::new()));
        let events = removed.clone();
        let cache = SyncCache::new(
            "test",
            1024,
            |_: &String, _: &Arc<usize>| 1,
            move |key, value, cause| events.lock().unwrap().push((key, value, cause)),
        );
        let value = Arc::new(7);
        cache.insert("a".to_string(), value.clone());
        cache.insert("b".to_string(), Arc::new(9));
        assert!(Arc::ptr_eq(&cache.get("a").unwrap(), &value));
        cache.invalidate("a");
        assert!(cache.get("a").is_none());
        assert_eq!(*cache.get("b").unwrap(), 9);
        cache.run_pending_tasks();
        assert_eq!(
            *removed.lock().unwrap(),
            vec![(Arc::new("a".to_string()), value, RemovalCause::Explicit)]
        );
    }
}
