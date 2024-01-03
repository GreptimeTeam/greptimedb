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

use tokio::sync::{OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock};

pub enum OwnedKeyRwLockGuard {
    Read(OwnedRwLockReadGuard<()>),
    Write(OwnedRwLockWriteGuard<()>),
}

impl From<OwnedRwLockReadGuard<()>> for OwnedKeyRwLockGuard {
    fn from(guard: OwnedRwLockReadGuard<()>) -> Self {
        OwnedKeyRwLockGuard::Read(guard)
    }
}

impl From<OwnedRwLockWriteGuard<()>> for OwnedKeyRwLockGuard {
    fn from(guard: OwnedRwLockWriteGuard<()>) -> Self {
        OwnedKeyRwLockGuard::Write(guard)
    }
}

/// Locks based on a key, allowing other keys to lock independently.
#[derive(Debug)]
pub struct KeyRwLock<K> {
    /// The inner map of locks for specific keys.
    inner: Mutex<HashMap<K, Arc<RwLock<()>>>>,
}

impl<K> KeyRwLock<K>
where
    K: Eq + Hash + Clone,
{
    pub fn new() -> Self {
        KeyRwLock {
            inner: Default::default(),
        }
    }

    /// Locks the key with shared read access, returning a guard.
    pub async fn read(&self, key: K) -> OwnedRwLockReadGuard<()> {
        let lock = {
            let mut locks = self.inner.lock().unwrap();
            locks.entry(key).or_default().clone()
        };

        lock.read_owned().await
    }

    /// Locks the key with exclusive write access, returning a guard.
    pub async fn write(&self, key: K) -> OwnedRwLockWriteGuard<()> {
        let lock = {
            let mut locks = self.inner.lock().unwrap();
            locks.entry(key).or_default().clone()
        };

        lock.write_owned().await
    }

    /// Clean up stale locks.
    ///
    /// Note: It only cleans a lock if
    /// - Its strong ref count equals one.
    /// - Able to acquire the write lock.
    pub fn clean_keys<'a>(&'a self, iter: impl IntoIterator<Item = &'a K>) {
        let mut locks = self.inner.lock().unwrap();
        let mut keys = Vec::new();
        for key in iter {
            if let Some(lock) = locks.get(key) {
                if lock.try_write().is_ok() {
                    debug_assert_eq!(Arc::weak_count(lock), 0);
                    // Ensures nobody keeps this ref.
                    if Arc::strong_count(lock) == 1 {
                        keys.push(key);
                    }
                }
            }
        }

        for key in keys {
            locks.remove(key);
        }
    }
}

#[cfg(test)]
impl<K> KeyRwLock<K>
where
    K: Eq + Hash + Clone,
{
    /// Tries to lock the key with shared read access, returning immediately.
    pub fn try_read(&self, key: K) -> Result<OwnedRwLockReadGuard<()>, tokio::sync::TryLockError> {
        let lock = {
            let mut locks = self.inner.lock().unwrap();
            locks.entry(key).or_default().clone()
        };

        lock.try_read_owned()
    }

    /// Tries lock this key with exclusive write access, returning immediately.
    pub fn try_write(
        &self,
        key: K,
    ) -> Result<OwnedRwLockWriteGuard<()>, tokio::sync::TryLockError> {
        let lock = {
            let mut locks = self.inner.lock().unwrap();
            locks.entry(key).or_default().clone()
        };

        lock.try_write_owned()
    }

    /// Returns number of keys.
    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().len()
    }

    /// Returns true the inner map is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_naive() {
        let lock_key = KeyRwLock::new();

        {
            let _guard = lock_key.read("test1").await;
            assert_eq!(lock_key.len(), 1);
            assert!(lock_key.try_read("test1").is_ok());
            assert!(lock_key.try_write("test1").is_err());
        }

        {
            let _guard0 = lock_key.write("test2").await;
            let _guard = lock_key.write("test1").await;
            assert_eq!(lock_key.len(), 2);
            assert!(lock_key.try_read("test1").is_err());
            assert!(lock_key.try_write("test1").is_err());
        }

        assert_eq!(lock_key.len(), 2);

        lock_key.clean_keys(&vec!["test1", "test2"]);
        assert!(lock_key.is_empty());

        let mut guards = Vec::new();
        for key in ["test1", "test2"] {
            guards.push(lock_key.read(key).await);
        }
        while !guards.is_empty() {
            guards.pop();
        }
        lock_key.clean_keys(vec![&"test1", &"test2"]);
        assert_eq!(lock_key.len(), 0);
    }

    #[tokio::test]
    async fn test_clean_keys() {
        let lock_key = KeyRwLock::<&str>::new();
        {
            let rwlock = {
                lock_key
                    .inner
                    .lock()
                    .unwrap()
                    .entry("test")
                    .or_default()
                    .clone()
            };
            assert_eq!(Arc::strong_count(&rwlock), 2);
            let _guard = rwlock.read_owned().await;

            {
                let inner = lock_key.inner.lock().unwrap();
                let rwlock = inner.get("test").unwrap();
                assert_eq!(Arc::strong_count(rwlock), 2);
            }
        }

        {
            let rwlock = {
                lock_key
                    .inner
                    .lock()
                    .unwrap()
                    .entry("test")
                    .or_default()
                    .clone()
            };
            assert_eq!(Arc::strong_count(&rwlock), 2);
            let _guard = rwlock.write_owned().await;

            {
                let inner = lock_key.inner.lock().unwrap();
                let rwlock = inner.get("test").unwrap();
                assert_eq!(Arc::strong_count(rwlock), 2);
            }
        }

        {
            let inner = lock_key.inner.lock().unwrap();
            let rwlock = inner.get("test").unwrap();
            assert_eq!(Arc::strong_count(rwlock), 1);
        }

        // Someone has the ref of the rwlock, but it waits to be granted the lock.
        let rwlock = {
            lock_key
                .inner
                .lock()
                .unwrap()
                .entry("test")
                .or_default()
                .clone()
        };
        assert_eq!(Arc::strong_count(&rwlock), 2);
        // However, One thread trying to remove the "test" key should have no effect.
        lock_key.clean_keys(vec![&"test"]);
        // Should get the rwlock.
        {
            let inner = lock_key.inner.lock().unwrap();
            inner.get("test").unwrap();
        }
    }
}
