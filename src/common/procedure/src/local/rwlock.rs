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
use std::result::Result;
use std::sync::{Arc, Mutex};

use tokio::sync::{OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock, TryLockError};

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
    inner: Arc<Mutex<HashMap<K, Arc<RwLock<()>>>>>,
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

    /// Tries to lock the key with shared read access, returning immediately.
    pub fn try_read(&self, key: K) -> Result<OwnedRwLockReadGuard<()>, TryLockError> {
        let lock = {
            let mut locks = self.inner.lock().unwrap();
            locks.entry(key).or_default().clone()
        };

        lock.try_read_owned()
    }

    /// Tries lock this key with exclusive write access, returning immediately.
    pub fn try_write(&self, key: K) -> Result<OwnedRwLockWriteGuard<()>, TryLockError> {
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

    /// Clean up stale locks.
    pub fn clean_keys<'a>(&'a self, iter: impl IntoIterator<Item = &'a K>) {
        let mut locks = self.inner.lock().unwrap();

        let mut keys = Vec::new();
        for key in iter {
            if let Some(lock) = locks.get(key) {
                if lock.try_write().is_ok() {
                    keys.push(key);
                }
            }
        }

        for key in keys {
            locks.remove(key);
        }
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
    }
}
