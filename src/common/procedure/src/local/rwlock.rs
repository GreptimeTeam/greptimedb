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

pub enum OwnedKeyRwLockGuard<K: Eq + Hash> {
    Read(OwnedKeyRwLockReadGuard<K>),
    Write(OwnedKeyRwLockWriteGuard<K>),
}

impl<K: Eq + Hash> From<OwnedKeyRwLockReadGuard<K>> for OwnedKeyRwLockGuard<K> {
    fn from(guard: OwnedKeyRwLockReadGuard<K>) -> Self {
        OwnedKeyRwLockGuard::Read(guard)
    }
}

impl<K: Eq + Hash> From<OwnedKeyRwLockWriteGuard<K>> for OwnedKeyRwLockGuard<K> {
    fn from(guard: OwnedKeyRwLockWriteGuard<K>) -> Self {
        OwnedKeyRwLockGuard::Write(guard)
    }
}

pub struct OwnedKeyRwLockReadGuard<K: Eq + Hash> {
    key: K,
    inner: Arc<Mutex<HashMap<K, Arc<RwLock<()>>>>>,
    guard: Option<OwnedRwLockReadGuard<()>>,
}

pub struct OwnedKeyRwLockWriteGuard<K: Eq + Hash> {
    key: K,
    inner: Arc<Mutex<HashMap<K, Arc<RwLock<()>>>>>,
    guard: Option<OwnedRwLockWriteGuard<()>>,
}

impl<K: Eq + Hash> Drop for OwnedKeyRwLockWriteGuard<K> {
    fn drop(&mut self) {
        // Always releases inner lock first.
        {
            self.guard.take().unwrap();
        }
        let mut locks = self.inner.lock().unwrap();
        KeyRwLock::remove_key(&mut locks, &self.key);
    }
}

impl<K: Eq + Hash> Drop for OwnedKeyRwLockReadGuard<K> {
    fn drop(&mut self) {
        // Always releases inner lock first.
        {
            self.guard.take().unwrap();
        }
        let mut locks = self.inner.lock().unwrap();
        KeyRwLock::remove_key(&mut locks, &self.key);
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
    K: Eq + Hash,
{
    /// Removes a key lock if it's exists and no one in use.
    pub(crate) fn remove_key(locks: &mut HashMap<K, Arc<RwLock<()>>>, key: &K) {
        if let Some(lock) = locks.get(key) {
            if lock.try_write().is_ok() {
                locks.remove(key);
            }
        }
    }
}

impl<K> KeyRwLock<K>
where
    K: Eq + Hash + Send + Clone,
{
    pub fn new() -> Self {
        KeyRwLock {
            inner: Default::default(),
        }
    }

    /// Locks the key with shared read access, returning a guard.
    pub async fn read(&self, key: K) -> OwnedKeyRwLockReadGuard<K> {
        let lock = {
            let mut locks = self.inner.lock().unwrap();
            locks.entry(key.clone()).or_default().clone()
        };

        OwnedKeyRwLockReadGuard {
            key,
            inner: self.inner.clone(),
            guard: Some(lock.read_owned().await),
        }
    }

    /// Locks the key with exclusive write access, returning a guard.
    pub async fn write(&self, key: K) -> OwnedKeyRwLockWriteGuard<K> {
        let lock = {
            let mut locks = self.inner.lock().unwrap();
            locks.entry(key.clone()).or_default().clone()
        };

        OwnedKeyRwLockWriteGuard {
            key,
            inner: self.inner.clone(),
            guard: Some(lock.write_owned().await),
        }
    }

    /// Tries to lock the key with shared read access, returning immediately.
    pub fn try_read(&self, key: K) -> Result<OwnedKeyRwLockReadGuard<K>, TryLockError> {
        let lock = {
            let mut locks = self.inner.lock().unwrap();
            locks.entry(key.clone()).or_default().clone()
        };

        let guard = lock.try_read_owned()?;

        Ok(OwnedKeyRwLockReadGuard {
            key,
            inner: self.inner.clone(),
            guard: Some(guard),
        })
    }

    /// Tries lock this key with exclusive write access, returning immediately.
    pub fn try_write(&self, key: K) -> Result<OwnedKeyRwLockWriteGuard<K>, TryLockError> {
        let lock = {
            let mut locks = self.inner.lock().unwrap();
            locks.entry(key.clone()).or_default().clone()
        };

        let guard = lock.try_write_owned()?;

        Ok(OwnedKeyRwLockWriteGuard {
            key,
            inner: self.inner.clone(),
            guard: Some(guard),
        })
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

        assert!(lock_key.is_empty());
    }
}
