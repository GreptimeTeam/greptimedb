// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Synchronization utilities

use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex, MutexGuard};

use arc_swap::ArcSwap;

/// A thread safe clone-on-write cell.
///
/// Each read returns a read only clone of the internal data and won't block
/// write. Write to the cell data needs to acquire a lock txn first and
/// modifications are not visible to others until the txn is committed.
#[derive(Debug)]
pub struct CowCell<T> {
    inner: ArcSwap<T>,
    mutex: Mutex<()>,
}

impl<T> CowCell<T> {
    /// Create a new cell.
    pub fn new(data: T) -> CowCell<T> {
        CowCell {
            inner: ArcSwap::from(Arc::new(data)),
            mutex: Mutex::new(()),
        }
    }

    /// Get a read only clone from the cell.
    pub fn get(&self) -> Arc<T> {
        self.inner.load_full()
    }
}

impl<T: Clone> CowCell<T> {
    /// Acquire a write txn, blocking the current thread.
    ///
    /// Note that this will clone the inner data.
    pub fn lock(&self) -> TxnGuard<T> {
        let _guard = self.mutex.lock().unwrap();
        // Acquire a clone of data inside lock.
        let data = (*self.get()).clone();

        TxnGuard {
            inner: &self.inner,
            data,
            _guard,
        }
    }
}

/// A RAII implementation of a write transaction of the [CowCell].
///
/// When this txn is dropped (falls out of scope or committed), the lock will be
/// unlocked, but updates to the content won't be visible unless the txn is committed.
#[must_use = "if unused the CowCell will immediately unlock"]
pub struct TxnGuard<'a, T: Clone> {
    inner: &'a ArcSwap<T>,
    data: T,
    _guard: MutexGuard<'a, ()>,
}

impl<T: Clone> TxnGuard<'_, T> {
    /// Commit updates to the cell and release the lock.
    pub fn commit(self) {
        let data = Arc::new(self.data);
        self.inner.store(data);
    }
}

impl<T: Clone> Deref for TxnGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.data
    }
}

impl<T: Clone> DerefMut for TxnGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.data
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cow_cell_commit() {
        let cell = CowCell::new(10);
        assert_eq!(10, *cell.get());

        let mut data = cell.lock();
        assert_eq!(10, *data);

        // It's okay to get read only clone from the cell during lock is held.
        assert_eq!(10, *cell.get());

        *data += 2;

        assert_eq!(*data, 12);
        // The modification is still not visible.
        assert_eq!(10, *cell.get());

        // Commit the txn.
        data.commit();

        // Once the guard is committed, the new data is visible.
        assert_eq!(12, *cell.get());
    }

    #[test]
    fn test_cow_cell_cancel() {
        let cell = CowCell::new(10);
        assert_eq!(10, *cell.get());

        {
            let mut data = cell.lock();
            *data += 2;
        }

        // The update is not committed, should not be visible.
        assert_eq!(10, *cell.get());
    }
}
