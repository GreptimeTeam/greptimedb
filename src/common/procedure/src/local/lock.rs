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
use std::sync::RwLock;

use crate::local::ProcedureMetaRef;
use crate::ProcedureId;

/// A lock entry.
#[derive(Debug)]
struct Lock {
    /// Current lock owner.
    owner: ProcedureMetaRef,
    /// Waiter procedures.
    waiters: VecDeque<ProcedureMetaRef>,
}

impl Lock {
    /// Returns a [Lock] with specific `owner` procedure.
    fn from_owner(owner: ProcedureMetaRef) -> Lock {
        Lock {
            owner,
            waiters: VecDeque::new(),
        }
    }

    /// Try to pop a waiter from the waiter list, set it as owner
    /// and wake up the new owner.
    ///
    /// Returns false if there is no waiter in the waiter list.
    fn switch_owner(&mut self) -> bool {
        if let Some(waiter) = self.waiters.pop_front() {
            // Update owner.
            self.owner = waiter.clone();
            // We need to use notify_one() since the waiter may have not called `notified()` yet.
            waiter.lock_notify.notify_one();
            true
        } else {
            false
        }
    }
}

/// Manages lock entries for procedures.
pub(crate) struct LockMap {
    locks: RwLock<HashMap<String, Lock>>,
}

impl LockMap {
    /// Returns a new [LockMap].
    pub(crate) fn new() -> LockMap {
        LockMap {
            locks: RwLock::new(HashMap::new()),
        }
    }

    /// Acquire lock by `key` for procedure with specific `meta`.
    ///
    /// Though `meta` is cloneable, callers must ensure that only one `meta`
    /// is acquiring and holding the lock at the same time.
    ///
    /// # Panics
    /// Panics if the procedure acquires the lock recursively.
    pub(crate) async fn acquire_lock(&self, key: &str, meta: ProcedureMetaRef) {
        assert!(!self.hold_lock(key, meta.id));

        {
            let mut locks = self.locks.write().unwrap();
            if let Some(lock) = locks.get_mut(key) {
                // Lock already exists, but we don't expect that a procedure acquires
                // the same lock again.
                assert_ne!(lock.owner.id, meta.id);

                // Add this procedure to the waiter list. Here we don't check
                // whether the procedure is already in the waiter list as we
                // expect that a procedure should not wait for two lock simultaneously.
                lock.waiters.push_back(meta.clone());
            } else {
                let _ = locks.insert(key.to_string(), Lock::from_owner(meta));

                return;
            }
        }

        // Wait for notify.
        meta.lock_notify.notified().await;

        assert!(self.hold_lock(key, meta.id));
    }

    /// Release lock by `key`.
    pub(crate) fn release_lock(&self, key: &str, procedure_id: ProcedureId) {
        let mut locks = self.locks.write().unwrap();
        if let Some(lock) = locks.get_mut(key) {
            if lock.owner.id != procedure_id {
                // This is not the lock owner.
                return;
            }

            if !lock.switch_owner() {
                // No body waits for this lock, we can remove the lock entry.
                let _ = locks.remove(key);
            }
        }
    }

    /// Returns true if the procedure with specific `procedure_id` holds the
    /// lock of `key`.
    fn hold_lock(&self, key: &str, procedure_id: ProcedureId) -> bool {
        let locks = self.locks.read().unwrap();
        locks
            .get(key)
            .map(|lock| lock.owner.id == procedure_id)
            .unwrap_or(false)
    }

    /// Returns true if the procedure is waiting for the lock `key`.
    #[cfg(test)]
    fn waiting_lock(&self, key: &str, procedure_id: ProcedureId) -> bool {
        let locks = self.locks.read().unwrap();
        locks
            .get(key)
            .map(|lock| lock.waiters.iter().any(|meta| meta.id == procedure_id))
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::local::test_util;

    #[test]
    fn test_lock_no_waiter() {
        let meta = Arc::new(test_util::procedure_meta_for_test());
        let mut lock = Lock::from_owner(meta);

        assert!(!lock.switch_owner());
    }

    #[tokio::test]
    async fn test_lock_with_waiter() {
        let owner = Arc::new(test_util::procedure_meta_for_test());
        let mut lock = Lock::from_owner(owner);

        let waiter = Arc::new(test_util::procedure_meta_for_test());
        lock.waiters.push_back(waiter.clone());

        assert!(lock.switch_owner());
        assert!(lock.waiters.is_empty());

        waiter.lock_notify.notified().await;
        assert_eq!(lock.owner.id, waiter.id);
    }

    #[tokio::test]
    async fn test_lock_map() {
        let key = "hello";

        let owner = Arc::new(test_util::procedure_meta_for_test());
        let lock_map = Arc::new(LockMap::new());
        lock_map.acquire_lock(key, owner.clone()).await;

        let waiter = Arc::new(test_util::procedure_meta_for_test());
        let waiter_id = waiter.id;

        // Waiter release the lock, this should not take effect.
        lock_map.release_lock(key, waiter_id);

        let lock_map2 = lock_map.clone();
        let owner_id = owner.id;
        let handle = tokio::spawn(async move {
            assert!(lock_map2.hold_lock(key, owner_id));
            assert!(!lock_map2.hold_lock(key, waiter_id));

            // Waiter wait for lock.
            lock_map2.acquire_lock(key, waiter.clone()).await;

            assert!(lock_map2.hold_lock(key, waiter_id));
        });

        // Owner still holds the lock.
        assert!(lock_map.hold_lock(key, owner_id));

        // Wait until the waiter acquired the lock
        while !lock_map.waiting_lock(key, waiter_id) {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        // Release lock
        lock_map.release_lock(key, owner_id);
        assert!(!lock_map.hold_lock(key, owner_id));

        // Wait for task.
        handle.await.unwrap();
        // The waiter should hold the lock now.
        assert!(lock_map.hold_lock(key, waiter_id));

        lock_map.release_lock(key, waiter_id);
    }
}
