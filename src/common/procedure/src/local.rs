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

mod lock;

use std::sync::{Arc, Mutex};

use tokio::sync::Notify;

use crate::{LockKey, ProcedureId, ProcedureState};

/// Mutable metadata of a procedure during execution.
#[derive(Debug)]
struct ExecMeta {
    /// Current procedure state.
    state: ProcedureState,
}

/// Shared metadata of a procedure.
///
/// # Note
/// [Notify] is not a condition variable, we can't guarantee the waiters are notified
/// if they didn't call `notified()` before we signal the notify. So we
/// 1. use dedicated notify for each condition, such as waiting for a lock, waiting
/// for children;
/// 2. always use `notify_one` and ensure there are only one waiter.
#[derive(Debug)]
struct ProcedureMeta {
    /// Id of this procedure.
    id: ProcedureId,
    /// Notify to waiting for a lock.
    lock_notify: Notify,
    /// Parent procedure id.
    parent_id: Option<ProcedureId>,
    /// Notify to waiting for subprocedures.
    child_notify: Notify,
    /// Locks inherted from the parent procedure.
    parent_locks: Vec<LockKey>,
    /// Lock not in `parent_locks` but required by this procedure.
    ///
    /// If the parent procedure already owns the lock that this procedure
    /// needs, we set this field to `None`.
    lock_key: Option<LockKey>,
    /// Mutable status during execution.
    exec_meta: Mutex<ExecMeta>,
}

impl ProcedureMeta {
    /// Return all locks the procedure needs.
    fn locks_needed(&self) -> Vec<LockKey> {
        let num_locks = self.parent_locks.len() + if self.lock_key.is_some() { 1 } else { 0 };
        let mut locks = Vec::with_capacity(num_locks);
        locks.extend_from_slice(&self.parent_locks);
        if let Some(key) = &self.lock_key {
            locks.push(key.clone());
        }

        locks
    }
}

/// Reference counted pointer to [ProcedureMeta].
type ProcedureMetaRef = Arc<ProcedureMeta>;

/// Create a new [ProcedureMeta] for test purpose.
#[cfg(test)]
fn procedure_meta_for_test() -> ProcedureMeta {
    ProcedureMeta {
        id: ProcedureId::random(),
        lock_notify: Notify::new(),
        parent_id: None,
        child_notify: Notify::new(),
        parent_locks: Vec::new(),
        lock_key: None,
        exec_meta: Mutex::new(ExecMeta {
            state: ProcedureState::Running,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_locks_needed() {
        let mut meta = procedure_meta_for_test();
        let locks = meta.locks_needed();
        assert!(locks.is_empty());

        let parent_locks = vec![LockKey::new("a"), LockKey::new("b")];
        meta.parent_locks = parent_locks.clone();
        let locks = meta.locks_needed();
        assert_eq!(parent_locks, locks);

        meta.lock_key = Some(LockKey::new("c"));
        let locks = meta.locks_needed();
        assert_eq!(
            vec![LockKey::new("a"), LockKey::new("b"), LockKey::new("c")],
            locks
        );
    }
}
