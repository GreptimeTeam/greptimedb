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

use std::sync::Arc;
use crate::{ProcedureId, ProcedureState, LockKey};
use tokio::sync::Notify;
use std::sync::Mutex;

/// Mutable metadata of a procedure during execution.
#[derive(Debug)]
struct ExecMeta {
    /// Current procedure state.
    state: ProcedureState,
}

/// Shared metadata of a procedure.
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
    /// Lock this procedure needs additionally.
    ///
    /// If the parent procedure already owns the lock this procedure
    /// needs, then we also set this field to `None`.
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

type ProcedureMetaRef = Arc<ProcedureMeta>;
