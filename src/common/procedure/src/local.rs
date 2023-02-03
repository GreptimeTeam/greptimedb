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
mod runner;

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use async_trait::async_trait;
use common_telemetry::logging;
use object_store::ObjectStore;
use snafu::ensure;
use tokio::sync::Notify;

use crate::error::{DuplicateProcedureSnafu, LoaderConflictSnafu, Result};
use crate::local::lock::LockMap;
use crate::local::runner::Runner;
use crate::procedure::BoxedProcedureLoader;
use crate::store::{ObjectStateStore, ProcedureMessage, ProcedureStore, StateStoreRef};
use crate::{
    BoxedProcedure, LockKey, ProcedureId, ProcedureManager, ProcedureState, ProcedureWithId,
};

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
pub(crate) struct ProcedureMeta {
    /// Id of this procedure.
    id: ProcedureId,
    /// Notify to wait for a lock.
    lock_notify: Notify,
    /// Parent procedure id.
    parent_id: Option<ProcedureId>,
    /// Notify to wait for subprocedures.
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

    /// Returns current [ProcedureState].
    fn state(&self) -> ProcedureState {
        let meta = self.exec_meta.lock().unwrap();
        meta.state.clone()
    }
}

/// Reference counted pointer to [ProcedureMeta].
type ProcedureMetaRef = Arc<ProcedureMeta>;
/// Procedure and its parent procedure id.
struct ProcedureAndParent(BoxedProcedure, Option<ProcedureId>);

/// Shared context of the manager.
pub(crate) struct ManagerContext {
    /// Procedure loaders. The key is the type name of the procedure which the loader returns.
    loaders: Mutex<HashMap<String, BoxedProcedureLoader>>,
    lock_map: LockMap,
    procedures: RwLock<HashMap<ProcedureId, ProcedureMetaRef>>,
    // TODO(yingwen): Now we never clean the messages. But when the root procedure is done, we
    // should be able to remove the its message and all its child messages.
    /// Messages loaded from the procedure store.
    messages: Mutex<HashMap<ProcedureId, ProcedureMessage>>,
}

impl ManagerContext {
    /// Returns a new [ManagerContext].
    fn new() -> ManagerContext {
        ManagerContext {
            loaders: Mutex::new(HashMap::new()),
            lock_map: LockMap::new(),
            procedures: RwLock::new(HashMap::new()),
            messages: Mutex::new(HashMap::new()),
        }
    }

    /// Returns true if the procedure with specific `procedure_id` exists.
    fn contains_procedure(&self, procedure_id: ProcedureId) -> bool {
        let procedures = self.procedures.read().unwrap();
        procedures.contains_key(&procedure_id)
    }

    /// Insert the procedure with specific `procedure_id`.
    ///
    /// # Panics
    /// Panics if the procedure already exists.
    fn insert_procedure(&self, procedure_id: ProcedureId, meta: ProcedureMetaRef) {
        let mut procedures = self.procedures.write().unwrap();
        let old = procedures.insert(procedure_id, meta);
        assert!(old.is_none());
    }

    /// Returns the [ProcedureState] of specific `procedure_id`.
    fn state(&self, procedure_id: ProcedureId) -> Option<ProcedureState> {
        let procedures = self.procedures.read().unwrap();
        procedures.get(&procedure_id).map(|meta| meta.state())
    }

    /// Notify a suspended parent procedure with specific `procedure_id` by its subprocedure.
    fn notify_by_subprocedure(&self, procedure_id: ProcedureId) {
        let procedures = self.procedures.read().unwrap();
        if let Some(meta) = procedures.get(&procedure_id) {
            meta.child_notify.notify_one();
        }
    }

    /// Load procedure with specific `procedure_id` from cached [ProcedureMessage]s.
    fn load_one_procedure(&self, procedure_id: ProcedureId) -> Option<ProcedureAndParent> {
        let messages = self.messages.lock().unwrap();
        let message = messages.get(&procedure_id)?;

        let loaders = self.loaders.lock().unwrap();
        let loader = loaders.get(&message.type_name).or_else(|| {
            logging::error!(
                "Loader not found, procedure_id: {}, type_name: {}",
                procedure_id,
                message.type_name
            );
            None
        })?;

        let procedure = loader(&message.data)
            .map_err(|e| {
                logging::error!(
                    "Failed to load procedure data, key: {}, source: {}",
                    procedure_id,
                    e
                );
                e
            })
            .ok()?;

        Some(ProcedureAndParent(procedure, message.parent_id))
    }
}

/// Config for [LocalManager].
#[derive(Debug)]
pub struct ManagerConfig {
    /// Object store
    object_store: ObjectStore,
}

/// A [ProcedureManager] that maintains procedure states locally.
pub struct LocalManager {
    manager_ctx: Arc<ManagerContext>,
    state_store: StateStoreRef,
}

impl LocalManager {
    /// Create a new [LocalManager] with specific `config`.
    pub fn new(config: ManagerConfig) -> LocalManager {
        LocalManager {
            manager_ctx: Arc::new(ManagerContext::new()),
            state_store: Arc::new(ObjectStateStore::new(config.object_store)),
        }
    }

    /// Submit a root procedure with given `procedure_id`.
    fn submit_root(&self, procedure_id: ProcedureId, step: u32, procedure: BoxedProcedure) {
        let meta = Arc::new(ProcedureMeta {
            id: procedure_id,
            lock_notify: Notify::new(),
            parent_id: None,
            child_notify: Notify::new(),
            parent_locks: Vec::new(),
            lock_key: procedure.lock_key(),
            exec_meta: Mutex::new(ExecMeta {
                state: ProcedureState::Running,
            }),
        });
        let runner = Runner {
            meta: meta.clone(),
            procedure,
            manager_ctx: self.manager_ctx.clone(),
            step,
            store: ProcedureStore::new(self.state_store.clone()),
        };

        self.manager_ctx.insert_procedure(procedure_id, meta);

        common_runtime::spawn_bg(async move {
            // Run the root procedure.
            runner.run().await
        });
    }
}

#[async_trait]
impl ProcedureManager for LocalManager {
    fn register_loader(&self, name: &str, loader: BoxedProcedureLoader) -> Result<()> {
        let mut loaders = self.manager_ctx.loaders.lock().unwrap();
        ensure!(!loaders.contains_key(name), LoaderConflictSnafu { name });

        loaders.insert(name.to_string(), loader);

        Ok(())
    }

    async fn submit(&self, procedure: ProcedureWithId) -> Result<()> {
        let procedure_id = procedure.id;
        ensure!(
            self.manager_ctx.contains_procedure(procedure_id),
            DuplicateProcedureSnafu { procedure_id }
        );

        self.submit_root(procedure.id, 0, procedure.procedure);

        Ok(())
    }

    async fn recover(&self) -> Result<()> {
        unimplemented!()
    }

    async fn procedure_state(&self, procedure_id: ProcedureId) -> Result<Option<ProcedureState>> {
        Ok(self.manager_ctx.state(procedure_id))
    }
}

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
