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

use std::collections::{HashMap, VecDeque};
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
    /// Id of child procedures.
    children: Vec<ProcedureId>,
}

impl Default for ExecMeta {
    fn default() -> ExecMeta {
        ExecMeta {
            state: ProcedureState::Running,
            children: Vec::new(),
        }
    }
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
    /// Lock required by this procedure.
    lock_key: Option<LockKey>,
    /// Mutable status during execution.
    exec_meta: Mutex<ExecMeta>,
}

impl ProcedureMeta {
    /// Returns current [ProcedureState].
    fn state(&self) -> ProcedureState {
        let meta = self.exec_meta.lock().unwrap();
        meta.state.clone()
    }

    /// Update current [ProcedureState].
    fn set_state(&self, state: ProcedureState) {
        let mut meta = self.exec_meta.lock().unwrap();
        meta.state = state;
    }

    /// Push `procedure_id` of the subprocedure to the metadata.
    fn push_child(&self, procedure_id: ProcedureId) {
        let mut meta = self.exec_meta.lock().unwrap();
        meta.children.push(procedure_id);
    }

    /// Append subprocedures to given `buffer`.
    fn list_children(&self, buffer: &mut Vec<ProcedureId>) {
        let meta = self.exec_meta.lock().unwrap();
        buffer.extend_from_slice(&meta.children);
    }

    /// Returns the number of subprocedures.
    fn num_children(&self) -> usize {
        self.exec_meta.lock().unwrap().children.len()
    }
}

/// Reference counted pointer to [ProcedureMeta].
type ProcedureMetaRef = Arc<ProcedureMeta>;

/// Procedure loaded from store.
struct LoadedProcedure {
    procedure: BoxedProcedure,
    parent_id: Option<ProcedureId>,
    step: u32,
}

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

    /// Try to insert the `procedure` to the context if there is no procedure
    /// with same [ProcedureId].
    ///
    /// Returns `false` if there is already a procedure using the same [ProcedureId].
    fn try_insert_procedure(&self, meta: ProcedureMetaRef) -> bool {
        let mut procedures = self.procedures.write().unwrap();
        if procedures.contains_key(&meta.id) {
            return false;
        }

        let old = procedures.insert(meta.id, meta);
        debug_assert!(old.is_none());

        true
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
    fn load_one_procedure(&self, procedure_id: ProcedureId) -> Option<LoadedProcedure> {
        let message = {
            let messages = self.messages.lock().unwrap();
            messages.get(&procedure_id).cloned()?
        };

        self.load_one_procedure_from_message(procedure_id, &message)
    }

    /// Load procedure from specific [ProcedureMessage].
    fn load_one_procedure_from_message(
        &self,
        procedure_id: ProcedureId,
        message: &ProcedureMessage,
    ) -> Option<LoadedProcedure> {
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

        Some(LoadedProcedure {
            procedure,
            parent_id: message.parent_id,
            step: message.step,
        })
    }

    /// Returns all procedures in the tree (including given `root` procedure).
    ///
    /// If callers need a consistent view of the tree, they must ensure no new
    /// procedure is added to the tree during using this method.
    fn procedures_in_tree(&self, root: &ProcedureMetaRef) -> Vec<ProcedureId> {
        let sub_num = root.num_children();
        // Reserve capacity for the root procedure and its children.
        let mut procedures = Vec::with_capacity(1 + sub_num);

        let mut queue = VecDeque::with_capacity(1 + sub_num);
        // Push the root procedure to the queue.
        queue.push_back(root.clone());

        let mut children_ids = Vec::with_capacity(sub_num);
        let mut children = Vec::with_capacity(sub_num);
        while let Some(meta) = queue.pop_front() {
            procedures.push(meta.id);

            // Find metadatas of children.
            children_ids.clear();
            meta.list_children(&mut children_ids);
            self.find_procedures(&children_ids, &mut children);

            // Traverse children later.
            for child in children.drain(..) {
                queue.push_back(child);
            }
        }

        procedures
    }

    /// Finds procedures by given `procedure_ids`.
    ///
    /// Ignores the id if corresponding procedure is not found.
    fn find_procedures(&self, procedure_ids: &[ProcedureId], metas: &mut Vec<ProcedureMetaRef>) {
        let procedures = self.procedures.read().unwrap();
        for procedure_id in procedure_ids {
            if let Some(meta) = procedures.get(procedure_id) {
                metas.push(meta.clone());
            }
        }
    }

    /// Remove cached [ProcedureMessage] by ids.
    fn remove_messages(&self, procedure_ids: &[ProcedureId]) {
        let mut messages = self.messages.lock().unwrap();
        for procedure_id in procedure_ids {
            messages.remove(procedure_id);
        }
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
    fn submit_root(
        &self,
        procedure_id: ProcedureId,
        step: u32,
        procedure: BoxedProcedure,
    ) -> Result<()> {
        let meta = Arc::new(ProcedureMeta {
            id: procedure_id,
            lock_notify: Notify::new(),
            parent_id: None,
            child_notify: Notify::new(),
            lock_key: procedure.lock_key(),
            exec_meta: Mutex::new(ExecMeta::default()),
        });
        let runner = Runner {
            meta: meta.clone(),
            procedure,
            manager_ctx: self.manager_ctx.clone(),
            step,
            store: ProcedureStore::new(self.state_store.clone()),
        };

        // Inserts meta into the manager before actually spawnd the runner.
        ensure!(
            self.manager_ctx.try_insert_procedure(meta),
            DuplicateProcedureSnafu { procedure_id },
        );

        common_runtime::spawn_bg(async move {
            // Run the root procedure.
            let _ = runner.run().await;
        });

        Ok(())
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
            !self.manager_ctx.contains_procedure(procedure_id),
            DuplicateProcedureSnafu { procedure_id }
        );

        self.submit_root(procedure.id, 0, procedure.procedure)?;

        Ok(())
    }

    async fn recover(&self) -> Result<()> {
        logging::info!("LocalManager start to recover");

        let procedure_store = ProcedureStore::new(self.state_store.clone());
        let messages = procedure_store.load_messages().await?;

        for (procedure_id, message) in &messages {
            if message.parent_id.is_none() {
                // This is the root procedure. We only submit the root procedure as it will
                // submit sub-procedures to the manager.
                let Some(loaded_procedure) = self.manager_ctx.load_one_procedure_from_message(*procedure_id, message) else {
                    // Try to load other procedures.
                    continue;
                };

                logging::info!(
                    "Recover root procedure {}-{}, step: {}",
                    loaded_procedure.procedure.type_name(),
                    procedure_id,
                    loaded_procedure.step
                );

                if let Err(e) = self.submit_root(
                    *procedure_id,
                    loaded_procedure.step,
                    loaded_procedure.procedure,
                ) {
                    logging::error!(e; "Failed to recover procedure {}", procedure_id);
                }
            }
        }

        Ok(())
    }

    async fn procedure_state(&self, procedure_id: ProcedureId) -> Result<Option<ProcedureState>> {
        Ok(self.manager_ctx.state(procedure_id))
    }
}

/// Create a new [ProcedureMeta] for test purpose.
#[cfg(test)]
mod test_util {
    use object_store::services::fs::Builder;
    use tempdir::TempDir;

    use super::*;

    pub(crate) fn procedure_meta_for_test() -> ProcedureMeta {
        ProcedureMeta {
            id: ProcedureId::random(),
            lock_notify: Notify::new(),
            parent_id: None,
            child_notify: Notify::new(),
            lock_key: None,
            exec_meta: Mutex::new(ExecMeta::default()),
        }
    }

    pub(crate) fn new_object_store(dir: &TempDir) -> ObjectStore {
        let store_dir = dir.path().to_str().unwrap();
        let accessor = Builder::default().root(store_dir).build().unwrap();
        ObjectStore::new(accessor)
    }
}

#[cfg(test)]
mod tests {
    use tempdir::TempDir;

    use super::*;
    use crate::error::Error;
    use crate::{Context, Procedure, Status};

    #[test]
    fn test_manager_context() {
        let ctx = ManagerContext::new();
        let meta = Arc::new(test_util::procedure_meta_for_test());

        assert!(!ctx.contains_procedure(meta.id));
        assert!(ctx.state(meta.id).is_none());

        assert!(ctx.try_insert_procedure(meta.clone()));
        assert!(ctx.contains_procedure(meta.id));

        assert_eq!(ProcedureState::Running, ctx.state(meta.id).unwrap());
        meta.set_state(ProcedureState::Done);
        assert_eq!(ProcedureState::Done, ctx.state(meta.id).unwrap());
    }

    #[test]
    fn test_manager_context_insert_duplicate() {
        let ctx = ManagerContext::new();
        let meta = Arc::new(test_util::procedure_meta_for_test());

        assert!(ctx.try_insert_procedure(meta.clone()));
        assert!(!ctx.try_insert_procedure(meta));
    }

    fn new_child(parent_id: ProcedureId, ctx: &ManagerContext) -> ProcedureMetaRef {
        let mut child = test_util::procedure_meta_for_test();
        child.parent_id = Some(parent_id);
        let child = Arc::new(child);
        assert!(ctx.try_insert_procedure(child.clone()));

        let mut parent = Vec::new();
        ctx.find_procedures(&[parent_id], &mut parent);
        parent[0].push_child(child.id);

        child
    }

    #[test]
    fn test_procedures_in_tree() {
        let ctx = ManagerContext::new();
        let root = Arc::new(test_util::procedure_meta_for_test());
        assert!(ctx.try_insert_procedure(root.clone()));

        assert_eq!(1, ctx.procedures_in_tree(&root).len());

        let child1 = new_child(root.id, &ctx);
        let child2 = new_child(root.id, &ctx);

        let child3 = new_child(child1.id, &ctx);
        let child4 = new_child(child1.id, &ctx);

        let child5 = new_child(child2.id, &ctx);

        let expect = vec![
            root.id, child1.id, child2.id, child3.id, child4.id, child5.id,
        ];
        assert_eq!(expect, ctx.procedures_in_tree(&root));
    }

    #[derive(Debug)]
    struct ProcedureToLoad {
        content: String,
    }

    #[async_trait]
    impl Procedure for ProcedureToLoad {
        fn type_name(&self) -> &str {
            "ProcedureToLoad"
        }

        async fn execute(&mut self, _ctx: &Context) -> Result<Status> {
            Ok(Status::Done)
        }

        fn dump(&self) -> Result<String> {
            Ok(self.content.clone())
        }

        fn lock_key(&self) -> Option<LockKey> {
            None
        }
    }

    impl ProcedureToLoad {
        fn new(content: &str) -> ProcedureToLoad {
            ProcedureToLoad {
                content: content.to_string(),
            }
        }

        fn loader() -> BoxedProcedureLoader {
            let f = |json: &str| {
                let procedure = ProcedureToLoad::new(json);
                Ok(Box::new(procedure) as _)
            };
            Box::new(f)
        }
    }

    #[test]
    fn test_register_loader() {
        let dir = TempDir::new("register").unwrap();
        let config = ManagerConfig {
            object_store: test_util::new_object_store(&dir),
        };
        let manager = LocalManager::new(config);

        manager
            .register_loader("ProcedureToLoad", ProcedureToLoad::loader())
            .unwrap();
        // Register duplicate loader.
        let err = manager
            .register_loader("ProcedureToLoad", ProcedureToLoad::loader())
            .unwrap_err();
        assert!(matches!(err, Error::LoaderConflict { .. }), "{err}");
    }

    #[tokio::test]
    async fn test_recover() {
        let dir = TempDir::new("recover").unwrap();
        let object_store = test_util::new_object_store(&dir);
        let config = ManagerConfig {
            object_store: object_store.clone(),
        };
        let manager = LocalManager::new(config);

        manager
            .register_loader("ProcedureToLoad", ProcedureToLoad::loader())
            .unwrap();

        // Prepare data
        let procedure_store = ProcedureStore::from(object_store.clone());
        let root: BoxedProcedure = Box::new(ProcedureToLoad::new("test recover manager"));
        let root_id = ProcedureId::random();
        // Prepare data for the root procedure.
        for step in 0..3 {
            procedure_store
                .store_procedure(root_id, step, &root, None)
                .await
                .unwrap();
        }

        let child: BoxedProcedure = Box::new(ProcedureToLoad::new("a child procedure"));
        let child_id = ProcedureId::random();
        // Prepare data for the child procedure
        for step in 0..2 {
            procedure_store
                .store_procedure(child_id, step, &child, Some(root_id))
                .await
                .unwrap();
        }

        // Recover the manager
        manager.recover().await.unwrap();

        // The manager should submit the root procedure.
        assert!(manager.procedure_state(root_id).await.unwrap().is_some());
        // Since the mocked root procedure actually doesn't submit subprocedures, so there is no
        // related state.
        assert!(manager.procedure_state(child_id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_submit_procedure() {
        let dir = TempDir::new("submit").unwrap();
        let config = ManagerConfig {
            object_store: test_util::new_object_store(&dir),
        };
        let manager = LocalManager::new(config);

        #[derive(Debug)]
        struct MockProcedure {}

        #[async_trait]
        impl Procedure for MockProcedure {
            fn type_name(&self) -> &str {
                "MockProcedure"
            }

            async fn execute(&mut self, _ctx: &Context) -> Result<Status> {
                unimplemented!()
            }

            fn dump(&self) -> Result<String> {
                unimplemented!()
            }

            fn lock_key(&self) -> Option<LockKey> {
                Some(LockKey::new("test.submit"))
            }
        }

        let procedure_id = ProcedureId::random();
        assert!(manager
            .procedure_state(procedure_id)
            .await
            .unwrap()
            .is_none());

        manager
            .submit(ProcedureWithId {
                id: procedure_id,
                procedure: Box::new(MockProcedure {}),
            })
            .await
            .unwrap();
        assert!(manager
            .procedure_state(procedure_id)
            .await
            .unwrap()
            .is_some());

        // Try to submit procedure with same id again.
        let err = manager
            .submit(ProcedureWithId {
                id: procedure_id,
                procedure: Box::new(MockProcedure {}),
            })
            .await
            .unwrap_err();
        assert!(matches!(err, Error::DuplicateProcedure { .. }), "{err}");
    }
}
