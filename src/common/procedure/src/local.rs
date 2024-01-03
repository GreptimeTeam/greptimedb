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

mod runner;
mod rwlock;

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use backon::ExponentialBuilder;
use common_runtime::{RepeatedTask, TaskFunction};
use common_telemetry::tracing_context::{FutureExt, TracingContext};
use common_telemetry::{info, logging, tracing};
use snafu::{ensure, ResultExt};
use tokio::sync::watch::{self, Receiver, Sender};
use tokio::sync::{Mutex as TokioMutex, Notify};

use self::rwlock::KeyRwLock;
use crate::error::{
    DuplicateProcedureSnafu, Error, LoaderConflictSnafu, ManagerNotStartSnafu, Result,
    StartRemoveOutdatedMetaTaskSnafu, StopRemoveOutdatedMetaTaskSnafu,
};
use crate::local::runner::Runner;
use crate::procedure::BoxedProcedureLoader;
use crate::store::{ProcedureMessage, ProcedureStore, StateStoreRef};
use crate::{
    BoxedProcedure, ContextProvider, LockKey, ProcedureId, ProcedureManager, ProcedureState,
    ProcedureWithId, Watcher,
};

/// The expired time of a procedure's metadata.
const META_TTL: Duration = Duration::from_secs(60 * 10);

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
    /// Parent procedure id.
    parent_id: Option<ProcedureId>,
    /// Notify to wait for subprocedures.
    child_notify: Notify,
    /// Lock required by this procedure.
    lock_key: LockKey,
    /// Sender to notify the procedure state.
    state_sender: Sender<ProcedureState>,
    /// Receiver to watch the procedure state.
    state_receiver: Receiver<ProcedureState>,
    /// Id of child procedures.
    children: Mutex<Vec<ProcedureId>>,
}

impl ProcedureMeta {
    fn new(id: ProcedureId, parent_id: Option<ProcedureId>, lock_key: LockKey) -> ProcedureMeta {
        let (state_sender, state_receiver) = watch::channel(ProcedureState::Running);
        ProcedureMeta {
            id,
            parent_id,
            child_notify: Notify::new(),
            lock_key,
            state_sender,
            state_receiver,
            children: Mutex::new(Vec::new()),
        }
    }

    /// Returns current [ProcedureState].
    fn state(&self) -> ProcedureState {
        self.state_receiver.borrow().clone()
    }

    /// Update current [ProcedureState].
    fn set_state(&self, state: ProcedureState) {
        // Safety: ProcedureMeta also holds the receiver, so `send()` should never fail.
        self.state_sender.send(state).unwrap();
    }

    /// Push `procedure_id` of the subprocedure to the metadata.
    fn push_child(&self, procedure_id: ProcedureId) {
        let mut children = self.children.lock().unwrap();
        children.push(procedure_id);
    }

    /// Append subprocedures to given `buffer`.
    fn list_children(&self, buffer: &mut Vec<ProcedureId>) {
        let children = self.children.lock().unwrap();
        buffer.extend_from_slice(&children);
    }

    /// Returns the number of subprocedures.
    fn num_children(&self) -> usize {
        self.children.lock().unwrap().len()
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
    key_lock: KeyRwLock<String>,
    procedures: RwLock<HashMap<ProcedureId, ProcedureMetaRef>>,
    /// Messages loaded from the procedure store.
    messages: Mutex<HashMap<ProcedureId, ProcedureMessage>>,
    /// Ids and finished time of finished procedures.
    finished_procedures: Mutex<VecDeque<(ProcedureId, Instant)>>,
    /// Running flag.
    running: Arc<AtomicBool>,
}

#[async_trait]
impl ContextProvider for ManagerContext {
    async fn procedure_state(&self, procedure_id: ProcedureId) -> Result<Option<ProcedureState>> {
        Ok(self.state(procedure_id))
    }
}

impl ManagerContext {
    /// Returns a new [ManagerContext].
    fn new() -> ManagerContext {
        ManagerContext {
            key_lock: KeyRwLock::new(),
            loaders: Mutex::new(HashMap::new()),
            procedures: RwLock::new(HashMap::new()),
            messages: Mutex::new(HashMap::new()),
            finished_procedures: Mutex::new(VecDeque::new()),
            running: Arc::new(AtomicBool::new(false)),
        }
    }

    #[cfg(test)]
    pub(crate) fn set_running(&self) {
        self.running.store(true, Ordering::Relaxed);
    }

    /// Set the running flag.
    pub(crate) fn start(&self) {
        self.running.store(true, Ordering::Relaxed);
    }

    pub(crate) fn stop(&self) {
        self.running.store(false, Ordering::Relaxed);
    }

    /// Return `ProcedureManager` is running.
    pub(crate) fn running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
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

    /// Returns the [Watcher] of specific `procedure_id`.
    fn watcher(&self, procedure_id: ProcedureId) -> Option<Watcher> {
        let procedures = self.procedures.read().unwrap();
        procedures
            .get(&procedure_id)
            .map(|meta| meta.state_receiver.clone())
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
                    "Failed to load procedure data, key: {}, source: {:?}",
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
            let _ = messages.remove(procedure_id);
        }
    }

    /// Clean resources of finished procedures.
    fn on_procedures_finish(&self, procedure_ids: &[ProcedureId]) {
        self.remove_messages(procedure_ids);

        // Since users need to query the procedure state, so we can't remove the
        // meta of the procedure directly.
        let now = Instant::now();
        let mut finished_procedures = self.finished_procedures.lock().unwrap();
        finished_procedures.extend(procedure_ids.iter().map(|id| (*id, now)));
    }

    /// Remove metadata of outdated procedures.
    fn remove_outdated_meta(&self, ttl: Duration) {
        let ids = {
            let mut finished_procedures = self.finished_procedures.lock().unwrap();
            if finished_procedures.is_empty() {
                return;
            }

            let mut ids_to_remove = Vec::new();
            while let Some((id, finish_time)) = finished_procedures.front() {
                if finish_time.elapsed() > ttl {
                    ids_to_remove.push(*id);
                    let _ = finished_procedures.pop_front();
                } else {
                    // The rest procedures are finished later, so we can break
                    // the loop.
                    break;
                }
            }
            ids_to_remove
        };

        if ids.is_empty() {
            return;
        }

        let mut procedures = self.procedures.write().unwrap();
        for id in ids {
            let _ = procedures.remove(&id);
        }
    }
}

/// Config for [LocalManager].
#[derive(Debug)]
pub struct ManagerConfig {
    pub parent_path: String,
    pub max_retry_times: usize,
    pub retry_delay: Duration,
    pub remove_outdated_meta_task_interval: Duration,
    pub remove_outdated_meta_ttl: Duration,
}

impl Default for ManagerConfig {
    fn default() -> Self {
        Self {
            parent_path: "".to_string(),
            max_retry_times: 3,
            retry_delay: Duration::from_millis(500),
            remove_outdated_meta_task_interval: Duration::from_secs(60 * 10),
            remove_outdated_meta_ttl: META_TTL,
        }
    }
}

/// A [ProcedureManager] that maintains procedure states locally.
pub struct LocalManager {
    manager_ctx: Arc<ManagerContext>,
    procedure_store: Arc<ProcedureStore>,
    max_retry_times: usize,
    retry_delay: Duration,
    /// GC task.
    remove_outdated_meta_task: TokioMutex<Option<RepeatedTask<Error>>>,
    config: ManagerConfig,
}

impl LocalManager {
    /// Create a new [LocalManager] with specific `config`.
    pub fn new(config: ManagerConfig, state_store: StateStoreRef) -> LocalManager {
        let manager_ctx = Arc::new(ManagerContext::new());

        LocalManager {
            manager_ctx,
            procedure_store: Arc::new(ProcedureStore::new(&config.parent_path, state_store)),
            max_retry_times: config.max_retry_times,
            retry_delay: config.retry_delay,
            remove_outdated_meta_task: TokioMutex::new(None),
            config,
        }
    }

    /// Build remove outedated meta task
    pub fn build_remove_outdated_meta_task(&self) -> RepeatedTask<Error> {
        RepeatedTask::new(
            self.config.remove_outdated_meta_task_interval,
            Box::new(RemoveOutdatedMetaFunction {
                manager_ctx: self.manager_ctx.clone(),
                ttl: self.config.remove_outdated_meta_ttl,
            }),
        )
    }

    /// Submit a root procedure with given `procedure_id`.
    fn submit_root(
        &self,
        procedure_id: ProcedureId,
        step: u32,
        procedure: BoxedProcedure,
    ) -> Result<Watcher> {
        ensure!(self.manager_ctx.running(), ManagerNotStartSnafu);

        let meta = Arc::new(ProcedureMeta::new(procedure_id, None, procedure.lock_key()));
        let runner = Runner {
            meta: meta.clone(),
            procedure,
            manager_ctx: self.manager_ctx.clone(),
            step,
            exponential_builder: ExponentialBuilder::default()
                .with_min_delay(self.retry_delay)
                .with_max_times(self.max_retry_times),
            store: self.procedure_store.clone(),
            rolling_back: false,
        };

        let watcher = meta.state_receiver.clone();

        // Inserts meta into the manager before actually spawnd the runner.
        ensure!(
            self.manager_ctx.try_insert_procedure(meta),
            DuplicateProcedureSnafu { procedure_id },
        );

        let tracing_context = TracingContext::from_current_span();

        let _handle = common_runtime::spawn_bg(async move {
            // Run the root procedure.
            // The task was moved to another runtime for execution.
            // In order not to interrupt tracing, a span needs to be created to continue tracing the current task.
            runner
                .run()
                .trace(
                    tracing_context
                        .attach(tracing::info_span!("LocalManager::submit_root_procedure")),
                )
                .await;
        });

        Ok(watcher)
    }

    /// Recovers unfinished procedures and reruns them.
    async fn recover(&self) -> Result<()> {
        logging::info!("LocalManager start to recover");
        let recover_start = Instant::now();

        let (messages, finished_ids) = self.procedure_store.load_messages().await?;

        for (procedure_id, message) in &messages {
            if message.parent_id.is_none() {
                // This is the root procedure. We only submit the root procedure as it will
                // submit sub-procedures to the manager.
                let Some(loaded_procedure) = self
                    .manager_ctx
                    .load_one_procedure_from_message(*procedure_id, message)
                else {
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

        if !finished_ids.is_empty() {
            logging::info!(
                "LocalManager try to clean finished procedures, num: {}",
                finished_ids.len()
            );

            for procedure_id in finished_ids {
                if let Err(e) = self.procedure_store.delete_procedure(procedure_id).await {
                    logging::error!(e; "Failed to delete procedure {}", procedure_id);
                }
            }
        }

        logging::info!(
            "LocalManager finish recovery, cost: {}ms",
            recover_start.elapsed().as_millis()
        );

        Ok(())
    }

    #[cfg(any(test, feature = "testing"))]
    /// Returns true if contains a specified loader.
    pub fn contains_loader(&self, name: &str) -> bool {
        let loaders = self.manager_ctx.loaders.lock().unwrap();
        loaders.contains_key(name)
    }
}

#[async_trait]
impl ProcedureManager for LocalManager {
    fn register_loader(&self, name: &str, loader: BoxedProcedureLoader) -> Result<()> {
        let mut loaders = self.manager_ctx.loaders.lock().unwrap();
        ensure!(!loaders.contains_key(name), LoaderConflictSnafu { name });

        let _ = loaders.insert(name.to_string(), loader);

        Ok(())
    }

    async fn start(&self) -> Result<()> {
        let mut task = self.remove_outdated_meta_task.lock().await;

        if task.is_some() {
            return Ok(());
        }

        let task_inner = self.build_remove_outdated_meta_task();

        task_inner
            .start(common_runtime::bg_runtime())
            .context(StartRemoveOutdatedMetaTaskSnafu)?;

        *task = Some(task_inner);

        self.manager_ctx.start();

        info!("LocalManager is start.");

        self.recover().await
    }

    async fn stop(&self) -> Result<()> {
        let mut task = self.remove_outdated_meta_task.lock().await;

        if let Some(task) = task.take() {
            task.stop().await.context(StopRemoveOutdatedMetaTaskSnafu)?;
        }

        self.manager_ctx.stop();

        info!("LocalManager is stopped.");

        Ok(())
    }

    async fn submit(&self, procedure: ProcedureWithId) -> Result<Watcher> {
        let procedure_id = procedure.id;
        ensure!(
            !self.manager_ctx.contains_procedure(procedure_id),
            DuplicateProcedureSnafu { procedure_id }
        );

        self.submit_root(procedure.id, 0, procedure.procedure)
    }

    async fn procedure_state(&self, procedure_id: ProcedureId) -> Result<Option<ProcedureState>> {
        Ok(self.manager_ctx.state(procedure_id))
    }

    fn procedure_watcher(&self, procedure_id: ProcedureId) -> Option<Watcher> {
        self.manager_ctx.watcher(procedure_id)
    }
}

struct RemoveOutdatedMetaFunction {
    manager_ctx: Arc<ManagerContext>,
    ttl: Duration,
}

#[async_trait::async_trait]
impl TaskFunction<Error> for RemoveOutdatedMetaFunction {
    fn name(&self) -> &str {
        "ProcedureManager-remove-outdated-meta-task"
    }

    async fn call(&mut self) -> Result<()> {
        self.manager_ctx.remove_outdated_meta(self.ttl);
        Ok(())
    }
}

/// Create a new [ProcedureMeta] for test purpose.
#[cfg(test)]
pub(crate) mod test_util {
    use common_test_util::temp_dir::TempDir;
    use object_store::services::Fs as Builder;
    use object_store::ObjectStore;

    use super::*;

    pub(crate) fn procedure_meta_for_test() -> ProcedureMeta {
        ProcedureMeta::new(ProcedureId::random(), None, LockKey::default())
    }

    pub(crate) fn new_object_store(dir: &TempDir) -> ObjectStore {
        let store_dir = dir.path().to_str().unwrap();
        let mut builder = Builder::default();
        let _ = builder.root(store_dir);
        ObjectStore::new(builder).unwrap().finish()
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use common_error::mock::MockError;
    use common_error::status_code::StatusCode;
    use common_test_util::temp_dir::create_temp_dir;

    use super::*;
    use crate::error::{self, Error};
    use crate::store::state_store::ObjectStateStore;
    use crate::{Context, Procedure, Status};

    #[test]
    fn test_manager_context() {
        let ctx = ManagerContext::new();
        let meta = Arc::new(test_util::procedure_meta_for_test());

        assert!(!ctx.contains_procedure(meta.id));
        assert!(ctx.state(meta.id).is_none());

        assert!(ctx.try_insert_procedure(meta.clone()));
        assert!(ctx.contains_procedure(meta.id));

        assert!(ctx.state(meta.id).unwrap().is_running());
        meta.set_state(ProcedureState::Done);
        assert!(ctx.state(meta.id).unwrap().is_done());
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
        lock_key: LockKey,
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

        fn lock_key(&self) -> LockKey {
            self.lock_key.clone()
        }
    }

    impl ProcedureToLoad {
        fn new(content: &str) -> ProcedureToLoad {
            ProcedureToLoad {
                content: content.to_string(),
                lock_key: LockKey::default(),
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
        let dir = create_temp_dir("register");
        let config = ManagerConfig {
            parent_path: "data/".to_string(),
            max_retry_times: 3,
            retry_delay: Duration::from_millis(500),
            ..Default::default()
        };
        let state_store = Arc::new(ObjectStateStore::new(test_util::new_object_store(&dir)));
        let manager = LocalManager::new(config, state_store);
        manager.manager_ctx.start();

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
        let dir = create_temp_dir("recover");
        let object_store = test_util::new_object_store(&dir);
        let config = ManagerConfig {
            parent_path: "data/".to_string(),
            max_retry_times: 3,
            retry_delay: Duration::from_millis(500),
            ..Default::default()
        };
        let state_store = Arc::new(ObjectStateStore::new(object_store.clone()));
        let manager = LocalManager::new(config, state_store);
        manager.manager_ctx.start();

        manager
            .register_loader("ProcedureToLoad", ProcedureToLoad::loader())
            .unwrap();

        // Prepare data
        let procedure_store = ProcedureStore::from_object_store(object_store.clone());
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
        let _ = manager.procedure_state(root_id).await.unwrap().unwrap();
        // Since the mocked root procedure actually doesn't submit subprocedures, so there is no
        // related state.
        assert!(manager.procedure_state(child_id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_submit_procedure() {
        let dir = create_temp_dir("submit");
        let config = ManagerConfig {
            parent_path: "data/".to_string(),
            max_retry_times: 3,
            retry_delay: Duration::from_millis(500),
            ..Default::default()
        };
        let state_store = Arc::new(ObjectStateStore::new(test_util::new_object_store(&dir)));
        let manager = LocalManager::new(config, state_store);
        manager.manager_ctx.start();

        let procedure_id = ProcedureId::random();
        assert!(manager
            .procedure_state(procedure_id)
            .await
            .unwrap()
            .is_none());
        assert!(manager.procedure_watcher(procedure_id).is_none());

        let mut procedure = ProcedureToLoad::new("submit");
        procedure.lock_key = LockKey::single_exclusive("test.submit");
        assert!(manager
            .submit(ProcedureWithId {
                id: procedure_id,
                procedure: Box::new(procedure),
            })
            .await
            .is_ok());
        assert!(manager
            .procedure_state(procedure_id)
            .await
            .unwrap()
            .is_some());
        // Wait for the procedure done.
        let mut watcher = manager.procedure_watcher(procedure_id).unwrap();
        watcher.changed().await.unwrap();
        assert!(watcher.borrow().is_done());

        // Try to submit procedure with same id again.
        let err = manager
            .submit(ProcedureWithId {
                id: procedure_id,
                procedure: Box::new(ProcedureToLoad::new("submit")),
            })
            .await
            .unwrap_err();
        assert!(matches!(err, Error::DuplicateProcedure { .. }), "{err}");
    }

    #[tokio::test]
    async fn test_state_changed_on_err() {
        let dir = create_temp_dir("on_err");
        let config = ManagerConfig {
            parent_path: "data/".to_string(),
            max_retry_times: 3,
            retry_delay: Duration::from_millis(500),
            ..Default::default()
        };
        let state_store = Arc::new(ObjectStateStore::new(test_util::new_object_store(&dir)));
        let manager = LocalManager::new(config, state_store);
        manager.manager_ctx.start();

        #[derive(Debug)]
        struct MockProcedure {
            panic: bool,
        }

        #[async_trait]
        impl Procedure for MockProcedure {
            fn type_name(&self) -> &str {
                "MockProcedure"
            }

            async fn execute(&mut self, _ctx: &Context) -> Result<Status> {
                if self.panic {
                    // Test the runner can set the state to failed even the procedure
                    // panics.
                    panic!();
                } else {
                    Err(Error::external(MockError::new(StatusCode::Unexpected)))
                }
            }

            fn dump(&self) -> Result<String> {
                Ok(String::new())
            }

            fn lock_key(&self) -> LockKey {
                LockKey::single_exclusive("test.submit")
            }
        }

        let check_procedure = |procedure| {
            async {
                let procedure_id = ProcedureId::random();
                let mut watcher = manager
                    .submit(ProcedureWithId {
                        id: procedure_id,
                        procedure: Box::new(procedure),
                    })
                    .await
                    .unwrap();
                // Wait for the notification.
                watcher.changed().await.unwrap();
                assert!(watcher.borrow().is_failed());
            }
        };

        check_procedure(MockProcedure { panic: false }).await;
        check_procedure(MockProcedure { panic: true }).await;
    }

    #[tokio::test]
    async fn test_procedure_manager_stopped() {
        let dir = create_temp_dir("procedure_manager_stopped");
        let config = ManagerConfig {
            parent_path: "data/".to_string(),
            max_retry_times: 3,
            retry_delay: Duration::from_millis(500),
            ..Default::default()
        };
        let state_store = Arc::new(ObjectStateStore::new(test_util::new_object_store(&dir)));
        let manager = LocalManager::new(config, state_store);

        let mut procedure = ProcedureToLoad::new("submit");
        procedure.lock_key = LockKey::single_exclusive("test.submit");
        let procedure_id = ProcedureId::random();
        assert_matches!(
            manager
                .submit(ProcedureWithId {
                    id: procedure_id,
                    procedure: Box::new(procedure),
                })
                .await
                .unwrap_err(),
            error::Error::ManagerNotStart { .. }
        );
    }

    #[tokio::test]
    async fn test_procedure_manager_restart() {
        let dir = create_temp_dir("procedure_manager_restart");
        let config = ManagerConfig {
            parent_path: "data/".to_string(),
            max_retry_times: 3,
            retry_delay: Duration::from_millis(500),
            ..Default::default()
        };
        let state_store = Arc::new(ObjectStateStore::new(test_util::new_object_store(&dir)));
        let manager = LocalManager::new(config, state_store);

        manager.start().await.unwrap();
        manager.stop().await.unwrap();
        manager.start().await.unwrap();

        let mut procedure = ProcedureToLoad::new("submit");
        procedure.lock_key = LockKey::single_exclusive("test.submit");
        let procedure_id = ProcedureId::random();
        assert!(manager
            .submit(ProcedureWithId {
                id: procedure_id,
                procedure: Box::new(procedure),
            })
            .await
            .is_ok());
        assert!(manager
            .procedure_state(procedure_id)
            .await
            .unwrap()
            .is_some());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_remove_outdated_meta_task() {
        let dir = create_temp_dir("remove_outdated_meta_task");
        let object_store = test_util::new_object_store(&dir);
        let config = ManagerConfig {
            parent_path: "data/".to_string(),
            max_retry_times: 3,
            retry_delay: Duration::from_millis(500),
            remove_outdated_meta_task_interval: Duration::from_millis(1),
            remove_outdated_meta_ttl: Duration::from_millis(1),
        };
        let state_store = Arc::new(ObjectStateStore::new(object_store.clone()));
        let manager = LocalManager::new(config, state_store);
        manager.manager_ctx.set_running();

        let mut procedure = ProcedureToLoad::new("submit");
        procedure.lock_key = LockKey::single_exclusive("test.submit");
        let procedure_id = ProcedureId::random();
        assert!(manager
            .submit(ProcedureWithId {
                id: procedure_id,
                procedure: Box::new(procedure),
            })
            .await
            .is_ok());
        let mut watcher = manager.procedure_watcher(procedure_id).unwrap();
        watcher.changed().await.unwrap();

        manager.start().await.unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(manager
            .procedure_state(procedure_id)
            .await
            .unwrap()
            .is_none());

        // The remove_outdated_meta method has been stopped, so any procedure meta-data will not be automatically removed.
        manager.stop().await.unwrap();
        let mut procedure = ProcedureToLoad::new("submit");
        procedure.lock_key = LockKey::single_exclusive("test.submit");
        let procedure_id = ProcedureId::random();

        manager.manager_ctx.set_running();
        assert!(manager
            .submit(ProcedureWithId {
                id: procedure_id,
                procedure: Box::new(procedure),
            })
            .await
            .is_ok());
        let mut watcher = manager.procedure_watcher(procedure_id).unwrap();
        watcher.changed().await.unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(manager
            .procedure_state(procedure_id)
            .await
            .unwrap()
            .is_some());

        // After restart
        let mut procedure = ProcedureToLoad::new("submit");
        procedure.lock_key = LockKey::single_exclusive("test.submit");
        let procedure_id = ProcedureId::random();
        assert!(manager
            .submit(ProcedureWithId {
                id: procedure_id,
                procedure: Box::new(procedure),
            })
            .await
            .is_ok());
        let mut watcher = manager.procedure_watcher(procedure_id).unwrap();
        watcher.changed().await.unwrap();

        manager.start().await.unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(manager
            .procedure_state(procedure_id)
            .await
            .unwrap()
            .is_none());
    }
}
