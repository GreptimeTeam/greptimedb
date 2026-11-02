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
use std::collections::hash_map::Entry;
use std::sync::{Arc, RwLock};

use common_meta::DatanodeId;
use common_meta::key::TableMetadataManagerRef;
use common_procedure::{ProcedureContext, ProcedureId, ProcedureManagerRef, ProcedureWithId};
use common_telemetry::info;
use common_time::util::current_time_millis;
use snafu::{ResultExt, ensure};
use tokio::sync::Mutex;

use crate::error::{self, Result};
use crate::key::NodeSchedulingRole;
use crate::metasrv::SelectorContext;
use crate::node_scheduling::NodeSchedulingManagerRef;
use crate::procedure::drain_datanode::{
    Context as DrainDatanodeContext, DEFAULT_DRAIN_TIMEOUT, DrainDatanodeProcedure,
    DrainDatanodeProcedureTask, MAX_PARALLELISM, PersistentContext,
};
use crate::procedure::region_migration::manager::RegionMigrationManagerRef;

pub type DrainDatanodeManagerRef = Arc<DrainDatanodeManager>;

/// Tracks the running drains, indexed by the drained datanode.
///
/// Reserves each datanode for one drain. The manager reuses matching submissions
/// and rejects conflicting ones instead of queuing them behind the procedure lock.
#[derive(Default, Clone)]
pub struct DrainDatanodeProcedureTracker {
    running_procedures: Arc<RwLock<HashMap<DatanodeId, RunningDrain>>>,
}

struct RunningDrain {
    task: DrainDatanodeProcedureTask,
    procedure_id: ProcedureId,
}

impl DrainDatanodeProcedureTracker {
    fn find(&self, task: &DrainDatanodeProcedureTask) -> Result<Option<ProcedureId>> {
        let procedures = self.running_procedures.read().unwrap();
        let Some(running) = procedures.get(&task.node_id) else {
            return Ok(None);
        };
        ensure!(
            running.task.target_node_id == task.target_node_id
                && running.task.parallelism == task.parallelism
                && running.task.strategy == task.strategy,
            error::DrainDatanodeRunningSnafu {
                node_id: task.node_id
            }
        );
        Ok(Some(running.procedure_id))
    }

    fn contains(&self, node_id: DatanodeId) -> bool {
        self.running_procedures
            .read()
            .unwrap()
            .contains_key(&node_id)
    }

    /// Returns the guard of a drain of `task.node_id`, or `None` if that datanode is already
    /// being drained.
    pub(crate) fn insert_running_procedure(
        &self,
        task: &DrainDatanodeProcedureTask,
        procedure_id: ProcedureId,
    ) -> Option<DrainDatanodeProcedureGuard> {
        let mut procedures = self.running_procedures.write().unwrap();
        match procedures.entry(task.node_id) {
            Entry::Occupied(_) => None,
            Entry::Vacant(v) => {
                v.insert(RunningDrain {
                    task: task.clone(),
                    procedure_id,
                });
                Some(DrainDatanodeProcedureGuard {
                    node_id: task.node_id,
                    running_procedures: self.running_procedures.clone(),
                })
            }
        }
    }
}

/// Removes the drained datanode from [`DrainDatanodeProcedureTracker`] when dropped.
pub(crate) struct DrainDatanodeProcedureGuard {
    node_id: DatanodeId,
    running_procedures: Arc<RwLock<HashMap<DatanodeId, RunningDrain>>>,
}

impl Drop for DrainDatanodeProcedureGuard {
    fn drop(&mut self) {
        self.running_procedures
            .write()
            .unwrap()
            .remove(&self.node_id);
    }
}

/// Manages the [`DrainDatanodeProcedure`]s.
///
/// Registers the procedure loader and serializes submissions with restoring scheduling.
/// Matching retries reuse the ID recorded by submission or recovery in the tracker.
pub struct DrainDatanodeManager {
    procedure_manager: ProcedureManagerRef,
    table_metadata_manager: TableMetadataManagerRef,
    node_scheduling_manager: NodeSchedulingManagerRef,
    region_migration_manager: RegionMigrationManagerRef,
    selector_ctx: SelectorContext,

    tracker: DrainDatanodeProcedureTracker,
    /// Serializes submissions with restoring scheduling, including their KV operations.
    admission: Mutex<()>,
}

impl DrainDatanodeManager {
    /// Returns a new [`DrainDatanodeManager`].
    pub fn new(
        procedure_manager: ProcedureManagerRef,
        table_metadata_manager: TableMetadataManagerRef,
        node_scheduling_manager: NodeSchedulingManagerRef,
        region_migration_manager: RegionMigrationManagerRef,
        selector_ctx: SelectorContext,
    ) -> Self {
        Self {
            procedure_manager,
            table_metadata_manager,
            node_scheduling_manager,
            region_migration_manager,
            selector_ctx,

            tracker: DrainDatanodeProcedureTracker::default(),
            admission: Mutex::new(()),
        }
    }

    /// Returns the context of a drain run.
    fn context(&self, persistent_ctx: PersistentContext) -> DrainDatanodeContext {
        DrainDatanodeContext::new(
            self.table_metadata_manager.clone(),
            self.node_scheduling_manager.clone(),
            self.region_migration_manager.clone(),
            self.selector_ctx.clone(),
            persistent_ctx,
        )
    }

    /// Submits a drain or returns the ID of an identical running drain.
    ///
    /// Retries do not extend the original deadline. Completion is decided by the procedure.
    pub async fn submit_procedure(
        &self,
        procedure_context: ProcedureContext,
        task: DrainDatanodeProcedureTask,
    ) -> Result<ProcedureId> {
        let _admission = self.admission.lock().await;
        if task.target_node_id == Some(task.node_id) {
            return error::InvalidArgumentsSnafu {
                err_msg: "the target datanode must differ from the drained datanode",
            }
            .fail();
        }
        if task.parallelism == 0 || task.parallelism > MAX_PARALLELISM {
            return error::InvalidArgumentsSnafu {
                err_msg: format!("the parallelism must be in 1..={}", MAX_PARALLELISM),
            }
            .fail();
        }
        let timeout = task.timeout.unwrap_or(DEFAULT_DRAIN_TIMEOUT);
        if timeout.is_zero() {
            return error::InvalidArgumentsSnafu {
                err_msg: "the timeout must be greater than zero",
            }
            .fail();
        }

        self.ensure_ready()?;
        if let Some(procedure_id) = self.tracker.find(&task)? {
            return Ok(procedure_id);
        }
        let procedure_id = ProcedureId::random();

        let Some(guard) = self.tracker.insert_running_procedure(&task, procedure_id) else {
            return error::DrainDatanodeRunningSnafu {
                node_id: task.node_id,
            }
            .fail();
        };

        let persistent_ctx = PersistentContext {
            node_id: task.node_id,
            target_node_id: task.target_node_id,
            parallelism: task.parallelism,
            strategy: task.strategy,
            // The budget starts when the task is submitted, so neither a slow start nor a
            // restart extends it.
            deadline_ms: current_time_millis() + timeout.as_millis() as i64,
        };
        let procedure = DrainDatanodeProcedure::new(self.context(persistent_ctx), vec![guard]);
        let procedure_with_id = ProcedureWithId {
            id: procedure_id,
            procedure: Box::new(procedure),
            context: procedure_context,
        };
        info!(
            "Starting drain datanode procedure {procedure_id} for datanode {}, timeout: {:?}",
            task.node_id, timeout
        );
        self.procedure_manager
            .submit(procedure_with_id)
            .await
            .context(error::SubmitProcedureSnafu)?;

        Ok(procedure_id)
    }

    /// Restores scheduling only when no drain owns the node.
    pub async fn mark_schedulable(&self, node_id: DatanodeId) -> Result<()> {
        let _admission = self.admission.lock().await;
        self.ensure_ready()?;
        ensure!(
            !self.tracker.contains(node_id),
            error::DrainDatanodeRunningSnafu { node_id }
        );
        self.node_scheduling_manager
            .mark_schedulable(NodeSchedulingRole::Datanode, node_id)
            .await?;
        Ok(())
    }

    fn ensure_ready(&self) -> Result<()> {
        ensure!(
            self.procedure_manager.is_ready(),
            error::RetryLaterSnafu {
                reason: "procedure manager has not completed recovery",
            }
        );
        Ok(())
    }

    /// Registers the loader of [`DrainDatanodeProcedure`].
    pub(crate) fn try_start(&self) -> Result<()> {
        let table_metadata_manager = self.table_metadata_manager.clone();
        let node_scheduling_manager = self.node_scheduling_manager.clone();
        let region_migration_manager = self.region_migration_manager.clone();
        let selector_ctx = self.selector_ctx.clone();
        let tracker = self.tracker.clone();
        self.procedure_manager
            .register_loader(
                DrainDatanodeProcedure::TYPE_NAME,
                Box::new(move |json| {
                    let table_metadata_manager = table_metadata_manager.clone();
                    let node_scheduling_manager = node_scheduling_manager.clone();
                    let region_migration_manager = region_migration_manager.clone();
                    let selector_ctx = selector_ctx.clone();
                    let factory = move |persistent_ctx| {
                        DrainDatanodeContext::new(
                            table_metadata_manager,
                            node_scheduling_manager,
                            region_migration_manager,
                            selector_ctx,
                            persistent_ctx,
                        )
                    };
                    DrainDatanodeProcedure::from_json(json, &tracker, factory)
                        .map(|procedure| Box::new(procedure) as _)
                }),
            )
            .context(error::RegisterProcedureLoaderSnafu {
                type_name: DrainDatanodeProcedure::TYPE_NAME,
            })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use common_procedure::store::state_store::StateStore;
    use common_procedure::{Procedure, ProcedureId, ProcedureManagerRef};

    use crate::error;
    use crate::key::NodeSchedulingRole;
    use crate::procedure::drain_datanode::manager::{
        DrainDatanodeManager, DrainDatanodeProcedureTracker,
    };
    use crate::procedure::drain_datanode::test_util::{TestEnv, new_persistent_context};
    use crate::procedure::drain_datanode::{
        DEFAULT_PARALLELISM, DrainDatanodeProcedure, DrainDatanodeProcedureTask,
        DrainDatanodeStrategy,
    };

    struct HoldDrainLock {
        entered: std::sync::Arc<tokio::sync::Notify>,
        release: std::sync::Arc<tokio::sync::Notify>,
    }

    #[async_trait::async_trait]
    impl common_procedure::Procedure for HoldDrainLock {
        fn type_name(&self) -> &str {
            "test-hold-drain-lock"
        }
        async fn execute(
            &mut self,
            _: &common_procedure::Context,
        ) -> common_procedure::Result<common_procedure::Status> {
            self.entered.notify_one();
            self.release.notified().await;
            Ok(common_procedure::Status::done())
        }
        fn dump(&self) -> common_procedure::Result<String> {
            Ok(String::new())
        }
        fn lock_key(&self) -> common_procedure::LockKey {
            common_procedure::LockKey::single_exclusive("__drain_datanode/1")
        }
    }

    fn task() -> DrainDatanodeProcedureTask {
        DrainDatanodeProcedureTask {
            node_id: 1,
            target_node_id: Some(2),
            parallelism: DEFAULT_PARALLELISM,
            strategy: DrainDatanodeStrategy::FailFast,
            timeout: None,
        }
    }

    async fn hold_drain_lock(
        procedure_manager: &ProcedureManagerRef,
    ) -> std::sync::Arc<tokio::sync::Notify> {
        let entered = std::sync::Arc::new(tokio::sync::Notify::new());
        let release = std::sync::Arc::new(tokio::sync::Notify::new());
        procedure_manager
            .submit(common_procedure::ProcedureWithId::with_random_id(Box::new(
                HoldDrainLock {
                    entered: entered.clone(),
                    release: release.clone(),
                },
            )))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), entered.notified())
            .await
            .unwrap();
        release
    }

    #[tokio::test]
    async fn test_submit_drain_concurrent_retries_and_scheduling_conflict() {
        let env = TestEnv::new().await;
        env.procedure_manager().start().await.unwrap();
        let release = hold_drain_lock(env.procedure_manager()).await;
        let manager = env.drain_manager();
        let (first, second) = tokio::join!(
            manager.submit_procedure(Default::default(), task()),
            manager.submit_procedure(Default::default(), task()),
        );
        let first = first.unwrap();
        assert_eq!(first, second.unwrap());
        assert_eq!(
            2,
            env.procedure_manager()
                .list_procedures()
                .await
                .unwrap()
                .len()
        );
        let mut conflicting = task();
        conflicting.target_node_id = Some(3);
        assert!(matches!(
            manager
                .submit_procedure(Default::default(), conflicting)
                .await,
            Err(error::Error::DrainDatanodeRunning { .. })
        ));
        assert!(matches!(
            manager.mark_schedulable(1).await,
            Err(error::Error::DrainDatanodeRunning { .. })
        ));
        release.notify_one();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if !manager
                    .tracker
                    .running_procedures
                    .read()
                    .unwrap()
                    .contains_key(&1)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        manager.mark_schedulable(1).await.unwrap();
        manager.mark_schedulable(1).await.unwrap();
        env.procedure_manager().stop().await.unwrap();
    }

    struct RecoveryGate {
        store: common_meta::state_store::KvStateStore,
        entered: tokio::sync::Notify,
        release: tokio::sync::Notify,
    }

    #[async_trait::async_trait]
    impl common_procedure::store::state_store::StateStore for RecoveryGate {
        async fn put(&self, key: &str, value: Vec<u8>) -> common_procedure::Result<()> {
            self.store.put(key, value).await
        }

        async fn walk_top_down(
            &self,
            path: &str,
        ) -> common_procedure::Result<common_procedure::store::state_store::KeyValueStream>
        {
            if path == "procedure/" {
                self.entered.notify_one();
                self.release.notified().await;
            }
            self.store.walk_top_down(path).await
        }

        async fn batch_delete(&self, keys: &[String]) -> common_procedure::Result<()> {
            self.store.batch_delete(keys).await
        }

        async fn delete(&self, key: &str) -> common_procedure::Result<()> {
            self.store.delete(key).await
        }
    }

    #[tokio::test]
    async fn test_drain_admission_waits_for_recovery_and_reuses_persisted_id() {
        use crate::procedure::region_migration::test_util::TestingEnv;
        use common_procedure::local::{LocalManager, ManagerConfig};
        use common_procedure::store::ProcedureMessage;
        use common_procedure::test_util::InMemoryPoisonStore;

        let env = TestEnv::new().await;
        let mut original = DrainDatanodeProcedure::new(
            env.context(new_persistent_context(
                Some(2),
                DrainDatanodeStrategy::FailFast,
            )),
            vec![],
        );
        assert!(
            original
                .execute(&TestingEnv::procedure_context())
                .await
                .unwrap()
                .need_persist()
        );
        let id = ProcedureId::random();
        let message = ProcedureMessage {
            type_name: DrainDatanodeProcedure::TYPE_NAME.to_string(),
            data: original.dump().unwrap(),
            parent_id: None,
            subprocedure_recovery: Default::default(),
            step: 0,
            error: None,
            context: Default::default(),
        };
        drop(original);
        let store = Arc::new(RecoveryGate {
            store: common_meta::state_store::KvStateStore::new(env.env.kv_backend()),
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        });
        store
            .put(
                &format!("procedure/{id}/0000000000.step"),
                serde_json::to_vec(&message).unwrap(),
            )
            .await
            .unwrap();
        let procedure_manager: ProcedureManagerRef = Arc::new(LocalManager::new(
            ManagerConfig::default(),
            store.clone(),
            Arc::new(InMemoryPoisonStore::default()),
            None,
            None,
        ));
        let manager = DrainDatanodeManager::new(
            procedure_manager.clone(),
            env.table_metadata_manager.clone(),
            env.node_scheduling_manager.clone(),
            env.region_migration_manager.clone(),
            env.selector_ctx.clone(),
        );
        manager.try_start().unwrap();
        let start = tokio::spawn({
            let procedure_manager = procedure_manager.clone();
            async move { procedure_manager.start().await }
        });
        tokio::time::timeout(Duration::from_secs(5), store.entered.notified())
            .await
            .unwrap();
        assert!(!procedure_manager.is_ready());
        assert!(matches!(
            manager.submit_procedure(Default::default(), task()).await,
            Err(error::Error::RetryLater { .. })
        ));
        assert!(matches!(
            manager.mark_schedulable(1).await,
            Err(error::Error::RetryLater { .. })
        ));
        assert!(
            !env.node_scheduling_manager
                .is_schedulable(NodeSchedulingRole::Datanode, 1)
                .await
                .unwrap()
        );

        // Hold execution while the real loader restores the checkpoint and its ID.
        let release = hold_drain_lock(&procedure_manager).await;
        store.release.notify_one();
        tokio::time::timeout(Duration::from_secs(5), start)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(procedure_manager.is_ready());
        for _ in 0..2 {
            assert_eq!(
                id,
                manager
                    .submit_procedure(Default::default(), task())
                    .await
                    .unwrap()
            );
        }
        let mut conflicting = task();
        conflicting.target_node_id = Some(3);
        assert!(matches!(
            manager
                .submit_procedure(Default::default(), conflicting)
                .await,
            Err(error::Error::DrainDatanodeRunning { .. })
        ));
        assert!(matches!(
            manager.mark_schedulable(1).await,
            Err(error::Error::DrainDatanodeRunning { .. })
        ));
        release.notify_one();
        tokio::time::timeout(Duration::from_secs(5), async {
            while manager.tracker.contains(1) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(
            procedure_manager
                .procedure_state(id)
                .await
                .unwrap()
                .unwrap()
                .is_done()
        );
        let second = manager
            .submit_procedure(Default::default(), task())
            .await
            .unwrap();
        assert_ne!(id, second);
        tokio::time::timeout(Duration::from_secs(5), async {
            while manager.tracker.contains(1) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(
            procedure_manager
                .procedure_state(second)
                .await
                .unwrap()
                .unwrap()
                .is_done()
        );
        manager.mark_schedulable(1).await.unwrap();
        procedure_manager.stop().await.unwrap();
        assert!(!procedure_manager.is_ready());
        assert!(matches!(
            manager.mark_schedulable(1).await,
            Err(error::Error::RetryLater { .. })
        ));
    }

    #[tokio::test]
    async fn test_failed_drain_can_be_submitted_again() {
        use crate::key::NodeSchedulingRole;
        use crate::procedure::drain_datanode::test_util::leader_region;
        let env = TestEnv::new().await;
        env.create_table(vec![leader_region(
            store_api::storage::RegionId::new(1024, 1),
            1,
        )])
        .await;
        env.node_scheduling_manager
            .mark_unschedulable(NodeSchedulingRole::Datanode, 2)
            .await
            .unwrap();
        env.procedure_manager().start().await.unwrap();
        let manager = env.drain_manager();
        let first = manager
            .submit_procedure(Default::default(), task())
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if env
                    .procedure_manager()
                    .procedure_state(first)
                    .await
                    .unwrap()
                    .unwrap()
                    .is_failed()
                    && !manager
                        .tracker
                        .running_procedures
                        .read()
                        .unwrap()
                        .contains_key(&1)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let second = manager
            .submit_procedure(Default::default(), task())
            .await
            .unwrap();
        assert_ne!(first, second);
        env.procedure_manager().stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_failed_submission_releases_drain_admission() {
        use common_procedure::local::{LocalManager, ManagerConfig};
        use common_procedure::test_util::InMemoryPoisonStore;

        let env = TestEnv::new().await;
        let procedure_manager: ProcedureManagerRef = Arc::new(LocalManager::new(
            ManagerConfig {
                max_running_procedures: 0,
                ..Default::default()
            },
            Arc::new(common_meta::state_store::KvStateStore::new(
                env.env.kv_backend(),
            )),
            Arc::new(InMemoryPoisonStore::default()),
            None,
            None,
        ));
        procedure_manager.start().await.unwrap();
        let manager = DrainDatanodeManager::new(
            procedure_manager.clone(),
            env.table_metadata_manager.clone(),
            env.node_scheduling_manager.clone(),
            env.region_migration_manager.clone(),
            env.selector_ctx.clone(),
        );
        assert!(matches!(
            manager.submit_procedure(Default::default(), task()).await,
            Err(error::Error::SubmitProcedure { .. })
        ));
        assert!(!manager.tracker.contains(1));
        manager.mark_schedulable(1).await.unwrap();
        procedure_manager.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_submit_procedure_rejects_invalid_task() {
        let env = TestEnv::new().await;
        let manager = env.drain_manager();

        // The pinned target must differ from the drained datanode.
        let error = manager
            .submit_procedure(
                common_procedure::ProcedureContext::default(),
                DrainDatanodeProcedureTask {
                    node_id: 1,
                    target_node_id: Some(1),
                    parallelism: DEFAULT_PARALLELISM,
                    strategy: DrainDatanodeStrategy::FailFast,
                    timeout: None,
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(error, error::Error::InvalidArguments { .. }));

        // The parallelism must be in range.
        for parallelism in [0, crate::procedure::drain_datanode::MAX_PARALLELISM + 1] {
            let error = manager
                .submit_procedure(
                    common_procedure::ProcedureContext::default(),
                    DrainDatanodeProcedureTask {
                        node_id: 1,
                        target_node_id: Some(2),
                        parallelism,
                        strategy: DrainDatanodeStrategy::FailFast,
                        timeout: None,
                    },
                )
                .await
                .unwrap_err();
            assert!(matches!(error, error::Error::InvalidArguments { .. }));
        }

        // The timeout must be greater than zero.
        let error = manager
            .submit_procedure(
                common_procedure::ProcedureContext::default(),
                DrainDatanodeProcedureTask {
                    node_id: 1,
                    target_node_id: Some(2),
                    parallelism: DEFAULT_PARALLELISM,
                    strategy: DrainDatanodeStrategy::FailFast,
                    timeout: Some(Duration::ZERO),
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(error, error::Error::InvalidArguments { .. }));
    }

    #[tokio::test]
    async fn test_drain_recovery_reserves_original_id_until_drop() {
        let env = TestEnv::new().await;
        let context = env.context(new_persistent_context(
            Some(2),
            DrainDatanodeStrategy::FailFast,
        ));
        let procedure = DrainDatanodeProcedure::new(context, vec![]);
        let json = procedure.dump().unwrap();
        let tracker = DrainDatanodeProcedureTracker::default();

        // Recovery reserves the original identity before any runner starts.
        let mut reloaded = DrainDatanodeProcedure::from_json(&json, &tracker, |persistent_ctx| {
            env.context(persistent_ctx)
        })
        .unwrap();
        let task = DrainDatanodeProcedureTask {
            node_id: 1,
            target_node_id: Some(2),
            parallelism: DEFAULT_PARALLELISM,
            strategy: DrainDatanodeStrategy::FailFast,
            timeout: None,
        };
        let id = ProcedureId::random();
        reloaded.recover(id).unwrap();
        assert_eq!(Some(id), tracker.find(&task).unwrap());
        assert!(
            tracker
                .insert_running_procedure(&task, ProcedureId::random())
                .is_none()
        );

        // Dropping the reloaded procedure releases the place.
        drop(reloaded);
        assert!(
            tracker
                .insert_running_procedure(&task, ProcedureId::random())
                .is_some()
        );
    }
}
