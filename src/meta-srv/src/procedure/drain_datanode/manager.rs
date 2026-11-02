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
use snafu::ResultExt;
use tokio::sync::Mutex;

use crate::key::NodeSchedulingRole;
use crate::procedure::drain_datanode::utils;

use crate::error::{self, Result};
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
    running_procedures: Arc<RwLock<HashMap<DatanodeId, DrainDatanodeProcedureTask>>>,
}

impl DrainDatanodeProcedureTracker {
    /// Returns the guard of a drain of `task.node_id`, or `None` if that datanode is already
    /// being drained.
    pub(crate) fn insert_running_procedure(
        &self,
        task: &DrainDatanodeProcedureTask,
    ) -> Option<DrainDatanodeProcedureGuard> {
        let mut procedures = self.running_procedures.write().unwrap();
        match procedures.entry(task.node_id) {
            Entry::Occupied(_) => None,
            Entry::Vacant(v) => {
                v.insert(task.clone());
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
    running_procedures: Arc<RwLock<HashMap<DatanodeId, DrainDatanodeProcedureTask>>>,
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
/// Matching retries resolve their id through the procedure manager, which also owns
/// recovered procedures.
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

    /// Returns the tracker of the running drains.
    #[cfg(test)]
    pub(crate) fn tracker(&self) -> DrainDatanodeProcedureTracker {
        self.tracker.clone()
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

    /// Submits a drain, reuses an identical running drain, or returns `None` when drained.
    ///
    /// Retries do not extend the original deadline. The running procedure manager owns
    /// the id, including after recovery; no separate persistent request index is needed.
    pub async fn submit_procedure(
        &self,
        procedure_context: ProcedureContext,
        task: DrainDatanodeProcedureTask,
    ) -> Result<Option<ProcedureId>> {
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

        let running = self
            .tracker
            .running_procedures
            .read()
            .unwrap()
            .get(&task.node_id)
            .cloned();
        if let Some(running) = running {
            if running.target_node_id == task.target_node_id
                && running.parallelism == task.parallelism
                && running.strategy == task.strategy
            {
                let lock_keys = common_procedure::LockKey::single_exclusive(format!(
                    "__drain_datanode/{}",
                    task.node_id
                ))
                .get_keys();
                let procedures = self
                    .procedure_manager
                    .list_procedures()
                    .await
                    .context(error::QueryProcedureSnafu)?;
                if let Some(procedure) = procedures.into_iter().find(|procedure| {
                    procedure.type_name == DrainDatanodeProcedure::TYPE_NAME
                        && procedure.lock_keys == lock_keys
                        && !procedure.state.is_done()
                        && !procedure.state.is_failed()
                }) {
                    return Ok(Some(procedure.id));
                }
            } else {
                return error::DrainDatanodeRunningSnafu {
                    node_id: task.node_id,
                }
                .fail();
            }
            // The runner may have completed between reading the tracker and querying
            // its state. Check the authoritative route before reporting a conflict.
        }

        // Drain removes leaders; follower replicas are deliberately retained by the
        // existing drain procedure. Match that completion contract here.
        if !self
            .node_scheduling_manager
            .is_schedulable(NodeSchedulingRole::Datanode, task.node_id)
            .await?
            && utils::build_datanode_plan(&self.table_metadata_manager, task.node_id)
                .await?
                .leader_regions
                .is_empty()
        {
            return Ok(None);
        }

        let Some(guard) = self.tracker.insert_running_procedure(&task) else {
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
        let procedure_with_id =
            ProcedureWithId::with_random_id(Box::new(procedure)).with_context(procedure_context);
        let procedure_id = procedure_with_id.id;
        info!(
            "Starting drain datanode procedure {procedure_id} for datanode {}, timeout: {:?}",
            task.node_id, timeout
        );
        self.procedure_manager
            .submit(procedure_with_id)
            .await
            .context(error::SubmitProcedureSnafu)?;

        Ok(Some(procedure_id))
    }

    /// Restores scheduling only when no drain owns the node.
    pub async fn mark_schedulable(&self, node_id: DatanodeId) -> Result<()> {
        let _admission = self.admission.lock().await;
        if self
            .tracker
            .running_procedures
            .read()
            .unwrap()
            .contains_key(&node_id)
        {
            return error::DrainDatanodeRunningSnafu { node_id }.fail();
        }
        self.node_scheduling_manager
            .mark_schedulable(NodeSchedulingRole::Datanode, node_id)
            .await?;
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
    use std::time::Duration;

    use common_procedure::Procedure;

    use crate::error;
    use crate::procedure::drain_datanode::manager::DrainDatanodeProcedureTracker;
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

    async fn hold_drain_lock(env: &TestEnv) -> std::sync::Arc<tokio::sync::Notify> {
        let entered = std::sync::Arc::new(tokio::sync::Notify::new());
        let release = std::sync::Arc::new(tokio::sync::Notify::new());
        env.procedure_manager()
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
        let release = hold_drain_lock(&env).await;
        let manager = env.drain_manager();
        let (first, second) = tokio::join!(
            manager.submit_procedure(Default::default(), task()),
            manager.submit_procedure(Default::default(), task()),
        );
        let first = first.unwrap().unwrap();
        assert_eq!(Some(first), second.unwrap());
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
        assert_eq!(
            None,
            manager
                .submit_procedure(Default::default(), task())
                .await
                .unwrap()
        );
        manager.mark_schedulable(1).await.unwrap();
        manager.mark_schedulable(1).await.unwrap();
        env.procedure_manager().stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_submit_drain_retry_reuses_recovered_procedure_id() {
        let env = TestEnv::new().await;
        env.procedure_manager().start().await.unwrap();
        let release = hold_drain_lock(&env).await;
        let manager = env.drain_manager();
        let original = DrainDatanodeProcedure::new(
            env.context(new_persistent_context(
                Some(2),
                DrainDatanodeStrategy::FailFast,
            )),
            vec![],
        );
        let checkpoint = original.dump().unwrap();
        let recovered =
            DrainDatanodeProcedure::from_json(&checkpoint, &manager.tracker(), |context| {
                env.context(context)
            })
            .unwrap();
        let procedure = common_procedure::ProcedureWithId::with_random_id(Box::new(recovered));
        let id = procedure.id;
        env.procedure_manager().submit(procedure).await.unwrap();
        assert_eq!(
            Some(id),
            manager
                .submit_procedure(Default::default(), task())
                .await
                .unwrap()
        );
        assert!(matches!(
            manager.mark_schedulable(1).await,
            Err(error::Error::DrainDatanodeRunning { .. })
        ));
        release.notify_one();
        env.procedure_manager().stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_completed_drain_retry_before_guard_release() {
        let env = TestEnv::new().await;
        let manager = env.drain_manager();
        let _guard = manager.tracker().insert_running_procedure(&task()).unwrap();
        env.node_scheduling_manager
            .mark_unschedulable(crate::key::NodeSchedulingRole::Datanode, 1)
            .await
            .unwrap();
        // Completion is visible before the runner releases its tracker reservation.
        assert_eq!(
            None,
            manager
                .submit_procedure(Default::default(), task())
                .await
                .unwrap()
        );
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
            .unwrap()
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
            .unwrap()
            .unwrap();
        assert_ne!(first, second);
        env.procedure_manager().stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_failed_submission_releases_drain_admission() {
        let env = TestEnv::new().await;
        let manager = env.drain_manager();
        assert!(
            manager
                .submit_procedure(Default::default(), task())
                .await
                .is_err()
        );
        assert!(
            manager
                .tracker
                .running_procedures
                .read()
                .unwrap()
                .is_empty()
        );
        manager.mark_schedulable(1).await.unwrap();
        env.procedure_manager().start().await.unwrap();
        assert!(
            manager
                .submit_procedure(Default::default(), task())
                .await
                .unwrap()
                .is_some()
        );
        env.procedure_manager().stop().await.unwrap();
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
    async fn test_submit_procedure_rejects_a_running_drain() {
        let env = TestEnv::new().await;
        let manager = env.drain_manager();
        let task = DrainDatanodeProcedureTask {
            node_id: 1,
            target_node_id: Some(2),
            parallelism: DEFAULT_PARALLELISM,
            strategy: DrainDatanodeStrategy::FailFast,
            timeout: None,
        };

        // A drain of datanode 1 is running, for example one recovered after a restart.
        let _guard = manager.tracker().insert_running_procedure(&task).unwrap();

        let error = manager
            .submit_procedure(common_procedure::ProcedureContext::default(), task)
            .await
            .unwrap_err();

        assert!(matches!(error, error::Error::DrainDatanodeRunning { .. }));
        assert!(!error.is_retryable());
    }

    #[tokio::test]
    async fn test_drain_datanode_takes_the_place_back_on_reload() {
        let env = TestEnv::new().await;
        let context = env.context(new_persistent_context(
            Some(2),
            DrainDatanodeStrategy::FailFast,
        ));
        let procedure = DrainDatanodeProcedure::new(context, vec![]);
        let json = procedure.dump().unwrap();
        let tracker = DrainDatanodeProcedureTracker::default();

        // Reloading takes the place back, so a second drain of the datanode is rejected.
        let reloaded = DrainDatanodeProcedure::from_json(&json, &tracker, |persistent_ctx| {
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
        assert!(tracker.insert_running_procedure(&task).is_none());

        // Dropping the reloaded procedure releases the place.
        drop(reloaded);
        assert!(tracker.insert_running_procedure(&task).is_some());
    }
}
