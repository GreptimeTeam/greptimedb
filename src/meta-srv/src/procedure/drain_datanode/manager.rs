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
/// Mirrors `RegionMigrationProcedureTracker`: a drain of a datanode that is already being
/// drained is rejected at submission instead of waiting for the procedure lock.
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
/// It registers the procedure loader and submits drain tasks. Procedures of the same
/// datanode are serialized by the procedure lock declared in
/// [`DrainDatanodeProcedure::lock_key`].
pub struct DrainDatanodeManager {
    procedure_manager: ProcedureManagerRef,
    table_metadata_manager: TableMetadataManagerRef,
    node_scheduling_manager: NodeSchedulingManagerRef,
    region_migration_manager: RegionMigrationManagerRef,
    selector_ctx: SelectorContext,

    tracker: DrainDatanodeProcedureTracker,
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

    /// Submits a [`DrainDatanodeProcedure`] and returns its id.
    pub async fn submit_procedure(
        &self,
        procedure_context: ProcedureContext,
        task: DrainDatanodeProcedureTask,
    ) -> Result<ProcedureId> {
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

        Ok(procedure_id)
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
