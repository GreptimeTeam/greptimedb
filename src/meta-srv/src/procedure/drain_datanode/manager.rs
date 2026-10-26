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
use snafu::ResultExt;

use crate::error::{self, Result};
use crate::node_scheduling::NodeSchedulingManagerRef;
use crate::procedure::drain_datanode::{
    Context as DrainDatanodeContext, DrainDatanodeProcedure, DrainDatanodeProcedureTask,
    PersistentContext,
};

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
/// It registers the procedure loader and submits drain tasks. A datanode is drained by at
/// most one procedure at a time: [`DrainDatanodeManager::submit_procedure`] rejects a second
/// drain through the [`DrainDatanodeProcedureTracker`].
pub struct DrainDatanodeManager {
    procedure_manager: ProcedureManagerRef,
    table_metadata_manager: TableMetadataManagerRef,
    node_scheduling_manager: NodeSchedulingManagerRef,
    tracker: DrainDatanodeProcedureTracker,
}

impl DrainDatanodeManager {
    /// Returns a new [`DrainDatanodeManager`].
    pub fn new(
        procedure_manager: ProcedureManagerRef,
        table_metadata_manager: TableMetadataManagerRef,
        node_scheduling_manager: NodeSchedulingManagerRef,
    ) -> Self {
        Self {
            procedure_manager,
            table_metadata_manager,
            node_scheduling_manager,
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

        let Some(guard) = self.tracker.insert_running_procedure(&task) else {
            return error::DrainDatanodeRunningSnafu {
                node_id: task.node_id,
            }
            .fail();
        };

        let persistent_ctx = PersistentContext {
            node_id: task.node_id,
            target_node_id: task.target_node_id,
        };
        let procedure = DrainDatanodeProcedure::new(self.context(persistent_ctx), vec![guard]);
        let procedure_with_id =
            ProcedureWithId::with_random_id(Box::new(procedure)).with_context(procedure_context);
        let procedure_id = procedure_with_id.id;
        info!(
            "Starting drain datanode procedure {procedure_id} for datanode {}",
            task.node_id
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
        let tracker = self.tracker.clone();
        self.procedure_manager
            .register_loader(
                DrainDatanodeProcedure::TYPE_NAME,
                Box::new(move |json| {
                    let table_metadata_manager = table_metadata_manager.clone();
                    let node_scheduling_manager = node_scheduling_manager.clone();
                    let factory = move |persistent_ctx| {
                        DrainDatanodeContext::new(
                            table_metadata_manager,
                            node_scheduling_manager,
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
