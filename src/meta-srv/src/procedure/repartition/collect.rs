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

use std::any::Any;

use common_procedure::{Context as ProcedureContext, ProcedureId, Status, watcher};
use common_telemetry::error;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

use crate::error::{RepartitionSubprocedureStateReceiverSnafu, Result};
use crate::procedure::repartition::deallocate_region::DeallocateRegion;
use crate::procedure::repartition::group::GroupId;
use crate::procedure::repartition::{Context, State};

/// Metadata for tracking a dispatched sub-procedure.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub struct ProcedureMeta {
    /// The index of the plan entry in the parent procedure's plan list.
    pub plan_index: usize,
    /// The group id of the repartition group.
    pub group_id: GroupId,
    /// The procedure id of the sub-procedure.
    pub procedure_id: ProcedureId,
}

/// State for collecting results from dispatched sub-procedures.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Collect {
    /// Sub-procedures that are currently in-flight.
    pub inflight_procedures: Vec<ProcedureMeta>,
    /// Sub-procedures that have completed successfully.
    pub succeeded_procedures: Vec<ProcedureMeta>,
    /// Sub-procedures that have failed.
    pub failed_procedures: Vec<ProcedureMeta>,
    /// Sub-procedures whose state could not be determined.
    pub unknown_procedures: Vec<ProcedureMeta>,
}

impl Collect {
    pub fn new(inflight_procedures: Vec<ProcedureMeta>) -> Self {
        Self {
            inflight_procedures,
            succeeded_procedures: Vec::new(),
            failed_procedures: Vec::new(),
            unknown_procedures: Vec::new(),
        }
    }
}

#[async_trait::async_trait]
#[typetag::serde]
impl State for Collect {
    async fn next(
        &mut self,
        _ctx: &mut Context,
        procedure_ctx: &ProcedureContext,
    ) -> Result<(Box<dyn State>, Status)> {
        for procedure_meta in self.inflight_procedures.iter() {
            let procedure_id = procedure_meta.procedure_id;
            let group_id = procedure_meta.group_id;
            let Some(mut receiver) = procedure_ctx
                .provider
                .procedure_state_receiver(procedure_id)
                .await
                .context(RepartitionSubprocedureStateReceiverSnafu { procedure_id })?
            else {
                error!(
                    "failed to get procedure state receiver, procedure_id: {}, group_id: {}",
                    procedure_id, group_id
                );
                self.unknown_procedures.push(*procedure_meta);
                continue;
            };

            match watcher::wait(&mut receiver).await {
                Ok(_) => self.succeeded_procedures.push(*procedure_meta),
                Err(e) => {
                    error!(e; "failed to wait for repartition subprocedure, procedure_id: {}, group_id: {}", procedure_id, group_id);
                    self.failed_procedures.push(*procedure_meta);
                }
            }
        }

        if !self.failed_procedures.is_empty() || !self.unknown_procedures.is_empty() {
            // TODO(weny): retry the failed or unknown procedures.
        }

        Ok((Box::new(DeallocateRegion), Status::executing(true)))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
