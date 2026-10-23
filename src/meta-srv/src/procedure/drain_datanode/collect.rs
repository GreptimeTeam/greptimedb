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
use common_telemetry::{error, info};
use serde::{Deserialize, Serialize};

use crate::error::{self, Result};
use crate::procedure::drain_datanode::check_drained::CheckDrained;
use crate::procedure::drain_datanode::dispatch::Dispatch;
use crate::procedure::drain_datanode::{Context, DrainDatanodeStrategy, State};

/// Collects the results of the dispatched region migration sub-procedures.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Collect;

#[async_trait::async_trait]
#[typetag::serde]
impl State for Collect {
    async fn next(
        &mut self,
        ctx: &mut Context,
        procedure_ctx: &ProcedureContext,
    ) -> Result<(Box<dyn State>, Status)> {
        let node_id = ctx.persistent_ctx.node_id;
        let inflight_procedures = std::mem::take(&mut ctx.persistent_ctx.inflight_procedures);
        for meta in inflight_procedures {
            let region_id = meta.region_id;
            if wait_subprocedure(procedure_ctx, meta.procedure_id).await {
                ctx.persistent_ctx
                    .leader_regions
                    .retain(|region| *region != region_id);
                ctx.persistent_ctx.migrated += 1;
                info!(
                    "Migrated region {} off datanode {}, procedure id: {}",
                    region_id, node_id, meta.procedure_id
                );
                continue;
            }

            match ctx.persistent_ctx.strategy {
                DrainDatanodeStrategy::FailFast => {
                    return error::DrainDatanodeAbortSnafu {
                        node_id,
                        failed_regions: vec![region_id],
                    }
                    .fail();
                }
                DrainDatanodeStrategy::TryAll => {
                    error!(
                        "Failed to migrate region {} off datanode {}, procedure id: {}",
                        region_id, node_id, meta.procedure_id
                    );
                    ctx.persistent_ctx
                        .leader_regions
                        .retain(|region| *region != region_id);
                    ctx.persistent_ctx.failed_regions.push(region_id);
                }
            }
        }

        if ctx.persistent_ctx.leader_regions.is_empty() {
            Ok((Box::new(CheckDrained), Status::executing(true)))
        } else {
            Ok((Box::new(Dispatch), Status::executing(true)))
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Returns true if the sub-procedure finished successfully.
///
/// A sub-procedure that is unknown to the procedure manager (for example after a metasrv
/// restart) is reported as failed.
async fn wait_subprocedure(procedure_ctx: &ProcedureContext, procedure_id: ProcedureId) -> bool {
    let Ok(Some(mut receiver)) = procedure_ctx
        .provider
        .procedure_state_receiver(procedure_id)
        .await
    else {
        return false;
    };

    watcher::wait(&mut receiver).await.is_ok()
}
