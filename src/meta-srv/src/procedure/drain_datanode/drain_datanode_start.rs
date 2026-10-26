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

use common_error::ext::BoxedError;
use common_procedure::{Context as ProcedureContext, Status};
use common_telemetry::info;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

use crate::error::{self, Result};
use crate::key::NodeSchedulingRole;
use crate::procedure::drain_datanode::drain_datanode_end::DrainDatanodeEnd;
use crate::procedure::drain_datanode::{Context, State, utils};

/// Fences the datanode and builds the drain plan.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DrainDatanodeStart;

#[async_trait::async_trait]
#[typetag::serde]
impl State for DrainDatanodeStart {
    async fn next(
        &mut self,
        ctx: &mut Context,
        _procedure_ctx: &ProcedureContext,
    ) -> Result<(Box<dyn State>, Status)> {
        let node_id = ctx.persistent_ctx.node_id;
        let marked = ctx
            .node_scheduling_manager
            .mark_unschedulable(NodeSchedulingRole::Datanode, node_id)
            .await
            .map_err(BoxedError::new)
            .context(error::OtherSnafu)?;
        if marked {
            info!("Marked datanode {} as unschedulable", node_id);
        }

        let plan = utils::build_datanode_plan(&ctx.table_metadata_manager, node_id).await?;
        info!(
            "Planned drain of datanode {}, leader regions: {}, follower regions: {}",
            node_id,
            plan.leader_regions.len(),
            plan.follower_region_count
        );
        ctx.persistent_ctx.leader_regions = plan.leader_regions;
        ctx.persistent_ctx.follower_region_count = plan.follower_region_count;

        Ok((Box::new(DrainDatanodeEnd), Status::executing(true)))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
