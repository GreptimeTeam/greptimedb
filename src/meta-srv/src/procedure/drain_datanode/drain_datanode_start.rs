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

use common_procedure::{Context as ProcedureContext, Status};
use common_telemetry::info;
use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::key::NodeSchedulingRole;
use crate::procedure::drain_datanode::drain_datanode_end::DrainDatanodeEnd;
use crate::procedure::drain_datanode::{Context, State};

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
            .await?;
        if marked {
            info!("Marked datanode {} as unschedulable", node_id);
        }

        ctx.ensure_plan().await?;
        info!(
            "Planned drain of datanode {}, leader regions: {}, follower regions: {}",
            node_id,
            ctx.volatile_ctx.leader_regions().len(),
            ctx.volatile_ctx.follower_regions().len()
        );

        Ok((Box::new(DrainDatanodeEnd), Status::executing(true)))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::procedure::drain_datanode::test_util::{
        TestEnv, leader_region, new_persistent_context,
    };
    use crate::procedure::region_migration::test_util::TestingEnv;
    use store_api::storage::RegionId;

    #[tokio::test]
    async fn test_drain_datanode_start_transitions_to_end() {
        let env = TestEnv::new().await;
        env.create_table(vec![leader_region(RegionId::new(1024, 1), 1)])
            .await;

        let mut ctx = env.context(new_persistent_context(Some(2)));
        let procedure_ctx = TestingEnv::procedure_context();

        let (next, status) = DrainDatanodeStart
            .next(&mut ctx, &procedure_ctx)
            .await
            .unwrap();

        assert!(next.as_any().downcast_ref::<DrainDatanodeEnd>().is_some());
        // Registering the procedure is the only persisted step of the run.
        assert!(matches!(status, Status::Executing { persist: true, .. }));
        assert_eq!(
            vec![RegionId::new(1024, 1)],
            ctx.volatile_ctx.leader_regions()
        );
        assert!(
            !ctx.node_scheduling_manager
                .is_schedulable(NodeSchedulingRole::Datanode, 1)
                .await
                .unwrap()
        );
    }
}
