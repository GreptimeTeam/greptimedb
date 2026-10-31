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
use common_telemetry::{error, info};
use serde::{Deserialize, Serialize};

use crate::error::{self, Result};
use crate::procedure::drain_datanode::{Context, State};

/// Finishes the procedure and reports the outcome.
///
/// The procedure state carries no output: only its status is observable through the
/// procedure API, so a drain that left regions behind ends as `Failed` with the failed
/// regions in the error instead of as `Done` with a report.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DrainDatanodeEnd;

#[async_trait::async_trait]
#[typetag::serde]
impl State for DrainDatanodeEnd {
    async fn next(
        &mut self,
        ctx: &mut Context,
        _procedure_ctx: &ProcedureContext,
    ) -> Result<(Box<dyn State>, Status)> {
        let node_id = ctx.persistent_ctx.node_id;
        let migrated = ctx.volatile_ctx.migrated;
        let follower_regions = ctx.volatile_ctx.follower_regions().len();
        let failed_regions = ctx.volatile_ctx.failed_regions.clone();

        if !failed_regions.is_empty() {
            error!(
                "Drained datanode {} except {} regions, failed regions: {:?}",
                node_id,
                failed_regions.len(),
                failed_regions
            );
            return error::DrainDatanodeAbortSnafu {
                node_id,
                failed_regions,
            }
            .fail();
        }

        info!(
            "Drained datanode {}, migrated leader regions: {}, follower regions left: {}",
            node_id, migrated, follower_regions
        );

        Ok((Box::new(DrainDatanodeEnd), Status::done()))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use common_procedure::Status;
    use store_api::storage::RegionId;

    use crate::error;
    use crate::procedure::drain_datanode::drain_datanode_end::DrainDatanodeEnd;
    use crate::procedure::drain_datanode::test_util::{TestEnv, new_persistent_context};
    use crate::procedure::drain_datanode::{DrainDatanodeStrategy, State};
    use crate::procedure::region_migration::test_util::TestingEnv;

    #[tokio::test]
    async fn test_drain_datanode_end_transitions_to_done() {
        let env = TestEnv::new().await;

        let mut ctx = env.context(new_persistent_context(
            Some(2),
            DrainDatanodeStrategy::TryAll,
        ));
        ctx.volatile_ctx.migrated = 1;
        let procedure_ctx = TestingEnv::procedure_context();

        let (next, status) = DrainDatanodeEnd
            .next(&mut ctx, &procedure_ctx)
            .await
            .unwrap();

        assert!(next.as_any().downcast_ref::<DrainDatanodeEnd>().is_some());
        assert!(matches!(status, Status::Done { output: None }));
    }

    #[tokio::test]
    async fn test_drain_datanode_end_aborts_when_regions_are_left() {
        let env = TestEnv::new().await;
        let region_id = RegionId::new(1024, 1);

        let mut ctx = env.context(new_persistent_context(
            Some(2),
            DrainDatanodeStrategy::TryAll,
        ));
        ctx.volatile_ctx.failed_regions = vec![region_id];
        let procedure_ctx = TestingEnv::procedure_context();

        let error = DrainDatanodeEnd
            .next(&mut ctx, &procedure_ctx)
            .await
            .unwrap_err();

        let error::Error::DrainDatanodeAbort { failed_regions, .. } = error else {
            panic!("expected the drain procedure to abort");
        };
        assert_eq!(vec![region_id], failed_regions);
    }
}
