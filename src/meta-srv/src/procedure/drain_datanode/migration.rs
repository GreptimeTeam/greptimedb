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

use common_meta::DatanodeId;
use common_meta::key::table_route::TableRouteValue;
use common_meta::peer::Peer;
use common_procedure::{
    Context as ProcedureContext, ProcedureContext as SubmissionContext, ProcedureWithId,
};
use snafu::ResultExt;
use store_api::storage::RegionId;

use crate::error::{self, Result};
use crate::procedure::drain_datanode::{Context, utils};
use crate::procedure::region_migration::DEFAULT_REGION_MIGRATION_TIMEOUT;
use crate::procedure::region_migration::manager::{
    RegionMigrationProcedureTask, RegionMigrationTriggerReason,
};

impl Context {
    /// Builds the migration sub-procedure of `region_id`.
    ///
    /// Returns `Ok(None)` when the region is no longer led by the drained datanode.
    /// Otherwise returns the sub-procedure and its target.
    pub async fn build_migration_procedure(
        &self,
        procedure_ctx: &ProcedureContext,
        region_id: RegionId,
    ) -> Result<Option<(ProcedureWithId, DatanodeId)>> {
        let node_id = self.persistent_ctx.node_id;
        let Some(table_route) = self
            .table_metadata_manager
            .table_route_manager()
            .table_route_storage()
            .get(region_id.table_id())
            .await
            .context(error::TableMetadataManagerSnafu)?
        else {
            return Ok(None);
        };
        let TableRouteValue::Physical(table_route) = table_route else {
            return Ok(None);
        };
        let Some(region_route) = table_route
            .region_routes
            .iter()
            .find(|region_route| region_route.region.id == region_id)
        else {
            return Ok(None);
        };
        if region_route.leader_peer.as_ref().map(|peer| peer.id) != Some(node_id) {
            return Ok(None);
        }

        let to_peer =
            utils::select_migration_target(self, region_id, &region_route.follower_peers).await?;

        let submission_context = match procedure_ctx.event_context.clone() {
            Some(event_context) => SubmissionContext::from_event_context(event_context),
            None => SubmissionContext::default(),
        };
        let target_node_id = to_peer.id;
        let child = self
            .region_migration_manager
            .build_subprocedure(
                submission_context,
                RegionMigrationProcedureTask::new(
                    region_id,
                    Peer::empty(node_id),
                    to_peer,
                    DEFAULT_REGION_MIGRATION_TIMEOUT,
                    RegionMigrationTriggerReason::Manual,
                ),
            )
            .await?;

        Ok(Some((child, target_node_id)))
    }
}
#[cfg(test)]
mod tests {
    use crate::error;
    use crate::procedure::drain_datanode::DrainDatanodeStrategy;
    use crate::procedure::drain_datanode::test_util::{
        TestEnv, leader_region, new_persistent_context,
    };
    use store_api::storage::RegionId;
    #[tokio::test]
    async fn test_build_migration_procedure_checks_route_and_guard() {
        for leader_id in [1, 2] {
            let env = TestEnv::new().await;
            let region_id = RegionId::new(1024, 1);
            env.create_table(vec![leader_region(region_id, leader_id)])
                .await;
            let ctx = env.context(new_persistent_context(
                Some(2),
                DrainDatanodeStrategy::FailFast,
            ));
            let procedure_ctx =
                crate::procedure::region_migration::test_util::TestingEnv::procedure_context();
            let child = ctx
                .build_migration_procedure(&procedure_ctx, region_id)
                .await
                .unwrap();
            if leader_id == 2 {
                assert!(child.is_none());
                assert!(!env.region_migration_manager.tracker().contains(region_id));
                continue;
            }
            let (child, target) = child.unwrap();
            assert_eq!(2, target);
            assert!(env.region_migration_manager.tracker().contains(region_id));
            let error = ctx
                .build_migration_procedure(&procedure_ctx, region_id)
                .await
                .unwrap_err();
            assert!(matches!(error, error::Error::MigrationRunning { .. }));
            drop(child);
            assert!(!env.region_migration_manager.tracker().contains(region_id));
        }
    }
}
