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
use crate::procedure::drain_datanode::dispatch::Dispatch;
use crate::procedure::drain_datanode::drain_datanode_end::DrainDatanodeEnd;
use crate::procedure::drain_datanode::{Context, State, utils};

/// Rescans the datanode for regions that still need a migration procedure.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct CheckDrained;

#[async_trait::async_trait]
#[typetag::serde]
impl State for CheckDrained {
    async fn next(
        &mut self,
        ctx: &mut Context,
        _procedure_ctx: &ProcedureContext,
    ) -> Result<(Box<dyn State>, Status)> {
        utils::ensure_drain_deadline(ctx)?;

        let node_id = ctx.persistent_ctx.node_id;
        // Reconcile against the table routes after settling all batches of this round.
        ctx.rebuild_plan().await?;
        // Regions given up by `DrainDatanodeStrategy::TryAll` are still led by the datanode;
        // they must not trigger another rescan round.
        // Migrations registered by recovery or another caller own their lifecycle. They
        // need neither a new child nor a restored parent link, even if still in progress.
        let tracker = ctx.region_migration_manager.tracker();
        let volatile_ctx = &mut ctx.volatile_ctx;
        if let Some(plan) = volatile_ctx.plan.as_mut() {
            plan.leader_regions.retain(|region_id| {
                !volatile_ctx.failed_regions.contains(region_id) && !tracker.contains(*region_id)
            });
        }

        let migrated = ctx.volatile_ctx.migrated;
        let rescan_round = ctx.volatile_ctx.rescan_round;
        let remaining_leader_regions = ctx.volatile_ctx.leader_regions().len();
        if remaining_leader_regions == 0 {
            let follower_regions = ctx.volatile_ctx.follower_regions().len();
            info!(
                "No more migrations to dispatch for datanode {}, migrated regions: {}, follower regions: {}",
                node_id, migrated, follower_regions
            );
            return Ok((Box::new(DrainDatanodeEnd), Status::executing(false)));
        }

        info!(
            "Datanode {} still leads {} regions, rescan round: {}",
            node_id, remaining_leader_regions, rescan_round
        );

        Ok((Box::new(Dispatch), Status::executing(false)))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use common_meta::peer::Peer;
    use common_procedure::Status;
    use store_api::storage::RegionId;

    use crate::error;
    use crate::procedure::drain_datanode::check_drained::CheckDrained;
    use crate::procedure::drain_datanode::dispatch::Dispatch;
    use crate::procedure::drain_datanode::drain_datanode_end::DrainDatanodeEnd;
    use crate::procedure::drain_datanode::test_util::{
        TestEnv, leader_region, leader_regions, new_persistent_context,
    };
    use crate::procedure::drain_datanode::{DrainDatanodeStrategy, MAX_RESCAN_ROUNDS, State};
    use crate::procedure::region_migration::manager::{
        RegionMigrationProcedureTask, RegionMigrationTriggerReason,
    };
    use crate::procedure::region_migration::test_util::TestingEnv;

    #[tokio::test]
    async fn test_drain_datanode_check_drained_excludes_running_migrations() {
        for has_pending_region in [false, true] {
            let env = TestEnv::new().await;
            let running = RegionId::new(1024, 1);
            let pending = RegionId::new(1024, 2);
            let failed = RegionId::new(1024, 3);
            let mut routes = vec![leader_region(running, 1), leader_region(failed, 1)];
            if has_pending_region {
                routes.push(leader_region(pending, 1));
            }
            env.create_table(routes).await;
            let guard = env
                .region_migration_manager
                .tracker()
                .insert_running_procedure(&RegionMigrationProcedureTask::new(
                    running,
                    Peer::empty(1),
                    Peer::empty(2),
                    Duration::from_secs(10),
                    RegionMigrationTriggerReason::Manual,
                ))
                .unwrap();
            let mut ctx = env.context(new_persistent_context(
                Some(2),
                DrainDatanodeStrategy::TryAll,
            ));
            ctx.volatile_ctx.failed_regions.push(failed);
            let procedure_ctx = TestingEnv::procedure_context();

            let (next, status) = CheckDrained.next(&mut ctx, &procedure_ctx).await.unwrap();
            assert!(matches!(status, Status::Executing { persist: false, .. }));
            assert_eq!(vec![failed], ctx.volatile_ctx.failed_regions);
            assert_eq!(0, ctx.volatile_ctx.migrated);
            if has_pending_region {
                assert!(next.as_any().is::<Dispatch>());
                assert_eq!(vec![pending], leader_regions(&ctx));
            } else {
                assert!(next.as_any().is::<DrainDatanodeEnd>());
                assert!(leader_regions(&ctx).is_empty());
            }

            // Once the migration releases its guard, an unchanged route needs a new child.
            drop(guard);
            let (next, _) = CheckDrained.next(&mut ctx, &procedure_ctx).await.unwrap();
            assert!(next.as_any().is::<Dispatch>());
            let expected = if has_pending_region {
                vec![running, pending]
            } else {
                vec![running]
            };
            assert_eq!(expected, leader_regions(&ctx));
        }
    }

    #[tokio::test]
    async fn test_drain_datanode_check_drained_transitions_to_end() {
        let env = TestEnv::new().await;

        let mut ctx = env.context(new_persistent_context(
            Some(2),
            DrainDatanodeStrategy::FailFast,
        ));
        let procedure_ctx = TestingEnv::procedure_context();

        let (next, status) = CheckDrained.next(&mut ctx, &procedure_ctx).await.unwrap();

        assert!(next.as_any().downcast_ref::<DrainDatanodeEnd>().is_some());
        assert!(matches!(status, Status::Executing { persist: false, .. }));
    }

    #[tokio::test]
    async fn test_drain_datanode_check_drained_rescan_requeues() {
        let env = TestEnv::new().await;
        let region_id = RegionId::new(1024, 1);
        env.create_table(vec![leader_region(region_id, 1)]).await;

        // The plan of the procedure is stale: a region still on the datanode is not listed.
        let mut ctx = env.context(new_persistent_context(
            None,
            DrainDatanodeStrategy::FailFast,
        ));
        let procedure_ctx = TestingEnv::procedure_context();

        let (next, status) = CheckDrained.next(&mut ctx, &procedure_ctx).await.unwrap();

        assert!(matches!(status, Status::Executing { .. }));
        assert!(next.as_any().downcast_ref::<Dispatch>().is_some());
        assert_eq!(vec![region_id], leader_regions(&ctx));
        assert_eq!(1, ctx.volatile_ctx.rescan_round);
    }

    #[tokio::test]
    async fn test_drain_datanode_check_drained_exceeded_rounds() {
        let env = TestEnv::new().await;
        let region_id = RegionId::new(1024, 1);
        env.create_table(vec![leader_region(region_id, 1)]).await;

        let mut ctx = env.context(new_persistent_context(
            None,
            DrainDatanodeStrategy::FailFast,
        ));
        ctx.volatile_ctx.rescan_round = MAX_RESCAN_ROUNDS;
        let procedure_ctx = TestingEnv::procedure_context();

        let error = CheckDrained
            .next(&mut ctx, &procedure_ctx)
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            error::Error::DrainDatanodeRescanExceeded { .. }
        ));
        assert!(!error.is_retryable());
    }

    #[tokio::test]
    async fn test_drain_datanode_check_drained_ignores_failed_regions() {
        let env = TestEnv::new().await;
        let region_id = RegionId::new(1024, 1);
        env.create_table(vec![leader_region(region_id, 1)]).await;

        let mut ctx = env.context(new_persistent_context(None, DrainDatanodeStrategy::TryAll));
        ctx.volatile_ctx.failed_regions = vec![region_id];
        let procedure_ctx = TestingEnv::procedure_context();

        let (next, status) = CheckDrained.next(&mut ctx, &procedure_ctx).await.unwrap();

        assert!(matches!(status, Status::Executing { .. }));
        assert!(next.as_any().downcast_ref::<DrainDatanodeEnd>().is_some());
    }
}
