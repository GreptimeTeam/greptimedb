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

/// Scans topic batches, prepares migrations, and confirms completion with a fresh scan.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DrainDatanodeCursor;

#[async_trait::async_trait]
#[typetag::serde]
impl State for DrainDatanodeCursor {
    async fn next(
        &mut self,
        ctx: &mut Context,
        _procedure_ctx: &ProcedureContext,
    ) -> Result<(Box<dyn State>, Status)> {
        utils::ensure_drain_deadline(ctx)?;

        loop {
            if !ctx.volatile_ctx.leader_regions().is_empty() {
                ctx.prepare_migrations().await?;
                if !ctx.volatile_ctx.migrations.is_empty() {
                    return Ok((Box::new(Dispatch), Status::executing(false)));
                }
            }
            if ctx.volatile_ctx.scan.is_none() {
                ctx.start_scan()?;
            }
            ctx.read_next_topic().await?;
            if ctx.volatile_ctx.topic_batch.is_some() {
                continue;
            }
            let produced_batch = ctx
                .volatile_ctx
                .scan
                .take()
                .is_some_and(|scan| scan.produced_batch);
            if produced_batch {
                // Settled work may have changed the routes. Confirm with another full scan.
                continue;
            }
            info!(
                "No more migrations to dispatch for datanode {}, migrated regions: {}, follower regions: {}",
                ctx.persistent_ctx.node_id,
                ctx.volatile_ctx.migrated,
                ctx.volatile_ctx.follower_count
            );
            return Ok((Box::new(DrainDatanodeEnd), Status::executing(false)));
        }
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
    use crate::procedure::drain_datanode::cursor::DrainDatanodeCursor;
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
    async fn test_drain_datanode_cursor_excludes_running_migrations() {
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

            let (next, status) = DrainDatanodeCursor
                .next(&mut ctx, &procedure_ctx)
                .await
                .unwrap();
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
            ctx.volatile_ctx.topic_batch = None;
            ctx.volatile_ctx.scan = None;
            let (next, _) = DrainDatanodeCursor
                .next(&mut ctx, &procedure_ctx)
                .await
                .unwrap();
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
    async fn test_drain_datanode_cursor_transitions_to_end() {
        let env = TestEnv::new().await;

        let mut ctx = env.context(new_persistent_context(
            Some(2),
            DrainDatanodeStrategy::FailFast,
        ));
        let procedure_ctx = TestingEnv::procedure_context();

        let (next, status) = DrainDatanodeCursor
            .next(&mut ctx, &procedure_ctx)
            .await
            .unwrap();

        assert!(next.as_any().downcast_ref::<DrainDatanodeEnd>().is_some());
        assert!(matches!(status, Status::Executing { persist: false, .. }));
    }

    #[tokio::test]
    async fn test_drain_datanode_cursor_rescan_requeues() {
        let env = TestEnv::new().await;
        let region_id = RegionId::new(1024, 1);
        env.create_table(vec![leader_region(region_id, 1)]).await;

        // The plan of the procedure is stale: a region still on the datanode is not listed.
        let mut ctx = env.context(new_persistent_context(
            None,
            DrainDatanodeStrategy::FailFast,
        ));
        let procedure_ctx = TestingEnv::procedure_context();

        let (next, status) = DrainDatanodeCursor
            .next(&mut ctx, &procedure_ctx)
            .await
            .unwrap();

        assert!(matches!(status, Status::Executing { .. }));
        assert!(next.as_any().downcast_ref::<Dispatch>().is_some());
        assert_eq!(vec![region_id], leader_regions(&ctx));
        assert_eq!(1, ctx.volatile_ctx.rescan_round);
    }

    #[tokio::test]
    async fn test_drain_datanode_cursor_exceeded_rounds() {
        let env = TestEnv::new().await;
        let region_id = RegionId::new(1024, 1);
        env.create_table(vec![leader_region(region_id, 1)]).await;

        let mut ctx = env.context(new_persistent_context(
            None,
            DrainDatanodeStrategy::FailFast,
        ));
        ctx.volatile_ctx.rescan_round = MAX_RESCAN_ROUNDS;
        let procedure_ctx = TestingEnv::procedure_context();

        let error = DrainDatanodeCursor
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
    async fn test_drain_datanode_cursor_ignores_failed_regions() {
        let env = TestEnv::new().await;
        let region_id = RegionId::new(1024, 1);
        env.create_table(vec![leader_region(region_id, 1)]).await;

        let mut ctx = env.context(new_persistent_context(None, DrainDatanodeStrategy::TryAll));
        ctx.volatile_ctx.failed_regions = vec![region_id];
        let procedure_ctx = TestingEnv::procedure_context();

        let (next, status) = DrainDatanodeCursor
            .next(&mut ctx, &procedure_ctx)
            .await
            .unwrap();

        assert!(matches!(status, Status::Executing { .. }));
        assert!(next.as_any().downcast_ref::<DrainDatanodeEnd>().is_some());
    }
    #[tokio::test]
    async fn test_cursor_drains_multiple_topic_windows_and_confirms_completion() {
        use crate::procedure::drain_datanode::DrainDatanodeProcedureTask;
        use crate::procedure::drain_datanode::test_util::{
            create_kafka_table, wait_for_terminal_state,
        };
        use common_procedure::ProcedureState;

        let mut env = TestEnv::new().await;
        create_kafka_table(&env, 1024, 13, 3).await;
        env.mock_datanodes().await;
        env.procedure_manager().start().await.unwrap();
        let id = env
            .drain_manager()
            .submit_procedure(
                common_procedure::ProcedureContext::default(),
                DrainDatanodeProcedureTask {
                    node_id: 1,
                    target_node_id: Some(2),
                    parallelism: 2,
                    strategy: DrainDatanodeStrategy::FailFast,
                    timeout: None,
                },
            )
            .await
            .unwrap();
        let state = wait_for_terminal_state(env.procedure_manager(), id).await;
        assert!(matches!(state, ProcedureState::Done { .. }), "{state:?}");
        let route = env
            .table_metadata_manager
            .table_route_manager()
            .table_route_storage()
            .get(1024)
            .await
            .unwrap()
            .unwrap();
        let regions = route.region_routes().unwrap();
        assert_eq!(13, regions.len());
        assert!(
            regions
                .iter()
                .all(|region| region.leader_peer.as_ref().is_some_and(|peer| peer.id == 2))
        );
        env.procedure_manager().stop().await.unwrap();
    }
}
