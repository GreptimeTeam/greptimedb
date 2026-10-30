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
use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::procedure::drain_datanode::cursor::DrainDatanodeCursor;
use crate::procedure::drain_datanode::utils;
use crate::procedure::drain_datanode::{Context, State};

/// Submits the cursor's prepared batches as detached children and collects their results.
/// Returns to the cursor after each round; recovered children retain their own lifecycle.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Dispatch;

#[async_trait::async_trait]
#[typetag::serde]
impl State for Dispatch {
    async fn next(
        &mut self,
        ctx: &mut Context,
        procedure_ctx: &ProcedureContext,
    ) -> Result<(Box<dyn State>, Status)> {
        if !ctx.volatile_ctx.inflight_subprocedures.is_empty() {
            ctx.wait_for_inflight_subprocedures(procedure_ctx, ctx.persistent_ctx.strategy)
                .await?;
            return Ok((Box::new(DrainDatanodeCursor), Status::executing(false)));
        }
        utils::ensure_drain_deadline(ctx)?;
        ctx.dispatch_migrations(procedure_ctx).await
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    use crate::procedure::test_util::{procedure_context_with_receivers, procedure_state_receiver};
    use api::v1::meta::heartbeat_request::NodeWorkloads;
    use common_meta::cluster::NodeInfo;
    use common_meta::error::Result as MetaResult;
    use common_meta::key::table_route::TableRouteKey;
    use common_meta::key::{MetadataKey, TableMetadataManager, TableMetadataManagerRef};
    use common_meta::kv_backend::test_util::MockKvBackendBuilder;
    use common_meta::peer::{Peer, PeerDiscovery, PeerDiscoveryRef};
    use common_meta::rpc::store::{BatchGetRequest, BatchGetResponse};
    use common_procedure::{ProcedureId, ProcedureState, Status};
    use store_api::storage::RegionId;

    use crate::error;
    use crate::key::NodeSchedulingRole;
    use crate::procedure::drain_datanode::cursor::DrainDatanodeCursor;
    use crate::procedure::drain_datanode::dispatch::Dispatch;
    use crate::procedure::drain_datanode::drain_datanode_start::DrainDatanodeStart;
    use crate::procedure::drain_datanode::test_util::{
        TestEnv, dispatch_next, follower_region, leader_region, leader_regions,
        new_persistent_context, run_until_done, seed_plan,
    };
    use crate::procedure::drain_datanode::{DrainDatanodeStrategy, State, SubprocedureMeta};
    use crate::procedure::region_migration::manager::{
        RegionMigrationManager, RegionMigrationProcedureTask, RegionMigrationTriggerReason,
    };
    use crate::procedure::region_migration::test_util::TestingEnv;

    #[tokio::test]
    async fn test_drain_datanode_dispatch_transitions_to_cursor() {
        let env = TestEnv::new().await;
        // Only a follower region: there is no leader region to migrate.
        env.create_table(vec![follower_region(RegionId::new(1024, 1), 2, 1)])
            .await;

        let mut ctx = env.context(new_persistent_context(
            Some(2),
            DrainDatanodeStrategy::FailFast,
        ));
        let procedure_ctx = TestingEnv::procedure_context();
        seed_plan(&mut ctx, vec![]);
        let (next, status) = dispatch_next(&mut ctx, &procedure_ctx).await.unwrap();

        assert!(
            next.as_any()
                .downcast_ref::<DrainDatanodeCursor>()
                .is_some()
        );
        assert!(matches!(status, Status::Executing { persist: false, .. }));
    }

    #[tokio::test]
    async fn test_drain_datanode_dispatches_migration() {
        let env = TestEnv::new().await;
        let region_id = RegionId::new(1024, 1);
        env.create_table(vec![leader_region(region_id, 1)]).await;

        let mut ctx = env.context(new_persistent_context(
            Some(2),
            DrainDatanodeStrategy::FailFast,
        ));
        let procedure_ctx = TestingEnv::procedure_context();

        let (mut next, _) = DrainDatanodeStart
            .next(&mut ctx, &procedure_ctx)
            .await
            .unwrap();
        let status = run_until_done(&mut next, &mut ctx, &procedure_ctx)
            .await
            .unwrap();

        let Status::Suspended {
            subprocedures,
            recovery,
            persist,
        } = status
        else {
            panic!("expected the dispatch state to suspend the procedure");
        };
        // Recovery resumes children independently and rebuilds the parent's plan.
        assert!(!persist);
        assert_eq!(common_procedure::SubprocedureRecovery::Detach, recovery);
        assert_eq!(1, subprocedures.len());
        assert_eq!(1, ctx.volatile_ctx.inflight_subprocedures.len());
        assert_eq!(
            region_id,
            ctx.volatile_ctx.inflight_subprocedures[0].region_ids[0]
        );
        // The region is only removed after its migration is settled.
        assert_eq!(vec![region_id], leader_regions(&ctx));
        assert!(next.as_any().downcast_ref::<Dispatch>().is_some());
    }

    #[tokio::test]
    async fn test_drain_datanode_dispatch_skips_region_moved_away() {
        let env = TestEnv::new().await;
        let region_id = RegionId::new(1024, 1);
        // The plan lists the region, but the table route says another datanode leads it.
        env.create_table(vec![leader_region(region_id, 2)]).await;

        let mut ctx = env.context(new_persistent_context(
            Some(3),
            DrainDatanodeStrategy::FailFast,
        ));
        seed_plan(&mut ctx, vec![region_id]);
        let procedure_ctx = TestingEnv::procedure_context();

        let (next, status) = dispatch_next(&mut ctx, &procedure_ctx).await.unwrap();

        assert!(matches!(status, Status::Executing { .. }));
        assert!(next.as_any().is::<DrainDatanodeCursor>());
        assert!(leader_regions(&ctx).is_empty());
        assert!(ctx.volatile_ctx.inflight_subprocedures.is_empty());
    }

    /// Changes the route after the drain's first read, during target discovery.
    struct ChangeRouteDuringDiscovery {
        inner: PeerDiscoveryRef,
        metadata: TableMetadataManagerRef,
        region_id: RegionId,
        leader_id: Option<u64>,
    }

    #[async_trait::async_trait]
    impl PeerDiscovery for ChangeRouteDuringDiscovery {
        async fn active_frontends(&self) -> MetaResult<Vec<NodeInfo>> {
            self.inner.active_frontends().await
        }

        async fn active_datanodes(
            &self,
            filter: Option<for<'a> fn(&'a NodeWorkloads) -> bool>,
        ) -> MetaResult<Vec<NodeInfo>> {
            let storage = self.metadata.table_route_manager().table_route_storage();
            let table_id = self.region_id.table_id();
            let current = storage.get_with_raw_bytes(table_id).await?.unwrap();
            if let Some(leader_id) = self.leader_id {
                let updated = current.update(vec![leader_region(self.region_id, leader_id)])?;
                let (txn, _) = storage.build_update_txn(table_id, &current, &updated)?;
                assert!(self.metadata.kv_backend().txn(txn).await?.succeeded);
            } else {
                let table_info = self
                    .metadata
                    .table_info_manager()
                    .get(table_id)
                    .await?
                    .unwrap();
                self.metadata
                    .delete_table_metadata(
                        table_id,
                        &table_info.table_name(),
                        &current,
                        &HashMap::new(),
                        None,
                    )
                    .await?;
            }
            self.inner.active_datanodes(filter).await
        }

        async fn active_flownodes(
            &self,
            filter: Option<for<'a> fn(&'a NodeWorkloads) -> bool>,
        ) -> MetaResult<Vec<NodeInfo>> {
            self.inner.active_flownodes(filter).await
        }
    }

    #[tokio::test]
    async fn test_drop_after_route_read_fail_fast() {
        assert_drop_after_route_read_is_skipped(DrainDatanodeStrategy::FailFast).await;
    }

    #[tokio::test]
    async fn test_drop_after_route_read_try_all() {
        assert_drop_after_route_read_is_skipped(DrainDatanodeStrategy::TryAll).await;
    }

    async fn assert_drop_after_route_read_is_skipped(strategy: DrainDatanodeStrategy) {
        let env = TestEnv::new().await;
        let region_id = RegionId::new(1024, 1);
        env.create_table(vec![leader_region(region_id, 1)]).await;
        let metadata = env.table_metadata_manager.clone();
        // Return the route snapshot even after DROP removes the table. The following
        // table-info batch read must see the deletion rather than abort the drain.
        let route_key = TableRouteKey::new(region_id.table_id()).to_bytes();
        let snapshot = metadata
            .kv_backend()
            .batch_get(BatchGetRequest {
                keys: vec![route_key.clone()],
            })
            .await
            .unwrap();
        assert_eq!(1, snapshot.kvs.len());
        let backend = MockKvBackendBuilder::default()
            .batch_get_fn(Arc::new(move |request| {
                if request.keys.contains(&route_key) {
                    Ok(snapshot.clone())
                } else {
                    Ok(BatchGetResponse { kvs: vec![] })
                }
            }))
            .build()
            .unwrap();
        let factory = env
            .env
            .context_factory_with_metadata(Arc::new(TableMetadataManager::new(Arc::new(backend))));
        let manager = Arc::new(RegionMigrationManager::new(
            env.procedure_manager().clone(),
            factory,
        ));
        let mut ctx = env.context(new_persistent_context(Some(2), strategy));
        ctx.region_migration_manager = manager.clone();
        seed_plan(&mut ctx, vec![region_id]);

        ctx.prepare_migrations().await.unwrap();
        let route = metadata
            .table_route_manager()
            .table_route_storage()
            .get_with_raw_bytes(region_id.table_id())
            .await
            .unwrap()
            .unwrap();
        let info = metadata
            .table_info_manager()
            .get(region_id.table_id())
            .await
            .unwrap()
            .unwrap();
        metadata
            .delete_table_metadata(
                region_id.table_id(),
                &info.table_name(),
                &route,
                &HashMap::new(),
                None,
            )
            .await
            .unwrap();

        let (next, status) = Dispatch
            .next(&mut ctx, &TestingEnv::procedure_context())
            .await
            .unwrap();

        assert!(next.as_any().is::<DrainDatanodeCursor>());
        assert!(matches!(status, Status::Executing { persist: false, .. }));
        assert!(leader_regions(&ctx).is_empty());
        assert!(ctx.volatile_ctx.failed_regions.is_empty());
        assert!(ctx.volatile_ctx.inflight_subprocedures.is_empty());
        assert_eq!(0, ctx.volatile_ctx.migrated);
        assert!(!manager.tracker().contains(region_id));
        assert!(
            env.table_metadata_manager
                .table_info_manager()
                .get(region_id.table_id())
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_drain_datanode_dispatch_skips_region_moved_or_dropped_during_discovery() {
        for strategy in [
            DrainDatanodeStrategy::FailFast,
            DrainDatanodeStrategy::TryAll,
        ] {
            // Target 2 yields RegionMigrated; leader 3 yields LeaderPeerChanged;
            // dropping the table yields TableRouteNotFound.
            for leader_id in [Some(2), Some(3), None] {
                let env = TestEnv::new().await;
                let region_id = RegionId::new(1024, 1);
                env.create_table(vec![leader_region(region_id, 1)]).await;
                let mut ctx = env.context(new_persistent_context(Some(2), strategy));
                ctx.selector_ctx.peer_discovery = Arc::new(ChangeRouteDuringDiscovery {
                    inner: ctx.selector_ctx.peer_discovery.clone(),
                    metadata: env.table_metadata_manager.clone(),
                    region_id,
                    leader_id,
                });
                seed_plan(&mut ctx, vec![region_id]);

                let (next, status) = dispatch_next(&mut ctx, &TestingEnv::procedure_context())
                    .await
                    .unwrap();

                assert!(next.as_any().is::<DrainDatanodeCursor>());
                assert!(matches!(status, Status::Executing { persist: false, .. }));
                assert!(leader_regions(&ctx).is_empty());
                assert!(ctx.volatile_ctx.failed_regions.is_empty());
                assert!(ctx.volatile_ctx.inflight_subprocedures.is_empty());
                assert_eq!(0, ctx.volatile_ctx.migrated);
                assert!(!env.region_migration_manager.tracker().contains(region_id));
            }
        }
    }

    #[tokio::test]
    async fn test_drain_datanode_skips_region_whose_migration_is_running() {
        let env = TestEnv::new().await;
        let region_id = RegionId::new(1024, 1);
        env.create_table(vec![leader_region(region_id, 1)]).await;

        let mut ctx = env.context(new_persistent_context(
            Some(2),
            DrainDatanodeStrategy::FailFast,
        ));
        seed_plan(&mut ctx, vec![region_id]);
        let procedure_ctx = TestingEnv::procedure_context();

        // A manual migration of the same region is already running. Building it keeps the
        // region in the running set of the migration manager until it is dropped.
        let _child = env
            .region_migration_manager
            .build_subprocedure(
                common_procedure::ProcedureContext::default(),
                RegionMigrationProcedureTask::new(
                    region_id,
                    Peer::empty(1),
                    Peer::empty(2),
                    Duration::from_secs(10),
                    RegionMigrationTriggerReason::Manual,
                ),
            )
            .await
            .expect("the manual migration should be buildable");

        let (next, status) = dispatch_next(&mut ctx, &procedure_ctx).await.unwrap();

        // The region is ignored for this round instead of failing the drain: it leaves the plan
        // and is neither migrated nor given up. The rescan re-adds it when the datanode still
        // leads it.
        assert!(matches!(status, Status::Executing { .. }));
        assert!(next.as_any().is::<DrainDatanodeCursor>());
        assert!(leader_regions(&ctx).is_empty());
        assert!(ctx.volatile_ctx.failed_regions.is_empty());
        assert!(ctx.volatile_ctx.inflight_subprocedures.is_empty());
    }

    #[tokio::test]
    async fn test_drain_datanode_fail_fast_on_unschedulable_target() {
        let env = TestEnv::new().await;
        let region_id = RegionId::new(1024, 1);
        env.create_table(vec![leader_region(region_id, 1)]).await;
        env.node_scheduling_manager
            .mark_unschedulable(NodeSchedulingRole::Datanode, 3)
            .await
            .unwrap();

        let mut ctx = env.context(new_persistent_context(
            Some(3),
            DrainDatanodeStrategy::FailFast,
        ));
        let procedure_ctx = TestingEnv::procedure_context();

        let (mut state, _) = DrainDatanodeStart
            .next(&mut ctx, &procedure_ctx)
            .await
            .unwrap();
        let error = run_until_done(&mut state, &mut ctx, &procedure_ctx)
            .await
            .unwrap_err();

        assert!(matches!(error, error::Error::DrainDatanodeAbort { .. }));
        assert!(!error.is_retryable());
        assert_eq!(vec![region_id], leader_regions(&ctx));
    }

    #[tokio::test]
    async fn test_drain_datanode_waits_for_migrated_batch() {
        let env = TestEnv::new().await;
        let region_id = RegionId::new(1024, 1);
        env.create_table(vec![leader_region(region_id, 1)]).await;

        let mut ctx = env.context(new_persistent_context(
            Some(2),
            DrainDatanodeStrategy::FailFast,
        ));
        seed_plan(&mut ctx, vec![region_id]);
        ctx.volatile_ctx.inflight_subprocedures = vec![SubprocedureMeta {
            region_ids: vec![region_id],
            procedure_id: ProcedureId::random(),
        }];
        let procedure_ctx = procedure_context_with_receivers(HashMap::from([(
            ctx.volatile_ctx.inflight_subprocedures[0].procedure_id,
            procedure_state_receiver(ProcedureState::Done { output: None }),
        )]));

        let (mut next, status) = dispatch_next(&mut ctx, &procedure_ctx).await.unwrap();

        assert!(matches!(status, Status::Executing { .. }));
        assert!(next.as_any().is::<DrainDatanodeCursor>());
        assert_eq!(1, ctx.volatile_ctx.migrated);
        assert!(leader_regions(&ctx).is_empty());
        assert!(ctx.volatile_ctx.inflight_subprocedures.is_empty());
        // The cursor immediately prepares another migration for the unchanged route.
        let (next, _) = next.next(&mut ctx, &procedure_ctx).await.unwrap();
        assert!(next.as_any().is::<Dispatch>());
        assert_eq!(vec![region_id], leader_regions(&ctx));
    }

    #[tokio::test]
    async fn test_drain_datanode_exceeded_deadline() {
        let env = TestEnv::new().await;
        let region_id = RegionId::new(1024, 1);
        env.create_table(vec![leader_region(region_id, 1)]).await;

        let mut persistent_ctx = new_persistent_context(Some(2), DrainDatanodeStrategy::FailFast);
        persistent_ctx.deadline_ms = 0;
        let mut ctx = env.context(persistent_ctx);
        seed_plan(&mut ctx, vec![region_id]);
        let procedure_ctx = TestingEnv::procedure_context();

        let error = dispatch_next(&mut ctx, &procedure_ctx).await.unwrap_err();

        assert!(matches!(error, error::Error::ExceededDeadline { .. }));
        assert!(!error.is_retryable());
        assert_eq!(vec![region_id], leader_regions(&ctx));
    }

    #[tokio::test]
    async fn test_drain_datanode_deadline_does_not_interrupt_batch() {
        let env = TestEnv::new().await;
        let region_id = RegionId::new(1024, 1);
        env.create_table(vec![leader_region(region_id, 1)]).await;

        let mut persistent_ctx = new_persistent_context(Some(2), DrainDatanodeStrategy::FailFast);
        persistent_ctx.deadline_ms = 0;
        let mut ctx = env.context(persistent_ctx);
        seed_plan(&mut ctx, vec![region_id]);
        ctx.volatile_ctx.inflight_subprocedures = vec![SubprocedureMeta {
            region_ids: vec![region_id],
            procedure_id: ProcedureId::random(),
        }];
        let procedure_ctx = procedure_context_with_receivers(HashMap::from([(
            ctx.volatile_ctx.inflight_subprocedures[0].procedure_id,
            procedure_state_receiver(ProcedureState::Done { output: None }),
        )]));

        // The in-flight batch is settled before the deadline is checked.
        let (_, status) = dispatch_next(&mut ctx, &procedure_ctx).await.unwrap();
        assert!(matches!(status, Status::Executing { .. }));
        assert_eq!(1, ctx.volatile_ctx.migrated);

        // The next step observes the exceeded deadline.
        let error = dispatch_next(&mut ctx, &procedure_ctx).await.unwrap_err();
        assert!(matches!(error, error::Error::ExceededDeadline { .. }));
    }
    #[tokio::test]
    async fn test_build_failure_releases_children_and_retains_prepared_batches() {
        let env = TestEnv::new().await;
        let first = RegionId::new(1024, 1);
        let second = RegionId::new(1024, 2);
        let mut second_route = leader_region(second, 1);
        second_route.follower_peers.push(Peer::empty(3));
        env.create_table(vec![leader_region(first, 1), second_route])
            .await;
        let mut ctx = env.context(new_persistent_context(
            Some(2),
            DrainDatanodeStrategy::FailFast,
        ));
        seed_plan(&mut ctx, vec![first, second]);
        ctx.prepare_migrations().await.unwrap();
        assert_eq!(2, ctx.volatile_ctx.migrations.len());
        // The first child is built before the second batch encounters a target conflict.
        ctx.volatile_ctx.migrations[1].to_peer = Peer::empty(3);
        let procedure_ctx = TestingEnv::procedure_context();
        let error = Dispatch.next(&mut ctx, &procedure_ctx).await.unwrap_err();
        assert!(matches!(error, error::Error::DrainDatanodeAbort { .. }));
        assert_eq!(2, ctx.volatile_ctx.migrations.len());
        assert_eq!(vec![first, second], leader_regions(&ctx));
        assert!(ctx.volatile_ctx.inflight_subprocedures.is_empty());
        let tracker = env.region_migration_manager.tracker();
        assert!(!tracker.contains(first));
        assert!(!tracker.contains(second));

        ctx.volatile_ctx.migrations[1].to_peer = Peer::empty(2);
        let (_, status) = Dispatch.next(&mut ctx, &procedure_ctx).await.unwrap();
        let Status::Suspended { subprocedures, .. } = status else {
            panic!("expected both retained batches to be submitted");
        };
        assert_eq!(2, subprocedures.len());
        assert!(ctx.volatile_ctx.migrations.is_empty());
        assert!(tracker.contains(first));
        assert!(tracker.contains(second));
        drop(subprocedures);
        assert!(!tracker.contains(first));
        assert!(!tracker.contains(second));
    }
}
