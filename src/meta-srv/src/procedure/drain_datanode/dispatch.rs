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

use common_procedure::{Context as ProcedureContext, Status, SubprocedureRecovery, watcher};
use common_telemetry::{error, info, warn};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use store_api::storage::RegionId;

use crate::error::{self, Result};
use crate::metrics::{METRIC_META_REGION_MIGRATION_DATANODES, METRIC_META_REGION_MIGRATION_FAIL};
use crate::procedure::drain_datanode::check_drained::CheckDrained;
use crate::procedure::drain_datanode::utils;
use crate::procedure::drain_datanode::{Context, DrainDatanodeStrategy, State, SubprocedureMeta};

/// Migrates the leader regions off the drained datanode in batches.
///
/// The state first settles the previous batch, then dispatches the next one. Sub-procedures
/// are tracked in this state, and the suspension is not persisted, so a metasrv
/// restart rebuilds the plan by rescanning the datanode. Recovered migrations run
/// independently and are excluded by the migration tracker; regions that already left
/// the datanode are skipped by the route check in
/// [`Context::build_migration_procedure`].
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub(crate) struct Dispatch {
    /// Dispatched children whose results have not been collected yet.
    #[serde(skip)]
    inflight_subprocedures: Vec<SubprocedureMeta>,
}

impl Dispatch {
    /// Waits for every migration in the batch before applying the drain strategy.
    ///
    /// Keep the batch and its results unchanged until all fallible route checks succeed,
    /// so a metadata retry cannot lose failures or apply progress twice.
    async fn wait_for_inflight_subprocedures(
        &mut self,
        ctx: &mut Context,
        procedure_ctx: &ProcedureContext,
        strategy: DrainDatanodeStrategy,
    ) -> Result<()> {
        let node_id = ctx.persistent_ctx.node_id;
        let mut migrated = Vec::new();
        let mut failed = Vec::new();
        for meta in &self.inflight_subprocedures {
            let receiver = procedure_ctx
                .provider
                .procedure_state_receiver(meta.procedure_id)
                .await
                .inspect_err(|e| {
                    error!(e; "Failed to get state receiver for migration sub-procedure {}", meta.procedure_id);
                })
                .context(error::QueryProcedureSnafu)?;
            let Some(mut receiver) = receiver else {
                warn!(
                    "Migration sub-procedure {} of regions {:?} is unknown, keep the region pending",
                    meta.procedure_id, meta.region_ids
                );
                continue;
            };
            match watcher::wait(&mut receiver).await {
                Ok(_) => migrated.extend(meta.region_ids.iter().copied()),
                Err(e) => {
                    error!(e; "Migration sub-procedure {} of regions {:?} failed", meta.procedure_id, meta.region_ids);
                    failed.extend(meta.region_ids.iter().copied());
                }
            }
        }

        let failed_count = failed.len();
        let mut failed_regions = Vec::new();
        for region_id in failed {
            // A concurrent failover may have achieved the goal despite the child failure.
            if utils::leads_region(&ctx.table_metadata_manager, node_id, region_id).await? {
                failed_regions.push(region_id);
            } else {
                migrated.push(region_id);
            }
        }

        METRIC_META_REGION_MIGRATION_FAIL.inc_by(failed_count as u64);
        for region_id in migrated {
            ctx.forget_leader_region(region_id);
            ctx.volatile_ctx.migrated += 1;
            info!("Region {} left datanode {}", region_id, node_id);
        }
        self.inflight_subprocedures.clear();
        match strategy {
            DrainDatanodeStrategy::FailFast if !failed_regions.is_empty() => {
                error::DrainDatanodeAbortSnafu {
                    node_id,
                    failed_regions,
                }
                .fail()
            }
            DrainDatanodeStrategy::TryAll => {
                for &region_id in &failed_regions {
                    ctx.forget_leader_region(region_id);
                }
                ctx.volatile_ctx.failed_regions.extend(failed_regions);
                Ok(())
            }
            DrainDatanodeStrategy::FailFast => Ok(()),
        }
    }
}

#[async_trait::async_trait]
#[typetag::serde]
impl State for Dispatch {
    async fn next(
        &mut self,
        ctx: &mut Context,
        procedure_ctx: &ProcedureContext,
    ) -> Result<(Box<dyn State>, Status)> {
        let node_id = ctx.persistent_ctx.node_id;

        // Collect the previous batch's results before dispatching more migrations.
        if !self.inflight_subprocedures.is_empty() {
            self.wait_for_inflight_subprocedures(ctx, procedure_ctx, ctx.persistent_ctx.strategy)
                .await?;
            return Ok((Box::new(Dispatch::default()), Status::executing(false)));
        }

        utils::ensure_drain_deadline(ctx)?;

        let parallelism = ctx.persistent_ctx.parallelism;
        let regions: Vec<RegionId> = ctx
            .volatile_ctx
            .leader_regions()
            .iter()
            .take(parallelism)
            .copied()
            .collect();
        if regions.is_empty() {
            return Ok((Box::new(CheckDrained), Status::executing(false)));
        }

        let mut children = Vec::with_capacity(regions.len());
        let mut inflight = Vec::with_capacity(regions.len());
        let mut targets = Vec::with_capacity(regions.len());
        for region_id in regions {
            match ctx
                .build_migration_procedure(procedure_ctx, region_id)
                .await
            {
                Ok(Some((child, target_node_id))) => {
                    info!(
                        "Dispatching migration of region {} off datanode {}, procedure id: {}",
                        region_id, node_id, child.id
                    );
                    inflight.push(SubprocedureMeta {
                        region_ids: vec![region_id],
                        procedure_id: child.id,
                    });
                    targets.push(target_node_id);
                    children.push(child);
                }
                Ok(None) => {
                    info!(
                        "Skipping region {}, it is no longer led by datanode {}",
                        region_id, node_id
                    );
                    ctx.forget_leader_region(region_id);
                }
                Err(error::Error::MigrationRunning { .. }) => {
                    // Another procedure is migrating the region already, for example a manual
                    // migration submitted before this batch. Ignore it for this round; the
                    // rescan adds it back when it is still led by the datanode.
                    warn!(
                        "A migration of region {} off datanode {} is already running, \
                         skipping the region",
                        region_id, node_id
                    );
                    ctx.forget_leader_region(region_id);
                }
                Err(
                    e @ (error::Error::TableRouteNotFound { .. }
                    | error::Error::TableInfoNotFound { .. }),
                ) => {
                    warn!(
                        e;
                        "Skipping region {} while draining datanode {}: table metadata is missing",
                        region_id, node_id
                    );
                    ctx.forget_leader_region(region_id);
                }
                Err(e) => {
                    // Another migration may finish between target selection and the
                    // migration manager's route check, releasing its tracker guard.
                    if matches!(
                        e,
                        error::Error::RegionMigrated { .. }
                            | error::Error::LeaderPeerChanged { .. }
                    ) && !utils::leads_region(&ctx.table_metadata_manager, node_id, region_id)
                        .await?
                    {
                        info!(
                            "Skipping region {}, it is no longer led by datanode {}",
                            region_id, node_id
                        );
                        ctx.forget_leader_region(region_id);
                        continue;
                    }

                    match ctx.persistent_ctx.strategy {
                        // Nothing is committed yet, so the batch can be retried as a whole.
                        DrainDatanodeStrategy::FailFast => return Err(e),
                        DrainDatanodeStrategy::TryAll => {
                            METRIC_META_REGION_MIGRATION_FAIL.inc();
                            error!(
                                e;
                                "Gave up draining region {} off datanode {}",
                                region_id,
                                node_id
                            );
                            ctx.forget_leader_region(region_id);
                            ctx.volatile_ctx.failed_regions.push(region_id);
                        }
                    }
                }
            }
        }
        self.inflight_subprocedures = inflight;

        if children.is_empty() {
            // The whole batch was handled without a sub-procedure, so keep draining the
            // remaining leaders instead of spending a rescan round on them.
            return Ok((Box::new(Dispatch::default()), Status::executing(false)));
        }

        for target_node_id in targets {
            METRIC_META_REGION_MIGRATION_DATANODES
                .with_label_values(&["src", &node_id.to_string()])
                .inc();
            METRIC_META_REGION_MIGRATION_DATANODES
                .with_label_values(&["desc", &target_node_id.to_string()])
                .inc();
        }

        // Recovered children continue independently. The parent rebuilds its plan and
        // consults the migration tracker instead of restoring the batch's child links.
        Ok((
            Box::new(Self {
                inflight_subprocedures: std::mem::take(&mut self.inflight_subprocedures),
            }),
            Status::Suspended {
                subprocedures: children,
                recovery: SubprocedureRecovery::Detach,
                persist: false,
            },
        ))
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
    use common_error::mock::MockError;
    use common_error::status_code::StatusCode;
    use common_meta::cluster::NodeInfo;
    use common_meta::error::Result as MetaResult;
    use common_meta::key::table_route::TableRouteKey;
    use common_meta::key::{MetadataKey, TableMetadataManager, TableMetadataManagerRef};
    use common_meta::kv_backend::test_util::MockKvBackendBuilder;
    use common_meta::peer::{Peer, PeerDiscovery, PeerDiscoveryRef};
    use common_meta::rpc::store::BatchGetResponse;
    use common_procedure::{Error as ProcedureError, ProcedureId, ProcedureState, Status};
    use store_api::storage::RegionId;
    use tokio::sync::watch;

    use crate::error;
    use crate::key::NodeSchedulingRole;
    use crate::procedure::drain_datanode::check_drained::CheckDrained;
    use crate::procedure::drain_datanode::dispatch::Dispatch;
    use crate::procedure::drain_datanode::drain_datanode_start::DrainDatanodeStart;
    use crate::procedure::drain_datanode::test_util::{
        TestEnv, follower_region, leader_region, leader_regions, new_persistent_context,
        run_until_done, seed_plan,
    };
    use crate::procedure::drain_datanode::{DrainDatanodeStrategy, State, SubprocedureMeta};
    use crate::procedure::region_migration::manager::{
        RegionMigrationManager, RegionMigrationProcedureTask, RegionMigrationTriggerReason,
    };
    use crate::procedure::region_migration::test_util::TestingEnv;

    #[tokio::test]
    async fn test_drain_datanode_dispatch_transitions_to_check_drained() {
        let mut dispatch = Dispatch::default();
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
        let (next, status) = dispatch.next(&mut ctx, &procedure_ctx).await.unwrap();

        assert!(next.as_any().downcast_ref::<CheckDrained>().is_some());
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
        let dispatched = next.as_any().downcast_ref::<Dispatch>().unwrap();
        assert_eq!(1, dispatched.inflight_subprocedures.len());
        assert_eq!(
            region_id,
            dispatched.inflight_subprocedures[0].region_ids[0]
        );
        // The region is only removed after its migration is settled.
        assert_eq!(vec![region_id], leader_regions(&ctx));
        assert!(next.as_any().downcast_ref::<Dispatch>().is_some());
    }

    #[tokio::test]
    async fn test_drain_datanode_dispatch_skips_region_moved_away() {
        let mut dispatch = Dispatch::default();
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

        let (next, status) = dispatch.next(&mut ctx, &procedure_ctx).await.unwrap();

        assert!(matches!(status, Status::Executing { .. }));
        assert!(next.as_any().downcast_ref::<Dispatch>().is_some());
        assert!(leader_regions(&ctx).is_empty());
        assert!(dispatch.inflight_subprocedures.is_empty());
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
        let mut dispatch = Dispatch::default();
        let env = TestEnv::new().await;
        let region_id = RegionId::new(1024, 1);
        env.create_table(vec![leader_region(region_id, 1)]).await;
        let metadata = env.table_metadata_manager.clone();
        // Only intercept the migration manager's reads. Return its route snapshot
        // after deleting the table, so its following table-info read finds nothing.
        let backend = MockKvBackendBuilder::default()
            // This fixture has no node-address metadata to remap the route.
            .batch_get_fn(Arc::new(|_| Ok(BatchGetResponse { kvs: vec![] })))
            .range_async_fn(Arc::new(move |request| {
                let metadata = metadata.clone();
                Box::pin(async move {
                    let response = metadata.kv_backend().range(request.clone()).await?;
                    if request.key == TableRouteKey::new(region_id.table_id()).to_bytes() {
                        let route = metadata
                            .table_route_manager()
                            .table_route_storage()
                            .get_with_raw_bytes(region_id.table_id())
                            .await?
                            .unwrap();
                        let info = metadata
                            .table_info_manager()
                            .get(region_id.table_id())
                            .await?
                            .unwrap();
                        metadata
                            .delete_table_metadata(
                                region_id.table_id(),
                                &info.table_name(),
                                &route,
                                &HashMap::new(),
                                None,
                            )
                            .await?;
                    }
                    Ok(response)
                })
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

        let (next, status) = dispatch
            .next(&mut ctx, &TestingEnv::procedure_context())
            .await
            .unwrap();

        assert!(next.as_any().is::<Dispatch>());
        assert!(matches!(status, Status::Executing { persist: false, .. }));
        assert!(leader_regions(&ctx).is_empty());
        assert!(ctx.volatile_ctx.failed_regions.is_empty());
        assert!(dispatch.inflight_subprocedures.is_empty());
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
        let mut dispatch = Dispatch::default();
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

                let (next, status) = dispatch
                    .next(&mut ctx, &TestingEnv::procedure_context())
                    .await
                    .unwrap();

                assert!(next.as_any().is::<Dispatch>());
                assert!(matches!(status, Status::Executing { persist: false, .. }));
                assert!(leader_regions(&ctx).is_empty());
                assert!(ctx.volatile_ctx.failed_regions.is_empty());
                assert!(dispatch.inflight_subprocedures.is_empty());
                assert_eq!(0, ctx.volatile_ctx.migrated);
                assert!(!env.region_migration_manager.tracker().contains(region_id));
            }
        }
    }

    #[tokio::test]
    async fn test_drain_datanode_skips_region_whose_migration_is_running() {
        let mut dispatch = Dispatch::default();
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

        let (next, status) = dispatch.next(&mut ctx, &procedure_ctx).await.unwrap();

        // The region is ignored for this round instead of failing the drain: it leaves the plan
        // and is neither migrated nor given up. The rescan re-adds it when the datanode still
        // leads it.
        assert!(matches!(status, Status::Executing { .. }));
        assert!(next.as_any().downcast_ref::<Dispatch>().is_some());
        assert!(leader_regions(&ctx).is_empty());
        assert!(ctx.volatile_ctx.failed_regions.is_empty());
        assert!(dispatch.inflight_subprocedures.is_empty());
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
        let mut dispatch = Dispatch::default();
        let env = TestEnv::new().await;
        let region_id = RegionId::new(1024, 1);
        env.create_table(vec![leader_region(region_id, 1)]).await;

        let mut ctx = env.context(new_persistent_context(
            Some(2),
            DrainDatanodeStrategy::FailFast,
        ));
        seed_plan(&mut ctx, vec![region_id]);
        dispatch.inflight_subprocedures = vec![SubprocedureMeta {
            region_ids: vec![region_id],
            procedure_id: ProcedureId::random(),
        }];
        let procedure_ctx = procedure_context_with_receivers(HashMap::from([(
            dispatch.inflight_subprocedures[0].procedure_id,
            procedure_state_receiver(ProcedureState::Done { output: None }),
        )]));

        let (mut next, status) = dispatch.next(&mut ctx, &procedure_ctx).await.unwrap();

        assert!(matches!(status, Status::Executing { .. }));
        assert!(next.as_any().downcast_ref::<Dispatch>().is_some());
        assert_eq!(1, ctx.volatile_ctx.migrated);
        assert!(leader_regions(&ctx).is_empty());
        assert!(dispatch.inflight_subprocedures.is_empty());
        // The next step checks the remaining regions.
        let (next, _) = next.next(&mut ctx, &procedure_ctx).await.unwrap();
        assert!(next.as_any().downcast_ref::<CheckDrained>().is_some());
    }

    #[tokio::test]
    async fn test_drain_datanode_exceeded_deadline() {
        let mut dispatch = Dispatch::default();
        let env = TestEnv::new().await;
        let region_id = RegionId::new(1024, 1);
        env.create_table(vec![leader_region(region_id, 1)]).await;

        let mut persistent_ctx = new_persistent_context(Some(2), DrainDatanodeStrategy::FailFast);
        persistent_ctx.deadline_ms = 0;
        let mut ctx = env.context(persistent_ctx);
        seed_plan(&mut ctx, vec![region_id]);
        let procedure_ctx = TestingEnv::procedure_context();

        let error = dispatch.next(&mut ctx, &procedure_ctx).await.unwrap_err();

        assert!(matches!(error, error::Error::ExceededDeadline { .. }));
        assert!(!error.is_retryable());
        assert_eq!(vec![region_id], leader_regions(&ctx));
    }

    #[tokio::test]
    async fn test_drain_datanode_deadline_does_not_interrupt_batch() {
        let mut dispatch = Dispatch::default();
        let env = TestEnv::new().await;
        let region_id = RegionId::new(1024, 1);
        env.create_table(vec![leader_region(region_id, 1)]).await;

        let mut persistent_ctx = new_persistent_context(Some(2), DrainDatanodeStrategy::FailFast);
        persistent_ctx.deadline_ms = 0;
        let mut ctx = env.context(persistent_ctx);
        seed_plan(&mut ctx, vec![region_id]);
        dispatch.inflight_subprocedures = vec![SubprocedureMeta {
            region_ids: vec![region_id],
            procedure_id: ProcedureId::random(),
        }];
        let procedure_ctx = procedure_context_with_receivers(HashMap::from([(
            dispatch.inflight_subprocedures[0].procedure_id,
            procedure_state_receiver(ProcedureState::Done { output: None }),
        )]));

        // The in-flight batch is settled before the deadline is checked.
        let (_, status) = dispatch.next(&mut ctx, &procedure_ctx).await.unwrap();
        assert!(matches!(status, Status::Executing { .. }));
        assert_eq!(1, ctx.volatile_ctx.migrated);

        // The next step observes the exceeded deadline.
        let error = dispatch.next(&mut ctx, &procedure_ctx).await.unwrap_err();
        assert!(matches!(error, error::Error::ExceededDeadline { .. }));
    }
    #[tokio::test]
    async fn test_drain_datanode_wait_failed_child_fail_fast() {
        let mut dispatch = Dispatch::default();
        let env = TestEnv::new().await;
        let region_id = RegionId::new(1024, 1);
        env.create_table(vec![leader_region(region_id, 1)]).await;

        let mut ctx = env.context(new_persistent_context(
            Some(2),
            DrainDatanodeStrategy::FailFast,
        ));
        seed_plan(&mut ctx, vec![region_id]);
        dispatch.inflight_subprocedures = vec![SubprocedureMeta {
            region_ids: vec![region_id],
            procedure_id: ProcedureId::random(),
        }];
        let procedure_ctx = procedure_context_with_receivers(HashMap::from([(
            dispatch.inflight_subprocedures[0].procedure_id,
            procedure_state_receiver(ProcedureState::failed(Arc::new(ProcedureError::external(
                MockError::new(StatusCode::Internal),
            )))),
        )]));

        let error = dispatch
            .wait_for_inflight_subprocedures(
                &mut ctx,
                &procedure_ctx,
                DrainDatanodeStrategy::FailFast,
            )
            .await
            .unwrap_err();

        // The whole batch is settled before the procedure aborts.
        assert!(matches!(error, error::Error::DrainDatanodeAbort { .. }));
        assert_eq!(0, ctx.volatile_ctx.migrated);
        assert_eq!(vec![region_id], leader_regions(&ctx));
        assert!(dispatch.inflight_subprocedures.is_empty());
    }

    #[tokio::test]
    async fn test_drain_datanode_wait_failed_child_try_all() {
        let mut dispatch = Dispatch::default();
        let env = TestEnv::new().await;
        let region_id = RegionId::new(1024, 1);
        env.create_table(vec![leader_region(region_id, 1)]).await;

        let mut ctx = env.context(new_persistent_context(
            Some(2),
            DrainDatanodeStrategy::FailFast,
        ));
        seed_plan(&mut ctx, vec![region_id]);
        dispatch.inflight_subprocedures = vec![SubprocedureMeta {
            region_ids: vec![region_id],
            procedure_id: ProcedureId::random(),
        }];
        let procedure_ctx = procedure_context_with_receivers(HashMap::from([(
            dispatch.inflight_subprocedures[0].procedure_id,
            procedure_state_receiver(ProcedureState::failed(Arc::new(ProcedureError::external(
                MockError::new(StatusCode::Internal),
            )))),
        )]));

        dispatch
            .wait_for_inflight_subprocedures(
                &mut ctx,
                &procedure_ctx,
                DrainDatanodeStrategy::TryAll,
            )
            .await
            .unwrap();

        assert_eq!(vec![region_id], ctx.volatile_ctx.failed_regions);
        assert_eq!(0, ctx.volatile_ctx.migrated);
        assert!(leader_regions(&ctx).is_empty());
    }

    #[tokio::test]
    async fn test_drain_datanode_wait_failed_child_counts_migrated_when_moved() {
        let mut dispatch = Dispatch::default();
        let env = TestEnv::new().await;
        let region_id = RegionId::new(1024, 1);
        // The route reports another datanode as the leader, which is what a failover that
        // raced with the migration leaves behind.
        env.create_table(vec![leader_region(region_id, 2)]).await;

        let mut ctx = env.context(new_persistent_context(
            Some(2),
            DrainDatanodeStrategy::FailFast,
        ));
        seed_plan(&mut ctx, vec![region_id]);
        dispatch.inflight_subprocedures = vec![SubprocedureMeta {
            region_ids: vec![region_id],
            procedure_id: ProcedureId::random(),
        }];
        let procedure_ctx = procedure_context_with_receivers(HashMap::from([(
            dispatch.inflight_subprocedures[0].procedure_id,
            procedure_state_receiver(ProcedureState::failed(Arc::new(ProcedureError::external(
                MockError::new(StatusCode::Internal),
            )))),
        )]));

        dispatch
            .wait_for_inflight_subprocedures(
                &mut ctx,
                &procedure_ctx,
                DrainDatanodeStrategy::FailFast,
            )
            .await
            .unwrap();

        assert_eq!(1, ctx.volatile_ctx.migrated);
        assert!(ctx.volatile_ctx.failed_regions.is_empty());
        assert!(leader_regions(&ctx).is_empty());
    }

    #[tokio::test]
    async fn test_drain_datanode_wait_unknown_child_keeps_pending() {
        let mut dispatch = Dispatch::default();
        let env = TestEnv::new().await;
        let region_id = RegionId::new(1024, 1);
        env.create_table(vec![leader_region(region_id, 1)]).await;

        let mut ctx = env.context(new_persistent_context(
            Some(2),
            DrainDatanodeStrategy::FailFast,
        ));
        seed_plan(&mut ctx, vec![region_id]);
        dispatch.inflight_subprocedures = vec![SubprocedureMeta {
            region_ids: vec![region_id],
            procedure_id: ProcedureId::random(),
        }];
        let procedure_ctx = procedure_context_with_receivers(HashMap::new());

        dispatch
            .wait_for_inflight_subprocedures(
                &mut ctx,
                &procedure_ctx,
                DrainDatanodeStrategy::FailFast,
            )
            .await
            .unwrap();

        // The region stays pending and is dispatched again instead of being reported as
        // failed.
        assert_eq!(vec![region_id], leader_regions(&ctx));
        assert_eq!(0, ctx.volatile_ctx.migrated);
        assert!(ctx.volatile_ctx.failed_regions.is_empty());
    }

    #[tokio::test]
    async fn test_drain_datanode_waits_for_entire_batch() {
        let mut dispatch = Dispatch::default();
        for strategy in [
            DrainDatanodeStrategy::FailFast,
            DrainDatanodeStrategy::TryAll,
        ] {
            let env = TestEnv::new().await;
            let failed_region = RegionId::new(1024, 1);
            let succeeded_region = RegionId::new(1024, 2);
            env.create_table(vec![
                leader_region(failed_region, 1),
                leader_region(succeeded_region, 1),
            ])
            .await;
            let mut ctx = env.context(new_persistent_context(
                Some(2),
                DrainDatanodeStrategy::FailFast,
            ));
            seed_plan(&mut ctx, vec![failed_region, succeeded_region]);
            let failed_id = ProcedureId::random();
            let pending_id = ProcedureId::random();
            dispatch.inflight_subprocedures = vec![
                SubprocedureMeta {
                    region_ids: vec![failed_region],
                    procedure_id: failed_id,
                },
                SubprocedureMeta {
                    region_ids: vec![succeeded_region],
                    procedure_id: pending_id,
                },
            ];
            let (sender, receiver) = watch::channel(ProcedureState::Running);
            let procedure_ctx = procedure_context_with_receivers(HashMap::from([
                (
                    failed_id,
                    procedure_state_receiver(ProcedureState::failed(Arc::new(
                        ProcedureError::external(MockError::new(StatusCode::Internal)),
                    ))),
                ),
                (pending_id, receiver),
            ]));
            let mut waiting = Box::pin(dispatch.wait_for_inflight_subprocedures(
                &mut ctx,
                &procedure_ctx,
                strategy,
            ));
            assert!(futures::poll!(&mut waiting).is_pending());
            sender.send(ProcedureState::Done { output: None }).unwrap();
            let result = waiting.await;
            match strategy {
                DrainDatanodeStrategy::FailFast => assert!(
                    matches!(result, Err(error::Error::DrainDatanodeAbort { failed_regions, .. }) if failed_regions == vec![failed_region])
                ),
                DrainDatanodeStrategy::TryAll => {
                    result.unwrap();
                    assert_eq!(vec![failed_region], ctx.volatile_ctx.failed_regions);
                }
            }
            assert_eq!(1, ctx.volatile_ctx.migrated);
            assert!(dispatch.inflight_subprocedures.is_empty());
        }
    }

    #[tokio::test]
    async fn test_drain_datanode_wait_preserves_batch_on_metadata_retry() {
        let mut dispatch = Dispatch::default();
        use common_meta::key::TableMetadataManager;
        use common_meta::kv_backend::test_util::MockKvBackendBuilder;

        for strategy in [
            DrainDatanodeStrategy::FailFast,
            DrainDatanodeStrategy::TryAll,
        ] {
            let env = TestEnv::new().await;
            let failed_region = RegionId::new(1024, 1);
            let succeeded_region = RegionId::new(1024, 2);
            env.create_table(vec![
                leader_region(failed_region, 1),
                leader_region(succeeded_region, 1),
            ])
            .await;
            let mut ctx = env.context(new_persistent_context(
                Some(2),
                DrainDatanodeStrategy::FailFast,
            ));
            seed_plan(&mut ctx, vec![failed_region, succeeded_region]);
            let failed_id = ProcedureId::random();
            let succeeded_id = ProcedureId::random();
            dispatch.inflight_subprocedures = vec![
                SubprocedureMeta {
                    region_ids: vec![succeeded_region],
                    procedure_id: succeeded_id,
                },
                SubprocedureMeta {
                    region_ids: vec![failed_region],
                    procedure_id: failed_id,
                },
            ];
            let procedure_ctx = procedure_context_with_receivers(HashMap::from([
                (
                    succeeded_id,
                    procedure_state_receiver(ProcedureState::Done { output: None }),
                ),
                (
                    failed_id,
                    procedure_state_receiver(ProcedureState::failed(Arc::new(
                        ProcedureError::external(MockError::new(StatusCode::Internal)),
                    ))),
                ),
            ]));
            ctx.table_metadata_manager = Arc::new(TableMetadataManager::new(Arc::new(
                MockKvBackendBuilder::default()
                    .range_fn(Arc::new(|_| {
                        common_meta::error::ElectionNoLeaderSnafu.fail()
                    }))
                    .build()
                    .unwrap(),
            )));
            let error = dispatch
                .wait_for_inflight_subprocedures(&mut ctx, &procedure_ctx, strategy)
                .await
                .unwrap_err();
            assert!(error.is_retryable());
            assert_eq!(2, dispatch.inflight_subprocedures.len());
            assert_eq!(0, ctx.volatile_ctx.migrated);
            assert!(ctx.volatile_ctx.failed_regions.is_empty());
            assert_eq!(vec![failed_region, succeeded_region], leader_regions(&ctx));

            ctx.table_metadata_manager = env.table_metadata_manager().clone();
            let result = dispatch
                .wait_for_inflight_subprocedures(&mut ctx, &procedure_ctx, strategy)
                .await;
            match strategy {
                DrainDatanodeStrategy::FailFast => assert!(
                    matches!(result, Err(error::Error::DrainDatanodeAbort { failed_regions, .. }) if failed_regions == vec![failed_region])
                ),
                DrainDatanodeStrategy::TryAll => {
                    result.unwrap();
                    assert_eq!(vec![failed_region], ctx.volatile_ctx.failed_regions);
                }
            }
            assert_eq!(1, ctx.volatile_ctx.migrated);
            assert!(dispatch.inflight_subprocedures.is_empty());
        }
    }
}
