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

use common_procedure::{
    Context as ProcedureContext, ProcedureContext as SubmissionContext, ProcedureWithId, Status,
    SubprocedureRecovery, watcher,
};
use common_telemetry::{error, info, warn};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use store_api::storage::RegionId;

use crate::error::{self, Result};
use crate::metrics::{METRIC_META_REGION_MIGRATION_DATANODES, METRIC_META_REGION_MIGRATION_FAIL};
use crate::procedure::drain_datanode::cursor::DrainDatanodeCursor;
use crate::procedure::drain_datanode::{
    Context, DrainDatanodeStrategy, State, SubprocedureMeta, utils,
};
use crate::procedure::region_migration::utils::RegionMigrationTaskBatch;

/// Submits the cursor's prepared batches as detached children and collects their results.
/// Returns to the cursor after each round; recovered children retain their own lifecycle.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Dispatch {
    /// Prepared batches retained until all children have been built successfully.
    #[serde(skip)]
    pub(crate) batches: Vec<RegionMigrationTaskBatch>,
    /// Dispatched children whose results have not been collected yet.
    #[serde(skip)]
    inflight_subprocedures: Vec<SubprocedureMeta>,
}

impl Dispatch {
    /// Creates a dispatch state for the batches prepared by the cursor.
    pub(crate) fn new(batches: Vec<RegionMigrationTaskBatch>) -> Self {
        Self {
            batches,
            inflight_subprocedures: vec![],
        }
    }

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
                ctx.volatile_ctx.failed_regions.extend(failed_regions);
                Ok(())
            }
            DrainDatanodeStrategy::FailFast => Ok(()),
        }
    }

    /// Rechecks ownership before recording a failed batch build.
    async fn collect_failed_regions(
        ctx: &mut Context,
        region_ids: &[RegionId],
    ) -> Result<Vec<RegionId>> {
        let mut failed = Vec::new();
        for &region_id in region_ids {
            if ctx.region_migration_manager.tracker().contains(region_id) {
                continue;
            }
            if utils::leads_region(
                &ctx.table_metadata_manager,
                ctx.persistent_ctx.node_id,
                region_id,
            )
            .await?
            {
                failed.push(region_id);
            }
        }
        Ok(failed)
    }

    /// Builds detached children from batches already selected by the cursor.
    async fn dispatch_migrations(
        &mut self,
        ctx: &mut Context,
        procedure_ctx: &ProcedureContext,
    ) -> Result<Vec<ProcedureWithId>> {
        let node_id = ctx.persistent_ctx.node_id;
        let mut children = Vec::new();
        let mut inflight = Vec::new();
        let mut targets = Vec::new();
        let mut failures = Vec::new();
        // Keep the prepared batches until construction succeeds, so retries do not
        // consume work from the metadata cursor twice.
        for task in self.batches.clone() {
            let target_node_id = task.to_peer.id;
            let region_ids = task.region_ids.clone();
            let submission_context = match procedure_ctx.event_context.clone() {
                Some(event_context) => SubmissionContext::from_event_context(event_context),
                None => SubmissionContext::default(),
            };
            match ctx
                .region_migration_manager
                .build_batch_procedure(submission_context, task)
                .await
            {
                Ok(batch) => {
                    let result = batch.analysis;
                    if !result.peer_conflict.is_empty() {
                        let err = error::DrainDatanodeAbortSnafu {
                            node_id,
                            failed_regions: result.peer_conflict.clone(),
                        }
                        .build();
                        if ctx.persistent_ctx.strategy == DrainDatanodeStrategy::FailFast {
                            return Err(err);
                        }
                        failures.push((err, result.peer_conflict));
                    }
                    if !result.table_not_found.is_empty() {
                        warn!(
                            "Skipping dropped regions while draining datanode {}: {:?}",
                            node_id, result.table_not_found
                        );
                    }
                    if let Some(child) = batch.procedure {
                        targets.push((target_node_id, result.pending.len()));
                        inflight.push(SubprocedureMeta {
                            region_ids: result.pending,
                            procedure_id: child.id,
                        });
                        children.push(child);
                    }
                }
                Err(e) => {
                    if ctx.persistent_ctx.strategy == DrainDatanodeStrategy::FailFast {
                        return Err(e);
                    }
                    let failed = Self::collect_failed_regions(ctx, &region_ids).await?;
                    if !failed.is_empty() {
                        failures.push((e, failed));
                    }
                }
            }
        }
        // Reconciliation can fail; publish failures only after every batch is checked.
        for (error, regions) in failures {
            ctx.handle_migration_error(error, &regions)?;
        }
        self.batches.clear();
        self.inflight_subprocedures = inflight;
        for (target, count) in targets {
            METRIC_META_REGION_MIGRATION_DATANODES
                .with_label_values(&["src", &node_id.to_string()])
                .inc_by(count as u64);
            METRIC_META_REGION_MIGRATION_DATANODES
                .with_label_values(&["desc", &target.to_string()])
                .inc_by(count as u64);
        }
        Ok(children)
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
        if !self.inflight_subprocedures.is_empty() {
            self.wait_for_inflight_subprocedures(ctx, procedure_ctx, ctx.persistent_ctx.strategy)
                .await?;
            return Ok((
                Box::new(DrainDatanodeCursor::default()),
                Status::executing(false),
            ));
        }
        utils::ensure_drain_deadline(ctx)?;
        let children = self.dispatch_migrations(ctx, procedure_ctx).await?;
        if children.is_empty() {
            return Ok((
                Box::new(DrainDatanodeCursor::default()),
                Status::executing(false),
            ));
        }
        Ok((
            Box::new(Self {
                batches: vec![],
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
    use common_error::mock::MockError;
    use common_error::status_code::StatusCode;
    use common_meta::key::table_route::TableRouteKey;
    use common_meta::key::{MetadataKey, TableMetadataManager};
    use common_meta::kv_backend::test_util::MockKvBackendBuilder;
    use common_meta::peer::Peer;
    use common_meta::rpc::store::{BatchGetRequest, BatchGetResponse};
    use common_procedure::{
        Error as ProcedureError, ProcedureId, ProcedureState, Status, SubprocedureRecovery,
    };
    use store_api::storage::RegionId;
    use tokio::sync::watch;

    use crate::error;
    use crate::procedure::drain_datanode::cursor::DrainDatanodeCursor;
    use crate::procedure::drain_datanode::dispatch::Dispatch;
    use crate::procedure::drain_datanode::test_util::create_kafka_table;
    use crate::procedure::drain_datanode::test_util::{
        TestEnv, follower_region, leader_region, new_persistent_context,
    };
    use crate::procedure::drain_datanode::{DrainDatanodeStrategy, State, SubprocedureMeta};
    use crate::procedure::region_migration::manager::{
        RegionMigrationManager, RegionMigrationTriggerReason,
    };
    use crate::procedure::region_migration::test_util::TestingEnv;
    use crate::procedure::region_migration::utils::RegionMigrationTaskBatch;

    #[tokio::test]
    async fn test_drain_datanode_dispatch_transitions_to_cursor() {
        let env = TestEnv::new().await;
        let mut dispatch = Dispatch::new(vec![]);
        // Only a follower region: there is no leader region to migrate.
        env.create_table(vec![follower_region(RegionId::new(1024, 1), 2, 1)])
            .await;

        let mut ctx = env.context(new_persistent_context(
            Some(2),
            DrainDatanodeStrategy::FailFast,
        ));
        let procedure_ctx = TestingEnv::procedure_context();

        let (next, status) = dispatch.next(&mut ctx, &procedure_ctx).await.unwrap();

        assert!(
            next.as_any()
                .downcast_ref::<DrainDatanodeCursor>()
                .is_some()
        );
        assert!(matches!(status, Status::Executing { persist: false, .. }));
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

        let mut dispatch = Dispatch::new(vec![RegionMigrationTaskBatch {
            region_ids: vec![region_id],
            from_peer: Peer::empty(1),
            to_peer: Peer::empty(2),
            timeout: Duration::from_secs(10),
            trigger_reason: RegionMigrationTriggerReason::Manual,
        }]);
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

        let (next, status) = dispatch
            .next(&mut ctx, &TestingEnv::procedure_context())
            .await
            .unwrap();

        assert!(next.as_any().is::<DrainDatanodeCursor>());
        assert!(matches!(status, Status::Executing { persist: false, .. }));
        assert!(dispatch.inflight_subprocedures.is_empty());
        assert!(ctx.volatile_ctx.failed_regions.is_empty());
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
    async fn test_drain_datanode_waits_for_migrated_batch() {
        let env = TestEnv::new().await;
        let mut dispatch = Dispatch::new(vec![]);
        let region_id = RegionId::new(1024, 1);
        env.create_table(vec![leader_region(region_id, 1)]).await;

        let mut ctx = env.context(new_persistent_context(
            Some(2),
            DrainDatanodeStrategy::FailFast,
        ));

        dispatch.inflight_subprocedures = vec![SubprocedureMeta {
            region_ids: vec![region_id],
            procedure_id: ProcedureId::random(),
        }];
        let procedure_ctx = procedure_context_with_receivers(HashMap::from([(
            dispatch.inflight_subprocedures[0].procedure_id,
            procedure_state_receiver(ProcedureState::Done { output: None }),
        )]));

        let (next, status) = dispatch.next(&mut ctx, &procedure_ctx).await.unwrap();

        assert!(matches!(status, Status::Executing { .. }));
        assert!(next.as_any().is::<DrainDatanodeCursor>());
        assert_eq!(1, ctx.volatile_ctx.migrated);
        assert!(dispatch.inflight_subprocedures.is_empty());
    }

    #[tokio::test]
    async fn test_drain_datanode_exceeded_deadline() {
        let env = TestEnv::new().await;
        let region_id = RegionId::new(1024, 1);
        let mut dispatch = Dispatch::new(vec![RegionMigrationTaskBatch {
            region_ids: vec![region_id],
            from_peer: Peer::empty(1),
            to_peer: Peer::empty(2),
            timeout: Duration::from_secs(10),
            trigger_reason: RegionMigrationTriggerReason::Manual,
        }]);
        env.create_table(vec![leader_region(region_id, 1)]).await;

        let mut persistent_ctx = new_persistent_context(Some(2), DrainDatanodeStrategy::FailFast);
        persistent_ctx.deadline_ms = 0;
        let mut ctx = env.context(persistent_ctx);

        let procedure_ctx = TestingEnv::procedure_context();

        let error = dispatch.next(&mut ctx, &procedure_ctx).await.unwrap_err();

        assert!(matches!(error, error::Error::ExceededDeadline { .. }));
        assert!(!error.is_retryable());
        assert_eq!(1, dispatch.batches.len());
        assert_eq!(vec![region_id], dispatch.batches[0].region_ids);
        assert!(!env.region_migration_manager.tracker().contains(region_id));
    }

    #[tokio::test]
    async fn test_drain_datanode_deadline_does_not_interrupt_batch() {
        let env = TestEnv::new().await;
        let mut dispatch = Dispatch::new(vec![]);
        let region_id = RegionId::new(1024, 1);
        env.create_table(vec![leader_region(region_id, 1)]).await;

        let mut persistent_ctx = new_persistent_context(Some(2), DrainDatanodeStrategy::FailFast);
        persistent_ctx.deadline_ms = 0;
        let mut ctx = env.context(persistent_ctx);

        dispatch.inflight_subprocedures = vec![SubprocedureMeta {
            region_ids: vec![region_id],
            procedure_id: ProcedureId::random(),
        }];
        let procedure_ctx = procedure_context_with_receivers(HashMap::from([(
            dispatch.inflight_subprocedures[0].procedure_id,
            procedure_state_receiver(ProcedureState::Done { output: None }),
        )]));

        // The in-flight batch is settled before the deadline is checked.
        let (mut next, status) = dispatch.next(&mut ctx, &procedure_ctx).await.unwrap();
        assert!(matches!(status, Status::Executing { .. }));
        assert_eq!(1, ctx.volatile_ctx.migrated);

        // The next step observes the exceeded deadline.
        let error = next.next(&mut ctx, &procedure_ctx).await.unwrap_err();
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

        let batches = vec![
            RegionMigrationTaskBatch {
                region_ids: vec![first],
                from_peer: Peer::empty(1),
                to_peer: Peer::empty(2),
                timeout: Duration::from_secs(10),
                trigger_reason: RegionMigrationTriggerReason::Manual,
            },
            RegionMigrationTaskBatch {
                region_ids: vec![second],
                from_peer: Peer::empty(1),
                to_peer: Peer::empty(2),
                timeout: Duration::from_secs(10),
                trigger_reason: RegionMigrationTriggerReason::Manual,
            },
        ];
        let mut dispatch = Dispatch::new(batches);
        assert_eq!(2, dispatch.batches.len());
        // The first child is built before the second batch encounters a target conflict.
        dispatch.batches[1].to_peer = Peer::empty(3);
        let procedure_ctx = TestingEnv::procedure_context();
        let error = dispatch.next(&mut ctx, &procedure_ctx).await.unwrap_err();
        assert!(matches!(error, error::Error::DrainDatanodeAbort { .. }));
        assert_eq!(2, dispatch.batches.len());
        assert_eq!(
            vec![first, second],
            dispatch
                .batches
                .iter()
                .flat_map(|batch| batch.region_ids.iter().copied())
                .collect::<Vec<_>>()
        );
        let tracker = env.region_migration_manager.tracker();
        assert!(!tracker.contains(first));
        assert!(!tracker.contains(second));

        dispatch.batches[1].to_peer = Peer::empty(2);
        let (_, status) = dispatch.next(&mut ctx, &procedure_ctx).await.unwrap();
        let Status::Suspended { subprocedures, .. } = status else {
            panic!("expected both retained batches to be submitted");
        };
        assert_eq!(2, subprocedures.len());
        assert!(dispatch.batches.is_empty());
        assert!(tracker.contains(first));
        assert!(tracker.contains(second));
        drop(subprocedures);
        assert!(!tracker.contains(first));
        assert!(!tracker.contains(second));
    }

    #[tokio::test]
    async fn test_build_failure_reconciles_each_region() {
        use crate::procedure::region_migration::manager::RegionMigrationProcedureTask;

        for strategy in [
            DrainDatanodeStrategy::FailFast,
            DrainDatanodeStrategy::TryAll,
        ] {
            for include_pending in [false, true] {
                let env = TestEnv::new().await;
                let pending = RegionId::new(1024, 1);
                let moved = RegionId::new(1024, 2);
                let running = RegionId::new(1024, 3);
                let removed = RegionId::new(2048, 1);
                env.create_table(vec![
                    leader_region(pending, 1),
                    leader_region(moved, 2),
                    leader_region(running, 1),
                ])
                .await;
                let backend = MockKvBackendBuilder::default()
                    .batch_get_fn(Arc::new(|_| {
                        common_meta::error::ElectionNoLeaderSnafu.fail()
                    }))
                    .build()
                    .unwrap();
                let factory =
                    env.env
                        .context_factory_with_metadata(Arc::new(TableMetadataManager::new(
                            Arc::new(backend),
                        )));
                let manager = Arc::new(RegionMigrationManager::new(
                    env.procedure_manager().clone(),
                    factory,
                ));
                let guard = manager
                    .tracker()
                    .insert_running_procedure(&RegionMigrationProcedureTask::new(
                        running,
                        Peer::empty(1),
                        Peer::empty(2),
                        Duration::from_secs(10),
                        RegionMigrationTriggerReason::Manual,
                    ))
                    .unwrap();
                let mut regions = vec![moved, removed, running];
                if include_pending {
                    regions.push(pending);
                }
                let mut dispatch = Dispatch::new(vec![RegionMigrationTaskBatch {
                    region_ids: regions,
                    from_peer: Peer::empty(1),
                    to_peer: Peer::empty(2),
                    timeout: Duration::from_secs(10),
                    trigger_reason: RegionMigrationTriggerReason::Manual,
                }]);
                let mut ctx = env.context(new_persistent_context(Some(2), strategy));
                ctx.region_migration_manager = manager.clone();
                let result = dispatch
                    .next(&mut ctx, &TestingEnv::procedure_context())
                    .await;
                if strategy == DrainDatanodeStrategy::FailFast {
                    assert!(matches!(
                        result.unwrap_err(),
                        error::Error::TableMetadataManager { .. }
                    ));
                    assert_eq!(1, dispatch.batches.len());
                    assert!(ctx.volatile_ctx.failed_regions.is_empty());
                } else {
                    let (next, status) = result.unwrap();
                    assert!(next.as_any().is::<DrainDatanodeCursor>());
                    assert!(matches!(status, Status::Executing { .. }));
                    assert!(dispatch.batches.is_empty());
                    assert_eq!(
                        if include_pending {
                            vec![pending]
                        } else {
                            vec![]
                        },
                        ctx.volatile_ctx.failed_regions
                    );
                }
                assert_eq!(0, ctx.volatile_ctx.migrated);
                assert!(!manager.tracker().contains(pending));
                assert!(!manager.tracker().contains(moved));
                assert!(!manager.tracker().contains(removed));
                assert!(manager.tracker().contains(running));
                drop(guard);
            }
        }
    }

    #[tokio::test]
    async fn test_build_reconciliation_retry_preserves_batches_and_failures() {
        use common_meta::key::table_info::TableInfoKey;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let env = TestEnv::new().await;
        let ready = RegionId::new(1024, 1);
        let failed = RegionId::new(2048, 1);
        let checked = RegionId::new(4096, 1);
        let unread = RegionId::new(4096, 2);
        for regions in [vec![ready], vec![failed], vec![checked, unread]] {
            env.env
                .create_physical_table_metadata(
                    common_meta::key::test_utils::new_test_table_info(regions[0].table_id()),
                    regions.into_iter().map(|id| leader_region(id, 1)).collect(),
                )
                .await;
        }
        let snapshot = env
            .table_metadata_manager
            .kv_backend()
            .batch_get(BatchGetRequest {
                keys: vec![
                    TableRouteKey::new(1024).to_bytes(),
                    TableInfoKey::new(1024).to_bytes(),
                ],
            })
            .await
            .unwrap();
        assert_eq!(2, snapshot.kvs.len());
        let backend = MockKvBackendBuilder::default()
            .batch_get_fn(Arc::new(move |request| {
                if [2048, 4096]
                    .iter()
                    .any(|id| request.keys.contains(&TableRouteKey::new(*id).to_bytes()))
                {
                    return common_meta::error::ElectionNoLeaderSnafu.fail();
                }
                Ok(BatchGetResponse {
                    kvs: snapshot
                        .kvs
                        .iter()
                        .filter(|kv| request.keys.contains(&kv.key))
                        .cloned()
                        .collect(),
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
        let mut ctx = env.context(new_persistent_context(
            Some(2),
            DrainDatanodeStrategy::TryAll,
        ));
        ctx.region_migration_manager = manager.clone();
        let reads = Arc::new(AtomicUsize::new(0));
        let reads_moved = reads.clone();
        let backend = env.table_metadata_manager.kv_backend().clone();
        let route_key = TableRouteKey::new(4096).to_bytes();
        ctx.table_metadata_manager = Arc::new(TableMetadataManager::new(Arc::new(
            MockKvBackendBuilder::default()
                .batch_get_fn(Arc::new(|_| Ok(BatchGetResponse { kvs: vec![] })))
                .range_async_fn(Arc::new(move |request| {
                    let fail = request.key == route_key
                        && reads_moved.fetch_add(1, Ordering::Relaxed) == 1;
                    let backend = backend.clone();
                    Box::pin(async move {
                        if fail {
                            return common_meta::error::ElectionNoLeaderSnafu.fail();
                        }
                        backend.range(request).await
                    })
                }))
                .build()
                .unwrap(),
        )));
        let mut dispatch = Dispatch::new(
            [vec![ready], vec![failed], vec![checked, unread]]
                .into_iter()
                .map(|region_ids| RegionMigrationTaskBatch {
                    region_ids,
                    from_peer: Peer::empty(1),
                    to_peer: Peer::empty(2),
                    timeout: Duration::from_secs(10),
                    trigger_reason: RegionMigrationTriggerReason::Manual,
                })
                .collect(),
        );
        let procedure_ctx = TestingEnv::procedure_context();
        let error = dispatch.next(&mut ctx, &procedure_ctx).await.unwrap_err();
        assert!(error.is_retryable());
        assert_eq!(2, reads.load(Ordering::Relaxed));
        assert_eq!(3, dispatch.batches.len());
        assert!(dispatch.inflight_subprocedures.is_empty());
        assert!(ctx.volatile_ctx.failed_regions.is_empty());
        assert_eq!(0, ctx.volatile_ctx.migrated);
        for region in [ready, failed, checked, unread] {
            assert!(!manager.tracker().contains(region));
        }

        ctx.table_metadata_manager = env.table_metadata_manager.clone();
        let (_, status) = dispatch.next(&mut ctx, &procedure_ctx).await.unwrap();
        let Status::Suspended {
            subprocedures,
            recovery,
            ..
        } = status
        else {
            panic!("expected the ready batch to be submitted");
        };
        assert_eq!(SubprocedureRecovery::Detach, recovery);
        assert_eq!(1, subprocedures.len());
        assert!(dispatch.batches.is_empty());
        assert_eq!(
            vec![failed, checked, unread],
            ctx.volatile_ctx.failed_regions
        );
        assert!(manager.tracker().contains(ready));
        drop(subprocedures);
        assert!(!manager.tracker().contains(ready));
    }

    #[tokio::test]
    async fn test_settled_failed_batch_reconciles_each_region() {
        for strategy in [
            DrainDatanodeStrategy::FailFast,
            DrainDatanodeStrategy::TryAll,
        ] {
            let env = TestEnv::new().await;
            let pending = RegionId::new(1024, 1);
            let moved = RegionId::new(1024, 2);
            let removed = RegionId::new(2048, 1);
            env.create_table(vec![leader_region(pending, 1), leader_region(moved, 2)])
                .await;
            let mut ctx = env.context(new_persistent_context(Some(2), strategy));
            let regions = vec![pending, moved, removed];

            let procedure_id = ProcedureId::random();
            let mut dispatch = Dispatch::new(vec![]);
            dispatch.inflight_subprocedures = vec![SubprocedureMeta {
                region_ids: regions,
                procedure_id,
            }];
            let procedure_ctx = procedure_context_with_receivers(HashMap::from([(
                procedure_id,
                procedure_state_receiver(ProcedureState::failed(Arc::new(
                    ProcedureError::external(MockError::new(StatusCode::Internal)),
                ))),
            )]));
            let result = dispatch
                .wait_for_inflight_subprocedures(&mut ctx, &procedure_ctx, strategy)
                .await;
            if strategy == DrainDatanodeStrategy::FailFast {
                let error::Error::DrainDatanodeAbort { failed_regions, .. } = result.unwrap_err()
                else {
                    panic!("unexpected error");
                };
                assert_eq!(vec![pending], failed_regions);
            } else {
                result.unwrap();
                assert_eq!(vec![pending], ctx.volatile_ctx.failed_regions);
            }
            assert_eq!(2, ctx.volatile_ctx.migrated);
            assert!(dispatch.inflight_subprocedures.is_empty());
        }
    }

    #[tokio::test]
    async fn test_collect_successful_batch() {
        let env = TestEnv::new().await;
        let region_id = RegionId::new(1024, 1);
        env.create_table(vec![leader_region(region_id, 1)]).await;

        let mut ctx = env.context(new_persistent_context(
            Some(2),
            DrainDatanodeStrategy::FailFast,
        ));

        let mut dispatch = Dispatch::new(vec![]);
        dispatch.inflight_subprocedures = vec![SubprocedureMeta {
            region_ids: vec![region_id],
            procedure_id: ProcedureId::random(),
        }];
        let procedure_ctx = procedure_context_with_receivers(HashMap::from([(
            dispatch.inflight_subprocedures[0].procedure_id,
            procedure_state_receiver(ProcedureState::Done { output: None }),
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
        assert!(dispatch.inflight_subprocedures.is_empty());
    }

    #[tokio::test]
    async fn test_drain_datanode_wait_failed_child_fail_fast() {
        let env = TestEnv::new().await;
        let region_id = RegionId::new(1024, 1);
        env.create_table(vec![leader_region(region_id, 1)]).await;

        let mut ctx = env.context(new_persistent_context(
            Some(2),
            DrainDatanodeStrategy::FailFast,
        ));

        let mut dispatch = Dispatch::new(vec![]);
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
        let error::Error::DrainDatanodeAbort { failed_regions, .. } = error else {
            panic!("expected the failed region in the drain error");
        };
        assert_eq!(vec![region_id], failed_regions);
        assert!(dispatch.inflight_subprocedures.is_empty());
    }

    #[tokio::test]
    async fn test_drain_datanode_wait_failed_child_try_all() {
        let env = TestEnv::new().await;
        let region_id = RegionId::new(1024, 1);
        env.create_table(vec![leader_region(region_id, 1)]).await;

        let mut ctx = env.context(new_persistent_context(
            Some(2),
            DrainDatanodeStrategy::FailFast,
        ));

        let mut dispatch = Dispatch::new(vec![]);
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
        assert!(dispatch.inflight_subprocedures.is_empty());
    }

    #[tokio::test]
    async fn test_drain_datanode_wait_failed_child_counts_migrated_when_moved() {
        let env = TestEnv::new().await;
        let region_id = RegionId::new(1024, 1);
        // The route reports another datanode as the leader, which is what a failover that
        // raced with the migration leaves behind.
        env.create_table(vec![leader_region(region_id, 2)]).await;

        let mut ctx = env.context(new_persistent_context(
            Some(2),
            DrainDatanodeStrategy::FailFast,
        ));

        let mut dispatch = Dispatch::new(vec![]);
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
        assert!(dispatch.inflight_subprocedures.is_empty());
    }

    #[tokio::test]
    async fn test_drain_datanode_wait_unknown_child_keeps_pending() {
        let env = TestEnv::new().await;
        let region_id = RegionId::new(1024, 1);
        env.create_table(vec![leader_region(region_id, 1)]).await;

        let mut ctx = env.context(new_persistent_context(
            Some(2),
            DrainDatanodeStrategy::FailFast,
        ));

        let mut dispatch = Dispatch::new(vec![]);
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
        let (next, _) = DrainDatanodeCursor::default()
            .next(&mut ctx, &procedure_ctx)
            .await
            .unwrap();
        let batches = &next.as_any().downcast_ref::<Dispatch>().unwrap().batches;
        assert_eq!(1, batches.len());
        assert_eq!(vec![region_id], batches[0].region_ids);
        assert_eq!(0, ctx.volatile_ctx.migrated);
        assert!(ctx.volatile_ctx.failed_regions.is_empty());
    }

    #[tokio::test]
    async fn test_drain_datanode_waits_for_entire_batch() {
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

            let failed_id = ProcedureId::random();
            let pending_id = ProcedureId::random();
            let mut dispatch = Dispatch::new(vec![]);
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

            let failed_id = ProcedureId::random();
            let succeeded_id = ProcedureId::random();
            let mut dispatch = Dispatch::new(vec![]);
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
            assert_eq!(
                vec![
                    (succeeded_id, vec![succeeded_region]),
                    (failed_id, vec![failed_region])
                ],
                dispatch
                    .inflight_subprocedures
                    .iter()
                    .map(|meta| (meta.procedure_id, meta.region_ids.clone()))
                    .collect::<Vec<_>>()
            );

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

    #[tokio::test]
    async fn test_dispatch_registers_and_releases_all_batch_regions() {
        let env = TestEnv::new().await;
        create_kafka_table(&env, 1024, 8, 2).await;
        let mut persistent = new_persistent_context(Some(2), DrainDatanodeStrategy::FailFast);
        persistent.parallelism = 3;
        let mut ctx = env.context(persistent);
        let regions = vec![
            RegionId::new(1024, 0),
            RegionId::new(1024, 2),
            RegionId::new(1024, 4),
        ];

        let mut dispatch = Dispatch::new(vec![RegionMigrationTaskBatch {
            region_ids: regions.clone(),
            from_peer: Peer::empty(1),
            to_peer: Peer::empty(2),
            timeout: Duration::from_secs(10),
            trigger_reason: RegionMigrationTriggerReason::Manual,
        }]);
        let (next, status) = dispatch
            .next(&mut ctx, &TestingEnv::procedure_context())
            .await
            .unwrap();
        let Status::Suspended {
            subprocedures,
            recovery,
            persist,
        } = status
        else {
            panic!("expected a detached batch");
        };
        assert!(matches!(recovery, SubprocedureRecovery::Detach));
        assert!(!persist);
        assert_eq!(1, subprocedures.len());
        assert_eq!(
            regions,
            next.as_any()
                .downcast_ref::<Dispatch>()
                .unwrap()
                .inflight_subprocedures[0]
                .region_ids
        );
        let tracker = env.region_migration_manager.tracker();
        for region_id in &regions {
            assert!(tracker.contains(*region_id));
        }
        drop(subprocedures);
        for region_id in regions {
            assert!(!tracker.contains(region_id));
        }
    }
}
