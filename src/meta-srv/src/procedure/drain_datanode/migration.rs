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

use common_meta::key::table_route::TableRouteValue;
use common_meta::peer::Peer;
use common_procedure::{
    Context as ProcedureContext, ProcedureContext as SubmissionContext, Status,
    SubprocedureRecovery, watcher,
};
use common_telemetry::{error, info, warn};
use snafu::ResultExt;
use store_api::storage::RegionId;

use crate::error::{self, Result};
use crate::metrics::{METRIC_META_REGION_MIGRATION_DATANODES, METRIC_META_REGION_MIGRATION_FAIL};
use crate::procedure::drain_datanode::cursor::DrainDatanodeCursor;
use crate::procedure::drain_datanode::dispatch::Dispatch;
use crate::procedure::drain_datanode::{
    Context, DrainDatanodeStrategy, State, SubprocedureMeta, utils,
};
use crate::procedure::region_migration::DEFAULT_REGION_MIGRATION_TIMEOUT;
use crate::procedure::region_migration::manager::{
    RegionMigrationProcedureTask, RegionMigrationTriggerReason,
};
use crate::procedure::region_migration::utils::RegionMigrationTaskBatch;
use crate::utils::batch_aggregator::BatchAggregator;

impl Context {
    /// Waits for every migration in the batch before applying the drain strategy.
    ///
    /// Keep the batch and its results unchanged until all fallible route checks succeed,
    /// so a metadata retry cannot lose failures or apply progress twice.
    pub async fn wait_for_inflight_subprocedures(
        &mut self,
        procedure_ctx: &ProcedureContext,
        strategy: DrainDatanodeStrategy,
    ) -> Result<()> {
        let node_id = self.persistent_ctx.node_id;
        let mut migrated = Vec::new();
        let mut failed = Vec::new();
        for meta in &self.volatile_ctx.inflight_subprocedures {
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
            if utils::leads_region(&self.table_metadata_manager, node_id, region_id).await? {
                failed_regions.push(region_id);
            } else {
                migrated.push(region_id);
            }
        }

        METRIC_META_REGION_MIGRATION_FAIL.inc_by(failed_count as u64);
        for region_id in migrated {
            self.forget_leader_region(region_id);
            self.volatile_ctx.migrated += 1;
            info!("Region {} left datanode {}", region_id, node_id);
        }
        self.volatile_ctx.inflight_subprocedures.clear();
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
                    self.forget_leader_region(region_id);
                }
                self.volatile_ctx.failed_regions.extend(failed_regions);
                Ok(())
            }
            DrainDatanodeStrategy::FailFast => Ok(()),
        }
    }
}

impl Context {
    /// Selects destinations within the current topic and prepares complete migration batches.
    pub(crate) async fn prepare_migrations(&mut self) -> Result<()> {
        self.volatile_ctx.migrations.clear();
        let Some(batch) = &self.volatile_ctx.topic_batch else {
            return Ok(());
        };
        let group_by_destination = batch.topic.is_some();
        let regions = batch.leader_regions.clone();
        let mut tasks = Vec::new();
        for region_id in regions {
            if self.volatile_ctx.failed_regions.contains(&region_id)
                || self.region_migration_manager.tracker().contains(region_id)
            {
                self.forget_leader_region(region_id);
                continue;
            }
            match self.prepare_migration_task(region_id).await {
                Ok(Some(task)) => tasks.push(task),
                Ok(None) => self.forget_leader_region(region_id),
                Err(e) => self.handle_batch_build_error(e, &[region_id])?,
            }
        }
        let groups = if group_by_destination {
            BatchAggregator::group(tasks.into_iter().map(|task| (task.to_peer.id, task)))
                .into_iter()
                .map(|(_, tasks)| tasks)
                .collect::<Vec<_>>()
        } else {
            tasks.into_iter().map(|task| vec![task]).collect()
        };
        self.volatile_ctx.migrations = groups
            .into_iter()
            .map(|tasks| {
                let first = &tasks[0];
                RegionMigrationTaskBatch {
                    region_ids: tasks.iter().map(|task| task.region_id).collect(),
                    from_peer: first.from_peer.clone(),
                    to_peer: first.to_peer.clone(),
                    timeout: first.timeout,
                    trigger_reason: first.trigger_reason,
                }
            })
            .collect();
        Ok(())
    }

    /// Builds detached children from batches already selected by the cursor.
    pub(crate) async fn dispatch_migrations(
        &mut self,
        procedure_ctx: &ProcedureContext,
    ) -> Result<(Box<dyn State>, Status)> {
        let node_id = self.persistent_ctx.node_id;
        let mut children = Vec::new();
        let mut inflight = Vec::new();
        let mut targets = Vec::new();
        // Keep the prepared batches until construction succeeds, so retries do not
        // consume work from the metadata cursor twice.
        for task in self.volatile_ctx.migrations.clone() {
            let target_node_id = task.to_peer.id;
            let region_ids = task.region_ids.clone();
            let submission_context = match procedure_ctx.event_context.clone() {
                Some(event_context) => SubmissionContext::from_event_context(event_context),
                None => SubmissionContext::default(),
            };
            match self
                .region_migration_manager
                .build_batch_subprocedure(submission_context, task)
                .await
            {
                Ok((result, child)) => {
                    if !result.peer_conflict.is_empty() {
                        let err = error::DrainDatanodeAbortSnafu {
                            node_id,
                            failed_regions: result.peer_conflict.clone(),
                        }
                        .build();
                        self.handle_batch_build_error(err, &result.peer_conflict)?;
                    }
                    for region_id in region_ids
                        .iter()
                        .filter(|id| !result.submitted.contains(id))
                    {
                        self.forget_leader_region(*region_id);
                    }
                    if !result.table_not_found.is_empty() {
                        warn!(
                            "Skipping dropped regions while draining datanode {}: {:?}",
                            node_id, result.table_not_found
                        );
                    }
                    if let Some(child) = child {
                        targets.push((target_node_id, result.submitted.len()));
                        inflight.push(SubprocedureMeta {
                            region_ids: result.submitted,
                            procedure_id: child.id,
                        });
                        children.push(child);
                    }
                }
                Err(e) => self.handle_batch_build_error(e, &region_ids)?,
            }
        }
        self.volatile_ctx.migrations.clear();
        self.volatile_ctx.inflight_subprocedures = inflight;
        if children.is_empty() {
            return Ok((Box::new(DrainDatanodeCursor), Status::executing(false)));
        }
        for (target, count) in targets {
            METRIC_META_REGION_MIGRATION_DATANODES
                .with_label_values(&["src", &node_id.to_string()])
                .inc_by(count as u64);
            METRIC_META_REGION_MIGRATION_DATANODES
                .with_label_values(&["desc", &target.to_string()])
                .inc_by(count as u64);
        }
        Ok((
            Box::new(Dispatch),
            Status::Suspended {
                subprocedures: children,
                recovery: SubprocedureRecovery::Detach,
                persist: false,
            },
        ))
    }

    async fn prepare_migration_task(
        &self,
        region_id: RegionId,
    ) -> Result<Option<RegionMigrationProcedureTask>> {
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

        Ok(Some(RegionMigrationProcedureTask::new(
            region_id,
            Peer::empty(node_id),
            to_peer,
            DEFAULT_REGION_MIGRATION_TIMEOUT,
            RegionMigrationTriggerReason::Manual,
        )))
    }

    fn handle_batch_build_error(&mut self, e: error::Error, regions: &[RegionId]) -> Result<()> {
        if self.persistent_ctx.strategy == DrainDatanodeStrategy::FailFast {
            return Err(e);
        }
        error!(e; "Gave up draining regions {:?}", regions);
        METRIC_META_REGION_MIGRATION_FAIL.inc_by(regions.len() as u64);
        for &region_id in regions {
            self.forget_leader_region(region_id);
            self.volatile_ctx.failed_regions.push(region_id);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::error;
    use crate::procedure::drain_datanode::test_util::{
        TestEnv, leader_region, leader_regions, new_persistent_context, seed_plan,
    };
    use crate::procedure::drain_datanode::{DrainDatanodeStrategy, SubprocedureMeta};
    use crate::procedure::test_util::{procedure_context_with_receivers, procedure_state_receiver};
    use common_error::mock::MockError;
    use common_error::status_code::StatusCode;
    use common_procedure::{Error as ProcedureError, ProcedureId, ProcedureState};
    use std::collections::HashMap;
    use std::sync::Arc;
    use store_api::storage::RegionId;
    use tokio::sync::watch;

    #[tokio::test]
    async fn test_failed_batch_reconciles_each_region() {
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
            seed_plan(&mut ctx, regions.clone());
            let procedure_id = ProcedureId::random();
            ctx.volatile_ctx.inflight_subprocedures = vec![SubprocedureMeta {
                region_ids: regions,
                procedure_id,
            }];
            let procedure_ctx = procedure_context_with_receivers(HashMap::from([(
                procedure_id,
                procedure_state_receiver(ProcedureState::failed(Arc::new(
                    ProcedureError::external(MockError::new(StatusCode::Internal)),
                ))),
            )]));
            let result = ctx
                .wait_for_inflight_subprocedures(&procedure_ctx, strategy)
                .await;
            if strategy == DrainDatanodeStrategy::FailFast {
                let error::Error::DrainDatanodeAbort { failed_regions, .. } = result.unwrap_err()
                else {
                    panic!("unexpected error");
                };
                assert_eq!(vec![pending], failed_regions);
                assert_eq!(vec![pending], leader_regions(&ctx));
            } else {
                result.unwrap();
                assert_eq!(vec![pending], ctx.volatile_ctx.failed_regions);
                assert!(leader_regions(&ctx).is_empty());
            }
            assert_eq!(2, ctx.volatile_ctx.migrated);
            assert!(ctx.volatile_ctx.inflight_subprocedures.is_empty());
        }
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

        ctx.wait_for_inflight_subprocedures(&procedure_ctx, DrainDatanodeStrategy::FailFast)
            .await
            .unwrap();

        assert_eq!(1, ctx.volatile_ctx.migrated);
        assert!(leader_regions(&ctx).is_empty());
        assert!(ctx.volatile_ctx.inflight_subprocedures.is_empty());
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
        seed_plan(&mut ctx, vec![region_id]);
        ctx.volatile_ctx.inflight_subprocedures = vec![SubprocedureMeta {
            region_ids: vec![region_id],
            procedure_id: ProcedureId::random(),
        }];
        let procedure_ctx = procedure_context_with_receivers(HashMap::from([(
            ctx.volatile_ctx.inflight_subprocedures[0].procedure_id,
            procedure_state_receiver(ProcedureState::failed(Arc::new(ProcedureError::external(
                MockError::new(StatusCode::Internal),
            )))),
        )]));

        let error = ctx
            .wait_for_inflight_subprocedures(&procedure_ctx, DrainDatanodeStrategy::FailFast)
            .await
            .unwrap_err();

        // The whole batch is settled before the procedure aborts.
        assert!(matches!(error, error::Error::DrainDatanodeAbort { .. }));
        assert_eq!(0, ctx.volatile_ctx.migrated);
        assert_eq!(vec![region_id], leader_regions(&ctx));
        assert!(ctx.volatile_ctx.inflight_subprocedures.is_empty());
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
        seed_plan(&mut ctx, vec![region_id]);
        ctx.volatile_ctx.inflight_subprocedures = vec![SubprocedureMeta {
            region_ids: vec![region_id],
            procedure_id: ProcedureId::random(),
        }];
        let procedure_ctx = procedure_context_with_receivers(HashMap::from([(
            ctx.volatile_ctx.inflight_subprocedures[0].procedure_id,
            procedure_state_receiver(ProcedureState::failed(Arc::new(ProcedureError::external(
                MockError::new(StatusCode::Internal),
            )))),
        )]));

        ctx.wait_for_inflight_subprocedures(&procedure_ctx, DrainDatanodeStrategy::TryAll)
            .await
            .unwrap();

        assert_eq!(vec![region_id], ctx.volatile_ctx.failed_regions);
        assert_eq!(0, ctx.volatile_ctx.migrated);
        assert!(leader_regions(&ctx).is_empty());
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
        seed_plan(&mut ctx, vec![region_id]);
        ctx.volatile_ctx.inflight_subprocedures = vec![SubprocedureMeta {
            region_ids: vec![region_id],
            procedure_id: ProcedureId::random(),
        }];
        let procedure_ctx = procedure_context_with_receivers(HashMap::from([(
            ctx.volatile_ctx.inflight_subprocedures[0].procedure_id,
            procedure_state_receiver(ProcedureState::failed(Arc::new(ProcedureError::external(
                MockError::new(StatusCode::Internal),
            )))),
        )]));

        ctx.wait_for_inflight_subprocedures(&procedure_ctx, DrainDatanodeStrategy::FailFast)
            .await
            .unwrap();

        assert_eq!(1, ctx.volatile_ctx.migrated);
        assert!(ctx.volatile_ctx.failed_regions.is_empty());
        assert!(leader_regions(&ctx).is_empty());
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
        seed_plan(&mut ctx, vec![region_id]);
        ctx.volatile_ctx.inflight_subprocedures = vec![SubprocedureMeta {
            region_ids: vec![region_id],
            procedure_id: ProcedureId::random(),
        }];
        let procedure_ctx = procedure_context_with_receivers(HashMap::new());

        ctx.wait_for_inflight_subprocedures(&procedure_ctx, DrainDatanodeStrategy::FailFast)
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
            ctx.volatile_ctx.inflight_subprocedures = vec![
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
            let mut waiting =
                Box::pin(ctx.wait_for_inflight_subprocedures(&procedure_ctx, strategy));
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
            assert!(ctx.volatile_ctx.inflight_subprocedures.is_empty());
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
            seed_plan(&mut ctx, vec![failed_region, succeeded_region]);
            let failed_id = ProcedureId::random();
            let succeeded_id = ProcedureId::random();
            ctx.volatile_ctx.inflight_subprocedures = vec![
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
            let error = ctx
                .wait_for_inflight_subprocedures(&procedure_ctx, strategy)
                .await
                .unwrap_err();
            assert!(error.is_retryable());
            assert_eq!(2, ctx.volatile_ctx.inflight_subprocedures.len());
            assert_eq!(0, ctx.volatile_ctx.migrated);
            assert!(ctx.volatile_ctx.failed_regions.is_empty());
            assert_eq!(vec![failed_region, succeeded_region], leader_regions(&ctx));

            ctx.table_metadata_manager = env.table_metadata_manager().clone();
            let result = ctx
                .wait_for_inflight_subprocedures(&procedure_ctx, strategy)
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
            assert!(ctx.volatile_ctx.inflight_subprocedures.is_empty());
        }
    }
}
