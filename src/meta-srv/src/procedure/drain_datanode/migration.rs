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
    Context as ProcedureContext, ProcedureContext as SubmissionContext, ProcedureWithId, watcher,
};
use common_telemetry::{error, info, warn};
use snafu::ResultExt;
use store_api::storage::RegionId;

use crate::error::{self, Result};
use crate::metrics::METRIC_META_REGION_MIGRATION_FAIL;
use crate::procedure::drain_datanode::{Context, DrainDatanodeStrategy, utils};
use crate::procedure::region_migration::DEFAULT_REGION_MIGRATION_TIMEOUT;
use crate::procedure::region_migration::manager::{
    RegionMigrationProcedureTask, RegionMigrationTriggerReason,
};

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
                    "Migration sub-procedure {} of region {} is unknown, keep the region pending",
                    meta.procedure_id, meta.region_id
                );
                continue;
            };
            match watcher::wait(&mut receiver).await {
                Ok(_) => migrated.push(meta.region_id),
                Err(e) => {
                    error!(e; "Migration sub-procedure {} of region {} failed", meta.procedure_id, meta.region_id);
                    failed.push(meta.region_id);
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
    async fn test_build_migration_procedure_checks_route_and_guard() {
        for leader_id in [1, 2] {
            let env = TestEnv::new().await;
            let region_id = RegionId::new(1024, 1);
            env.create_table(vec![leader_region(region_id, leader_id)])
                .await;
            let ctx = env.context(new_persistent_context(Some(2)));
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

    #[tokio::test]
    async fn test_drain_datanode_waits_for_migrated_batch() {
        let env = TestEnv::new().await;
        let region_id = RegionId::new(1024, 1);
        env.create_table(vec![leader_region(region_id, 1)]).await;

        let mut ctx = env.context(new_persistent_context(Some(2)));
        seed_plan(&mut ctx, vec![region_id]);
        ctx.volatile_ctx.inflight_subprocedures = vec![SubprocedureMeta {
            region_id,
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

        let mut ctx = env.context(new_persistent_context(Some(2)));
        seed_plan(&mut ctx, vec![region_id]);
        ctx.volatile_ctx.inflight_subprocedures = vec![SubprocedureMeta {
            region_id,
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

        let mut ctx = env.context(new_persistent_context(Some(2)));
        seed_plan(&mut ctx, vec![region_id]);
        ctx.volatile_ctx.inflight_subprocedures = vec![SubprocedureMeta {
            region_id,
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

        let mut ctx = env.context(new_persistent_context(Some(2)));
        seed_plan(&mut ctx, vec![region_id]);
        ctx.volatile_ctx.inflight_subprocedures = vec![SubprocedureMeta {
            region_id,
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

        let mut ctx = env.context(new_persistent_context(Some(2)));
        seed_plan(&mut ctx, vec![region_id]);
        ctx.volatile_ctx.inflight_subprocedures = vec![SubprocedureMeta {
            region_id,
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
            let mut ctx = env.context(new_persistent_context(Some(2)));
            seed_plan(&mut ctx, vec![failed_region, succeeded_region]);
            let failed_id = ProcedureId::random();
            let pending_id = ProcedureId::random();
            ctx.volatile_ctx.inflight_subprocedures = vec![
                SubprocedureMeta {
                    region_id: failed_region,
                    procedure_id: failed_id,
                },
                SubprocedureMeta {
                    region_id: succeeded_region,
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
            let mut ctx = env.context(new_persistent_context(Some(2)));
            seed_plan(&mut ctx, vec![failed_region, succeeded_region]);
            let failed_id = ProcedureId::random();
            let succeeded_id = ProcedureId::random();
            ctx.volatile_ctx.inflight_subprocedures = vec![
                SubprocedureMeta {
                    region_id: succeeded_region,
                    procedure_id: succeeded_id,
                },
                SubprocedureMeta {
                    region_id: failed_region,
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
