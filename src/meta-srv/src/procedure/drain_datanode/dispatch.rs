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

use common_error::ext::BoxedError;
use common_meta::DatanodeId;
use common_meta::key::table_route::TableRouteValue;
use common_meta::peer::Peer;
use common_procedure::{
    Context as ProcedureContext, ProcedureContext as SubmissionContext, ProcedureWithId, Status,
    watcher,
};
use common_telemetry::{error, info, warn};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use store_api::storage::RegionId;

use crate::error::{self, Result};
use crate::metrics::{METRIC_META_REGION_MIGRATION_DATANODES, METRIC_META_REGION_MIGRATION_FAIL};
use crate::procedure::drain_datanode::check_drained::CheckDrained;
use crate::procedure::drain_datanode::utils;
use crate::procedure::drain_datanode::{Context, DrainDatanodeStrategy, InflightMigration, State};
use crate::procedure::region_migration::manager::{
    RegionMigrationProcedureTask, RegionMigrationTriggerReason,
};

/// Migrates the leader regions off the drained datanode in batches.
///
/// The state first settles the previous batch, then dispatches the next one. Sub-procedures
/// are tracked in the volatile context, and the suspension is not persisted, so a metasrv
/// restart resumes from the last settled batch and re-dispatches the regions that are still
/// led by the drained datanode; regions that already left it are skipped by the route check
/// in [`build_migration_procedure`].
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
        let node_id = ctx.persistent_ctx.node_id;

        // Settle the previous batch first, so the results are persisted before the next
        // batch starts.
        if !ctx.volatile_ctx.inflight.is_empty() {
            settle(ctx, procedure_ctx).await?;
            return Ok((Box::new(Dispatch), Status::executing(true)));
        }

        utils::ensure_drain_deadline(&ctx.persistent_ctx)?;

        if ctx.persistent_ctx.leader_regions.is_empty() {
            return Ok((Box::new(CheckDrained), Status::executing(true)));
        }

        let regions: Vec<RegionId> = ctx
            .persistent_ctx
            .leader_regions
            .iter()
            .copied()
            .take(ctx.persistent_ctx.parallelism)
            .collect();

        let mut children = Vec::with_capacity(regions.len());
        let mut inflight = Vec::with_capacity(regions.len());
        let mut targets = Vec::with_capacity(regions.len());
        let mut skipped = Vec::new();
        let mut given_up = Vec::new();
        for region_id in regions {
            match build_migration_procedure(ctx, procedure_ctx, region_id).await {
                Ok(Some((child, target_node_id))) => {
                    info!(
                        "Dispatching migration of region {} off datanode {}, procedure id: {}",
                        region_id, node_id, child.id
                    );
                    inflight.push(InflightMigration {
                        region_id,
                        procedure_id: child.id,
                    });
                    targets.push(target_node_id);
                    children.push(child);
                }
                Ok(None) => skipped.push(region_id),
                Err(e) => match ctx.persistent_ctx.strategy {
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
                        given_up.push(region_id);
                    }
                },
            }
        }

        for region_id in &skipped {
            info!(
                "Skipping region {}, it is not led by datanode {} anymore",
                region_id, node_id
            );
        }
        ctx.persistent_ctx
            .leader_regions
            .retain(|region| !skipped.contains(region) && !given_up.contains(region));
        ctx.persistent_ctx.failed_regions.extend(given_up);
        ctx.volatile_ctx.inflight = inflight;

        if children.is_empty() {
            // The whole batch was handled without a sub-procedure. Persist the progress and
            // keep draining the remaining leaders instead of spending a rescan round on them.
            return Ok((Box::new(Dispatch), Status::executing(true)));
        }

        for target_node_id in targets {
            METRIC_META_REGION_MIGRATION_DATANODES
                .with_label_values(&["src", &node_id.to_string()])
                .inc();
            METRIC_META_REGION_MIGRATION_DATANODES
                .with_label_values(&["desc", &target_node_id.to_string()])
                .inc();
        }

        // The suspended status is not persisted: the sub-procedures are not recovered after
        // a restart, so the parent must not wait for them either.
        Ok((Box::new(Dispatch), Status::suspended(children, false)))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Waits for every sub-procedure of the current batch and applies the results.
///
/// The whole batch is settled before the strategy is applied, so a failing sub-procedure
/// never leaves its siblings running behind a failed parent procedure.
async fn settle(ctx: &mut Context, procedure_ctx: &ProcedureContext) -> Result<()> {
    let node_id = ctx.persistent_ctx.node_id;
    let inflight = std::mem::take(&mut ctx.volatile_ctx.inflight);
    let mut migrated = Vec::with_capacity(inflight.len());
    let mut failed = Vec::new();

    for meta in inflight {
        let region_id = meta.region_id;
        let receiver = procedure_ctx
            .provider
            .procedure_state_receiver(meta.procedure_id)
            .await
            .map_err(BoxedError::new)
            .context(error::OtherSnafu)?;
        let Some(mut receiver) = receiver else {
            // Only reachable if the sub-procedure was created but never submitted, or if its
            // runner is gone. Keep the region pending instead of reporting a failure.
            warn!(
                "Migration sub-procedure {} of region {} is unknown, keep the region pending",
                meta.procedure_id, region_id
            );
            continue;
        };

        if watcher::wait(&mut receiver).await.is_ok() {
            migrated.push(region_id);
        } else {
            METRIC_META_REGION_MIGRATION_FAIL.inc();
            failed.push(meta);
        }
    }

    for region_id in migrated {
        ctx.persistent_ctx
            .leader_regions
            .retain(|region| *region != region_id);
        ctx.persistent_ctx.migrated += 1;
        info!("Migrated region {} off datanode {}", region_id, node_id);
    }

    let mut failed_regions = Vec::new();
    for meta in failed {
        let region_id = meta.region_id;
        // The placement is authoritative: a failed sub-procedure may still have moved the
        // region, for example when a failover took the region over.
        if !utils::leads_region(&ctx.table_metadata_manager, node_id, region_id).await? {
            ctx.persistent_ctx
                .leader_regions
                .retain(|region| *region != region_id);
            ctx.persistent_ctx.migrated += 1;
            info!(
                "Region {} already left datanode {}, procedure id: {}",
                region_id, node_id, meta.procedure_id
            );
            continue;
        }

        match ctx.persistent_ctx.strategy {
            DrainDatanodeStrategy::FailFast => failed_regions.push(region_id),
            DrainDatanodeStrategy::TryAll => {
                error!(
                    "Failed to migrate region {} off datanode {}, procedure id: {}",
                    region_id, node_id, meta.procedure_id
                );
                ctx.persistent_ctx
                    .leader_regions
                    .retain(|region| *region != region_id);
                ctx.persistent_ctx.failed_regions.push(region_id);
            }
        }
    }

    if !failed_regions.is_empty() {
        return error::DrainDatanodeAbortSnafu {
            node_id,
            failed_regions,
        }
        .fail();
    }

    Ok(())
}

/// Builds the migration sub-procedure of `region_id`.
///
/// Returns `Ok(None)` when the region is not led by the drained datanode anymore, and the
/// built sub-procedure together with its target datanode otherwise.
async fn build_migration_procedure(
    ctx: &Context,
    procedure_ctx: &ProcedureContext,
    region_id: RegionId,
) -> Result<Option<(ProcedureWithId, DatanodeId)>> {
    let node_id = ctx.persistent_ctx.node_id;
    let Some(table_route) = ctx
        .table_metadata_manager
        .table_route_manager()
        .table_route_storage()
        .get(region_id.table_id())
        .await
        .map_err(BoxedError::new)
        .context(error::OtherSnafu)?
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
        utils::select_migration_target(ctx, region_id, &region_route.follower_peers).await?;

    let submission_context = match procedure_ctx.event_context.clone() {
        Some(event_context) => SubmissionContext::from_event_context(event_context),
        None => SubmissionContext::default(),
    };
    let target_node_id = to_peer.id;
    let child = ctx
        .region_migration_manager
        .build_subprocedure(
            submission_context,
            RegionMigrationProcedureTask::new(
                region_id,
                Peer::empty(node_id),
                to_peer,
                ctx.migration_timeout,
                RegionMigrationTriggerReason::Manual,
            ),
        )
        .await?;

    Ok(Some((child, target_node_id)))
}
