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
};
use common_telemetry::{error, info};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use store_api::storage::RegionId;

use crate::error::{self, Result};
use crate::metrics::METRIC_META_REGION_MIGRATION_DATANODES;
use crate::procedure::drain_datanode::check_drained::CheckDrained;
use crate::procedure::drain_datanode::collect::Collect;
use crate::procedure::drain_datanode::utils;
use crate::procedure::drain_datanode::{Context, DrainDatanodeStrategy, ProcedureMeta, State};
use crate::procedure::region_migration::manager::{
    RegionMigrationProcedureTask, RegionMigrationTriggerReason,
};

/// Dispatches a batch of region migration sub-procedures.
///
/// A region leaves [`PersistentContext::leader_regions`](crate::procedure::drain_datanode::PersistentContext::leader_regions)
/// only after its sub-procedure succeeded or was given up, and the batch commits its
/// bookkeeping only after the whole batch is built, so a failed dispatch leaves the
/// persisted state untouched and can be retried.
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
        if ctx.persistent_ctx.leader_regions.is_empty() {
            return Ok((Box::new(CheckDrained), Status::executing(true)));
        }

        let regions: Vec<RegionId> = ctx
            .persistent_ctx
            .leader_regions
            .iter()
            .filter(|region_id| {
                !ctx.persistent_ctx
                    .inflight_procedures
                    .iter()
                    .any(|meta| meta.region_id == **region_id)
            })
            .copied()
            .take(ctx.persistent_ctx.batch_size)
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
                    inflight.push(ProcedureMeta {
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
        ctx.persistent_ctx.inflight_procedures.extend(inflight);

        if children.is_empty() {
            // The whole batch was settled without a sub-procedure. Keep draining the
            // remaining leaders instead of spending a rescan round on them.
            if !ctx.persistent_ctx.leader_regions.is_empty() {
                return Ok((Box::new(Dispatch), Status::executing(true)));
            }
            return Ok((Box::new(CheckDrained), Status::executing(true)));
        }

        for target_node_id in targets {
            METRIC_META_REGION_MIGRATION_DATANODES
                .with_label_values(&["src", &node_id.to_string()])
                .inc();
            METRIC_META_REGION_MIGRATION_DATANODES
                .with_label_values(&["desc", &target_node_id.to_string()])
                .inc();
        }

        Ok((Box::new(Collect), Status::suspended(children, true)))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
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
                ctx.timeout,
                RegionMigrationTriggerReason::Manual,
            ),
        )
        .await?;

    Ok(Some((child, target_node_id)))
}
