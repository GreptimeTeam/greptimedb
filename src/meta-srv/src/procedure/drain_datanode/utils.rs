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

use std::collections::BTreeSet;

use common_meta::DatanodeId;
use common_meta::key::TableMetadataManagerRef;
use common_meta::key::table_route::TableRouteValue;
use common_meta::peer::Peer;
use common_meta::rpc::router::region_distribution;
use common_time::util::current_time_millis;
use futures::TryStreamExt;
use snafu::ResultExt;
use store_api::storage::RegionId;

use crate::error::{self, Result};
use crate::procedure::drain_datanode::{Context, DrainDatanodePlan};
use crate::selector::common;

/// Builds the drain plan of `node_id`.
///
/// Walks the datanode table index and reads the authoritative table routes, so the plan
/// reflects the routes rather than the index. Each table route is split by role with
/// [`region_distribution`], which keys the regions by datanode id.
pub(crate) async fn build_datanode_plan(
    table_metadata_manager: &TableMetadataManagerRef,
    node_id: DatanodeId,
) -> Result<DrainDatanodePlan> {
    let mut leader_regions = BTreeSet::new();
    let mut follower_regions = BTreeSet::new();

    let mut tables = table_metadata_manager
        .datanode_table_manager()
        .tables(node_id);
    while let Some(value) = tables
        .try_next()
        .await
        .context(error::TableMetadataManagerSnafu)?
    {
        let Some(table_route) = table_metadata_manager
            .table_route_manager()
            .table_route_storage()
            .get(value.table_id)
            .await
            .context(error::TableMetadataManagerSnafu)?
        else {
            continue;
        };
        let TableRouteValue::Physical(table_route) = table_route else {
            continue;
        };

        let mut distribution = region_distribution(&table_route.region_routes);
        let Some(roles) = distribution.remove(&node_id) else {
            continue;
        };
        leader_regions.extend(
            roles
                .leader_regions
                .into_iter()
                .map(|number| RegionId::new(value.table_id, number)),
        );
        follower_regions.extend(
            roles
                .follower_regions
                .into_iter()
                .map(|number| RegionId::new(value.table_id, number)),
        );
    }

    Ok(DrainDatanodePlan {
        leader_regions: leader_regions.into_iter().collect(),
        follower_regions: follower_regions.into_iter().collect(),
    })
}

/// Returns true if `node_id` still leads `region_id`.
///
/// The table route is authoritative, so a region that is not listed anymore is reported as
/// not led by the datanode.
pub(crate) async fn leads_region(
    table_metadata_manager: &TableMetadataManagerRef,
    node_id: DatanodeId,
    region_id: RegionId,
) -> Result<bool> {
    let Some(table_route) = table_metadata_manager
        .table_route_manager()
        .table_route_storage()
        .get(region_id.table_id())
        .await
        .context(error::TableMetadataManagerSnafu)?
    else {
        return Ok(false);
    };
    let TableRouteValue::Physical(table_route) = table_route else {
        return Ok(false);
    };
    let Some(region_route) = table_route
        .region_routes
        .iter()
        .find(|region_route| region_route.region.id == region_id)
    else {
        return Ok(false);
    };

    Ok(region_route.leader_peer.as_ref().map(|peer| peer.id) == Some(node_id))
}

/// Returns an error if the drain exceeded its deadline.
pub(crate) fn ensure_drain_deadline(ctx: &Context) -> Result<()> {
    if current_time_millis() < ctx.persistent_ctx.deadline_ms {
        return Ok(());
    }

    let remaining_leader_regions = ctx
        .volatile_ctx
        .plan
        .as_ref()
        .map_or(0, |plan| plan.leader_regions.len());
    error::ExceededDeadlineSnafu {
        operation: format!(
            "Drain datanode {}, migrated: {}, remaining leader regions: {}",
            ctx.persistent_ctx.node_id, ctx.volatile_ctx.migrated, remaining_leader_regions
        ),
    }
    .fail()
}

/// Selects the migration destination of `region_id` off the drained datanode.
///
/// Returns the pinned target when the procedure has one, otherwise the schedulable
/// datanode with the smallest peer id. Candidates come from the
/// peer discovery, so the returned peer carries its address and was not marked
/// unschedulable; the drained datanode and the existing followers are excluded.
pub(crate) async fn select_migration_target(
    ctx: &Context,
    region_id: RegionId,
    follower_peers: &[Peer],
) -> Result<Peer> {
    let node_id = ctx.persistent_ctx.node_id;
    let target_node_id = ctx.persistent_ctx.target_node_id;
    let target = common::active_schedulable_datanodes(&ctx.selector_ctx, None)
        .await?
        .into_iter()
        .map(|datanode| datanode.peer)
        .filter(|peer| peer.id != node_id)
        .filter(|peer| target_node_id.is_none_or(|target| peer.id == target))
        .filter(|peer| !follower_peers.iter().any(|follower| follower.id == peer.id))
        .min_by_key(|peer| peer.id);

    match target {
        Some(peer) => Ok(peer),
        None => error::DrainDatanodeAbortSnafu {
            node_id,
            failed_regions: vec![region_id],
        }
        .fail(),
    }
}

#[cfg(test)]
mod tests {
    use common_meta::peer::Peer;
    use store_api::storage::RegionId;

    use crate::error;
    use crate::key::NodeSchedulingRole;
    use crate::procedure::drain_datanode::test_util::{
        TestEnv, follower_region, leader_region, new_persistent_context,
    };
    use crate::procedure::drain_datanode::{DrainDatanodeStrategy, utils};

    #[tokio::test]
    async fn test_build_datanode_plan() {
        let env = TestEnv::new().await;
        let first = RegionId::new(1024, 1);
        let second = RegionId::new(1024, 2);
        env.create_table(vec![leader_region(first, 1), follower_region(second, 2, 1)])
            .await;

        let plan = utils::build_datanode_plan(env.table_metadata_manager(), 1)
            .await
            .unwrap();

        assert_eq!(vec![first], plan.leader_regions);
        assert_eq!(vec![second], plan.follower_regions);
    }

    #[tokio::test]
    async fn test_build_datanode_plan_of_empty_datanode() {
        let env = TestEnv::new().await;

        let plan = utils::build_datanode_plan(env.table_metadata_manager(), 1)
            .await
            .unwrap();

        assert!(plan.leader_regions.is_empty());
        assert!(plan.follower_regions.is_empty());
    }

    #[tokio::test]
    async fn test_build_datanode_plan_reads_the_table_route() {
        let env = TestEnv::new().await;
        let region_id = RegionId::new(1024, 1);
        // The datanode is listed as a follower of the region, so it is not a leader even
        // though the table index of the datanode contains the table.
        env.create_table(vec![follower_region(region_id, 2, 1)])
            .await;

        let plan = utils::build_datanode_plan(env.table_metadata_manager(), 1)
            .await
            .unwrap();

        assert!(plan.leader_regions.is_empty());
        assert_eq!(vec![region_id], plan.follower_regions);
    }

    #[tokio::test]
    async fn test_select_migration_target() {
        let env = TestEnv::new().await;
        let region_id = RegionId::new(1024, 1);

        let mut ctx = env.context(new_persistent_context(
            None,
            DrainDatanodeStrategy::FailFast,
        ));
        let peer = utils::select_migration_target(&ctx, region_id, &[])
            .await
            .unwrap();
        assert_eq!(2, peer.id);

        // The existing followers and the drained datanode are excluded.
        let peer = utils::select_migration_target(&ctx, region_id, &[Peer::empty(2)])
            .await
            .unwrap();
        assert_eq!(3, peer.id);
        assert_eq!("dn3:3001", peer.addr);

        // A pinned target is resolved to the active peer, so it carries its address.
        ctx.persistent_ctx.target_node_id = Some(2);
        let peer = utils::select_migration_target(&ctx, region_id, &[])
            .await
            .unwrap();
        assert_eq!(2, peer.id);
        assert_eq!("dn2:3001", peer.addr);

        // A target that is already a follower, the drained datanode itself or an
        // unschedulable datanode is rejected.
        ctx.persistent_ctx.target_node_id = Some(2);
        let error = utils::select_migration_target(&ctx, region_id, &[Peer::empty(2)])
            .await
            .unwrap_err();
        assert!(matches!(error, error::Error::DrainDatanodeAbort { .. }));

        ctx.persistent_ctx.target_node_id = Some(1);
        let error = utils::select_migration_target(&ctx, region_id, &[])
            .await
            .unwrap_err();
        assert!(matches!(error, error::Error::DrainDatanodeAbort { .. }));

        env.node_scheduling_manager
            .mark_unschedulable(NodeSchedulingRole::Datanode, 3)
            .await
            .unwrap();
        ctx.persistent_ctx.target_node_id = Some(3);
        let error = utils::select_migration_target(&ctx, region_id, &[])
            .await
            .unwrap_err();
        assert!(matches!(error, error::Error::DrainDatanodeAbort { .. }));
    }
}
