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

pub(crate) mod batch_aggregator;
pub(crate) mod scan;

use common_meta::DatanodeId;
use common_meta::key::TableMetadataManagerRef;
use common_meta::key::table_route::TableRouteValue;
use common_meta::peer::Peer;
use common_time::util::current_time_millis;
use snafu::ResultExt;
use store_api::storage::RegionId;

use crate::error::{self, Result};
use crate::metasrv::SelectorContext;
use crate::procedure::drain_datanode::Context;
use crate::selector::common;

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

    error::ExceededDeadlineSnafu {
        operation: format!(
            "Drain datanode {}, migrated: {}",
            ctx.persistent_ctx.node_id, ctx.volatile_ctx.migrated
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
    selector_ctx: &SelectorContext,
    node_id: DatanodeId,
    target_node_id: Option<DatanodeId>,
    region_id: RegionId,
    follower_peers: &[Peer],
) -> Result<Peer> {
    let target = common::active_schedulable_datanodes(selector_ctx, None)
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
    use crate::procedure::drain_datanode::test_util::{TestEnv, new_persistent_context};
    use crate::procedure::drain_datanode::{DrainDatanodeStrategy, utils};

    #[tokio::test]
    async fn test_select_migration_target() {
        let env = TestEnv::new().await;
        let region_id = RegionId::new(1024, 1);

        let mut ctx = env.context(new_persistent_context(
            None,
            DrainDatanodeStrategy::FailFast,
        ));
        let peer = utils::select_migration_target(
            &ctx.selector_ctx,
            ctx.persistent_ctx.node_id,
            ctx.persistent_ctx.target_node_id,
            region_id,
            &[],
        )
        .await
        .unwrap();
        assert_eq!(2, peer.id);

        // The existing followers and the drained datanode are excluded.
        let peer = utils::select_migration_target(
            &ctx.selector_ctx,
            ctx.persistent_ctx.node_id,
            ctx.persistent_ctx.target_node_id,
            region_id,
            &[Peer::empty(2)],
        )
        .await
        .unwrap();
        assert_eq!(3, peer.id);
        assert_eq!("dn3:3001", peer.addr);

        // A pinned target is resolved to the active peer, so it carries its address.
        ctx.persistent_ctx.target_node_id = Some(2);
        let peer = utils::select_migration_target(
            &ctx.selector_ctx,
            ctx.persistent_ctx.node_id,
            ctx.persistent_ctx.target_node_id,
            region_id,
            &[],
        )
        .await
        .unwrap();
        assert_eq!(2, peer.id);
        assert_eq!("dn2:3001", peer.addr);

        // A target that is already a follower, the drained datanode itself or an
        // unschedulable datanode is rejected.
        ctx.persistent_ctx.target_node_id = Some(2);
        let error = utils::select_migration_target(
            &ctx.selector_ctx,
            ctx.persistent_ctx.node_id,
            ctx.persistent_ctx.target_node_id,
            region_id,
            &[Peer::empty(2)],
        )
        .await
        .unwrap_err();
        assert!(matches!(error, error::Error::DrainDatanodeAbort { .. }));

        ctx.persistent_ctx.target_node_id = Some(1);
        let error = utils::select_migration_target(
            &ctx.selector_ctx,
            ctx.persistent_ctx.node_id,
            ctx.persistent_ctx.target_node_id,
            region_id,
            &[],
        )
        .await
        .unwrap_err();
        assert!(matches!(error, error::Error::DrainDatanodeAbort { .. }));

        env.node_scheduling_manager
            .mark_unschedulable(NodeSchedulingRole::Datanode, 3)
            .await
            .unwrap();
        ctx.persistent_ctx.target_node_id = Some(3);
        let error = utils::select_migration_target(
            &ctx.selector_ctx,
            ctx.persistent_ctx.node_id,
            ctx.persistent_ctx.target_node_id,
            region_id,
            &[],
        )
        .await
        .unwrap_err();
        assert!(matches!(error, error::Error::DrainDatanodeAbort { .. }));
    }
}
