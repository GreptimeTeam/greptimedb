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

use common_error::ext::BoxedError;
use common_meta::DatanodeId;
use common_meta::key::TableMetadataManagerRef;
use common_meta::key::table_route::TableRouteValue;
use common_meta::peer::Peer;
use futures::TryStreamExt;
use snafu::ResultExt;
use store_api::storage::RegionId;

use crate::error::{self, Result};
use crate::procedure::drain_datanode::{Context, DrainDatanodePlan};
use crate::selector::common;

/// Builds the drain plan of `node_id`.
///
/// Lists the leader regions of `node_id` and counts its follower occurrences by walking
/// the datanode table index and reading the authoritative table routes.
pub(crate) async fn build_datanode_plan(
    table_metadata_manager: &TableMetadataManagerRef,
    node_id: DatanodeId,
) -> Result<DrainDatanodePlan> {
    let mut leader_regions = BTreeSet::new();
    let mut follower_region_count = 0;

    let mut tables = table_metadata_manager
        .datanode_table_manager()
        .tables(node_id);
    while let Some(value) = tables
        .try_next()
        .await
        .map_err(BoxedError::new)
        .context(error::OtherSnafu)?
    {
        let Some(table_route) = table_metadata_manager
            .table_route_manager()
            .table_route_storage()
            .get(value.table_id)
            .await
            .map_err(BoxedError::new)
            .context(error::OtherSnafu)?
        else {
            continue;
        };
        let TableRouteValue::Physical(table_route) = table_route else {
            continue;
        };

        for region_route in &table_route.region_routes {
            if region_route.leader_peer.as_ref().map(|peer| peer.id) == Some(node_id) {
                leader_regions.insert(region_route.region.id);
            } else if region_route
                .follower_peers
                .iter()
                .any(|peer| peer.id == node_id)
            {
                follower_region_count += 1;
            }
        }
    }

    Ok(DrainDatanodePlan {
        leader_regions: leader_regions.into_iter().collect(),
        follower_region_count,
    })
}

/// Selects the migration destination of `region_id` off the drained datanode.
///
/// Returns the pinned target when
/// [`PersistentContext::target_node_id`](crate::procedure::drain_datanode::PersistentContext::target_node_id)
/// is `Some`, otherwise the schedulable datanode with the smallest peer id. Candidates
/// come from the peer discovery, so the returned peer carries its address and was not
/// marked unschedulable; the drained datanode and the existing followers are excluded.
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
    use std::collections::HashMap;
    use std::sync::Arc;

    use common_meta::key::TableMetadataManager;
    use common_meta::key::test_utils::new_test_table_info;
    use common_meta::kv_backend::memory::MemoryKvBackend;
    use common_meta::rpc::router::{Region, RegionRoute};

    use super::*;

    async fn create_table(region_routes: Vec<RegionRoute>) -> TableMetadataManagerRef {
        let kv_backend = Arc::new(MemoryKvBackend::new());
        let table_metadata_manager = Arc::new(TableMetadataManager::new(kv_backend));
        table_metadata_manager
            .create_table_metadata(
                new_test_table_info(1024),
                TableRouteValue::physical(region_routes),
                HashMap::default(),
            )
            .await
            .unwrap();
        table_metadata_manager
    }

    #[tokio::test]
    async fn test_build_datanode_plan() {
        let region_1 = RegionId::new(1024, 1);
        let region_2 = RegionId::new(1024, 2);
        let table_metadata_manager = create_table(vec![
            RegionRoute {
                region: Region::new_test(region_1),
                leader_peer: Some(Peer::empty(1)),
                follower_peers: vec![Peer::empty(2)],
                ..Default::default()
            },
            RegionRoute {
                region: Region::new_test(region_2),
                leader_peer: Some(Peer::empty(2)),
                follower_peers: vec![Peer::empty(1)],
                ..Default::default()
            },
        ])
        .await;

        let plan = build_datanode_plan(&table_metadata_manager, 1)
            .await
            .unwrap();

        assert_eq!(vec![region_1], plan.leader_regions);
        assert_eq!(1, plan.follower_region_count);
    }

    #[tokio::test]
    async fn test_build_datanode_plan_of_empty_datanode() {
        let table_metadata_manager = create_table(vec![RegionRoute {
            region: Region::new_test(RegionId::new(1024, 1)),
            leader_peer: Some(Peer::empty(1)),
            follower_peers: vec![],
            ..Default::default()
        }])
        .await;

        let plan = build_datanode_plan(&table_metadata_manager, 2)
            .await
            .unwrap();

        assert!(plan.leader_regions.is_empty());
        assert_eq!(0, plan.follower_region_count);
    }
}
