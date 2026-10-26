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
use futures::TryStreamExt;
use snafu::ResultExt;

use crate::error::{self, Result};
use crate::procedure::drain_datanode::DrainDatanodePlan;

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
