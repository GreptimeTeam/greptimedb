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
use common_meta::rpc::router::region_distribution;
use futures::TryStreamExt;
use snafu::ResultExt;
use store_api::storage::RegionId;

use crate::error::{self, Result};
use crate::procedure::drain_datanode::DrainDatanodePlan;

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
