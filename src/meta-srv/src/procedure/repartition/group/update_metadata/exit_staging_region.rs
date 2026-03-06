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

use std::collections::HashMap;

use common_error::ext::BoxedError;
use common_meta::rpc::router::RegionRoute;
use common_telemetry::{error, info};
use snafu::{OptionExt, ResultExt};

use crate::error::{self, Result};
use crate::procedure::repartition::group::update_metadata::UpdateMetadata;
use crate::procedure::repartition::group::{Context, GroupId, region_routes};
use crate::procedure::repartition::plan::RegionDescriptor;

impl UpdateMetadata {
    fn exit_staging_region_routes(
        group_id: GroupId,
        sources: &[RegionDescriptor],
        targets: &[RegionDescriptor],
        current_region_routes: &[RegionRoute],
    ) -> Result<Vec<RegionRoute>> {
        let mut region_routes = current_region_routes.to_vec();
        let mut region_routes_map = region_routes
            .iter_mut()
            .map(|route| (route.region.id, route))
            .collect::<HashMap<_, _>>();

        for target in targets {
            let region_route = region_routes_map.get_mut(&target.region_id).context(
                error::RepartitionTargetRegionMissingSnafu {
                    group_id,
                    region_id: target.region_id,
                },
            )?;
            region_route.clear_leader_staging();
        }

        for source in sources {
            let region_route = region_routes_map.get_mut(&source.region_id).context(
                error::RepartitionSourceRegionMissingSnafu {
                    group_id,
                    region_id: source.region_id,
                },
            )?;
            region_route.clear_leader_staging();
        }

        Ok(region_routes)
    }

    /// Exits the staging regions.
    ///
    /// Abort:
    /// - Table route is not physical.
    /// - Target region not found.
    /// - Source region not found.
    /// - Failed to update the table route.
    /// - Central region datanode table value not found.
    pub(crate) async fn exit_staging_regions(&self, ctx: &mut Context) -> Result<()> {
        let table_id = ctx.persistent_ctx.table_id;
        let group_id = ctx.persistent_ctx.group_id;
        let current_table_route_value = ctx.get_table_route_value().await?;
        let region_routes = region_routes(table_id, current_table_route_value.get_inner_ref())?;
        let new_region_routes = Self::exit_staging_region_routes(
            group_id,
            &ctx.persistent_ctx.sources,
            &ctx.persistent_ctx.targets,
            region_routes,
        )?;

        let source_count = ctx.persistent_ctx.sources.len();
        let target_count = ctx.persistent_ctx.targets.len();
        info!(
            "Exit staging regions for repartition, table_id: {}, group_id: {}, sources: {}, targets: {}",
            table_id, group_id, source_count, target_count
        );

        if let Err(err) = ctx
            .update_table_route(&current_table_route_value, new_region_routes)
            .await
        {
            error!(err; "Failed to update the table route during the updating metadata for repartition: {table_id}, group_id: {group_id}");
            return Err(BoxedError::new(err)).context(error::RetryLaterWithSourceSnafu {
                reason: format!(
                    "Failed to update the table route during the updating metadata for repartition: {table_id}, group_id: {group_id}"
                ),
            });
        };

        ctx.update_table_repart_mapping().await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use common_meta::peer::Peer;
    use common_meta::rpc::router::{LeaderState, Region, RegionRoute};
    use store_api::storage::RegionId;
    use uuid::Uuid;

    use crate::procedure::repartition::group::update_metadata::UpdateMetadata;
    use crate::procedure::repartition::plan::RegionDescriptor;
    use crate::procedure::repartition::test_util::range_expr;

    #[test]
    fn test_exit_staging_region_routes_keep_reject_all_writes() {
        let group_id = Uuid::new_v4();
        let table_id = 1024;
        let source_region = RegionDescriptor {
            region_id: RegionId::new(table_id, 1),
            partition_expr: range_expr("x", 0, 100),
        };
        let target_region = RegionDescriptor {
            region_id: RegionId::new(table_id, 2),
            partition_expr: range_expr("x", 0, 50),
        };
        let mut source_route = RegionRoute {
            region: Region {
                id: source_region.region_id,
                partition_expr: range_expr("x", 0, 100).as_json_str().unwrap(),
                ..Default::default()
            },
            leader_peer: Some(Peer::empty(1)),
            leader_state: Some(LeaderState::Staging),
            ..Default::default()
        };
        source_route.set_ignore_all_writes();

        let mut target_route = RegionRoute {
            region: Region {
                id: target_region.region_id,
                partition_expr: range_expr("x", 0, 50).as_json_str().unwrap(),
                ..Default::default()
            },
            leader_peer: Some(Peer::empty(1)),
            leader_state: Some(LeaderState::Staging),
            ..Default::default()
        };
        target_route.set_ignore_all_writes();

        let new_region_routes = UpdateMetadata::exit_staging_region_routes(
            group_id,
            &[source_region],
            &[target_region],
            &[source_route, target_route],
        )
        .unwrap();

        assert!(!new_region_routes[0].is_leader_staging());
        assert!(new_region_routes[0].is_ignore_all_writes());
        assert!(!new_region_routes[1].is_leader_staging());
        assert!(new_region_routes[1].is_ignore_all_writes());
    }
}
