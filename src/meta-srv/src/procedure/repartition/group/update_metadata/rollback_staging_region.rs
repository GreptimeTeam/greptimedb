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

impl UpdateMetadata {
    /// Rolls back the staging regions.
    ///
    /// Abort:
    /// - Source region not found.
    /// - Target region not found.
    fn rollback_staging_region_routes(
        group_id: GroupId,
        source_routes: &[RegionRoute],
        target_routes: &[RegionRoute],
        current_region_routes: &[RegionRoute],
    ) -> Result<Vec<RegionRoute>> {
        let mut region_routes = current_region_routes.to_vec();
        let mut region_routes_map = region_routes
            .iter_mut()
            .map(|route| (route.region.id, route))
            .collect::<HashMap<_, _>>();

        for source in source_routes {
            let region_route = region_routes_map.get_mut(&source.region.id).context(
                error::RepartitionSourceRegionMissingSnafu {
                    group_id,
                    region_id: source.region.id,
                },
            )?;
            region_route.region.partition_expr = source.region.partition_expr.clone();
            region_route.clear_leader_staging();
        }

        for target in target_routes {
            let region_route = region_routes_map.get_mut(&target.region.id).context(
                error::RepartitionTargetRegionMissingSnafu {
                    group_id,
                    region_id: target.region.id,
                },
            )?;
            region_route.clear_leader_staging();
        }

        Ok(region_routes)
    }

    /// Rolls back the metadata for staging regions.
    ///
    /// Abort:
    /// - Table route is not physical.
    /// - Source region not found.
    /// - Target region not found.
    /// - Failed to update the table route.
    /// - Central region datanode table value not found.
    pub(crate) async fn rollback_staging_regions(&self, ctx: &mut Context) -> Result<()> {
        let table_id = ctx.persistent_ctx.table_id;
        let group_id = ctx.persistent_ctx.group_id;
        let current_table_route_value = ctx.get_table_route_value().await?;
        let region_routes = region_routes(table_id, current_table_route_value.get_inner_ref())?;
        // Safety: prepare result is set in [RepartitionStart] state.
        let prepare_result = ctx.persistent_ctx.group_prepare_result.as_ref().unwrap();
        let new_region_routes = Self::rollback_staging_region_routes(
            group_id,
            &prepare_result.source_routes,
            &prepare_result.target_routes,
            region_routes,
        )?;

        let source_count = prepare_result.source_routes.len();
        let target_count = prepare_result.target_routes.len();
        info!(
            "Rollback staging regions for repartition, table_id: {}, group_id: {}, sources: {}, targets: {}",
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
    use crate::procedure::repartition::test_util::range_expr;

    #[test]
    fn test_rollback_staging_region_routes() {
        let group_id = Uuid::new_v4();
        let table_id = 1024;
        let region_routes = vec![
            RegionRoute {
                region: Region {
                    id: RegionId::new(table_id, 1),
                    partition_expr: range_expr("x", 0, 100).as_json_str().unwrap(),
                    ..Default::default()
                },
                leader_peer: Some(Peer::empty(1)),
                leader_state: Some(LeaderState::Staging),
                ..Default::default()
            },
            RegionRoute {
                region: Region {
                    id: RegionId::new(table_id, 2),
                    partition_expr: String::new(),
                    ..Default::default()
                },
                leader_peer: Some(Peer::empty(1)),
                leader_state: Some(LeaderState::Staging),
                ..Default::default()
            },
            RegionRoute {
                region: Region {
                    id: RegionId::new(table_id, 3),
                    partition_expr: String::new(),
                    ..Default::default()
                },
                leader_peer: Some(Peer::empty(1)),
                leader_state: Some(LeaderState::Downgrading),
                ..Default::default()
            },
        ];
        let source_routes = vec![RegionRoute {
            region: Region {
                id: RegionId::new(table_id, 1),
                partition_expr: range_expr("x", 0, 20).as_json_str().unwrap(),
                ..Default::default()
            },
            leader_peer: Some(Peer::empty(1)),
            ..Default::default()
        }];
        let target_routes = vec![RegionRoute {
            region: Region {
                id: RegionId::new(table_id, 2),
                partition_expr: range_expr("x", 0, 20).as_json_str().unwrap(),
                ..Default::default()
            },
            leader_peer: Some(Peer::empty(1)),
            ..Default::default()
        }];
        let new_region_routes = UpdateMetadata::rollback_staging_region_routes(
            group_id,
            &source_routes,
            &target_routes,
            &region_routes,
        )
        .unwrap();
        assert!(!new_region_routes[0].is_leader_staging());
        assert_eq!(
            new_region_routes[0].region.partition_expr,
            range_expr("x", 0, 20).as_json_str().unwrap(),
        );
        assert!(!new_region_routes[1].is_leader_staging());
        assert!(new_region_routes[2].is_leader_downgrading());
    }
}
