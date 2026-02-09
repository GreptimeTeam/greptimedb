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
    /// Applies the new partition expressions for staging regions.
    ///
    /// Abort:
    /// - Target region not found.
    /// - Source region not found.
    fn apply_staging_region_routes(
        group_id: GroupId,
        sources: &[RegionDescriptor],
        targets: &[RegionDescriptor],
        pending_deallocate_region_ids: &[store_api::storage::RegionId],
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
            region_route.region.partition_expr = target
                .partition_expr
                .as_json_str()
                .context(error::SerializePartitionExprSnafu)?;
            region_route.set_leader_staging();
            region_route.clear_reject_all_writes();
        }

        for source in sources {
            let region_route = region_routes_map.get_mut(&source.region_id).context(
                error::RepartitionSourceRegionMissingSnafu {
                    group_id,
                    region_id: source.region_id,
                },
            )?;
            region_route.set_leader_staging();
            if pending_deallocate_region_ids.contains(&source.region_id) {
                region_route.set_reject_all_writes();
            } else {
                region_route.clear_reject_all_writes();
            }
        }

        Ok(region_routes)
    }

    /// Applies the new partition expressions for staging regions.
    ///
    /// Abort:
    /// - Table route is not physical.
    /// - Target region not found.
    /// - Source region not found.
    /// - Failed to update the table route.
    /// - Central region datanode table value not found.
    pub(crate) async fn apply_staging_regions(&self, ctx: &mut Context) -> Result<()> {
        let table_id = ctx.persistent_ctx.table_id;
        let group_id = ctx.persistent_ctx.group_id;
        let current_table_route_value = ctx.get_table_route_value().await?;
        let region_routes = region_routes(table_id, current_table_route_value.get_inner_ref())?;
        let new_region_routes = Self::apply_staging_region_routes(
            group_id,
            &ctx.persistent_ctx.sources,
            &ctx.persistent_ctx.targets,
            &ctx.persistent_ctx.pending_deallocate_region_ids,
            region_routes,
        )?;

        let source_count = ctx.persistent_ctx.sources.len();
        let target_count = ctx.persistent_ctx.targets.len();
        info!(
            "Apply staging regions for repartition, table_id: {}, group_id: {}, sources: {}, targets: {}",
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
    use common_meta::rpc::router::{Region, RegionRoute};
    use store_api::storage::RegionId;
    use uuid::Uuid;

    use crate::procedure::repartition::group::update_metadata::UpdateMetadata;
    use crate::procedure::repartition::plan::RegionDescriptor;
    use crate::procedure::repartition::test_util::range_expr;

    #[test]
    fn test_generate_region_routes() {
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
                ..Default::default()
            },
            RegionRoute {
                region: Region {
                    id: RegionId::new(table_id, 2),
                    partition_expr: String::new(),
                    ..Default::default()
                },
                leader_peer: Some(Peer::empty(1)),
                ..Default::default()
            },
            RegionRoute {
                region: Region {
                    id: RegionId::new(table_id, 3),
                    partition_expr: String::new(),
                    ..Default::default()
                },
                leader_peer: Some(Peer::empty(1)),
                ..Default::default()
            },
        ];
        let source_region = RegionDescriptor {
            region_id: RegionId::new(table_id, 1),
            partition_expr: range_expr("x", 0, 100),
        };
        let target_region = RegionDescriptor {
            region_id: RegionId::new(table_id, 2),
            partition_expr: range_expr("x", 0, 10),
        };

        let new_region_routes = UpdateMetadata::apply_staging_region_routes(
            group_id,
            &[source_region],
            &[target_region],
            &[],
            &region_routes,
        )
        .unwrap();
        assert!(new_region_routes[0].is_leader_staging());
        assert_eq!(
            new_region_routes[0].region.partition_expr,
            range_expr("x", 0, 100).as_json_str().unwrap()
        );
        assert_eq!(
            new_region_routes[1].region.partition_expr,
            range_expr("x", 0, 10).as_json_str().unwrap()
        );
        assert!(new_region_routes[1].is_leader_staging());
        assert!(!new_region_routes[2].is_leader_staging());
    }

    #[test]
    fn test_generate_region_routes_mark_pending_deallocate_reject_all_writes() {
        let group_id = Uuid::new_v4();
        let table_id = 1024;
        let pending_deallocate_region_id = RegionId::new(table_id, 1);
        let region_routes = vec![
            RegionRoute {
                region: Region {
                    id: pending_deallocate_region_id,
                    partition_expr: range_expr("x", 0, 100).as_json_str().unwrap(),
                    ..Default::default()
                },
                leader_peer: Some(Peer::empty(1)),
                ..Default::default()
            },
            RegionRoute {
                region: Region {
                    id: RegionId::new(table_id, 2),
                    partition_expr: String::new(),
                    ..Default::default()
                },
                leader_peer: Some(Peer::empty(1)),
                ..Default::default()
            },
        ];
        let source_region = RegionDescriptor {
            region_id: pending_deallocate_region_id,
            partition_expr: range_expr("x", 0, 100),
        };
        let target_region = RegionDescriptor {
            region_id: RegionId::new(table_id, 2),
            partition_expr: range_expr("x", 0, 10),
        };

        let new_region_routes = UpdateMetadata::apply_staging_region_routes(
            group_id,
            &[source_region],
            &[target_region],
            &[pending_deallocate_region_id],
            &region_routes,
        )
        .unwrap();

        assert!(new_region_routes[0].is_leader_staging());
        assert!(new_region_routes[0].is_reject_all_writes());
        assert!(new_region_routes[1].is_leader_staging());
        assert!(!new_region_routes[1].is_reject_all_writes());
    }
}
