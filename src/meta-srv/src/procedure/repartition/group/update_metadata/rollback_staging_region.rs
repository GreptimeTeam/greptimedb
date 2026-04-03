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
use store_api::storage::RegionId;

use crate::error::{self, Result};
use crate::procedure::repartition::group::update_metadata::UpdateMetadata;
use crate::procedure::repartition::group::{Context, GroupId, region_routes};
use crate::procedure::repartition::plan::RegionDescriptor;

impl UpdateMetadata {
    /// Rolls back the staging regions.
    ///
    /// Abort:
    /// - Source region not found.
    /// - Target region not found.
    fn rollback_staging_region_routes(
        group_id: GroupId,
        sources: &[RegionDescriptor],
        target_routes: &[RegionRoute],
        pending_deallocate_region_ids: &[RegionId],
        current_region_routes: &[RegionRoute],
    ) -> Result<Vec<RegionRoute>> {
        let mut region_routes = current_region_routes.to_vec();
        let mut region_routes_map = region_routes
            .iter_mut()
            .map(|route| (route.region.id, route))
            .collect::<HashMap<_, _>>();
        for source in sources {
            let region_route = region_routes_map.get_mut(&source.region_id).context(
                error::RepartitionSourceRegionMissingSnafu {
                    group_id,
                    region_id: source.region_id,
                },
            )?;
            region_route.clear_leader_staging();
            if pending_deallocate_region_ids.contains(&source.region_id) {
                region_route.clear_ignore_all_writes();
            }
        }

        for target in target_routes {
            let region_route = region_routes_map.get_mut(&target.region.id).context(
                error::RepartitionTargetRegionMissingSnafu {
                    group_id,
                    region_id: target.region.id,
                },
            )?;

            region_route.region.partition_expr = target.region.partition_expr.clone();
            region_route.write_route_policy = target.write_route_policy;

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
            &ctx.persistent_ctx.sources,
            &prepare_result.target_routes,
            &ctx.persistent_ctx.pending_deallocate_region_ids,
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
    use std::collections::HashSet;

    use common_meta::peer::Peer;
    use common_meta::rpc::router::{LeaderState, Region, RegionRoute};
    use store_api::storage::RegionId;
    use uuid::Uuid;

    use crate::procedure::repartition::group::update_metadata::UpdateMetadata;
    use crate::procedure::repartition::plan::RegionDescriptor;
    use crate::procedure::repartition::test_util::range_expr;

    fn new_region_route(
        region_id: RegionId,
        partition_expr: &str,
        leader_state: Option<LeaderState>,
        ignore_all_writes: bool,
    ) -> RegionRoute {
        let mut route = RegionRoute {
            region: Region {
                id: region_id,
                partition_expr: partition_expr.to_string(),
                ..Default::default()
            },
            leader_peer: Some(Peer::empty(1)),
            leader_state,
            ..Default::default()
        };

        if ignore_all_writes {
            route.set_ignore_all_writes();
        }

        route
    }

    fn original_target_routes(
        region_routes: &[RegionRoute],
        targets: &[RegionDescriptor],
    ) -> Vec<RegionRoute> {
        let target_ids = targets
            .iter()
            .map(|target| target.region_id)
            .collect::<HashSet<_>>();
        region_routes
            .iter()
            .filter(|route| target_ids.contains(&route.region.id))
            .cloned()
            .collect()
    }

    #[test]
    fn test_rollback_staging_region_routes_split_case() {
        let group_id = Uuid::new_v4();
        let table_id = 1024;
        let original_region_routes = vec![
            new_region_route(
                RegionId::new(table_id, 1),
                &range_expr("x", 0, 100).as_json_str().unwrap(),
                None,
                false,
            ),
            new_region_route(
                RegionId::new(table_id, 2),
                &range_expr("x", 100, 200).as_json_str().unwrap(),
                None,
                false,
            ),
            new_region_route(RegionId::new(table_id, 3), "", None, false),
        ];
        let sources = vec![RegionDescriptor {
            region_id: RegionId::new(table_id, 1),
            partition_expr: range_expr("x", 0, 100),
        }];
        let targets = vec![
            RegionDescriptor {
                region_id: RegionId::new(table_id, 1),
                partition_expr: range_expr("x", 0, 50),
            },
            RegionDescriptor {
                region_id: RegionId::new(table_id, 3),
                partition_expr: range_expr("x", 50, 100),
            },
        ];
        let applied_region_routes = UpdateMetadata::apply_staging_region_routes(
            group_id,
            &sources,
            &targets,
            &[],
            &original_region_routes,
        )
        .unwrap();
        let target_routes = original_target_routes(&original_region_routes, &targets);
        let new_region_routes = UpdateMetadata::rollback_staging_region_routes(
            group_id,
            &sources,
            &target_routes,
            &[],
            &applied_region_routes,
        )
        .unwrap();

        assert_eq!(new_region_routes, original_region_routes);
    }

    #[test]
    fn test_rollback_staging_region_routes_merge_case_is_idempotent() {
        let group_id = Uuid::new_v4();
        let table_id = 1024;
        let original_region_routes = vec![
            new_region_route(
                RegionId::new(table_id, 1),
                &range_expr("x", 0, 100).as_json_str().unwrap(),
                None,
                false,
            ),
            new_region_route(
                RegionId::new(table_id, 2),
                &range_expr("x", 100, 200).as_json_str().unwrap(),
                None,
                false,
            ),
            new_region_route(
                RegionId::new(table_id, 3),
                &range_expr("x", 200, 300).as_json_str().unwrap(),
                None,
                false,
            ),
        ];
        let sources = vec![
            RegionDescriptor {
                region_id: RegionId::new(table_id, 1),
                partition_expr: range_expr("x", 0, 100),
            },
            RegionDescriptor {
                region_id: RegionId::new(table_id, 2),
                partition_expr: range_expr("x", 100, 200),
            },
        ];
        let targets = vec![RegionDescriptor {
            region_id: RegionId::new(table_id, 1),
            partition_expr: range_expr("x", 0, 200),
        }];
        let target_routes = original_target_routes(&original_region_routes, &targets);
        let applied_region_routes = UpdateMetadata::apply_staging_region_routes(
            group_id,
            &sources,
            &targets,
            &[RegionId::new(table_id, 2)],
            &original_region_routes,
        )
        .unwrap();

        let once = UpdateMetadata::rollback_staging_region_routes(
            group_id,
            &sources,
            &target_routes,
            &[RegionId::new(table_id, 2)],
            &applied_region_routes,
        )
        .unwrap();
        let twice = UpdateMetadata::rollback_staging_region_routes(
            group_id,
            &sources,
            &target_routes,
            &[RegionId::new(table_id, 2)],
            &once,
        )
        .unwrap();

        assert_eq!(once, original_region_routes);
        assert_eq!(once, twice);
    }
}
