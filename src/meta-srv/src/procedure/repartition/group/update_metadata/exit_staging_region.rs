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
    pub(crate) fn exit_staging_region_routes(
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

    /// Applies the new partition expressions for staging regions.
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

        Ok(())
    }
}
