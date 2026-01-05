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

use std::any::Any;
use std::collections::{HashMap, HashSet};

use common_meta::ddl::drop_table::executor::DropTableExecutor;
use common_meta::node_manager::NodeManagerRef;
use common_meta::region_keeper::{MemoryRegionKeeperRef, OperatingRegionGuard};
use common_meta::region_registry::LeaderRegionRegistryRef;
use common_meta::rpc::router::{RegionRoute, operating_leader_regions};
use common_procedure::{Context as ProcedureContext, Status};
use common_telemetry::warn;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt};
use store_api::storage::{RegionId, TableId};
use table::table_name::TableName;
use table::table_reference::TableReference;

use crate::error::{self, Result};
use crate::procedure::repartition::group::region_routes;
use crate::procedure::repartition::repartition_end::RepartitionEnd;
use crate::procedure::repartition::{Context, State};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeallocateRegion;

#[async_trait::async_trait]
#[typetag::serde]
impl State for DeallocateRegion {
    async fn next(
        &mut self,
        ctx: &mut Context,
        _procedure_ctx: &ProcedureContext,
    ) -> Result<(Box<dyn State>, Status)> {
        let region_to_deallocate = ctx
            .persistent_ctx
            .plans
            .iter()
            .map(|p| p.pending_deallocate_region_ids.len())
            .sum::<usize>();
        if region_to_deallocate == 0 {
            return Ok((Box::new(RepartitionEnd), Status::done()));
        }

        let table_id = ctx.persistent_ctx.table_id;
        let pending_deallocate_region_ids = ctx
            .persistent_ctx
            .plans
            .iter()
            .flat_map(|p| p.pending_deallocate_region_ids.iter())
            .cloned()
            .collect::<HashSet<_>>();
        let table_route_value = ctx.get_table_route_value().await?.into_inner();
        let deallocating_regions = {
            let region_routes = region_routes(table_id, &table_route_value)?;
            Self::filter_deallocatable_region_routes(
                table_id,
                region_routes,
                &pending_deallocate_region_ids,
            )
        };

        Self::register_deallocating_regions(
            &ctx.memory_region_keeper,
            &mut ctx.volatile_ctx.deallocating_regions,
            &deallocating_regions,
        )?;
        let table_ref = TableReference::full(
            &ctx.persistent_ctx.catalog_name,
            &ctx.persistent_ctx.schema_name,
            &ctx.persistent_ctx.table_name,
        );
        Self::deallocate_regions(
            &ctx.node_manager,
            &ctx.leader_region_registry,
            table_ref.into(),
            table_id,
            &deallocating_regions,
        )
        .await?;
        // Clear the deallocating regions.
        ctx.volatile_ctx.deallocating_regions.clear();
        ctx.invalidate_table_cache().await?;

        Ok((Box::new(RepartitionEnd), Status::executing(false)))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl DeallocateRegion {
    #[allow(dead_code)]
    async fn deallocate_regions(
        node_manager: &NodeManagerRef,
        leader_region_registry: &LeaderRegionRegistryRef,
        table: TableName,
        table_id: TableId,
        region_routes: &[RegionRoute],
    ) -> Result<()> {
        let executor = DropTableExecutor::new(table, table_id, false);
        // Note: Consider adding an option to forcefully drop the physical region,
        // which would involve dropping all logical regions associated with that physical region.
        executor
            .on_drop_regions(node_manager, leader_region_registry, region_routes, false)
            .await
            .context(error::DeallocateRegionsSnafu { table_id })?;

        Ok(())
    }

    #[allow(dead_code)]
    fn register_deallocating_regions(
        memory_region_keeper: &MemoryRegionKeeperRef,
        deallocating_regions: &mut Vec<OperatingRegionGuard>,
        region_routes: &[RegionRoute],
    ) -> Result<()> {
        if !deallocating_regions.is_empty() {
            return Ok(());
        }
        let deallocatable_regions = operating_leader_regions(region_routes);
        for (region_id, datanode_id) in deallocatable_regions {
            let guard = memory_region_keeper
                .register(datanode_id, region_id)
                .context(error::RegionOperatingRaceSnafu {
                    region_id,
                    peer_id: datanode_id,
                })?;

            deallocating_regions.push(guard);
        }
        Ok(())
    }

    #[allow(dead_code)]
    fn filter_deallocatable_region_routes(
        table_id: TableId,
        region_routes: &[RegionRoute],
        pending_deallocate_region_ids: &HashSet<RegionId>,
    ) -> Vec<RegionRoute> {
        let region_routes_map = region_routes
            .iter()
            .map(|r| (r.region.id, r.clone()))
            .collect::<HashMap<_, _>>();
        pending_deallocate_region_ids
            .iter()
            .filter_map(|region_id| match region_routes_map.get(region_id) {
                Some(region_route) => Some(region_route.clone()),
                None => {
                    warn!(
                        "Region {} not found during deallocate regions for table {:?}",
                        region_id, table_id
                    );
                    None
                }
            })
            .collect::<Vec<_>>()
    }
}
