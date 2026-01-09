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

use common_meta::ddl::create_table::executor::CreateTableExecutor;
use common_meta::ddl::create_table::template::{
    CreateRequestBuilder, build_template_from_raw_table_info,
};
use common_meta::lock_key::TableLock;
use common_meta::node_manager::NodeManagerRef;
use common_meta::region_keeper::{MemoryRegionKeeperRef, OperatingRegionGuard};
use common_meta::rpc::router::{RegionRoute, operating_leader_regions};
use common_procedure::{Context as ProcedureContext, Status};
use common_telemetry::info;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt};
use store_api::storage::{RegionNumber, TableId};
use table::metadata::RawTableInfo;
use table::table_reference::TableReference;

use crate::error::{self, Result};
use crate::procedure::repartition::dispatch::Dispatch;
use crate::procedure::repartition::plan::{
    AllocationPlanEntry, RegionDescriptor, RepartitionPlanEntry,
    convert_allocation_plan_to_repartition_plan,
};
use crate::procedure::repartition::{Context, State};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AllocateRegion {
    plan_entries: Vec<AllocationPlanEntry>,
}

#[async_trait::async_trait]
#[typetag::serde]
impl State for AllocateRegion {
    async fn next(
        &mut self,
        ctx: &mut Context,
        procedure_ctx: &ProcedureContext,
    ) -> Result<(Box<dyn State>, Status)> {
        let table_id = ctx.persistent_ctx.table_id;
        let table_route_value = ctx.get_table_route_value().await?;
        // Safety: it is physical table route value.
        let region_routes = table_route_value.region_routes().unwrap();
        let mut next_region_number =
            Self::get_next_region_number(table_route_value.max_region_number().unwrap());

        // Converts allocation plan to repartition plan.
        let repartition_plan_entries = Self::convert_to_repartition_plans(
            table_id,
            &mut next_region_number,
            &self.plan_entries,
        );
        let plan_count = repartition_plan_entries.len();
        let to_allocate = Self::count_regions_to_allocate(&repartition_plan_entries);
        info!(
            "Repartition allocate regions start, table_id: {}, groups: {}, regions_to_allocate: {}",
            table_id, plan_count, to_allocate
        );

        // If no region to allocate, directly dispatch the plan.
        if Self::count_regions_to_allocate(&repartition_plan_entries) == 0 {
            ctx.persistent_ctx.plans = repartition_plan_entries;
            return Ok((Box::new(Dispatch), Status::executing(true)));
        }

        let allocate_regions = Self::collect_allocate_regions(&repartition_plan_entries);
        let region_number_and_partition_exprs =
            Self::prepare_region_allocation_data(&allocate_regions)?;
        let table_info_value = ctx.get_table_info_value().await?;
        let new_allocated_region_routes = ctx
            .region_routes_allocator
            .allocate(
                table_id,
                &region_number_and_partition_exprs
                    .iter()
                    .map(|(n, p)| (*n, p.as_str()))
                    .collect::<Vec<_>>(),
            )
            .await
            .context(error::AllocateRegionRoutesSnafu { table_id })?;
        let wal_options = ctx
            .wal_options_allocator
            .allocate(
                &allocate_regions
                    .iter()
                    .map(|r| r.region_id.region_number())
                    .collect::<Vec<_>>(),
                table_info_value.table_info.meta.options.skip_wal,
            )
            .await
            .context(error::AllocateWalOptionsSnafu { table_id })?;

        let new_region_count = new_allocated_region_routes.len();
        let new_regions_brief: Vec<_> = new_allocated_region_routes
            .iter()
            .map(|route| {
                let region_id = route.region.id;
                let peer = route.leader_peer.as_ref().map(|p| p.id).unwrap_or_default();
                format!("region_id: {}, peer: {}", region_id, peer)
            })
            .collect();
        info!(
            "Allocated regions for repartition, table_id: {}, new_region_count: {}, new_regions: {:?}",
            table_id, new_region_count, new_regions_brief
        );

        let _operating_guards = Self::register_operating_regions(
            &ctx.memory_region_keeper,
            &new_allocated_region_routes,
        )?;
        // Allocates the regions on datanodes.
        Self::allocate_regions(
            &ctx.node_manager,
            &table_info_value.table_info,
            &new_allocated_region_routes,
            &wal_options,
        )
        .await?;

        // TODO(weny): for metric engine, sync logical regions from the the central region.

        // Updates the table routes.
        let table_lock = TableLock::Write(table_id).into();
        let _guard = procedure_ctx.provider.acquire_lock(&table_lock).await;
        let new_region_routes =
            Self::generate_region_routes(region_routes, &new_allocated_region_routes);
        ctx.update_table_route(&table_route_value, new_region_routes, wal_options)
            .await?;
        ctx.invalidate_table_cache().await?;

        ctx.persistent_ctx.plans = repartition_plan_entries;
        Ok((Box::new(Dispatch), Status::executing(true)))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl AllocateRegion {
    pub fn new(plan_entries: Vec<AllocationPlanEntry>) -> Self {
        Self { plan_entries }
    }

    fn register_operating_regions(
        memory_region_keeper: &MemoryRegionKeeperRef,
        region_routes: &[RegionRoute],
    ) -> Result<Vec<OperatingRegionGuard>> {
        let mut operating_guards = Vec::with_capacity(region_routes.len());
        for (region_id, datanode_id) in operating_leader_regions(region_routes) {
            let guard = memory_region_keeper
                .register(datanode_id, region_id)
                .context(error::RegionOperatingRaceSnafu {
                    peer_id: datanode_id,
                    region_id,
                })?;
            operating_guards.push(guard);
        }
        Ok(operating_guards)
    }

    fn generate_region_routes(
        region_routes: &[RegionRoute],
        new_allocated_region_ids: &[RegionRoute],
    ) -> Vec<RegionRoute> {
        let region_ids = region_routes
            .iter()
            .map(|r| r.region.id)
            .collect::<HashSet<_>>();
        let mut new_region_routes = region_routes.to_vec();
        for new_allocated_region_id in new_allocated_region_ids {
            if !region_ids.contains(&new_allocated_region_id.region.id) {
                new_region_routes.push(new_allocated_region_id.clone());
            }
        }
        new_region_routes
    }

    /// Converts allocation plan entries to repartition plan entries.
    ///
    /// This method takes the allocation plan entries and converts them to repartition plan entries,
    /// updating `next_region_number` for each newly allocated region.
    fn convert_to_repartition_plans(
        table_id: TableId,
        next_region_number: &mut RegionNumber,
        plan_entries: &[AllocationPlanEntry],
    ) -> Vec<RepartitionPlanEntry> {
        plan_entries
            .iter()
            .map(|plan_entry| {
                convert_allocation_plan_to_repartition_plan(
                    table_id,
                    next_region_number,
                    plan_entry,
                )
            })
            .collect()
    }

    /// Collects all regions that need to be allocated from the repartition plan entries.
    fn collect_allocate_regions(
        repartition_plan_entries: &[RepartitionPlanEntry],
    ) -> Vec<&RegionDescriptor> {
        repartition_plan_entries
            .iter()
            .flat_map(|p| p.allocate_regions())
            .collect()
    }

    /// Prepares region allocation data: region numbers and their partition expressions.
    fn prepare_region_allocation_data(
        allocate_regions: &[&RegionDescriptor],
    ) -> Result<Vec<(RegionNumber, String)>> {
        allocate_regions
            .iter()
            .map(|r| {
                Ok((
                    r.region_id.region_number(),
                    r.partition_expr
                        .as_json_str()
                        .context(error::SerializePartitionExprSnafu)?,
                ))
            })
            .collect()
    }

    /// Calculates the total number of regions that need to be allocated.
    fn count_regions_to_allocate(repartition_plan_entries: &[RepartitionPlanEntry]) -> usize {
        repartition_plan_entries
            .iter()
            .map(|p| p.allocated_region_ids.len())
            .sum()
    }

    /// Gets the next region number from the physical table route.
    fn get_next_region_number(max_region_number: RegionNumber) -> RegionNumber {
        max_region_number + 1
    }

    async fn allocate_regions(
        node_manager: &NodeManagerRef,
        raw_table_info: &RawTableInfo,
        region_routes: &[RegionRoute],
        wal_options: &HashMap<RegionNumber, String>,
    ) -> Result<()> {
        let table_ref = TableReference::full(
            &raw_table_info.catalog_name,
            &raw_table_info.schema_name,
            &raw_table_info.name,
        );
        let table_id = raw_table_info.ident.table_id;
        let request = build_template_from_raw_table_info(raw_table_info)
            .context(error::BuildCreateRequestSnafu { table_id })?;
        let builder = CreateRequestBuilder::new(request, None);
        let region_count = region_routes.len();
        let wal_region_count = wal_options.len();
        info!(
            "Allocating regions on datanodes, table_id: {}, region_count: {}, wal_regions: {}",
            table_id, region_count, wal_region_count
        );
        let executor = CreateTableExecutor::new(table_ref.into(), false, builder);
        executor
            .on_create_regions(node_manager, table_id, region_routes, wal_options)
            .await
            .context(error::AllocateRegionsSnafu { table_id })?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use store_api::storage::RegionId;
    use uuid::Uuid;

    use super::*;
    use crate::procedure::repartition::test_util::range_expr;

    fn create_region_descriptor(
        table_id: TableId,
        region_number: u32,
        col: &str,
        start: i64,
        end: i64,
    ) -> RegionDescriptor {
        RegionDescriptor {
            region_id: RegionId::new(table_id, region_number),
            partition_expr: range_expr(col, start, end),
        }
    }

    fn create_allocation_plan_entry(
        table_id: TableId,
        source_region_numbers: &[u32],
        target_ranges: &[(i64, i64)],
    ) -> AllocationPlanEntry {
        let source_regions = source_region_numbers
            .iter()
            .enumerate()
            .map(|(i, &n)| {
                let start = i as i64 * 100;
                let end = (i + 1) as i64 * 100;
                create_region_descriptor(table_id, n, "x", start, end)
            })
            .collect();

        let target_partition_exprs = target_ranges
            .iter()
            .map(|&(start, end)| range_expr("x", start, end))
            .collect();

        AllocationPlanEntry {
            group_id: Uuid::new_v4(),
            source_regions,
            target_partition_exprs,
            transition_map: vec![],
        }
    }

    #[test]
    fn test_convert_to_repartition_plans_no_allocation() {
        let table_id = 1024;
        let mut next_region_number = 10;

        // 2 source -> 2 target (no allocation needed)
        let plan_entries = vec![create_allocation_plan_entry(
            table_id,
            &[1, 2],
            &[(0, 50), (50, 200)],
        )];

        let result = AllocateRegion::convert_to_repartition_plans(
            table_id,
            &mut next_region_number,
            &plan_entries,
        );

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].target_regions.len(), 2);
        assert!(result[0].allocated_region_ids.is_empty());
        // next_region_number should not change
        assert_eq!(next_region_number, 10);
    }

    #[test]
    fn test_convert_to_repartition_plans_with_allocation() {
        let table_id = 1024;
        let mut next_region_number = 10;

        // 2 source -> 4 target (need to allocate 2 regions)
        let plan_entries = vec![create_allocation_plan_entry(
            table_id,
            &[1, 2],
            &[(0, 50), (50, 100), (100, 150), (150, 200)],
        )];

        let result = AllocateRegion::convert_to_repartition_plans(
            table_id,
            &mut next_region_number,
            &plan_entries,
        );

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].target_regions.len(), 4);
        assert_eq!(result[0].allocated_region_ids.len(), 2);
        assert_eq!(
            result[0].allocated_region_ids[0],
            RegionId::new(table_id, 10)
        );
        assert_eq!(
            result[0].allocated_region_ids[1],
            RegionId::new(table_id, 11)
        );
        // next_region_number should be incremented by 2
        assert_eq!(next_region_number, 12);
    }

    #[test]
    fn test_convert_to_repartition_plans_multiple_entries() {
        let table_id = 1024;
        let mut next_region_number = 10;

        // Multiple plan entries with different allocation needs
        let plan_entries = vec![
            create_allocation_plan_entry(table_id, &[1], &[(0, 50), (50, 100)]), // need 1 allocation
            create_allocation_plan_entry(table_id, &[2, 3], &[(100, 150), (150, 200)]), // no allocation
            create_allocation_plan_entry(table_id, &[4], &[(200, 250), (250, 300), (300, 400)]), // need 2 allocations
        ];

        let result = AllocateRegion::convert_to_repartition_plans(
            table_id,
            &mut next_region_number,
            &plan_entries,
        );

        assert_eq!(result.len(), 3);
        assert_eq!(result[0].allocated_region_ids.len(), 1);
        assert_eq!(result[1].allocated_region_ids.len(), 0);
        assert_eq!(result[2].allocated_region_ids.len(), 2);
        // next_region_number should be incremented by 3 total
        assert_eq!(next_region_number, 13);
    }

    #[test]
    fn test_count_regions_to_allocate() {
        let table_id = 1024;
        let mut next_region_number = 10;

        let plan_entries = vec![
            create_allocation_plan_entry(table_id, &[1], &[(0, 50), (50, 100)]), // 1 allocation
            create_allocation_plan_entry(table_id, &[2, 3], &[(100, 200)]), // 0 allocation (deallocate)
            create_allocation_plan_entry(table_id, &[4], &[(200, 250), (250, 300)]), // 1 allocation
        ];

        let repartition_plans = AllocateRegion::convert_to_repartition_plans(
            table_id,
            &mut next_region_number,
            &plan_entries,
        );

        let count = AllocateRegion::count_regions_to_allocate(&repartition_plans);
        assert_eq!(count, 2);
    }

    #[test]
    fn test_collect_allocate_regions() {
        let table_id = 1024;
        let mut next_region_number = 10;

        let plan_entries = vec![
            create_allocation_plan_entry(table_id, &[1], &[(0, 50), (50, 100)]), // 1 allocation
            create_allocation_plan_entry(table_id, &[2], &[(100, 150), (150, 200)]), // 1 allocation
        ];

        let repartition_plans = AllocateRegion::convert_to_repartition_plans(
            table_id,
            &mut next_region_number,
            &plan_entries,
        );

        let allocate_regions = AllocateRegion::collect_allocate_regions(&repartition_plans);
        assert_eq!(allocate_regions.len(), 2);
        assert_eq!(allocate_regions[0].region_id, RegionId::new(table_id, 10));
        assert_eq!(allocate_regions[1].region_id, RegionId::new(table_id, 11));
    }

    #[test]
    fn test_prepare_region_allocation_data() {
        let table_id = 1024;
        let regions = [
            create_region_descriptor(table_id, 10, "x", 0, 50),
            create_region_descriptor(table_id, 11, "x", 50, 100),
        ];
        let region_refs: Vec<&RegionDescriptor> = regions.iter().collect();

        let result = AllocateRegion::prepare_region_allocation_data(&region_refs).unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].0, 10);
        assert_eq!(result[1].0, 11);
        // Verify partition expressions are serialized
        assert!(!result[0].1.is_empty());
        assert!(!result[1].1.is_empty());
    }
}
