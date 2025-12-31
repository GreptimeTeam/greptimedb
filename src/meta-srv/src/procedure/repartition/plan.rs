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

use partition::expr::PartitionExpr;
use serde::{Deserialize, Serialize};
use store_api::storage::RegionId;

use crate::procedure::repartition::group::GroupId;

/// Metadata describing a region involved in the plan.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RegionDescriptor {
    /// The region id of the region involved in the plan.
    pub region_id: RegionId,
    /// The partition expression of the region.
    pub partition_expr: PartitionExpr,
}

/// A plan entry for the region allocation phase, describing source regions
/// and target partition expressions before allocation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AllocationPlanEntry {
    /// The group id for this plan entry.
    pub group_id: GroupId,
    /// Source region descriptors involved in the plan.
    pub source_regions: Vec<RegionDescriptor>,
    /// The target partition expressions for the new or changed regions.
    pub target_partition_exprs: Vec<PartitionExpr>,
    /// The number of regions that need to be allocated (target count - source count, if positive).
    pub regions_to_allocate: usize,
    /// The number of regions that need to be deallocated (source count - target count, if positive).
    pub regions_to_deallocate: usize,
    /// For each `source_regions[k]`, the corresponding vector contains global
    /// `target_partition_exprs` that overlap with it.
    pub transition_map: Vec<Vec<usize>>,
}

/// A plan entry for the dispatch phase after region allocation,
/// with concrete source and target region descriptors.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RepartitionPlanEntry {
    /// The group id for this plan entry.
    pub group_id: GroupId,
    /// The source region descriptors involved in the plan.
    pub source_regions: Vec<RegionDescriptor>,
    /// The target region descriptors involved in the plan.
    pub target_regions: Vec<RegionDescriptor>,
    /// The region ids of the allocated regions.
    pub allocated_region_ids: Vec<RegionId>,
    /// The region ids of the regions that are pending deallocation.
    pub pending_deallocate_region_ids: Vec<RegionId>,
    /// For each `source_regions[k]`, the corresponding vector contains global
    /// `target_regions` that overlap with it.
    pub transition_map: Vec<Vec<usize>>,
}

impl RepartitionPlanEntry {
    /// Converts an allocation plan entry into a repartition plan entry.
    ///
    /// The target regions are derived from the source regions and the target partition expressions.
    /// The allocated region ids and pending deallocate region ids are empty.
    pub fn from_allocation_plan_entry(
        AllocationPlanEntry {
            group_id,
            source_regions,
            target_partition_exprs,
            regions_to_allocate,
            regions_to_deallocate,
            transition_map,
        }: &AllocationPlanEntry,
    ) -> Self {
        debug_assert!(*regions_to_allocate == 0 && *regions_to_deallocate == 0);
        let target_regions = source_regions
            .iter()
            .zip(target_partition_exprs.iter())
            .map(|(source_region, target_partition_expr)| RegionDescriptor {
                region_id: source_region.region_id,
                partition_expr: target_partition_expr.clone(),
            })
            .collect::<Vec<_>>();

        Self {
            group_id: *group_id,
            source_regions: source_regions.clone(),
            target_regions,
            allocated_region_ids: vec![],
            pending_deallocate_region_ids: vec![],
            transition_map: transition_map.clone(),
        }
    }
}
