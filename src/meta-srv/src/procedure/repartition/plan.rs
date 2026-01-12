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

use std::cmp::Ordering;

use partition::expr::PartitionExpr;
use serde::{Deserialize, Serialize};
use store_api::storage::{RegionId, RegionNumber, TableId};

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
    /// Returns the target regions that are newly allocated.
    pub(crate) fn allocate_regions(&self) -> Vec<&RegionDescriptor> {
        self.target_regions
            .iter()
            .filter(|r| self.allocated_region_ids.contains(&r.region_id))
            .collect()
    }
}

/// Converts an allocation plan to a repartition plan.
///
/// Converts an [`AllocationPlanEntry`] (which contains abstract region allocation intents)
/// into a [`RepartitionPlanEntry`] with concrete source and target region descriptors,
/// plus records information on which regions are newly allocated and/or pending deallocation.
///
/// # Returns
///
/// A [`Result`] containing the [`RepartitionPlanEntry`] describing exactly the source regions,
/// the target regions (including any that need to be newly allocated), and transition mappings.
///
/// # Notes
/// - If new regions are needed, their region ids are constructed using `table_id` and incrementing
///   from `next_region_number`.
/// - For each target, associates the correct region descriptor; for new regions, the region id
///   is assigned sequentially.
pub fn convert_allocation_plan_to_repartition_plan(
    table_id: TableId,
    next_region_number: &mut RegionNumber,
    AllocationPlanEntry {
        group_id,
        source_regions,
        target_partition_exprs,
        transition_map,
        ..
    }: &AllocationPlanEntry,
) -> RepartitionPlanEntry {
    match source_regions.len().cmp(&target_partition_exprs.len()) {
        Ordering::Less => {
            // requires to allocate regions
            let pending_allocate_target_partition_exprs = target_partition_exprs
                .iter()
                .skip(source_regions.len())
                .map(|target_partition_expr| {
                    let desc = RegionDescriptor {
                        region_id: RegionId::new(table_id, *next_region_number),
                        partition_expr: target_partition_expr.clone(),
                    };
                    *next_region_number += 1;
                    desc
                })
                .collect::<Vec<_>>();

            let allocated_region_ids = pending_allocate_target_partition_exprs
                .iter()
                .map(|rd| rd.region_id)
                .collect::<Vec<_>>();

            let target_regions = source_regions
                .iter()
                .zip(target_partition_exprs.iter())
                .map(|(source_region, target_partition_expr)| RegionDescriptor {
                    region_id: source_region.region_id,
                    partition_expr: target_partition_expr.clone(),
                })
                .chain(pending_allocate_target_partition_exprs)
                .collect::<Vec<_>>();

            RepartitionPlanEntry {
                group_id: *group_id,
                source_regions: source_regions.clone(),
                target_regions,
                allocated_region_ids,
                pending_deallocate_region_ids: vec![],
                transition_map: transition_map.clone(),
            }
        }
        Ordering::Equal => {
            let target_regions = source_regions
                .iter()
                .zip(target_partition_exprs.iter())
                .map(|(source_region, target_partition_expr)| RegionDescriptor {
                    region_id: source_region.region_id,
                    partition_expr: target_partition_expr.clone(),
                })
                .collect::<Vec<_>>();

            RepartitionPlanEntry {
                group_id: *group_id,
                source_regions: source_regions.clone(),
                target_regions,
                allocated_region_ids: vec![],
                pending_deallocate_region_ids: vec![],
                transition_map: transition_map.clone(),
            }
        }
        Ordering::Greater => {
            // requires to deallocate regions
            let target_regions = source_regions
                .iter()
                .take(target_partition_exprs.len())
                .zip(target_partition_exprs.iter())
                .map(|(source_region, target_partition_expr)| RegionDescriptor {
                    region_id: source_region.region_id,
                    partition_expr: target_partition_expr.clone(),
                })
                .collect::<Vec<_>>();

            let pending_deallocate_region_ids = source_regions
                .iter()
                .skip(target_partition_exprs.len())
                .map(|source_region| source_region.region_id)
                .collect::<Vec<_>>();

            RepartitionPlanEntry {
                group_id: *group_id,
                source_regions: source_regions.clone(),
                target_regions,
                allocated_region_ids: vec![],
                pending_deallocate_region_ids,
                transition_map: transition_map.clone(),
            }
        }
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

    #[test]
    fn test_convert_plan_equal_regions() {
        let group_id = Uuid::new_v4();
        let table_id = 1024;
        let mut next_region_number = 10;
        let source_regions = vec![
            create_region_descriptor(table_id, 1, "x", 0, 100),
            create_region_descriptor(table_id, 2, "x", 100, 200),
        ];
        let target_partition_exprs = vec![range_expr("x", 0, 50), range_expr("x", 50, 200)];
        let allocation_plan = AllocationPlanEntry {
            group_id,
            source_regions: source_regions.clone(),
            target_partition_exprs: target_partition_exprs.clone(),
            transition_map: Vec::new(),
        };
        let result = convert_allocation_plan_to_repartition_plan(
            table_id,
            &mut next_region_number,
            &allocation_plan,
        );
        assert_eq!(result.group_id, group_id);
        assert_eq!(result.source_regions, source_regions);
        assert_eq!(result.target_regions.len(), 2);
        assert!(result.allocated_region_ids.is_empty());
        assert!(result.pending_deallocate_region_ids.is_empty());
        // next_region_number should not change for equal regions
        assert_eq!(next_region_number, 10);
        // Verify target regions
        assert_eq!(
            result.target_regions[0].region_id,
            RegionId::new(table_id, 1)
        );
        assert_eq!(
            result.target_regions[0].partition_expr,
            target_partition_exprs[0]
        );
        assert_eq!(
            result.target_regions[1].region_id,
            RegionId::new(table_id, 2)
        );
        assert_eq!(
            result.target_regions[1].partition_expr,
            target_partition_exprs[1]
        );
    }

    #[test]
    fn test_convert_plan_allocate_regions() {
        let group_id = Uuid::new_v4();
        let table_id = 1024;
        let mut next_region_number = 10;

        // 3 source regions -> 5 target partition expressions
        let source_regions = vec![
            create_region_descriptor(table_id, 1, "x", 0, 100),
            create_region_descriptor(table_id, 2, "x", 100, 200),
            create_region_descriptor(table_id, 3, "x", 200, 300),
        ];
        let target_partition_exprs = vec![
            range_expr("x", 0, 50),
            range_expr("x", 50, 100),
            range_expr("x", 100, 150),
            range_expr("x", 150, 200),
            range_expr("x", 200, 300),
        ];
        let allocation_plan = AllocationPlanEntry {
            group_id,
            source_regions: source_regions.clone(),
            target_partition_exprs: target_partition_exprs.clone(),
            transition_map: vec![],
        };
        let result = convert_allocation_plan_to_repartition_plan(
            table_id,
            &mut next_region_number,
            &allocation_plan,
        );
        assert_eq!(result.group_id, group_id);
        assert_eq!(result.source_regions, source_regions);
        assert_eq!(result.target_regions.len(), 5);
        assert_eq!(result.allocated_region_ids.len(), 2);
        assert!(result.pending_deallocate_region_ids.is_empty());
        assert_eq!(next_region_number, 12);

        // Verify first 3 target regions use source region ids with target partition exprs
        assert_eq!(
            result.target_regions[0].region_id,
            RegionId::new(table_id, 1)
        );
        assert_eq!(
            result.target_regions[0].partition_expr,
            target_partition_exprs[0]
        );
        assert_eq!(
            result.target_regions[1].region_id,
            RegionId::new(table_id, 2)
        );
        assert_eq!(
            result.target_regions[1].partition_expr,
            target_partition_exprs[1]
        );
        assert_eq!(
            result.target_regions[2].region_id,
            RegionId::new(table_id, 3)
        );
        assert_eq!(
            result.target_regions[2].partition_expr,
            target_partition_exprs[2]
        );

        // Verify last 2 target regions are newly allocated
        assert_eq!(
            result.target_regions[3].region_id,
            RegionId::new(table_id, 10)
        );
        assert_eq!(
            result.target_regions[3].partition_expr,
            target_partition_exprs[3]
        );
        assert_eq!(
            result.target_regions[4].region_id,
            RegionId::new(table_id, 11)
        );
        assert_eq!(
            result.target_regions[4].partition_expr,
            target_partition_exprs[4]
        );

        // Verify allocated region ids
        assert_eq!(result.allocated_region_ids[0], RegionId::new(table_id, 10));
        assert_eq!(result.allocated_region_ids[1], RegionId::new(table_id, 11));
    }

    #[test]
    fn test_convert_plan_deallocate_regions() {
        let group_id = Uuid::new_v4();
        let table_id = 1024;

        // 5 source regions -> 3 target partition expressions
        let source_regions = vec![
            create_region_descriptor(table_id, 1, "x", 0, 50),
            create_region_descriptor(table_id, 2, "x", 50, 100),
            create_region_descriptor(table_id, 3, "x", 100, 150),
            create_region_descriptor(table_id, 4, "x", 150, 200),
            create_region_descriptor(table_id, 5, "x", 200, 300),
        ];
        let target_partition_exprs = vec![
            range_expr("x", 0, 100),
            range_expr("x", 100, 200),
            range_expr("x", 200, 300),
        ];
        let allocation_plan = AllocationPlanEntry {
            group_id,
            source_regions: source_regions.clone(),
            target_partition_exprs: target_partition_exprs.clone(),
            transition_map: vec![],
        };
        let mut next_region_number = 10;
        let result = convert_allocation_plan_to_repartition_plan(
            table_id,
            &mut next_region_number,
            &allocation_plan,
        );
        assert_eq!(next_region_number, 10);
        assert_eq!(result.group_id, group_id);
        assert_eq!(result.source_regions, source_regions);
        assert_eq!(result.target_regions.len(), 3);
        assert!(result.allocated_region_ids.is_empty());
        assert_eq!(result.pending_deallocate_region_ids.len(), 2);

        // Verify first 3 source regions are kept as target regions with new partition exprs
        assert_eq!(
            result.target_regions[0].region_id,
            RegionId::new(table_id, 1)
        );
        assert_eq!(
            result.target_regions[0].partition_expr,
            target_partition_exprs[0]
        );
        assert_eq!(
            result.target_regions[1].region_id,
            RegionId::new(table_id, 2)
        );
        assert_eq!(
            result.target_regions[1].partition_expr,
            target_partition_exprs[1]
        );
        assert_eq!(
            result.target_regions[2].region_id,
            RegionId::new(table_id, 3)
        );
        assert_eq!(
            result.target_regions[2].partition_expr,
            target_partition_exprs[2]
        );

        // Verify last 2 source regions are pending deallocation
        assert_eq!(
            result.pending_deallocate_region_ids[0],
            RegionId::new(table_id, 4)
        );
        assert_eq!(
            result.pending_deallocate_region_ids[1],
            RegionId::new(table_id, 5)
        );
    }

    #[test]
    fn test_convert_plan_allocate_single_region() {
        let group_id = Uuid::new_v4();
        let table_id = 1024;
        let mut next_region_number = 5;
        // 1 source region -> 2 target partition expressions
        let source_regions = vec![create_region_descriptor(table_id, 1, "x", 0, 100)];
        let target_partition_exprs = vec![range_expr("x", 0, 50), range_expr("x", 50, 100)];
        let allocation_plan = AllocationPlanEntry {
            group_id,
            source_regions: source_regions.clone(),
            target_partition_exprs: target_partition_exprs.clone(),
            transition_map: vec![],
        };

        let result = convert_allocation_plan_to_repartition_plan(
            table_id,
            &mut next_region_number,
            &allocation_plan,
        );
        assert_eq!(result.target_regions.len(), 2);
        assert_eq!(result.allocated_region_ids.len(), 1);
        assert_eq!(result.pending_deallocate_region_ids.len(), 0);
        // First target uses source region id
        assert_eq!(
            result.target_regions[0].region_id,
            RegionId::new(table_id, 1)
        );
        assert_eq!(
            result.target_regions[0].partition_expr,
            target_partition_exprs[0]
        );
        // Second target is newly allocated
        assert_eq!(
            result.target_regions[1].region_id,
            RegionId::new(table_id, 5)
        );
        assert_eq!(
            result.target_regions[1].partition_expr,
            target_partition_exprs[1]
        );
        assert_eq!(next_region_number, 6);
    }

    #[test]
    fn test_convert_plan_deallocate_to_single_region() {
        let group_id = Uuid::new_v4();
        let table_id = 1024;

        // 3 source regions -> 1 target partition expression
        let source_regions = vec![
            create_region_descriptor(table_id, 1, "x", 0, 100),
            create_region_descriptor(table_id, 2, "x", 100, 200),
            create_region_descriptor(table_id, 3, "x", 200, 300),
        ];
        let target_partition_exprs = vec![range_expr("x", 0, 300)];
        let allocation_plan = AllocationPlanEntry {
            group_id,
            source_regions: source_regions.clone(),
            target_partition_exprs: target_partition_exprs.clone(),
            transition_map: vec![],
        };
        let mut next_region_number = 10;
        let result = convert_allocation_plan_to_repartition_plan(
            table_id,
            &mut next_region_number,
            &allocation_plan,
        );
        assert_eq!(result.target_regions.len(), 1);
        assert_eq!(result.allocated_region_ids.len(), 0);
        assert_eq!(result.pending_deallocate_region_ids.len(), 2);

        // Only first source region is kept
        assert_eq!(
            result.target_regions[0].region_id,
            RegionId::new(table_id, 1)
        );
        assert_eq!(
            result.target_regions[0].partition_expr,
            target_partition_exprs[0]
        );

        // Other regions are pending deallocation
        assert_eq!(
            result.pending_deallocate_region_ids[0],
            RegionId::new(table_id, 2)
        );
        assert_eq!(
            result.pending_deallocate_region_ids[1],
            RegionId::new(table_id, 3)
        );
    }
}
