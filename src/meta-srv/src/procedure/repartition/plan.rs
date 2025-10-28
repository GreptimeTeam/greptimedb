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

use common_meta::key::table_route::PhysicalTableRouteValue;
use partition::expr::PartitionExpr;
use partition::subtask::RepartitionSubtask;
use serde::{Deserialize, Serialize};
use store_api::storage::{RegionId, TableId};
use uuid::Uuid;

/// Identifier of a plan group.
pub type PlanGroupId = Uuid;

/// Logical description of the repartition plan.
///
/// The plan is persisted by the procedure framework so it must remain
/// serializable/deserializable across versions.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RepartitionPlan {
    pub table_id: TableId,
    pub entries: Vec<PlanEntry>,
    pub resource_demand: ResourceDemand,
    pub route_snapshot: PhysicalTableRouteValue,
}

impl RepartitionPlan {
    pub fn new(
        table_id: TableId,
        entries: Vec<PlanEntry>,
        resource_demand: ResourceDemand,
        route_snapshot: PhysicalTableRouteValue,
    ) -> Self {
        Self {
            table_id,
            entries,
            resource_demand,
            route_snapshot,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PlanEntry {
    pub group_id: PlanGroupId,
    pub subtask: RepartitionSubtask,
    pub sources: Vec<RegionDescriptor>,
    pub targets: Vec<RegionDescriptor>,
}

impl PlanEntry {
    /// Construct a plan entry consisting of the connected component returned by
    /// the planner.
    pub fn new(
        group_id: PlanGroupId,
        subtask: RepartitionSubtask,
        sources: Vec<RegionDescriptor>,
        targets: Vec<RegionDescriptor>,
    ) -> Self {
        Self {
            group_id,
            subtask,
            sources,
            targets,
        }
    }
}

/// Metadata describing a region involved in the plan.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RegionDescriptor {
    pub region_id: Option<RegionId>,
    pub partition_expr: PartitionExpr,
}

/// Auxiliary information about resources required to execute the plan.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct ResourceDemand {}

impl ResourceDemand {
    pub fn from_plan_entries(_entries: &[PlanEntry]) -> Self {
        // placeholder
        Self {}
    }
}
