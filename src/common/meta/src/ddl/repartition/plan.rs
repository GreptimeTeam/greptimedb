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

use serde::{Deserialize, Serialize};
use store_api::storage::{RegionId, TableId};
use uuid::Uuid;

/// Identifier of a plan group.
pub type PlanGroupId = Uuid;

/// Logical description of the repartition plan.
///
/// The plan is persisted by the procedure framework so it must remain
/// serializable/deserializable across versions.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RepartitionPlan {
    /// Identifier of the physical table to repartition.
    pub table_id: TableId,
    /// Deterministic hash of the generated plan.  Used for idempotence checks.
    pub plan_hash: String,
    /// Plan groups to execute.  Each group is independent from others.
    pub groups: Vec<PlanGroup>,
    /// Aggregate resource expectation.
    pub resource_demand: ResourceDemand,
}

impl RepartitionPlan {
    /// Creates an empty plan.  Primarily used in tests and early skeleton code.
    pub fn empty(table_id: TableId) -> Self {
        Self {
            table_id,
            plan_hash: String::new(),
            groups: Vec::new(),
            resource_demand: ResourceDemand::default(),
        }
    }

    /// Returns `true` if the plan does not contain any work.
    pub fn is_trivial(&self) -> bool {
        self.groups.is_empty()
    }
}

/// A group of repartition operations that can be executed as a sub-procedure.
///
/// Groups are designed to be independent so that failure in one group does not
/// propagate to others.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PlanGroup {
    /// Stable identifier of the group.
    pub group_id: PlanGroupId,
    /// Regions that provide the data source.
    pub source_regions: Vec<RegionId>,
    /// Target regions that should exist after this group finishes.
    pub target_regions: Vec<RegionId>,
    /// Ordered list of logical changes required by this group.
    pub changes: Vec<PartitionChange>,
    /// Estimated resource demand contributed by this group.
    pub resource_hint: ResourceDemand,
}

impl PlanGroup {
    /// Convenience constructor for skeleton code and tests.
    pub fn new(group_id: PlanGroupId) -> Self {
        Self {
            group_id,
            source_regions: Vec::new(),
            target_regions: Vec::new(),
            changes: Vec::new(),
            resource_hint: ResourceDemand::default(),
        }
    }
}

/// Diff between the old and the new partition rules.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct PartitionRuleDiff {
    /// Ordered list of changes to transform the old rule into the new rule.
    pub changes: Vec<PartitionChange>,
}

impl PartitionRuleDiff {
    /// Returns `true` if there is no change between two rules.
    pub fn is_empty(&self) -> bool {
        self.changes.is_empty()
    }
}

/// Primitive repartition changes recognised by the planner.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum PartitionChange {
    /// Split one region into multiple target regions.
    Split {
        from: RegionId,
        to: Vec<RegionId>,
    },
    /// Merge multiple regions into one target region.
    Merge {
        from: Vec<RegionId>,
        to: RegionId,
    },
    /// No-op placeholder for future operations (e.g. rule rewrite).
    Unsupported,
}

impl PartitionChange {
    /// Returns the regions referenced by this change.
    pub fn referenced_regions(&self) -> Vec<RegionId> {
        match self {
            PartitionChange::Split { from, to } => {
                let mut regions = Vec::with_capacity(1 + to.len());
                regions.push(*from);
                regions.extend(to.iter().copied());
                regions
            }
            PartitionChange::Merge { from, to } => {
                let mut regions = Vec::with_capacity(from.len() + 1);
                regions.extend(from.iter().copied());
                regions.push(*to);
                regions
            }
            PartitionChange::Unsupported => Vec::new(),
        }
    }
}

/// Resource estimation for executing a plan or a plan group.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct ResourceDemand {
    /// Number of brand-new regions that must be allocated before execution.
    pub new_regions: u32,
    /// Rough estimate of data volume to rewrite (in bytes).
    pub estimated_bytes: u64,
}
