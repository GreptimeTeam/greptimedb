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
use partition::subtask::RepartitionSubtask;
use serde::{Deserialize, Serialize};
use store_api::storage::RegionId;
use uuid::Uuid;

pub type RepartitionUnitId = Uuid;

/// A shard of a repartition plan.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RepartitionUnit {
    pub id: RepartitionUnitId,
    pub subtask: RepartitionSubtask,
    pub sources: Vec<RegionDescriptor>,
    pub targets: Vec<RegionDescriptor>,
}

/// Metadata describing a region involved in the plan.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RegionDescriptor {
    pub region_id: Option<RegionId>,
    pub partition_expr: PartitionExpr,
}
