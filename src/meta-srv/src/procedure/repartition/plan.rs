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
use serde::{Deserialize, Serialize};

use crate::procedure::repartition::unit::RepartitionUnit;

/// Logical description of the repartition plan.
///
/// The plan is persisted by the procedure framework so it must remain
/// serializable/deserializable across versions.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RepartitionPlan {
    units: Vec<RepartitionUnit>,
    resource_demand: ResourceDemand,
    route_snapshot: PhysicalTableRouteValue,

}

/// Resources required to execute the plan.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct ResourceDemand {
    // TODO(zhongzc): add fields here.
}

