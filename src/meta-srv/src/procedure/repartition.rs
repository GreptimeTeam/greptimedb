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

mod plan;
mod unit;

use std::collections::HashMap;

use common_meta::ddl::DdlContext;
use common_procedure::ProcedureId;
use partition::expr::PartitionExpr;
use serde::{Deserialize, Serialize};
use store_api::storage::TableId;
use strum::AsRefStr;

use crate::procedure::repartition::plan::RepartitionPlan;
use crate::procedure::repartition::unit::RepartitionUnitId;

/// Task payload passed from the DDL entry point.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepartitionTask {
    pub catalog_name: String,
    pub schema_name: String,
    pub table_name: String,
    pub table_id: TableId,
    /// Partition expressions representing the source regions.
    pub from_exprs: Vec<PartitionExpr>,
    /// Partition expressions representing the target regions.
    pub into_exprs: Vec<PartitionExpr>,
}

/// Serialized data of the repartition procedure.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct RepartitionData {
    state: RepartitionState,
    task: RepartitionTask,
    #[serde(default)]
    plan: Option<RepartitionPlan>,
    #[serde(default)]
    resource_allocated: bool,
    #[serde(default)]
    pending_units: Vec<RepartitionUnitId>,
    #[serde(default)]
    succeeded_units: Vec<RepartitionUnitId>,
    #[serde(default)]
    failed_units: Vec<RepartitionUnitId>,
    #[serde(default)]
    rollback_triggered: bool,
    #[serde(default)]
    group_subprocedures: HashMap<RepartitionUnitId, ProcedureId>,
}

/// High level states of the repartition procedure.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, AsRefStr)]
enum RepartitionState {
    Prepare,
    AllocateResources,
    DispatchSubprocedures,
    CollectSubprocedures,
    Finalize,
    Finished,
}

/// Procedure that orchestrates the repartition flow.
pub struct RepartitionProcedure {
    context: DdlContext,
    data: RepartitionData,
}
