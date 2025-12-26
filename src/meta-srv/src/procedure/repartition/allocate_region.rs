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

use common_procedure::{Context as ProcedureContext, Status};
use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::procedure::repartition::dispatch::Dispatch;
use crate::procedure::repartition::plan::{AllocationPlanEntry, RepartitionPlanEntry};
use crate::procedure::repartition::{Context, State};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AllocateRegion {
    plan_entries: Vec<AllocationPlanEntry>,
}

impl AllocateRegion {
    pub fn new(plan_entries: Vec<AllocationPlanEntry>) -> Self {
        Self { plan_entries }
    }
}

#[async_trait::async_trait]
#[typetag::serde]
impl State for AllocateRegion {
    async fn next(
        &mut self,
        ctx: &mut Context,
        _procedure_ctx: &ProcedureContext,
    ) -> Result<(Box<dyn State>, Status)> {
        let region_to_allocate = self
            .plan_entries
            .iter()
            .map(|p| p.regions_to_allocate)
            .sum::<usize>();

        if region_to_allocate == 0 {
            let repartition_plan_entries = self
                .plan_entries
                .iter()
                .map(RepartitionPlanEntry::from_allocation_plan_entry)
                .collect::<Vec<_>>();
            ctx.persistent_ctx.plans = repartition_plan_entries;
            return Ok((Box::new(Dispatch), Status::executing(true)));
        }

        // TODO(weny): allocate regions.
        todo!()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
