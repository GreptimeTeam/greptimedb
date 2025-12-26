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

use common_procedure::{Context as ProcedureContext, ProcedureWithId, Status};
use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::procedure::repartition::collect::{Collect, ProcedureMeta};
use crate::procedure::repartition::group::RepartitionGroupProcedure;
use crate::procedure::repartition::{self, Context, State};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Dispatch;

#[async_trait::async_trait]
#[typetag::serde]
impl State for Dispatch {
    async fn next(
        &mut self,
        ctx: &mut Context,
        _procedure_ctx: &ProcedureContext,
    ) -> Result<(Box<dyn State>, Status)> {
        let table_id = ctx.persistent_ctx.table_id;
        let mut procedures = Vec::with_capacity(ctx.persistent_ctx.plans.len());
        let mut procedure_metas = Vec::with_capacity(ctx.persistent_ctx.plans.len());
        for (plan_index, plan) in ctx.persistent_ctx.plans.iter().enumerate() {
            let persistent_ctx = repartition::group::PersistentContext::new(
                plan.group_id,
                table_id,
                plan.source_regions.clone(),
                plan.target_regions.clone(),
            );

            let group_procedure = RepartitionGroupProcedure::new(persistent_ctx, ctx);
            let procedure = ProcedureWithId::with_random_id(Box::new(group_procedure));
            procedure_metas.push(ProcedureMeta {
                plan_index,
                group_id: plan.group_id,
                procedure_id: procedure.id,
            });
            procedures.push(procedure);
        }

        Ok((
            Box::new(Collect::new(procedure_metas)),
            Status::suspended(procedures, true),
        ))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
