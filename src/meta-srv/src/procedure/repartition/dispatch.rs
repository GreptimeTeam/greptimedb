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
use std::collections::HashMap;

use common_procedure::{Context as ProcedureContext, ProcedureWithId, Status};
use common_telemetry::info;
use serde::{Deserialize, Serialize};
use store_api::metric_engine_consts::METRIC_ENGINE_NAME;
use store_api::storage::RegionId;

use crate::error::Result;
use crate::procedure::repartition::collect::{Collect, ProcedureMeta};
use crate::procedure::repartition::group::RepartitionGroupProcedure;
use crate::procedure::repartition::plan::RegionDescriptor;
use crate::procedure::repartition::{self, Context, State};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Dispatch;

fn build_region_mapping(
    source_regions: &[RegionDescriptor],
    target_regions: &[RegionDescriptor],
    transition_map: &[Vec<usize>],
) -> HashMap<RegionId, Vec<RegionId>> {
    transition_map
        .iter()
        .enumerate()
        .map(|(source_idx, indices)| {
            let source_region = source_regions[source_idx].region_id;
            let target_regions = indices
                .iter()
                .map(|&target_idx| target_regions[target_idx].region_id)
                .collect::<Vec<_>>();
            (source_region, target_regions)
        })
        .collect::<HashMap<RegionId, _>>()
}

#[async_trait::async_trait]
#[typetag::serde]
impl State for Dispatch {
    async fn next(
        &mut self,
        ctx: &mut Context,
        _procedure_ctx: &ProcedureContext,
    ) -> Result<(Box<dyn State>, Status)> {
        let table_id = ctx.persistent_ctx.table_id;
        let table_info_value = ctx.get_table_info_value().await?;
        let table_engine = table_info_value.table_info.meta.engine;
        let sync_region = table_engine == METRIC_ENGINE_NAME;
        let plan_count = ctx.persistent_ctx.plans.len();
        let mut procedures = Vec::with_capacity(plan_count);
        let mut procedure_metas = Vec::with_capacity(plan_count);
        for (plan_index, plan) in ctx.persistent_ctx.plans.iter().enumerate() {
            let region_mapping = build_region_mapping(
                &plan.source_regions,
                &plan.target_regions,
                &plan.transition_map,
            );
            let persistent_ctx = repartition::group::PersistentContext::new(
                plan.group_id,
                table_id,
                ctx.persistent_ctx.catalog_name.clone(),
                ctx.persistent_ctx.schema_name.clone(),
                plan.source_regions.clone(),
                plan.target_regions.clone(),
                region_mapping,
                sync_region,
                plan.allocated_region_ids.clone(),
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

        let group_ids: Vec<_> = procedure_metas.iter().map(|m| m.group_id).collect();
        info!(
            "Dispatch repartition groups for table_id: {}, group_count: {}, group_ids: {:?}",
            table_id,
            group_ids.len(),
            group_ids
        );

        Ok((
            Box::new(Collect::new(procedure_metas)),
            Status::suspended(procedures, true),
        ))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
