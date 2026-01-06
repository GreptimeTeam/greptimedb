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

use common_meta::key::table_route::PhysicalTableRouteValue;
use common_procedure::{Context as ProcedureContext, Status};
use partition::expr::PartitionExpr;
use partition::subtask::{self, RepartitionSubtask};
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt};
use uuid::Uuid;

use crate::error::{self, Result};
use crate::procedure::repartition::allocate_region::AllocateRegion;
use crate::procedure::repartition::plan::{AllocationPlanEntry, RegionDescriptor};
use crate::procedure::repartition::repartition_end::RepartitionEnd;
use crate::procedure::repartition::{Context, State};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepartitionStart {
    from_exprs: Vec<PartitionExpr>,
    to_exprs: Vec<PartitionExpr>,
}

impl RepartitionStart {
    pub fn new(from_exprs: Vec<PartitionExpr>, to_exprs: Vec<PartitionExpr>) -> Self {
        Self {
            from_exprs,
            to_exprs,
        }
    }
}

#[async_trait::async_trait]
#[typetag::serde]
impl State for RepartitionStart {
    async fn next(
        &mut self,
        ctx: &mut Context,
        _: &ProcedureContext,
    ) -> Result<(Box<dyn State>, Status)> {
        let (_, table_route) = ctx
            .table_metadata_manager
            .table_route_manager()
            .get_physical_table_route(ctx.persistent_ctx.table_id)
            .await
            .context(error::TableMetadataManagerSnafu)?;

        let plans = Self::build_plan(&table_route, &self.from_exprs, &self.to_exprs)?;

        if plans.is_empty() {
            return Ok((Box::new(RepartitionEnd), Status::done()));
        }

        Ok((
            Box::new(AllocateRegion::new(plans)),
            Status::executing(false),
        ))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl RepartitionStart {
    #[allow(dead_code)]
    fn build_plan(
        physical_route: &PhysicalTableRouteValue,
        from_exprs: &[PartitionExpr],
        to_exprs: &[PartitionExpr],
    ) -> Result<Vec<AllocationPlanEntry>> {
        let subtasks = subtask::create_subtasks(from_exprs, to_exprs)
            .context(error::RepartitionCreateSubtasksSnafu)?;
        if subtasks.is_empty() {
            return Ok(vec![]);
        }

        let src_descriptors = Self::source_region_descriptors(from_exprs, physical_route)?;
        Ok(Self::build_plan_entries(
            subtasks,
            &src_descriptors,
            to_exprs,
        ))
    }

    #[allow(dead_code)]
    fn build_plan_entries(
        subtasks: Vec<RepartitionSubtask>,
        source_index: &[RegionDescriptor],
        target_exprs: &[PartitionExpr],
    ) -> Vec<AllocationPlanEntry> {
        subtasks
            .into_iter()
            .map(|subtask| {
                let group_id = Uuid::new_v4();
                let source_regions = subtask
                    .from_expr_indices
                    .iter()
                    .map(|&idx| source_index[idx].clone())
                    .collect::<Vec<_>>();

                let target_partition_exprs = subtask
                    .to_expr_indices
                    .iter()
                    .map(|&idx| target_exprs[idx].clone())
                    .collect::<Vec<_>>();
                let regions_to_allocate = target_partition_exprs
                    .len()
                    .saturating_sub(source_regions.len());
                let regions_to_deallocate = source_regions
                    .len()
                    .saturating_sub(target_partition_exprs.len());
                AllocationPlanEntry {
                    group_id,
                    source_regions,
                    target_partition_exprs,
                    regions_to_allocate,
                    regions_to_deallocate,
                    transition_map: subtask.transition_map,
                }
            })
            .collect::<Vec<_>>()
    }

    fn source_region_descriptors(
        from_exprs: &[PartitionExpr],
        physical_route: &PhysicalTableRouteValue,
    ) -> Result<Vec<RegionDescriptor>> {
        let existing_regions = physical_route
            .region_routes
            .iter()
            .map(|route| (route.region.id, route.region.partition_expr()))
            .collect::<Vec<_>>();

        let descriptors = from_exprs
            .iter()
            .map(|expr| {
                let expr_json = expr
                    .as_json_str()
                    .context(error::SerializePartitionExprSnafu)?;

                let matched_region_id = existing_regions
                    .iter()
                    .find_map(|(region_id, existing_expr)| {
                        (existing_expr == &expr_json).then_some(*region_id)
                    })
                    .with_context(|| error::RepartitionSourceExprMismatchSnafu {
                        expr: expr_json,
                    })?;

                Ok(RegionDescriptor {
                    region_id: matched_region_id,
                    partition_expr: expr.clone(),
                })
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(descriptors)
    }
}
