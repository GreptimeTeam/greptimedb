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

#[cfg(test)]
mod tests;

use std::any::TypeId;
use std::sync::Arc;

use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{ChildrenPropertiesMode, ExecutionPlan, ReplaceChildrenOptions};
use datafusion_common::Result as DfResult;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_physical_expr::ScalarFunctionExpr;
use datafusion_physical_expr::expressions::{Column, IsNotNullExpr};
use promql::extension_plan::RangeManipulateExec;
use promql::functions::Rate;

/// Fuses the physical PromQL rate projection into its range manipulation node.
#[derive(Debug)]
pub struct PromqlRateFusion;

impl PhysicalOptimizerRule for PromqlRateFusion {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        plan.transform_up(Self::rewrite).data()
    }

    fn name(&self) -> &str {
        "PromqlRateFusion"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

impl PromqlRateFusion {
    fn rewrite(plan: Arc<dyn ExecutionPlan>) -> DfResult<Transformed<Arc<dyn ExecutionPlan>>> {
        let Some(aggregate) = plan.downcast_ref::<AggregateExec>() else {
            return Ok(Transformed::no(plan));
        };
        if aggregate.mode() != &AggregateMode::Partial
            || aggregate.aggr_expr().len() != 1
            || aggregate.group_expr().has_grouping_set()
            || aggregate.limit_options().is_some()
            || aggregate.filter_expr().iter().any(Option::is_some)
        {
            return Ok(Transformed::no(plan));
        }
        let sum = &aggregate.aggr_expr()[0];
        if sum.is_distinct()
            || !sum.order_bys().is_empty()
            || sum.fun().name() != "sum"
            || sum.fun().inner().as_ref().type_id()
                != TypeId::of::<datafusion::functions_aggregate::sum::Sum>()
            || sum.field().data_type() != &arrow_schema::DataType::Float64
        {
            return Ok(Transformed::no(plan));
        }
        let sum_args = sum.expressions();
        let [sum_arg] = sum_args.as_slice() else {
            return Ok(Transformed::no(plan));
        };
        let Some(sum_column) = sum_arg.downcast_ref::<Column>() else {
            return Ok(Transformed::no(plan));
        };

        let Some(filter) = aggregate.input().downcast_ref::<FilterExec>() else {
            return Ok(Transformed::no(plan));
        };
        if filter.fetch().is_some() {
            return Ok(Transformed::no(plan));
        }
        let Some(not_null) = filter.predicate().downcast_ref::<IsNotNullExpr>() else {
            return Ok(Transformed::no(plan));
        };
        let Some(rate_column) = not_null.arg().downcast_ref::<Column>() else {
            return Ok(Transformed::no(plan));
        };
        if rate_column.index() != sum_column.index() {
            return Ok(Transformed::no(plan));
        }
        let Some(projection) = filter.input().downcast_ref::<ProjectionExec>() else {
            return Ok(Transformed::no(plan));
        };
        let rate_exprs = projection
            .expr()
            .iter()
            .enumerate()
            .filter_map(|(index, expr)| {
                expr.expr
                    .downcast_ref::<ScalarFunctionExpr>()
                    .filter(|func| func.name() == Rate::name() && func.args().len() == 4)
                    .map(|func| (index, func))
            })
            .collect::<Vec<_>>();
        if rate_exprs.len() != 1
            || projection.expr().len() != aggregate.group_expr().expr().len() + 1
        {
            return Ok(Transformed::no(plan));
        }
        let (rate_output_index, _) = rate_exprs[0];
        if rate_output_index != rate_column.index()
            || projection.expr().iter().enumerate().any(|(index, expr)| {
                index != rate_output_index && expr.expr.downcast_ref::<Column>().is_none()
            })
        {
            return Ok(Transformed::no(plan));
        }
        for group_expr in aggregate.group_expr().expr() {
            let Some(group_column) = group_expr.0.downcast_ref::<Column>() else {
                return Ok(Transformed::no(plan));
            };
            if group_column.index() == rate_column.index()
                || projection
                    .expr()
                    .get(group_column.index())
                    .is_none_or(|expr| expr.expr.downcast_ref::<Column>().is_none())
            {
                return Ok(Transformed::no(plan));
            }
        }
        let Some(range) = projection.input().downcast_ref::<RangeManipulateExec>() else {
            return Ok(Transformed::no(plan));
        };
        let Some(fused) = range.try_fuse_rate_projection(projection)? else {
            return Ok(Transformed::no(plan));
        };
        let new_filter = aggregate.input().clone().replace_children(
            vec![fused],
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )?;
        let new_aggregate = plan.replace_children(
            vec![new_filter],
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )?;
        Ok(Transformed::yes(new_aggregate))
    }
}
