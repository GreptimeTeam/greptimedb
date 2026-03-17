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

use datafusion::config::ConfigOptions;
use datafusion::functions_aggregate::min_max::max_udaf;
use datafusion::functions_aggregate::sum::sum_udaf;
use datafusion::logical_expr::{Cast, LogicalPlan, LogicalPlanBuilder, Sort};
use datafusion_common::Result;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_expr::{Expr, Operator, col, lit};
use datatypes::arrow::datatypes::DataType;
use promql::extension_plan::InstantManipulate;

use crate::QueryEngineContext;
use crate::optimizer::ExtensionAnalyzerRule;

const NON_NAN_COLUMN: &str = "__prom_non_nan";

/// Rewrites `count(count(<vector_selector>) by (...))` into a presence-based group count.
///
/// This stays intentionally narrow:
/// - both aggregates must be plain `count`
/// - the inner input must be the direct instant-vector-selector plan
/// - the outer count must only group by the evaluation timestamp
#[derive(Debug)]
pub struct PromqlNestedCountRewriteRule;

impl ExtensionAnalyzerRule for PromqlNestedCountRewriteRule {
    fn analyze(
        &self,
        plan: LogicalPlan,
        _ctx: &QueryEngineContext,
        _config: &ConfigOptions,
    ) -> Result<LogicalPlan> {
        plan.transform_down(&Self::rewrite_plan).map(|x| x.data)
    }
}

impl PromqlNestedCountRewriteRule {
    fn rewrite_plan(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        let LogicalPlan::Sort(sort) = plan else {
            return Ok(Transformed::no(plan));
        };

        if let Some(rewritten) = Self::try_rewrite_sort(&sort)? {
            Ok(Transformed::yes(rewritten))
        } else {
            Ok(Transformed::no(LogicalPlan::Sort(sort)))
        }
    }

    fn try_rewrite_sort(sort: &Sort) -> Result<Option<LogicalPlan>> {
        if sort.fetch.is_some() {
            return Ok(None);
        }

        let LogicalPlan::Aggregate(outer_agg) = sort.input.as_ref() else {
            return Ok(None);
        };
        if outer_agg.group_expr.len() != 1 || outer_agg.aggr_expr.len() != 1 {
            return Ok(None);
        }
        let outer_time_expr = outer_agg.group_expr[0].clone();
        let outer_count_arg = match Self::count_arg(&outer_agg.aggr_expr[0]) {
            Some(arg) => arg,
            None => return Ok(None),
        };

        let LogicalPlan::Sort(inner_sort) = outer_agg.input.as_ref() else {
            return Ok(None);
        };
        if inner_sort.fetch.is_some() {
            return Ok(None);
        }

        let LogicalPlan::Aggregate(inner_agg) = inner_sort.input.as_ref() else {
            return Ok(None);
        };
        if inner_agg.aggr_expr.len() != 1 || inner_agg.group_expr.is_empty() {
            return Ok(None);
        }
        let inner_value_expr = match Self::count_arg(&inner_agg.aggr_expr[0]) {
            Some(arg) => arg,
            None => return Ok(None),
        };
        let Expr::Column(_inner_value_column) = inner_value_expr else {
            return Ok(None);
        };

        let Expr::Column(outer_count_column) = outer_count_arg else {
            return Ok(None);
        };
        let inner_output_field = inner_agg.schema.field(inner_agg.group_expr.len());
        if outer_count_column.name != *inner_output_field.name() {
            return Ok(None);
        }

        if !Self::is_projection_chain_to_instant(inner_agg.input.as_ref()) {
            return Ok(None);
        }

        if !inner_agg
            .group_expr
            .iter()
            .all(|expr| matches!(expr, Expr::Column(_)))
        {
            return Ok(None);
        }

        let Some(time_expr_pos) = inner_agg
            .group_expr
            .iter()
            .position(|expr| expr == &outer_time_expr)
        else {
            return Ok(None);
        };

        let mut presence_group_exprs = Vec::with_capacity(inner_agg.group_expr.len());
        presence_group_exprs.push(outer_time_expr.clone());
        presence_group_exprs.extend(
            inner_agg
                .group_expr
                .iter()
                .enumerate()
                .filter(|(idx, _)| *idx != time_expr_pos)
                .map(|(_, expr)| expr.clone()),
        );

        let presence_input = LogicalPlanBuilder::from(inner_agg.input.as_ref().clone())
            .project(
                presence_group_exprs
                    .iter()
                    .cloned()
                    .chain(Some(
                        Expr::Cast(Cast {
                            expr: Box::new(Expr::BinaryExpr(datafusion_expr::BinaryExpr {
                                left: Box::new(inner_value_expr.clone()),
                                op: Operator::Eq,
                                right: Box::new(inner_value_expr.clone()),
                            })),
                            data_type: DataType::Int64,
                        })
                        .alias(NON_NAN_COLUMN),
                    ))
                    .collect::<Vec<_>>(),
            )?
            .build()?;

        let grouped = LogicalPlanBuilder::from(presence_input)
            .aggregate(
                presence_group_exprs.clone(),
                vec![
                    max_udaf()
                        .call(vec![col(NON_NAN_COLUMN)])
                        .alias(NON_NAN_COLUMN),
                ],
            )?
            .build()?;

        let active_groups = LogicalPlanBuilder::from(grouped)
            .filter(col(NON_NAN_COLUMN).eq(lit(1_i64)))?
            .build()?;

        let outer_value_name = outer_agg
            .schema
            .field(outer_agg.group_expr.len())
            .name()
            .clone();
        let rewritten = LogicalPlanBuilder::from(active_groups)
            .aggregate(
                outer_agg.group_expr.clone(),
                vec![
                    sum_udaf()
                        .call(vec![col(NON_NAN_COLUMN)])
                        .alias(outer_value_name),
                ],
            )?
            .sort(sort.expr.clone())?
            .build()?;

        Ok(Some(rewritten))
    }

    fn count_arg(expr: &Expr) -> Option<&Expr> {
        let Expr::AggregateFunction(func) = expr else {
            return None;
        };
        if func.func.name() != "count"
            || func.params.filter.is_some()
            || func.params.distinct
            || !func.params.order_by.is_empty()
            || func.params.args.len() != 1
        {
            return None;
        }

        Some(&func.params.args[0])
    }

    fn is_projection_chain_to_instant(plan: &LogicalPlan) -> bool {
        let mut current = plan;
        loop {
            match current {
                LogicalPlan::Projection(projection) => current = projection.input.as_ref(),
                LogicalPlan::Extension(ext) => {
                    return ext.node.as_any().is::<InstantManipulate>();
                }
                _ => return false,
            }
        }
    }
}
