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

use std::collections::BTreeSet;
use std::sync::Arc;

use datafusion::config::ConfigOptions;
use datafusion::functions_aggregate::count::count_udaf;
use datafusion::logical_expr::{Extension, LogicalPlan, LogicalPlanBuilder, Sort};
use datafusion_common::Result;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_expr::{Expr, UserDefinedLogicalNodeCore, lit};
use promql::extension_plan::{InstantManipulate, SeriesDivide, SeriesNormalize};
use store_api::metric_engine_consts::DATA_SCHEMA_TSID_COLUMN_NAME;

use crate::QueryEngineContext;
use crate::optimizer::ExtensionAnalyzerRule;

/// Rewrites `count(<presence-preserving-agg>(<vector_selector>) by (...))` into a presence-based
/// group count.
///
/// This stays intentionally narrow:
/// - the outer aggregate must be plain `count`
/// - the inner aggregate must be a plain aggregate whose result existence is equivalent to input
///   group existence
/// - the inner input must be the direct instant-vector-selector plan
/// - the outer count must only group by the evaluation timestamp
#[derive(Debug)]
pub struct CountNestAggrRule;

impl ExtensionAnalyzerRule for CountNestAggrRule {
    fn analyze(
        &self,
        plan: LogicalPlan,
        _ctx: &QueryEngineContext,
        _config: &ConfigOptions,
    ) -> Result<LogicalPlan> {
        plan.transform_down(&Self::rewrite_plan).map(|x| x.data)
    }
}

impl CountNestAggrRule {
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
        let outer_count_arg =
            match Self::aggregate_arg_if(&outer_agg.aggr_expr[0], |name| name == "count") {
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
        let inner_value_expr = match Self::aggregate_arg_if(&inner_agg.aggr_expr[0], |name| {
            Self::is_supported_inner_aggregate(name)
        }) {
            Some(arg) => arg,
            None => return Ok(None),
        };
        let Expr::Column(_) = inner_value_expr else {
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

        let mut required_input_columns =
            Self::collect_required_input_columns(&presence_group_exprs, inner_value_expr);
        let presence_source = Self::prune_projection_chain_to_instant(
            inner_agg.input.as_ref(),
            &mut required_input_columns,
        )?;

        let outer_value_name = outer_agg
            .schema
            .field(outer_agg.group_expr.len())
            .name()
            .clone();
        let presence_input = LogicalPlanBuilder::from(presence_source)
            .project(presence_group_exprs.clone())?
            .distinct()?
            .build()?;

        let rewritten = LogicalPlanBuilder::from(presence_input)
            .aggregate(
                outer_agg.group_expr.clone(),
                vec![count_udaf().call(vec![lit(1_i64)]).alias(outer_value_name)],
            )?
            .sort(sort.expr.clone())?
            .build()?;

        Ok(Some(rewritten))
    }

    fn collect_required_input_columns(group_exprs: &[Expr], value_expr: &Expr) -> BTreeSet<String> {
        let mut required = BTreeSet::new();

        for expr in group_exprs {
            if let Expr::Column(column) = expr {
                required.insert(column.name.clone());
            }
        }
        if let Expr::Column(column) = value_expr {
            // Keep the value column in the pruned instant input so `InstantManipulate`
            // can still perform stale-NaN filtering before we project down to keys.
            required.insert(column.name.clone());
        }

        required
    }

    fn aggregate_arg_if<F>(expr: &Expr, accept_name: F) -> Option<&Expr>
    where
        F: FnOnce(&str) -> bool,
    {
        let Expr::AggregateFunction(func) = expr else {
            return None;
        };
        if !accept_name(func.func.name())
            || func.params.filter.is_some()
            || func.params.distinct
            || !func.params.order_by.is_empty()
            || func.params.args.len() != 1
        {
            return None;
        }

        Some(&func.params.args[0])
    }

    fn is_supported_inner_aggregate(name: &str) -> bool {
        matches!(
            name,
            "count" | "sum" | "avg" | "min" | "max" | "stddev_pop" | "var_pop"
        )
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

    fn prune_projection_chain_to_instant(
        plan: &LogicalPlan,
        required_columns: &mut BTreeSet<String>,
    ) -> Result<LogicalPlan> {
        match plan {
            LogicalPlan::Projection(projection) => {
                let input = Self::prune_projection_chain_to_instant(
                    projection.input.as_ref(),
                    required_columns,
                )?;
                LogicalPlanBuilder::from(input)
                    .project(projection.expr.clone())?
                    .build()
            }
            LogicalPlan::Extension(extension) => {
                if let Some(instant) = extension.node.as_any().downcast_ref::<InstantManipulate>() {
                    let input =
                        Self::prune_instant_input(extension.node.inputs()[0], required_columns)?;
                    return Ok(LogicalPlan::Extension(Extension {
                        node: Arc::new(instant.with_exprs_and_inputs(vec![], vec![input])?),
                    }));
                }

                Ok(plan.clone())
            }
            _ => Ok(plan.clone()),
        }
    }

    fn prune_instant_input(
        plan: &LogicalPlan,
        required_columns: &mut BTreeSet<String>,
    ) -> Result<LogicalPlan> {
        match plan {
            LogicalPlan::Extension(extension) => {
                if let Some(normalize) = extension.node.as_any().downcast_ref::<SeriesNormalize>() {
                    let input =
                        Self::prune_instant_input(extension.node.inputs()[0], required_columns)?;
                    return Ok(LogicalPlan::Extension(Extension {
                        node: Arc::new(normalize.with_exprs_and_inputs(vec![], vec![input])?),
                    }));
                }

                if let Some(divide) = extension.node.as_any().downcast_ref::<SeriesDivide>() {
                    let divide_input = extension.node.inputs()[0].clone();
                    for expr in extension.node.expressions() {
                        if let Expr::Column(column) = expr {
                            required_columns.insert(column.name.clone());
                        }
                    }
                    if divide_input
                        .schema()
                        .fields()
                        .iter()
                        .any(|field| field.name() == DATA_SCHEMA_TSID_COLUMN_NAME)
                    {
                        required_columns.insert(DATA_SCHEMA_TSID_COLUMN_NAME.to_string());
                    }

                    let projection_exprs = divide_input
                        .schema()
                        .fields()
                        .iter()
                        .filter(|field| required_columns.contains(field.name()))
                        .map(|field| {
                            Expr::Column(datafusion_common::Column::from_name(field.name().clone()))
                        })
                        .collect::<Vec<_>>();
                    let projected_input = LogicalPlanBuilder::from(divide_input)
                        .project(projection_exprs)?
                        .build()?;

                    return Ok(LogicalPlan::Extension(Extension {
                        node: Arc::new(
                            divide.with_exprs_and_inputs(vec![], vec![projected_input])?,
                        ),
                    }));
                }

                Ok(plan.clone())
            }
            _ => Ok(plan.clone()),
        }
    }
}
