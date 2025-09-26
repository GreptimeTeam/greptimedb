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

use std::sync::Arc;

use common_telemetry::debug;
use datafusion::config::ConfigOptions;
use datafusion::optimizer::AnalyzerRule;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRewriter};
use datafusion_expr::{AggregateUDF, Expr, ExprSchemable, LogicalPlan};

use crate::aggrs::aggr_wrapper::StateWrapper;

/// Traverse the plan, found all `__<aggr_name>_state` and fix their ordering fields
/// if their input aggr is with order by, this is currently only useful for `first_value` and `last_value` udaf
///
/// should be applied to datanode's query engine
/// TODO(discord9): proper way to extend substrait's serde ability to allow carry more info for custom udaf with more info
#[derive(Debug, Default)]
pub struct FixStateUdafOrderingAnalyzer;

impl AnalyzerRule for FixStateUdafOrderingAnalyzer {
    fn name(&self) -> &str {
        "FixStateUdafOrderingAnalyzer"
    }

    fn analyze(
        &self,
        plan: LogicalPlan,
        _config: &ConfigOptions,
    ) -> datafusion_common::Result<LogicalPlan> {
        plan.rewrite_with_subqueries(&mut FixOrderingRewriter::new(true))
            .map(|t| t.data)
    }
}

/// Traverse the plan, found all `__<aggr_name>_state` and remove their ordering fields
/// this is currently only useful for `first_value` and `last_value` udaf when need to encode to substrait
///
#[derive(Debug, Default)]
pub struct UnFixStateUdafOrderingAnalyzer;

impl AnalyzerRule for UnFixStateUdafOrderingAnalyzer {
    fn name(&self) -> &str {
        "UnFixStateUdafOrderingAnalyzer"
    }

    fn analyze(
        &self,
        plan: LogicalPlan,
        _config: &ConfigOptions,
    ) -> datafusion_common::Result<LogicalPlan> {
        plan.rewrite_with_subqueries(&mut FixOrderingRewriter::new(false))
            .map(|t| t.data)
    }
}

struct FixOrderingRewriter {
    /// once fixed, mark dirty, and always recompute schema from bottom up
    is_dirty: bool,
    /// if true, will add the ordering field from outer aggr expr
    /// if false, will remove the ordering field
    is_fix: bool,
}

impl FixOrderingRewriter {
    pub fn new(is_fix: bool) -> Self {
        Self {
            is_dirty: false,
            is_fix,
        }
    }
}

impl TreeNodeRewriter for FixOrderingRewriter {
    type Node = LogicalPlan;

    /// found all `__<aggr_name>_state` and fix their ordering fields
    /// if their input aggr is with order by
    fn f_up(
        &mut self,
        node: Self::Node,
    ) -> datafusion_common::Result<datafusion_common::tree_node::Transformed<Self::Node>> {
        let LogicalPlan::Aggregate(mut aggregate) = node else {
            return if self.is_dirty {
                let node = node.recompute_schema()?;
                Ok(Transformed::yes(node))
            } else {
                Ok(Transformed::no(node))
            };
        };

        // regex to match state udaf name
        for aggr_expr in &mut aggregate.aggr_expr {
            let new_aggr_expr = aggr_expr
                .clone()
                .transform_up(|expr| rewrite_expr(expr, &aggregate.input, self.is_fix))?;

            if new_aggr_expr.transformed {
                *aggr_expr = new_aggr_expr.data;
                self.is_dirty = true;
            }
        }

        if self.is_dirty {
            let node = LogicalPlan::Aggregate(aggregate).recompute_schema()?;
            debug!(
                "FixStateUdafOrderingAnalyzer: plan schema's field changed to {:?}",
                node.schema().fields()
            );

            Ok(Transformed::yes(node))
        } else {
            Ok(Transformed::no(LogicalPlan::Aggregate(aggregate)))
        }
    }
}

/// first see the aggr node in expr
/// as it could be nested aggr like alias(aggr(sort))
/// if contained aggr expr have a order by, and the aggr name match the regex
/// then we need to fix the ordering field of the state udaf
/// to be the same as the aggr expr
fn rewrite_expr(
    expr: Expr,
    aggregate_input: &Arc<LogicalPlan>,
    is_fix: bool,
) -> Result<Transformed<Expr>, datafusion_common::DataFusionError> {
    let Expr::AggregateFunction(aggregate_function) = expr else {
        return Ok(Transformed::no(expr));
    };

    let Some(old_state_wrapper) = aggregate_function
        .func
        .inner()
        .as_any()
        .downcast_ref::<StateWrapper>()
    else {
        return Ok(Transformed::no(Expr::AggregateFunction(aggregate_function)));
    };

    let mut state_wrapper = old_state_wrapper.clone();
    if is_fix {
        // then always fix the ordering field&distinct flag and more
        let order_by = aggregate_function.params.order_by.clone();
        let ordering_fields: Vec<_> = order_by
            .iter()
            .map(|sort_expr| {
                sort_expr
                    .expr
                    .to_field(&aggregate_input.schema())
                    .map(|(_, f)| f)
            })
            .collect::<datafusion_common::Result<Vec<_>>>()?;
        let distinct = aggregate_function.params.distinct;

        // fixing up
        state_wrapper.ordering = ordering_fields;
        state_wrapper.distinct = distinct;
    } else {
        // remove the ordering field & distinct flag
        state_wrapper.ordering = vec![];
        state_wrapper.distinct = false;
    }

    debug!(
        "FixStateUdafOrderingAnalyzer: fix state udaf from {old_state_wrapper:?} to {:?}",
        state_wrapper
    );

    let mut aggregate_function = aggregate_function;

    aggregate_function.func = Arc::new(AggregateUDF::new_from_impl(state_wrapper));

    Ok(Transformed::yes(Expr::AggregateFunction(
        aggregate_function,
    )))
}
