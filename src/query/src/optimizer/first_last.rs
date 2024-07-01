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

//! Optimize rule to push down first/last row function.

use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion, TreeNodeVisitor};
use datafusion_common::Result;
use datafusion_expr::expr::{AggregateFunction, AggregateFunctionDefinition};
use datafusion_expr::{aggregate_function, Expr, LogicalPlan};
use datafusion_optimizer::{OptimizerConfig, OptimizerRule};

/// This rule pushes down `last_value`/`first_value` function as a hint to the
/// leaf table scan node.
pub struct FirstLastPushDownRule;

impl OptimizerRule for FirstLastPushDownRule {
    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        let mut visitor = TopValueVisitor::default();
        plan.visit(&mut visitor)?;

        // if let Some(is_last) = visitor.is_last {
        //     let new_plan = plan.clone();
        //     let new_plan = new_plan.transform_down(&|plan| {
        //         Self::set_top_value_hint(plan, &visitor.group_expr, is_last)
        //     })?;

        //     Ok(Some(new_plan))
        // } else {
        //     Ok(Some(plan.clone()))
        // }

        Ok(Some(plan.clone()))
    }

    fn name(&self) -> &str {
        "FirstLastPushDownRule"
    }
}

/// Find the most closest first/last value function to the leaf node.
#[derive(Default)]
struct TopValueVisitor {
    group_expr: Vec<Expr>,
    is_last: Option<bool>,
}

impl TreeNodeVisitor<'_> for TopValueVisitor {
    type Node = LogicalPlan;

    fn f_down(&mut self, node: &Self::Node) -> Result<TreeNodeRecursion> {
        if let LogicalPlan::Aggregate(aggregate) = node {
            common_telemetry::info!("group is {:?}", aggregate.group_expr);
            // TODO(yingwen): Support first value.
            for expr in &aggregate.aggr_expr {
                let Expr::AggregateFunction(func) = expr else {
                    self.group_expr.clear();
                    self.is_last = None;
                    break;
                };
                common_telemetry::info!("func is {:?}", func);
                // match func {
                //     AggregateFunction {
                //         func_def: AggregateFunctionDefinition::BuiltIn(aggregate_function::AggregateFunction::),
                //         args: _,
                //         distinct: false,
                //         filter: None,
                //         order_by: None,
                //         null_treatment: None,
                //     } => {
                //         // TODO(yingwen): check args.
                //         self.is_last = Some(true);
                //     }
                //     _ => {
                //         self.group_expr.clear();
                //         self.is_last = None;
                //         break;
                //     }
                // }
            }
            if self.is_last.is_some() {
                self.group_expr = aggregate.group_expr.clone();
            }
        }

        Ok(TreeNodeRecursion::Continue)
    }
}
