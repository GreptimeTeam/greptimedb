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

use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_common::{Result, plan_err};
use datafusion_expr::{Expr, LogicalPlan, Partitioning};
use datafusion_optimizer::analyzer::AnalyzerRule;
use promql::extension_plan::EmptyMetric;

use crate::dist_plan::{MergeScanLogicalPlan, MergeSortLogicalPlan};
use crate::range_select::plan::RangeSelect;

// A conservative optimizer input limit, not a guarantee for every recursive operation
// performed while parsing, planning, or dropping an arbitrarily deep query.
const MAX_EXPR_DEPTH: usize = 128;

/// Checks the analyzer output, including the SessionState EXPLAIN path.
#[derive(Debug)]
pub(crate) struct ExprDepthRule;

impl AnalyzerRule for ExprDepthRule {
    fn name(&self) -> &str {
        "expr_depth"
    }

    fn analyze(&self, plan: LogicalPlan, _: &ConfigOptions) -> Result<LogicalPlan> {
        validate_plan(&plan)?;
        Ok(plan)
    }
}

/// Rejects deep expressions before recursive optimizer traversal, cloning, or display.
/// Both plan and expression traversal are iterative. Subquery Arcs are retained so
/// expressions supplied temporarily by extension nodes can also be inspected safely.
pub(crate) fn validate_plan(plan: &LogicalPlan) -> Result<()> {
    let mut subqueries = Vec::new();
    validate_plan_exprs(plan, &mut subqueries)?;
    while let Some(plan) = subqueries.pop() {
        validate_plan_exprs(&plan, &mut subqueries)?;
    }
    Ok(())
}

fn validate_plan_exprs(plan: &LogicalPlan, subqueries: &mut Vec<Arc<LogicalPlan>>) -> Result<()> {
    let mut plans = vec![plan];
    while let Some(plan) = plans.pop() {
        let mut check = |expr: &Expr| {
            validate_expr(expr, subqueries)?;
            Ok(TreeNodeRecursion::Continue)
        };
        match plan {
            // apply_expressions clones range ordering expressions and extension
            // expressions. Borrow the built-in nodes that can contain deep trees.
            LogicalPlan::Repartition(repartition)
                if let Partitioning::Range(range) = &repartition.partitioning_scheme =>
            {
                for sort in range.ordering() {
                    check(&sort.expr)?;
                }
            }
            LogicalPlan::Extension(extension) => {
                let node = extension.node.as_any();
                if let Some(range) = node.downcast_ref::<RangeSelect>() {
                    for expr in &range.range_expr {
                        check(&expr.expr)?;
                    }
                    check(&range.time_expr)?;
                    for expr in &range.by {
                        check(expr)?;
                    }
                } else if let Some(sort) = node.downcast_ref::<MergeSortLogicalPlan>() {
                    for expr in &sort.expr {
                        check(&expr.expr)?;
                    }
                } else if let Some(metric) = node.downcast_ref::<EmptyMetric>() {
                    if let Some(expr) = metric.expr() {
                        check(expr)?;
                    }
                } else if let Some(scan) = node.downcast_ref::<MergeScanLogicalPlan>() {
                    // MergeScan deliberately hides this input from optimizer visitors.
                    plans.push(scan.input());
                } else {
                    // The remaining built-in nodes only expose column expressions.
                    // External extensions own the safety of their expressions() implementation.
                    plan.apply_expressions(check)?;
                }
            }
            LogicalPlan::Subquery(subquery) => {
                for expr in &subquery.outer_ref_columns {
                    check(expr)?;
                }
            }
            _ => {
                plan.apply_expressions(check)?;
            }
        }
        plans.extend(plan.inputs());
    }
    Ok(())
}

fn validate_expr(expr: &Expr, subqueries: &mut Vec<Arc<LogicalPlan>>) -> Result<()> {
    let mut expressions = vec![(expr, 1)];
    while let Some((expr, depth)) = expressions.pop() {
        if depth > MAX_EXPR_DEPTH {
            return plan_err!(
                "Expression depth exceeds the maximum supported depth of {MAX_EXPR_DEPTH}"
            );
        }
        let subquery = match expr {
            Expr::Exists(exists) => Some(&exists.subquery),
            Expr::ScalarSubquery(subquery) => Some(subquery),
            Expr::InSubquery(in_subquery) => Some(&in_subquery.subquery),
            Expr::SetComparison(comparison) => Some(&comparison.subquery),
            _ => None,
        };
        if let Some(subquery) = subquery {
            subqueries.push(Arc::clone(&subquery.subquery));
            expressions.extend(subquery.outer_ref_columns.iter().map(|expr| (expr, 1)));
        }
        expr.apply_children(|child| {
            expressions.push((child, depth + 1));
            Ok(TreeNodeRecursion::Continue)
        })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use arrow_schema::{DataType, Field, Schema};
    use datafusion_common::DFSchema;
    use datafusion_expr::expr::{Exists, InSubquery, SetComparison, SetQuantifier};
    use datafusion_expr::{EmptyRelation, Filter, Operator, Subquery, lit};

    use super::*;

    fn filter(predicate: Expr) -> LogicalPlan {
        LogicalPlan::Filter(
            Filter::try_new(
                predicate,
                Arc::new(LogicalPlan::EmptyRelation(EmptyRelation {
                    produce_one_row: true,
                    schema: Arc::new(
                        DFSchema::try_from(Schema::new(vec![Field::new(
                            "value",
                            DataType::Boolean,
                            false,
                        )]))
                        .unwrap(),
                    ),
                })),
            )
            .unwrap(),
        )
    }

    #[test]
    fn test_depth_in_subqueries_and_hidden_input() {
        std::thread::Builder::new()
            .stack_size(2 * 1024 * 1024)
            .spawn(|| {
                for depth in [MAX_EXPR_DEPTH, MAX_EXPR_DEPTH + 1, 512] {
                    let mut predicate = lit(true);
                    for _ in 1..depth {
                        predicate = predicate.or(lit(false));
                    }
                    // MergeScan hides its input, and apply_children on an Expr
                    // does not visit the plan embedded in its subquery.
                    let plan = MergeScanLogicalPlan::new(filter(predicate), false)
                        .into_logical_plan();
                    let subquery = Subquery {
                        subquery: Arc::new(plan),
                        outer_ref_columns: vec![],
                        spans: Default::default(),
                    };
                    for expr in [
                        Expr::Exists(Exists::new(subquery.clone(), false)),
                        Expr::ScalarSubquery(subquery.clone()),
                        Expr::InSubquery(InSubquery::new(
                            Box::new(lit(true)),
                            subquery.clone(),
                            false,
                        )),
                        Expr::SetComparison(SetComparison::new(
                            Box::new(lit(true)),
                            subquery.clone(),
                            Operator::Eq,
                            SetQuantifier::Any,
                        )),
                    ] {
                        let result = validate_plan(&filter(expr));
                        if depth <= MAX_EXPR_DEPTH {
                            result.unwrap();
                        } else {
                            assert!(matches!(
                                result,
                                Err(datafusion_common::DataFusionError::Plan(message))
                                    if message == "Expression depth exceeds the maximum supported depth of 128"
                            ));
                        }
                    }
                }
            })
            .unwrap()
            .join()
            .unwrap();
    }
}
