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

use common_function::scalars::matches_term::MatchesTermFunction;
use common_function::scalars::udf::create_udf;
use common_function::state::FunctionState;
use datafusion::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRewriter};
use datafusion_common::Result;
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::{Expr, LogicalPlan};
use datafusion_optimizer::analyzer::AnalyzerRule;
use session::context::QueryContext;

use crate::plan::ExtractExpr;

/// TranscribeAtatRule is an analyzer rule that transcribes `@@` operator
/// to `matches_term` function.
///
/// Example:
/// ```sql
/// SELECT matches_term('cat!', 'cat') as result;
///
/// SELECT matches_term(`log_message`, '/start') as `matches_start` FROM t;
/// ```
///
/// to
///
/// ```sql
/// SELECT 'cat!' @@ 'cat' as result;
///
/// SELECT `log_message` @@ '/start' as `matches_start` FROM t;
/// ```
#[derive(Debug)]
pub struct TranscribeAtatRule;

impl AnalyzerRule for TranscribeAtatRule {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform(Self::do_analyze).map(|x| x.data)
    }

    fn name(&self) -> &str {
        "TranscribeAtatRule"
    }
}

impl TranscribeAtatRule {
    fn do_analyze(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        let mut rewriter = TranscribeAtatRewriter::default();
        let new_expr = plan
            .expressions_consider_join()
            .into_iter()
            .map(|e| e.rewrite(&mut rewriter).map(|x| x.data))
            .collect::<Result<Vec<_>>>()?;

        if rewriter.transcribed {
            let inputs = plan.inputs().into_iter().cloned().collect::<Vec<_>>();
            plan.with_new_exprs(new_expr, inputs).map(Transformed::yes)
        } else {
            Ok(Transformed::no(plan))
        }
    }
}

#[derive(Default)]
struct TranscribeAtatRewriter {
    transcribed: bool,
}

impl TreeNodeRewriter for TranscribeAtatRewriter {
    type Node = Expr;

    fn f_up(&mut self, expr: Expr) -> Result<Transformed<Expr>> {
        if let Expr::BinaryExpr(binary_expr) = &expr
            && matches!(binary_expr.op, datafusion_expr::Operator::AtAt)
        {
            self.transcribed = true;
            let scalar_udf = create_udf(
                Arc::new(MatchesTermFunction),
                QueryContext::arc(),
                Arc::new(FunctionState::default()),
            );
            let exprs = vec![
                binary_expr.left.as_ref().clone(),
                binary_expr.right.as_ref().clone(),
            ];
            Ok(Transformed::yes(Expr::ScalarFunction(
                ScalarFunction::new_udf(Arc::new(scalar_udf), exprs),
            )))
        } else {
            Ok(Transformed::no(expr))
        }
    }
}
#[cfg(test)]
mod tests {

    use arrow_schema::SchemaRef;
    use datafusion::datasource::{provider_as_source, MemTable};
    use datafusion::logical_expr::{col, lit, LogicalPlan, LogicalPlanBuilder};
    use datafusion_expr::{BinaryExpr, Operator};
    use datatypes::arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    fn optimize(plan: LogicalPlan) -> Result<LogicalPlan> {
        TranscribeAtatRule.analyze(plan, &ConfigOptions::default())
    }

    fn prepare_test_plan_builder() -> LogicalPlanBuilder {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Utf8, false),
            Field::new("b", DataType::Utf8, false),
        ]);
        let table = MemTable::try_new(SchemaRef::from(schema), vec![]).unwrap();
        LogicalPlanBuilder::scan("t", provider_as_source(Arc::new(table)), None).unwrap()
    }

    #[test]
    fn test_multiple_atat() {
        let plan = prepare_test_plan_builder()
            .filter(
                Expr::BinaryExpr(BinaryExpr {
                    left: Box::new(col("a")),
                    op: Operator::AtAt,
                    right: Box::new(lit("foo")),
                })
                .and(Expr::BinaryExpr(BinaryExpr {
                    left: Box::new(col("b")),
                    op: Operator::AtAt,
                    right: Box::new(lit("bar")),
                })),
            )
            .unwrap()
            .project(vec![
                Expr::BinaryExpr(BinaryExpr {
                    left: Box::new(col("a")),
                    op: Operator::AtAt,
                    right: Box::new(col("b")),
                }),
                col("b"),
            ])
            .unwrap()
            .build()
            .unwrap();

        let expected = r#"Projection: matches_term(t.a, t.b), t.b
  Filter: matches_term(t.a, Utf8("foo")) AND matches_term(t.b, Utf8("bar"))
    TableScan: t"#;

        let optimized_plan = optimize(plan).unwrap();
        let formatted = optimized_plan.to_string();

        assert_eq!(formatted, expected);
    }

    #[test]
    fn test_nested_atat() {
        let plan = prepare_test_plan_builder()
            .filter(
                Expr::BinaryExpr(BinaryExpr {
                    left: Box::new(col("a")),
                    op: Operator::AtAt,
                    right: Box::new(lit("foo")),
                })
                .and(
                    Expr::BinaryExpr(BinaryExpr {
                        left: Box::new(col("b")),
                        op: Operator::AtAt,
                        right: Box::new(lit("bar")),
                    })
                    .or(Expr::BinaryExpr(BinaryExpr {
                        left: Box::new(
                            // Nested case in function argument
                            Expr::BinaryExpr(BinaryExpr {
                                left: Box::new(col("a")),
                                op: Operator::AtAt,
                                right: Box::new(lit("nested")),
                            }),
                        ),
                        op: Operator::Eq,
                        right: Box::new(lit(true)),
                    })),
                ),
            )
            .unwrap()
            .project(vec![
                col("a"),
                // Complex nested expression with multiple @@ operators
                Expr::BinaryExpr(BinaryExpr {
                    left: Box::new(Expr::BinaryExpr(BinaryExpr {
                        left: Box::new(col("a")),
                        op: Operator::AtAt,
                        right: Box::new(lit("foo")),
                    })),
                    op: Operator::And,
                    right: Box::new(Expr::BinaryExpr(BinaryExpr {
                        left: Box::new(col("b")),
                        op: Operator::AtAt,
                        right: Box::new(lit("bar")),
                    })),
                }),
            ])
            .unwrap()
            .build()
            .unwrap();

        let expected = r#"Projection: t.a, matches_term(t.a, Utf8("foo")) AND matches_term(t.b, Utf8("bar"))
  Filter: matches_term(t.a, Utf8("foo")) AND (matches_term(t.b, Utf8("bar")) OR matches_term(t.a, Utf8("nested")) = Boolean(true))
    TableScan: t"#;

        let optimized_plan = optimize(plan).unwrap();
        let formatted = optimized_plan.to_string();

        assert_eq!(formatted, expected);
    }
}
