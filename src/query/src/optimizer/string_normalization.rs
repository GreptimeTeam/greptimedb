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

use arrow_schema::DataType;
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::expr::Cast;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRewriter};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{Expr, LogicalPlan};
use datafusion_optimizer::analyzer::AnalyzerRule;

use crate::plan::ExtractExpr;

/// StringNormalizationRule normalizes(trims) string values in logical plan.
/// Mainly used for timestamp trimming
#[derive(Debug)]
pub struct StringNormalizationRule;

impl AnalyzerRule for StringNormalizationRule {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform(|plan| match plan {
            LogicalPlan::Projection(_)
            | LogicalPlan::Filter(_)
            | LogicalPlan::Window(_)
            | LogicalPlan::Aggregate(_)
            | LogicalPlan::Sort(_)
            | LogicalPlan::Join(_)
            | LogicalPlan::Repartition(_)
            | LogicalPlan::Union(_)
            | LogicalPlan::TableScan(_)
            | LogicalPlan::EmptyRelation(_)
            | LogicalPlan::Subquery(_)
            | LogicalPlan::SubqueryAlias(_)
            | LogicalPlan::Statement(_)
            | LogicalPlan::Values(_)
            | LogicalPlan::Analyze(_)
            | LogicalPlan::Extension(_)
            | LogicalPlan::Dml(_)
            | LogicalPlan::Copy(_)
            | LogicalPlan::RecursiveQuery(_) => {
                let mut converter = StringNormalizationConverter;
                let inputs = plan.inputs().into_iter().cloned().collect::<Vec<_>>();
                let expr = plan
                    .expressions_consider_join()
                    .into_iter()
                    .map(|e| e.rewrite(&mut converter).map(|x| x.data))
                    .collect::<Result<Vec<_>>>()?;
                if expr != plan.expressions_consider_join() {
                    plan.with_new_exprs(expr, inputs).map(Transformed::yes)
                } else {
                    Ok(Transformed::no(plan))
                }
            }
            LogicalPlan::Distinct(_)
            | LogicalPlan::Limit(_)
            | LogicalPlan::Explain(_)
            | LogicalPlan::Unnest(_)
            | LogicalPlan::Ddl(_)
            | LogicalPlan::DescribeTable(_) => Ok(Transformed::no(plan)),
        })
        .map(|x| x.data)
    }

    fn name(&self) -> &str {
        "StringNormalizationRule"
    }
}

struct StringNormalizationConverter;

impl TreeNodeRewriter for StringNormalizationConverter {
    type Node = Expr;

    /// remove extra whitespaces from the String value when
    /// there is a CAST from a String to Timestamp.
    /// Otherwise - no modifications applied
    fn f_up(&mut self, expr: Expr) -> Result<Transformed<Expr>> {
        let new_expr = match expr {
            Expr::Cast(Cast { expr, data_type }) => {
                let expr = match data_type {
                    DataType::Timestamp(_, _) => match *expr {
                        Expr::Literal(value, _) => match value {
                            ScalarValue::Utf8(Some(s)) => trim_utf_expr(s),
                            _ => Expr::Literal(value, None),
                        },
                        expr => expr,
                    },
                    _ => *expr,
                };
                Expr::Cast(Cast {
                    expr: Box::new(expr),
                    data_type,
                })
            }
            expr => expr,
        };
        Ok(Transformed::yes(new_expr))
    }
}

fn trim_utf_expr(s: String) -> Expr {
    let parts: Vec<_> = s.split_whitespace().collect();
    let trimmed = parts.join(" ");
    Expr::Literal(ScalarValue::Utf8(Some(trimmed)), None)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::TimeUnit::{Microsecond, Millisecond, Nanosecond, Second};
    use arrow::datatypes::{DataType, SchemaRef};
    use arrow_schema::{Field, Schema, TimeUnit};
    use datafusion::datasource::{provider_as_source, MemTable};
    use datafusion_common::config::ConfigOptions;
    use datafusion_expr::{lit, Cast, Expr, LogicalPlan, LogicalPlanBuilder};
    use datafusion_optimizer::analyzer::AnalyzerRule;

    use crate::optimizer::string_normalization::StringNormalizationRule;

    #[test]
    fn test_normalization_for_string_with_extra_whitespaces_to_timestamp_cast() {
        let timestamp_str_with_whitespaces = "    2017-07-23    13:10:11   ";
        let config = &ConfigOptions::default();
        let projects = vec![
            create_timestamp_cast_project(Nanosecond, timestamp_str_with_whitespaces),
            create_timestamp_cast_project(Microsecond, timestamp_str_with_whitespaces),
            create_timestamp_cast_project(Millisecond, timestamp_str_with_whitespaces),
            create_timestamp_cast_project(Second, timestamp_str_with_whitespaces),
        ];
        for (time_unit, proj) in projects {
            let plan = create_test_plan_with_project(proj);
            let result = StringNormalizationRule.analyze(plan, config).unwrap();
            let expected = format!("Projection: CAST(Utf8(\"2017-07-23 13:10:11\") AS Timestamp({:#?}, None))\n  TableScan: t",
                                   time_unit
            );
            assert_eq!(expected, result.to_string());
        }
    }

    #[test]
    fn test_normalization_for_non_timestamp_casts() {
        let config = &ConfigOptions::default();
        let proj_int_to_timestamp = vec![Expr::Cast(Cast::new(
            Box::new(lit(158412331400600000_i64)),
            DataType::Timestamp(Nanosecond, None),
        ))];
        let int_to_timestamp_plan = create_test_plan_with_project(proj_int_to_timestamp);
        let result = StringNormalizationRule
            .analyze(int_to_timestamp_plan, config)
            .unwrap();
        let expected = String::from(
            "Projection: CAST(Int64(158412331400600000) AS Timestamp(Nanosecond, None))\n  TableScan: t"
        );
        assert_eq!(expected, result.to_string());

        let proj_string_to_int = vec![Expr::Cast(Cast::new(
            Box::new(lit("  5   ")),
            DataType::Int32,
        ))];
        let string_to_int_plan = create_test_plan_with_project(proj_string_to_int);
        let result = StringNormalizationRule
            .analyze(string_to_int_plan, &ConfigOptions::default())
            .unwrap();
        let expected = String::from("Projection: CAST(Utf8(\"  5   \") AS Int32)\n  TableScan: t");
        assert_eq!(expected, result.to_string());
    }

    fn create_test_plan_with_project(proj: Vec<Expr>) -> LogicalPlan {
        prepare_test_plan_builder()
            .project(proj)
            .unwrap()
            .build()
            .unwrap()
    }

    fn create_timestamp_cast_project(unit: TimeUnit, timestamp_str: &str) -> (TimeUnit, Vec<Expr>) {
        let proj = vec![Expr::Cast(Cast::new(
            Box::new(lit(timestamp_str)),
            DataType::Timestamp(unit, None),
        ))];
        (unit, proj)
    }

    fn prepare_test_plan_builder() -> LogicalPlanBuilder {
        let schema = Schema::new(vec![Field::new("f", DataType::Float64, false)]);
        let table = MemTable::try_new(SchemaRef::from(schema), vec![]).unwrap();
        LogicalPlanBuilder::scan("t", provider_as_source(Arc::new(table)), None).unwrap()
    }
}
