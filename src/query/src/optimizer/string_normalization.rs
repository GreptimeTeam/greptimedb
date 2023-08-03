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
use datafusion::logical_expr::expr::Cast;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRewriter};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{Expr, LogicalPlan};
use datafusion_optimizer::analyzer::AnalyzerRule;

/// StringNormalizationRule normalizes(trims) string values in logical plan.
/// Mainly used for timestamp trimming
pub struct StringNormalizationRule;

impl AnalyzerRule for StringNormalizationRule {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform(&|plan| {
            let mut converter = StringNormalizationConverter;
            let inputs = plan.inputs().into_iter().cloned().collect::<Vec<_>>();
            let expr = plan
                .expressions()
                .into_iter()
                .map(|e| e.rewrite(&mut converter))
                .collect::<Result<Vec<_>>>()?;
            datafusion_expr::utils::from_plan(&plan, &expr, &inputs).map(Transformed::Yes)
        })
    }

    fn name(&self) -> &str {
        "StringNormalizationRule"
    }
}

struct StringNormalizationConverter;

impl TreeNodeRewriter for StringNormalizationConverter {
    type N = Expr;

    fn mutate(&mut self, expr: Expr) -> Result<Expr> {
        let new_expr = match expr {
            Expr::Cast(Cast { expr, data_type }) => {
                let expr = match *expr {
                    Expr::Literal(value) => match value {
                        ScalarValue::Utf8(Some(s)) => trim_utf_expr(s),
                        _ => Expr::Literal(value),
                    },
                    expr => expr,
                };
                Expr::Cast(Cast {
                    expr: Box::new(expr),
                    data_type,
                })
            }
            expr => expr,
        };
        Ok(new_expr)
    }
}

fn trim_utf_expr(s: String) -> Expr {
    let parts: Vec<_> = s.split_whitespace().collect();
    let trimmed = parts.join(" ");
    Expr::Literal(ScalarValue::Utf8(Some(trimmed)))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, StringArray};
    use arrow::datatypes::TimeUnit::Nanosecond;
    use arrow::datatypes::{DataType, SchemaRef};
    use arrow::record_batch::RecordBatch;
    use datafusion::datasource::{provider_as_source, MemTable};
    use datafusion_common::config::ConfigOptions;
    use datafusion_expr::{lit, Cast, Expr, LogicalPlanBuilder};
    use datafusion_optimizer::analyzer::AnalyzerRule;

    use crate::optimizer::string_normalization::StringNormalizationRule;

    #[test]
    fn test_string_normalization_with_extra_spaces() {
        let proj = vec![Expr::Cast(Cast::new(
            Box::new(lit("    2017-07-23    13:10:11   ")),
            DataType::Timestamp(Nanosecond, None),
        ))];
        let plan = prepare_test_plan_builder()
            .project(proj)
            .unwrap()
            .build()
            .unwrap();

        let result = StringNormalizationRule {}
            .analyze(plan, &ConfigOptions::default())
            .unwrap();
        let expected = String::from(
               "Projection: CAST(Utf8(\"2017-07-23 13:10:11\") AS Timestamp(Nanosecond, None))\n  TableScan: t"
        );
        assert_eq!(expected, format!("{:?}", result));
    }

    fn prepare_test_plan_builder() -> LogicalPlanBuilder {
        let batch = RecordBatch::try_from_iter_with_nullable(vec![
            ("f1", to_string_array(&["foo", "bar"]), true),
            ("f2", to_string_array(&["baz", "bzz"]), true),
        ])
        .expect("created new record batch");
        let schema: SchemaRef = batch.schema();
        let data: Vec<RecordBatch> = vec![batch];
        let table = MemTable::try_new(schema, vec![data]).unwrap();
        LogicalPlanBuilder::scan("t", provider_as_source(Arc::new(table)), None).unwrap()
    }

    fn to_string_array(strs: &[&str]) -> ArrayRef {
        let array: StringArray = strs.iter().map(|s| Some(*s)).collect();
        Arc::new(array)
    }
}
