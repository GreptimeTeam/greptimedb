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

use datafusion::datasource::DefaultTableSource;
use datafusion_common::tree_node::{
    Transformed, TransformedResult, TreeNode, TreeNodeRecursion, TreeNodeVisitor,
};
use datafusion_common::{Column, Result as DataFusionResult, ScalarValue};
use datafusion_expr::expr::{AggregateFunction, WindowFunction};
use datafusion_expr::utils::COUNT_STAR_EXPANSION;
use datafusion_expr::{col, lit, Expr, LogicalPlan, WindowFunctionDefinition};
use datafusion_optimizer::utils::NamePreserver;
use datafusion_optimizer::AnalyzerRule;
use datafusion_sql::TableReference;
use table::table::adapter::DfTableProviderAdapter;

/// A replacement to DataFusion's [`CountWildcardRule`]. This rule
/// would prefer to use TIME INDEX for counting wildcard as it's
/// faster to read comparing to PRIMARY KEYs.
///
/// [`CountWildcardRule`]: datafusion::optimizer::analyzer::CountWildcardRule
#[derive(Debug)]
pub struct CountWildcardToTimeIndexRule;

impl AnalyzerRule for CountWildcardToTimeIndexRule {
    fn name(&self) -> &str {
        "count_wildcard_to_time_index_rule"
    }

    fn analyze(
        &self,
        plan: LogicalPlan,
        _config: &datafusion::config::ConfigOptions,
    ) -> DataFusionResult<LogicalPlan> {
        plan.transform_down_with_subqueries(&Self::analyze_internal)
            .data()
    }
}

impl CountWildcardToTimeIndexRule {
    fn analyze_internal(plan: LogicalPlan) -> DataFusionResult<Transformed<LogicalPlan>> {
        let name_preserver = NamePreserver::new(&plan);
        let new_arg = if let Some(time_index) = Self::try_find_time_index_col(&plan) {
            vec![col(time_index)]
        } else {
            vec![lit(COUNT_STAR_EXPANSION)]
        };
        plan.map_expressions(|expr| {
            let original_name = name_preserver.save(&expr);
            let transformed_expr = expr.transform_up(|expr| match expr {
                Expr::WindowFunction(mut window_function)
                    if Self::is_count_star_window_aggregate(&window_function) =>
                {
                    window_function.params.args.clone_from(&new_arg);
                    Ok(Transformed::yes(Expr::WindowFunction(window_function)))
                }
                Expr::AggregateFunction(mut aggregate_function)
                    if Self::is_count_star_aggregate(&aggregate_function) =>
                {
                    aggregate_function.params.args.clone_from(&new_arg);
                    Ok(Transformed::yes(Expr::AggregateFunction(
                        aggregate_function,
                    )))
                }
                _ => Ok(Transformed::no(expr)),
            })?;
            Ok(transformed_expr.update_data(|data| original_name.restore(data)))
        })
    }

    fn try_find_time_index_col(plan: &LogicalPlan) -> Option<Column> {
        let mut finder = TimeIndexFinder::default();
        // Safety: `TimeIndexFinder` won't throw error.
        plan.visit(&mut finder).unwrap();
        let col = finder.into_column();

        // check if the time index is a valid column as for current plan
        if let Some(col) = &col {
            let mut is_valid = false;
            for input in plan.inputs() {
                if input.schema().has_column(col) {
                    is_valid = true;
                    break;
                }
            }
            if !is_valid {
                return None;
            }
        }

        col
    }
}

/// Utility functions from the original rule.
impl CountWildcardToTimeIndexRule {
    #[expect(deprecated)]
    fn args_at_most_wildcard_or_literal_one(args: &[Expr]) -> bool {
        match args {
            [] => true,
            [Expr::Literal(ScalarValue::Int64(Some(v)), _)] => *v == 1,
            [Expr::Wildcard { .. }] => true,
            _ => false,
        }
    }

    fn is_count_star_aggregate(aggregate_function: &AggregateFunction) -> bool {
        let args = &aggregate_function.params.args;
        matches!(aggregate_function,
            AggregateFunction {
                func,
                ..
            } if func.name() == "count" && Self::args_at_most_wildcard_or_literal_one(args))
    }

    fn is_count_star_window_aggregate(window_function: &WindowFunction) -> bool {
        let args = &window_function.params.args;
        matches!(window_function.fun,
                WindowFunctionDefinition::AggregateUDF(ref udaf)
                    if udaf.name() == "count" && Self::args_at_most_wildcard_or_literal_one(args))
    }
}

#[derive(Default)]
struct TimeIndexFinder {
    time_index_col: Option<String>,
    table_alias: Option<TableReference>,
}

impl TreeNodeVisitor<'_> for TimeIndexFinder {
    type Node = LogicalPlan;

    fn f_down(&mut self, node: &Self::Node) -> DataFusionResult<TreeNodeRecursion> {
        if let LogicalPlan::SubqueryAlias(subquery_alias) = node {
            self.table_alias = Some(subquery_alias.alias.clone());
        }

        if let LogicalPlan::TableScan(table_scan) = &node {
            if let Some(source) = table_scan
                .source
                .as_any()
                .downcast_ref::<DefaultTableSource>()
            {
                if let Some(adapter) = source
                    .table_provider
                    .as_any()
                    .downcast_ref::<DfTableProviderAdapter>()
                {
                    let table_info = adapter.table().table_info();
                    self.table_alias
                        .get_or_insert(TableReference::bare(table_info.name.clone()));
                    self.time_index_col = table_info
                        .meta
                        .schema
                        .timestamp_column()
                        .map(|c| c.name.clone());

                    return Ok(TreeNodeRecursion::Stop);
                }
            }
        }

        Ok(TreeNodeRecursion::Continue)
    }

    fn f_up(&mut self, _node: &Self::Node) -> DataFusionResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Stop)
    }
}

impl TimeIndexFinder {
    fn into_column(self) -> Option<Column> {
        self.time_index_col
            .map(|c| Column::new(self.table_alias, c))
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use datafusion::functions_aggregate::count::count_all;
    use datafusion_expr::LogicalPlanBuilder;
    use table::table::numbers::NumbersTable;

    use super::*;

    #[test]
    fn uppercase_table_name() {
        let numbers_table = NumbersTable::table_with_name(0, "AbCdE".to_string());
        let table_source = Arc::new(DefaultTableSource::new(Arc::new(
            DfTableProviderAdapter::new(numbers_table),
        )));

        let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
            .unwrap()
            .aggregate(Vec::<Expr>::new(), vec![count_all()])
            .unwrap()
            .alias(r#""FgHiJ""#)
            .unwrap()
            .build()
            .unwrap();

        let mut finder = TimeIndexFinder::default();
        plan.visit(&mut finder).unwrap();

        assert_eq!(finder.table_alias, Some(TableReference::bare("FgHiJ")));
        assert!(finder.time_index_col.is_none());
    }
}
