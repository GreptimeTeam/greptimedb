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
use datafusion_common::Result as DataFusionResult;
use datafusion_expr::expr::{AggregateFunction, AggregateFunctionDefinition, WindowFunction};
use datafusion_expr::utils::COUNT_STAR_EXPANSION;
use datafusion_expr::{col, lit, Expr, LogicalPlan, WindowFunctionDefinition};
use datafusion_optimizer::utils::NamePreserver;
use datafusion_optimizer::AnalyzerRule;
use table::table::adapter::DfTableProviderAdapter;

/// A replacement to DataFusion's [`CountWildcardRule`]. This rule
/// would prefer to use TIME INDEX for counting wildcard as it's
/// faster to read comparing to PRIMARY KEYs.
///
/// [`CountWildcardRule`]: datafusion::optimizer::analyzer::CountWildcardRule
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
            let original_name = name_preserver.save(&expr)?;
            let transformed_expr = expr.transform_up_mut(&mut |expr| match expr {
                Expr::WindowFunction(mut window_function)
                    if Self::is_count_star_window_aggregate(&window_function) =>
                {
                    window_function.args = new_arg.clone();
                    Ok(Transformed::yes(Expr::WindowFunction(window_function)))
                }
                Expr::AggregateFunction(mut aggregate_function)
                    if Self::is_count_star_aggregate(&aggregate_function) =>
                {
                    aggregate_function.args = new_arg.clone();
                    Ok(Transformed::yes(Expr::AggregateFunction(
                        aggregate_function,
                    )))
                }
                _ => Ok(Transformed::no(expr)),
            })?;
            transformed_expr.map_data(|data| original_name.restore(data))
        })
    }

    fn try_find_time_index_col(plan: &LogicalPlan) -> Option<String> {
        let mut finder = TimeIndexFinder { time_index: None };
        // Safety: `TimeIndexFinder` won't throw error.
        plan.visit_with_subqueries(&mut finder).unwrap();
        finder.time_index
    }
}

/// Utility functions from the original rule.
impl CountWildcardToTimeIndexRule {
    fn is_wildcard(expr: &Expr) -> bool {
        matches!(expr, Expr::Wildcard { qualifier: None })
    }

    fn is_count_star_aggregate(aggregate_function: &AggregateFunction) -> bool {
        matches!(
            &aggregate_function.func_def,
            AggregateFunctionDefinition::BuiltIn(
                datafusion_expr::aggregate_function::AggregateFunction::Count,
            )
        ) && aggregate_function.args.len() == 1
            && Self::is_wildcard(&aggregate_function.args[0])
    }

    fn is_count_star_window_aggregate(window_function: &WindowFunction) -> bool {
        matches!(
            &window_function.fun,
            WindowFunctionDefinition::AggregateFunction(
                datafusion_expr::aggregate_function::AggregateFunction::Count,
            )
        ) && window_function.args.len() == 1
            && Self::is_wildcard(&window_function.args[0])
    }
}

struct TimeIndexFinder {
    time_index: Option<String>,
}

impl TreeNodeVisitor for TimeIndexFinder {
    type Node = LogicalPlan;

    fn f_down(&mut self, node: &Self::Node) -> DataFusionResult<TreeNodeRecursion> {
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
                    self.time_index = adapter
                        .table()
                        .table_info()
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
