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

use arrow_schema::SortOptions;
use common_recordbatch::OrderOption;
use datafusion::datasource::DefaultTableSource;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeVisitor, VisitRecursion};
use datafusion_common::Result as DataFusionResult;
use datafusion_expr::expr::Sort;
use datafusion_expr::{Expr, LogicalPlan};
use datafusion_optimizer::{OptimizerConfig, OptimizerRule};
use table::table::adapter::DfTableProviderAdapter;

/// This rule will pass the nearest order requirement to the leaf table
/// scan node as ordering hint.
pub struct OrderHintRule;

impl OptimizerRule for OrderHintRule {
    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> DataFusionResult<Option<LogicalPlan>> {
        Self::optimize(plan).map(Some)
    }

    fn name(&self) -> &str {
        "OrderHintRule"
    }
}

impl OrderHintRule {
    fn optimize(plan: &LogicalPlan) -> DataFusionResult<LogicalPlan> {
        let mut visitor = OrderHintVisitor::default();
        let _ = plan.visit(&mut visitor)?;

        if let Some(order_expr) = visitor.order_expr.take() {
            plan.clone()
                .transform_down(&|plan| Self::set_ordering_hint(plan, &order_expr))
        } else {
            Ok(plan.clone())
        }
    }

    fn set_ordering_hint(
        plan: LogicalPlan,
        order_expr: &[Sort],
    ) -> DataFusionResult<Transformed<LogicalPlan>> {
        match &plan {
            LogicalPlan::TableScan(table_scan) => {
                let mut transformed = false;
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
                        let mut opts = Vec::with_capacity(order_expr.len());
                        for sort in order_expr {
                            let name = match sort.expr.try_into_col() {
                                Ok(col) => col.name,
                                Err(_) => return Ok(Transformed::No(plan)),
                            };
                            opts.push(OrderOption {
                                name,
                                options: SortOptions {
                                    descending: !sort.asc,
                                    nulls_first: sort.nulls_first,
                                },
                            })
                        }
                        adapter.with_ordering_hint(&opts);
                        transformed = true;
                    }
                }
                if transformed {
                    Ok(Transformed::Yes(plan))
                } else {
                    Ok(Transformed::No(plan))
                }
            }
            _ => Ok(Transformed::No(plan)),
        }
    }
}

/// Find the most closest order requirement to the leaf node.
#[derive(Default)]
struct OrderHintVisitor {
    order_expr: Option<Vec<Sort>>,
}

impl TreeNodeVisitor for OrderHintVisitor {
    type N = LogicalPlan;

    fn pre_visit(&mut self, node: &Self::N) -> DataFusionResult<VisitRecursion> {
        if let LogicalPlan::Sort(sort) = node {
            let mut exprs = vec![];
            for expr in &sort.expr {
                if let Expr::Sort(sort_expr) = expr {
                    exprs.push(sort_expr.clone());
                }
            }
            self.order_expr = Some(exprs);
        }
        Ok(VisitRecursion::Continue)
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use datafusion_expr::{col, LogicalPlanBuilder};
    use datafusion_optimizer::OptimizerContext;
    use table::table::numbers::NumbersTable;

    use super::*;

    #[test]
    #[allow(clippy::bool_assert_comparison)]
    fn set_order_hint() {
        let numbers_table = NumbersTable::table(0);
        let adapter = Arc::new(DfTableProviderAdapter::new(numbers_table));
        let table_source = Arc::new(DefaultTableSource::new(adapter.clone()));

        let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
            .unwrap()
            .sort(vec![col("number").sort(true, false)])
            .unwrap()
            .sort(vec![col("number").sort(false, true)])
            .unwrap()
            .build()
            .unwrap();

        let context = OptimizerContext::default();
        let _ = OrderHintRule.try_optimize(&plan, &context).unwrap();

        // should read the first (with `.sort(true, false)`) sort option
        let scan_req = adapter.get_scan_req();
        assert_eq!("number", &scan_req.output_ordering.clone().unwrap()[0].name);
        assert_eq!(
            true,
            !scan_req.output_ordering.clone().unwrap()[0]
                .options
                .descending // the previous parameter is `asc`
        );
        assert_eq!(
            false,
            scan_req.output_ordering.unwrap()[0].options.nulls_first
        );
    }
}
