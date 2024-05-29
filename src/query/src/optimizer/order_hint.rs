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
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion, TreeNodeVisitor};
use datafusion_common::Result as DataFusionResult;
use datafusion_expr::expr::Sort;
use datafusion_expr::{Expr, LogicalPlan};
use datafusion_optimizer::{OptimizerConfig, OptimizerRule};

use crate::dummy_catalog::DummyTableProvider;

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
                .map(|x| x.data)
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
                    // The provider in the region server is [DummyTableProvider].
                    if let Some(adapter) = source
                        .table_provider
                        .as_any()
                        .downcast_ref::<DummyTableProvider>()
                    {
                        let mut opts = Vec::with_capacity(order_expr.len());
                        for sort in order_expr {
                            let name = match sort.expr.try_as_col() {
                                Some(col) => col.name.clone(),
                                None => return Ok(Transformed::no(plan)),
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
                    Ok(Transformed::yes(plan))
                } else {
                    Ok(Transformed::no(plan))
                }
            }
            _ => Ok(Transformed::no(plan)),
        }
    }
}

/// Find the most closest order requirement to the leaf node.
#[derive(Default)]
struct OrderHintVisitor {
    order_expr: Option<Vec<Sort>>,
}

impl TreeNodeVisitor<'_> for OrderHintVisitor {
    type Node = LogicalPlan;

    fn f_down(&mut self, node: &Self::Node) -> DataFusionResult<TreeNodeRecursion> {
        if let LogicalPlan::Sort(sort) = node {
            let mut exprs = vec![];
            for expr in &sort.expr {
                if let Expr::Sort(sort_expr) = expr {
                    exprs.push(sort_expr.clone());
                }
            }
            self.order_expr = Some(exprs);
        }
        Ok(TreeNodeRecursion::Continue)
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use datafusion_expr::{col, LogicalPlanBuilder};
    use datafusion_optimizer::OptimizerContext;
    use store_api::storage::RegionId;

    use super::*;
    use crate::optimizer::test_util::mock_table_provider;

    #[test]
    fn set_order_hint() {
        let provider = Arc::new(mock_table_provider(RegionId::new(1, 1)));
        let table_source = Arc::new(DefaultTableSource::new(provider.clone()));
        let plan = LogicalPlanBuilder::scan("t", table_source, None)
            .unwrap()
            .sort(vec![col("ts").sort(true, false)])
            .unwrap()
            .sort(vec![col("ts").sort(false, true)])
            .unwrap()
            .build()
            .unwrap();

        let context = OptimizerContext::default();
        OrderHintRule.try_optimize(&plan, &context).unwrap();

        // should read the first (with `.sort(true, false)`) sort option
        let scan_req = provider.scan_request();
        assert_eq!(
            OrderOption {
                name: "ts".to_string(),
                options: SortOptions {
                    descending: false,
                    nulls_first: false
                }
            },
            scan_req.output_ordering.as_ref().unwrap()[0]
        );
    }
}
