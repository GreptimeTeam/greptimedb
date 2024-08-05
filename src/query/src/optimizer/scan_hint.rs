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

use std::collections::HashSet;

use api::v1::SemanticType;
use arrow_schema::SortOptions;
use common_recordbatch::OrderOption;
use datafusion::datasource::DefaultTableSource;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion, TreeNodeVisitor};
use datafusion_common::{Column, Result as DataFusionResult};
use datafusion_expr::expr::Sort;
use datafusion_expr::{utils, Expr, LogicalPlan};
use datafusion_optimizer::{OptimizerConfig, OptimizerRule};
use store_api::storage::TimeSeriesRowSelector;

use crate::dummy_catalog::DummyTableProvider;

/// This rule will traverse the plan to collect necessary hints for leaf
/// table scan node and set them in [`ScanRequest`]. Hints include:
/// - the nearest order requirement to the leaf table scan node as ordering hint.
/// - the group by columns when all aggregate functions are `last_value` as
///   time series row selector hint.
///
/// [`ScanRequest`]: store_api::storage::ScanRequest
pub struct ScanHintRule;

impl OptimizerRule for ScanHintRule {
    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> DataFusionResult<Option<LogicalPlan>> {
        Self::optimize(plan).map(Some)
    }

    fn name(&self) -> &str {
        "ScanHintRule"
    }
}

impl ScanHintRule {
    fn optimize(plan: &LogicalPlan) -> DataFusionResult<LogicalPlan> {
        let mut visitor = ScanHintVisitor::default();
        let _ = plan.visit(&mut visitor)?;

        if visitor.need_rewrite() {
            plan.clone()
                .transform_down(&|plan| Self::set_hints(plan, &visitor))
                .map(|x| x.data)
        } else {
            Ok(plan.clone())
        }
    }

    fn set_hints(
        plan: LogicalPlan,
        visitor: &ScanHintVisitor,
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
                        // set order_hint
                        if let Some(order_expr) = &visitor.order_expr {
                            Self::set_order_hint(adapter, order_expr);
                        }

                        // set time series selector hint
                        if let Some((group_by_cols, order_by_col)) = &visitor.ts_row_selector {
                            Self::set_time_series_row_selector_hint(
                                adapter,
                                group_by_cols,
                                order_by_col,
                            );
                        }

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

    fn set_order_hint(adapter: &DummyTableProvider, order_expr: &Vec<Sort>) {
        let mut opts = Vec::with_capacity(order_expr.len());
        for sort in order_expr {
            let name = match sort.expr.try_as_col() {
                Some(col) => col.name.clone(),
                None => return,
            };
            opts.push(OrderOption {
                name,
                options: SortOptions {
                    descending: !sort.asc,
                    nulls_first: sort.nulls_first,
                },
            });
        }
        adapter.with_ordering_hint(&opts);
    }

    fn set_time_series_row_selector_hint(
        adapter: &DummyTableProvider,
        group_by_cols: &HashSet<Column>,
        order_by_col: &Column,
    ) {
        let region_metadata = adapter.region_metadata();
        let mut should_set_selector_hint = true;
        // check if order_by column is time index
        if let Some(column_metadata) = region_metadata.column_by_name(&order_by_col.name) {
            if column_metadata.semantic_type != SemanticType::Timestamp {
                should_set_selector_hint = false;
            }
        } else {
            should_set_selector_hint = false;
        }

        // check if all group_by columns are primary key
        for col in group_by_cols {
            let Some(column_metadata) = region_metadata.column_by_name(&col.name) else {
                should_set_selector_hint = false;
                break;
            };
            if column_metadata.semantic_type != SemanticType::Tag {
                should_set_selector_hint = false;
                break;
            }
        }

        if should_set_selector_hint {
            adapter.with_time_series_selector_hint(TimeSeriesRowSelector::LastRow);
        }
    }
}

/// Traverse and fetch hints.
#[derive(Default)]
struct ScanHintVisitor {
    /// The closest order requirement to the leaf node.
    order_expr: Option<Vec<Sort>>,
    /// Row selection on time series distribution.
    /// This field stores saved `group_by` columns when all aggregate functions are `last_value`
    /// and the `order_by` column which should be time index.
    ts_row_selector: Option<(HashSet<Column>, Column)>,
}

impl TreeNodeVisitor<'_> for ScanHintVisitor {
    type Node = LogicalPlan;

    fn f_down(&mut self, node: &Self::Node) -> DataFusionResult<TreeNodeRecursion> {
        // Get order requirement from sort plan
        if let LogicalPlan::Sort(sort) = node {
            let mut exprs = vec![];
            for expr in &sort.expr {
                if let Expr::Sort(sort_expr) = expr {
                    exprs.push(sort_expr.clone());
                }
            }
            self.order_expr = Some(exprs);
        }

        // Get time series row selector from aggr plan
        if let LogicalPlan::Aggregate(aggregate) = node {
            let mut is_all_last_value = !aggregate.aggr_expr.is_empty();
            let mut order_by_expr = None;
            for expr in &aggregate.aggr_expr {
                // check function name
                let Expr::AggregateFunction(func) = expr else {
                    is_all_last_value = false;
                    break;
                };
                if func.func_def.name() != "last_value" || func.filter.is_some() || func.distinct {
                    is_all_last_value = false;
                    break;
                }
                // check order by requirement
                if let Some(order_by) = &func.order_by
                    && let Some(first_order_by) = order_by.first()
                    && order_by.len() == 1
                {
                    if let Some(existing_order_by) = &order_by_expr {
                        if existing_order_by != first_order_by {
                            is_all_last_value = false;
                            break;
                        }
                    } else if let Expr::Sort(sort_expr) = first_order_by {
                        // only allow `order by xxx [ASC]`, xxx is a bare column reference so `last_value()` is the max
                        // value of the column.
                        if !sort_expr.asc || !matches!(&*sort_expr.expr, Expr::Column(_)) {
                            is_all_last_value = false;
                            break;
                        }
                        order_by_expr = Some(first_order_by.clone());
                    }
                }
            }
            is_all_last_value &= order_by_expr.is_some();
            if is_all_last_value {
                // make sure all the exprs are DIRECT `col` and collect them
                let mut group_by_cols = HashSet::with_capacity(aggregate.group_expr.len());
                for expr in &aggregate.group_expr {
                    if let Expr::Column(col) = expr {
                        group_by_cols.insert(col.clone());
                    } else {
                        is_all_last_value = false;
                        break;
                    }
                }
                // Safety: checked in the above loop
                let Expr::Sort(Sort {
                    expr: order_by_col, ..
                }) = order_by_expr.unwrap()
                else {
                    unreachable!()
                };
                let Expr::Column(order_by_col) = *order_by_col else {
                    unreachable!()
                };
                if is_all_last_value {
                    self.ts_row_selector = Some((group_by_cols, order_by_col));
                }
            }
        }

        if self.ts_row_selector.is_some()
            && (matches!(node, LogicalPlan::Subquery(_)) || node.inputs().len() > 1)
        {
            // clean previous time series selector hint when encounter subqueries or join
            self.ts_row_selector = None;
        }

        if let LogicalPlan::Filter(filter) = node
            && let Some(group_by_exprs) = &self.ts_row_selector
        {
            let mut filter_referenced_cols = HashSet::default();
            utils::expr_to_columns(&filter.predicate, &mut filter_referenced_cols)?;
            // ensure only group_by columns are used in filter
            if !filter_referenced_cols.is_subset(&group_by_exprs.0) {
                self.ts_row_selector = None;
            }
        }

        Ok(TreeNodeRecursion::Continue)
    }
}

impl ScanHintVisitor {
    fn need_rewrite(&self) -> bool {
        self.order_expr.is_some() || self.ts_row_selector.is_some()
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use datafusion_expr::expr::{AggregateFunction, AggregateFunctionDefinition};
    use datafusion_expr::{col, LogicalPlanBuilder};
    use datafusion_optimizer::OptimizerContext;
    use datafusion_physical_expr::expressions::LastValue;
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
        ScanHintRule.try_optimize(&plan, &context).unwrap();

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

    #[test]
    fn set_time_series_row_selector_hint() {
        let provider = Arc::new(mock_table_provider(RegionId::new(1, 1)));
        let table_source = Arc::new(DefaultTableSource::new(provider.clone()));
        let plan = LogicalPlanBuilder::scan("t", table_source, None)
            .unwrap()
            .aggregate(
                vec![col("k0")],
                vec![Expr::AggregateFunction(AggregateFunction {
                    func_def: AggregateFunctionDefinition::UDF(Arc::new(LastValue::new().into())),
                    args: vec![col("v0")],
                    distinct: false,
                    filter: None,
                    order_by: Some(vec![Expr::Sort(Sort {
                        expr: Box::new(col("ts")),
                        asc: true,
                        nulls_first: true,
                    })]),
                    null_treatment: None,
                })],
            )
            .unwrap()
            .build()
            .unwrap();

        let context = OptimizerContext::default();
        ScanHintRule.try_optimize(&plan, &context).unwrap();

        let scan_req = provider.scan_request();
        let _ = scan_req.series_row_selector.unwrap();
    }
}
