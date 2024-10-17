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

use api::v1::SemanticType;
use arrow_schema::SortOptions;
use common_recordbatch::OrderOption;
use datafusion::datasource::DefaultTableSource;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion, TreeNodeVisitor};
use datafusion_common::{Column, Result as DataFusionResult};
use datafusion_expr::expr::Sort;
use datafusion_expr::{utils, Expr, LogicalPlan};
use datafusion_optimizer::{OptimizerConfig, OptimizerRule};
use datafusion_physical_expr::expressions::Column as PhysicalColumn;
use store_api::region_engine::PartitionRange;
use store_api::storage::TimeSeriesRowSelector;
use table::table::scan::RegionScanExec;

use crate::dummy_catalog::DummyTableProvider;
use crate::plan::windowed_sort::WindowedSort;
use crate::window_sort::WindowedSortExec;

/// Optimize rule for windowed sort.
///
/// This is expected to run after [`ScanHint`] and [`ParallelizeScan`].
/// It would change the original sort to a custom plan. To make sure
/// other rules are applied correctly, this rule can be run as later as
/// possible.
///
/// [`ScanHint`]: crate::optimizer::scan_hint::ScanHintRule
/// [`ParallelizeScan`]: crate::optimizer::parallelize_scan::ParallelizeScan
pub struct WindowedSortRule;

impl OptimizerRule for WindowedSortRule {
    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> DataFusionResult<Option<LogicalPlan>> {
        Self::optimize(plan).map(Some)
    }

    fn name(&self) -> &str {
        "WindowedSortRule"
    }
}

impl WindowedSortRule {
    fn optimize(plan: &LogicalPlan) -> DataFusionResult<LogicalPlan> {
        // let mut visitor = WindowedSortVisitor::new();
        // visitor.visit(plan)?;
        // Ok(visitor.plan)
        todo!()
    }
}

struct WindowedSortVisitor {
    partitions: Option<Vec<Vec<PartitionRange>>>,
    time_index: Option<String>,
    nearest_sort_order: Option<Vec<Expr>>,
}

impl TreeNodeVisitor<'_> for WindowedSortVisitor {
    type Node = LogicalPlan;

    fn f_down(&mut self, node: &Self::Node) -> DataFusionResult<TreeNodeRecursion> {
        match node {
            LogicalPlan::Sort(sort) => {
                self.nearest_sort_order = Some(sort.expr.clone());
                todo!()
            }
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
                        // if let Some(order_expr) = &visitor.order_expr {
                        //     Self::set_order_hint(adapter, order_expr);
                        // }

                        // get order hint
                        // let scan_output_order = adapter.scan_request().output_ordering;

                        // get ts column name
                        self.time_index = Some(
                            adapter
                                .region_metadata()
                                .time_index_column()
                                .column_schema
                                .name
                                .clone(),
                        );
                        let time_index = self.time_index.as_ref().unwrap();

                        // // set time series selector hint
                        // if let Some((group_by_cols, order_by_col)) = &visitor.ts_row_selector {
                        //     Self::set_time_series_row_selector_hint(
                        //         adapter,
                        //         group_by_cols,
                        //         order_by_col,
                        //     );
                        // }

                        if let Some(sort_exprs) = &self.nearest_sort_order
                            && let Some(first_expr) = sort_exprs.first()
                            && let Expr::Sort(sort_expr) = first_expr
                            && let Expr::Column(col) = &*sort_expr.expr
                            && &col.name == time_index
                        {
                        } else {
                            self.nearest_sort_order = None;
                        }

                        // transformed = true;
                    }
                }
                // if transformed {
                //     Ok(Transformed::yes(plan))
                // } else {
                //     Ok(Transformed::no(plan))
                // }
            }

            LogicalPlan::Projection(projection) => todo!(),
            LogicalPlan::Filter(filter) => todo!(),
            LogicalPlan::Window(window) => todo!(),
            LogicalPlan::Aggregate(aggregate) => todo!(),
            LogicalPlan::Join(join) => todo!(),
            LogicalPlan::CrossJoin(cross_join) => todo!(),
            LogicalPlan::Repartition(repartition) => todo!(),
            LogicalPlan::Union(union) => todo!(),
            LogicalPlan::EmptyRelation(empty_relation) => todo!(),
            LogicalPlan::Subquery(subquery) => todo!(),
            LogicalPlan::SubqueryAlias(subquery_alias) => todo!(),
            LogicalPlan::Limit(limit) => todo!(),
            LogicalPlan::Statement(statement) => todo!(),
            LogicalPlan::Values(values) => todo!(),
            LogicalPlan::Explain(explain) => todo!(),
            LogicalPlan::Analyze(analyze) => todo!(),
            LogicalPlan::Extension(extension) => todo!(),
            LogicalPlan::Distinct(distinct) => todo!(),
            LogicalPlan::Prepare(prepare) => todo!(),
            LogicalPlan::Dml(dml_statement) => todo!(),
            LogicalPlan::Ddl(ddl_statement) => todo!(),
            LogicalPlan::Copy(copy_to) => todo!(),
            LogicalPlan::DescribeTable(describe_table) => todo!(),
            LogicalPlan::Unnest(unnest) => todo!(),
            LogicalPlan::RecursiveQuery(recursive_query) => todo!(),
        }

        Ok(TreeNodeRecursion::Continue)
    }

    fn f_up(&mut self, node: &Self::Node) -> DataFusionResult<TreeNodeRecursion> {
        match node {
            LogicalPlan::Sort(sort) => {
                let Some(sort_order) = self.nearest_sort_order.take() else {
                    return Ok(TreeNodeRecursion::Continue);
                };
            }

            LogicalPlan::Projection(projection) => todo!(),
            LogicalPlan::Filter(filter) => todo!(),
            LogicalPlan::Window(window) => todo!(),
            LogicalPlan::Aggregate(aggregate) => todo!(),
            LogicalPlan::Join(join) => todo!(),
            LogicalPlan::CrossJoin(cross_join) => todo!(),
            LogicalPlan::Repartition(repartition) => todo!(),
            LogicalPlan::Union(union) => todo!(),
            LogicalPlan::TableScan(table_scan) => todo!(),
            LogicalPlan::EmptyRelation(empty_relation) => todo!(),
            LogicalPlan::Subquery(subquery) => todo!(),
            LogicalPlan::SubqueryAlias(subquery_alias) => todo!(),
            LogicalPlan::Limit(limit) => todo!(),
            LogicalPlan::Statement(statement) => todo!(),
            LogicalPlan::Values(values) => todo!(),
            LogicalPlan::Explain(explain) => todo!(),
            LogicalPlan::Analyze(analyze) => todo!(),
            LogicalPlan::Extension(extension) => todo!(),
            LogicalPlan::Distinct(distinct) => todo!(),
            LogicalPlan::Prepare(prepare) => todo!(),
            LogicalPlan::Dml(dml_statement) => todo!(),
            LogicalPlan::Ddl(ddl_statement) => todo!(),
            LogicalPlan::Copy(copy_to) => todo!(),
            LogicalPlan::DescribeTable(describe_table) => todo!(),
            LogicalPlan::Unnest(unnest) => todo!(),
            LogicalPlan::RecursiveQuery(recursive_query) => todo!(),
        }

        Ok(TreeNodeRecursion::Continue)
    }
}

pub struct WindowedSortPhysicalRule;

impl PhysicalOptimizerRule for WindowedSortPhysicalRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &datafusion::config::ConfigOptions,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Self::do_optimize(plan, config)
    }

    fn name(&self) -> &str {
        "WindowedSortRule"
    }

    fn schema_check(&self) -> bool {
        false
    }
}

impl WindowedSortPhysicalRule {
    fn do_optimize(
        plan: Arc<dyn ExecutionPlan>,
        _config: &datafusion::config::ConfigOptions,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let result = plan
            .transform_down(|plan| {
                if let Some(sort_exec) = plan.as_any().downcast_ref::<SortExec>() {
                    // TODO: support multiple expr in windowed sort
                    if !sort_exec.preserve_partitioning() || sort_exec.expr().len() != 1 {
                        return Ok(Transformed::no(plan));
                    }

                    let Some(scanner_info) = fetch_partition_range(sort_exec.input().clone())?
                    else {
                        return Ok(Transformed::no(plan));
                    };

                    if let Some(first_sort_expr) = sort_exec.expr().first()
                        && let Some(column_expr) = first_sort_expr
                            .expr
                            .as_any()
                            .downcast_ref::<PhysicalColumn>()
                        && column_expr.name() == &scanner_info.time_index
                    {
                    } else {
                        return Ok(Transformed::no(plan));
                    }

                    // TODO: append another pre-sort plan
                    let windowed_sort_exec = WindowedSortExec::try_new(
                        sort_exec.expr().first().unwrap().clone(),
                        sort_exec.fetch(),
                        scanner_info.partition_ranges,
                        sort_exec.input().clone(),
                    )?;

                    return Ok(Transformed::yes(Arc::new(windowed_sort_exec)));
                }

                Ok(Transformed::no(plan))
            })?
            .data;

        Ok(result)
    }
}

struct ScannerInfo {
    partition_ranges: Vec<Vec<PartitionRange>>,
    time_index: String,
}

fn fetch_partition_range(input: Arc<dyn ExecutionPlan>) -> DataFusionResult<Option<ScannerInfo>> {
    let mut partition_ranges = None;
    let mut time_index = None;

    input.transform_up(|plan| {
        // Unappliable case, reset the result.
        if plan.as_any().is::<RepartitionExec>()
            || plan.as_any().is::<CoalesceBatchesExec>()
            || plan.as_any().is::<CoalescePartitionsExec>()
            || plan.as_any().is::<SortExec>()
        {
            partition_ranges = None;
        }

        if let Some(region_scan_exec) = plan.as_any().downcast_ref::<RegionScanExec>() {
            partition_ranges = Some(region_scan_exec.get_uncollapsed_partition_ranges());
            time_index = region_scan_exec.time_index();
        }

        Ok(Transformed::no(plan))
    })?;

    let result = try {
        ScannerInfo {
            partition_ranges: partition_ranges?,
            time_index: time_index?,
        }
    };

    Ok(result)
}
