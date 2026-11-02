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
use common_function::aggrs::aggr_wrapper::aggr_state_func_name;
use common_recordbatch::OrderOption;
use common_recordbatch::filter::SimpleFilterEvaluator;
use common_time::timestamp::TimeUnit;
use datafusion::datasource::DefaultTableSource;
use datafusion_common::tree_node::{Transformed, TreeNodeRewriter};
use datafusion_common::{Column, Result};
use datafusion_expr::expr::Sort;
use datafusion_expr::{Expr, LogicalPlan, utils};
use datafusion_optimizer::{OptimizerConfig, OptimizerRule};
use datatypes::arrow::datatypes::{DataType, TimeUnit as ArrowTimeUnit};
use promql::extension_plan::{InstantManipulate, SeriesDivide, SeriesNormalize};
use store_api::metric_engine_consts::DATA_SCHEMA_TSID_COLUMN_NAME;
use store_api::storage::{TimeSeriesDistribution, TimeSeriesRowSelector};

use crate::dummy_catalog::DummyTableProvider;
#[cfg(feature = "vector_index")]
mod vector_search;
#[cfg(feature = "vector_index")]
use vector_search::VectorSearchState;

/// This rule will traverse the plan to collect necessary hints for leaf
/// table scan node and set them in [`ScanRequest`]. Hints include:
/// - the nearest order requirement to the leaf table scan node as ordering hint.
/// - the group by columns when all aggregate functions are `last_value` as
///   time series row selector hint.
///
/// [`ScanRequest`]: store_api::storage::ScanRequest
#[derive(Debug)]
pub struct ScanHintRule;

impl OptimizerRule for ScanHintRule {
    fn name(&self) -> &str {
        "ScanHintRule"
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        Self::optimize(plan)
    }
}

impl ScanHintRule {
    fn optimize(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        let mut rewriter = ScanHintRewriter::default();
        // The extension's input is included by DataFusion's normal TreeNode
        // rewrite, so this is one scoped recursive walk (subquery expressions
        // are included by the dedicated API as well).
        plan.rewrite_with_subqueries(&mut rewriter)
    }

    fn set_hints(
        plan: LogicalPlan,
        rewriter: &mut ScanHintRewriter,
    ) -> Result<Transformed<LogicalPlan>> {
        let LogicalPlan::TableScan(mut table_scan) = plan else {
            return Ok(Transformed::no(plan));
        };
        let Some(source) = table_scan.source.downcast_ref::<DefaultTableSource>() else {
            return Ok(Transformed::no(LogicalPlan::TableScan(table_scan)));
        };
        // The provider in the region server is [DummyTableProvider].
        let Some(original) = source.table_provider.downcast_ref::<DummyTableProvider>() else {
            return Ok(Transformed::no(LogicalPlan::TableScan(table_scan)));
        };

        // Attached scan filters are checked below; residual Filter nodes are
        // rejected by the single-evaluation path allowlist.
        let filters_preserve_last_row = if rewriter.inside_single_evaluation {
            Self::filters_preserve_last_row(&table_scan, original)
        } else {
            true
        };
        let use_last_row = rewriter.inside_single_evaluation && filters_preserve_last_row;

        #[cfg(feature = "vector_index")]
        let has_vector_hint = rewriter.vector_search.need_rewrite();
        #[cfg(not(feature = "vector_index"))]
        let has_vector_hint = false;
        let has_hint = rewriter.order_expr.is_some()
            || rewriter.ts_row_selector.is_some()
            || use_last_row
            || has_vector_hint;
        if !has_hint {
            return Ok(Transformed::no(LogicalPlan::TableScan(table_scan)));
        }

        // A provider can be used by several TableScan nodes. Fork its request
        // for every hinted use-site before applying hints, rather than mutating
        // the shared catalog provider. This keeps order/vector/legacy hints
        // local as well as the new LastRow hint.
        let adapter = original.clone_for_scan();
        Self::apply_hints(&adapter, rewriter, &table_scan, use_last_row);
        if use_last_row {
            // Apply the instant-derived hint after the aggregate hint. Both
            // select LastRow today, and this ordering preserves the existing
            // aggregate selector when the instant guard rejects a scan.
            adapter.with_time_series_selector_hint(TimeSeriesRowSelector::LastRow {
                after_merge: true,
            });
        }
        table_scan.source =
            std::sync::Arc::new(DefaultTableSource::new(std::sync::Arc::new(adapter)));
        Ok(Transformed::yes(LogicalPlan::TableScan(table_scan)))
    }

    /// Checks whether attached scan predicates permit instant-derived LastRow selection.
    ///
    /// Selecting the newest row too early can discard an older matching sample if a
    /// predicate later rejects that row. Only recognized tag/time predicates are
    /// allowed: tags select whole series, and supported time predicates constrain
    /// the scan window before row selection. Field or unrecognized predicates are
    /// conservatively rejected. Finer-than-millisecond timestamps are also excluded
    /// because instant evaluation can conflate distinct samples at that precision.
    ///
    /// This checks only attached predicates; the path allowlist separately rejects
    /// residual Filter nodes between InstantManipulate and the scan.
    fn filters_preserve_last_row(
        table_scan: &datafusion_expr::logical_plan::TableScan,
        provider: &DummyTableProvider,
    ) -> bool {
        let metadata = provider.region_metadata();
        // Instant evaluation is millisecond-based, so finer time units can
        // conflate timestamps and must not use the LastRow hint.
        if !matches!(
            metadata.time_index_type().unit(),
            TimeUnit::Second | TimeUnit::Millisecond
        ) {
            return false;
        }
        for filter in &table_scan.filters {
            let Some(filter) = SimpleFilterEvaluator::try_new(filter) else {
                return false;
            };
            let Some(column_metadata) = metadata.column_by_name(filter.column_name()) else {
                return false;
            };
            if !matches!(
                column_metadata.semantic_type,
                SemanticType::Tag | SemanticType::Timestamp
            ) {
                return false;
            }
        }
        true
    }

    fn apply_hints(
        adapter: &DummyTableProvider,
        rewriter: &mut ScanHintRewriter,
        table_scan: &datafusion_expr::logical_plan::TableScan,
        use_last_row: bool,
    ) {
        #[cfg(not(feature = "vector_index"))]
        let _ = (table_scan, use_last_row);
        if let Some(order_expr) = &rewriter.order_expr {
            Self::set_order_hint(adapter, order_expr);
        }
        if let Some((group_by_cols, order_by_col)) = &rewriter.ts_row_selector {
            Self::set_time_series_row_selector_hint(adapter, group_by_cols, order_by_col);
        }
        #[cfg(feature = "vector_index")]
        if use_last_row {
            // LastRow and vector search are mutually exclusive for one scan:
            // vector search would bypass the ordinary sort/limit path needed by
            // the single-evaluation semantics. Still consume the queued hint so
            // it cannot be applied to a later scan of the same table.
            let _ = rewriter
                .vector_search
                .take_vector_request_from_dummy(adapter, &table_scan.table_name);
        } else if let Some(vector_request) = rewriter
            .vector_search
            .take_vector_request_from_dummy(adapter, &table_scan.table_name)
        {
            adapter.with_vector_search_hint(vector_request);
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

        let region_metadata = adapter.region_metadata();
        let time_index_name = region_metadata
            .time_index_column()
            .column_schema
            .name
            .as_str();
        let sort_cols = order_expr
            .iter()
            .filter_map(|s| s.expr.try_as_col())
            .collect::<Vec<_>>();

        // Special-case metric engine: when the nearest sort requirement is `__tsid, <time index>`,
        // we can safely enable per-series distribution hint so the region can use `SeriesScan`.
        //
        // This pattern is produced by promql planning when `__tsid` is available and is used as the
        // series identifier (instead of expanding to all tag columns).
        if sort_cols.len() == 2
            && sort_cols[0].name == DATA_SCHEMA_TSID_COLUMN_NAME
            && sort_cols[1].name == time_index_name
        {
            adapter.with_distribution(TimeSeriesDistribution::PerSeries);
            return;
        }

        let mut sort_expr_cursor = sort_cols.into_iter();
        // ignore table without pk
        if region_metadata.primary_key.is_empty() {
            return;
        }
        let mut pk_column_iter = region_metadata.primary_key_columns();
        let mut curr_sort_expr = sort_expr_cursor.next();
        let mut curr_pk_col = pk_column_iter.next();

        while let (Some(sort_expr), Some(pk_col)) = (curr_sort_expr, curr_pk_col) {
            if sort_expr.name == pk_col.column_schema.name {
                curr_sort_expr = sort_expr_cursor.next();
                curr_pk_col = pk_column_iter.next();
            } else {
                return;
            }
        }

        let next_remaining = sort_expr_cursor.next();
        match (curr_sort_expr, next_remaining) {
            (Some(expr), None)
                if expr.name == region_metadata.time_index_column().column_schema.name =>
            {
                adapter.with_distribution(TimeSeriesDistribution::PerSeries);
            }
            (None, _) => adapter.with_distribution(TimeSeriesDistribution::PerSeries),
            (Some(_), _) => {}
        }
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
            adapter.with_time_series_selector_hint(TimeSeriesRowSelector::LastRow {
                after_merge: false,
            });
        }
    }
}

/// Traverse and apply hints with state scoped to the current logical-plan path.
///
/// Rewriting the scan while walking down the tree is important: the state then
/// describes the actual parent path of that scan, and a shared provider is forked
/// at that exact use-site. No traversal-order identity is involved.
#[derive(Default)]
struct ScanHintRewriter {
    order_expr: Option<Vec<Sort>>,
    order_stack: Vec<Option<Vec<Sort>>>,
    ts_row_selector: Option<(HashSet<Column>, Column)>,
    ts_stack: Vec<Option<(HashSet<Column>, Column)>>,
    inside_single_evaluation: bool,
    single_evaluation_stack: Vec<bool>,
    #[cfg(feature = "vector_index")]
    vector_search: VectorSearchState,
}

impl TreeNodeRewriter for ScanHintRewriter {
    type Node = LogicalPlan;

    fn f_down(&mut self, node: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        self.order_stack.push(self.order_expr.clone());
        self.ts_stack.push(self.ts_row_selector.clone());
        self.single_evaluation_stack
            .push(self.inside_single_evaluation);

        if let LogicalPlan::Sort(sort) = &node {
            self.order_expr = Some(sort.expr.clone());
        }
        if let LogicalPlan::Extension(extension) = &node
            && let Some(instant) = extension.node.as_any().downcast_ref::<InstantManipulate>()
        {
            self.inside_single_evaluation = instant.is_single_evaluation();
        } else if self.inside_single_evaluation {
            // This allowlist is coupled to the controlled PromQL planner. It
            // permits only nodes known to preserve the newest row per series;
            // every other node is a sticky boundary until a nested instant
            // extension establishes a new evaluation scope.
            self.inside_single_evaluation = single_evaluation_node_allowed(&node);
        }
        if let LogicalPlan::Aggregate(aggregate) = &node {
            self.ts_row_selector = Self::extract_last_value_selector(aggregate);
        }

        let is_branching = matches!(
            node,
            LogicalPlan::Subquery(_) | LogicalPlan::SubqueryAlias(_)
        ) || node.inputs().len() > 1;
        if is_branching {
            self.ts_row_selector = None;
        }
        if let LogicalPlan::Filter(filter) = &node
            && let Some(group_by_exprs) = &self.ts_row_selector
        {
            let mut referenced = HashSet::default();
            utils::expr_to_columns(&filter.predicate, &mut referenced)?;
            if !referenced.is_subset(&group_by_exprs.0) {
                self.ts_row_selector = None;
            }
        }

        #[cfg(feature = "vector_index")]
        {
            if let LogicalPlan::Limit(limit) = &node {
                self.vector_search.on_limit_enter(limit);
            }
            if let LogicalPlan::Sort(sort) = &node {
                self.vector_search.on_sort_enter(sort);
            }
            if is_branching_for_vector(&node) {
                self.vector_search.on_branching_enter();
            }
            if let LogicalPlan::Filter(filter) = &node {
                self.vector_search.on_filter_enter(&filter.predicate);
            }
            if let LogicalPlan::TableScan(table_scan) = &node {
                self.vector_search.on_table_scan(table_scan);
            }
        }

        ScanHintRule::set_hints(node, self)
    }

    fn f_up(&mut self, node: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        #[cfg(feature = "vector_index")]
        {
            match &node {
                LogicalPlan::Limit(_) => self.vector_search.on_limit_exit(),
                LogicalPlan::Sort(_) => self.vector_search.on_sort_exit(),
                LogicalPlan::Filter(_) => self.vector_search.on_filter_exit(),
                LogicalPlan::Subquery(_) | LogicalPlan::SubqueryAlias(_)
                    if is_branching_for_vector(&node) =>
                {
                    self.vector_search.on_branching_exit()
                }
                _ if node.inputs().len() > 1 => self.vector_search.on_branching_exit(),
                _ => {}
            }
        }
        if let Some(previous) = self.order_stack.pop() {
            self.order_expr = previous;
        }
        if let Some(previous) = self.ts_stack.pop() {
            self.ts_row_selector = previous;
        }
        if let Some(previous) = self.single_evaluation_stack.pop() {
            self.inside_single_evaluation = previous;
        }
        Ok(Transformed::no(node))
    }
}

/// Returns whether a plan node can occur on the scan path of a controlled
/// PromQL single evaluation without changing which row is newest per series.
fn single_evaluation_node_allowed(node: &LogicalPlan) -> bool {
    match node {
        LogicalPlan::TableScan(_) | LogicalPlan::SubqueryAlias(_) => true,
        LogicalPlan::Sort(sort) => sort.fetch.is_none(),
        LogicalPlan::Projection(projection) => projection
            .expr
            .iter()
            .all(|expr| single_evaluation_projection_expr_allowed(expr, projection)),
        LogicalPlan::Extension(extension) => {
            let extension = extension.node.as_any();
            extension.is::<SeriesDivide>()
                || extension
                    .downcast_ref::<SeriesNormalize>()
                    .is_some_and(|normalize| !normalize.filter_stale_markers())
        }
        _ => false,
    }
}

/// This whitelist assumes the planner preserves time-index and series identity;
/// it is not a proof that an arbitrary plan does so.
fn single_evaluation_projection_expr_allowed(
    expr: &Expr,
    projection: &datafusion_expr::logical_plan::Projection,
) -> bool {
    match expr {
        Expr::Column(_) => true,
        Expr::Alias(alias) => match alias.expr.as_ref() {
            Expr::Column(column) => alias.name == column.name,
            Expr::Cast(cast) => {
                let Expr::Column(column) = cast.expr.as_ref() else {
                    return false;
                };
                alias.name == column.name
                    && matches!(
                        cast.field.data_type(),
                        DataType::Timestamp(ArrowTimeUnit::Millisecond, None)
                    )
                    && matches!(
                        projection
                            .input
                            .schema()
                            .qualified_field_from_column(column),
                        Ok((_, field))
                            if matches!(field.data_type(), DataType::Timestamp(ArrowTimeUnit::Second | ArrowTimeUnit::Millisecond, None))
                    )
            }
            _ => false,
        },
        _ => false,
    }
}

impl ScanHintRewriter {
    fn extract_last_value_selector(
        aggregate: &datafusion_expr::logical_plan::Aggregate,
    ) -> Option<(HashSet<Column>, Column)> {
        let mut order_by_expr = None;
        if aggregate.aggr_expr.is_empty() {
            return None;
        }
        for expr in &aggregate.aggr_expr {
            let Expr::AggregateFunction(func) = expr else {
                return None;
            };
            if (func.func.name() != "last_value"
                && func.func.name() != aggr_state_func_name("last_value"))
                || func.params.filter.is_some()
                || func.params.distinct
            {
                return None;
            }
            let order_by = &func.params.order_by;
            if order_by.len() != 1 || !order_by[0].asc {
                return None;
            }
            if let Some(existing) = &order_by_expr {
                if existing != &order_by[0] {
                    return None;
                }
            } else {
                order_by_expr = Some(order_by[0].clone());
            }
        }
        let Expr::Column(order_by_col) = order_by_expr?.expr else {
            return None;
        };
        let mut group_by_cols = HashSet::with_capacity(aggregate.group_expr.len());
        for expr in &aggregate.group_expr {
            let Expr::Column(col) = expr else {
                return None;
            };
            group_by_cols.insert(col.clone());
        }
        Some((group_by_cols, order_by_col))
    }
}

#[cfg(feature = "vector_index")]
fn is_branching_for_vector(node: &LogicalPlan) -> bool {
    if node.inputs().len() > 1 {
        return true;
    }

    match node {
        LogicalPlan::Subquery(subquery) => has_non_inlineable_ops(subquery.subquery.as_ref()),
        LogicalPlan::SubqueryAlias(alias) => has_non_inlineable_ops(alias.input.as_ref()),
        _ => false,
    }
}

#[cfg(feature = "vector_index")]
fn has_non_inlineable_ops(plan: &LogicalPlan) -> bool {
    if matches!(
        plan,
        LogicalPlan::Limit(_)
            | LogicalPlan::Sort(_)
            | LogicalPlan::Distinct(_)
            | LogicalPlan::Aggregate(_)
            | LogicalPlan::Window(_)
            | LogicalPlan::Union(_)
            | LogicalPlan::Join(_)
    ) {
        return true;
    }

    for input in plan.inputs() {
        if has_non_inlineable_ops(input) {
            return true;
        }
    }

    false
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use std::sync::Arc;

    use datafusion::functions_aggregate::first_last::last_value_udaf;
    use datafusion::functions_aggregate::min_max::max_udaf;
    use datafusion::functions_window::row_number::RowNumber;
    use datafusion::logical_expr::expr::WindowFunction;
    use datafusion::logical_expr::{WindowFrame, WindowFunctionDefinition};
    use datafusion::prelude::JoinType;
    use datafusion_common::tree_node::TreeNodeRecursion;
    use datafusion_expr::expr::{
        AggregateFunction, AggregateFunctionParams, Cast, WindowFunctionParams,
    };
    use datafusion_expr::expr_fn::scalar_subquery;
    use datafusion_expr::{Extension, LogicalPlan, LogicalPlanBuilder, col, lit};
    use datafusion_optimizer::OptimizerContext;
    use datatypes::arrow::datatypes::DataType;
    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::ColumnSchema;
    use promql::extension_plan::RangeManipulate;
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};
    use store_api::metric_engine_consts::DATA_SCHEMA_TSID_COLUMN_NAME;
    use store_api::storage::{RegionId, TimeSeriesRowSelector};

    use super::*;

    fn scan_requests(plan: &LogicalPlan) -> Vec<store_api::storage::ScanRequest> {
        scan_requests_with_names(plan)
            .into_iter()
            .map(|(_, request)| request)
            .collect()
    }

    fn scan_requests_with_names(
        plan: &LogicalPlan,
    ) -> Vec<(String, store_api::storage::ScanRequest)> {
        let mut requests = Vec::new();
        plan.apply_with_subqueries(|node| {
            if let LogicalPlan::TableScan(scan) = node
                && let Some(source) = scan.source.downcast_ref::<DefaultTableSource>()
                && let Some(provider) = source.table_provider.downcast_ref::<DummyTableProvider>()
            {
                requests.push((scan.table_name.to_string(), provider.scan_request()));
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .unwrap();
        requests
    }

    fn instant_plan(provider: Arc<DummyTableProvider>, end: i64) -> LogicalPlan {
        instant_plan_named(provider, "t", end)
    }

    fn instant_plan_with_filters(
        provider: Arc<DummyTableProvider>,
        filters: Vec<Expr>,
    ) -> LogicalPlan {
        let scan = scan_plan(provider, "t");
        let LogicalPlan::TableScan(mut scan) = scan else {
            unreachable!();
        };
        scan.filters = filters;
        LogicalPlan::Extension(Extension {
            node: Arc::new(InstantManipulate::new(
                1000,
                1000,
                1000,
                1000,
                "ts".to_string(),
                vec![],
                Some("v0".to_string()),
                LogicalPlan::TableScan(scan),
            )),
        })
    }

    fn scan_plan(provider: Arc<DummyTableProvider>, table_name: &str) -> LogicalPlan {
        LogicalPlanBuilder::scan(
            table_name,
            Arc::new(DefaultTableSource::new(provider)),
            None,
        )
        .unwrap()
        .build()
        .unwrap()
    }

    fn mock_table_provider_with_timestamp(
        region_id: RegionId,
        timestamp_type: ConcreteDataType,
    ) -> DummyTableProvider {
        let mut builder = RegionMetadataBuilder::new(region_id);
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("k0", ConcreteDataType::string_datatype(), true),
                semantic_type: SemanticType::Tag,
                column_id: 1,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("ts", timestamp_type, false),
                semantic_type: SemanticType::Timestamp,
                column_id: 2,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("v0", ConcreteDataType::float64_datatype(), false),
                semantic_type: SemanticType::Field,
                column_id: 3,
            })
            .primary_key(vec![1]);
        let metadata = Arc::new(builder.build().unwrap());
        let engine = Arc::new(MetaRegionEngine::with_metadata(metadata.clone()));
        DummyTableProvider::new(region_id, engine, metadata)
    }

    fn last_value_aggregate(input: LogicalPlan) -> LogicalPlan {
        LogicalPlanBuilder::from(input)
            .aggregate(
                vec![col("k0")],
                vec![Expr::AggregateFunction(AggregateFunction {
                    func: last_value_udaf(),
                    params: AggregateFunctionParams {
                        args: vec![col("v0")],
                        distinct: false,
                        filter: None,
                        order_by: vec![Sort {
                            expr: col("ts"),
                            asc: true,
                            nulls_first: true,
                        }],
                        null_treatment: None,
                    },
                })],
            )
            .unwrap()
            .build()
            .unwrap()
    }

    fn single_evaluation(input: LogicalPlan) -> LogicalPlan {
        LogicalPlan::Extension(Extension {
            node: Arc::new(InstantManipulate::new(
                1000,
                1000,
                1000,
                1000,
                "ts".to_string(),
                vec![],
                Some("v0".to_string()),
                input,
            )),
        })
    }

    fn instant_with_expression_subquery(
        outer_provider: Arc<DummyTableProvider>,
        inner_plan: LogicalPlan,
        outer_end: i64,
    ) -> LogicalPlan {
        let outer_scan = scan_plan(outer_provider, "outer");
        let input = LogicalPlanBuilder::from(outer_scan)
            .project(vec![
                col("ts"),
                col("v0"),
                scalar_subquery(Arc::new(inner_plan)),
            ])
            .unwrap()
            .build()
            .unwrap();
        LogicalPlan::Extension(Extension {
            node: Arc::new(InstantManipulate::new(
                1000,
                outer_end,
                1000,
                1000,
                "ts".to_string(),
                vec![],
                Some("v0".to_string()),
                input,
            )),
        })
    }

    fn instant_plan_named(
        provider: Arc<DummyTableProvider>,
        table_name: &str,
        end: i64,
    ) -> LogicalPlan {
        let input = LogicalPlanBuilder::scan(
            table_name,
            Arc::new(DefaultTableSource::new(provider)),
            None,
        )
        .unwrap()
        .build()
        .unwrap();
        LogicalPlan::Extension(Extension {
            node: Arc::new(InstantManipulate::new(
                1000,
                end,
                1000,
                1000,
                "ts".to_string(),
                vec![],
                Some("v0".to_string()),
                input,
            )),
        })
    }
    use crate::optimizer::test_util::{
        MetaRegionEngine, mock_table_provider, mock_table_provider_with_tsid,
    };

    #[test]
    fn single_evaluation_sets_last_row_on_the_rewritten_scan() {
        let provider = Arc::new(mock_table_provider(RegionId::new(1, 1)));
        let plan = instant_plan(provider.clone(), 1000);
        let rewritten = ScanHintRule
            .rewrite(plan, &OptimizerContext::default())
            .unwrap()
            .data;

        assert_eq!(
            scan_requests(&rewritten)[0].series_row_selector,
            Some(TimeSeriesRowSelector::LastRow { after_merge: true })
        );

        assert_eq!(provider.scan_request().series_row_selector, None);
    }

    #[test]
    fn single_evaluation_limit_sort_does_not_set_last_row_below_limit() {
        let mut selectors = Vec::new();
        for input in [
            // Instant(1000, lookback=1000) -> Limit(0, 1) -> Sort(ts ASC) -> Scan.
            LogicalPlanBuilder::from(scan_plan(
                Arc::new(mock_table_provider(RegionId::new(1, 1))),
                "t",
            ))
            .sort(vec![col("ts").sort(true, false)])
            .unwrap()
            .limit(0, Some(1))
            .unwrap()
            .build()
            .unwrap(),
            // Sort.fetch is a limit embedded in the Sort node and has the same boundary.
            LogicalPlanBuilder::from(scan_plan(
                Arc::new(mock_table_provider(RegionId::new(1, 1))),
                "t",
            ))
            .sort_with_limit(vec![col("ts").sort(true, false)], Some(1))
            .unwrap()
            .build()
            .unwrap(),
        ] {
            let rewritten = ScanHintRule
                .rewrite(single_evaluation(input), &OptimizerContext::default())
                .unwrap()
                .data;
            selectors.push(scan_requests(&rewritten)[0].series_row_selector);
        }

        // With samples (ts=100, v=10) and (ts=900, v=1), an unhinted ascending
        // sort/limit pipeline returns 10. LastRow at the scan instead leaves only
        // (900, 1) before the limit. A LastRow scan hint cannot cross either limit.
        assert_eq!(selectors, vec![None, None]);
    }

    #[test]
    fn single_evaluation_allows_controlled_promql_selector_chain() {
        let provider = Arc::new(mock_table_provider(RegionId::new(1, 1)));
        let projection = LogicalPlanBuilder::from(scan_plan(provider, "t"))
            .project(vec![
                col("k0").alias("k0"),
                Expr::Cast(Cast::new(
                    Box::new(col("ts")),
                    DataType::Timestamp(ArrowTimeUnit::Millisecond, None),
                ))
                .alias("ts"),
                col("v0"),
            ])
            .unwrap()
            .sort(vec![col("ts").sort(true, false)])
            .unwrap()
            .build()
            .unwrap();
        let divide = LogicalPlan::Extension(Extension {
            node: Arc::new(SeriesDivide::new(
                vec!["k0".to_string()],
                "ts".to_string(),
                projection,
            )),
        });
        let normalize = LogicalPlan::Extension(Extension {
            node: Arc::new(SeriesNormalize::new(
                42,
                "ts",
                false,
                vec!["k0".to_string()],
                divide,
            )),
        });
        let rewritten = ScanHintRule
            .rewrite(single_evaluation(normalize), &OptimizerContext::default())
            .unwrap()
            .data;

        assert_eq!(
            scan_requests(&rewritten)[0].series_row_selector,
            Some(TimeSeriesRowSelector::LastRow { after_merge: true })
        );
    }

    #[test]
    fn single_evaluation_filtering_normalize_does_not_set_last_row() {
        let provider = Arc::new(mock_table_provider(RegionId::new(1, 1)));
        let normalize = LogicalPlan::Extension(Extension {
            node: Arc::new(SeriesNormalize::new(
                42,
                "ts",
                true,
                vec!["k0".to_string()],
                scan_plan(provider, "t"),
            )),
        });
        let rewritten = ScanHintRule
            .rewrite(single_evaluation(normalize), &OptimizerContext::default())
            .unwrap()
            .data;

        // An older finite sample can precede a newest stale marker. LastRow
        // would discard that sample before SeriesNormalize filters the marker.
        assert_eq!(scan_requests(&rewritten)[0].series_row_selector, None);
    }

    #[test]
    fn single_evaluation_rejects_row_changing_nodes() {
        let provider = || Arc::new(mock_table_provider(RegionId::new(1, 1)));
        let window = LogicalPlanBuilder::from(scan_plan(provider(), "window"))
            .window(vec![Expr::WindowFunction(Box::new(WindowFunction {
                fun: WindowFunctionDefinition::WindowUDF(Arc::new(RowNumber::new().into())),
                params: WindowFunctionParams {
                    args: vec![],
                    partition_by: vec![col("k0")],
                    order_by: vec![col("ts").sort(true, true)],
                    window_frame: WindowFrame::new(Some(true)),
                    filter: None,
                    null_treatment: None,
                    distinct: false,
                },
            }))])
            .unwrap()
            .build()
            .unwrap();
        let join = LogicalPlanBuilder::from(scan_plan(provider(), "left"))
            .join(
                scan_plan(provider(), "right"),
                JoinType::Inner,
                (Vec::<Column>::new(), Vec::<Column>::new()),
                None,
            )
            .unwrap()
            .build()
            .unwrap();
        let nonlast_aggregate = LogicalPlanBuilder::from(scan_plan(provider(), "aggregate"))
            .aggregate(
                vec![col("k0")],
                vec![Expr::AggregateFunction(AggregateFunction {
                    func: max_udaf(),
                    params: AggregateFunctionParams {
                        args: vec![col("v0")],
                        distinct: false,
                        filter: None,
                        order_by: vec![],
                        null_treatment: None,
                    },
                })],
            )
            .unwrap()
            .build()
            .unwrap();
        let range = LogicalPlan::Extension(Extension {
            node: Arc::new(
                RangeManipulate::new(
                    1000,
                    1000,
                    1000,
                    1000,
                    "ts".to_string(),
                    vec!["v0".to_string()],
                    scan_plan(provider(), "range"),
                )
                .unwrap(),
            ),
        });

        for (plan, scan_count) in [(window, 1), (join, 2), (nonlast_aggregate, 1), (range, 1)] {
            let rewritten = ScanHintRule
                .rewrite(single_evaluation(plan), &OptimizerContext::default())
                .unwrap()
                .data;
            assert_eq!(
                scan_requests(&rewritten)
                    .into_iter()
                    .map(|request| request.series_row_selector)
                    .collect::<Vec<_>>(),
                vec![None; scan_count]
            );
        }
    }

    #[test]
    fn single_evaluation_last_value_aggregate_keeps_legacy_selector() {
        let provider = Arc::new(mock_table_provider(RegionId::new(1, 1)));
        let rewritten = ScanHintRule
            .rewrite(
                single_evaluation(last_value_aggregate(scan_plan(provider, "t"))),
                &OptimizerContext::default(),
            )
            .unwrap()
            .data;

        assert_eq!(
            scan_requests(&rewritten)[0].series_row_selector,
            Some(TimeSeriesRowSelector::LastRow { after_merge: false })
        );
    }

    #[test]
    fn single_evaluation_allows_second_to_millisecond_time_index_cast() {
        let provider = Arc::new(mock_table_provider_with_timestamp(
            RegionId::new(1, 1),
            ConcreteDataType::timestamp_second_datatype(),
        ));
        let input = LogicalPlanBuilder::from(scan_plan(provider, "t"))
            .project(vec![
                Expr::Cast(Cast::new(
                    Box::new(Expr::Column(Column::new(Some("t"), "ts"))),
                    DataType::Timestamp(ArrowTimeUnit::Millisecond, None),
                ))
                .alias("ts"),
            ])
            .unwrap()
            .build()
            .unwrap();
        let rewritten = ScanHintRule
            .rewrite(single_evaluation(input), &OptimizerContext::default())
            .unwrap()
            .data;

        assert_eq!(
            scan_requests(&rewritten)[0].series_row_selector,
            Some(TimeSeriesRowSelector::LastRow { after_merge: true })
        );
    }

    #[test]
    fn single_evaluation_rejects_microsecond_and_nanosecond_time_index_casts() {
        for timestamp_type in [
            ConcreteDataType::timestamp_microsecond_datatype(),
            ConcreteDataType::timestamp_nanosecond_datatype(),
        ] {
            let provider = Arc::new(mock_table_provider_with_timestamp(
                RegionId::new(1, 1),
                timestamp_type,
            ));
            let input = LogicalPlanBuilder::from(scan_plan(provider, "t"))
                .project(vec![
                    Expr::Cast(Cast::new(
                        Box::new(Expr::Column(Column::new(Some("t"), "ts"))),
                        DataType::Timestamp(ArrowTimeUnit::Millisecond, None),
                    ))
                    .alias("ts"),
                ])
                .unwrap()
                .build()
                .unwrap();
            let rewritten = ScanHintRule
                .rewrite(single_evaluation(input), &OptimizerContext::default())
                .unwrap()
                .data;

            assert_eq!(scan_requests(&rewritten)[0].series_row_selector, None);
        }
    }

    #[test]
    fn single_evaluation_rejects_unresolved_time_index_cast_qualifier() {
        let provider = Arc::new(mock_table_provider_with_timestamp(
            RegionId::new(1, 1),
            ConcreteDataType::timestamp_second_datatype(),
        ));
        let input = LogicalPlanBuilder::from(scan_plan(provider, "t"))
            .project(vec![col("ts")])
            .unwrap()
            .build()
            .unwrap();
        let LogicalPlan::Projection(projection) = input else {
            unreachable!();
        };
        let unresolved_cast = Expr::Cast(Cast::new(
            Box::new(Expr::Column(Column::new(Some("missing"), "ts"))),
            DataType::Timestamp(ArrowTimeUnit::Millisecond, None),
        ))
        .alias("ts");

        assert!(!single_evaluation_projection_expr_allowed(
            &unresolved_cast,
            &projection
        ));
    }

    #[test]
    fn single_evaluation_rejects_projection_expressions_that_change_rows() {
        let invalid_projections = [
            vec![col("ts").alias("renamed")],
            vec![
                Expr::BinaryExpr(datafusion_expr::expr::BinaryExpr::new(
                    Box::new(col("v0")),
                    datafusion_expr::Operator::Plus,
                    Box::new(lit(1.0_f64)),
                ))
                .alias("v0"),
            ],
            vec![
                Expr::Cast(Cast::new(
                    Box::new(col("ts")),
                    DataType::Timestamp(ArrowTimeUnit::Second, None),
                ))
                .alias("ts"),
            ],
            vec![Expr::Cast(Cast::new(Box::new(col("v0")), DataType::Int64)).alias("v0")],
            vec![
                Expr::Cast(Cast::new(
                    Box::new(col("ts")),
                    DataType::Timestamp(ArrowTimeUnit::Microsecond, None),
                ))
                .alias("ts"),
            ],
            vec![
                Expr::Cast(Cast::new(
                    Box::new(col("ts")),
                    DataType::Timestamp(ArrowTimeUnit::Nanosecond, None),
                ))
                .alias("ts"),
            ],
        ];

        for expressions in invalid_projections {
            let provider = Arc::new(mock_table_provider(RegionId::new(1, 1)));
            let input = LogicalPlanBuilder::from(scan_plan(provider, "t"))
                .project(expressions)
                .unwrap()
                .build()
                .unwrap();
            let rewritten = ScanHintRule
                .rewrite(single_evaluation(input), &OptimizerContext::default())
                .unwrap()
                .data;

            assert_eq!(scan_requests(&rewritten)[0].series_row_selector, None);
        }
    }

    #[test]
    fn range_evaluation_does_not_set_last_row() {
        let provider = Arc::new(mock_table_provider(RegionId::new(1, 1)));
        let plan = instant_plan(provider.clone(), 2000);
        let rewritten = ScanHintRule
            .rewrite(plan, &OptimizerContext::default())
            .unwrap()
            .data;

        assert_eq!(scan_requests(&rewritten)[0].series_row_selector, None);

        assert_eq!(provider.scan_request().series_row_selector, None);
    }

    #[test]
    fn residual_field_filter_does_not_set_last_row() {
        let provider = Arc::new(mock_table_provider(RegionId::new(1, 1)));
        let input = LogicalPlanBuilder::from(scan_plan(provider, "t"))
            .filter(col("v0").gt(lit(1.0_f64)))
            .unwrap()
            .build()
            .unwrap();
        let rewritten = ScanHintRule
            .rewrite(single_evaluation(input), &OptimizerContext::default())
            .unwrap()
            .data;

        assert_eq!(scan_requests(&rewritten)[0].series_row_selector, None);
    }

    #[test]
    fn outer_residual_filter_does_not_block_last_row() {
        let provider = Arc::new(mock_table_provider(RegionId::new(1, 1)));
        let plan = LogicalPlanBuilder::from(single_evaluation(scan_plan(provider, "t")))
            .filter(col("v0").gt(lit(1.0_f64)))
            .unwrap()
            .build()
            .unwrap();
        let rewritten = ScanHintRule
            .rewrite(plan, &OptimizerContext::default())
            .unwrap()
            .data;

        assert_eq!(
            scan_requests(&rewritten)[0].series_row_selector,
            Some(TimeSeriesRowSelector::LastRow { after_merge: true })
        );
    }

    #[test]
    fn branch_local_residual_filter_isolation_in_both_orders() {
        for filtered_first in [true, false] {
            let provider = Arc::new(mock_table_provider(RegionId::new(1, 1)));
            let filtered = single_evaluation(
                LogicalPlanBuilder::from(scan_plan(provider.clone(), "filtered"))
                    .filter(col("v0").gt(lit(1.0_f64)))
                    .unwrap()
                    .build()
                    .unwrap(),
            );
            let plain = single_evaluation(scan_plan(provider.clone(), "plain"));
            let union = if filtered_first {
                LogicalPlanBuilder::from(filtered)
                    .union(plain)
                    .unwrap()
                    .build()
                    .unwrap()
            } else {
                LogicalPlanBuilder::from(plain)
                    .union(filtered)
                    .unwrap()
                    .build()
                    .unwrap()
            };
            let rewritten = ScanHintRule
                .rewrite(union, &OptimizerContext::default())
                .unwrap()
                .data;
            let requests = scan_requests_with_names(&rewritten)
                .into_iter()
                .collect::<HashMap<_, _>>();

            assert_eq!(requests["filtered"].series_row_selector, None);
            assert_eq!(
                requests["plain"].series_row_selector,
                Some(TimeSeriesRowSelector::LastRow { after_merge: true })
            );
            assert_eq!(provider.scan_request().series_row_selector, None);
        }
    }

    #[test]
    fn union_inside_single_evaluation_blocks_both_branches() {
        let provider = Arc::new(mock_table_provider(RegionId::new(1, 1)));
        let union = LogicalPlanBuilder::from(scan_plan(provider.clone(), "left"))
            .union(scan_plan(provider, "right"))
            .unwrap()
            .build()
            .unwrap();
        let rewritten = ScanHintRule
            .rewrite(single_evaluation(union), &OptimizerContext::default())
            .unwrap()
            .data;

        assert_eq!(
            scan_requests(&rewritten)
                .into_iter()
                .map(|request| request.series_row_selector)
                .collect::<Vec<_>>(),
            vec![None, None]
        );
    }

    #[test]
    fn residual_time_filters_do_not_set_last_row() {
        for predicate in [
            col("ts").lt(lit(1_i64)),
            Expr::Cast(Cast::new(Box::new(col("ts")), DataType::Int64)).gt(lit(1_i64)),
        ] {
            let provider = Arc::new(mock_table_provider(RegionId::new(1, 1)));
            let input = LogicalPlanBuilder::from(scan_plan(provider, "t"))
                .filter(predicate)
                .unwrap()
                .build()
                .unwrap();
            let rewritten = ScanHintRule
                .rewrite(single_evaluation(input), &OptimizerContext::default())
                .unwrap()
                .data;

            assert_eq!(scan_requests(&rewritten)[0].series_row_selector, None);
        }
    }

    #[test]
    fn inner_single_evaluation_resets_residual_filter() {
        let provider = Arc::new(mock_table_provider(RegionId::new(1, 1)));
        let filtered = LogicalPlanBuilder::from(single_evaluation(scan_plan(provider, "t")))
            .filter(col("v0").gt(lit(1.0_f64)))
            .unwrap()
            .build()
            .unwrap();
        let rewritten = ScanHintRule
            .rewrite(single_evaluation(filtered), &OptimizerContext::default())
            .unwrap()
            .data;

        assert_eq!(
            scan_requests(&rewritten)[0].series_row_selector,
            Some(TimeSeriesRowSelector::LastRow { after_merge: true })
        );
    }

    #[test]
    fn single_evaluation_with_tag_filter_sets_last_row() {
        let provider = Arc::new(mock_table_provider(RegionId::new(1, 1)));
        let plan = instant_plan_with_filters(provider, vec![col("k0").eq(lit("tag"))]);
        let rewritten = ScanHintRule
            .rewrite(plan, &OptimizerContext::default())
            .unwrap()
            .data;

        assert_eq!(
            scan_requests(&rewritten)[0].series_row_selector,
            Some(TimeSeriesRowSelector::LastRow { after_merge: true })
        );
    }

    #[test]
    fn single_evaluation_with_timestamp_filter_sets_last_row() {
        let provider = Arc::new(mock_table_provider(RegionId::new(1, 1)));
        let plan = instant_plan_with_filters(provider, vec![col("ts").gt_eq(lit(1_i64))]);
        let rewritten = ScanHintRule
            .rewrite(plan, &OptimizerContext::default())
            .unwrap()
            .data;

        assert_eq!(
            scan_requests(&rewritten)[0].series_row_selector,
            Some(TimeSeriesRowSelector::LastRow { after_merge: true })
        );
    }

    #[test]
    fn single_evaluation_with_cast_timestamp_filter_does_not_set_last_row() {
        let provider = Arc::new(mock_table_provider(RegionId::new(1, 1)));
        let filter = Expr::Cast(Cast::new(Box::new(col("ts")), DataType::Int64)).gt(lit(1_i64));
        let plan = instant_plan_with_filters(provider, vec![filter]);
        let rewritten = ScanHintRule
            .rewrite(plan, &OptimizerContext::default())
            .unwrap()
            .data;

        assert_eq!(scan_requests(&rewritten)[0].series_row_selector, None);
    }

    #[test]
    fn single_evaluation_with_multi_column_timestamp_filter_does_not_set_last_row() {
        let provider = Arc::new(mock_table_provider(RegionId::new(1, 1)));
        let plan = instant_plan_with_filters(provider, vec![col("ts").gt(col("k0"))]);
        let rewritten = ScanHintRule
            .rewrite(plan, &OptimizerContext::default())
            .unwrap()
            .data;

        assert_eq!(scan_requests(&rewritten)[0].series_row_selector, None);
    }

    #[test]
    fn single_evaluation_with_field_filter_does_not_set_last_row() {
        let provider = Arc::new(mock_table_provider(RegionId::new(1, 1)));
        let plan = instant_plan_with_filters(provider, vec![col("v0").gt(lit(1.0_f64))]);
        let rewritten = ScanHintRule
            .rewrite(plan, &OptimizerContext::default())
            .unwrap()
            .data;

        assert_eq!(scan_requests(&rewritten)[0].series_row_selector, None);
    }

    #[test]
    fn single_evaluation_with_unknown_filter_does_not_set_last_row() {
        let provider = Arc::new(mock_table_provider(RegionId::new(1, 1)));
        let plan = instant_plan_with_filters(provider, vec![col("unknown").eq(lit(1_i64))]);
        let rewritten = ScanHintRule
            .rewrite(plan, &OptimizerContext::default())
            .unwrap()
            .data;

        assert_eq!(scan_requests(&rewritten)[0].series_row_selector, None);
    }

    #[test]
    fn expression_subquery_isolated_from_outer_single_evaluation() {
        let outer_provider = Arc::new(mock_table_provider(RegionId::new(1, 1)));
        let inner_provider = Arc::new(mock_table_provider(RegionId::new(2, 1)));
        let plan = instant_with_expression_subquery(
            outer_provider.clone(),
            instant_plan_named(inner_provider, "inner", 2000),
            1000,
        );
        let rewritten = ScanHintRule
            .rewrite(plan, &OptimizerContext::default())
            .unwrap()
            .data;

        let requests = scan_requests_with_names(&rewritten)
            .into_iter()
            .collect::<HashMap<_, _>>();
        assert_eq!(requests["outer"].series_row_selector, None);
        assert_eq!(requests["inner"].series_row_selector, None);
    }

    #[test]
    fn expression_subquery_can_start_its_own_single_evaluation() {
        let outer_provider = Arc::new(mock_table_provider(RegionId::new(1, 1)));
        let inner_provider = Arc::new(mock_table_provider(RegionId::new(2, 1)));
        let plan = instant_with_expression_subquery(
            outer_provider,
            instant_plan_named(inner_provider, "inner", 1000),
            1000,
        );
        let rewritten = ScanHintRule
            .rewrite(plan, &OptimizerContext::default())
            .unwrap()
            .data;

        let requests = scan_requests_with_names(&rewritten)
            .into_iter()
            .collect::<HashMap<_, _>>();
        assert_eq!(requests["outer"].series_row_selector, None);
        assert_eq!(
            requests["inner"].series_row_selector,
            Some(TimeSeriesRowSelector::LastRow { after_merge: true })
        );
    }

    #[test]
    fn nested_single_outer_range_inner_does_not_set_inner_last_row() {
        let provider = Arc::new(mock_table_provider(RegionId::new(1, 1)));
        let plan = instant_plan_named(provider.clone(), "nested", 2000);
        let plan = LogicalPlan::Extension(Extension {
            node: Arc::new(InstantManipulate::new(
                1000,
                1000,
                1000,
                1000,
                "ts".to_string(),
                vec![],
                Some("v0".to_string()),
                plan,
            )),
        });
        let rewritten = ScanHintRule
            .rewrite(plan, &OptimizerContext::default())
            .unwrap()
            .data;
        assert_eq!(scan_requests(&rewritten)[0].series_row_selector, None);
        assert_eq!(provider.scan_request().series_row_selector, None);
    }

    #[test]
    fn nested_range_outer_single_inner_sets_inner_last_row() {
        let provider = Arc::new(mock_table_provider(RegionId::new(1, 1)));
        let plan = instant_plan_named(provider.clone(), "nested", 1000);
        let plan = LogicalPlan::Extension(Extension {
            node: Arc::new(InstantManipulate::new(
                1000,
                2000,
                1000,
                1000,
                "ts".to_string(),
                vec![],
                Some("v0".to_string()),
                plan,
            )),
        });
        let rewritten = ScanHintRule
            .rewrite(plan, &OptimizerContext::default())
            .unwrap()
            .data;
        assert_eq!(
            scan_requests(&rewritten)[0].series_row_selector,
            Some(TimeSeriesRowSelector::LastRow { after_merge: true })
        );
    }

    #[test]
    fn shared_provider_isolated_between_single_and_range_scans() {
        let provider = Arc::new(mock_table_provider(RegionId::new(1, 1)));
        let plan = LogicalPlanBuilder::from(instant_plan(provider.clone(), 1000))
            .union(instant_plan(provider.clone(), 2000))
            .unwrap()
            .build()
            .unwrap();
        let rewritten = ScanHintRule
            .rewrite(plan, &OptimizerContext::default())
            .unwrap()
            .data;
        let requests = scan_requests(&rewritten);

        assert_eq!(requests.len(), 2);
        assert_eq!(
            requests[0].series_row_selector,
            Some(TimeSeriesRowSelector::LastRow { after_merge: true })
        );
        assert_eq!(requests[1].series_row_selector, None);
        assert_eq!(provider.scan_request().series_row_selector, None);
    }

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
        let rewritten = ScanHintRule.rewrite(plan, &context).unwrap().data;

        // should read the first (with `.sort(true, false)`) sort option
        let scan_req = scan_requests(&rewritten)[0].clone();
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
        let plan = last_value_aggregate(scan_plan(provider.clone(), "t"));

        let context = OptimizerContext::default();
        let rewritten = ScanHintRule.rewrite(plan, &context).unwrap().data;

        let scan_req = scan_requests(&rewritten)[0].clone();
        assert_eq!(
            scan_req.series_row_selector,
            Some(TimeSeriesRowSelector::LastRow { after_merge: false })
        );
    }

    #[test]
    fn set_order_hint_sets_per_series_distribution_for_tsid_sort() {
        let provider = Arc::new(mock_table_provider_with_tsid(RegionId::new(1, 1)));
        let table_source = Arc::new(DefaultTableSource::new(provider.clone()));
        let plan = LogicalPlanBuilder::scan("t", table_source, None)
            .unwrap()
            .sort(vec![
                col(DATA_SCHEMA_TSID_COLUMN_NAME).sort(true, true),
                col("ts").sort(true, true),
            ])
            .unwrap()
            .build()
            .unwrap();

        let context = OptimizerContext::default();
        let rewritten = ScanHintRule.rewrite(plan, &context).unwrap().data;

        let scan_req = scan_requests(&rewritten)[0].clone();
        assert_eq!(
            scan_req.distribution,
            Some(TimeSeriesDistribution::PerSeries)
        );
    }
}
