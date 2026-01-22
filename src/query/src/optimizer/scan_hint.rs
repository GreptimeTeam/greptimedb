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
#[cfg(feature = "vector_index")]
use std::collections::{HashMap, VecDeque};

use api::v1::SemanticType;
use arrow_schema::SortOptions;
use common_function::aggrs::aggr_wrapper::aggr_state_func_name;
use common_recordbatch::OrderOption;
use datafusion::datasource::DefaultTableSource;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion, TreeNodeVisitor};
use datafusion_common::{Column, Result};
#[cfg(feature = "vector_index")]
use datafusion_common::ScalarValue;
use datafusion_expr::expr::Sort;
#[cfg(feature = "vector_index")]
use datafusion_expr::logical_plan::FetchType;
use datafusion_expr::{Expr, LogicalPlan, utils};
#[cfg(feature = "vector_index")]
use datafusion_expr::SortExpr;
use datafusion_optimizer::{OptimizerConfig, OptimizerRule};
#[cfg(feature = "vector_index")]
use datafusion_sql::TableReference;
#[cfg(feature = "vector_index")]
use datatypes::types::parse_string_to_vector_type_value;
use store_api::storage::{TimeSeriesDistribution, TimeSeriesRowSelector};
#[cfg(feature = "vector_index")]
use store_api::storage::{VectorDistanceMetric, VectorSearchRequest};
#[cfg(feature = "vector_index")]
use table::table::adapter::DfTableProviderAdapter;

use crate::dummy_catalog::DummyTableProvider;
#[cfg(feature = "vector_index")]
use common_function::scalars::vector::distance::{
    VEC_COS_DISTANCE, VEC_DOT_PRODUCT, VEC_L2SQ_DISTANCE,
};

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
        let mut visitor = ScanHintVisitor::default();
        let _ = plan.visit(&mut visitor)?;

        if visitor.need_rewrite() {
            plan.transform_down(&mut |plan| Self::set_hints(plan, &mut visitor))
        } else {
            Ok(Transformed::no(plan))
        }
    }

    fn set_hints(
        plan: LogicalPlan,
        visitor: &mut ScanHintVisitor,
    ) -> Result<Transformed<LogicalPlan>> {
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

                        #[cfg(feature = "vector_index")]
                        if let Some(hint) = visitor.take_vector_hint(&table_scan.table_name)
                            && let Some(vector_request) = visitor.build_vector_request_from_dummy(
                                adapter,
                                &table_scan.table_name,
                                &hint,
                            )
                        {
                            adapter.with_vector_search_hint(vector_request);
                        }
                        transformed = true;
                    }

                    #[cfg(feature = "vector_index")]
                    if let Some(adapter) = source
                        .table_provider
                        .as_any()
                        .downcast_ref::<DfTableProviderAdapter>()
                        && let Some(hint) = visitor.take_vector_hint(&table_scan.table_name)
                        && let Some(vector_request) = visitor.build_vector_request_from_adapter(
                            adapter,
                            &table_scan.table_name,
                            &hint,
                        )
                    {
                        adapter.with_vector_search_hint(vector_request);
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

        let mut sort_expr_cursor = order_expr.iter().filter_map(|s| s.expr.try_as_col());
        let region_metadata = adapter.region_metadata();
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
    /// Vector distance function info extracted from ORDER BY.
    #[cfg(feature = "vector_index")]
    current_distance: Option<VectorDistanceInfo>,
    /// LIMIT value (k for KNN).
    #[cfg(feature = "vector_index")]
    current_limit: Option<VectorLimitInfo>,
    #[cfg(feature = "vector_index")]
    distance_stack: Vec<Option<VectorDistanceInfo>>,
    #[cfg(feature = "vector_index")]
    limit_stack: Vec<Option<VectorLimitInfo>>,
    #[cfg(feature = "vector_index")]
    vector_hints: HashMap<TableReference, VecDeque<VectorHintEntry>>,
}

#[cfg(feature = "vector_index")]
#[derive(Clone)]
struct VectorDistanceInfo {
    table_reference: Option<TableReference>,
    column_name: String,
    query_vector: Vec<f32>,
    metric: VectorDistanceMetric,
}

#[cfg(feature = "vector_index")]
#[derive(Clone)]
struct VectorLimitInfo {
    fetch: usize,
    skip: usize,
}

#[cfg(feature = "vector_index")]
impl VectorLimitInfo {
    fn k(&self) -> Option<usize> {
        self.fetch.checked_add(self.skip)
    }
}

#[cfg(feature = "vector_index")]
#[derive(Clone)]
struct VectorHintEntry {
    distance: VectorDistanceInfo,
    limit: VectorLimitInfo,
}

impl TreeNodeVisitor<'_> for ScanHintVisitor {
    type Node = LogicalPlan;

    fn f_down(&mut self, node: &Self::Node) -> Result<TreeNodeRecursion> {
        #[cfg(feature = "vector_index")]
        if let LogicalPlan::Limit(limit) = node {
            self.limit_stack.push(self.current_limit.take());
            self.current_limit = Self::extract_limit_info(limit);
        }

        // Get order requirement from sort plan
        if let LogicalPlan::Sort(sort) = node {
            self.order_expr = Some(sort.expr.clone());

            #[cfg(feature = "vector_index")]
            {
                self.distance_stack.push(self.current_distance.take());
                self.current_distance = Self::extract_distance_from_sort(sort);
            }
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
                if (func.func.name() != "last_value"
                    && func.func.name() != aggr_state_func_name("last_value"))
                    || func.params.filter.is_some()
                    || func.params.distinct
                {
                    is_all_last_value = false;
                    break;
                }
                // check order by requirement
                let order_by = &func.params.order_by;
                if let Some(first_order_by) = order_by.first()
                    && order_by.len() == 1
                {
                    if let Some(existing_order_by) = &order_by_expr {
                        if existing_order_by != first_order_by {
                            is_all_last_value = false;
                            break;
                        }
                    } else {
                        // only allow `order by xxx [ASC]`, xxx is a bare column reference so `last_value()` is the max
                        // value of the column.
                        if !first_order_by.asc || !matches!(&first_order_by.expr, Expr::Column(_)) {
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
                let order_by_expr = order_by_expr.unwrap();
                let Expr::Column(order_by_col) = order_by_expr.expr else {
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

        #[cfg(feature = "vector_index")]
        if let LogicalPlan::TableScan(table_scan) = node {
            self.record_vector_hint(table_scan);
        }

        Ok(TreeNodeRecursion::Continue)
    }

    fn f_up(&mut self, _node: &Self::Node) -> Result<TreeNodeRecursion> {
        #[cfg(feature = "vector_index")]
        match _node {
            LogicalPlan::Limit(_) => {
                if let Some(previous) = self.limit_stack.pop() {
                    self.current_limit = previous;
                }
            }
            LogicalPlan::Sort(_) => {
                if let Some(previous) = self.distance_stack.pop() {
                    self.current_distance = previous;
                }
            }
            _ => {}
        }

        Ok(TreeNodeRecursion::Continue)
    }
}

impl ScanHintVisitor {
    fn need_rewrite(&self) -> bool {
        let base = self.order_expr.is_some() || self.ts_row_selector.is_some();
        #[cfg(feature = "vector_index")]
        {
            return base || !self.vector_hints.is_empty();
        }
        #[cfg(not(feature = "vector_index"))]
        {
            base
        }
    }

    #[cfg(feature = "vector_index")]
    fn build_vector_request_from_dummy(
        &self,
        provider: &DummyTableProvider,
        table_name: &TableReference,
        hint: &VectorHintEntry,
    ) -> Option<VectorSearchRequest> {
        let info = &hint.distance;
        let k = hint.limit.k()?;

        if let Some(ref hint_table) = info.table_reference
            && table_name != hint_table
        {
            return None;
        }

        let metadata = provider.region_metadata();
        let column = metadata.column_by_name(&info.column_name)?;

        Some(VectorSearchRequest {
            column_id: column.column_id,
            query_vector: info.query_vector.clone(),
            k,
            metric: info.metric,
        })
    }

    #[cfg(feature = "vector_index")]
    fn build_vector_request_from_adapter(
        &self,
        adapter: &DfTableProviderAdapter,
        table_name: &TableReference,
        hint: &VectorHintEntry,
    ) -> Option<VectorSearchRequest> {
        let info = &hint.distance;
        let k = hint.limit.k()?;

        if let Some(ref hint_table) = info.table_reference
            && table_name != hint_table
        {
            return None;
        }

        let table = adapter.table();
        let table_info = table.table_info();
        let schema = &table_info.meta.schema;

        let column_index = schema.column_index_by_name(&info.column_name)?;
        let column_id = *table_info.meta.column_ids.get(column_index)?;

        Some(VectorSearchRequest {
            column_id,
            query_vector: info.query_vector.clone(),
            k,
            metric: info.metric,
        })
    }

    #[cfg(feature = "vector_index")]
    fn extract_distance_info(expr: &Expr) -> Option<VectorDistanceInfo> {
        let Expr::ScalarFunction(func) = expr else {
            return None;
        };

        let func_name = func.name().to_lowercase();
        let metric = match func_name.as_str() {
            VEC_L2SQ_DISTANCE => VectorDistanceMetric::L2sq,
            VEC_COS_DISTANCE => VectorDistanceMetric::Cosine,
            VEC_DOT_PRODUCT => VectorDistanceMetric::InnerProduct,
            _ => return None,
        };

        if func.args.len() != 2 {
            return None;
        }

        let (table_reference, column_name) = match &func.args[0] {
            Expr::Column(col) => (col.relation.clone(), col.name.clone()),
            _ => return None,
        };

        let query_vector = Self::extract_query_vector(&func.args[1])?;

        Some(VectorDistanceInfo {
            table_reference,
            column_name,
            query_vector,
            metric,
        })
    }

    #[cfg(feature = "vector_index")]
    fn extract_distance_from_sort(
        sort: &datafusion_expr::logical_plan::Sort,
    ) -> Option<VectorDistanceInfo> {
        if sort.expr.len() != 1 {
            return None;
        }
        let sort_expr: &SortExpr = &sort.expr[0];
        let info = Self::extract_distance_info(&sort_expr.expr)?;
        let expected_asc = info.metric != VectorDistanceMetric::InnerProduct;
        if sort_expr.asc == expected_asc {
            Some(info)
        } else {
            None
        }
    }

    #[cfg(feature = "vector_index")]
    fn extract_limit_info(limit: &datafusion_expr::logical_plan::Limit) -> Option<VectorLimitInfo> {
        let fetch = match limit.get_fetch_type().ok()? {
            FetchType::Literal(fetch) => fetch?,
            FetchType::UnsupportedExpr => return None,
        };
        let skip = match limit.get_skip_type().ok()? {
            datafusion_expr::logical_plan::SkipType::Literal(skip) => skip,
            datafusion_expr::logical_plan::SkipType::UnsupportedExpr => return None,
        };
        Some(VectorLimitInfo { fetch, skip })
    }

    #[cfg(feature = "vector_index")]
    fn record_vector_hint(&mut self, table_scan: &datafusion_expr::logical_plan::TableScan) {
        let Some(limit) = self.current_limit.as_ref() else {
            return;
        };
        let Some(distance) = self.current_distance.as_ref() else {
            return;
        };
        if let Some(ref hint_table) = distance.table_reference
            && hint_table != &table_scan.table_name
        {
            return;
        }

        self.vector_hints
            .entry(table_scan.table_name.clone())
            .or_default()
            .push_back(VectorHintEntry {
                distance: distance.clone(),
                limit: limit.clone(),
            });
    }

    #[cfg(feature = "vector_index")]
    fn take_vector_hint(&mut self, table_name: &TableReference) -> Option<VectorHintEntry> {
        let hints = self.vector_hints.get_mut(table_name)?;
        let hint = hints.pop_front();
        if hints.is_empty() {
            self.vector_hints.remove(table_name);
        }
        hint
    }

    #[cfg(feature = "vector_index")]
    fn extract_query_vector(expr: &Expr) -> Option<Vec<f32>> {
        match expr {
            Expr::Literal(scalar, _) => match scalar {
                ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => {
                    Self::parse_json_vector(s)
                }
                ScalarValue::Binary(Some(bytes)) | ScalarValue::LargeBinary(Some(bytes)) => {
                    Self::parse_binary_vector(bytes)
                }
                _ => None,
            },
            _ => None,
        }
    }

    #[cfg(feature = "vector_index")]
    fn parse_json_vector(s: &str) -> Option<Vec<f32>> {
        let trimmed = s.trim();
        if !trimmed.starts_with('[') || !trimmed.ends_with(']') {
            return None;
        }
        parse_string_to_vector_type_value(trimmed, None)
            .ok()
            .and_then(|bytes| if bytes.is_empty() { None } else { Some(bytes) })
            .and_then(|bytes| Self::parse_binary_vector(&bytes))
    }

    #[cfg(feature = "vector_index")]
    fn parse_binary_vector(bytes: &[u8]) -> Option<Vec<f32>> {
        if !bytes.len().is_multiple_of(4) {
            return None;
        }
        Some(
            bytes
                .chunks(4)
                .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
                .collect(),
        )
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use datafusion::functions_aggregate::first_last::last_value_udaf;
    use datafusion_expr::expr::{AggregateFunction, AggregateFunctionParams};
    use datafusion_expr::{LogicalPlanBuilder, col};
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
        ScanHintRule.rewrite(plan, &context).unwrap();

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
            .unwrap();

        let context = OptimizerContext::default();
        ScanHintRule.rewrite(plan, &context).unwrap();

        let scan_req = provider.scan_request();
        let _ = scan_req.series_row_selector.unwrap();
    }
}

#[cfg(all(test, feature = "vector_index"))]
mod vector_search_tests {
    use std::sync::Arc;

    use api::v1::SemanticType;
    use common_function::function::Function;
    use common_function::scalars::udf::create_udf;
    use common_recordbatch::{RecordBatch, RecordBatchStream, SendableRecordBatchStream};
    use datafusion::datasource::DefaultTableSource;
    use datafusion::logical_expr::ColumnarValue;
    use datafusion_common::{Column, DataFusionError, ScalarValue};
    use datafusion_expr::expr::ScalarFunction;
    use datafusion_expr::logical_plan::JoinType;
    use datafusion_expr::{LogicalPlanBuilder, Signature, Volatility, col, lit};
    use datafusion_optimizer::{OptimizerContext, OptimizerRule};
    use datatypes::schema::{ColumnSchema, SchemaBuilder, SchemaRef};
    use futures::Stream;
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};
    use store_api::storage::ConcreteDataType;
    use table::metadata::{FilterPushDownType, TableInfoBuilder, TableMeta, TableType};
    use table::table::adapter::DfTableProviderAdapter;
    use table::{Table, TableRef};

    use super::*;
    use crate::optimizer::test_util::MetaRegionEngine;

    struct TestVectorFunction {
        name: &'static str,
        signature: Signature,
    }

    impl TestVectorFunction {
        fn new(name: &'static str) -> Self {
            Self {
                name,
                signature: Signature::any(2, Volatility::Immutable),
            }
        }
    }

    impl std::fmt::Display for TestVectorFunction {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.name)
        }
    }

    impl Function for TestVectorFunction {
        fn name(&self) -> &str {
            self.name
        }

        fn return_type(
            &self,
            _input_types: &[datatypes::arrow::datatypes::DataType],
        ) -> Result<datatypes::arrow::datatypes::DataType> {
            Ok(datatypes::arrow::datatypes::DataType::Float32)
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn invoke_with_args(
            &self,
            _args: datafusion_expr::ScalarFunctionArgs,
        ) -> Result<ColumnarValue> {
            Err(DataFusionError::Execution(
                "test udf should not be invoked".to_string(),
            ))
        }
    }

    fn vec_distance_expr(function_name: &'static str) -> Expr {
        let udf = create_udf(Arc::new(TestVectorFunction::new(function_name)));
        Expr::ScalarFunction(ScalarFunction::new_udf(
            Arc::new(udf),
            vec![
                col("v"),
                lit(ScalarValue::Utf8(Some("[1.0, 2.0]".to_string()))),
            ],
        ))
    }

    fn build_dummy_provider(column_id: u32) -> Arc<DummyTableProvider> {
        let mut builder = RegionMetadataBuilder::new(0.into());
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("k0", ConcreteDataType::string_datatype(), true),
                semantic_type: SemanticType::Tag,
                column_id: 1,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "ts",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Timestamp,
                column_id: 2,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("v", ConcreteDataType::vector_datatype(2), false),
                semantic_type: SemanticType::Field,
                column_id,
            })
            .primary_key(vec![1]);
        let metadata = Arc::new(builder.build().unwrap());
        let engine = Arc::new(MetaRegionEngine::with_metadata(metadata.clone()));
        Arc::new(DummyTableProvider::new(0.into(), engine, metadata))
    }

    fn build_vector_table() -> TableRef {
        let schema = {
            let columns = vec![
                ColumnSchema::new("k0", ConcreteDataType::string_datatype(), true),
                ColumnSchema::new(
                    "ts",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                )
                .with_time_index(true),
                ColumnSchema::new("v", ConcreteDataType::vector_datatype(2), false),
            ];
            Arc::new(
                SchemaBuilder::try_from_columns(columns)
                    .unwrap()
                    .build()
                    .unwrap(),
            )
        };

        struct TestDataSource {
            schema: SchemaRef,
        }

        impl TestDataSource {
            fn new(schema: SchemaRef) -> Self {
                Self { schema }
            }
        }

        struct EmptyStream {
            schema: SchemaRef,
        }

        impl RecordBatchStream for EmptyStream {
            fn schema(&self) -> SchemaRef {
                self.schema.clone()
            }

            fn output_ordering(&self) -> Option<&[common_recordbatch::OrderOption]> {
                None
            }

            fn metrics(&self) -> Option<common_recordbatch::adapter::RecordBatchMetrics> {
                None
            }
        }

        impl Stream for EmptyStream {
            type Item = common_recordbatch::error::Result<RecordBatch>;

            fn poll_next(
                self: std::pin::Pin<&mut Self>,
                _cx: &mut std::task::Context<'_>,
            ) -> std::task::Poll<Option<Self::Item>> {
                std::task::Poll::Ready(None)
            }
        }

        impl store_api::data_source::DataSource for TestDataSource {
            fn get_stream(
                &self,
                request: store_api::storage::ScanRequest,
            ) -> Result<SendableRecordBatchStream, common_error::ext::BoxedError> {
                let projected_schema = match &request.projection {
                    Some(projection) => Arc::new(self.schema.try_project(projection).unwrap()),
                    None => self.schema.clone(),
                };
                Ok(Box::pin(EmptyStream {
                    schema: projected_schema,
                }))
            }
        }

        let table_meta = TableMeta {
            schema: schema.clone(),
            primary_key_indices: vec![0],
            value_indices: vec![2],
            engine: "test_engine".to_string(),
            next_column_id: 4,
            options: Default::default(),
            created_on: Default::default(),
            updated_on: Default::default(),
            partition_key_indices: vec![0],
            column_ids: vec![1, 2, 3],
        };

        let table_info = TableInfoBuilder::default()
            .table_id(1)
            .name("t".to_string())
            .catalog_name(common_catalog::consts::DEFAULT_CATALOG_NAME)
            .schema_name(common_catalog::consts::DEFAULT_SCHEMA_NAME)
            .table_version(0)
            .table_type(TableType::Base)
            .meta(table_meta)
            .build()
            .unwrap();

        let data_source = Arc::new(TestDataSource::new(schema));
        Arc::new(Table::new(
            Arc::new(table_info),
            FilterPushDownType::Unsupported,
            data_source,
        ))
    }

    #[test]
    fn test_parse_json_vector() {
        assert_eq!(
            ScanHintVisitor::parse_json_vector("[1.0, 2.0, 3.0]"),
            Some(vec![1.0, 2.0, 3.0])
        );
        assert_eq!(
            ScanHintVisitor::parse_json_vector("[1.5, -2.3, 0.0]"),
            Some(vec![1.5, -2.3, 0.0])
        );
        assert_eq!(ScanHintVisitor::parse_json_vector("invalid"), None);
        assert_eq!(ScanHintVisitor::parse_json_vector("[]"), None);
        assert_eq!(ScanHintVisitor::parse_json_vector(""), None);
        assert_eq!(ScanHintVisitor::parse_json_vector("["), None);
        assert_eq!(ScanHintVisitor::parse_json_vector("[1.0, abc]"), None);
    }

    #[test]
    fn test_parse_binary_vector() {
        let v1: f32 = 1.0;
        let v2: f32 = 2.0;
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&v1.to_le_bytes());
        bytes.extend_from_slice(&v2.to_le_bytes());

        let result = ScanHintVisitor::parse_binary_vector(&bytes);
        assert_eq!(result, Some(vec![1.0, 2.0]));

        let result = ScanHintVisitor::parse_binary_vector(&[1, 2, 3]);
        assert_eq!(result, None);
    }

    #[test]
    fn test_dummy_provider_vector_hint() {
        let dummy_provider = build_dummy_provider(10);
        let table_source = Arc::new(DefaultTableSource::new(dummy_provider.clone()));
        let expr = vec_distance_expr(VEC_L2SQ_DISTANCE);
        let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
            .unwrap()
            .sort(vec![expr.sort(true, false)])
            .unwrap()
            .limit(0, Some(5))
            .unwrap()
            .build()
            .unwrap();

        let context = OptimizerContext::default();
        let _ = ScanHintRule.rewrite(plan, &context).unwrap();

        let hint = dummy_provider.get_vector_search_hint().unwrap();
        assert_eq!(hint.column_id, 10);
        assert_eq!(hint.k, 5);
        assert_eq!(hint.metric, VectorDistanceMetric::L2sq);
        assert_eq!(hint.query_vector, vec![1.0, 2.0]);
    }

    #[test]
    fn test_limit_offset_for_vector_hint() {
        let dummy_provider = build_dummy_provider(10);
        let table_source = Arc::new(DefaultTableSource::new(dummy_provider.clone()));
        let expr = vec_distance_expr(VEC_L2SQ_DISTANCE);
        let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
            .unwrap()
            .sort(vec![expr.sort(true, false)])
            .unwrap()
            .limit(5, Some(10))
            .unwrap()
            .build()
            .unwrap();

        let context = OptimizerContext::default();
        let _ = ScanHintRule.rewrite(plan, &context).unwrap();

        let hint = dummy_provider.get_vector_search_hint().unwrap();
        assert_eq!(hint.k, 15);
    }

    #[test]
    fn test_inner_product_sort_direction() {
        let dummy_provider = build_dummy_provider(10);
        let table_source = Arc::new(DefaultTableSource::new(dummy_provider.clone()));
        let expr = vec_distance_expr(VEC_DOT_PRODUCT);
        let plan = LogicalPlanBuilder::scan_with_filters("t", table_source.clone(), None, vec![])
            .unwrap()
            .sort(vec![expr.clone().sort(true, false)])
            .unwrap()
            .limit(0, Some(3))
            .unwrap()
            .build()
            .unwrap();

        let context = OptimizerContext::default();
        let _ = ScanHintRule.rewrite(plan, &context).unwrap();
        assert!(dummy_provider.get_vector_search_hint().is_none());

        let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
            .unwrap()
            .sort(vec![expr.sort(false, false)])
            .unwrap()
            .limit(0, Some(3))
            .unwrap()
            .build()
            .unwrap();
        let _ = ScanHintRule.rewrite(plan, &context).unwrap();
        let hint = dummy_provider.get_vector_search_hint().unwrap();
        assert_eq!(hint.metric, VectorDistanceMetric::InnerProduct);
    }

    #[test]
    fn test_adapter_overfetch_with_filter() {
        let table = build_vector_table();
        let adapter = Arc::new(DfTableProviderAdapter::new(table));
        let table_source = Arc::new(DefaultTableSource::new(adapter.clone()));
        let expr = vec_distance_expr(VEC_L2SQ_DISTANCE);
        let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
            .unwrap()
            .filter(col("k0").eq(lit("hello")))
            .unwrap()
            .sort(vec![expr.sort(true, false)])
            .unwrap()
            .limit(0, Some(7))
            .unwrap()
            .build()
            .unwrap();

        let context = OptimizerContext::default();
        let _ = ScanHintRule.rewrite(plan, &context).unwrap();

        let hint = adapter.get_vector_search_hint().unwrap();
        assert_eq!(hint.column_id, 3);
        assert_eq!(hint.k, 7);
        assert_eq!(hint.metric, VectorDistanceMetric::L2sq);
    }

    #[test]
    fn test_no_limit_clause() {
        let dummy_provider = build_dummy_provider(10);
        let table_source = Arc::new(DefaultTableSource::new(dummy_provider.clone()));
        let expr = vec_distance_expr(VEC_L2SQ_DISTANCE);
        let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
            .unwrap()
            .sort(vec![expr.sort(true, false)])
            .unwrap()
            .build()
            .unwrap();

        let context = OptimizerContext::default();
        let _ = ScanHintRule.rewrite(plan, &context).unwrap();

        assert!(dummy_provider.get_vector_search_hint().is_none());
    }

    #[test]
    fn test_limit_scoped_to_sort_branch() {
        let t1_provider = build_dummy_provider(10);
        let t2_provider = build_dummy_provider(20);
        let t1_source = Arc::new(DefaultTableSource::new(t1_provider.clone()));
        let t2_source = Arc::new(DefaultTableSource::new(t2_provider.clone()));
        let expr = vec_distance_expr(VEC_L2SQ_DISTANCE);

        let left = LogicalPlanBuilder::scan_with_filters("t1", t1_source, None, vec![])
            .unwrap()
            .limit(0, Some(5))
            .unwrap()
            .build()
            .unwrap();

        let right = LogicalPlanBuilder::scan_with_filters("t2", t2_source, None, vec![])
            .unwrap()
            .sort(vec![expr.sort(true, false)])
            .unwrap()
            .build()
            .unwrap();

        let join_keys: (Vec<Column>, Vec<Column>) = (vec![], vec![]);
        let plan = LogicalPlanBuilder::from(left)
            .join(right, JoinType::Inner, join_keys, None)
            .unwrap()
            .build()
            .unwrap();

        let context = OptimizerContext::default();
        let _ = ScanHintRule.rewrite(plan, &context).unwrap();

        assert!(t1_provider.get_vector_search_hint().is_none());
        assert!(t2_provider.get_vector_search_hint().is_none());
    }

    #[test]
    fn test_multiple_order_by_expressions() {
        let dummy_provider = build_dummy_provider(10);
        let table_source = Arc::new(DefaultTableSource::new(dummy_provider.clone()));
        let expr = vec_distance_expr(VEC_L2SQ_DISTANCE);
        let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
            .unwrap()
            .sort(vec![expr.sort(true, false), col("k0").sort(true, false)])
            .unwrap()
            .limit(0, Some(5))
            .unwrap()
            .build()
            .unwrap();

        let context = OptimizerContext::default();
        let _ = ScanHintRule.rewrite(plan, &context).unwrap();

        assert!(dummy_provider.get_vector_search_hint().is_none());
    }

    fn vec_distance_expr_qualified(
        function_name: &'static str,
        table_name: &str,
        column_name: &str,
    ) -> Expr {
        use datafusion_common::Column;

        let udf = create_udf(Arc::new(TestVectorFunction::new(function_name)));
        let qualified_col = Expr::Column(Column::new(Some(table_name.to_string()), column_name));
        Expr::ScalarFunction(ScalarFunction::new_udf(
            Arc::new(udf),
            vec![
                qualified_col,
                lit(ScalarValue::Utf8(Some("[1.0, 2.0]".to_string()))),
            ],
        ))
    }

    #[test]
    fn test_qualified_column_scopes_hint_to_correct_table() {
        let t1_provider = build_dummy_provider(10);
        let t2_provider = build_dummy_provider(20);
        let t1_source = Arc::new(DefaultTableSource::new(t1_provider.clone()));
        let t2_source = Arc::new(DefaultTableSource::new(t2_provider.clone()));

        let expr = vec_distance_expr_qualified(VEC_L2SQ_DISTANCE, "t2", "v");

        let t1_plan = LogicalPlanBuilder::scan_with_filters("t1", t1_source, None, vec![])
            .unwrap()
            .sort(vec![expr.clone().sort(true, false)])
            .unwrap()
            .limit(0, Some(5))
            .unwrap()
            .build()
            .unwrap();

        let t2_plan = LogicalPlanBuilder::scan_with_filters("t2", t2_source, None, vec![])
            .unwrap()
            .sort(vec![expr.sort(true, false)])
            .unwrap()
            .limit(0, Some(5))
            .unwrap()
            .build()
            .unwrap();

        let context = OptimizerContext::default();

        let _ = ScanHintRule.rewrite(t1_plan, &context).unwrap();
        assert!(t1_provider.get_vector_search_hint().is_none());

        let _ = ScanHintRule.rewrite(t2_plan, &context).unwrap();
        let hint = t2_provider.get_vector_search_hint().unwrap();
        assert_eq!(hint.column_id, 20);
        assert_eq!(hint.k, 5);
    }

    #[test]
    fn test_scan_hint_pipeline_keeps_vector_search_hint() {
        let dummy_provider = build_dummy_provider(10);
        let table_source = Arc::new(DefaultTableSource::new(dummy_provider.clone()));
        let expr = vec_distance_expr(VEC_L2SQ_DISTANCE);
        let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
            .unwrap()
            .sort(vec![expr.sort(true, false)])
            .unwrap()
            .limit(0, Some(5))
            .unwrap()
            .build()
            .unwrap();

        let context = OptimizerContext::default();
        let _ = ScanHintRule.rewrite(plan, &context).unwrap();

        let scan_req = dummy_provider.scan_request();
        assert!(scan_req.vector_search.is_some());
    }
}
