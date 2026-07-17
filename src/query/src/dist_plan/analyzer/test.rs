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

use std::collections::{BTreeSet, HashMap, HashSet};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use api::v1::region::{RemoteDynFilterUnregister, RemoteDynFilterUpdate};
use arrow::array::{Float64Array, StringArray, TimestampMillisecondArray, UInt32Array};
use arrow::datatypes::{DataType, IntervalDayTime, TimeUnit};
use async_trait::async_trait;
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_error::ext::{BoxedError, PlainError};
use common_error::status_code::StatusCode;
use common_function::aggrs::aggr_wrapper::{StateMergeHelper, StateWrapper};
use common_recordbatch::adapter::{RecordBatchMetrics, RecordBatchStreamAdapter};
use common_recordbatch::error::Result as RecordBatchResult;
use common_recordbatch::{
    DfRecordBatch, OrderOption, RecordBatch, RecordBatchStream, SendableRecordBatchStream,
};
use common_telemetry::init_default_ut_logging;
use datafusion::datasource::{DefaultTableSource, MemTable};
use datafusion::execution::context::{QueryPlanner, SessionConfig};
use datafusion::execution::{SessionState, SessionStateBuilder};
use datafusion::functions_aggregate::expr_fn::avg;
use datafusion::functions_aggregate::min_max::{max, min};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner};
use datafusion::prelude::SessionContext;
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::{ExprSchema, JoinType, ScalarValue};
use datafusion_expr::expr::{Exists, ScalarFunction};
use datafusion_expr::{
    AggregateUDF, Expr, ExprSchemable as _, LogicalPlanBuilder, Operator, Subquery, binary_expr,
    col, lit,
};
use datafusion_functions::datetime::date_bin;
use datafusion_functions::datetime::expr_fn::now;
use datafusion_sql::TableReference;
use datatypes::data_type::ConcreteDataType;
use datatypes::schema::{ColumnSchema, SchemaBuilder, SchemaRef};
use futures::Stream;
use futures::task::{Context, Poll};
use pretty_assertions::assert_eq;
use regex::Regex;
use session::ReadPreference;
use session::context::QueryContext;
use snafu::ResultExt;
use store_api::data_source::DataSource;
use store_api::storage::{RegionId, ScanRequest};
use table::metadata::{
    FilterPushDownType, TableId, TableInfoBuilder, TableInfoRef, TableMeta, TableType,
};
use table::table::adapter::DfTableProviderAdapter;
use table::table::numbers::NumbersTable;
use table::table_name::TableName;
use table::{Table, TableRef};
use tokio::time::timeout;

use super::*;
use crate::dist_plan::MergeScanExec;
use crate::error::QueryExecutionSnafu;
use crate::query_engine::DefaultSerializer;
use crate::range_select::lowering::{RangeSelectLoweringAnalyzer, RangeSelectOptions};
use crate::range_select::plan::{
    RangeFn, RangeSelect, RangeSelectExec, RangeSelectExecMode, RangeSelectMode,
};
use crate::range_select::planner::RangeSelectPlanner;
use crate::region_query::RegionQueryHandler;

fn collect_merge_scan_remote_dyn_filter_producer_ids(
    plan: &LogicalPlan,
    producer_ids: &mut BTreeSet<RemoteDynFilterProducerId>,
) {
    let mut producer_id_list = Vec::new();
    collect_merge_scan_remote_dyn_filter_producer_id_list(plan, &mut producer_id_list);
    producer_ids.extend(producer_id_list);
}

struct MergeScanRemoteDynFilterProducerIdCollector<'a> {
    producer_ids: &'a mut Vec<RemoteDynFilterProducerId>,
}

impl TreeNodeRewriter for MergeScanRemoteDynFilterProducerIdCollector<'_> {
    type Node = LogicalPlan;

    fn f_up(&mut self, node: Self::Node) -> DfResult<Transformed<Self::Node>> {
        if let LogicalPlan::Extension(extension) = &node
            && let Some(merge_scan) = extension
                .node
                .as_any()
                .downcast_ref::<MergeScanLogicalPlan>()
        {
            self.producer_ids.push(
                merge_scan
                    .remote_dyn_filter_producer_id()
                    .expect("MergeScan remote dynamic filter producer id must be assigned"),
            );
        }

        Ok(Transformed::no(node))
    }
}

fn collect_merge_scan_remote_dyn_filter_producer_id_list(
    plan: &LogicalPlan,
    producer_ids: &mut Vec<RemoteDynFilterProducerId>,
) {
    let _ = plan
        .clone()
        .rewrite_with_subqueries(&mut MergeScanRemoteDynFilterProducerIdCollector { producer_ids })
        .unwrap();
}

fn assert_remote_table_scan_filters_are_safe(plan: &LogicalPlan) {
    let mut checked_filters = 0;
    assert_remote_table_scan_filters_are_safe_inner(plan, false, &mut checked_filters);
    assert!(
        checked_filters > 0,
        "expected at least one remote TableScan filter in plan:\n{plan}"
    );
}

fn assert_remote_table_scan_filters_are_safe_inner(
    plan: &LogicalPlan,
    in_merge_scan_remote_input: bool,
    checked_filters: &mut usize,
) {
    if let LogicalPlan::Extension(extension) = plan
        && let Some(merge_scan) = extension
            .node
            .as_any()
            .downcast_ref::<MergeScanLogicalPlan>()
    {
        assert_remote_table_scan_filters_are_safe_inner(merge_scan.input(), true, checked_filters);
    }

    if in_merge_scan_remote_input && let LogicalPlan::TableScan(table_scan) = plan {
        for filter in &table_scan.filters {
            assert_table_scan_filter_is_remote_safe(table_scan, filter);
            *checked_filters += 1;
        }
    }

    for child in plan.inputs() {
        assert_remote_table_scan_filters_are_safe_inner(
            child,
            in_merge_scan_remote_input,
            checked_filters,
        );
    }
}

fn assert_table_scan_filter_is_remote_safe(
    table_scan: &datafusion_expr::logical_plan::TableScan,
    filter: &Expr,
) {
    filter
        .apply(|expr| match expr {
            Expr::Exists(_)
            | Expr::InSubquery(_)
            | Expr::ScalarSubquery(_)
            | Expr::SetComparison(_)
            | Expr::OuterReferenceColumn(_, _) => {
                panic!("remote TableScan filter contains non-scan-local expression: {filter}")
            }
            _ => Ok(TreeNodeRecursion::Continue),
        })
        .unwrap();

    let mut columns = HashSet::new();
    expr_to_columns(filter, &mut columns).unwrap();
    for column in columns {
        assert!(
            table_scan
                .projected_schema
                .field_from_column(&column)
                .is_ok(),
            "remote TableScan filter references non-scan column {column}: {filter}\nscan schema: {:?}",
            table_scan.projected_schema
        );
    }
}

pub(crate) struct TestTable;

impl TestTable {
    pub fn table_with_name(table_id: TableId, name: String) -> TableRef {
        Self::table_with_filter_pushdown(table_id, name, FilterPushDownType::Unsupported)
    }

    pub fn table_with_filter_pushdown(
        table_id: TableId,
        name: String,
        filter_pushdown: FilterPushDownType,
    ) -> TableRef {
        let data_source = Arc::new(TestDataSource::new(Self::schema()));
        let table = Table::new(
            Self::table_info(table_id, name, "test_engine".to_string()),
            filter_pushdown,
            data_source,
        );
        Arc::new(table)
    }

    pub fn schema() -> SchemaRef {
        let column_schemas = vec![
            ColumnSchema::new("pk1", ConcreteDataType::string_datatype(), false),
            ColumnSchema::new("pk2", ConcreteDataType::string_datatype(), false),
            ColumnSchema::new("pk3", ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
            ColumnSchema::new("number", ConcreteDataType::uint32_datatype(), true),
        ];
        let schema = SchemaBuilder::try_from_columns(column_schemas)
            .unwrap()
            .build()
            .unwrap();
        Arc::new(schema)
    }

    pub fn table_info(table_id: TableId, name: String, engine: String) -> TableInfoRef {
        let table_meta = TableMeta {
            schema: Self::schema(),
            primary_key_indices: vec![0, 1, 2],
            value_indices: vec![4],
            engine,
            next_column_id: 5,
            options: Default::default(),
            created_on: Default::default(),
            updated_on: Default::default(),
            partition_key_indices: vec![0, 1],
            column_ids: vec![0, 1, 2, 3, 4],
        };

        let table_info = TableInfoBuilder::default()
            .table_id(table_id)
            .name(name)
            .catalog_name(DEFAULT_CATALOG_NAME)
            .schema_name(DEFAULT_SCHEMA_NAME)
            .table_version(0)
            .table_type(TableType::Base)
            .meta(table_meta)
            .build()
            .unwrap();
        Arc::new(table_info)
    }
}

struct TestDataSource {
    schema: SchemaRef,
}

impl TestDataSource {
    pub fn new(schema: SchemaRef) -> Self {
        Self { schema }
    }
}

impl DataSource for TestDataSource {
    fn get_stream(&self, request: ScanRequest) -> Result<SendableRecordBatchStream, BoxedError> {
        let projected_schema = match request.projection_indices() {
            Some(projection) => Arc::new(self.schema.try_project(projection).unwrap()),
            None => self.schema.clone(),
        };
        Ok(Box::pin(EmptyStream {
            schema: projected_schema,
        }))
    }
}

struct EmptyStream {
    schema: SchemaRef,
}

impl RecordBatchStream for EmptyStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_ordering(&self) -> Option<&[OrderOption]> {
        None
    }

    fn metrics(&self) -> Option<RecordBatchMetrics> {
        None
    }
}

impl Stream for EmptyStream {
    type Item = RecordBatchResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(None)
    }
}

#[cfg(feature = "vector_index")]
mod vector_search_tests {
    use std::sync::Arc;

    use common_function::function::Function;
    use common_function::scalars::udf::create_udf;
    use datafusion_expr::expr::ScalarFunction;
    use datafusion_expr::{Expr, LogicalPlanBuilder, Signature, Volatility, col, lit};
    use datatypes::schema::{ColumnSchema, SchemaBuilder};
    use store_api::storage::ConcreteDataType;
    use table::metadata::{FilterPushDownType, TableInfoBuilder, TableMeta, TableType};
    use table::table::adapter::DfTableProviderAdapter;
    use table::{Table, TableRef};

    use super::*;
    use crate::dist_plan::MergeScanLogicalPlan;

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
        ) -> datafusion_common::Result<datatypes::arrow::datatypes::DataType> {
            Ok(datatypes::arrow::datatypes::DataType::Float32)
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn invoke_with_args(
            &self,
            _args: datafusion_expr::ScalarFunctionArgs,
        ) -> datafusion_common::Result<datafusion_expr::ColumnarValue> {
            Err(datafusion_common::DataFusionError::Execution(
                "test udf should not be invoked".to_string(),
            ))
        }
    }

    fn build_vector_table(table_id: TableId) -> TableRef {
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

        let table_meta = TableMeta {
            schema: schema.clone(),
            primary_key_indices: vec![0],
            value_indices: vec![2],
            engine: "test_engine".to_string(),
            next_column_id: 3,
            options: Default::default(),
            created_on: Default::default(),
            updated_on: Default::default(),
            partition_key_indices: vec![0],
            column_ids: vec![0, 1, 2],
        };

        let table_info = TableInfoBuilder::default()
            .table_id(table_id)
            .name("t".to_string())
            .catalog_name(DEFAULT_CATALOG_NAME)
            .schema_name(DEFAULT_SCHEMA_NAME)
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

    fn vector_distance_expr() -> Expr {
        let udf = create_udf(Arc::new(TestVectorFunction::new("vec_l2sq_distance")));
        Expr::ScalarFunction(ScalarFunction::new_udf(
            Arc::new(udf),
            vec![
                col("v"),
                lit(ScalarValue::Utf8(Some("[1.0, 2.0]".to_string()))),
            ],
        ))
    }

    #[test]
    fn vector_search_rewrite_keeps_sort_in_child_plan() {
        init_default_ut_logging();
        let table = build_vector_table(0);
        let table_source = Arc::new(DefaultTableSource::new(Arc::new(
            DfTableProviderAdapter::new(table),
        )));

        let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
            .unwrap()
            .sort(vec![vector_distance_expr().sort(true, false)])
            .unwrap()
            .limit(0, Some(5))
            .unwrap()
            .build()
            .unwrap();

        let config = ConfigOptions::default();
        let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();

        let plan_str = result.to_string();
        assert!(plan_str.contains("MergeSort: vec_l2sq_distance"));
        assert!(plan_str.contains("Sort: vec_l2sq_distance"));
        assert!(plan_str.contains(MergeScanLogicalPlan::name()));
    }

    #[test]
    fn vector_search_rewrite_with_filter_keeps_sort_in_child_plan() {
        init_default_ut_logging();
        let table = build_vector_table(0);
        let table_source = Arc::new(DefaultTableSource::new(Arc::new(
            DfTableProviderAdapter::new(table),
        )));

        let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
            .unwrap()
            .filter(col("k0").eq(lit("hello")))
            .unwrap()
            .sort(vec![vector_distance_expr().sort(true, false)])
            .unwrap()
            .limit(0, Some(5))
            .unwrap()
            .build()
            .unwrap();

        let config = ConfigOptions::default();
        let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();

        let plan_str = result.to_string();
        assert!(plan_str.contains("MergeSort: vec_l2sq_distance"));
        assert!(plan_str.contains("Sort: vec_l2sq_distance"));
        assert!(plan_str.contains("Filter: t.k0 = Utf8(\"hello\")"));
        assert!(plan_str.contains(MergeScanLogicalPlan::name()));
    }
}

fn try_encode_decode_substrait(plan: &LogicalPlan, state: SessionState) {
    let sub_plan_bytes = substrait::DFLogicalSubstraitConvertor
        .encode(plan, crate::query_engine::DefaultSerializer)
        .unwrap();
    let inner = sub_plan_bytes.clone();
    let inner_state = state.clone();
    let decoded_plan = futures::executor::block_on(async move {
        substrait::DFLogicalSubstraitConvertor
            .decode(inner, inner_state)
            .await
    }).inspect_err(|e|{
        use prost::Message;
        let sub_plan = substrait::substrait_proto_df::proto::Plan::decode(sub_plan_bytes).unwrap();
        common_telemetry::error!("Failed to decode substrait plan: {e},substrait plan: {sub_plan:#?}\nlogical plan: {plan:#?}");
    })
    .unwrap();

    assert_eq!(*plan, decoded_plan);
}

fn range_select_plan(time_expr: Expr, by: Vec<Expr>) -> LogicalPlan {
    range_select_plan_with_window(
        time_expr,
        by,
        Duration::from_secs(5),
        Duration::from_secs(5),
    )
}

fn range_select_plan_with_window(
    time_expr: Expr,
    by: Vec<Expr>,
    align: Duration,
    range: Duration,
) -> LogicalPlan {
    let test_table = TestTable::table_with_name(0, "t".to_string());
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(test_table),
    )));
    let scan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
        .unwrap()
        .build()
        .unwrap();
    let scan = LogicalPlanBuilder::from(scan)
        .project(vec![col("pk1"), col("ts"), col("number")])
        .unwrap()
        .build()
        .unwrap();
    range_select_plan_from_input(scan, time_expr, by, align, range)
}

fn range_select_plan_from_input(
    input: LogicalPlan,
    time_expr: Expr,
    by: Vec<Expr>,
    align: Duration,
    range: Duration,
) -> LogicalPlan {
    let range_expr = avg(col("number"));
    let projection_expr = std::iter::once(range_expr.clone())
        .chain([time_expr.clone()])
        .chain(by.clone())
        .collect::<Vec<_>>();
    let range_select = RangeSelect::try_new(
        Arc::new(input),
        vec![RangeFn {
            name: "avg(t.number)".to_string(),
            data_type: DataType::Float64,
            expr: range_expr.clone(),
            range,
            fill: None,
            need_cast: false,
        }],
        align,
        0,
        time_expr,
        by,
        &projection_expr,
    )
    .unwrap();
    LogicalPlan::Extension(datafusion_expr::logical_plan::Extension {
        node: Arc::new(range_select),
    })
}

fn lower_range_select_for_dist(plan: LogicalPlan, enabled: bool) -> LogicalPlan {
    let mut config = ConfigOptions::default();
    config.extensions.insert(RangeSelectOptions {
        experimental_enable_range_select_pushdown: enabled,
    });
    RangeSelectLoweringAnalyzer.analyze(plan, &config).unwrap()
}

fn range_select_from_extension(plan: &LogicalPlan) -> &RangeSelect {
    let LogicalPlan::Extension(extension) = plan else {
        panic!("expected RangeSelect extension, got: {plan}");
    };
    extension
        .node
        .as_any()
        .downcast_ref::<RangeSelect>()
        .expect("expected RangeSelect extension")
}

fn merge_scan_from_extension(plan: &LogicalPlan) -> &MergeScanLogicalPlan {
    let LogicalPlan::Extension(extension) = plan else {
        panic!("expected MergeScan extension, got: {plan}");
    };
    extension
        .node
        .as_any()
        .downcast_ref::<MergeScanLogicalPlan>()
        .expect("expected MergeScan extension")
}

#[derive(Debug)]
struct NoopRangeSelectRegionQueryHandler;

#[async_trait]
impl RegionQueryHandler for NoopRangeSelectRegionQueryHandler {
    async fn do_get(
        &self,
        _read_preference: ReadPreference,
        _request: common_query::request::QueryRequest,
    ) -> crate::error::Result<SendableRecordBatchStream> {
        unreachable!("physical shape tests must not execute MergeScan")
    }

    async fn handle_remote_dyn_filter_update(
        &self,
        _region_id: RegionId,
        _query_id: String,
        _update: RemoteDynFilterUpdate,
    ) -> crate::error::Result<()> {
        unreachable!("physical shape tests must not send remote dynamic filters")
    }

    async fn handle_remote_dyn_filter_unregister(
        &self,
        _region_id: RegionId,
        _query_id: String,
        _unregister: RemoteDynFilterUnregister,
    ) -> crate::error::Result<()> {
        unreachable!("physical shape tests must not unregister remote dynamic filters")
    }
}

fn region_query_error(message: impl Into<String>) -> crate::error::Error {
    Err::<(), _>(BoxedError::new(PlainError::new(
        message.into(),
        StatusCode::Unexpected,
    )))
    .context(QueryExecutionSnafu {})
    .unwrap_err()
}

#[derive(Debug)]
struct ExecutingRangeSelectRegionQueryHandler {
    batches: HashMap<RegionId, DfRecordBatch>,
    requests: Mutex<Vec<RegionId>>,
}

impl ExecutingRangeSelectRegionQueryHandler {
    fn new(batches: HashMap<RegionId, DfRecordBatch>) -> Self {
        Self {
            batches,
            requests: Mutex::new(Vec::new()),
        }
    }

    fn requests(&self) -> Vec<RegionId> {
        self.requests.lock().unwrap().clone()
    }
}

#[async_trait]
impl RegionQueryHandler for ExecutingRangeSelectRegionQueryHandler {
    async fn do_get(
        &self,
        _read_preference: ReadPreference,
        request: common_query::request::QueryRequest,
    ) -> crate::error::Result<SendableRecordBatchStream> {
        self.requests.lock().unwrap().push(request.region_id);
        let LogicalPlan::Extension(extension) = &request.plan else {
            return Err(region_query_error(
                "remote request root is not an extension",
            ));
        };
        let Some(partial) = extension.node.as_any().downcast_ref::<RangeSelect>() else {
            return Err(region_query_error(
                "remote request root is not RangeSelect Partial",
            ));
        };
        if !matches!(partial.mode(), RangeSelectMode::Partial(_)) {
            return Err(region_query_error(
                "remote request RangeSelect is not Partial",
            ));
        }
        if !matches!(partial.input.as_ref(), LogicalPlan::Projection(_)) {
            return Err(region_query_error(
                "remote request Partial lacks its materialization Projection",
            ));
        }
        let expected_schema = request.plan.schema().as_arrow().clone();
        if expected_schema.fields().len() != 3
            || !matches!(expected_schema.field(0).data_type(), DataType::Struct(_))
            || expected_schema.field(0).is_nullable()
            || expected_schema.field(1).data_type()
                != &DataType::Timestamp(TimeUnit::Millisecond, None)
            || expected_schema
                .fields()
                .iter()
                .any(|field| field.name() == "number")
        {
            return Err(region_query_error(
                "remote request Partial schema must contain only non-null state, bucket, and BY",
            ));
        }

        let raw_batch = self
            .batches
            .get(&request.region_id)
            .cloned()
            .ok_or_else(|| {
                region_query_error(format!("missing raw batch for {}", request.region_id))
            })?;
        let remote_context = SessionContext::new_with_state(range_select_physical_session_state());
        remote_context
            .register_table(
                "t",
                Arc::new(
                    MemTable::try_new(raw_batch.schema(), vec![vec![raw_batch]])
                        .map_err(|error| region_query_error(error.to_string()))?,
                ),
            )
            .map_err(|error| region_query_error(error.to_string()))?;
        let remote_state = remote_context.state();
        let bytes = substrait::DFLogicalSubstraitConvertor
            .encode(&request.plan, DefaultSerializer)
            .map_err(|error| region_query_error(error.to_string()))?;
        let decoded = substrait::DFLogicalSubstraitConvertor
            .decode(bytes, remote_state.clone())
            .await
            .map_err(|error| region_query_error(error.to_string()))?;
        let decoded_partial = range_select_from_extension(&decoded);
        if !matches!(decoded_partial.mode(), RangeSelectMode::Partial(_))
            || !matches!(decoded_partial.input.as_ref(), LogicalPlan::Projection(_))
        {
            return Err(region_query_error(
                "decoded remote request is not a materialized RangeSelect Partial",
            ));
        }
        let physical = remote_state
            .create_physical_plan(&decoded)
            .await
            .map_err(|error| region_query_error(error.to_string()))?;
        let physical = optimize_range_select_physical_plan(&remote_state, physical)
            .map_err(|error| region_query_error(error.to_string()))?;
        let Some(partial_exec) = physical.as_any().downcast_ref::<RangeSelectExec>() else {
            return Err(region_query_error(
                "decoded remote physical root is not RangeSelectExec",
            ));
        };
        if partial_exec.mode() != RangeSelectExecMode::Partial
            || partial_exec.schema() != Arc::new(expected_schema)
        {
            return Err(region_query_error(
                "remote Partial physical schema does not match MergeScan expected schema",
            ));
        }
        let stream = physical
            .execute(0, remote_context.task_ctx())
            .map_err(|error| region_query_error(error.to_string()))?;
        Ok(Box::pin(
            RecordBatchStreamAdapter::try_new(stream)
                .map_err(|error| region_query_error(error.to_string()))?,
        ))
    }

    async fn handle_remote_dyn_filter_update(
        &self,
        _region_id: RegionId,
        _query_id: String,
        _update: RemoteDynFilterUpdate,
    ) -> crate::error::Result<()> {
        Ok(())
    }

    async fn handle_remote_dyn_filter_unregister(
        &self,
        _region_id: RegionId,
        _query_id: String,
        _unregister: RemoteDynFilterUnregister,
    ) -> crate::error::Result<()> {
        Ok(())
    }
}

struct RangeSelectMergeScanTestPlanner {
    region_query_handler: Arc<dyn RegionQueryHandler>,
}

impl std::fmt::Debug for RangeSelectMergeScanTestPlanner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RangeSelectMergeScanTestPlanner").finish()
    }
}

#[async_trait]
impl ExtensionPlanner for RangeSelectMergeScanTestPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn datafusion_expr::UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        _physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> DfResult<Option<Arc<dyn ExecutionPlan>>> {
        let Some(merge_scan) = node.as_any().downcast_ref::<MergeScanLogicalPlan>() else {
            return Ok(None);
        };
        Ok(Some(Arc::new(MergeScanExec::new(
            session_state,
            TableName::new(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, "t"),
            vec![RegionId::new(0, 0), RegionId::new(0, 1)],
            merge_scan.input().clone(),
            merge_scan.input().schema().as_arrow(),
            self.region_query_handler.clone(),
            QueryContext::arc(),
            2,
            merge_scan.partition_cols().clone(),
            merge_scan.remote_dyn_filter_producer_id(),
            false,
        )?)))
    }
}

struct RangeSelectPhysicalQueryPlanner {
    planner: DefaultPhysicalPlanner,
}

impl std::fmt::Debug for RangeSelectPhysicalQueryPlanner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RangeSelectPhysicalQueryPlanner").finish()
    }
}

#[async_trait]
impl QueryPlanner for RangeSelectPhysicalQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        self.planner
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}

fn range_select_physical_session_state_with_handler(
    region_query_handler: Arc<dyn RegionQueryHandler>,
) -> SessionState {
    let planner = DefaultPhysicalPlanner::with_extension_planners(vec![
        Arc::new(RangeSelectPlanner),
        Arc::new(RangeSelectMergeScanTestPlanner {
            region_query_handler,
        }),
    ]);
    SessionStateBuilder::new()
        .with_config(SessionConfig::new().with_target_partitions(2))
        .with_default_features()
        .with_serializer_registry(Arc::new(DefaultSerializer))
        .with_query_planner(Arc::new(RangeSelectPhysicalQueryPlanner { planner }))
        .build()
}

fn range_select_physical_session_state() -> SessionState {
    range_select_physical_session_state_with_handler(Arc::new(NoopRangeSelectRegionQueryHandler))
}

fn assert_arrow_schema_matches_logical(
    physical: &arrow::datatypes::Schema,
    logical: &datafusion_common::DFSchema,
) {
    assert_eq!(physical.fields().len(), logical.fields().len());
    for (index, physical_field) in physical.fields().iter().enumerate() {
        let (_, logical_field) = logical.qualified_field(index);
        assert_eq!(physical_field.name(), logical_field.name());
        assert_eq!(physical_field.data_type(), logical_field.data_type());
        assert_eq!(physical_field.is_nullable(), logical_field.is_nullable());
        assert_eq!(physical_field.metadata(), logical_field.metadata());
    }
}

fn optimize_range_select_physical_plan(
    state: &SessionState,
    plan: Arc<dyn ExecutionPlan>,
) -> DfResult<Arc<dyn ExecutionPlan>> {
    state
        .physical_optimizers()
        .iter()
        .try_fold(plan, |plan, optimizer| {
            optimizer.optimize(plan, state.config_options())
        })
}

#[test]
fn range_select_complete_keeps_legacy_narrow_remote_input_when_pushdown_disabled() {
    let plan = range_select_plan(col("ts"), vec![col("pk1")]);
    let plan = lower_range_select_for_dist(plan, false);
    assert!(matches!(
        range_select_from_extension(&plan).mode(),
        RangeSelectMode::Complete
    ));

    let result = DistPlannerAnalyzer {}
        .analyze(plan, &ConfigOptions::default())
        .unwrap();
    let range_select = range_select_from_extension(&result);
    assert!(matches!(range_select.mode(), RangeSelectMode::Complete));
    let LogicalPlan::Projection(projection) = range_select.input.as_ref() else {
        panic!(
            "expected schema-restoring Projection below legacy RangeSelect, got: {}",
            range_select.input
        );
    };
    let merge_scan = merge_scan_from_extension(projection.input.as_ref());

    let LogicalPlan::Projection(remote_projection) = merge_scan.input() else {
        panic!(
            "expected narrow remote Projection, got: {}",
            merge_scan.input()
        );
    };
    assert_eq!(
        remote_projection
            .schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        vec!["pk1", "ts", "number"]
    );
}

#[test]
fn range_select_pushdown_places_partial_below_merge_scan_with_state_only_schema() {
    let original = range_select_plan(col("ts"), vec![col("pk1")]);
    let original_schema = original.schema().clone();
    let lowered = lower_range_select_for_dist(original, true);
    let lowered_final = range_select_from_extension(&lowered);
    assert!(matches!(lowered_final.mode(), RangeSelectMode::Final(_)));

    let result = DistPlannerAnalyzer {}
        .analyze(lowered, &ConfigOptions::default())
        .unwrap();
    let final_range = range_select_from_extension(&result);
    assert!(matches!(final_range.mode(), RangeSelectMode::Final(_)));
    assert_eq!(final_range.schema, original_schema);

    let LogicalPlan::Projection(restoring_projection) = final_range.input.as_ref() else {
        panic!("expected schema-restoring Projection below Final RangeSelect");
    };
    let merge_scan = merge_scan_from_extension(restoring_projection.input.as_ref());
    assert_eq!(
        restoring_projection.schema,
        merge_scan.input().schema().clone(),
        "Final must receive precisely the remote Partial schema"
    );

    let remote_partial = range_select_from_extension(merge_scan.input());
    assert!(matches!(remote_partial.mode(), RangeSelectMode::Partial(_)));
    let LogicalPlan::Projection(materialization) = remote_partial.input.as_ref() else {
        panic!("expected materialization Projection below remote Partial RangeSelect");
    };
    assert_eq!(
        materialization
            .schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        vec!["ts", "pk1", "__range_arg_0"]
    );

    let remote_schema = merge_scan.input().schema();
    assert_eq!(
        remote_schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        vec!["__range_state_0", "__range_bucket_ms", "pk1"]
    );
    assert!(matches!(
        remote_schema.fields()[0].data_type(),
        DataType::Struct(_)
    ));
    assert_eq!(
        remote_schema.fields()[1].data_type(),
        &DataType::Timestamp(TimeUnit::Millisecond, None)
    );
    assert_eq!(remote_schema.fields()[2].data_type(), &DataType::Utf8);
    assert!(
        substrait::DFLogicalSubstraitConvertor
            .encode(merge_scan.input(), crate::query_engine::DefaultSerializer)
            .is_ok(),
        "the exact remote Partial subtree must be serializable"
    );
}

#[test]
fn range_select_pushdown_success_is_not_replaced_by_pre_dispatch_fallback() {
    // `expand_sort_alias_conflict_limit` fixes the generic local DistPlanner
    // fallback contract. It happens before dispatch and must not turn a
    // successfully planned RangeSelect Partial into a Complete retry.
    let lowered = lower_range_select_for_dist(range_select_plan(col("ts"), vec![col("pk1")]), true);
    let mut config = ConfigOptions::default();
    config.extensions.insert(DistPlannerOptions {
        allow_query_fallback: true,
    });

    let result = DistPlannerAnalyzer {}.analyze(lowered, &config).unwrap();
    let final_range = range_select_from_extension(&result);
    assert!(matches!(final_range.mode(), RangeSelectMode::Final(_)));

    let LogicalPlan::Projection(restoring_projection) = final_range.input.as_ref() else {
        panic!("expected schema-restoring Projection below Final RangeSelect");
    };
    let merge_scan = merge_scan_from_extension(restoring_projection.input.as_ref());
    let remote_partial = range_select_from_extension(merge_scan.input());
    assert!(matches!(remote_partial.mode(), RangeSelectMode::Partial(_)));
}

#[test]
fn range_select_unsupported_shape_keeps_complete_fallback_without_remote_partial() {
    let plan = range_select_plan(col("ts").alias("ts"), vec![col("pk1")]);
    let lowered = lower_range_select_for_dist(plan.clone(), true);
    assert_eq!(
        lowered, plan,
        "unsupported RangeSelect shape must not split"
    );

    let result = DistPlannerAnalyzer {}
        .analyze(lowered, &ConfigOptions::default())
        .unwrap();
    let complete = range_select_from_extension(&result);
    assert!(matches!(complete.mode(), RangeSelectMode::Complete));
    let LogicalPlan::Projection(restoring_projection) = complete.input.as_ref() else {
        panic!("expected legacy schema-restoring Projection below Complete RangeSelect");
    };
    let merge_scan = merge_scan_from_extension(restoring_projection.input.as_ref());
    assert!(
        !matches!(merge_scan.input(), LogicalPlan::Extension(_)),
        "unsupported RangeSelect must keep the legacy raw/narrow remote plan"
    );
}

#[tokio::test]
async fn range_select_pushdown_physical_plans_preserve_frontend_and_remote_boundaries() {
    let logical = DistPlannerAnalyzer {}
        .analyze(
            lower_range_select_for_dist(range_select_plan(col("ts"), vec![col("pk1")]), true),
            &ConfigOptions::default(),
        )
        .unwrap();
    let final_range = range_select_from_extension(&logical);
    let LogicalPlan::Projection(restoring_projection) = final_range.input.as_ref() else {
        panic!("Final RangeSelect must have a schema-restoring Projection input");
    };
    let merge_scan = merge_scan_from_extension(restoring_projection.input.as_ref());
    let remote_logical = merge_scan.input().clone();
    let remote_partial = range_select_from_extension(&remote_logical);
    assert!(matches!(final_range.mode(), RangeSelectMode::Final(_)));
    assert!(matches!(remote_partial.mode(), RangeSelectMode::Partial(_)));

    let frontend_state = range_select_physical_session_state();
    let frontend = frontend_state.create_physical_plan(&logical).await.unwrap();
    let frontend = optimize_range_select_physical_plan(&frontend_state, frontend).unwrap();
    let final_exec = frontend
        .as_any()
        .downcast_ref::<RangeSelectExec>()
        .expect("frontend root must be RangeSelectExec(Final)");
    assert_eq!(final_exec.mode(), RangeSelectExecMode::Final);
    assert!(
        final_exec
            .schema()
            .fields()
            .iter()
            .all(|field| field.name() != "number"),
        "frontend Final output must not reintroduce raw values"
    );
    assert_eq!(final_exec.required_input_distribution().len(), 1);
    assert!(matches!(
        final_exec.required_input_distribution()[0],
        datafusion_physical_expr::Distribution::SinglePartition
    ));
    assert_eq!(
        final_exec
            .properties()
            .output_partitioning()
            .partition_count(),
        1
    );
    assert_eq!(
        final_exec.properties().emission_type,
        datafusion::physical_plan::execution_plan::EmissionType::Final
    );
    assert_eq!(
        final_exec.properties().boundedness,
        datafusion::physical_plan::execution_plan::Boundedness::Bounded
    );
    assert_arrow_schema_matches_logical(final_exec.schema().as_ref(), final_range.schema.as_ref());

    let final_children = final_exec.children();
    let coalesce = final_children[0]
        .as_any()
        .downcast_ref::<CoalescePartitionsExec>()
        .expect("physical optimizer must satisfy Final single-partition input");
    let coalesce_children = coalesce.children();
    // The logical schema-restoring Projection is an identity projection for this
    // Partial state schema, so physical projection pushdown removes it while
    // retaining its schema contract on MergeScanExec.
    let frontend_merge_scan = coalesce_children[0]
        .as_any()
        .downcast_ref::<MergeScanExec>()
        .expect("physical optimizer may only elide the identity restore ProjectionExec");
    assert!(
        frontend_merge_scan
            .schema()
            .fields()
            .iter()
            .all(|field| field.name() != "number"),
        "Final input must expose only Partial state, bucket, and BY columns"
    );
    assert_eq!(
        frontend_merge_scan
            .properties()
            .output_partitioning()
            .partition_count(),
        2
    );
    assert_arrow_schema_matches_logical(
        frontend_merge_scan.schema().as_ref(),
        remote_logical.schema().as_ref(),
    );
    assert_arrow_schema_matches_logical(
        frontend_merge_scan.schema().as_ref(),
        restoring_projection.schema.as_ref(),
    );

    let remote_bytes = substrait::DFLogicalSubstraitConvertor
        .encode(&remote_logical, DefaultSerializer)
        .expect("remote Partial must encode with DefaultSerializer");
    let remote_context = SessionContext::new_with_state(range_select_physical_session_state());
    remote_context
        .register_table(
            "t",
            Arc::new(
                MemTable::try_new(
                    TestTable::schema().arrow_schema().clone(),
                    vec![vec![], vec![]],
                )
                .unwrap(),
            ),
        )
        .unwrap();
    let remote_state = remote_context.state();
    let remote_decoded = substrait::DFLogicalSubstraitConvertor
        .decode(remote_bytes, remote_state.clone())
        .await
        .expect("remote Partial must decode as a datanode-style plan");
    let decoded_partial = range_select_from_extension(&remote_decoded);
    assert!(matches!(
        decoded_partial.mode(),
        RangeSelectMode::Partial(_)
    ));
    let remote = remote_state
        .create_physical_plan(&remote_decoded)
        .await
        .unwrap();
    let remote = optimize_range_select_physical_plan(&remote_state, remote).unwrap();
    let partial_exec = remote
        .as_any()
        .downcast_ref::<RangeSelectExec>()
        .expect("remote root must be RangeSelectExec(Partial)");
    assert_eq!(partial_exec.mode(), RangeSelectExecMode::Partial);
    assert!(matches!(
        partial_exec.required_input_distribution()[0],
        datafusion_physical_expr::Distribution::UnspecifiedDistribution
    ));
    assert_eq!(
        partial_exec
            .properties()
            .output_partitioning()
            .partition_count(),
        2
    );
    assert_arrow_schema_matches_logical(
        partial_exec.schema().as_ref(),
        decoded_partial.schema.as_ref(),
    );
    assert!(matches!(
        partial_exec.schema().field(0).data_type(),
        DataType::Struct(_)
    ));
    assert!(!partial_exec.schema().field(0).is_nullable());
    assert_eq!(
        partial_exec.schema().field(1).data_type(),
        &DataType::Timestamp(TimeUnit::Millisecond, None)
    );
    let (original_by_qualifier, original_by_field) = remote_partial.schema.qualified_field(2);
    assert!(
        original_by_qualifier.is_none(),
        "the materialization Projection intentionally publishes the BY field unqualified"
    );
    assert_eq!(original_by_field.name(), "pk1");
    assert_eq!(original_by_field.data_type(), &DataType::Utf8);
    assert_eq!(
        partial_exec.schema().field(2).name(),
        original_by_field.name()
    );
    assert_eq!(
        partial_exec.schema().field(2).data_type(),
        original_by_field.data_type()
    );
    assert_eq!(
        partial_exec.schema().field(2).is_nullable(),
        original_by_field.is_nullable()
    );
    assert_eq!(
        partial_exec.schema().field(2).metadata(),
        original_by_field.metadata()
    );

    let partial_children = partial_exec.children();
    let materialization = partial_children[0]
        .as_any()
        .downcast_ref::<ProjectionExec>()
        .expect("remote Partial must consume its materialization ProjectionExec directly");
    let materialization_children = materialization.children();
    assert!(
        materialization_children[0]
            .as_any()
            .downcast_ref::<CoalescePartitionsExec>()
            .is_none(),
        "Partial must not gain a CoalescePartitionsExec before its materialization input"
    );
    assert!(
        materialization_children[0]
            .as_any()
            .downcast_ref::<datafusion::physical_plan::repartition::RepartitionExec>()
            .is_none(),
        "Partial must not gain a RepartitionExec before its materialization input"
    );
    assert_eq!(
        materialization_children[0]
            .properties()
            .output_partitioning()
            .partition_count(),
        2
    );
    let batches = datafusion::physical_plan::collect(remote, remote_context.task_ctx())
        .await
        .unwrap();
    assert!(batches.is_empty());
}

fn range_select_raw_batch(timestamps: Vec<i64>, values: Vec<u32>) -> DfRecordBatch {
    let schema = TestTable::schema().arrow_schema().clone();
    let hosts = vec!["host"; values.len()];
    DfRecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(hosts.clone())),
            Arc::new(StringArray::from(vec!["pk2"; values.len()])),
            Arc::new(StringArray::from(vec!["pk3"; values.len()])),
            Arc::new(TimestampMillisecondArray::from(timestamps)),
            Arc::new(UInt32Array::from(
                values.into_iter().map(Some).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

fn range_select_output_rows(batches: &[DfRecordBatch]) -> Vec<(i64, String, f64)> {
    let mut rows = Vec::new();
    for batch in batches {
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let timestamps = batch
            .column(1)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        let by = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for index in 0..batch.num_rows() {
            rows.push((
                timestamps.value(index),
                by.value(index).to_string(),
                values.value(index),
            ));
        }
    }
    rows.sort_by(|left, right| left.0.cmp(&right.0).then(left.1.cmp(&right.1)));
    rows
}

#[tokio::test]
async fn range_select_two_region_partial_states_execute_through_merge_scan() {
    let region_a = RegionId::new(0, 0);
    let region_b = RegionId::new(0, 1);
    // `range=10s` and `align=5s` make the samples at 0ms and 5000ms
    // contribute to overlapping buckets. Bucket 0 receives 1, 3, and 9.
    let batch_a = range_select_raw_batch(vec![0, 5_000], vec![1, 3]);
    let batch_b = range_select_raw_batch(vec![5_000], vec![9]);
    let handler = Arc::new(ExecutingRangeSelectRegionQueryHandler::new(HashMap::from(
        [(region_a, batch_a.clone()), (region_b, batch_b.clone())],
    )));

    let logical = DistPlannerAnalyzer {}
        .analyze(
            lower_range_select_for_dist(
                range_select_plan_with_window(
                    col("ts"),
                    vec![col("pk1")],
                    Duration::from_secs(5),
                    Duration::from_secs(10),
                ),
                true,
            ),
            &ConfigOptions::default(),
        )
        .unwrap();
    let frontend_state = range_select_physical_session_state_with_handler(handler.clone());
    let frontend = frontend_state.create_physical_plan(&logical).await.unwrap();
    let frontend = optimize_range_select_physical_plan(&frontend_state, frontend).unwrap();
    let final_exec = frontend
        .as_any()
        .downcast_ref::<RangeSelectExec>()
        .expect("frontend root must be RangeSelectExec(Final)");
    assert_eq!(final_exec.mode(), RangeSelectExecMode::Final);
    let final_children = final_exec.children();
    let coalesce = final_children[0]
        .as_any()
        .downcast_ref::<CoalescePartitionsExec>()
        .expect("Final must coalesce MergeScan partitions");
    let coalesce_children = coalesce.children();
    let merge_scan = coalesce_children[0]
        .as_any()
        .downcast_ref::<MergeScanExec>()
        .expect("physical projection pushdown must leave MergeScan below Coalesce");
    assert!(
        merge_scan
            .schema()
            .fields()
            .iter()
            .all(|field| field.name() != "number"),
        "frontend MergeScan must not receive raw values"
    );
    assert!(matches!(
        merge_scan.schema().field(0).data_type(),
        DataType::Struct(_)
    ));
    assert!(!merge_scan.schema().field(0).is_nullable());

    let frontend_schema = frontend.schema();
    let frontend_batches = timeout(
        Duration::from_secs(5),
        datafusion::physical_plan::collect(frontend, SessionContext::new().task_ctx()),
    )
    .await
    .expect("two-region frontend execution timed out")
    .unwrap();
    let mut requests = handler.requests();
    requests.sort();
    assert_eq!(requests, vec![region_a, region_b]);

    let union_source =
        Arc::new(MemTable::try_new(batch_a.schema(), vec![vec![batch_a], vec![batch_b]]).unwrap());
    let union_scan = LogicalPlanBuilder::scan_with_filters(
        "t",
        Arc::new(DefaultTableSource::new(union_source)),
        None,
        vec![],
    )
    .unwrap()
    .project(vec![col("pk1"), col("ts"), col("number")])
    .unwrap()
    .build()
    .unwrap();
    let complete = range_select_plan_from_input(
        union_scan,
        col("ts"),
        vec![col("pk1")],
        Duration::from_secs(5),
        Duration::from_secs(10),
    );
    let baseline_state = range_select_physical_session_state();
    let baseline = baseline_state
        .create_physical_plan(&complete)
        .await
        .unwrap();
    let baseline = optimize_range_select_physical_plan(&baseline_state, baseline).unwrap();
    let baseline_schema = baseline.schema();
    let baseline_batches = timeout(
        Duration::from_secs(5),
        datafusion::physical_plan::collect(baseline, SessionContext::new().task_ctx()),
    )
    .await
    .expect("Complete baseline execution timed out")
    .unwrap();
    assert_eq!(frontend_schema, baseline_schema);

    let final_rows = range_select_output_rows(&frontend_batches);
    let complete_rows = range_select_output_rows(&baseline_batches);
    assert_eq!(final_rows.len(), complete_rows.len());
    for (final_row, complete_row) in final_rows.iter().zip(&complete_rows) {
        assert_eq!(final_row.0, complete_row.0);
        assert_eq!(final_row.1, complete_row.1);
        assert!(
            (final_row.2 - complete_row.2).abs() < 1e-12,
            "Final row {final_row:?} differs from Complete baseline {complete_row:?}"
        );
    }
    assert!(
        final_rows
            .iter()
            .any(|(_, _, value)| (*value - 13.0 / 3.0).abs() < 1e-12),
        "merged states must produce the raw-sample AVG 13/3, not an average of regional averages"
    );
}

#[test]
fn expand_proj_sort_proj() {
    // use logging for better debugging
    init_default_ut_logging();
    let test_table = TestTable::table_with_name(0, "t".to_string());
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(test_table),
    )));
    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
        .unwrap()
        .project(vec![col("number"), col("pk1"), col("pk2"), col("pk3")])
        .unwrap()
        .project(vec![
            col("number"),
            col("pk1"),
            col("pk3"),
            col("pk1").eq(col("pk2")),
        ])
        .unwrap()
        .sort(vec![col("t.pk1 = t.pk2").sort(true, true)])
        .unwrap()
        .project(vec![col("number")])
        .unwrap()
        .project(vec![col("number")])
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();

    let expected = [
        "Projection: t.number",
        "  MergeSort: t.pk1 = t.pk2 ASC NULLS FIRST",
        "    MergeScan [is_placeholder=false, remote_input=[",
        "Projection: t.number, t.pk1 = t.pk2",
        "  Projection: t.number, t.pk1 = t.pk2", // notice both projections added `t.pk1 = t.pk2` column requirement
        "    Sort: t.pk1 = t.pk2 ASC NULLS FIRST",
        "      Projection: t.number, t.pk1, t.pk3, t.pk2 = t.pk1 AS t.pk1 = t.pk2",
        "        Projection: t.number, t.pk1, t.pk2, t.pk3", // notice this projection doesn't add `t.pk1 = t.pk2` column requirement
        "          TableScan: t",
        "]]",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());
}

#[test]
fn expand_proj_sort_partial_proj() {
    // use logging for better debugging
    init_default_ut_logging();
    let test_table = TestTable::table_with_name(0, "t".to_string());
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(test_table),
    )));
    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
        .unwrap()
        .project(vec![col("number"), col("pk1"), col("pk2"), col("pk3")])
        .unwrap()
        .project(vec![
            col("number"),
            col("pk1"),
            col("pk3"),
            col("pk1").eq(col("pk2")),
        ])
        .unwrap()
        .sort(vec![col("t.pk1 = t.pk2").sort(true, true)])
        .unwrap()
        .project(vec![col("number"), col("t.pk1 = t.pk2").alias("eq_sorted")])
        .unwrap()
        .project(vec![col("number")])
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();

    let expected = [
        "Projection: t.number",
        "  MergeSort: eq_sorted ASC NULLS FIRST", // notice how `eq_sorted` is used here
        "    MergeScan [is_placeholder=false, remote_input=[",
        "Projection: t.number, eq_sorted", // notice how `eq_sorted` is added not `t.pk1 = t.pk2`
        "  Projection: t.number, t.pk1 = t.pk2 AS eq_sorted",
        "    Sort: t.pk1 = t.pk2 ASC NULLS FIRST",
        "      Projection: t.number, t.pk1, t.pk3, t.pk2 = t.pk1 AS t.pk1 = t.pk2",
        "        Projection: t.number, t.pk1, t.pk2, t.pk3", // notice this projection doesn't add `t.pk1 = t.pk2` column requirement
        "          TableScan: t",
        "]]",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());
}

#[test]
fn expand_sort_limit() {
    // use logging for better debugging
    init_default_ut_logging();
    let test_table = TestTable::table_with_name(0, "t".to_string());
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(test_table),
    )));
    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
        .unwrap()
        .sort(vec![col("pk1").sort(true, false)])
        .unwrap()
        .limit(0, Some(10))
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();

    let expected = [
        "Projection: t.pk1, t.pk2, t.pk3, t.ts, t.number",
        "  Limit: skip=0, fetch=10",
        "    MergeSort: t.pk1 ASC NULLS LAST",
        "      MergeScan [is_placeholder=false, remote_input=[",
        "Limit: skip=0, fetch=10",
        "  Sort: t.pk1 ASC NULLS LAST",
        "    TableScan: t",
        "]]",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());
}

/// Test merge sort can apply enforce dist requirement columns correctly and use the aliased column correctly, as there is
/// a aliased sort column, there is no need to add a duplicate sort column using it's original column name
#[test]
fn expand_sort_alias_limit() {
    // use logging for better debugging
    init_default_ut_logging();
    let test_table = TestTable::table_with_name(0, "t".to_string());
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(test_table),
    )));
    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
        .unwrap()
        .sort(vec![col("pk1").sort(true, false)])
        .unwrap()
        .project(vec![col("pk1").alias("something")])
        .unwrap()
        .limit(0, Some(10))
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();

    let expected = [
        "Projection: something",
        "  Limit: skip=0, fetch=10",
        "    MergeSort: something ASC NULLS LAST",
        "      MergeScan [is_placeholder=false, remote_input=[",
        "Limit: skip=0, fetch=10",
        "  Projection: t.pk1 AS something",
        "    Sort: t.pk1 ASC NULLS LAST",
        "      TableScan: t",
        "]]",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());
}

/// FIXME(discord9): alias to same name with col req makes it ambiguous
/// for now since it bugged, will use fallback plan rewriter to only push down table scan node
#[test]
fn expand_sort_alias_conflict_limit() {
    // use logging for better debugging
    init_default_ut_logging();
    let test_table = TestTable::table_with_name(0, "t".to_string());
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(test_table),
    )));
    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
        .unwrap()
        .sort(vec![col("pk1").sort(true, false)])
        .unwrap()
        .project(vec![col("pk2").alias("pk1")])
        .unwrap()
        .limit(0, Some(10))
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan.clone(), &config);
    assert!(result.is_err(), "Expected error for ambiguous alias");
    assert!(format!("{result:?}").contains("AmbiguousReference"));

    let mut config = ConfigOptions::default();
    config.extensions.insert(DistPlannerOptions {
        allow_query_fallback: true,
    });
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();

    let expected = [
        "Limit: skip=0, fetch=10",
        "  Projection: t.pk2 AS pk1",
        "    Sort: t.pk1 ASC NULLS LAST",
        "      MergeScan [is_placeholder=false, remote_input=[",
        "TableScan: t",
        "]]",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());
}

#[test]
fn expand_sort_alias_conflict_but_not_really_limit() {
    // use logging for better debugging
    init_default_ut_logging();
    let test_table = TestTable::table_with_name(0, "t".to_string());
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(test_table),
    )));
    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
        .unwrap()
        .sort(vec![col("pk1").sort(true, false)])
        .unwrap()
        .project(vec![col("pk2").alias("t.pk1")])
        .unwrap()
        .limit(0, Some(10))
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();

    let expected = [
        "Projection: t.pk1",
        "  Limit: skip=0, fetch=10",
        "    MergeSort: t.pk1 ASC NULLS LAST",
        "      MergeScan [is_placeholder=false, remote_input=[",
        "Limit: skip=0, fetch=10",
        "  Projection: t.pk2 AS t.pk1, t.pk1",
        "    Sort: t.pk1 ASC NULLS LAST",
        "      TableScan: t",
        "]]",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());
}

/// TODO(discord9): it is possible to expand `Sort` and `Limit` in the same step,
/// but it's too complicated to implement now, and probably not worth it since `Limit` already
/// greatly reduces the amount of data to sort.
#[test]
fn expand_limit_sort() {
    // use logging for better debugging
    init_default_ut_logging();
    let test_table = TestTable::table_with_name(0, "t".to_string());
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(test_table),
    )));
    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
        .unwrap()
        .limit(0, Some(10))
        .unwrap()
        .sort(vec![col("pk1").sort(true, false)])
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();

    let expected = [
        "Sort: t.pk1 ASC NULLS LAST",
        "  Projection: t.pk1, t.pk2, t.pk3, t.ts, t.number",
        "    Limit: skip=0, fetch=10",
        "      MergeScan [is_placeholder=false, remote_input=[",
        "Limit: skip=0, fetch=10",
        "  TableScan: t",
        "]]",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());
}

#[test]
fn expand_sort_limit_sort() {
    // use logging for better debugging
    init_default_ut_logging();
    let test_table = TestTable::table_with_name(0, "t".to_string());
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(test_table),
    )));
    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
        .unwrap()
        .sort(vec![col("pk1").sort(true, false)])
        .unwrap()
        .limit(0, Some(10))
        .unwrap()
        .sort(vec![col("pk1").sort(true, false)])
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();

    let expected = [
        "Sort: t.pk1 ASC NULLS LAST",
        "  Projection: t.pk1, t.pk2, t.pk3, t.ts, t.number",
        "    Limit: skip=0, fetch=10",
        "      MergeSort: t.pk1 ASC NULLS LAST",
        "        MergeScan [is_placeholder=false, remote_input=[",
        "Limit: skip=0, fetch=10",
        "  Sort: t.pk1 ASC NULLS LAST",
        "    TableScan: t",
        "]]",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());
}

/// test plan like:
/// ```
/// Aggregate: min(t.number)
///  Projection: t.number
/// ```
/// which means aggr introduce new column requirements that shouldn't be updated in lower projection
///
/// this help test expand need actually add new column requirements
/// because ``Limit` doesn't introduce new column requirements
/// only `Sort/Aggregate` does, and for now since `aggregate` get expanded immediately, it's col requirements are not used anyway
#[test]
fn expand_proj_step_aggr() {
    // use logging for better debugging
    init_default_ut_logging();
    let test_table = TestTable::table_with_name(0, "t".to_string());
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(test_table),
    )));
    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
        .unwrap()
        .project(vec![col("number")])
        .unwrap()
        .aggregate(Vec::<Expr>::new(), vec![min(col("number"))])
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();

    let expected = [
        "Projection: min(t.number)",
        "  Aggregate: groupBy=[[]], aggr=[[__min_merge(__min_state(t.number)) AS min(t.number)]]",
        "    MergeScan [is_placeholder=false, remote_input=[",
        "Aggregate: groupBy=[[]], aggr=[[__min_state(t.number)]]",
        "  Projection: t.number", // This Projection shouldn't add new column requirements
        "    TableScan: t",
        "]]",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());
}

/// Shouldn't push down the fake partition column aggregate(which is steppable)
/// as the `pk1` is a alias for `pk3` which is not partition column
#[test]
fn expand_proj_alias_fake_part_col_aggr() {
    // use logging for better debugging
    init_default_ut_logging();
    let test_table = TestTable::table_with_name(0, "t".to_string());
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(test_table),
    )));
    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
        .unwrap()
        .project(vec![
            col("number"),
            col("pk3").alias("pk1"),
            col("pk2").alias("pk3"),
        ])
        .unwrap()
        .project(vec![
            col("number"),
            col("pk1").alias("pk2"),
            col("pk3").alias("pk1"),
        ])
        .unwrap()
        .aggregate(vec![col("pk1"), col("pk2")], vec![min(col("number"))])
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();

    let expected = [
        "Projection: pk1, pk2, min(t.number)",
        "  Aggregate: groupBy=[[pk1, pk2]], aggr=[[__min_merge(__min_state(t.number)) AS min(t.number)]]",
        "    MergeScan [is_placeholder=false, remote_input=[",
        "Aggregate: groupBy=[[pk1, pk2]], aggr=[[__min_state(t.number)]]",
        "  Projection: t.number, pk1 AS pk2, pk3 AS pk1",
        "    Projection: t.number, t.pk3 AS pk1, t.pk2 AS pk3",
        "      TableScan: t",
        "]]",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());
}

#[test]
fn expand_proj_alias_aliased_part_col_aggr() {
    // use logging for better debugging
    init_default_ut_logging();
    let test_table = TestTable::table_with_name(0, "t".to_string());
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(test_table),
    )));
    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
        .unwrap()
        .project(vec![
            col("number"),
            col("pk1").alias("pk3"),
            col("pk2").alias("pk4"),
        ])
        .unwrap()
        .project(vec![
            col("number"),
            col("pk3").alias("pk42"),
            col("pk4").alias("pk43"),
        ])
        .unwrap()
        .aggregate(vec![col("pk42"), col("pk43")], vec![min(col("number"))])
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();

    let expected = [
        "Projection: pk42, pk43, min(t.number)",
        "  MergeScan [is_placeholder=false, remote_input=[",
        "Aggregate: groupBy=[[pk42, pk43]], aggr=[[min(t.number)]]",
        "  Projection: t.number, pk3 AS pk42, pk4 AS pk43",
        "    Projection: t.number, t.pk1 AS pk3, t.pk2 AS pk4",
        "      TableScan: t",
        "]]",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());
}

/// notice that step aggr then part col aggr seems impossible as the partition columns for part col aggr
/// can't pass through the step aggr without making step aggr also a part col aggr
/// so here only test part col aggr -> step aggr case
#[test]
fn expand_part_col_aggr_step_aggr() {
    // use logging for better debugging
    init_default_ut_logging();
    let test_table = TestTable::table_with_name(0, "t".to_string());
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(test_table),
    )));
    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
        .unwrap()
        .aggregate(vec![col("pk1"), col("pk2")], vec![max(col("number"))])
        .unwrap()
        .aggregate(Vec::<Expr>::new(), vec![min(col("max(t.number)"))])
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();

    let expected = [
        "Projection: min(max(t.number))",
        "  Aggregate: groupBy=[[]], aggr=[[__min_merge(__min_state(max(t.number))) AS min(max(t.number))]]",
        "    MergeScan [is_placeholder=false, remote_input=[",
        "Aggregate: groupBy=[[]], aggr=[[__min_state(max(t.number))]]",
        "  Aggregate: groupBy=[[t.pk1, t.pk2]], aggr=[[max(t.number)]]",
        "    TableScan: t",
        "]]",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());
}

#[test]
fn expand_step_aggr_step_aggr() {
    // use logging for better debugging
    init_default_ut_logging();
    let test_table = TestTable::table_with_name(0, "t".to_string());
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(test_table),
    )));
    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
        .unwrap()
        .aggregate(Vec::<Expr>::new(), vec![max(col("number"))])
        .unwrap()
        .aggregate(Vec::<Expr>::new(), vec![min(col("max(t.number)"))])
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();

    let expected = [
        "Aggregate: groupBy=[[]], aggr=[[min(max(t.number))]]",
        "  Projection: max(t.number)",
        "    Aggregate: groupBy=[[]], aggr=[[__max_merge(__max_state(t.number)) AS max(t.number)]]",
        "      MergeScan [is_placeholder=false, remote_input=[",
        "Aggregate: groupBy=[[]], aggr=[[__max_state(t.number)]]",
        "  TableScan: t",
        "]]",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());
}

#[test]
fn expand_part_col_aggr_part_col_aggr() {
    // use logging for better debugging
    init_default_ut_logging();
    let test_table = TestTable::table_with_name(0, "t".to_string());
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(test_table),
    )));
    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
        .unwrap()
        .aggregate(vec![col("pk1"), col("pk2")], vec![max(col("number"))])
        .unwrap()
        .aggregate(
            vec![col("pk1"), col("pk2")],
            vec![min(col("max(t.number)"))],
        )
        .unwrap()
        .build()
        .unwrap();

    let expected_original = [
        // See DataFusion #14860 for change details.
        "Aggregate: groupBy=[[t.pk1, t.pk2]], aggr=[[min(max(t.number))]]",
        "  Aggregate: groupBy=[[t.pk1, t.pk2]], aggr=[[max(t.number)]]",
        "    TableScan: t",
    ]
    .join("\n");
    assert_eq!(expected_original, plan.to_string());

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();

    let expected = [
        "Projection: t.pk1, t.pk2, min(max(t.number))",
        "  MergeScan [is_placeholder=false, remote_input=[",
        "Aggregate: groupBy=[[t.pk1, t.pk2]], aggr=[[min(max(t.number))]]",
        "  Aggregate: groupBy=[[t.pk1, t.pk2]], aggr=[[max(t.number)]]",
        "    TableScan: t",
        "]]",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());
}

#[test]
fn expand_step_aggr_proj() {
    // use logging for better debugging
    init_default_ut_logging();
    let test_table = TestTable::table_with_name(0, "t".to_string());
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(test_table),
    )));
    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
        .unwrap()
        .aggregate(vec![col("pk1")], vec![min(col("number"))])
        .unwrap()
        .project(vec![col("min(t.number)")])
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();

    let expected = [
        "Projection: min(t.number)",
        "  Projection: t.pk1, min(t.number)",
        "    Aggregate: groupBy=[[t.pk1]], aggr=[[__min_merge(__min_state(t.number)) AS min(t.number)]]",
        "      MergeScan [is_placeholder=false, remote_input=[",
        "Aggregate: groupBy=[[t.pk1]], aggr=[[__min_state(t.number)]]",
        "  TableScan: t",
        "]]",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());
}

/// Make sure that `SeriesDivide` special handling correctly clean up column requirements from it's previous sort
#[test]
fn expand_complex_col_req_sort_pql() {
    // use logging for better debugging
    init_default_ut_logging();
    let test_table = TestTable::table_with_name(0, "t".to_string());
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(test_table),
    )));

    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source.clone(), None, vec![])
        .unwrap()
        .sort(vec![
            col("pk1").sort(true, false),
            col("pk2").sort(true, false),
            col("pk3").sort(true, false), // make some col req here
        ])
        .unwrap()
        .build()
        .unwrap();
    let plan = SeriesDivide::new(
        vec!["pk1".to_string(), "pk2".to_string(), "pk3".to_string()],
        "ts".to_string(),
        plan,
    );
    let plan = LogicalPlan::Extension(datafusion_expr::Extension {
        node: Arc::new(plan),
    });

    let plan = LogicalPlanBuilder::from(plan)
        .aggregate(vec![col("pk1"), col("pk2")], vec![min(col("number"))])
        .unwrap()
        .sort(vec![
            col("pk1").sort(true, false),
            col("pk2").sort(true, false),
        ])
        .unwrap()
        .project(vec![col("pk1"), col("pk2")])
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();

    let expected = [
        "Projection: t.pk1, t.pk2",
        "  MergeSort: t.pk1 ASC NULLS LAST, t.pk2 ASC NULLS LAST",
        "    MergeScan [is_placeholder=false, remote_input=[",
        "Projection: t.pk1, t.pk2",
        "  Sort: t.pk1 ASC NULLS LAST, t.pk2 ASC NULLS LAST",
        "    Aggregate: groupBy=[[t.pk1, t.pk2]], aggr=[[min(t.number)]]",
        r#"      PromSeriesDivide: tags=["pk1", "pk2", "pk3"]"#,
        "        Sort: t.pk1 ASC NULLS LAST, t.pk2 ASC NULLS LAST, t.pk3 ASC NULLS LAST",
        "          TableScan: t",
        "]]",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());
}

/// should only expand `Sort`, notice `Sort` before `Aggregate` usually can and
/// will be optimized out, and dist planner shouldn't handle that case, but
/// for now, still handle that be expanding the `Sort` node
#[test]
fn expand_proj_sort_step_aggr_limit() {
    // use logging for better debugging
    init_default_ut_logging();
    let test_table = TestTable::table_with_name(0, "t".to_string());
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(test_table),
    )));
    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
        .unwrap()
        .sort(vec![col("pk1").sort(true, false)])
        .unwrap()
        .project(vec![Expr::Column(Column::new(
            Some(TableReference::bare("t")),
            "number",
        ))])
        .unwrap()
        .aggregate(Vec::<Expr>::new(), vec![min(col("number"))])
        .unwrap()
        .limit(0, Some(10))
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();
    let expected = [
        "Limit: skip=0, fetch=10",
        "  Aggregate: groupBy=[[]], aggr=[[min(t.number)]]",
        "    Projection: t.number",
        "      MergeSort: t.pk1 ASC NULLS LAST",
        "        MergeScan [is_placeholder=false, remote_input=[",
        "Projection: t.number, t.pk1",
        "  Sort: t.pk1 ASC NULLS LAST",
        "    TableScan: t",
        "]]",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());
}

#[test]
fn expand_proj_sort_limit_step_aggr() {
    // use logging for better debugging
    init_default_ut_logging();
    let test_table = TestTable::table_with_name(0, "t".to_string());
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(test_table),
    )));
    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
        .unwrap()
        .sort(vec![col("pk1").sort(true, false)])
        .unwrap()
        .project(vec![Expr::Column(Column::new(
            Some(TableReference::bare("t")),
            "number",
        ))])
        .unwrap()
        .limit(0, Some(10))
        .unwrap()
        .aggregate(Vec::<Expr>::new(), vec![min(col("number"))])
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();
    let expected = [
        "Aggregate: groupBy=[[]], aggr=[[min(t.number)]]",
        "  Projection: t.number",
        "    Limit: skip=0, fetch=10",
        "      MergeSort: t.pk1 ASC NULLS LAST",
        "        MergeScan [is_placeholder=false, remote_input=[",
        "Limit: skip=0, fetch=10",
        "  Projection: t.number, t.pk1",
        "    Sort: t.pk1 ASC NULLS LAST",
        "      TableScan: t",
        "]]",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());
}

#[test]
fn expand_proj_limit_step_aggr_sort() {
    // use logging for better debugging
    init_default_ut_logging();
    let test_table = TestTable::table_with_name(0, "t".to_string());
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(test_table),
    )));
    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
        .unwrap()
        .project(vec![Expr::Column(Column::new(
            Some(TableReference::bare("t")),
            "number",
        ))])
        .unwrap()
        .limit(0, Some(10))
        .unwrap()
        .aggregate(Vec::<Expr>::new(), vec![min(col("number"))])
        .unwrap()
        .sort(vec![col("min(t.number)").sort(true, false)])
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();
    let expected = [
        "Sort: min(t.number) ASC NULLS LAST",
        "  Aggregate: groupBy=[[]], aggr=[[min(t.number)]]",
        "    Projection: t.number",
        "      Limit: skip=0, fetch=10",
        "        MergeScan [is_placeholder=false, remote_input=[",
        "Limit: skip=0, fetch=10",
        "  Projection: t.number",
        "    TableScan: t",
        "]]",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());
}

#[test]
fn expand_proj_sort_part_col_aggr_limit() {
    // use logging for better debugging
    init_default_ut_logging();
    let test_table = TestTable::table_with_name(0, "t".to_string());
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(test_table),
    )));
    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
        .unwrap()
        .sort(vec![col("pk3").sort(true, false)])
        .unwrap()
        .project(vec![
            Expr::Column(Column::new(Some(TableReference::bare("t")), "number")),
            col("pk1"),
            col("pk2"),
        ])
        .unwrap()
        .aggregate(vec![col("pk1"), col("pk2")], vec![min(col("number"))])
        .unwrap()
        .limit(0, Some(10))
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();
    let expected = [
        "Limit: skip=0, fetch=10",
        "  Aggregate: groupBy=[[t.pk1, t.pk2]], aggr=[[min(t.number)]]",
        "    Projection: t.number, t.pk1, t.pk2",
        "      MergeSort: t.pk3 ASC NULLS LAST",
        "        MergeScan [is_placeholder=false, remote_input=[",
        "Projection: t.number, t.pk1, t.pk2, t.pk3",
        "  Sort: t.pk3 ASC NULLS LAST",
        "    TableScan: t",
        "]]",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());
}

#[test]
fn expand_proj_sort_limit_part_col_aggr() {
    // use logging for better debugging
    init_default_ut_logging();
    let test_table = TestTable::table_with_name(0, "t".to_string());
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(test_table),
    )));
    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
        .unwrap()
        .sort(vec![col("pk3").sort(true, false)])
        .unwrap()
        .project(vec![
            Expr::Column(Column::new(Some(TableReference::bare("t")), "number")),
            col("pk1"),
            col("pk2"),
        ])
        .unwrap()
        .limit(0, Some(10))
        .unwrap()
        .aggregate(vec![col("pk1"), col("pk2")], vec![min(col("number"))])
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();
    let expected = [
        "Aggregate: groupBy=[[t.pk1, t.pk2]], aggr=[[min(t.number)]]",
        "  Projection: t.number, t.pk1, t.pk2",
        "    Limit: skip=0, fetch=10",
        "      MergeSort: t.pk3 ASC NULLS LAST",
        "        MergeScan [is_placeholder=false, remote_input=[",
        "Limit: skip=0, fetch=10",
        "  Projection: t.number, t.pk1, t.pk2, t.pk3",
        "    Sort: t.pk3 ASC NULLS LAST",
        "      TableScan: t",
        "]]",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());
}
#[test]
fn expand_proj_part_col_aggr_limit_sort() {
    // use logging for better debugging
    init_default_ut_logging();
    let test_table = TestTable::table_with_name(0, "t".to_string());
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(test_table),
    )));
    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
        .unwrap()
        .project(vec![
            Expr::Column(Column::new(Some(TableReference::bare("t")), "number")),
            col("pk1"),
            col("pk2"),
        ])
        .unwrap()
        .aggregate(vec![col("pk1"), col("pk2")], vec![min(col("number"))])
        .unwrap()
        .limit(0, Some(10))
        .unwrap()
        .sort(vec![col("pk2").sort(true, false)])
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();
    let expected = [
        "Sort: t.pk2 ASC NULLS LAST",
        "  Projection: t.pk1, t.pk2, min(t.number)",
        "    Limit: skip=0, fetch=10",
        "      MergeScan [is_placeholder=false, remote_input=[",
        "Limit: skip=0, fetch=10",
        "  Aggregate: groupBy=[[t.pk1, t.pk2]], aggr=[[min(t.number)]]",
        "    Projection: t.number, t.pk1, t.pk2",
        "      TableScan: t",
        "]]",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());
}

#[test]
fn expand_proj_part_col_aggr_sort_limit() {
    // use logging for better debugging
    init_default_ut_logging();
    let test_table = TestTable::table_with_name(0, "t".to_string());
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(test_table),
    )));
    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
        .unwrap()
        .project(vec![
            Expr::Column(Column::new(Some(TableReference::bare("t")), "number")),
            col("pk1"),
            col("pk2"),
        ])
        .unwrap()
        .aggregate(vec![col("pk1"), col("pk2")], vec![min(col("number"))])
        .unwrap()
        .sort(vec![col("pk2").sort(true, false)])
        .unwrap()
        .limit(0, Some(10))
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();
    let expected = [
        "Projection: t.pk1, t.pk2, min(t.number)",
        "  Limit: skip=0, fetch=10",
        "    MergeSort: t.pk2 ASC NULLS LAST",
        "      MergeScan [is_placeholder=false, remote_input=[",
        "Limit: skip=0, fetch=10",
        "  Sort: t.pk2 ASC NULLS LAST",
        "    Aggregate: groupBy=[[t.pk1, t.pk2]], aggr=[[min(t.number)]]",
        "      Projection: t.number, t.pk1, t.pk2",
        "        TableScan: t",
        "]]",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());
}

#[test]
fn test_simplify_select_now_expression() {
    init_default_ut_logging();
    let test_table = TestTable::table_with_name(0, "t".to_string());
    let table_provider = Arc::new(DfTableProviderAdapter::new(test_table));
    let table_source = Arc::new(DefaultTableSource::new(table_provider.clone()));
    let ctx = SessionContext::new();
    ctx.register_table(TableReference::bare("t"), table_provider.clone() as _)
        .unwrap();

    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source.clone(), None, vec![])
        .unwrap()
        .project(vec![now()])
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}
        .analyze(plan.clone(), &config)
        .unwrap();

    common_telemetry::info!("Analyzed plan: {}", result);

    let result_str = result.to_string();
    // Normalize timestamp values to make test deterministic
    let re = Regex::new(r"TimestampNanosecond\(\d+,").unwrap();
    let normalized = re.replace_all(&result_str, "TimestampNanosecond(<TIME>,");

    let expected = [
        "Projection: now()",
        "  MergeScan [is_placeholder=false, remote_input=[",
        r#"Projection: TimestampNanosecond(<TIME>, None) AS now()"#,
        "  TableScan: t",
        "]]",
    ]
    .join("\n");
    assert_eq!(expected, normalized);
}

#[test]
fn sibling_merge_scans_have_unique_remote_dyn_filter_producer_ids() {
    init_default_ut_logging();
    let left_table = TestTable::table_with_name(0, "left_table".to_string());
    let right_table = TestTable::table_with_name(1, "right_table".to_string());

    let left_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(left_table),
    )));
    let right_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(right_table),
    )));

    let left_sorted =
        LogicalPlanBuilder::scan_with_filters("left_table", left_source, None, vec![])
            .unwrap()
            .sort(vec![col("pk1").sort(true, false)])
            .unwrap()
            .build()
            .unwrap();

    let right_sorted =
        LogicalPlanBuilder::scan_with_filters("right_table", right_source, None, vec![])
            .unwrap()
            .sort(vec![col("pk1").sort(true, false)])
            .unwrap()
            .build()
            .unwrap();

    let plan = LogicalPlanBuilder::from(left_sorted)
        .cross_join(right_sorted)
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();

    let mut producer_ids = Vec::new();
    collect_merge_scan_remote_dyn_filter_producer_id_list(&result, &mut producer_ids);
    let unique_producer_ids = producer_ids.iter().copied().collect::<BTreeSet<_>>();

    assert!(
        producer_ids.len() >= 2,
        "Expected at least 2 RemoteDynFilterProducerIds, got {}: {producer_ids:?}",
        producer_ids.len()
    );
    assert_eq!(
        producer_ids.len(),
        unique_producer_ids.len(),
        "Expected all sibling RemoteDynFilterProducerIds to be unique, got ids: {producer_ids:?}"
    );
}

#[test]
fn pre_merge_scan_optimizer_eliminates_projected_false_filter() {
    init_default_ut_logging();
    let left_table =
        TestTable::table_with_filter_pushdown(0, "i1".to_string(), FilterPushDownType::Inexact);
    let right_table =
        TestTable::table_with_filter_pushdown(1, "i2".to_string(), FilterPushDownType::Inexact);

    let left_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(left_table),
    )));
    let right_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(right_table),
    )));

    let left = LogicalPlanBuilder::scan_with_filters("i1", left_source, None, vec![])
        .unwrap()
        .build()
        .unwrap();
    let right = LogicalPlanBuilder::scan_with_filters("i2", right_source, None, vec![])
        .unwrap()
        .build()
        .unwrap();

    let plan = LogicalPlanBuilder::from(left)
        .cross_join(right)
        .unwrap()
        .project(vec![lit(false).alias("cond")])
        .unwrap()
        .filter(col("cond"))
        .unwrap()
        .sort(vec![col("cond").sort(true, true)])
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();

    assert_eq!("EmptyRelation: rows=0", result.to_string());
}

#[test]
fn test_simplify_now_expression() {
    init_default_ut_logging();
    let test_table = TestTable::table_with_name(0, "t".to_string());
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(test_table),
    )));
    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source.clone(), None, vec![])
        .unwrap()
        .build()
        .unwrap();

    // CAST(t.ts AS Timestamp(Millisecond, Some("+00:00")))
    let ts_cast_type = DataType::Timestamp(TimeUnit::Millisecond, Some("+00:00".into()));

    let ts_expr = col("ts").cast_to(&ts_cast_type, plan.schema()).unwrap();

    // CAST(now() - interval AS Timestamp(Millisecond, Some("+00:00")))
    let interval = lit(ScalarValue::new_interval_mdn(0, 0, 2700000000000)); // 2700s = 45m
    let right_expr = binary_expr(now(), Operator::Minus, interval);
    let right_expr_cast = right_expr.cast_to(&ts_cast_type, plan.schema()).unwrap();

    let filter_expr = ts_expr.lt_eq(right_expr_cast);

    // Projection: t.b, count(Int64(1))
    //   Aggregate: groupBy=[[my_table.b]], aggr=[[count(my_table.ts) AS count(Int64(1))]]
    //     Filter: CAST(my_table.ts AS Timestamp(Millisecond, Some("+00:00"))) <= CAST(now() - IntervalMonthDayNano("IntervalMonthDayNano { months: 0, days: 0, nanoseconds: 2700000000000 }") AS Timestamp(Millisecond, Some("+00:00")))
    //       TableScan: my_table
    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
        .unwrap()
        .filter(filter_expr)
        .unwrap()
        .aggregate(
            vec![col("pk1")],
            vec![
                datafusion::functions_aggregate::expr_fn::count(col("ts")).alias("count(Int64(1))"),
            ],
        )
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();

    let plan_str = result.to_string();
    common_telemetry::info!("Analyzed plan: {}", plan_str);

    // If simplified, "now()" should be replaced by a literal.
    assert!(
        !plan_str.contains("now()"),
        "Plan should be simplified but contains now(): {}",
        plan_str
    );
}

#[test]
fn expand_proj_limit_part_col_aggr_sort() {
    // use logging for better debugging
    init_default_ut_logging();
    let test_table = TestTable::table_with_name(0, "t".to_string());
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(test_table),
    )));
    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
        .unwrap()
        .project(vec![
            Expr::Column(Column::new(Some(TableReference::bare("t")), "number")),
            col("pk1"),
            col("pk2"),
        ])
        .unwrap()
        .limit(0, Some(10))
        .unwrap()
        .aggregate(vec![col("pk1"), col("pk2")], vec![min(col("number"))])
        .unwrap()
        .sort(vec![col("pk2").sort(true, false)])
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();
    // Pre-MergeScan optimizer intentionally excludes PushDownLimit, so the
    // remote plan shows an explicit Limit node instead of `fetch=10` on
    // TableScan.
    let expected = [
        "Sort: t.pk2 ASC NULLS LAST",
        "  Aggregate: groupBy=[[t.pk1, t.pk2]], aggr=[[min(t.number)]]",
        "    Projection: t.number, t.pk1, t.pk2",
        "      Limit: skip=0, fetch=10",
        "        MergeScan [is_placeholder=false, remote_input=[",
        "Limit: skip=0, fetch=10",
        "  Projection: t.number, t.pk1, t.pk2",
        "    TableScan: t",
        "]]",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());
}

#[test]
fn expand_proj_limit_sort_part_col_aggr() {
    // use logging for better debugging
    init_default_ut_logging();
    let test_table = TestTable::table_with_name(0, "t".to_string());
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(test_table),
    )));
    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
        .unwrap()
        .project(vec![
            Expr::Column(Column::new(Some(TableReference::bare("t")), "number")),
            col("pk1"),
            col("pk2"),
        ])
        .unwrap()
        .limit(0, Some(10))
        .unwrap()
        .sort(vec![col("pk2").sort(true, false)])
        .unwrap()
        .aggregate(vec![col("pk1"), col("pk2")], vec![min(col("number"))])
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();
    // Pre-MergeScan optimizer intentionally excludes PushDownLimit, so the
    // remote plan shows an explicit Limit node instead of `fetch=10` on
    // TableScan.
    let expected = [
        "Aggregate: groupBy=[[t.pk1, t.pk2]], aggr=[[min(t.number)]]",
        "  Sort: t.pk2 ASC NULLS LAST",
        "    Projection: t.number, t.pk1, t.pk2",
        "      Limit: skip=0, fetch=10",
        "        MergeScan [is_placeholder=false, remote_input=[",
        "Limit: skip=0, fetch=10",
        "  Projection: t.number, t.pk1, t.pk2",
        "    TableScan: t",
        "]]",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());
}

/// Notice how this limit can't be push down, or results will be wrong
#[test]
fn expand_step_aggr_limit() {
    // use logging for better debugging
    init_default_ut_logging();
    let test_table = TestTable::table_with_name(0, "t".to_string());
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(test_table),
    )));
    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
        .unwrap()
        .aggregate(vec![col("pk1")], vec![min(col("number"))])
        .unwrap()
        .limit(0, Some(10))
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();
    let expected = [
        "Limit: skip=0, fetch=10",
        "  Projection: t.pk1, min(t.number)",
        "    Aggregate: groupBy=[[t.pk1]], aggr=[[__min_merge(__min_state(t.number)) AS min(t.number)]]",
        "      MergeScan [is_placeholder=false, remote_input=[",
        "Aggregate: groupBy=[[t.pk1]], aggr=[[__min_state(t.number)]]",
        "  TableScan: t",
        "]]",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());
}

/// Test how avg get expanded
#[test]
fn expand_step_aggr_avg_limit() {
    // use logging for better debugging
    init_default_ut_logging();
    let test_table = TestTable::table_with_name(0, "t".to_string());
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(test_table),
    )));
    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
        .unwrap()
        .aggregate(vec![col("pk1")], vec![avg(col("number"))])
        .unwrap()
        .limit(0, Some(10))
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();
    let expected = [
        "Limit: skip=0, fetch=10",
        "  Projection: t.pk1, avg(t.number)",
        "    Aggregate: groupBy=[[t.pk1]], aggr=[[__avg_merge(__avg_state(t.number)) AS avg(t.number)]]",
        "      MergeScan [is_placeholder=false, remote_input=[",
        "Aggregate: groupBy=[[t.pk1]], aggr=[[__avg_state(CAST(t.number AS Float64))]]",
        "  TableScan: t",
        "]]",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());
}

/// notice how `Limit` can still get expanded
#[test]
fn expand_part_col_aggr_limit() {
    // use logging for better debugging
    init_default_ut_logging();
    let test_table = TestTable::table_with_name(0, "t".to_string());
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(test_table),
    )));
    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
        .unwrap()
        .aggregate(vec![col("pk1"), col("pk2")], vec![min(col("number"))])
        .unwrap()
        .limit(0, Some(10))
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();
    let expected = [
        "Projection: t.pk1, t.pk2, min(t.number)",
        "  Limit: skip=0, fetch=10",
        "    MergeScan [is_placeholder=false, remote_input=[",
        "Limit: skip=0, fetch=10",
        "  Aggregate: groupBy=[[t.pk1, t.pk2]], aggr=[[min(t.number)]]",
        "    TableScan: t",
        "]]",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());
}

#[ignore = "Projection is disabled for https://github.com/apache/arrow-datafusion/issues/6489"]
#[test]
fn transform_simple_projection_filter() {
    let numbers_table = NumbersTable::table(0);
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(numbers_table),
    )));

    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
        .unwrap()
        .filter(col("number").lt(lit(10)))
        .unwrap()
        .project(vec![col("number")])
        .unwrap()
        .distinct()
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();
    let expected = [
        "Distinct:",
        "  MergeScan [is_placeholder=false]",
        "    Distinct:",
        "      Projection: t.number",
        "        Filter: t.number < Int32(10)",
        "          TableScan: t",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());
}

#[test]
fn transform_aggregator() {
    let numbers_table = NumbersTable::table(0);
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(numbers_table),
    )));

    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
        .unwrap()
        .aggregate(Vec::<Expr>::new(), vec![avg(col("number"))])
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();
    let expected = "Projection: avg(t.number)\
        \n  MergeScan [is_placeholder=false, remote_input=[\
        \nAggregate: groupBy=[[]], aggr=[[avg(t.number)]]\
        \n  TableScan: t\
        \n]]";
    assert_eq!(expected, result.to_string());
}

#[test]
fn transform_distinct_order() {
    let numbers_table = NumbersTable::table(0);
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(numbers_table),
    )));

    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
        .unwrap()
        .distinct()
        .unwrap()
        .sort(vec![col("number").sort(true, false)])
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();
    let expected = [
        "Projection: t.number",
        "  MergeScan [is_placeholder=false, remote_input=[
Sort: t.number ASC NULLS LAST
  Distinct:
    TableScan: t
]]",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());
}

#[test]
fn transform_single_limit() {
    let numbers_table = NumbersTable::table(0);
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(numbers_table),
    )));

    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
        .unwrap()
        .limit(0, Some(1))
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();
    let expected = "Projection: t.number\
        \n  MergeScan [is_placeholder=false, remote_input=[
Limit: skip=0, fetch=1
  TableScan: t
]]";
    assert_eq!(expected, result.to_string());
}

#[test]
fn transform_unalighed_join_with_alias() {
    let left = NumbersTable::table(0);
    let right = NumbersTable::table(1);
    let left_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(left),
    )));
    let right_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(right),
    )));

    let right_plan = LogicalPlanBuilder::scan_with_filters("t", right_source, None, vec![])
        .unwrap()
        .alias("right")
        .unwrap()
        .build()
        .unwrap();

    let plan = LogicalPlanBuilder::scan_with_filters("t", left_source, None, vec![])
        .unwrap()
        .join_on(
            right_plan,
            JoinType::LeftSemi,
            vec![col("t.number").eq(col("right.number"))],
        )
        .unwrap()
        .limit(0, Some(1))
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();
    let expected = [
        "Limit: skip=0, fetch=1",
        "  LeftSemi Join: t.number = right.number",
        "    Projection: t.number",
        "      MergeScan [is_placeholder=false, remote_input=[",
        "TableScan: t",
        "]]",
        "    Projection: right.number",
        "      MergeScan [is_placeholder=false, remote_input=[",
        "SubqueryAlias: right",
        "  TableScan: t",
        "]]",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());
}

#[test]
fn transform_subquery_sort_alias() {
    init_default_ut_logging();

    let test_table = TestTable::table_with_name(0, "numbers".to_string());
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(test_table),
    )));

    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
        .unwrap()
        .alias("a")
        .unwrap()
        .sort(vec![col("a.number").sort(true, false)])
        .unwrap()
        .build()
        .unwrap();
    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();
    let expected = [
        "Projection: a.pk1, a.pk2, a.pk3, a.ts, a.number",
        "  MergeSort: a.number ASC NULLS LAST",
        "    MergeScan [is_placeholder=false, remote_input=[",
        "Sort: a.number ASC NULLS LAST",
        "  SubqueryAlias: a",
        "    TableScan: t",
        "]]",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());
}

#[test]
fn transform_sort_subquery_alias() {
    init_default_ut_logging();
    let test_table = TestTable::table_with_name(0, "numbers".to_string());
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(test_table),
    )));

    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
        .unwrap()
        .sort(vec![col("t.number").sort(true, false)])
        .unwrap()
        .alias("a")
        .unwrap()
        .build()
        .unwrap();
    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();
    let expected = [
        "Projection: a.pk1, a.pk2, a.pk3, a.ts, a.number",
        "  MergeSort: a.number ASC NULLS LAST",
        "    MergeScan [is_placeholder=false, remote_input=[",
        "SubqueryAlias: a",
        "  Sort: t.number ASC NULLS LAST",
        "    TableScan: t",
        "]]",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());
}

#[test]
fn remote_dyn_filter_producer_ids_do_not_collide_between_subquery_and_outer_plan() {
    let test_table = TestTable::table_with_name(0, "numbers".to_string());
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(test_table),
    )));
    let subquery_plan =
        LogicalPlanBuilder::scan_with_filters("inner", table_source.clone(), None, vec![])
            .unwrap()
            .build()
            .unwrap();
    let subquery = Subquery {
        subquery: Arc::new(subquery_plan),
        outer_ref_columns: Default::default(),
        spans: Default::default(),
    };
    let outer_plan = LogicalPlanBuilder::scan_with_filters("outer", table_source, None, vec![])
        .unwrap()
        .filter(Expr::Exists(Exists {
            subquery,
            negated: false,
        }))
        .unwrap()
        .build()
        .unwrap();
    let rewritten = DistPlannerAnalyzer {}.try_push_down(outer_plan).unwrap();

    let mut producer_ids = BTreeSet::new();
    collect_merge_scan_remote_dyn_filter_producer_ids(&rewritten, &mut producer_ids);

    assert_eq!(producer_ids.len(), 2);
}

#[test]
fn date_bin_ts_group_by() {
    init_default_ut_logging();
    let test_table = TestTable::table_with_name(0, "t".to_string());
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(test_table),
    )));
    let date_bin_call = Expr::ScalarFunction(ScalarFunction::new_udf(
        date_bin(),
        vec![
            lit(datafusion_common::ScalarValue::IntervalDayTime(Some(
                IntervalDayTime::new(0, 60 * 1000), // 1 minute in millis
            ))),
            col("ts"),
        ],
    ));
    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
        .unwrap()
        .aggregate(vec![date_bin_call], vec![min(col("number"))])
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();

    let expected = [
        r#"Projection: date_bin(IntervalDayTime("IntervalDayTime { days: 0, milliseconds: 60000 }"),t.ts), min(t.number)"#,
        r#"  Aggregate: groupBy=[[date_bin(IntervalDayTime("IntervalDayTime { days: 0, milliseconds: 60000 }"),t.ts)]], aggr=[[__min_merge(__min_state(t.number)) AS min(t.number)]]"#,
        "    MergeScan [is_placeholder=false, remote_input=[",
        r#"Aggregate: groupBy=[[date_bin(IntervalDayTime("IntervalDayTime { days: 0, milliseconds: 60000 }"), t.ts)]], aggr=[[__min_state(t.number)]]"#,
        "  TableScan: t",
        "]]",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());
}

#[test]
fn test_last_value_order_by() {
    init_default_ut_logging();
    let test_table = TestTable::table_with_name(0, "t".to_string());
    let table_provider = Arc::new(DfTableProviderAdapter::new(test_table));
    let table_source = Arc::new(DefaultTableSource::new(table_provider.clone() as _));
    let ctx = SessionContext::new();
    ctx.register_table(TableReference::bare("t"), table_provider.clone() as _)
        .unwrap();
    ctx.register_udaf(AggregateUDF::new_from_impl(
        StateWrapper::new(
            datafusion::functions_aggregate::first_last::last_value_udaf()
                .as_ref()
                .clone(),
        )
        .unwrap(),
    ));

    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source.clone(), None, vec![])
        .unwrap()
        .aggregate(
            Vec::<Expr>::new(),
            vec![datafusion::functions_aggregate::first_last::last_value(
                col("ts"),
                vec![col("ts").sort(true, true)],
            )],
        )
        .unwrap()
        .build()
        .unwrap();

    try_encode_decode_substrait(&plan, ctx.state());

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}
        .analyze(plan.clone(), &config)
        .unwrap();

    let expected = [
        "Projection: last_value(t.ts) ORDER BY [t.ts ASC NULLS FIRST]",
        "  Aggregate: groupBy=[[]], aggr=[[__last_value_merge(__last_value_state(t.ts) ORDER BY [t.ts ASC NULLS FIRST]) AS last_value(t.ts) ORDER BY [t.ts ASC NULLS FIRST]]]",
        "    MergeScan [is_placeholder=false, remote_input=[",
        "Aggregate: groupBy=[[]], aggr=[[__last_value_state(t.ts) ORDER BY [t.ts ASC NULLS FIRST]]]",
        "  TableScan: t",
        "]]",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());

    let LogicalPlan::Aggregate(aggr_plan) = plan else {
        panic!("expect Aggregate plan");
    };
    let split = StateMergeHelper::split_aggr_node(aggr_plan).unwrap();

    try_encode_decode_substrait(&split.lower_state, ctx.state());
}

/// try remove the order by to see if it still works
#[test]
fn test_last_value_no_order_by() {
    init_default_ut_logging();
    let test_table = TestTable::table_with_name(0, "t".to_string());
    let table_provider = Arc::new(DfTableProviderAdapter::new(test_table));
    let table_source = Arc::new(DefaultTableSource::new(table_provider.clone() as _));
    let ctx = SessionContext::new();
    ctx.register_table(TableReference::bare("t"), table_provider.clone() as _)
        .unwrap();
    ctx.register_udaf(AggregateUDF::new_from_impl(
        StateWrapper::new(
            datafusion::functions_aggregate::first_last::last_value_udaf()
                .as_ref()
                .clone(),
        )
        .unwrap(),
    ));

    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
        .unwrap()
        .aggregate(
            Vec::<Expr>::new(),
            vec![datafusion::functions_aggregate::first_last::last_value(
                col("ts"),
                vec![],
            )],
        )
        .unwrap()
        .build()
        .unwrap();

    let LogicalPlan::Aggregate(aggr_plan) = plan.clone() else {
        panic!("expect Aggregate plan");
    };
    let split = StateMergeHelper::split_aggr_node(aggr_plan).unwrap();

    try_encode_decode_substrait(&split.lower_state, ctx.state());

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}
        .analyze(plan.clone(), &config)
        .unwrap();

    let expected = [
        "Projection: last_value(t.ts)",
        "  Aggregate: groupBy=[[]], aggr=[[__last_value_merge(__last_value_state(t.ts)) AS last_value(t.ts)]]",
        "    MergeScan [is_placeholder=false, remote_input=[",
        "Aggregate: groupBy=[[]], aggr=[[__last_value_state(t.ts)]]",
        "  TableScan: t",
        "]]",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());
}

#[test]
fn test_table_scan_projection() {
    init_default_ut_logging();
    let test_table = TestTable::table_with_name(0, "t".to_string());
    let table_provider = Arc::new(DfTableProviderAdapter::new(test_table));
    let table_source = Arc::new(DefaultTableSource::new(table_provider.clone() as _));
    let ctx = SessionContext::new();
    ctx.register_table(TableReference::bare("t"), table_provider.clone() as _)
        .unwrap();

    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, Some(vec![3]), vec![])
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}
        .analyze(plan.clone(), &config)
        .unwrap();
    let expected = [
        "Projection: t.ts",
        "  MergeScan [is_placeholder=false, remote_input=[",
        "TableScan: t projection=[ts]",
        "]]",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());
}

#[test]
fn test_table_scan_cast_projection_pushdown() {
    init_default_ut_logging();
    let test_table = TestTable::table_with_name(0, "t".to_string());
    let table_provider = Arc::new(DfTableProviderAdapter::new(test_table));
    let table_source = Arc::new(DefaultTableSource::new(table_provider.clone() as _));
    let ctx = SessionContext::new();
    ctx.register_table(TableReference::bare("t"), table_provider.clone() as _)
        .unwrap();

    let scan = LogicalPlanBuilder::scan_with_filters("t", table_source, Some(vec![3]), vec![])
        .unwrap()
        .build()
        .unwrap();
    let ts_cast_type = DataType::Timestamp(TimeUnit::Millisecond, Some("+00:00".into()));
    let ts_cast_expr = col("ts").cast_to(&ts_cast_type, scan.schema()).unwrap();
    let plan = LogicalPlanBuilder::from(scan)
        .project(vec![ts_cast_expr.alias("ts")])
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}
        .analyze(plan.clone(), &config)
        .unwrap();
    let expected = [
        "Projection: ts",
        "  MergeScan [is_placeholder=false, remote_input=[",
        "Projection: CAST(t.ts AS Timestamp(ms, \"+00:00\")) AS ts",
        "  TableScan: t projection=[ts]",
        "]]",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());
}

#[test]
fn test_cast_filter_simplified_after_pushdown() {
    // This test invokes `DistPlannerAnalyzer` directly rather than the full
    // query `SessionState`, so the globally-registered `ConstNormalizationRule`
    // does not run here. The native timestamp filter below proves the focused
    // pre-MergeScan pass can do this by running DataFusion `SimplifyExpressions`
    // after `PushDownFilter` has pushed the alias predicate through the cast
    // projection into `TableScan.filters`.
    init_default_ut_logging();
    let test_table =
        TestTable::table_with_filter_pushdown(0, "t".to_string(), FilterPushDownType::Inexact);
    let table_provider = Arc::new(DfTableProviderAdapter::new(test_table));
    let table_source = Arc::new(DefaultTableSource::new(table_provider.clone() as _));
    let ctx = SessionContext::new();
    ctx.register_table(TableReference::bare("t"), table_provider.clone() as _)
        .unwrap();

    let scan = LogicalPlanBuilder::scan_with_filters("t", table_source, Some(vec![3]), vec![])
        .unwrap()
        .build()
        .unwrap();
    let ts_cast_type = DataType::Timestamp(TimeUnit::Second, None);
    let ts_cast_expr = col("ts").cast_to(&ts_cast_type, scan.schema()).unwrap();
    let plan = LogicalPlanBuilder::from(scan)
        .project(vec![ts_cast_expr.alias("ts")])
        .unwrap()
        .filter(col("ts").gt_eq(lit(ScalarValue::TimestampSecond(Some(10), None))))
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}
        .analyze(plan.clone(), &config)
        .unwrap();
    let expected = [
        "Projection: ts",
        "  MergeScan [is_placeholder=false, remote_input=[",
        "Projection: CAST(t.ts AS Timestamp(s)) AS ts",
        "  Filter: t.ts >= TimestampMillisecond(10000, None)",
        "    TableScan: t projection=[ts], partial_filters=[t.ts >= TimestampMillisecond(10000, None)]",
        "]]",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());
}

/// Test that static side-local predicates on a JOIN input reach the remote
/// region TableScan before MergeScan wrapping (issue #8338).
///
/// Plan shape: Filter(t1.pk1 = 'v') -> Join(t1.number = t2.number) -> TableScan(t1), TableScan(t2)
///
/// After PushDownFilter runs, the side-local filter should be pushed into the
/// left child branch (inside the MergeScan remote_input), making it visible for
/// time-index / bloom / skipping pruning.
#[test]
fn test_join_side_local_filter_pushdown_into_merge_scan() {
    init_default_ut_logging();
    let left_table =
        TestTable::table_with_filter_pushdown(0, "t1".to_string(), FilterPushDownType::Inexact);
    let right_table =
        TestTable::table_with_filter_pushdown(1, "t2".to_string(), FilterPushDownType::Inexact);
    let left_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(left_table),
    )));
    let right_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(right_table),
    )));

    let right_plan = LogicalPlanBuilder::scan_with_filters("t2", right_source, None, vec![])
        .unwrap()
        .build()
        .unwrap();

    // Plan: Filter -> Join -> TableScan(left), TableScan(right)
    let plan = LogicalPlanBuilder::scan_with_filters("t1", left_source, None, vec![])
        .unwrap()
        .join_on(
            right_plan,
            JoinType::Inner,
            vec![col("t1.number").eq(col("t2.number"))],
        )
        .unwrap()
        .filter(col("t1.pk1").eq(lit("v"))) // side-local filter on left partition column
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();
    assert_remote_table_scan_filters_are_safe(&result);

    let plan_str = result.to_string();
    // After PushDownFilter runs, the predicate `t1.pk1 = Utf8("v")` should appear
    // inside the left MergeScan's remote_input. The pre-MergeScan optimizer may
    // combine it with join-derived IS NOT NULL pushdowns, so it may not appear as
    // a standalone Filter: line. It must still be in TableScan partial_filters
    // and below the Inner Join.
    assert!(
        plan_str.contains("t1.pk1 = Utf8(\"v\")"),
        "Expected predicate t1.pk1 = Utf8(\"v\") in plan, got:\n{plan_str}"
    );
    assert!(
        plan_str.contains(
            "TableScan: t1, partial_filters=[t1.pk1 = Utf8(\"v\"), t1.number IS NOT NULL]"
        ),
        "Expected t1 TableScan partial_filters to contain pushed predicate, got:\n{plan_str}"
    );

    // Find the position of the filter and verify it appears after a MergeScan
    // opening (i.e., inside remote_input) rather than before the Join.
    let filter_pos = plan_str
        .find("TableScan: t1, partial_filters=[t1.pk1 = Utf8(\"v\"), t1.number IS NOT NULL]")
        .unwrap();
    let join_pos = plan_str.find("Inner Join").unwrap();
    // The filter should be after the Join (meaning it was pushed down below the Join,
    // into a MergeScan's remote_input)
    assert!(
        filter_pos > join_pos,
        "Filter should be pushed below Join (into MergeScan remote_input), but found before Join"
    );
}

/// LEFT JOIN preserves the left side, so a left-local WHERE predicate is safe
/// to push into the left scan before MergeScan wrapping.
#[test]
fn test_left_join_left_side_filter_pushdown_into_merge_scan() {
    init_default_ut_logging();
    let left_table =
        TestTable::table_with_filter_pushdown(0, "t1".to_string(), FilterPushDownType::Inexact);
    let right_table =
        TestTable::table_with_filter_pushdown(1, "t2".to_string(), FilterPushDownType::Inexact);
    let left_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(left_table),
    )));
    let right_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(right_table),
    )));

    let right_plan = LogicalPlanBuilder::scan_with_filters("t2", right_source, None, vec![])
        .unwrap()
        .build()
        .unwrap();

    let plan = LogicalPlanBuilder::scan_with_filters("t1", left_source, None, vec![])
        .unwrap()
        .join_on(
            right_plan,
            JoinType::Left,
            vec![col("t1.number").eq(col("t2.number"))],
        )
        .unwrap()
        .filter(col("t1.pk1").eq(lit("v")))
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();
    assert_remote_table_scan_filters_are_safe(&result);

    let plan_str = result.to_string();
    assert!(
        plan_str.contains("TableScan: t1, partial_filters=[t1.pk1 = Utf8(\"v\")]"),
        "Expected left-side TableScan partial_filters under LEFT JOIN, got:\n{plan_str}"
    );
    let scan_filter_pos = plan_str
        .find("TableScan: t1, partial_filters=[t1.pk1 = Utf8(\"v\")]")
        .unwrap();
    let join_pos = plan_str.find("Left Join").unwrap();
    assert!(
        scan_filter_pos > join_pos,
        "Left-side filter should be pushed below LEFT JOIN into MergeScan remote_input:\n{plan_str}"
    );
}

/// Negative case: cross-table predicate t1.pk1 = t2.pk2 should NOT become a
/// side-local scan filter but remain as a join filter.
#[test]
fn test_join_cross_table_predicate_not_pushed_to_single_side() {
    init_default_ut_logging();
    let left_table =
        TestTable::table_with_filter_pushdown(0, "t1".to_string(), FilterPushDownType::Inexact);
    let right_table =
        TestTable::table_with_filter_pushdown(1, "t2".to_string(), FilterPushDownType::Inexact);
    let left_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(left_table),
    )));
    let right_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(right_table),
    )));

    let right_plan = LogicalPlanBuilder::scan_with_filters("t2", right_source, None, vec![])
        .unwrap()
        .build()
        .unwrap();

    // Plan: Filter(t1.pk1 = t2.pk2) -> Join(t1.number = t2.number) -> ...
    // The filter involves columns from both tables, so PushDownFilter should
    // keep it as a join filter (not push into a single side's scan).
    let plan = LogicalPlanBuilder::scan_with_filters("t1", left_source, None, vec![])
        .unwrap()
        .join_on(
            right_plan,
            JoinType::Inner,
            vec![col("t1.number").eq(col("t2.number"))],
        )
        .unwrap()
        .filter(col("t1.pk1").eq(col("t2.pk2"))) // cross-table predicate
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();
    assert_remote_table_scan_filters_are_safe(&result);

    let plan_str = result.to_string();
    // The cross-table predicate should NOT appear as a filter on a single table's
    // scan inside a MergeScan remote_input. It should remain as part of the
    // Join's filter.
    // The key assertion: it should NOT appear as "Filter: t1.pk1 = t2.pk2"
    assert!(
        !plan_str.contains("Filter: t1.pk1 = t2.pk2"),
        "Cross-table predicate should not become a side-local Filter:\n{plan_str}"
    );
    assert!(
        plan_str.contains("t1.pk1 = t2.pk2") || plan_str.contains("t2.pk2 = t1.pk1"),
        "Cross-table predicate should remain in the join plan:\n{plan_str}"
    );
    assert!(
        !plan_str.contains("partial_filters=[t1.pk1 = t2.pk2]")
            && !plan_str.contains("partial_filters=[t2.pk2 = t1.pk1]")
            && !plan_str.contains("full_filters=[t1.pk1 = t2.pk2]")
            && !plan_str.contains("full_filters=[t2.pk2 = t1.pk1]"),
        "Cross-table predicate should not become a single-side TableScan filter:\n{plan_str}"
    );
}

/// When `ScheduledTimeExtension` is injected into `ConfigOptions`, the
/// `SimplifyExpressions` pass (driven by `PatchOptimizerContext`) uses the
/// scheduled time instead of wall-clock time. The remote sub-plan must contain
/// the scheduled literal — not a variable wall-clock value.
#[test]
fn scheduled_now_yields_stable_literal_in_remote_plan() {
    init_default_ut_logging();
    let scheduled_time =
        chrono::DateTime::<chrono::Utc>::from_timestamp_millis(1700000000000).unwrap();
    let scheduled_ns = 1700000000000000000i64;

    let test_table = TestTable::table_with_name(0, "t".to_string());
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(test_table),
    )));

    // Build a plan with `now()` in filter and both `now()` and its
    // `current_timestamp()` alias in projection.
    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source.clone(), None, vec![])
        .unwrap()
        .filter(binary_expr(now(), Operator::LtEq, col("ts")))
        .unwrap()
        .project(vec![now(), now().alias("current_timestamp()")])
        .unwrap()
        .build()
        .unwrap();

    let mut config = ConfigOptions::default();
    config.extensions.insert(ScheduledTimeExtension {
        scheduled_time: Some(scheduled_time),
    });

    let result = DistPlannerAnalyzer {}
        .analyze(plan.clone(), &config)
        .unwrap();
    let result_str = result.to_string();
    common_telemetry::info!("Analyzed plan with scheduled time: {}", result_str);

    // The top-level should still say `Projection: now()` (schema recovery).
    assert!(
        result_str.contains("Projection: now()"),
        "Expected top-level Projection: now(), got:\n{result_str}"
    );
    assert!(
        result_str.contains("current_timestamp()"),
        "Expected top-level current_timestamp() alias, got:\n{result_str}"
    );

    // The remote sub-plan must contain the scheduled-time literal, i.e.
    // `TimestampNanosecond(1700000000000000000, None)`.
    let expected_literal = format!("TimestampNanosecond({scheduled_ns}, None)");
    assert!(
        result_str.contains(&expected_literal),
        "Expected remote plan literal '{expected_literal}', got:\n{result_str}"
    );

    // The remote sub-plan must contain the simplified literal (TimestampNanosecond).
    let remote_section = if let Some(idx) = result_str.find("remote_input=[") {
        &result_str[idx..]
    } else {
        ""
    };
    assert!(
        remote_section.contains("TimestampNanosecond("),
        "Remote plan should contain TimestampNanosecond literal:\n{result_str}"
    );

    // Absent the extension (default config), the same plan simplifies to a
    // wall-clock literal — the remote_input contains a variable timestamp.
    let default_config = ConfigOptions::default();
    let wall_result = DistPlannerAnalyzer {}
        .analyze(plan, &default_config)
        .unwrap();
    let wall_str = wall_result.to_string();
    // The wall-clock result must NOT contain the scheduled literal.
    assert!(
        !wall_str.contains(&expected_literal),
        "Wall-clock result should not contain scheduled literal {expected_literal}:\n{wall_str}"
    );
    // It should still simplify to a literal (TimestampNanosecond present in remote).
    let wall_remote = if let Some(idx) = wall_str.find("remote_input=[") {
        &wall_str[idx..]
    } else {
        ""
    };
    assert!(
        wall_remote.contains("TimestampNanosecond("),
        "Wall-clock remote plan should contain TimestampNanosecond:\n{wall_str}"
    );
}

/// `current_timestamp()` is an alias of `now()` in DataFusion, but keep a
/// dedicated SQL-level regression: when it appears in a side-local filter, it
/// must be folded to the scheduled literal before the filter interacts with the
/// pre-MergeScan pushdown / remote planning path.
#[test]
fn scheduled_current_timestamp_filter_folds_before_remote_pushdown() {
    init_default_ut_logging();
    let scheduled_time =
        chrono::DateTime::<chrono::Utc>::from_timestamp_millis(1700000000000).unwrap();

    let left_table =
        TestTable::table_with_filter_pushdown(0, "t1".to_string(), FilterPushDownType::Inexact);
    let right_table =
        TestTable::table_with_filter_pushdown(1, "t2".to_string(), FilterPushDownType::Inexact);
    let left_provider = Arc::new(DfTableProviderAdapter::new(left_table));
    let right_provider = Arc::new(DfTableProviderAdapter::new(right_table));
    let ctx = SessionContext::new();
    ctx.register_table(TableReference::bare("t1"), left_provider)
        .unwrap();
    ctx.register_table(TableReference::bare("t2"), right_provider)
        .unwrap();

    let plan = futures::executor::block_on(async {
        ctx.sql(
            "SELECT t1.number \
             FROM t1 JOIN t2 ON t1.number = t2.number \
             WHERE t1.ts < date_trunc('second', current_timestamp())",
        )
        .await
        .unwrap()
    })
    .into_unoptimized_plan();
    assert!(
        plan.to_string().contains("current_timestamp") || plan.to_string().contains("now()"),
        "Unoptimized plan should contain the time function spelling before analysis:\n{plan}"
    );

    let mut config = ConfigOptions::default();
    config.extensions.insert(ScheduledTimeExtension {
        scheduled_time: Some(scheduled_time),
    });

    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();
    assert_remote_table_scan_filters_are_safe(&result);

    let result_str = result.to_string();
    common_telemetry::info!("Analyzed current_timestamp filter plan: {}", result_str);
    let remote_section = result_str
        .find("remote_input=[")
        .map(|idx| &result_str[idx..])
        .unwrap_or("");

    assert!(
        remote_section.contains("TableScan: t1") && remote_section.contains("partial_filters="),
        "Expected left-side filter to be pushed into the remote TableScan:\n{result_str}"
    );
    assert!(
        remote_section.contains("1700000000000000000") || remote_section.contains("1700000000000"),
        "Expected scheduled timestamp literal in remote filter:\n{result_str}"
    );
    assert!(
        !remote_section.contains("current_timestamp") && !remote_section.contains("now()"),
        "Remote filter should be folded before pushdown, got:\n{result_str}"
    );
}

/// When `ScheduledTimeExtension.scheduled_time` is `None`, the analyzer
/// must fall back to wall-clock behavior (same as no extension at all).
#[test]
fn scheduled_none_falls_back_to_wall_clock() {
    init_default_ut_logging();

    let test_table = TestTable::table_with_name(0, "t".to_string());
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(test_table),
    )));

    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
        .unwrap()
        .project(vec![now()])
        .unwrap()
        .build()
        .unwrap();

    let mut config = ConfigOptions::default();
    config.extensions.insert(ScheduledTimeExtension {
        scheduled_time: None,
    });

    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();
    let result_str = result.to_string();

    // Must still simplify now() — remotely a literal, top is now().
    assert!(
        result_str.contains("Projection: now()"),
        "Top-level projection: {result_str}"
    );
    let remote_section = if let Some(idx) = result_str.find("remote_input=[") {
        &result_str[idx..]
    } else {
        ""
    };
    // The literal must be TimestampNanosecond (since fallback uses wall clock).
    assert!(
        remote_section.contains("TimestampNanosecond("),
        "Remote should contain TimestampNanosecond:\n{result_str}"
    );
}
