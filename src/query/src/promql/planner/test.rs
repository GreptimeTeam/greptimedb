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

use std::collections::HashMap;
use std::time::{Duration, UNIX_EPOCH};

use catalog::RegisterTableRequest;
use catalog::memory::{MemoryCatalogManager, new_memory_catalog_manager};
use common_base::Plugins;
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_query::native_histogram::{
    CUSTOM_BUCKETS_SCHEMA, CounterResetHint, NativeHistogram, build_histogram_array,
};
use common_query::prelude::{greptime_native_histogram, greptime_timestamp, greptime_value};
use common_query::prometheus::{
    PROMETHEUS_STALE_NAN_BITS, PROMQL_FIELD_ROLE_KEY, PROMQL_METRIC_NAME_ROLE,
};
use common_query::test_util::DummyDecoder;
use common_recordbatch::RecordBatch as GreptimeRecordBatch;
use datafusion::arrow::array::{
    Array, ArrayRef, Float64Array, Int64Array, StringArray, TimestampMillisecondArray, UInt32Array,
    UInt64Array,
};
use datafusion::arrow::datatypes::{Field, Schema as ArrowSchema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::datasource::{MemTable, provider_as_source};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{Extension, UserDefinedLogicalNode, col};
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::{ColumnSchema, Schema};
use promql::extension_plan::HistogramFold;
use promql_parser::label::Labels;
use promql_parser::parser;
use session::context::QueryContext;
use substrait::{DFLogicalSubstraitConvertor, SubstraitPlan};
use table::Table;
use table::metadata::{FilterPushDownType, TableInfoBuilder, TableMetaBuilder};
use table::test_util::{EmptyTable, MemTable as GreptimeMemTable};

use super::*;
use crate::QueryEngineContext;
use crate::options::QueryOptions;
use crate::parser::QueryLanguageParser;
use crate::query_engine::DefaultSerializer;

mod delta;

fn find_instant_manipulate(plan: &LogicalPlan) -> Option<&InstantManipulate> {
    if let LogicalPlan::Extension(Extension { node }) = plan
        && let Some(instant_manipulate) = node.as_any().downcast_ref::<InstantManipulate>()
    {
        return Some(instant_manipulate);
    }

    plan.inputs().into_iter().find_map(find_instant_manipulate)
}

fn find_histogram_fold(plan: &LogicalPlan) -> Option<&HistogramFold> {
    if let LogicalPlan::Extension(Extension { node }) = plan
        && let Some(histogram_fold) = node.as_any().downcast_ref::<HistogramFold>()
    {
        return Some(histogram_fold);
    }

    plan.inputs().into_iter().find_map(find_histogram_fold)
}

/// The name of the single `Float64` sample column of `plan`, ignoring the metric-name marker.
fn float_sample_column(plan: &LogicalPlan) -> String {
    let marker = PromPlanner::metric_name_column(plan.schema())
        .unwrap()
        .map(|marker| marker.name);
    let mut sample_columns = plan
        .schema()
        .fields()
        .iter()
        .filter(|field| {
            field.data_type() == &ArrowDataType::Float64 && marker.as_ref() != Some(field.name())
        })
        .map(|field| field.name().clone());
    let sample_column = sample_columns
        .next()
        .unwrap_or_else(|| panic!("expected a float sample column: {}", plan.display_indent()));
    assert!(
        sample_columns.next().is_none(),
        "expected exactly one float sample column: {}",
        plan.display_indent()
    );
    sample_column
}

/// `(series tag, timestamp in milliseconds, sample value)` of every emitted `cv_metric` sample,
/// sorted by series tag.
fn cv_rows(batches: &[RecordBatch], value_column: &str) -> Vec<(String, i64, f64)> {
    let mut rows = batches
        .iter()
        .flat_map(|batch| {
            let tag = batch
                .column_by_name("k")
                .expect("no series tag column")
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("the series tag must be a string column");
            let timestamp = batch
                .column_by_name("timestamp")
                .expect("no timestamp column")
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .expect("the time index must be a millisecond timestamp column");
            let value = batch
                .column_by_name(value_column)
                .unwrap_or_else(|| {
                    panic!(
                        "no sample value column {value_column} in {}",
                        batch.schema()
                    )
                })
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("the sample value must be a float column");
            (0..batch.num_rows())
                .map(|row| {
                    (
                        tag.value(row).to_string(),
                        timestamp.value(row),
                        value.value(row),
                    )
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| left.0.cmp(&right.0));
    rows
}

/// The dropped metric-name identity must be gone from the physical batches as well.
fn assert_metric_name_not_in_batches(batches: &[RecordBatch]) {
    for batch in batches {
        let schema = batch.schema();
        assert!(
            schema.field_with_name(PROMQL_METRIC_NAME_COLUMN).is_err(),
            "the metric-name marker must not reach the result batches: {schema:?}"
        );
        assert!(
            schema
                .fields()
                .iter()
                .all(|field| field.metadata().get(PROMQL_FIELD_ROLE_KEY).is_none()),
            "no result batch field may carry the metric-name role: {schema:?}"
        );
    }
}

/// The kept metric-name identity must still be visible in the result batches' Arrow metadata.
fn assert_metric_name_in_batches(batches: &[RecordBatch], marker: &str) {
    assert!(!batches.is_empty(), "expected at least one result batch");
    for batch in batches {
        let schema = batch.schema();
        let field = schema
            .field_with_name(marker)
            .unwrap_or_else(|_| panic!("the marker must reach the result batches: {schema:?}"));
        assert_eq!(
            field
                .metadata()
                .get(PROMQL_FIELD_ROLE_KEY)
                .map(String::as_str),
            Some(PROMQL_METRIC_NAME_ROLE),
            "the result batch must mark the metric-name field: {schema:?}"
        );
    }
}

fn build_query_engine_state() -> QueryEngineState {
    QueryEngineState::new(
        new_memory_catalog_manager().unwrap(),
        None,
        None,
        None,
        None,
        None,
        false,
        Plugins::default(),
        QueryOptions::default(),
    )
}

#[test]
fn common_label_type_preserves_only_shared_dictionary_encoding() {
    let dictionary = ArrowDataType::Dictionary(
        Box::new(ArrowDataType::UInt32),
        Box::new(ArrowDataType::Utf8),
    );
    let other_dictionary = ArrowDataType::Dictionary(
        Box::new(ArrowDataType::Int32),
        Box::new(ArrowDataType::Utf8),
    );

    assert_eq!(
        Some(dictionary.clone()),
        PromPlanner::common_label_data_type(Some(&dictionary), Some(&dictionary))
    );
    assert_eq!(
        Some(ArrowDataType::Utf8),
        PromPlanner::common_label_data_type(Some(&dictionary), Some(&ArrowDataType::Utf8))
    );
    assert_eq!(
        Some(ArrowDataType::Utf8),
        PromPlanner::common_label_data_type(Some(&dictionary), Some(&other_dictionary))
    );
    assert_eq!(
        Some(ArrowDataType::Utf8),
        PromPlanner::common_label_data_type(Some(&dictionary), None)
    );
}

async fn build_optimized_promql_plan(
    table_provider: DfTableSourceProvider,
    eval_stmt: &EvalStmt,
) -> LogicalPlan {
    let state = build_query_engine_state();
    let raw_plan = PromPlanner::stmt_to_plan(table_provider, eval_stmt, &state)
        .await
        .unwrap();
    let context = QueryEngineContext::new(state.session_state(), QueryContext::arc());
    state
        .optimize_by_extension_rules(raw_plan, &context)
        .unwrap()
}

async fn build_optimized_tsid_plan(
    query: &str,
    num_tag: usize,
    num_field: usize,
    end_secs: u64,
    lookback_secs: u64,
) -> String {
    let eval_stmt = EvalStmt {
        expr: parser::parse(query).unwrap(),
        start: UNIX_EPOCH,
        end: UNIX_EPOCH
            .checked_add(Duration::from_secs(end_secs))
            .unwrap(),
        interval: Duration::from_secs(5),
        lookback_delta: Duration::from_secs(lookback_secs),
    };
    let table_provider = build_test_table_provider_with_tsid(
        &[(DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string())],
        num_tag,
        num_field,
    )
    .await;

    build_optimized_promql_plan(table_provider, &eval_stmt)
        .await
        .display_indent_schema()
        .to_string()
}

async fn assert_nested_count_rewrite_applies(query: &str, expected_outer_agg: &str) {
    let plan_str = build_optimized_tsid_plan(query, 2, 1, 100_000, 1).await;

    assert!(plan_str.contains("PromSeriesDivide: tags=[\"__tsid\"]"));
    assert!(plan_str.contains("Projection: some_metric.timestamp, some_metric.tag_0"));
    assert!(plan_str.contains("Distinct:"));
    assert!(plan_str.contains(expected_outer_agg), "{plan_str}");
    assert!(!plan_str.contains("PromSeriesDivide: tags=[\"tag_0\"]"));
}

async fn assert_nested_count_rewrite_missing(query: &str, num_tag: usize, lookback_secs: u64) {
    let plan_str = build_optimized_tsid_plan(query, num_tag, 1, 100_000, lookback_secs).await;
    assert!(!plan_str.contains("Distinct:"), "{plan_str}");
}

fn build_eval_stmt(expr: &str) -> EvalStmt {
    EvalStmt {
        expr: parser::parse(expr).unwrap(),
        start: UNIX_EPOCH,
        end: UNIX_EPOCH
            .checked_add(Duration::from_secs(100_000))
            .unwrap(),
        interval: Duration::from_secs(5),
        lookback_delta: Duration::from_secs(1),
    }
}

enum DirectOrValue {
    Float64(f64),
    Int64(i64),
    NativeHistogram(NativeHistogram),
    Utf8(&'static str),
}

impl DirectOrValue {
    fn data_type(&self) -> ArrowDataType {
        match self {
            Self::Float64(_) => ArrowDataType::Float64,
            Self::Int64(_) => ArrowDataType::Int64,
            Self::NativeHistogram(_) => native_histogram_value_type().as_arrow_type(),
            Self::Utf8(_) => ArrowDataType::Utf8,
        }
    }
    fn array(&self) -> Arc<dyn Array> {
        match self {
            Self::Float64(v) => Arc::new(Float64Array::from(vec![*v])),
            Self::Int64(v) => Arc::new(Int64Array::from(vec![*v])),
            Self::NativeHistogram(v) => build_histogram_array(&[Some(v.clone())]),
            Self::Utf8(v) => Arc::new(StringArray::from(vec![*v])),
        }
    }
}

fn direct_or_histogram() -> NativeHistogram {
    NativeHistogram {
        schema: 0,
        zero_threshold: 0.0,
        sum: 1.0,
        reset_hint: CounterResetHint::Unknown,
        start_timestamp: None,
        custom_values: vec![],
        positive_spans: vec![],
        negative_spans: vec![],
        count: 1.0,
        zero_count: 1.0,
        positive_buckets: vec![],
        negative_buckets: vec![],
    }
}

fn operator_metric_table(
    name: &str,
    table_id: u32,
    tag: &str,
    le: Option<&str>,
    value: DirectOrValue,
) -> table::TableRef {
    let value_type = match &value {
        DirectOrValue::Float64(_) => ConcreteDataType::float64_datatype(),
        DirectOrValue::Int64(_) => ConcreteDataType::int64_datatype(),
        DirectOrValue::NativeHistogram(_) => native_histogram_value_type().clone(),
        DirectOrValue::Utf8(_) => ConcreteDataType::string_datatype(),
    };
    let tag_count = 1 + usize::from(le.is_some());
    let mut columns = vec![ColumnSchema::new(
        "tag".to_string(),
        ConcreteDataType::string_datatype(),
        false,
    )];
    if le.is_some() {
        columns.push(ColumnSchema::new(
            LE_COLUMN_NAME.to_string(),
            ConcreteDataType::string_datatype(),
            false,
        ));
    }
    columns.extend([
        ColumnSchema::new(
            "ts".to_string(),
            ConcreteDataType::timestamp_millisecond_datatype(),
            false,
        )
        .with_time_index(true),
        ColumnSchema::new("v".to_string(), value_type, true),
    ]);
    let schema = Arc::new(Schema::new(columns));
    let mut arrays = vec![Arc::new(StringArray::from(vec![tag])) as Arc<dyn Array>];
    if let Some(le) = le {
        arrays.push(Arc::new(StringArray::from(vec![le])));
    }
    arrays.extend([
        Arc::new(TimestampMillisecondArray::from(vec![1_000])) as Arc<dyn Array>,
        value.array(),
    ]);
    let batch = RecordBatch::try_new(schema.arrow_schema().clone(), arrays).unwrap();
    let backing = GreptimeMemTable::new_with_catalog(
        name,
        GreptimeRecordBatch::from_df_record_batch(schema.clone(), batch),
        table_id,
        DEFAULT_CATALOG_NAME.to_string(),
        DEFAULT_SCHEMA_NAME.to_string(),
    );
    let value_index = tag_count + 1;
    let meta = TableMetaBuilder::empty()
        .schema(schema)
        .primary_key_indices((0..tag_count).collect())
        .value_indices(vec![value_index])
        .next_column_id((value_index + 1) as u32)
        .build()
        .unwrap();
    let info = Arc::new(
        TableInfoBuilder::default()
            .table_id(table_id)
            .name(name)
            .meta(meta)
            .build()
            .unwrap(),
    );
    Arc::new(Table::new(
        info,
        FilterPushDownType::Unsupported,
        backing.data_source(),
    ))
}

fn operator_table_provider() -> DfTableSourceProvider {
    let catalog = MemoryCatalogManager::with_default_setup();
    let tables = [
        operator_metric_table("lf", 2_001, "a", None, DirectOrValue::Float64(2.0)),
        operator_metric_table(
            "lh",
            2_002,
            "b",
            None,
            DirectOrValue::NativeHistogram(direct_or_histogram()),
        ),
        operator_metric_table("rf", 2_003, "b", None, DirectOrValue::Float64(3.0)),
        operator_metric_table(
            "rh",
            2_004,
            "a",
            None,
            DirectOrValue::NativeHistogram(direct_or_histogram()),
        ),
        operator_metric_table("fallback", 2_005, "c", None, DirectOrValue::Float64(7.0)),
        operator_metric_table(
            "bad_classic",
            2_006,
            "d",
            Some("broken"),
            DirectOrValue::Float64(1.0),
        ),
        operator_metric_table(
            "bad_native",
            2_007,
            "d",
            None,
            DirectOrValue::NativeHistogram(direct_or_histogram()),
        ),
    ];
    for table in tables {
        let info = table.table_info();
        catalog
            .register_table_sync(RegisterTableRequest {
                catalog: DEFAULT_CATALOG_NAME.to_string(),
                schema: DEFAULT_SCHEMA_NAME.to_string(),
                table_name: info.name.clone(),
                table_id: info.ident.table_id,
                table,
            })
            .unwrap();
    }
    DfTableSourceProvider::new(
        catalog,
        false,
        QueryContext::arc(),
        DummyDecoder::arc(),
        false,
    )
}

fn operator_eval_stmt(expr: &str) -> EvalStmt {
    let time = UNIX_EPOCH.checked_add(Duration::from_secs(1)).unwrap();
    EvalStmt {
        expr: parser::parse(expr).unwrap(),
        start: time,
        end: time,
        interval: Duration::from_secs(1),
        lookback_delta: Duration::from_secs(5),
    }
}

struct DirectOrSource {
    name: &'static str,
    empty: bool,
    timestamp: i64,
    tags: Vec<(&'static str, Option<&'static str>)>,
    value: DirectOrValue,
}

fn source(
    name: &'static str,
    empty: bool,
    timestamp: i64,
    tags: Vec<(&'static str, Option<&'static str>)>,
    value: DirectOrValue,
) -> DirectOrSource {
    DirectOrSource {
        name,
        empty,
        timestamp,
        tags,
        value,
    }
}

fn tagged_source(
    name: &'static str,
    empty: bool,
    tag: (&'static str, Option<&'static str>),
    value: DirectOrValue,
) -> DirectOrSource {
    source(name, empty, 1, vec![("job", Some("job")), tag], value)
}

fn job_source(name: &'static str, value: DirectOrValue) -> DirectOrSource {
    source(name, true, 1, vec![("job", Some("job"))], value)
}

fn table(source: &DirectOrSource) -> Arc<MemTable> {
    let mut fields = vec![Field::new(
        "ts",
        ArrowDataType::Timestamp(ArrowTimeUnit::Millisecond, None),
        false,
    )];
    fields.extend(
        source
            .tags
            .iter()
            .map(|(name, _)| Field::new(*name, ArrowDataType::Utf8, true)),
    );
    fields.push(Field::new("v", source.value.data_type(), true));
    let schema = Arc::new(ArrowSchema::new(fields));
    let partitions = if source.empty {
        vec![vec![]]
    } else {
        let mut columns: Vec<Arc<dyn Array>> =
            vec![Arc::new(TimestampMillisecondArray::from(vec![
                source.timestamp,
            ]))];
        columns.extend(
            source
                .tags
                .iter()
                .map(|(_, value)| Arc::new(StringArray::from(vec![*value])) as Arc<dyn Array>),
        );
        columns.push(source.value.array());
        vec![vec![RecordBatch::try_new(schema.clone(), columns).unwrap()]]
    };
    Arc::new(MemTable::try_new(schema, partitions).unwrap())
}

fn scan(source: &DirectOrSource) -> LogicalPlan {
    LogicalPlanBuilder::scan(source.name, provider_as_source(table(source)), None)
        .unwrap()
        .build()
        .unwrap()
}

fn direct_or_context(qualifier: &str, tags: &[&str], field: &str) -> PromPlannerContext {
    PromPlannerContext {
        table_name: Some(qualifier.to_string()),
        time_index_column: Some("ts".to_string()),
        field_columns: vec![field.to_string()],
        tag_columns: tags.iter().map(|tag| (*tag).to_string()).collect(),
        ..Default::default()
    }
}

fn or_modifier(expr: &str) -> Option<BinModifier> {
    let PromExpr::Binary(expr) = parser::parse(expr).unwrap() else {
        unreachable!()
    };
    expr.modifier
}

async fn plan_direct_or(
    left: LogicalPlan,
    right: LogicalPlan,
    left_context: PromPlannerContext,
    right_context: PromPlannerContext,
    modifier: &Option<BinModifier>,
) -> LogicalPlan {
    let table_provider = build_test_table_provider_with_fields(
        &[(DEFAULT_SCHEMA_NAME.to_string(), "dummy".to_string())],
        &[],
    )
    .await;
    let mut planner = PromPlanner {
        table_provider,
        ctx: PromPlannerContext::default(),
        promql_annotations: None,
    };
    planner
        .or_operator(
            left,
            right,
            left_context.tag_columns.iter().cloned().collect(),
            right_context.tag_columns.iter().cloned().collect(),
            left_context,
            right_context,
            modifier,
        )
        .unwrap()
}

async fn execute(plan: LogicalPlan, state: &QueryEngineState) -> (LogicalPlan, Vec<RecordBatch>) {
    let context = QueryEngineContext::new(state.session_state(), QueryContext::arc());
    let optimized = state.optimize_by_extension_rules(plan, &context).unwrap();
    let physical = state
        .session_state()
        .create_physical_plan(&optimized)
        .await
        .unwrap();
    let batches = datafusion::physical_plan::collect(physical, state.session_state().task_ctx())
        .await
        .unwrap();
    (optimized, batches)
}

async fn run(
    left: &DirectOrSource,
    right: &DirectOrSource,
    left_context: PromPlannerContext,
    right_context: PromPlannerContext,
    modifier: &Option<BinModifier>,
) -> (LogicalPlan, Vec<RecordBatch>) {
    let plan = plan_direct_or(
        scan(left),
        scan(right),
        left_context,
        right_context,
        modifier,
    )
    .await;
    execute(plan, &build_query_engine_state()).await
}

async fn mixed_direct_or(histogram_on_left: bool) -> (PromPlanner, LogicalPlan) {
    let sample = |histogram: bool| {
        if histogram {
            DirectOrValue::NativeHistogram(direct_or_histogram())
        } else {
            DirectOrValue::Float64(1.25)
        }
    };
    let left = tagged_source(
        "lhs",
        false,
        (
            "k",
            Some(if histogram_on_left {
                "histogram"
            } else {
                "float"
            }),
        ),
        sample(histogram_on_left),
    );
    let right = tagged_source(
        "rhs",
        false,
        (
            "k",
            Some(if histogram_on_left {
                "float"
            } else {
                "histogram"
            }),
        ),
        sample(!histogram_on_left),
    );
    let table_provider = build_test_table_provider_with_fields(
        &[(DEFAULT_SCHEMA_NAME.to_string(), "dummy".to_string())],
        &[],
    )
    .await;
    let mut planner = PromPlanner {
        table_provider,
        ctx: PromPlannerContext::default(),
        promql_annotations: None,
    };
    let left_context = direct_or_context("lhs", &["job", "k"], "v");
    let right_context = direct_or_context("rhs", &["job", "k"], "v");
    let plan = planner
        .or_operator(
            scan(&left),
            scan(&right),
            left_context.tag_columns.iter().cloned().collect(),
            right_context.tag_columns.iter().cloned().collect(),
            left_context,
            right_context,
            &or_modifier("lhs or on(k) rhs"),
        )
        .unwrap();
    (planner, plan)
}

async fn mixed_aggregate_input(histograms: Vec<NativeHistogram>) -> (PromPlanner, LogicalPlan) {
    let float_field = format!("{OR_FLOAT_FIELD_PREFIX}0");
    let histogram_field = format!("{OR_HISTOGRAM_FIELD_PREFIX}0");
    let row_count = histograms.len() + 1;
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new(
            "ts",
            ArrowDataType::Timestamp(ArrowTimeUnit::Millisecond, None),
            false,
        ),
        Field::new("k", ArrowDataType::Utf8, false),
        Field::new(&float_field, ArrowDataType::Float64, true),
        Field::new(
            &histogram_field,
            native_histogram_value_type().as_arrow_type(),
            true,
        ),
    ]));
    let mut histogram_values = Vec::with_capacity(row_count);
    histogram_values.push(None);
    histogram_values.extend(histograms.into_iter().map(Some));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(TimestampMillisecondArray::from(vec![1; row_count])),
            Arc::new(StringArray::from_iter_values(
                (0..row_count).map(|row| format!("kind_{row}")),
            )),
            Arc::new(Float64Array::from_iter(
                (0..row_count).map(|row| (row == 0).then_some(1.25)),
            )),
            build_histogram_array(&histogram_values),
        ],
    )
    .unwrap();
    let table = Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap());
    let plan = LogicalPlanBuilder::scan("mixed", provider_as_source(table), None)
        .unwrap()
        .build()
        .unwrap();
    let table_provider = build_test_table_provider_with_fields(
        &[(DEFAULT_SCHEMA_NAME.to_string(), "dummy".to_string())],
        &[],
    )
    .await;
    let planner = PromPlanner {
        table_provider,
        ctx: PromPlannerContext {
            table_name: Some("mixed".to_string()),
            time_index_column: Some("ts".to_string()),
            field_columns: vec![float_field, histogram_field],
            tag_columns: vec!["k".to_string()],
            ..Default::default()
        },
        promql_annotations: None,
    };
    (planner, plan)
}

fn assert_no_internal_or_keys(schema: &DFSchema) {
    assert!(
        schema
            .fields()
            .iter()
            .all(|field| !field.name().starts_with("__promql_or_match_")),
        "{schema:?}"
    );
}

fn values(batches: &[RecordBatch], column: &str) -> Vec<f64> {
    batches
        .iter()
        .flat_map(|batch| {
            batch
                .column_by_name(column)
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .iter()
                .flatten()
        })
        .collect()
}

fn numeric_values(batches: &[RecordBatch], column: &str) -> Vec<f64> {
    batches
        .iter()
        .flat_map(|batch| {
            let values = datafusion::arrow::compute::cast(
                batch.column_by_name(column).unwrap(),
                &ArrowDataType::Float64,
            )
            .unwrap();
            values
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .iter()
                .flatten()
                .collect::<Vec<_>>()
        })
        .collect()
}

fn histograms(batches: &[RecordBatch], column: &str) -> Vec<NativeHistogram> {
    batches
        .iter()
        .flat_map(|batch| {
            let values = batch
                .column_by_name(column)
                .unwrap()
                .as_any()
                .downcast_ref::<datafusion::arrow::array::StructArray>()
                .unwrap();
            (0..values.len()).filter_map(|row| {
                common_query::native_histogram::read_histogram(values, row).unwrap()
            })
        })
        .collect()
}

fn rows(batches: &[RecordBatch]) -> Vec<(f64, Option<String>)> {
    let mut rows = batches
        .iter()
        .flat_map(|batch| {
            let values = batch
                .column_by_name("v")
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            let labels = batch
                .column_by_name("k")
                .map(|column| column.as_any().downcast_ref::<StringArray>().unwrap());
            (0..batch.num_rows()).map(move |i| {
                (
                    values.value(i),
                    labels.and_then(|labels| {
                        (!labels.is_null(i)).then(|| labels.value(i).to_string())
                    }),
                )
            })
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| left.0.total_cmp(&right.0));
    rows
}

fn matrix_source(
    name: &'static str,
    k: Option<Option<&'static str>>,
    timestamp: i64,
    value: f64,
) -> DirectOrSource {
    let mut tags = vec![("job", Some("job"))];
    if let Some(k) = k {
        tags.push(("k", k));
    }
    source(name, false, timestamp, tags, DirectOrValue::Float64(value))
}

fn matrix_context(name: &str, k: Option<Option<&str>>) -> PromPlannerContext {
    direct_or_context(
        name,
        if k.is_some() { &["job", "k"] } else { &["job"] },
        "v",
    )
}

async fn build_missing_le_or_normal_metric_table_provider() -> DfTableSourceProvider {
    build_test_table_provider_with_fields(
        &[
            (
                DEFAULT_SCHEMA_NAME.to_string(),
                "non_existent_histogram_bucket".to_string(),
            ),
            (DEFAULT_SCHEMA_NAME.to_string(), "normal_metric".to_string()),
        ],
        &["pod", "instance"],
    )
    .await
}

/// Whether a schema field is the metadata-marked PromQL metric-name identity rather than an
/// ordinary label or sample column.
fn is_marked_metric_name_field(field: &Field) -> bool {
    field
        .metadata()
        .get(PROMQL_FIELD_ROLE_KEY)
        .map(String::as_str)
        == Some(PROMQL_METRIC_NAME_ROLE)
}

/// A raw PromQL plan carries the metadata-marked metric-name identity next to exactly four
/// ordinary fields: the `pod`/`instance` labels, the time index and the sample.
fn assert_normal_metric_schema(plan: &LogicalPlan) {
    let (marked, ordinary): (Vec<_>, Vec<_>) = plan
        .schema()
        .fields()
        .iter()
        .partition(|field| is_marked_metric_name_field(field));
    assert_eq!(marked.len(), 1, "{marked:?}");
    assert_eq!(
        marked[0].name().as_str(),
        PROMQL_METRIC_NAME_COLUMN,
        "{marked:?}"
    );
    assert_eq!(marked[0].data_type(), &ArrowDataType::Utf8, "{marked:?}");
    assert_eq!(ordinary.len(), 4, "{ordinary:?}");
    assert!(
        ordinary.iter().any(|field| field.name() == "pod"),
        "{ordinary:?}"
    );
    assert!(
        ordinary.iter().any(|field| field.name() == "instance"),
        "{ordinary:?}"
    );
    assert!(
        ordinary
            .iter()
            .any(|field| field.name() == greptime_timestamp()),
        "{ordinary:?}"
    );
    assert!(
        ordinary.iter().any(|field| {
            field.name() == greptime_value() && field.data_type() == &ArrowDataType::Float64
        }),
        "{ordinary:?}"
    );
}

async fn build_test_table_provider_with_distinct_tags(
    table_tags: &[(&str, &[&str])],
) -> DfTableSourceProvider {
    let catalog_list = MemoryCatalogManager::with_default_setup();
    for (table_name, tags) in table_tags {
        let mut columns = tags
            .iter()
            .map(|tag| {
                ColumnSchema::new(
                    (*tag).to_string(),
                    ConcreteDataType::string_datatype(),
                    false,
                )
            })
            .collect::<Vec<_>>();
        columns.push(
            ColumnSchema::new(
                greptime_timestamp().to_string(),
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
        );
        columns.push(ColumnSchema::new(
            greptime_value().to_string(),
            ConcreteDataType::float64_datatype(),
            true,
        ));
        let table_meta = TableMetaBuilder::empty()
            .schema(Arc::new(Schema::new(columns)))
            .primary_key_indices((0..tags.len()).collect())
            .next_column_id(1024)
            .build()
            .unwrap();
        let table_info = TableInfoBuilder::default()
            .name((*table_name).to_string())
            .meta(table_meta)
            .build()
            .unwrap();

        assert!(
            catalog_list
                .register_table_sync(RegisterTableRequest {
                    catalog: DEFAULT_CATALOG_NAME.to_string(),
                    schema: DEFAULT_SCHEMA_NAME.to_string(),
                    table_name: (*table_name).to_string(),
                    table_id: 1024,
                    table: EmptyTable::from_table_info(&table_info),
                })
                .is_ok()
        );
    }

    DfTableSourceProvider::new(
        catalog_list,
        false,
        QueryContext::arc(),
        DummyDecoder::arc(),
        false,
    )
}

fn contains_histogram_fold(plan: &LogicalPlan) -> bool {
    matches!(plan, LogicalPlan::Extension(Extension { node }) if node.as_any().is::<HistogramFold>())
        || plan.inputs().into_iter().any(contains_histogram_fold)
}

async fn build_set_op_context_table_provider() -> DfTableSourceProvider {
    build_test_table_provider_with_distinct_tags(&[
        ("bucket_metric", &["job", "le"]),
        ("normal_metric", &["job"]),
        ("fallback_metric", &["instance"]),
    ])
    .await
}

async fn build_or_context_table_provider() -> DfTableSourceProvider {
    build_test_table_provider_with_distinct_tags(&[
        ("normal_metric", &["job"]),
        ("other_metric", &["instance"]),
        ("non_hist_metric", &["instance"]),
    ])
    .await
}

async fn optimize_and_create_physical_plan(
    state: &QueryEngineState,
    plan: LogicalPlan,
) -> (
    LogicalPlan,
    Arc<dyn datafusion::physical_plan::ExecutionPlan>,
) {
    let context = QueryEngineContext::new(state.session_state(), QueryContext::arc());
    let optimized = state.optimize_by_extension_rules(plan, &context).unwrap();
    let physical = state
        .session_state()
        .create_physical_plan(&optimized)
        .await
        .unwrap();
    (optimized, physical)
}

async fn build_test_table_provider(
    table_name_tuples: &[(String, String)],
    num_tag: usize,
    num_field: usize,
) -> DfTableSourceProvider {
    let catalog_list = MemoryCatalogManager::with_default_setup();
    for (schema_name, table_name) in table_name_tuples {
        let mut columns = vec![];
        for i in 0..num_tag {
            columns.push(ColumnSchema::new(
                format!("tag_{i}"),
                ConcreteDataType::string_datatype(),
                false,
            ));
        }
        columns.push(
            ColumnSchema::new(
                "timestamp".to_string(),
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
        );
        for i in 0..num_field {
            columns.push(ColumnSchema::new(
                format!("field_{i}"),
                ConcreteDataType::float64_datatype(),
                true,
            ));
        }
        let schema = Arc::new(Schema::new(columns));
        let table_meta = TableMetaBuilder::empty()
            .schema(schema)
            .primary_key_indices((0..num_tag).collect())
            .value_indices((num_tag + 1..num_tag + 1 + num_field).collect())
            .next_column_id(1024)
            .build()
            .unwrap();
        let table_info = TableInfoBuilder::default()
            .name(table_name.clone())
            .meta(table_meta)
            .build()
            .unwrap();
        let table = EmptyTable::from_table_info(&table_info);

        assert!(
            catalog_list
                .register_table_sync(RegisterTableRequest {
                    catalog: DEFAULT_CATALOG_NAME.to_string(),
                    schema: schema_name.clone(),
                    table_name: table_name.clone(),
                    table_id: 1024,
                    table,
                })
                .is_ok()
        );
    }

    DfTableSourceProvider::new(
        catalog_list,
        false,
        QueryContext::arc(),
        DummyDecoder::arc(),
        false,
    )
}

async fn build_test_native_histogram_table_provider(table_name: &str) -> DfTableSourceProvider {
    build_test_native_histogram_table_provider_with_marker(table_name, false).await
}

async fn build_test_native_histogram_table_provider_with_marker(
    table_name: &str,
    temporality_marker: bool,
) -> DfTableSourceProvider {
    let catalog_list = MemoryCatalogManager::with_default_setup();
    let mut columns = vec![
        ColumnSchema::new(
            "tag_0".to_string(),
            ConcreteDataType::string_datatype(),
            false,
        ),
        ColumnSchema::new(
            LE_COLUMN_NAME.to_string(),
            ConcreteDataType::string_datatype(),
            true,
        ),
    ];
    if temporality_marker {
        columns.push(ColumnSchema::new(
            OTLP_AGGREGATION_TEMPORALITY_LABEL.to_string(),
            ConcreteDataType::string_datatype(),
            true,
        ));
    }
    let tag_count = columns.len();
    columns.extend([
        ColumnSchema::new(
            "timestamp".to_string(),
            ConcreteDataType::timestamp_millisecond_datatype(),
            false,
        )
        .with_time_index(true),
        ColumnSchema::new(
            greptime_native_histogram().to_string(),
            native_histogram_value_type().clone(),
            true,
        ),
    ]);
    let schema = Arc::new(Schema::new(columns));
    let table_meta = TableMetaBuilder::empty()
        .schema(schema)
        .primary_key_indices((0..tag_count).collect())
        .value_indices(vec![tag_count + 1])
        .next_column_id(1024)
        .build()
        .unwrap();
    let table_info = TableInfoBuilder::default()
        .name(table_name)
        .meta(table_meta)
        .build()
        .unwrap();
    let table = EmptyTable::from_table_info(&table_info);

    assert!(
        catalog_list
            .register_table_sync(RegisterTableRequest {
                catalog: DEFAULT_CATALOG_NAME.to_string(),
                schema: DEFAULT_SCHEMA_NAME.to_string(),
                table_name: table_name.to_string(),
                table_id: 1024,
                table,
            })
            .is_ok()
    );

    DfTableSourceProvider::new(
        catalog_list,
        false,
        QueryContext::arc(),
        DummyDecoder::arc(),
        false,
    )
}

async fn build_test_multi_histogram_table_provider(table_name: &str) -> DfTableSourceProvider {
    let catalog_list = MemoryCatalogManager::with_default_setup();
    let columns = vec![
        ColumnSchema::new(
            "tag_0".to_string(),
            ConcreteDataType::string_datatype(),
            false,
        ),
        ColumnSchema::new(
            "timestamp".to_string(),
            ConcreteDataType::timestamp_millisecond_datatype(),
            false,
        )
        .with_time_index(true),
        ColumnSchema::new(
            greptime_native_histogram().to_string(),
            native_histogram_value_type().clone(),
            true,
        ),
        ColumnSchema::new(
            "native_histogram_2".to_string(),
            native_histogram_value_type().clone(),
            true,
        ),
    ];
    let schema = Arc::new(Schema::new(columns));
    let table_meta = TableMetaBuilder::empty()
        .schema(schema)
        .primary_key_indices(vec![0])
        .value_indices(vec![2, 3])
        .next_column_id(1024)
        .build()
        .unwrap();
    let table_info = TableInfoBuilder::default()
        .name(table_name)
        .meta(table_meta)
        .build()
        .unwrap();
    let table = EmptyTable::from_table_info(&table_info);

    assert!(
        catalog_list
            .register_table_sync(RegisterTableRequest {
                catalog: DEFAULT_CATALOG_NAME.to_string(),
                schema: DEFAULT_SCHEMA_NAME.to_string(),
                table_name: table_name.to_string(),
                table_id: 1024,
                table,
            })
            .is_ok()
    );

    DfTableSourceProvider::new(
        catalog_list,
        false,
        QueryContext::arc(),
        DummyDecoder::arc(),
        false,
    )
}

async fn build_test_mixed_native_histogram_table_provider(
    table_name: &str,
) -> DfTableSourceProvider {
    build_test_mixed_native_histogram_table_provider_with_marker(table_name, false).await
}

async fn build_test_mixed_native_histogram_table_provider_with_marker(
    table_name: &str,
    temporality_marker: bool,
) -> DfTableSourceProvider {
    let catalog_list = MemoryCatalogManager::with_default_setup();
    let mut columns = vec![ColumnSchema::new(
        "tag_0".to_string(),
        ConcreteDataType::string_datatype(),
        false,
    )];
    if temporality_marker {
        columns.push(ColumnSchema::new(
            OTLP_AGGREGATION_TEMPORALITY_LABEL.to_string(),
            ConcreteDataType::string_datatype(),
            true,
        ));
    }
    let tag_count = columns.len();
    columns.extend([
        ColumnSchema::new(
            "timestamp".to_string(),
            ConcreteDataType::timestamp_millisecond_datatype(),
            false,
        )
        .with_time_index(true),
        ColumnSchema::new(
            greptime_native_histogram().to_string(),
            native_histogram_value_type().clone(),
            true,
        ),
        ColumnSchema::new(
            greptime_value().to_string(),
            ConcreteDataType::float64_datatype(),
            true,
        ),
    ]);
    let schema = Arc::new(Schema::new(columns));
    let table_meta = TableMetaBuilder::empty()
        .schema(schema.clone())
        .primary_key_indices((0..tag_count).collect())
        .value_indices(vec![tag_count + 1, tag_count + 2])
        .next_column_id(1024)
        .build()
        .unwrap();
    let table_info = Arc::new(
        TableInfoBuilder::default()
            .name(table_name)
            .meta(table_meta)
            .build()
            .unwrap(),
    );
    let mut arrays: Vec<Arc<dyn Array>> =
        vec![Arc::new(StringArray::from(vec!["float", "histogram"]))];
    if temporality_marker {
        arrays.push(Arc::new(StringArray::from(vec![
            Some(GREPTIME_TEMPORALITY_DELTA),
            Some(GREPTIME_TEMPORALITY_DELTA),
        ])));
    }
    arrays.extend([
        Arc::new(TimestampMillisecondArray::from(vec![1_000, 1_000])) as Arc<dyn Array>,
        build_histogram_array(&[None, Some(direct_or_histogram())]),
        Arc::new(Float64Array::from(vec![Some(2.0), None])),
    ]);
    let batch = RecordBatch::try_new(schema.arrow_schema().clone(), arrays).unwrap();
    let backing = GreptimeMemTable::new_with_catalog(
        table_name,
        GreptimeRecordBatch::from_df_record_batch(schema, batch),
        1024,
        DEFAULT_CATALOG_NAME.to_string(),
        DEFAULT_SCHEMA_NAME.to_string(),
    );
    let table = Arc::new(Table::new(
        table_info,
        FilterPushDownType::Unsupported,
        backing.data_source(),
    ));

    assert!(
        catalog_list
            .register_table_sync(RegisterTableRequest {
                catalog: DEFAULT_CATALOG_NAME.to_string(),
                schema: DEFAULT_SCHEMA_NAME.to_string(),
                table_name: table_name.to_string(),
                table_id: 1024,
                table,
            })
            .is_ok()
    );

    DfTableSourceProvider::new(
        catalog_list,
        false,
        QueryContext::arc(),
        DummyDecoder::arc(),
        false,
    )
}

fn classic_and_native_histogram_table_provider(
    native_tag: &str,
    native_le: Option<&str>,
    native_histogram: NativeHistogram,
) -> DfTableSourceProvider {
    let table_name = "mixed_histogram";
    let catalog = MemoryCatalogManager::with_default_setup();
    let schema = Arc::new(Schema::new(vec![
        // A dotted tag name guards the mixed histogram_quantile projection
        // against qualified-name parsing (#9390).
        ColumnSchema::new(
            "service.name".to_string(),
            ConcreteDataType::string_datatype(),
            false,
        ),
        ColumnSchema::new(
            LE_COLUMN_NAME.to_string(),
            ConcreteDataType::string_datatype(),
            true,
        ),
        ColumnSchema::new(
            "timestamp".to_string(),
            ConcreteDataType::timestamp_millisecond_datatype(),
            false,
        )
        .with_time_index(true),
        ColumnSchema::new(
            greptime_native_histogram().to_string(),
            native_histogram_value_type().clone(),
            true,
        ),
        ColumnSchema::new(
            greptime_value().to_string(),
            ConcreteDataType::float64_datatype(),
            true,
        ),
    ]));
    let table_meta = TableMetaBuilder::empty()
        .schema(schema.clone())
        .primary_key_indices(vec![0, 1])
        .value_indices(vec![3, 4])
        .next_column_id(5)
        .build()
        .unwrap();
    let table_info = Arc::new(
        TableInfoBuilder::default()
            .name(table_name)
            .meta(table_meta)
            .build()
            .unwrap(),
    );
    let batch = RecordBatch::try_new(
        schema.arrow_schema().clone(),
        vec![
            Arc::new(StringArray::from(vec![
                "classic", "classic", native_tag, "classic", "classic", native_tag,
            ])),
            Arc::new(StringArray::from(vec![
                Some("1"),
                Some("+Inf"),
                native_le,
                Some("1"),
                Some("+Inf"),
                native_le,
            ])),
            Arc::new(TimestampMillisecondArray::from(vec![
                1_000, 1_000, 1_000, 2_000, 2_000, 2_000,
            ])),
            build_histogram_array(&[
                None,
                None,
                Some(native_histogram.clone()),
                None,
                None,
                Some(native_histogram),
            ]),
            Arc::new(Float64Array::from(vec![
                Some(2.0),
                Some(4.0),
                None,
                Some(2.0),
                Some(4.0),
                None,
            ])),
        ],
    )
    .unwrap();
    let backing = GreptimeMemTable::new_with_catalog(
        table_name,
        GreptimeRecordBatch::from_df_record_batch(schema, batch),
        2_200,
        DEFAULT_CATALOG_NAME.to_string(),
        DEFAULT_SCHEMA_NAME.to_string(),
    );
    let table = Arc::new(Table::new(
        table_info,
        FilterPushDownType::Unsupported,
        backing.data_source(),
    ));
    catalog
        .register_table_sync(RegisterTableRequest {
            catalog: DEFAULT_CATALOG_NAME.to_string(),
            schema: DEFAULT_SCHEMA_NAME.to_string(),
            table_name: table_name.to_string(),
            table_id: 2_200,
            table,
        })
        .unwrap();

    DfTableSourceProvider::new(
        catalog,
        false,
        QueryContext::arc(),
        DummyDecoder::arc(),
        false,
    )
}

async fn build_test_table_provider_with_tsid(
    table_name_tuples: &[(String, String)],
    num_tag: usize,
    num_field: usize,
) -> DfTableSourceProvider {
    let table_specs = table_name_tuples
        .iter()
        .map(|(schema_name, table_name)| ((schema_name.clone(), table_name.clone()), num_field))
        .collect::<Vec<_>>();
    build_test_table_provider_with_tsid_fields(&table_specs, num_tag).await
}

async fn build_test_table_provider_with_tsid_fields(
    table_specs: &[((String, String), usize)],
    num_tag: usize,
) -> DfTableSourceProvider {
    let table_specs = table_specs
        .iter()
        .map(|(table_name_tuple, num_field)| (table_name_tuple.clone(), num_tag, *num_field))
        .collect::<Vec<_>>();
    build_test_table_provider_with_tsid_tag_fields(&table_specs).await
}

async fn build_test_table_provider_with_tsid_tag_fields(
    table_specs: &[((String, String), usize, usize)],
) -> DfTableSourceProvider {
    let catalog_list = MemoryCatalogManager::with_default_setup();

    let physical_table_name = "phy";
    let physical_table_id = 999u32;
    let physical_num_tag = table_specs
        .iter()
        .map(|(_, num_tag, _)| *num_tag)
        .max()
        .unwrap_or(0);
    let physical_num_field = table_specs
        .iter()
        .map(|(_, _, num_field)| *num_field)
        .max()
        .unwrap_or(0);

    // Register a metric engine physical table with internal columns.
    {
        let mut columns = vec![
            ColumnSchema::new(
                DATA_SCHEMA_TABLE_ID_COLUMN_NAME.to_string(),
                ConcreteDataType::uint32_datatype(),
                false,
            ),
            ColumnSchema::new(
                DATA_SCHEMA_TSID_COLUMN_NAME.to_string(),
                ConcreteDataType::uint64_datatype(),
                false,
            ),
        ];
        for i in 0..physical_num_tag {
            columns.push(ColumnSchema::new(
                format!("tag_{i}"),
                ConcreteDataType::string_datatype(),
                false,
            ));
        }
        columns.push(
            ColumnSchema::new(
                "timestamp".to_string(),
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
        );
        for i in 0..physical_num_field {
            columns.push(ColumnSchema::new(
                format!("field_{i}"),
                ConcreteDataType::float64_datatype(),
                true,
            ));
        }

        let schema = Arc::new(Schema::new(columns));
        let primary_key_indices = (0..(2 + physical_num_tag)).collect::<Vec<_>>();
        let table_meta = TableMetaBuilder::empty()
            .schema(schema)
            .primary_key_indices(primary_key_indices)
            .value_indices(
                (2 + physical_num_tag..2 + physical_num_tag + 1 + physical_num_field).collect(),
            )
            .engine(METRIC_ENGINE_NAME.to_string())
            .next_column_id(1024)
            .build()
            .unwrap();
        let table_info = TableInfoBuilder::default()
            .table_id(physical_table_id)
            .name(physical_table_name)
            .meta(table_meta)
            .build()
            .unwrap();
        let table = EmptyTable::from_table_info(&table_info);

        assert!(
            catalog_list
                .register_table_sync(RegisterTableRequest {
                    catalog: DEFAULT_CATALOG_NAME.to_string(),
                    schema: DEFAULT_SCHEMA_NAME.to_string(),
                    table_name: physical_table_name.to_string(),
                    table_id: physical_table_id,
                    table,
                })
                .is_ok()
        );
    }

    // Register metric engine logical tables without `__tsid`, referencing the physical table.
    for (idx, ((schema_name, table_name), num_tag, num_field)) in table_specs.iter().enumerate() {
        let mut columns = vec![];
        for i in 0..*num_tag {
            columns.push(ColumnSchema::new(
                format!("tag_{i}"),
                ConcreteDataType::string_datatype(),
                false,
            ));
        }
        columns.push(
            ColumnSchema::new(
                "timestamp".to_string(),
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
        );
        for i in 0..*num_field {
            columns.push(ColumnSchema::new(
                format!("field_{i}"),
                ConcreteDataType::float64_datatype(),
                true,
            ));
        }

        let schema = Arc::new(Schema::new(columns));
        let mut options = table::requests::TableOptions::default();
        options.extra_options.insert(
            LOGICAL_TABLE_METADATA_KEY.to_string(),
            physical_table_name.to_string(),
        );
        let table_id = 1024u32 + idx as u32;
        let table_meta = TableMetaBuilder::empty()
            .schema(schema)
            .primary_key_indices((0..*num_tag).collect())
            .value_indices((*num_tag + 1..*num_tag + 1 + *num_field).collect())
            .engine(METRIC_ENGINE_NAME.to_string())
            .options(options)
            .next_column_id(1024)
            .build()
            .unwrap();
        let table_info = TableInfoBuilder::default()
            .table_id(table_id)
            .name(table_name.clone())
            .meta(table_meta)
            .build()
            .unwrap();
        let table = EmptyTable::from_table_info(&table_info);

        assert!(
            catalog_list
                .register_table_sync(RegisterTableRequest {
                    catalog: DEFAULT_CATALOG_NAME.to_string(),
                    schema: schema_name.clone(),
                    table_name: table_name.clone(),
                    table_id,
                    table,
                })
                .is_ok()
        );
    }

    DfTableSourceProvider::new(
        catalog_list,
        false,
        QueryContext::arc(),
        DummyDecoder::arc(),
        false,
    )
}

/// The `__tsid` constants used by [`build_cross_logical_table_tsid_provider`]'s physical rows.
///
/// `query` does not depend on `metric-engine`, so the fixture cannot call
/// `metric_engine::row_modifier::TsidGenerator` (which hashes the sorted present label
/// names and values with `FxHasher`); the values are hand-written instead. What these
/// tests establish is the binary-match/execution behavior for equal TSIDs across logical
/// tables (and the absence of a match for distinct TSIDs), not TSID generation itself.
const CROSS_TABLE_TSID: u64 = 0x0123_4567_89ab_cdef;
const OTHER_TABLE_TSID: u64 = 0xfedc_ba98_7654_3210;

/// Builds a metric-engine-shaped catalogue with real data: a physical table holding one
/// sample for each of the two logical tables `left_metric` (table id 1024, value 2.0) and
/// `right_metric` (table id 1025, value 3.0), both with the same tag `tag_0="a"` and the
/// same timestamp. `left_tsid`/`right_tsid` set the two rows' `__tsid` so a binary match
/// pairs the logical tables only when the TSIDs are equal.
fn build_cross_logical_table_tsid_provider(
    left_tsid: u64,
    right_tsid: u64,
) -> DfTableSourceProvider {
    const PHYSICAL_TABLE_ID: u32 = 999;
    const LEFT_TABLE_ID: u32 = 1024;
    const RIGHT_TABLE_ID: u32 = 1025;

    let catalog = MemoryCatalogManager::with_default_setup();

    // The shared physical table: `__table_id` and `__tsid` are internal columns, and each
    // row carries the values its logical table would have written.
    let physical_name = "phy";
    let columns = vec![
        ColumnSchema::new(
            DATA_SCHEMA_TABLE_ID_COLUMN_NAME.to_string(),
            ConcreteDataType::uint32_datatype(),
            false,
        ),
        ColumnSchema::new(
            DATA_SCHEMA_TSID_COLUMN_NAME.to_string(),
            ConcreteDataType::uint64_datatype(),
            false,
        ),
        ColumnSchema::new(
            "tag_0".to_string(),
            ConcreteDataType::string_datatype(),
            false,
        ),
        ColumnSchema::new(
            "timestamp".to_string(),
            ConcreteDataType::timestamp_millisecond_datatype(),
            false,
        )
        .with_time_index(true),
        ColumnSchema::new(
            "field_0".to_string(),
            ConcreteDataType::float64_datatype(),
            true,
        ),
    ];
    let schema = Arc::new(Schema::new(columns));
    let batch = RecordBatch::try_new(
        schema.arrow_schema().clone(),
        vec![
            Arc::new(UInt32Array::from(vec![LEFT_TABLE_ID, RIGHT_TABLE_ID])) as Arc<dyn Array>,
            Arc::new(UInt64Array::from(vec![left_tsid, right_tsid])),
            Arc::new(StringArray::from(vec!["a", "a"])),
            Arc::new(TimestampMillisecondArray::from(vec![1_000, 1_000])),
            Arc::new(Float64Array::from(vec![2.0, 3.0])),
        ],
    )
    .unwrap();
    let backing = GreptimeMemTable::new_with_catalog(
        physical_name,
        GreptimeRecordBatch::from_df_record_batch(schema.clone(), batch),
        PHYSICAL_TABLE_ID,
        DEFAULT_CATALOG_NAME.to_string(),
        DEFAULT_SCHEMA_NAME.to_string(),
    );
    let table_meta = TableMetaBuilder::empty()
        .schema(schema)
        .primary_key_indices(vec![0, 1, 2])
        .value_indices(vec![3, 4])
        .engine(METRIC_ENGINE_NAME.to_string())
        .next_column_id(1024)
        .build()
        .unwrap();
    let table_info = Arc::new(
        TableInfoBuilder::default()
            .table_id(PHYSICAL_TABLE_ID)
            .name(physical_name)
            .meta(table_meta)
            .build()
            .unwrap(),
    );
    let physical = Arc::new(Table::new(
        table_info,
        FilterPushDownType::Unsupported,
        backing.data_source(),
    ));
    assert!(
        catalog
            .register_table_sync(RegisterTableRequest {
                catalog: DEFAULT_CATALOG_NAME.to_string(),
                schema: DEFAULT_SCHEMA_NAME.to_string(),
                table_name: physical_name.to_string(),
                table_id: PHYSICAL_TABLE_ID,
                table: physical,
            })
            .is_ok()
    );

    // The two logical tables only describe the shared label set; their samples live in
    // the physical table, which the planner scans with a `__table_id` filter.
    for (table_name, table_id) in [
        ("left_metric", LEFT_TABLE_ID),
        ("right_metric", RIGHT_TABLE_ID),
    ] {
        let columns = vec![
            ColumnSchema::new(
                "tag_0".to_string(),
                ConcreteDataType::string_datatype(),
                false,
            ),
            ColumnSchema::new(
                "timestamp".to_string(),
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
            ColumnSchema::new(
                "field_0".to_string(),
                ConcreteDataType::float64_datatype(),
                true,
            ),
        ];
        let schema = Arc::new(Schema::new(columns));
        let mut options = table::requests::TableOptions::default();
        options.extra_options.insert(
            LOGICAL_TABLE_METADATA_KEY.to_string(),
            physical_name.to_string(),
        );
        let table_meta = TableMetaBuilder::empty()
            .schema(schema)
            .primary_key_indices(vec![0])
            .value_indices(vec![2])
            .engine(METRIC_ENGINE_NAME.to_string())
            .options(options)
            .next_column_id(1024)
            .build()
            .unwrap();
        let table_info = TableInfoBuilder::default()
            .table_id(table_id)
            .name(table_name)
            .meta(table_meta)
            .build()
            .unwrap();
        let table = EmptyTable::from_table_info(&table_info);

        assert!(
            catalog
                .register_table_sync(RegisterTableRequest {
                    catalog: DEFAULT_CATALOG_NAME.to_string(),
                    schema: DEFAULT_SCHEMA_NAME.to_string(),
                    table_name: table_name.to_string(),
                    table_id,
                    table,
                })
                .is_ok()
        );
    }

    DfTableSourceProvider::new(
        catalog,
        false,
        QueryContext::arc(),
        DummyDecoder::arc(),
        false,
    )
}

async fn build_test_table_provider_with_fields(
    table_name_tuples: &[(String, String)],
    tags: &[&str],
) -> DfTableSourceProvider {
    let catalog_list = MemoryCatalogManager::with_default_setup();
    for (schema_name, table_name) in table_name_tuples {
        let mut columns = vec![];
        let num_tag = tags.len();
        for tag in tags {
            columns.push(ColumnSchema::new(
                tag.to_string(),
                ConcreteDataType::string_datatype(),
                false,
            ));
        }
        columns.push(
            ColumnSchema::new(
                greptime_timestamp().to_string(),
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
        );
        columns.push(ColumnSchema::new(
            greptime_value().to_string(),
            ConcreteDataType::float64_datatype(),
            true,
        ));
        let schema = Arc::new(Schema::new(columns));
        let table_meta = TableMetaBuilder::empty()
            .schema(schema)
            .primary_key_indices((0..num_tag).collect())
            .next_column_id(1024)
            .build()
            .unwrap();
        let table_info = TableInfoBuilder::default()
            .name(table_name.clone())
            .meta(table_meta)
            .build()
            .unwrap();
        let table = EmptyTable::from_table_info(&table_info);

        assert!(
            catalog_list
                .register_table_sync(RegisterTableRequest {
                    catalog: DEFAULT_CATALOG_NAME.to_string(),
                    schema: schema_name.clone(),
                    table_name: table_name.clone(),
                    table_id: 1024,
                    table,
                })
                .is_ok()
        );
    }

    DfTableSourceProvider::new(
        catalog_list,
        false,
        QueryContext::arc(),
        DummyDecoder::arc(),
        false,
    )
}

// {
//     input: `abs(some_metric{foo!="bar"})`,
//     expected: &Call{
//         Func: MustGetFunction("abs"),
//         Args: Expressions{
//             &VectorSelector{
//                 Name: "some_metric",
//                 LabelMatchers: []*labels.Matcher{
//                     MustLabelMatcher(labels.MatchNotEqual, "foo", "bar"),
//                     MustLabelMatcher(labels.MatchEqual, model.MetricNameLabel, "some_metric"),
//                 },
//             },
//         },
//     },
// },
async fn do_single_instant_function_call(fn_name: &'static str, plan_name: &str) {
    let prom_expr = parser::parse(&format!("{fn_name}(some_metric{{tag_0!=\"bar\"}})")).unwrap();
    let eval_stmt = EvalStmt {
        expr: prom_expr,
        start: UNIX_EPOCH,
        end: UNIX_EPOCH
            .checked_add(Duration::from_secs(100_000))
            .unwrap(),
        interval: Duration::from_secs(5),
        lookback_delta: Duration::from_secs(1),
    };

    let table_provider = build_test_table_provider(
        &[(DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string())],
        1,
        1,
    )
    .await;
    let plan = PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
        .await
        .unwrap();

    let expected = String::from(
        "Projection: some_metric.timestamp, TEMPLATE(field_0), some_metric.tag_0 [timestamp:Timestamp(ms), TEMPLATE(field_0):Float64;N, tag_0:Utf8]\
            \n  Filter: TEMPLATE(field_0) IS NOT NULL [timestamp:Timestamp(ms), TEMPLATE(field_0):Float64;N, tag_0:Utf8, __promql_metric_name:Utf8]\
            \n    Projection: some_metric.timestamp, TEMPLATE(some_metric.field_0) AS TEMPLATE(field_0), some_metric.tag_0, __promql_metric_name [timestamp:Timestamp(ms), TEMPLATE(field_0):Float64;N, tag_0:Utf8, __promql_metric_name:Utf8]\
            \n      Projection: some_metric.tag_0, some_metric.timestamp, some_metric.field_0, Utf8(\"some_metric\") AS __promql_metric_name [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, __promql_metric_name:Utf8]\
            \n        PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n          PromSeriesDivide: tags=[\"tag_0\"] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n            Sort: some_metric.tag_0 ASC NULLS FIRST, some_metric.timestamp ASC NULLS FIRST [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n              Filter: some_metric.tag_0 != Utf8(\"bar\") AND some_metric.timestamp >= TimestampMillisecond(-999, None) AND some_metric.timestamp <= TimestampMillisecond(100000000, None) [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n                TableScan: some_metric [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]"
    ).replace("TEMPLATE", plan_name);

    assert_eq!(plan.display_indent_schema().to_string(), expected);
}

#[tokio::test]
async fn single_abs() {
    do_single_instant_function_call("abs", "abs").await;
}

#[tokio::test]
#[should_panic]
async fn single_absent() {
    do_single_instant_function_call("absent", "").await;
}

#[tokio::test]
async fn single_ceil() {
    do_single_instant_function_call("ceil", "ceil").await;
}

#[tokio::test]
async fn single_exp() {
    do_single_instant_function_call("exp", "exp").await;
}

#[tokio::test]
async fn single_ln() {
    do_single_instant_function_call("ln", "ln").await;
}

#[tokio::test]
async fn single_log2() {
    do_single_instant_function_call("log2", "log2").await;
}

#[tokio::test]
async fn single_log10() {
    do_single_instant_function_call("log10", "log10").await;
}

#[tokio::test]
#[should_panic]
async fn single_scalar() {
    do_single_instant_function_call("scalar", "").await;
}

#[tokio::test]
#[should_panic]
async fn single_sgn() {
    do_single_instant_function_call("sgn", "").await;
}

#[tokio::test]
#[should_panic]
async fn single_sort() {
    do_single_instant_function_call("sort", "").await;
}

#[tokio::test]
#[should_panic]
async fn single_sort_desc() {
    do_single_instant_function_call("sort_desc", "").await;
}

#[tokio::test]
async fn single_sqrt() {
    do_single_instant_function_call("sqrt", "sqrt").await;
}

#[tokio::test]
async fn single_timestamp_plan_preserves_source_value() {
    let eval_stmt = build_eval_stmt(r#"timestamp(some_metric{tag_0!="bar"})"#);
    let table_provider = build_test_table_provider(
        &[(DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string())],
        1,
        1,
    )
    .await;
    let plan = PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
        .await
        .unwrap();

    let expected = String::from(
        "Filter: value IS NOT NULL [timestamp:Timestamp(ms), value:Float64, tag_0:Utf8]\
            \n  Projection: some_metric.timestamp, value AS value, some_metric.tag_0 [timestamp:Timestamp(ms), value:Float64, tag_0:Utf8]\
            \n    Projection: some_metric.timestamp, __promql_timestamp_value_ AS value, some_metric.tag_0 [timestamp:Timestamp(ms), value:Float64, tag_0:Utf8]\
            \n      Filter: some_metric.field_0 IS NOT NULL [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, __promql_timestamp_value_:Float64]\
            \n        PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, __promql_timestamp_value_:Float64]\
            \n          Projection: some_metric.tag_0, some_metric.timestamp, some_metric.field_0, CAST(CAST(some_metric.timestamp AS Int64) AS Float64) / Float64(1000) AS __promql_timestamp_value_ [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, __promql_timestamp_value_:Float64]\
            \n            PromSeriesDivide: tags=[\"tag_0\"] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n              Sort: some_metric.tag_0 ASC NULLS FIRST, some_metric.timestamp ASC NULLS FIRST [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n                Filter: some_metric.tag_0 != Utf8(\"bar\") AND some_metric.timestamp >= TimestampMillisecond(-999, None) AND some_metric.timestamp <= TimestampMillisecond(100000000, None) [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n                  TableScan: some_metric [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]",
    );

    assert_eq!(plan.display_indent_schema().to_string(), expected);
}

#[tokio::test]
async fn single_acos() {
    do_single_instant_function_call("acos", "acos").await;
}

#[tokio::test]
#[should_panic]
async fn single_acosh() {
    do_single_instant_function_call("acosh", "").await;
}

#[tokio::test]
async fn single_asin() {
    do_single_instant_function_call("asin", "asin").await;
}

#[tokio::test]
#[should_panic]
async fn single_asinh() {
    do_single_instant_function_call("asinh", "").await;
}

#[tokio::test]
async fn single_atan() {
    do_single_instant_function_call("atan", "atan").await;
}

#[tokio::test]
#[should_panic]
async fn single_atanh() {
    do_single_instant_function_call("atanh", "").await;
}

#[tokio::test]
async fn single_cos() {
    do_single_instant_function_call("cos", "cos").await;
}

#[tokio::test]
#[should_panic]
async fn single_cosh() {
    do_single_instant_function_call("cosh", "").await;
}

#[tokio::test]
async fn single_sin() {
    do_single_instant_function_call("sin", "sin").await;
}

#[tokio::test]
#[should_panic]
async fn single_sinh() {
    do_single_instant_function_call("sinh", "").await;
}

#[tokio::test]
async fn single_tan() {
    do_single_instant_function_call("tan", "tan").await;
}

#[tokio::test]
#[should_panic]
async fn single_tanh() {
    do_single_instant_function_call("tanh", "").await;
}

#[tokio::test]
#[should_panic]
async fn single_deg() {
    do_single_instant_function_call("deg", "").await;
}

#[tokio::test]
#[should_panic]
async fn single_rad() {
    do_single_instant_function_call("rad", "").await;
}

// {
//     input: "avg by (foo)(some_metric)",
//     expected: &AggregateExpr{
//         Op: AVG,
//         Expr: &VectorSelector{
//             Name: "some_metric",
//             LabelMatchers: []*labels.Matcher{
//                 MustLabelMatcher(labels.MatchEqual, model.MetricNameLabel, "some_metric"),
//             },
//             PosRange: PositionRange{
//                 Start: 13,
//                 End:   24,
//             },
//         },
//         Grouping: []string{"foo"},
//         PosRange: PositionRange{
//             Start: 0,
//             End:   25,
//         },
//     },
// },
async fn do_aggregate_expr_plan(fn_name: &str, plan_name: &str) {
    let prom_expr = parser::parse(&format!(
        "{fn_name} by (tag_1)(some_metric{{tag_0!=\"bar\"}})",
    ))
    .unwrap();
    let mut eval_stmt = EvalStmt {
        expr: prom_expr,
        start: UNIX_EPOCH,
        end: UNIX_EPOCH
            .checked_add(Duration::from_secs(100_000))
            .unwrap(),
        interval: Duration::from_secs(5),
        lookback_delta: Duration::from_secs(1),
    };

    // test group by
    let table_provider = build_test_table_provider(
        &[(DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string())],
        2,
        2,
    )
    .await;
    let plan = PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
        .await
        .unwrap();
    let expected_no_without = String::from(
        "Sort: some_metric.tag_1 ASC NULLS LAST, some_metric.timestamp ASC NULLS LAST [tag_1:Utf8, timestamp:Timestamp(ms), TEMPLATE(some_metric.field_0):Float64;N, TEMPLATE(some_metric.field_1):Float64;N]\
            \n  Aggregate: groupBy=[[some_metric.tag_1, some_metric.timestamp]], aggr=[[TEMPLATE(some_metric.field_0), TEMPLATE(some_metric.field_1)]] [tag_1:Utf8, timestamp:Timestamp(ms), TEMPLATE(some_metric.field_0):Float64;N, TEMPLATE(some_metric.field_1):Float64;N]\
            \n    Projection: some_metric.tag_0, some_metric.tag_1, some_metric.timestamp, some_metric.field_0, some_metric.field_1, Utf8(\"some_metric\") AS __promql_metric_name [tag_0:Utf8, tag_1:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, field_1:Float64;N, __promql_metric_name:Utf8]\
            \n      PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, tag_1:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, field_1:Float64;N]\
            \n        PromSeriesDivide: tags=[\"tag_0\", \"tag_1\"] [tag_0:Utf8, tag_1:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, field_1:Float64;N]\
            \n          Sort: some_metric.tag_0 ASC NULLS FIRST, some_metric.tag_1 ASC NULLS FIRST, some_metric.timestamp ASC NULLS FIRST [tag_0:Utf8, tag_1:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, field_1:Float64;N]\
            \n            Filter: some_metric.tag_0 != Utf8(\"bar\") AND some_metric.timestamp >= TimestampMillisecond(-999, None) AND some_metric.timestamp <= TimestampMillisecond(100000000, None) [tag_0:Utf8, tag_1:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, field_1:Float64;N]\
            \n              TableScan: some_metric [tag_0:Utf8, tag_1:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, field_1:Float64;N]"
    ).replace("TEMPLATE", plan_name);
    assert_eq!(
        plan.display_indent_schema().to_string(),
        expected_no_without
    );

    // test group without
    if let PromExpr::Aggregate(AggregateExpr { modifier, .. }) = &mut eval_stmt.expr {
        *modifier = Some(LabelModifier::Exclude(Labels {
            labels: vec![String::from("tag_1")].into_iter().collect(),
        }));
    }
    let table_provider = build_test_table_provider(
        &[(DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string())],
        2,
        2,
    )
    .await;
    let plan = PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
        .await
        .unwrap();
    let expected_without = String::from(
        "Sort: some_metric.tag_0 ASC NULLS LAST, some_metric.timestamp ASC NULLS LAST [tag_0:Utf8, timestamp:Timestamp(ms), TEMPLATE(some_metric.field_0):Float64;N, TEMPLATE(some_metric.field_1):Float64;N]\
            \n  Aggregate: groupBy=[[some_metric.tag_0, some_metric.timestamp]], aggr=[[TEMPLATE(some_metric.field_0), TEMPLATE(some_metric.field_1)]] [tag_0:Utf8, timestamp:Timestamp(ms), TEMPLATE(some_metric.field_0):Float64;N, TEMPLATE(some_metric.field_1):Float64;N]\
            \n    Projection: some_metric.tag_0, some_metric.tag_1, some_metric.timestamp, some_metric.field_0, some_metric.field_1, Utf8(\"some_metric\") AS __promql_metric_name [tag_0:Utf8, tag_1:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, field_1:Float64;N, __promql_metric_name:Utf8]\
            \n      PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, tag_1:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, field_1:Float64;N]\
            \n        PromSeriesDivide: tags=[\"tag_0\", \"tag_1\"] [tag_0:Utf8, tag_1:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, field_1:Float64;N]\
            \n          Sort: some_metric.tag_0 ASC NULLS FIRST, some_metric.tag_1 ASC NULLS FIRST, some_metric.timestamp ASC NULLS FIRST [tag_0:Utf8, tag_1:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, field_1:Float64;N]\
            \n            Filter: some_metric.tag_0 != Utf8(\"bar\") AND some_metric.timestamp >= TimestampMillisecond(-999, None) AND some_metric.timestamp <= TimestampMillisecond(100000000, None) [tag_0:Utf8, tag_1:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, field_1:Float64;N]\
            \n              TableScan: some_metric [tag_0:Utf8, tag_1:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, field_1:Float64;N]"
    ).replace("TEMPLATE", plan_name);
    assert_eq!(plan.display_indent_schema().to_string(), expected_without);
}

#[tokio::test]
async fn aggregate_sum() {
    do_aggregate_expr_plan("sum", "sum").await;
}

#[tokio::test]
async fn tsid_is_used_for_series_divide_when_available() {
    let prom_expr = parser::parse("some_metric").unwrap();
    let eval_stmt = EvalStmt {
        expr: prom_expr,
        start: UNIX_EPOCH,
        end: UNIX_EPOCH
            .checked_add(Duration::from_secs(100_000))
            .unwrap(),
        interval: Duration::from_secs(5),
        lookback_delta: Duration::from_secs(1),
    };

    let table_provider = build_test_table_provider_with_tsid(
        &[(DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string())],
        1,
        1,
    )
    .await;
    let plan = PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
        .await
        .unwrap();

    let plan_str = plan.display_indent_schema().to_string();
    assert!(plan_str.contains("PromSeriesDivide: tags=[\"__tsid\"]"));
    assert!(plan_str.contains("__tsid ASC NULLS FIRST"));
    assert!(
        !plan
            .schema()
            .fields()
            .iter()
            .any(|field| field.name() == DATA_SCHEMA_TSID_COLUMN_NAME)
    );

    let manipulate = find_instant_manipulate(&plan).unwrap();
    let exec = manipulate.to_execution_plan(Arc::new(DataSourceExec::new(Arc::new(
        MemorySourceConfig::try_new(
            &[],
            Arc::new(
                datafusion_expr::UserDefinedLogicalNodeCore::inputs(manipulate)[0]
                    .schema()
                    .as_arrow()
                    .clone(),
            ),
            None,
        )
        .unwrap(),
    ))));
    assert!(format!("{exec:?}").contains("reuse_tsid_column: true"));
}

async fn build_at_modifier_plan(query: &str, start_secs: u64, end_secs: u64) -> LogicalPlan {
    let eval_stmt = build_at_modifier_eval_stmt(query, start_secs, end_secs);
    let table_provider = build_test_table_provider(
        &[(DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string())],
        1,
        1,
    )
    .await;
    PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
        .await
        .unwrap()
}

fn build_at_modifier_eval_stmt(query: &str, start_secs: u64, end_secs: u64) -> EvalStmt {
    EvalStmt {
        expr: parser::parse(query).unwrap(),
        start: UNIX_EPOCH
            .checked_add(Duration::from_secs(start_secs))
            .unwrap(),
        end: UNIX_EPOCH
            .checked_add(Duration::from_secs(end_secs))
            .unwrap(),
        interval: Duration::from_secs(5),
        lookback_delta: Duration::from_secs(1),
    }
}

/// Every selector of an `@` anchored query must scan around the anchor only, instead of the
/// whole evaluation range.
#[tokio::test]
async fn at_modifier_anchors_selector_scan_window() {
    // `@ 100` anchors at t=100s; the lookback delta is 1s, so the scan covers (99s, 100s].
    let plan = build_at_modifier_plan("some_metric @ 100", 0, 1000).await;
    let plan_str = plan.display_indent_schema().to_string();
    assert!(
        plan_str.contains(
            "some_metric.timestamp >= TimestampMillisecond(99001, None) AND some_metric.timestamp <= TimestampMillisecond(100000, None)"
        ),
        "{plan_str}"
    );
    // The result is reported at the evaluation timestamps, not at the anchor.
    assert!(plan_str.contains("range=[0..1000000]"), "{plan_str}");

    // `@ start()` / `@ end()` resolve to the evaluation range of the statement.
    let plan = build_at_modifier_plan("some_metric @ start()", 0, 1000).await;
    let plan_str = plan.display_indent_schema().to_string();
    assert!(
        plan_str.contains(
            "some_metric.timestamp >= TimestampMillisecond(-999, None) AND some_metric.timestamp <= TimestampMillisecond(0, None)"
        ),
        "{plan_str}"
    );

    let plan = build_at_modifier_plan("some_metric @ end()", 0, 1000).await;
    let plan_str = plan.display_indent_schema().to_string();
    assert!(
        plan_str.contains(
            "some_metric.timestamp >= TimestampMillisecond(999001, None) AND some_metric.timestamp <= TimestampMillisecond(1000000, None)"
        ),
        "{plan_str}"
    );

    // `offset` moves the anchor backwards and is not applied twice.
    let plan = build_at_modifier_plan("some_metric @ 200 offset 50s", 0, 1000).await;
    let plan_str = plan.display_indent_schema().to_string();
    assert!(
        plan_str.contains(
            "some_metric.timestamp >= TimestampMillisecond(149001, None) AND some_metric.timestamp <= TimestampMillisecond(150000, None)"
        ),
        "{plan_str}"
    );

    // A timestamp before the Unix epoch is accepted, as in Prometheus.
    let plan = build_at_modifier_plan("some_metric @ -1", 0, 1000).await;
    let plan_str = plan.display_indent_schema().to_string();
    assert!(
        plan_str.contains(
            "some_metric.timestamp >= TimestampMillisecond(-1999, None) AND some_metric.timestamp <= TimestampMillisecond(-1000, None)"
        ),
        "{plan_str}"
    );
}

/// A call over one range selector anchored by `@` is evaluated once, at the start of the
/// evaluation, and its result is reported at every step: the window is folded around the anchor
/// instead of following the outer evaluation grid. This is the planner's counterpart of
/// Prometheus' `StepInvariantExpr` wrapper; see [`PromPlanner::promotes_anchored_range_call`].
#[tokio::test]
async fn at_modifier_promotes_anchored_range_call() {
    let plan = build_at_modifier_plan("rate(some_metric[5m] @ 300)", 0, 1000).await;
    let plan_str = plan.display_indent_schema().to_string();
    // The scan is limited to the anchored window (offset by `eval_start - anchor`).
    assert!(
        plan_str.contains(
            "some_metric.timestamp >= TimestampMillisecond(1, None) AND some_metric.timestamp <= TimestampMillisecond(300000, None)"
        ),
        "{plan_str}"
    );
    // A single fold, at the anchor...
    assert_eq!(
        plan_str
            .matches("PromRangeManipulate: req range=[0..0]")
            .count(),
        1,
        "{plan_str}"
    );
    // ... never a fold per step of the outer grid.
    assert!(
        !plan_str.contains("PromRangeManipulate: req range=[0..1000000]"),
        "{plan_str}"
    );
    // A single replay of the function result over the whole grid...
    assert_eq!(
        plan_str.matches("PromInstantManipulate").count(),
        1,
        "{plan_str}"
    );
    assert!(
        plan_str.contains("PromInstantManipulate: range=[0..1000000], lookback=[1000001]"),
        "{plan_str}"
    );
    // ... so `rate` itself is evaluated below that replay node, on the single evaluation
    // instant of the anchored subtree, instead of once per step.
    let replay = plan_str
        .find("PromInstantManipulate")
        .expect("instant manipulate node");
    let rate = plan_str.find("prom_rate(").expect("rate projection");
    assert!(
        replay < rate,
        "`rate` must be evaluated below the replay node:\n{plan_str}"
    );
}

/// Parentheses around the range argument are transparent: `rate((some_metric[5m] @ 300))` gets
/// the same fixed-window promotion as `rate(some_metric[5m] @ 300)`, with the anchored window
/// folded once and its result replayed at every step of the grid. Only that one argument is
/// looked through, so a parenthesis above the call promotes no operator of its own and a
/// parenthesized subtree is planned exactly like the bare one.
#[tokio::test]
async fn at_modifier_promotes_parenthesized_range_argument() {
    // Each form is planned exactly like its unparenthesized counterpart: parentheses below the
    // call are transparent, a parenthesis around the call adds nothing, and a parenthesis above
    // it does not widen the promotion.
    for (query, plain) in [
        (
            "rate((some_metric[5m] @ 300))",
            "rate(some_metric[5m] @ 300)",
        ),
        (
            "rate(((some_metric[5m] @ 300)))",
            "rate(some_metric[5m] @ 300)",
        ),
        (
            "(rate(some_metric[5m] @ 300))",
            "rate(some_metric[5m] @ 300)",
        ),
        (
            "abs((rate(some_metric[5m] @ 300)))",
            "abs(rate(some_metric[5m] @ 300))",
        ),
    ] {
        assert_eq!(
            build_at_modifier_plan(query, 0, 1000)
                .await
                .display_indent_schema()
                .to_string(),
            build_at_modifier_plan(plain, 0, 1000)
                .await
                .display_indent_schema()
                .to_string(),
            "`{query}` must be planned like `{plain}`"
        );
    }

    // The parentheses do not push the enclosing operator into the promotion either: `abs` stays
    // above the replay of the promoted call, exactly as it does without them. The promotion
    // itself (one anchored fold, one replay, `rate` below it) is asserted for the
    // unparenthesized form by `at_modifier_promotes_anchored_range_call`, and the form above is
    // planned identically to it.
    let plan_str = build_at_modifier_plan("abs((rate(some_metric[5m] @ 300)))", 0, 1000)
        .await
        .display_indent_schema()
        .to_string();
    let replay = plan_str
        .find("PromInstantManipulate")
        .expect("instant manipulate node");
    assert!(
        plan_str.find("abs(").expect("`abs` projection") < replay,
        "`abs` must be evaluated above the replay of the promoted call:\n{plan_str}"
    );
}

/// `@ start()` and `@ end()` are fixed anchors for the whole statement, so a call using them is
/// promoted as well.
#[tokio::test]
async fn at_modifier_promotes_start_and_end_anchored_call() {
    for query in [
        "rate(some_metric[5m] @ start())",
        "rate(some_metric[5m] @ end())",
        "max_over_time(some_metric[5m] @ end())",
    ] {
        let plan = build_at_modifier_plan(query, 0, 1000).await;
        let plan_str = plan.display_indent_schema().to_string();
        assert_eq!(
            plan_str
                .matches("PromRangeManipulate: req range=[0..0]")
                .count(),
            1,
            "{query}:\n{plan_str}"
        );
        assert!(
            plan_str.contains("PromInstantManipulate: range=[0..1000000], lookback=[1000001]"),
            "{query}:\n{plan_str}"
        );
    }

    // Only the call itself is promoted: a call above it (`abs`) is planned as usual and
    // evaluated at every step over the replayed result of the promoted call. The window is
    // still folded once, around the anchor.
    let plan = build_at_modifier_plan("abs(max_over_time(some_metric[5m] @ end()))", 0, 1000).await;
    let plan_str = plan.display_indent_schema().to_string();
    assert_eq!(
        plan_str
            .matches("PromRangeManipulate: req range=[0..0]")
            .count(),
        1,
        "{plan_str}"
    );
    let abs = plan_str.find("abs(").expect("`abs` projection");
    let replay = plan_str
        .find("PromInstantManipulate: range=[0..1000000], lookback=[1000001]")
        .expect("replay node");
    assert!(
        abs < replay,
        "`abs` must be evaluated above the replay of the promoted call:\n{plan_str}"
    );
    // The window is the only one folded once, and it sits below the replay: the promoted call
    // feeds the grid from there.
    let fold = plan_str.find("PromRangeManipulate").expect("range fold");
    assert!(
        replay < fold,
        "the anchored window must be folded below the replay:\n{plan_str}"
    );

    // An aggregation above the promoted call stays above it as well: `sum` aggregates the
    // replayed per-series rows at every step, instead of aggregating the single anchored
    // instant and replaying the aggregation — which would also have to replay rows of several
    // groups through the one-series-per-batch `InstantManipulate`.
    let plan = build_at_modifier_plan("sum(rate(some_metric[5m] @ start()))", 0, 1000).await;
    let plan_str = plan.display_indent_schema().to_string();
    assert_eq!(
        plan_str
            .matches("PromRangeManipulate: req range=[0..0]")
            .count(),
        1,
        "{plan_str}"
    );
    // A single replay, and it sits below the aggregation node: `sum` aggregates the replayed
    // per-series rows at every step.
    assert_eq!(
        plan_str.matches("PromInstantManipulate").count(),
        1,
        "{plan_str}"
    );
    let aggregate = plan_str.find("Aggregate:").expect("aggregate node");
    let replay = plan_str
        .find("PromInstantManipulate: range=[0..1000000], lookback=[1000001]")
        .expect("replay node");
    assert!(
        aggregate < replay,
        "the aggregation must stay above the replay of the promoted call:\n{plan_str}"
    );

    // Without `@` nothing is promoted: the function keeps folding one window per step.
    let plan = build_at_modifier_plan("rate(some_metric[5m])", 0, 1000).await;
    let plan_str = plan.display_indent_schema().to_string();
    assert!(
        plan_str.contains("PromRangeManipulate: req range=[0..1000000]"),
        "{plan_str}"
    );
    assert!(!plan_str.contains("lookback=[1000001]"), "{plan_str}");
}

/// A call or a unary operator above the anchored range call is planned as usual: the inner range
/// call is promoted on its own (it is the direct call over the anchored range selector), and the
/// operator above it is evaluated at every step over the replayed result.
#[tokio::test]
async fn at_modifier_promotes_inner_range_call_below_wrappers() {
    for (query, wrapper) in [
        ("abs(rate(some_metric[5m] @ 300))", "abs(prom_rate("),
        ("-rate(some_metric[5m] @ 300)", "(- prom_rate("),
        (
            "abs(max_over_time(some_metric[5m] @ 300))",
            "abs(prom_max_over_time(",
        ),
    ] {
        let plan = build_at_modifier_plan(query, 0, 1000).await;
        let plan_str = plan.display_indent_schema().to_string();
        // The anchored window is folded once, and the wrapper sits above its replay.
        assert_eq!(
            plan_str
                .matches("PromRangeManipulate: req range=[0..0]")
                .count(),
            1,
            "{query}:\n{plan_str}"
        );
        assert_eq!(
            plan_str.matches("PromInstantManipulate").count(),
            1,
            "{query}:\n{plan_str}"
        );
        let wrapper = plan_str
            .find(wrapper)
            .unwrap_or_else(|| panic!("no `{wrapper}` projection in:\n{plan_str}"));
        let replay = plan_str
            .find("PromInstantManipulate: range=[0..1000000], lookback=[1000001]")
            .expect("replay node");
        assert!(
            wrapper < replay,
            "the wrapper must be evaluated above the replay of the range call:\n{plan_str}"
        );
    }
}

/// `anchored + plain`: only the binary operand that is a call over the anchored range selector
/// is promoted, and the plain side keeps following the evaluation step.
#[tokio::test]
async fn at_modifier_promotes_only_anchored_binary_operand() {
    let plan = build_at_modifier_plan("rate(some_metric[5m] @ 300) + some_metric", 0, 1000).await;
    let plan_str = plan.display_indent_schema().to_string();
    // The anchored operand is folded once and its result replayed over the whole grid.
    assert_eq!(
        plan_str
            .matches("PromRangeManipulate: req range=[0..0]")
            .count(),
        1,
        "{plan_str}"
    );
    assert!(
        plan_str.contains("PromInstantManipulate: range=[0..1000000], lookback=[1000001]"),
        "{plan_str}"
    );
    // The plain operand still selects one sample per step with the default lookback.
    assert!(
        plan_str.contains("PromInstantManipulate: range=[0..1000000], lookback=[1000]"),
        "{plan_str}"
    );
    assert_eq!(
        plan_str.matches("PromInstantManipulate").count(),
        2,
        "{plan_str}"
    );

    // Both operands anchored: the binary expression is not promoted as a whole, because a join
    // emits the rows of several series in shared batches and the replay needs one series per
    // batch. Each operand anchors and replays on its own instead, and the join runs at every
    // step over those per-series results.
    let plan = build_at_modifier_plan("some_metric @ 300 + some_metric @ 0", 0, 1000).await;
    let plan_str = plan.display_indent_schema().to_string();
    // One anchoring node and one replay per operand...
    assert_eq!(
        plan_str.matches("PromInstantManipulate").count(),
        4,
        "{plan_str}"
    );
    assert_eq!(
        plan_str
            .matches("PromInstantManipulate: range=[0..1000000], lookback=[1000001]")
            .count(),
        2,
        "{plan_str}"
    );
    // ... and the two operands are anchored at different timestamps, so both select their own
    // sample.
    assert_eq!(
        plan_str
            .matches("PromInstantManipulate: range=[0..0], lookback=[1000]")
            .count(),
        2,
        "{plan_str}"
    );
}

/// Only a direct call over an anchored range selector is promoted. A value function or a unary
/// operator over an anchored *instant* selector needs no promotion: the selector anchors and
/// replays its sample per series on its own, and the operator above it is row-wise, so it can be
/// evaluated at every step over that replay.
#[tokio::test]
async fn at_modifier_does_not_promote_value_calls_over_anchored_selectors() {
    for (query, value_expr) in [
        ("abs(some_metric @ 300)", "abs(some_metric.field_0)"),
        ("-some_metric @ 300", "(- some_metric.field_0)"),
    ] {
        let plan = build_at_modifier_plan(query, 0, 1000).await;
        let plan_str = plan.display_indent_schema().to_string();
        // The scan is limited to the anchored sample (the lookback delta of this test is 1s).
        assert!(
            plan_str.contains(
                "some_metric.timestamp >= TimestampMillisecond(299001, None) AND some_metric.timestamp <= TimestampMillisecond(300000, None)"
            ),
            "{query}:\n{plan_str}"
        );
        // ... and the result is replayed at every step by the selector itself: the anchored
        // selection, then the grid replay, with no promoted subtree on top of the operator.
        assert!(
            !plan_str.starts_with("PromInstantManipulate"),
            "{query}:\n{plan_str}"
        );
        assert_eq!(
            plan_str
                .matches("PromInstantManipulate: range=[0..0], lookback=[1000]")
                .count(),
            1,
            "{query}:\n{plan_str}"
        );
        let replay = plan_str
            .find("PromInstantManipulate: range=[0..1000000], lookback=[1000001]")
            .unwrap_or_else(|| panic!("replay node:\n{plan_str}"));
        let value = plan_str
            .find(value_expr)
            .unwrap_or_else(|| panic!("no `{value_expr}` projection in:\n{plan_str}"));
        assert!(
            value < replay,
            "`{value_expr}` must be evaluated above the per-series replay:\n{plan_str}"
        );
    }
}

/// A call whose argument merges series (an aggregation or a join) or whose own output reorders
/// the whole vector (`sort*`, the histogram folds) is never the promoted root either: it is
/// planned as usual over the leaf-level anchoring of its selectors, which replays every selector
/// per series and keeps the one-series-per-batch layout the replay needs.
#[tokio::test]
async fn at_modifier_keeps_multi_series_roots_out_of_promoted_subtree() {
    // An aggregation below the call emits one row per group in shared batches, so the call is
    // not promoted: the replay of the anchored selector stays below the aggregate.
    let plan = build_at_modifier_plan("abs(sum(some_metric @ 300))", 0, 1000).await;
    let plan_str = plan.display_indent_schema().to_string();
    let aggregate = plan_str.find("Aggregate:").expect("aggregate node");
    let replay = plan_str
        .find("PromInstantManipulate: range=[0..1000000], lookback=[1000001]")
        .expect("replay node");
    assert!(
        aggregate < replay,
        "the aggregation must stay above the replay:\n{plan_str}"
    );

    // A join of two anchored selectors below the call: neither the join nor the call is
    // promoted, so each operand is replayed on its own.
    let plan = build_at_modifier_plan("abs(some_metric @ 300 + some_metric @ 0)", 0, 1000).await;
    let plan_str = plan.display_indent_schema().to_string();
    assert!(!plan_str.starts_with("PromInstantManipulate"), "{plan_str}");
    assert_eq!(
        plan_str
            .matches("PromInstantManipulate: range=[0..1000000], lookback=[1000001]")
            .count(),
        2,
        "{plan_str}"
    );

    // A call that reorders the whole vector keeps its sort above the per-series replay.
    let plan = build_at_modifier_plan("sort_by_label(some_metric @ 300, \"tag_0\")", 0, 1000).await;
    let plan_str = plan.display_indent_schema().to_string();
    assert!(plan_str.starts_with("Sort:"), "{plan_str}");
    assert_eq!(
        plan_str
            .matches("PromInstantManipulate: range=[0..1000000], lookback=[1000001]")
            .count(),
        1,
        "{plan_str}"
    );
}

/// `label_join` rewrites the labels of its input series, so it is never the promoted root: the
/// anchored selector keeps replaying one series per batch, and the join runs at every step above
/// that replay (see [`Self::promotes_anchored_range_call`]). Promoting it would replay the
/// joined rows through the labels the join just rewrote, which merges the distinct input series
/// into one timeline.
#[tokio::test]
async fn at_modifier_does_not_promote_label_join() {
    for query in [
        // Directly above the anchored instant selector...
        "label_join(some_metric @ 300, \"tag_0\", \"-\", \"tag_0\", \"tag_0\")",
        // ... and below another call, which is planned as usual over the join.
        "abs(label_join(some_metric @ 300, \"tag_0\", \"-\", \"tag_0\", \"tag_0\"))",
    ] {
        let plan = build_at_modifier_plan(query, 0, 1000).await;
        let plan_str = plan.display_indent_schema().to_string();
        // The join is not wrapped in a replay of its own; the only replay over the grid is the
        // one of the anchored selector...
        assert!(
            !plan_str.starts_with("PromInstantManipulate"),
            "{query}:\n{plan_str}"
        );
        assert_eq!(
            plan_str
                .matches("PromInstantManipulate: range=[0..1000000], lookback=[1000001]")
                .count(),
            1,
            "{query}:\n{plan_str}"
        );
        // ... and the projected join stays above it, evaluated at every step.
        let join = plan_str
            .find("concat_ws(")
            .unwrap_or_else(|| panic!("no `label_join` projection in:\n{plan_str}"));
        let replay = plan_str
            .find("PromInstantManipulate: range=[0..1000000], lookback=[1000001]")
            .expect("replay node");
        assert!(
            join < replay,
            "`label_join` must be evaluated above the per-series replay:\n{plan_str}"
        );
    }

    // A range call below the join is still promoted on its own: the anchored window is folded
    // once per series, and the join above it is evaluated at every step over that replay.
    let plan = build_at_modifier_plan(
        "label_join(rate(some_metric[5m] @ 300), \"tag_0\", \"-\", \"tag_0\", \"tag_0\")",
        0,
        1000,
    )
    .await;
    let plan_str = plan.display_indent_schema().to_string();
    assert_eq!(
        plan_str
            .matches("PromRangeManipulate: req range=[0..0]")
            .count(),
        1,
        "{plan_str}"
    );
    assert!(!plan_str.starts_with("PromInstantManipulate"), "{plan_str}");
    let join = plan_str.find("concat_ws(").expect("join projection");
    let replay = plan_str
        .find("PromInstantManipulate: range=[0..1000000], lookback=[1000001]")
        .expect("replay node");
    assert!(
        join < replay,
        "the join must be evaluated above the replay of the range call:\n{plan_str}"
    );
}

#[test]
fn at_modifier_rejects_subtraction_overflow() {
    for (anchor, offset) in [(i64::MAX, -1), (i64::MIN, 1)] {
        let err = PromPlanner::anchor_sub(anchor, offset).unwrap_err();
        assert_eq!(err.status_code(), StatusCode::InvalidArguments);
        assert!(
            err.to_string()
                .contains("Timestamp out of range for the `@` modifier"),
            "{err}"
        );
    }
    assert_eq!(PromPlanner::anchor_sub(-1, 1).unwrap(), -2);
}

/// `@` beyond the representable millisecond range is rejected instead of silently wrapping.
///
/// `@ 1e16` is 10^19 milliseconds, beyond `i64::MAX`. A Unix `SystemTime` can hold it, so
/// the planner rejects the anchor it cannot represent. A Windows `SystemTime` tops out
/// below `i64::MAX` milliseconds, so the same literal is already rejected while parsing.
#[tokio::test]
async fn at_modifier_rejects_unrepresentable_timestamp() {
    #[cfg(windows)]
    {
        let err = parser::parse("some_metric @ 1e16").unwrap_err();
        assert!(
            err.to_string()
                .contains("timestamp out of bounds for @ modifier"),
            "{err}"
        );
    }

    #[cfg(not(windows))]
    {
        let eval_stmt = build_eval_stmt("some_metric @ 1e16");
        let table_provider = build_test_table_provider(
            &[(DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string())],
            1,
            1,
        )
        .await;
        let err =
            PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
                .await
                .unwrap_err();
        assert!(
            err.to_string()
                .contains("Timestamp out of range for the `@` modifier"),
            "{err}"
        );
        assert_eq!(err.status_code(), StatusCode::InvalidArguments);
    }
}

#[tokio::test]
async fn default_binary_join_uses_tsid_when_available() {
    let eval_stmt = build_eval_stmt("some_metric / some_alt_metric");

    let table_provider = build_test_table_provider_with_tsid(
        &[
            (DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string()),
            (
                DEFAULT_SCHEMA_NAME.to_string(),
                "some_alt_metric".to_string(),
            ),
        ],
        1,
        1,
    )
    .await;
    let plan = PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
        .await
        .unwrap();

    let plan_str = plan.display_indent_schema().to_string();
    assert!(
        plan_str.contains("some_metric.__tsid = some_alt_metric.__tsid"),
        "{plan_str}"
    );
    assert!(
        !plan_str.contains("some_metric.tag_0 = some_alt_metric.tag_0"),
        "{plan_str}"
    );
}

/// Cross-logical-table regression: two logical metric tables whose physical rows share a
/// tag and a timestamp but carry the same `__tsid` must match on `__tsid` and keep the
/// complete sample (tag, time, value) through `left_metric + right_metric`.
#[tokio::test]
async fn tsid_cross_metric_exec_equal_tsid_keeps_complete_sample() {
    let state = build_query_engine_state();
    let table_provider =
        build_cross_logical_table_tsid_provider(CROSS_TABLE_TSID, CROSS_TABLE_TSID);
    let plan = PromPlanner::stmt_to_plan(
        table_provider,
        &operator_eval_stmt("left_metric + right_metric"),
        &state,
    )
    .await
    .unwrap();

    let plan_str = plan.display_indent_schema().to_string();
    // Each side reads only its own logical table's row from the shared physical table.
    assert_eq!(
        plan_str.matches("__table_id = UInt32(1024)").count(),
        1,
        "{plan_str}"
    );
    assert_eq!(
        plan_str.matches("__table_id = UInt32(1025)").count(),
        1,
        "{plan_str}"
    );
    // The cross-table match goes through `__tsid`, not the shared `tag_0` label.
    assert!(
        plan_str.contains("left_metric.__tsid = right_metric.__tsid"),
        "{plan_str}"
    );
    assert!(
        !plan_str.contains("left_metric.tag_0 = right_metric.tag_0"),
        "{plan_str}"
    );

    let (optimized, batches) = execute(plan, &state).await;
    let value_column = float_sample_column(&optimized);
    let rows = batches
        .iter()
        .flat_map(|batch| {
            let tags = batch
                .column_by_name("tag_0")
                .expect("no tag column")
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("the tag must be a string column");
            let timestamps = batch
                .column_by_name("timestamp")
                .expect("no timestamp column")
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .expect("the time index must be a millisecond timestamp column");
            let values = batch
                .column_by_name(&value_column)
                .expect("no sample value column")
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("the sample value must be a float column");
            (0..batch.num_rows())
                .map(|row| {
                    (
                        tags.value(row).to_string(),
                        timestamps.value(row),
                        values.value(row),
                    )
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(rows, vec![("a".to_string(), 1_000, 5.0)], "{plan_str}");

    // The arithmetic result must not keep a metric-name identity marker.
    assert!(
        PromPlanner::metric_name_column(optimized.schema())
            .unwrap()
            .is_none(),
        "{plan_str}"
    );
    assert_metric_name_not_in_batches(&batches);

    // Neither internal column may leak into the arithmetic result.
    for field in optimized.schema().fields() {
        assert_ne!(field.name(), DATA_SCHEMA_TSID_COLUMN_NAME, "{plan_str}");
        assert_ne!(field.name(), DATA_SCHEMA_TABLE_ID_COLUMN_NAME, "{plan_str}");
    }
    for batch in &batches {
        for field in batch.schema().fields() {
            assert_ne!(field.name(), DATA_SCHEMA_TSID_COLUMN_NAME, "{plan_str}");
            assert_ne!(field.name(), DATA_SCHEMA_TABLE_ID_COLUMN_NAME, "{plan_str}");
        }
    }
}

/// Control for [`tsid_cross_metric_exec_equal_tsid_keeps_complete_sample`]: the two rows
/// still share the tag and the timestamp, but carry distinct `__tsid`s, so the TSID-based
/// `__tsid` match must not pair the logical tables and the result must stay empty.
#[tokio::test]
async fn tsid_cross_metric_exec_distinct_tsid_matches_nothing() {
    let state = build_query_engine_state();
    let table_provider =
        build_cross_logical_table_tsid_provider(CROSS_TABLE_TSID, OTHER_TABLE_TSID);
    let plan = PromPlanner::stmt_to_plan(
        table_provider,
        &operator_eval_stmt("left_metric + right_metric"),
        &state,
    )
    .await
    .unwrap();

    let plan_str = plan.display_indent_schema().to_string();
    assert!(
        plan_str.contains("left_metric.__tsid = right_metric.__tsid"),
        "{plan_str}"
    );

    let (_, batches) = execute(plan, &state).await;
    assert_eq!(
        batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        0,
        "{plan_str}"
    );
}

#[tokio::test]
async fn reject_binary_fill_modifiers() {
    let state = build_query_engine_state();

    for query in [
        "some_metric + fill(0) some_alt_metric",
        "some_metric + fill_left(0) some_alt_metric",
        "some_metric + fill_right(0) some_alt_metric",
        "(some_metric + fill(0) some_alt_metric) + some_metric",
    ] {
        let eval_stmt = build_eval_stmt(query);
        let table_provider = build_test_table_provider(&[], 0, 0).await;
        let err = PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &state)
            .await
            .unwrap_err();

        assert!(
            matches!(
                &err,
                crate::promql::error::Error::UnsupportedExpr { name, .. }
                    if name == "PromQL fill modifiers"
            ),
            "{err}"
        );
    }
}

#[tokio::test]
async fn timestamp_binary_join_falls_back_when_tsid_is_projected_out() {
    for query in [
        "timestamp(some_metric) / some_metric",
        "some_metric / timestamp(some_metric)",
    ] {
        let eval_stmt = build_eval_stmt(query);

        let table_provider = build_test_table_provider_with_tsid(
            &[(DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string())],
            1,
            1,
        )
        .await;
        let plan =
            PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
                .await
                .unwrap();

        let plan_str = plan.display_indent_schema().to_string();
        assert!(!plan_str.contains("__tsid ="), "{query}: {plan_str}");
        assert!(
            plan_str.contains("lhs.tag_0 = rhs.tag_0"),
            "{query}: {plan_str}"
        );
        assert!(
            !plan
                .schema()
                .fields()
                .iter()
                .any(|field| field.name() == DATA_SCHEMA_TSID_COLUMN_NAME),
            "{query}: {plan_str}"
        );
    }
}

#[tokio::test]
async fn timestamp_binary_join_rejects_default_matching_on_mismatched_labels() {
    let eval_stmt = build_eval_stmt("timestamp(left_host_job) / right_by_job");

    let table_provider = build_test_table_provider_with_tsid_tag_fields(&[
        (
            (DEFAULT_SCHEMA_NAME.to_string(), "left_host_job".to_string()),
            2,
            1,
        ),
        (
            (DEFAULT_SCHEMA_NAME.to_string(), "right_by_job".to_string()),
            1,
            1,
        ),
    ])
    .await;
    let plan = PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
        .await
        .unwrap();
    let plan_str = plan.display_indent_schema().to_string();

    assert!(
        plan_str.contains("Boolean(false)") || plan_str.contains("false"),
        "{plan_str}"
    );
}

#[tokio::test]
async fn tsid_is_preserved_for_nested_default_binary_joins() {
    let eval_stmt = build_eval_stmt("(some_metric - some_alt_metric) / some_third_metric");

    let table_provider = build_test_table_provider_with_tsid(
        &[
            (DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string()),
            (
                DEFAULT_SCHEMA_NAME.to_string(),
                "some_alt_metric".to_string(),
            ),
            (
                DEFAULT_SCHEMA_NAME.to_string(),
                "some_third_metric".to_string(),
            ),
        ],
        1,
        1,
    )
    .await;
    let plan = PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
        .await
        .unwrap();

    let plan_str = plan.display_indent_schema().to_string();
    assert_eq!(plan_str.matches("__tsid =").count(), 2, "{plan_str}");
    assert!(!plan_str.contains("tag_0 ="), "{plan_str}");
}

#[tokio::test]
async fn repeated_tsid_binary_operand_reuses_leaf_plan() {
    let eval_stmt = build_eval_stmt("((some_metric - some_alt_metric) / some_metric) * 100");

    let table_provider = build_test_table_provider_with_tsid(
        &[
            (DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string()),
            (
                DEFAULT_SCHEMA_NAME.to_string(),
                "some_alt_metric".to_string(),
            ),
        ],
        1,
        1,
    )
    .await;
    let plan = PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
        .await
        .unwrap();

    let plan_str = plan.display_indent_schema().to_string();
    assert_eq!(plan_str.matches("__tsid =").count(), 1, "{plan_str}");
    assert_eq!(
        plan_str
            .matches("Filter: phy.__table_id = UInt32(1024)")
            .count(),
        1,
        "{plan_str}"
    );
    assert_eq!(
        plan_str.matches("PromInstantManipulate").count(),
        2,
        "{plan_str}"
    );
    assert!(!plan_str.contains("tag_0 ="), "{plan_str}");
}

#[tokio::test]
async fn repeated_tsid_binary_operand_reuses_shorter_field_side() {
    let eval_stmt =
        build_eval_stmt("((two_field_metric - one_field_metric) / one_field_metric) * 100");

    let table_provider = build_test_table_provider_with_tsid_fields(
        &[
            (
                (
                    DEFAULT_SCHEMA_NAME.to_string(),
                    "two_field_metric".to_string(),
                ),
                2,
            ),
            (
                (
                    DEFAULT_SCHEMA_NAME.to_string(),
                    "one_field_metric".to_string(),
                ),
                1,
            ),
        ],
        1,
    )
    .await;
    let plan = PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
        .await
        .unwrap();

    let field_names = plan
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect::<Vec<_>>();
    let value_columns = field_names
        .iter()
        .filter(|name| {
            *name != "tag_0" && *name != "timestamp" && *name != DATA_SCHEMA_TSID_COLUMN_NAME
        })
        .count();
    assert_eq!(value_columns, 1, "{field_names:?}");
    let plan_str = plan.display_indent_schema().to_string();
    assert_eq!(plan_str.matches("__tsid =").count(), 1, "{plan_str}");
    assert_eq!(
        plan_str
            .matches("Filter: phy.__table_id = UInt32(1025)")
            .count(),
        1,
        "{plan_str}"
    );
    assert!(!plan_str.contains("tag_0 ="), "{plan_str}");
}

#[tokio::test]
async fn binary_island_reuses_self_operand_without_join() {
    let eval_stmt = build_eval_stmt("some_metric / some_metric");

    let table_provider = build_test_table_provider_with_tsid(
        &[(DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string())],
        1,
        1,
    )
    .await;
    let plan = PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
        .await
        .unwrap();

    let plan_str = plan.display_indent_schema().to_string();
    assert_eq!(plan_str.matches("__tsid =").count(), 0, "{plan_str}");
    assert_eq!(
        plan_str
            .matches("Filter: phy.__table_id = UInt32(1024)")
            .count(),
        1,
        "{plan_str}"
    );
    assert_eq!(
        plan_str.matches("PromInstantManipulate").count(),
        1,
        "{plan_str}"
    );
}

#[tokio::test]
async fn binary_island_reuses_leaf_across_two_branches() {
    let eval_stmt =
        build_eval_stmt("(some_metric + some_alt_metric) / (some_metric + third_metric)");

    let table_provider = build_test_table_provider_with_tsid(
        &[
            (DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string()),
            (
                DEFAULT_SCHEMA_NAME.to_string(),
                "some_alt_metric".to_string(),
            ),
            (DEFAULT_SCHEMA_NAME.to_string(), "third_metric".to_string()),
        ],
        1,
        1,
    )
    .await;
    let plan = PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
        .await
        .unwrap();

    let plan_str = plan.display_indent_schema().to_string();
    assert_eq!(plan_str.matches("__tsid =").count(), 2, "{plan_str}");
    assert_eq!(
        plan_str
            .matches("Filter: phy.__table_id = UInt32(1024)")
            .count(),
        1,
        "{plan_str}"
    );
    assert_eq!(
        plan_str.matches("PromInstantManipulate").count(),
        3,
        "{plan_str}"
    );
}

#[tokio::test]
async fn binary_island_generated_alias_avoids_user_column_names() {
    let eval_stmt = build_eval_stmt("(some_metric + some_alt_metric) / some_metric");

    let table_provider = build_test_table_provider_with_fields(
        &[
            (DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string()),
            (
                DEFAULT_SCHEMA_NAME.to_string(),
                "some_alt_metric".to_string(),
            ),
        ],
        &["prom_v0", "__prom_v0"],
    )
    .await;
    let plan = PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
        .await
        .unwrap();

    let field_names = plan.schema().field_names();
    assert!(field_names.iter().any(|name| name.ends_with(".prom_v0")));
    assert!(field_names.iter().any(|name| name.ends_with(".__prom_v0")));

    let plan_str = plan.display_indent_schema().to_string();
    assert!(plan_str.contains("SubqueryAlias: __prom_v0"), "{plan_str}");
    assert_eq!(
        plan_str.matches("PromInstantManipulate").count(),
        2,
        "{plan_str}"
    );
}

#[tokio::test]
async fn binary_island_clears_qualifier_for_nested_unary_projection() {
    let eval_stmt = build_eval_stmt("-((some_metric + some_alt_metric) / some_metric)");

    let table_provider = build_test_table_provider_with_tsid(
        &[
            (DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string()),
            (
                DEFAULT_SCHEMA_NAME.to_string(),
                "some_alt_metric".to_string(),
            ),
        ],
        1,
        1,
    )
    .await;
    let plan = PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
        .await
        .unwrap();

    let plan_str = plan.display_indent_schema().to_string();
    assert_eq!(plan_str.matches("__tsid =").count(), 1, "{plan_str}");
    assert_eq!(
        plan_str.matches("PromInstantManipulate").count(),
        2,
        "{plan_str}"
    );
}

#[tokio::test]
async fn binary_island_keeps_distinct_matcher_leaves() {
    let eval_stmt = build_eval_stmt(
        "(some_metric{tag_0=\"foo\"} + some_alt_metric) / some_metric{tag_0=\"bar\"}",
    );

    let table_provider = build_test_table_provider_with_tsid(
        &[
            (DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string()),
            (
                DEFAULT_SCHEMA_NAME.to_string(),
                "some_alt_metric".to_string(),
            ),
        ],
        1,
        1,
    )
    .await;
    let plan = PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
        .await
        .unwrap();

    let plan_str = plan.display_indent_schema().to_string();
    assert_eq!(plan_str.matches("__tsid =").count(), 2, "{plan_str}");
    assert_eq!(
        plan_str.matches("PromInstantManipulate").count(),
        3,
        "{plan_str}"
    );
}

#[tokio::test]
async fn binary_island_keeps_offset_leaves_distinct() {
    let eval_stmt = build_eval_stmt("(some_metric offset 5m + some_alt_metric) / some_metric");

    let table_provider = build_test_table_provider_with_tsid(
        &[
            (DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string()),
            (
                DEFAULT_SCHEMA_NAME.to_string(),
                "some_alt_metric".to_string(),
            ),
        ],
        1,
        1,
    )
    .await;
    let plan = PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
        .await
        .unwrap();

    let plan_str = plan.display_indent_schema().to_string();
    assert_eq!(plan_str.matches("__tsid =").count(), 2, "{plan_str}");
    assert_eq!(
        plan_str.matches("PromInstantManipulate").count(),
        3,
        "{plan_str}"
    );
}

#[tokio::test]
async fn binary_island_falls_back_for_group_modifier() {
    let eval_stmt =
        build_eval_stmt("(some_metric + ignoring(tag_0) group_left some_alt_metric) / some_metric");

    let table_provider = build_test_table_provider_with_tsid(
        &[
            (DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string()),
            (
                DEFAULT_SCHEMA_NAME.to_string(),
                "some_alt_metric".to_string(),
            ),
        ],
        1,
        1,
    )
    .await;
    let plan = PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
        .await
        .unwrap();

    let plan_str = plan.display_indent_schema().to_string();
    assert_eq!(
        plan_str.matches("PromInstantManipulate").count(),
        3,
        "{plan_str}"
    );
}

#[tokio::test]
async fn binary_island_falls_back_for_comparison_filter() {
    let eval_stmt = build_eval_stmt("(some_metric > some_alt_metric) / some_metric");

    let table_provider = build_test_table_provider_with_tsid(
        &[
            (DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string()),
            (
                DEFAULT_SCHEMA_NAME.to_string(),
                "some_alt_metric".to_string(),
            ),
        ],
        1,
        1,
    )
    .await;
    let plan = PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
        .await
        .unwrap();

    let plan_str = plan.display_indent_schema().to_string();
    assert_eq!(plan_str.matches("__tsid =").count(), 2, "{plan_str}");
    assert_eq!(
        plan_str.matches("PromInstantManipulate").count(),
        3,
        "{plan_str}"
    );
}

#[tokio::test]
async fn tsid_binary_join_uses_shorter_field_side() {
    let eval_stmt = build_eval_stmt("one_field_metric / two_field_metric");

    let table_provider = build_test_table_provider_with_tsid_fields(
        &[
            (
                (
                    DEFAULT_SCHEMA_NAME.to_string(),
                    "one_field_metric".to_string(),
                ),
                1,
            ),
            (
                (
                    DEFAULT_SCHEMA_NAME.to_string(),
                    "two_field_metric".to_string(),
                ),
                2,
            ),
        ],
        1,
    )
    .await;
    let plan = PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
        .await
        .unwrap();

    let field_names = plan
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect::<Vec<_>>();
    let value_columns = field_names
        .iter()
        .filter(|name| {
            *name != "tag_0" && *name != "timestamp" && *name != DATA_SCHEMA_TSID_COLUMN_NAME
        })
        .count();
    assert_eq!(value_columns, 1, "{field_names:?}");
}

#[tokio::test]
async fn comparison_binary_join_uses_shorter_field_side() {
    let eval_stmt = build_eval_stmt("two_field_metric > one_field_metric");

    let table_provider = build_test_table_provider_with_tsid_fields(
        &[
            (
                (
                    DEFAULT_SCHEMA_NAME.to_string(),
                    "two_field_metric".to_string(),
                ),
                2,
            ),
            (
                (
                    DEFAULT_SCHEMA_NAME.to_string(),
                    "one_field_metric".to_string(),
                ),
                1,
            ),
        ],
        1,
    )
    .await;
    let plan = PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
        .await
        .unwrap();

    let field_names = plan
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect::<Vec<_>>();
    assert!(
        field_names.iter().any(|name| name == "field_0"),
        "{field_names:?}"
    );
    assert!(
        !field_names.iter().any(|name| name == "field_1"),
        "{field_names:?}"
    );
}

#[tokio::test]
async fn label_matching_modifier_disables_tsid_binary_join() {
    let eval_stmt = build_eval_stmt("some_metric / ignoring(tag_0) some_alt_metric");

    let table_provider = build_test_table_provider_with_tsid(
        &[
            (DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string()),
            (
                DEFAULT_SCHEMA_NAME.to_string(),
                "some_alt_metric".to_string(),
            ),
        ],
        2,
        1,
    )
    .await;
    let plan = PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
        .await
        .unwrap();

    let plan_str = plan.display_indent_schema().to_string();
    assert!(!plan_str.contains("__tsid ="), "{plan_str}");
    assert!(
        plan_str.contains("some_metric.tag_1 = some_alt_metric.tag_1"),
        "{plan_str}"
    );
}

#[tokio::test]
async fn ignoring_absent_label_keeps_tsid_binary_join() {
    let eval_stmt = build_eval_stmt("some_metric / ignoring(missing) some_alt_metric");

    let table_provider = build_test_table_provider_with_tsid(
        &[
            (DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string()),
            (
                DEFAULT_SCHEMA_NAME.to_string(),
                "some_alt_metric".to_string(),
            ),
        ],
        2,
        1,
    )
    .await;
    let plan = PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
        .await
        .unwrap();

    let plan_str = plan.display_indent_schema().to_string();
    assert!(
        plan_str.contains("some_metric.__tsid = some_alt_metric.__tsid"),
        "{plan_str}"
    );
    assert!(!plan_str.contains("tag_0 ="), "{plan_str}");
    assert!(!plan_str.contains("tag_1 ="), "{plan_str}");
}

#[tokio::test]
async fn range_function_keeps_tsid_for_absent_ignoring_binary_join() {
    let eval_stmt = build_eval_stmt("rate(some_metric[5m]) / ignoring(missing) some_alt_metric");

    let table_provider = build_test_table_provider_with_tsid(
        &[
            (DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string()),
            (
                DEFAULT_SCHEMA_NAME.to_string(),
                "some_alt_metric".to_string(),
            ),
        ],
        2,
        1,
    )
    .await;
    let plan = PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
        .await
        .unwrap();

    let plan_str = plan.display_indent_schema().to_string();
    assert!(
        plan_str.contains("some_metric.__tsid = some_alt_metric.__tsid"),
        "{plan_str}"
    );
    assert!(!plan_str.contains("tag_0 ="), "{plan_str}");
    assert!(!plan_str.contains("tag_1 ="), "{plan_str}");
}

#[tokio::test]
async fn on_full_label_set_keeps_tsid_binary_join() {
    let eval_stmt = build_eval_stmt("some_metric / on(tag_0, tag_1) some_alt_metric");

    let table_provider = build_test_table_provider_with_tsid(
        &[
            (DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string()),
            (
                DEFAULT_SCHEMA_NAME.to_string(),
                "some_alt_metric".to_string(),
            ),
        ],
        2,
        1,
    )
    .await;
    let plan = PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
        .await
        .unwrap();

    let plan_str = plan.display_indent_schema().to_string();
    assert!(
        plan_str.contains("some_metric.__tsid = some_alt_metric.__tsid"),
        "{plan_str}"
    );
    assert!(!plan_str.contains("tag_0 ="), "{plan_str}");
    assert!(!plan_str.contains("tag_1 ="), "{plan_str}");
}

#[tokio::test]
async fn on_partial_label_set_disables_tsid_binary_join() {
    let eval_stmt = build_eval_stmt("some_metric / on(tag_0) some_alt_metric");

    let table_provider = build_test_table_provider_with_tsid(
        &[
            (DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string()),
            (
                DEFAULT_SCHEMA_NAME.to_string(),
                "some_alt_metric".to_string(),
            ),
        ],
        2,
        1,
    )
    .await;
    let plan = PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
        .await
        .unwrap();

    let plan_str = plan.display_indent_schema().to_string();
    assert!(!plan_str.contains("__tsid ="), "{plan_str}");
    assert!(
        plan_str.contains("some_metric.tag_0 = some_alt_metric.tag_0"),
        "{plan_str}"
    );
    assert!(!plan_str.contains("tag_1 ="), "{plan_str}");
}

#[tokio::test]
async fn on_label_set_must_cover_both_sides_to_use_tsid_binary_join() {
    let eval_stmt = build_eval_stmt("some_metric / on(tag_0) some_alt_metric");

    let table_provider = build_test_table_provider_with_tsid_tag_fields(&[
        (
            (DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string()),
            2,
            1,
        ),
        (
            (
                DEFAULT_SCHEMA_NAME.to_string(),
                "some_alt_metric".to_string(),
            ),
            1,
            1,
        ),
    ])
    .await;
    let plan = PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
        .await
        .unwrap();

    let plan_str = plan.display_indent_schema().to_string();
    assert!(!plan_str.contains("__tsid ="), "{plan_str}");
    assert!(
        plan_str.contains("some_metric.tag_0 = some_alt_metric.tag_0"),
        "{plan_str}"
    );
    assert!(!plan_str.contains("tag_1 ="), "{plan_str}");
}

#[tokio::test]
async fn comparison_binary_join_uses_tsid_and_keeps_it_in_filtered_result() {
    let eval_stmt = build_eval_stmt("some_metric > some_alt_metric");

    let table_provider = build_test_table_provider_with_tsid(
        &[
            (DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string()),
            (
                DEFAULT_SCHEMA_NAME.to_string(),
                "some_alt_metric".to_string(),
            ),
        ],
        2,
        1,
    )
    .await;
    let mut planner = PromPlanner {
        table_provider,
        ctx: PromPlannerContext::from_eval_stmt(&eval_stmt),
        promql_annotations: None,
    };
    let plan = planner
        .prom_expr_to_plan(&eval_stmt.expr, &build_query_engine_state())
        .await
        .unwrap();

    let plan_str = plan.display_indent_schema().to_string();
    assert!(
        plan_str.contains("some_metric.__tsid = some_alt_metric.__tsid"),
        "{plan_str}"
    );
    assert!(
        plan.schema()
            .fields()
            .iter()
            .any(|field| field.name() == DATA_SCHEMA_TSID_COLUMN_NAME),
        "{plan_str}"
    );
    assert!(planner.ctx.use_tsid, "{plan_str}");
}

#[tokio::test]
async fn comparison_bool_binary_join_uses_tsid_when_available() {
    let eval_stmt = build_eval_stmt("some_metric > bool some_alt_metric");

    let table_provider = build_test_table_provider_with_tsid(
        &[
            (DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string()),
            (
                DEFAULT_SCHEMA_NAME.to_string(),
                "some_alt_metric".to_string(),
            ),
        ],
        2,
        1,
    )
    .await;
    let plan = PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
        .await
        .unwrap();

    let plan_str = plan.display_indent_schema().to_string();
    assert!(
        plan_str.contains("some_metric.__tsid = some_alt_metric.__tsid"),
        "{plan_str}"
    );
    assert!(!plan_str.contains("tag_0 ="), "{plan_str}");
    assert!(!plan_str.contains("tag_1 ="), "{plan_str}");
}

#[tokio::test]
async fn scalar_count_count_range_keeps_full_window() {
    let plan_str = build_optimized_tsid_plan(
        "scalar(count(count(some_metric) by (tag_0)))",
        1,
        1,
        100_000,
        1,
    )
    .await;
    assert!(plan_str.contains("ScalarCalculate: tags=[]"));
    assert!(plan_str.contains("PromInstantManipulate: range=[0..100000000]"));
    assert!(!plan_str.contains("PromInstantManipulate: range=[99999000..99999000]"));
}

#[tokio::test]
async fn scalar_count_count_rewrite_applies_inside_binary_expr_for_tsid_input() {
    let plan_str = build_optimized_tsid_plan(
        "sum(irate(some_metric[1h])) / scalar(count(count(some_metric) by (tag_0)))",
        2,
        1,
        10,
        300,
    )
    .await;
    assert!(plan_str.contains("Distinct:"), "{plan_str}");
}

#[tokio::test]
async fn nested_count_rewrite_keeps_full_series_key_with_tsid_input() {
    assert_nested_count_rewrite_applies(
        "count(count(some_metric) by (tag_0))",
        "Aggregate: groupBy=[[some_metric.timestamp]], aggr=[[count(Int64(1)) AS count(count(some_metric.field_0))]]"
    )
    .await;
}

#[tokio::test]
async fn nested_sum_count_rewrite_keeps_full_series_key_with_tsid_input() {
    assert_nested_count_rewrite_applies(
        "count(sum(some_metric) by (tag_0))",
        "Aggregate: groupBy=[[some_metric.timestamp]], aggr=[[count(Int64(1)) AS count(sum(some_metric.field_0))]]"
    )
    .await;
}

#[tokio::test]
async fn nested_supported_inner_aggs_rewrite_apply_for_tsid_input() {
    for (query, expected_outer_agg) in [
        (
            "count(avg(some_metric) by (tag_0))",
            "Aggregate: groupBy=[[some_metric.timestamp]], aggr=[[count(Int64(1)) AS count(avg(some_metric.field_0))]]",
        ),
        (
            "count(min(some_metric) by (tag_0))",
            "Aggregate: groupBy=[[some_metric.timestamp]], aggr=[[count(Int64(1)) AS count(min(some_metric.field_0))]]",
        ),
        (
            "count(max(some_metric) by (tag_0))",
            "Aggregate: groupBy=[[some_metric.timestamp]], aggr=[[count(Int64(1)) AS count(max(some_metric.field_0))]]",
        ),
        (
            "count(stddev(some_metric) by (tag_0))",
            "Aggregate: groupBy=[[some_metric.timestamp]], aggr=[[count(Int64(1)) AS count(stddev_pop(some_metric.field_0))]]",
        ),
        (
            "count(stdvar(some_metric) by (tag_0))",
            "Aggregate: groupBy=[[some_metric.timestamp]], aggr=[[count(Int64(1)) AS count(var_pop(some_metric.field_0))]]",
        ),
    ] {
        assert_nested_count_rewrite_applies(query, expected_outer_agg).await;
    }
}

#[tokio::test]
async fn nested_non_count_inner_aggs_rewrite_filter_null_values_for_tsid_input() {
    let count_plan =
        build_optimized_tsid_plan("count(count(some_metric) by (tag_0))", 2, 1, 100_000, 1).await;
    assert!(
        !count_plan.contains("some_metric.field_0 IS NOT NULL"),
        "{count_plan}"
    );

    for query in [
        "count(sum(some_metric) by (tag_0))",
        "count(avg(some_metric) by (tag_0))",
        "count(min(some_metric) by (tag_0))",
        "count(max(some_metric) by (tag_0))",
        "count(stddev(some_metric) by (tag_0))",
        "count(stdvar(some_metric) by (tag_0))",
    ] {
        let plan_str = build_optimized_tsid_plan(query, 2, 1, 100_000, 1).await;
        assert!(
            plan_str.contains("Filter: some_metric.field_0 IS NOT NULL"),
            "{query}: {plan_str}"
        );
    }
}

#[tokio::test]
async fn nested_unsupported_or_non_direct_inner_aggs_do_not_rewrite() {
    assert_nested_count_rewrite_missing("count(group(some_metric) by (tag_0))", 2, 1).await;
    assert_nested_count_rewrite_missing("count(sum(irate(some_metric[1h])) by (tag_0))", 2, 300)
        .await;
}

#[tokio::test]
async fn physical_table_name_is_not_leaked_in_plan() {
    let prom_expr = parser::parse("some_metric").unwrap();
    let eval_stmt = EvalStmt {
        expr: prom_expr,
        start: UNIX_EPOCH,
        end: UNIX_EPOCH
            .checked_add(Duration::from_secs(100_000))
            .unwrap(),
        interval: Duration::from_secs(5),
        lookback_delta: Duration::from_secs(1),
    };

    let table_provider = build_test_table_provider_with_tsid(
        &[(DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string())],
        1,
        1,
    )
    .await;
    let plan = PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
        .await
        .unwrap();

    let plan_str = plan.display_indent_schema().to_string();
    assert!(plan_str.contains("TableScan: phy"), "{plan}");
    assert!(plan_str.contains("SubqueryAlias: some_metric"));
    assert!(plan_str.contains("Filter: phy.__table_id = UInt32(1024)"));
    assert!(!plan_str.contains("TableScan: some_metric"));
}

#[tokio::test]
async fn sum_without_does_not_group_by_tsid() {
    let prom_expr = parser::parse("sum without (tag_0) (some_metric)").unwrap();
    let eval_stmt = EvalStmt {
        expr: prom_expr,
        start: UNIX_EPOCH,
        end: UNIX_EPOCH
            .checked_add(Duration::from_secs(100_000))
            .unwrap(),
        interval: Duration::from_secs(5),
        lookback_delta: Duration::from_secs(1),
    };

    let table_provider = build_test_table_provider_with_tsid(
        &[(DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string())],
        1,
        1,
    )
    .await;
    let plan = PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
        .await
        .unwrap();

    let plan_str = plan.display_indent_schema().to_string();
    assert!(plan_str.contains("PromSeriesDivide: tags=[\"__tsid\"]"));

    let aggr_line = plan_str
        .lines()
        .find(|line| line.contains("Aggregate: groupBy="))
        .unwrap();
    assert!(!aggr_line.contains(DATA_SCHEMA_TSID_COLUMN_NAME));
}

#[tokio::test]
async fn topk_without_does_not_partition_by_tsid() {
    let prom_expr = parser::parse("topk without (tag_0) (1, some_metric)").unwrap();
    let eval_stmt = EvalStmt {
        expr: prom_expr,
        start: UNIX_EPOCH,
        end: UNIX_EPOCH
            .checked_add(Duration::from_secs(100_000))
            .unwrap(),
        interval: Duration::from_secs(5),
        lookback_delta: Duration::from_secs(1),
    };

    let table_provider = build_test_table_provider_with_tsid(
        &[(DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string())],
        1,
        1,
    )
    .await;
    let plan = PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
        .await
        .unwrap();

    let plan_str = plan.display_indent_schema().to_string();
    assert!(plan_str.contains("PromSeriesDivide: tags=[\"__tsid\"]"));

    let window_line = plan_str
        .lines()
        .find(|line| line.contains("WindowAggr: windowExpr=[[row_number()"))
        .unwrap();
    let partition_by = window_line
        .split("PARTITION BY [")
        .nth(1)
        .and_then(|s| s.split("] ORDER BY").next())
        .unwrap();
    assert!(!partition_by.contains(DATA_SCHEMA_TSID_COLUMN_NAME));
}

#[tokio::test]
async fn sum_by_does_not_group_by_tsid() {
    let prom_expr = parser::parse("sum by (__tsid) (some_metric)").unwrap();
    let eval_stmt = EvalStmt {
        expr: prom_expr,
        start: UNIX_EPOCH,
        end: UNIX_EPOCH
            .checked_add(Duration::from_secs(100_000))
            .unwrap(),
        interval: Duration::from_secs(5),
        lookback_delta: Duration::from_secs(1),
    };

    let table_provider = build_test_table_provider_with_tsid(
        &[(DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string())],
        1,
        1,
    )
    .await;
    let plan = PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
        .await
        .unwrap();

    let plan_str = plan.display_indent_schema().to_string();
    assert!(plan_str.contains("PromSeriesDivide: tags=[\"__tsid\"]"));

    let aggr_line = plan_str
        .lines()
        .find(|line| line.contains("Aggregate: groupBy="))
        .unwrap();
    assert!(!aggr_line.contains(DATA_SCHEMA_TSID_COLUMN_NAME));
}

#[tokio::test]
async fn aggregate_over_binary_time_function_expr() {
    for op in ["sum", "min", "max", "avg"] {
        let prom_expr = parser::parse(&format!(
            "{op} by (tag_0, tag_1, tag_2) (time() - some_metric)"
        ))
        .unwrap();
        let eval_stmt = EvalStmt {
            expr: prom_expr,
            start: UNIX_EPOCH,
            end: UNIX_EPOCH
                .checked_add(Duration::from_secs(100_000))
                .unwrap(),
            interval: Duration::from_secs(5),
            lookback_delta: Duration::from_secs(1),
        };

        let table_provider = build_test_table_provider_with_tsid(
            &[(DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string())],
            3,
            1,
        )
        .await;
        let plan =
            PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
                .await
                .unwrap();

        let plan_str = plan.display_indent_schema().to_string();
        let aggr_line = plan_str
            .lines()
            .find(|line| line.contains("Aggregate: groupBy="))
            .unwrap();
        assert!(aggr_line.contains(op), "{plan_str}");
        assert!(aggr_line.contains("first_value"), "{plan_str}");
        assert!(
            !plan
                .schema()
                .fields()
                .iter()
                .any(|field| { field.name() == DATA_SCHEMA_TSID_COLUMN_NAME })
        );
    }
}

#[tokio::test]
async fn topk_by_does_not_partition_by_tsid() {
    let prom_expr = parser::parse("topk by (__tsid) (1, some_metric)").unwrap();
    let eval_stmt = EvalStmt {
        expr: prom_expr,
        start: UNIX_EPOCH,
        end: UNIX_EPOCH
            .checked_add(Duration::from_secs(100_000))
            .unwrap(),
        interval: Duration::from_secs(5),
        lookback_delta: Duration::from_secs(1),
    };

    let table_provider = build_test_table_provider_with_tsid(
        &[(DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string())],
        1,
        1,
    )
    .await;
    let plan = PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
        .await
        .unwrap();

    let plan_str = plan.display_indent_schema().to_string();
    assert!(plan_str.contains("PromSeriesDivide: tags=[\"__tsid\"]"));

    let window_line = plan_str
        .lines()
        .find(|line| line.contains("WindowAggr: windowExpr=[[row_number()"))
        .unwrap();
    let partition_by = window_line
        .split("PARTITION BY [")
        .nth(1)
        .and_then(|s| s.split("] ORDER BY").next())
        .unwrap();
    assert!(!partition_by.contains(DATA_SCHEMA_TSID_COLUMN_NAME));
}

#[tokio::test]
async fn selector_matcher_on_tsid_does_not_use_internal_column() {
    let prom_expr = parser::parse(r#"some_metric{__tsid="123"}"#).unwrap();
    let eval_stmt = EvalStmt {
        expr: prom_expr,
        start: UNIX_EPOCH,
        end: UNIX_EPOCH
            .checked_add(Duration::from_secs(100_000))
            .unwrap(),
        interval: Duration::from_secs(5),
        lookback_delta: Duration::from_secs(1),
    };

    let table_provider = build_test_table_provider_with_tsid(
        &[(DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string())],
        1,
        1,
    )
    .await;
    let plan = PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
        .await
        .unwrap();

    fn collect_filter_cols(plan: &LogicalPlan, out: &mut HashSet<Column>) {
        if let LogicalPlan::Filter(filter) = plan {
            datafusion_expr::utils::expr_to_columns(&filter.predicate, out).unwrap();
        }
        for input in plan.inputs() {
            collect_filter_cols(input, out);
        }
    }

    let mut filter_cols = HashSet::new();
    collect_filter_cols(&plan, &mut filter_cols);
    assert!(
        !filter_cols
            .iter()
            .any(|c| c.name == DATA_SCHEMA_TSID_COLUMN_NAME)
    );
}

#[tokio::test]
async fn tsid_is_not_used_when_physical_table_is_missing() {
    let prom_expr = parser::parse("some_metric").unwrap();
    let eval_stmt = EvalStmt {
        expr: prom_expr,
        start: UNIX_EPOCH,
        end: UNIX_EPOCH
            .checked_add(Duration::from_secs(100_000))
            .unwrap(),
        interval: Duration::from_secs(5),
        lookback_delta: Duration::from_secs(1),
    };

    let catalog_list = MemoryCatalogManager::with_default_setup();

    // Register a metric engine logical table referencing a missing physical table.
    let mut columns = vec![ColumnSchema::new(
        "tag_0".to_string(),
        ConcreteDataType::string_datatype(),
        false,
    )];
    columns.push(
        ColumnSchema::new(
            "timestamp".to_string(),
            ConcreteDataType::timestamp_millisecond_datatype(),
            false,
        )
        .with_time_index(true),
    );
    columns.push(ColumnSchema::new(
        "field_0".to_string(),
        ConcreteDataType::float64_datatype(),
        true,
    ));
    let schema = Arc::new(Schema::new(columns));
    let mut options = table::requests::TableOptions::default();
    options
        .extra_options
        .insert(LOGICAL_TABLE_METADATA_KEY.to_string(), "phy".to_string());
    let table_meta = TableMetaBuilder::empty()
        .schema(schema)
        .primary_key_indices(vec![0])
        .value_indices(vec![2])
        .engine(METRIC_ENGINE_NAME.to_string())
        .options(options)
        .next_column_id(1024)
        .build()
        .unwrap();
    let table_info = TableInfoBuilder::default()
        .table_id(1024)
        .name("some_metric")
        .meta(table_meta)
        .build()
        .unwrap();
    let table = EmptyTable::from_table_info(&table_info);
    catalog_list
        .register_table_sync(RegisterTableRequest {
            catalog: DEFAULT_CATALOG_NAME.to_string(),
            schema: DEFAULT_SCHEMA_NAME.to_string(),
            table_name: "some_metric".to_string(),
            table_id: 1024,
            table,
        })
        .unwrap();

    let table_provider = DfTableSourceProvider::new(
        catalog_list,
        false,
        QueryContext::arc(),
        DummyDecoder::arc(),
        false,
    );

    let plan = PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
        .await
        .unwrap();

    let plan_str = plan.display_indent_schema().to_string();
    assert!(plan_str.contains("PromSeriesDivide: tags=[\"tag_0\"]"));
    assert!(!plan_str.contains("PromSeriesDivide: tags=[\"__tsid\"]"));
}

#[tokio::test]
async fn tsid_is_carried_only_when_aggregate_preserves_label_set() {
    let prom_expr = parser::parse("sum by (tag_0) (some_metric)").unwrap();
    let eval_stmt = EvalStmt {
        expr: prom_expr,
        start: UNIX_EPOCH,
        end: UNIX_EPOCH
            .checked_add(Duration::from_secs(100_000))
            .unwrap(),
        interval: Duration::from_secs(5),
        lookback_delta: Duration::from_secs(1),
    };

    let table_provider = build_test_table_provider_with_tsid(
        &[(DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string())],
        1,
        1,
    )
    .await;
    let plan = PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
        .await
        .unwrap();

    let plan_str = plan.display_indent_schema().to_string();
    assert!(plan_str.contains("first_value") && plan_str.contains("__tsid"));
    assert!(
        !plan
            .schema()
            .fields()
            .iter()
            .any(|field| field.name() == DATA_SCHEMA_TSID_COLUMN_NAME)
    );

    // Merging aggregate: label set is reduced, tsid should not be carried.
    let prom_expr = parser::parse("sum(some_metric)").unwrap();
    let eval_stmt = EvalStmt {
        expr: prom_expr,
        start: UNIX_EPOCH,
        end: UNIX_EPOCH
            .checked_add(Duration::from_secs(100_000))
            .unwrap(),
        interval: Duration::from_secs(5),
        lookback_delta: Duration::from_secs(1),
    };
    let table_provider = build_test_table_provider_with_tsid(
        &[(DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string())],
        1,
        1,
    )
    .await;
    let plan = PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
        .await
        .unwrap();
    let plan_str = plan.display_indent_schema().to_string();
    assert!(!plan_str.contains("first_value"));
}

#[tokio::test]
async fn or_operator_with_unknown_metric_does_not_require_tsid() {
    let prom_expr = parser::parse("unknown_metric or some_metric").unwrap();
    let eval_stmt = EvalStmt {
        expr: prom_expr,
        start: UNIX_EPOCH,
        end: UNIX_EPOCH
            .checked_add(Duration::from_secs(100_000))
            .unwrap(),
        interval: Duration::from_secs(5),
        lookback_delta: Duration::from_secs(1),
    };

    let table_provider = build_test_table_provider_with_tsid(
        &[(DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string())],
        1,
        1,
    )
    .await;

    let plan = PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
        .await
        .unwrap();

    assert!(
        !plan
            .schema()
            .fields()
            .iter()
            .any(|field| field.name() == DATA_SCHEMA_TSID_COLUMN_NAME)
    );
}

#[tokio::test]
async fn aggregate_avg() {
    do_aggregate_expr_plan("avg", "avg").await;
}

#[tokio::test]
#[should_panic] // output type doesn't match
async fn aggregate_count() {
    do_aggregate_expr_plan("count", "count").await;
}

#[tokio::test]
async fn aggregate_min() {
    do_aggregate_expr_plan("min", "min").await;
}

#[tokio::test]
async fn aggregate_max() {
    do_aggregate_expr_plan("max", "max").await;
}

#[tokio::test]
async fn aggregate_group() {
    // Regression test for `group()` aggregator.
    // PromQL: sum(group by (cluster)(kubernetes_build_info{service="kubernetes",job="apiserver"}))
    // should be plannable, and `group()` should produce constant 1 for each group.
    let prom_expr = parser::parse(
        "sum(group by (cluster)(kubernetes_build_info{service=\"kubernetes\",job=\"apiserver\"}))",
    )
    .unwrap();
    let eval_stmt = EvalStmt {
        expr: prom_expr,
        start: UNIX_EPOCH,
        end: UNIX_EPOCH
            .checked_add(Duration::from_secs(100_000))
            .unwrap(),
        interval: Duration::from_secs(5),
        lookback_delta: Duration::from_secs(1),
    };

    let table_provider = build_test_table_provider_with_fields(
        &[(
            DEFAULT_SCHEMA_NAME.to_string(),
            "kubernetes_build_info".to_string(),
        )],
        &["cluster", "service", "job"],
    )
    .await;
    let plan = PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
        .await
        .unwrap();

    let plan_str = plan.display_indent_schema().to_string();
    assert!(plan_str.contains("max(Float64(1"));
}

#[tokio::test]
async fn aggregate_stddev() {
    do_aggregate_expr_plan("stddev", "stddev_pop").await;
}

#[tokio::test]
async fn aggregate_stdvar() {
    do_aggregate_expr_plan("stdvar", "var_pop").await;
}

// TODO(ruihang): add range fn tests once exprs are ready.

// {
//     input: "some_metric{tag_0="foo"} + some_metric{tag_0="bar"}",
//     expected: &BinaryExpr{
//         Op: ADD,
//         LHS: &VectorSelector{
//             Name: "a",
//             LabelMatchers: []*labels.Matcher{
//                     MustLabelMatcher(labels.MatchEqual, "tag_0", "foo"),
//                     MustLabelMatcher(labels.MatchEqual, model.MetricNameLabel, "some_metric"),
//             },
//         },
//         RHS: &VectorSelector{
//             Name: "sum",
//             LabelMatchers: []*labels.Matcher{
//                     MustLabelMatcher(labels.MatchxEqual, "tag_0", "bar"),
//                     MustLabelMatcher(labels.MatchEqual, model.MetricNameLabel, "some_metric"),
//             },
//         },
//         VectorMatching: &VectorMatching{},
//     },
// },
#[tokio::test]
async fn binary_op_column_column() {
    let prom_expr =
        parser::parse(r#"some_metric{tag_0="foo"} + some_metric{tag_0="bar"}"#).unwrap();
    let eval_stmt = EvalStmt {
        expr: prom_expr,
        start: UNIX_EPOCH,
        end: UNIX_EPOCH
            .checked_add(Duration::from_secs(100_000))
            .unwrap(),
        interval: Duration::from_secs(5),
        lookback_delta: Duration::from_secs(1),
    };

    let table_provider = build_test_table_provider(
        &[(DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string())],
        1,
        1,
    )
    .await;
    let plan = PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
        .await
        .unwrap();

    let expected = String::from(
        "Projection: rhs.tag_0, rhs.timestamp, lhs.field_0 + rhs.field_0 [tag_0:Utf8, timestamp:Timestamp(ms), lhs.field_0 + rhs.field_0:Float64;N]\
            \n  Projection: rhs.tag_0, rhs.__promql_metric_name, rhs.timestamp, CAST(lhs.field_0 AS Float64) + CAST(rhs.field_0 AS Float64) AS lhs.field_0 + rhs.field_0 [tag_0:Utf8, __promql_metric_name:Utf8, timestamp:Timestamp(ms), lhs.field_0 + rhs.field_0:Float64;N]\
            \n    Inner Join: lhs.tag_0 = rhs.tag_0, lhs.timestamp = rhs.timestamp [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, __promql_metric_name:Utf8, tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, __promql_metric_name:Utf8]\
            \n      SubqueryAlias: lhs [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, __promql_metric_name:Utf8]\
            \n        Projection: some_metric.tag_0, some_metric.timestamp, some_metric.field_0, Utf8(\"some_metric\") AS __promql_metric_name [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, __promql_metric_name:Utf8]\
            \n          PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n            PromSeriesDivide: tags=[\"tag_0\"] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n              Sort: some_metric.tag_0 ASC NULLS FIRST, some_metric.timestamp ASC NULLS FIRST [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n                Filter: some_metric.tag_0 = Utf8(\"foo\") AND some_metric.tag_0 = Utf8(\"bar\") AND some_metric.timestamp >= TimestampMillisecond(-999, None) AND some_metric.timestamp <= TimestampMillisecond(100000000, None) [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n                  TableScan: some_metric [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n      SubqueryAlias: rhs [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, __promql_metric_name:Utf8]\
            \n        Projection: some_metric.tag_0, some_metric.timestamp, some_metric.field_0, __promql_metric_name [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, __promql_metric_name:Utf8]\
            \n          Filter: prom_assert_unique_match_group(__promql_match_group_count, some_metric.tag_0) [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, __promql_metric_name:Utf8, __promql_match_group_count:Int64]\
            \n            WindowAggr: windowExpr=[[count(Int64(1)) PARTITION BY [some_metric.tag_0, some_metric.timestamp] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING AS __promql_match_group_count]] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, __promql_metric_name:Utf8, __promql_match_group_count:Int64]\
            \n              Projection: some_metric.tag_0, some_metric.timestamp, some_metric.field_0, Utf8(\"some_metric\") AS __promql_metric_name [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, __promql_metric_name:Utf8]\
            \n                PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n                  PromSeriesDivide: tags=[\"tag_0\"] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n                    Sort: some_metric.tag_0 ASC NULLS FIRST, some_metric.timestamp ASC NULLS FIRST [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n                      Filter: some_metric.tag_0 = Utf8(\"bar\") AND some_metric.tag_0 = Utf8(\"foo\") AND some_metric.timestamp >= TimestampMillisecond(-999, None) AND some_metric.timestamp <= TimestampMillisecond(100000000, None) [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n                        TableScan: some_metric [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]",
    );

    assert_eq!(plan.display_indent_schema().to_string(), expected);
}

async fn indie_query_plan(query: &str) -> String {
    let prom_expr = parser::parse(query).unwrap();
    let eval_stmt = EvalStmt {
        expr: prom_expr,
        start: UNIX_EPOCH,
        end: UNIX_EPOCH
            .checked_add(Duration::from_secs(100_000))
            .unwrap(),
        interval: Duration::from_secs(5),
        lookback_delta: Duration::from_secs(1),
    };

    let table_provider = build_test_table_provider(
        &[
            (DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string()),
            (
                "greptime_private".to_string(),
                "some_alt_metric".to_string(),
            ),
        ],
        1,
        1,
    )
    .await;
    let plan = PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
        .await
        .unwrap();

    plan.display_indent_schema().to_string()
}

async fn indie_query_plan_compare<T: AsRef<str>>(query: &str, expected: T) {
    let plan = indie_query_plan(query).await;
    assert_eq!(plan, expected.as_ref());
}

#[tokio::test]
async fn binary_op_literal_column() {
    let query = r#"1 + some_metric{tag_0="bar"}"#;
    let expected = String::from(
        "Projection: some_metric.tag_0, some_metric.timestamp, Float64(1) + field_0 [tag_0:Utf8, timestamp:Timestamp(ms), Float64(1) + field_0:Float64;N]\
            \n  Projection: some_metric.tag_0, __promql_metric_name, some_metric.timestamp, Float64(1) + CAST(some_metric.field_0 AS Float64) AS Float64(1) + field_0 [tag_0:Utf8, __promql_metric_name:Utf8, timestamp:Timestamp(ms), Float64(1) + field_0:Float64;N]\
            \n    Projection: some_metric.tag_0, some_metric.timestamp, some_metric.field_0, Utf8(\"some_metric\") AS __promql_metric_name [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, __promql_metric_name:Utf8]\
            \n      PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n        PromSeriesDivide: tags=[\"tag_0\"] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n          Sort: some_metric.tag_0 ASC NULLS FIRST, some_metric.timestamp ASC NULLS FIRST [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n            Filter: some_metric.tag_0 = Utf8(\"bar\") AND some_metric.timestamp >= TimestampMillisecond(-999, None) AND some_metric.timestamp <= TimestampMillisecond(100000000, None) [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n              TableScan: some_metric [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]",
    );

    indie_query_plan_compare(query, expected).await;
}

#[tokio::test]
async fn binary_op_literal_literal() {
    let query = r#"1 + 1"#;
    let expected = r#"EmptyMetric: range=[0..100000000], interval=[5000] [time:Timestamp(ms), value:Float64;N]
  TableScan: dummy [time:Timestamp(ms), value:Float64;N]"#;
    indie_query_plan_compare(query, expected).await;
}

#[tokio::test]
async fn simple_bool_grammar() {
    let query = "some_metric != bool 1.2345";
    let expected = String::from(
        "Projection: some_metric.tag_0, some_metric.timestamp, field_0 != Float64(1.2345) [tag_0:Utf8, timestamp:Timestamp(ms), field_0 != Float64(1.2345):Float64;N]\
            \n  Projection: some_metric.tag_0, __promql_metric_name, some_metric.timestamp, CAST(some_metric.field_0 != Float64(1.2345) AS Float64) AS field_0 != Float64(1.2345) [tag_0:Utf8, __promql_metric_name:Utf8, timestamp:Timestamp(ms), field_0 != Float64(1.2345):Float64;N]\
            \n    Projection: some_metric.tag_0, some_metric.timestamp, some_metric.field_0, Utf8(\"some_metric\") AS __promql_metric_name [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, __promql_metric_name:Utf8]\
            \n      PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n        PromSeriesDivide: tags=[\"tag_0\"] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n          Sort: some_metric.tag_0 ASC NULLS FIRST, some_metric.timestamp ASC NULLS FIRST [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n            Filter: some_metric.timestamp >= TimestampMillisecond(-999, None) AND some_metric.timestamp <= TimestampMillisecond(100000000, None) [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n              TableScan: some_metric [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]",
    );

    indie_query_plan_compare(query, expected).await;
}

#[tokio::test]
async fn bool_with_additional_arithmetic() {
    let query = "some_metric + (1 == bool 2)";
    let expected = String::from(
        "Projection: some_metric.tag_0, some_metric.timestamp, field_0 + Float64(1) = Float64(2) [tag_0:Utf8, timestamp:Timestamp(ms), field_0 + Float64(1) = Float64(2):Float64;N]\
            \n  Projection: some_metric.tag_0, __promql_metric_name, some_metric.timestamp, CAST(some_metric.field_0 AS Float64) + CAST(Float64(1) = Float64(2) AS Float64) AS field_0 + Float64(1) = Float64(2) [tag_0:Utf8, __promql_metric_name:Utf8, timestamp:Timestamp(ms), field_0 + Float64(1) = Float64(2):Float64;N]\
            \n    Projection: some_metric.tag_0, some_metric.timestamp, some_metric.field_0, Utf8(\"some_metric\") AS __promql_metric_name [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, __promql_metric_name:Utf8]\
            \n      PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n        PromSeriesDivide: tags=[\"tag_0\"] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n          Sort: some_metric.tag_0 ASC NULLS FIRST, some_metric.timestamp ASC NULLS FIRST [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n            Filter: some_metric.timestamp >= TimestampMillisecond(-999, None) AND some_metric.timestamp <= TimestampMillisecond(100000000, None) [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n              TableScan: some_metric [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]",
    );

    indie_query_plan_compare(query, expected).await;
}

#[tokio::test]
async fn simple_unary() {
    let query = "-some_metric";
    let expected = String::from(
        "Projection: some_metric.tag_0, some_metric.timestamp, (- field_0) [tag_0:Utf8, timestamp:Timestamp(ms), (- field_0):Float64;N]\
            \n  Projection: some_metric.tag_0, __promql_metric_name, some_metric.timestamp, (- some_metric.field_0) AS (- field_0) [tag_0:Utf8, __promql_metric_name:Utf8, timestamp:Timestamp(ms), (- field_0):Float64;N]\
            \n    Projection: some_metric.tag_0, some_metric.timestamp, some_metric.field_0, Utf8(\"some_metric\") AS __promql_metric_name [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, __promql_metric_name:Utf8]\
            \n      PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n        PromSeriesDivide: tags=[\"tag_0\"] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n          Sort: some_metric.tag_0 ASC NULLS FIRST, some_metric.timestamp ASC NULLS FIRST [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n            Filter: some_metric.timestamp >= TimestampMillisecond(-999, None) AND some_metric.timestamp <= TimestampMillisecond(100000000, None) [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n              TableScan: some_metric [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]",
    );

    indie_query_plan_compare(query, expected).await;
}

#[tokio::test]
async fn increase_aggr() {
    let query = "increase(some_metric[5m])";
    let expected = String::from(
        "Projection: some_metric.timestamp, prom_increase(timestamp_range,field_0,timestamp,Int64(300000)), some_metric.tag_0 [timestamp:Timestamp(ms), prom_increase(timestamp_range,field_0,timestamp,Int64(300000)):Float64;N, tag_0:Utf8]\
            \n  Filter: prom_increase(timestamp_range,field_0,timestamp,Int64(300000)) IS NOT NULL [timestamp:Timestamp(ms), prom_increase(timestamp_range,field_0,timestamp,Int64(300000)):Float64;N, tag_0:Utf8, __promql_metric_name:Utf8]\
            \n    Projection: some_metric.timestamp, prom_increase(timestamp_range, field_0, some_metric.timestamp, Int64(300000)) AS prom_increase(timestamp_range,field_0,timestamp,Int64(300000)), some_metric.tag_0, __promql_metric_name [timestamp:Timestamp(ms), prom_increase(timestamp_range,field_0,timestamp,Int64(300000)):Float64;N, tag_0:Utf8, __promql_metric_name:Utf8]\
            \n      Projection: some_metric.tag_0, some_metric.timestamp, field_0, timestamp_range, Utf8(\"some_metric\") AS __promql_metric_name [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Dictionary(Int64, Float64);N, timestamp_range:Dictionary(Int64, Timestamp(ms)), __promql_metric_name:Utf8]\
            \n        PromRangeManipulate: req range=[0..100000000], interval=[5000], eval range=[300000], time index=[timestamp], values=[\"field_0\"] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Dictionary(Int64, Float64);N, timestamp_range:Dictionary(Int64, Timestamp(ms))]\
            \n          PromSeriesNormalize: offset=[0], time index=[timestamp], filter NaN: [true] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n            PromSeriesDivide: tags=[\"tag_0\"] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n              Sort: some_metric.tag_0 ASC NULLS FIRST, some_metric.timestamp ASC NULLS FIRST [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n                Filter: some_metric.timestamp >= TimestampMillisecond(-299999, None) AND some_metric.timestamp <= TimestampMillisecond(100000000, None) [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n                  TableScan: some_metric [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]",
    );

    indie_query_plan_compare(query, expected).await;
}

#[tokio::test]
async fn predict_linear_injects_the_eval_timestamp() {
    // The fourth argument is the evaluation instant of each row, which the fold offset
    // recovers from the row's time index (here: no `@` and no `offset`, so the step itself).
    let query = "predict_linear(some_metric[5m], 60)";
    let expected = String::from(
        "Filter: prom_predict_linear(timestamp_range,field_0,Float64(60)) IS NOT NULL [timestamp:Timestamp(ms), prom_predict_linear(timestamp_range,field_0,Float64(60)):Float64;N, tag_0:Utf8]\
        \n  Projection: some_metric.timestamp, prom_predict_linear(timestamp_range, field_0, CAST(Float64(60) AS Int64), CAST(CAST(some_metric.timestamp AS Int64) + Int64(0) AS Timestamp(ms))) AS prom_predict_linear(timestamp_range,field_0,Float64(60)), some_metric.tag_0 [timestamp:Timestamp(ms), prom_predict_linear(timestamp_range,field_0,Float64(60)):Float64;N, tag_0:Utf8]\
        \n    PromRangeManipulate: req range=[0..100000000], interval=[5000], eval range=[300000], time index=[timestamp], values=[\"field_0\"] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Dictionary(Int64, Float64);N, timestamp_range:Dictionary(Int64, Timestamp(ms))]\
        \n      PromSeriesNormalize: offset=[0], time index=[timestamp], filter NaN: [true] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
        \n        PromSeriesDivide: tags=[\"tag_0\"] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
        \n          Sort: some_metric.tag_0 ASC NULLS FIRST, some_metric.timestamp ASC NULLS FIRST [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
        \n            Filter: some_metric.timestamp >= TimestampMillisecond(-299999, None) AND some_metric.timestamp <= TimestampMillisecond(100000000, None) [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
        \n              TableScan: some_metric [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]",
    );

    indie_query_plan_compare(query, expected).await;
}

/// The evaluation instant follows the selector's fold offset, not the projection's: an
/// `@`-anchored selector is folded with `at_offset`, which is what the window's timestamps were
/// shifted by. The evaluation starts at 0s here, so `@ 100` anchors 100s in the future and
/// yields `at_offset` = -100000ms.
#[tokio::test]
async fn predict_linear_eval_ts_follows_the_fold_offset() {
    for (query, expected_offset) in [
        (
            "predict_linear(some_metric[5m] @ 100, 60)",
            "Int64(-100000)",
        ),
        (
            "predict_linear(some_metric[5m] offset 2m, 60)",
            "Int64(120000)",
        ),
    ] {
        let plan = indie_query_plan(query).await;
        assert!(
            plan.contains(&format!(
                "some_metric.timestamp AS Int64) + {expected_offset}"
            )),
            "{query}\n{plan}"
        );
    }
}

async fn native_histogram_plan(query: &str) -> String {
    let table_provider = build_test_native_histogram_table_provider("some_metric").await;
    let plan = PromPlanner::stmt_to_plan(
        table_provider,
        &build_eval_stmt(query),
        &build_query_engine_state(),
    )
    .await
    .unwrap();
    plan.display_indent_schema().to_string()
}

#[tokio::test]
async fn native_histogram_count_uses_native_udf() {
    let plan = native_histogram_plan("histogram_count(some_metric)").await;

    assert!(plan.contains("prom_native_histogram_count"), "{plan}");
    assert!(!plan.contains("HistogramFold:"), "{plan}");
}

#[tokio::test]
async fn timestamp_filters_native_histogram_stale_marker_before_projection() {
    let mut stale = direct_or_histogram();
    stale.sum = f64::from_bits(PROMETHEUS_STALE_NAN_BITS);
    let table = operator_metric_table(
        "stale_histogram",
        2_100,
        "a",
        None,
        DirectOrValue::NativeHistogram(stale),
    );
    let catalog = MemoryCatalogManager::with_default_setup();
    catalog
        .register_table_sync(RegisterTableRequest {
            catalog: DEFAULT_CATALOG_NAME.to_string(),
            schema: DEFAULT_SCHEMA_NAME.to_string(),
            table_name: "stale_histogram".to_string(),
            table_id: 2_100,
            table,
        })
        .unwrap();
    let provider = DfTableSourceProvider::new(
        catalog,
        false,
        QueryContext::arc(),
        DummyDecoder::arc(),
        false,
    );
    let state = build_query_engine_state();
    let plan = PromPlanner::stmt_to_plan(
        provider,
        &operator_eval_stmt("timestamp(stale_histogram)"),
        &state,
    )
    .await
    .unwrap();
    let plan_text = plan.display_indent_schema().to_string();
    assert!(plan_text.contains(TIMESTAMP_VALUE_PREFIX), "{plan_text}");

    let (_, batches) = execute(plan, &state).await;
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
}

#[tokio::test]
async fn timestamp_filters_stale_marker_from_mixed_sample_companion() {
    let histograms = build_histogram_array(&[None]);
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new(
            "timestamp",
            ArrowDataType::Timestamp(ArrowTimeUnit::Millisecond, None),
            false,
        ),
        Field::new(
            greptime_native_histogram(),
            histograms.data_type().clone(),
            true,
        ),
        Field::new(greptime_value(), ArrowDataType::Float64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(TimestampMillisecondArray::from(vec![1_000])),
            histograms,
            Arc::new(Float64Array::from(vec![f64::from_bits(
                PROMETHEUS_STALE_NAN_BITS,
            )])),
        ],
    )
    .unwrap();
    let table = Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap());
    let input = LogicalPlanBuilder::scan("mixed", provider_as_source(table), None)
        .unwrap()
        .build()
        .unwrap();
    let input = LogicalPlan::Extension(Extension {
        node: Arc::new(SeriesDivide::new(
            Vec::new(),
            "timestamp".to_string(),
            input,
        )),
    });
    let input = LogicalPlan::Extension(Extension {
        node: Arc::new(InstantManipulate::new(
            1_000,
            1_000,
            5_000,
            1_000,
            0,
            "timestamp".to_string(),
            Vec::new(),
            Some(greptime_native_histogram().to_string()),
            input,
        )),
    });
    // Match timestamp()'s parent projection, which otherwise prunes the companion lane.
    let plan = LogicalPlanBuilder::from(input)
        .project([col("timestamp")])
        .unwrap()
        .build()
        .unwrap();

    let (_, batches) = execute(plan, &build_query_engine_state()).await;
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
}

#[tokio::test]
async fn native_histogram_rate_can_feed_count() {
    let plan = native_histogram_plan("histogram_count(rate(some_metric[5m]))").await;

    assert!(plan.contains("prom_native_histogram_rate"), "{plan}");
    assert!(plan.contains("prom_native_histogram_count"), "{plan}");
}

#[tokio::test]
async fn native_histogram_quantile_skips_classic_fold() {
    let plan = native_histogram_plan("histogram_quantile(0.9, some_metric)").await;

    assert!(plan.contains("prom_native_histogram_quantile"), "{plan}");
    assert!(!plan.contains("HistogramFold:"), "{plan}");
    assert!(plan.contains("some_metric.le"), "{plan}");
    // The phi literal is threaded into the native quantile UDF as its second argument.
    assert!(plan.contains("Float64(0.9)"), "{plan}");
    // The empty-values filter drops NULL quantile results so the output is empty
    // when all native histogram samples are dropped.
    assert!(plan.contains("IS NOT NULL"), "{plan}");
}

#[tokio::test]
async fn mixed_native_histogram_quantile_uses_histogram_field() {
    let table_provider = build_test_mixed_native_histogram_table_provider("some_metric").await;
    let plan = PromPlanner::stmt_to_plan(
        table_provider,
        &build_eval_stmt("histogram_quantile(0.9, some_metric)"),
        &build_query_engine_state(),
    )
    .await
    .unwrap()
    .display_indent_schema()
    .to_string();

    assert!(
        plan.contains("prom_native_histogram_quantile(greptime_native_histogram"),
        "{plan}"
    );
    assert!(!plan.contains("EmptyRelation"), "{plan}");
}

#[tokio::test]
async fn mixed_histogram_helpers_execute_classic_and_native_samples() {
    let state = build_query_engine_state();
    for (query, expected) in [
        (
            "histogram_quantile(0.5, mixed_histogram)",
            vec![("classic", 1.0), ("native", 0.0)],
        ),
        (
            "histogram_fraction(-Inf, +Inf, mixed_histogram)",
            vec![("classic", 1.0), ("native", 1.0)],
        ),
    ] {
        let plan = PromPlanner::stmt_to_plan(
            classic_and_native_histogram_table_provider("native", None, direct_or_histogram()),
            &operator_eval_stmt(query),
            &state,
        )
        .await
        .unwrap();
        let plan_text = plan.display_indent_schema().to_string();
        assert!(plan_text.contains("HistogramFold:"), "{plan_text}");
        assert!(plan_text.contains("prom_native_histogram_"), "{plan_text}");
        let value_field = plan
            .schema()
            .fields()
            .iter()
            .find(|field| field.data_type() == &ArrowDataType::Float64)
            .unwrap()
            .name()
            .clone();

        let (_, batches) = execute(plan, &state).await;
        let mut actual = batches
            .iter()
            .flat_map(|batch| {
                let tags = batch
                    .column_by_name("service.name")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let values = batch
                    .column_by_name(&value_field)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();
                (0..batch.num_rows()).map(|row| (tags.value(row), values.value(row)))
            })
            .collect::<Vec<_>>();
        actual.sort_by_key(|(tag, _)| *tag);
        assert_eq!(actual, expected, "{query}");
    }
}

#[tokio::test]
async fn mixed_histogram_helpers_report_annotations() {
    let state = build_query_engine_state();
    let mut native_histogram = direct_or_histogram();
    native_histogram.count = 2.0;
    native_histogram.sum = f64::NAN;
    for (native_tag, expected_rows, expected_warnings, expected_infos) in [
        (
            "classic",
            0,
            vec!["vector contains a mix of classic and native histograms"],
            vec![],
        ),
        (
            "native",
            2,
            vec![],
            vec!["input to histogram_quantile has NaN observations, result is skewed higher"],
        ),
    ] {
        let collector = PromqlAnnotationCollector::default();
        let plan = PromPlanner::stmt_to_plan_with_annotations(
            classic_and_native_histogram_table_provider(native_tag, None, native_histogram.clone()),
            &operator_eval_stmt("histogram_quantile(0.5, mixed_histogram)"),
            &state,
            Some(collector.clone()),
        )
        .await
        .unwrap();

        let (_, batches) = execute(plan, &state).await;
        assert_eq!(
            batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
            expected_rows
        );
        let mut warnings = vec![];
        let mut infos = vec![];
        collector.append_to(&mut warnings, &mut infos);
        assert_eq!(warnings, expected_warnings);
        assert_eq!(infos, expected_infos);
    }
}

#[tokio::test]
async fn mixed_histogram_helper_preserves_native_le_and_scans_once() {
    let state = build_query_engine_state();
    let mut stmt = operator_eval_stmt("histogram_quantile(0.5, mixed_histogram)");
    stmt.end = UNIX_EPOCH.checked_add(Duration::from_secs(2)).unwrap();
    let plan = PromPlanner::stmt_to_plan(
        classic_and_native_histogram_table_provider(
            "classic",
            Some("native"),
            direct_or_histogram(),
        ),
        &stmt,
        &state,
    )
    .await
    .unwrap();
    let plan_text = plan.display_indent_schema().to_string();
    assert_eq!(
        plan_text.matches("TableScan: mixed_histogram").count(),
        1,
        "{plan_text}"
    );

    let value_field = plan
        .schema()
        .fields()
        .iter()
        .find(|field| field.data_type() == &ArrowDataType::Float64)
        .unwrap()
        .name()
        .clone();
    let (_, batches) = execute(plan, &state).await;
    let mut actual = batches
        .iter()
        .flat_map(|batch| {
            let le = batch
                .column_by_name(LE_COLUMN_NAME)
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let timestamps = batch
                .column_by_name("timestamp")
                .unwrap()
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
            let values = batch
                .column_by_name(&value_field)
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            (0..batch.num_rows()).map(|row| {
                (
                    timestamps.value(row),
                    (!le.is_null(row)).then(|| le.value(row).to_string()),
                    values.value(row),
                )
            })
        })
        .collect::<Vec<_>>();
    actual.sort_by(|lhs, rhs| (lhs.0, &lhs.1).cmp(&(rhs.0, &rhs.1)));
    assert_eq!(
        actual,
        vec![
            (1_000, None, 1.0),
            (1_000, Some("native".to_string()), 0.0),
            (2_000, None, 1.0),
            (2_000, Some("native".to_string()), 0.0),
        ]
    );
}

#[tokio::test]
async fn nested_histogram_helpers_ignore_unparsable_bucket_labels() {
    let state = build_query_engine_state();
    for native_le in [None, Some("native")] {
        for query in [
            "histogram_quantile(0.5, histogram_quantile(0.5, mixed_histogram))",
            "histogram_fraction(-Inf, +Inf, histogram_fraction(-Inf, +Inf, mixed_histogram))",
        ] {
            let plan = PromPlanner::stmt_to_plan(
                classic_and_native_histogram_table_provider(
                    "native",
                    native_le,
                    direct_or_histogram(),
                ),
                &operator_eval_stmt(query),
                &state,
            )
            .await
            .unwrap();

            let (_, batches) = execute(plan, &state).await;
            assert_eq!(
                batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
                0,
                "native_le={native_le:?}, query={query}"
            );
        }
    }
}

#[tokio::test]
async fn native_histogram_quantile_rejects_multi_field_input() {
    let table_provider = build_test_multi_histogram_table_provider("some_metric").await;
    let result = PromPlanner::stmt_to_plan(
        table_provider,
        &build_eval_stmt("histogram_quantile(0.9, some_metric)"),
        &build_query_engine_state(),
    )
    .await;

    let err = result.expect_err("histogram_quantile on two native histogram fields must fail");
    assert!(
        err.to_string()
            .contains("Multi fields calculation is not supported in histogram_quantile"),
        "{err}"
    );
}

#[tokio::test]
async fn native_histogram_topk_uses_drop_udf() {
    let plan = native_histogram_plan("topk(1, some_metric)").await;

    assert!(plan.contains("prom_native_histogram_drop_float"), "{plan}");
    assert!(
        plan.contains("Filter: prom_native_histogram_drop_float") && plan.contains("IS NOT NULL"),
        "{plan}"
    );
}

#[tokio::test]
async fn mixed_or_topk_bottomk_ignore_native_histograms() {
    for op in ["topk", "bottomk"] {
        let collector = PromqlAnnotationCollector::default();
        let state = build_query_engine_state();
        let plan = PromPlanner::stmt_to_plan_with_annotations(
            operator_table_provider(),
            &operator_eval_stmt(&format!("{op}(1, lf or on(tag) lh)")),
            &state,
            Some(collector.clone()),
        )
        .await
        .unwrap();
        let float_field = plan
            .schema()
            .fields()
            .iter()
            .find(|field| field.data_type() == &ArrowDataType::Float64)
            .unwrap()
            .name()
            .clone();
        assert!(
            plan.schema()
                .fields()
                .iter()
                .all(|field| field.data_type() != &PromPlanner::native_histogram_arrow_type()),
            "{plan:?}"
        );

        let (_, batches) = execute(plan, &state).await;
        assert_eq!(values(&batches, &float_field), vec![2.0], "{op}");
        let mut warnings = vec![];
        let mut infos = vec![];
        collector.append_to(&mut warnings, &mut infos);
        assert!(warnings.is_empty());
        assert_eq!(
            infos,
            vec![format!(
                "{op}: dropped native histogram samples because this aggregation is not supported for native histograms"
            )]
        );
    }
}

#[tokio::test]
async fn native_histogram_scalar_is_ignored_before_scalar_calculate() {
    let plan = native_histogram_plan("scalar(some_metric)").await;

    assert!(plan.contains("ScalarCalculate"), "{plan}");
    assert!(plan.contains("Filter: Boolean(false)"), "{plan}");
    assert!(!plan.contains("prom_native_histogram_drop"), "{plan}");
}

#[tokio::test]
async fn native_histogram_value_sort_is_empty_but_label_sort_preserves_samples() {
    for function in ["sort", "sort_desc"] {
        let plan = native_histogram_plan(&format!("{function}(some_metric)")).await;

        assert!(plan.contains("Float64(NULL) IS NOT NULL"), "{plan}");
        assert!(
            !plan.contains(&format!("Sort: {}", greptime_native_histogram())),
            "{plan}"
        );
        assert!(!plan.contains("prom_native_histogram_drop"), "{plan}");
    }

    for (function, direction) in [("sort_by_label", "ASC"), ("sort_by_label_desc", "DESC")] {
        let plan = native_histogram_plan(&format!("{function}(some_metric, \"tag_0\")")).await;

        assert!(plan.contains(&format!("tag_0 {direction}")), "{plan}");
        assert!(plan.contains(greptime_native_histogram()), "{plan}");
        assert!(!plan.contains("Float64(NULL) IS NOT NULL"), "{plan}");
    }
}

#[tokio::test]
async fn unsupported_native_histogram_functions_use_drop_udf() {
    for query in [
        "deriv(some_metric[5m])",
        "min_over_time(some_metric[5m])",
        "quantile_over_time(0.9, some_metric[5m])",
        "predict_linear(some_metric[5m], 60)",
        "round(some_metric)",
        "abs(some_metric)",
    ] {
        let plan = native_histogram_plan(query).await;

        assert!(
            plan.contains("prom_native_histogram_drop_float"),
            "{query}\n{plan}"
        );
    }
}

#[tokio::test]
async fn native_histogram_absent_over_time_uses_native_udf() {
    let plan = native_histogram_plan("absent_over_time(some_metric[5m])").await;

    assert!(
        plan.contains("prom_native_histogram_absent_over_time"),
        "{plan}"
    );
}

#[tokio::test]
async fn native_histogram_all_function_arms_route_correctly() {
    // Every native-histogram match arm in `create_function_expr` must route to the
    // expected UDF when all field columns are native histograms. `holt_winters` shares
    // the `double_exponential_smoothing` arm but is not registered in the promql
    // parser (0.10), so it cannot be exercised through a query string.
    let cases = [
        // Range functions routed to native histogram UDFs.
        (
            "increase(some_metric[5m])",
            "prom_native_histogram_increase",
        ),
        ("rate(some_metric[5m])", "prom_native_histogram_rate"),
        ("delta(some_metric[5m])", "prom_native_histogram_delta"),
        ("idelta(some_metric[5m])", "prom_native_histogram_idelta"),
        ("irate(some_metric[5m])", "prom_native_histogram_irate"),
        ("resets(some_metric[5m])", "prom_native_histogram_resets"),
        ("changes(some_metric[5m])", "prom_native_histogram_changes"),
        (
            "avg_over_time(some_metric[5m])",
            "prom_native_histogram_avg_over_time",
        ),
        (
            "sum_over_time(some_metric[5m])",
            "prom_native_histogram_sum_over_time",
        ),
        (
            "count_over_time(some_metric[5m])",
            "prom_native_histogram_count_over_time",
        ),
        (
            "last_over_time(some_metric[5m])",
            "prom_native_histogram_last_over_time",
        ),
        (
            "present_over_time(some_metric[5m])",
            "prom_native_histogram_present_over_time",
        ),
        // Unsupported functions dropped with the float-null UDF.
        ("deriv(some_metric[5m])", "prom_native_histogram_drop_float"),
        (
            "min_over_time(some_metric[5m])",
            "prom_native_histogram_drop_float",
        ),
        (
            "max_over_time(some_metric[5m])",
            "prom_native_histogram_drop_float",
        ),
        (
            "stddev_over_time(some_metric[5m])",
            "prom_native_histogram_drop_float",
        ),
        (
            "stdvar_over_time(some_metric[5m])",
            "prom_native_histogram_drop_float",
        ),
        (
            "quantile_over_time(0.9, some_metric[5m])",
            "prom_native_histogram_drop_float",
        ),
        (
            "predict_linear(some_metric[5m], 60)",
            "prom_native_histogram_drop_float",
        ),
        (
            "double_exponential_smoothing(some_metric[5m], 0.5, 0.5)",
            "prom_native_histogram_drop_float",
        ),
        ("round(some_metric)", "prom_native_histogram_drop_float"),
        ("rad(some_metric)", "prom_native_histogram_drop_float"),
        ("deg(some_metric)", "prom_native_histogram_drop_float"),
        ("sgn(some_metric)", "prom_native_histogram_drop_float"),
        // Instant helper functions routed to native histogram UDFs.
        (
            "histogram_count(some_metric)",
            "prom_native_histogram_count",
        ),
        ("histogram_sum(some_metric)", "prom_native_histogram_sum"),
        ("histogram_avg(some_metric)", "prom_native_histogram_avg"),
        (
            "histogram_stddev(some_metric)",
            "prom_native_histogram_stddev",
        ),
        (
            "histogram_stdvar(some_metric)",
            "prom_native_histogram_stdvar",
        ),
        (
            "histogram_fraction(-2 + 1, 2 / 2, some_metric)",
            "prom_native_histogram_fraction",
        ),
    ];

    for (query, expected_udf) in cases {
        let plan = native_histogram_plan(query).await;
        assert!(plan.contains(expected_udf), "{query}\n{plan}");
        if query.starts_with("histogram_fraction") {
            assert!(plan.contains("Float64(-1)"), "{query}\n{plan}");
        }
    }
}

#[tokio::test]
async fn mixed_native_histogram_ranges_use_coordinated_udfs() {
    let dual_output = [
        "increase(some_metric[5m])",
        "rate(some_metric[5m])",
        "delta(some_metric[5m])",
        "idelta(some_metric[5m])",
        "irate(some_metric[5m])",
        "avg_over_time(some_metric[5m])",
        "sum_over_time(some_metric[5m])",
        "last_over_time(some_metric[5m])",
    ];
    let float_output = [
        "resets(some_metric[5m])",
        "changes(some_metric[5m])",
        "deriv(some_metric[5m])",
        "min_over_time(some_metric[5m])",
        "max_over_time(some_metric[5m])",
        "count_over_time(some_metric[5m])",
        "absent_over_time(some_metric[5m])",
        "present_over_time(some_metric[5m])",
        "stddev_over_time(some_metric[5m])",
        "stdvar_over_time(some_metric[5m])",
        "quantile_over_time(0.9, some_metric[5m])",
        "predict_linear(some_metric[5m], 60)",
        "double_exponential_smoothing(some_metric[5m], 0.5, 0.5)",
    ];

    for query in dual_output.iter().chain(float_output.iter()) {
        let plan = PromPlanner::stmt_to_plan(
            build_test_mixed_native_histogram_table_provider("some_metric").await,
            &build_eval_stmt(query),
            &build_query_engine_state(),
        )
        .await
        .unwrap()
        .display_indent_schema()
        .to_string();
        assert!(plan.contains("prom_mixed_range_float"), "{query}\n{plan}");
        assert_eq!(
            plan.contains("prom_mixed_range_histogram"),
            dual_output.contains(query),
            "{query}\n{plan}"
        );
    }

    let plan = PromPlanner::stmt_to_plan(
        build_test_mixed_native_histogram_table_provider("some_metric").await,
        &build_eval_stmt("sum_over_time(rate(some_metric[5m])[10m:1m])"),
        &build_query_engine_state(),
    )
    .await
    .unwrap()
    .display_indent_schema()
    .to_string();
    let expected = r#"Filter: greptime_value IS NOT NULL OR greptime_native_histogram IS NOT NULL [timestamp:Timestamp(ms), greptime_value:Float64;N, greptime_native_histogram:Struct("schema": Int32, "zero_threshold": Float64, "sum": Float64, "reset_hint": Int32, "start_timestamp": Timestamp(ms), "custom_values": List(Float64), "positive_span_offsets": List(Int32), "positive_span_lengths": List(Int32), "negative_span_offsets": List(Int32), "negative_span_lengths": List(Int32), "count_i64": Int64, "zero_count_i64": Int64, "positive_buckets_i64": List(Int64), "negative_buckets_i64": List(Int64), "count_f64": Float64, "zero_count_f64": Float64, "positive_buckets_f64": List(Float64), "negative_buckets_f64": List(Float64));N, tag_0:Utf8]
  Projection: some_metric.timestamp, prom_mixed_range_float(Utf8("sum_over_time"), timestamp_range, greptime_value, greptime_native_histogram) AS greptime_value, prom_mixed_range_histogram(Utf8("sum_over_time"), timestamp_range, greptime_value, greptime_native_histogram) AS greptime_native_histogram, some_metric.tag_0 [timestamp:Timestamp(ms), greptime_value:Float64;N, greptime_native_histogram:Struct("schema": Int32, "zero_threshold": Float64, "sum": Float64, "reset_hint": Int32, "start_timestamp": Timestamp(ms), "custom_values": List(Float64), "positive_span_offsets": List(Int32), "positive_span_lengths": List(Int32), "negative_span_offsets": List(Int32), "negative_span_lengths": List(Int32), "count_i64": Int64, "zero_count_i64": Int64, "positive_buckets_i64": List(Int64), "negative_buckets_i64": List(Int64), "count_f64": Float64, "zero_count_f64": Float64, "positive_buckets_f64": List(Float64), "negative_buckets_f64": List(Float64));N, tag_0:Utf8]
    PromRangeManipulate: req range=[0..100000000], interval=[5000], eval range=[600000], time index=[timestamp], values=["greptime_value", "greptime_native_histogram"] [timestamp:Timestamp(ms), greptime_value:Dictionary(Int64, Float64);N, greptime_native_histogram:Dictionary(Int64, Struct("schema": Int32, "zero_threshold": Float64, "sum": Float64, "reset_hint": Int32, "start_timestamp": Timestamp(ms), "custom_values": List(Float64), "positive_span_offsets": List(Int32), "positive_span_lengths": List(Int32), "negative_span_offsets": List(Int32), "negative_span_lengths": List(Int32), "count_i64": Int64, "zero_count_i64": Int64, "positive_buckets_i64": List(Int64), "negative_buckets_i64": List(Int64), "count_f64": Float64, "zero_count_f64": Float64, "positive_buckets_f64": List(Float64), "negative_buckets_f64": List(Float64)));N, tag_0:Utf8, timestamp_range:Dictionary(Int64, Timestamp(ms))]
      PromSeriesDivide: tags=["tag_0"] [timestamp:Timestamp(ms), greptime_value:Float64;N, greptime_native_histogram:Struct("schema": Int32, "zero_threshold": Float64, "sum": Float64, "reset_hint": Int32, "start_timestamp": Timestamp(ms), "custom_values": List(Float64), "positive_span_offsets": List(Int32), "positive_span_lengths": List(Int32), "negative_span_offsets": List(Int32), "negative_span_lengths": List(Int32), "count_i64": Int64, "zero_count_i64": Int64, "positive_buckets_i64": List(Int64), "negative_buckets_i64": List(Int64), "count_f64": Float64, "zero_count_f64": Float64, "positive_buckets_f64": List(Float64), "negative_buckets_f64": List(Float64));N, tag_0:Utf8]
        Sort: some_metric.tag_0 ASC NULLS FIRST, some_metric.timestamp ASC NULLS FIRST [timestamp:Timestamp(ms), greptime_value:Float64;N, greptime_native_histogram:Struct("schema": Int32, "zero_threshold": Float64, "sum": Float64, "reset_hint": Int32, "start_timestamp": Timestamp(ms), "custom_values": List(Float64), "positive_span_offsets": List(Int32), "positive_span_lengths": List(Int32), "negative_span_offsets": List(Int32), "negative_span_lengths": List(Int32), "count_i64": Int64, "zero_count_i64": Int64, "positive_buckets_i64": List(Int64), "negative_buckets_i64": List(Int64), "count_f64": Float64, "zero_count_f64": Float64, "positive_buckets_f64": List(Float64), "negative_buckets_f64": List(Float64));N, tag_0:Utf8]
          Projection: some_metric.timestamp, greptime_value, greptime_native_histogram, some_metric.tag_0 [timestamp:Timestamp(ms), greptime_value:Float64;N, greptime_native_histogram:Struct("schema": Int32, "zero_threshold": Float64, "sum": Float64, "reset_hint": Int32, "start_timestamp": Timestamp(ms), "custom_values": List(Float64), "positive_span_offsets": List(Int32), "positive_span_lengths": List(Int32), "negative_span_offsets": List(Int32), "negative_span_lengths": List(Int32), "count_i64": Int64, "zero_count_i64": Int64, "positive_buckets_i64": List(Int64), "negative_buckets_i64": List(Int64), "count_f64": Float64, "zero_count_f64": Float64, "positive_buckets_f64": List(Float64), "negative_buckets_f64": List(Float64));N, tag_0:Utf8]
            Filter: greptime_value IS NOT NULL OR greptime_native_histogram IS NOT NULL [timestamp:Timestamp(ms), greptime_value:Float64;N, greptime_native_histogram:Struct("schema": Int32, "zero_threshold": Float64, "sum": Float64, "reset_hint": Int32, "start_timestamp": Timestamp(ms), "custom_values": List(Float64), "positive_span_offsets": List(Int32), "positive_span_lengths": List(Int32), "negative_span_offsets": List(Int32), "negative_span_lengths": List(Int32), "count_i64": Int64, "zero_count_i64": Int64, "positive_buckets_i64": List(Int64), "negative_buckets_i64": List(Int64), "count_f64": Float64, "zero_count_f64": Float64, "positive_buckets_f64": List(Float64), "negative_buckets_f64": List(Float64));N, tag_0:Utf8, __promql_metric_name:Utf8]
              Projection: some_metric.timestamp, prom_mixed_range_float(Utf8("rate"), timestamp_range, greptime_value, greptime_native_histogram, some_metric.timestamp, Int64(300000)) AS greptime_value, prom_mixed_range_histogram(Utf8("rate"), timestamp_range, greptime_value, greptime_native_histogram, some_metric.timestamp, Int64(300000)) AS greptime_native_histogram, some_metric.tag_0, __promql_metric_name [timestamp:Timestamp(ms), greptime_value:Float64;N, greptime_native_histogram:Struct("schema": Int32, "zero_threshold": Float64, "sum": Float64, "reset_hint": Int32, "start_timestamp": Timestamp(ms), "custom_values": List(Float64), "positive_span_offsets": List(Int32), "positive_span_lengths": List(Int32), "negative_span_offsets": List(Int32), "negative_span_lengths": List(Int32), "count_i64": Int64, "zero_count_i64": Int64, "positive_buckets_i64": List(Int64), "negative_buckets_i64": List(Int64), "count_f64": Float64, "zero_count_f64": Float64, "positive_buckets_f64": List(Float64), "negative_buckets_f64": List(Float64));N, tag_0:Utf8, __promql_metric_name:Utf8]
                Projection: some_metric.tag_0, some_metric.timestamp, greptime_native_histogram, greptime_value, timestamp_range, Utf8("some_metric") AS __promql_metric_name [tag_0:Utf8, timestamp:Timestamp(ms), greptime_native_histogram:Dictionary(Int64, Struct("schema": Int32, "zero_threshold": Float64, "sum": Float64, "reset_hint": Int32, "start_timestamp": Timestamp(ms), "custom_values": List(Float64), "positive_span_offsets": List(Int32), "positive_span_lengths": List(Int32), "negative_span_offsets": List(Int32), "negative_span_lengths": List(Int32), "count_i64": Int64, "zero_count_i64": Int64, "positive_buckets_i64": List(Int64), "negative_buckets_i64": List(Int64), "count_f64": Float64, "zero_count_f64": Float64, "positive_buckets_f64": List(Float64), "negative_buckets_f64": List(Float64)));N, greptime_value:Dictionary(Int64, Float64);N, timestamp_range:Dictionary(Int64, Timestamp(ms)), __promql_metric_name:Utf8]
                  PromRangeManipulate: req range=[-540000..100000000], interval=[60000], eval range=[300000], time index=[timestamp], values=["greptime_native_histogram", "greptime_value"] [tag_0:Utf8, timestamp:Timestamp(ms), greptime_native_histogram:Dictionary(Int64, Struct("schema": Int32, "zero_threshold": Float64, "sum": Float64, "reset_hint": Int32, "start_timestamp": Timestamp(ms), "custom_values": List(Float64), "positive_span_offsets": List(Int32), "positive_span_lengths": List(Int32), "negative_span_offsets": List(Int32), "negative_span_lengths": List(Int32), "count_i64": Int64, "zero_count_i64": Int64, "positive_buckets_i64": List(Int64), "negative_buckets_i64": List(Int64), "count_f64": Float64, "zero_count_f64": Float64, "positive_buckets_f64": List(Float64), "negative_buckets_f64": List(Float64)));N, greptime_value:Dictionary(Int64, Float64);N, timestamp_range:Dictionary(Int64, Timestamp(ms))]
                    PromSeriesNormalize: offset=[0], time index=[timestamp], filter NaN: [true] [tag_0:Utf8, timestamp:Timestamp(ms), greptime_native_histogram:Struct("schema": Int32, "zero_threshold": Float64, "sum": Float64, "reset_hint": Int32, "start_timestamp": Timestamp(ms), "custom_values": List(Float64), "positive_span_offsets": List(Int32), "positive_span_lengths": List(Int32), "negative_span_offsets": List(Int32), "negative_span_lengths": List(Int32), "count_i64": Int64, "zero_count_i64": Int64, "positive_buckets_i64": List(Int64), "negative_buckets_i64": List(Int64), "count_f64": Float64, "zero_count_f64": Float64, "positive_buckets_f64": List(Float64), "negative_buckets_f64": List(Float64));N, greptime_value:Float64;N]
                      PromSeriesDivide: tags=["tag_0"] [tag_0:Utf8, timestamp:Timestamp(ms), greptime_native_histogram:Struct("schema": Int32, "zero_threshold": Float64, "sum": Float64, "reset_hint": Int32, "start_timestamp": Timestamp(ms), "custom_values": List(Float64), "positive_span_offsets": List(Int32), "positive_span_lengths": List(Int32), "negative_span_offsets": List(Int32), "negative_span_lengths": List(Int32), "count_i64": Int64, "zero_count_i64": Int64, "positive_buckets_i64": List(Int64), "negative_buckets_i64": List(Int64), "count_f64": Float64, "zero_count_f64": Float64, "positive_buckets_f64": List(Float64), "negative_buckets_f64": List(Float64));N, greptime_value:Float64;N]
                        Sort: some_metric.tag_0 ASC NULLS FIRST, some_metric.timestamp ASC NULLS FIRST [tag_0:Utf8, timestamp:Timestamp(ms), greptime_native_histogram:Struct("schema": Int32, "zero_threshold": Float64, "sum": Float64, "reset_hint": Int32, "start_timestamp": Timestamp(ms), "custom_values": List(Float64), "positive_span_offsets": List(Int32), "positive_span_lengths": List(Int32), "negative_span_offsets": List(Int32), "negative_span_lengths": List(Int32), "count_i64": Int64, "zero_count_i64": Int64, "positive_buckets_i64": List(Int64), "negative_buckets_i64": List(Int64), "count_f64": Float64, "zero_count_f64": Float64, "positive_buckets_f64": List(Float64), "negative_buckets_f64": List(Float64));N, greptime_value:Float64;N]
                          Filter: some_metric.timestamp >= TimestampMillisecond(-839999, None) AND some_metric.timestamp <= TimestampMillisecond(100000000, None) [tag_0:Utf8, timestamp:Timestamp(ms), greptime_native_histogram:Struct("schema": Int32, "zero_threshold": Float64, "sum": Float64, "reset_hint": Int32, "start_timestamp": Timestamp(ms), "custom_values": List(Float64), "positive_span_offsets": List(Int32), "positive_span_lengths": List(Int32), "negative_span_offsets": List(Int32), "negative_span_lengths": List(Int32), "count_i64": Int64, "zero_count_i64": Int64, "positive_buckets_i64": List(Int64), "negative_buckets_i64": List(Int64), "count_f64": Float64, "zero_count_f64": Float64, "positive_buckets_f64": List(Float64), "negative_buckets_f64": List(Float64));N, greptime_value:Float64;N]
                            TableScan: some_metric [tag_0:Utf8, timestamp:Timestamp(ms), greptime_native_histogram:Struct("schema": Int32, "zero_threshold": Float64, "sum": Float64, "reset_hint": Int32, "start_timestamp": Timestamp(ms), "custom_values": List(Float64), "positive_span_offsets": List(Int32), "positive_span_lengths": List(Int32), "negative_span_offsets": List(Int32), "negative_span_lengths": List(Int32), "count_i64": Int64, "zero_count_i64": Int64, "positive_buckets_i64": List(Int64), "negative_buckets_i64": List(Int64), "count_f64": Float64, "zero_count_f64": Float64, "positive_buckets_f64": List(Float64), "negative_buckets_f64": List(Float64));N, greptime_value:Float64;N]"#;
    assert_eq!(plan, expected);
}

#[tokio::test]
async fn mixed_native_histogram_predict_linear_forwards_the_eval_timestamp() {
    let query = "predict_linear(some_metric[5m], 60)";
    let plan = PromPlanner::stmt_to_plan(
        build_test_mixed_native_histogram_table_provider("some_metric").await,
        &build_eval_stmt(query),
        &build_query_engine_state(),
    )
    .await
    .unwrap()
    .display_indent_schema()
    .to_string();

    assert!(
        plan.contains(
            "prom_mixed_range_float(Utf8(\"predict_linear\"), timestamp_range, greptime_value, greptime_native_histogram, CAST(Float64(60) AS Int64), CAST(CAST(some_metric.timestamp AS Int64) + Int64(0) AS Timestamp(ms)))"
        ),
        "{query}\n{plan}"
    );
}

#[tokio::test]
async fn mixed_native_histogram_rate_executes_real_ranges() {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new(
            "timestamp",
            ArrowDataType::Timestamp(ArrowTimeUnit::Millisecond, None),
            false,
        ),
        Field::new(greptime_value(), ArrowDataType::Float64, true),
        Field::new(
            greptime_native_histogram(),
            native_histogram_value_type().as_arrow_type(),
            true,
        ),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(TimestampMillisecondArray::from(vec![1000, 2000, 3000])),
            Arc::new(Float64Array::from(vec![Some(1.0), None, Some(3.0)])),
            build_histogram_array(&[None, Some(direct_or_histogram()), None]),
        ],
    )
    .unwrap();
    let table = Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap());
    let input = LogicalPlanBuilder::scan("mixed", provider_as_source(table), None)
        .unwrap()
        .build()
        .unwrap();
    let collector = PromqlAnnotationCollector::default();
    let mut planner = PromPlanner {
        table_provider: build_test_table_provider_with_fields(
            &[(DEFAULT_SCHEMA_NAME.to_string(), "dummy".to_string())],
            &[],
        )
        .await,
        ctx: PromPlannerContext {
            start: 3000,
            end: 3000,
            interval: 1000,
            range: Some(3000),
            time_index_column: Some("timestamp".to_string()),
            field_columns: vec![
                greptime_native_histogram().to_string(),
                greptime_value().to_string(),
            ],
            ..Default::default()
        },
        promql_annotations: Some(collector.clone()),
    };
    let input = LogicalPlan::Extension(Extension {
        node: Arc::new(
            RangeManipulate::new(
                3000,
                3000,
                1000,
                0,
                3000,
                "timestamp".to_string(),
                planner.ctx.field_columns.clone(),
                input,
            )
            .unwrap(),
        ),
    });
    let PromExpr::Call(call) = parser::parse("rate(mixed[3s])").unwrap() else {
        unreachable!()
    };
    let preserve_any_value = PromPlanner::field_columns_are_alternative_samples(
        input.schema(),
        &planner.ctx.field_columns,
    );
    let state = build_query_engine_state();
    let (mut exprs, _) = planner
        .create_function_expr(&call.func, vec![], input.schema(), &state, None)
        .unwrap();
    exprs.insert(0, planner.create_time_index_column_expr().unwrap());
    let plan = LogicalPlanBuilder::from(input)
        .project(exprs)
        .unwrap()
        .filter(
            planner
                .create_empty_values_filter_expr(preserve_any_value)
                .unwrap(),
        )
        .unwrap()
        .build()
        .unwrap();
    let (_, batches) = execute(plan, &state).await;
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
    let mut warnings = Vec::new();
    collector.append_to(&mut warnings, &mut Vec::new());
    assert!(
        warnings
            .iter()
            .any(|warning| warning.contains("mix of float and native histogram"))
    );
}

#[tokio::test]
async fn native_histogram_mixed_field_table_behaves() {
    // Exercise function planning after float and histogram samples have already been
    // represented as alternative nullable fields. Histogram functions must select the
    // histogram field without adding a NULL float field that would reject every row.
    let table_provider = build_test_mixed_native_histogram_table_provider("some_metric").await;
    let plan = PromPlanner::stmt_to_plan(
        table_provider,
        &build_eval_stmt("histogram_count(some_metric)"),
        &build_query_engine_state(),
    )
    .await
    .unwrap();
    let plan_str = plan.display_indent_schema().to_string();
    assert!(
        plan_str.contains("prom_native_histogram_count"),
        "{plan_str}"
    );
    assert!(!plan_str.contains("Float64(NULL)"), "{plan_str}");
    assert!(
        plan_str.contains("prom_native_histogram_count(greptime_native_histogram) IS NOT NULL"),
        "{plan_str}"
    );

    // Value sorting keeps the float column and never sorts by the histogram column.
    let table_provider = build_test_mixed_native_histogram_table_provider("some_metric").await;
    let plan = PromPlanner::stmt_to_plan(
        table_provider,
        &build_eval_stmt("sort(some_metric)"),
        &build_query_engine_state(),
    )
    .await
    .unwrap();
    let plan_str = plan.display_indent_schema().to_string();
    assert!(
        plan_str.contains("greptime_value ASC NULLS FIRST"),
        "{plan_str}"
    );
    assert!(
        !plan_str.contains("greptime_native_histogram ASC"),
        "{plan_str}"
    );

    // scalar() ignores histogram samples and evaluates only the float field.
    let table_provider = build_test_mixed_native_histogram_table_provider("some_metric").await;
    let plan = PromPlanner::stmt_to_plan(
        table_provider,
        &build_eval_stmt("scalar(some_metric)"),
        &build_query_engine_state(),
    )
    .await
    .unwrap();
    let plan_str = plan.display_indent_schema().to_string();
    assert!(plan_str.contains("ScalarCalculate"), "{plan_str}");
    assert!(
        plan_str.contains("greptime_value IS NOT NULL"),
        "{plan_str}"
    );

    // Functions that preserve both alternative fields keep rows with either sample type.
    let table_provider = build_test_mixed_native_histogram_table_provider("some_metric").await;
    let plan = PromPlanner::stmt_to_plan(
        table_provider,
        &build_eval_stmt(r#"label_replace(some_metric, "copied", "$1", "tag_0", "(.*)")"#),
        &build_query_engine_state(),
    )
    .await
    .unwrap();
    let plan_str = plan.display_indent_schema().to_string();
    let filter = plan_str.lines().next().unwrap();
    assert!(
        filter.starts_with("Filter: ")
            && filter.contains("greptime_native_histogram IS NOT NULL")
            && filter.contains(" OR ")
            && filter.contains("greptime_value IS NOT NULL"),
        "{plan_str}"
    );
}

#[tokio::test]
async fn less_filter_on_value() {
    let query = "some_metric < 1.2345";
    let expected = String::from(
        "Filter: some_metric.field_0 < Float64(1.2345) [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, __promql_metric_name:Utf8]\
            \n  Projection: some_metric.tag_0, some_metric.timestamp, some_metric.field_0, Utf8(\"some_metric\") AS __promql_metric_name [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, __promql_metric_name:Utf8]\
            \n    PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n      PromSeriesDivide: tags=[\"tag_0\"] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n        Sort: some_metric.tag_0 ASC NULLS FIRST, some_metric.timestamp ASC NULLS FIRST [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n          Filter: some_metric.timestamp >= TimestampMillisecond(-999, None) AND some_metric.timestamp <= TimestampMillisecond(100000000, None) [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n            TableScan: some_metric [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]",
    );

    indie_query_plan_compare(query, expected).await;
}

#[tokio::test]
async fn count_over_time() {
    let query = "count_over_time(some_metric[5m])";
    let expected = String::from(
        "Projection: some_metric.timestamp, prom_count_over_time(timestamp_range,field_0), some_metric.tag_0 [timestamp:Timestamp(ms), prom_count_over_time(timestamp_range,field_0):Float64;N, tag_0:Utf8]\
            \n  Filter: prom_count_over_time(timestamp_range,field_0) IS NOT NULL [timestamp:Timestamp(ms), prom_count_over_time(timestamp_range,field_0):Float64;N, tag_0:Utf8, __promql_metric_name:Utf8]\
            \n    Projection: some_metric.timestamp, prom_count_over_time(timestamp_range, field_0) AS prom_count_over_time(timestamp_range,field_0), some_metric.tag_0, __promql_metric_name [timestamp:Timestamp(ms), prom_count_over_time(timestamp_range,field_0):Float64;N, tag_0:Utf8, __promql_metric_name:Utf8]\
            \n      Projection: some_metric.tag_0, some_metric.timestamp, field_0, timestamp_range, Utf8(\"some_metric\") AS __promql_metric_name [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Dictionary(Int64, Float64);N, timestamp_range:Dictionary(Int64, Timestamp(ms)), __promql_metric_name:Utf8]\
            \n        PromRangeManipulate: req range=[0..100000000], interval=[5000], eval range=[300000], time index=[timestamp], values=[\"field_0\"] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Dictionary(Int64, Float64);N, timestamp_range:Dictionary(Int64, Timestamp(ms))]\
            \n          PromSeriesNormalize: offset=[0], time index=[timestamp], filter NaN: [true] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n            PromSeriesDivide: tags=[\"tag_0\"] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n              Sort: some_metric.tag_0 ASC NULLS FIRST, some_metric.timestamp ASC NULLS FIRST [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n                Filter: some_metric.timestamp >= TimestampMillisecond(-299999, None) AND some_metric.timestamp <= TimestampMillisecond(100000000, None) [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n                  TableScan: some_metric [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]",
    );

    indie_query_plan_compare(query, expected).await;
}

/// The outer `PromRangeManipulate` from a subquery must be preceded by
/// `Sort` + `PromSeriesDivide`.
#[tokio::test]
async fn count_over_time_subquery() {
    let query = "count_over_time(some_metric[10m:1m])";
    let expected = String::from(
        "Projection: some_metric.timestamp, prom_count_over_time(timestamp_range,field_0), some_metric.tag_0 [timestamp:Timestamp(ms), prom_count_over_time(timestamp_range,field_0):Float64;N, tag_0:Utf8]\
            \n  Filter: prom_count_over_time(timestamp_range,field_0) IS NOT NULL [timestamp:Timestamp(ms), prom_count_over_time(timestamp_range,field_0):Float64;N, tag_0:Utf8, __promql_metric_name:Utf8]\
            \n    Projection: some_metric.timestamp, prom_count_over_time(timestamp_range, field_0) AS prom_count_over_time(timestamp_range,field_0), some_metric.tag_0, __promql_metric_name [timestamp:Timestamp(ms), prom_count_over_time(timestamp_range,field_0):Float64;N, tag_0:Utf8, __promql_metric_name:Utf8]\
            \n      PromRangeManipulate: req range=[0..100000000], interval=[5000], eval range=[600000], time index=[timestamp], values=[\"field_0\"] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Dictionary(Int64, Float64);N, __promql_metric_name:Utf8, timestamp_range:Dictionary(Int64, Timestamp(ms))]\
            \n        PromSeriesDivide: tags=[\"tag_0\", \"__promql_metric_name\"] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, __promql_metric_name:Utf8]\
            \n          Sort: some_metric.tag_0 ASC NULLS FIRST, __promql_metric_name ASC NULLS FIRST, some_metric.timestamp ASC NULLS FIRST [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, __promql_metric_name:Utf8]\
            \n            Projection: some_metric.tag_0, some_metric.timestamp, some_metric.field_0, Utf8(\"some_metric\") AS __promql_metric_name [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, __promql_metric_name:Utf8]\
            \n              PromInstantManipulate: range=[-540000..100000000], lookback=[1000], interval=[60000], time index=[timestamp] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n                PromSeriesDivide: tags=[\"tag_0\"] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n                  Sort: some_metric.tag_0 ASC NULLS FIRST, some_metric.timestamp ASC NULLS FIRST [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n                    Filter: some_metric.timestamp >= TimestampMillisecond(-540999, None) AND some_metric.timestamp <= TimestampMillisecond(100000000, None) [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n                      TableScan: some_metric [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]",
    );
    indie_query_plan_compare(query, expected).await;
}

/// `offset` on a subquery must shift the inner evaluation window back and be
/// carried into the outer range manipulation. See
/// <https://github.com/GreptimeTeam/greptimedb/issues/9330>.
#[tokio::test]
async fn count_over_time_subquery_with_offset() {
    let query = "count_over_time(some_metric[10m:1m] offset 5m)";
    let expected = String::from(
        "Filter: prom_count_over_time(timestamp_range,field_0) IS NOT NULL [timestamp:Timestamp(ms), prom_count_over_time(timestamp_range,field_0):Float64;N, tag_0:Utf8]\
        \n  Projection: some_metric.timestamp, prom_count_over_time(timestamp_range, field_0) AS prom_count_over_time(timestamp_range,field_0), some_metric.tag_0 [timestamp:Timestamp(ms), prom_count_over_time(timestamp_range,field_0):Float64;N, tag_0:Utf8]\
        \n    PromRangeManipulate: req range=[0..100000000], interval=[5000], eval range=[600000], time index=[timestamp], values=[\"field_0\"] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Dictionary(Int64, Float64);N, timestamp_range:Dictionary(Int64, Timestamp(ms))]\
        \n      PromSeriesNormalize: offset=[300000], time index=[timestamp], filter NaN: [false] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
        \n        PromSeriesDivide: tags=[\"tag_0\"] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
        \n          Sort: some_metric.tag_0 ASC NULLS FIRST, some_metric.timestamp ASC NULLS FIRST [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
        \n            PromInstantManipulate: range=[-840000..99700000], lookback=[1000], interval=[60000], time index=[timestamp] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
        \n              PromSeriesDivide: tags=[\"tag_0\"] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
        \n                Sort: some_metric.tag_0 ASC NULLS FIRST, some_metric.timestamp ASC NULLS FIRST [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
        \n                  Filter: some_metric.timestamp >= TimestampMillisecond(-840999, None) AND some_metric.timestamp <= TimestampMillisecond(99700000, None) [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
        \n                    TableScan: some_metric [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]",
    );
    indie_query_plan_compare(query, expected).await;
}

#[tokio::test]
async fn test_hash_join() {
    let mut eval_stmt = EvalStmt {
        expr: PromExpr::NumberLiteral(NumberLiteral { val: 1.0 }),
        start: UNIX_EPOCH,
        end: UNIX_EPOCH
            .checked_add(Duration::from_secs(100_000))
            .unwrap(),
        interval: Duration::from_secs(5),
        lookback_delta: Duration::from_secs(1),
    };

    let case = r#"http_server_requests_seconds_sum{uri="/accounts/login"} / ignoring(kubernetes_pod_name,kubernetes_namespace) http_server_requests_seconds_count{uri="/accounts/login"}"#;

    let prom_expr = parser::parse(case).unwrap();
    eval_stmt.expr = prom_expr;
    let table_provider = build_test_table_provider_with_fields(
        &[
            (
                DEFAULT_SCHEMA_NAME.to_string(),
                "http_server_requests_seconds_sum".to_string(),
            ),
            (
                DEFAULT_SCHEMA_NAME.to_string(),
                "http_server_requests_seconds_count".to_string(),
            ),
        ],
        &["uri", "kubernetes_namespace", "kubernetes_pod_name"],
    )
    .await;
    // Should be ok
    let plan = PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
        .await
        .unwrap();
    let expected = "Projection: http_server_requests_seconds_sum.uri, http_server_requests_seconds_count.greptime_timestamp, http_server_requests_seconds_sum.greptime_value / http_server_requests_seconds_count.greptime_value\
            \n  Projection: http_server_requests_seconds_sum.uri, http_server_requests_seconds_sum.__promql_metric_name, http_server_requests_seconds_count.greptime_timestamp, CAST(http_server_requests_seconds_sum.greptime_value AS Float64) / CAST(http_server_requests_seconds_count.greptime_value AS Float64) AS http_server_requests_seconds_sum.greptime_value / http_server_requests_seconds_count.greptime_value\
            \n    Projection: http_server_requests_seconds_sum.uri, http_server_requests_seconds_sum.kubernetes_namespace, http_server_requests_seconds_sum.kubernetes_pod_name, http_server_requests_seconds_sum.greptime_timestamp, http_server_requests_seconds_sum.greptime_value, http_server_requests_seconds_sum.__promql_metric_name, http_server_requests_seconds_count.uri, http_server_requests_seconds_count.kubernetes_namespace, http_server_requests_seconds_count.kubernetes_pod_name, http_server_requests_seconds_count.greptime_timestamp, http_server_requests_seconds_count.greptime_value, http_server_requests_seconds_count.__promql_metric_name\
            \n      Filter: prom_assert_unique_match_group(__promql_match_group_count, http_server_requests_seconds_sum.uri, http_server_requests_seconds_sum.__promql_metric_name)\
            \n        WindowAggr: windowExpr=[[count(Int64(1)) PARTITION BY [http_server_requests_seconds_sum.uri, http_server_requests_seconds_sum.__promql_metric_name, http_server_requests_seconds_sum.greptime_timestamp] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING AS __promql_match_group_count]]\
            \n          Inner Join: http_server_requests_seconds_sum.greptime_timestamp = http_server_requests_seconds_count.greptime_timestamp, http_server_requests_seconds_sum.uri = http_server_requests_seconds_count.uri\
            \n            SubqueryAlias: http_server_requests_seconds_sum\
            \n              Projection: http_server_requests_seconds_sum.uri, http_server_requests_seconds_sum.kubernetes_namespace, http_server_requests_seconds_sum.kubernetes_pod_name, http_server_requests_seconds_sum.greptime_timestamp, http_server_requests_seconds_sum.greptime_value, Utf8(\"http_server_requests_seconds_sum\") AS __promql_metric_name\
            \n                PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[greptime_timestamp]\
            \n                  PromSeriesDivide: tags=[\"uri\", \"kubernetes_namespace\", \"kubernetes_pod_name\"]\
            \n                    Sort: http_server_requests_seconds_sum.uri ASC NULLS FIRST, http_server_requests_seconds_sum.kubernetes_namespace ASC NULLS FIRST, http_server_requests_seconds_sum.kubernetes_pod_name ASC NULLS FIRST, http_server_requests_seconds_sum.greptime_timestamp ASC NULLS FIRST\
            \n                      Filter: http_server_requests_seconds_sum.uri = Utf8(\"/accounts/login\") AND http_server_requests_seconds_sum.greptime_timestamp >= TimestampMillisecond(-999, None) AND http_server_requests_seconds_sum.greptime_timestamp <= TimestampMillisecond(100000000, None)\
            \n                        TableScan: http_server_requests_seconds_sum\
            \n            SubqueryAlias: http_server_requests_seconds_count\
            \n              Projection: http_server_requests_seconds_count.uri, http_server_requests_seconds_count.kubernetes_namespace, http_server_requests_seconds_count.kubernetes_pod_name, http_server_requests_seconds_count.greptime_timestamp, http_server_requests_seconds_count.greptime_value, __promql_metric_name\
            \n                Filter: prom_assert_unique_match_group(__promql_match_group_count, http_server_requests_seconds_count.uri)\
            \n                  WindowAggr: windowExpr=[[count(Int64(1)) PARTITION BY [http_server_requests_seconds_count.uri, http_server_requests_seconds_count.greptime_timestamp] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING AS __promql_match_group_count]]\
            \n                    Projection: http_server_requests_seconds_count.uri, http_server_requests_seconds_count.kubernetes_namespace, http_server_requests_seconds_count.kubernetes_pod_name, http_server_requests_seconds_count.greptime_timestamp, http_server_requests_seconds_count.greptime_value, Utf8(\"http_server_requests_seconds_count\") AS __promql_metric_name\
            \n                      PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[greptime_timestamp]\
            \n                        PromSeriesDivide: tags=[\"uri\", \"kubernetes_namespace\", \"kubernetes_pod_name\"]\
            \n                          Sort: http_server_requests_seconds_count.uri ASC NULLS FIRST, http_server_requests_seconds_count.kubernetes_namespace ASC NULLS FIRST, http_server_requests_seconds_count.kubernetes_pod_name ASC NULLS FIRST, http_server_requests_seconds_count.greptime_timestamp ASC NULLS FIRST\
            \n                            Filter: http_server_requests_seconds_count.uri = Utf8(\"/accounts/login\") AND http_server_requests_seconds_count.greptime_timestamp >= TimestampMillisecond(-999, None) AND http_server_requests_seconds_count.greptime_timestamp <= TimestampMillisecond(100000000, None)\
            \n                              TableScan: http_server_requests_seconds_count";
    assert_eq!(plan.to_string(), expected);
}

#[tokio::test]
async fn test_nested_histogram_quantile() {
    let mut eval_stmt = EvalStmt {
        expr: PromExpr::NumberLiteral(NumberLiteral { val: 1.0 }),
        start: UNIX_EPOCH,
        end: UNIX_EPOCH
            .checked_add(Duration::from_secs(100_000))
            .unwrap(),
        interval: Duration::from_secs(5),
        lookback_delta: Duration::from_secs(1),
    };

    let case = r#"label_replace(histogram_quantile(0.99, sum by(pod, le, path, code) (rate(greptime_servers_grpc_requests_elapsed_bucket{container="frontend"}[1m0s]))), "pod_new", "$1", "pod", "greptimedb-frontend-[0-9a-z]*-(.*)")"#;

    let prom_expr = parser::parse(case).unwrap();
    eval_stmt.expr = prom_expr;
    let table_provider = build_test_table_provider_with_fields(
        &[(
            DEFAULT_SCHEMA_NAME.to_string(),
            "greptime_servers_grpc_requests_elapsed_bucket".to_string(),
        )],
        &["pod", "le", "path", "code", "container"],
    )
    .await;
    // Should be ok
    let _ = PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
        .await
        .unwrap();
}

#[tokio::test]
async fn test_histogram_quantile_binary_op() {
    let mut eval_stmt = EvalStmt {
        expr: PromExpr::NumberLiteral(NumberLiteral { val: 1.0 }),
        start: UNIX_EPOCH,
        end: UNIX_EPOCH
            .checked_add(Duration::from_secs(100_000))
            .unwrap(),
        interval: Duration::from_secs(5),
        lookback_delta: Duration::from_secs(1),
    };

    // Arithmetic applied to a histogram_quantile() result. Regression for #8144:
    // HistogramFold used to drop the input column qualifiers, so the binary-op
    // projection failed to resolve the qualified tag column.
    let case = r#"histogram_quantile(0.5, sum by (le, pod) (rate(http_request_duration_seconds_bucket[5m]))) + 0"#;

    let prom_expr = parser::parse(case).unwrap();
    eval_stmt.expr = prom_expr;
    let table_provider = build_test_table_provider_with_fields(
        &[(
            DEFAULT_SCHEMA_NAME.to_string(),
            "http_request_duration_seconds_bucket".to_string(),
        )],
        &["pod", "le"],
    )
    .await;
    // Should plan without a "No field named ..." error.
    let _ = PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
        .await
        .unwrap();
}

#[tokio::test]
async fn test_parse_and_operator() {
    let mut eval_stmt = EvalStmt {
        expr: PromExpr::NumberLiteral(NumberLiteral { val: 1.0 }),
        start: UNIX_EPOCH,
        end: UNIX_EPOCH
            .checked_add(Duration::from_secs(100_000))
            .unwrap(),
        interval: Duration::from_secs(5),
        lookback_delta: Duration::from_secs(1),
    };

    let cases = [
        r#"count (max by (persistentvolumeclaim,namespace) (kubelet_volume_stats_used_bytes{namespace=~".+"} ) and (max by (persistentvolumeclaim,namespace) (kubelet_volume_stats_used_bytes{namespace=~".+"} )) / (max by (persistentvolumeclaim,namespace) (kubelet_volume_stats_capacity_bytes{namespace=~".+"} )) >= (80 / 100)) or vector (0)"#,
        r#"count (max by (persistentvolumeclaim,namespace) (kubelet_volume_stats_used_bytes{namespace=~".+"} ) unless (max by (persistentvolumeclaim,namespace) (kubelet_volume_stats_used_bytes{namespace=~".+"} )) / (max by (persistentvolumeclaim,namespace) (kubelet_volume_stats_capacity_bytes{namespace=~".+"} )) >= (80 / 100)) or vector (0)"#,
    ];

    for case in cases {
        let prom_expr = parser::parse(case).unwrap();
        eval_stmt.expr = prom_expr;
        let table_provider = build_test_table_provider_with_fields(
            &[
                (
                    DEFAULT_SCHEMA_NAME.to_string(),
                    "kubelet_volume_stats_used_bytes".to_string(),
                ),
                (
                    DEFAULT_SCHEMA_NAME.to_string(),
                    "kubelet_volume_stats_capacity_bytes".to_string(),
                ),
            ],
            &["namespace", "persistentvolumeclaim"],
        )
        .await;
        // Should be ok
        let _ = PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn test_nested_binary_op() {
    let mut eval_stmt = EvalStmt {
        expr: PromExpr::NumberLiteral(NumberLiteral { val: 1.0 }),
        start: UNIX_EPOCH,
        end: UNIX_EPOCH
            .checked_add(Duration::from_secs(100_000))
            .unwrap(),
        interval: Duration::from_secs(5),
        lookback_delta: Duration::from_secs(1),
    };

    let case = r#"sum(rate(nginx_ingress_controller_requests{job=~".*"}[2m])) -
        (
            sum(rate(nginx_ingress_controller_requests{namespace=~".*"}[2m]))
            or
            vector(0)
        )"#;

    let prom_expr = parser::parse(case).unwrap();
    eval_stmt.expr = prom_expr;
    let table_provider = build_test_table_provider_with_fields(
        &[(
            DEFAULT_SCHEMA_NAME.to_string(),
            "nginx_ingress_controller_requests".to_string(),
        )],
        &["namespace", "job"],
    )
    .await;
    // Should be ok
    let _ = PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
        .await
        .unwrap();
}

#[tokio::test]
async fn test_parse_or_operator() {
    let mut eval_stmt = EvalStmt {
        expr: PromExpr::NumberLiteral(NumberLiteral { val: 1.0 }),
        start: UNIX_EPOCH,
        end: UNIX_EPOCH
            .checked_add(Duration::from_secs(100_000))
            .unwrap(),
        interval: Duration::from_secs(5),
        lookback_delta: Duration::from_secs(1),
    };

    let case = r#"
        sum(rate(sysstat{tenant_name=~"tenant1",cluster_name=~"cluster1"}[120s])) by (cluster_name,tenant_name) /
        (sum(sysstat{tenant_name=~"tenant1",cluster_name=~"cluster1"}) by (cluster_name,tenant_name) * 100)
            or
        200 * sum(sysstat{tenant_name=~"tenant1",cluster_name=~"cluster1"}) by (cluster_name,tenant_name) /
        sum(sysstat{tenant_name=~"tenant1",cluster_name=~"cluster1"}) by (cluster_name,tenant_name)"#;

    let table_provider = build_test_table_provider_with_fields(
        &[(DEFAULT_SCHEMA_NAME.to_string(), "sysstat".to_string())],
        &["tenant_name", "cluster_name"],
    )
    .await;
    eval_stmt.expr = parser::parse(case).unwrap();
    let _ = PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
        .await
        .unwrap();

    let case = r#"sum(delta(sysstat{tenant_name=~"sys",cluster_name=~"cluster1"}[2m])/120) by (cluster_name,tenant_name) /
            (sum(delta(sysstat{tenant_name=~"sys",cluster_name=~"cluster1"}[2m])/120) by (cluster_name,tenant_name) *1000) +
            sum(delta(sysstat{tenant_name=~"sys",cluster_name=~"cluster1"}[2m])/120) by (cluster_name,tenant_name) /
            (sum(delta(sysstat{tenant_name=~"sys",cluster_name=~"cluster1"}[2m])/120) by (cluster_name,tenant_name) *1000) >= 0
            or
            sum(delta(sysstat{tenant_name=~"sys",cluster_name=~"cluster1"}[2m])/120) by (cluster_name,tenant_name) /
            (sum(delta(sysstat{tenant_name=~"sys",cluster_name=~"cluster1"}[2m])/120) by (cluster_name,tenant_name) *1000) >= 0
            or
            sum(delta(sysstat{tenant_name=~"sys",cluster_name=~"cluster1"}[2m])/120) by (cluster_name,tenant_name) /
            (sum(delta(sysstat{tenant_name=~"sys",cluster_name=~"cluster1"}[2m])/120) by (cluster_name,tenant_name) *1000) >= 0"#;
    let table_provider = build_test_table_provider_with_fields(
        &[(DEFAULT_SCHEMA_NAME.to_string(), "sysstat".to_string())],
        &["tenant_name", "cluster_name"],
    )
    .await;
    eval_stmt.expr = parser::parse(case).unwrap();
    let _ = PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
        .await
        .unwrap();

    let case = r#"(sum(background_waitevent_cnt{tenant_name=~"sys",cluster_name=~"cluster1"}) by (cluster_name,tenant_name) +
            sum(foreground_waitevent_cnt{tenant_name=~"sys",cluster_name=~"cluster1"}) by (cluster_name,tenant_name)) or
            (sum(background_waitevent_cnt{tenant_name=~"sys",cluster_name=~"cluster1"}) by (cluster_name,tenant_name)) or
            (sum(foreground_waitevent_cnt{tenant_name=~"sys",cluster_name=~"cluster1"}) by (cluster_name,tenant_name))"#;
    let table_provider = build_test_table_provider_with_fields(
        &[
            (
                DEFAULT_SCHEMA_NAME.to_string(),
                "background_waitevent_cnt".to_string(),
            ),
            (
                DEFAULT_SCHEMA_NAME.to_string(),
                "foreground_waitevent_cnt".to_string(),
            ),
        ],
        &["tenant_name", "cluster_name"],
    )
    .await;
    eval_stmt.expr = parser::parse(case).unwrap();
    let _ = PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
        .await
        .unwrap();

    let case = r#"avg(node_load1{cluster_name=~"cluster1"}) by (cluster_name,host_name) or max(container_cpu_load_average_10s{cluster_name=~"cluster1"}) by (cluster_name,host_name) * 100 / max(container_spec_cpu_quota{cluster_name=~"cluster1"}) by (cluster_name,host_name)"#;
    let table_provider = build_test_table_provider_with_fields(
        &[
            (DEFAULT_SCHEMA_NAME.to_string(), "node_load1".to_string()),
            (
                DEFAULT_SCHEMA_NAME.to_string(),
                "container_cpu_load_average_10s".to_string(),
            ),
            (
                DEFAULT_SCHEMA_NAME.to_string(),
                "container_spec_cpu_quota".to_string(),
            ),
        ],
        &["cluster_name", "host_name"],
    )
    .await;
    eval_stmt.expr = parser::parse(case).unwrap();
    let _ = PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
        .await
        .unwrap();
}

#[tokio::test]
async fn value_matcher() {
    // template
    let mut eval_stmt = EvalStmt {
        expr: PromExpr::NumberLiteral(NumberLiteral { val: 1.0 }),
        start: UNIX_EPOCH,
        end: UNIX_EPOCH
            .checked_add(Duration::from_secs(100_000))
            .unwrap(),
        interval: Duration::from_secs(5),
        lookback_delta: Duration::from_secs(1),
    };

    let cases = [
        // single equal matcher
        (
            r#"some_metric{__field__="field_1"}"#,
            vec![
                "some_metric.field_1",
                "some_metric.tag_0",
                "some_metric.tag_1",
                "some_metric.tag_2",
                "some_metric.timestamp",
            ],
        ),
        // two equal matchers
        (
            r#"some_metric{__field__="field_1", __field__="field_0"}"#,
            vec![
                "some_metric.field_0",
                "some_metric.field_1",
                "some_metric.tag_0",
                "some_metric.tag_1",
                "some_metric.tag_2",
                "some_metric.timestamp",
            ],
        ),
        // single not_eq matcher
        (
            r#"some_metric{__field__!="field_1"}"#,
            vec![
                "some_metric.field_0",
                "some_metric.field_2",
                "some_metric.tag_0",
                "some_metric.tag_1",
                "some_metric.tag_2",
                "some_metric.timestamp",
            ],
        ),
        // two not_eq matchers
        (
            r#"some_metric{__field__!="field_1", __field__!="field_2"}"#,
            vec![
                "some_metric.field_0",
                "some_metric.tag_0",
                "some_metric.tag_1",
                "some_metric.tag_2",
                "some_metric.timestamp",
            ],
        ),
        // equal and not_eq matchers (no conflict)
        (
            r#"some_metric{__field__="field_1", __field__!="field_0"}"#,
            vec![
                "some_metric.field_1",
                "some_metric.tag_0",
                "some_metric.tag_1",
                "some_metric.tag_2",
                "some_metric.timestamp",
            ],
        ),
        // equal and not_eq matchers (conflict)
        (
            r#"some_metric{__field__="field_2", __field__!="field_2"}"#,
            vec![
                "some_metric.tag_0",
                "some_metric.tag_1",
                "some_metric.tag_2",
                "some_metric.timestamp",
            ],
        ),
        // single regex eq matcher
        (
            r#"some_metric{__field__=~"field_1|field_2"}"#,
            vec![
                "some_metric.field_1",
                "some_metric.field_2",
                "some_metric.tag_0",
                "some_metric.tag_1",
                "some_metric.tag_2",
                "some_metric.timestamp",
            ],
        ),
        // single regex not_eq matcher
        (
            r#"some_metric{__field__!~"field_1|field_2"}"#,
            vec![
                "some_metric.field_0",
                "some_metric.tag_0",
                "some_metric.tag_1",
                "some_metric.tag_2",
                "some_metric.timestamp",
            ],
        ),
    ];

    for case in cases {
        let prom_expr = parser::parse(case.0).unwrap();
        eval_stmt.expr = prom_expr;
        let table_provider = build_test_table_provider(
            &[(DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string())],
            3,
            3,
        )
        .await;
        let plan =
            PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
                .await
                .unwrap();
        // The raw selector carries its metric-name identity as a metadata-marked field; the
        // ordinary fields are the value matchers' projection of the table's columns.
        let marked = plan
            .schema()
            .fields()
            .iter()
            .filter(|field| is_marked_metric_name_field(field))
            .collect::<Vec<_>>();
        assert_eq!(marked.len(), 1, "case: {:?}", case.0);
        assert_eq!(
            marked[0].name().as_str(),
            PROMQL_METRIC_NAME_COLUMN,
            "case: {:?}",
            case.0
        );
        let mut fields = plan
            .schema()
            .iter()
            .filter(|(_, field)| !is_marked_metric_name_field(field))
            .map(|(qualifier, field)| datafusion_common::qualified_name(qualifier, field.name()))
            .collect::<Vec<_>>();
        let mut expected = case.1.into_iter().map(String::from).collect::<Vec<_>>();
        fields.sort();
        expected.sort();
        assert_eq!(fields, expected, "case: {:?}", case.0);
    }

    let bad_cases = [
        r#"some_metric{__field__="nonexistent"}"#,
        r#"some_metric{__field__!="nonexistent"}"#,
    ];

    for case in bad_cases {
        let prom_expr = parser::parse(case).unwrap();
        eval_stmt.expr = prom_expr;
        let table_provider = build_test_table_provider(
            &[(DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string())],
            3,
            3,
        )
        .await;
        let plan =
            PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
                .await;
        assert!(plan.is_err(), "case: {:?}", case);
    }
}

#[tokio::test]
async fn custom_schema() {
    let query = "some_alt_metric{__schema__=\"greptime_private\"}";
    let expected = String::from(
        "Projection: greptime_private.some_alt_metric.tag_0, greptime_private.some_alt_metric.timestamp, greptime_private.some_alt_metric.field_0, Utf8(\"some_alt_metric\") AS __promql_metric_name [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, __promql_metric_name:Utf8]\
            \n  PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n    PromSeriesDivide: tags=[\"tag_0\"] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n      Sort: greptime_private.some_alt_metric.tag_0 ASC NULLS FIRST, greptime_private.some_alt_metric.timestamp ASC NULLS FIRST [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n        Filter: greptime_private.some_alt_metric.timestamp >= TimestampMillisecond(-999, None) AND greptime_private.some_alt_metric.timestamp <= TimestampMillisecond(100000000, None) [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n          TableScan: greptime_private.some_alt_metric [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]",
    );

    indie_query_plan_compare(query, expected).await;

    let query = "some_alt_metric{__database__=\"greptime_private\"}";
    let expected = String::from(
        "Projection: greptime_private.some_alt_metric.tag_0, greptime_private.some_alt_metric.timestamp, greptime_private.some_alt_metric.field_0, Utf8(\"some_alt_metric\") AS __promql_metric_name [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, __promql_metric_name:Utf8]\
            \n  PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n    PromSeriesDivide: tags=[\"tag_0\"] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n      Sort: greptime_private.some_alt_metric.tag_0 ASC NULLS FIRST, greptime_private.some_alt_metric.timestamp ASC NULLS FIRST [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n        Filter: greptime_private.some_alt_metric.timestamp >= TimestampMillisecond(-999, None) AND greptime_private.some_alt_metric.timestamp <= TimestampMillisecond(100000000, None) [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n          TableScan: greptime_private.some_alt_metric [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]",
    );

    indie_query_plan_compare(query, expected).await;

    let query = "some_alt_metric{__schema__=\"greptime_private\"} / some_metric";
    let expected = String::from(
        "Projection: some_metric.tag_0, some_metric.timestamp, greptime_private.some_alt_metric.field_0 / some_metric.field_0 [tag_0:Utf8, timestamp:Timestamp(ms), greptime_private.some_alt_metric.field_0 / some_metric.field_0:Float64;N]\
            \n  Projection: some_metric.tag_0, some_metric.__promql_metric_name, some_metric.timestamp, CAST(greptime_private.some_alt_metric.field_0 AS Float64) / CAST(some_metric.field_0 AS Float64) AS greptime_private.some_alt_metric.field_0 / some_metric.field_0 [tag_0:Utf8, __promql_metric_name:Utf8, timestamp:Timestamp(ms), greptime_private.some_alt_metric.field_0 / some_metric.field_0:Float64;N]\
            \n    Inner Join: greptime_private.some_alt_metric.tag_0 = some_metric.tag_0, greptime_private.some_alt_metric.timestamp = some_metric.timestamp [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, __promql_metric_name:Utf8, tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, __promql_metric_name:Utf8]\
            \n      SubqueryAlias: greptime_private.some_alt_metric [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, __promql_metric_name:Utf8]\
            \n        Projection: greptime_private.some_alt_metric.tag_0, greptime_private.some_alt_metric.timestamp, greptime_private.some_alt_metric.field_0, Utf8(\"some_alt_metric\") AS __promql_metric_name [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, __promql_metric_name:Utf8]\
            \n          PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n            PromSeriesDivide: tags=[\"tag_0\"] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n              Sort: greptime_private.some_alt_metric.tag_0 ASC NULLS FIRST, greptime_private.some_alt_metric.timestamp ASC NULLS FIRST [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n                Filter: greptime_private.some_alt_metric.timestamp >= TimestampMillisecond(-999, None) AND greptime_private.some_alt_metric.timestamp <= TimestampMillisecond(100000000, None) [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n                  TableScan: greptime_private.some_alt_metric [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n      SubqueryAlias: some_metric [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, __promql_metric_name:Utf8]\
            \n        Projection: some_metric.tag_0, some_metric.timestamp, some_metric.field_0, __promql_metric_name [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, __promql_metric_name:Utf8]\
            \n          Filter: prom_assert_unique_match_group(__promql_match_group_count, some_metric.tag_0) [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, __promql_metric_name:Utf8, __promql_match_group_count:Int64]\
            \n            WindowAggr: windowExpr=[[count(Int64(1)) PARTITION BY [some_metric.tag_0, some_metric.timestamp] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING AS __promql_match_group_count]] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, __promql_metric_name:Utf8, __promql_match_group_count:Int64]\
            \n              Projection: some_metric.tag_0, some_metric.timestamp, some_metric.field_0, Utf8(\"some_metric\") AS __promql_metric_name [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, __promql_metric_name:Utf8]\
            \n                PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n                  PromSeriesDivide: tags=[\"tag_0\"] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n                    Sort: some_metric.tag_0 ASC NULLS FIRST, some_metric.timestamp ASC NULLS FIRST [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n                      Filter: some_metric.timestamp >= TimestampMillisecond(-999, None) AND some_metric.timestamp <= TimestampMillisecond(100000000, None) [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n                        TableScan: some_metric [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]",
    );

    indie_query_plan_compare(query, expected).await;
}

#[tokio::test]
async fn only_equals_is_supported_for_special_matcher() {
    let queries = &[
        "some_alt_metric{__schema__!=\"greptime_private\"}",
        "some_alt_metric{__schema__=~\"lalala\"}",
        "some_alt_metric{__database__!=\"greptime_private\"}",
        "some_alt_metric{__database__=~\"lalala\"}",
    ];

    for query in queries {
        let prom_expr = parser::parse(query).unwrap();
        let eval_stmt = EvalStmt {
            expr: prom_expr,
            start: UNIX_EPOCH,
            end: UNIX_EPOCH
                .checked_add(Duration::from_secs(100_000))
                .unwrap(),
            interval: Duration::from_secs(5),
            lookback_delta: Duration::from_secs(1),
        };

        let table_provider = build_test_table_provider(
            &[
                (DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string()),
                (
                    "greptime_private".to_string(),
                    "some_alt_metric".to_string(),
                ),
            ],
            1,
            1,
        )
        .await;

        let plan =
            PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
                .await;
        assert!(plan.is_err(), "query: {:?}", query);
    }
}

#[tokio::test]
async fn native_scan_bounds_preserve_zero_lookback_and_overflow() {
    let table_provider = build_test_table_provider(
        &[(DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string())],
        1,
        1,
    )
    .await;
    let mut planner = PromPlanner {
        table_provider,
        ctx: PromPlannerContext::from_eval_stmt(&build_eval_stmt("some_metric")),
        promql_annotations: None,
    };
    planner.ctx.time_index_column = Some("timestamp".to_string());
    planner.ctx.start = 1_000;
    planner.ctx.lookback_delta = 0;
    let schema = Arc::new(
        DFSchema::try_from(ArrowSchema::new(vec![Field::new(
            "timestamp",
            ArrowDataType::Timestamp(ArrowTimeUnit::Nanosecond, None),
            false,
        )]))
        .unwrap(),
    );
    for (end, interval, windows) in [
        (1_000, 1_000, 1),
        (2_000, 1_000, 1),
        (7_201_000, 7_200_000, 2),
    ] {
        planner.ctx.end = end;
        planner.ctx.interval = interval;
        let filter = planner
            .build_time_index_filter(0, &schema)
            .unwrap()
            .unwrap()
            .to_string();
        assert_eq!(filter.matches(">=").count(), windows, "{filter}");
        assert!(
            filter.contains("TimestampNanosecond(1000000000, None)"),
            "{filter}"
        );
    }
    planner.ctx.end = i64::MAX;
    let filter = planner
        .build_time_index_filter(0, &schema)
        .unwrap()
        .unwrap()
        .to_string();
    assert!(
        filter.contains("timestamp >= TimestampNanosecond(1000000000, None)"),
        "{filter}"
    );

    // A lookback subtraction can underflow milliseconds while the upper bound remains
    // representable. Keep that upper bound so LastRow cannot select a future sample.
    let ms_schema = Arc::new(
        DFSchema::try_from(ArrowSchema::new(vec![Field::new(
            "timestamp",
            ArrowDataType::Timestamp(ArrowTimeUnit::Millisecond, None),
            false,
        )]))
        .unwrap(),
    );
    planner.ctx.start = i64::MIN + 100;
    planner.ctx.end = planner.ctx.start;
    planner.ctx.lookback_delta = 200;
    let filter = planner
        .build_time_index_filter(0, &ms_schema)
        .unwrap()
        .unwrap()
        .to_string();
    assert_eq!(
        filter,
        format!(
            "timestamp <= TimestampMillisecond({}, None)",
            i64::MIN + 100
        )
    );

    // The lower bound can also overflow while converting milliseconds to native nanoseconds.
    // Its representable upper bound still has to reach the scan.
    planner.ctx.start = 0;
    planner.ctx.end = 0;
    planner.ctx.lookback_delta = 300_000;
    let filter = planner
        .build_time_index_filter(9_223_372_036_854, &schema)
        .unwrap()
        .unwrap()
        .to_string();
    assert_eq!(
        filter,
        "timestamp <= TimestampNanosecond(-9223372036854000000, None)"
    );
}

#[tokio::test]
async fn test_non_ms_precision() {
    let catalog_list = MemoryCatalogManager::with_default_setup();
    let columns = vec![
        ColumnSchema::new(
            "tag".to_string(),
            ConcreteDataType::string_datatype(),
            false,
        ),
        ColumnSchema::new(
            "timestamp".to_string(),
            ConcreteDataType::timestamp_nanosecond_datatype(),
            false,
        )
        .with_time_index(true),
        ColumnSchema::new(
            "field".to_string(),
            ConcreteDataType::float64_datatype(),
            true,
        ),
    ];
    let schema = Arc::new(Schema::new(columns));
    let table_meta = TableMetaBuilder::empty()
        .schema(schema)
        .primary_key_indices(vec![0])
        .value_indices(vec![2])
        .next_column_id(1024)
        .build()
        .unwrap();
    let table_info = TableInfoBuilder::default()
        .name("metrics".to_string())
        .meta(table_meta)
        .build()
        .unwrap();
    let table = EmptyTable::from_table_info(&table_info);
    assert!(
        catalog_list
            .register_table_sync(RegisterTableRequest {
                catalog: DEFAULT_CATALOG_NAME.to_string(),
                schema: DEFAULT_SCHEMA_NAME.to_string(),
                table_name: "metrics".to_string(),
                table_id: 1024,
                table,
            })
            .is_ok()
    );

    let plan = PromPlanner::stmt_to_plan(
        DfTableSourceProvider::new(
            catalog_list.clone(),
            false,
            QueryContext::arc(),
            DummyDecoder::arc(),
            true,
        ),
        &EvalStmt {
            expr: parser::parse("metrics{tag = \"1\"}").unwrap(),
            start: UNIX_EPOCH,
            end: UNIX_EPOCH
                .checked_add(Duration::from_secs(100_000))
                .unwrap(),
            interval: Duration::from_secs(5),
            lookback_delta: Duration::from_secs(1),
        },
        &build_query_engine_state(),
    )
    .await
    .unwrap();
    assert_eq!(
        plan.display_indent_schema().to_string(),
        "Projection: metrics.field, metrics.tag, metrics.timestamp, Utf8(\"metrics\") AS __promql_metric_name [field:Float64;N, tag:Utf8, timestamp:Timestamp(ms), __promql_metric_name:Utf8]\
            \n  PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [field:Float64;N, tag:Utf8, timestamp:Timestamp(ms)]\
            \n    PromSeriesDivide: tags=[\"tag\"] [field:Float64;N, tag:Utf8, timestamp:Timestamp(ns)]\
            \n      Sort: metrics.tag ASC NULLS FIRST, metrics.timestamp ASC NULLS FIRST [field:Float64;N, tag:Utf8, timestamp:Timestamp(ns)]\
            \n        Filter: metrics.tag = Utf8(\"1\") AND metrics.timestamp > TimestampNanosecond(-1000000000, None) AND metrics.timestamp <= TimestampNanosecond(100000000000000, None) [field:Float64;N, tag:Utf8, timestamp:Timestamp(ns)]\
            \n          Projection: metrics.field, metrics.tag, metrics.timestamp [field:Float64;N, tag:Utf8, timestamp:Timestamp(ns)]\
            \n            TableScan: metrics [tag:Utf8, timestamp:Timestamp(ns), field:Float64;N]"
    );
    let plan = PromPlanner::stmt_to_plan(
        DfTableSourceProvider::new(
            catalog_list.clone(),
            false,
            QueryContext::arc(),
            DummyDecoder::arc(),
            true,
        ),
        &EvalStmt {
            expr: parser::parse("avg_over_time(metrics{tag = \"1\"}[5s])").unwrap(),
            start: UNIX_EPOCH,
            end: UNIX_EPOCH
                .checked_add(Duration::from_secs(100_000))
                .unwrap(),
            interval: Duration::from_secs(5),
            lookback_delta: Duration::from_secs(1),
        },
        &build_query_engine_state(),
    )
    .await
    .unwrap();
    assert_eq!(
        plan.display_indent_schema().to_string(),
        "Projection: metrics.timestamp, prom_avg_over_time(timestamp_range,field), metrics.tag [timestamp:Timestamp(ms), prom_avg_over_time(timestamp_range,field):Float64;N, tag:Utf8]\
            \n  Filter: prom_avg_over_time(timestamp_range,field) IS NOT NULL [timestamp:Timestamp(ms), prom_avg_over_time(timestamp_range,field):Float64;N, tag:Utf8, __promql_metric_name:Utf8]\
            \n    Projection: metrics.timestamp, prom_avg_over_time(timestamp_range, field) AS prom_avg_over_time(timestamp_range,field), metrics.tag, __promql_metric_name [timestamp:Timestamp(ms), prom_avg_over_time(timestamp_range,field):Float64;N, tag:Utf8, __promql_metric_name:Utf8]\
            \n      Projection: field, metrics.tag, metrics.timestamp, timestamp_range, Utf8(\"metrics\") AS __promql_metric_name [field:Dictionary(Int64, Float64);N, tag:Utf8, timestamp:Timestamp(ms), timestamp_range:Dictionary(Int64, Timestamp(ms)), __promql_metric_name:Utf8]\
            \n        PromRangeManipulate: req range=[0..100000000], interval=[5000], eval range=[5000], time index=[timestamp], values=[\"field\"] [field:Dictionary(Int64, Float64);N, tag:Utf8, timestamp:Timestamp(ms), timestamp_range:Dictionary(Int64, Timestamp(ms))]\
            \n          PromSeriesNormalize: offset=[0], time index=[timestamp], filter NaN: [true] [field:Float64;N, tag:Utf8, timestamp:Timestamp(ns)]\
            \n            PromSeriesDivide: tags=[\"tag\"] [field:Float64;N, tag:Utf8, timestamp:Timestamp(ns)]\
            \n              Sort: metrics.tag ASC NULLS FIRST, metrics.timestamp ASC NULLS FIRST [field:Float64;N, tag:Utf8, timestamp:Timestamp(ns)]\
            \n                Filter: metrics.tag = Utf8(\"1\") AND metrics.timestamp > TimestampNanosecond(-5000000000, None) AND metrics.timestamp <= TimestampNanosecond(100000000000000, None) [field:Float64;N, tag:Utf8, timestamp:Timestamp(ns)]\
            \n                  Projection: metrics.field, metrics.tag, metrics.timestamp [field:Float64;N, tag:Utf8, timestamp:Timestamp(ns)]\
            \n                    TableScan: metrics [tag:Utf8, timestamp:Timestamp(ns), field:Float64;N]"
    );
}

#[tokio::test]
async fn test_nonexistent_label() {
    // template
    let mut eval_stmt = EvalStmt {
        expr: PromExpr::NumberLiteral(NumberLiteral { val: 1.0 }),
        start: UNIX_EPOCH,
        end: UNIX_EPOCH
            .checked_add(Duration::from_secs(100_000))
            .unwrap(),
        interval: Duration::from_secs(5),
        lookback_delta: Duration::from_secs(1),
    };

    let case = r#"some_metric{nonexistent="hi"}"#;
    let prom_expr = parser::parse(case).unwrap();
    eval_stmt.expr = prom_expr;
    let table_provider = build_test_table_provider(
        &[(DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string())],
        3,
        3,
    )
    .await;
    // Should be ok
    let _ = PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
        .await
        .unwrap();
}

#[tokio::test]
async fn test_label_join() {
    let prom_expr =
        parser::parse("label_join(up{tag_0='api-server'}, 'foo', ',', 'tag_1', 'tag_2', 'tag_3')")
            .unwrap();
    let eval_stmt = EvalStmt {
        expr: prom_expr,
        start: UNIX_EPOCH,
        end: UNIX_EPOCH
            .checked_add(Duration::from_secs(100_000))
            .unwrap(),
        interval: Duration::from_secs(5),
        lookback_delta: Duration::from_secs(1),
    };

    let table_provider =
        build_test_table_provider(&[(DEFAULT_SCHEMA_NAME.to_string(), "up".to_string())], 4, 1)
            .await;
    let plan = PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
        .await
        .unwrap();

    let expected = r#"
Filter: up.field_0 IS NOT NULL [timestamp:Timestamp(ms), field_0:Float64;N, foo:Utf8;N, tag_0:Utf8, tag_1:Utf8, tag_2:Utf8, tag_3:Utf8, __promql_metric_name:Utf8]
  Projection: up.timestamp, up.field_0, nullif(concat_ws(Utf8(","), coalesce(up.tag_1, Utf8("")), coalesce(up.tag_2, Utf8("")), coalesce(up.tag_3, Utf8(""))), Utf8("")) AS foo, up.tag_0, up.tag_1, up.tag_2, up.tag_3, __promql_metric_name [timestamp:Timestamp(ms), field_0:Float64;N, foo:Utf8;N, tag_0:Utf8, tag_1:Utf8, tag_2:Utf8, tag_3:Utf8, __promql_metric_name:Utf8]
    Projection: up.tag_0, up.tag_1, up.tag_2, up.tag_3, up.timestamp, up.field_0, Utf8("up") AS __promql_metric_name [tag_0:Utf8, tag_1:Utf8, tag_2:Utf8, tag_3:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, __promql_metric_name:Utf8]
      PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, tag_1:Utf8, tag_2:Utf8, tag_3:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]
        PromSeriesDivide: tags=["tag_0", "tag_1", "tag_2", "tag_3"] [tag_0:Utf8, tag_1:Utf8, tag_2:Utf8, tag_3:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]
          Sort: up.tag_0 ASC NULLS FIRST, up.tag_1 ASC NULLS FIRST, up.tag_2 ASC NULLS FIRST, up.tag_3 ASC NULLS FIRST, up.timestamp ASC NULLS FIRST [tag_0:Utf8, tag_1:Utf8, tag_2:Utf8, tag_3:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]
            Filter: up.tag_0 = Utf8("api-server") AND up.timestamp >= TimestampMillisecond(-999, None) AND up.timestamp <= TimestampMillisecond(100000000, None) [tag_0:Utf8, tag_1:Utf8, tag_2:Utf8, tag_3:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]
              TableScan: up [tag_0:Utf8, tag_1:Utf8, tag_2:Utf8, tag_3:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]"#;

    let ret = plan.display_indent_schema().to_string();
    assert_eq!(format!("\n{ret}"), expected, "\n{}", ret);
}

#[tokio::test]
async fn test_label_replace() {
    let prom_expr =
        parser::parse("label_replace(up{tag_0=\"a:c\"}, \"foo\", \"$1\", \"tag_0\", \"(.*):.*\")")
            .unwrap();
    let eval_stmt = EvalStmt {
        expr: prom_expr,
        start: UNIX_EPOCH,
        end: UNIX_EPOCH
            .checked_add(Duration::from_secs(100_000))
            .unwrap(),
        interval: Duration::from_secs(5),
        lookback_delta: Duration::from_secs(1),
    };

    let table_provider =
        build_test_table_provider(&[(DEFAULT_SCHEMA_NAME.to_string(), "up".to_string())], 1, 1)
            .await;
    let plan = PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
        .await
        .unwrap();

    let expected = r#"
Filter: up.field_0 IS NOT NULL [timestamp:Timestamp(ms), field_0:Float64;N, foo:Utf8;N, tag_0:Utf8, __promql_metric_name:Utf8]
  Projection: up.timestamp, up.field_0, CASE WHEN regexp_like(coalesce(up.tag_0, Utf8("")), Utf8("^(?s:(.*):.*)$")) THEN nullif(regexp_replace(coalesce(up.tag_0, Utf8("")), Utf8("^(?s:(.*):.*)$"), Utf8("$1")), Utf8("")) ELSE Utf8(NULL) END AS foo, up.tag_0, __promql_metric_name [timestamp:Timestamp(ms), field_0:Float64;N, foo:Utf8;N, tag_0:Utf8, __promql_metric_name:Utf8]
    Projection: up.tag_0, up.timestamp, up.field_0, Utf8("up") AS __promql_metric_name [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, __promql_metric_name:Utf8]
      PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]
        PromSeriesDivide: tags=["tag_0"] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]
          Sort: up.tag_0 ASC NULLS FIRST, up.timestamp ASC NULLS FIRST [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]
            Filter: up.tag_0 = Utf8("a:c") AND up.timestamp >= TimestampMillisecond(-999, None) AND up.timestamp <= TimestampMillisecond(100000000, None) [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]
              TableScan: up [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]"#;

    let ret = plan.display_indent_schema().to_string();
    assert_eq!(format!("\n{ret}"), expected, "\n{}", ret);
}

#[tokio::test]
async fn label_replace_aggregation_queries_plan_successfully() {
    let aggregate = r#"sum by (foo) (label_replace(some_metric, "foo", "$1", "tag_0", "(.*)"))"#;
    let queries = [
        aggregate.to_string(),
        format!("{aggregate} <= 10"),
        format!("{aggregate} * 0.8"),
        format!("0.8 * {aggregate}"),
        format!("{aggregate} <= {aggregate} * 0.8"),
    ];
    let state = build_query_engine_state();
    let mut failures = Vec::new();

    for query in queries {
        let table_provider = build_test_table_provider(
            &[(DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string())],
            1,
            1,
        )
        .await;
        if let Err(error) =
            PromPlanner::stmt_to_plan(table_provider, &build_eval_stmt(&query), &state).await
        {
            failures.push(format!("{query}: {error:?}"));
        }
    }

    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[tokio::test]
async fn test_matchers_to_expr() {
    let mut eval_stmt = EvalStmt {
        expr: PromExpr::NumberLiteral(NumberLiteral { val: 1.0 }),
        start: UNIX_EPOCH,
        end: UNIX_EPOCH
            .checked_add(Duration::from_secs(100_000))
            .unwrap(),
        interval: Duration::from_secs(5),
        lookback_delta: Duration::from_secs(1),
    };
    let case =
        r#"sum(prometheus_tsdb_head_series{tag_1=~"(10.0.160.237:8080|10.0.160.237:9090)"})"#;

    let prom_expr = parser::parse(case).unwrap();
    eval_stmt.expr = prom_expr;
    let table_provider = build_test_table_provider(
        &[(
            DEFAULT_SCHEMA_NAME.to_string(),
            "prometheus_tsdb_head_series".to_string(),
        )],
        3,
        3,
    )
    .await;
    let plan = PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
        .await
        .unwrap();
    let expected = "Sort: prometheus_tsdb_head_series.timestamp ASC NULLS LAST [timestamp:Timestamp(ms), sum(prometheus_tsdb_head_series.field_0):Float64;N, sum(prometheus_tsdb_head_series.field_1):Float64;N, sum(prometheus_tsdb_head_series.field_2):Float64;N]\
            \n  Aggregate: groupBy=[[prometheus_tsdb_head_series.timestamp]], aggr=[[sum(prometheus_tsdb_head_series.field_0), sum(prometheus_tsdb_head_series.field_1), sum(prometheus_tsdb_head_series.field_2)]] [timestamp:Timestamp(ms), sum(prometheus_tsdb_head_series.field_0):Float64;N, sum(prometheus_tsdb_head_series.field_1):Float64;N, sum(prometheus_tsdb_head_series.field_2):Float64;N]\
            \n    Projection: prometheus_tsdb_head_series.tag_0, prometheus_tsdb_head_series.tag_1, prometheus_tsdb_head_series.tag_2, prometheus_tsdb_head_series.timestamp, prometheus_tsdb_head_series.field_0, prometheus_tsdb_head_series.field_1, prometheus_tsdb_head_series.field_2, Utf8(\"prometheus_tsdb_head_series\") AS __promql_metric_name [tag_0:Utf8, tag_1:Utf8, tag_2:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, field_1:Float64;N, field_2:Float64;N, __promql_metric_name:Utf8]\
            \n      PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, tag_1:Utf8, tag_2:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, field_1:Float64;N, field_2:Float64;N]\
            \n        PromSeriesDivide: tags=[\"tag_0\", \"tag_1\", \"tag_2\"] [tag_0:Utf8, tag_1:Utf8, tag_2:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, field_1:Float64;N, field_2:Float64;N]\
            \n          Sort: prometheus_tsdb_head_series.tag_0 ASC NULLS FIRST, prometheus_tsdb_head_series.tag_1 ASC NULLS FIRST, prometheus_tsdb_head_series.tag_2 ASC NULLS FIRST, prometheus_tsdb_head_series.timestamp ASC NULLS FIRST [tag_0:Utf8, tag_1:Utf8, tag_2:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, field_1:Float64;N, field_2:Float64;N]\
            \n            Filter: prometheus_tsdb_head_series.tag_1 ~ Utf8(\"^(?:(10.0.160.237:8080|10.0.160.237:9090))$\") AND prometheus_tsdb_head_series.timestamp >= TimestampMillisecond(-999, None) AND prometheus_tsdb_head_series.timestamp <= TimestampMillisecond(100000000, None) [tag_0:Utf8, tag_1:Utf8, tag_2:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, field_1:Float64;N, field_2:Float64;N]\
            \n              TableScan: prometheus_tsdb_head_series [tag_0:Utf8, tag_1:Utf8, tag_2:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, field_1:Float64;N, field_2:Float64;N]";
    assert_eq!(plan.display_indent_schema().to_string(), expected);
}

#[tokio::test]
async fn test_topk_expr() {
    let mut eval_stmt = EvalStmt {
        expr: PromExpr::NumberLiteral(NumberLiteral { val: 1.0 }),
        start: UNIX_EPOCH,
        end: UNIX_EPOCH
            .checked_add(Duration::from_secs(100_000))
            .unwrap(),
        interval: Duration::from_secs(5),
        lookback_delta: Duration::from_secs(1),
    };
    let case = r#"topk(10, sum(prometheus_tsdb_head_series{ip=~"(10.0.160.237:8080|10.0.160.237:9090)"}) by (ip))"#;

    let prom_expr = parser::parse(case).unwrap();
    eval_stmt.expr = prom_expr;
    let table_provider = build_test_table_provider_with_fields(
        &[
            (
                DEFAULT_SCHEMA_NAME.to_string(),
                "prometheus_tsdb_head_series".to_string(),
            ),
            (
                DEFAULT_SCHEMA_NAME.to_string(),
                "http_server_requests_seconds_count".to_string(),
            ),
        ],
        &["ip"],
    )
    .await;

    let plan = PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
        .await
        .unwrap();
    let expected = "Projection: sum(prometheus_tsdb_head_series.greptime_value), prometheus_tsdb_head_series.ip, prometheus_tsdb_head_series.greptime_timestamp [sum(prometheus_tsdb_head_series.greptime_value):Float64;N, ip:Utf8, greptime_timestamp:Timestamp(ms)]\
            \n  Sort: prometheus_tsdb_head_series.greptime_timestamp ASC NULLS LAST, row_number() PARTITION BY [prometheus_tsdb_head_series.greptime_timestamp] ORDER BY [sum(prometheus_tsdb_head_series.greptime_value) DESC NULLS FIRST, prometheus_tsdb_head_series.ip DESC NULLS FIRST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ASC NULLS LAST [ip:Utf8, greptime_timestamp:Timestamp(ms), sum(prometheus_tsdb_head_series.greptime_value):Float64;N, row_number() PARTITION BY [prometheus_tsdb_head_series.greptime_timestamp] ORDER BY [sum(prometheus_tsdb_head_series.greptime_value) DESC NULLS FIRST, prometheus_tsdb_head_series.ip DESC NULLS FIRST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW:UInt64]\
            \n    Filter: row_number() PARTITION BY [prometheus_tsdb_head_series.greptime_timestamp] ORDER BY [sum(prometheus_tsdb_head_series.greptime_value) DESC NULLS FIRST, prometheus_tsdb_head_series.ip DESC NULLS FIRST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW <= Float64(10) [ip:Utf8, greptime_timestamp:Timestamp(ms), sum(prometheus_tsdb_head_series.greptime_value):Float64;N, row_number() PARTITION BY [prometheus_tsdb_head_series.greptime_timestamp] ORDER BY [sum(prometheus_tsdb_head_series.greptime_value) DESC NULLS FIRST, prometheus_tsdb_head_series.ip DESC NULLS FIRST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW:UInt64]\
            \n      WindowAggr: windowExpr=[[row_number() PARTITION BY [prometheus_tsdb_head_series.greptime_timestamp] ORDER BY [sum(prometheus_tsdb_head_series.greptime_value) DESC NULLS FIRST, prometheus_tsdb_head_series.ip DESC NULLS FIRST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]] [ip:Utf8, greptime_timestamp:Timestamp(ms), sum(prometheus_tsdb_head_series.greptime_value):Float64;N, row_number() PARTITION BY [prometheus_tsdb_head_series.greptime_timestamp] ORDER BY [sum(prometheus_tsdb_head_series.greptime_value) DESC NULLS FIRST, prometheus_tsdb_head_series.ip DESC NULLS FIRST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW:UInt64]\
            \n        Sort: prometheus_tsdb_head_series.ip ASC NULLS LAST, prometheus_tsdb_head_series.greptime_timestamp ASC NULLS LAST [ip:Utf8, greptime_timestamp:Timestamp(ms), sum(prometheus_tsdb_head_series.greptime_value):Float64;N]\
            \n          Aggregate: groupBy=[[prometheus_tsdb_head_series.ip, prometheus_tsdb_head_series.greptime_timestamp]], aggr=[[sum(prometheus_tsdb_head_series.greptime_value)]] [ip:Utf8, greptime_timestamp:Timestamp(ms), sum(prometheus_tsdb_head_series.greptime_value):Float64;N]\
            \n            Projection: prometheus_tsdb_head_series.ip, prometheus_tsdb_head_series.greptime_timestamp, prometheus_tsdb_head_series.greptime_value, Utf8(\"prometheus_tsdb_head_series\") AS __promql_metric_name [ip:Utf8, greptime_timestamp:Timestamp(ms), greptime_value:Float64;N, __promql_metric_name:Utf8]\
            \n              PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[greptime_timestamp] [ip:Utf8, greptime_timestamp:Timestamp(ms), greptime_value:Float64;N]\
            \n                PromSeriesDivide: tags=[\"ip\"] [ip:Utf8, greptime_timestamp:Timestamp(ms), greptime_value:Float64;N]\
            \n                  Sort: prometheus_tsdb_head_series.ip ASC NULLS FIRST, prometheus_tsdb_head_series.greptime_timestamp ASC NULLS FIRST [ip:Utf8, greptime_timestamp:Timestamp(ms), greptime_value:Float64;N]\
            \n                    Filter: prometheus_tsdb_head_series.ip ~ Utf8(\"^(?:(10.0.160.237:8080|10.0.160.237:9090))$\") AND prometheus_tsdb_head_series.greptime_timestamp >= TimestampMillisecond(-999, None) AND prometheus_tsdb_head_series.greptime_timestamp <= TimestampMillisecond(100000000, None) [ip:Utf8, greptime_timestamp:Timestamp(ms), greptime_value:Float64;N]\
            \n                      TableScan: prometheus_tsdb_head_series [ip:Utf8, greptime_timestamp:Timestamp(ms), greptime_value:Float64;N]";

    assert_eq!(plan.display_indent_schema().to_string(), expected);
}

#[tokio::test]
async fn test_count_values_expr() {
    let mut eval_stmt = EvalStmt {
        expr: PromExpr::NumberLiteral(NumberLiteral { val: 1.0 }),
        start: UNIX_EPOCH,
        end: UNIX_EPOCH
            .checked_add(Duration::from_secs(100_000))
            .unwrap(),
        interval: Duration::from_secs(5),
        lookback_delta: Duration::from_secs(1),
    };
    let case = r#"count_values('series', prometheus_tsdb_head_series{ip=~"(10.0.160.237:8080|10.0.160.237:9090)"}) by (ip)"#;

    let prom_expr = parser::parse(case).unwrap();
    eval_stmt.expr = prom_expr;
    let table_provider = build_test_table_provider_with_fields(
        &[
            (
                DEFAULT_SCHEMA_NAME.to_string(),
                "prometheus_tsdb_head_series".to_string(),
            ),
            (
                DEFAULT_SCHEMA_NAME.to_string(),
                "http_server_requests_seconds_count".to_string(),
            ),
        ],
        &["ip"],
    )
    .await;

    let plan = PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
        .await
        .unwrap();
    let expected = "Sort: prometheus_tsdb_head_series.ip ASC NULLS LAST, prometheus_tsdb_head_series.greptime_timestamp ASC NULLS LAST, prometheus_tsdb_head_series.series ASC NULLS LAST [count(prometheus_tsdb_head_series.greptime_value):Int64, ip:Utf8, greptime_timestamp:Timestamp(ms), series:Utf8;N]\
        \n  Projection: count(prometheus_tsdb_head_series.greptime_value), prometheus_tsdb_head_series.ip, prometheus_tsdb_head_series.greptime_timestamp, prom_float_to_string(prometheus_tsdb_head_series.greptime_value) AS series [count(prometheus_tsdb_head_series.greptime_value):Int64, ip:Utf8, greptime_timestamp:Timestamp(ms), series:Utf8;N]\
        \n    Aggregate: groupBy=[[prometheus_tsdb_head_series.ip, prometheus_tsdb_head_series.greptime_timestamp, prom_float_to_string(prometheus_tsdb_head_series.greptime_value)]], aggr=[[count(prometheus_tsdb_head_series.greptime_value)]] [ip:Utf8, greptime_timestamp:Timestamp(ms), prom_float_to_string(prometheus_tsdb_head_series.greptime_value):Utf8;N, count(prometheus_tsdb_head_series.greptime_value):Int64]\
        \n      Projection: prometheus_tsdb_head_series.ip, prometheus_tsdb_head_series.greptime_timestamp, prometheus_tsdb_head_series.greptime_value, Utf8(\"prometheus_tsdb_head_series\") AS __promql_metric_name [ip:Utf8, greptime_timestamp:Timestamp(ms), greptime_value:Float64;N, __promql_metric_name:Utf8]\
        \n        PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[greptime_timestamp] [ip:Utf8, greptime_timestamp:Timestamp(ms), greptime_value:Float64;N]\
        \n          PromSeriesDivide: tags=[\"ip\"] [ip:Utf8, greptime_timestamp:Timestamp(ms), greptime_value:Float64;N]\
        \n            Sort: prometheus_tsdb_head_series.ip ASC NULLS FIRST, prometheus_tsdb_head_series.greptime_timestamp ASC NULLS FIRST [ip:Utf8, greptime_timestamp:Timestamp(ms), greptime_value:Float64;N]\
        \n              Filter: prometheus_tsdb_head_series.ip ~ Utf8(\"^(?:(10.0.160.237:8080|10.0.160.237:9090))$\") AND prometheus_tsdb_head_series.greptime_timestamp >= TimestampMillisecond(-999, None) AND prometheus_tsdb_head_series.greptime_timestamp <= TimestampMillisecond(100000000, None) [ip:Utf8, greptime_timestamp:Timestamp(ms), greptime_value:Float64;N]\
        \n                TableScan: prometheus_tsdb_head_series [ip:Utf8, greptime_timestamp:Timestamp(ms), greptime_value:Float64;N]";

    assert_eq!(plan.display_indent_schema().to_string(), expected);
}

#[tokio::test]
async fn test_value_alias() {
    let mut eval_stmt = EvalStmt {
        expr: PromExpr::NumberLiteral(NumberLiteral { val: 1.0 }),
        start: UNIX_EPOCH,
        end: UNIX_EPOCH
            .checked_add(Duration::from_secs(100_000))
            .unwrap(),
        interval: Duration::from_secs(5),
        lookback_delta: Duration::from_secs(1),
    };
    let case = r#"count_values('series', prometheus_tsdb_head_series{ip=~"(10.0.160.237:8080|10.0.160.237:9090)"}) by (ip)"#;

    let prom_expr = parser::parse(case).unwrap();
    eval_stmt.expr = prom_expr;
    eval_stmt = QueryLanguageParser::apply_alias_extension(eval_stmt, "my_series");
    let table_provider = build_test_table_provider_with_fields(
        &[
            (
                DEFAULT_SCHEMA_NAME.to_string(),
                "prometheus_tsdb_head_series".to_string(),
            ),
            (
                DEFAULT_SCHEMA_NAME.to_string(),
                "http_server_requests_seconds_count".to_string(),
            ),
        ],
        &["ip"],
    )
    .await;

    let plan = PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
        .await
        .unwrap();
    let expected = r#"
Projection: count(prometheus_tsdb_head_series.greptime_value) AS my_series, prometheus_tsdb_head_series.ip, prometheus_tsdb_head_series.series, prometheus_tsdb_head_series.greptime_timestamp [my_series:Int64, ip:Utf8, series:Utf8;N, greptime_timestamp:Timestamp(ms)]
  Sort: prometheus_tsdb_head_series.ip ASC NULLS LAST, prometheus_tsdb_head_series.greptime_timestamp ASC NULLS LAST, prometheus_tsdb_head_series.series ASC NULLS LAST [count(prometheus_tsdb_head_series.greptime_value):Int64, ip:Utf8, greptime_timestamp:Timestamp(ms), series:Utf8;N]
    Projection: count(prometheus_tsdb_head_series.greptime_value), prometheus_tsdb_head_series.ip, prometheus_tsdb_head_series.greptime_timestamp, prom_float_to_string(prometheus_tsdb_head_series.greptime_value) AS series [count(prometheus_tsdb_head_series.greptime_value):Int64, ip:Utf8, greptime_timestamp:Timestamp(ms), series:Utf8;N]
      Aggregate: groupBy=[[prometheus_tsdb_head_series.ip, prometheus_tsdb_head_series.greptime_timestamp, prom_float_to_string(prometheus_tsdb_head_series.greptime_value)]], aggr=[[count(prometheus_tsdb_head_series.greptime_value)]] [ip:Utf8, greptime_timestamp:Timestamp(ms), prom_float_to_string(prometheus_tsdb_head_series.greptime_value):Utf8;N, count(prometheus_tsdb_head_series.greptime_value):Int64]
        Projection: prometheus_tsdb_head_series.ip, prometheus_tsdb_head_series.greptime_timestamp, prometheus_tsdb_head_series.greptime_value, Utf8("prometheus_tsdb_head_series") AS __promql_metric_name [ip:Utf8, greptime_timestamp:Timestamp(ms), greptime_value:Float64;N, __promql_metric_name:Utf8]
          PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[greptime_timestamp] [ip:Utf8, greptime_timestamp:Timestamp(ms), greptime_value:Float64;N]
            PromSeriesDivide: tags=["ip"] [ip:Utf8, greptime_timestamp:Timestamp(ms), greptime_value:Float64;N]
              Sort: prometheus_tsdb_head_series.ip ASC NULLS FIRST, prometheus_tsdb_head_series.greptime_timestamp ASC NULLS FIRST [ip:Utf8, greptime_timestamp:Timestamp(ms), greptime_value:Float64;N]
                Filter: prometheus_tsdb_head_series.ip ~ Utf8("^(?:(10.0.160.237:8080|10.0.160.237:9090))$") AND prometheus_tsdb_head_series.greptime_timestamp >= TimestampMillisecond(-999, None) AND prometheus_tsdb_head_series.greptime_timestamp <= TimestampMillisecond(100000000, None) [ip:Utf8, greptime_timestamp:Timestamp(ms), greptime_value:Float64;N]
                  TableScan: prometheus_tsdb_head_series [ip:Utf8, greptime_timestamp:Timestamp(ms), greptime_value:Float64;N]"#;
    assert_eq!(format!("\n{}", plan.display_indent_schema()), expected);
}

#[tokio::test]
async fn test_quantile_expr() {
    let mut eval_stmt = EvalStmt {
        expr: PromExpr::NumberLiteral(NumberLiteral { val: 1.0 }),
        start: UNIX_EPOCH,
        end: UNIX_EPOCH
            .checked_add(Duration::from_secs(100_000))
            .unwrap(),
        interval: Duration::from_secs(5),
        lookback_delta: Duration::from_secs(1),
    };
    let case = r#"quantile(0.3, sum(prometheus_tsdb_head_series{ip=~"(10.0.160.237:8080|10.0.160.237:9090)"}) by (ip))"#;

    let prom_expr = parser::parse(case).unwrap();
    eval_stmt.expr = prom_expr;
    let table_provider = build_test_table_provider_with_fields(
        &[
            (
                DEFAULT_SCHEMA_NAME.to_string(),
                "prometheus_tsdb_head_series".to_string(),
            ),
            (
                DEFAULT_SCHEMA_NAME.to_string(),
                "http_server_requests_seconds_count".to_string(),
            ),
        ],
        &["ip"],
    )
    .await;

    let plan = PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
        .await
        .unwrap();
    let expected = "Sort: prometheus_tsdb_head_series.greptime_timestamp ASC NULLS LAST [greptime_timestamp:Timestamp(ms), quantile(Float64(0.3),sum(prometheus_tsdb_head_series.greptime_value)):Float64;N]\
            \n  Aggregate: groupBy=[[prometheus_tsdb_head_series.greptime_timestamp]], aggr=[[quantile(Float64(0.3), sum(prometheus_tsdb_head_series.greptime_value))]] [greptime_timestamp:Timestamp(ms), quantile(Float64(0.3),sum(prometheus_tsdb_head_series.greptime_value)):Float64;N]\
            \n    Sort: prometheus_tsdb_head_series.ip ASC NULLS LAST, prometheus_tsdb_head_series.greptime_timestamp ASC NULLS LAST [ip:Utf8, greptime_timestamp:Timestamp(ms), sum(prometheus_tsdb_head_series.greptime_value):Float64;N]\
            \n      Aggregate: groupBy=[[prometheus_tsdb_head_series.ip, prometheus_tsdb_head_series.greptime_timestamp]], aggr=[[sum(prometheus_tsdb_head_series.greptime_value)]] [ip:Utf8, greptime_timestamp:Timestamp(ms), sum(prometheus_tsdb_head_series.greptime_value):Float64;N]\
            \n        Projection: prometheus_tsdb_head_series.ip, prometheus_tsdb_head_series.greptime_timestamp, prometheus_tsdb_head_series.greptime_value, Utf8(\"prometheus_tsdb_head_series\") AS __promql_metric_name [ip:Utf8, greptime_timestamp:Timestamp(ms), greptime_value:Float64;N, __promql_metric_name:Utf8]\
            \n          PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[greptime_timestamp] [ip:Utf8, greptime_timestamp:Timestamp(ms), greptime_value:Float64;N]\
            \n            PromSeriesDivide: tags=[\"ip\"] [ip:Utf8, greptime_timestamp:Timestamp(ms), greptime_value:Float64;N]\
            \n              Sort: prometheus_tsdb_head_series.ip ASC NULLS FIRST, prometheus_tsdb_head_series.greptime_timestamp ASC NULLS FIRST [ip:Utf8, greptime_timestamp:Timestamp(ms), greptime_value:Float64;N]\
            \n                Filter: prometheus_tsdb_head_series.ip ~ Utf8(\"^(?:(10.0.160.237:8080|10.0.160.237:9090))$\") AND prometheus_tsdb_head_series.greptime_timestamp >= TimestampMillisecond(-999, None) AND prometheus_tsdb_head_series.greptime_timestamp <= TimestampMillisecond(100000000, None) [ip:Utf8, greptime_timestamp:Timestamp(ms), greptime_value:Float64;N]\
            \n                  TableScan: prometheus_tsdb_head_series [ip:Utf8, greptime_timestamp:Timestamp(ms), greptime_value:Float64;N]";

    assert_eq!(plan.display_indent_schema().to_string(), expected);
}

#[tokio::test]
async fn test_or_not_exists_table_label() {
    let state = build_query_engine_state();
    let provider = build_test_table_provider_with_fields(
        &[(DEFAULT_SCHEMA_NAME.to_string(), "normal_metric".to_string())],
        &["job"],
    )
    .await;
    let raw = PromPlanner::stmt_to_plan(
        provider,
        &build_eval_stmt(r#"missing_metric or on(absent_label) normal_metric"#),
        &state,
    )
    .await
    .unwrap();
    // `on(absent_label)` names no label either operand carries, so the matching compares the
    // timestamp alone and the plan generates no internal match key. The result keeps the marked
    // metric-name identity of its left side.
    assert_eq!(
        PromPlanner::metric_name_column(raw.schema())
            .unwrap()
            .map(|marker| marker.name),
        Some(PROMQL_METRIC_NAME_COLUMN.to_string()),
        "{}",
        raw.display_indent()
    );
    assert_no_internal_or_keys(raw.schema());
    let (optimized, batches) = execute(raw, &state).await;
    assert_no_internal_or_keys(optimized.schema());
    assert!(batches.iter().all(|batch| {
        batch
            .schema()
            .fields()
            .iter()
            .all(|field| !field.name().starts_with("__promql_or_match_"))
    }));
    // `missing_metric` does not exist and `normal_metric` holds no rows, so the OR emits none.
    assert_eq!(
        batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        0,
        "{batches:?}"
    );
}

#[tokio::test]
async fn test_histogram_quantile_missing_le_column() {
    let mut eval_stmt = EvalStmt {
        expr: PromExpr::NumberLiteral(NumberLiteral { val: 1.0 }),
        start: UNIX_EPOCH,
        end: UNIX_EPOCH
            .checked_add(Duration::from_secs(100_000))
            .unwrap(),
        interval: Duration::from_secs(5),
        lookback_delta: Duration::from_secs(1),
    };

    // Test case: histogram_quantile with a table that doesn't have 'le' column
    let case = r#"histogram_quantile(0.99, sum by(pod,instance,le) (rate(non_existent_histogram_bucket{instance=~"xxx"}[1m])))"#;

    let prom_expr = parser::parse(case).unwrap();
    eval_stmt.expr = prom_expr;

    // Create a table provider with a table that doesn't have 'le' column
    let table_provider = build_test_table_provider_with_fields(
        &[(
            DEFAULT_SCHEMA_NAME.to_string(),
            "non_existent_histogram_bucket".to_string(),
        )],
        &["pod", "instance"], // Note: no 'le' column
    )
    .await;

    // Should return empty result instead of error
    let result =
        PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state()).await;

    // This should succeed now (returning empty result) instead of failing with "Cannot find column le"
    assert!(
        result.is_ok(),
        "Expected successful plan creation with empty result, but got error: {:?}",
        result.err()
    );

    // Verify that the result is an EmptyRelation
    let plan = result.unwrap();
    match plan {
        LogicalPlan::EmptyRelation(_) => {
            // This is what we expect
        }
        _ => panic!("Expected EmptyRelation, but got: {:?}", plan),
    }
}

#[tokio::test]
async fn test_direct_or_normalizes_missing_match_labels() {
    type Case<'a> = (
        Option<Option<&'a str>>,
        Option<Option<&'a str>>,
        i64,
        i64,
        &'a [(f64, Option<&'a str>)],
    );

    let modifier = or_modifier("lhs or on(k) rhs");
    #[rustfmt::skip]
    let cases: &[Case<'_>] = &[
        (None, None, 1, 1, &[(1.0, None)]),
        (None, Some(Some("")), 1, 1, &[(1.0, None)]),
        (Some(Some("")), None, 1, 1, &[(1.0, Some(""))]),
        (None, Some(Some("r")), 1, 1, &[(1.0, None), (2.0, Some("r"))]),
        (Some(Some("l")), None, 1, 1, &[(1.0, Some("l")), (2.0, None)]),
        (Some(None), Some(Some("")), 1, 1, &[(1.0, None)]),
        (Some(None), Some(Some("r")), 1, 1, &[(1.0, None), (2.0, Some("r"))]),
        (Some(Some("same")), Some(Some("same")), 1, 2, &[(1.0, Some("same")), (2.0, Some("same"))]),
    ];
    for &(left, right, left_ts, right_ts, expected) in cases {
        let (optimized, batches) = run(
            &matrix_source("lhs", left, left_ts, 1.0),
            &matrix_source("rhs", right, right_ts, 2.0),
            matrix_context("lhs", left),
            matrix_context("rhs", right),
            &modifier,
        )
        .await;
        assert_no_internal_or_keys(optimized.schema());
        assert_eq!(
            rows(&batches),
            expected
                .iter()
                .map(|(value, label)| (*value, label.map(str::to_string)))
                .collect::<Vec<_>>()
        );
    }
}

#[tokio::test]
async fn test_direct_or_match_modifiers() {
    for (modifier, left, right, expected) in [
        (None, "left", "right", 2),
        (or_modifier("lhs or on(k) rhs"), "same", "same", 1),
        (or_modifier("lhs or on() rhs"), "left", "right", 1),
        (or_modifier("lhs or ignoring(k) rhs"), "left", "right", 1),
    ] {
        let (_, batches) = run(
            &matrix_source("lhs", Some(Some(left)), 1, 1.0),
            &matrix_source("rhs", Some(Some(right)), 1, 2.0),
            direct_or_context("lhs", &["job", "k"], "v"),
            direct_or_context("rhs", &["job", "k"], "v"),
            &modifier,
        )
        .await;
        assert_eq!(
            batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
            expected
        );
    }
}

#[tokio::test]
async fn test_direct_or_nested_projection_uses_left_context() {
    let left = matrix_source("lhs", Some(Some("k")), 1, 1.0);
    let right = matrix_source("rhs", Some(Some("k")), 1, 2.0);
    let raw = plan_direct_or(
        scan(&left),
        scan(&right),
        direct_or_context("lhs", &["job", "k"], "v"),
        direct_or_context("rhs", &["job", "k"], "v"),
        &or_modifier("lhs or on(k) rhs"),
    )
    .await;
    assert!(raw.schema().iter().any(|(qualifier, field)| {
        qualifier.as_ref().is_some_and(|q| q.to_string() == "lhs") && field.name() == "v"
    }));
    let nested = LogicalPlanBuilder::from(raw)
        .project(vec![
            DfExpr::BinaryExpr(BinaryExpr {
                left: Box::new(DfExpr::Column(Column::new(
                    Some(TableReference::bare("lhs")),
                    "v",
                ))),
                op: Operator::Plus,
                right: Box::new(lit(1.0)),
            })
            .alias("v_plus"),
        ])
        .unwrap()
        .build()
        .unwrap();
    let (_, batches) = execute(nested, &build_query_engine_state()).await;
    assert_eq!(values(&batches, "v_plus"), vec![2.0]);
}

#[tokio::test]
async fn test_direct_or_skips_user_internal_key_name() {
    const USER_TAG: &str = "__promql_or_match_0";
    let left = tagged_source(
        "lhs",
        false,
        (USER_TAG, Some("left")),
        DirectOrValue::Float64(1.0),
    );
    let right = tagged_source(
        "rhs",
        false,
        (USER_TAG, Some("right")),
        DirectOrValue::Float64(2.0),
    );
    let raw = plan_direct_or(
        scan(&left),
        scan(&right),
        direct_or_context("lhs", &["job", USER_TAG], "v"),
        direct_or_context("rhs", &["job", USER_TAG], "v"),
        &or_modifier("lhs or on(missing_label) rhs"),
    )
    .await;
    // The only column named like an internal match key is the user's own tag column, and the
    // plan must not reuse that occupied name for a generated key.
    let user_tag_columns = |schema: &DFSchema| {
        schema
            .fields()
            .iter()
            .filter(|field| field.name().starts_with("__promql_or_match_"))
            .map(|field| field.name().clone())
            .collect::<Vec<_>>()
    };
    assert_eq!(
        user_tag_columns(raw.schema()),
        vec![USER_TAG.to_string()],
        "{}",
        raw.display_indent_schema()
    );
    let (optimized, batches) = execute(raw, &build_query_engine_state()).await;
    assert_eq!(
        user_tag_columns(optimized.schema()),
        vec![USER_TAG.to_string()],
        "{optimized:?}"
    );
    // `on(missing_label)` names no label either operand carries, so the matching compares the
    // timestamp alone; the right row is the left row's match and the `or` returns the left row,
    // its sample and its own `__promql_or_match_0` tag value.
    assert_eq!(
        batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        1,
        "{batches:?}"
    );
    assert_eq!(values(&batches, "v"), vec![1.0]);
    for batch in &batches {
        let tag = batch
            .column_by_name(USER_TAG)
            .unwrap_or_else(|| panic!("the user column must be kept: {}", batch.schema()))
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            (0..batch.num_rows())
                .map(|row| tag.value(row).to_string())
                .collect::<Vec<_>>(),
            vec!["left".to_string()],
            "the result must be the left row"
        );
    }
}

#[tokio::test]
async fn test_direct_or_substrait_round_trip_with_normalized_key() {
    let state = build_query_engine_state();
    let ctx = SessionContext::new_with_state(state.session_state());
    let catalog = Arc::new(MemoryCatalogProvider::new());
    catalog
        .register_schema("public", Arc::new(MemorySchemaProvider::new()))
        .unwrap();
    ctx.register_catalog("datafusion", catalog);
    let left = matrix_source("lhs", Some(Some("")), 1, 1.0);
    let right = matrix_source("rhs", None, 1, 2.0);
    ctx.register_table(
        TableReference::full("datafusion", "public", "lhs"),
        table(&left),
    )
    .unwrap();
    ctx.register_table(
        TableReference::full("datafusion", "public", "rhs"),
        table(&right),
    )
    .unwrap();
    let raw = plan_direct_or(
        ctx.table("datafusion.public.lhs")
            .await
            .unwrap()
            .into_unoptimized_plan(),
        ctx.table("datafusion.public.rhs")
            .await
            .unwrap()
            .into_unoptimized_plan(),
        direct_or_context("lhs", &["job", "k"], "v"),
        direct_or_context("rhs", &["job"], "v"),
        &or_modifier("lhs or on(k) rhs"),
    )
    .await;
    let decoded = DFLogicalSubstraitConvertor
        .decode(
            DFLogicalSubstraitConvertor
                .encode(&raw, DefaultSerializer)
                .unwrap(),
            ctx.state(),
        )
        .await
        .unwrap();
    let (optimized, batches) = execute(decoded, &state).await;
    assert_no_internal_or_keys(optimized.schema());
    assert!(batches.iter().all(|batch| {
        batch
            .schema()
            .fields()
            .iter()
            .all(|field| !field.name().starts_with("__promql_or_match_"))
    }));
    assert_eq!(values(&batches, "v"), vec![1.0]);
}

#[tokio::test]
async fn test_direct_or_numeric_value_types() {
    let left = tagged_source("lhs", true, ("k", Some("lhs")), DirectOrValue::Int64(0));
    let right = tagged_source(
        "rhs",
        false,
        ("k", Some("rhs")),
        DirectOrValue::Float64(0.5),
    );
    let (optimized, batches) = run(
        &left,
        &right,
        direct_or_context("lhs", &["job", "k"], "v"),
        direct_or_context("rhs", &["job", "k"], "v"),
        &or_modifier("lhs or on(k) rhs"),
    )
    .await;
    assert_eq!(
        optimized
            .schema()
            .field_with_name(None, "v")
            .unwrap()
            .data_type(),
        &ArrowDataType::Float64
    );
    assert_eq!(values(&batches, "v"), vec![0.5]);
    let provider = build_test_table_provider_with_fields(
        &[(DEFAULT_SCHEMA_NAME.to_string(), "dummy".to_string())],
        &[],
    )
    .await;
    let mut planner = PromPlanner {
        table_provider: provider,
        ctx: PromPlannerContext::default(),
        promql_annotations: None,
    };
    let left_context = direct_or_context("lhs", &["job"], "v");
    let right_context = direct_or_context("rhs", &["job"], "v");
    let error = planner
        .or_operator(
            scan(&job_source("lhs", DirectOrValue::Utf8("x"))),
            scan(&job_source("rhs", DirectOrValue::Float64(1.0))),
            left_context.tag_columns.iter().cloned().collect(),
            right_context.tag_columns.iter().cloned().collect(),
            left_context,
            right_context,
            &or_modifier("lhs or on() rhs"),
        )
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("OR value fields have incompatible types")
    );
}

#[tokio::test]
async fn test_or_with_histogram_quantile_missing_le_column() {
    let case = r#"histogram_quantile(0.99, non_existent_histogram_bucket) or normal_metric"#;
    let eval_stmt = build_eval_stmt(case);
    let table_provider = build_missing_le_or_normal_metric_table_provider().await;

    let plan = PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
        .await
        .unwrap();
    assert_normal_metric_schema(&plan);
}

#[tokio::test]
async fn test_or_with_right_empty_histogram_restores_left_context() {
    let eval_stmt = build_eval_stmt(
        r#"abs(sum by(instance) (normal_metric) or histogram_quantile(0.99, sum by(pod) (non_existent_histogram_bucket)))"#,
    );
    let table_provider = build_missing_le_or_normal_metric_table_provider().await;

    PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
        .await
        .unwrap();
}

#[tokio::test]
async fn test_or_with_both_empty_histograms() {
    let eval_stmt = build_eval_stmt(
        r#"histogram_quantile(0.99, sum by(pod) (left_histogram_bucket)) or histogram_quantile(0.99, sum by(instance) (right_histogram_bucket))"#,
    );
    let table_provider = build_test_table_provider_with_fields(
        &[
            (
                DEFAULT_SCHEMA_NAME.to_string(),
                "left_histogram_bucket".to_string(),
            ),
            (
                DEFAULT_SCHEMA_NAME.to_string(),
                "right_histogram_bucket".to_string(),
            ),
        ],
        &["pod", "instance"],
    )
    .await;

    let plan = PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
        .await
        .unwrap();
    match plan {
        LogicalPlan::EmptyRelation(relation) => {
            assert!(!relation.produce_one_row);
            assert!(!relation.schema.fields().is_empty());
            assert!(
                relation
                    .schema
                    .fields()
                    .iter()
                    .any(|field| field.data_type() == &ArrowDataType::Float64)
            );
            assert!(
                relation
                    .schema
                    .fields()
                    .iter()
                    .any(|field| field.name() == "pod")
            );
            assert!(
                !relation
                    .schema
                    .fields()
                    .iter()
                    .any(|field| field.name() == "instance")
            );
        }
        _ => panic!("Expected EmptyRelation, but got: {plan:?}"),
    }
}

#[tokio::test]
async fn test_nested_or_with_both_empty_histograms() {
    for case in [
        r#"abs(histogram_quantile(0.99, left_histogram_bucket) or histogram_quantile(0.99, right_histogram_bucket))"#,
        r#"(histogram_quantile(0.99, left_histogram_bucket) or histogram_quantile(0.99, right_histogram_bucket)) + 1"#,
    ] {
        let eval_stmt = build_eval_stmt(case);
        let table_provider = build_test_table_provider_with_fields(
            &[
                (
                    DEFAULT_SCHEMA_NAME.to_string(),
                    "left_histogram_bucket".to_string(),
                ),
                (
                    DEFAULT_SCHEMA_NAME.to_string(),
                    "right_histogram_bucket".to_string(),
                ),
            ],
            &["pod", "instance"],
        )
        .await;

        PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn test_or_with_empty_histogram_modifiers() {
    for case in [
        r#"histogram_quantile(0.99, non_existent_histogram_bucket) or on(pod) normal_metric"#,
        r#"normal_metric or ignoring(instance) histogram_quantile(0.99, non_existent_histogram_bucket)"#,
    ] {
        let eval_stmt = build_eval_stmt(case);
        let table_provider = build_missing_le_or_normal_metric_table_provider().await;

        let plan =
            PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
                .await
                .unwrap();
        assert_normal_metric_schema(&plan);
    }
}

#[tokio::test]
async fn test_unless_preserves_left_context_for_histogram() {
    let eval_stmt = build_eval_stmt(
        r#"histogram_quantile(0.99, bucket_metric unless on(job) normal_metric) or fallback_metric"#,
    );
    let state = build_query_engine_state();
    let plan = PromPlanner::stmt_to_plan(
        build_set_op_context_table_provider().await,
        &eval_stmt,
        &state,
    )
    .await
    .unwrap();
    assert!(contains_histogram_fold(&plan), "{plan:?}");
    let (optimized, physical) = optimize_and_create_physical_plan(&state, plan).await;
    assert!(contains_histogram_fold(&optimized), "{optimized:?}");
    let batches = datafusion::physical_plan::collect(physical, state.session_state().task_ctx())
        .await
        .unwrap();
    assert!(batches.iter().all(|batch| batch.num_rows() == 0));
}

#[tokio::test]
async fn test_and_preserves_left_context_for_histogram() {
    let eval_stmt = build_eval_stmt(
        r#"histogram_quantile(0.99, bucket_metric and on(job) normal_metric) or fallback_metric"#,
    );
    let plan = PromPlanner::stmt_to_plan(
        build_set_op_context_table_provider().await,
        &eval_stmt,
        &build_query_engine_state(),
    )
    .await
    .unwrap();
    assert!(contains_histogram_fold(&plan), "{plan:?}");
}

#[tokio::test]
async fn test_and_preserves_left_context_when_le_is_missing() {
    let eval_stmt =
        build_eval_stmt(r#"histogram_quantile(0.99, normal_metric and on(job) bucket_metric)"#);
    let plan = PromPlanner::stmt_to_plan(
        build_set_op_context_table_provider().await,
        &eval_stmt,
        &build_query_engine_state(),
    )
    .await
    .unwrap();
    assert!(
        PromPlanner::metric_name_column(plan.schema())
            .unwrap()
            .is_none()
    );
    assert!(!plan.schema().fields().is_empty());
    assert!(!contains_histogram_fold(&plan), "{plan:?}");
    // The result must stay empty, not turn into a non-empty plan that just lost its
    // identity: the plan bottoms out in an `EmptyRelation` (possibly below a projection
    // that only reshapes the schema), so no sample can be emitted.
    let mut plans = vec![&plan];
    let mut is_empty = false;
    while let Some(next) = plans.pop() {
        is_empty |= matches!(next, LogicalPlan::EmptyRelation(_));
        plans.extend(next.inputs());
    }
    assert!(is_empty, "the missing `le` case must stay empty: {plan:?}");
}

async fn build_matching_filter_plan(query: &str) -> String {
    let table_provider = build_test_table_provider_with_distinct_tags(&[
        ("metric_a", &["host", "device"]),
        ("metric_b", &["host", "device"]),
    ])
    .await;
    let plan = PromPlanner::stmt_to_plan(
        table_provider,
        &build_eval_stmt(query),
        &build_query_engine_state(),
    )
    .await
    .unwrap();
    plan.display_indent().to_string()
}

/// [`build_test_table_provider_with_distinct_tags`] plus a `status` string column: a value
/// field that is neither a primary key nor a value column of the metric, so
/// `count by(status) (...)` still reports it among the aggregation's tag columns.
async fn build_test_table_provider_with_string_field(
    table_tags: &[(&str, &[&str])],
) -> DfTableSourceProvider {
    let catalog_list = MemoryCatalogManager::with_default_setup();
    for (table_name, tags) in table_tags {
        let mut columns = tags
            .iter()
            .map(|tag| {
                ColumnSchema::new(
                    (*tag).to_string(),
                    ConcreteDataType::string_datatype(),
                    false,
                )
            })
            .collect::<Vec<_>>();
        columns.push(
            ColumnSchema::new(
                greptime_timestamp().to_string(),
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
        );
        columns.push(ColumnSchema::new(
            greptime_value().to_string(),
            ConcreteDataType::float64_datatype(),
            true,
        ));
        columns.push(ColumnSchema::new(
            "status".to_string(),
            ConcreteDataType::string_datatype(),
            true,
        ));
        let table_meta = TableMetaBuilder::empty()
            .schema(Arc::new(Schema::new(columns)))
            .primary_key_indices((0..tags.len()).collect())
            .next_column_id(1024)
            .build()
            .unwrap();
        let table_info = TableInfoBuilder::default()
            .name((*table_name).to_string())
            .meta(table_meta)
            .build()
            .unwrap();

        assert!(
            catalog_list
                .register_table_sync(RegisterTableRequest {
                    catalog: DEFAULT_CATALOG_NAME.to_string(),
                    schema: DEFAULT_SCHEMA_NAME.to_string(),
                    table_name: (*table_name).to_string(),
                    table_id: 1024,
                    table: EmptyTable::from_table_info(&table_info),
                })
                .is_ok()
        );
    }

    DfTableSourceProvider::new(
        catalog_list,
        false,
        QueryContext::arc(),
        DummyDecoder::arc(),
        false,
    )
}

async fn build_matching_filter_plan_with_string_field(query: &str) -> String {
    let table_provider = build_test_table_provider_with_string_field(&[
        ("metric_a", &["host", "device"]),
        ("metric_b", &["host", "device"]),
    ])
    .await;
    let plan = PromPlanner::stmt_to_plan(
        table_provider,
        &build_eval_stmt(query),
        &build_query_engine_state(),
    )
    .await
    .unwrap();
    plan.display_indent().to_string()
}

#[tokio::test]
async fn binary_matching_label_filter_reaches_both_operands() {
    for query in [
        r#"metric_a / metric_b{host="foo"}"#,
        r#"metric_a / on(host, device) metric_b{host="foo"}"#,
        r#"count_over_time(metric_a[1m]) / on(host) count_over_time(metric_b{host="foo"}[1m])"#,
        r#"metric_a / ignoring(device) metric_b{host="foo"}"#,
        r#"sum by(host) (metric_a) / on(host) sum by(host) (metric_b{host="foo"})"#,
        r#"metric_a / on(host) avg without(device) (metric_b{host="foo"})"#,
    ] {
        let plan = build_matching_filter_plan(query).await;
        assert_eq!(
            plan.matches(r#"host = Utf8("foo")"#).count(),
            2,
            "{query}\n{plan}"
        );
    }
}

#[tokio::test]
async fn binary_matching_label_filter_reaches_scalar_ranking_and_grouped_operands() {
    for query in [
        r#"(8 * metric_a{host="foo"}) / on(host) metric_b"#,
        r#"topk(1, metric_a{host="foo"}) / on(host, device) metric_b"#,
        r#"(8 * metric_a{host="foo"}) / on(host) group_left topk by(host)(1, max by(host)(metric_b))"#,
    ] {
        let plan = build_matching_filter_plan(query).await;
        assert_eq!(
            plan.matches(r#"host = Utf8("foo")"#).count(),
            2,
            "{query}\n{plan}"
        );
    }
    // A global ranking one-side must see every host, so the matcher stays put.
    let query = r#"metric_a{host="foo"} / on(host) group_left topk(1, max by(host)(metric_b))"#;
    let plan = build_matching_filter_plan(query).await;
    assert_eq!(
        plan.matches(r#"host = Utf8("foo")"#).count(),
        1,
        "{query}\n{plan}"
    );
}

#[tokio::test]
async fn binary_matching_label_filter_skips_selecting_aggregations() {
    // `topk` ranks its input, so filtering before it changes the candidate set.
    let query = r#"topk(1, metric_a) / on(host, device) metric_b{host="foo"}"#;
    let plan = build_matching_filter_plan(query).await;
    assert_eq!(plan.matches("foo").count(), 1, "{query}\n{plan}");
}

#[tokio::test]
async fn binary_value_field_matcher_stays_on_its_own_operand() {
    let value = greptime_value();
    for query in [
        format!(r#"metric_a / metric_b{{{value}="2"}}"#),
        format!(r#"metric_a / on(host, device, {value}) metric_b{{{value}="2"}}"#),
    ] {
        let plan = build_matching_filter_plan(&query).await;
        assert_eq!(plan.matches(r#"Utf8("2")"#).count(), 1, "{query}\n{plan}");
    }
}

#[tokio::test]
async fn binary_value_field_matcher_is_not_copied_across_aggregations() {
    // `status` varies between the samples of one series, so filtering the other operand by it
    // would drop the newest sample before sample selection (#9242).
    for query in [
        r#"count by(status) (metric_a) / on(status) count by(status) (metric_b{status="ready"})"#,
        r#"(8 * count by(status)(metric_a{__field__="status"})) / on(status) topk by(status)(1, count by(status)(metric_b{__field__="status",status="ready"}))"#,
        r#"count by(status)(metric_a{__field__="status"}) / on(status) group_left topk by(status)(1, count by(status)(metric_b{__field__="status",status="ready"}))"#,
    ] {
        let plan = build_matching_filter_plan_with_string_field(query).await;
        assert_eq!(
            plan.matches(r#"Utf8("ready")"#).count(),
            1,
            "{query}\n{plan}"
        );
    }
}

#[tokio::test]
async fn binary_matching_label_filter_reaches_aggregations_grouping_by_tags() {
    // `count`, not `sum`: the string value field is not summable.
    let query = r#"count by(host) (metric_a) / on(host) count by(host) (metric_b{host="foo"})"#;
    let plan = build_matching_filter_plan_with_string_field(query).await;
    assert_eq!(plan.matches(r#"Utf8("foo")"#).count(), 2, "{query}\n{plan}");
}

#[tokio::test]
async fn binary_matching_label_filter_skips_unproven_expressions() {
    for query in [
        r#"metric_a > on(host, device) metric_b{host="foo"}"#,
        r#"metric_a / on(host) group_left metric_b{host="foo"}"#,
        r#"metric_a / on(host) label_replace(metric_b{host="foo"},"extra","e","host",".*")"#,
        r#"metric_a / on(device) metric_b{host="foo"}"#,
    ] {
        let plan = build_matching_filter_plan(query).await;
        assert_eq!(plan.matches("foo").count(), 1, "{query}\n{plan}");
    }
}

#[tokio::test]
async fn test_or_context_uses_left_qualified_output() {
    let case = r#"(normal_metric or other_metric) + 1"#;
    let eval_stmt = build_eval_stmt(case);
    let state = build_query_engine_state();
    let plan =
        PromPlanner::stmt_to_plan(build_or_context_table_provider().await, &eval_stmt, &state)
            .await
            .unwrap();
    assert!(
        plan.schema()
            .fields()
            .iter()
            .any(|field| field.data_type() == &ArrowDataType::Float64),
        "{plan:?}"
    );
    let (_optimized, _physical) = optimize_and_create_physical_plan(&state, plan).await;
}

#[tokio::test]
async fn test_or_context_uses_left_qualified_empty_histogram_output() {
    let case = r#"(abs(histogram_quantile(0.99, non_hist_metric)) or normal_metric) + 1"#;
    let eval_stmt = build_eval_stmt(case);
    let plan = PromPlanner::stmt_to_plan(
        build_or_context_table_provider().await,
        &eval_stmt,
        &build_query_engine_state(),
    )
    .await
    .unwrap();
    assert!(
        plan.schema()
            .fields()
            .iter()
            .any(|field| field.data_type() == &ArrowDataType::Float64),
        "{plan:?}"
    );
}

#[tokio::test]
async fn test_direct_or_preserves_float_and_native_histogram_samples() {
    for histogram_on_left in [false, true] {
        let (planner, plan) = mixed_direct_or(histogram_on_left).await;

        let float_field = &planner.ctx.field_columns[0];
        let histogram_field = &planner.ctx.field_columns[1];
        assert!(float_field.starts_with(OR_FLOAT_FIELD_PREFIX));
        assert!(histogram_field.starts_with(OR_HISTOGRAM_FIELD_PREFIX));
        assert_eq!(
            plan.schema()
                .field_with_name(None, float_field)
                .unwrap()
                .data_type(),
            &ArrowDataType::Float64
        );
        assert_eq!(
            plan.schema()
                .field_with_name(None, histogram_field)
                .unwrap()
                .data_type(),
            &native_histogram_value_type().as_arrow_type()
        );

        let (optimized, batches) = execute(plan, &build_query_engine_state()).await;
        assert_no_internal_or_keys(optimized.schema());
        let mut sample_kinds = batches
            .iter()
            .flat_map(|batch| {
                let values = batch.column_by_name(float_field).unwrap();
                let histograms = batch.column_by_name(histogram_field).unwrap();
                (0..batch.num_rows()).map(|row| (values.is_valid(row), histograms.is_valid(row)))
            })
            .collect::<Vec<_>>();
        sample_kinds.sort_unstable();
        assert_eq!(sample_kinds, vec![(false, true), (true, false)]);
    }
}

#[tokio::test]
async fn malformed_classic_bucket_does_not_drop_native_histogram() {
    let state = build_query_engine_state();
    let collector = PromqlAnnotationCollector::default();
    let plan = PromPlanner::stmt_to_plan_with_annotations(
        operator_table_provider(),
        &operator_eval_stmt("histogram_quantile(0.5, bad_classic or bad_native)"),
        &state,
        Some(collector.clone()),
    )
    .await
    .unwrap();
    let value_field = plan
        .schema()
        .fields()
        .iter()
        .find(|field| field.data_type() == &ArrowDataType::Float64)
        .unwrap()
        .name()
        .clone();

    let (_, batches) = execute(plan, &state).await;
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    assert_eq!(values(&batches, &value_field), vec![0.0]);
    let mut warnings = vec![];
    let mut infos = vec![];
    collector.append_to(&mut warnings, &mut infos);
    assert!(warnings.is_empty());
    assert!(infos.is_empty());
}

#[tokio::test]
async fn test_mixed_binary_operator_aligns_both_alternative_inputs() {
    let state = build_query_engine_state();
    let plan = PromPlanner::stmt_to_plan(
        operator_table_provider(),
        &operator_eval_stmt("(lf or on(tag) lh) * on(tag) (rf or on(tag) rh)"),
        &state,
    )
    .await
    .unwrap();
    let plan_text = plan.display_indent_schema().to_string();
    assert!(
        plan_text.contains("prom_native_histogram_mul_scalar"),
        "{plan_text}"
    );
    assert!(
        plan_text.contains("prom_native_histogram_scalar_mul"),
        "{plan_text}"
    );
    let float_field = plan
        .schema()
        .fields()
        .iter()
        .find(|field| field.name().starts_with(OR_FLOAT_FIELD_PREFIX))
        .unwrap()
        .name()
        .clone();
    let histogram_field = plan
        .schema()
        .fields()
        .iter()
        .find(|field| field.name().starts_with(OR_HISTOGRAM_FIELD_PREFIX))
        .unwrap()
        .name()
        .clone();

    let (_, batches) = execute(plan, &state).await;
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
    assert!(values(&batches, &float_field).is_empty());
    let mut sums = histograms(&batches, &histogram_field)
        .into_iter()
        .map(|histogram| histogram.sum)
        .collect::<Vec<_>>();
    sums.sort_by(f64::total_cmp);
    assert_eq!(sums, vec![2.0, 3.0]);
}

#[tokio::test]
async fn test_mixed_binary_operator_reports_only_dropped_samples() {
    for (query, expected_rows, expected_infos) in [
        ("(lf or on(tag) lh) + on(tag) (rf or on(tag) rh)", 0, 1),
        ("(lf or on(tag) lh) + on(tag) (lf or on(tag) lh)", 2, 0),
        ("(lf or on(tag) lh) % on(tag) lh", 0, 1),
    ] {
        let state = build_query_engine_state();
        let annotations = PromqlAnnotationCollector::default();
        let plan = PromPlanner::stmt_to_plan_with_annotations(
            operator_table_provider(),
            &operator_eval_stmt(query),
            &state,
            Some(annotations.clone()),
        )
        .await
        .unwrap();

        let (_, batches) = execute(plan, &state).await;
        assert_eq!(
            batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
            expected_rows,
            "{query}"
        );
        let mut warnings = vec![];
        let mut infos = vec![];
        annotations.append_to(&mut warnings, &mut infos);
        assert!(warnings.is_empty(), "{query}: {warnings:?}");
        assert_eq!(infos.len(), expected_infos, "{query}: {infos:?}");
    }
}

#[tokio::test]
async fn test_histogram_only_min_drops_empty_aggregate_group() {
    // `min` over native-histogram-only input drops every sample in the group, so the
    // NULL-valued aggregate row must be filtered out. Otherwise an outer expression
    // like `group()` resurrects the group Prometheus considers unseen.
    let state = build_query_engine_state();
    for query in ["min(lh)", "group(min(lh))"] {
        let plan = PromPlanner::stmt_to_plan(
            operator_table_provider(),
            &operator_eval_stmt(query),
            &state,
        )
        .await
        .unwrap();
        let (_, batches) = execute(plan, &state).await;
        assert_eq!(
            batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
            0,
            "{query}"
        );
    }
}

#[tokio::test]
async fn test_mixed_min_drops_histogram_only_group() {
    // With alternative float/histogram fields, `min by (tag)` keeps float-only groups
    // (tag=a from `lf`) and drops histogram-only groups (tag=b from `lh`) instead of
    // emitting a NULL-valued row for them.
    let state = build_query_engine_state();
    let plan = PromPlanner::stmt_to_plan(
        operator_table_provider(),
        &operator_eval_stmt("min by (tag) (lf or on(tag) lh)"),
        &state,
    )
    .await
    .unwrap();
    let float_field = plan
        .schema()
        .fields()
        .iter()
        .find(|field| field.data_type() == &ArrowDataType::Float64)
        .unwrap()
        .name()
        .clone();
    let (_, batches) = execute(plan, &state).await;
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    assert_eq!(values(&batches, &float_field), vec![2.0]);
}

#[tokio::test]
async fn test_mixed_or_can_feed_another_or() {
    let state = build_query_engine_state();
    let plan = PromPlanner::stmt_to_plan(
        operator_table_provider(),
        &operator_eval_stmt("lf or on(tag) lh or on(tag) fallback"),
        &state,
    )
    .await
    .unwrap();
    let float_field = plan
        .schema()
        .fields()
        .iter()
        .find(|field| field.name().starts_with(OR_FLOAT_FIELD_PREFIX))
        .unwrap()
        .name()
        .clone();
    let histogram_field = plan
        .schema()
        .fields()
        .iter()
        .find(|field| field.name().starts_with(OR_HISTOGRAM_FIELD_PREFIX))
        .unwrap()
        .name()
        .clone();

    let (_, batches) = execute(plan, &state).await;
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
    let mut float_values = values(&batches, &float_field);
    float_values.sort_by(f64::total_cmp);
    assert_eq!(float_values, vec![2.0, 7.0]);
    assert_eq!(histograms(&batches, &histogram_field).len(), 1);
}

#[tokio::test]
async fn test_mixed_fields_align_with_single_float_vector() {
    let (planner, mixed) = mixed_direct_or(false).await;
    let scale = tagged_source(
        "scale",
        false,
        ("k", Some("float")),
        DirectOrValue::Float64(2.0),
    );
    let scale = scan(&scale);
    let scale_fields = vec!["v".to_string()];
    let PromExpr::Binary(binary) = parser::parse("lhs * rhs").unwrap() else {
        unreachable!()
    };

    let (groups, invalid_pairs) = PromPlanner::align_binary_field_columns(
        mixed.schema(),
        scale.schema(),
        &planner.ctx.field_columns,
        &scale_fields,
        binary.op,
        false,
        false,
    );
    assert!(invalid_pairs.is_empty());
    assert_eq!(
        groups
            .iter()
            .map(|(output, _)| output.clone())
            .collect::<Vec<_>>(),
        planner.ctx.field_columns
    );
    assert_eq!(groups.len(), 2);
    assert!(
        groups
            .iter()
            .flat_map(|(_, pairs)| pairs)
            .all(|(_, right)| *right == &scale_fields[0])
    );

    let (groups, invalid_pairs) = PromPlanner::align_binary_field_columns(
        scale.schema(),
        mixed.schema(),
        &scale_fields,
        &planner.ctx.field_columns,
        binary.op,
        false,
        false,
    );
    assert!(invalid_pairs.is_empty());
    assert_eq!(
        groups
            .iter()
            .map(|(output, _)| output.clone())
            .collect::<Vec<_>>(),
        planner.ctx.field_columns
    );
    assert_eq!(groups.len(), 2);
    assert!(
        groups
            .iter()
            .flat_map(|(_, pairs)| pairs)
            .all(|(left, _)| *left == &scale_fields[0])
    );
}

#[tokio::test]
async fn test_non_bool_comparison_filters_mixed_sample_lanes() {
    let (planner, input) = mixed_direct_or(false).await;
    let input_schema = input.schema().clone();
    let plan = planner
        .filter_on_field_column(input, |field| {
            if PromPlanner::field_column_is_native_histogram(&input_schema, field) {
                Ok(lit(false))
            } else {
                Ok(col(field).gt(lit(0.0)))
            }
        })
        .unwrap();
    let float_field = planner.ctx.field_columns[0].clone();

    let (_, batches) = execute(plan, &build_query_engine_state()).await;
    assert_eq!(values(&batches, &float_field), vec![1.25]);
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
}

#[tokio::test]
async fn test_mixed_left_and_unless_preserve_sample_lanes() {
    for (expression, expected_sample_kind) in [
        ("lhs and on(k) mask", (false, true)),
        ("lhs unless on(k) mask", (true, false)),
    ] {
        let (mut planner, left) = mixed_direct_or(false).await;
        let left_context = planner.ctx.clone();
        let float_field = left_context.field_columns[0].clone();
        let histogram_field = left_context.field_columns[1].clone();
        let mask = tagged_source(
            "mask",
            false,
            ("k", Some("histogram")),
            DirectOrValue::Float64(1.0),
        );
        let PromExpr::Binary(binary) = parser::parse(expression).unwrap() else {
            unreachable!()
        };
        let plan = planner
            .set_op_on_non_field_columns(
                left,
                scan(&mask),
                left_context,
                direct_or_context("mask", &["job", "k"], "v"),
                binary.op,
                &binary.modifier,
            )
            .unwrap();

        let (_, batches) = execute(plan, &build_query_engine_state()).await;
        let sample_kinds = batches
            .iter()
            .flat_map(|batch| {
                let floats = batch.column_by_name(&float_field).unwrap();
                let histograms = batch.column_by_name(&histogram_field).unwrap();
                (0..batch.num_rows()).map(|row| (floats.is_valid(row), histograms.is_valid(row)))
            })
            .collect::<Vec<_>>();
        assert_eq!(sample_kinds, vec![expected_sample_kind], "{expression}");
    }
}

#[tokio::test]
async fn test_mixed_fields_arithmetic_broadcasts_computed_scalar() {
    let plan = PromPlanner::stmt_to_plan(
        build_test_mixed_native_histogram_table_provider("some_metric").await,
        &build_eval_stmt("some_metric * scalar(vector(2))"),
        &build_query_engine_state(),
    )
    .await
    .unwrap();
    let schema = plan.schema();
    assert_eq!(
        schema
            .field_with_unqualified_name(greptime_value())
            .unwrap()
            .data_type(),
        &ArrowDataType::Float64
    );
    assert_eq!(
        schema
            .field_with_unqualified_name(greptime_native_histogram())
            .unwrap()
            .data_type(),
        &native_histogram_value_type().as_arrow_type()
    );
    assert!(
        plan.display_indent_schema()
            .to_string()
            .contains("prom_native_histogram_mul_scalar"),
        "{plan:?}"
    );
}

#[tokio::test]
async fn test_unsupported_histogram_binary_does_not_block_or_fallback() {
    let state = build_query_engine_state();
    let plan = PromPlanner::stmt_to_plan(
        operator_table_provider(),
        &operator_eval_stmt("((lf or on(tag) lh) % 2) or on(tag) lh"),
        &state,
    )
    .await
    .unwrap();
    let float_field = plan
        .schema()
        .fields()
        .iter()
        .find(|field| field.data_type() == &ArrowDataType::Float64)
        .unwrap()
        .name()
        .clone();
    let histogram_field = plan
        .schema()
        .fields()
        .iter()
        .find(|field| field.data_type() == &native_histogram_value_type().as_arrow_type())
        .unwrap()
        .name()
        .clone();

    let (_, batches) = execute(plan, &state).await;
    assert_eq!(values(&batches, &float_field), vec![0.0]);
    assert_eq!(histograms(&batches, &histogram_field).len(), 1);
}

#[tokio::test]
async fn test_unary_negates_mixed_float_and_native_histogram_samples() {
    for histogram_on_left in [false, true] {
        let (mut planner, input) = mixed_direct_or(histogram_on_left).await;
        let plan = planner.negate_field_columns(input).unwrap();
        assert!(PromPlanner::field_columns_are_alternative_samples(
            plan.schema(),
            &planner.ctx.field_columns
        ));
        let float_field = planner
            .ctx
            .field_columns
            .iter()
            .find(|field| field.starts_with(OR_FLOAT_FIELD_PREFIX))
            .unwrap();
        let histogram_field = planner
            .ctx
            .field_columns
            .iter()
            .find(|field| field.starts_with(OR_HISTOGRAM_FIELD_PREFIX))
            .unwrap();

        let (_, batches) = execute(plan, &build_query_engine_state()).await;
        assert_eq!(values(&batches, float_field), vec![-1.25]);
        let histogram = batches
            .iter()
            .find_map(|batch| {
                let values = batch
                    .column_by_name(histogram_field)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::StructArray>()
                    .unwrap();
                (0..values.len()).find_map(|row| {
                    common_query::native_histogram::read_histogram(values, row).unwrap()
                })
            })
            .unwrap();
        assert_eq!(histogram.count, -1.0);
        assert_eq!(histogram.sum, -1.0);
        assert_eq!(histogram.reset_hint, CounterResetHint::Gauge);
    }
}

#[tokio::test]
async fn test_native_histogram_sum_and_avg_execute_real_batches() {
    for op_name in ["sum", "avg"] {
        for incompatible in [false, true] {
            let mut second = direct_or_histogram();
            if incompatible {
                second.schema = CUSTOM_BUCKETS_SCHEMA;
                second.custom_values = vec![1.0];
            }
            let collector = PromqlAnnotationCollector::default();
            let (mut planner, input) =
                mixed_aggregate_input(vec![direct_or_histogram(), second]).await;
            planner.promql_annotations = Some(collector.clone());
            let histogram_column = planner.ctx.field_columns[1].clone();
            planner.ctx.field_columns = vec![histogram_column.clone()];
            let input = LogicalPlanBuilder::from(input)
                .project([col("ts"), col(&histogram_column)])
                .unwrap()
                .build()
                .unwrap();
            let PromExpr::Aggregate(AggregateExpr { op, param, .. }) =
                parser::parse(&format!("{op_name}(mixed)")).unwrap()
            else {
                unreachable!()
            };
            let (aggregate_exprs, _) = planner.create_aggregate_exprs(op, &param, &input).unwrap();
            let plan = LogicalPlanBuilder::from(input)
                .aggregate(vec![col("ts")], aggregate_exprs)
                .unwrap()
                .filter(planner.create_empty_values_filter_expr(false).unwrap())
                .unwrap()
                .build()
                .unwrap();

            let (_, batches) = execute(plan, &build_query_engine_state()).await;
            let mut warnings = vec![];
            let mut infos = vec![];
            collector.append_to(&mut warnings, &mut infos);
            assert!(infos.is_empty());
            if incompatible {
                assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
                assert!(warnings.iter().any(|warning| {
                    warning
                        == &format!(
                            "prom_native_histogram_agg_{op_name}: dropped native histogram aggregate with incompatible schemas"
                        )
                }));
            } else {
                let histograms = histograms(&batches, &histogram_column);
                assert_eq!(histograms.len(), 1);
                let expected = if op_name == "sum" { 2.0 } else { 1.0 };
                assert_eq!(histograms[0].count, expected);
                assert_eq!(histograms[0].sum, expected);
                assert!(warnings.is_empty());
            }
        }
    }
}

#[tokio::test]
async fn test_canonical_mixed_count_group_and_count_values_execute() {
    let state = build_query_engine_state();
    for (query, expected) in [
        ("count(some_metric)", vec![2.0]),
        ("group(some_metric)", vec![1.0]),
        (r#"count_values("sample", some_metric)"#, vec![1.0, 1.0]),
    ] {
        let plan = PromPlanner::stmt_to_plan(
            build_test_mixed_native_histogram_table_provider("some_metric").await,
            &operator_eval_stmt(query),
            &state,
        )
        .await
        .unwrap();
        assert!(
            plan.schema()
                .fields()
                .iter()
                .all(|field| !field.name().starts_with("__promql_sample_count")),
            "{query}: {plan:?}"
        );
        let value_fields = plan
            .schema()
            .fields()
            .iter()
            .filter(|field| {
                matches!(
                    field.data_type(),
                    ArrowDataType::Float64 | ArrowDataType::Int64 | ArrowDataType::UInt64
                ) || field.data_type() == &native_histogram_value_type().as_arrow_type()
            })
            .collect::<Vec<_>>();
        assert_eq!(value_fields.len(), 1, "{query}: {plan:?}");
        assert_ne!(
            value_fields[0].data_type(),
            &native_histogram_value_type().as_arrow_type(),
            "{query}: {plan:?}"
        );
        let value_column = value_fields[0].name().clone();

        let (_, batches) = execute(plan, &state).await;
        let mut actual = numeric_values(&batches, &value_column);
        actual.sort_by(f64::total_cmp);
        assert_eq!(actual, expected, "{query}");

        if query.starts_with("count_values") {
            let mut sample_labels = batches
                .iter()
                .flat_map(|batch| {
                    batch
                        .column_by_name("sample")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap()
                        .iter()
                        .flatten()
                        .map(str::to_string)
                })
                .collect::<Vec<_>>();
            sample_labels.sort();
            let mut expected_labels = vec!["2".to_string(), direct_or_histogram().promql_string()];
            expected_labels.sort();
            assert_eq!(sample_labels, expected_labels);
        }
    }
}

#[tokio::test]
async fn test_mixed_or_sum_aggregates_each_sample_type() {
    let PromExpr::Aggregate(AggregateExpr { op, param, .. }) = parser::parse("sum(lhs)").unwrap()
    else {
        unreachable!()
    };

    let collector = PromqlAnnotationCollector::default();
    let (mut planner, input) = mixed_direct_or(false).await;
    planner.promql_annotations = Some(collector.clone());
    let float_column = planner.ctx.field_columns[0].clone();
    let histogram_column = planner.ctx.field_columns[1].clone();
    let (aggregate_exprs, _) = planner.create_aggregate_exprs(op, &param, &input).unwrap();
    let plan = LogicalPlanBuilder::from(input)
        .aggregate(vec![col("ts"), col("k")], aggregate_exprs)
        .unwrap()
        .filter(
            planner
                .mixed_aggregate_filter_expr(op, &float_column, &histogram_column)
                .unwrap(),
        )
        .unwrap()
        .project([
            col(&float_column),
            col(&histogram_column),
            col("ts"),
            col("k"),
        ])
        .unwrap()
        .build()
        .unwrap();

    let (_, batches) = execute(plan, &build_query_engine_state()).await;
    assert_eq!(values(&batches, &float_column), vec![1.25]);
    let histogram = batches
        .iter()
        .find_map(|batch| {
            let values = batch
                .column_by_name(&histogram_column)?
                .as_any()
                .downcast_ref::<datafusion::arrow::array::StructArray>()?;
            (0..values.len()).find_map(|row| {
                common_query::native_histogram::read_histogram(values, row).unwrap()
            })
        })
        .unwrap();
    assert_eq!(histogram.count, 1.0);
    let mut warnings = vec![];
    let mut infos = vec![];
    collector.append_to(&mut warnings, &mut infos);
    assert!(warnings.is_empty());

    let collector = PromqlAnnotationCollector::default();
    let (mut planner, input) = mixed_direct_or(false).await;
    planner.promql_annotations = Some(collector.clone());
    let float_column = planner.ctx.field_columns[0].clone();
    let histogram_column = planner.ctx.field_columns[1].clone();
    let (aggregate_exprs, _) = planner.create_aggregate_exprs(op, &param, &input).unwrap();
    let plan = LogicalPlanBuilder::from(input)
        .aggregate(vec![col("ts")], aggregate_exprs)
        .unwrap()
        .filter(
            planner
                .mixed_aggregate_filter_expr(op, &float_column, &histogram_column)
                .unwrap(),
        )
        .unwrap()
        .project([col(&float_column), col(&histogram_column), col("ts")])
        .unwrap()
        .build()
        .unwrap();

    let (_, batches) = execute(plan, &build_query_engine_state()).await;
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
    let mut warnings = vec![];
    let mut infos = vec![];
    collector.append_to(&mut warnings, &mut infos);
    assert_eq!(
        warnings,
        vec!["sum: dropped aggregation result containing both float and native histogram samples"]
    );
}

#[tokio::test]
async fn test_mixed_or_sum_drops_incompatible_mixed_group() {
    let PromExpr::Aggregate(AggregateExpr { op, param, .. }) = parser::parse("sum(lhs)").unwrap()
    else {
        unreachable!()
    };
    let mut custom = direct_or_histogram();
    custom.schema = CUSTOM_BUCKETS_SCHEMA;
    custom.custom_values = vec![1.0];
    let collector = PromqlAnnotationCollector::default();
    let (mut planner, input) = mixed_aggregate_input(vec![direct_or_histogram(), custom]).await;
    planner.promql_annotations = Some(collector.clone());
    let float_column = planner.ctx.field_columns[0].clone();
    let histogram_column = planner.ctx.field_columns[1].clone();
    let (aggregate_exprs, _) = planner.create_aggregate_exprs(op, &param, &input).unwrap();
    let plan = LogicalPlanBuilder::from(input)
        .aggregate(vec![col("ts")], aggregate_exprs)
        .unwrap()
        .filter(
            planner
                .mixed_aggregate_filter_expr(op, &float_column, &histogram_column)
                .unwrap(),
        )
        .unwrap()
        .project([col(&float_column), col(&histogram_column), col("ts")])
        .unwrap()
        .build()
        .unwrap();

    let (_, batches) = execute(plan, &build_query_engine_state()).await;
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
    let mut warnings = vec![];
    let mut infos = vec![];
    collector.append_to(&mut warnings, &mut infos);
    assert!(warnings.iter().any(|warning| {
        warning
            == "sum: dropped aggregation result containing both float and native histogram samples"
    }));
}

#[tokio::test]
async fn test_mixed_or_min_records_only_present_histograms() {
    let PromExpr::Aggregate(AggregateExpr { op, param, .. }) = parser::parse("min(lhs)").unwrap()
    else {
        unreachable!()
    };
    let expected_info = "min: dropped native histogram samples because this aggregation is not supported for native histograms";

    for (histograms, expected_infos) in [
        (vec![], vec![]),
        (vec![direct_or_histogram()], vec![expected_info]),
    ] {
        let collector = PromqlAnnotationCollector::default();
        let (mut planner, input) = mixed_aggregate_input(histograms).await;
        planner.promql_annotations = Some(collector.clone());
        let float_column = planner.ctx.field_columns[0].clone();
        let histogram_column = planner.ctx.field_columns[1].clone();
        let (aggregate_exprs, _) = planner.create_aggregate_exprs(op, &param, &input).unwrap();
        let plan = LogicalPlanBuilder::from(input)
            .aggregate(vec![col("ts")], aggregate_exprs)
            .unwrap()
            .filter(
                planner
                    .mixed_ignored_histogram_filter_expr(op, &histogram_column)
                    .unwrap(),
            )
            .unwrap()
            .project([col(&float_column), col("ts")])
            .unwrap()
            .build()
            .unwrap();

        let (_, batches) = execute(plan, &build_query_engine_state()).await;
        assert_eq!(values(&batches, &float_column), vec![1.25]);
        let mut warnings = vec![];
        let mut infos = vec![];
        collector.append_to(&mut warnings, &mut infos);
        assert!(warnings.is_empty());
        assert_eq!(infos, expected_infos);
    }
}

#[tokio::test]
async fn test_mixed_or_value_aliases_do_not_replace_labels() {
    let left = source(
        "lhs",
        false,
        1,
        vec![("job", Some("job")), ("k", Some("float"))],
        DirectOrValue::Float64(1.0),
    );
    let right = source(
        "rhs",
        false,
        1,
        vec![
            ("job", Some("job")),
            ("k", Some("histogram")),
            (greptime_value(), Some("value-label")),
        ],
        DirectOrValue::NativeHistogram(direct_or_histogram()),
    );
    let table_provider = build_test_table_provider_with_fields(
        &[(DEFAULT_SCHEMA_NAME.to_string(), "dummy".to_string())],
        &[],
    )
    .await;
    let mut planner = PromPlanner {
        table_provider,
        ctx: PromPlannerContext::default(),
        promql_annotations: None,
    };
    let left = LogicalPlanBuilder::from(scan(&left))
        .project(vec![
            col("ts"),
            col("job"),
            col("k"),
            col("v").alias(greptime_value()),
        ])
        .unwrap()
        .build()
        .unwrap();
    let left_context = direct_or_context("lhs", &["job", "k"], greptime_value());
    let right_context = direct_or_context("rhs", &["job", "k", greptime_value()], "v");
    let plan = planner
        .or_operator(
            left,
            scan(&right),
            left_context.tag_columns.iter().cloned().collect(),
            right_context.tag_columns.iter().cloned().collect(),
            left_context,
            right_context,
            &or_modifier("lhs or on(k) rhs"),
        )
        .unwrap();

    assert_eq!(
        plan.schema()
            .field_with_name(None, greptime_value())
            .unwrap()
            .data_type(),
        &ArrowDataType::Utf8
    );
    assert!(
        planner
            .ctx
            .field_columns
            .iter()
            .all(|field| { field != greptime_value() && field != greptime_native_histogram() })
    );
    assert!(PromPlanner::field_columns_are_alternative_samples(
        plan.schema(),
        &planner.ctx.field_columns
    ));
    let (_, batches) = execute(plan, &build_query_engine_state()).await;
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
    let labels = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column_by_name(greptime_value())
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .flatten()
        })
        .collect::<Vec<_>>();
    assert_eq!(labels, vec!["value-label"]);
}

#[tokio::test]
async fn test_mixed_or_routes_float_histogram_and_label_functions() {
    for (function, expected) in [("abs", 1.25), ("round", 1.0), ("histogram_count", 1.0)] {
        let (mut planner, input) = mixed_direct_or(false).await;
        let preserve_any_value = PromPlanner::field_columns_are_alternative_samples(
            input.schema(),
            &planner.ctx.field_columns,
        );
        let PromExpr::Call(call) = parser::parse(&format!("{function}(lhs)")).unwrap() else {
            unreachable!()
        };
        let state = build_query_engine_state();
        let (mut exprs, _) = planner
            .create_function_expr(&call.func, vec![], input.schema(), &state, None)
            .unwrap();
        exprs.insert(0, planner.create_time_index_column_expr().unwrap());
        exprs.extend(planner.create_tag_column_exprs().unwrap());
        let plan = LogicalPlanBuilder::from(input)
            .project(exprs)
            .unwrap()
            .filter(
                planner
                    .create_empty_values_filter_expr(preserve_any_value)
                    .unwrap(),
            )
            .unwrap()
            .build()
            .unwrap();
        let (_, batches) = execute(plan, &state).await;
        let values = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .schema()
                    .fields()
                    .iter()
                    .position(|field| field.data_type() == &ArrowDataType::Float64)
                    .map(|index| {
                        batch
                            .column(index)
                            .as_any()
                            .downcast_ref::<Float64Array>()
                            .unwrap()
                            .iter()
                            .flatten()
                    })
                    .into_iter()
                    .flatten()
            })
            .collect::<Vec<_>>();
        assert_eq!(values, vec![expected], "{function}");
    }

    let (mut planner, input) = mixed_direct_or(false).await;
    let preserve_any_value = PromPlanner::field_columns_are_alternative_samples(
        input.schema(),
        &planner.ctx.field_columns,
    );
    let PromExpr::Call(call) =
        parser::parse(r#"label_replace(lhs, "copy", "$1", "k", "(.*)")"#).unwrap()
    else {
        unreachable!()
    };
    let args = planner.create_function_args(&call.args.args).unwrap();
    let state = build_query_engine_state();
    let (mut exprs, _) = planner
        .create_function_expr(&call.func, args.literals, input.schema(), &state, None)
        .unwrap();
    exprs.insert(0, planner.create_time_index_column_expr().unwrap());
    exprs.extend(planner.create_tag_column_exprs().unwrap());
    let plan = LogicalPlanBuilder::from(input)
        .project(exprs)
        .unwrap()
        .filter(
            planner
                .create_empty_values_filter_expr(preserve_any_value)
                .unwrap(),
        )
        .unwrap()
        .build()
        .unwrap();
    let (_, batches) = execute(plan, &state).await;
    let sample_count = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
    assert_eq!(sample_count, 2);
}

/// Table provider with a single metric `cv_metric` holding three series at `ts=1000`:
/// `k="a"` and `k="c"` carry the value 1.0, `k="b"` carries 2.0.
async fn build_count_values_table_provider() -> DfTableSourceProvider {
    build_count_values_table_provider_with_values(&[1.0, 2.0, 1.0]).await
}

/// Table provider with a single metric `cv_metric` holding one series per given value at
/// `ts=1000` (`k` names the series, the sample value is the given value).
async fn build_count_values_table_provider_with_values(values: &[f64]) -> DfTableSourceProvider {
    build_count_values_table_provider_with_value_array(Arc::new(Float64Array::from(
        values.to_vec(),
    )))
    .await
}

/// Like [`build_count_values_table_provider_with_values`], but with a caller provided value
/// column, so tests can cover value columns that are not `Float64` (e.g. `BIGINT`).
async fn build_count_values_table_provider_with_value_array(
    values: ArrayRef,
) -> DfTableSourceProvider {
    build_count_values_table_provider_with_tag_value_array("k", values).await
}

/// Like [`build_count_values_table_provider_with_values`] with `[1.0, 1.0, 1.0]`, but with `tag`
/// as the name of the series tag column, so tests can cover tag names that collide with the
/// internal metric-name column while keeping the same `k0`/`k1`/`k2` rows: three series with
/// equal sample values that must stay separate groups.
async fn build_count_values_collision_table_provider(tag: &str) -> DfTableSourceProvider {
    build_count_values_table_provider_with_tag_value_array(
        tag,
        Arc::new(Float64Array::from(vec![1.0; 3])),
    )
    .await
}

/// Like [`build_count_values_table_provider_with_value_array`], but with a caller provided tag
/// name.
async fn build_count_values_table_provider_with_tag_value_array(
    tag: &str,
    values: ArrayRef,
) -> DfTableSourceProvider {
    let value_data_type = ConcreteDataType::from_arrow_type(values.data_type());
    let catalog_list = MemoryCatalogManager::with_default_setup();
    let columns = vec![
        ColumnSchema::new(tag.to_string(), ConcreteDataType::string_datatype(), false),
        ColumnSchema::new(
            "timestamp".to_string(),
            ConcreteDataType::timestamp_millisecond_datatype(),
            false,
        )
        .with_time_index(true),
        ColumnSchema::new(greptime_value().to_string(), value_data_type, true),
    ];
    let schema = Arc::new(Schema::new(columns));
    let table_meta = TableMetaBuilder::empty()
        .schema(schema.clone())
        .primary_key_indices(vec![0])
        .value_indices(vec![2])
        .next_column_id(1024)
        .build()
        .unwrap();
    let table_info = Arc::new(
        TableInfoBuilder::default()
            .table_id(3_001)
            .name("cv_metric")
            .meta(table_meta)
            .build()
            .unwrap(),
    );
    let batch = RecordBatch::try_new(
        schema.arrow_schema().clone(),
        vec![
            Arc::new(StringArray::from(
                (0..values.len())
                    .map(|index| format!("k{index}"))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(TimestampMillisecondArray::from(vec![1_000; values.len()])),
            values.clone(),
        ],
    )
    .unwrap();
    let backing = GreptimeMemTable::new_with_catalog(
        "cv_metric",
        GreptimeRecordBatch::from_df_record_batch(schema, batch),
        3_001,
        DEFAULT_CATALOG_NAME.to_string(),
        DEFAULT_SCHEMA_NAME.to_string(),
    );
    let table = Arc::new(Table::new(
        table_info,
        FilterPushDownType::Unsupported,
        backing.data_source(),
    ));

    assert!(
        catalog_list
            .register_table_sync(RegisterTableRequest {
                catalog: DEFAULT_CATALOG_NAME.to_string(),
                schema: DEFAULT_SCHEMA_NAME.to_string(),
                table_name: "cv_metric".to_string(),
                table_id: 3_001,
                table,
            })
            .is_ok()
    );

    DfTableSourceProvider::new(
        catalog_list,
        false,
        QueryContext::arc(),
        DummyDecoder::arc(),
        false,
    )
}

/// Collects `(label, value)` pairs of a `count_values` result, where `label` is the
/// PromQL label generated by `count_values` and `value` is the aggregated sample value.
///
/// The generated label holds the original sample value in PromQL's textual form, so it
/// is asserted as a string: comparing it as a number would not catch formatting bugs
/// (`1.0` instead of `1`, scientific notation, ...).
fn count_values_rows<'a>(batches: &'a [RecordBatch], label: &str) -> Vec<(&'a str, f64)> {
    let mut rows = batches
        .iter()
        .flat_map(|batch| {
            // The aggregated value is the only numeric column that is not the generated label.
            let value_index = batch
                .schema()
                .fields()
                .iter()
                .position(|field| {
                    field.name() != label
                        && matches!(
                            field.data_type(),
                            ArrowDataType::Float64 | ArrowDataType::Int64 | ArrowDataType::UInt64
                        )
                })
                .expect("no aggregated value column");
            let labels = batch
                .column_by_name(label)
                .expect("no generated label column")
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("the generated label must be a string column");
            let values = datafusion::arrow::compute::cast(
                batch.column(value_index),
                &ArrowDataType::Float64,
            )
            .unwrap();
            let values = values.as_any().downcast_ref::<Float64Array>().unwrap();
            labels
                .iter()
                .zip(values.iter())
                .map(|(label, value)| (label.unwrap(), value.unwrap()))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| left.0.cmp(right.0).then(left.1.total_cmp(&right.1)));
    rows
}

/// Asserts that a `count_values` result holds one sample per label set and evaluation
/// timestamp: Prometheus groups by the generated label, so a timestamp must never repeat
/// a label set (that would mean the samples were still grouped by the overwritten input
/// label).
fn assert_unique_label_set_per_timestamp(batches: &[RecordBatch], label: &str) {
    let mut seen = HashMap::<i64, HashSet<String>>::new();
    for batch in batches {
        let timestamp_index = batch
            .schema()
            .fields()
            .iter()
            .position(|field| matches!(field.data_type(), ArrowDataType::Timestamp(..)))
            .expect("no timestamp column");
        let timestamps = batch
            .column(timestamp_index)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .expect("timestamp column is not a millisecond timestamp");
        let labels = batch
            .column_by_name(label)
            .expect("no generated label column")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("the generated label must be a string column");
        for (timestamp, label) in timestamps.iter().zip(labels.iter()) {
            let timestamp = timestamp.unwrap();
            let label = label.unwrap();
            assert!(
                seen.entry(timestamp).or_default().insert(label.to_string()),
                "duplicated label set `{label}` at timestamp {timestamp}"
            );
        }
    }
}

#[tokio::test]
async fn test_count_values_generated_label_survives_enclosing_expr() {
    // https://github.com/GreptimeTeam/greptimedb/issues/9181
    for (case, label) in [
        (r#"count_values("v", prometheus_tsdb_head_series)"#, "v"),
        (
            r#"abs(count_values("v", prometheus_tsdb_head_series))"#,
            "v",
        ),
        (
            r#"round(count_values("v", prometheus_tsdb_head_series))"#,
            "v",
        ),
        (r#"count_values("v", prometheus_tsdb_head_series) + 1"#, "v"),
        (
            r#"topk(1, count_values("v", prometheus_tsdb_head_series))"#,
            "v",
        ),
        (
            r#"sum by (v) (count_values("v", prometheus_tsdb_head_series))"#,
            "v",
        ),
        (
            r#"label_replace(count_values("v", prometheus_tsdb_head_series), "vcopy", "$1", "v", "(.*)")"#,
            "v",
        ),
        (
            r#"count_values("v", prometheus_tsdb_head_series) by (ip) + 1"#,
            "v",
        ),
        // The generated label overwrites an input label with the same name.
        (
            r#"count_values("ip", prometheus_tsdb_head_series) by (ip)"#,
            "ip",
        ),
        (
            r#"count_values("ip", prometheus_tsdb_head_series) by (ip) + 1"#,
            "ip",
        ),
    ] {
        let plan = PromPlanner::stmt_to_plan(
            build_test_table_provider_with_fields(
                &[(
                    DEFAULT_SCHEMA_NAME.to_string(),
                    "prometheus_tsdb_head_series".to_string(),
                )],
                &["ip"],
            )
            .await,
            &build_eval_stmt(case),
            &build_query_engine_state(),
        )
        .await
        .unwrap();

        let label_columns = plan
            .schema()
            .fields()
            .iter()
            .filter(|field| field.name() == label)
            .count();
        assert_eq!(
            label_columns,
            1,
            "{case}: the `{label}` label must survive: {}",
            plan.display_indent()
        );
    }
}

#[tokio::test]
async fn test_count_values_generated_label_in_enclosing_expr_execute() {
    // https://github.com/GreptimeTeam/greptimedb/issues/9181
    let state = build_query_engine_state();
    // (query, generated label, expected `(label value, aggregated value)` pairs)
    for (query, label, expected) in [
        (
            r#"count_values("v", cv_metric)"#,
            "v",
            vec![("1", 2.0), ("2", 1.0)],
        ),
        (
            r#"abs(count_values("v", cv_metric))"#,
            "v",
            vec![("1", 2.0), ("2", 1.0)],
        ),
        (
            r#"round(count_values("v", cv_metric))"#,
            "v",
            vec![("1", 2.0), ("2", 1.0)],
        ),
        (
            r#"count_values("v", cv_metric) + 1"#,
            "v",
            vec![("1", 3.0), ("2", 2.0)],
        ),
        (
            r#"sum by (v) (count_values("v", cv_metric))"#,
            "v",
            vec![("1", 2.0), ("2", 1.0)],
        ),
        (
            r#"topk(10, count_values("v", cv_metric))"#,
            "v",
            vec![("1", 2.0), ("2", 1.0)],
        ),
        // The generated label overwrites the input label with the same name, and the
        // samples are grouped by the generated label only: `{k="1"}` holds the two
        // samples of value `1.0` instead of one row per (overwritten label, value).
        (
            r#"count_values("k", cv_metric) by (k)"#,
            "k",
            vec![("1", 2.0), ("2", 1.0)],
        ),
    ] {
        let plan = PromPlanner::stmt_to_plan(
            build_count_values_table_provider().await,
            &operator_eval_stmt(query),
            &state,
        )
        .await
        .unwrap_or_else(|err| panic!("{query}: {err}"));

        assert_eq!(
            plan.schema()
                .fields()
                .iter()
                .filter(|field| field.name() == label)
                .count(),
            1,
            "{query}: {}",
            plan.display_indent()
        );

        let (_, batches) = execute(plan, &state).await;
        assert_eq!(count_values_rows(&batches, label), expected, "{query}");
        assert_unique_label_set_per_timestamp(&batches, label);
    }
}

#[tokio::test]
async fn test_count_values_label_is_prometheus_formatted_value() {
    // PromQL materializes the generated label with `strconv.FormatFloat(value, 'f', -1, 64)`:
    // the shortest decimal form of the sample value without an exponent. The label is a
    // label, so it must be a string column holding exactly that text: arrow's
    // `Float64 -> Utf8` cast would render `1`/`200`/`1e21` as `1.0`/`200.0`/`1e21`.
    let state = build_query_engine_state();
    // Samples are grouped by that formatted text, exactly like Prometheus groups by the
    // generated label: `-0.0` and `0.0` are two series (`-0` and `0`), while values that
    // round to the same text share one group. `0.0` formats as "0".
    let plan = PromPlanner::stmt_to_plan(
        build_count_values_table_provider_with_values(&[
            -0.0, 0.0, 1.0, 0.5, 200.0, 1e21, 1e-7, 2.5,
        ])
        .await,
        &operator_eval_stmt(r#"count_values("v", cv_metric)"#),
        &state,
    )
    .await
    .unwrap();

    let (_, batches) = execute(plan, &state).await;
    assert_unique_label_set_per_timestamp(&batches, "v");
    let mut labels = count_values_rows(&batches, "v")
        .into_iter()
        .map(|(label, _)| label)
        .collect::<Vec<_>>();
    labels.sort();
    assert_eq!(
        labels,
        vec![
            "-0",
            "0",
            "0.0000001",
            "0.5",
            "1",
            "1000000000000000000000",
            "2.5",
            "200",
        ]
    );
}

#[tokio::test]
async fn test_count_values_groups_by_formatted_value_for_bigint_input() {
    // The grouping key of `count_values` is the formatted sample value, not the raw input
    // value. Two `BIGINT` values that differ below the `Float64` precision (`2^53` and
    // `2^53 + 1`) cast and format to the same label, so they must share one group and one
    // count, exactly like Prometheus, which groups by the generated label text.
    let state = build_query_engine_state();
    let plan = PromPlanner::stmt_to_plan(
        build_count_values_table_provider_with_value_array(Arc::new(Int64Array::from(vec![
            9_007_199_254_740_992_i64,
            9_007_199_254_740_993_i64,
        ])))
        .await,
        &operator_eval_stmt(r#"count_values("v", cv_metric)"#),
        &state,
    )
    .await
    .unwrap();

    let (_, batches) = execute(plan, &state).await;
    // One timestamp must never carry the same label set twice.
    assert_unique_label_set_per_timestamp(&batches, "v");
    assert_eq!(
        count_values_rows(&batches, "v"),
        vec![("9007199254740992", 2.0)]
    );
}

/// Collects `(series tag, metric-name marker, sample value)` triples of every row in `batches`,
/// one row per emitted sample of the `cv_metric` fixture, sorted by series tag.
fn metric_name_rows(batches: &[RecordBatch], marker: &str) -> Vec<(String, String, f64)> {
    metric_name_rows_of(batches, marker, greptime_value())
}

/// Like [`metric_name_rows`], but reading the sample from `value_column` (range functions name
/// their output column after the function instead of `greptime_value`).
fn metric_name_rows_of(
    batches: &[RecordBatch],
    marker: &str,
    value_column: &str,
) -> Vec<(String, String, f64)> {
    let mut rows = batches
        .iter()
        .flat_map(|batch| {
            let tag = batch
                .column_by_name("k")
                .expect("no series tag column")
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("the series tag must be a string column");
            let name = batch
                .column_by_name(marker)
                .expect("no metric-name marker column")
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("the metric-name marker must be a string column");
            let value = batch
                .column_by_name(value_column)
                .unwrap_or_else(|| {
                    panic!(
                        "no sample value column {value_column} in {}",
                        batch.schema()
                    )
                })
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("the sample value must be a float column");
            (0..batch.num_rows())
                .map(|row| {
                    (
                        tag.value(row).to_string(),
                        name.value(row).to_string(),
                        value.value(row),
                    )
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| left.0.cmp(&right.0));
    rows
}

#[tokio::test]
async fn test_raw_selector_attaches_metric_name_identity() {
    let state = build_query_engine_state();
    for query in [
        "cv_metric",
        r#"cv_metric{k=~"k.*"}"#,
        r#"{__name__="cv_metric"}"#,
    ] {
        let plan = PromPlanner::stmt_to_plan(
            build_count_values_table_provider().await,
            &operator_eval_stmt(query),
            &state,
        )
        .await
        .unwrap();

        let marker = PromPlanner::metric_name_column(plan.schema())
            .unwrap()
            .unwrap_or_else(|| panic!("{query}: the selector must attach its metric name"));
        assert_eq!(marker.name, PROMQL_METRIC_NAME_COLUMN, "{query}");

        let (_, batches) = execute(plan, &state).await;
        assert_eq!(
            metric_name_rows(&batches, &marker.name),
            vec![
                ("k0".to_string(), "cv_metric".to_string(), 1.0),
                ("k1".to_string(), "cv_metric".to_string(), 2.0),
                ("k2".to_string(), "cv_metric".to_string(), 1.0),
            ],
            "{query}"
        );
    }
}

#[tokio::test]
async fn test_collision_metric_name_label_is_preserved() {
    let plan = PromPlanner::stmt_to_plan(
        build_test_table_provider_with_distinct_tags(&[(
            "collision_metric",
            &[PROMQL_METRIC_NAME_COLUMN, "job"],
        )])
        .await,
        &operator_eval_stmt("collision_metric"),
        &build_query_engine_state(),
    )
    .await
    .unwrap();

    // The label physically named like the marker keeps its name and metadata; the marker
    // moves to a suffixed column instead of overwriting it.
    let physical = plan
        .schema()
        .fields()
        .iter()
        .find(|field| field.name() == PROMQL_METRIC_NAME_COLUMN)
        .expect("the physical label must be preserved");
    assert!(
        physical.metadata().get(PROMQL_FIELD_ROLE_KEY).is_none(),
        "the physical label must not carry the marker role"
    );
    let marker = PromPlanner::metric_name_column(plan.schema())
        .unwrap()
        .expect("the selector must attach its metric name");
    assert_eq!(
        marker.name,
        format!("{PROMQL_METRIC_NAME_COLUMN}_"),
        "{}",
        plan.display_indent()
    );
}

#[tokio::test]
async fn test_dropping_identity_keeps_physical_column_with_the_same_name() {
    // Dropping the identity must remove only the marked column, never the physical label
    // that happens to be spelled the same way.
    let plan = PromPlanner::stmt_to_plan(
        build_test_table_provider_with_distinct_tags(&[(
            "collision_metric",
            &[PROMQL_METRIC_NAME_COLUMN, "job"],
        )])
        .await,
        // Unary minus drops the metric name of its operand.
        &operator_eval_stmt("-collision_metric"),
        &build_query_engine_state(),
    )
    .await
    .unwrap();

    let schema = plan.schema();
    assert!(
        PromPlanner::metric_name_column(schema).unwrap().is_none(),
        "the identity must be dropped: {}",
        plan.display_indent()
    );
    assert!(
        schema
            .field_with_name(None, PROMQL_METRIC_NAME_COLUMN)
            .is_ok(),
        "the physical label must survive the drop: {}",
        plan.display_indent()
    );
    assert!(
        schema
            .field_with_name(None, &format!("{PROMQL_METRIC_NAME_COLUMN}_"))
            .is_err(),
        "the marker column must be gone: {}",
        plan.display_indent()
    );
}

#[tokio::test]
async fn test_unary_sign_controls_metric_name_identity() {
    let state = build_query_engine_state();
    // The parser returns the operand of unary plus unchanged and wraps unary minus in a
    // negation, which drops the metric name.
    for (sign, negated) in [("-", true), ("+", false)] {
        let query = format!("{sign}cv_metric");
        let plan = PromPlanner::stmt_to_plan(
            build_count_values_table_provider().await,
            &operator_eval_stmt(&query),
            &state,
        )
        .await
        .unwrap();

        let marker = PromPlanner::metric_name_column(plan.schema()).unwrap();
        assert_eq!(
            marker.is_some(),
            !negated,
            "{query}: {}",
            plan.display_indent()
        );

        // The negated sample column is aliased after the negation expression and no longer
        // carries the source field name, so identify it by type. The fixture has a single
        // `Float64` column; the tag, time index and marker are not `Float64`.
        let marker_name = marker.as_ref().map(|marker| marker.name.clone());
        let sample_columns = plan
            .schema()
            .fields()
            .iter()
            .filter(|field| {
                field.data_type() == &ArrowDataType::Float64
                    && marker_name.as_ref() != Some(field.name())
            })
            .map(|field| field.name().clone())
            .collect::<Vec<_>>();
        assert_eq!(
            sample_columns.len(),
            1,
            "{query}: expected exactly one float sample column: {}",
            plan.display_indent()
        );
        let sample_column = sample_columns.into_iter().next().unwrap();

        let (_, batches) = execute(plan, &state).await;
        match marker {
            Some(marker) => assert_eq!(
                metric_name_rows(&batches, &marker.name),
                vec![
                    ("k0".to_string(), "cv_metric".to_string(), 1.0),
                    ("k1".to_string(), "cv_metric".to_string(), 2.0),
                    ("k2".to_string(), "cv_metric".to_string(), 1.0),
                ],
                "{query}"
            ),
            None => {
                let mut values = values(&batches, &sample_column);
                values.sort_by(f64::total_cmp);
                assert_eq!(values, vec![-2.0, -1.0, -1.0], "{query}");
            }
        }
    }
}

#[tokio::test]
async fn test_ordinary_functions_drop_metric_name_from_their_result() {
    let state = build_query_engine_state();
    for query in ["abs(cv_metric)", "ceil(cv_metric)"] {
        let plan = PromPlanner::stmt_to_plan(
            build_count_values_table_provider().await,
            &operator_eval_stmt(query),
            &state,
        )
        .await
        .unwrap();

        assert!(
            PromPlanner::metric_name_column(plan.schema())
                .unwrap()
                .is_none(),
            "{query}: an ordinary function must drop the metric name: {}",
            plan.display_indent()
        );

        let sample_column = float_sample_column(&plan);
        let (_, batches) = execute(plan, &state).await;
        assert_metric_name_not_in_batches(&batches);
        assert_eq!(
            cv_rows(&batches, &sample_column),
            vec![
                ("k0".to_string(), 1_000, 1.0),
                ("k1".to_string(), 1_000, 2.0),
                ("k2".to_string(), 1_000, 1.0),
            ],
            "{query}"
        );
    }
}

#[tokio::test]
async fn test_sort_functions_keep_metric_name_identity() {
    let state = build_query_engine_state();
    for (query, expected_values) in [
        ("sort(cv_metric)", vec![1.0, 1.0, 2.0]),
        ("sort_desc(cv_metric)", vec![2.0, 1.0, 1.0]),
    ] {
        let plan = PromPlanner::stmt_to_plan(
            build_count_values_table_provider().await,
            &operator_eval_stmt(query),
            &state,
        )
        .await
        .unwrap();

        let marker = PromPlanner::metric_name_column(plan.schema())
            .unwrap()
            .unwrap_or_else(|| panic!("{query}: sort keeps the metric name"));
        assert_eq!(marker.name, PROMQL_METRIC_NAME_COLUMN, "{query}");

        let sample_column = float_sample_column(&plan);
        let (_, batches) = execute(plan, &state).await;
        assert_metric_name_in_batches(&batches, &marker.name);
        assert_eq!(
            metric_name_rows(&batches, &marker.name),
            vec![
                ("k0".to_string(), "cv_metric".to_string(), 1.0),
                ("k1".to_string(), "cv_metric".to_string(), 2.0),
                ("k2".to_string(), "cv_metric".to_string(), 1.0),
            ],
            "{query}"
        );
        assert_eq!(values(&batches, &sample_column), expected_values, "{query}");
    }
}

#[tokio::test]
async fn test_sort_by_label_resolves_semantic_name_to_the_marker() {
    let state = build_query_engine_state();
    // `__name__` is not a physical column: it resolves to the marked identity column, while
    // ordinary labels keep their existing lookup.
    for (query, sort_column, direction) in [
        (
            r#"sort_by_label(cv_metric, "__name__")"#,
            "__promql_metric_name",
            "ASC",
        ),
        (r#"sort_by_label_desc(cv_metric, "k")"#, "k", "DESC"),
    ] {
        let plan = PromPlanner::stmt_to_plan(
            build_count_values_table_provider().await,
            &operator_eval_stmt(query),
            &state,
        )
        .await
        .unwrap();

        let plan_text = plan.display_indent().to_string();
        assert!(
            plan_text.contains(&format!("{sort_column} {direction}")),
            "{query}: {plan_text}"
        );
        let marker = PromPlanner::metric_name_column(plan.schema())
            .unwrap()
            .unwrap_or_else(|| panic!("{query}: sort_by_label keeps the metric name"));
        assert_eq!(marker.name, PROMQL_METRIC_NAME_COLUMN, "{query}");

        let (_, batches) = execute(plan, &state).await;
        assert_metric_name_in_batches(&batches, &marker.name);
        assert_eq!(
            metric_name_rows(&batches, &marker.name),
            vec![
                ("k0".to_string(), "cv_metric".to_string(), 1.0),
                ("k1".to_string(), "cv_metric".to_string(), 2.0),
                ("k2".to_string(), "cv_metric".to_string(), 1.0),
            ],
            "{query}"
        );
    }
}

#[tokio::test]
async fn test_range_functions_control_metric_name_identity() {
    let state = build_query_engine_state();
    // The fixture's only samples sit at `ts=1000`; the evaluation point is the same second.
    for (query, keeps_name) in [
        ("last_over_time(cv_metric[5m])", true),
        ("sum_over_time(cv_metric[5m])", false),
    ] {
        let plan = PromPlanner::stmt_to_plan(
            build_count_values_table_provider().await,
            &operator_eval_stmt(query),
            &state,
        )
        .await
        .unwrap();

        let marker = PromPlanner::metric_name_column(plan.schema()).unwrap();
        assert_eq!(
            marker.is_some(),
            keeps_name,
            "{query}: {}",
            plan.display_indent()
        );

        let sample_column = float_sample_column(&plan);
        let (_, batches) = execute(plan, &state).await;
        assert_eq!(
            cv_rows(&batches, &sample_column),
            vec![
                ("k0".to_string(), 1_000, 1.0),
                ("k1".to_string(), 1_000, 2.0),
                ("k2".to_string(), 1_000, 1.0),
            ],
            "{query}"
        );
        match marker {
            Some(marker) => {
                assert_metric_name_in_batches(&batches, &marker.name);
                assert_eq!(
                    metric_name_rows_of(&batches, &marker.name, &sample_column),
                    vec![
                        ("k0".to_string(), "cv_metric".to_string(), 1.0),
                        ("k1".to_string(), "cv_metric".to_string(), 2.0),
                        ("k2".to_string(), "cv_metric".to_string(), 1.0),
                    ],
                    "{query}"
                );
            }
            None => assert_metric_name_not_in_batches(&batches),
        }
    }
}

#[tokio::test]
async fn test_histogram_quantile_folds_with_identity_then_drops_it() {
    let state = build_query_engine_state();
    let plan = PromPlanner::stmt_to_plan(
        classic_and_native_histogram_table_provider("native", None, direct_or_histogram()),
        &operator_eval_stmt("histogram_quantile(0.5, mixed_histogram)"),
        &state,
    )
    .await
    .unwrap();

    // The completed result drops the metric name ...
    assert!(
        PromPlanner::metric_name_column(plan.schema())
            .unwrap()
            .is_none(),
        "{}",
        plan.display_indent()
    );
    // ... while the fold still groups by the input's identity.
    let fold = find_histogram_fold(&plan).expect("the plan must contain a HistogramFold");
    assert!(
        PromPlanner::metric_name_column(fold.inputs()[0].schema())
            .unwrap()
            .is_some(),
        "the fold must see the input metric identity: {}",
        plan.display_indent()
    );

    let value_field = float_sample_column(&plan);
    let (_, batches) = execute(plan, &state).await;
    assert_metric_name_not_in_batches(&batches);
    let mut actual = batches
        .iter()
        .flat_map(|batch| {
            let tags = batch
                .column_by_name("tag")
                .expect("no series tag column")
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("the series tag must be a string column");
            let values = batch
                .column_by_name(&value_field)
                .expect("no sample value column")
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("the sample value must be a float column");
            (0..batch.num_rows())
                .map(|row| (tags.value(row).to_string(), values.value(row)))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    actual.sort_by(|left, right| left.0.cmp(&right.0));
    assert_eq!(
        actual,
        vec![("classic".to_string(), 1.0), ("native".to_string(), 0.0)]
    );
}

#[tokio::test]
async fn test_sort_by_label_semantic_name_without_marker_keeps_missing_label_error() {
    let state = build_query_engine_state();
    let provider = || async {
        build_test_table_provider_with_distinct_tags(&[("collision_metric", &["__name__", "job"])])
            .await
    };
    let plan = PromPlanner::stmt_to_plan(
        provider().await,
        &operator_eval_stmt("abs(collision_metric)"),
        &state,
    )
    .await
    .unwrap();
    assert!(
        plan.schema()
            .field_with_unqualified_name("__name__")
            .is_ok()
    );
    assert!(
        PromPlanner::metric_name_column(plan.schema())
            .unwrap()
            .is_none()
    );
    for function in ["sort_by_label", "sort_by_label_desc"] {
        let query = format!(r#"{function}(abs(collision_metric), "__name__")"#);
        let error =
            PromPlanner::stmt_to_plan(provider().await, &operator_eval_stmt(&query), &state)
                .await
                .unwrap_err();
        assert!(
            matches!(
                error,
                crate::promql::error::Error::DataFusionPlanning { .. }
            ),
            "{error}"
        );
        let query = format!(r#"{function}(collision_metric, "__name__")"#);
        let plan = PromPlanner::stmt_to_plan(provider().await, &operator_eval_stmt(&query), &state)
            .await
            .unwrap();
        let marker = PromPlanner::metric_name_column(plan.schema())
            .unwrap()
            .unwrap();
        assert!(
            plan.display_indent()
                .to_string()
                .contains(&format!("Sort: {}", marker.name))
        );
    }
}

/// Aggregations follow the metric-name lifecycle: the semantic `__name__` resolves through the
/// marked identity column only. An explicit `by (__name__, ...)` retains the name, every other
/// modifier drops it.
#[tokio::test]
async fn test_aggregation_metric_name_lifecycle() {
    let state = build_query_engine_state();
    // (query, metric-name column of the result, sorted sample values)
    for (query, expected_marker, expected_values) in [
        ("sum(cv_metric)", None, vec![4.0]),
        ("sum without (k) (cv_metric)", None, vec![4.0]),
        ("sum by (k) (cv_metric)", None, vec![1.0, 1.0, 2.0]),
        (
            "sum by (__name__) (cv_metric)",
            Some(PROMQL_METRIC_NAME_COLUMN),
            vec![4.0],
        ),
    ] {
        let plan = PromPlanner::stmt_to_plan(
            build_count_values_table_provider().await,
            &operator_eval_stmt(query),
            &state,
        )
        .await
        .unwrap_or_else(|err| panic!("{query}: {err}"));

        let sample_column = float_sample_column(&plan);
        let marker = PromPlanner::metric_name_column(plan.schema())
            .unwrap()
            .map(|marker| marker.name);
        assert_eq!(
            marker.as_deref(),
            expected_marker,
            "{query}: {}",
            plan.display_indent()
        );

        let (_, batches) = execute(plan, &state).await;
        let mut actual = values(&batches, &sample_column);
        actual.sort_by(f64::total_cmp);
        assert_eq!(actual, expected_values, "{query}");
        match expected_marker {
            // An explicit `by (__name__, ...)` groups by the marked identity column, so the result
            // keeps the name of the input series as the metric name of each result series.
            Some(marker) => {
                assert_metric_name_in_batches(&batches, marker);
                assert_eq!(
                    count_values_rows(&batches, marker),
                    vec![("cv_metric", 4.0)],
                    "{query}"
                );
            }
            None => assert_metric_name_not_in_batches(&batches),
        }
    }
}

/// `topk`/`bottomk` are aggregation operators that select a subset of the original samples with
/// their labels, so the metric-name identity and the samples must survive.
#[tokio::test]
async fn test_topk_bottomk_preserve_metric_name_and_samples() {
    let state = build_query_engine_state();
    for (query, expected) in [
        (
            "topk(1, cv_metric)",
            vec![("k2".to_string(), "cv_metric".to_string(), 3.0)],
        ),
        (
            "bottomk(1, cv_metric)",
            vec![("k0".to_string(), "cv_metric".to_string(), 1.0)],
        ),
    ] {
        let plan = PromPlanner::stmt_to_plan(
            build_count_values_table_provider_with_values(&[1.0, 2.0, 3.0]).await,
            &operator_eval_stmt(query),
            &state,
        )
        .await
        .unwrap_or_else(|err| panic!("{query}: {err}"));

        let marker = PromPlanner::metric_name_column(plan.schema())
            .unwrap()
            .unwrap_or_else(|| panic!("{query}: the metric name must survive"));
        assert_eq!(marker.name, PROMQL_METRIC_NAME_COLUMN, "{query}");

        let (_, batches) = execute(plan, &state).await;
        assert_metric_name_in_batches(&batches, &marker.name);
        assert_eq!(
            metric_name_rows(&batches, &marker.name),
            expected,
            "{query}"
        );
    }
}

/// `count_values("__name__", ...)` overwrites the identity of its input with the formatted
/// sample value: the result carries exactly one marked column, holding the sample value, and
/// the identity of the input must not survive as a second marker.
#[tokio::test]
async fn test_count_values_semantic_name_overwrites_metric_identity() {
    let state = build_query_engine_state();
    // (query, expected `(generated name, aggregated value)` rows)
    for (query, expected) in [
        (
            r#"count_values("__name__", cv_metric)"#,
            vec![("1", 2.0), ("2", 1.0)],
        ),
        (
            r#"count_values("__name__", cv_metric) by (__name__)"#,
            vec![("1", 2.0), ("2", 1.0)],
        ),
        (
            r#"sum by (__name__) (count_values("__name__", cv_metric))"#,
            vec![("1", 2.0), ("2", 1.0)],
        ),
        // Without an input identity the generated column must take neither the name of a
        // physical field of the input nor the semantic `__name__` itself.
        (
            r#"count_values("__name__", abs(cv_metric))"#,
            vec![("1", 2.0), ("2", 1.0)],
        ),
    ] {
        let plan = PromPlanner::stmt_to_plan(
            build_count_values_table_provider().await,
            &operator_eval_stmt(query),
            &state,
        )
        .await
        .unwrap_or_else(|err| panic!("{query}: {err}"));

        let markers = plan
            .schema()
            .fields()
            .iter()
            .filter(|field| {
                field
                    .metadata()
                    .get(PROMQL_FIELD_ROLE_KEY)
                    .map(String::as_str)
                    == Some(PROMQL_METRIC_NAME_ROLE)
            })
            .map(|field| field.name().clone())
            .collect::<Vec<_>>();
        assert_eq!(
            markers,
            vec![PROMQL_METRIC_NAME_COLUMN.to_string()],
            "{query}: {}",
            plan.display_indent()
        );

        let (_, batches) = execute(plan, &state).await;
        assert_metric_name_in_batches(&batches, PROMQL_METRIC_NAME_COLUMN);
        assert_eq!(
            count_values_rows(&batches, PROMQL_METRIC_NAME_COLUMN),
            expected,
            "{query}"
        );
    }
}

/// `count_values("__name__", ...)` writes the semantic metric name, which lives in the marked
/// identity column and never in a physical column of the input: a physical `__name__` label (or a
/// physical column occupying the internal marker name) survives unmarked, while the generated
/// name moves to the sole marked column, suffixed on collision.
#[tokio::test]
async fn test_count_values_semantic_name_keeps_physical_collision_columns() {
    let state = build_query_engine_state();
    // (physical tag columns, physical column that must survive, generated name column, query)
    for (tags, physical, generated, query) in [
        (
            vec![METRIC_NAME, "job"],
            METRIC_NAME,
            PROMQL_METRIC_NAME_COLUMN.to_string(),
            r#"count_values("__name__", collision_metric) without(job)"#,
        ),
        (
            vec![PROMQL_METRIC_NAME_COLUMN, "job"],
            PROMQL_METRIC_NAME_COLUMN,
            format!("{PROMQL_METRIC_NAME_COLUMN}_"),
            r#"count_values("__name__", abs(collision_metric)) without(job)"#,
        ),
    ] {
        let plan = PromPlanner::stmt_to_plan(
            build_test_table_provider_with_distinct_tags(&[("collision_metric", tags.as_slice())])
                .await,
            &operator_eval_stmt(query),
            &state,
        )
        .await
        .unwrap_or_else(|err| panic!("{query}: {err}"));

        let field = plan
            .schema()
            .field_with_unqualified_name(physical)
            .unwrap_or_else(|_| panic!("{query}: the physical `{physical}` column must survive"));
        assert!(
            field.metadata().get(PROMQL_FIELD_ROLE_KEY).is_none(),
            "{query}: the physical `{physical}` column must stay unmarked: {}",
            plan.display_indent()
        );
        let marked = plan
            .schema()
            .fields()
            .iter()
            .filter(|field| {
                field
                    .metadata()
                    .get(PROMQL_FIELD_ROLE_KEY)
                    .map(String::as_str)
                    == Some(PROMQL_METRIC_NAME_ROLE)
            })
            .map(|field| field.name().clone())
            .collect::<Vec<_>>();
        assert_eq!(
            marked,
            vec![generated],
            "{query}: {}",
            plan.display_indent()
        );
    }
}

/// A physical `__name__` label is an ordinary label, not the semantic metric name: with
/// `without(...)` grouping `count_values("__name__", ...)` keeps it and the generated name goes to
/// the sole marked column. Distinct physical labels with equal sample values must still be
/// separate groups.
#[tokio::test]
async fn test_count_values_keeps_distinct_physical_name_labels() {
    let state = build_query_engine_state();
    let plan = PromPlanner::stmt_to_plan(
        build_count_values_collision_table_provider(METRIC_NAME).await,
        &operator_eval_stmt(r#"count_values("__name__", cv_metric) without(job)"#),
        &state,
    )
    .await
    .unwrap();

    let physical = plan
        .schema()
        .field_with_unqualified_name(METRIC_NAME)
        .expect("the physical `__name__` label must survive");
    assert!(physical.metadata().get(PROMQL_FIELD_ROLE_KEY).is_none());
    let marker = PromPlanner::metric_name_column(plan.schema())
        .unwrap()
        .unwrap_or_else(|| {
            panic!(
                "the generated name must be marked: {}",
                plan.display_indent()
            )
        });
    assert_eq!(marker.name, PROMQL_METRIC_NAME_COLUMN);

    let (_, batches) = execute(plan, &state).await;
    assert_metric_name_in_batches(&batches, &marker.name);
    // `k0`, `k1` and `k2` share one sample value and one timestamp: a single group would mean the
    // physical `__name__` label was dropped from the grouping key.
    assert_eq!(
        count_values_rows(&batches, &marker.name),
        vec![("1", 1.0), ("1", 1.0), ("1", 1.0)]
    );
    let mut physical_labels = batches
        .iter()
        .flat_map(|batch| {
            let labels = batch
                .column_by_name(METRIC_NAME)
                .expect("the physical `__name__` label must be projected")
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("the physical `__name__` label must be a string column");
            (0..batch.num_rows())
                .map(|row| labels.value(row).to_string())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    physical_labels.sort();
    assert_eq!(physical_labels, vec!["k0", "k1", "k2"]);
}

/// `absent()` drops the metric-name identity of its input - even when the selector matched no
/// series - and must still emit one 1-valued sample per evaluation timestamp instead of an
/// empty result.
#[tokio::test]
async fn test_absent_drops_metric_name_and_emits_one_sample() {
    let state = build_query_engine_state();
    for query in [
        // A metric that does not exist at all.
        "abs(absent(nonexistent_metric))",
        // An equality label of an existing metric matching no series.
        r#"absent(cv_metric{k="missing"})"#,
    ] {
        let plan = PromPlanner::stmt_to_plan(
            build_count_values_table_provider().await,
            &operator_eval_stmt(query),
            &state,
        )
        .await
        .unwrap_or_else(|err| panic!("{query}: {err}"));

        assert!(
            PromPlanner::metric_name_column(plan.schema())
                .unwrap()
                .is_none(),
            "{query}: {}",
            plan.display_indent()
        );
        let sample_column = float_sample_column(&plan);
        let (_, batches) = execute(plan, &state).await;
        assert_metric_name_not_in_batches(&batches);
        assert_eq!(
            values(&batches, &sample_column),
            vec![1.0],
            "{query}: absent must emit one 1-valued sample"
        );
    }
}

/// Binary operations between a vector and a scalar follow the metric-name lifecycle: arithmetic
/// and `bool` comparisons build a new identity and drop the name, while comparison filters
/// without `bool` keep the vector side, including its name and its sample values.
#[tokio::test]
async fn test_binary_scalar_metric_name_lifecycle() {
    let state = build_query_engine_state();
    // (query, whether the metric-name identity survives, `(series tag, sample value)` at ts=1000)
    for (query, keeps_marker, expected) in [
        (
            "cv_metric + 1",
            false,
            vec![("k0", 2.0), ("k1", 3.0), ("k2", 2.0)],
        ),
        (
            "1 + cv_metric",
            false,
            vec![("k0", 2.0), ("k1", 3.0), ("k2", 2.0)],
        ),
        (
            "cv_metric > bool 1",
            false,
            vec![("k0", 0.0), ("k1", 1.0), ("k2", 0.0)],
        ),
        (
            "1 < bool cv_metric",
            false,
            vec![("k0", 0.0), ("k1", 1.0), ("k2", 0.0)],
        ),
        ("cv_metric > 1", true, vec![("k1", 2.0)]),
        ("1 < cv_metric", true, vec![("k1", 2.0)]),
        // Computed scalars join instead of projecting against a literal.
        (
            "cv_metric + scalar(sum(cv_metric))",
            false,
            vec![("k0", 5.0), ("k1", 6.0), ("k2", 5.0)],
        ),
        (
            "scalar(sum(cv_metric)) + cv_metric",
            false,
            vec![("k0", 5.0), ("k1", 6.0), ("k2", 5.0)],
        ),
        (
            "cv_metric > scalar(min(cv_metric))",
            true,
            vec![("k1", 2.0)],
        ),
        (
            "scalar(min(cv_metric)) < cv_metric",
            true,
            vec![("k1", 2.0)],
        ),
        // The ordinary path: `abs` already dropped the name, `sort` keeps it and must lose it.
        (
            "abs(cv_metric) + 1",
            false,
            vec![("k0", 2.0), ("k1", 3.0), ("k2", 2.0)],
        ),
        (
            "sort(cv_metric) + 1",
            false,
            vec![("k0", 2.0), ("k1", 3.0), ("k2", 2.0)],
        ),
    ] {
        let plan = PromPlanner::stmt_to_plan(
            build_count_values_table_provider().await,
            &operator_eval_stmt(query),
            &state,
        )
        .await
        .unwrap_or_else(|err| panic!("{query}: {err}"));

        let sample_column = float_sample_column(&plan);
        let marker = PromPlanner::metric_name_column(plan.schema()).unwrap();
        assert_eq!(
            marker.is_some(),
            keeps_marker,
            "{query}: {}",
            plan.display_indent()
        );

        let (_, batches) = execute(plan, &state).await;
        if keeps_marker {
            assert_metric_name_in_batches(&batches, PROMQL_METRIC_NAME_COLUMN);
            // A comparison filter keeps the vector side's value (2.0), never the scalar's (1).
            assert_eq!(
                metric_name_rows(&batches, PROMQL_METRIC_NAME_COLUMN),
                expected
                    .iter()
                    .map(|(tag, value)| (tag.to_string(), "cv_metric".to_string(), *value))
                    .collect::<Vec<_>>(),
                "{query}"
            );
        } else {
            assert_metric_name_not_in_batches(&batches);
            assert_eq!(
                cv_rows(&batches, &sample_column),
                expected
                    .iter()
                    .map(|(tag, value)| (tag.to_string(), 1_000, *value))
                    .collect::<Vec<_>>(),
                "{query}"
            );
        }
    }
}

/// A binary island plans a repeated selector once and computes a new sample per series, so its
/// result must not keep the metric name of the underlying selector.
#[tokio::test]
async fn test_binary_island_drops_metric_name_of_nested_arithmetic() {
    let state = build_query_engine_state();
    let query = "(cv_metric + 1) * (cv_metric + 2)";
    let plan = PromPlanner::stmt_to_plan(
        build_count_values_table_provider().await,
        &operator_eval_stmt(query),
        &state,
    )
    .await
    .unwrap_or_else(|err| panic!("{query}: {err}"));

    // One underlying scan, aliased once: the island reused the shared selector.
    let plan_str = plan.display_indent_schema().to_string();
    assert_eq!(
        plan_str.matches("SubqueryAlias: __prom_v0").count(),
        1,
        "{plan_str}"
    );
    assert_eq!(
        plan_str.matches("TableScan: cv_metric").count(),
        1,
        "{plan_str}"
    );

    assert!(
        PromPlanner::metric_name_column(plan.schema())
            .unwrap()
            .is_none(),
        "{query}: {}",
        plan.display_indent()
    );

    let sample_column = float_sample_column(&plan);
    let (_, batches) = execute(plan, &state).await;
    assert_metric_name_not_in_batches(&batches);
    assert_eq!(
        cv_rows(&batches, &sample_column),
        vec![
            ("k0".to_string(), 1_000, 6.0),
            ("k1".to_string(), 1_000, 12.0),
            ("k2".to_string(), 1_000, 6.0),
        ],
        "{query}"
    );
}

/// Table provider with two single-series metrics that share the ordinary label `tag="shared"` at
/// `ts=1000`: `left_metric` holds 2.0 and `right_metric` 3.0, so a vector operation of the two
/// matches them on that label alone.
fn binary_metric_name_table_provider() -> DfTableSourceProvider {
    let catalog = MemoryCatalogManager::with_default_setup();
    let tables = [
        operator_metric_table(
            "left_metric",
            2_101,
            "shared",
            None,
            DirectOrValue::Float64(2.0),
        ),
        operator_metric_table(
            "right_metric",
            2_102,
            "shared",
            None,
            DirectOrValue::Float64(3.0),
        ),
    ];
    for table in tables {
        let info = table.table_info();
        catalog
            .register_table_sync(RegisterTableRequest {
                catalog: DEFAULT_CATALOG_NAME.to_string(),
                schema: DEFAULT_SCHEMA_NAME.to_string(),
                table_name: info.name.clone(),
                table_id: info.ident.table_id,
                table,
            })
            .unwrap();
    }
    DfTableSourceProvider::new(
        catalog,
        false,
        QueryContext::arc(),
        DummyDecoder::arc(),
        false,
    )
}

/// `(series tag, metric name, sample value)` rows of a binary result, sorted. A label or an
/// identity the operation dropped reads as `None`, and so does a column the operation dropped
/// entirely, as `ignoring(tag)` or `on(__name__)` drop the only ordinary label.
fn binary_result_rows(
    batches: &[RecordBatch],
    marker: Option<&str>,
    tag_column: &str,
    value_column: &str,
) -> Vec<(Option<String>, Option<String>, f64)> {
    let mut rows = batches
        .iter()
        .flat_map(|batch| {
            let tag = batch
                .column_by_name(tag_column)
                .map(|column| column.as_any().downcast_ref::<StringArray>().unwrap());
            let name = marker.map(|marker| {
                batch
                    .column_by_name(marker)
                    .unwrap_or_else(|| {
                        panic!("no metric-name column {marker} in {}", batch.schema())
                    })
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("the metric-name column must be a string column")
            });
            let value = batch
                .column_by_name(value_column)
                .unwrap_or_else(|| {
                    panic!(
                        "no sample value column {value_column} in {}",
                        batch.schema()
                    )
                })
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("the sample value must be a float column");
            (0..batch.num_rows())
                .map(|row| {
                    assert!(value.is_valid(row), "the sample value must be valid");
                    let tag = tag
                        .filter(|tag| tag.is_valid(row))
                        .map(|tag| tag.value(row).to_string());
                    let name = name
                        .filter(|name| name.is_valid(row))
                        .map(|name| name.value(row).to_string());
                    (tag, name, value.value(row))
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| left.partial_cmp(right).unwrap());
    rows
}

/// Expected rows of a binary result as `(series tag, metric name, sample value)`.
type BinaryResultRow = (Option<&'static str>, Option<&'static str>, f64);

fn expected_binary_result_rows(
    expected: &[BinaryResultRow],
) -> Vec<(Option<String>, Option<String>, f64)> {
    expected
        .iter()
        .map(|(tag, name, value)| (tag.map(str::to_string), name.map(str::to_string), *value))
        .collect()
}

/// Vector-vector binary operations derive the metric-name identity from their matching: the
/// default and `ignoring(...)` matching compare every ordinary label, `on(...)` keeps only the
/// named ones, and a group modifier copies `__name__` from the "one" side. Arithmetic and `bool`
/// comparisons build a new identity and drop the name, while a comparison that filters keeps the
/// projected side - name and sample values included.
#[tokio::test]
async fn test_binary_vector_metric_name_lifecycle() {
    let state = build_query_engine_state();
    // (query, whether the plan keeps the identity, expected result rows)
    for (query, keeps_marker, expected) in [
        // Different metric names, same ordinary label: the default matching ignores the name.
        (
            "left_metric + right_metric",
            false,
            vec![(Some("shared"), None, 5.0)],
        ),
        (
            "left_metric < bool right_metric",
            false,
            vec![(Some("shared"), None, 1.0)],
        ),
        // A comparison that filters keeps the left side, its name and its sample value.
        (
            "left_metric < right_metric",
            true,
            vec![(Some("shared"), Some("left_metric"), 2.0)],
        ),
        // `on(tag)` drops the identity from the result labels.
        (
            "left_metric + on(tag) right_metric",
            false,
            vec![(Some("shared"), None, 5.0)],
        ),
        (
            "left_metric < on(tag) right_metric",
            false,
            vec![(Some("shared"), None, 2.0)],
        ),
        // `ignoring(tag)` keeps the identity in the result labels, and the matching no longer
        // compares `tag`, so the two operands still match.
        (
            "left_metric < ignoring(tag) right_metric",
            true,
            vec![(None, Some("left_metric"), 2.0)],
        ),
        // An explicit `on(__name__)` distinguishes different names and matches equal ones.
        ("left_metric < on(__name__) right_metric", true, vec![]),
        (
            "left_metric <= on(__name__) left_metric",
            true,
            vec![(None, Some("left_metric"), 2.0)],
        ),
        // `group_left(__name__)`/`group_right(__name__)` copy the "one" side's name.
        (
            "left_metric < on(tag) group_left(__name__) right_metric",
            true,
            vec![(Some("shared"), Some("right_metric"), 2.0)],
        ),
        (
            "left_metric < on(tag) group_right(__name__) right_metric",
            true,
            vec![(Some("shared"), Some("left_metric"), 2.0)],
        ),
        // A "one" side that dropped its own name clears the identity.
        (
            "left_metric < on(tag) group_left(__name__) abs(right_metric)",
            true,
            vec![(Some("shared"), None, 2.0)],
        ),
        // The set operators keep the left operand's identity, and match on `__name__` when asked.
        (
            "left_metric and right_metric",
            true,
            vec![(Some("shared"), Some("left_metric"), 2.0)],
        ),
        // `or` matches like the default and keeps the row it selected, with that row's own name.
        (
            "left_metric or right_metric",
            true,
            vec![(Some("shared"), Some("left_metric"), 2.0)],
        ),
        ("left_metric and on(__name__) right_metric", true, vec![]),
        (
            "left_metric unless on(__name__) right_metric",
            true,
            vec![(Some("shared"), Some("left_metric"), 2.0)],
        ),
        ("left_metric unless right_metric", true, vec![]),
    ] {
        let plan = PromPlanner::stmt_to_plan(
            binary_metric_name_table_provider(),
            &operator_eval_stmt(query),
            &state,
        )
        .await
        .unwrap_or_else(|err| panic!("{query}: {err}"));
        let marker = PromPlanner::metric_name_column(plan.schema()).unwrap();
        assert_eq!(
            marker.as_ref().map(|marker| marker.name.as_str()),
            keeps_marker.then_some(PROMQL_METRIC_NAME_COLUMN),
            "{query}: {}",
            plan.display_indent()
        );

        let sample_column = float_sample_column(&plan);
        let (_, batches) = execute(plan, &state).await;
        if !batches.is_empty() {
            if keeps_marker {
                assert_metric_name_in_batches(&batches, PROMQL_METRIC_NAME_COLUMN);
            } else {
                assert_metric_name_not_in_batches(&batches);
            }
        }
        assert_eq!(
            binary_result_rows(
                &batches,
                marker.as_ref().map(|marker| marker.name.as_str()),
                "tag",
                &sample_column,
            ),
            expected_binary_result_rows(&expected),
            "{query}"
        );
    }
}

/// A private physical column that already occupies the internal marker name is an ordinary label:
/// the identity is attached under the next free name, `on(__name__)` compares that identity and
/// never the physical column, and the set and binary operators keep the two apart.
#[tokio::test]
async fn test_binary_metric_name_private_collision() {
    let state = build_query_engine_state();
    let collision_marker = format!("{PROMQL_METRIC_NAME_COLUMN}_");
    // The collision metric holds three series at `ts=1000` with the sample value 1.0 each, so
    // `sum by (__name__)` holds 3.0.
    for (query, keeps_marker, expected) in [
        (
            "cv_metric + cv_metric",
            false,
            vec![
                (Some("k0"), None, 2.0),
                (Some("k1"), None, 2.0),
                (Some("k2"), None, 2.0),
            ],
        ),
        (
            "abs(cv_metric) or cv_metric",
            true,
            vec![
                (Some("k0"), None, 1.0),
                (Some("k1"), None, 1.0),
                (Some("k2"), None, 1.0),
            ],
        ),
    ] {
        let plan = PromPlanner::stmt_to_plan(
            build_count_values_collision_table_provider(PROMQL_METRIC_NAME_COLUMN).await,
            &operator_eval_stmt(query),
            &state,
        )
        .await
        .unwrap_or_else(|err| panic!("{query}: {err}"));
        let marker = PromPlanner::metric_name_column(plan.schema()).unwrap();
        assert_eq!(
            marker.as_ref().map(|marker| marker.name.as_str()),
            keeps_marker.then_some(collision_marker.as_str()),
            "{query}: {}",
            plan.display_indent()
        );

        let sample_column = float_sample_column(&plan);
        let (_, batches) = execute(plan, &state).await;
        if !batches.is_empty() {
            if keeps_marker {
                assert_metric_name_in_batches(&batches, &collision_marker);
            } else {
                // The fixture's ordinary label is named like the marker itself, so the role, not
                // the name, tells whether a result field is the identity.
                for batch in &batches {
                    let schema = batch.schema();
                    assert!(
                        schema.field_with_name(&collision_marker).is_err(),
                        "the metric-name marker must not reach the result batches: {schema:?}"
                    );
                    assert!(
                        schema
                            .fields()
                            .iter()
                            .all(|field| field.metadata().get(PROMQL_FIELD_ROLE_KEY).is_none()),
                        "no result field may carry the metric-name role: {schema:?}"
                    );
                }
            }
        }
        assert_eq!(
            binary_result_rows(
                &batches,
                marker.as_ref().map(|marker| marker.name.as_str()),
                PROMQL_METRIC_NAME_COLUMN,
                &sample_column,
            ),
            expected_binary_result_rows(&expected),
            "{query}"
        );
    }

    // `on(__name__)` resolves to the marked identity, never to the physical private column.
    let plan = PromPlanner::stmt_to_plan(
        build_count_values_collision_table_provider(PROMQL_METRIC_NAME_COLUMN).await,
        &operator_eval_stmt("cv_metric == on(__name__) cv_metric"),
        &state,
    )
    .await
    .unwrap();
    let plan_str = plan.display_indent_schema().to_string();
    let join = plan_str
        .lines()
        .find(|line| line.contains("Inner Join:"))
        .unwrap_or_else(|| panic!("{plan_str}"));
    assert!(
        join.contains("__promql_metric_name_ ="),
        "`on(__name__)` must compare the marked identity: {plan_str}"
    );
    assert!(
        !join.contains("__promql_metric_name ="),
        "`on(__name__)` must not compare the physical private label: {plan_str}"
    );
}

#[tokio::test]
async fn test_name_only_vector_matching_does_not_broadcast() {
    let state = build_query_engine_state();

    for (query, expected_tag, expected_name, expected_value) in [
        (
            "sum by (__name__) (left_metric) < on(__name__) sum by (__name__) (right_metric)",
            None,
            None,
            None,
        ),
        (
            "sum by (__name__) (left_metric) <= on(__name__) sum by (__name__) (left_metric)",
            None,
            Some("left_metric"),
            Some(2.0),
        ),
        (
            "left_metric < on() group_left(__name__) sum by (__name__) (right_metric)",
            Some("shared"),
            Some("right_metric"),
            Some(2.0),
        ),
        (
            "left_metric < on() group_left(__name__) sum(right_metric)",
            Some("shared"),
            None,
            Some(2.0),
        ),
    ] {
        let plan = PromPlanner::stmt_to_plan(
            binary_metric_name_table_provider(),
            &operator_eval_stmt(query),
            &state,
        )
        .await
        .unwrap_or_else(|err| panic!("{query}: {err}"));
        let marker = PromPlanner::metric_name_column(plan.schema()).unwrap();
        let sample = float_sample_column(&plan);
        let (_, batches) = execute(plan, &state).await;
        let rows = binary_result_rows(
            &batches,
            marker.as_ref().map(|marker| marker.name.as_str()),
            "tag",
            &sample,
        );
        match expected_value {
            Some(value) => {
                assert_eq!(
                    rows,
                    expected_binary_result_rows(&[(expected_tag, expected_name, value)]),
                    "{query}"
                );
            }
            None => assert!(rows.is_empty(), "{query}: {rows:?}"),
        }
    }
}

/// `or` keeps every chosen row's own identity, including the typed NULL of an operand that
/// dropped its name, and never invents an identity when neither operand has one.
#[tokio::test]
async fn test_or_preserves_metric_name_identity() {
    let state = build_query_engine_state();
    for (query, keeps_marker, expected) in [
        (
            "lf or rf",
            true,
            vec![(Some("a"), Some("lf"), 2.0), (Some("b"), Some("rf"), 3.0)],
        ),
        (
            "abs(lf) or rf",
            true,
            vec![(Some("a"), None, 2.0), (Some("b"), Some("rf"), 3.0)],
        ),
        (
            "abs(lf) or abs(rf)",
            false,
            vec![(Some("a"), None, 2.0), (Some("b"), None, 3.0)],
        ),
    ] {
        let plan = PromPlanner::stmt_to_plan(
            operator_table_provider(),
            &operator_eval_stmt(query),
            &state,
        )
        .await
        .unwrap_or_else(|err| panic!("{query}: {err}"));
        let marker = PromPlanner::metric_name_column(plan.schema()).unwrap();
        assert_eq!(
            marker.as_ref().map(|marker| marker.name.as_str()),
            keeps_marker.then_some(PROMQL_METRIC_NAME_COLUMN),
            "{query}: {}",
            plan.display_indent()
        );

        let sample_column = float_sample_column(&plan);
        let (_, batches) = execute(plan, &state).await;
        if keeps_marker {
            assert_metric_name_in_batches(&batches, PROMQL_METRIC_NAME_COLUMN);
        } else {
            assert_metric_name_not_in_batches(&batches);
        }
        assert_eq!(
            binary_result_rows(
                &batches,
                marker.as_ref().map(|marker| marker.name.as_str()),
                "tag",
                &sample_column,
            ),
            expected_binary_result_rows(&expected),
            "{query}"
        );
    }
}

/// `(series tag, value)` pairs of the string column `column` of `batches`, sorted by series
/// tag; a NULL value is `None`. The row order of a plan is not stable, so pairs are used
/// whenever the identity of a row matters.
fn tagged_string_rows(batches: &[RecordBatch], column: &str) -> Vec<(String, Option<String>)> {
    let mut rows = batches
        .iter()
        .flat_map(|batch| {
            let tag = batch
                .column_by_name("k")
                .expect("no series tag column")
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("the series tag must be a string column");
            let values = batch
                .column_by_name(column)
                .unwrap_or_else(|| panic!("no column {column} in {}", batch.schema()))
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap_or_else(|| panic!("column {column} is not a string column"));
            (0..batch.num_rows())
                .map(|row| {
                    (
                        tag.value(row).to_string(),
                        (!values.is_null(row)).then(|| values.value(row).to_string()),
                    )
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| left.0.cmp(&right.0));
    rows
}

/// Every value of the string column `column` of `batches`, row by row: `None` is a NULL.
fn string_values(batches: &[RecordBatch], column: &str) -> Vec<Option<String>> {
    batches
        .iter()
        .flat_map(|batch| {
            let values = batch
                .column_by_name(column)
                .unwrap_or_else(|| panic!("no column {column} in {}", batch.schema()))
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap_or_else(|| panic!("column {column} is not a string column"));
            (0..batch.num_rows())
                .map(|row| (!values.is_null(row)).then(|| values.value(row).to_string()))
                .collect::<Vec<_>>()
        })
        .collect()
}

/// The semantic `__name__` is a source for ordinary labels: it resolves through the marked
/// identity of the input, never through the sample or a physical column of that name, and the
/// identity itself stays attached to the result.
#[tokio::test]
async fn test_label_functions_semantic_name_source_into_ordinary_label() {
    let state = build_query_engine_state();
    // (query, expected values of the new ordinary label, one per series)
    for (query, expected) in [
        (
            r#"label_replace(cv_metric, "name_copy", "$1", "__name__", "(.*)")"#,
            vec!["cv_metric", "cv_metric", "cv_metric"],
        ),
        (
            r#"label_join(cv_metric, "name_copy", "/", "__name__", "missing_label")"#,
            vec!["cv_metric/", "cv_metric/", "cv_metric/"],
        ),
    ] {
        let plan = PromPlanner::stmt_to_plan(
            build_count_values_table_provider().await,
            &operator_eval_stmt(query),
            &state,
        )
        .await
        .unwrap_or_else(|err| panic!("{query}: {err}"));

        let marker = PromPlanner::metric_name_column(plan.schema())
            .unwrap()
            .unwrap_or_else(|| panic!("{query}: the identity must survive"));
        assert_eq!(marker.name, PROMQL_METRIC_NAME_COLUMN, "{query}");

        let (_, batches) = execute(plan, &state).await;
        assert_metric_name_in_batches(&batches, &marker.name);
        assert_eq!(
            string_values(&batches, "name_copy"),
            expected
                .iter()
                .map(|value| Some(value.to_string()))
                .collect::<Vec<_>>(),
            "{query}"
        );
        assert_eq!(
            cv_rows(&batches, greptime_value()),
            vec![
                ("k0".to_string(), 1_000, 1.0),
                ("k1".to_string(), 1_000, 2.0),
                ("k2".to_string(), 1_000, 1.0),
            ],
            "{query}: the samples must be untouched"
        );
    }
}

/// The semantic `__name__` is a destination: it overwrites the marked identity of the input in
/// place. A matching regex writes the replacement, a non-match keeps the identity of the input,
/// and a missing source label is the empty string the regex is matched against. The samples keep
/// their series and their values either way.
#[tokio::test]
async fn test_label_functions_semantic_name_destination_overwrites_identity() {
    let state = build_query_engine_state();
    // (query, expected `(series tag, identity, sample value)` rows)
    for (query, expected) in [
        // Matches one series only.
        (
            r#"label_replace(cv_metric, "__name__", "renamed", "k", "k1")"#,
            vec![
                ("k0", "cv_metric", 1.0),
                ("k1", "renamed", 2.0),
                ("k2", "cv_metric", 1.0),
            ],
        ),
        // Matches nothing: the input keeps its identity.
        (
            r#"label_replace(cv_metric, "__name__", "renamed", "k", "nomatch")"#,
            vec![
                ("k0", "cv_metric", 1.0),
                ("k1", "cv_metric", 2.0),
                ("k2", "cv_metric", 1.0),
            ],
        ),
        // The replacement expands the captured value.
        (
            r#"label_replace(cv_metric, "__name__", "renamed_$1", "k", "k(.)")"#,
            vec![
                ("k0", "renamed_0", 1.0),
                ("k1", "renamed_1", 2.0),
                ("k2", "renamed_2", 1.0),
            ],
        ),
        // A missing source label is the empty string, so a regex matching it replaces the name.
        (
            r#"label_replace(cv_metric, "__name__", "empty_source", "missing_label", "()")"#,
            vec![
                ("k0", "empty_source", 1.0),
                ("k1", "empty_source", 2.0),
                ("k2", "empty_source", 1.0),
            ],
        ),
        // ... and a regex that does not match it leaves the identity alone.
        (
            r#"label_replace(cv_metric, "__name__", "empty_source", "missing_label", "nomatch")"#,
            vec![
                ("k0", "cv_metric", 1.0),
                ("k1", "cv_metric", 2.0),
                ("k2", "cv_metric", 1.0),
            ],
        ),
        // `label_join` writes the joined label, and a missing component keeps its separator.
        (
            r#"label_join(cv_metric, "__name__", "/", "k", "missing_label")"#,
            vec![("k0", "k0/", 1.0), ("k1", "k1/", 2.0), ("k2", "k2/", 1.0)],
        ),
        // The identity is a source of the joined label as well.
        (
            r#"label_join(cv_metric, "__name__", "/", "__name__", "k")"#,
            vec![
                ("k0", "cv_metric/k0", 1.0),
                ("k1", "cv_metric/k1", 2.0),
                ("k2", "cv_metric/k2", 1.0),
            ],
        ),
    ] {
        let plan = PromPlanner::stmt_to_plan(
            build_count_values_table_provider().await,
            &operator_eval_stmt(query),
            &state,
        )
        .await
        .unwrap_or_else(|err| panic!("{query}: {err}"));

        let marker = PromPlanner::metric_name_column(plan.schema())
            .unwrap()
            .unwrap_or_else(|| panic!("{query}: the destination must be marked"));
        assert_eq!(marker.name, PROMQL_METRIC_NAME_COLUMN, "{query}");

        let (_, batches) = execute(plan, &state).await;
        assert_metric_name_in_batches(&batches, &marker.name);
        assert_eq!(
            metric_name_rows(&batches, &marker.name),
            expected
                .iter()
                .map(|(tag, name, value)| (tag.to_string(), name.to_string(), *value))
                .collect::<Vec<_>>(),
            "{query}"
        );
    }
}

/// An input without a marked identity gets a fresh internal identity column: the replacement
/// writes it on a match, and a non-matching regex leaves it unset - a typed NULL - instead of
/// manufacturing the replacement.
#[tokio::test]
async fn test_label_functions_semantic_name_destination_without_input_identity() {
    let state = build_query_engine_state();
    // (query, expected identity values, `None` for an unset identity)
    for (query, expected) in [
        (
            r#"label_replace(abs(cv_metric), "__name__", "renamed_$1", "k", "(.*)")"#,
            vec![Some("renamed_k0"), Some("renamed_k1"), Some("renamed_k2")],
        ),
        (
            r#"label_replace(abs(cv_metric), "__name__", "renamed", "k", "nomatch")"#,
            vec![None, None, None],
        ),
        // The semantic source of an input without an identity is the empty string.
        (
            r#"label_replace(abs(cv_metric), "__name__", "was_empty", "__name__", "()")"#,
            vec![Some("was_empty"), Some("was_empty"), Some("was_empty")],
        ),
        (
            r#"label_replace(abs(cv_metric), "__name__", "was_empty", "__name__", "nomatch")"#,
            vec![None, None, None],
        ),
        (
            r#"label_join(abs(cv_metric), "__name__", "/", "__name__", "k")"#,
            vec![Some("/k0"), Some("/k1"), Some("/k2")],
        ),
    ] {
        let plan = PromPlanner::stmt_to_plan(
            build_count_values_table_provider().await,
            &operator_eval_stmt(query),
            &state,
        )
        .await
        .unwrap_or_else(|err| panic!("{query}: {err}"));

        let marker = PromPlanner::metric_name_column(plan.schema())
            .unwrap()
            .unwrap_or_else(|| panic!("{query}: the destination must be marked"));
        assert_eq!(marker.name, PROMQL_METRIC_NAME_COLUMN, "{query}");
        // The input functions rename the sample column after themselves.
        let sample_column = float_sample_column(&plan);

        let (_, batches) = execute(plan, &state).await;
        assert_metric_name_in_batches(&batches, &marker.name);
        assert_eq!(
            tagged_string_rows(&batches, &marker.name),
            expected
                .iter()
                .enumerate()
                .map(|(index, value)| (format!("k{index}"), value.map(|value| value.to_string())))
                .collect::<Vec<_>>(),
            "{query}"
        );
        assert_eq!(
            cv_rows(&batches, &sample_column),
            vec![
                ("k0".to_string(), 1_000, 1.0),
                ("k1".to_string(), 1_000, 2.0),
                ("k2".to_string(), 1_000, 1.0),
            ],
            "{query}: the samples must be untouched"
        );
    }
}

/// A physical label named `__name__` is an ordinary label, not the semantic metric name: the
/// semantic source resolves through the marked identity instead, and the semantic destination
/// overwrites the identity, while the physical label keeps its values and its unmarked metadata.
#[tokio::test]
async fn test_label_functions_semantic_name_keeps_physical_name_label() {
    let state = build_query_engine_state();
    // (query, ordinary destination to check with its expected values, expected identity values)
    for (query, ordinary, expected_identity) in [
        (
            // The semantic source is the identity, never the physical `__name__` label.
            r#"label_replace(cv_metric, "name_copy", "$1", "__name__", "(.*)")"#,
            Some(("name_copy", vec!["cv_metric", "cv_metric", "cv_metric"])),
            vec!["cv_metric", "cv_metric", "cv_metric"],
        ),
        (
            // ... and the semantic destination replaces the identity (matching regex) ...
            r#"label_replace(cv_metric, "__name__", "renamed", "__name__", "(.*)")"#,
            None,
            vec!["renamed", "renamed", "renamed"],
        ),
        (
            // ... or keeps it (non-matching regex), including for `label_join`.
            r#"label_join(cv_metric, "__name__", "/", "__name__", "missing_label")"#,
            None,
            vec!["cv_metric/", "cv_metric/", "cv_metric/"],
        ),
    ] {
        let plan = PromPlanner::stmt_to_plan(
            // The metric carries a physical label named `__name__`.
            build_count_values_collision_table_provider(METRIC_NAME).await,
            &operator_eval_stmt(query),
            &state,
        )
        .await
        .unwrap_or_else(|err| panic!("{query}: {err}"));

        let marker = PromPlanner::metric_name_column(plan.schema())
            .unwrap()
            .unwrap_or_else(|| panic!("{query}: the identity must be attached"));
        assert_eq!(marker.name, PROMQL_METRIC_NAME_COLUMN, "{query}");
        let physical = plan
            .schema()
            .field_with_unqualified_name(METRIC_NAME)
            .unwrap_or_else(|_| panic!("{query}: the physical `__name__` label must survive"));
        assert!(
            physical.metadata().get(PROMQL_FIELD_ROLE_KEY).is_none(),
            "{query}: the physical `__name__` label must stay unmarked",
        );

        let (_, batches) = execute(plan, &state).await;
        assert_metric_name_in_batches(&batches, &marker.name);
        // Every row keeps the physical label and the sample value of its series.
        let mut rows = batches
            .iter()
            .flat_map(|batch| {
                let physical = batch
                    .column_by_name(METRIC_NAME)
                    .expect("the physical `__name__` label must be projected")
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("the physical `__name__` label must be a string column");
                let identity = batch
                    .column_by_name(&marker.name)
                    .expect("the identity must be projected")
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("the identity must be a string column");
                let value = batch
                    .column_by_name(greptime_value())
                    .expect("the sample value must be projected")
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .expect("the sample value must be a float column");
                (0..batch.num_rows())
                    .map(|row| {
                        (
                            physical.value(row).to_string(),
                            identity.value(row).to_string(),
                            value.value(row),
                        )
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| left.0.cmp(&right.0));
        assert_eq!(
            rows,
            ["k0", "k1", "k2"]
                .iter()
                .enumerate()
                .map(|(index, label)| (
                    label.to_string(),
                    expected_identity[index].to_string(),
                    1.0
                ))
                .collect::<Vec<_>>(),
            "{query}"
        );
        if let Some((column, expected)) = ordinary {
            assert_eq!(
                string_values(&batches, column),
                expected
                    .iter()
                    .map(|value| Some(value.to_string()))
                    .collect::<Vec<_>>(),
                "{query}"
            );
        }
    }
}

/// An input whose physical columns occupy the internal identity name gets a suffixed internal
/// identity column: the physical column keeps its name, its values and its unmarked metadata.
#[tokio::test]
async fn test_label_functions_semantic_name_destination_avoids_physical_collision() {
    let state = build_query_engine_state();
    let query =
        r#"label_replace(abs(cv_metric), "__name__", "renamed", "__promql_metric_name", "(.*)")"#;
    let plan = PromPlanner::stmt_to_plan(
        // The physical label occupies the internal identity name.
        build_count_values_collision_table_provider(PROMQL_METRIC_NAME_COLUMN).await,
        &operator_eval_stmt(query),
        &state,
    )
    .await
    .unwrap_or_else(|err| panic!("{query}: {err}"));

    let marker = PromPlanner::metric_name_column(plan.schema())
        .unwrap()
        .unwrap_or_else(|| panic!("{query}: the destination must be marked"));
    assert_eq!(
        marker.name,
        format!("{PROMQL_METRIC_NAME_COLUMN}_"),
        "{query}: {}",
        plan.display_indent()
    );
    let physical = plan
        .schema()
        .field_with_unqualified_name(PROMQL_METRIC_NAME_COLUMN)
        .unwrap_or_else(|_| panic!("{query}: the physical column must survive"));
    assert!(
        physical.metadata().get(PROMQL_FIELD_ROLE_KEY).is_none(),
        "{query}: the physical column must stay unmarked",
    );

    let (_, batches) = execute(plan, &state).await;
    assert_metric_name_in_batches(&batches, &marker.name);
    assert_eq!(
        string_values(&batches, &marker.name),
        vec![
            Some("renamed".to_string()),
            Some("renamed".to_string()),
            Some("renamed".to_string()),
        ],
        "{query}"
    );
    let mut physical_values = string_values(&batches, PROMQL_METRIC_NAME_COLUMN);
    physical_values.sort();
    assert_eq!(
        physical_values,
        vec![
            Some("k0".to_string()),
            Some("k1".to_string()),
            Some("k2".to_string()),
        ],
        "{query}: the physical column must keep its values"
    );
}

/// An ordinary `label_replace`/`label_join` - neither source nor destination names `__name__` -
/// keeps the existing handling: the destination label receives `regexp_replace` output even
/// without a match, a missing component of a join is NULL and skipped, the historical shortcuts
/// stay in place, and an already present destination is still refused.
#[tokio::test]
async fn test_label_functions_ordinary_only_keeps_existing_behavior() {
    let state = build_query_engine_state();
    // (query, destination column, expected `(series tag, destination value)` rows)
    for (query, column, expected) in [
        // The anchored regex matches the captured series suffix.
        (
            r#"label_replace(cv_metric, "copy", "renamed_$1", "k", "k(.)")"#,
            "copy",
            vec![
                Some("renamed_0".to_string()),
                Some("renamed_1".to_string()),
                Some("renamed_2".to_string()),
            ],
        ),
        // The anchored regex matches nothing, so the destination keeps the source it replaced.
        (
            r#"label_replace(cv_metric, "copy", "renamed", "k", "nomatch")"#,
            "copy",
            vec![
                Some("k0".to_string()),
                Some("k1".to_string()),
                Some("k2".to_string()),
            ],
        ),
        // A missing source with a non-empty replacement keeps the existing shortcut.
        (
            r#"label_replace(cv_metric, "copy", "addressed", "missing_label", "nomatch")"#,
            "copy",
            vec![
                Some("addressed".to_string()),
                Some("addressed".to_string()),
                Some("addressed".to_string()),
            ],
        ),
        // A missing component of an ordinary join is skipped, so it contributes no separator.
        (
            r#"label_join(cv_metric, "joined", ",", "k", "missing_label")"#,
            "joined",
            vec![
                Some("k0".to_string()),
                Some("k1".to_string()),
                Some("k2".to_string()),
            ],
        ),
    ] {
        let plan = PromPlanner::stmt_to_plan(
            build_count_values_table_provider().await,
            &operator_eval_stmt(query),
            &state,
        )
        .await
        .unwrap_or_else(|err| panic!("{query}: {err}"));

        let (_, batches) = execute(plan, &state).await;
        assert_eq!(
            tagged_string_rows(&batches, column),
            expected
                .iter()
                .enumerate()
                .map(|(index, value)| (format!("k{index}"), value.clone()))
                .collect::<Vec<_>>(),
            "{query}"
        );
    }

    // A present source and an empty regex still add no destination label at all.
    let query = r#"label_replace(cv_metric, "copy", "renamed", "k", "")"#;
    let plan = PromPlanner::stmt_to_plan(
        build_count_values_table_provider().await,
        &operator_eval_stmt(query),
        &state,
    )
    .await
    .unwrap_or_else(|err| panic!("{query}: {err}"));
    assert!(
        plan.schema().field_with_unqualified_name("copy").is_err(),
        "{query}: {}",
        plan.display_indent()
    );
    let sample_column = float_sample_column(&plan);
    let (_, batches) = execute(plan, &state).await;
    assert_eq!(
        cv_rows(&batches, &sample_column),
        vec![
            ("k0".to_string(), 1_000, 1.0),
            ("k1".to_string(), 1_000, 2.0),
            ("k2".to_string(), 1_000, 1.0),
        ],
        "{query}"
    );

    // An already present destination is still refused on the ordinary path.
    let query = r#"label_replace(cv_metric, "k", "x", "k", "(.*)")"#;
    let err = PromPlanner::stmt_to_plan(
        build_count_values_table_provider().await,
        &operator_eval_stmt(query),
        &state,
    )
    .await
    .expect_err(query)
    .to_string();
    assert!(
        err.contains("labelset"),
        "{query}: expected the same-label-set refusal, got {err}"
    );
}

/// A semantic `__name__` replacement into an ordinary label leaves that label NULL on the rows
/// the regex does not match; a following function that uses such a label as its source reads it
/// as the empty string, and the samples and the identity of the input survive.
#[tokio::test]
async fn test_label_functions_nulled_ordinary_source_is_empty() {
    let state = build_query_engine_state();
    // The inner replacement matches nothing, so `extra` is NULL on every row of its result.
    let null_extra = r#"label_replace(cv_metric, "extra", "x", "__name__", "nomatch")"#;
    let plan = PromPlanner::stmt_to_plan(
        build_count_values_table_provider().await,
        &operator_eval_stmt(null_extra),
        &state,
    )
    .await
    .unwrap_or_else(|err| panic!("{null_extra}: {err}"));
    let marker = PromPlanner::metric_name_column(plan.schema())
        .unwrap()
        .expect("the input identity must survive");
    let (_, batches) = execute(plan, &state).await;
    assert_metric_name_in_batches(&batches, &marker.name);
    assert_eq!(
        tagged_string_rows(&batches, "extra"),
        vec![
            ("k0".to_string(), None),
            ("k1".to_string(), None),
            ("k2".to_string(), None),
        ],
        "{null_extra}"
    );

    // (query, expected `(series tag, semantic name)` rows)
    for (query, expected) in [
        // The NULL ordinary label is the empty string the semantic destination matches.
        (
            format!(r#"label_replace({null_extra}, "__name__", "renamed", "extra", "()")"#),
            vec![("k0", "renamed"), ("k1", "renamed"), ("k2", "renamed")],
        ),
        // The NULL ordinary label joins as the empty string, next to a series tag.
        (
            format!(r#"label_join({null_extra}, "__name__", "/", "extra", "k")"#),
            vec![("k0", "/k0"), ("k1", "/k1"), ("k2", "/k2")],
        ),
        // ... and next to the identity of the input, which the inner replacement kept.
        (
            format!(r#"label_join({null_extra}, "__name__", "/", "extra", "__name__")"#),
            vec![
                ("k0", "/cv_metric"),
                ("k1", "/cv_metric"),
                ("k2", "/cv_metric"),
            ],
        ),
    ] {
        let plan = PromPlanner::stmt_to_plan(
            build_count_values_table_provider().await,
            &operator_eval_stmt(&query),
            &state,
        )
        .await
        .unwrap_or_else(|err| panic!("{query}: {err}"));

        let marker = PromPlanner::metric_name_column(plan.schema())
            .unwrap()
            .expect("the destination must be marked");
        assert_eq!(marker.name, PROMQL_METRIC_NAME_COLUMN, "{query}");
        let sample_column = float_sample_column(&plan);
        let (_, batches) = execute(plan, &state).await;
        assert_metric_name_in_batches(&batches, &marker.name);
        assert_eq!(
            tagged_string_rows(&batches, &marker.name),
            expected
                .iter()
                .map(|(tag, value)| ((*tag).to_string(), Some((*value).to_string())))
                .collect::<Vec<_>>(),
            "{query}"
        );
        // The samples of every series are kept.
        assert_eq!(
            cv_rows(&batches, &sample_column),
            vec![
                ("k0".to_string(), 1_000, 1.0),
                ("k1".to_string(), 1_000, 2.0),
                ("k2".to_string(), 1_000, 1.0),
            ],
            "{query}"
        );
    }
}

/// A marked identity a prior function left NULL is the empty string when it is used as a source:
/// the regex is matched against it and it joins with the separator it contributes, while a
/// destination left unset still stays NULL.
#[tokio::test]
async fn test_label_functions_null_identity_source_is_empty() {
    let state = build_query_engine_state();
    // A `label_replace` that matches nothing leaves the freshly attached identity NULL on every
    // row instead of keeping it.
    let unset_identity = r#"label_replace(abs(cv_metric), "__name__", "x", "k", "nomatch")"#;
    // (query, destination column, expected `(series tag, destination value)` rows)
    for (query, column, expected) in [
        (
            format!(r#"label_replace({unset_identity}, "copy", "[$1]", "__name__", "(.*)")"#),
            "copy",
            vec![("k0", Some("[]")), ("k1", Some("[]")), ("k2", Some("[]"))],
        ),
        (
            format!(r#"label_join({unset_identity}, "joined", ",", "__name__", "k")"#),
            "joined",
            vec![
                ("k0", Some(",k0")),
                ("k1", Some(",k1")),
                ("k2", Some(",k2")),
            ],
        ),
        (
            format!(r#"label_replace({unset_identity}, "copy", "z", "__name__", "nomatch")"#),
            "copy",
            vec![("k0", None), ("k1", None), ("k2", None)],
        ),
    ] {
        let plan = PromPlanner::stmt_to_plan(
            build_count_values_table_provider().await,
            &operator_eval_stmt(&query),
            &state,
        )
        .await
        .unwrap_or_else(|err| panic!("{query}: {err}"));

        let (_, batches) = execute(plan, &state).await;
        assert_eq!(
            tagged_string_rows(&batches, column),
            expected
                .iter()
                .map(|(tag, value)| ((*tag).to_string(), value.map(str::to_string)))
                .collect::<Vec<_>>(),
            "{query}"
        );
    }
}

/// An empty regex is anchored like any other: it matches the empty source - a label the input
/// does not carry, or a marked identity a prior function left NULL - and nothing else.
#[tokio::test]
async fn test_label_replace_empty_regex_matches_empty_source() {
    let state = build_query_engine_state();
    // (query, expected `(series tag, identity value)` rows, `None` for an unset identity)
    for (query, expected) in [
        // The input carries no identity at all, and none of the labels the source could name.
        (
            r#"label_replace(abs(cv_metric), "__name__", "renamed", "__name__", "")"#,
            vec![
                ("k0", Some("renamed")),
                ("k1", Some("renamed")),
                ("k2", Some("renamed")),
            ],
        ),
        (
            r#"label_replace(abs(cv_metric), "__name__", "renamed", "missing_label", "")"#,
            vec![
                ("k0", Some("renamed")),
                ("k1", Some("renamed")),
                ("k2", Some("renamed")),
            ],
        ),
        // A marked identity a prior function left NULL is the empty string, so it matches ...
        (
            r#"label_replace(label_replace(abs(cv_metric), "__name__", "x", "k", "nomatch"), "__name__", "renamed", "__name__", "")"#,
            vec![
                ("k0", Some("renamed")),
                ("k1", Some("renamed")),
                ("k2", Some("renamed")),
            ],
        ),
        // ... a non-matching regex leaves the destination unset ...
        (
            r#"label_replace(abs(cv_metric), "__name__", "renamed", "missing_label", "nomatch")"#,
            vec![("k0", None), ("k1", None), ("k2", None)],
        ),
        // ... and an identity the input does carry is not empty, so it is kept.
        (
            r#"label_replace(cv_metric, "__name__", "renamed", "__name__", "")"#,
            vec![
                ("k0", Some("cv_metric")),
                ("k1", Some("cv_metric")),
                ("k2", Some("cv_metric")),
            ],
        ),
    ] {
        let plan = PromPlanner::stmt_to_plan(
            build_count_values_table_provider().await,
            &operator_eval_stmt(query),
            &state,
        )
        .await
        .unwrap_or_else(|err| panic!("{query}: {err}"));

        let marker = PromPlanner::metric_name_column(plan.schema())
            .unwrap()
            .unwrap_or_else(|| panic!("{query}: the destination must be marked"));
        assert_eq!(marker.name, PROMQL_METRIC_NAME_COLUMN, "{query}");

        let (_, batches) = execute(plan, &state).await;
        assert_metric_name_in_batches(&batches, &marker.name);
        assert_eq!(
            tagged_string_rows(&batches, &marker.name),
            expected
                .iter()
                .map(|(tag, value)| ((*tag).to_string(), value.map(str::to_string)))
                .collect::<Vec<_>>(),
            "{query}"
        );
    }
}

/// Replacing the semantic name into a label the input already carries overwrites that label in
/// place: a matching regex writes the replacement without projecting the label twice, and a
/// non-matching one keeps the label and the samples of every series.
#[tokio::test]
async fn test_label_replace_semantic_source_into_existing_ordinary_label() {
    let state = build_query_engine_state();
    // (query, expected `k` values, one per series)
    for (query, expected_k) in [
        (
            r#"label_replace(cv_metric, "k", "$1", "__name__", "(.*)")"#,
            vec!["cv_metric", "cv_metric", "cv_metric"],
        ),
        (
            r#"label_replace(cv_metric, "k", "renamed_$1", "__name__", "nomatch(.*)")"#,
            vec!["k0", "k1", "k2"],
        ),
    ] {
        let plan = PromPlanner::stmt_to_plan(
            build_count_values_table_provider().await,
            &operator_eval_stmt(query),
            &state,
        )
        .await
        .unwrap_or_else(|err| panic!("{query}: {err}"));

        // The replaced label keeps its single projected column.
        assert_eq!(
            plan.schema()
                .fields()
                .iter()
                .filter(|field| field.name() == "k")
                .count(),
            1,
            "{query}: {}",
            plan.display_indent()
        );
        let marker = PromPlanner::metric_name_column(plan.schema())
            .unwrap()
            .expect("the input identity must survive");
        let sample_column = float_sample_column(&plan);

        let (_, batches) = execute(plan, &state).await;
        assert_metric_name_in_batches(&batches, &marker.name);
        let mut labels = string_values(&batches, "k");
        labels.sort();
        assert_eq!(
            labels,
            expected_k
                .iter()
                .map(|value| Some((*value).to_string()))
                .collect::<Vec<_>>(),
            "{query}"
        );
        let mut names = string_values(&batches, &marker.name);
        names.sort();
        assert_eq!(names, vec![Some("cv_metric".to_string()); 3], "{query}");
        let mut samples = values(&batches, &sample_column);
        samples.sort_by(f64::total_cmp);
        assert_eq!(samples, vec![1.0, 1.0, 2.0], "{query}");
    }
}

/// The ordinary `label_replace` destination keeps its historical value when the anchored regex
/// does not match: the un-replaced source label, not a NULL. This is the pre-existing behavior
/// of the function on its ordinary path.
#[tokio::test]
async fn test_label_replace_ordinary_destination_keeps_existing_value() {
    let state = build_query_engine_state();
    // (query, expected `(series tag, destination value)` rows)
    for (query, expected) in [
        (
            r#"label_replace(cv_metric, "copy", "renamed_$1", "k", "k(.)")"#,
            vec![
                Some("renamed_0".to_string()),
                Some("renamed_1".to_string()),
                Some("renamed_2".to_string()),
            ],
        ),
        // No match: the destination holds the source value the replacement did not change.
        (
            r#"label_replace(cv_metric, "copy", "renamed", "k", "nomatch")"#,
            vec![
                Some("k0".to_string()),
                Some("k1".to_string()),
                Some("k2".to_string()),
            ],
        ),
        // A source the input does not carry keeps the historical shortcut.
        (
            r#"label_replace(cv_metric, "copy", "addressed", "missing_label", "nomatch")"#,
            vec![
                Some("addressed".to_string()),
                Some("addressed".to_string()),
                Some("addressed".to_string()),
            ],
        ),
    ] {
        let plan = PromPlanner::stmt_to_plan(
            build_count_values_table_provider().await,
            &operator_eval_stmt(query),
            &state,
        )
        .await
        .unwrap_or_else(|err| panic!("{query}: {err}"));

        let (_, batches) = execute(plan, &state).await;
        assert_eq!(
            tagged_string_rows(&batches, "copy"),
            expected
                .iter()
                .enumerate()
                .map(|(index, value)| (format!("k{index}"), value.clone()))
                .collect::<Vec<_>>(),
            "{query}"
        );
    }

    // An empty replacement with a missing source keeps everything unchanged, so no destination
    // label exists at all and the samples stay untouched.
    let query = r#"label_replace(cv_metric, "copy", "", "missing_label", "nomatch")"#;
    let plan = PromPlanner::stmt_to_plan(
        build_count_values_table_provider().await,
        &operator_eval_stmt(query),
        &state,
    )
    .await
    .unwrap_or_else(|err| panic!("{query}: {err}"));
    assert!(
        plan.schema().field_with_unqualified_name("copy").is_err(),
        "{query}: {}",
        plan.display_indent()
    );
    let sample_column = float_sample_column(&plan);
    let (_, batches) = execute(plan, &state).await;
    assert_eq!(
        cv_rows(&batches, &sample_column),
        vec![
            ("k0".to_string(), 1_000, 1.0),
            ("k1".to_string(), 1_000, 2.0),
            ("k2".to_string(), 1_000, 1.0),
        ],
        "{query}"
    );
}
