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

use std::time::{Duration, UNIX_EPOCH};

use catalog::RegisterTableRequest;
use catalog::memory::{MemoryCatalogManager, new_memory_catalog_manager};
use common_base::Plugins;
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_query::native_histogram::{
    CUSTOM_BUCKETS_SCHEMA, CounterResetHint, NativeHistogram, build_histogram_array,
};
use common_query::prelude::{greptime_native_histogram, greptime_timestamp, greptime_value};
use common_query::prometheus::PROMETHEUS_STALE_NAN_BITS;
use common_query::test_util::DummyDecoder;
use common_recordbatch::RecordBatch as GreptimeRecordBatch;
use datafusion::arrow::array::{
    Array, Float64Array, Int64Array, StringArray, TimestampMillisecondArray,
};
use datafusion::arrow::datatypes::{Field, Schema as ArrowSchema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::datasource::{MemTable, provider_as_source};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::Extension;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::{ColumnSchema, Schema};
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

fn assert_normal_metric_schema(plan: &LogicalPlan) {
    let fields = plan.schema().fields();
    assert_eq!(fields.len(), 4, "{fields:?}");
    assert!(
        fields.iter().any(|field| field.name() == "pod"),
        "{fields:?}"
    );
    assert!(
        fields.iter().any(|field| field.name() == "instance"),
        "{fields:?}"
    );
    assert!(
        fields
            .iter()
            .any(|field| field.name() == greptime_timestamp()),
        "{fields:?}"
    );
    assert!(
        fields.iter().any(|field| {
            field.name() == greptime_value() && field.data_type() == &ArrowDataType::Float64
        }),
        "{fields:?}"
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
        ColumnSchema::new(
            "tag".to_string(),
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
        "Filter: TEMPLATE(field_0) IS NOT NULL [timestamp:Timestamp(ms), TEMPLATE(field_0):Float64;N, tag_0:Utf8]\
            \n  Projection: some_metric.timestamp, TEMPLATE(some_metric.field_0) AS TEMPLATE(field_0), some_metric.tag_0 [timestamp:Timestamp(ms), TEMPLATE(field_0):Float64;N, tag_0:Utf8]\
            \n    PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n      PromSeriesDivide: tags=[\"tag_0\"] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n        Sort: some_metric.tag_0 ASC NULLS FIRST, some_metric.timestamp ASC NULLS FIRST [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
	            \n          Filter: some_metric.tag_0 != Utf8(\"bar\") AND some_metric.timestamp >= TimestampMillisecond(-999, None) AND some_metric.timestamp <= TimestampMillisecond(100000000, None) [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n            TableScan: some_metric [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]"
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
            \n      PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, __promql_timestamp_value_:Float64]\
            \n        Projection: some_metric.tag_0, some_metric.timestamp, some_metric.field_0, CAST(CAST(CAST(CAST(some_metric.timestamp AS Int64) AS Decimal128(19, 0)) * Decimal128(1,1,0) + Decimal128(0,19,0) AS Int64) AS Float64) / Float64(1000) AS __promql_timestamp_value_ [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, __promql_timestamp_value_:Float64]\
            \n          PromSeriesDivide: tags=[\"tag_0\"] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n            Sort: some_metric.tag_0 ASC NULLS FIRST, some_metric.timestamp ASC NULLS FIRST [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n              Filter: some_metric.tag_0 != Utf8(\"bar\") AND some_metric.timestamp >= TimestampMillisecond(-999, None) AND some_metric.timestamp <= TimestampMillisecond(100000000, None) [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n                TableScan: some_metric [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]",
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
            \n    PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, tag_1:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, field_1:Float64;N]\
            \n      PromSeriesDivide: tags=[\"tag_0\", \"tag_1\"] [tag_0:Utf8, tag_1:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, field_1:Float64;N]\
            \n        Sort: some_metric.tag_0 ASC NULLS FIRST, some_metric.tag_1 ASC NULLS FIRST, some_metric.timestamp ASC NULLS FIRST [tag_0:Utf8, tag_1:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, field_1:Float64;N]\
            \n          Filter: some_metric.tag_0 != Utf8(\"bar\") AND some_metric.timestamp >= TimestampMillisecond(-999, None) AND some_metric.timestamp <= TimestampMillisecond(100000000, None) [tag_0:Utf8, tag_1:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, field_1:Float64;N]\
            \n            TableScan: some_metric [tag_0:Utf8, tag_1:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, field_1:Float64;N]"
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
            \n    PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, tag_1:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, field_1:Float64;N]\
            \n      PromSeriesDivide: tags=[\"tag_0\", \"tag_1\"] [tag_0:Utf8, tag_1:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, field_1:Float64;N]\
            \n        Sort: some_metric.tag_0 ASC NULLS FIRST, some_metric.tag_1 ASC NULLS FIRST, some_metric.timestamp ASC NULLS FIRST [tag_0:Utf8, tag_1:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, field_1:Float64;N]\
            \n          Filter: some_metric.tag_0 != Utf8(\"bar\") AND some_metric.timestamp >= TimestampMillisecond(-999, None) AND some_metric.timestamp <= TimestampMillisecond(100000000, None) [tag_0:Utf8, tag_1:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, field_1:Float64;N]\
            \n            TableScan: some_metric [tag_0:Utf8, tag_1:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, field_1:Float64;N]"
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
        "Projection: rhs.tag_0, rhs.timestamp, CAST(lhs.field_0 AS Float64) + CAST(rhs.field_0 AS Float64) AS lhs.field_0 + rhs.field_0 [tag_0:Utf8, timestamp:Timestamp(ms), lhs.field_0 + rhs.field_0:Float64;N]\
            \n  Inner Join: lhs.tag_0 = rhs.tag_0, lhs.timestamp = rhs.timestamp [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n    SubqueryAlias: lhs [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n      PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n        PromSeriesDivide: tags=[\"tag_0\"] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n          Sort: some_metric.tag_0 ASC NULLS FIRST, some_metric.timestamp ASC NULLS FIRST [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n            Filter: some_metric.tag_0 = Utf8(\"foo\") AND some_metric.tag_0 = Utf8(\"bar\") AND some_metric.timestamp >= TimestampMillisecond(-999, None) AND some_metric.timestamp <= TimestampMillisecond(100000000, None) [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n              TableScan: some_metric [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n    SubqueryAlias: rhs [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n      PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n        PromSeriesDivide: tags=[\"tag_0\"] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n          Sort: some_metric.tag_0 ASC NULLS FIRST, some_metric.timestamp ASC NULLS FIRST [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n            Filter: some_metric.tag_0 = Utf8(\"bar\") AND some_metric.tag_0 = Utf8(\"foo\") AND some_metric.timestamp >= TimestampMillisecond(-999, None) AND some_metric.timestamp <= TimestampMillisecond(100000000, None) [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n              TableScan: some_metric [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]",
    );

    assert_eq!(plan.display_indent_schema().to_string(), expected);
}

async fn indie_query_plan_compare<T: AsRef<str>>(query: &str, expected: T) {
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

    assert_eq!(plan.display_indent_schema().to_string(), expected.as_ref());
}

#[tokio::test]
async fn binary_op_literal_column() {
    let query = r#"1 + some_metric{tag_0="bar"}"#;
    let expected = String::from(
        "Projection: some_metric.tag_0, some_metric.timestamp, Float64(1) + CAST(some_metric.field_0 AS Float64) AS Float64(1) + field_0 [tag_0:Utf8, timestamp:Timestamp(ms), Float64(1) + field_0:Float64;N]\
            \n  PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n    PromSeriesDivide: tags=[\"tag_0\"] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n      Sort: some_metric.tag_0 ASC NULLS FIRST, some_metric.timestamp ASC NULLS FIRST [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n        Filter: some_metric.tag_0 = Utf8(\"bar\") AND some_metric.timestamp >= TimestampMillisecond(-999, None) AND some_metric.timestamp <= TimestampMillisecond(100000000, None) [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n          TableScan: some_metric [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]",
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
        "Projection: some_metric.tag_0, some_metric.timestamp, CAST(some_metric.field_0 != Float64(1.2345) AS Float64) AS field_0 != Float64(1.2345) [tag_0:Utf8, timestamp:Timestamp(ms), field_0 != Float64(1.2345):Float64;N]\
            \n  PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n    PromSeriesDivide: tags=[\"tag_0\"] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n      Sort: some_metric.tag_0 ASC NULLS FIRST, some_metric.timestamp ASC NULLS FIRST [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n        Filter: some_metric.timestamp >= TimestampMillisecond(-999, None) AND some_metric.timestamp <= TimestampMillisecond(100000000, None) [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n          TableScan: some_metric [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]",
    );

    indie_query_plan_compare(query, expected).await;
}

#[tokio::test]
async fn bool_with_additional_arithmetic() {
    let query = "some_metric + (1 == bool 2)";
    let expected = String::from(
        "Projection: some_metric.tag_0, some_metric.timestamp, CAST(some_metric.field_0 AS Float64) + CAST(Float64(1) = Float64(2) AS Float64) AS field_0 + Float64(1) = Float64(2) [tag_0:Utf8, timestamp:Timestamp(ms), field_0 + Float64(1) = Float64(2):Float64;N]\
            \n  PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n    PromSeriesDivide: tags=[\"tag_0\"] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n      Sort: some_metric.tag_0 ASC NULLS FIRST, some_metric.timestamp ASC NULLS FIRST [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n        Filter: some_metric.timestamp >= TimestampMillisecond(-999, None) AND some_metric.timestamp <= TimestampMillisecond(100000000, None) [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n          TableScan: some_metric [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]",
    );

    indie_query_plan_compare(query, expected).await;
}

#[tokio::test]
async fn simple_unary() {
    let query = "-some_metric";
    let expected = String::from(
        "Projection: some_metric.tag_0, some_metric.timestamp, (- some_metric.field_0) AS (- field_0) [tag_0:Utf8, timestamp:Timestamp(ms), (- field_0):Float64;N]\
            \n  PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n    PromSeriesDivide: tags=[\"tag_0\"] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n      Sort: some_metric.tag_0 ASC NULLS FIRST, some_metric.timestamp ASC NULLS FIRST [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n        Filter: some_metric.timestamp >= TimestampMillisecond(-999, None) AND some_metric.timestamp <= TimestampMillisecond(100000000, None) [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n          TableScan: some_metric [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]",
    );

    indie_query_plan_compare(query, expected).await;
}

#[tokio::test]
async fn increase_aggr() {
    let query = "increase(some_metric[5m])";
    let expected = String::from(
        "Filter: prom_increase(timestamp_range,field_0,timestamp,Int64(300000)) IS NOT NULL [timestamp:Timestamp(ms), prom_increase(timestamp_range,field_0,timestamp,Int64(300000)):Float64;N, tag_0:Utf8]\
            \n  Projection: some_metric.timestamp, prom_increase(timestamp_range, field_0, some_metric.timestamp, Int64(300000)) AS prom_increase(timestamp_range,field_0,timestamp,Int64(300000)), some_metric.tag_0 [timestamp:Timestamp(ms), prom_increase(timestamp_range,field_0,timestamp,Int64(300000)):Float64;N, tag_0:Utf8]\
            \n    PromRangeManipulate: req range=[0..100000000], interval=[5000], eval range=[300000], time index=[timestamp], values=[\"field_0\"] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Dictionary(Int64, Float64);N, timestamp_range:Dictionary(Int64, Timestamp(ms))]\
            \n      PromSeriesNormalize: offset=[0], time index=[timestamp], filter NaN: [true] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n        PromSeriesDivide: tags=[\"tag_0\"] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n          Sort: some_metric.tag_0 ASC NULLS FIRST, some_metric.timestamp ASC NULLS FIRST [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n            Filter: some_metric.timestamp >= TimestampMillisecond(-299999, None) AND some_metric.timestamp <= TimestampMillisecond(100000000, None) [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n              TableScan: some_metric [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]",
    );

    indie_query_plan_compare(query, expected).await;
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
                    .column_by_name("tag")
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
          Filter: greptime_value IS NOT NULL OR greptime_native_histogram IS NOT NULL [timestamp:Timestamp(ms), greptime_value:Float64;N, greptime_native_histogram:Struct("schema": Int32, "zero_threshold": Float64, "sum": Float64, "reset_hint": Int32, "start_timestamp": Timestamp(ms), "custom_values": List(Float64), "positive_span_offsets": List(Int32), "positive_span_lengths": List(Int32), "negative_span_offsets": List(Int32), "negative_span_lengths": List(Int32), "count_i64": Int64, "zero_count_i64": Int64, "positive_buckets_i64": List(Int64), "negative_buckets_i64": List(Int64), "count_f64": Float64, "zero_count_f64": Float64, "positive_buckets_f64": List(Float64), "negative_buckets_f64": List(Float64));N, tag_0:Utf8]
            Projection: some_metric.timestamp, prom_mixed_range_float(Utf8("rate"), timestamp_range, greptime_value, greptime_native_histogram, some_metric.timestamp, Int64(300000)) AS greptime_value, prom_mixed_range_histogram(Utf8("rate"), timestamp_range, greptime_value, greptime_native_histogram, some_metric.timestamp, Int64(300000)) AS greptime_native_histogram, some_metric.tag_0 [timestamp:Timestamp(ms), greptime_value:Float64;N, greptime_native_histogram:Struct("schema": Int32, "zero_threshold": Float64, "sum": Float64, "reset_hint": Int32, "start_timestamp": Timestamp(ms), "custom_values": List(Float64), "positive_span_offsets": List(Int32), "positive_span_lengths": List(Int32), "negative_span_offsets": List(Int32), "negative_span_lengths": List(Int32), "count_i64": Int64, "zero_count_i64": Int64, "positive_buckets_i64": List(Int64), "negative_buckets_i64": List(Int64), "count_f64": Float64, "zero_count_f64": Float64, "positive_buckets_f64": List(Float64), "negative_buckets_f64": List(Float64));N, tag_0:Utf8]
              PromRangeManipulate: req range=[-540000..100000000], interval=[60000], eval range=[300000], time index=[timestamp], values=["greptime_native_histogram", "greptime_value"] [tag_0:Utf8, timestamp:Timestamp(ms), greptime_native_histogram:Dictionary(Int64, Struct("schema": Int32, "zero_threshold": Float64, "sum": Float64, "reset_hint": Int32, "start_timestamp": Timestamp(ms), "custom_values": List(Float64), "positive_span_offsets": List(Int32), "positive_span_lengths": List(Int32), "negative_span_offsets": List(Int32), "negative_span_lengths": List(Int32), "count_i64": Int64, "zero_count_i64": Int64, "positive_buckets_i64": List(Int64), "negative_buckets_i64": List(Int64), "count_f64": Float64, "zero_count_f64": Float64, "positive_buckets_f64": List(Float64), "negative_buckets_f64": List(Float64)));N, greptime_value:Dictionary(Int64, Float64);N, timestamp_range:Dictionary(Int64, Timestamp(ms))]
                PromSeriesNormalize: offset=[0], time index=[timestamp], filter NaN: [true] [tag_0:Utf8, timestamp:Timestamp(ms), greptime_native_histogram:Struct("schema": Int32, "zero_threshold": Float64, "sum": Float64, "reset_hint": Int32, "start_timestamp": Timestamp(ms), "custom_values": List(Float64), "positive_span_offsets": List(Int32), "positive_span_lengths": List(Int32), "negative_span_offsets": List(Int32), "negative_span_lengths": List(Int32), "count_i64": Int64, "zero_count_i64": Int64, "positive_buckets_i64": List(Int64), "negative_buckets_i64": List(Int64), "count_f64": Float64, "zero_count_f64": Float64, "positive_buckets_f64": List(Float64), "negative_buckets_f64": List(Float64));N, greptime_value:Float64;N]
                  PromSeriesDivide: tags=["tag_0"] [tag_0:Utf8, timestamp:Timestamp(ms), greptime_native_histogram:Struct("schema": Int32, "zero_threshold": Float64, "sum": Float64, "reset_hint": Int32, "start_timestamp": Timestamp(ms), "custom_values": List(Float64), "positive_span_offsets": List(Int32), "positive_span_lengths": List(Int32), "negative_span_offsets": List(Int32), "negative_span_lengths": List(Int32), "count_i64": Int64, "zero_count_i64": Int64, "positive_buckets_i64": List(Int64), "negative_buckets_i64": List(Int64), "count_f64": Float64, "zero_count_f64": Float64, "positive_buckets_f64": List(Float64), "negative_buckets_f64": List(Float64));N, greptime_value:Float64;N]
                    Sort: some_metric.tag_0 ASC NULLS FIRST, some_metric.timestamp ASC NULLS FIRST [tag_0:Utf8, timestamp:Timestamp(ms), greptime_native_histogram:Struct("schema": Int32, "zero_threshold": Float64, "sum": Float64, "reset_hint": Int32, "start_timestamp": Timestamp(ms), "custom_values": List(Float64), "positive_span_offsets": List(Int32), "positive_span_lengths": List(Int32), "negative_span_offsets": List(Int32), "negative_span_lengths": List(Int32), "count_i64": Int64, "zero_count_i64": Int64, "positive_buckets_i64": List(Int64), "negative_buckets_i64": List(Int64), "count_f64": Float64, "zero_count_f64": Float64, "positive_buckets_f64": List(Float64), "negative_buckets_f64": List(Float64));N, greptime_value:Float64;N]
                      Filter: some_metric.timestamp >= TimestampMillisecond(-839999, None) AND some_metric.timestamp <= TimestampMillisecond(100000000, None) [tag_0:Utf8, timestamp:Timestamp(ms), greptime_native_histogram:Struct("schema": Int32, "zero_threshold": Float64, "sum": Float64, "reset_hint": Int32, "start_timestamp": Timestamp(ms), "custom_values": List(Float64), "positive_span_offsets": List(Int32), "positive_span_lengths": List(Int32), "negative_span_offsets": List(Int32), "negative_span_lengths": List(Int32), "count_i64": Int64, "zero_count_i64": Int64, "positive_buckets_i64": List(Int64), "negative_buckets_i64": List(Int64), "count_f64": Float64, "zero_count_f64": Float64, "positive_buckets_f64": List(Float64), "negative_buckets_f64": List(Float64));N, greptime_value:Float64;N]
                        TableScan: some_metric [tag_0:Utf8, timestamp:Timestamp(ms), greptime_native_histogram:Struct("schema": Int32, "zero_threshold": Float64, "sum": Float64, "reset_hint": Int32, "start_timestamp": Timestamp(ms), "custom_values": List(Float64), "positive_span_offsets": List(Int32), "positive_span_lengths": List(Int32), "negative_span_offsets": List(Int32), "negative_span_lengths": List(Int32), "count_i64": Int64, "zero_count_i64": Int64, "positive_buckets_i64": List(Int64), "negative_buckets_i64": List(Int64), "count_f64": Float64, "zero_count_f64": Float64, "positive_buckets_f64": List(Float64), "negative_buckets_f64": List(Float64));N, greptime_value:Float64;N]"#;
    assert_eq!(plan, expected);
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
        .create_function_expr(&call.func, vec![], input.schema(), &state)
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
        "Filter: some_metric.field_0 < Float64(1.2345) [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n  PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n    PromSeriesDivide: tags=[\"tag_0\"] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n      Sort: some_metric.tag_0 ASC NULLS FIRST, some_metric.timestamp ASC NULLS FIRST [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n        Filter: some_metric.timestamp >= TimestampMillisecond(-999, None) AND some_metric.timestamp <= TimestampMillisecond(100000000, None) [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n          TableScan: some_metric [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]",
    );

    indie_query_plan_compare(query, expected).await;
}

#[tokio::test]
async fn count_over_time() {
    let query = "count_over_time(some_metric[5m])";
    let expected = String::from(
        "Filter: prom_count_over_time(timestamp_range,field_0) IS NOT NULL [timestamp:Timestamp(ms), prom_count_over_time(timestamp_range,field_0):Float64;N, tag_0:Utf8]\
            \n  Projection: some_metric.timestamp, prom_count_over_time(timestamp_range, field_0) AS prom_count_over_time(timestamp_range,field_0), some_metric.tag_0 [timestamp:Timestamp(ms), prom_count_over_time(timestamp_range,field_0):Float64;N, tag_0:Utf8]\
            \n    PromRangeManipulate: req range=[0..100000000], interval=[5000], eval range=[300000], time index=[timestamp], values=[\"field_0\"] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Dictionary(Int64, Float64);N, timestamp_range:Dictionary(Int64, Timestamp(ms))]\
            \n      PromSeriesNormalize: offset=[0], time index=[timestamp], filter NaN: [true] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n        PromSeriesDivide: tags=[\"tag_0\"] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n          Sort: some_metric.tag_0 ASC NULLS FIRST, some_metric.timestamp ASC NULLS FIRST [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n            Filter: some_metric.timestamp >= TimestampMillisecond(-299999, None) AND some_metric.timestamp <= TimestampMillisecond(100000000, None) [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n              TableScan: some_metric [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]",
    );

    indie_query_plan_compare(query, expected).await;
}

/// The outer `PromRangeManipulate` from a subquery must be preceded by
/// `Sort` + `PromSeriesDivide`.
#[tokio::test]
async fn count_over_time_subquery() {
    let query = "count_over_time(some_metric[10m:1m])";
    let expected = String::from(
        "Filter: prom_count_over_time(timestamp_range,field_0) IS NOT NULL [timestamp:Timestamp(ms), prom_count_over_time(timestamp_range,field_0):Float64;N, tag_0:Utf8]\
            \n  Projection: some_metric.timestamp, prom_count_over_time(timestamp_range, field_0) AS prom_count_over_time(timestamp_range,field_0), some_metric.tag_0 [timestamp:Timestamp(ms), prom_count_over_time(timestamp_range,field_0):Float64;N, tag_0:Utf8]\
            \n    PromRangeManipulate: req range=[0..100000000], interval=[5000], eval range=[600000], time index=[timestamp], values=[\"field_0\"] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Dictionary(Int64, Float64);N, timestamp_range:Dictionary(Int64, Timestamp(ms))]\
            \n      PromSeriesDivide: tags=[\"tag_0\"] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n        Sort: some_metric.tag_0 ASC NULLS FIRST, some_metric.timestamp ASC NULLS FIRST [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n          PromInstantManipulate: range=[-540000..100000000], lookback=[1000], interval=[60000], time index=[timestamp] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n            PromSeriesDivide: tags=[\"tag_0\"] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n              Sort: some_metric.tag_0 ASC NULLS FIRST, some_metric.timestamp ASC NULLS FIRST [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n                Filter: some_metric.timestamp >= TimestampMillisecond(-540999, None) AND some_metric.timestamp <= TimestampMillisecond(100000000, None) [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n                  TableScan: some_metric [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]",
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
    let expected = "Projection: http_server_requests_seconds_sum.uri, http_server_requests_seconds_count.greptime_timestamp, CAST(http_server_requests_seconds_sum.greptime_value AS Float64) / CAST(http_server_requests_seconds_count.greptime_value AS Float64) AS http_server_requests_seconds_sum.greptime_value / http_server_requests_seconds_count.greptime_value\
            \n  Projection: http_server_requests_seconds_sum.uri, http_server_requests_seconds_sum.kubernetes_namespace, http_server_requests_seconds_sum.kubernetes_pod_name, http_server_requests_seconds_sum.greptime_timestamp, http_server_requests_seconds_sum.greptime_value, http_server_requests_seconds_count.uri, http_server_requests_seconds_count.kubernetes_namespace, http_server_requests_seconds_count.kubernetes_pod_name, http_server_requests_seconds_count.greptime_timestamp, http_server_requests_seconds_count.greptime_value\
            \n    Filter: prom_assert_unique_match_group(__promql_match_group_count, http_server_requests_seconds_sum.uri)\
            \n      WindowAggr: windowExpr=[[count(Int64(1)) PARTITION BY [http_server_requests_seconds_sum.uri, http_server_requests_seconds_sum.greptime_timestamp] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING AS __promql_match_group_count]]\
            \n        Inner Join: http_server_requests_seconds_sum.greptime_timestamp = http_server_requests_seconds_count.greptime_timestamp, http_server_requests_seconds_sum.uri = http_server_requests_seconds_count.uri\
            \n          SubqueryAlias: http_server_requests_seconds_sum\
            \n            PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[greptime_timestamp]\
            \n              PromSeriesDivide: tags=[\"uri\", \"kubernetes_namespace\", \"kubernetes_pod_name\"]\
            \n                Sort: http_server_requests_seconds_sum.uri ASC NULLS FIRST, http_server_requests_seconds_sum.kubernetes_namespace ASC NULLS FIRST, http_server_requests_seconds_sum.kubernetes_pod_name ASC NULLS FIRST, http_server_requests_seconds_sum.greptime_timestamp ASC NULLS FIRST\
            \n                  Filter: http_server_requests_seconds_sum.uri = Utf8(\"/accounts/login\") AND http_server_requests_seconds_sum.greptime_timestamp >= TimestampMillisecond(-999, None) AND http_server_requests_seconds_sum.greptime_timestamp <= TimestampMillisecond(100000000, None)\
            \n                    TableScan: http_server_requests_seconds_sum\
            \n          SubqueryAlias: http_server_requests_seconds_count\
            \n            Projection: http_server_requests_seconds_count.uri, http_server_requests_seconds_count.kubernetes_namespace, http_server_requests_seconds_count.kubernetes_pod_name, http_server_requests_seconds_count.greptime_timestamp, http_server_requests_seconds_count.greptime_value\
            \n              Filter: prom_assert_unique_match_group(__promql_match_group_count, http_server_requests_seconds_count.uri)\
            \n                WindowAggr: windowExpr=[[count(Int64(1)) PARTITION BY [http_server_requests_seconds_count.uri, http_server_requests_seconds_count.greptime_timestamp] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING AS __promql_match_group_count]]\
            \n                  PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[greptime_timestamp]\
            \n                    PromSeriesDivide: tags=[\"uri\", \"kubernetes_namespace\", \"kubernetes_pod_name\"]\
            \n                      Sort: http_server_requests_seconds_count.uri ASC NULLS FIRST, http_server_requests_seconds_count.kubernetes_namespace ASC NULLS FIRST, http_server_requests_seconds_count.kubernetes_pod_name ASC NULLS FIRST, http_server_requests_seconds_count.greptime_timestamp ASC NULLS FIRST\
            \n                        Filter: http_server_requests_seconds_count.uri = Utf8(\"/accounts/login\") AND http_server_requests_seconds_count.greptime_timestamp >= TimestampMillisecond(-999, None) AND http_server_requests_seconds_count.greptime_timestamp <= TimestampMillisecond(100000000, None)\
            \n                          TableScan: http_server_requests_seconds_count";
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
        let mut fields = plan.schema().field_names();
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
        "PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n  PromSeriesDivide: tags=[\"tag_0\"] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n    Sort: greptime_private.some_alt_metric.tag_0 ASC NULLS FIRST, greptime_private.some_alt_metric.timestamp ASC NULLS FIRST [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n      Filter: greptime_private.some_alt_metric.timestamp >= TimestampMillisecond(-999, None) AND greptime_private.some_alt_metric.timestamp <= TimestampMillisecond(100000000, None) [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n        TableScan: greptime_private.some_alt_metric [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]",
    );

    indie_query_plan_compare(query, expected).await;

    let query = "some_alt_metric{__database__=\"greptime_private\"}";
    let expected = String::from(
        "PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n  PromSeriesDivide: tags=[\"tag_0\"] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n    Sort: greptime_private.some_alt_metric.tag_0 ASC NULLS FIRST, greptime_private.some_alt_metric.timestamp ASC NULLS FIRST [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n      Filter: greptime_private.some_alt_metric.timestamp >= TimestampMillisecond(-999, None) AND greptime_private.some_alt_metric.timestamp <= TimestampMillisecond(100000000, None) [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n        TableScan: greptime_private.some_alt_metric [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]",
    );

    indie_query_plan_compare(query, expected).await;

    let query = "some_alt_metric{__schema__=\"greptime_private\"} / some_metric";
    let expected = String::from(
        "Projection: some_metric.tag_0, some_metric.timestamp, CAST(greptime_private.some_alt_metric.field_0 AS Float64) / CAST(some_metric.field_0 AS Float64) AS greptime_private.some_alt_metric.field_0 / some_metric.field_0 [tag_0:Utf8, timestamp:Timestamp(ms), greptime_private.some_alt_metric.field_0 / some_metric.field_0:Float64;N]\
            \n  Inner Join: greptime_private.some_alt_metric.tag_0 = some_metric.tag_0, greptime_private.some_alt_metric.timestamp = some_metric.timestamp [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n    SubqueryAlias: greptime_private.some_alt_metric [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n      PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n        PromSeriesDivide: tags=[\"tag_0\"] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n          Sort: greptime_private.some_alt_metric.tag_0 ASC NULLS FIRST, greptime_private.some_alt_metric.timestamp ASC NULLS FIRST [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n            Filter: greptime_private.some_alt_metric.timestamp >= TimestampMillisecond(-999, None) AND greptime_private.some_alt_metric.timestamp <= TimestampMillisecond(100000000, None) [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n              TableScan: greptime_private.some_alt_metric [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n    SubqueryAlias: some_metric [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n      PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n        PromSeriesDivide: tags=[\"tag_0\"] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n          Sort: some_metric.tag_0 ASC NULLS FIRST, some_metric.timestamp ASC NULLS FIRST [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n            Filter: some_metric.timestamp >= TimestampMillisecond(-999, None) AND some_metric.timestamp <= TimestampMillisecond(100000000, None) [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n              TableScan: some_metric [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]",
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
        "PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [field:Float64;N, tag:Utf8, timestamp:Timestamp(ms)]\n  PromSeriesDivide: tags=[\"tag\"] [field:Float64;N, tag:Utf8, timestamp:Timestamp(ns)]\n    Sort: metrics.tag ASC NULLS FIRST, metrics.timestamp ASC NULLS FIRST [field:Float64;N, tag:Utf8, timestamp:Timestamp(ns)]\n      Filter: metrics.tag = Utf8(\"1\") AND metrics.timestamp > TimestampNanosecond(-1000000000, None) AND metrics.timestamp <= TimestampNanosecond(100000000000000, None) [field:Float64;N, tag:Utf8, timestamp:Timestamp(ns)]\n        Projection: metrics.field, metrics.tag, metrics.timestamp [field:Float64;N, tag:Utf8, timestamp:Timestamp(ns)]\n          TableScan: metrics [tag:Utf8, timestamp:Timestamp(ns), field:Float64;N]"
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
        "Filter: prom_avg_over_time(timestamp_range,field) IS NOT NULL [timestamp:Timestamp(ms), prom_avg_over_time(timestamp_range,field):Float64;N, tag:Utf8]\n  Projection: metrics.timestamp, prom_avg_over_time(timestamp_range, field) AS prom_avg_over_time(timestamp_range,field), metrics.tag [timestamp:Timestamp(ms), prom_avg_over_time(timestamp_range,field):Float64;N, tag:Utf8]\n    PromRangeManipulate: req range=[0..100000000], interval=[5000], eval range=[5000], time index=[timestamp], values=[\"field\"] [field:Dictionary(Int64, Float64);N, tag:Utf8, timestamp:Timestamp(ms), timestamp_range:Dictionary(Int64, Timestamp(ms))]\n      PromSeriesNormalize: offset=[0], time index=[timestamp], filter NaN: [true] [field:Float64;N, tag:Utf8, timestamp:Timestamp(ns)]\n        PromSeriesDivide: tags=[\"tag\"] [field:Float64;N, tag:Utf8, timestamp:Timestamp(ns)]\n          Sort: metrics.tag ASC NULLS FIRST, metrics.timestamp ASC NULLS FIRST [field:Float64;N, tag:Utf8, timestamp:Timestamp(ns)]\n            Filter: metrics.tag = Utf8(\"1\") AND metrics.timestamp > TimestampNanosecond(-5000000000, None) AND metrics.timestamp <= TimestampNanosecond(100000000000000, None) [field:Float64;N, tag:Utf8, timestamp:Timestamp(ns)]\n              Projection: metrics.field, metrics.tag, metrics.timestamp [field:Float64;N, tag:Utf8, timestamp:Timestamp(ns)]\n                TableScan: metrics [tag:Utf8, timestamp:Timestamp(ns), field:Float64;N]"
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
Filter: up.field_0 IS NOT NULL [timestamp:Timestamp(ms), field_0:Float64;N, foo:Utf8;N, tag_0:Utf8, tag_1:Utf8, tag_2:Utf8, tag_3:Utf8]
  Projection: up.timestamp, up.field_0, concat_ws(Utf8(","), up.tag_1, up.tag_2, up.tag_3) AS foo, up.tag_0, up.tag_1, up.tag_2, up.tag_3 [timestamp:Timestamp(ms), field_0:Float64;N, foo:Utf8;N, tag_0:Utf8, tag_1:Utf8, tag_2:Utf8, tag_3:Utf8]
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
Filter: up.field_0 IS NOT NULL [timestamp:Timestamp(ms), field_0:Float64;N, foo:Utf8;N, tag_0:Utf8]
  Projection: up.timestamp, up.field_0, regexp_replace(up.tag_0, Utf8("^(?s:(.*):.*)$"), Utf8("$1")) AS foo, up.tag_0 [timestamp:Timestamp(ms), field_0:Float64;N, foo:Utf8;N, tag_0:Utf8]
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
        \n    PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, tag_1:Utf8, tag_2:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, field_1:Float64;N, field_2:Float64;N]\
        \n      PromSeriesDivide: tags=[\"tag_0\", \"tag_1\", \"tag_2\"] [tag_0:Utf8, tag_1:Utf8, tag_2:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, field_1:Float64;N, field_2:Float64;N]\
        \n        Sort: prometheus_tsdb_head_series.tag_0 ASC NULLS FIRST, prometheus_tsdb_head_series.tag_1 ASC NULLS FIRST, prometheus_tsdb_head_series.tag_2 ASC NULLS FIRST, prometheus_tsdb_head_series.timestamp ASC NULLS FIRST [tag_0:Utf8, tag_1:Utf8, tag_2:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, field_1:Float64;N, field_2:Float64;N]\
        \n          Filter: prometheus_tsdb_head_series.tag_1 ~ Utf8(\"^(?:(10.0.160.237:8080|10.0.160.237:9090))$\") AND prometheus_tsdb_head_series.timestamp >= TimestampMillisecond(-999, None) AND prometheus_tsdb_head_series.timestamp <= TimestampMillisecond(100000000, None) [tag_0:Utf8, tag_1:Utf8, tag_2:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, field_1:Float64;N, field_2:Float64;N]\
        \n            TableScan: prometheus_tsdb_head_series [tag_0:Utf8, tag_1:Utf8, tag_2:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, field_1:Float64;N, field_2:Float64;N]";
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
        \n            PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[greptime_timestamp] [ip:Utf8, greptime_timestamp:Timestamp(ms), greptime_value:Float64;N]\
        \n              PromSeriesDivide: tags=[\"ip\"] [ip:Utf8, greptime_timestamp:Timestamp(ms), greptime_value:Float64;N]\
        \n                Sort: prometheus_tsdb_head_series.ip ASC NULLS FIRST, prometheus_tsdb_head_series.greptime_timestamp ASC NULLS FIRST [ip:Utf8, greptime_timestamp:Timestamp(ms), greptime_value:Float64;N]\
        \n                  Filter: prometheus_tsdb_head_series.ip ~ Utf8(\"^(?:(10.0.160.237:8080|10.0.160.237:9090))$\") AND prometheus_tsdb_head_series.greptime_timestamp >= TimestampMillisecond(-999, None) AND prometheus_tsdb_head_series.greptime_timestamp <= TimestampMillisecond(100000000, None) [ip:Utf8, greptime_timestamp:Timestamp(ms), greptime_value:Float64;N]\
        \n                    TableScan: prometheus_tsdb_head_series [ip:Utf8, greptime_timestamp:Timestamp(ms), greptime_value:Float64;N]";

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
    let expected = "Sort: prometheus_tsdb_head_series.ip ASC NULLS LAST, prometheus_tsdb_head_series.greptime_timestamp ASC NULLS LAST, series ASC NULLS LAST [count(prometheus_tsdb_head_series.greptime_value):Int64, ip:Utf8, greptime_timestamp:Timestamp(ms), series:Float64;N]\
        \n  Projection: count(prometheus_tsdb_head_series.greptime_value), prometheus_tsdb_head_series.ip, prometheus_tsdb_head_series.greptime_timestamp, prometheus_tsdb_head_series.greptime_value AS series [count(prometheus_tsdb_head_series.greptime_value):Int64, ip:Utf8, greptime_timestamp:Timestamp(ms), series:Float64;N]\
        \n    Aggregate: groupBy=[[prometheus_tsdb_head_series.ip, prometheus_tsdb_head_series.greptime_timestamp, prometheus_tsdb_head_series.greptime_value]], aggr=[[count(prometheus_tsdb_head_series.greptime_value)]] [ip:Utf8, greptime_timestamp:Timestamp(ms), greptime_value:Float64;N, count(prometheus_tsdb_head_series.greptime_value):Int64]\
        \n      PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[greptime_timestamp] [ip:Utf8, greptime_timestamp:Timestamp(ms), greptime_value:Float64;N]\
        \n        PromSeriesDivide: tags=[\"ip\"] [ip:Utf8, greptime_timestamp:Timestamp(ms), greptime_value:Float64;N]\
        \n          Sort: prometheus_tsdb_head_series.ip ASC NULLS FIRST, prometheus_tsdb_head_series.greptime_timestamp ASC NULLS FIRST [ip:Utf8, greptime_timestamp:Timestamp(ms), greptime_value:Float64;N]\
        \n            Filter: prometheus_tsdb_head_series.ip ~ Utf8(\"^(?:(10.0.160.237:8080|10.0.160.237:9090))$\") AND prometheus_tsdb_head_series.greptime_timestamp >= TimestampMillisecond(-999, None) AND prometheus_tsdb_head_series.greptime_timestamp <= TimestampMillisecond(100000000, None) [ip:Utf8, greptime_timestamp:Timestamp(ms), greptime_value:Float64;N]\
        \n              TableScan: prometheus_tsdb_head_series [ip:Utf8, greptime_timestamp:Timestamp(ms), greptime_value:Float64;N]";

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
Projection: count(prometheus_tsdb_head_series.greptime_value) AS my_series, prometheus_tsdb_head_series.ip, prometheus_tsdb_head_series.greptime_timestamp [my_series:Int64, ip:Utf8, greptime_timestamp:Timestamp(ms)]
  Sort: prometheus_tsdb_head_series.ip ASC NULLS LAST, prometheus_tsdb_head_series.greptime_timestamp ASC NULLS LAST, series ASC NULLS LAST [count(prometheus_tsdb_head_series.greptime_value):Int64, ip:Utf8, greptime_timestamp:Timestamp(ms), series:Float64;N]
    Projection: count(prometheus_tsdb_head_series.greptime_value), prometheus_tsdb_head_series.ip, prometheus_tsdb_head_series.greptime_timestamp, prometheus_tsdb_head_series.greptime_value AS series [count(prometheus_tsdb_head_series.greptime_value):Int64, ip:Utf8, greptime_timestamp:Timestamp(ms), series:Float64;N]
      Aggregate: groupBy=[[prometheus_tsdb_head_series.ip, prometheus_tsdb_head_series.greptime_timestamp, prometheus_tsdb_head_series.greptime_value]], aggr=[[count(prometheus_tsdb_head_series.greptime_value)]] [ip:Utf8, greptime_timestamp:Timestamp(ms), greptime_value:Float64;N, count(prometheus_tsdb_head_series.greptime_value):Int64]
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
        \n        PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[greptime_timestamp] [ip:Utf8, greptime_timestamp:Timestamp(ms), greptime_value:Float64;N]\
        \n          PromSeriesDivide: tags=[\"ip\"] [ip:Utf8, greptime_timestamp:Timestamp(ms), greptime_value:Float64;N]\
        \n            Sort: prometheus_tsdb_head_series.ip ASC NULLS FIRST, prometheus_tsdb_head_series.greptime_timestamp ASC NULLS FIRST [ip:Utf8, greptime_timestamp:Timestamp(ms), greptime_value:Float64;N]\
        \n              Filter: prometheus_tsdb_head_series.ip ~ Utf8(\"^(?:(10.0.160.237:8080|10.0.160.237:9090))$\") AND prometheus_tsdb_head_series.greptime_timestamp >= TimestampMillisecond(-999, None) AND prometheus_tsdb_head_series.greptime_timestamp <= TimestampMillisecond(100000000, None) [ip:Utf8, greptime_timestamp:Timestamp(ms), greptime_value:Float64;N]\
        \n                TableScan: prometheus_tsdb_head_series [ip:Utf8, greptime_timestamp:Timestamp(ms), greptime_value:Float64;N]";

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
    assert!(
        raw.display_indent_schema()
            .to_string()
            .contains("__promql_or_match_0@")
    );
    let (optimized, batches) = execute(raw, &state).await;
    assert_no_internal_or_keys(optimized.schema());
    assert!(batches.iter().all(|batch| {
        batch
            .schema()
            .fields()
            .iter()
            .all(|field| !field.name().starts_with("__promql_or_match_"))
    }));
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
    assert!(
        raw.display_indent_schema()
            .to_string()
            .contains("__promql_or_match_1@")
    );
    let (_, batches) = execute(raw, &build_query_engine_state()).await;
    assert!(
        batches
            .iter()
            .all(|batch| batch.column_by_name(USER_TAG).is_some())
    );
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
    assert!(matches!(&plan, LogicalPlan::EmptyRelation(_)), "{plan:?}");
    assert!(!plan.schema().fields().is_empty());
    assert!(!contains_histogram_fold(&plan), "{plan:?}");
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
            .create_function_expr(&call.func, vec![], input.schema(), &state)
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
        .create_function_expr(&call.func, args.literals, input.schema(), &state)
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
