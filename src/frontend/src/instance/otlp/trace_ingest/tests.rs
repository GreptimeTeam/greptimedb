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

use std::collections::{HashMap, HashSet};

use api::v1::column_data_type_extension::TypeExt;
use api::v1::value::ValueData;
use api::v1::{
    ColumnDataType, ColumnDataTypeExtension, ColumnSchema, JsonTypeExtension, Row,
    RowInsertRequest, Rows, SemanticType, Value,
};
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::{
    ColumnSchema as DatatypesColumnSchema, SchemaBuilder as DatatypesSchemaBuilder,
};
use opentelemetry_proto::tonic::common::v1::any_value::Value as OtlpValue;
use opentelemetry_proto::tonic::common::v1::{AnyValue, ArrayValue, KeyValue};
use servers::otlp::trace::SERVICE_NAME_COLUMN;
use servers::otlp::trace::attributes::Attributes;
use servers::otlp::trace::span::{SpanEvents, SpanLinks, TraceSpan};
use servers::otlp::trace::v1::{TraceBinaryType, TraceRetryColumn};
use servers::query_handler::TraceIngestOutcome;

use super::{
    ChunkFailureReaction, Instance, TraceChunkRetry, TraceChunkSchemaState, TraceRequestSchema,
    TraceRequestSchemaPlan, TraceSpanMetadata, TraceTablePreAlter, chunk_owned,
    wrap_trace_alter_failure,
};
use crate::metrics::OTLP_TRACES_FAILURE_COUNT;

#[test]
fn test_chunk_owned() {
    let chunks = chunk_owned(vec![1, 2, 3], 2);
    assert_eq!(chunks.iter().map(Vec::len).collect::<Vec<_>>(), vec![2, 1]);

    let chunks = chunk_owned(vec![1, 2, 3], 0);
    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0].len(), 3);

    assert!(chunk_owned::<i32>(Vec::new(), 0).is_empty());
}

#[test]
fn test_classify_trace_chunk_failure() {
    assert_eq!(
        Instance::classify_trace_chunk_failure(StatusCode::InvalidArguments),
        ChunkFailureReaction::RetryPerSpan
    );
    assert_eq!(
        Instance::classify_trace_chunk_failure(StatusCode::InvalidSyntax),
        ChunkFailureReaction::RetryPerSpan
    );
    assert_eq!(
        Instance::classify_trace_chunk_failure(StatusCode::Unsupported),
        ChunkFailureReaction::RetryPerSpan
    );
    assert_eq!(
        Instance::classify_trace_chunk_failure(StatusCode::TableColumnNotFound),
        ChunkFailureReaction::RetryPerSpan
    );
    assert_eq!(
        Instance::classify_trace_chunk_failure(StatusCode::TableNotFound),
        ChunkFailureReaction::RetryPerSpan
    );
    assert_eq!(
        Instance::classify_trace_chunk_failure(StatusCode::DatabaseNotFound),
        ChunkFailureReaction::DiscardChunk
    );
    assert_eq!(
        Instance::classify_trace_chunk_failure(StatusCode::DeadlineExceeded),
        ChunkFailureReaction::Propagate
    );
    assert_eq!(
        Instance::classify_trace_chunk_failure(StatusCode::Cancelled),
        ChunkFailureReaction::Propagate
    );
    assert_eq!(
        Instance::classify_trace_chunk_failure(StatusCode::StorageUnavailable),
        ChunkFailureReaction::Propagate
    );
    assert_eq!(
        Instance::classify_trace_chunk_failure(StatusCode::Internal),
        ChunkFailureReaction::Propagate
    );
    assert_eq!(
        Instance::classify_trace_chunk_failure(StatusCode::RegionNotReady),
        ChunkFailureReaction::Propagate
    );
    assert_eq!(
        Instance::classify_trace_chunk_failure(StatusCode::TableUnavailable),
        ChunkFailureReaction::Propagate
    );
    assert_eq!(
        Instance::classify_trace_chunk_failure(StatusCode::RegionBusy),
        ChunkFailureReaction::Propagate
    );
    assert_eq!(
        Instance::classify_trace_chunk_failure(StatusCode::RuntimeResourcesExhausted),
        ChunkFailureReaction::Propagate
    );
}

#[test]
fn test_classify_trace_span_failure() {
    assert!(Instance::should_propagate_trace_span_failure(
        StatusCode::DeadlineExceeded
    ));
    assert!(Instance::should_propagate_trace_span_failure(
        StatusCode::StorageUnavailable
    ));
    assert!(!Instance::should_propagate_trace_span_failure(
        StatusCode::InvalidArguments
    ));
}

#[test]
fn test_add_trace_write_cost() {
    let mut outcome = TraceIngestOutcome::default();
    Instance::add_trace_write_cost(&mut outcome, 3);
    Instance::add_trace_write_cost(&mut outcome, 5);
    assert_eq!(outcome.write_cost, 8);
}

#[test]
fn test_finish_trace_failure_message() {
    let message = Instance::finish_trace_failure_message(
        3,
        2,
        vec!["Rejected span trace:span (InvalidArguments)".to_string()],
    )
    .unwrap();
    assert!(message.contains("Accepted 3 spans, rejected 2 spans"));
    assert!(message.contains("Rejected span trace:span"));

    assert_eq!(Instance::finish_trace_failure_message(2, 0, vec![]), None);
}

#[test]
fn test_finish_trace_failure_message_without_detail_messages() {
    assert_eq!(
        Instance::finish_trace_failure_message(0, 2, vec![]),
        Some("Accepted 0 spans, rejected 2 spans".to_string())
    );
}

#[test]
fn test_push_trace_failure_message_increments_labeled_counter() {
    let label = "retry_per_span_counter_test";
    let initial = OTLP_TRACES_FAILURE_COUNT.with_label_values(&[label]).get();
    let mut messages = Vec::new();

    Instance::push_trace_failure_message(
        &mut messages,
        label,
        "Chunk fallback triggered by InvalidArguments".to_string(),
    );

    assert_eq!(messages.len(), 1);
    assert_eq!(
        OTLP_TRACES_FAILURE_COUNT.with_label_values(&[label]).get(),
        initial + 1
    );
}

#[test]
fn test_push_trace_failure_message_caps_recorded_messages() {
    let label = "retry_per_span_limit_test";
    let mut messages = Vec::new();

    for idx in 0..=4 {
        Instance::push_trace_failure_message(&mut messages, label, format!("failure-{idx}"));
    }

    assert_eq!(messages.len(), 4);
    assert_eq!(
        messages,
        vec![
            "failure-0".to_string(),
            "failure-1".to_string(),
            "failure-2".to_string(),
            "failure-3".to_string()
        ]
    );
}

#[test]
fn test_classify_trace_chunk_failure_defaults_to_discard() {
    assert_eq!(
        Instance::classify_trace_chunk_failure(StatusCode::Unknown),
        ChunkFailureReaction::DiscardChunk
    );
}

#[test]
fn test_wrap_trace_alter_failure_preserves_status_code() {
    let err = wrap_trace_alter_failure(
        servers::error::TableNotFoundSnafu {
            catalog: "greptime".to_string(),
            schema: "public".to_string(),
            table: "trace_type_missing".to_string(),
        }
        .build(),
    );

    assert_eq!(err.status_code(), StatusCode::TableNotFound);
}

fn field_schema(name: &str, datatype: ColumnDataType) -> ColumnSchema {
    ColumnSchema {
        column_name: name.to_string(),
        datatype: datatype as i32,
        semantic_type: SemanticType::Field as i32,
        ..Default::default()
    }
}

fn tag_schema(name: &str, datatype: ColumnDataType) -> ColumnSchema {
    ColumnSchema {
        semantic_type: SemanticType::Tag as i32,
        ..field_schema(name, datatype)
    }
}

fn json_field_schema(name: &str) -> ColumnSchema {
    ColumnSchema {
        datatype_extension: Some(ColumnDataTypeExtension {
            type_ext: Some(TypeExt::JsonType(JsonTypeExtension::JsonBinary.into())),
        }),
        ..field_schema(name, ColumnDataType::Binary)
    }
}

fn row(values: Vec<Option<ValueData>>) -> Row {
    Row {
        values: values
            .into_iter()
            .map(|value_data| Value { value_data })
            .collect(),
    }
}

fn row_insert_request(
    table_name: &str,
    schema: Vec<ColumnSchema>,
    rows: Vec<Row>,
) -> RowInsertRequest {
    RowInsertRequest {
        table_name: table_name.to_string(),
        rows: Some(Rows { schema, rows }),
    }
}

fn trace_attribute(key: &str, value: OtlpValue) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue { value: Some(value) }),
    }
}

fn trace_span(span_id: &str, span_attributes: Vec<KeyValue>) -> TraceSpan {
    TraceSpan {
        service_name: Some("service".to_string()),
        trace_id: "trace-id".to_string(),
        span_id: span_id.to_string(),
        parent_span_id: None,
        resource_attributes: Attributes::from(vec![]),
        scope_name: "scope".to_string(),
        scope_version: "v1".to_string(),
        scope_attributes: Attributes::from(vec![]),
        trace_state: String::new(),
        span_name: "operation".to_string(),
        span_kind: "SPAN_KIND_SERVER".to_string(),
        span_status_code: "STATUS_CODE_UNSET".to_string(),
        span_status_message: String::new(),
        span_attributes: Attributes::from(span_attributes),
        span_events: SpanEvents::from(vec![]),
        span_links: SpanLinks::from(vec![]),
        start_in_nanosecond: 1,
        end_in_nanosecond: 2,
    }
}

fn ready_plan(plan: TraceRequestSchemaPlan) -> TraceTablePreAlter {
    let TraceRequestSchemaPlan::Ready(plan) = plan else {
        panic!("expected a ready trace schema plan");
    };
    plan
}

fn prepare_and_apply(
    request_schema: &TraceRequestSchema,
    batch_index: usize,
    request: &mut RowInsertRequest,
) {
    if let Some(prepared) = request_schema
        .prepare_request_rewrite(batch_index, request)
        .unwrap()
    {
        TraceRequestSchema::apply_request_rewrite(request, prepared);
    }
}

#[test]
fn test_trace_chunk_retry_drops_absent_sparse_columns() {
    let schema = vec![
        field_schema("parent_span_id", ColumnDataType::String),
        field_schema("service_name", ColumnDataType::String),
        field_schema("span_attributes.absent", ColumnDataType::Int64),
        field_schema("scope_attributes.present", ColumnDataType::Boolean),
    ];
    let requests = row_insert_request(
        "traces",
        schema,
        vec![
            row(vec![None, None, None, Some(ValueData::BoolValue(true))]),
            row(vec![
                None,
                Some(ValueData::StringValue("service".to_string())),
                Some(ValueData::I64Value(1)),
                None,
            ]),
        ],
    );
    let retry = TraceChunkRetry::try_new(
        requests.table_name,
        requests.rows.unwrap(),
        ["span-1", "span-2"]
            .into_iter()
            .map(|span_id| TraceSpanMetadata {
                trace_id: "trace-id".to_string(),
                span_id: span_id.to_string(),
                operation: None,
            })
            .collect(),
        HashMap::from([
            (
                "service_name".to_string(),
                TraceRetryColumn {
                    present_rows: vec![1],
                    binary_types: Vec::new(),
                },
            ),
            (
                "span_attributes.absent".to_string(),
                TraceRetryColumn {
                    present_rows: vec![1],
                    binary_types: Vec::new(),
                },
            ),
            (
                "scope_attributes.present".to_string(),
                TraceRetryColumn {
                    present_rows: vec![0],
                    binary_types: Vec::new(),
                },
            ),
        ]),
    )
    .unwrap();
    assert_eq!(retry.rows[0].values.len(), 2);

    let mut rows = retry.rows.into_iter();
    let (_, projected) = rows.next().unwrap().into_rows(&retry.schema).unwrap();

    assert_eq!(
        projected
            .schema
            .iter()
            .map(|column| column.column_name.as_str())
            .collect::<Vec<_>>(),
        ["parent_span_id", "scope_attributes.present"]
    );
    assert_eq!(projected.rows[0].values.len(), 2);
    assert_eq!(projected.rows[0].values[0].value_data, None);
    assert_eq!(
        projected.rows[0].values[1].value_data,
        Some(ValueData::BoolValue(true))
    );
}

#[test]
fn test_trace_chunk_retry_restores_row_logical_types() {
    let column_name = "span_attributes.payload";
    let requests = row_insert_request(
        "traces",
        vec![field_schema(column_name, ColumnDataType::Binary)],
        vec![
            row(vec![Some(ValueData::BinaryValue(vec![1, 2, 3]))]),
            row(vec![Some(ValueData::BinaryValue(vec![4, 5, 6]))]),
            row(vec![Some(ValueData::StringValue("value".to_string()))]),
        ],
    );
    let metadata = ["span-1", "span-2", "span-3"]
        .into_iter()
        .map(|span_id| TraceSpanMetadata {
            trace_id: "trace-id".to_string(),
            span_id: span_id.to_string(),
            operation: None,
        })
        .collect();
    let retry = TraceChunkRetry::try_new(
        requests.table_name,
        requests.rows.unwrap(),
        metadata,
        HashMap::from([(
            column_name.to_string(),
            TraceRetryColumn {
                present_rows: vec![0, 1, 2],
                binary_types: vec![(0, TraceBinaryType::Binary), (1, TraceBinaryType::Json)],
            },
        )]),
    )
    .unwrap();

    let projected = retry
        .rows
        .into_iter()
        .map(|row| row.into_rows(&retry.schema).unwrap().1)
        .collect::<Vec<_>>();

    assert!(projected[0].schema[0].datatype_extension.is_none());
    assert!(matches!(
        projected[1].schema[0]
            .datatype_extension
            .as_ref()
            .and_then(|extension| extension.type_ext.as_ref()),
        Some(TypeExt::JsonType(_))
    ));
    assert_eq!(
        projected[2].schema[0].datatype,
        ColumnDataType::String as i32
    );
    assert!(projected[2].schema[0].datatype_extension.is_none());
}

#[test]
fn test_trace_chunk_retry_reconstructs_converter_request() {
    let spans = vec![
        trace_span(
            "span-1",
            vec![
                trace_attribute("first", OtlpValue::IntValue(1)),
                trace_attribute("payload", OtlpValue::BytesValue(vec![1, 2, 3])),
            ],
        ),
        trace_span(
            "span-2",
            vec![
                trace_attribute("second", OtlpValue::DoubleValue(2.5)),
                trace_attribute(
                    "payload",
                    OtlpValue::ArrayValue(ArrayValue {
                        values: vec![AnyValue {
                            value: Some(OtlpValue::StringValue("value".to_string())),
                        }],
                    }),
                ),
            ],
        ),
    ];
    let metadata = spans.iter().map(TraceSpanMetadata::from).collect();
    let (expected_table_data, _) =
        servers::otlp::trace::v1::v1_to_main_table_data_with_schema(spans.clone()).unwrap();
    let (expected_schema, mut expected_rows) = expected_table_data.into_schema_and_rows();
    for row in &mut expected_rows {
        row.values.resize(expected_schema.len(), Value::default());
    }
    let expected = row_insert_request("traces", expected_schema, expected_rows);
    let (table_data, batch_schema) =
        servers::otlp::trace::v1::v1_to_main_table_data_with_schema(spans).unwrap();
    let (schema, rows) = table_data.into_schema_and_rows();
    let retry = TraceChunkRetry::try_new(
        "traces".to_string(),
        Rows { schema, rows },
        metadata,
        batch_schema.into_retry_columns(),
    )
    .unwrap();

    assert_eq!(retry.to_request().unwrap(), expected);
}

#[test]
fn test_trace_request_schema_detects_datatype_extension_conflicts() {
    let column_name = "span_attributes.payload";
    let binary_schema = field_schema(column_name, ColumnDataType::Binary);
    let json_schema = json_field_schema(column_name);
    let legacy_json_schema = ColumnSchema {
        datatype: ColumnDataType::Json as i32,
        ..json_schema.clone()
    };

    let mut request_schema = TraceRequestSchema::default();
    request_schema.observe_trace_column(0, &binary_schema, None);
    request_schema.observe_trace_column(1, &json_schema, None);
    assert_eq!(
        request_schema.incompatible_schema_observations(None),
        HashMap::from([(column_name.to_string(), HashSet::from([0, 1]))])
    );

    let mut equivalent_json_request_schema = TraceRequestSchema::default();
    equivalent_json_request_schema.observe_trace_column(0, &json_schema, None);
    equivalent_json_request_schema.observe_trace_column(1, &legacy_json_schema, None);
    assert!(
        equivalent_json_request_schema
            .incompatible_schema_observations(None)
            .is_empty()
    );

    let mut incompatible_json_request_schema = TraceRequestSchema::default();
    incompatible_json_request_schema.observe_trace_column(0, &binary_schema, None);
    incompatible_json_request_schema.observe_trace_column(1, &legacy_json_schema, None);
    assert_eq!(
        incompatible_json_request_schema.incompatible_schema_observations(None),
        HashMap::from([(column_name.to_string(), HashSet::from([0, 1]))])
    );

    let existing_json_schema =
        DatatypesSchemaBuilder::try_from_columns(vec![DatatypesColumnSchema::new(
            column_name,
            ConcreteDataType::json_datatype(),
            true,
        )])
        .unwrap()
        .build()
        .unwrap();
    let mut binary_request_schema = TraceRequestSchema::default();
    binary_request_schema.observe_trace_column(0, &binary_schema, None);
    assert_eq!(
        binary_request_schema.incompatible_schema_observations(Some(&existing_json_schema)),
        HashMap::from([(column_name.to_string(), HashSet::from([0]))])
    );

    let mut json_request_schema = TraceRequestSchema::default();
    json_request_schema.observe_trace_column(0, &json_schema, None);
    assert!(
        json_request_schema
            .incompatible_schema_observations(Some(&existing_json_schema))
            .is_empty()
    );
}

#[test]
fn test_trace_request_schema_isolates_non_candidate_batch() {
    let column_name = "span_attributes.payload";
    let string_schema = field_schema(column_name, ColumnDataType::String);
    let binary_schema = field_schema(column_name, ColumnDataType::Binary);
    let existing_string_schema =
        DatatypesSchemaBuilder::try_from_columns(vec![DatatypesColumnSchema::new(
            column_name,
            ConcreteDataType::string_datatype(),
            true,
        )])
        .unwrap()
        .build()
        .unwrap();

    let mut request_schema = TraceRequestSchema::default();
    request_schema.observe_trace_column(0, &string_schema, Some(ColumnDataType::String));
    request_schema.observe_trace_column(1, &binary_schema, Some(ColumnDataType::Binary));

    assert_eq!(
        request_schema.incompatible_schema_observations(Some(&existing_string_schema)),
        HashMap::from([(column_name.to_string(), HashSet::from([1]))])
    );
}

#[test]
fn test_trace_request_schema_isolates_unsupported_type_batches() {
    let mixed_column = "span_attributes.mixed";
    let unrelated_column = "span_attributes.unrelated";
    let int_schema = field_schema(mixed_column, ColumnDataType::Int64);
    let bool_schema = field_schema(mixed_column, ColumnDataType::Boolean);
    let string_schema = field_schema(unrelated_column, ColumnDataType::String);
    let mut request_schema = TraceRequestSchema::default();
    request_schema.observe_trace_column(0, &int_schema, Some(ColumnDataType::Int64));
    request_schema.observe_trace_column(0, &string_schema, Some(ColumnDataType::String));
    request_schema.observe_trace_column(1, &bool_schema, Some(ColumnDataType::Boolean));
    request_schema.observe_trace_column(1, &string_schema, Some(ColumnDataType::String));

    let TraceRequestSchemaPlan::IncompatibleObservations(exclusions) =
        request_schema.resolve_table_schema(None)
    else {
        panic!("expected incompatible trace schema batches");
    };
    assert_eq!(
        exclusions,
        HashMap::from([(mixed_column.to_string(), HashSet::from([0, 1]))])
    );

    let batch_indexes = exclusions
        .values()
        .flatten()
        .copied()
        .collect::<HashSet<_>>();
    request_schema.remove_batches(&batch_indexes);
    let plan = ready_plan(request_schema.resolve_table_schema(None));
    assert!(!plan.requires_ddl());
}

#[test]
fn test_trace_request_schema_drops_failed_batch_observations() {
    let table_name = "trace_failed_batch";
    let column_name = "span_attributes.value";
    let failed_only_column = "span_attributes.server.port";
    let safe_column = "span_attributes.safe";
    let string_schema = field_schema(column_name, ColumnDataType::String);
    let float_schema = field_schema(column_name, ColumnDataType::Float64);
    let failed_only_schema = field_schema(failed_only_column, ColumnDataType::String);
    let service_schema = tag_schema(SERVICE_NAME_COLUMN, ColumnDataType::String);
    let safe_schema = field_schema(safe_column, ColumnDataType::Boolean);

    let mut good_request = row_insert_request(
        table_name,
        vec![string_schema.clone()],
        vec![row(vec![Some(ValueData::StringValue("1.5".to_string()))])],
    );
    let mut bad_request = row_insert_request(
        table_name,
        vec![
            string_schema.clone(),
            failed_only_schema.clone(),
            service_schema.clone(),
            safe_schema.clone(),
        ],
        vec![row(vec![
            Some(ValueData::StringValue("not-a-number".to_string())),
            Some(ValueData::StringValue("invalid-port".to_string())),
            Some(ValueData::StringValue("frontend".to_string())),
            Some(ValueData::BoolValue(true)),
        ])],
    );
    let original_bad_request = bad_request.clone();

    let mut request_schema = TraceRequestSchema::default();
    request_schema.observe_trace_column(0, &string_schema, Some(ColumnDataType::String));
    request_schema.observe_trace_column(0, &float_schema, Some(ColumnDataType::Float64));
    request_schema.observe_trace_column(1, &string_schema, Some(ColumnDataType::String));
    request_schema.observe_trace_column(1, &failed_only_schema, Some(ColumnDataType::String));
    request_schema.observe_trace_column(1, &service_schema, Some(ColumnDataType::String));
    request_schema.observe_trace_column(1, &safe_schema, Some(ColumnDataType::Boolean));
    ready_plan(request_schema.resolve_table_schema(None));

    let mut chunk_states = [
        TraceChunkSchemaState::Prepared,
        TraceChunkSchemaState::Prepared,
    ];
    assert_eq!(
        Instance::prepare_trace_v1_chunk_rewrite(
            &request_schema,
            0,
            &mut good_request,
            &mut chunk_states[0],
        )
        .unwrap(),
        None
    );
    let failed_column = Instance::prepare_trace_v1_chunk_rewrite(
        &request_schema,
        1,
        &mut bad_request,
        &mut chunk_states[1],
    )
    .unwrap();
    let exclusions = HashMap::from([(failed_column.unwrap(), HashSet::from([1]))]);
    assert_eq!(
        exclusions,
        HashMap::from([(column_name.to_string(), HashSet::from([1]))])
    );
    assert_eq!(chunk_states[0], TraceChunkSchemaState::Prepared);
    assert_eq!(chunk_states[1], TraceChunkSchemaState::ReconcilePerChunk);

    Instance::exclude_trace_v1_schema_observations(
        &mut request_schema,
        &mut chunk_states,
        &exclusions,
    );
    assert!(
        !request_schema
            .column_indexes
            .contains_key(failed_only_column)
    );
    assert!(
        !request_schema
            .column_indexes
            .contains_key(SERVICE_NAME_COLUMN)
    );
    assert!(!request_schema.column_indexes.contains_key(safe_column));
    let plan = ready_plan(request_schema.resolve_table_schema(None));
    assert_eq!(plan.ensure_columns.len(), 1);
    assert_eq!(plan.ensure_columns[0].column_name, column_name);
    assert_eq!(
        Instance::prepare_trace_v1_chunk_rewrite(
            &request_schema,
            1,
            &mut bad_request,
            &mut chunk_states[1],
        )
        .unwrap(),
        None
    );

    let good_rows = good_request.rows.as_ref().unwrap();
    assert_eq!(good_rows.schema[0].datatype, ColumnDataType::Float64 as i32);
    assert_eq!(
        good_rows.rows[0].values[0].value_data,
        Some(ValueData::F64Value(1.5))
    );
    assert_eq!(bad_request, original_bad_request);
}

#[test]
fn test_trace_request_schema_uses_global_columns_and_types() {
    let table_name = "trace_global_schema";
    let attr_num = "span_attributes.attr_num";
    let attr_later = "span_attributes.attr_later";
    let mut first_chunk = row_insert_request(
        table_name,
        vec![field_schema(attr_num, ColumnDataType::Int64)],
        vec![row(vec![Some(ValueData::I64Value(1))])],
    );
    let mut second_chunk = row_insert_request(
        table_name,
        vec![
            field_schema(attr_num, ColumnDataType::Float64),
            field_schema(attr_later, ColumnDataType::Boolean),
        ],
        vec![row(vec![
            Some(ValueData::F64Value(2.5)),
            Some(ValueData::BoolValue(true)),
        ])],
    );

    let mut request_schema = TraceRequestSchema::default();
    let attr_num_i64 = field_schema(attr_num, ColumnDataType::Int64);
    let attr_num_f64 = field_schema(attr_num, ColumnDataType::Float64);
    let attr_later_bool = field_schema(attr_later, ColumnDataType::Boolean);
    request_schema.observe_trace_column(0, &attr_num_i64, Some(ColumnDataType::Int64));
    request_schema.observe_trace_column(1, &attr_num_f64, Some(ColumnDataType::Float64));
    request_schema.observe_trace_column(1, &attr_later_bool, Some(ColumnDataType::Boolean));

    let existing_schema =
        DatatypesSchemaBuilder::try_from_columns(vec![DatatypesColumnSchema::new(
            attr_num,
            ConcreteDataType::int64_datatype(),
            true,
        )])
        .unwrap()
        .build()
        .unwrap();
    let pre_alter = ready_plan(request_schema.resolve_table_schema(Some(&existing_schema)));
    assert_eq!(pre_alter.modify_float64_columns, vec![attr_num.to_string()]);
    assert_eq!(pre_alter.ensure_columns.len(), 1);
    assert_eq!(pre_alter.ensure_columns[0].column_name, attr_later);

    prepare_and_apply(&request_schema, 0, &mut first_chunk);
    prepare_and_apply(&request_schema, 1, &mut second_chunk);

    let rows = first_chunk.rows.as_ref().unwrap();
    assert_eq!(rows.schema.len(), 1);
    assert_eq!(rows.schema[0].column_name, attr_num);
    assert_eq!(rows.schema[0].datatype, ColumnDataType::Float64 as i32);
    assert_eq!(
        rows.rows[0].values[0].value_data,
        Some(ValueData::F64Value(1.0))
    );

    let rows = second_chunk.rows.as_ref().unwrap();
    assert_eq!(rows.schema.len(), 2);
    assert_eq!(rows.schema[0].column_name, attr_num);
    assert_eq!(rows.schema[0].datatype, ColumnDataType::Float64 as i32);
    assert_eq!(rows.schema[1].column_name, attr_later);
    assert_eq!(rows.schema[1].datatype, ColumnDataType::Boolean as i32);
    assert_eq!(
        rows.rows[0].values[0].value_data,
        Some(ValueData::F64Value(2.5))
    );
    assert_eq!(
        rows.rows[0].values[1].value_data,
        Some(ValueData::BoolValue(true))
    );
}

#[test]
fn test_trace_request_schema_resolves_three_compatible_types() {
    let column_name = "span_attributes.value";
    let mut request_schema = TraceRequestSchema::default();
    for (batch_index, datatype) in [
        ColumnDataType::String,
        ColumnDataType::Int64,
        ColumnDataType::Float64,
    ]
    .into_iter()
    .enumerate()
    {
        let schema = field_schema(column_name, datatype);
        request_schema.observe_trace_column(batch_index, &schema, Some(datatype));
    }

    let plan = ready_plan(request_schema.resolve_table_schema(None));
    assert_eq!(plan.ensure_columns.len(), 1);
    assert_eq!(
        plan.ensure_columns[0].datatype,
        ColumnDataType::Float64 as i32
    );
}

#[test]
fn test_trace_request_schema_creates_table_without_padding_batches() {
    let table_name = "trace_create_schema";
    let attr_num = "span_attributes.attr_num";
    let attr_later = "span_attributes.attr_later";
    let mut first_chunk = row_insert_request(
        table_name,
        vec![field_schema(attr_num, ColumnDataType::Int64)],
        vec![row(vec![Some(ValueData::I64Value(1))])],
    );
    let mut second_chunk = row_insert_request(
        table_name,
        vec![
            field_schema(attr_num, ColumnDataType::Float64),
            field_schema(attr_later, ColumnDataType::Boolean),
        ],
        vec![row(vec![
            Some(ValueData::F64Value(2.5)),
            Some(ValueData::BoolValue(true)),
        ])],
    );

    let mut request_schema = TraceRequestSchema::default();
    let attr_num_i64 = field_schema(attr_num, ColumnDataType::Int64);
    let attr_num_f64 = field_schema(attr_num, ColumnDataType::Float64);
    let attr_later_bool = field_schema(attr_later, ColumnDataType::Boolean);
    request_schema.observe_trace_column(0, &attr_num_i64, Some(ColumnDataType::Int64));
    request_schema.observe_trace_column(1, &attr_num_f64, Some(ColumnDataType::Float64));
    request_schema.observe_trace_column(1, &attr_later_bool, Some(ColumnDataType::Boolean));

    let pre_alter = ready_plan(request_schema.resolve_table_schema(None));
    assert!(pre_alter.modify_float64_columns.is_empty());
    assert_eq!(pre_alter.ensure_columns.len(), 2);
    assert_eq!(pre_alter.ensure_columns[0].column_name, attr_num);
    assert_eq!(
        pre_alter.ensure_columns[0].datatype,
        ColumnDataType::Float64 as i32
    );
    assert_eq!(pre_alter.ensure_columns[1].column_name, attr_later);

    prepare_and_apply(&request_schema, 0, &mut first_chunk);
    prepare_and_apply(&request_schema, 1, &mut second_chunk);

    let rows = first_chunk.rows.as_ref().unwrap();
    assert_eq!(rows.schema.len(), 1);
    assert_eq!(rows.schema[0].column_name, attr_num);
    assert_eq!(rows.schema[0].datatype, ColumnDataType::Float64 as i32);
    assert_eq!(
        rows.rows[0].values[0].value_data,
        Some(ValueData::F64Value(1.0))
    );
    assert_eq!(rows.rows[0].values.len(), 1);

    let rows = second_chunk.rows.as_ref().unwrap();
    assert_eq!(rows.schema.len(), 2);
    assert_eq!(rows.schema[0].column_name, attr_num);
    assert_eq!(rows.schema[1].column_name, attr_later);
}

#[test]
fn test_trace_request_schema_re_resolves_after_concurrent_create() {
    let table_name = "trace_concurrent_create";
    let column_name = "span_attributes.value";
    let string_schema = field_schema(column_name, ColumnDataType::String);
    let mut requests = row_insert_request(
        table_name,
        vec![string_schema.clone()],
        vec![row(vec![Some(ValueData::StringValue("42".to_string()))])],
    );

    let mut request_schema = TraceRequestSchema::default();
    request_schema.observe_trace_column(0, &string_schema, Some(ColumnDataType::String));
    let initial_plan = ready_plan(request_schema.resolve_table_schema(None));
    assert!(!initial_plan.ensure_columns.is_empty());
    let initial_targets = request_schema.resolved_target_types();

    let concurrent_schema =
        DatatypesSchemaBuilder::try_from_columns(vec![DatatypesColumnSchema::new(
            column_name,
            ConcreteDataType::int64_datatype(),
            true,
        )])
        .unwrap()
        .build()
        .unwrap();
    let final_plan = ready_plan(request_schema.resolve_table_schema(Some(&concurrent_schema)));
    assert!(!final_plan.requires_ddl());
    assert_ne!(initial_targets, request_schema.resolved_target_types());

    prepare_and_apply(&request_schema, 0, &mut requests);
    let rows = requests.rows.as_ref().unwrap();
    assert_eq!(rows.schema[0].datatype, ColumnDataType::Int64 as i32);
    assert_eq!(
        rows.rows[0].values[0].value_data,
        Some(ValueData::I64Value(42))
    );
}

#[test]
fn test_trace_request_schema_keeps_request_intact_on_coercion_failure() {
    let table_name = "trace_atomic_rewrite";
    let column_name = "span_attributes.value";
    let string_schema = field_schema(column_name, ColumnDataType::String);
    let float_schema = field_schema(column_name, ColumnDataType::Float64);
    let requests = row_insert_request(
        table_name,
        vec![string_schema.clone()],
        vec![row(vec![Some(ValueData::StringValue(
            "not-a-number".to_string(),
        ))])],
    );
    let original = requests.clone();

    let mut request_schema = TraceRequestSchema::default();
    request_schema.observe_trace_column(0, &string_schema, Some(ColumnDataType::String));
    request_schema.observe_trace_column(0, &float_schema, Some(ColumnDataType::Float64));
    ready_plan(request_schema.resolve_table_schema(None));

    assert!(
        request_schema
            .prepare_request_rewrite(0, &requests)
            .is_err()
    );
    assert_eq!(requests, original);
}

use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans};

use super::{ExportTraceServiceRequest, trace_conventions};

fn resource_spans(resource_url: &str, scope_urls: &[&str]) -> ResourceSpans {
    ResourceSpans {
        schema_url: resource_url.to_string(),
        scope_spans: scope_urls
            .iter()
            .map(|u| ScopeSpans {
                schema_url: u.to_string(),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

#[test]
fn test_trace_conventions() {
    let unknown = ExportTraceServiceRequest::default();
    assert_eq!(trace_conventions(&unknown), "unknown");

    let url = "https://opentelemetry.io/schemas/1.27.0";
    let single = ExportTraceServiceRequest {
        resource_spans: vec![resource_spans("", &[url, url])],
    };
    assert_eq!(trace_conventions(&single), url);

    let resource_level = ExportTraceServiceRequest {
        resource_spans: vec![resource_spans(url, &[""])],
    };
    assert_eq!(trace_conventions(&resource_level), url);

    let conflicting = ExportTraceServiceRequest {
        resource_spans: vec![resource_spans(
            "",
            &[url, "https://opentelemetry.io/schemas/1.30.0"],
        )],
    };
    assert_eq!(trace_conventions(&conflicting), "mixed");
}
