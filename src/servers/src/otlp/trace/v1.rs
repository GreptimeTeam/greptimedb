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

use api::v1::value::ValueData;
use api::v1::{ColumnDataType, RowInsertRequests, Value};
use common_catalog::consts::{trace_operations_table_name, trace_services_table_name};
use common_grpc::precision::Precision;
use opentelemetry_proto::tonic::common::v1::any_value::Value as OtlpValue;
use pipeline::{GreptimePipelineParams, PipelineWay};
use session::context::QueryContextRef;

use crate::error::Result;
use crate::otlp::trace::attributes::Attributes;
use crate::otlp::trace::span::TraceSpan;
use crate::otlp::trace::{
    DURATION_NANO_COLUMN, KEY_SERVICE_NAME, PARENT_SPAN_ID_COLUMN, SCOPE_NAME_COLUMN,
    SCOPE_VERSION_COLUMN, SERVICE_NAME_COLUMN, SPAN_EVENTS_COLUMN, SPAN_ID_COLUMN,
    SPAN_KIND_COLUMN, SPAN_NAME_COLUMN, SPAN_STATUS_CODE, SPAN_STATUS_MESSAGE_COLUMN,
    TIMESTAMP_COLUMN, TRACE_ID_COLUMN, TRACE_STATE_COLUMN, TraceAuxData,
};
use crate::otlp::utils::{any_value_to_jsonb, make_column_data, make_string_column_data};
use crate::query_handler::PipelineHandlerRef;
use crate::row_writer::{self, MultiTableData, TableData};

const APPROXIMATE_COLUMN_COUNT: usize = 30;

// Use a timestamp(2100-01-01 00:00:00) as large as possible.
const MAX_TIMESTAMP: i64 = 4102444800000000000;

/// Converts trace spans into row insert requests for the main v1 trace table.
///
/// Auxiliary service and operation table writes are built separately so the
/// caller can update them only after the main span write succeeds.
pub fn v1_to_grpc_main_insert_requests(
    spans: &[TraceSpan],
    _pipeline: &PipelineWay,
    _pipeline_params: &GreptimePipelineParams,
    table_name: &str,
    _query_ctx: &QueryContextRef,
    _pipeline_handler: PipelineHandlerRef,
) -> Result<(RowInsertRequests, usize)> {
    let mut multi_table_writer = MultiTableData::default();
    let trace_writer = build_trace_table_data(spans)?;
    multi_table_writer.add_table_data(table_name, trace_writer);

    Ok(multi_table_writer.into_row_insert_requests())
}

/// Builds the row-oriented payload for the main v1 trace table.
pub fn build_trace_table_data(spans: &[TraceSpan]) -> Result<TableData> {
    let mut trace_writer = TableData::new(APPROXIMATE_COLUMN_COUNT, spans.len());
    for span in spans.iter().cloned() {
        write_span_to_row(&mut trace_writer, span)?;
    }

    Ok(trace_writer)
}

/// Builds row insert requests for the v1 trace auxiliary tables.
pub fn build_aux_table_requests(
    aux_data: TraceAuxData,
    table_name: &str,
) -> Result<(RowInsertRequests, usize)> {
    let mut multi_table_writer = MultiTableData::default();
    let mut trace_services_writer = TableData::new(APPROXIMATE_COLUMN_COUNT, 1);
    let mut trace_operations_writer = TableData::new(APPROXIMATE_COLUMN_COUNT, 1);

    write_trace_services_to_row(&mut trace_services_writer, aux_data.services)?;
    write_trace_operations_to_row(&mut trace_operations_writer, aux_data.operations)?;

    multi_table_writer.add_table_data(trace_services_table_name(table_name), trace_services_writer);
    multi_table_writer.add_table_data(
        trace_operations_table_name(table_name),
        trace_operations_writer,
    );

    Ok(multi_table_writer.into_row_insert_requests())
}

pub fn write_span_to_row(writer: &mut TableData, span: TraceSpan) -> Result<()> {
    let mut row = writer.alloc_one_row();

    // write ts
    row_writer::write_ts_to_nanos(
        writer,
        TIMESTAMP_COLUMN,
        Some(span.start_in_nanosecond as i64),
        Precision::Nanosecond,
        &mut row,
    )?;

    // write fields
    let fields = vec![
        make_column_data(
            "timestamp_end",
            ColumnDataType::TimestampNanosecond,
            Some(ValueData::TimestampNanosecondValue(
                span.end_in_nanosecond as i64,
            )),
        ),
        make_column_data(
            DURATION_NANO_COLUMN,
            ColumnDataType::Uint64,
            Some(ValueData::U64Value(
                span.end_in_nanosecond - span.start_in_nanosecond,
            )),
        ),
        make_string_column_data(SERVICE_NAME_COLUMN, span.service_name),
        make_string_column_data(PARENT_SPAN_ID_COLUMN, span.parent_span_id),
        make_string_column_data(SPAN_ID_COLUMN, Some(span.span_id)),
        make_string_column_data(SPAN_KIND_COLUMN, Some(span.span_kind)),
        make_string_column_data(SPAN_NAME_COLUMN, Some(span.span_name)),
        make_string_column_data(SPAN_STATUS_CODE, Some(span.span_status_code)),
        make_string_column_data(SPAN_STATUS_MESSAGE_COLUMN, Some(span.span_status_message)),
        make_string_column_data(TRACE_STATE_COLUMN, Some(span.trace_state)),
        make_string_column_data(SCOPE_NAME_COLUMN, Some(span.scope_name)),
        make_string_column_data(SCOPE_VERSION_COLUMN, Some(span.scope_version)),
    ];
    row_writer::write_fields(writer, fields.into_iter(), &mut row)?;

    row_writer::write_tags(
        writer,
        std::iter::once((TRACE_ID_COLUMN.to_string(), span.trace_id)),
        &mut row,
    )?;

    write_attributes(writer, "span_attributes", span.span_attributes, &mut row)?;
    write_attributes(writer, "scope_attributes", span.scope_attributes, &mut row)?;
    write_attributes(
        writer,
        "resource_attributes",
        span.resource_attributes,
        &mut row,
    )?;

    row_writer::write_json(
        writer,
        SPAN_EVENTS_COLUMN,
        span.span_events.into(),
        &mut row,
    )?;
    row_writer::write_json(writer, "span_links", span.span_links.into(), &mut row)?;

    writer.add_row(row);

    Ok(())
}

fn write_trace_services_to_row(writer: &mut TableData, services: HashSet<String>) -> Result<()> {
    for service_name in services {
        let mut row = writer.alloc_one_row();
        // Write the timestamp as 0.
        row_writer::write_ts_to_nanos(
            writer,
            TIMESTAMP_COLUMN,
            Some(MAX_TIMESTAMP),
            Precision::Nanosecond,
            &mut row,
        )?;

        // Write the `service_name` column.
        row_writer::write_tags(
            writer,
            std::iter::once((SERVICE_NAME_COLUMN.to_string(), service_name)),
            &mut row,
        )?;
        writer.add_row(row);
    }

    Ok(())
}

fn write_trace_operations_to_row(
    writer: &mut TableData,
    operations: HashSet<(String, String, String)>,
) -> Result<()> {
    for (service_name, span_name, span_kind) in operations {
        let mut row = writer.alloc_one_row();
        // Write the timestamp as 0.
        row_writer::write_ts_to_nanos(
            writer,
            TIMESTAMP_COLUMN,
            Some(MAX_TIMESTAMP),
            Precision::Nanosecond,
            &mut row,
        )?;

        // Write the `service_name`, `span_name`, and `span_kind` columns as tags.
        row_writer::write_tags(
            writer,
            vec![
                (SERVICE_NAME_COLUMN.to_string(), service_name),
                (SPAN_NAME_COLUMN.to_string(), span_name),
                (SPAN_KIND_COLUMN.to_string(), span_kind),
            ]
            .into_iter(),
            &mut row,
        )?;
        writer.add_row(row);
    }

    Ok(())
}

pub(crate) fn write_attributes(
    writer: &mut TableData,
    prefix: &str,
    attributes: Attributes,
    row: &mut Vec<Value>,
) -> Result<()> {
    for attr in attributes.take().into_iter() {
        let key_suffix = attr.key;
        // skip resource_attributes.service.name because its already copied to
        // top level as `SERVICE_NAME_COLUMN`
        if prefix == "resource_attributes" && key_suffix == KEY_SERVICE_NAME {
            continue;
        }

        let key = format!("{}.{}", prefix, key_suffix);
        match attr.value.and_then(|v| v.value) {
            Some(OtlpValue::StringValue(v)) => {
                // Keep the raw request value here. Mixed trace types are reconciled later
                // in the frontend once we can also see the existing table schema.
                writer.write_field_unchecked(
                    &key,
                    ColumnDataType::String,
                    Some(ValueData::StringValue(v)),
                    row,
                );
            }
            Some(OtlpValue::BoolValue(v)) => {
                // Do not coerce or promote types while building the request-local rows.
                writer.write_field_unchecked(
                    &key,
                    ColumnDataType::Boolean,
                    Some(ValueData::BoolValue(v)),
                    row,
                );
            }
            Some(OtlpValue::IntValue(v)) => {
                // Preserving the original value avoids order-dependent behavior inside one batch.
                writer.write_field_unchecked(
                    &key,
                    ColumnDataType::Int64,
                    Some(ValueData::I64Value(v)),
                    row,
                );
            }
            Some(OtlpValue::DoubleValue(v)) => {
                writer.write_field_unchecked(
                    &key,
                    ColumnDataType::Float64,
                    Some(ValueData::F64Value(v)),
                    row,
                );
            }
            Some(OtlpValue::ArrayValue(v)) => row_writer::write_json(
                writer,
                key,
                any_value_to_jsonb(OtlpValue::ArrayValue(v)),
                row,
            )?,
            Some(OtlpValue::KvlistValue(v)) => row_writer::write_json(
                writer,
                key,
                any_value_to_jsonb(OtlpValue::KvlistValue(v)),
                row,
            )?,
            Some(OtlpValue::BytesValue(v)) => {
                row_writer::write_fields(
                    writer,
                    std::iter::once(make_column_data(
                        &key,
                        ColumnDataType::Binary,
                        Some(ValueData::BinaryValue(v)),
                    )),
                    row,
                )?;
            }
            None => {}
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use api::v1::value::ValueData;
    use opentelemetry_proto::tonic::common::v1::any_value::Value as OtlpValue;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};

    use super::*;
    use crate::otlp::trace::TraceAuxData;
    use crate::otlp::trace::attributes::Attributes;
    use crate::otlp::trace::span::{SpanEvents, SpanLinks};
    use crate::row_writer::TableData;

    fn make_kv(key: &str, value: OtlpValue) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(AnyValue { value: Some(value) }),
        }
    }

    fn make_span(service_name: &str, trace_id: &str, span_id: &str) -> TraceSpan {
        TraceSpan {
            service_name: Some(service_name.to_string()),
            trace_id: trace_id.to_string(),
            span_id: span_id.to_string(),
            parent_span_id: None,
            resource_attributes: Attributes::from(vec![]),
            scope_name: "scope".to_string(),
            scope_version: "v1".to_string(),
            scope_attributes: Attributes::from(vec![]),
            trace_state: String::new(),
            span_name: "op".to_string(),
            span_kind: "SPAN_KIND_SERVER".to_string(),
            span_status_code: "STATUS_CODE_UNSET".to_string(),
            span_status_message: String::new(),
            span_attributes: Attributes::from(vec![]),
            span_events: SpanEvents::from(vec![]),
            span_links: SpanLinks::from(vec![]),
            start_in_nanosecond: 1,
            end_in_nanosecond: 2,
        }
    }

    #[test]
    fn test_keep_mixed_numeric_values_until_frontend_reconciliation() {
        let mut writer = TableData::new(4, 2);

        let attrs1 = Attributes::from(vec![make_kv("val", OtlpValue::DoubleValue(1.5))]);
        let mut row1 = writer.alloc_one_row();
        write_attributes(&mut writer, "attr", attrs1, &mut row1).unwrap();
        writer.add_row(row1);

        let attrs2 = Attributes::from(vec![make_kv("val", OtlpValue::IntValue(42))]);
        let mut row2 = writer.alloc_one_row();
        write_attributes(&mut writer, "attr", attrs2, &mut row2).unwrap();
        writer.add_row(row2);

        let (schema, rows) = writer.into_schema_and_rows();

        let col_idx = schema
            .iter()
            .position(|c| c.column_name == "attr.val")
            .unwrap();
        assert_eq!(schema[col_idx].datatype, ColumnDataType::Float64 as i32);

        assert_eq!(
            rows[0].values[col_idx].value_data,
            Some(ValueData::F64Value(1.5))
        );
        assert_eq!(
            rows[1].values[col_idx].value_data,
            Some(ValueData::I64Value(42))
        );
    }

    #[test]
    fn test_keep_mixed_string_and_int_values_until_frontend_reconciliation() {
        let mut writer = TableData::new(4, 2);

        let attrs1 = Attributes::from(vec![make_kv("val", OtlpValue::IntValue(10))]);
        let mut row1 = writer.alloc_one_row();
        write_attributes(&mut writer, "attr", attrs1, &mut row1).unwrap();
        writer.add_row(row1);

        let attrs2 = Attributes::from(vec![make_kv(
            "val",
            OtlpValue::StringValue("20".to_string()),
        )]);
        let mut row2 = writer.alloc_one_row();
        write_attributes(&mut writer, "attr", attrs2, &mut row2).unwrap();
        writer.add_row(row2);

        let (schema, rows) = writer.into_schema_and_rows();
        let col_idx = schema
            .iter()
            .position(|c| c.column_name == "attr.val")
            .unwrap();
        assert_eq!(schema[col_idx].datatype, ColumnDataType::Int64 as i32);
        assert_eq!(
            rows[1].values[col_idx].value_data,
            Some(ValueData::StringValue("20".to_string()))
        );
    }

    #[test]
    fn test_keep_first_seen_schema_until_frontend_reconciliation() {
        let mut writer = TableData::new(4, 2);

        let attrs1 = Attributes::from(vec![make_kv(
            "val",
            OtlpValue::StringValue("10".to_string()),
        )]);
        let mut row1 = writer.alloc_one_row();
        write_attributes(&mut writer, "attr", attrs1, &mut row1).unwrap();
        writer.add_row(row1);

        let attrs2 = Attributes::from(vec![make_kv("val", OtlpValue::IntValue(20))]);
        let mut row2 = writer.alloc_one_row();
        write_attributes(&mut writer, "attr", attrs2, &mut row2).unwrap();
        writer.add_row(row2);

        let (schema, rows) = writer.into_schema_and_rows();
        let col_idx = schema
            .iter()
            .position(|c| c.column_name == "attr.val")
            .unwrap();
        assert_eq!(schema[col_idx].datatype, ColumnDataType::String as i32);
        assert_eq!(
            rows[0].values[col_idx].value_data,
            Some(ValueData::StringValue("10".to_string()))
        );
        assert_eq!(
            rows[1].values[col_idx].value_data,
            Some(ValueData::I64Value(20))
        );
    }

    #[test]
    fn test_keep_mixed_string_and_float_values_until_frontend_reconciliation() {
        let mut writer = TableData::new(4, 2);

        let attrs1 = Attributes::from(vec![make_kv("val", OtlpValue::DoubleValue(1.5))]);
        let mut row1 = writer.alloc_one_row();
        write_attributes(&mut writer, "attr", attrs1, &mut row1).unwrap();
        writer.add_row(row1);

        let attrs2 = Attributes::from(vec![make_kv(
            "val",
            OtlpValue::StringValue("1.5".to_string()),
        )]);
        let mut row2 = writer.alloc_one_row();
        write_attributes(&mut writer, "attr", attrs2, &mut row2).unwrap();
        writer.add_row(row2);

        let (schema, rows) = writer.into_schema_and_rows();
        let col_idx = schema
            .iter()
            .position(|c| c.column_name == "attr.val")
            .unwrap();
        assert_eq!(schema[col_idx].datatype, ColumnDataType::Float64 as i32);
        assert_eq!(
            rows[1].values[col_idx].value_data,
            Some(ValueData::StringValue("1.5".to_string()))
        );
    }

    #[test]
    fn test_keep_mixed_string_and_bool_values_until_frontend_reconciliation() {
        let mut writer = TableData::new(4, 2);

        let attrs1 = Attributes::from(vec![make_kv(
            "val",
            OtlpValue::StringValue("true".to_string()),
        )]);
        let mut row1 = writer.alloc_one_row();
        write_attributes(&mut writer, "attr", attrs1, &mut row1).unwrap();
        writer.add_row(row1);

        let attrs2 = Attributes::from(vec![make_kv("val", OtlpValue::BoolValue(false))]);
        let mut row2 = writer.alloc_one_row();
        write_attributes(&mut writer, "attr", attrs2, &mut row2).unwrap();
        writer.add_row(row2);

        let (schema, rows) = writer.into_schema_and_rows();
        let col_idx = schema
            .iter()
            .position(|c| c.column_name == "attr.val")
            .unwrap();
        assert_eq!(schema[col_idx].datatype, ColumnDataType::String as i32);
        assert_eq!(
            rows[0].values[col_idx].value_data,
            Some(ValueData::StringValue("true".to_string()))
        );
        assert_eq!(
            rows[1].values[col_idx].value_data,
            Some(ValueData::BoolValue(false))
        );
    }

    #[test]
    fn test_keep_mixed_binary_and_string_values_until_frontend_reconciliation() {
        let mut writer = TableData::new(4, 2);

        let attrs1 = Attributes::from(vec![make_kv(
            "val",
            OtlpValue::BytesValue(vec![1_u8, 2, 3]),
        )]);
        let mut row1 = writer.alloc_one_row();
        write_attributes(&mut writer, "attr", attrs1, &mut row1).unwrap();
        writer.add_row(row1);

        let attrs2 = Attributes::from(vec![make_kv(
            "val",
            OtlpValue::StringValue("false".to_string()),
        )]);
        let mut row2 = writer.alloc_one_row();
        write_attributes(&mut writer, "attr", attrs2, &mut row2).unwrap();
        writer.add_row(row2);

        let (schema, rows) = writer.into_schema_and_rows();
        let col_idx = schema
            .iter()
            .position(|c| c.column_name == "attr.val")
            .unwrap();
        assert_eq!(schema[col_idx].datatype, ColumnDataType::Binary as i32);
        assert_eq!(
            rows[0].values[col_idx].value_data,
            Some(ValueData::BinaryValue(vec![1_u8, 2, 3]))
        );
        assert_eq!(
            rows[1].values[col_idx].value_data,
            Some(ValueData::StringValue("false".to_string()))
        );
    }

    #[test]
    fn test_build_aux_table_requests_deduplicates_services_and_operations() {
        let spans = vec![
            make_span("svc-a", "trace-a", "span-a"),
            make_span("svc-a", "trace-b", "span-b"),
        ];
        let mut aux_data = TraceAuxData::default();
        for span in &spans {
            aux_data.observe_span(span);
        }

        let (requests, total_rows) =
            build_aux_table_requests(aux_data, "opentelemetry_traces").unwrap();
        assert_eq!(requests.inserts.len(), 2);
        assert_eq!(total_rows, 2);
    }
    // Conversion matrix coverage lives in the shared coercion helper tests.
}
