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
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value as OtlpValue;
use pipeline::{GreptimePipelineParams, PipelineWay};
use session::context::QueryContextRef;

use crate::error::{InvalidParameterSnafu, Result};
use crate::otlp::trace::attributes::Attributes;
use crate::otlp::trace::coerce::{coerce_non_null_value, is_supported_trace_coercion};
use crate::otlp::trace::span::{TraceSpan, parse};
use crate::otlp::trace::{
    DURATION_NANO_COLUMN, KEY_SERVICE_NAME, PARENT_SPAN_ID_COLUMN, SCOPE_NAME_COLUMN,
    SCOPE_VERSION_COLUMN, SERVICE_NAME_COLUMN, SPAN_EVENTS_COLUMN, SPAN_ID_COLUMN,
    SPAN_KIND_COLUMN, SPAN_NAME_COLUMN, SPAN_STATUS_CODE, SPAN_STATUS_MESSAGE_COLUMN,
    TIMESTAMP_COLUMN, TRACE_ID_COLUMN, TRACE_STATE_COLUMN,
};
use crate::otlp::utils::{any_value_to_jsonb, make_column_data, make_string_column_data};
use crate::query_handler::PipelineHandlerRef;
use crate::row_writer::{self, MultiTableData, TableData};

const APPROXIMATE_COLUMN_COUNT: usize = 30;

// Use a timestamp(2100-01-01 00:00:00) as large as possible.
const MAX_TIMESTAMP: i64 = 4102444800000000000;

/// Convert SpanTraces to GreptimeDB row insert requests.
/// Returns `InsertRequests` and total number of rows to ingest
///
/// Compared with v0, this v1 implementation:
/// 1. flattens all attribute data into columns.
/// 2. treat `span_id` and `parent_trace_id` as fields.
/// 3. removed `service_name` column because it's already in
///    `resource_attributes.service_name`
///
/// For other compound data structures like span_links and span_events here we
/// are still using `json` data structure.
pub fn v1_to_grpc_insert_requests(
    request: ExportTraceServiceRequest,
    _pipeline: PipelineWay,
    _pipeline_params: GreptimePipelineParams,
    table_name: String,
    _query_ctx: &QueryContextRef,
    _pipeline_handler: PipelineHandlerRef,
) -> Result<(RowInsertRequests, usize)> {
    let spans = parse(request);
    let mut multi_table_writer = MultiTableData::default();
    let mut trace_writer = TableData::new(APPROXIMATE_COLUMN_COUNT, spans.len());
    let mut trace_services_writer = TableData::new(APPROXIMATE_COLUMN_COUNT, 1);
    let mut trace_operations_writer = TableData::new(APPROXIMATE_COLUMN_COUNT, 1);

    let mut services = HashSet::new();
    let mut operations = HashSet::new();
    for span in spans {
        if let Some(service_name) = &span.service_name {
            // Only insert the service name if it's not already in the set.
            if !services.contains(service_name) {
                services.insert(service_name.clone());
            }

            // Only insert the operation if it's not already in the set.
            let operation = (
                service_name.clone(),
                span.span_name.clone(),
                span.span_kind.clone(),
            );
            if !operations.contains(&operation) {
                operations.insert(operation);
            }
        }
        write_span_to_row(&mut trace_writer, span)?;
    }
    write_trace_services_to_row(&mut trace_services_writer, services)?;
    write_trace_operations_to_row(&mut trace_operations_writer, operations)?;

    multi_table_writer.add_table_data(
        trace_services_table_name(&table_name),
        trace_services_writer,
    );
    multi_table_writer.add_table_data(
        trace_operations_table_name(&table_name),
        trace_operations_writer,
    );
    multi_table_writer.add_table_data(table_name, trace_writer);

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
        make_string_column_data(PARENT_SPAN_ID_COLUMN, span.parent_span_id),
        make_string_column_data(TRACE_ID_COLUMN, Some(span.trace_id)),
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

    if let Some(service_name) = span.service_name {
        row_writer::write_tags(
            writer,
            std::iter::once((SERVICE_NAME_COLUMN.to_string(), service_name)),
            &mut row,
        )?;
    }

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

fn prefer_incoming_non_string_type(
    existing_datatype: i32,
    incoming_datatype: ColumnDataType,
) -> bool {
    existing_datatype == ColumnDataType::String as i32
        && incoming_datatype != ColumnDataType::String
}

fn coerce_string_value_to_type(target: ColumnDataType, value: &ValueData) -> Option<ValueData> {
    coerce_non_null_value(target, ColumnDataType::String, value)
        .filter(|_| is_supported_trace_coercion(ColumnDataType::String, target))
}

fn coerce_to_existing_type(
    existing_datatype: i32,
    incoming_datatype: ColumnDataType,
    value: &ValueData,
) -> Option<(ColumnDataType, ValueData)> {
    let existing_datatype = ColumnDataType::try_from(existing_datatype).ok()?;
    let value = coerce_non_null_value(existing_datatype, incoming_datatype, value)?;
    Some((existing_datatype, value))
}

fn promote_string_column_to_type(
    writer: &mut TableData,
    key: &str,
    target: ColumnDataType,
) -> Result<()> {
    let Some(column_index) = writer.column_index(key) else {
        return Ok(());
    };

    for row in writer.rows_mut() {
        let Some(value) = row
            .values
            .get_mut(column_index)
            .and_then(|value| value.value_data.as_mut())
        else {
            continue;
        };

        let Some(coerced) = coerce_string_value_to_type(target, value) else {
            return InvalidParameterSnafu {
                reason: format!(
                    "failed to coerce existing trace column '{}' from {:?} to {:?}",
                    key,
                    ColumnDataType::String,
                    target
                ),
            }
            .fail();
        };
        *value = coerced;
    }

    if let Some(column_schema) = writer.column_schema_mut(column_index) {
        column_schema.datatype = target as i32;
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
        // for some types, see [`coerce.rs`] for the type list
        // we'll try to coerce the incoming value to the existing type, if the coercion is supported.
        // if the table doesn't have the column, and the batch has both string type and non-string type,
        // we'll promote the string column to the non-string type.
        match attr.value.and_then(|v| v.value) {
            Some(OtlpValue::StringValue(v)) => {
                let incoming_value = ValueData::StringValue(v);
                let (datatype, value_data) = if let Some(existing) = writer.column_datatype(&key) {
                    coerce_to_existing_type(existing, ColumnDataType::String, &incoming_value)
                        .unwrap_or((ColumnDataType::String, incoming_value))
                } else {
                    (ColumnDataType::String, incoming_value)
                };
                row_writer::write_fields(
                    writer,
                    std::iter::once(make_column_data(&key, datatype, Some(value_data))),
                    row,
                )?;
            }
            Some(OtlpValue::BoolValue(v)) => {
                if let Some(existing) = writer.column_datatype(&key)
                    && prefer_incoming_non_string_type(existing, ColumnDataType::Boolean)
                {
                    promote_string_column_to_type(writer, &key, ColumnDataType::Boolean)?;
                }
                row_writer::write_fields(
                    writer,
                    std::iter::once(make_column_data(
                        &key,
                        ColumnDataType::Boolean,
                        Some(ValueData::BoolValue(v)),
                    )),
                    row,
                )?;
            }
            Some(OtlpValue::IntValue(v)) => {
                let (datatype, value_data) = if let Some(existing) = writer.column_datatype(&key) {
                    if prefer_incoming_non_string_type(existing, ColumnDataType::Int64) {
                        promote_string_column_to_type(writer, &key, ColumnDataType::Int64)?;
                        (ColumnDataType::Int64, ValueData::I64Value(v))
                    } else {
                        coerce_to_existing_type(
                            existing,
                            ColumnDataType::Int64,
                            &ValueData::I64Value(v),
                        )
                        .unwrap_or((ColumnDataType::Int64, ValueData::I64Value(v)))
                    }
                } else {
                    (ColumnDataType::Int64, ValueData::I64Value(v))
                };
                row_writer::write_fields(
                    writer,
                    std::iter::once(make_column_data(&key, datatype, Some(value_data))),
                    row,
                )?;
            }
            Some(OtlpValue::DoubleValue(v)) => {
                if let Some(existing) = writer.column_datatype(&key)
                    && prefer_incoming_non_string_type(existing, ColumnDataType::Float64)
                {
                    promote_string_column_to_type(writer, &key, ColumnDataType::Float64)?;
                }
                row_writer::write_fields(
                    writer,
                    std::iter::once(make_column_data(
                        &key,
                        ColumnDataType::Float64,
                        Some(ValueData::F64Value(v)),
                    )),
                    row,
                )?;
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
    use crate::otlp::trace::attributes::Attributes;
    use crate::row_writer::TableData;

    fn make_kv(key: &str, value: OtlpValue) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(AnyValue { value: Some(value) }),
        }
    }

    #[test]
    fn test_coerce_int_to_float_within_batch() {
        let mut writer = TableData::new(4, 2);

        // First row: attribute "val" arrives as DoubleValue -> Float64 column
        let attrs1 = Attributes::from(vec![make_kv("val", OtlpValue::DoubleValue(1.5))]);
        let mut row1 = writer.alloc_one_row();
        write_attributes(&mut writer, "attr", attrs1, &mut row1).unwrap();
        writer.add_row(row1);

        // Second row: same attribute "val" arrives as IntValue -> should be coerced to Float64
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
            Some(ValueData::F64Value(42.0))
        );
    }

    #[test]
    fn test_coerce_string_to_int_within_batch() {
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
            Some(ValueData::I64Value(20))
        );
    }

    #[test]
    fn test_prefer_int_over_existing_string_within_batch() {
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
        assert_eq!(schema[col_idx].datatype, ColumnDataType::Int64 as i32);
        assert_eq!(
            rows[0].values[col_idx].value_data,
            Some(ValueData::I64Value(10))
        );
        assert_eq!(
            rows[1].values[col_idx].value_data,
            Some(ValueData::I64Value(20))
        );
    }

    #[test]
    fn test_coerce_string_to_float_within_batch() {
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
            Some(ValueData::F64Value(1.5))
        );
    }

    #[test]
    fn test_prefer_float_over_existing_string_within_batch() {
        let mut writer = TableData::new(4, 2);

        let attrs1 = Attributes::from(vec![make_kv(
            "val",
            OtlpValue::StringValue("1.5".to_string()),
        )]);
        let mut row1 = writer.alloc_one_row();
        write_attributes(&mut writer, "attr", attrs1, &mut row1).unwrap();
        writer.add_row(row1);

        let attrs2 = Attributes::from(vec![make_kv("val", OtlpValue::DoubleValue(2.5))]);
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
            Some(ValueData::F64Value(2.5))
        );
    }

    #[test]
    fn test_coerce_string_to_bool_within_batch() {
        let mut writer = TableData::new(4, 2);

        let attrs1 = Attributes::from(vec![make_kv("val", OtlpValue::BoolValue(true))]);
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
        assert_eq!(schema[col_idx].datatype, ColumnDataType::Boolean as i32);
        assert_eq!(
            rows[1].values[col_idx].value_data,
            Some(ValueData::BoolValue(false))
        );
    }

    #[test]
    fn test_prefer_bool_over_existing_string_within_batch() {
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
        assert_eq!(schema[col_idx].datatype, ColumnDataType::Boolean as i32);
        assert_eq!(
            rows[0].values[col_idx].value_data,
            Some(ValueData::BoolValue(true))
        );
        assert_eq!(
            rows[1].values[col_idx].value_data,
            Some(ValueData::BoolValue(false))
        );
    }

    #[test]
    fn test_no_coerce_float_to_int() {
        let mut writer = TableData::new(4, 2);

        // First row establishes Int64
        let attrs1 = Attributes::from(vec![make_kv("val", OtlpValue::IntValue(10))]);
        let mut row1 = writer.alloc_one_row();
        write_attributes(&mut writer, "attr", attrs1, &mut row1).unwrap();
        writer.add_row(row1);

        // Second row: DoubleValue should NOT be coerced to Int64 (lossy)
        let attrs2 = Attributes::from(vec![make_kv("val", OtlpValue::DoubleValue(1.5))]);
        let mut row2 = writer.alloc_one_row();
        let result = write_attributes(&mut writer, "attr", attrs2, &mut row2);
        assert!(result.is_err());
    }
    // Conversion matrix coverage lives in the shared coercion helper tests.
}
