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

use api::v1::value::ValueData;
use api::v1::{ColumnDataType, RowInsertRequests, Value};
use common_grpc::precision::Precision;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value as OtlpValue;
use pipeline::{GreptimePipelineParams, PipelineWay};
use session::context::QueryContextRef;

use super::attributes::Attributes;
use super::span::{parse, TraceSpan};
use super::{
    DURATION_NANO_COLUMN, PARENT_SPAN_ID_COLUMN, SERVICE_NAME_COLUMN, SPAN_ID_COLUMN,
    SPAN_KIND_COLUMN, SPAN_NAME_COLUMN, TIMESTAMP_COLUMN, TRACE_ID_COLUMN,
};
use crate::error::Result;
use crate::otlp::trace::{KEY_SERVICE_NAME, SPAN_EVENTS_COLUMN};
use crate::otlp::utils::{any_value_to_jsonb, make_column_data, make_string_column_data};
use crate::query_handler::PipelineHandlerRef;
use crate::row_writer::{self, MultiTableData, TableData};

const APPROXIMATE_COLUMN_COUNT: usize = 30;

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

    let one_table_writer = multi_table_writer.get_or_default_table_data(
        table_name,
        APPROXIMATE_COLUMN_COUNT,
        spans.len(),
    );

    for span in spans {
        write_span_to_row(one_table_writer, span)?;
    }

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
    // write ts fields
    let fields = vec![
        make_column_data(
            "timestamp_end",
            ColumnDataType::TimestampNanosecond,
            ValueData::TimestampNanosecondValue(span.end_in_nanosecond as i64),
        ),
        make_column_data(
            DURATION_NANO_COLUMN,
            ColumnDataType::Uint64,
            ValueData::U64Value(span.end_in_nanosecond - span.start_in_nanosecond),
        ),
    ];
    row_writer::write_fields(writer, fields.into_iter(), &mut row)?;

    // tags
    let tags = vec![
        (TRACE_ID_COLUMN.to_string(), span.trace_id),
        (SPAN_ID_COLUMN.to_string(), span.span_id),
    ];
    row_writer::write_tags(writer, tags.into_iter(), &mut row)?;

    // write fields
    let fields = vec![
        make_string_column_data(PARENT_SPAN_ID_COLUMN, span.parent_span_id),
        make_string_column_data(SPAN_KIND_COLUMN, span.span_kind),
        make_string_column_data(SPAN_NAME_COLUMN, span.span_name),
        make_string_column_data("span_status_code", span.span_status_code),
        make_string_column_data("span_status_message", span.span_status_message),
        make_string_column_data("trace_state", span.trace_state),
        make_string_column_data("scope_name", span.scope_name),
        make_string_column_data("scope_version", span.scope_version),
    ];
    row_writer::write_fields(writer, fields.into_iter(), &mut row)?;

    if let Some(service_name) = span.service_name {
        row_writer::write_fields(
            writer,
            std::iter::once(make_string_column_data(SERVICE_NAME_COLUMN, service_name)),
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

fn write_attributes(
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
                row_writer::write_fields(
                    writer,
                    std::iter::once(make_string_column_data(&key, v)),
                    row,
                )?;
            }
            Some(OtlpValue::BoolValue(v)) => {
                row_writer::write_fields(
                    writer,
                    std::iter::once(make_column_data(
                        &key,
                        ColumnDataType::Boolean,
                        ValueData::BoolValue(v),
                    )),
                    row,
                )?;
            }
            Some(OtlpValue::IntValue(v)) => {
                row_writer::write_fields(
                    writer,
                    std::iter::once(make_column_data(
                        &key,
                        ColumnDataType::Int64,
                        ValueData::I64Value(v),
                    )),
                    row,
                )?;
            }
            Some(OtlpValue::DoubleValue(v)) => {
                row_writer::write_fields(
                    writer,
                    std::iter::once(make_column_data(
                        &key,
                        ColumnDataType::Float64,
                        ValueData::F64Value(v),
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
                        ValueData::BinaryValue(v),
                    )),
                    row,
                )?;
            }
            None => {}
        }
    }

    Ok(())
}
