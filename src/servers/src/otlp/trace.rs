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
use api::v1::{ColumnDataType, RowInsertRequests};
use common_grpc::precision::Precision;
use itertools::Itertools;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value;

use self::span::{parse_span, TraceSpan, TraceSpans};
use crate::error::Result;
use crate::otlp::utils::{make_column_data, make_string_column_data};
use crate::row_writer::{self, MultiTableData, TableData};

const APPROXIMATE_COLUMN_COUNT: usize = 24;

pub const TRACE_TABLE_NAME: &str = "opentelemetry_traces";
pub const SERVICE_NAME_COLUMN: &str = "service_name";
pub const TRACE_ID_COLUMN: &str = "trace_id";
pub const TIMESTAMP_COLUMN: &str = "timestamp";
pub const DURATION_NANO_COLUMN: &str = "duration_nano";
pub const SPAN_ID_COLUMN: &str = "span_id";
pub const SPAN_NAME_COLUMN: &str = "span_name";
pub const SPAN_KIND_COLUMN: &str = "span_kind";
pub const SPAN_ATTRIBUTES_COLUMN: &str = "span_attributes";

/// The span kind prefix in the database.
/// If the span kind is `server`, it will be stored as `SPAN_KIND_SERVER` in the database.
pub const SPAN_KIND_PREFIX: &str = "SPAN_KIND_";

pub mod attributes;
pub mod span;

/// Convert OpenTelemetry traces to SpanTraces
///
/// See
/// <https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/trace/v1/trace.proto>
/// for data structure of OTLP traces.
pub fn parse(request: ExportTraceServiceRequest) -> TraceSpans {
    let span_size = request
        .resource_spans
        .iter()
        .flat_map(|res| res.scope_spans.iter())
        .flat_map(|scope| scope.spans.iter())
        .count();
    let mut spans = Vec::with_capacity(span_size);
    for resource_spans in request.resource_spans {
        let resource_attrs = resource_spans
            .resource
            .map(|r| r.attributes)
            .unwrap_or_default();
        let service_name = resource_attrs
            .iter()
            .find_or_first(|kv| kv.key == "service.name")
            .and_then(|kv| kv.value.clone())
            .and_then(|v| match v.value {
                Some(any_value::Value::StringValue(s)) => Some(s),
                Some(any_value::Value::BytesValue(b)) => {
                    Some(String::from_utf8_lossy(&b).to_string())
                }
                _ => None,
            });

        for scope_spans in resource_spans.scope_spans {
            let scope = scope_spans.scope.unwrap_or_default();
            for span in scope_spans.spans {
                spans.push(parse_span(
                    service_name.clone(),
                    &resource_attrs,
                    &scope,
                    span,
                ));
            }
        }
    }
    spans
}

/// Convert SpanTraces to GreptimeDB row insert requests.
/// Returns `InsertRequests` and total number of rows to ingest
pub fn to_grpc_insert_requests(
    table_name: String,
    spans: TraceSpans,
) -> Result<(RowInsertRequests, usize)> {
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
        "timestamp",
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
            "duration_nano",
            ColumnDataType::Uint64,
            ValueData::U64Value(span.end_in_nanosecond - span.start_in_nanosecond),
        ),
    ];
    row_writer::write_fields(writer, fields.into_iter(), &mut row)?;

    if let Some(service_name) = span.service_name {
        row_writer::write_tag(writer, "service_name", service_name, &mut row)?;
    }

    // tags
    let iter = vec![
        ("trace_id", span.trace_id),
        ("span_id", span.span_id),
        ("parent_span_id", span.parent_span_id),
    ]
    .into_iter()
    .map(|(col, val)| (col.to_string(), val));
    row_writer::write_tags(writer, iter, &mut row)?;

    // write fields
    let fields = vec![
        make_string_column_data("span_kind", span.span_kind),
        make_string_column_data("span_name", span.span_name),
        make_string_column_data("span_status_code", span.span_status_code),
        make_string_column_data("span_status_message", span.span_status_message),
        make_string_column_data("trace_state", span.trace_state),
    ];
    row_writer::write_fields(writer, fields.into_iter(), &mut row)?;

    row_writer::write_json(
        writer,
        "span_attributes",
        span.span_attributes.into(),
        &mut row,
    )?;
    row_writer::write_json(writer, "span_events", span.span_events.into(), &mut row)?;
    row_writer::write_json(writer, "span_links", span.span_links.into(), &mut row)?;

    // write fields
    let fields = vec![
        make_string_column_data("scope_name", span.scope_name),
        make_string_column_data("scope_version", span.scope_version),
    ];
    row_writer::write_fields(writer, fields.into_iter(), &mut row)?;

    row_writer::write_json(
        writer,
        "scope_attributes",
        span.scope_attributes.into(),
        &mut row,
    )?;

    row_writer::write_json(
        writer,
        "resource_attributes",
        span.resource_attributes.into(),
        &mut row,
    )?;

    writer.add_row(row);

    Ok(())
}
