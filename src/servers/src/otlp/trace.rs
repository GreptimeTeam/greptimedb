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
use common_grpc::writer::Precision;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;

use self::span::{parse_span, TraceSpan, TraceSpans};
use super::{GREPTIME_TIMESTAMP, GREPTIME_VALUE};
use crate::error::Result;
use crate::row_writer::{self, MultiTableData, TableData};

const APPROXIMATE_COLUMN_COUNT: usize = 24;
pub const TRACE_TABLE_NAME: &str = "traces_preview_v01";

pub mod attributes;
pub mod span;

/// Convert OpenTelemetry traces to SpanTraces
///
/// See
/// <https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/trace/v1/trace.proto>
/// for data structure of OTLP traces.
pub fn parse(request: ExportTraceServiceRequest) -> TraceSpans {
    let mut spans = vec![];
    for resource_spans in request.resource_spans {
        let resource_attrs = resource_spans
            .resource
            .map(|r| r.attributes)
            .unwrap_or_default();
        for scope_spans in resource_spans.scope_spans {
            let scope = scope_spans.scope.unwrap_or_default();
            for span in scope_spans.spans {
                spans.push(parse_span(resource_attrs.clone(), scope.clone(), span));
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
    {
        // tags
        let iter = vec![
            ("trace_id", span.trace_id),
            ("span_id", span.span_id),
            ("parent_span_id", span.parent_span_id),
        ]
        .into_iter()
        .map(|(col, val)| (col.to_string(), val));
        row_writer::write_tags(writer, iter, &mut row)?;
    }
    {
        // fields
        let str_fields_iter = vec![
            ("resource_attributes", span.resource_attributes.to_string()),
            ("scope_name", span.scope_name),
            ("scope_version", span.scope_version),
            ("scope_attributes", span.scope_attributes.to_string()),
            ("trace_state", span.trace_state),
            ("span_name", span.span_name),
            ("span_kind", span.span_kind),
            ("span_status_code", span.span_status_code),
            ("span_status_message", span.span_status_message),
            ("span_attributes", span.span_attributes.to_string()),
            ("span_events", span.span_events.to_string()),
            ("span_links", span.span_links.to_string()),
        ]
        .into_iter()
        .map(|(col, val)| {
            (
                col.into(),
                ColumnDataType::String,
                ValueData::StringValue(val),
            )
        });

        let time_fields_iter = vec![
            ("start", span.start_in_nanosecond),
            ("end", span.end_in_nanosecond),
        ]
        .into_iter()
        .map(|(col, val)| {
            (
                col.into(),
                ColumnDataType::TimestampNanosecond,
                ValueData::TimestampNanosecondValue(val as i64),
            )
        });

        row_writer::write_fields(writer, str_fields_iter, &mut row)?;
        row_writer::write_fields(writer, time_fields_iter, &mut row)?;
        row_writer::write_fields(writer, span.uplifted_span_attributes.into_iter(), &mut row)?;
    }

    row_writer::write_f64(
        writer,
        GREPTIME_VALUE,
        (span.end_in_nanosecond - span.start_in_nanosecond) as f64 / 1_000_000.0, // duration in millisecond
        &mut row,
    )?;
    row_writer::write_ts_precision(
        writer,
        GREPTIME_TIMESTAMP,
        Some(span.start_in_nanosecond as i64),
        Precision::Nanosecond,
        &mut row,
    )?;

    writer.add_row(row);

    Ok(())
}
