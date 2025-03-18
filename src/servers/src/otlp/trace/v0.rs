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
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use pipeline::{GreptimePipelineParams, PipelineWay};
use session::context::QueryContextRef;

use super::span::{parse, TraceSpan};
use super::{
    DURATION_NANO_COLUMN, PARENT_SPAN_ID_COLUMN, SERVICE_NAME_COLUMN, SPAN_ATTRIBUTES_COLUMN,
    SPAN_ID_COLUMN, SPAN_KIND_COLUMN, SPAN_NAME_COLUMN, TIMESTAMP_COLUMN, TRACE_ID_COLUMN,
};
use crate::error::Result;
use crate::otlp::trace::SPAN_EVENTS_COLUMN;
use crate::otlp::utils::{make_column_data, make_string_column_data};
use crate::query_handler::PipelineHandlerRef;
use crate::row_writer::{self, MultiTableData, TableData};

const APPROXIMATE_COLUMN_COUNT: usize = 24;

/// Convert SpanTraces to GreptimeDB row insert requests.
/// Returns `InsertRequests` and total number of rows to ingest
pub fn v0_to_grpc_insert_requests(
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

    if let Some(service_name) = span.service_name {
        row_writer::write_tag(writer, SERVICE_NAME_COLUMN, service_name, &mut row)?;
    }

    // write fields
    let fields = vec![
        make_string_column_data(TRACE_ID_COLUMN, span.trace_id),
        make_string_column_data(SPAN_ID_COLUMN, span.span_id),
        make_string_column_data(
            PARENT_SPAN_ID_COLUMN,
            span.parent_span_id.unwrap_or_default(),
        ),
        make_string_column_data(SPAN_KIND_COLUMN, span.span_kind),
        make_string_column_data(SPAN_NAME_COLUMN, span.span_name),
        make_string_column_data("span_status_code", span.span_status_code),
        make_string_column_data("span_status_message", span.span_status_message),
        make_string_column_data("trace_state", span.trace_state),
    ];
    row_writer::write_fields(writer, fields.into_iter(), &mut row)?;

    row_writer::write_json(
        writer,
        SPAN_ATTRIBUTES_COLUMN,
        span.span_attributes.into(),
        &mut row,
    )?;
    row_writer::write_json(
        writer,
        SPAN_EVENTS_COLUMN,
        span.span_events.into(),
        &mut row,
    )?;
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
