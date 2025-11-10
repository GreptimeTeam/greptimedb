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
use api::v1::{ColumnDataType, RowInsertRequests};
use common_catalog::consts::{trace_operations_table_name, trace_services_table_name};
use common_grpc::precision::Precision;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use pipeline::{GreptimePipelineParams, PipelineWay};
use session::context::QueryContextRef;

use crate::error::Result;
use crate::otlp::trace::span::{TraceSpan, parse};
use crate::otlp::trace::{
    DURATION_NANO_COLUMN, PARENT_SPAN_ID_COLUMN, SERVICE_NAME_COLUMN, SPAN_ATTRIBUTES_COLUMN,
    SPAN_EVENTS_COLUMN, SPAN_ID_COLUMN, SPAN_KIND_COLUMN, SPAN_NAME_COLUMN, SPAN_STATUS_CODE,
    SPAN_STATUS_MESSAGE_COLUMN, TIMESTAMP_COLUMN, TRACE_ID_COLUMN, TRACE_STATE_COLUMN,
};
use crate::otlp::utils::{make_column_data, make_string_column_data};
use crate::query_handler::PipelineHandlerRef;
use crate::row_writer::{self, MultiTableData, TableData};

const APPROXIMATE_COLUMN_COUNT: usize = 24;

// Use a timestamp(2100-01-01 00:00:00) as large as possible.
const MAX_TIMESTAMP: i64 = 4102444800000000000;

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

            // Collect operations (service_name + span_name + span_kind).
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
        make_string_column_data(TRACE_ID_COLUMN, Some(span.trace_id)),
        make_string_column_data(SPAN_ID_COLUMN, Some(span.span_id)),
        make_string_column_data(PARENT_SPAN_ID_COLUMN, span.parent_span_id),
        make_string_column_data(SPAN_KIND_COLUMN, Some(span.span_kind)),
        make_string_column_data(SPAN_NAME_COLUMN, Some(span.span_name)),
        make_string_column_data(SPAN_STATUS_CODE, Some(span.span_status_code)),
        make_string_column_data(SPAN_STATUS_MESSAGE_COLUMN, Some(span.span_status_message)),
        make_string_column_data(TRACE_STATE_COLUMN, Some(span.trace_state)),
    ];
    row_writer::write_fields(writer, fields.into_iter(), &mut row)?;

    if let Some(service_name) = span.service_name {
        row_writer::write_tag(writer, SERVICE_NAME_COLUMN, service_name, &mut row)?;
    }

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
        make_string_column_data("scope_name", Some(span.scope_name)),
        make_string_column_data("scope_version", Some(span.scope_version)),
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
        row_writer::write_fields(
            writer,
            std::iter::once(make_string_column_data(
                SERVICE_NAME_COLUMN,
                Some(service_name),
            )),
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

        // Write the `service_name`, `span_name`, and `span_kind` columns.
        row_writer::write_fields(
            writer,
            vec![
                make_string_column_data(SERVICE_NAME_COLUMN, Some(service_name)),
                make_string_column_data(SPAN_NAME_COLUMN, Some(span_name)),
                make_string_column_data(SPAN_KIND_COLUMN, Some(span_kind)),
            ]
            .into_iter(),
            &mut row,
        )?;
        writer.add_row(row);
    }

    Ok(())
}
