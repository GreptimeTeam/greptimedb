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
use api::v1::{ColumnDataType, ColumnSchema, RowInsertRequests, SemanticType};
use snafu::ensure;

use crate::error::{Result, TimestampOverflowSnafu};
use crate::otlp::trace::span::TraceSpan;
use crate::otlp::trace::v1::span_duration_nano;
use crate::otlp::trace::{
    DURATION_NANO_COLUMN, PARENT_SPAN_ID_COLUMN, RESOURCE_ATTRIBUTES_COLUMN,
    SCOPE_ATTRIBUTES_COLUMN, SCOPE_NAME_COLUMN, SCOPE_VERSION_COLUMN, SERVICE_NAME_COLUMN,
    SPAN_ATTRIBUTES_COLUMN, SPAN_EVENTS_COLUMN, SPAN_ID_COLUMN, SPAN_KIND_COLUMN,
    SPAN_LINKS_COLUMN, SPAN_NAME_COLUMN, SPAN_STATUS_CODE, SPAN_STATUS_MESSAGE_COLUMN,
    TIMESTAMP_COLUMN, TIMESTAMP_END_COLUMN, TRACE_ID_COLUMN, TRACE_STATE_COLUMN,
};
use crate::row_writer::{self, MultiTableData, TableData};

// Preallocate for the fixed v2 schema: 3 timing columns, 3 span/trace IDs,
// 2 kind/name columns, 2 status columns, 1 trace state, 2 scope name/version
// columns, 1 service name, 3 JSON2 attribute columns, and 2 JSON event/link columns.
const APPROXIMATE_COLUMN_COUNT: usize = 19;

struct FixedTraceColumnIndexes {
    timestamp: usize,
    timestamp_end: usize,
    duration_nano: usize,
    parent_span_id: usize,
    trace_id: usize,
    span_id: usize,
    span_kind: usize,
    span_name: usize,
    span_status_code: usize,
    span_status_message: usize,
    trace_state: usize,
    scope_name: usize,
    scope_version: usize,
    service_name: usize,
    span_attributes: usize,
    scope_attributes: usize,
    resource_attributes: usize,
    span_events: usize,
    span_links: usize,
}

impl FixedTraceColumnIndexes {
    fn resolve(writer: &mut TableData) -> Result<Self> {
        let timestamp = writer.ensure_column(api::v1::helper::time_index_column_schema(
            TIMESTAMP_COLUMN,
            ColumnDataType::TimestampNanosecond,
        ))?;
        let mut field = |name: &str, datatype: ColumnDataType| {
            writer.ensure_column(ColumnSchema {
                column_name: name.to_string(),
                datatype: datatype as i32,
                semantic_type: SemanticType::Field as i32,
                ..Default::default()
            })
        };
        Ok(Self {
            timestamp,
            timestamp_end: field(TIMESTAMP_END_COLUMN, ColumnDataType::TimestampNanosecond)?,
            duration_nano: field(DURATION_NANO_COLUMN, ColumnDataType::Int64)?,
            parent_span_id: field(PARENT_SPAN_ID_COLUMN, ColumnDataType::String)?,
            trace_id: field(TRACE_ID_COLUMN, ColumnDataType::String)?,
            span_id: field(SPAN_ID_COLUMN, ColumnDataType::String)?,
            span_kind: field(SPAN_KIND_COLUMN, ColumnDataType::String)?,
            span_name: field(SPAN_NAME_COLUMN, ColumnDataType::String)?,
            span_status_code: field(SPAN_STATUS_CODE, ColumnDataType::String)?,
            span_status_message: field(SPAN_STATUS_MESSAGE_COLUMN, ColumnDataType::String)?,
            trace_state: field(TRACE_STATE_COLUMN, ColumnDataType::String)?,
            scope_name: field(SCOPE_NAME_COLUMN, ColumnDataType::String)?,
            scope_version: field(SCOPE_VERSION_COLUMN, ColumnDataType::String)?,
            service_name: writer.ensure_column(ColumnSchema {
                column_name: SERVICE_NAME_COLUMN.to_string(),
                datatype: ColumnDataType::String as i32,
                semantic_type: SemanticType::Tag as i32,
                ..Default::default()
            })?,
            span_attributes: writer.ensure_column(row_writer::build_json2_column_schema(
                SPAN_ATTRIBUTES_COLUMN,
            ))?,
            scope_attributes: writer.ensure_column(row_writer::build_json2_column_schema(
                SCOPE_ATTRIBUTES_COLUMN,
            ))?,
            resource_attributes: writer.ensure_column(row_writer::build_json2_column_schema(
                RESOURCE_ATTRIBUTES_COLUMN,
            ))?,
            span_events: writer
                .ensure_column(row_writer::build_json_column_schema(SPAN_EVENTS_COLUMN))?,
            span_links: writer
                .ensure_column(row_writer::build_json_column_schema(SPAN_LINKS_COLUMN))?,
        })
    }
}

/// Converts trace spans into row insert requests for the main v2 trace table.
pub(super) fn v2_to_grpc_main_insert_requests(
    spans: &[TraceSpan],
    table_name: &str,
) -> Result<(RowInsertRequests, usize)> {
    let mut tables = MultiTableData::default();
    tables.add_table_data(table_name, build_trace_table_data(spans)?);
    Ok(tables.into_row_insert_requests())
}

/// Builds the fixed row-oriented payload for the main v2 trace table.
fn build_trace_table_data(spans: &[TraceSpan]) -> Result<TableData> {
    let mut writer = TableData::new(APPROXIMATE_COLUMN_COUNT, spans.len());
    if spans.is_empty() {
        return Ok(writer);
    }
    let columns = FixedTraceColumnIndexes::resolve(&mut writer)?;
    for span in spans {
        write_span_to_row(&mut writer, span, &columns)?;
    }
    Ok(writer)
}

fn write_span_to_row(
    writer: &mut TableData,
    span: &TraceSpan,
    columns: &FixedTraceColumnIndexes,
) -> Result<()> {
    ensure!(
        span.start_in_nanosecond <= i64::MAX as u64,
        TimestampOverflowSnafu {
            error: "`span.start_in_nanosecond`",
        }
    );
    ensure!(
        span.end_in_nanosecond <= i64::MAX as u64,
        TimestampOverflowSnafu {
            error: "`span.end_in_nanosecond`",
        }
    );
    let mut row = writer.alloc_one_row();

    let duration = span_duration_nano(span);
    for (index, value) in [
        (
            columns.timestamp,
            Some(ValueData::TimestampNanosecondValue(
                span.start_in_nanosecond as i64,
            )),
        ),
        (
            columns.timestamp_end,
            Some(ValueData::TimestampNanosecondValue(
                span.end_in_nanosecond as i64,
            )),
        ),
        (columns.duration_nano, Some(ValueData::I64Value(duration))),
        (
            columns.parent_span_id,
            span.parent_span_id.clone().map(ValueData::StringValue),
        ),
        (
            columns.trace_id,
            Some(ValueData::StringValue(span.trace_id.clone())),
        ),
        (
            columns.span_id,
            Some(ValueData::StringValue(span.span_id.clone())),
        ),
        (
            columns.span_kind,
            Some(ValueData::StringValue(span.span_kind.clone())),
        ),
        (
            columns.span_name,
            Some(ValueData::StringValue(span.span_name.clone())),
        ),
        (
            columns.span_status_code,
            Some(ValueData::StringValue(span.span_status_code.clone())),
        ),
        (
            columns.span_status_message,
            Some(ValueData::StringValue(span.span_status_message.clone())),
        ),
        (
            columns.trace_state,
            Some(ValueData::StringValue(span.trace_state.clone())),
        ),
        (
            columns.scope_name,
            Some(ValueData::StringValue(span.scope_name.clone())),
        ),
        (
            columns.scope_version,
            Some(ValueData::StringValue(span.scope_version.clone())),
        ),
        (
            columns.service_name,
            span.service_name.clone().map(ValueData::StringValue),
        ),
        (
            columns.span_attributes,
            Some(row_writer::encode_json2(&span.span_attributes)?),
        ),
        (
            columns.scope_attributes,
            Some(row_writer::encode_json2(span.scope_attributes.as_ref())?),
        ),
        (
            columns.resource_attributes,
            Some(row_writer::encode_json2(span.resource_attributes.as_ref())?),
        ),
        (
            columns.span_events,
            Some(ValueData::BinaryValue(
                jsonb::Value::from(span.span_events.clone()).to_vec(),
            )),
        ),
        (
            columns.span_links,
            Some(ValueData::BinaryValue(
                jsonb::Value::from(span.span_links.clone()).to_vec(),
            )),
        ),
    ] {
        row[index].value_data = value;
    }

    writer.add_row(row);
    Ok(())
}
