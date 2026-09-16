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
use common_grpc::precision::Precision;
use snafu::ensure;

use crate::error::{Result, TimestampOverflowSnafu};
use crate::otlp::trace::span::TraceSpan;
use crate::otlp::trace::v1::span_duration_nano;
use crate::otlp::trace::{
    DURATION_NANO_COLUMN, PARENT_SPAN_ID_COLUMN, RESOURCE_ATTRIBUTES_COLUMN,
    SCOPE_ATTRIBUTES_COLUMN, SCOPE_NAME_COLUMN, SCOPE_VERSION_COLUMN, SERVICE_NAME_COLUMN,
    SPAN_ATTRIBUTES_COLUMN, SPAN_ID_COLUMN, SPAN_KIND_COLUMN, SPAN_NAME_COLUMN, SPAN_STATUS_CODE,
    SPAN_STATUS_MESSAGE_COLUMN, TIMESTAMP_COLUMN, TIMESTAMP_END_COLUMN, TRACE_ID_COLUMN,
    TRACE_STATE_COLUMN,
};
use crate::otlp::utils::{make_column_data, make_string_column_data};
use crate::row_writer::{self, MultiTableData, TableData};

// Preallocate for the fixed v2 schema: 3 timing columns, 3 span/trace IDs,
// 2 kind/name columns, 2 status columns, 1 trace state, 2 scope name/version
// columns, 1 service name, and 3 JSON2 attribute columns (17 total).
// Events and links are excluded until ARRAY(JSON2) is supported.
const APPROXIMATE_COLUMN_COUNT: usize = 17;

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
    for span in spans.iter().cloned() {
        write_span_to_row(&mut writer, span)?;
    }
    Ok(writer)
}

fn write_span_to_row(writer: &mut TableData, span: TraceSpan) -> Result<()> {
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

    row_writer::write_ts_to_nanos(
        writer,
        TIMESTAMP_COLUMN,
        Some(span.start_in_nanosecond as i64),
        Precision::Nanosecond,
        &mut row,
    )?;
    row_writer::write_fields(
        writer,
        vec![
            make_column_data(
                TIMESTAMP_END_COLUMN,
                ColumnDataType::TimestampNanosecond,
                Some(ValueData::TimestampNanosecondValue(
                    span.end_in_nanosecond as i64,
                )),
            ),
            make_column_data(
                DURATION_NANO_COLUMN,
                ColumnDataType::Int64,
                Some(ValueData::I64Value(span_duration_nano(&span))),
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
        ]
        .into_iter(),
        &mut row,
    )?;
    row_writer::write_by_schema(
        writer,
        std::iter::once((
            ColumnSchema {
                column_name: SERVICE_NAME_COLUMN.to_string(),
                datatype: ColumnDataType::String as i32,
                semantic_type: SemanticType::Tag as i32,
                ..Default::default()
            },
            span.service_name.map(ValueData::StringValue),
        )),
        &mut row,
    )?;

    row_writer::write_json2(
        writer,
        SPAN_ATTRIBUTES_COLUMN,
        span.span_attributes,
        &mut row,
    )?;
    row_writer::write_json2(
        writer,
        SCOPE_ATTRIBUTES_COLUMN,
        span.scope_attributes,
        &mut row,
    )?;
    row_writer::write_json2(
        writer,
        RESOURCE_ATTRIBUTES_COLUMN,
        span.resource_attributes,
        &mut row,
    )?;

    // TODO(LFC): Store span_events and span_links once ARRAY(JSON2) is supported.

    writer.add_row(row);
    Ok(())
}
