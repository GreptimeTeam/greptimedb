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

use std::collections::HashMap;

use api::v1::value::ValueData;
use api::v1::{ColumnDataType, RowInsertRequests, Value};
use common_grpc::writer::Precision;
use common_time::time::Time;
use itertools::Itertools;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value as OtlpValue;
use opentelemetry_proto::tonic::common::v1::{
    AnyValue, ArrayValue, InstrumentationScope, KeyValue, KeyValueList,
};
use opentelemetry_proto::tonic::trace::v1::span::{Event, Link};
use opentelemetry_proto::tonic::trace::v1::{Span, Status};
use serde_json::json;

use super::{GREPTIME_TIMESTAMP, GREPTIME_VALUE};
use crate::error::Result;
use crate::row_writer::{self, MultiTableData, TableData};

const APPROXIMATE_COLUMN_COUNT: usize = 16;
const TRACE_TABLE_NAME: &str = "traces_preview_v01";

/// Convert OpenTelemetry traces to GreptimeDB row insert requests
///
/// See
/// <https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/trace/v1/trace.proto>
/// for data structure of OTLP metrics.
///
/// Returns `InsertRequests` and total number of rows to ingest
pub fn to_grpc_insert_requests(
    request: ExportTraceServiceRequest,
) -> Result<(RowInsertRequests, usize)> {
    let mut multi_table_writer = MultiTableData::default();
    let one_table_writer = multi_table_writer.get_or_default_table_data(
        TRACE_TABLE_NAME,
        APPROXIMATE_COLUMN_COUNT,
        APPROXIMATE_COLUMN_COUNT,
    );
    for resource_spans in request.resource_spans {
        let resource_attrs = resource_spans
            .resource
            .map(|r| r.attributes)
            .unwrap_or_default();
        for scope_spans in resource_spans.scope_spans {
            let scope = scope_spans.scope.unwrap_or_default();
            for span in scope_spans.spans {
                let row = one_table_writer.alloc_one_row();
                write_span(one_table_writer, row, &resource_attrs, &scope, span)?;
            }
        }
    }

    Ok(multi_table_writer.into_row_insert_requests())
}

fn write_span(
    writer: &mut TableData,
    mut row: Vec<Value>,
    resource_attrs: &[KeyValue],
    scope: &InstrumentationScope,
    span: Span,
) -> Result<()> {
    write_span_tags(writer, &mut row, &span)?;
    write_span_str_fields(writer, &mut row, resource_attrs, scope, &span)?;
    write_span_time_fields(writer, &mut row, &span)?;
    write_span_value(writer, &mut row, &span)?;
    write_span_timestamp(writer, &mut row, &span)?;

    writer.add_row(row);
    Ok(())
}

fn write_span_tags(writer: &mut TableData, row: &mut Vec<Value>, span: &Span) -> Result<()> {
    let iter = vec![
        ("trace_id", bytes_to_hex_string(&span.trace_id)),
        ("span_id", bytes_to_hex_string(&span.span_id)),
        ("parent_span_id", bytes_to_hex_string(&span.parent_span_id)),
    ]
    .into_iter()
    .map(|(col, val)| (col.into(), val));

    row_writer::write_tags(writer, iter, row)
}

pub fn bytes_to_hex_string(bs: &[u8]) -> String {
    bs.iter().map(|b| format!("{:02x}", b)).join("")
}

pub fn arr_vals_to_string(arr: &ArrayValue) -> String {
    let vs: Vec<String> = arr
        .values
        .iter()
        .filter_map(|val| any_value_to_string(val.clone()))
        .collect();

    serde_json::to_string(&vs).unwrap_or_else(|_| "[]".into())
}

pub fn vec_kv_to_string(vec: &[KeyValue]) -> String {
    let vs: HashMap<String, String> = vec
        .iter()
        .map(|kv| {
            let val = kv
                .value
                .clone()
                .and_then(any_value_to_string)
                .unwrap_or_default();
            (kv.key.clone(), val)
        })
        .collect();

    serde_json::to_string(&vs).unwrap_or_else(|_| "{}".into())
}
pub fn kvlist_to_string(kvlist: &KeyValueList) -> String {
    vec_kv_to_string(&kvlist.values)
}

pub fn any_value_to_string(val: AnyValue) -> Option<String> {
    val.value.map(|value| match value {
        OtlpValue::StringValue(s) => s,
        OtlpValue::BoolValue(b) => b.to_string(),
        OtlpValue::IntValue(i) => i.to_string(),
        OtlpValue::DoubleValue(d) => d.to_string(),
        OtlpValue::ArrayValue(arr) => arr_vals_to_string(&arr),
        OtlpValue::KvlistValue(kv) => kvlist_to_string(&kv),
        OtlpValue::BytesValue(bs) => bytes_to_hex_string(&bs),
    })
}

pub fn event_to_string(event: &Event) -> String {
    json!({
        "name": event.name,
        "time": Time::new_nanosecond(event.time_unix_nano as i64).to_iso8601_string(),
        "attrs": vec_kv_to_string(&event.attributes),
    })
    .to_string()
}

pub fn events_to_string(events: &[Event]) -> String {
    let v: Vec<String> = events.iter().map(event_to_string).collect();
    serde_json::to_string(&v).unwrap_or_else(|_| "[]".into())
}

pub fn link_to_string(link: &Link) -> String {
    json!({
        "trace_id": link.trace_id,
        "span_id": link.span_id,
        "trace_state": link.trace_state,
        "attributes": vec_kv_to_string(&link.attributes),
    })
    .to_string()
}

pub fn links_to_string(links: &[Link]) -> String {
    let v: Vec<String> = links.iter().map(link_to_string).collect();
    serde_json::to_string(&v).unwrap_or_else(|_| "[]".into())
}

pub fn status_to_string(status: &Option<Status>) -> (String, String) {
    match status {
        Some(status) => (status.code().as_str_name().into(), status.message.clone()),
        None => ("".into(), "".into()),
    }
}

fn write_span_str_fields(
    writer: &mut TableData,
    row: &mut Vec<Value>,
    resource_attrs: &[KeyValue],
    scope: &InstrumentationScope,
    span: &Span,
) -> Result<()> {
    let (code, message) = status_to_string(&span.status);
    let iter = vec![
        ("resource", vec_kv_to_string(resource_attrs)),
        ("scope_name", scope.name.clone()),
        ("scope_version", scope.version.clone()),
        ("scope_attributes", vec_kv_to_string(&scope.attributes)),
        ("trace_state", span.trace_state.clone()),
        ("span_name", span.name.clone()),
        ("span_kind", span.kind().as_str_name().into()),
        ("span_status_code", code),
        ("span_status_message", message),
        ("span_attributes", vec_kv_to_string(&span.attributes)),
        ("span_events", events_to_string(&span.events)),
        ("span_links", links_to_string(&span.links)),
    ]
    .into_iter()
    .map(|(col, val)| {
        (
            col.into(),
            ColumnDataType::String,
            ValueData::StringValue(val),
        )
    });

    row_writer::write_fields(writer, iter, row)
}

fn write_span_time_fields(writer: &mut TableData, row: &mut Vec<Value>, span: &Span) -> Result<()> {
    let iter = vec![
        ("start", span.start_time_unix_nano),
        ("end", span.end_time_unix_nano),
    ]
    .into_iter()
    .map(|(col, val)| {
        (
            col.into(),
            ColumnDataType::TimestampNanosecond,
            ValueData::TimestampNanosecondValue(val as i64),
        )
    });

    row_writer::write_fields(writer, iter, row)
}

// duration in milliseconds as the value
fn write_span_value(writer: &mut TableData, row: &mut Vec<Value>, span: &Span) -> Result<()> {
    row_writer::write_f64(
        writer,
        GREPTIME_VALUE,
        (span.end_time_unix_nano - span.start_time_unix_nano) as f64 / 1_000_000.0,
        row,
    )
}

fn write_span_timestamp(writer: &mut TableData, row: &mut Vec<Value>, span: &Span) -> Result<()> {
    row_writer::write_ts_precision(
        writer,
        GREPTIME_TIMESTAMP,
        Some(span.start_time_unix_nano as i64),
        Precision::Nanosecond,
        row,
    )
}
