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

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct TraceSpan {
    // the following are tags
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: String,

    // the following are fields
    pub resource_attributes: String, // TODO(yuanbohan): Map in the future
    pub scope_name: String,
    pub scope_version: String,
    pub scope_attributes: String, // TODO(yuanbohan): Map in the future
    pub trace_state: String,
    pub span_name: String,
    pub span_kind: String,
    pub span_status_code: String,
    pub span_status_message: String,
    pub span_attributes: String,  // TODO(yuanbohan): Map in the future
    pub span_events: String,      // TODO(yuanbohan): List in the future
    pub span_links: String,       // TODO(yuanbohan): List in the future
    pub start_in_nanosecond: u64, // this is also the Timestamp Index
    pub end_in_nanosecond: u64,

    pub uplift_fields: Vec<(String, ColumnDataType, ValueData)>,
}

pub type TraceSpans = Vec<TraceSpan>;

/// Convert OpenTelemetry traces to GreptimeDB row insert requests
///
/// See
/// <https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/trace/v1/trace.proto>
/// for data structure of OTLP metrics.
///
/// Returns `InsertRequests` and total number of rows to ingest
pub fn to_grpc_insert_requests(spans: TraceSpans) -> Result<(RowInsertRequests, usize)> {
    let mut multi_table_writer = MultiTableData::default();
    let one_table_writer = multi_table_writer.get_or_default_table_data(
        TRACE_TABLE_NAME,
        APPROXIMATE_COLUMN_COUNT,
        APPROXIMATE_COLUMN_COUNT,
    );

    for span in spans {
        let row = one_table_writer.alloc_one_row();
        write_span_to_row(one_table_writer, row, span)?;
    }

    Ok(multi_table_writer.into_row_insert_requests())
}

pub fn write_span_to_row(
    writer: &mut TableData,
    mut row: Vec<Value>,
    span: TraceSpan,
) -> Result<()> {
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
            ("resource_attributes", span.resource_attributes),
            ("scope_name", span.scope_name),
            ("scope_version", span.scope_version),
            ("scope_attributes", span.scope_attributes),
            ("trace_state", span.trace_state),
            ("span_name", span.span_name),
            ("span_kind", span.span_kind),
            ("span_status_code", span.span_status_code),
            ("span_status_message", span.span_status_message),
            ("span_attributes", span.span_attributes),
            ("span_events", span.span_events),
            ("span_links", span.span_links),
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

pub fn parse_span(
    resource_attrs: &[KeyValue],
    scope: &InstrumentationScope,
    span: Span,
) -> TraceSpan {
    let (code, message) = status_to_string(&span.status);
    TraceSpan {
        trace_id: bytes_to_hex_string(&span.trace_id),
        span_id: bytes_to_hex_string(&span.span_id),
        parent_span_id: bytes_to_hex_string(&span.parent_span_id),

        resource_attributes: vec_kv_to_string(resource_attrs),
        scope_name: scope.name.clone(),
        scope_version: scope.version.clone(),
        scope_attributes: vec_kv_to_string(&scope.attributes),
        trace_state: span.trace_state.clone(),
        span_name: span.name.clone(),
        span_kind: span.kind().as_str_name().into(),
        span_status_code: code,
        span_status_message: message,
        span_attributes: vec_kv_to_string(&span.attributes),
        span_events: events_to_string(&span.events),
        span_links: links_to_string(&span.links),

        start_in_nanosecond: span.start_time_unix_nano,
        end_in_nanosecond: span.end_time_unix_nano,

        uplift_fields: vec![],
    }
}

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
                spans.push(parse_span(&resource_attrs, &scope, span));
            }
        }
    }
    spans
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

#[cfg(test)]
mod tests {
    use common_time::time::Time;
    use opentelemetry_proto::tonic::common::v1::{
        any_value, AnyValue, ArrayValue, KeyValue, KeyValueList,
    };
    use opentelemetry_proto::tonic::trace::v1::span::Event;
    use opentelemetry_proto::tonic::trace::v1::Status;
    use serde_json::json;

    use crate::otlp::trace::{
        arr_vals_to_string, bytes_to_hex_string, event_to_string, kvlist_to_string,
        status_to_string, vec_kv_to_string,
    };

    #[test]
    fn test_bytes_to_hex_string() {
        assert_eq!(
            "24fe79948641b110a29bc27859307e8d",
            bytes_to_hex_string(&[
                36, 254, 121, 148, 134, 65, 177, 16, 162, 155, 194, 120, 89, 48, 126, 141,
            ])
        );

        assert_eq!(
            "baffeedd7b8debc0",
            bytes_to_hex_string(&[186, 255, 238, 221, 123, 141, 235, 192,])
        );
    }

    #[test]
    fn test_arr_vals_to_string() {
        assert_eq!("[]", arr_vals_to_string(&ArrayValue { values: vec![] }));

        let arr = ArrayValue {
            values: vec![
                AnyValue {
                    value: Some(any_value::Value::StringValue("string_value".into())),
                },
                AnyValue {
                    value: Some(any_value::Value::BoolValue(true)),
                },
                AnyValue {
                    value: Some(any_value::Value::IntValue(1)),
                },
                AnyValue {
                    value: Some(any_value::Value::DoubleValue(1.2)),
                },
            ],
        };
        let expect = json!(["string_value", "true", "1", "1.2"]).to_string();
        assert_eq!(expect, arr_vals_to_string(&arr));
    }

    #[test]
    fn test_kv_list_to_string() {
        let kvlist = KeyValueList {
            values: vec![KeyValue {
                key: "str_key".into(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue("val1".into())),
                }),
            }],
        };
        let expect = json!({
            "str_key": "val1",
        })
        .to_string();
        assert_eq!(expect, kvlist_to_string(&kvlist))
    }

    #[test]
    fn test_event_to_string() {
        let attributes = vec![KeyValue {
            key: "str_key".into(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue("val1".into())),
            }),
        }];
        let event = Event {
            time_unix_nano: 1697620662450128000_u64,
            name: "event_name".into(),
            attributes,
            dropped_attributes_count: 0,
        };
        let event_string = event_to_string(&event);
        let expect = json!({
            "name": event.name,
            "time": Time::new_nanosecond(event.time_unix_nano as i64).to_iso8601_string(),
            "attrs": vec_kv_to_string(&event.attributes),
        });

        assert_eq!(
            expect,
            serde_json::from_str::<serde_json::value::Value>(event_string.as_str()).unwrap()
        );
    }

    #[test]
    fn test_status_to_string() {
        let message = String::from("status message");
        let status = Status {
            code: 1,
            message: message.clone(),
        };

        assert_eq!(
            ("STATUS_CODE_OK".into(), message),
            status_to_string(&Some(status)),
        );
    }
}
