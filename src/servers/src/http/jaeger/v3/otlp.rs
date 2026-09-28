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

use axum::http::StatusCode;
use opentelemetry_proto::tonic::common::v1::{
    AnyValue, ArrayValue, InstrumentationScope, KeyValue, KeyValueList, any_value,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::span::{Event, Link, SpanKind};
use opentelemetry_proto::tonic::trace::v1::status::StatusCode as SpanStatusCode;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span, Status, TracesData};
use serde_json::Value;

use crate::http::HttpRecordsOutput;
use crate::http::jaeger::v3::ApiError;
use crate::otlp::trace::{
    KEY_SERVICE_NAME, PARENT_SPAN_ID_COLUMN, RESOURCE_ATTRIBUTES_COLUMN, SCOPE_ATTRIBUTES_COLUMN,
    SCOPE_NAME_COLUMN, SCOPE_VERSION_COLUMN, SERVICE_NAME_COLUMN, SPAN_ATTRIBUTES_COLUMN,
    SPAN_EVENTS_COLUMN, SPAN_ID_COLUMN, SPAN_KIND_COLUMN, SPAN_LINKS_COLUMN, SPAN_NAME_COLUMN,
    SPAN_STATUS_CODE, SPAN_STATUS_MESSAGE_COLUMN, TIMESTAMP_COLUMN, TIMESTAMP_END_COLUMN,
    TRACE_ID_COLUMN, TRACE_STATE_COLUMN,
};

/// Converts stored trace rows directly to OTLP, without the legacy microsecond conversion.
pub fn traces_from_records(records: HttpRecordsOutput) -> Result<TracesData, ApiError> {
    let mut resource_spans = Vec::with_capacity(records.rows.len());
    for row in &records.rows {
        let cells: HashMap<_, _> = records
            .schema
            .column_schemas
            .iter()
            .zip(row)
            .map(|(column, cell)| (column.name.as_str(), cell))
            .collect();
        let string = |name| cells.get(name).and_then(|v| v.as_str()).unwrap_or_default();
        let timestamp = |name| {
            cells
                .get(name)
                .and_then(|v| v.as_u64())
                .ok_or_else(|| invalid_data(format!("Invalid timestamp column: {name}")))
        };
        let span = Span {
            trace_id: decode_id(string(TRACE_ID_COLUMN), 16)?,
            span_id: decode_id(string(SPAN_ID_COLUMN), 8)?,
            parent_span_id: if string(PARENT_SPAN_ID_COLUMN).is_empty() {
                vec![]
            } else {
                decode_id(string(PARENT_SPAN_ID_COLUMN), 8)?
            },
            trace_state: string(TRACE_STATE_COLUMN).into(),
            name: string(SPAN_NAME_COLUMN).into(),
            kind: SpanKind::from_str_name(string(SPAN_KIND_COLUMN)).unwrap_or(SpanKind::Unspecified)
                as i32,
            start_time_unix_nano: timestamp(TIMESTAMP_COLUMN)?,
            end_time_unix_nano: timestamp(TIMESTAMP_END_COLUMN)?,
            attributes: attributes(&cells, SPAN_ATTRIBUTES_COLUMN)?,
            events: events(cells.get(SPAN_EVENTS_COLUMN).copied())?,
            links: links(cells.get(SPAN_LINKS_COLUMN).copied())?,
            status: Some(Status {
                code: SpanStatusCode::from_str_name(string(SPAN_STATUS_CODE))
                    .unwrap_or(SpanStatusCode::Unset) as i32,
                message: string(SPAN_STATUS_MESSAGE_COLUMN).into(),
            }),
            ..Default::default()
        };
        let mut resource_attributes = attributes(&cells, RESOURCE_ATTRIBUTES_COLUMN)?;
        if !resource_attributes
            .iter()
            .any(|attr| attr.key == KEY_SERVICE_NAME)
            && !string(SERVICE_NAME_COLUMN).is_empty()
        {
            resource_attributes.push(KeyValue {
                key: KEY_SERVICE_NAME.into(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue(
                        string(SERVICE_NAME_COLUMN).into(),
                    )),
                }),
                ..Default::default()
            });
        }
        // Each row retains its own resource and scope. Service name alone does not identify
        // a resource: different instances of one service can have different attributes.
        resource_spans.push(ResourceSpans {
            resource: Some(Resource {
                attributes: resource_attributes,
                ..Default::default()
            }),
            scope_spans: vec![ScopeSpans {
                scope: Some(InstrumentationScope {
                    name: string(SCOPE_NAME_COLUMN).into(),
                    version: string(SCOPE_VERSION_COLUMN).into(),
                    attributes: attributes(&cells, SCOPE_ATTRIBUTES_COLUMN)?,
                    ..Default::default()
                }),
                spans: vec![span],
                ..Default::default()
            }],
            ..Default::default()
        });
    }
    Ok(TracesData { resource_spans })
}

fn invalid_data(message: impl Into<String>) -> ApiError {
    ApiError {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        message: message.into(),
    }
}

fn decode_id(value: &str, length: usize) -> Result<Vec<u8>, ApiError> {
    let bytes = hex::decode(value).map_err(|_| invalid_data("Invalid stored span or trace ID"))?;
    if bytes.len() != length {
        return Err(invalid_data("Invalid stored span or trace ID length"));
    }
    Ok(bytes)
}

fn attributes(cells: &HashMap<&str, &Value>, name: &str) -> Result<Vec<KeyValue>, ApiError> {
    if let Some(value) = cells.get(name) {
        return object_attributes(value);
    }
    let prefix = format!("{name}.");
    let mut attrs = cells
        .iter()
        .filter_map(|(key, value)| {
            key.strip_prefix(&prefix)
                .filter(|_| !value.is_null())
                .map(|key| (key, *value))
        })
        .map(|(key, value)| {
            Ok(KeyValue {
                key: key.into(),
                value: Some(any_value(value)?),
                ..Default::default()
            })
        })
        .collect::<Result<Vec<_>, ApiError>>()?;
    attrs.sort_unstable_by(|a, b| a.key.cmp(&b.key));
    Ok(attrs)
}

fn object_attributes(value: &Value) -> Result<Vec<KeyValue>, ApiError> {
    match value {
        Value::Null => Ok(vec![]),
        Value::Object(values) => values
            .iter()
            .map(|(key, value)| {
                Ok(KeyValue {
                    key: key.clone(),
                    value: Some(any_value(value)?),
                    ..Default::default()
                })
            })
            .collect(),
        _ => Err(invalid_data("Stored attributes must be a JSON object")),
    }
}

fn any_value(value: &Value) -> Result<AnyValue, ApiError> {
    use any_value::Value as OtlpValue;

    let value = match value {
        Value::Null => None,
        Value::String(value) => Some(OtlpValue::StringValue(value.clone())),
        Value::Bool(value) => Some(OtlpValue::BoolValue(*value)),
        Value::Number(value) => Some(if let Some(value) = value.as_i64() {
            OtlpValue::IntValue(value)
        } else {
            OtlpValue::DoubleValue(
                value
                    .as_f64()
                    .ok_or_else(|| invalid_data("Invalid numeric attribute"))?,
            )
        }),
        Value::Array(values) => Some(OtlpValue::ArrayValue(ArrayValue {
            values: values.iter().map(any_value).collect::<Result<_, _>>()?,
        })),
        Value::Object(_) => Some(OtlpValue::KvlistValue(KeyValueList {
            values: object_attributes(value)?,
        })),
    };
    Ok(AnyValue { value })
}

fn events(value: Option<&Value>) -> Result<Vec<Event>, ApiError> {
    let Some(value) = value.filter(|value| !value.is_null()) else {
        return Ok(vec![]);
    };
    let values = value
        .as_array()
        .ok_or_else(|| invalid_data("Invalid stored span events"))?;
    values
        .iter()
        .map(|event| {
            let time = event["time"]
                .as_str()
                .ok_or_else(|| invalid_data("Missing span event time"))?;
            let time = chrono::DateTime::parse_from_str(time, "%Y-%m-%d %H:%M:%S%.f%z")
                .ok()
                .and_then(|time| time.timestamp_nanos_opt())
                .and_then(|time| u64::try_from(time).ok())
                .ok_or_else(|| invalid_data("Invalid stored span event time"))?;
            Ok(Event {
                time_unix_nano: time,
                name: event["name"]
                    .as_str()
                    .ok_or_else(|| invalid_data("Missing span event name"))?
                    .into(),
                attributes: object_attributes(&event["attributes"])?,
                ..Default::default()
            })
        })
        .collect()
}

fn links(value: Option<&Value>) -> Result<Vec<Link>, ApiError> {
    let Some(value) = value.filter(|value| !value.is_null()) else {
        return Ok(vec![]);
    };
    let values = value
        .as_array()
        .ok_or_else(|| invalid_data("Invalid stored span links"))?;
    values
        .iter()
        .map(|link| {
            Ok(Link {
                trace_id: decode_id(link["trace_id"].as_str().unwrap_or_default(), 16)?,
                span_id: decode_id(link["span_id"].as_str().unwrap_or_default(), 8)?,
                trace_state: link["trace_state"].as_str().unwrap_or_default().into(),
                attributes: object_attributes(&link["attributes"])?,
                ..Default::default()
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn test_event_timestamp_precision() {
        for (time, expected) in [
            ("1970-01-01 00:00:01+0000", 1_000_000_000),
            ("1970-01-01 00:00:01.123+0000", 1_123_000_000),
            ("1970-01-01 00:00:01.123456+0000", 1_123_456_000),
            ("1970-01-01 00:00:01.123456789+0000", 1_123_456_789),
        ] {
            let value = json!([{"name":"event", "time":time, "attributes":{}}]);
            let events = events(Some(&value)).unwrap();
            assert_eq!(events[0].time_unix_nano, expected);
        }
    }
}
