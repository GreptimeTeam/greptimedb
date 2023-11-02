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

use std::fmt::Display;

use api::v1::value::ValueData;
use api::v1::ColumnDataType;
use common_time::timestamp::Timestamp;
use itertools::Itertools;
use opentelemetry_proto::tonic::common::v1::{InstrumentationScope, KeyValue};
use opentelemetry_proto::tonic::trace::v1::span::{Event, Link};
use opentelemetry_proto::tonic::trace::v1::{Span, Status};
use serde::Serialize;

use super::attributes::Attributes;

#[derive(Debug, Clone)]
pub struct TraceSpan {
    // the following are tags
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: String,

    // the following are fields
    pub resource_attributes: Attributes, // TODO(yuanbohan): Map in the future
    pub scope_name: String,
    pub scope_version: String,
    pub scope_attributes: Attributes, // TODO(yuanbohan): Map in the future
    pub trace_state: String,
    pub span_name: String,
    pub span_kind: String,
    pub span_status_code: String,
    pub span_status_message: String,
    pub span_attributes: Attributes, // TODO(yuanbohan): Map in the future
    pub span_events: SpanEvents,     // TODO(yuanbohan): List in the future
    pub span_links: SpanLinks,       // TODO(yuanbohan): List in the future
    pub start_in_nanosecond: u64,    // this is also the Timestamp Index
    pub end_in_nanosecond: u64,

    pub uplifted_span_attributes: Vec<(String, ColumnDataType, ValueData)>,
}

pub type TraceSpans = Vec<TraceSpan>;

#[derive(Debug, Clone, Serialize)]
pub struct SpanLink {
    pub trace_id: String,
    pub span_id: String,
    pub trace_state: String,
    pub attributes: Attributes, // TODO(yuanbohan): Map in the future
}

impl From<Link> for SpanLink {
    fn from(link: Link) -> Self {
        Self {
            trace_id: bytes_to_hex_string(&link.trace_id),
            span_id: bytes_to_hex_string(&link.span_id),
            trace_state: link.trace_state,
            attributes: Attributes::from(link.attributes),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct SpanLinks(Vec<SpanLink>);

impl From<Vec<Link>> for SpanLinks {
    fn from(value: Vec<Link>) -> Self {
        let links = value.into_iter().map(SpanLink::from).collect_vec();
        Self(links)
    }
}

impl Display for SpanLinks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", serde_json::to_string(self).unwrap_or_default())
    }
}

impl SpanLinks {
    pub fn get_ref(&self) -> &Vec<SpanLink> {
        &self.0
    }

    pub fn get_mut(&mut self) -> &mut Vec<SpanLink> {
        &mut self.0
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct SpanEvent {
    pub name: String,
    pub time: String,
    pub attributes: Attributes, // TODO(yuanbohan): Map in the future
}

impl From<Event> for SpanEvent {
    fn from(event: Event) -> Self {
        Self {
            name: event.name,
            time: Timestamp::new_nanosecond(event.time_unix_nano as i64).to_iso8601_string(),
            attributes: Attributes::from(event.attributes),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct SpanEvents(Vec<SpanEvent>);

impl From<Vec<Event>> for SpanEvents {
    fn from(value: Vec<Event>) -> Self {
        let events = value.into_iter().map(SpanEvent::from).collect_vec();
        Self(events)
    }
}

impl Display for SpanEvents {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", serde_json::to_string(self).unwrap_or_default())
    }
}

impl SpanEvents {
    pub fn get_ref(&self) -> &Vec<SpanEvent> {
        &self.0
    }

    pub fn get_mut(&mut self) -> &mut Vec<SpanEvent> {
        &mut self.0
    }
}

pub fn parse_span(
    resource_attrs: Vec<KeyValue>,
    scope: InstrumentationScope,
    span: Span,
) -> TraceSpan {
    let (span_status_code, span_status_message) = status_to_string(&span.status);
    let span_kind = span.kind().as_str_name().into();
    TraceSpan {
        trace_id: bytes_to_hex_string(&span.trace_id),
        span_id: bytes_to_hex_string(&span.span_id),
        parent_span_id: bytes_to_hex_string(&span.parent_span_id),

        resource_attributes: Attributes::from(resource_attrs),
        trace_state: span.trace_state,

        scope_name: scope.name,
        scope_version: scope.version,
        scope_attributes: Attributes::from(scope.attributes),

        span_name: span.name,
        span_kind,
        span_status_code,
        span_status_message,
        span_attributes: Attributes::from(span.attributes),
        span_events: SpanEvents::from(span.events),
        span_links: SpanLinks::from(span.links),

        start_in_nanosecond: span.start_time_unix_nano,
        end_in_nanosecond: span.end_time_unix_nano,

        uplifted_span_attributes: vec![],
    }
}

pub fn bytes_to_hex_string(bs: &[u8]) -> String {
    bs.iter().map(|b| format!("{:02x}", b)).join("")
}

pub fn status_to_string(status: &Option<Status>) -> (String, String) {
    match status {
        Some(status) => (status.code().as_str_name().into(), status.message.clone()),
        None => ("".into(), "".into()),
    }
}

#[cfg(test)]
mod tests {
    use opentelemetry_proto::tonic::trace::v1::Status;

    use crate::otlp::trace::span::{bytes_to_hex_string, status_to_string};

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
