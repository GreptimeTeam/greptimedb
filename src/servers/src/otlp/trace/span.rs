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

use common_time::timestamp::Timestamp;
use itertools::Itertools;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{InstrumentationScope, any_value};
use opentelemetry_proto::tonic::trace::v1::span::{Event, Link};
use opentelemetry_proto::tonic::trace::v1::{Span, Status};
use serde::Serialize;

use crate::otlp::trace::KEY_SERVICE_NAME;
use crate::otlp::trace::attributes::{Attributes, SharedAttributes};
use crate::otlp::utils::bytes_to_hex_string;

#[derive(Debug, Clone)]
pub struct TraceSpan {
    // the following are tags
    pub service_name: Option<String>,
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: Option<String>,

    // the following are fields
    pub resource_attributes: SharedAttributes,
    pub scope_name: String,
    pub scope_version: String,
    pub scope_attributes: SharedAttributes,
    pub trace_state: String,
    pub span_name: String,
    pub span_kind: String,
    pub span_status_code: String,
    pub span_status_message: String,
    pub span_attributes: Attributes,
    pub span_events: SpanEvents,  // TODO(yuanbohan): List in the future
    pub span_links: SpanLinks,    // TODO(yuanbohan): List in the future
    pub start_in_nanosecond: u64, // this is also the Timestamp Index
    pub end_in_nanosecond: u64,
}

pub type TraceSpans = Vec<TraceSpan>;

#[derive(Debug, Clone)]
pub struct TraceSpanGroup {
    pub service_name: Option<String>,
    pub resource_attributes: SharedAttributes,
    pub scope_name: String,
    pub scope_version: String,
    pub scope_attributes: SharedAttributes,
    pub spans: TraceSpans,
}

pub type TraceSpanGroups = Vec<TraceSpanGroup>;

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

impl From<SpanLink> for jsonb::Value<'static> {
    fn from(value: SpanLink) -> jsonb::Value<'static> {
        jsonb::Value::Object(
            vec![
                (
                    "trace_id".to_string(),
                    jsonb::Value::String(value.trace_id.into()),
                ),
                (
                    "span_id".to_string(),
                    jsonb::Value::String(value.span_id.into()),
                ),
                (
                    "trace_state".to_string(),
                    jsonb::Value::String(value.trace_state.into()),
                ),
                ("attributes".to_string(), value.attributes.into()),
            ]
            .into_iter()
            .collect(),
        )
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

impl From<SpanLinks> for jsonb::Value<'static> {
    fn from(value: SpanLinks) -> jsonb::Value<'static> {
        jsonb::Value::Array(value.0.into_iter().map(Into::into).collect())
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

impl From<SpanEvent> for jsonb::Value<'static> {
    fn from(value: SpanEvent) -> jsonb::Value<'static> {
        jsonb::Value::Object(
            vec![
                ("name".to_string(), jsonb::Value::String(value.name.into())),
                ("time".to_string(), jsonb::Value::String(value.time.into())),
                ("attributes".to_string(), value.attributes.into()),
            ]
            .into_iter()
            .collect(),
        )
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

impl From<SpanEvents> for jsonb::Value<'static> {
    fn from(value: SpanEvents) -> jsonb::Value<'static> {
        jsonb::Value::Array(value.0.into_iter().map(Into::into).collect())
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
    service_name: Option<String>,
    resource_attrs: &SharedAttributes,
    scope_name: &str,
    scope_version: &str,
    scope_attrs: &SharedAttributes,
    span: Span,
) -> TraceSpan {
    let (span_status_code, span_status_message) = status_to_string(&span.status);
    let span_kind = span.kind().as_str_name().into();
    TraceSpan {
        service_name,
        trace_id: bytes_to_hex_string(&span.trace_id),
        span_id: bytes_to_hex_string(&span.span_id),
        parent_span_id: if span.parent_span_id.is_empty() {
            None
        } else {
            Some(bytes_to_hex_string(&span.parent_span_id))
        },

        resource_attributes: resource_attrs.clone(),
        trace_state: span.trace_state,

        scope_name: scope_name.to_string(),
        scope_version: scope_version.to_string(),
        scope_attributes: scope_attrs.clone(),

        span_name: span.name,
        span_kind,
        span_status_code,
        span_status_message,
        span_attributes: Attributes::from(span.attributes),
        span_events: SpanEvents::from(span.events),
        span_links: SpanLinks::from(span.links),

        start_in_nanosecond: span.start_time_unix_nano,
        end_in_nanosecond: span.end_time_unix_nano,
    }
}

pub fn status_to_string(status: &Option<Status>) -> (String, String) {
    match status {
        Some(status) => (status.code().as_str_name().into(), status.message.clone()),
        None => ("".into(), "".into()),
    }
}

/// Convert OpenTelemetry traces to SpanTraces
///
/// See
/// <https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/trace/v1/trace.proto>
/// for data structure of OTLP traces.
pub fn parse(request: ExportTraceServiceRequest) -> TraceSpanGroups {
    let group_size = request
        .resource_spans
        .iter()
        .flat_map(|res| res.scope_spans.iter())
        .count();
    let mut groups = Vec::with_capacity(group_size);
    for resource_spans in request.resource_spans {
        let resource_attrs = resource_spans
            .resource
            .map(|r| r.attributes)
            .unwrap_or_default();

        // "service.name" is required; SDKs MUST provide a fallback when it is unset.
        // This is in the specification:
        // https://opentelemetry.io/docs/specs/semconv/resource/service/
        // Tolerate missing names from senders without using unrelated attributes as names.
        let service_name = resource_attrs
            .iter()
            .find(|kv| kv.key == KEY_SERVICE_NAME)
            .and_then(|kv| kv.value.clone())
            .and_then(|v| match v.value {
                Some(any_value::Value::StringValue(s)) => Some(s),
                Some(any_value::Value::BytesValue(b)) => {
                    Some(String::from_utf8_lossy(&b).to_string())
                }
                _ => None,
            });

        let resource_attrs = SharedAttributes::from(Attributes::from(resource_attrs));
        for scope_spans in resource_spans.scope_spans {
            let InstrumentationScope {
                name: scope_name,
                version: scope_version,
                attributes: scope_attrs,
                ..
            } = scope_spans.scope.unwrap_or_default();
            let scope_attrs = SharedAttributes::from(Attributes::from(scope_attrs));
            let mut spans = Vec::with_capacity(scope_spans.spans.len());
            for span in scope_spans.spans {
                spans.push(parse_span(
                    service_name.clone(),
                    &resource_attrs,
                    &scope_name,
                    &scope_version,
                    &scope_attrs,
                    span,
                ));
            }
            groups.push(TraceSpanGroup {
                service_name: service_name.clone(),
                resource_attributes: resource_attrs.clone(),
                scope_name,
                scope_version,
                scope_attributes: scope_attrs,
                spans,
            });
        }
    }
    groups
}

#[cfg(test)]
mod tests {
    use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
    use opentelemetry_proto::tonic::common::v1::{
        AnyValue, InstrumentationScope, KeyValue, any_value,
    };
    use opentelemetry_proto::tonic::resource::v1::Resource;
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span, Status};

    use crate::otlp::trace::KEY_SERVICE_NAME;
    use crate::otlp::trace::span::{bytes_to_hex_string, parse, status_to_string};

    fn make_kv(key: &str, value: &str) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(value.to_string())),
            }),
            ..Default::default()
        }
    }

    fn make_span(trace_id: u8, span_id: u8) -> Span {
        Span {
            trace_id: vec![trace_id; 16],
            span_id: vec![span_id; 8],
            ..Default::default()
        }
    }

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

    #[test]
    fn test_parse_preserves_resource_scope_groups() {
        let request = ExportTraceServiceRequest {
            resource_spans: vec![
                ResourceSpans {
                    resource: Some(Resource {
                        attributes: vec![make_kv(KEY_SERVICE_NAME, "svc-a")],
                        ..Default::default()
                    }),
                    scope_spans: vec![
                        ScopeSpans {
                            scope: Some(InstrumentationScope {
                                name: "scope-1".to_string(),
                                attributes: vec![make_kv("scope.key", "scope-1-value")],
                                ..Default::default()
                            }),
                            spans: vec![make_span(0x11, 0x21), make_span(0x12, 0x22)],
                            ..Default::default()
                        },
                        ScopeSpans {
                            scope: Some(InstrumentationScope {
                                name: "scope-2".to_string(),
                                attributes: vec![make_kv("scope.key", "scope-2-value")],
                                ..Default::default()
                            }),
                            spans: vec![make_span(0x13, 0x23)],
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                },
                ResourceSpans {
                    resource: Some(Resource {
                        attributes: vec![make_kv(KEY_SERVICE_NAME, "svc-b")],
                        ..Default::default()
                    }),
                    scope_spans: vec![ScopeSpans {
                        scope: Some(InstrumentationScope {
                            name: "scope-3".to_string(),
                            ..Default::default()
                        }),
                        spans: vec![make_span(0x14, 0x24)],
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            ],
        };

        let groups = parse(request);
        assert_eq!(groups.len(), 3);
        assert_eq!(groups[0].service_name.as_deref(), Some("svc-a"));
        assert_eq!(groups[0].scope_name, "scope-1");
        assert_eq!(groups[0].spans.len(), 2);
        assert_eq!(groups[1].scope_name, "scope-2");
        assert_eq!(groups[1].spans.len(), 1);
        assert_eq!(groups[2].service_name.as_deref(), Some("svc-b"));
        assert_eq!(groups[2].scope_name, "scope-3");
        assert_eq!(
            groups[0].scope_attributes.as_ref().get_ref(),
            &[make_kv("scope.key", "scope-1-value")]
        );

        // Scopes of one resource share a single copy of the resource attributes,
        // spans share their group's attributes, and unrelated resources and
        // scopes stay separate.
        assert!(std::ptr::eq(
            groups[0].resource_attributes.as_ref(),
            groups[1].resource_attributes.as_ref(),
        ));
        assert!(!std::ptr::eq(
            groups[0].resource_attributes.as_ref(),
            groups[2].resource_attributes.as_ref(),
        ));
        assert!(!std::ptr::eq(
            groups[0].scope_attributes.as_ref(),
            groups[1].scope_attributes.as_ref(),
        ));
        for group in &groups {
            for span in &group.spans {
                assert!(std::ptr::eq(
                    group.resource_attributes.as_ref(),
                    span.resource_attributes.as_ref()
                ));
                assert!(std::ptr::eq(
                    group.scope_attributes.as_ref(),
                    span.scope_attributes.as_ref()
                ));
            }
        }
    }
}
