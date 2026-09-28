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

use std::collections::{BTreeMap, HashSet};

use opentelemetry_proto::tonic::common::v1::any_value;
use opentelemetry_proto::tonic::trace::v1::status::StatusCode;
use opentelemetry_proto::tonic::trace::v1::{Span, TracesData};
use serde_json::{Value, json};

use crate::otlp::trace::KEY_SERVICE_NAME;

pub fn summarize(traces: TracesData) -> Vec<Value> {
    let mut by_trace: BTreeMap<&[u8], Vec<(&str, &Span)>> = BTreeMap::new();
    for resource in &traces.resource_spans {
        let service = resource
            .resource
            .as_ref()
            .and_then(|resource| {
                resource
                    .attributes
                    .iter()
                    .find(|attr| attr.key == KEY_SERVICE_NAME)
                    .and_then(|attr| attr.value.as_ref())
                    .and_then(|value| match &value.value {
                        Some(any_value::Value::StringValue(name)) => Some(name.as_str()),
                        _ => None,
                    })
            })
            .unwrap_or_default();
        for scope in &resource.scope_spans {
            for span in &scope.spans {
                by_trace
                    .entry(&span.trace_id)
                    .or_default()
                    .push((service, span));
            }
        }
    }
    by_trace
        .into_iter()
        .map(|(trace_id, spans)| {
            let ids: HashSet<_> = spans
                .iter()
                .map(|(_, span)| span.span_id.as_slice())
                .collect();
            let mut services: BTreeMap<&str, (usize, usize)> = BTreeMap::new();
            let mut min_start = u64::MAX;
            let mut max_end = 0;
            let mut errors = 0;
            let mut orphans = 0;
            let mut root: Option<(&str, &Span)> = None;
            for &(service, span) in &spans {
                let stats = services.entry(service).or_default();
                stats.0 += 1;
                if span
                    .status
                    .as_ref()
                    .is_some_and(|status| status.code == StatusCode::Error as i32)
                {
                    stats.1 += 1;
                    errors += 1;
                }
                min_start = min_start.min(span.start_time_unix_nano);
                max_end = max_end.max(span.end_time_unix_nano);
                if span.parent_span_id.iter().all(|byte| *byte == 0) {
                    if root.is_none_or(|(_, root)| {
                        span.start_time_unix_nano < root.start_time_unix_nano
                    }) {
                        root = Some((service, span));
                    }
                } else if !ids.contains(span.parent_span_id.as_slice()) {
                    orphans += 1;
                }
            }
            let (root_service, root_operation) =
                root.map_or(("", ""), |(service, span)| (service, span.name.as_str()));
            let services: Vec<_> = services
                .into_iter()
                .map(|(name, (spans, errors))| {
                    json!({
                        "name": name, "spanCount": spans, "errorSpanCount": errors,
                    })
                })
                .collect();
            json!({
                "traceId": hex::encode(trace_id),
                "rootServiceName": root_service,
                "rootOperationName": root_operation,
                "minStartTimeUnixNano": min_start.to_string(),
                "maxEndTimeUnixNano": max_end.to_string(),
                "spanCount": spans.len(),
                "errorSpanCount": errors,
                "orphanSpanCount": orphans,
                "services": services,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_summary_roots_and_orphans() {
        let traces: TracesData = serde_json::from_value(json!({"resourceSpans":[
            {"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"b"}}]},"scopeSpans":[{"spans":[
                {"traceId":"00000000000000000000000000000001","spanId":"0000000000000001","name":"late-root","startTimeUnixNano":"20","endTimeUnixNano":"50"},
                {"traceId":"00000000000000000000000000000001","spanId":"0000000000000002","parentSpanId":"0000000000000099","name":"orphan","startTimeUnixNano":"1","endTimeUnixNano":"60","status":{"code":2}},
                {"traceId":"00000000000000000000000000000002","spanId":"0000000000000003","parentSpanId":"0000000000000001","startTimeUnixNano":"1","endTimeUnixNano":"2"}
            ]}]},
            {"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"a"}}]},"scopeSpans":[{"spans":[
                {"traceId":"00000000000000000000000000000001","spanId":"0000000000000004","parentSpanId":"0000000000000000","name":"early-root","startTimeUnixNano":"10","endTimeUnixNano":"20"},
                {"traceId":"00000000000000000000000000000001","spanId":"0000000000000005","parentSpanId":"0000000000000004","startTimeUnixNano":"11","endTimeUnixNano":"15","status":{"code":2}}
            ]}]}
        ]})).unwrap();
        let summaries = summarize(traces);
        assert_eq!(
            summaries[0],
            json!({
                "traceId":"00000000000000000000000000000001", "rootServiceName":"a", "rootOperationName":"early-root",
                "minStartTimeUnixNano":"1", "maxEndTimeUnixNano":"60", "spanCount":4, "errorSpanCount":2, "orphanSpanCount":1,
                "services":[{"name":"a","spanCount":2,"errorSpanCount":1},{"name":"b","spanCount":2,"errorSpanCount":1}]
            })
        );
        assert_eq!(summaries[1]["orphanSpanCount"], 1);
        assert_eq!(summaries[1]["rootServiceName"], "");
        assert_eq!(summaries[1]["rootOperationName"], "");
    }
}
