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
use std::sync::Arc;

use api::v1::{RowInsertRequest, RowInsertRequests};
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{any_value, AnyValue, InstrumentationScope, KeyValue};
use opentelemetry_proto::tonic::logs::v1::LogRecord;
use pipeline::{Array, GreptimeTransformer, Map, Pipeline, Value as PipelineValue};

use super::trace::attributes::OtlpAnyValue;
use crate::error::Result;
use crate::otlp::trace::span::bytes_to_hex_string;

/// Normalize otlp instrumentation, metric and attribute names
///
/// <https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/metrics/api.md#instrument-name-syntax>
/// - since the name are case-insensitive, we transform them to lowercase for
/// better sql usability
/// - replace `.` and `-` with `_`
fn normalize_otlp_name(name: &str) -> String {
    name.to_lowercase().replace(|c| c == '.' || c == '-', "_")
}

/// Convert OpenTelemetry metrics to GreptimeDB insert requests
///
/// See
/// <https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/metrics/v1/metrics.proto>
/// for data structure of OTLP metrics.
///
/// Returns `InsertRequests` and total number of rows to ingest
pub fn to_grpc_insert_requests(
    request: ExportLogsServiceRequest,
    pipeline: Arc<Pipeline<GreptimeTransformer>>,
    table_name: String,
) -> Result<(RowInsertRequests, usize)> {
    let result = parse_export_logs_service_request(request);
    let transformed_data = pipeline
        .exec(PipelineValue::Array(Array { values: result }))
        .unwrap();
    let len = transformed_data.rows.len();
    let insert_request = RowInsertRequest {
        rows: Some(transformed_data),
        table_name: table_name,
    };
    let insert_requests = RowInsertRequests {
        inserts: vec![insert_request],
    };
    Ok((insert_requests, len))
}

fn scope_to_pipeline_value(
    scope: Option<InstrumentationScope>,
) -> (PipelineValue, PipelineValue, PipelineValue) {
    scope
        .map(|x| {
            (
                PipelineValue::Map(Map {
                    values: key_value_to_map(x.attributes),
                }),
                PipelineValue::String(x.version),
                PipelineValue::String(x.name),
            )
        })
        .unwrap_or((
            PipelineValue::Null,
            PipelineValue::Null,
            PipelineValue::Null,
        ))
}

fn log_to_pipeline_value(
    log: LogRecord,
    resource_schema_url: PipelineValue,
    resource_attr: PipelineValue,
    scope_schema_url: PipelineValue,
    scope_name: PipelineValue,
    scope_version: PipelineValue,
    scope_attrs: PipelineValue,
) -> PipelineValue {
    let log_attrs = PipelineValue::Map(Map {
        values: key_value_to_map(log.attributes),
    });
    let mut map = HashMap::new();
    map.insert(
        "Timestamp".to_string(),
        PipelineValue::Uint64(log.time_unix_nano),
    );
    map.insert(
        "ObservedTimestamp".to_string(),
        PipelineValue::Uint64(log.observed_time_unix_nano),
    );

    // need to be convert to string
    map.insert(
        "TraceId".to_string(),
        PipelineValue::String(bytes_to_hex_string(&log.trace_id)),
    );
    map.insert(
        "SpanId".to_string(),
        PipelineValue::String(bytes_to_hex_string(&log.span_id)),
    );
    map.insert("TraceFlags".to_string(), PipelineValue::Uint32(log.flags));
    map.insert(
        "SeverityText".to_string(),
        PipelineValue::String(log.severity_text),
    );
    map.insert(
        "SeverityNumber".to_string(),
        PipelineValue::Int32(log.severity_number),
    );
    // need to be convert to string
    map.insert(
        "Body".to_string(),
        log.body
            .as_ref()
            .map(|x| PipelineValue::String(log_body_to_string(x)))
            .unwrap_or(PipelineValue::Null),
    );
    map.insert("ResourceSchemaUrl".to_string(), resource_schema_url);

    map.insert("ResourceAttributes".to_string(), resource_attr);
    map.insert("ScopeSchemaUrl".to_string(), scope_schema_url);
    map.insert("ScopeName".to_string(), scope_name);
    map.insert("ScopeVersion".to_string(), scope_version);
    map.insert("ScopeAttributes".to_string(), scope_attrs);
    map.insert("LogAttributes".to_string(), log_attrs);
    PipelineValue::Map(Map { values: map })
}

/// transform otlp logs request to pipeline value
/// https://opentelemetry.io/docs/concepts/signals/logs/
fn parse_export_logs_service_request(request: ExportLogsServiceRequest) -> Vec<PipelineValue> {
    let mut result = Vec::new();
    for r in request.resource_logs {
        let resource_attr = r
            .resource
            .map(|x| {
                PipelineValue::Map(Map {
                    values: key_value_to_map(x.attributes),
                })
            })
            .unwrap_or(PipelineValue::Null);
        let resource_schema_url = PipelineValue::String(r.schema_url);
        for scope_logs in r.scope_logs {
            let (scope_attrs, scope_version, scope_name) =
                scope_to_pipeline_value(scope_logs.scope);
            let scope_schema_url = PipelineValue::String(scope_logs.schema_url);
            for log in scope_logs.log_records {
                let value = log_to_pipeline_value(
                    log,
                    resource_schema_url.clone(),
                    resource_attr.clone(),
                    scope_schema_url.clone(),
                    scope_name.clone(),
                    scope_version.clone(),
                    scope_attrs.clone(),
                );
                result.push(value);
            }
        }
    }
    result
}

// convert AnyValue to pipeline value
fn any_value_to_pipeline_value(value: any_value::Value) -> PipelineValue {
    match value {
        any_value::Value::StringValue(s) => PipelineValue::String(s),
        any_value::Value::IntValue(i) => PipelineValue::Int64(i),
        any_value::Value::DoubleValue(d) => PipelineValue::Float64(d),
        any_value::Value::BoolValue(b) => PipelineValue::Boolean(b),
        any_value::Value::ArrayValue(a) => {
            let values = a
                .values
                .into_iter()
                .map(|v| match v.value {
                    Some(value) => any_value_to_pipeline_value(value),
                    None => PipelineValue::Null,
                })
                .collect();
            PipelineValue::Array(Array { values })
        }
        any_value::Value::KvlistValue(kv) => {
            let value = key_value_to_map(kv.values);
            PipelineValue::Map(Map { values: value })
        }
        any_value::Value::BytesValue(b) => PipelineValue::String(bytes_to_hex_string(&b)),
    }
}

// convert otlp keyValue vec to map
fn key_value_to_map(key_values: Vec<KeyValue>) -> HashMap<String, PipelineValue> {
    let mut map = HashMap::new();
    for kv in key_values {
        let value = match kv.value {
            Some(value) => match value.value {
                Some(value) => any_value_to_pipeline_value(value),
                None => PipelineValue::Null,
            },
            None => PipelineValue::Null,
        };
        map.insert(normalize_otlp_name(&kv.key), value);
    }
    map
}

fn log_body_to_string(body: &AnyValue) -> String {
    let otlp_value = OtlpAnyValue::from(body);
    otlp_value.to_string()
}
