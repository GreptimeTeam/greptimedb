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

use std::collections::{BTreeMap, HashMap as StdHashMap};

use api::v1::column_data_type_extension::TypeExt;
use api::v1::value::ValueData;
use api::v1::{
    ColumnDataType, ColumnDataTypeExtension, ColumnOptions, ColumnSchema, JsonTypeExtension, Row,
    RowInsertRequest, RowInsertRequests, Rows, SemanticType, Value as GreptimeValue,
};
use common_telemetry::warn;
use jsonb::{Number as JsonbNumber, Value as JsonbValue};
use jsonpath_rust::JsonPath;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{any_value, AnyValue, InstrumentationScope, KeyValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use pipeline::{Array, Map, PipelineWay, SchemaInfo, SelectInfo, Value as PipelineValue};
use snafu::ResultExt;

use super::jsonb_path::Jsonb;
use super::trace::attributes::OtlpAnyValue;
use crate::error::{OpenTelemetryLogSnafu, Result};
use crate::otlp::trace::span::bytes_to_hex_string;

/// Convert OpenTelemetry metrics to GreptimeDB insert requests
///
/// See
/// <https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/metrics/v1/metrics.proto>
/// for data structure of OTLP metrics.
///
/// Returns `InsertRequests` and total number of rows to ingest
pub fn to_grpc_insert_requests(
    request: ExportLogsServiceRequest,
    pipeline: PipelineWay,
    table_name: String,
) -> Result<(RowInsertRequests, usize)> {
    match pipeline {
        PipelineWay::BuildInOtlpLog(select_info) => {
            let rows = parse_export_logs_service_request_to_rows(request, select_info);
            let len = rows.rows.len();
            let insert_request = RowInsertRequest {
                rows: Some(rows),
                table_name,
            };
            Ok((
                RowInsertRequests {
                    inserts: vec![insert_request],
                },
                len,
            ))
        }
        PipelineWay::Custom(p) => {
            let request = parse_export_logs_service_request(request);
            let mut result = Vec::new();
            let mut intermediate_state = p.init_intermediate_state();
            for v in request {
                p.prepare_pipeline_value(v, &mut intermediate_state)
                    .context(OpenTelemetryLogSnafu)?;
                let r = p
                    .exec_mut(&mut intermediate_state)
                    .context(OpenTelemetryLogSnafu)?;
                result.push(r);
            }
            let len = result.len();
            let rows = Rows {
                schema: p.schemas().clone(),
                rows: result,
            };
            let insert_request = RowInsertRequest {
                rows: Some(rows),
                table_name,
            };
            let insert_requests = RowInsertRequests {
                inserts: vec![insert_request],
            };
            Ok((insert_requests, len))
        }
    }
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

fn scope_to_jsonb(
    scope: Option<InstrumentationScope>,
) -> (JsonbValue<'static>, Option<String>, Option<String>) {
    scope
        .map(|x| {
            (
                key_value_to_jsonb(x.attributes),
                Some(x.version),
                Some(x.name),
            )
        })
        .unwrap_or((JsonbValue::Null, None, None))
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
    let mut map = BTreeMap::new();
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

fn build_otlp_logs_identity_schema() -> Vec<ColumnSchema> {
    [
        (
            "scope_name",
            ColumnDataType::String,
            SemanticType::Tag,
            None,
            None,
        ),
        (
            "scope_version",
            ColumnDataType::String,
            SemanticType::Field,
            None,
            None,
        ),
        (
            "scope_attributes",
            ColumnDataType::Binary,
            SemanticType::Field,
            Some(ColumnDataTypeExtension {
                type_ext: Some(TypeExt::JsonType(JsonTypeExtension::JsonBinary.into())),
            }),
            None,
        ),
        (
            "resource_attributes",
            ColumnDataType::Binary,
            SemanticType::Field,
            Some(ColumnDataTypeExtension {
                type_ext: Some(TypeExt::JsonType(JsonTypeExtension::JsonBinary.into())),
            }),
            None,
        ),
        (
            "log_attributes",
            ColumnDataType::Binary,
            SemanticType::Field,
            Some(ColumnDataTypeExtension {
                type_ext: Some(TypeExt::JsonType(JsonTypeExtension::JsonBinary.into())),
            }),
            None,
        ),
        (
            "timestamp",
            ColumnDataType::TimestampNanosecond,
            SemanticType::Timestamp,
            None,
            None,
        ),
        (
            "observed_timestamp",
            ColumnDataType::TimestampNanosecond,
            SemanticType::Field,
            None,
            None,
        ),
        (
            "trace_id",
            ColumnDataType::String,
            SemanticType::Tag,
            None,
            None,
        ),
        (
            "span_id",
            ColumnDataType::String,
            SemanticType::Tag,
            None,
            None,
        ),
        (
            "trace_flags",
            ColumnDataType::Uint32,
            SemanticType::Field,
            None,
            None,
        ),
        (
            "severity_text",
            ColumnDataType::String,
            SemanticType::Field,
            None,
            None,
        ),
        (
            "severity_number",
            ColumnDataType::Int32,
            SemanticType::Field,
            None,
            None,
        ),
        (
            "body",
            ColumnDataType::String,
            SemanticType::Field,
            None,
            Some(ColumnOptions {
                options: StdHashMap::from([(
                    "fulltext".to_string(),
                    r#"{"enable":true}"#.to_string(),
                )]),
            }),
        ),
    ]
    .into_iter()
    .map(
        |(field_name, column_type, semantic_type, datatype_extension, options)| ColumnSchema {
            column_name: field_name.to_string(),
            datatype: column_type as i32,
            semantic_type: semantic_type as i32,
            datatype_extension,
            options,
        },
    )
    .collect::<Vec<ColumnSchema>>()
}

fn build_otlp_build_in_row(
    log: LogRecord,
    resource_attr: JsonbValue<'static>,
    scope_name: Option<String>,
    scope_version: Option<String>,
    scope_attrs: JsonbValue<'static>,
) -> Row {
    let log_attr = key_value_to_jsonb(log.attributes);
    let row = vec![
        GreptimeValue {
            value_data: scope_name.map(ValueData::StringValue),
        },
        GreptimeValue {
            value_data: scope_version.map(ValueData::StringValue),
        },
        GreptimeValue {
            value_data: Some(ValueData::BinaryValue(scope_attrs.to_vec())),
        },
        GreptimeValue {
            value_data: Some(ValueData::BinaryValue(resource_attr.to_vec())),
        },
        GreptimeValue {
            value_data: Some(ValueData::BinaryValue(log_attr.to_vec())),
        },
        GreptimeValue {
            value_data: Some(ValueData::TimestampNanosecondValue(
                log.time_unix_nano as i64,
            )),
        },
        GreptimeValue {
            value_data: Some(ValueData::TimestampNanosecondValue(
                log.observed_time_unix_nano as i64,
            )),
        },
        GreptimeValue {
            value_data: Some(ValueData::StringValue(bytes_to_hex_string(&log.trace_id))),
        },
        GreptimeValue {
            value_data: Some(ValueData::StringValue(bytes_to_hex_string(&log.span_id))),
        },
        GreptimeValue {
            value_data: Some(ValueData::U32Value(log.flags)),
        },
        GreptimeValue {
            value_data: Some(ValueData::StringValue(log.severity_text)),
        },
        GreptimeValue {
            value_data: Some(ValueData::I32Value(log.severity_number)),
        },
        GreptimeValue {
            value_data: log
                .body
                .as_ref()
                .map(|x| ValueData::StringValue(log_body_to_string(x))),
        },
    ];
    Row { values: row }
}

fn extract_field_from_attr_and_combine_schema(
    schema_info: &mut SchemaInfo,
    log_select: &BTreeMap<String, String>,
    jsonb: &Jsonb,
) -> Vec<GreptimeValue> {
    let mut append_value = Vec::new();
    for _ in schema_info.schema.iter() {
        append_value.push(GreptimeValue { value_data: None });
    }
    for (k, json_path_str) in log_select {
        let index = schema_info.index.get(k).copied();
        let json_path = JsonPath::try_from(json_path_str.as_str()).unwrap();
        let value = json_path.find(jsonb).0;
        let result = (
            k,
            value.as_array().and_then(|x| x.clone().into_iter().next()),
        );
        if let Some((schema, value)) = result
            .1
            .and_then(|value| decide_column_schema(result.0.clone(), value))
        {
            if let Some(index) = index {
                let column_schema = &schema_info.schema[index];
                if column_schema.datatype != schema.datatype {
                    warn!(
                        "Column {} datatype is different from schema expected {}, actual {}",
                        column_schema.column_name, column_schema.datatype, schema.datatype
                    );
                    continue;
                }
                append_value[index] = value;
            } else {
                let key = k.clone();
                schema_info.schema.push(schema);
                schema_info.index.insert(key, schema_info.schema.len() - 1);
                append_value.push(value);
            }
        }
    }
    append_value
}

fn decide_column_schema(
    column_name: String,
    value: JsonbValue,
) -> Option<(ColumnSchema, GreptimeValue)> {
    let column_info = match value {
        JsonbValue::String(s) => Some((
            GreptimeValue {
                value_data: Some(ValueData::StringValue(s.into())),
            },
            ColumnDataType::String,
            SemanticType::Tag,
            None,
        )),
        JsonbValue::Number(n) => match n {
            JsonbNumber::Int64(i) => Some((
                GreptimeValue {
                    value_data: Some(ValueData::I64Value(i)),
                },
                ColumnDataType::Int64,
                SemanticType::Tag,
                None,
            )),
            JsonbNumber::Float64(f) => Some((
                GreptimeValue {
                    value_data: Some(ValueData::F64Value(f)),
                },
                ColumnDataType::Float64,
                SemanticType::Tag,
                None,
            )),
            JsonbNumber::UInt64(u) => Some((
                GreptimeValue {
                    value_data: Some(ValueData::U64Value(u)),
                },
                ColumnDataType::Uint64,
                SemanticType::Tag,
                None,
            )),
        },
        JsonbValue::Bool(b) => Some((
            GreptimeValue {
                value_data: Some(ValueData::BoolValue(b)),
            },
            ColumnDataType::Boolean,
            SemanticType::Field,
            None,
        )),
        JsonbValue::Array(_) | JsonbValue::Object(_) => Some((
            GreptimeValue {
                value_data: Some(ValueData::BinaryValue(value.to_vec())),
            },
            ColumnDataType::Binary,
            SemanticType::Field,
            Some(ColumnDataTypeExtension {
                type_ext: Some(TypeExt::JsonType(JsonTypeExtension::JsonBinary.into())),
            }),
        )),
        JsonbValue::Null => None,
    };
    column_info.map(|(value, column_type, semantic_type, datatype_extension)| {
        (
            ColumnSchema {
                column_name,
                datatype: column_type as i32,
                semantic_type: semantic_type as i32,
                datatype_extension,
                options: None,
            },
            value,
        )
    })
}

fn parse_export_logs_service_request_to_rows(
    request: ExportLogsServiceRequest,
    select_info: Box<SelectInfo>,
) -> Rows {
    let mut results = Vec::new();
    let mut schemas = build_otlp_logs_identity_schema();
    let mut extra_resource_schema = SchemaInfo::default();
    let mut extra_scope_schema = SchemaInfo::default();
    let mut extra_log_schema = SchemaInfo::default();
    parse_resource(
        &mut results,
        &select_info,
        &mut extra_resource_schema,
        &mut extra_scope_schema,
        &mut extra_log_schema,
        request.resource_logs,
    );
    println!(
        "--------------------{:?} {:?} {:?}",
        extra_resource_schema.schema, extra_scope_schema.schema, extra_log_schema.schema
    );
    schemas.extend(extra_resource_schema.schema);
    schemas.extend(extra_scope_schema.schema);
    schemas.extend(extra_log_schema.schema);

    let column_count = schemas.len();
    for row in results.iter_mut() {
        let diff = column_count - row.values.len();
        for _ in 0..diff {
            row.values.push(GreptimeValue { value_data: None });
        }
    }
    Rows {
        schema: schemas,
        rows: results,
    }
}

fn parse_resource(
    results: &mut Vec<Row>,
    select_info: &Box<SelectInfo>,
    extra_resource_schema: &mut SchemaInfo,
    extra_scope_schema: &mut SchemaInfo,
    extra_log_schema: &mut SchemaInfo,
    resource_logs_vec: Vec<ResourceLogs>,
) {
    for r in resource_logs_vec {
        let resource_attr = r
            .resource
            .map(|resource| key_value_to_jsonb(resource.attributes))
            .unwrap_or(JsonbValue::Null);
        let resource_jsonb = Jsonb::new(resource_attr.clone());
        let resource_values = extract_field_from_attr_and_combine_schema(
            extra_resource_schema,
            &select_info.resource_attributes,
            &resource_jsonb,
        );
        parse_scope(
            results,
            extra_log_schema,
            extra_scope_schema,
            select_info,
            r.scope_logs,
            resource_attr,
            resource_values,
        );
    }
}

fn parse_scope(
    results: &mut Vec<Row>,
    extra_log_schema: &mut SchemaInfo,
    extra_scope_schema: &mut SchemaInfo,
    select_info: &Box<SelectInfo>,
    scopes_log_vec: Vec<ScopeLogs>,
    resource_attr: JsonbValue<'static>,
    resource_values: Vec<GreptimeValue>,
) {
    for scope_logs in scopes_log_vec {
        let (scope_attrs, scope_version, scope_name) = scope_to_jsonb(scope_logs.scope);
        let scope_jsonb = Jsonb::new(scope_attrs.clone());
        let scope_values = extract_field_from_attr_and_combine_schema(
            extra_scope_schema,
            &select_info.scope_attributes,
            &scope_jsonb,
        );
        let row = parse_log(
            extra_log_schema,
            &select_info,
            scope_logs.log_records,
            resource_attr.clone(),
            scope_name,
            scope_version,
            scope_attrs,
            resource_values.clone(),
            scope_values,
        );
        results.extend(row);
    }
}

fn parse_log(
    extra_log_schema: &mut SchemaInfo,
    select_info: &Box<SelectInfo>,
    log_records: Vec<LogRecord>,
    resource_attr: JsonbValue<'static>,
    scope_name: Option<String>,
    scope_version: Option<String>,
    scope_attrs: JsonbValue<'static>,
    resource_values: Vec<GreptimeValue>,
    scope_values: Vec<GreptimeValue>,
) -> Vec<Row> {
    let mut result = Vec::with_capacity(log_records.len());
    for log in log_records {
        let log_attr = key_value_to_jsonb(log.attributes.clone());

        let mut row = build_otlp_build_in_row(
            log,
            resource_attr.clone(),
            scope_name.clone(),
            scope_version.clone(),
            scope_attrs.clone(),
        );
        let log_jsonb = Jsonb::new(log_attr);

        let log_values = extract_field_from_attr_and_combine_schema(
            extra_log_schema,
            &select_info.log_attributes,
            &log_jsonb,
        );
        println!(
            "-----------------values {:?} {:?} {:?}",
            resource_values, scope_values, log_values
        );
        row.values.extend(resource_values.clone());
        row.values.extend(scope_values.clone());
        row.values.extend(log_values);
        result.push(row);
    }
    result
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
fn key_value_to_map(key_values: Vec<KeyValue>) -> BTreeMap<String, PipelineValue> {
    let mut map = BTreeMap::new();
    for kv in key_values {
        let value = match kv.value {
            Some(value) => match value.value {
                Some(value) => any_value_to_pipeline_value(value),
                None => PipelineValue::Null,
            },
            None => PipelineValue::Null,
        };
        map.insert(kv.key.clone(), value);
    }
    map
}

fn any_value_to_jsonb(value: any_value::Value) -> JsonbValue<'static> {
    match value {
        any_value::Value::StringValue(s) => JsonbValue::String(s.into()),
        any_value::Value::IntValue(i) => JsonbValue::Number(JsonbNumber::Int64(i)),
        any_value::Value::DoubleValue(d) => JsonbValue::Number(JsonbNumber::Float64(d)),
        any_value::Value::BoolValue(b) => JsonbValue::Bool(b),
        any_value::Value::ArrayValue(a) => {
            let values = a
                .values
                .into_iter()
                .map(|v| match v.value {
                    Some(value) => any_value_to_jsonb(value),
                    None => JsonbValue::Null,
                })
                .collect();
            JsonbValue::Array(values)
        }
        any_value::Value::KvlistValue(kv) => key_value_to_jsonb(kv.values),
        any_value::Value::BytesValue(b) => JsonbValue::String(bytes_to_hex_string(&b).into()),
    }
}

fn key_value_to_jsonb(key_values: Vec<KeyValue>) -> JsonbValue<'static> {
    let mut map = BTreeMap::new();
    for kv in key_values {
        let value = match kv.value {
            Some(value) => match value.value {
                Some(value) => any_value_to_jsonb(value),
                None => JsonbValue::Null,
            },
            None => JsonbValue::Null,
        };
        map.insert(kv.key.clone(), value);
    }
    JsonbValue::Object(map)
}

fn log_body_to_string(body: &AnyValue) -> String {
    let otlp_value = OtlpAnyValue::from(body);
    otlp_value.to_string()
}
