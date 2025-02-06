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
use jsonb::{Number as JsonbNumber, Value as JsonbValue};
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{any_value, AnyValue, InstrumentationScope, KeyValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use pipeline::{Array, Map, PipelineWay, SchemaInfo, SelectInfo, Value as PipelineValue};
use snafu::{ensure, ResultExt};

use super::trace::attributes::OtlpAnyValue;
use super::utils::{bytes_to_hex_string, key_value_to_jsonb};
use crate::error::{
    IncompatibleSchemaSnafu, OpenTelemetryLogSnafu, Result, UnsupportedJsonDataTypeForTagSnafu,
};

pub const LOG_TABLE_NAME: &str = "opentelemetry_logs";

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
        PipelineWay::OtlpLog(select_info) => {
            let rows = parse_export_logs_service_request_to_rows(request, select_info)?;
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
            "timestamp",
            ColumnDataType::TimestampNanosecond,
            SemanticType::Timestamp,
            None,
            None,
        ),
        (
            "trace_id",
            ColumnDataType::String,
            SemanticType::Field,
            None,
            None,
        ),
        (
            "span_id",
            ColumnDataType::String,
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
            "trace_flags",
            ColumnDataType::Uint32,
            SemanticType::Field,
            None,
            None,
        ),
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
            "scope_schema_url",
            ColumnDataType::String,
            SemanticType::Field,
            None,
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
            "resource_schema_url",
            ColumnDataType::String,
            SemanticType::Field,
            None,
            None,
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
    parse_ctx: &mut ParseContext,
) -> (Row, JsonbValue<'static>) {
    let log_attr = key_value_to_jsonb(log.attributes);
    let ts = if log.time_unix_nano != 0 {
        log.time_unix_nano
    } else {
        log.observed_time_unix_nano
    };

    let row = vec![
        GreptimeValue {
            value_data: Some(ValueData::TimestampNanosecondValue(ts as i64)),
        },
        GreptimeValue {
            value_data: Some(ValueData::StringValue(bytes_to_hex_string(&log.trace_id))),
        },
        GreptimeValue {
            value_data: Some(ValueData::StringValue(bytes_to_hex_string(&log.span_id))),
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
        GreptimeValue {
            value_data: Some(ValueData::BinaryValue(log_attr.to_vec())),
        },
        GreptimeValue {
            value_data: Some(ValueData::U32Value(log.flags)),
        },
        GreptimeValue {
            value_data: parse_ctx.scope_name.clone().map(ValueData::StringValue),
        },
        GreptimeValue {
            value_data: parse_ctx.scope_version.clone().map(ValueData::StringValue),
        },
        GreptimeValue {
            value_data: Some(ValueData::BinaryValue(parse_ctx.scope_attrs.to_vec())),
        },
        GreptimeValue {
            value_data: Some(ValueData::StringValue(parse_ctx.scope_url.clone())),
        },
        GreptimeValue {
            value_data: Some(ValueData::BinaryValue(parse_ctx.resource_attr.to_vec())),
        },
        GreptimeValue {
            value_data: Some(ValueData::StringValue(parse_ctx.resource_url.clone())),
        },
    ];
    (Row { values: row }, log_attr)
}

fn extract_field_from_attr_and_combine_schema(
    parse_ctx: &mut ParseContext,
    jsonb: &jsonb::Value,
) -> Result<()> {
    if parse_ctx.select_info.keys.is_empty() {
        return Ok(());
    }

    let schema_info = &mut parse_ctx.extra_schema;
    let extracted_values = &mut parse_ctx.extracted_values;

    for k in parse_ctx.select_info.keys.iter() {
        let Some(value) = jsonb.get_by_name_ignore_case(k).cloned() else {
            continue;
        };
        let Some((schema, value)) = decide_column_schema(k, value)? else {
            continue;
        };

        if let Some(index) = schema_info.index.get(k) {
            let column_schema = &schema_info.schema[*index];
            ensure!(
                column_schema.datatype == schema.datatype,
                IncompatibleSchemaSnafu {
                    column_name: k.clone(),
                    datatype: column_schema.datatype().as_str_name(),
                    expected: column_schema.datatype,
                    actual: schema.datatype,
                }
            );
            extracted_values[*index] = value;
        } else {
            let key = k.clone();
            schema_info.schema.push(schema);
            schema_info.index.insert(key, schema_info.schema.len() - 1);
            extracted_values.push(value);
        }
    }

    Ok(())
}

fn decide_column_schema(
    column_name: &str,
    value: JsonbValue,
) -> Result<Option<(ColumnSchema, GreptimeValue)>> {
    let column_info = match value {
        JsonbValue::String(s) => Ok(Some((
            GreptimeValue {
                value_data: Some(ValueData::StringValue(s.into())),
            },
            ColumnDataType::String,
            SemanticType::Tag,
            None,
        ))),
        JsonbValue::Number(n) => match n {
            JsonbNumber::Int64(i) => Ok(Some((
                GreptimeValue {
                    value_data: Some(ValueData::I64Value(i)),
                },
                ColumnDataType::Int64,
                SemanticType::Tag,
                None,
            ))),
            JsonbNumber::Float64(_) => UnsupportedJsonDataTypeForTagSnafu {
                ty: "FLOAT".to_string(),
                key: column_name,
            }
            .fail(),
            JsonbNumber::UInt64(u) => Ok(Some((
                GreptimeValue {
                    value_data: Some(ValueData::U64Value(u)),
                },
                ColumnDataType::Uint64,
                SemanticType::Tag,
                None,
            ))),
        },
        JsonbValue::Bool(b) => Ok(Some((
            GreptimeValue {
                value_data: Some(ValueData::BoolValue(b)),
            },
            ColumnDataType::Boolean,
            SemanticType::Tag,
            None,
        ))),
        JsonbValue::Array(_) | JsonbValue::Object(_) => UnsupportedJsonDataTypeForTagSnafu {
            ty: "Json".to_string(),
            key: column_name,
        }
        .fail(),
        JsonbValue::Null => Ok(None),
    };
    column_info.map(|c| {
        c.map(|(value, column_type, semantic_type, datatype_extension)| {
            (
                ColumnSchema {
                    column_name: column_name.to_string(),
                    datatype: column_type as i32,
                    semantic_type: semantic_type as i32,
                    datatype_extension,
                    options: None,
                },
                value,
            )
        })
    })
}

fn parse_export_logs_service_request_to_rows(
    request: ExportLogsServiceRequest,
    select_info: Box<SelectInfo>,
) -> Result<Rows> {
    let mut schemas = build_otlp_logs_identity_schema();

    let mut parse_ctx = ParseContext::new(select_info);
    let parse_infos = parse_resource(&mut parse_ctx, request.resource_logs)?;

    let extra_schema_len = parse_ctx.extra_schema.schema.len();
    schemas.extend(parse_ctx.extra_schema.schema);

    let mut results = Vec::with_capacity(parse_infos.len());
    for parse_info in parse_infos.into_iter() {
        let mut row = parse_info.values;
        let mut extras = parse_info.extracted_values;
        if extras.len() < extra_schema_len {
            extras.resize(extra_schema_len, GreptimeValue::default());
        }
        row.values.extend(extras);
        results.push(row);
    }

    Ok(Rows {
        schema: schemas,
        rows: results,
    })
}

fn parse_resource(
    parse_ctx: &mut ParseContext,
    resource_logs_vec: Vec<ResourceLogs>,
) -> Result<Vec<ParseInfo>> {
    let mut results = Vec::new();

    for r in resource_logs_vec {
        let resource_attr = r
            .resource
            .map(|resource| key_value_to_jsonb(resource.attributes))
            .unwrap_or(JsonbValue::Null);
        parse_ctx.resource_url = r.schema_url;

        extract_field_from_attr_and_combine_schema(parse_ctx, &resource_attr)?;
        parse_ctx.resource_attr = resource_attr;

        let rows = parse_scope(r.scope_logs, parse_ctx)?;
        results.extend(rows);
    }
    Ok(results)
}

struct ParseContext<'a> {
    // selector keys
    select_info: Box<SelectInfo>,
    // selector schema
    extra_schema: SchemaInfo,
    // extracted values
    extracted_values: Vec<GreptimeValue>,

    // passdown values
    resource_url: String,
    resource_attr: JsonbValue<'a>,
    scope_name: Option<String>,
    scope_version: Option<String>,
    scope_url: String,
    scope_attrs: JsonbValue<'a>,
}

impl<'a> ParseContext<'a> {
    pub fn new(select_info: Box<SelectInfo>) -> ParseContext<'a> {
        let extract_len = select_info.keys.len();
        ParseContext {
            select_info,
            extra_schema: SchemaInfo::new_with_capacity(extract_len),
            extracted_values: Vec::with_capacity(extract_len),
            resource_url: String::new(),
            resource_attr: JsonbValue::Null,
            scope_name: None,
            scope_version: None,
            scope_url: String::new(),
            scope_attrs: JsonbValue::Null,
        }
    }
}

fn parse_scope(
    scopes_log_vec: Vec<ScopeLogs>,
    parse_ctx: &mut ParseContext,
) -> Result<Vec<ParseInfo>> {
    let mut results = Vec::new();
    for scope_logs in scopes_log_vec {
        let (scope_attrs, scope_version, scope_name) = scope_to_jsonb(scope_logs.scope);
        parse_ctx.scope_name = scope_name;
        parse_ctx.scope_version = scope_version;
        parse_ctx.scope_url = scope_logs.schema_url;

        extract_field_from_attr_and_combine_schema(parse_ctx, &scope_attrs)?;

        parse_ctx.scope_attrs = scope_attrs;

        let rows = parse_log(scope_logs.log_records, parse_ctx)?;
        results.extend(rows);
    }
    Ok(results)
}

fn parse_log(log_records: Vec<LogRecord>, parse_ctx: &mut ParseContext) -> Result<Vec<ParseInfo>> {
    let mut result = Vec::with_capacity(log_records.len());

    for log in log_records {
        let (row, log_attr) = build_otlp_build_in_row(log, parse_ctx);

        extract_field_from_attr_and_combine_schema(parse_ctx, &log_attr)?;

        let parse_info = ParseInfo {
            values: row,
            extracted_values: parse_ctx.extracted_values.clone(),
        };
        result.push(parse_info);
    }
    Ok(result)
}

struct ParseInfo {
    values: Row,
    extracted_values: Vec<GreptimeValue>,
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

fn log_body_to_string(body: &AnyValue) -> String {
    let otlp_value = OtlpAnyValue::from(body);
    otlp_value.to_string()
}
