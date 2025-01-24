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
use std::mem;

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
use pipeline::error::PipelineTransformSnafu;
use pipeline::{GreptimePipelineParams, PipelineWay, SchemaInfo, SelectInfo};
use serde_json::{Map, Value};
use session::context::QueryContextRef;
use snafu::{ensure, ResultExt};

use super::trace::attributes::OtlpAnyValue;
use super::utils::{bytes_to_hex_string, key_value_to_jsonb};
use crate::error::{
    IncompatibleSchemaSnafu, PipelineSnafu, Result, UnsupportedJsonDataTypeForTagSnafu,
};
use crate::pipeline::run_pipeline;
use crate::query_handler::PipelineHandlerRef;

pub const LOG_TABLE_NAME: &str = "opentelemetry_logs";

/// Convert OpenTelemetry metrics to GreptimeDB insert requests
///
/// See
/// <https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/metrics/v1/metrics.proto>
/// for data structure of OTLP metrics.
///
/// Returns `InsertRequests` and total number of rows to ingest
pub async fn to_grpc_insert_requests(
    request: ExportLogsServiceRequest,
    pipeline: PipelineWay,
    pipeline_params: GreptimePipelineParams,
    table_name: String,
    query_ctx: &QueryContextRef,
    pipeline_handler: PipelineHandlerRef,
) -> Result<(RowInsertRequests, usize)> {
    match pipeline {
        PipelineWay::OtlpLogDirect(select_info) => {
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
        PipelineWay::Pipeline(pipeline_def) => {
            let data = parse_export_logs_service_request(request);
            let array = pipeline::json_array_to_intermediate_state(data)
                .context(PipelineTransformSnafu)
                .context(PipelineSnafu)?;

            let inserts = run_pipeline(
                &pipeline_handler,
                pipeline_def,
                &pipeline_params,
                array,
                table_name,
                query_ctx,
                true,
            )
            .await?;
            let len = inserts
                .iter()
                .map(|insert| {
                    insert
                        .rows
                        .as_ref()
                        .map(|rows| rows.rows.len())
                        .unwrap_or(0)
                })
                .sum();

            let insert_requests = RowInsertRequests { inserts };
            Ok((insert_requests, len))
        }
    }
}

fn scope_to_pipeline_value(scope: Option<InstrumentationScope>) -> (Value, Value, Value) {
    scope
        .map(|x| {
            (
                Value::Object(key_value_to_map(x.attributes)),
                Value::String(x.version),
                Value::String(x.name),
            )
        })
        .unwrap_or((Value::Null, Value::Null, Value::Null))
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
    resource_schema_url: Value,
    resource_attr: Value,
    scope_schema_url: Value,
    scope_name: Value,
    scope_version: Value,
    scope_attrs: Value,
) -> Value {
    let log_attrs = Value::Object(key_value_to_map(log.attributes));
    let mut map = Map::new();
    map.insert("Timestamp".to_string(), Value::from(log.time_unix_nano));
    map.insert(
        "ObservedTimestamp".to_string(),
        Value::from(log.observed_time_unix_nano),
    );

    // need to be convert to string
    map.insert(
        "TraceId".to_string(),
        Value::String(bytes_to_hex_string(&log.trace_id)),
    );
    map.insert(
        "SpanId".to_string(),
        Value::String(bytes_to_hex_string(&log.span_id)),
    );
    map.insert("TraceFlags".to_string(), Value::from(log.flags));
    map.insert("SeverityText".to_string(), Value::String(log.severity_text));
    map.insert(
        "SeverityNumber".to_string(),
        Value::from(log.severity_number),
    );
    // need to be convert to string
    map.insert(
        "Body".to_string(),
        log.body
            .as_ref()
            .map(|x| Value::String(log_body_to_string(x)))
            .unwrap_or(Value::Null),
    );
    map.insert("ResourceSchemaUrl".to_string(), resource_schema_url);

    map.insert("ResourceAttributes".to_string(), resource_attr);
    map.insert("ScopeSchemaUrl".to_string(), scope_schema_url);
    map.insert("ScopeName".to_string(), scope_name);
    map.insert("ScopeVersion".to_string(), scope_version);
    map.insert("ScopeAttributes".to_string(), scope_attrs);
    map.insert("LogAttributes".to_string(), log_attrs);
    Value::Object(map)
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

fn build_otlp_build_in_row(log: LogRecord, parse_ctx: &mut ParseContext) -> Row {
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
    Row { values: row }
}

fn extract_field_from_attr_and_combine_schema(
    schema_info: &mut SchemaInfo,
    log_select: &SelectInfo,
    jsonb: &jsonb::Value,
) -> Result<Vec<GreptimeValue>> {
    if log_select.keys.is_empty() {
        return Ok(Vec::new());
    }
    let mut append_value = Vec::with_capacity(schema_info.schema.len());
    for _ in schema_info.schema.iter() {
        append_value.push(GreptimeValue { value_data: None });
    }
    for k in &log_select.keys {
        let index = schema_info.index.get(k).copied();
        if let Some(value) = jsonb.get_by_name_ignore_case(k).cloned() {
            if let Some((schema, value)) = decide_column_schema(k, value)? {
                if let Some(index) = index {
                    let column_schema = &schema_info.schema[index];
                    ensure!(
                        column_schema.datatype == schema.datatype,
                        IncompatibleSchemaSnafu {
                            column_name: k.clone(),
                            datatype: column_schema.datatype().as_str_name(),
                            expected: column_schema.datatype,
                            actual: schema.datatype,
                        }
                    );
                    append_value[index] = value;
                } else {
                    let key = k.clone();
                    schema_info.schema.push(schema);
                    schema_info.index.insert(key, schema_info.schema.len() - 1);
                    append_value.push(value);
                }
            }
        }
    }
    Ok(append_value)
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

#[derive(Debug, Clone, Copy)]
enum OpenTelemetryLogRecordAttrType {
    Resource,
    Scope,
    Log,
}

fn merge_schema(
    input_schemas: Vec<(&SchemaInfo, OpenTelemetryLogRecordAttrType)>,
) -> BTreeMap<&String, (OpenTelemetryLogRecordAttrType, usize, &ColumnSchema)> {
    let mut schemas = BTreeMap::new();
    input_schemas
        .into_iter()
        .for_each(|(schema_info, attr_type)| {
            for (key, index) in schema_info.index.iter() {
                if let Some(col_schema) = schema_info.schema.get(*index) {
                    schemas.insert(key, (attr_type, *index, col_schema));
                }
            }
        });
    schemas
}

fn parse_export_logs_service_request_to_rows(
    request: ExportLogsServiceRequest,
    select_info: Box<SelectInfo>,
) -> Result<Rows> {
    let mut schemas = build_otlp_logs_identity_schema();
    let mut extra_resource_schema = SchemaInfo::default();
    let mut extra_scope_schema = SchemaInfo::default();
    let mut extra_log_schema = SchemaInfo::default();
    let mut parse_ctx = ParseContext::new(
        &mut extra_resource_schema,
        &mut extra_scope_schema,
        &mut extra_log_schema,
    );
    let parse_infos = parse_resource(&select_info, &mut parse_ctx, request.resource_logs)?;

    // order of schema is important
    // resource < scope < log
    // do not change the order
    let final_extra_schema_info = merge_schema(vec![
        (
            &extra_resource_schema,
            OpenTelemetryLogRecordAttrType::Resource,
        ),
        (&extra_scope_schema, OpenTelemetryLogRecordAttrType::Scope),
        (&extra_log_schema, OpenTelemetryLogRecordAttrType::Log),
    ]);

    let final_extra_schema = final_extra_schema_info
        .iter()
        .map(|(_, (_, _, v))| (*v).clone())
        .collect::<Vec<_>>();

    let extra_schema_len = final_extra_schema.len();
    schemas.extend(final_extra_schema);

    let mut results = Vec::with_capacity(parse_infos.len());
    for parse_info in parse_infos.into_iter() {
        let mut row = parse_info.values;
        let mut resource_values = parse_info.resource_extracted_values;
        let mut scope_values = parse_info.scope_extracted_values;
        let mut log_values = parse_info.log_extracted_values;

        let mut final_extra_values = vec![GreptimeValue { value_data: None }; extra_schema_len];
        for (idx, (_, (attr_type, index, _))) in final_extra_schema_info.iter().enumerate() {
            let value = match attr_type {
                OpenTelemetryLogRecordAttrType::Resource => resource_values.get_mut(*index),
                OpenTelemetryLogRecordAttrType::Scope => scope_values.get_mut(*index),
                OpenTelemetryLogRecordAttrType::Log => log_values.get_mut(*index),
            };
            if let Some(value) = value {
                // swap value to final_extra_values
                mem::swap(&mut final_extra_values[idx], value);
            }
        }

        row.values.extend(final_extra_values);
        results.push(row);
    }
    Ok(Rows {
        schema: schemas,
        rows: results,
    })
}

fn parse_resource(
    select_info: &SelectInfo,
    parse_ctx: &mut ParseContext,
    resource_logs_vec: Vec<ResourceLogs>,
) -> Result<Vec<ParseInfo>> {
    let mut results = Vec::new();

    for r in resource_logs_vec {
        parse_ctx.resource_attr = r
            .resource
            .map(|resource| key_value_to_jsonb(resource.attributes))
            .unwrap_or(JsonbValue::Null);
        parse_ctx.resource_url = r.schema_url;

        let resource_extracted_values = extract_field_from_attr_and_combine_schema(
            parse_ctx.extra_resource_schema,
            select_info,
            &parse_ctx.resource_attr,
        )?;
        let rows = parse_scope(
            select_info,
            r.scope_logs,
            parse_ctx,
            resource_extracted_values,
        )?;
        results.extend(rows);
    }
    Ok(results)
}

struct ParseContext<'a> {
    // selector schema
    extra_resource_schema: &'a mut SchemaInfo,
    extra_scope_schema: &'a mut SchemaInfo,
    extra_log_schema: &'a mut SchemaInfo,

    // passdown values
    resource_url: String,
    resource_attr: JsonbValue<'a>,
    scope_name: Option<String>,
    scope_version: Option<String>,
    scope_url: String,
    scope_attrs: JsonbValue<'a>,
}

impl<'a> ParseContext<'a> {
    pub fn new(
        extra_resource_schema: &'a mut SchemaInfo,
        extra_scope_schema: &'a mut SchemaInfo,
        extra_log_schema: &'a mut SchemaInfo,
    ) -> ParseContext<'a> {
        ParseContext {
            extra_resource_schema,
            extra_scope_schema,
            extra_log_schema,
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
    select_info: &SelectInfo,
    scopes_log_vec: Vec<ScopeLogs>,
    parse_ctx: &mut ParseContext,
    resource_extracted_values: Vec<GreptimeValue>,
) -> Result<Vec<ParseInfo>> {
    let mut results = Vec::new();
    for scope_logs in scopes_log_vec {
        let (scope_attrs, scope_version, scope_name) = scope_to_jsonb(scope_logs.scope);
        parse_ctx.scope_name = scope_name;
        parse_ctx.scope_version = scope_version;
        parse_ctx.scope_attrs = scope_attrs;
        parse_ctx.scope_url = scope_logs.schema_url;

        let scope_extracted_values = extract_field_from_attr_and_combine_schema(
            parse_ctx.extra_scope_schema,
            select_info,
            &parse_ctx.scope_attrs,
        )?;

        let rows = parse_log(
            select_info,
            scope_logs.log_records,
            parse_ctx,
            &resource_extracted_values,
            &scope_extracted_values,
        )?;
        results.extend(rows);
    }
    Ok(results)
}

fn parse_log(
    select_info: &SelectInfo,
    log_records: Vec<LogRecord>,
    parse_ctx: &mut ParseContext,
    resource_extracted_values: &[GreptimeValue],
    scope_extracted_values: &[GreptimeValue],
) -> Result<Vec<ParseInfo>> {
    let mut result = Vec::with_capacity(log_records.len());

    for log in log_records {
        let log_attr = key_value_to_jsonb(log.attributes.clone());

        let row = build_otlp_build_in_row(log, parse_ctx);

        let log_extracted_values = extract_field_from_attr_and_combine_schema(
            parse_ctx.extra_log_schema,
            select_info,
            &log_attr,
        )?;

        let parse_info = ParseInfo {
            values: row,
            resource_extracted_values: resource_extracted_values.to_vec(),
            scope_extracted_values: scope_extracted_values.to_vec(),
            log_extracted_values,
        };
        result.push(parse_info);
    }
    Ok(result)
}

struct ParseInfo {
    values: Row,
    resource_extracted_values: Vec<GreptimeValue>,
    scope_extracted_values: Vec<GreptimeValue>,
    log_extracted_values: Vec<GreptimeValue>,
}

/// transform otlp logs request to pipeline value
/// https://opentelemetry.io/docs/concepts/signals/logs/
fn parse_export_logs_service_request(request: ExportLogsServiceRequest) -> Vec<Value> {
    let mut result = Vec::new();
    for r in request.resource_logs {
        let resource_attr = r
            .resource
            .map(|x| Value::Object(key_value_to_map(x.attributes)))
            .unwrap_or(Value::Null);
        let resource_schema_url = Value::String(r.schema_url);
        for scope_logs in r.scope_logs {
            let (scope_attrs, scope_version, scope_name) =
                scope_to_pipeline_value(scope_logs.scope);
            let scope_schema_url = Value::String(scope_logs.schema_url);
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
fn any_value_to_pipeline_value(value: any_value::Value) -> Value {
    match value {
        any_value::Value::StringValue(s) => Value::String(s),
        any_value::Value::IntValue(i) => Value::from(i),
        any_value::Value::DoubleValue(d) => Value::from(d),
        any_value::Value::BoolValue(b) => Value::Bool(b),
        any_value::Value::ArrayValue(a) => {
            let values = a
                .values
                .into_iter()
                .map(|v| match v.value {
                    Some(value) => any_value_to_pipeline_value(value),
                    None => Value::Null,
                })
                .collect();
            Value::Array(values)
        }
        any_value::Value::KvlistValue(kv) => {
            let value = key_value_to_map(kv.values);
            Value::Object(value)
        }
        any_value::Value::BytesValue(b) => Value::String(bytes_to_hex_string(&b)),
    }
}

// convert otlp keyValue vec to map
fn key_value_to_map(key_values: Vec<KeyValue>) -> Map<String, Value> {
    let mut map = Map::new();
    for kv in key_values {
        let value = match kv.value {
            Some(value) => match value.value {
                Some(value) => any_value_to_pipeline_value(value),
                None => Value::Null,
            },
            None => Value::Null,
        };
        map.insert(kv.key.clone(), value);
    }
    map
}

fn log_body_to_string(body: &AnyValue) -> String {
    let otlp_value = OtlpAnyValue::from(body);
    otlp_value.to_string()
}
