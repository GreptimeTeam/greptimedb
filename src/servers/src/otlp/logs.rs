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

use std::collections::BTreeMap;

use ahash::{HashMap, HashMapExt};
use api::helper::ColumnDataTypeWrapper;
use api::v1::column_data_type_extension::TypeExt;
use api::v1::column_def::options_from_column_schema;
use api::v1::value::ValueData;
use api::v1::{
    ColumnDataType, ColumnDataTypeExtension, ColumnOptions, ColumnSchema, JsonTypeExtension, Row,
    RowInsertRequest, Rows, SemanticType, Value as GreptimeValue,
};
use bytes::Bytes;
use common_time::Timestamp;
use common_time::timestamp::TimeUnit;
use jsonb::{Number as JsonbNumber, Value as JsonbValue};
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use pipeline::{
    ContextReq, GreptimePipelineParams, PipelineContext, PipelineWay, SchemaInfo, SelectInfo,
};
use session::context::QueryContextRef;
use snafu::ensure;
use vrl::prelude::NotNan;
use vrl::value::{KeyString, Value as VrlValue};

use crate::error::{
    Error, IncompatibleSchemaSnafu, InvalidParameterSnafu, NotSupportedSnafu, Result,
    UnsupportedJsonDataTypeForTagSnafu,
};
use crate::http::event::PipelineIngestRequest;
use crate::otlp::coerce::coerce_value_data;
use crate::otlp::trace::attributes::OtlpAnyValue;
use crate::otlp::utils::{bytes_to_hex_string, key_value_to_jsonb};
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
) -> Result<ContextReq> {
    match pipeline {
        PipelineWay::OtlpLogDirect(select_info) => {
            let table = pipeline_handler
                .get_table(&table_name, query_ctx)
                .await
                .map_err(Error::from)?;
            let existing_schema = table
                .as_deref()
                .map(ExistingLogSchema::try_from_table)
                .transpose()?;
            let rows = parse_export_logs_service_request_to_rows(
                request,
                select_info,
                existing_schema.as_ref(),
                &table_name,
            )?;
            let insert_request = RowInsertRequest {
                rows: Some(rows),
                table_name,
            };

            Ok(ContextReq::default_opt_with_reqs(vec![insert_request]))
        }
        PipelineWay::Pipeline(pipeline_def) => {
            let array = parse_export_logs_service_request(request);

            let pipeline_ctx =
                PipelineContext::new(&pipeline_def, &pipeline_params, query_ctx.channel());
            run_pipeline(
                &pipeline_handler,
                &pipeline_ctx,
                PipelineIngestRequest {
                    table: table_name,
                    values: array,
                },
                query_ctx,
                true,
            )
            .await
        }
        _ => NotSupportedSnafu {
            feat: "Unsupported pipeline for logs",
        }
        .fail(),
    }
}

fn scope_to_pipeline_value(scope: Option<InstrumentationScope>) -> (VrlValue, VrlValue, VrlValue) {
    scope
        .map(|x| {
            (
                VrlValue::Object(key_value_to_map(x.attributes)),
                VrlValue::Bytes(x.version.into()),
                VrlValue::Bytes(x.name.into()),
            )
        })
        .unwrap_or((VrlValue::Null, VrlValue::Null, VrlValue::Null))
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
    resource_schema_url: VrlValue,
    resource_attr: VrlValue,
    scope_schema_url: VrlValue,
    scope_name: VrlValue,
    scope_version: VrlValue,
    scope_attrs: VrlValue,
) -> VrlValue {
    let log_attrs = VrlValue::Object(key_value_to_map(log.attributes));
    let mut map = BTreeMap::new();
    map.insert(
        "Timestamp".into(),
        VrlValue::Integer(log.time_unix_nano as i64),
    );
    map.insert(
        "ObservedTimestamp".into(),
        VrlValue::Integer(log.observed_time_unix_nano as i64),
    );

    // need to be convert to string
    map.insert(
        "TraceId".into(),
        VrlValue::Bytes(bytes_to_hex_string(&log.trace_id).into()),
    );
    map.insert(
        "SpanId".into(),
        VrlValue::Bytes(bytes_to_hex_string(&log.span_id).into()),
    );
    map.insert("TraceFlags".into(), VrlValue::Integer(log.flags as i64));
    map.insert(
        "SeverityText".into(),
        VrlValue::Bytes(log.severity_text.into()),
    );
    map.insert(
        "SeverityNumber".into(),
        VrlValue::Integer(log.severity_number as i64),
    );
    // need to be convert to string
    map.insert(
        "Body".into(),
        log.body
            .as_ref()
            .map(|x| VrlValue::Bytes(log_body_to_string(x).into()))
            .unwrap_or(VrlValue::Null),
    );
    map.insert("ResourceSchemaUrl".into(), resource_schema_url);

    map.insert("ResourceAttributes".into(), resource_attr);
    map.insert("ScopeSchemaUrl".into(), scope_schema_url);
    map.insert("ScopeName".into(), scope_name);
    map.insert("ScopeVersion".into(), scope_version);
    map.insert("ScopeAttributes".into(), scope_attrs);
    map.insert("LogAttributes".into(), log_attrs);
    VrlValue::Object(map)
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
                options: std::collections::HashMap::from([(
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

#[derive(Clone)]
struct ExistingLogColumn {
    schema: ColumnSchema,
    datatype: ColumnDataType,
}

impl ExistingLogColumn {
    fn schema_for_request_type(&self, request_type: ColumnDataType) -> ColumnSchema {
        let mut schema = self.schema.clone();
        if request_type == ColumnDataType::Binary && self.is_json_binary() {
            schema.datatype = ColumnDataType::Binary as i32;
        }
        schema
    }

    fn is_json_binary(&self) -> bool {
        self.datatype == ColumnDataType::Json
            && matches!(
                self.schema
                    .datatype_extension
                    .as_ref()
                    .and_then(|datatype_extension| datatype_extension.type_ext.as_ref()),
                Some(TypeExt::JsonType(json_type))
                     if *json_type == JsonTypeExtension::JsonBinary as i32
            )
    }
}

#[derive(Default)]
struct ExistingLogSchema {
    columns: HashMap<String, ExistingLogColumn>,
}

impl ExistingLogSchema {
    fn try_from_table(table: &table::Table) -> Result<Self> {
        let table_info = table.table_info();
        Self::try_from_schema_parts(
            table.schema_ref().column_schemas(),
            &table_info.meta.primary_key_indices,
        )
    }

    fn try_from_schema_parts(
        column_schemas: &[datatypes::schema::ColumnSchema],
        primary_key_indices: &[usize],
    ) -> Result<Self> {
        let mut columns = HashMap::with_capacity(column_schemas.len());

        for (index, column_schema) in column_schemas.iter().enumerate() {
            let (datatype, datatype_extension) =
                ColumnDataTypeWrapper::try_from(column_schema.data_type.clone())
                    .map(|wrapper| wrapper.into_parts())
                    .map_err(Error::from)?;
            let semantic_type = if column_schema.is_time_index() {
                SemanticType::Timestamp
            } else if primary_key_indices.contains(&index) {
                SemanticType::Tag
            } else {
                SemanticType::Field
            };
            let schema = ColumnSchema {
                column_name: column_schema.name.clone(),
                datatype: datatype as i32,
                semantic_type: semantic_type as i32,
                datatype_extension,
                options: options_from_column_schema(column_schema),
            };
            columns.insert(
                schema.column_name.clone(),
                ExistingLogColumn { schema, datatype },
            );
        }

        Ok(Self { columns })
    }

    fn get(&self, column_name: &str) -> Option<&ExistingLogColumn> {
        self.columns.get(column_name)
    }
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
    select_info: &SelectInfo,
    select_schema: &mut SchemaInfo,
    attrs: &jsonb::Value,
    existing_schema: Option<&ExistingLogSchema>,
    table_name: &str,
) -> Result<Vec<GreptimeValue>> {
    // note we use schema.len instead of select_keys.len
    // because the len of the row value should always matches the len of the schema
    let mut extracted_values = vec![GreptimeValue::default(); select_schema.schema.len()];

    for key in select_info.keys.iter() {
        let Some(value) = attrs.get_by_name_ignore_case(key).cloned() else {
            continue;
        };
        let Some((schema, value)) =
            decide_column_schema_and_convert_value(key, value, existing_schema, table_name)?
        else {
            continue;
        };

        if let Some(index) = select_schema.index.get(key) {
            let column_schema = &select_schema.schema[*index];
            let column_schema: ColumnSchema = column_schema.clone().try_into()?;
            // datatype of the same column name should be the same
            ensure!(
                column_schema.datatype == schema.datatype,
                IncompatibleSchemaSnafu {
                    column_name: key,
                    datatype: column_schema.datatype().as_str_name(),
                    expected: column_schema.datatype,
                    actual: schema.datatype,
                }
            );
            extracted_values[*index] = value;
        } else {
            select_schema.schema.push(schema.into());
            select_schema
                .index
                .insert(key.clone(), select_schema.schema.len() - 1);
            extracted_values.push(value);
        }
    }

    Ok(extracted_values)
}

fn decide_column_schema_and_convert_value(
    column_name: &str,
    value: JsonbValue,
    existing_schema: Option<&ExistingLogSchema>,
    table_name: &str,
) -> Result<Option<(ColumnSchema, GreptimeValue)>> {
    if let Some(existing_column) = existing_schema.and_then(|schema| schema.get(column_name)) {
        return decide_existing_column_schema_and_convert_value(
            column_name,
            value,
            existing_column,
            table_name,
        );
    }

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

fn decide_existing_column_schema_and_convert_value(
    column_name: &str,
    value: JsonbValue,
    existing_column: &ExistingLogColumn,
    table_name: &str,
) -> Result<Option<(ColumnSchema, GreptimeValue)>> {
    let Some((value_data, request_type)) = jsonb_value_to_log_value_data(column_name, value, true)?
    else {
        return Ok(None);
    };
    let value_data = coerce_log_value_data(
        Some(value_data),
        existing_column.datatype,
        existing_column.schema.semantic_type(),
        request_type,
        existing_column.is_json_binary(),
        column_name,
        table_name,
    )?;

    Ok(Some((
        existing_column.schema.clone(),
        GreptimeValue { value_data },
    )))
}

fn jsonb_value_to_log_value_data(
    column_name: &str,
    value: JsonbValue,
    allow_float: bool,
) -> Result<Option<(ValueData, ColumnDataType)>> {
    match value {
        JsonbValue::String(s) => Ok(Some((
            ValueData::StringValue(s.into()),
            ColumnDataType::String,
        ))),
        JsonbValue::Number(n) => match n {
            JsonbNumber::Int64(i) => Ok(Some((ValueData::I64Value(i), ColumnDataType::Int64))),
            JsonbNumber::Float64(f) if allow_float => {
                Ok(Some((ValueData::F64Value(f), ColumnDataType::Float64)))
            }
            JsonbNumber::Float64(_) => UnsupportedJsonDataTypeForTagSnafu {
                ty: "FLOAT".to_string(),
                key: column_name,
            }
            .fail(),
            JsonbNumber::UInt64(u) => Ok(Some((ValueData::U64Value(u), ColumnDataType::Uint64))),
        },
        JsonbValue::Bool(b) => Ok(Some((ValueData::BoolValue(b), ColumnDataType::Boolean))),
        JsonbValue::Array(_) | JsonbValue::Object(_) => UnsupportedJsonDataTypeForTagSnafu {
            ty: "Json".to_string(),
            key: column_name,
        }
        .fail(),
        JsonbValue::Null => Ok(None),
    }
}

fn align_rows_with_existing_schema(
    schemas: &mut [ColumnSchema],
    rows: &mut [Row],
    existing_schema: Option<&ExistingLogSchema>,
    table_name: &str,
) -> Result<()> {
    let Some(existing_schema) = existing_schema else {
        return Ok(());
    };

    for (column_idx, schema) in schemas.iter_mut().enumerate() {
        let request_type = schema.datatype();
        let Some(existing_column) = existing_schema.get(&schema.column_name) else {
            // Existing tables own their primary key definition; request-only
            // columns must not expand it.
            if schema.semantic_type() == SemanticType::Tag {
                schema.semantic_type = SemanticType::Field as i32;
            }
            continue;
        };

        let target_type = existing_column.datatype;
        let semantic_type = existing_column.schema.semantic_type();
        let target_is_json_binary = existing_column.is_json_binary();
        for row in rows.iter_mut() {
            let Some(value) = row.values.get_mut(column_idx) else {
                continue;
            };
            value.value_data = coerce_log_value_data(
                value.value_data.take(),
                target_type,
                semantic_type,
                request_type,
                target_is_json_binary,
                &schema.column_name,
                table_name,
            )?;
        }
        *schema = existing_column.schema_for_request_type(request_type);
    }

    Ok(())
}

fn coerce_log_value_data(
    value_data: Option<ValueData>,
    target_type: ColumnDataType,
    _semantic_type: SemanticType,
    request_type: ColumnDataType,
    target_is_json_binary: bool,
    column_name: &str,
    table_name: &str,
) -> Result<Option<ValueData>> {
    let Some(value_data) = value_data else {
        return Ok(None);
    };

    if request_type == target_type {
        return Ok(Some(value_data));
    }

    if request_type == ColumnDataType::Binary && target_is_json_binary {
        return Ok(Some(value_data));
    }

    if is_timestamp_type(request_type)
        && let Some(target_unit) = timestamp_unit(target_type)
    {
        return align_timestamp_value(value_data, target_unit, column_name, table_name).map(Some);
    }

    if target_type == ColumnDataType::String {
        if let Ok(value_data) =
            coerce_value_data(&Some(value_data.clone()), target_type, request_type)
        {
            return Ok(value_data);
        }
        if let Some(value_data) = stringify_scalar_value(value_data) {
            return Ok(Some(value_data));
        }
    }

    InvalidParameterSnafu {
        reason: format!(
            "failed to align log column '{}' in table '{}' from {:?} to {:?}",
            column_name, table_name, request_type, target_type
        ),
    }
    .fail()
}

fn stringify_scalar_value(value_data: ValueData) -> Option<ValueData> {
    let value = match value_data {
        ValueData::StringValue(value) => value,
        ValueData::BoolValue(value) => value.to_string(),
        ValueData::I8Value(value) => value.to_string(),
        ValueData::I16Value(value) => value.to_string(),
        ValueData::I32Value(value) => value.to_string(),
        ValueData::I64Value(value) => value.to_string(),
        ValueData::U8Value(value) => value.to_string(),
        ValueData::U16Value(value) => value.to_string(),
        ValueData::U32Value(value) => value.to_string(),
        ValueData::U64Value(value) => value.to_string(),
        ValueData::F32Value(value) => value.to_string(),
        ValueData::F64Value(value) => value.to_string(),
        _ => return None,
    };
    Some(ValueData::StringValue(value))
}

fn align_timestamp_value(
    value_data: ValueData,
    target_unit: TimeUnit,
    column_name: &str,
    table_name: &str,
) -> Result<ValueData> {
    let timestamp = match value_data {
        ValueData::TimestampSecondValue(value) => Timestamp::new_second(value),
        ValueData::TimestampMillisecondValue(value) => Timestamp::new_millisecond(value),
        ValueData::TimestampMicrosecondValue(value) => Timestamp::new_microsecond(value),
        ValueData::TimestampNanosecondValue(value) => Timestamp::new_nanosecond(value),
        value_data => {
            return InvalidParameterSnafu {
                reason: format!(
                    "failed to align log column '{}' in table '{}' from non-timestamp value {:?}",
                    column_name, table_name, value_data
                ),
            }
            .fail();
        }
    };
    let timestamp = timestamp.convert_to(target_unit).ok_or_else(|| {
        InvalidParameterSnafu {
            reason: format!(
                "failed to align log column '{}' in table '{}' to timestamp unit {}",
                column_name, table_name, target_unit
            ),
        }
        .build()
    })?;

    Ok(match target_unit {
        TimeUnit::Second => ValueData::TimestampSecondValue(timestamp.value()),
        TimeUnit::Millisecond => ValueData::TimestampMillisecondValue(timestamp.value()),
        TimeUnit::Microsecond => ValueData::TimestampMicrosecondValue(timestamp.value()),
        TimeUnit::Nanosecond => ValueData::TimestampNanosecondValue(timestamp.value()),
    })
}

fn is_timestamp_type(datatype: ColumnDataType) -> bool {
    timestamp_unit(datatype).is_some()
}

fn timestamp_unit(datatype: ColumnDataType) -> Option<TimeUnit> {
    match datatype {
        ColumnDataType::TimestampSecond => Some(TimeUnit::Second),
        ColumnDataType::TimestampMillisecond => Some(TimeUnit::Millisecond),
        ColumnDataType::TimestampMicrosecond => Some(TimeUnit::Microsecond),
        ColumnDataType::TimestampNanosecond => Some(TimeUnit::Nanosecond),
        _ => None,
    }
}

fn parse_export_logs_service_request_to_rows(
    request: ExportLogsServiceRequest,
    select_info: Box<SelectInfo>,
    existing_schema: Option<&ExistingLogSchema>,
    table_name: &str,
) -> Result<Rows> {
    let mut schemas = build_otlp_logs_identity_schema();

    let mut parse_ctx = ParseContext::new(select_info, existing_schema, table_name);
    let mut rows = parse_resource(&mut parse_ctx, request.resource_logs)?;

    schemas.extend(parse_ctx.select_schema.column_schemas()?);
    align_rows_with_existing_schema(&mut schemas, &mut rows, existing_schema, table_name)?;

    rows.iter_mut().for_each(|row| {
        row.values.resize(schemas.len(), GreptimeValue::default());
    });

    Ok(Rows {
        schema: schemas,
        rows,
    })
}

fn parse_resource(
    parse_ctx: &mut ParseContext,
    resource_logs_vec: Vec<ResourceLogs>,
) -> Result<Vec<Row>> {
    let total_len = resource_logs_vec
        .iter()
        .flat_map(|r| r.scope_logs.iter())
        .map(|s| s.log_records.len())
        .sum();

    let mut results = Vec::with_capacity(total_len);

    for r in resource_logs_vec {
        parse_ctx.resource_attr = r
            .resource
            .map(|resource| key_value_to_jsonb(resource.attributes))
            .unwrap_or(JsonbValue::Null);

        parse_ctx.resource_url = r.schema_url;

        parse_ctx.resource_uplift_values = extract_field_from_attr_and_combine_schema(
            &parse_ctx.select_info,
            &mut parse_ctx.select_schema,
            &parse_ctx.resource_attr,
            parse_ctx.existing_schema,
            parse_ctx.table_name,
        )?;

        let rows = parse_scope(r.scope_logs, parse_ctx)?;
        results.extend(rows);
    }
    Ok(results)
}

struct ParseContext<'a> {
    // input selected keys
    select_info: Box<SelectInfo>,
    existing_schema: Option<&'a ExistingLogSchema>,
    table_name: &'a str,
    // schema infos for selected keys from resource/scope/log for current request
    // since the value override from bottom to top, the max capacity is the length of the keys
    select_schema: SchemaInfo,

    // extracted and uplifted values using select keys
    resource_uplift_values: Vec<GreptimeValue>,
    scope_uplift_values: Vec<GreptimeValue>,

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
        select_info: Box<SelectInfo>,
        existing_schema: Option<&'a ExistingLogSchema>,
        table_name: &'a str,
    ) -> ParseContext<'a> {
        let len = select_info.keys.len();
        ParseContext {
            select_info,
            existing_schema,
            table_name,
            select_schema: SchemaInfo::with_capacity(len),
            resource_uplift_values: vec![],
            scope_uplift_values: vec![],
            resource_url: String::new(),
            resource_attr: JsonbValue::Null,
            scope_name: None,
            scope_version: None,
            scope_url: String::new(),
            scope_attrs: JsonbValue::Null,
        }
    }
}

fn parse_scope(scopes_log_vec: Vec<ScopeLogs>, parse_ctx: &mut ParseContext) -> Result<Vec<Row>> {
    let len = scopes_log_vec.iter().map(|l| l.log_records.len()).sum();
    let mut results = Vec::with_capacity(len);

    for scope_logs in scopes_log_vec {
        let (scope_attrs, scope_version, scope_name) = scope_to_jsonb(scope_logs.scope);
        parse_ctx.scope_name = scope_name;
        parse_ctx.scope_version = scope_version;
        parse_ctx.scope_url = scope_logs.schema_url;
        parse_ctx.scope_attrs = scope_attrs;

        parse_ctx.scope_uplift_values = extract_field_from_attr_and_combine_schema(
            &parse_ctx.select_info,
            &mut parse_ctx.select_schema,
            &parse_ctx.scope_attrs,
            parse_ctx.existing_schema,
            parse_ctx.table_name,
        )?;

        let rows = parse_log(scope_logs.log_records, parse_ctx)?;
        results.extend(rows);
    }
    Ok(results)
}

fn parse_log(log_records: Vec<LogRecord>, parse_ctx: &mut ParseContext) -> Result<Vec<Row>> {
    let mut result = Vec::with_capacity(log_records.len());

    for log in log_records {
        let (mut row, log_attr) = build_otlp_build_in_row(log, parse_ctx);

        let log_values = extract_field_from_attr_and_combine_schema(
            &parse_ctx.select_info,
            &mut parse_ctx.select_schema,
            &log_attr,
            parse_ctx.existing_schema,
            parse_ctx.table_name,
        )?;

        let extracted_values = merge_values(
            log_values,
            &parse_ctx.scope_uplift_values,
            &parse_ctx.resource_uplift_values,
        );

        row.values.extend(extracted_values);

        result.push(row);
    }
    Ok(result)
}

fn merge_values(
    log: Vec<GreptimeValue>,
    scope: &[GreptimeValue],
    resource: &[GreptimeValue],
) -> Vec<GreptimeValue> {
    log.into_iter()
        .enumerate()
        .map(|(i, value)| GreptimeValue {
            value_data: value
                .value_data
                .or_else(|| scope.get(i).and_then(|x| x.value_data.clone()))
                .or_else(|| resource.get(i).and_then(|x| x.value_data.clone())),
        })
        .collect()
}

/// transform otlp logs request to pipeline value
/// https://opentelemetry.io/docs/concepts/signals/logs/
fn parse_export_logs_service_request(request: ExportLogsServiceRequest) -> Vec<VrlValue> {
    let mut result = Vec::new();
    for r in request.resource_logs {
        let resource_attr = r
            .resource
            .map(|x| VrlValue::Object(key_value_to_map(x.attributes)))
            .unwrap_or(VrlValue::Null);
        let resource_schema_url = VrlValue::Bytes(r.schema_url.into());
        for scope_logs in r.scope_logs {
            let (scope_attrs, scope_version, scope_name) =
                scope_to_pipeline_value(scope_logs.scope);
            let scope_schema_url = VrlValue::Bytes(scope_logs.schema_url.into());
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
fn any_value_to_vrl_value(value: any_value::Value) -> VrlValue {
    match value {
        any_value::Value::StringValue(s) => VrlValue::Bytes(s.into()),
        any_value::Value::IntValue(i) => VrlValue::Integer(i),
        any_value::Value::DoubleValue(d) => VrlValue::Float(NotNan::new(d).unwrap()),
        any_value::Value::BoolValue(b) => VrlValue::Boolean(b),
        any_value::Value::ArrayValue(array_value) => {
            let values = array_value
                .values
                .into_iter()
                .filter_map(|v| v.value.map(any_value_to_vrl_value))
                .collect();
            VrlValue::Array(values)
        }
        any_value::Value::KvlistValue(key_value_list) => {
            VrlValue::Object(key_value_to_map(key_value_list.values))
        }
        any_value::Value::BytesValue(items) => VrlValue::Bytes(Bytes::from(items)),
    }
}

// convert otlp keyValue vec to map
fn key_value_to_map(key_values: Vec<KeyValue>) -> BTreeMap<KeyString, VrlValue> {
    let mut map = BTreeMap::new();
    for kv in key_values {
        let value = match kv.value {
            Some(value) => match value.value {
                Some(value) => any_value_to_vrl_value(value),
                None => VrlValue::Null,
            },
            None => VrlValue::Null,
        };
        map.insert(kv.key.into(), value);
    }
    map
}

fn log_body_to_string(body: &AnyValue) -> String {
    let otlp_value = OtlpAnyValue::from(body);
    otlp_value.to_string()
}

#[cfg(test)]
mod tests {
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::ColumnSchema as DatatypesColumnSchema;
    use opentelemetry_proto::tonic::common::v1::any_value::Value as OtlpValue;

    use super::*;

    fn time_column(datatype: ConcreteDataType) -> DatatypesColumnSchema {
        DatatypesColumnSchema::new("timestamp", datatype, false).with_time_index(true)
    }

    fn column(name: &str, datatype: ConcreteDataType) -> DatatypesColumnSchema {
        DatatypesColumnSchema::new(name, datatype, true)
    }

    fn existing_schema(
        columns: Vec<DatatypesColumnSchema>,
        primary_key_indices: &[usize],
    ) -> ExistingLogSchema {
        ExistingLogSchema::try_from_schema_parts(&columns, primary_key_indices).unwrap()
    }

    fn kv(key: &str, value: OtlpValue) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(AnyValue { value: Some(value) }),
        }
    }

    fn request_with_log_attrs(attrs: Vec<KeyValue>) -> ExportLogsServiceRequest {
        ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord {
                        time_unix_nano: 1_234_000_000,
                        trace_id: vec![1; 16],
                        attributes: attrs,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    fn parse_with_select(
        request: ExportLogsServiceRequest,
        select: &str,
        existing_schema: Option<&ExistingLogSchema>,
    ) -> Result<Rows> {
        parse_export_logs_service_request_to_rows(
            request,
            Box::new(SelectInfo::from(select.to_string())),
            existing_schema,
            "test_logs",
        )
    }

    fn column_index(rows: &Rows, name: &str) -> usize {
        rows.schema
            .iter()
            .position(|schema| schema.column_name == name)
            .unwrap()
    }

    #[test]
    fn test_no_existing_table_preserves_direct_schema() {
        let rows = parse_with_select(request_with_log_attrs(vec![]), "", None).unwrap();

        assert_eq!(rows.schema[0].column_name, "timestamp");
        assert_eq!(
            rows.schema[0].datatype,
            ColumnDataType::TimestampNanosecond as i32
        );
        assert_eq!(rows.schema[0].semantic_type, SemanticType::Timestamp as i32);
        let scope_name_idx = column_index(&rows, "scope_name");
        assert_eq!(
            rows.schema[scope_name_idx].semantic_type,
            SemanticType::Tag as i32
        );
    }

    #[test]
    fn test_existing_primary_key_updates_builtin_column_semantic_type() {
        let existing = existing_schema(
            vec![
                time_column(ConcreteDataType::timestamp_nanosecond_datatype()),
                column("trace_id", ConcreteDataType::string_datatype()),
            ],
            &[1],
        );

        let rows = parse_with_select(request_with_log_attrs(vec![]), "", Some(&existing)).unwrap();
        let trace_id_idx = column_index(&rows, "trace_id");

        assert_eq!(
            rows.schema[trace_id_idx].semantic_type,
            SemanticType::Tag as i32
        );
    }

    #[test]
    fn test_existing_string_primary_key_stringifies_selected_scalar_values() {
        let existing = existing_schema(
            vec![
                time_column(ConcreteDataType::timestamp_nanosecond_datatype()),
                column("host", ConcreteDataType::string_datatype()),
            ],
            &[1],
        );
        let rows = parse_with_select(
            request_with_log_attrs(vec![kv("host", OtlpValue::IntValue(42))]),
            "host",
            Some(&existing),
        )
        .unwrap();
        let host_idx = column_index(&rows, "host");

        assert_eq!(
            rows.schema[host_idx].datatype,
            ColumnDataType::String as i32
        );
        assert_eq!(
            rows.schema[host_idx].semantic_type,
            SemanticType::Tag as i32
        );
        assert_eq!(
            rows.rows[0].values[host_idx].value_data,
            Some(ValueData::StringValue("42".to_string()))
        );
    }

    #[test]
    fn test_existing_string_field_stringifies_selected_scalar_values() {
        let existing = existing_schema(
            vec![
                time_column(ConcreteDataType::timestamp_nanosecond_datatype()),
                column("host", ConcreteDataType::string_datatype()),
            ],
            &[],
        );
        let rows = parse_with_select(
            request_with_log_attrs(vec![kv("host", OtlpValue::IntValue(42))]),
            "host",
            Some(&existing),
        )
        .unwrap();
        let host_idx = column_index(&rows, "host");

        assert_eq!(
            rows.schema[host_idx].datatype,
            ColumnDataType::String as i32
        );
        assert_eq!(
            rows.schema[host_idx].semantic_type,
            SemanticType::Field as i32
        );
        assert_eq!(
            rows.rows[0].values[host_idx].value_data,
            Some(ValueData::StringValue("42".to_string()))
        );
    }

    #[test]
    fn test_existing_non_string_primary_key_rejects_incompatible_selected_value() {
        let existing = existing_schema(
            vec![
                time_column(ConcreteDataType::timestamp_nanosecond_datatype()),
                column("host", ConcreteDataType::int64_datatype()),
            ],
            &[1],
        );
        let err = parse_with_select(
            request_with_log_attrs(vec![kv(
                "host",
                OtlpValue::StringValue("node-a".to_string()),
            )]),
            "host",
            Some(&existing),
        )
        .unwrap_err();

        assert!(
            err.to_string()
                .contains("failed to align log column 'host'")
        );
    }

    #[test]
    fn test_existing_timestamp_unit_is_respected() {
        let existing = existing_schema(
            vec![time_column(
                ConcreteDataType::timestamp_millisecond_datatype(),
            )],
            &[],
        );
        let rows = parse_with_select(request_with_log_attrs(vec![]), "", Some(&existing)).unwrap();

        assert_eq!(
            rows.schema[0].datatype,
            ColumnDataType::TimestampMillisecond as i32
        );
        assert_eq!(
            rows.rows[0].values[0].value_data,
            Some(ValueData::TimestampMillisecondValue(1234))
        );
    }

    #[test]
    fn test_missing_existing_primary_key_is_not_generated() {
        let existing = existing_schema(
            vec![
                time_column(ConcreteDataType::timestamp_nanosecond_datatype()),
                column("host", ConcreteDataType::string_datatype()),
            ],
            &[1],
        );
        let rows = parse_with_select(request_with_log_attrs(vec![]), "", Some(&existing)).unwrap();

        assert!(
            !rows
                .schema
                .iter()
                .any(|schema| schema.column_name == "host")
        );
    }

    #[test]
    fn test_existing_table_keeps_new_generated_columns_as_fields() {
        let existing = existing_schema(
            vec![
                time_column(ConcreteDataType::timestamp_nanosecond_datatype()),
                column("trace_id", ConcreteDataType::string_datatype()),
            ],
            &[1],
        );
        let rows = parse_with_select(
            request_with_log_attrs(vec![kv(
                "host",
                OtlpValue::StringValue("node-a".to_string()),
            )]),
            "host",
            Some(&existing),
        )
        .unwrap();
        let host_idx = column_index(&rows, "host");
        let scope_name_idx = column_index(&rows, "scope_name");

        assert_eq!(
            rows.schema[host_idx].semantic_type,
            SemanticType::Field as i32
        );
        assert_eq!(
            rows.schema[scope_name_idx].semantic_type,
            SemanticType::Field as i32
        );
    }
}
