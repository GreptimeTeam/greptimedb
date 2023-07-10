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

use std::ops::Deref;

use chrono::{NaiveDate, NaiveDateTime};
use datafusion_common::ScalarValue;
use datatypes::prelude::{ConcreteDataType, Value};
use datatypes::schema::Schema;
use datatypes::types::TimestampType;
use pgwire::api::portal::{Format, Portal};
use pgwire::api::results::{DataRowEncoder, FieldInfo};
use pgwire::api::Type;
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use query::plan::LogicalPlan;

use crate::error::{self, Error, Result};
use crate::SqlPlan;

pub(super) fn schema_to_pg(origin: &Schema, field_formats: &Format) -> Result<Vec<FieldInfo>> {
    origin
        .column_schemas()
        .iter()
        .enumerate()
        .map(|(idx, col)| {
            Ok(FieldInfo::new(
                col.name.clone(),
                None,
                None,
                type_gt_to_pg(&col.data_type)?,
                field_formats.format_for(idx),
            ))
        })
        .collect::<Result<Vec<FieldInfo>>>()
}

pub(super) fn encode_value(value: &Value, builder: &mut DataRowEncoder) -> PgWireResult<()> {
    match value {
        Value::Null => builder.encode_field(&None::<&i8>),
        Value::Boolean(v) => builder.encode_field(v),
        Value::UInt8(v) => builder.encode_field(&(*v as i8)),
        Value::UInt16(v) => builder.encode_field(&(*v as i16)),
        Value::UInt32(v) => builder.encode_field(v),
        Value::UInt64(v) => builder.encode_field(&(*v as i64)),
        Value::Int8(v) => builder.encode_field(v),
        Value::Int16(v) => builder.encode_field(v),
        Value::Int32(v) => builder.encode_field(v),
        Value::Int64(v) => builder.encode_field(v),
        Value::Float32(v) => builder.encode_field(&v.0),
        Value::Float64(v) => builder.encode_field(&v.0),
        Value::String(v) => builder.encode_field(&v.as_utf8()),
        Value::Binary(v) => builder.encode_field(&v.deref()),
        Value::Date(v) => {
            if let Some(date) = v.to_chrono_date() {
                builder.encode_field(&date)
            } else {
                Err(PgWireError::ApiError(Box::new(Error::Internal {
                    err_msg: format!("Failed to convert date to postgres type {v:?}",),
                })))
            }
        }
        Value::DateTime(v) => {
            if let Some(datetime) = v.to_chrono_datetime() {
                builder.encode_field(&datetime)
            } else {
                Err(PgWireError::ApiError(Box::new(Error::Internal {
                    err_msg: format!("Failed to convert date to postgres type {v:?}",),
                })))
            }
        }
        Value::Timestamp(v) => {
            if let Some(datetime) = v.to_chrono_datetime() {
                builder.encode_field(&datetime)
            } else {
                Err(PgWireError::ApiError(Box::new(Error::Internal {
                    err_msg: format!("Failed to convert date to postgres type {v:?}",),
                })))
            }
        }
        Value::List(_) => Err(PgWireError::ApiError(Box::new(Error::Internal {
            err_msg: format!(
                "cannot write value {:?} in postgres protocol: unimplemented",
                &value
            ),
        }))),
    }
}

pub(super) fn type_gt_to_pg(origin: &ConcreteDataType) -> Result<Type> {
    match origin {
        &ConcreteDataType::Null(_) => Ok(Type::UNKNOWN),
        &ConcreteDataType::Boolean(_) => Ok(Type::BOOL),
        &ConcreteDataType::Int8(_) | &ConcreteDataType::UInt8(_) => Ok(Type::CHAR),
        &ConcreteDataType::Int16(_) | &ConcreteDataType::UInt16(_) => Ok(Type::INT2),
        &ConcreteDataType::Int32(_) | &ConcreteDataType::UInt32(_) => Ok(Type::INT4),
        &ConcreteDataType::Int64(_) | &ConcreteDataType::UInt64(_) => Ok(Type::INT8),
        &ConcreteDataType::Float32(_) => Ok(Type::FLOAT4),
        &ConcreteDataType::Float64(_) => Ok(Type::FLOAT8),
        &ConcreteDataType::Binary(_) => Ok(Type::BYTEA),
        &ConcreteDataType::String(_) => Ok(Type::VARCHAR),
        &ConcreteDataType::Date(_) => Ok(Type::DATE),
        &ConcreteDataType::DateTime(_) => Ok(Type::TIMESTAMP),
        &ConcreteDataType::Timestamp(_) => Ok(Type::TIMESTAMP),
        &ConcreteDataType::List(_) | &ConcreteDataType::Dictionary(_) => error::InternalSnafu {
            err_msg: format!("not implemented for column datatype {origin:?}"),
        }
        .fail(),
    }
}

#[allow(dead_code)]
pub(super) fn type_pg_to_gt(origin: &Type) -> Result<ConcreteDataType> {
    // Note that we only support a small amount of pg data types
    match origin {
        &Type::BOOL => Ok(ConcreteDataType::boolean_datatype()),
        &Type::CHAR => Ok(ConcreteDataType::int8_datatype()),
        &Type::INT2 => Ok(ConcreteDataType::int16_datatype()),
        &Type::INT4 => Ok(ConcreteDataType::int32_datatype()),
        &Type::INT8 => Ok(ConcreteDataType::int64_datatype()),
        &Type::VARCHAR | &Type::TEXT => Ok(ConcreteDataType::string_datatype()),
        &Type::TIMESTAMP => Ok(ConcreteDataType::timestamp_datatype(
            common_time::timestamp::TimeUnit::Millisecond,
        )),
        &Type::DATE => Ok(ConcreteDataType::date_datatype()),
        &Type::TIME => Ok(ConcreteDataType::datetime_datatype()),
        _ => error::InternalSnafu {
            err_msg: format!("unimplemented datatype {origin:?}"),
        }
        .fail(),
    }
}

pub(super) fn parameter_to_string(portal: &Portal<SqlPlan>, idx: usize) -> PgWireResult<String> {
    // the index is managed from portal's parameters count so it's safe to
    // unwrap here.
    let param_type = portal.statement().parameter_types().get(idx).unwrap();
    match param_type {
        &Type::VARCHAR | &Type::TEXT => Ok(format!(
            "'{}'",
            portal.parameter::<String>(idx)?.as_deref().unwrap_or("")
        )),
        &Type::BOOL => Ok(portal
            .parameter::<bool>(idx)?
            .map(|v| v.to_string())
            .unwrap_or_else(|| "".to_owned())),
        &Type::INT4 => Ok(portal
            .parameter::<i32>(idx)?
            .map(|v| v.to_string())
            .unwrap_or_else(|| "".to_owned())),
        &Type::INT8 => Ok(portal
            .parameter::<i64>(idx)?
            .map(|v| v.to_string())
            .unwrap_or_else(|| "".to_owned())),
        &Type::FLOAT4 => Ok(portal
            .parameter::<f32>(idx)?
            .map(|v| v.to_string())
            .unwrap_or_else(|| "".to_owned())),
        &Type::FLOAT8 => Ok(portal
            .parameter::<f64>(idx)?
            .map(|v| v.to_string())
            .unwrap_or_else(|| "".to_owned())),
        &Type::DATE => Ok(portal
            .parameter::<NaiveDate>(idx)?
            .map(|v| v.format("%Y-%m-%d").to_string())
            .unwrap_or_else(|| "".to_owned())),
        &Type::TIMESTAMP => Ok(portal
            .parameter::<NaiveDateTime>(idx)?
            .map(|v| v.format("%Y-%m-%d %H:%M:%S%.6f").to_string())
            .unwrap_or_else(|| "".to_owned())),
        _ => Err(invalid_parameter_error(
            "unsupported_parameter_type",
            Some(&param_type.to_string()),
        )),
    }
}

pub(super) fn invalid_parameter_error(msg: &str, detail: Option<&str>) -> PgWireError {
    let mut error_info = ErrorInfo::new("ERROR".to_owned(), "22023".to_owned(), msg.to_owned());
    error_info.set_detail(detail.map(|s| s.to_owned()));
    PgWireError::UserError(Box::new(error_info))
}

fn to_timestamp_scalar_value<T>(
    data: Option<T>,
    unit: &TimestampType,
    ctype: &ConcreteDataType,
) -> PgWireResult<ScalarValue>
where
    T: Into<i64>,
{
    if let Some(n) = data {
        Value::Timestamp(unit.create_timestamp(n.into()))
            .try_to_scalar_value(ctype)
            .map_err(|e| PgWireError::ApiError(Box::new(e)))
    } else {
        Ok(ScalarValue::Null)
    }
}

pub(super) fn parameters_to_scalar_values(
    plan: &LogicalPlan,
    portal: &Portal<SqlPlan>,
) -> PgWireResult<Vec<ScalarValue>> {
    let param_count = portal.parameter_len();
    let mut results = Vec::with_capacity(param_count);

    let client_param_types = portal.statement().parameter_types();
    let param_types = plan
        .get_param_types()
        .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

    // ensure parameter count consistent for: client parameter types, server
    // parameter types and parameter count
    if param_types.len() != param_count {
        return Err(invalid_parameter_error(
            "invalid_parameter_count",
            Some(&format!(
                "Expected: {}, found: {}",
                param_types.len(),
                param_count
            )),
        ));
    }
    if client_param_types.len() != param_count {
        return Err(invalid_parameter_error(
            "invalid_parameter_count",
            Some(&format!(
                "Expected: {}, found: {}",
                client_param_types.len(),
                param_count
            )),
        ));
    }

    for (idx, client_type) in client_param_types.iter().enumerate() {
        let Some(Some(server_type)) = param_types.get(&format!("${}", idx + 1)) else { continue };
        let value = match client_type {
            &Type::VARCHAR | &Type::TEXT => {
                let data = portal.parameter::<String>(idx)?;
                match server_type {
                    ConcreteDataType::String(_) => ScalarValue::Utf8(data),
                    _ => {
                        return Err(invalid_parameter_error(
                            "invalid_parameter_type",
                            Some(&format!(
                                "Expected: {}, found: {}",
                                server_type, client_type
                            )),
                        ))
                    }
                }
            }
            &Type::BOOL => {
                let data = portal.parameter::<bool>(idx)?;
                match server_type {
                    ConcreteDataType::Boolean(_) => ScalarValue::Boolean(data),
                    _ => {
                        return Err(invalid_parameter_error(
                            "invalid_parameter_type",
                            Some(&format!(
                                "Expected: {}, found: {}",
                                server_type, client_type
                            )),
                        ))
                    }
                }
            }
            &Type::INT2 => {
                let data = portal.parameter::<i16>(idx)?;
                match server_type {
                    ConcreteDataType::Int8(_) => ScalarValue::Int8(data.map(|n| n as i8)),
                    ConcreteDataType::Int16(_) => ScalarValue::Int16(data),
                    ConcreteDataType::Int32(_) => ScalarValue::Int32(data.map(|n| n as i32)),
                    ConcreteDataType::Int64(_) => ScalarValue::Int64(data.map(|n| n as i64)),
                    ConcreteDataType::UInt8(_) => ScalarValue::UInt8(data.map(|n| n as u8)),
                    ConcreteDataType::UInt16(_) => ScalarValue::UInt16(data.map(|n| n as u16)),
                    ConcreteDataType::UInt32(_) => ScalarValue::UInt32(data.map(|n| n as u32)),
                    ConcreteDataType::UInt64(_) => ScalarValue::UInt64(data.map(|n| n as u64)),
                    ConcreteDataType::Timestamp(unit) => {
                        to_timestamp_scalar_value(data, unit, server_type)?
                    }
                    ConcreteDataType::DateTime(_) => ScalarValue::Date64(data.map(|d| d as i64)),
                    _ => {
                        return Err(invalid_parameter_error(
                            "invalid_parameter_type",
                            Some(&format!(
                                "Expected: {}, found: {}",
                                server_type, client_type
                            )),
                        ))
                    }
                }
            }
            &Type::INT4 => {
                let data = portal.parameter::<i32>(idx)?;
                match server_type {
                    ConcreteDataType::Int8(_) => ScalarValue::Int8(data.map(|n| n as i8)),
                    ConcreteDataType::Int16(_) => ScalarValue::Int16(data.map(|n| n as i16)),
                    ConcreteDataType::Int32(_) => ScalarValue::Int32(data),
                    ConcreteDataType::Int64(_) => ScalarValue::Int64(data.map(|n| n as i64)),
                    ConcreteDataType::UInt8(_) => ScalarValue::UInt8(data.map(|n| n as u8)),
                    ConcreteDataType::UInt16(_) => ScalarValue::UInt16(data.map(|n| n as u16)),
                    ConcreteDataType::UInt32(_) => ScalarValue::UInt32(data.map(|n| n as u32)),
                    ConcreteDataType::UInt64(_) => ScalarValue::UInt64(data.map(|n| n as u64)),
                    ConcreteDataType::Timestamp(unit) => {
                        to_timestamp_scalar_value(data, unit, server_type)?
                    }
                    ConcreteDataType::DateTime(_) => ScalarValue::Date64(data.map(|d| d as i64)),
                    _ => {
                        return Err(invalid_parameter_error(
                            "invalid_parameter_type",
                            Some(&format!(
                                "Expected: {}, found: {}",
                                server_type, client_type
                            )),
                        ))
                    }
                }
            }
            &Type::INT8 => {
                let data = portal.parameter::<i64>(idx)?;
                match server_type {
                    ConcreteDataType::Int8(_) => ScalarValue::Int8(data.map(|n| n as i8)),
                    ConcreteDataType::Int16(_) => ScalarValue::Int16(data.map(|n| n as i16)),
                    ConcreteDataType::Int32(_) => ScalarValue::Int32(data.map(|n| n as i32)),
                    ConcreteDataType::Int64(_) => ScalarValue::Int64(data),
                    ConcreteDataType::UInt8(_) => ScalarValue::UInt8(data.map(|n| n as u8)),
                    ConcreteDataType::UInt16(_) => ScalarValue::UInt16(data.map(|n| n as u16)),
                    ConcreteDataType::UInt32(_) => ScalarValue::UInt32(data.map(|n| n as u32)),
                    ConcreteDataType::UInt64(_) => ScalarValue::UInt64(data.map(|n| n as u64)),
                    ConcreteDataType::Timestamp(unit) => {
                        to_timestamp_scalar_value(data, unit, server_type)?
                    }
                    ConcreteDataType::DateTime(_) => ScalarValue::Date64(data),
                    _ => {
                        return Err(invalid_parameter_error(
                            "invalid_parameter_type",
                            Some(&format!(
                                "Expected: {}, found: {}",
                                server_type, client_type
                            )),
                        ))
                    }
                }
            }
            &Type::FLOAT4 => {
                let data = portal.parameter::<f32>(idx)?;
                match server_type {
                    ConcreteDataType::Int8(_) => ScalarValue::Int8(data.map(|n| n as i8)),
                    ConcreteDataType::Int16(_) => ScalarValue::Int16(data.map(|n| n as i16)),
                    ConcreteDataType::Int32(_) => ScalarValue::Int32(data.map(|n| n as i32)),
                    ConcreteDataType::Int64(_) => ScalarValue::Int64(data.map(|n| n as i64)),
                    ConcreteDataType::UInt8(_) => ScalarValue::UInt8(data.map(|n| n as u8)),
                    ConcreteDataType::UInt16(_) => ScalarValue::UInt16(data.map(|n| n as u16)),
                    ConcreteDataType::UInt32(_) => ScalarValue::UInt32(data.map(|n| n as u32)),
                    ConcreteDataType::UInt64(_) => ScalarValue::UInt64(data.map(|n| n as u64)),
                    ConcreteDataType::Float32(_) => ScalarValue::Float32(data),
                    ConcreteDataType::Float64(_) => ScalarValue::Float64(data.map(|n| n as f64)),
                    _ => {
                        return Err(invalid_parameter_error(
                            "invalid_parameter_type",
                            Some(&format!(
                                "Expected: {}, found: {}",
                                server_type, client_type
                            )),
                        ))
                    }
                }
            }
            &Type::FLOAT8 => {
                let data = portal.parameter::<f64>(idx)?;
                match server_type {
                    ConcreteDataType::Int8(_) => ScalarValue::Int8(data.map(|n| n as i8)),
                    ConcreteDataType::Int16(_) => ScalarValue::Int16(data.map(|n| n as i16)),
                    ConcreteDataType::Int32(_) => ScalarValue::Int32(data.map(|n| n as i32)),
                    ConcreteDataType::Int64(_) => ScalarValue::Int64(data.map(|n| n as i64)),
                    ConcreteDataType::UInt8(_) => ScalarValue::UInt8(data.map(|n| n as u8)),
                    ConcreteDataType::UInt16(_) => ScalarValue::UInt16(data.map(|n| n as u16)),
                    ConcreteDataType::UInt32(_) => ScalarValue::UInt32(data.map(|n| n as u32)),
                    ConcreteDataType::UInt64(_) => ScalarValue::UInt64(data.map(|n| n as u64)),
                    ConcreteDataType::Float32(_) => ScalarValue::Float32(data.map(|n| n as f32)),
                    ConcreteDataType::Float64(_) => ScalarValue::Float64(data),
                    _ => {
                        return Err(invalid_parameter_error(
                            "invalid_parameter_type",
                            Some(&format!(
                                "Expected: {}, found: {}",
                                server_type, client_type
                            )),
                        ))
                    }
                }
            }
            &Type::TIMESTAMP => {
                let data = portal.parameter::<NaiveDateTime>(idx)?;
                match server_type {
                    ConcreteDataType::Timestamp(unit) => match *unit {
                        TimestampType::Second(_) => {
                            ScalarValue::TimestampSecond(data.map(|ts| ts.timestamp()), None)
                        }
                        TimestampType::Millisecond(_) => ScalarValue::TimestampMillisecond(
                            data.map(|ts| ts.timestamp_millis()),
                            None,
                        ),
                        TimestampType::Microsecond(_) => ScalarValue::TimestampMicrosecond(
                            data.map(|ts| ts.timestamp_micros()),
                            None,
                        ),
                        TimestampType::Nanosecond(_) => ScalarValue::TimestampNanosecond(
                            data.map(|ts| ts.timestamp_micros()),
                            None,
                        ),
                    },
                    ConcreteDataType::DateTime(_) => {
                        ScalarValue::Date64(data.map(|d| d.timestamp_millis()))
                    }
                    _ => {
                        return Err(invalid_parameter_error(
                            "invalid_parameter_type",
                            Some(&format!(
                                "Expected: {}, found: {}",
                                server_type, client_type
                            )),
                        ))
                    }
                }
            }
            &Type::DATE => {
                let data = portal.parameter::<NaiveDate>(idx)?;
                match server_type {
                    ConcreteDataType::Date(_) => ScalarValue::Date32(data.map(|d| {
                        (d - NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days() as i32
                    })),
                    _ => {
                        return Err(invalid_parameter_error(
                            "invalid_parameter_type",
                            Some(&format!(
                                "Expected: {}, found: {}",
                                server_type, client_type
                            )),
                        ));
                    }
                }
            }
            &Type::BYTEA => {
                let data = portal.parameter::<Vec<u8>>(idx)?;
                match server_type {
                    ConcreteDataType::String(_) => {
                        ScalarValue::Utf8(data.map(|d| String::from_utf8_lossy(&d).to_string()))
                    }
                    ConcreteDataType::Binary(_) => ScalarValue::Binary(data),
                    _ => {
                        return Err(invalid_parameter_error(
                            "invalid_parameter_type",
                            Some(&format!(
                                "Expected: {}, found: {}",
                                server_type, client_type
                            )),
                        ));
                    }
                }
            }
            _ => Err(invalid_parameter_error(
                "unsupported_parameter_value",
                Some(&format!("Found type: {}", client_type)),
            ))?,
        };
        results.push(value);
    }

    Ok(results)
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use datatypes::schema::{ColumnSchema, Schema};
    use datatypes::value::ListValue;
    use pgwire::api::results::{FieldFormat, FieldInfo};
    use pgwire::api::Type;

    use super::*;

    #[test]
    fn test_schema_convert() {
        let column_schemas = vec![
            ColumnSchema::new("nulls", ConcreteDataType::null_datatype(), true),
            ColumnSchema::new("bools", ConcreteDataType::boolean_datatype(), true),
            ColumnSchema::new("int8s", ConcreteDataType::int8_datatype(), true),
            ColumnSchema::new("int16s", ConcreteDataType::int16_datatype(), true),
            ColumnSchema::new("int32s", ConcreteDataType::int32_datatype(), true),
            ColumnSchema::new("int64s", ConcreteDataType::int64_datatype(), true),
            ColumnSchema::new("uint8s", ConcreteDataType::uint8_datatype(), true),
            ColumnSchema::new("uint16s", ConcreteDataType::uint16_datatype(), true),
            ColumnSchema::new("uint32s", ConcreteDataType::uint32_datatype(), true),
            ColumnSchema::new("uint64s", ConcreteDataType::uint64_datatype(), true),
            ColumnSchema::new("float32s", ConcreteDataType::float32_datatype(), true),
            ColumnSchema::new("float64s", ConcreteDataType::float64_datatype(), true),
            ColumnSchema::new("binaries", ConcreteDataType::binary_datatype(), true),
            ColumnSchema::new("strings", ConcreteDataType::string_datatype(), true),
            ColumnSchema::new(
                "timestamps",
                ConcreteDataType::timestamp_millisecond_datatype(),
                true,
            ),
            ColumnSchema::new("dates", ConcreteDataType::date_datatype(), true),
        ];
        let pg_field_info = vec![
            FieldInfo::new("nulls".into(), None, None, Type::UNKNOWN, FieldFormat::Text),
            FieldInfo::new("bools".into(), None, None, Type::BOOL, FieldFormat::Text),
            FieldInfo::new("int8s".into(), None, None, Type::CHAR, FieldFormat::Text),
            FieldInfo::new("int16s".into(), None, None, Type::INT2, FieldFormat::Text),
            FieldInfo::new("int32s".into(), None, None, Type::INT4, FieldFormat::Text),
            FieldInfo::new("int64s".into(), None, None, Type::INT8, FieldFormat::Text),
            FieldInfo::new("uint8s".into(), None, None, Type::CHAR, FieldFormat::Text),
            FieldInfo::new("uint16s".into(), None, None, Type::INT2, FieldFormat::Text),
            FieldInfo::new("uint32s".into(), None, None, Type::INT4, FieldFormat::Text),
            FieldInfo::new("uint64s".into(), None, None, Type::INT8, FieldFormat::Text),
            FieldInfo::new(
                "float32s".into(),
                None,
                None,
                Type::FLOAT4,
                FieldFormat::Text,
            ),
            FieldInfo::new(
                "float64s".into(),
                None,
                None,
                Type::FLOAT8,
                FieldFormat::Text,
            ),
            FieldInfo::new(
                "binaries".into(),
                None,
                None,
                Type::BYTEA,
                FieldFormat::Text,
            ),
            FieldInfo::new(
                "strings".into(),
                None,
                None,
                Type::VARCHAR,
                FieldFormat::Text,
            ),
            FieldInfo::new(
                "timestamps".into(),
                None,
                None,
                Type::TIMESTAMP,
                FieldFormat::Text,
            ),
            FieldInfo::new("dates".into(), None, None, Type::DATE, FieldFormat::Text),
        ];
        let schema = Schema::new(column_schemas);
        let fs = schema_to_pg(&schema, &Format::UnifiedText).unwrap();
        assert_eq!(fs, pg_field_info);
    }

    #[test]
    fn test_encode_text_format_data() {
        let schema = vec![
            FieldInfo::new("nulls".into(), None, None, Type::UNKNOWN, FieldFormat::Text),
            FieldInfo::new("bools".into(), None, None, Type::BOOL, FieldFormat::Text),
            FieldInfo::new("uint8s".into(), None, None, Type::CHAR, FieldFormat::Text),
            FieldInfo::new("uint16s".into(), None, None, Type::INT2, FieldFormat::Text),
            FieldInfo::new("uint32s".into(), None, None, Type::INT4, FieldFormat::Text),
            FieldInfo::new("uint64s".into(), None, None, Type::INT8, FieldFormat::Text),
            FieldInfo::new("int8s".into(), None, None, Type::CHAR, FieldFormat::Text),
            FieldInfo::new("int8s".into(), None, None, Type::CHAR, FieldFormat::Text),
            FieldInfo::new("int16s".into(), None, None, Type::INT2, FieldFormat::Text),
            FieldInfo::new("int16s".into(), None, None, Type::INT2, FieldFormat::Text),
            FieldInfo::new("int32s".into(), None, None, Type::INT4, FieldFormat::Text),
            FieldInfo::new("int32s".into(), None, None, Type::INT4, FieldFormat::Text),
            FieldInfo::new("int64s".into(), None, None, Type::INT8, FieldFormat::Text),
            FieldInfo::new("int64s".into(), None, None, Type::INT8, FieldFormat::Text),
            FieldInfo::new(
                "float32s".into(),
                None,
                None,
                Type::FLOAT4,
                FieldFormat::Text,
            ),
            FieldInfo::new(
                "float32s".into(),
                None,
                None,
                Type::FLOAT4,
                FieldFormat::Text,
            ),
            FieldInfo::new(
                "float32s".into(),
                None,
                None,
                Type::FLOAT4,
                FieldFormat::Text,
            ),
            FieldInfo::new(
                "float64s".into(),
                None,
                None,
                Type::FLOAT8,
                FieldFormat::Text,
            ),
            FieldInfo::new(
                "float64s".into(),
                None,
                None,
                Type::FLOAT8,
                FieldFormat::Text,
            ),
            FieldInfo::new(
                "float64s".into(),
                None,
                None,
                Type::FLOAT8,
                FieldFormat::Text,
            ),
            FieldInfo::new(
                "strings".into(),
                None,
                None,
                Type::VARCHAR,
                FieldFormat::Text,
            ),
            FieldInfo::new(
                "binaries".into(),
                None,
                None,
                Type::BYTEA,
                FieldFormat::Text,
            ),
            FieldInfo::new("dates".into(), None, None, Type::DATE, FieldFormat::Text),
            FieldInfo::new(
                "datetimes".into(),
                None,
                None,
                Type::TIMESTAMP,
                FieldFormat::Text,
            ),
            FieldInfo::new(
                "timestamps".into(),
                None,
                None,
                Type::TIMESTAMP,
                FieldFormat::Text,
            ),
        ];

        let values = vec![
            Value::Null,
            Value::Boolean(true),
            Value::UInt8(u8::MAX),
            Value::UInt16(u16::MAX),
            Value::UInt32(u32::MAX),
            Value::UInt64(u64::MAX),
            Value::Int8(i8::MAX),
            Value::Int8(i8::MIN),
            Value::Int16(i16::MAX),
            Value::Int16(i16::MIN),
            Value::Int32(i32::MAX),
            Value::Int32(i32::MIN),
            Value::Int64(i64::MAX),
            Value::Int64(i64::MIN),
            Value::Float32(f32::MAX.into()),
            Value::Float32(f32::MIN.into()),
            Value::Float32(0f32.into()),
            Value::Float64(f64::MAX.into()),
            Value::Float64(f64::MIN.into()),
            Value::Float64(0f64.into()),
            Value::String("greptime".into()),
            Value::Binary("greptime".as_bytes().into()),
            Value::Date(1001i32.into()),
            Value::DateTime(1000001i64.into()),
            Value::Timestamp(1000001i64.into()),
        ];
        let mut builder = DataRowEncoder::new(Arc::new(schema));
        for i in values.iter() {
            encode_value(i, &mut builder).unwrap();
        }

        let err = encode_value(
            &Value::List(ListValue::new(
                Some(Box::default()),
                ConcreteDataType::int16_datatype(),
            )),
            &mut builder,
        )
        .unwrap_err();
        match err {
            PgWireError::ApiError(e) => {
                assert!(format!("{e}").contains("Internal error:"));
            }
            _ => {
                unreachable!()
            }
        }
    }
}
