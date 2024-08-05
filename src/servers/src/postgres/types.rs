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

mod bytea;
mod datetime;
mod error;
mod interval;

use std::collections::HashMap;
use std::ops::Deref;

use chrono::{NaiveDate, NaiveDateTime};
use common_time::Interval;
use datafusion_common::ScalarValue;
use datatypes::prelude::{ConcreteDataType, Value};
use datatypes::schema::Schema;
use datatypes::types::TimestampType;
use pgwire::api::portal::{Format, Portal};
use pgwire::api::results::{DataRowEncoder, FieldInfo};
use pgwire::api::Type;
use pgwire::error::{PgWireError, PgWireResult};
use query::plan::LogicalPlan;
use session::context::QueryContextRef;
use session::session_config::PGByteaOutputValue;

use self::bytea::{EscapeOutputBytea, HexOutputBytea};
use self::datetime::{StylingDate, StylingDateTime};
pub use self::error::PgErrorCode;
use self::interval::PgInterval;
use crate::error::{self as server_error, Error, Result};
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

pub(super) fn encode_value(
    query_ctx: &QueryContextRef,
    value: &Value,
    builder: &mut DataRowEncoder,
) -> PgWireResult<()> {
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
        Value::Binary(v) => {
            let bytea_output = query_ctx.configuration_parameter().postgres_bytea_output();
            match *bytea_output {
                PGByteaOutputValue::ESCAPE => builder.encode_field(&EscapeOutputBytea(v.deref())),
                PGByteaOutputValue::HEX => builder.encode_field(&HexOutputBytea(v.deref())),
            }
        }
        Value::Date(v) => {
            if let Some(date) = v.to_chrono_date() {
                let (style, order) = *query_ctx.configuration_parameter().pg_datetime_style();
                builder.encode_field(&StylingDate(&date, style, order))
            } else {
                Err(PgWireError::ApiError(Box::new(Error::Internal {
                    err_msg: format!("Failed to convert date to postgres type {v:?}",),
                })))
            }
        }
        Value::DateTime(v) => {
            if let Some(datetime) = v.to_chrono_datetime() {
                let (style, order) = *query_ctx.configuration_parameter().pg_datetime_style();
                builder.encode_field(&StylingDateTime(&datetime, style, order))
            } else {
                Err(PgWireError::ApiError(Box::new(Error::Internal {
                    err_msg: format!("Failed to convert date to postgres type {v:?}",),
                })))
            }
        }
        Value::Timestamp(v) => {
            if let Some(datetime) = v.to_chrono_datetime() {
                let (style, order) = *query_ctx.configuration_parameter().pg_datetime_style();
                builder.encode_field(&StylingDateTime(&datetime, style, order))
            } else {
                Err(PgWireError::ApiError(Box::new(Error::Internal {
                    err_msg: format!("Failed to convert date to postgres type {v:?}",),
                })))
            }
        }
        Value::Time(v) => {
            if let Some(time) = v.to_chrono_time() {
                builder.encode_field(&time)
            } else {
                Err(PgWireError::ApiError(Box::new(Error::Internal {
                    err_msg: format!("Failed to convert time to postgres type {v:?}",),
                })))
            }
        }
        Value::Interval(v) => builder.encode_field(&PgInterval::from(*v)),
        Value::Decimal128(v) => builder.encode_field(&v.to_string()),
        Value::List(_) | Value::Duration(_) => {
            Err(PgWireError::ApiError(Box::new(Error::Internal {
                err_msg: format!(
                    "cannot write value {:?} in postgres protocol: unimplemented",
                    &value
                ),
            })))
        }
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
        &ConcreteDataType::Time(_) => Ok(Type::TIME),
        &ConcreteDataType::Interval(_) => Ok(Type::INTERVAL),
        &ConcreteDataType::Decimal128(_) => Ok(Type::NUMERIC),
        &ConcreteDataType::Duration(_)
        | &ConcreteDataType::List(_)
        | &ConcreteDataType::Dictionary(_) => server_error::UnsupportedDataTypeSnafu {
            data_type: origin,
            reason: "not implemented",
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
        _ => server_error::InternalSnafu {
            err_msg: format!("unimplemented datatype {origin:?}"),
        }
        .fail(),
    }
}

pub(super) fn parameter_to_string(portal: &Portal<SqlPlan>, idx: usize) -> PgWireResult<String> {
    // the index is managed from portal's parameters count so it's safe to
    // unwrap here.
    let param_type = portal.statement.parameter_types.get(idx).unwrap();
    match param_type {
        &Type::VARCHAR | &Type::TEXT => Ok(format!(
            "'{}'",
            portal
                .parameter::<String>(idx, param_type)?
                .as_deref()
                .unwrap_or("")
        )),
        &Type::BOOL => Ok(portal
            .parameter::<bool>(idx, param_type)?
            .map(|v| v.to_string())
            .unwrap_or_else(|| "".to_owned())),
        &Type::INT4 => Ok(portal
            .parameter::<i32>(idx, param_type)?
            .map(|v| v.to_string())
            .unwrap_or_else(|| "".to_owned())),
        &Type::INT8 => Ok(portal
            .parameter::<i64>(idx, param_type)?
            .map(|v| v.to_string())
            .unwrap_or_else(|| "".to_owned())),
        &Type::FLOAT4 => Ok(portal
            .parameter::<f32>(idx, param_type)?
            .map(|v| v.to_string())
            .unwrap_or_else(|| "".to_owned())),
        &Type::FLOAT8 => Ok(portal
            .parameter::<f64>(idx, param_type)?
            .map(|v| v.to_string())
            .unwrap_or_else(|| "".to_owned())),
        &Type::DATE => Ok(portal
            .parameter::<NaiveDate>(idx, param_type)?
            .map(|v| v.format("%Y-%m-%d").to_string())
            .unwrap_or_else(|| "".to_owned())),
        &Type::TIMESTAMP => Ok(portal
            .parameter::<NaiveDateTime>(idx, param_type)?
            .map(|v| v.format("%Y-%m-%d %H:%M:%S%.6f").to_string())
            .unwrap_or_else(|| "".to_owned())),
        &Type::INTERVAL => Ok(portal
            .parameter::<PgInterval>(idx, param_type)?
            .map(|v| v.to_string())
            .unwrap_or_else(|| "".to_owned())),
        _ => Err(invalid_parameter_error(
            "unsupported_parameter_type",
            Some(&param_type.to_string()),
        )),
    }
}

pub(super) fn invalid_parameter_error(msg: &str, detail: Option<&str>) -> PgWireError {
    let mut error_info = PgErrorCode::Ec22023.to_err_info(msg.to_string());
    error_info.detail = detail.map(|s| s.to_owned());
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

    let client_param_types = &portal.statement.parameter_types;
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

    for idx in 0..param_count {
        let server_type =
            if let Some(Some(server_infer_type)) = param_types.get(&format!("${}", idx + 1)) {
                server_infer_type
            } else {
                // at the moment we require type information inferenced by
                // server so here we return error if the type is unknown from
                // server-side.
                //
                // It might be possible to parse the parameter just using client
                // specified type, we will implement that if there is a case.
                return Err(invalid_parameter_error("unknown_parameter_type", None));
            };

        let client_type = if let Some(client_given_type) = client_param_types.get(idx) {
            client_given_type.clone()
        } else {
            type_gt_to_pg(server_type).map_err(|e| PgWireError::ApiError(Box::new(e)))?
        };

        let value = match &client_type {
            &Type::VARCHAR | &Type::TEXT => {
                let data = portal.parameter::<String>(idx, &client_type)?;
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
                let data = portal.parameter::<bool>(idx, &client_type)?;
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
                let data = portal.parameter::<i16>(idx, &client_type)?;
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
                let data = portal.parameter::<i32>(idx, &client_type)?;
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
                let data = portal.parameter::<i64>(idx, &client_type)?;
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
                let data = portal.parameter::<f32>(idx, &client_type)?;
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
                let data = portal.parameter::<f64>(idx, &client_type)?;
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
                let data = portal.parameter::<NaiveDateTime>(idx, &client_type)?;
                match server_type {
                    ConcreteDataType::Timestamp(unit) => match *unit {
                        TimestampType::Second(_) => ScalarValue::TimestampSecond(
                            data.map(|ts| ts.and_utc().timestamp()),
                            None,
                        ),
                        TimestampType::Millisecond(_) => ScalarValue::TimestampMillisecond(
                            data.map(|ts| ts.and_utc().timestamp_millis()),
                            None,
                        ),
                        TimestampType::Microsecond(_) => ScalarValue::TimestampMicrosecond(
                            data.map(|ts| ts.and_utc().timestamp_micros()),
                            None,
                        ),
                        TimestampType::Nanosecond(_) => ScalarValue::TimestampNanosecond(
                            data.map(|ts| ts.and_utc().timestamp_micros()),
                            None,
                        ),
                    },
                    ConcreteDataType::DateTime(_) => {
                        ScalarValue::Date64(data.map(|d| d.and_utc().timestamp_millis()))
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
                let data = portal.parameter::<NaiveDate>(idx, &client_type)?;
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
            &Type::INTERVAL => {
                let data = portal.parameter::<PgInterval>(idx, &client_type)?;
                match server_type {
                    ConcreteDataType::Interval(_) => {
                        ScalarValue::IntervalMonthDayNano(data.map(|i| Interval::from(i).to_i128()))
                    }
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
                let data = portal.parameter::<Vec<u8>>(idx, &client_type)?;
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

pub(super) fn param_types_to_pg_types(
    param_types: &HashMap<String, Option<ConcreteDataType>>,
) -> Result<Vec<Type>> {
    let param_count = param_types.len();
    let mut types = Vec::with_capacity(param_count);
    for i in 0..param_count {
        if let Some(Some(param_type)) = param_types.get(&format!("${}", i + 1)) {
            let pg_type = type_gt_to_pg(param_type)?;
            types.push(pg_type);
        } else {
            types.push(Type::UNKNOWN);
        }
    }
    Ok(types)
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use datatypes::schema::{ColumnSchema, Schema};
    use datatypes::value::ListValue;
    use pgwire::api::results::{FieldFormat, FieldInfo};
    use pgwire::api::Type;
    use session::context::QueryContextBuilder;

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
            ColumnSchema::new("times", ConcreteDataType::time_second_datatype(), true),
            ColumnSchema::new(
                "intervals",
                ConcreteDataType::interval_month_day_nano_datatype(),
                true,
            ),
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
            FieldInfo::new("times".into(), None, None, Type::TIME, FieldFormat::Text),
            FieldInfo::new(
                "intervals".into(),
                None,
                None,
                Type::INTERVAL,
                FieldFormat::Text,
            ),
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
            FieldInfo::new("times".into(), None, None, Type::TIME, FieldFormat::Text),
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
            FieldInfo::new(
                "intervals".into(),
                None,
                None,
                Type::INTERVAL,
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
            Value::Time(1001i64.into()),
            Value::DateTime(1000001i64.into()),
            Value::Timestamp(1000001i64.into()),
            Value::Interval(1000001i128.into()),
        ];
        let query_context = QueryContextBuilder::default()
            .configuration_parameter(Default::default())
            .build()
            .into();
        let mut builder = DataRowEncoder::new(Arc::new(schema));
        for i in values.iter() {
            encode_value(&query_context, i, &mut builder).unwrap();
        }

        let err = encode_value(
            &query_context,
            &Value::List(ListValue::new(vec![], ConcreteDataType::int16_datatype())),
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

    #[test]
    fn test_invalid_parameter() {
        // test for refactor with PgErrorCode
        let msg = "invalid_parameter_count";
        let error = invalid_parameter_error(msg, None);
        if let PgWireError::UserError(value) = error {
            assert_eq!("ERROR", value.severity);
            assert_eq!("22023", value.code);
            assert_eq!(msg, value.message);
        } else {
            panic!("test_invalid_parameter failed");
        }
    }
}
