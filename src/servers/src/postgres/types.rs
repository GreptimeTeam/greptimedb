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

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime};
use common_time::{IntervalDayTime, IntervalMonthDayNano, IntervalYearMonth};
use datafusion_common::ScalarValue;
use datafusion_expr::LogicalPlan;
use datatypes::arrow::datatypes::DataType as ArrowDataType;
use datatypes::prelude::{ConcreteDataType, Value};
use datatypes::schema::Schema;
use datatypes::types::{json_type_value_to_string, IntervalType, TimestampType};
use datatypes::value::ListValue;
use pgwire::api::portal::{Format, Portal};
use pgwire::api::results::{DataRowEncoder, FieldInfo};
use pgwire::api::Type;
use pgwire::error::{PgWireError, PgWireResult};
use session::context::QueryContextRef;
use session::session_config::PGByteaOutputValue;
use snafu::ResultExt;

use self::bytea::{EscapeOutputBytea, HexOutputBytea};
use self::datetime::{StylingDate, StylingDateTime};
pub use self::error::{PgErrorCode, PgErrorSeverity};
use self::interval::PgInterval;
use crate::error::{self as server_error, DataFusionSnafu, Error, Result};
use crate::postgres::utils::convert_err;
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

fn encode_array(
    query_ctx: &QueryContextRef,
    value_list: &ListValue,
    builder: &mut DataRowEncoder,
) -> PgWireResult<()> {
    match value_list.datatype() {
        &ConcreteDataType::Boolean(_) => {
            let array = value_list
                .items()
                .iter()
                .map(|v| match v {
                    Value::Null => Ok(None),
                    Value::Boolean(v) => Ok(Some(*v)),
                    _ => Err(convert_err(Error::Internal {
                        err_msg: format!("Invalid list item type, find {v:?}, expected bool",),
                    })),
                })
                .collect::<PgWireResult<Vec<Option<bool>>>>()?;
            builder.encode_field(&array)
        }
        &ConcreteDataType::Int8(_) | &ConcreteDataType::UInt8(_) => {
            let array = value_list
                .items()
                .iter()
                .map(|v| match v {
                    Value::Null => Ok(None),
                    Value::Int8(v) => Ok(Some(*v)),
                    Value::UInt8(v) => Ok(Some(*v as i8)),
                    _ => Err(convert_err(Error::Internal {
                        err_msg: format!(
                            "Invalid list item type, find {v:?}, expected int8 or uint8",
                        ),
                    })),
                })
                .collect::<PgWireResult<Vec<Option<i8>>>>()?;
            builder.encode_field(&array)
        }
        &ConcreteDataType::Int16(_) | &ConcreteDataType::UInt16(_) => {
            let array = value_list
                .items()
                .iter()
                .map(|v| match v {
                    Value::Null => Ok(None),
                    Value::Int16(v) => Ok(Some(*v)),
                    Value::UInt16(v) => Ok(Some(*v as i16)),
                    _ => Err(convert_err(Error::Internal {
                        err_msg: format!(
                            "Invalid list item type, find {v:?}, expected int16 or uint16",
                        ),
                    })),
                })
                .collect::<PgWireResult<Vec<Option<i16>>>>()?;
            builder.encode_field(&array)
        }
        &ConcreteDataType::Int32(_) | &ConcreteDataType::UInt32(_) => {
            let array = value_list
                .items()
                .iter()
                .map(|v| match v {
                    Value::Null => Ok(None),
                    Value::Int32(v) => Ok(Some(*v)),
                    Value::UInt32(v) => Ok(Some(*v as i32)),
                    _ => Err(convert_err(Error::Internal {
                        err_msg: format!(
                            "Invalid list item type, find {v:?}, expected int32 or uint32",
                        ),
                    })),
                })
                .collect::<PgWireResult<Vec<Option<i32>>>>()?;
            builder.encode_field(&array)
        }
        &ConcreteDataType::Int64(_) | &ConcreteDataType::UInt64(_) => {
            let array = value_list
                .items()
                .iter()
                .map(|v| match v {
                    Value::Null => Ok(None),
                    Value::Int64(v) => Ok(Some(*v)),
                    Value::UInt64(v) => Ok(Some(*v as i64)),
                    _ => Err(convert_err(Error::Internal {
                        err_msg: format!(
                            "Invalid list item type, find {v:?}, expected int64 or uint64",
                        ),
                    })),
                })
                .collect::<PgWireResult<Vec<Option<i64>>>>()?;
            builder.encode_field(&array)
        }
        &ConcreteDataType::Float32(_) => {
            let array = value_list
                .items()
                .iter()
                .map(|v| match v {
                    Value::Null => Ok(None),
                    Value::Float32(v) => Ok(Some(v.0)),
                    _ => Err(convert_err(Error::Internal {
                        err_msg: format!("Invalid list item type, find {v:?}, expected float32",),
                    })),
                })
                .collect::<PgWireResult<Vec<Option<f32>>>>()?;
            builder.encode_field(&array)
        }
        &ConcreteDataType::Float64(_) => {
            let array = value_list
                .items()
                .iter()
                .map(|v| match v {
                    Value::Null => Ok(None),
                    Value::Float64(v) => Ok(Some(v.0)),
                    _ => Err(convert_err(Error::Internal {
                        err_msg: format!("Invalid list item type, find {v:?}, expected float64",),
                    })),
                })
                .collect::<PgWireResult<Vec<Option<f64>>>>()?;
            builder.encode_field(&array)
        }
        &ConcreteDataType::Binary(_) | &ConcreteDataType::Vector(_) => {
            let bytea_output = query_ctx.configuration_parameter().postgres_bytea_output();

            match *bytea_output {
                PGByteaOutputValue::ESCAPE => {
                    let array = value_list
                        .items()
                        .iter()
                        .map(|v| match v {
                            Value::Null => Ok(None),
                            Value::Binary(v) => Ok(Some(EscapeOutputBytea(v.deref()))),

                            _ => Err(convert_err(Error::Internal {
                                err_msg: format!(
                                    "Invalid list item type, find {v:?}, expected binary",
                                ),
                            })),
                        })
                        .collect::<PgWireResult<Vec<Option<EscapeOutputBytea>>>>()?;
                    builder.encode_field(&array)
                }
                PGByteaOutputValue::HEX => {
                    let array = value_list
                        .items()
                        .iter()
                        .map(|v| match v {
                            Value::Null => Ok(None),
                            Value::Binary(v) => Ok(Some(HexOutputBytea(v.deref()))),

                            _ => Err(convert_err(Error::Internal {
                                err_msg: format!(
                                    "Invalid list item type, find {v:?}, expected binary",
                                ),
                            })),
                        })
                        .collect::<PgWireResult<Vec<Option<HexOutputBytea>>>>()?;
                    builder.encode_field(&array)
                }
            }
        }
        &ConcreteDataType::String(_) => {
            let array = value_list
                .items()
                .iter()
                .map(|v| match v {
                    Value::Null => Ok(None),
                    Value::String(v) => Ok(Some(v.as_utf8())),
                    _ => Err(convert_err(Error::Internal {
                        err_msg: format!("Invalid list item type, find {v:?}, expected string",),
                    })),
                })
                .collect::<PgWireResult<Vec<Option<&str>>>>()?;
            builder.encode_field(&array)
        }
        &ConcreteDataType::Date(_) => {
            let array = value_list
                .items()
                .iter()
                .map(|v| match v {
                    Value::Null => Ok(None),
                    Value::Date(v) => {
                        if let Some(date) = v.to_chrono_date() {
                            let (style, order) =
                                *query_ctx.configuration_parameter().pg_datetime_style();
                            Ok(Some(StylingDate(date, style, order)))
                        } else {
                            Err(convert_err(Error::Internal {
                                err_msg: format!("Failed to convert date to postgres type {v:?}",),
                            }))
                        }
                    }
                    _ => Err(convert_err(Error::Internal {
                        err_msg: format!("Invalid list item type, find {v:?}, expected date",),
                    })),
                })
                .collect::<PgWireResult<Vec<Option<StylingDate>>>>()?;
            builder.encode_field(&array)
        }
        &ConcreteDataType::Timestamp(_) => {
            let array = value_list
                .items()
                .iter()
                .map(|v| match v {
                    Value::Null => Ok(None),
                    Value::Timestamp(v) => {
                        if let Some(datetime) =
                            v.to_chrono_datetime_with_timezone(Some(&query_ctx.timezone()))
                        {
                            let (style, order) =
                                *query_ctx.configuration_parameter().pg_datetime_style();
                            Ok(Some(StylingDateTime(datetime, style, order)))
                        } else {
                            Err(convert_err(Error::Internal {
                                err_msg: format!("Failed to convert date to postgres type {v:?}",),
                            }))
                        }
                    }
                    _ => Err(convert_err(Error::Internal {
                        err_msg: format!("Invalid list item type, find {v:?}, expected timestamp",),
                    })),
                })
                .collect::<PgWireResult<Vec<Option<StylingDateTime>>>>()?;
            builder.encode_field(&array)
        }
        &ConcreteDataType::Time(_) => {
            let array = value_list
                .items()
                .iter()
                .map(|v| match v {
                    Value::Null => Ok(None),
                    Value::Time(v) => Ok(v.to_chrono_time()),
                    _ => Err(convert_err(Error::Internal {
                        err_msg: format!("Invalid list item type, find {v:?}, expected time",),
                    })),
                })
                .collect::<PgWireResult<Vec<Option<NaiveTime>>>>()?;
            builder.encode_field(&array)
        }
        &ConcreteDataType::Interval(_) => {
            let array = value_list
                .items()
                .iter()
                .map(|v| match v {
                    Value::Null => Ok(None),
                    Value::IntervalYearMonth(v) => Ok(Some(PgInterval::from(*v))),
                    Value::IntervalDayTime(v) => Ok(Some(PgInterval::from(*v))),
                    Value::IntervalMonthDayNano(v) => Ok(Some(PgInterval::from(*v))),
                    _ => Err(convert_err(Error::Internal {
                        err_msg: format!("Invalid list item type, find {v:?}, expected interval",),
                    })),
                })
                .collect::<PgWireResult<Vec<Option<PgInterval>>>>()?;
            builder.encode_field(&array)
        }
        &ConcreteDataType::Decimal128(_) => {
            let array = value_list
                .items()
                .iter()
                .map(|v| match v {
                    Value::Null => Ok(None),
                    Value::Decimal128(v) => Ok(Some(v.to_string())),
                    _ => Err(convert_err(Error::Internal {
                        err_msg: format!("Invalid list item type, find {v:?}, expected decimal",),
                    })),
                })
                .collect::<PgWireResult<Vec<Option<String>>>>()?;
            builder.encode_field(&array)
        }
        &ConcreteDataType::Json(j) => {
            let array = value_list
                .items()
                .iter()
                .map(|v| match v {
                    Value::Null => Ok(None),
                    Value::Binary(v) => {
                        let s = json_type_value_to_string(v, &j.format).map_err(convert_err)?;
                        Ok(Some(s))
                    }
                    _ => Err(convert_err(Error::Internal {
                        err_msg: format!("Invalid list item type, find {v:?}, expected json",),
                    })),
                })
                .collect::<PgWireResult<Vec<Option<String>>>>()?;
            builder.encode_field(&array)
        }
        _ => Err(convert_err(Error::Internal {
            err_msg: format!(
                "cannot write array type {:?} in postgres protocol: unimplemented",
                value_list.datatype()
            ),
        })),
    }
}

pub(super) fn encode_value(
    query_ctx: &QueryContextRef,
    value: &Value,
    builder: &mut DataRowEncoder,
    datatype: &ConcreteDataType,
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
        Value::Binary(v) => match datatype {
            ConcreteDataType::Json(j) => {
                let s = json_type_value_to_string(v, &j.format).map_err(convert_err)?;
                builder.encode_field(&s)
            }
            _ => {
                let bytea_output = query_ctx.configuration_parameter().postgres_bytea_output();
                match *bytea_output {
                    PGByteaOutputValue::ESCAPE => {
                        builder.encode_field(&EscapeOutputBytea(v.deref()))
                    }
                    PGByteaOutputValue::HEX => builder.encode_field(&HexOutputBytea(v.deref())),
                }
            }
        },
        Value::Date(v) => {
            if let Some(date) = v.to_chrono_date() {
                let (style, order) = *query_ctx.configuration_parameter().pg_datetime_style();
                builder.encode_field(&StylingDate(date, style, order))
            } else {
                Err(convert_err(Error::Internal {
                    err_msg: format!("Failed to convert date to postgres type {v:?}",),
                }))
            }
        }
        Value::Timestamp(v) => {
            if let Some(datetime) = v.to_chrono_datetime_with_timezone(Some(&query_ctx.timezone()))
            {
                let (style, order) = *query_ctx.configuration_parameter().pg_datetime_style();
                builder.encode_field(&StylingDateTime(datetime, style, order))
            } else {
                Err(convert_err(Error::Internal {
                    err_msg: format!("Failed to convert date to postgres type {v:?}",),
                }))
            }
        }
        Value::Time(v) => {
            if let Some(time) = v.to_chrono_time() {
                builder.encode_field(&time)
            } else {
                Err(convert_err(Error::Internal {
                    err_msg: format!("Failed to convert time to postgres type {v:?}",),
                }))
            }
        }
        Value::IntervalYearMonth(v) => builder.encode_field(&PgInterval::from(*v)),
        Value::IntervalDayTime(v) => builder.encode_field(&PgInterval::from(*v)),
        Value::IntervalMonthDayNano(v) => builder.encode_field(&PgInterval::from(*v)),
        Value::Decimal128(v) => builder.encode_field(&v.to_string()),
        Value::Duration(d) => match PgInterval::try_from(*d) {
            Ok(i) => builder.encode_field(&i),
            Err(e) => Err(convert_err(Error::Internal {
                err_msg: e.to_string(),
            })),
        },
        Value::List(values) => encode_array(query_ctx, values, builder),
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
        &ConcreteDataType::Binary(_) | &ConcreteDataType::Vector(_) => Ok(Type::BYTEA),
        &ConcreteDataType::String(_) => Ok(Type::VARCHAR),
        &ConcreteDataType::Date(_) => Ok(Type::DATE),
        &ConcreteDataType::Timestamp(_) => Ok(Type::TIMESTAMP),
        &ConcreteDataType::Time(_) => Ok(Type::TIME),
        &ConcreteDataType::Interval(_) => Ok(Type::INTERVAL),
        &ConcreteDataType::Decimal128(_) => Ok(Type::NUMERIC),
        &ConcreteDataType::Json(_) => Ok(Type::JSON),
        ConcreteDataType::List(list) => match list.item_type() {
            &ConcreteDataType::Null(_) => Ok(Type::UNKNOWN),
            &ConcreteDataType::Boolean(_) => Ok(Type::BOOL_ARRAY),
            &ConcreteDataType::Int8(_) | &ConcreteDataType::UInt8(_) => Ok(Type::CHAR_ARRAY),
            &ConcreteDataType::Int16(_) | &ConcreteDataType::UInt16(_) => Ok(Type::INT2_ARRAY),
            &ConcreteDataType::Int32(_) | &ConcreteDataType::UInt32(_) => Ok(Type::INT4_ARRAY),
            &ConcreteDataType::Int64(_) | &ConcreteDataType::UInt64(_) => Ok(Type::INT8_ARRAY),
            &ConcreteDataType::Float32(_) => Ok(Type::FLOAT4_ARRAY),
            &ConcreteDataType::Float64(_) => Ok(Type::FLOAT8_ARRAY),
            &ConcreteDataType::Binary(_) => Ok(Type::BYTEA_ARRAY),
            &ConcreteDataType::String(_) => Ok(Type::VARCHAR_ARRAY),
            &ConcreteDataType::Date(_) => Ok(Type::DATE_ARRAY),
            &ConcreteDataType::Timestamp(_) => Ok(Type::TIMESTAMP_ARRAY),
            &ConcreteDataType::Time(_) => Ok(Type::TIME_ARRAY),
            &ConcreteDataType::Interval(_) => Ok(Type::INTERVAL_ARRAY),
            &ConcreteDataType::Decimal128(_) => Ok(Type::NUMERIC_ARRAY),
            &ConcreteDataType::Json(_) => Ok(Type::JSON_ARRAY),
            &ConcreteDataType::Duration(_) => Ok(Type::INTERVAL_ARRAY),
            &ConcreteDataType::Dictionary(_)
            | &ConcreteDataType::Vector(_)
            | &ConcreteDataType::List(_)
            | &ConcreteDataType::Struct(_) => server_error::UnsupportedDataTypeSnafu {
                data_type: origin,
                reason: "not implemented",
            }
            .fail(),
        },
        &ConcreteDataType::Dictionary(_) => server_error::UnsupportedDataTypeSnafu {
            data_type: origin,
            reason: "not implemented",
        }
        .fail(),
        &ConcreteDataType::Duration(_) => Ok(Type::INTERVAL),
        &ConcreteDataType::Struct(_) => server_error::UnsupportedDataTypeSnafu {
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
        &Type::TIME => Ok(ConcreteDataType::timestamp_datatype(
            common_time::timestamp::TimeUnit::Microsecond,
        )),
        &Type::CHAR_ARRAY => Ok(ConcreteDataType::list_datatype(
            ConcreteDataType::int8_datatype(),
        )),
        &Type::INT2_ARRAY => Ok(ConcreteDataType::list_datatype(
            ConcreteDataType::int16_datatype(),
        )),
        &Type::INT4_ARRAY => Ok(ConcreteDataType::list_datatype(
            ConcreteDataType::int32_datatype(),
        )),
        &Type::INT8_ARRAY => Ok(ConcreteDataType::list_datatype(
            ConcreteDataType::int64_datatype(),
        )),
        &Type::VARCHAR_ARRAY => Ok(ConcreteDataType::list_datatype(
            ConcreteDataType::string_datatype(),
        )),
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
            Some(param_type.to_string()),
        )),
    }
}

pub(super) fn invalid_parameter_error(msg: &str, detail: Option<String>) -> PgWireError {
    let mut error_info = PgErrorCode::Ec22023.to_err_info(msg.to_string());
    error_info.detail = detail;
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
            .map_err(convert_err)
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
        .get_parameter_types()
        .context(DataFusionSnafu)
        .map_err(convert_err)?
        .into_iter()
        .map(|(k, v)| (k, v.map(|v| ConcreteDataType::from_arrow_type(&v))))
        .collect::<HashMap<_, _>>();

    for idx in 0..param_count {
        let server_type = param_types
            .get(&format!("${}", idx + 1))
            .and_then(|t| t.as_ref());

        let client_type = if let Some(client_given_type) = client_param_types.get(idx) {
            client_given_type.clone()
        } else if let Some(server_provided_type) = &server_type {
            type_gt_to_pg(server_provided_type).map_err(convert_err)?
        } else {
            return Err(invalid_parameter_error(
                "unknown_parameter_type",
                Some(format!(
                    "Cannot get parameter type information for parameter {}",
                    idx
                )),
            ));
        };

        let value = match &client_type {
            &Type::VARCHAR | &Type::TEXT => {
                let data = portal.parameter::<String>(idx, &client_type)?;
                if let Some(server_type) = &server_type {
                    match server_type {
                        ConcreteDataType::String(_) => ScalarValue::Utf8(data),
                        _ => {
                            return Err(invalid_parameter_error(
                                "invalid_parameter_type",
                                Some(format!("Expected: {}, found: {}", server_type, client_type)),
                            ))
                        }
                    }
                } else {
                    ScalarValue::Utf8(data)
                }
            }
            &Type::BOOL => {
                let data = portal.parameter::<bool>(idx, &client_type)?;
                if let Some(server_type) = &server_type {
                    match server_type {
                        ConcreteDataType::Boolean(_) => ScalarValue::Boolean(data),
                        _ => {
                            return Err(invalid_parameter_error(
                                "invalid_parameter_type",
                                Some(format!("Expected: {}, found: {}", server_type, client_type)),
                            ))
                        }
                    }
                } else {
                    ScalarValue::Boolean(data)
                }
            }
            &Type::INT2 => {
                let data = portal.parameter::<i16>(idx, &client_type)?;
                if let Some(server_type) = &server_type {
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
                        _ => {
                            return Err(invalid_parameter_error(
                                "invalid_parameter_type",
                                Some(format!("Expected: {}, found: {}", server_type, client_type)),
                            ))
                        }
                    }
                } else {
                    ScalarValue::Int16(data)
                }
            }
            &Type::INT4 => {
                let data = portal.parameter::<i32>(idx, &client_type)?;
                if let Some(server_type) = &server_type {
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
                        _ => {
                            return Err(invalid_parameter_error(
                                "invalid_parameter_type",
                                Some(format!("Expected: {}, found: {}", server_type, client_type)),
                            ))
                        }
                    }
                } else {
                    ScalarValue::Int32(data)
                }
            }
            &Type::INT8 => {
                let data = portal.parameter::<i64>(idx, &client_type)?;
                if let Some(server_type) = &server_type {
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
                        _ => {
                            return Err(invalid_parameter_error(
                                "invalid_parameter_type",
                                Some(format!("Expected: {}, found: {}", server_type, client_type)),
                            ))
                        }
                    }
                } else {
                    ScalarValue::Int64(data)
                }
            }
            &Type::FLOAT4 => {
                let data = portal.parameter::<f32>(idx, &client_type)?;
                if let Some(server_type) = &server_type {
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
                        ConcreteDataType::Float64(_) => {
                            ScalarValue::Float64(data.map(|n| n as f64))
                        }
                        _ => {
                            return Err(invalid_parameter_error(
                                "invalid_parameter_type",
                                Some(format!("Expected: {}, found: {}", server_type, client_type)),
                            ))
                        }
                    }
                } else {
                    ScalarValue::Float32(data)
                }
            }
            &Type::FLOAT8 => {
                let data = portal.parameter::<f64>(idx, &client_type)?;
                if let Some(server_type) = &server_type {
                    match server_type {
                        ConcreteDataType::Int8(_) => ScalarValue::Int8(data.map(|n| n as i8)),
                        ConcreteDataType::Int16(_) => ScalarValue::Int16(data.map(|n| n as i16)),
                        ConcreteDataType::Int32(_) => ScalarValue::Int32(data.map(|n| n as i32)),
                        ConcreteDataType::Int64(_) => ScalarValue::Int64(data.map(|n| n as i64)),
                        ConcreteDataType::UInt8(_) => ScalarValue::UInt8(data.map(|n| n as u8)),
                        ConcreteDataType::UInt16(_) => ScalarValue::UInt16(data.map(|n| n as u16)),
                        ConcreteDataType::UInt32(_) => ScalarValue::UInt32(data.map(|n| n as u32)),
                        ConcreteDataType::UInt64(_) => ScalarValue::UInt64(data.map(|n| n as u64)),
                        ConcreteDataType::Float32(_) => {
                            ScalarValue::Float32(data.map(|n| n as f32))
                        }
                        ConcreteDataType::Float64(_) => ScalarValue::Float64(data),
                        _ => {
                            return Err(invalid_parameter_error(
                                "invalid_parameter_type",
                                Some(format!("Expected: {}, found: {}", server_type, client_type)),
                            ))
                        }
                    }
                } else {
                    ScalarValue::Float64(data)
                }
            }
            &Type::TIMESTAMP => {
                let data = portal.parameter::<NaiveDateTime>(idx, &client_type)?;
                if let Some(server_type) = &server_type {
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
                        _ => {
                            return Err(invalid_parameter_error(
                                "invalid_parameter_type",
                                Some(format!("Expected: {}, found: {}", server_type, client_type)),
                            ))
                        }
                    }
                } else {
                    ScalarValue::TimestampMillisecond(
                        data.map(|ts| ts.and_utc().timestamp_millis()),
                        None,
                    )
                }
            }
            &Type::DATE => {
                let data = portal.parameter::<NaiveDate>(idx, &client_type)?;
                if let Some(server_type) = &server_type {
                    match server_type {
                        ConcreteDataType::Date(_) => ScalarValue::Date32(
                            data.map(|d| (d - DateTime::UNIX_EPOCH.date_naive()).num_days() as i32),
                        ),
                        _ => {
                            return Err(invalid_parameter_error(
                                "invalid_parameter_type",
                                Some(format!("Expected: {}, found: {}", server_type, client_type)),
                            ));
                        }
                    }
                } else {
                    ScalarValue::Date32(
                        data.map(|d| (d - DateTime::UNIX_EPOCH.date_naive()).num_days() as i32),
                    )
                }
            }
            &Type::INTERVAL => {
                let data = portal.parameter::<PgInterval>(idx, &client_type)?;
                if let Some(server_type) = &server_type {
                    match server_type {
                        ConcreteDataType::Interval(IntervalType::YearMonth(_)) => {
                            ScalarValue::IntervalYearMonth(
                                data.map(|i| {
                                    if i.days != 0 || i.microseconds != 0 {
                                        Err(invalid_parameter_error(
                                            "invalid_parameter_type",
                                            Some(format!(
                                                "Expected: {}, found: {}",
                                                server_type, client_type
                                            )),
                                        ))
                                    } else {
                                        Ok(IntervalYearMonth::new(i.months).to_i32())
                                    }
                                })
                                .transpose()?,
                            )
                        }
                        ConcreteDataType::Interval(IntervalType::DayTime(_)) => {
                            ScalarValue::IntervalDayTime(
                                data.map(|i| {
                                    if i.months != 0 || i.microseconds % 1000 != 0 {
                                        Err(invalid_parameter_error(
                                            "invalid_parameter_type",
                                            Some(format!(
                                                "Expected: {}, found: {}",
                                                server_type, client_type
                                            )),
                                        ))
                                    } else {
                                        Ok(IntervalDayTime::new(
                                            i.days,
                                            (i.microseconds / 1000) as i32,
                                        )
                                        .into())
                                    }
                                })
                                .transpose()?,
                            )
                        }
                        ConcreteDataType::Interval(IntervalType::MonthDayNano(_)) => {
                            ScalarValue::IntervalMonthDayNano(
                                data.map(|i| IntervalMonthDayNano::from(i).into()),
                            )
                        }
                        _ => {
                            return Err(invalid_parameter_error(
                                "invalid_parameter_type",
                                Some(format!("Expected: {}, found: {}", server_type, client_type)),
                            ));
                        }
                    }
                } else {
                    ScalarValue::IntervalMonthDayNano(
                        data.map(|i| IntervalMonthDayNano::from(i).into()),
                    )
                }
            }
            &Type::BYTEA => {
                let data = portal.parameter::<Vec<u8>>(idx, &client_type)?;
                if let Some(server_type) = &server_type {
                    match server_type {
                        ConcreteDataType::String(_) => {
                            ScalarValue::Utf8(data.map(|d| String::from_utf8_lossy(&d).to_string()))
                        }
                        ConcreteDataType::Binary(_) => ScalarValue::Binary(data),
                        _ => {
                            return Err(invalid_parameter_error(
                                "invalid_parameter_type",
                                Some(format!("Expected: {}, found: {}", server_type, client_type)),
                            ));
                        }
                    }
                } else {
                    ScalarValue::Binary(data)
                }
            }
            &Type::JSONB => {
                let data = portal.parameter::<serde_json::Value>(idx, &client_type)?;
                if let Some(server_type) = &server_type {
                    match server_type {
                        ConcreteDataType::Binary(_) => {
                            ScalarValue::Binary(data.map(|d| d.to_string().into_bytes()))
                        }
                        _ => {
                            return Err(invalid_parameter_error(
                                "invalid_parameter_type",
                                Some(format!("Expected: {}, found: {}", server_type, client_type)),
                            ));
                        }
                    }
                } else {
                    ScalarValue::Binary(data.map(|d| d.to_string().into_bytes()))
                }
            }
            &Type::INT2_ARRAY => {
                let data = portal.parameter::<Vec<i16>>(idx, &client_type)?;
                if let Some(data) = data {
                    let values = data.into_iter().map(|i| i.into()).collect::<Vec<_>>();
                    ScalarValue::List(ScalarValue::new_list(&values, &ArrowDataType::Int16, true))
                } else {
                    ScalarValue::Null
                }
            }
            &Type::INT4_ARRAY => {
                let data = portal.parameter::<Vec<i32>>(idx, &client_type)?;
                if let Some(data) = data {
                    let values = data.into_iter().map(|i| i.into()).collect::<Vec<_>>();
                    ScalarValue::List(ScalarValue::new_list(&values, &ArrowDataType::Int32, true))
                } else {
                    ScalarValue::Null
                }
            }
            &Type::INT8_ARRAY => {
                let data = portal.parameter::<Vec<i64>>(idx, &client_type)?;
                if let Some(data) = data {
                    let values = data.into_iter().map(|i| i.into()).collect::<Vec<_>>();
                    ScalarValue::List(ScalarValue::new_list(&values, &ArrowDataType::Int64, true))
                } else {
                    ScalarValue::Null
                }
            }
            &Type::VARCHAR_ARRAY => {
                let data = portal.parameter::<Vec<String>>(idx, &client_type)?;
                if let Some(data) = data {
                    let values = data.into_iter().map(|i| i.into()).collect::<Vec<_>>();
                    ScalarValue::List(ScalarValue::new_list(&values, &ArrowDataType::Utf8, true))
                } else {
                    ScalarValue::Null
                }
            }
            _ => Err(invalid_parameter_error(
                "unsupported_parameter_value",
                Some(format!("Found type: {}", client_type)),
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

    use common_time::interval::IntervalUnit;
    use common_time::timestamp::TimeUnit;
    use common_time::Timestamp;
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
                "timestamps".into(),
                None,
                None,
                Type::TIMESTAMP,
                FieldFormat::Text,
            ),
            FieldInfo::new(
                "interval_year_month".into(),
                None,
                None,
                Type::INTERVAL,
                FieldFormat::Text,
            ),
            FieldInfo::new(
                "interval_day_time".into(),
                None,
                None,
                Type::INTERVAL,
                FieldFormat::Text,
            ),
            FieldInfo::new(
                "interval_month_day_nano".into(),
                None,
                None,
                Type::INTERVAL,
                FieldFormat::Text,
            ),
            FieldInfo::new(
                "int_list".into(),
                None,
                None,
                Type::INT8_ARRAY,
                FieldFormat::Text,
            ),
            FieldInfo::new(
                "float_list".into(),
                None,
                None,
                Type::FLOAT8_ARRAY,
                FieldFormat::Text,
            ),
            FieldInfo::new(
                "string_list".into(),
                None,
                None,
                Type::VARCHAR_ARRAY,
                FieldFormat::Text,
            ),
            FieldInfo::new(
                "timestamp_list".into(),
                None,
                None,
                Type::TIMESTAMP_ARRAY,
                FieldFormat::Text,
            ),
        ];

        let datatypes = vec![
            ConcreteDataType::null_datatype(),
            ConcreteDataType::boolean_datatype(),
            ConcreteDataType::uint8_datatype(),
            ConcreteDataType::uint16_datatype(),
            ConcreteDataType::uint32_datatype(),
            ConcreteDataType::uint64_datatype(),
            ConcreteDataType::int8_datatype(),
            ConcreteDataType::int8_datatype(),
            ConcreteDataType::int16_datatype(),
            ConcreteDataType::int16_datatype(),
            ConcreteDataType::int32_datatype(),
            ConcreteDataType::int32_datatype(),
            ConcreteDataType::int64_datatype(),
            ConcreteDataType::int64_datatype(),
            ConcreteDataType::float32_datatype(),
            ConcreteDataType::float32_datatype(),
            ConcreteDataType::float32_datatype(),
            ConcreteDataType::float64_datatype(),
            ConcreteDataType::float64_datatype(),
            ConcreteDataType::float64_datatype(),
            ConcreteDataType::string_datatype(),
            ConcreteDataType::binary_datatype(),
            ConcreteDataType::date_datatype(),
            ConcreteDataType::time_datatype(TimeUnit::Second),
            ConcreteDataType::timestamp_datatype(TimeUnit::Second),
            ConcreteDataType::interval_datatype(IntervalUnit::YearMonth),
            ConcreteDataType::interval_datatype(IntervalUnit::DayTime),
            ConcreteDataType::interval_datatype(IntervalUnit::MonthDayNano),
            ConcreteDataType::list_datatype(ConcreteDataType::int64_datatype()),
            ConcreteDataType::list_datatype(ConcreteDataType::float64_datatype()),
            ConcreteDataType::list_datatype(ConcreteDataType::string_datatype()),
            ConcreteDataType::list_datatype(ConcreteDataType::timestamp_second_datatype()),
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
            Value::Timestamp(1000001i64.into()),
            Value::IntervalYearMonth(IntervalYearMonth::new(1)),
            Value::IntervalDayTime(IntervalDayTime::new(1, 10)),
            Value::IntervalMonthDayNano(IntervalMonthDayNano::new(1, 1, 10)),
            Value::List(ListValue::new(
                vec![Value::Int64(1i64)],
                ConcreteDataType::int64_datatype(),
            )),
            Value::List(ListValue::new(
                vec![Value::Float64(1.0f64.into())],
                ConcreteDataType::float64_datatype(),
            )),
            Value::List(ListValue::new(
                vec![Value::String("tom".into())],
                ConcreteDataType::string_datatype(),
            )),
            Value::List(ListValue::new(
                vec![Value::Timestamp(Timestamp::new(1i64, TimeUnit::Second))],
                ConcreteDataType::timestamp_second_datatype(),
            )),
        ];
        let query_context = QueryContextBuilder::default()
            .configuration_parameter(Default::default())
            .build()
            .into();
        let mut builder = DataRowEncoder::new(Arc::new(schema));
        for (value, datatype) in values.iter().zip(datatypes) {
            encode_value(&query_context, value, &mut builder, &datatype).unwrap();
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
