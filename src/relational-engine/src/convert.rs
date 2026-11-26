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

//! Decoding a `mysql_async::Value` into a GreptimeDB [`Value`].
//!
//! The mapping is strict: a driver-reported value that does not match the
//! frozen column type is rejected with [`DecodeRow`], never silently cast.

use common_decimal::Decimal128;
use datatypes::prelude::{ConcreteDataType, DataType, LogicalTypeId, Value};
use mysql_async::consts::{ColumnFlags, ColumnType};
use mysql_async::Value as MySqlValue;

use crate::error::{DecodeRowSnafu, Result};

/// Decodes a single driver value against the frozen column type.
pub(crate) fn decode_value(
    value: &MySqlValue,
    data_type: &ConcreteDataType,
    column: &str,
) -> Result<Value> {
    use ConcreteDataType::{
        Binary, Date, Decimal128, Float32, Float64, Int16, Int32, Int64, Int8, Json, String,
        Timestamp, UInt16, UInt32, UInt64, UInt8,
    };

    match data_type {
        Int8(_) => decode_signed(value, column, i8::MIN as i64, i8::MAX as i64)
            .map(|v| Value::Int8(v as i8)),
        Int16(_) => decode_signed(value, column, i16::MIN as i64, i16::MAX as i64)
            .map(|v| Value::Int16(v as i16)),
        Int32(_) => decode_signed(value, column, i32::MIN as i64, i32::MAX as i64)
            .map(|v| Value::Int32(v as i32)),
        Int64(_) => as_i64(value, column).map(Value::Int64),
        UInt8(_) => decode_unsigned(value, column, u8::MAX as u64).map(|v| Value::UInt8(v as u8)),
        UInt16(_) => decode_unsigned(value, column, u16::MAX as u64).map(|v| Value::UInt16(v as u16)),
        UInt32(_) => decode_unsigned(value, column, u32::MAX as u64).map(|v| Value::UInt32(v as u32)),
        UInt64(_) => as_u64(value, column).map(Value::UInt64),
        Float32(_) => decode_f32(value, column).map(Value::from),
        Float64(_) => decode_f64(value, column).map(Value::from),
        Decimal128(dt) => decode_decimal(value, column, dt.precision(), dt.scale()),
        String(_) => {
            let bytes = as_bytes(value, column)?;
            let s = std::str::from_utf8(bytes)
                .map_err(|_| decode_err(column, "invalid UTF-8 in string column"))?;
            Ok(Value::from(s))
        }
        Binary(_) => Ok(Value::from(as_bytes(value, column)?)),
        Date(_) => {
            let (year, month, day, ..) = as_date(value, column)?;
            let s = format!("{year:04}-{month:02}-{day:02}");
            let date = common_time::Date::from_str_utc(&s)
                .map_err(|_| decode_err(column, format!("invalid date `{s}`")))?;
            Ok(Value::from(date))
        }
        Timestamp(_) => {
            let target = data_type
                .as_timestamp()
                .ok_or_else(|| decode_err(column, "expected a timestamp type"))?;
            let s = timestamp_string(value, column)?;
            let ts = common_time::Timestamp::from_str_utc(&s)
                .map_err(|_| decode_err(column, format!("invalid timestamp `{s}`")))?;
            let converted = ts
                .convert_to(target.unit())
                .ok_or_else(|| decode_err(column, "timestamp out of range"))?;
            Ok(Value::from(converted))
        }
        Json(_) => {
            let bytes = as_bytes(value, column)?;
            let text = std::str::from_utf8(bytes)
                .map_err(|_| decode_err(column, "invalid UTF-8 in json column"))?;
            let jsonb = datatypes::types::parse_string_to_jsonb(text)
                .map_err(|_| decode_err(column, format!("invalid json `{}`", truncate(text, 64))))?;
            Ok(Value::from(jsonb))
        }
        // Every other frozen type (boolean, time, duration, interval, list,
        // struct, vector) has no MySQL source mapping, so it cannot appear in a
        // relational external table. Reject rather than guess.
        other => Err(decode_err(
            column,
            format!("unsupported column type {other}"),
        )),
    }
}

pub(crate) fn validate_column(
    column: &mysql_async::Column,
    data_type: &ConcreteDataType,
    name: &str,
) -> Result<()> {
    let column_type = column.column_type();
    let unsigned = column.flags().contains(ColumnFlags::UNSIGNED_FLAG);
    let matches = match (data_type.logical_type_id(), column_type) {
        (LogicalTypeId::Int8, ColumnType::MYSQL_TYPE_TINY) => !unsigned,
        (LogicalTypeId::UInt8, ColumnType::MYSQL_TYPE_TINY) => unsigned,
        (LogicalTypeId::Int16, ColumnType::MYSQL_TYPE_SHORT) => !unsigned,
        (LogicalTypeId::Int16, ColumnType::MYSQL_TYPE_YEAR) => true,
        (LogicalTypeId::UInt16, ColumnType::MYSQL_TYPE_SHORT) => unsigned,
        (LogicalTypeId::Int32, ColumnType::MYSQL_TYPE_LONG | ColumnType::MYSQL_TYPE_INT24) => {
            !unsigned
        }
        (LogicalTypeId::UInt32, ColumnType::MYSQL_TYPE_LONG | ColumnType::MYSQL_TYPE_INT24) => {
            unsigned
        }
        (LogicalTypeId::Int64, ColumnType::MYSQL_TYPE_LONGLONG) => !unsigned,
        (LogicalTypeId::UInt64, ColumnType::MYSQL_TYPE_LONGLONG) => unsigned,
        (LogicalTypeId::Float32, ColumnType::MYSQL_TYPE_FLOAT) => true,
        (LogicalTypeId::Float64, ColumnType::MYSQL_TYPE_DOUBLE) => true,
        (
            LogicalTypeId::Decimal128,
            ColumnType::MYSQL_TYPE_DECIMAL | ColumnType::MYSQL_TYPE_NEWDECIMAL,
        ) => true,
        (
            LogicalTypeId::String,
            ColumnType::MYSQL_TYPE_VARCHAR
            | ColumnType::MYSQL_TYPE_VAR_STRING
            | ColumnType::MYSQL_TYPE_STRING
            | ColumnType::MYSQL_TYPE_ENUM
            | ColumnType::MYSQL_TYPE_SET
            | ColumnType::MYSQL_TYPE_BLOB
            | ColumnType::MYSQL_TYPE_TINY_BLOB
            | ColumnType::MYSQL_TYPE_MEDIUM_BLOB
            | ColumnType::MYSQL_TYPE_LONG_BLOB,
        ) => true,
        (
            LogicalTypeId::Binary,
            ColumnType::MYSQL_TYPE_BLOB
            | ColumnType::MYSQL_TYPE_TINY_BLOB
            | ColumnType::MYSQL_TYPE_MEDIUM_BLOB
            | ColumnType::MYSQL_TYPE_LONG_BLOB
            | ColumnType::MYSQL_TYPE_VAR_STRING
            | ColumnType::MYSQL_TYPE_STRING,
        ) => true,
        (LogicalTypeId::Date, ColumnType::MYSQL_TYPE_DATE | ColumnType::MYSQL_TYPE_NEWDATE) => true,
        (
            LogicalTypeId::TimestampSecond
            | LogicalTypeId::TimestampMillisecond
            | LogicalTypeId::TimestampMicrosecond
            | LogicalTypeId::TimestampNanosecond,
            ColumnType::MYSQL_TYPE_DATETIME
            | ColumnType::MYSQL_TYPE_DATETIME2
            | ColumnType::MYSQL_TYPE_TIMESTAMP
            | ColumnType::MYSQL_TYPE_TIMESTAMP2,
        ) => true,
        (LogicalTypeId::Json, ColumnType::MYSQL_TYPE_JSON) => true,
        _ => false,
    };
    if matches {
        Ok(())
    } else {
        Err(decode_err(
            name,
            format!("remote column type {column_type:?} does not match the frozen type {data_type}"),
        ))
    }
}

fn decode_err(column: &str, reason: impl std::fmt::Display) -> crate::error::Error {
    DecodeRowSnafu {
        column: column.to_string(),
        reason: reason.to_string(),
    }
    .build()
}

/// Truncates a raw value for an error message so a decode error never echoes
/// an entire remote field back to the client.
fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        let cut: String = s.chars().take(max).collect();
        format!("{cut}…")
    }
}

/// A length-capped `Debug` rendering of a driver value, for error messages.
fn value_debug(value: &MySqlValue) -> String {
    truncate(&format!("{value:?}"), 64)
}

fn as_i64(value: &MySqlValue, column: &str) -> Result<i64> {
    match value {
        MySqlValue::Int(v) => Ok(*v),
        MySqlValue::UInt(v) => i64::try_from(*v)
            .map_err(|_| decode_err(column, "unsigned integer overflows a signed column")),
        other => Err(decode_err(
            column,
            format!("expected an integer, got {}", value_debug(other)),
        )),
    }
}

fn as_u64(value: &MySqlValue, column: &str) -> Result<u64> {
    match value {
        MySqlValue::UInt(v) => Ok(*v),
        MySqlValue::Int(v) => u64::try_from(*v)
            .map_err(|_| decode_err(column, "negative integer overflows an unsigned column")),
        other => Err(decode_err(
            column,
            format!("expected an integer, got {}", value_debug(other)),
        )),
    }
}

/// Range-checked signed decode (never truncate with `as`).
fn decode_signed(value: &MySqlValue, column: &str, min: i64, max: i64) -> Result<i64> {
    let v = as_i64(value, column)?;
    if v < min || v > max {
        return Err(decode_err(
            column,
            format!("{v} is out of range for the column [{min}, {max}]"),
        ));
    }
    Ok(v)
}

/// Range-checked unsigned decode (never truncate with `as`).
fn decode_unsigned(value: &MySqlValue, column: &str, max: u64) -> Result<u64> {
    let v = as_u64(value, column)?;
    if v > max {
        return Err(decode_err(
            column,
            format!("{v} is out of range for the column [0, {max}]"),
        ));
    }
    Ok(v)
}

fn decode_f32(value: &MySqlValue, column: &str) -> Result<f32> {
    match value {
        MySqlValue::Float(v) => Ok(*v),
        other => Err(decode_err(
            column,
            format!("expected a float, got {}", value_debug(other)),
        )),
    }
}

fn decode_f64(value: &MySqlValue, column: &str) -> Result<f64> {
    match value {
        MySqlValue::Double(v) => Ok(*v),
        other => Err(decode_err(
            column,
            format!("expected a double, got {}", value_debug(other)),
        )),
    }
}

fn as_bytes<'a>(value: &'a MySqlValue, column: &str) -> Result<&'a [u8]> {
    match value {
        MySqlValue::Bytes(b) => Ok(b.as_slice()),
        other => Err(decode_err(
            column,
            format!("expected bytes, got {}", value_debug(other)),
        )),
    }
}

fn as_date(value: &MySqlValue, column: &str) -> Result<(u16, u8, u8, u8, u8, u8, u32)> {
    match value {
        MySqlValue::Date(year, month, day, hour, minute, second, micros) => {
            Ok((*year, *month, *day, *hour, *minute, *second, *micros))
        }
        other => Err(decode_err(
            column,
            format!("expected a date, got {}", value_debug(other)),
        )),
    }
}

/// Formats a temporal driver value as an ISO-8601 string for [`Timestamp`].
fn timestamp_string(value: &MySqlValue, column: &str) -> Result<String> {
    match value {
        MySqlValue::Date(year, month, day, hour, minute, second, micros) => {
            if *micros == 0 {
                Ok(format!(
                    "{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}"
                ))
            } else {
                Ok(format!(
                    "{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{micros:06}"
                ))
            }
        }
        MySqlValue::Bytes(b) => std::str::from_utf8(b)
            .map(str::to_string)
            .map_err(|_| decode_err(column, "invalid UTF-8 in timestamp")),
        // A MySQL `TIME` value is not a timestamp; `TIME` columns are rejected at
        // create time, so this is a defensive branch only.
        MySqlValue::Time(..) => Err(decode_err(column, "got a TIME value, not a timestamp")),
        other => Err(decode_err(
            column,
            format!("expected a timestamp, got {}", value_debug(other)),
        )),
    }
}

/// Decodes a `DECIMAL` string (e.g. `123.45`) into a [`Decimal128`] with the
/// column's frozen precision and scale, rescaling the parsed value.
fn decode_decimal(value: &MySqlValue, column: &str, precision: u8, scale: i8) -> Result<Value> {
    let bytes = as_bytes(value, column)?;
    let s = std::str::from_utf8(bytes)
        .map_err(|_| decode_err(column, "invalid UTF-8 in decimal"))?;
    let s = s.trim();

    let (coefficient, parsed_scale) = parse_decimal_str(s, column)?;

    let target_scale = scale as i32;
    let diff = target_scale - parsed_scale;
    let rescaled = if diff >= 0 {
        coefficient
            .checked_mul(10_i128.pow(diff as u32))
            .ok_or_else(|| decode_err(column, format!("decimal `{s}` overflows")))?
    } else {
        return Err(decode_err(
            column,
            format!("decimal `{s}` has more fractional digits than the column scale {scale}"),
        ));
    };

    let max = 10_i128.pow(precision as u32);
    if rescaled >= max || rescaled <= -max {
        return Err(decode_err(
            column,
            format!("decimal `{s}` exceeds the column precision {precision}"),
        ));
    }

    Ok(Value::Decimal128(Decimal128::new(rescaled, precision, scale)))
}

/// Parses a decimal string into its coefficient and scale without relying on
/// [`Decimal128`]'s `FromStr` (which derives the precision from the text and
/// therefore rejects values whose scale exceeds their coefficient digit count).
fn parse_decimal_str(s: &str, column: &str) -> Result<(i128, i32)> {
    let (negative, unsigned) = match s.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, s.strip_prefix('+').unwrap_or(s)),
    };
    let (int_part, frac_part) = match unsigned.split_once('.') {
        Some((i, f)) => (i, f),
        None => (unsigned, ""),
    };
    if int_part.is_empty() && frac_part.is_empty() {
        return Err(decode_err(column, format!("invalid decimal `{s}`")));
    }
    if !int_part.bytes().all(|b| b.is_ascii_digit())
        || !frac_part.bytes().all(|b| b.is_ascii_digit())
    {
        return Err(decode_err(column, format!("invalid decimal `{s}`")));
    }

    let scale = frac_part.len() as i32;
    let mut combined = format!("{int_part}{frac_part}");
    combined = combined.trim_start_matches('0').to_string();
    if combined.is_empty() {
        combined = "0".to_string();
    }
    let mut coefficient = if negative { String::from("-") } else { String::new() };
    coefficient.push_str(&combined);
    let value: i128 = coefficient
        .parse()
        .map_err(|_| decode_err(column, format!("decimal `{s}` overflows")))?;
    Ok((value, scale))
}
