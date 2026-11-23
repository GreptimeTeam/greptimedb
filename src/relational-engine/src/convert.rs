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
//! frozen column type is rejected with [`DecodeRow`], never silently cast
//! (commit2 lesson 10.2: no auto-cast, no silent downgrade).

use common_decimal::Decimal128;
use datatypes::prelude::{ConcreteDataType, Value};
use mysql_async::Value as MySqlValue;

use crate::error::{DecodeRowSnafu, Result};

/// Decodes a single driver value against the frozen column type.
pub(crate) fn decode_value(
    value: &MySqlValue,
    data_type: &ConcreteDataType,
    column: &str,
) -> Result<Value> {
    use ConcreteDataType::*;

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
            let json: serde_json::Value = serde_json::from_str(text)
                .map_err(|_| decode_err(column, format!("invalid json `{text}`")))?;
            // `json` maps to `json_datatype()`, the legacy Jsonb format stored as
            // binary, so encode the parsed JSON as jsonb bytes rather than a
            // Json2 `Value::Json` (which would not fit the binary-backed column).
            Ok(Value::from(jsonb::Value::from(json).to_vec()))
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

fn decode_err(column: &str, reason: impl std::fmt::Display) -> crate::error::Error {
    DecodeRowSnafu {
        column: column.to_string(),
        reason: reason.to_string(),
    }
    .build()
}

fn as_i64(value: &MySqlValue, column: &str) -> Result<i64> {
    match value {
        MySqlValue::Int(v) => Ok(*v),
        MySqlValue::UInt(v) => i64::try_from(*v)
            .map_err(|_| decode_err(column, "unsigned integer overflows a signed column")),
        MySqlValue::Bytes(b) => {
            let s = std::str::from_utf8(b)
                .map_err(|_| decode_err(column, "expected an integer, got non-UTF-8 bytes"))?;
            s.trim()
                .parse::<i64>()
                .map_err(|_| decode_err(column, format!("expected an integer, got `{s}`")))
        }
        other => Err(decode_err(column, format!("expected an integer, got {other:?}"))),
    }
}

fn as_u64(value: &MySqlValue, column: &str) -> Result<u64> {
    match value {
        MySqlValue::UInt(v) => Ok(*v),
        MySqlValue::Int(v) => u64::try_from(*v)
            .map_err(|_| decode_err(column, "negative integer overflows an unsigned column")),
        MySqlValue::Bytes(b) => {
            let s = std::str::from_utf8(b)
                .map_err(|_| decode_err(column, "expected an integer, got non-UTF-8 bytes"))?;
            s.trim()
                .parse::<u64>()
                .map_err(|_| decode_err(column, format!("expected an integer, got `{s}`")))
        }
        other => Err(decode_err(column, format!("expected an integer, got {other:?}"))),
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
        MySqlValue::Double(v) => Ok(*v as f32),
        MySqlValue::Int(v) => Ok(*v as f32),
        MySqlValue::UInt(v) => Ok(*v as f32),
        other => Err(decode_err(column, format!("expected a float, got {other:?}"))),
    }
}

fn decode_f64(value: &MySqlValue, column: &str) -> Result<f64> {
    match value {
        MySqlValue::Double(v) => Ok(*v),
        MySqlValue::Float(v) => Ok(*v as f64),
        MySqlValue::Int(v) => Ok(*v as f64),
        MySqlValue::UInt(v) => Ok(*v as f64),
        other => Err(decode_err(column, format!("expected a double, got {other:?}"))),
    }
}

fn as_bytes<'a>(value: &'a MySqlValue, column: &str) -> Result<&'a [u8]> {
    match value {
        MySqlValue::Bytes(b) => Ok(b.as_slice()),
        other => Err(decode_err(column, format!("expected bytes, got {other:?}"))),
    }
}

fn as_date(value: &MySqlValue, column: &str) -> Result<(u16, u8, u8, u8, u8, u8, u32)> {
    match value {
        MySqlValue::Date(year, month, day, hour, minute, second, micros) => {
            Ok((*year, *month, *day, *hour, *minute, *second, *micros))
        }
        other => Err(decode_err(column, format!("expected a date, got {other:?}"))),
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
        other => Err(decode_err(column, format!("expected a timestamp, got {other:?}"))),
    }
}

/// Decodes a `DECIMAL` string (e.g. `123.45`) into a [`Decimal128`] with the
/// column's frozen precision and scale, rescaling the parsed value.
fn decode_decimal(value: &MySqlValue, column: &str, precision: u8, scale: i8) -> Result<Value> {
    let bytes = as_bytes(value, column)?;
    let s = std::str::from_utf8(bytes)
        .map_err(|_| decode_err(column, "invalid UTF-8 in decimal"))?;
    let parsed = s
        .trim()
        .parse::<Decimal128>()
        .map_err(|_| decode_err(column, format!("invalid decimal `{s}`")))?;

    let value = parsed.val();
    let parsed_scale = parsed.scale() as i32;
    let target_scale = scale as i32;
    let diff = target_scale - parsed_scale;
    let rescaled = if diff >= 0 {
        value
            .checked_mul(10_i128.pow(diff as u32))
            .ok_or_else(|| decode_err(column, format!("decimal `{s}` overflows")))?
    } else {
        value / 10_i128.pow((-diff) as u32)
    };
    Ok(Value::Decimal128(Decimal128::new(rescaled, precision, scale)))
}
