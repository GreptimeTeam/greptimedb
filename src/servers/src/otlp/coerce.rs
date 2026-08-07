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

use api::v1::ColumnDataType;
use api::v1::value::ValueData;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TraceCoerceError {
    Unsupported,
}

// For now we support the following coercions:
// - Int64 to Float64
// - Int64 to String
// - Float64 to String
// - Boolean to String
// The following coercions are supported with parse, which could fail:
// If fails, we will return TraceCoerceError::Unsupported.
// - String to Int64
// - String to Float64
// - String to Boolean
//
// Lossless signed-to-unsigned integer casts. These let the built-in data
// models move from unsigned to signed integers while existing unsigned tables
// keep accepting new signed ingest without an `ALTER TABLE`: an existing
// UInt64/UInt32 column coerces an incoming Int64/Int32 request into the
// existing type. Lossless for the non-negative values these built-in fields
// always carry (counts, durations, flags):
// - Int64 to UInt64  (e.g. trace `duration_nano`)
// - Int32 to UInt32  (e.g. log `trace_flags`)
pub fn is_supported_trace_coercion(
    request_type: ColumnDataType,
    target_type: ColumnDataType,
) -> bool {
    matches!(
        (request_type, target_type),
        (ColumnDataType::Int64, ColumnDataType::Float64)
            | (ColumnDataType::Int64, ColumnDataType::String)
            | (ColumnDataType::Float64, ColumnDataType::String)
            | (ColumnDataType::Boolean, ColumnDataType::String)
            | (ColumnDataType::String, ColumnDataType::Int64)
            | (ColumnDataType::String, ColumnDataType::Float64)
            | (ColumnDataType::String, ColumnDataType::Boolean)
            | (ColumnDataType::Int64, ColumnDataType::Uint64)
            | (ColumnDataType::Int32, ColumnDataType::Uint32)
    )
}

pub fn coerce_value_data(
    value: &Option<ValueData>,
    target: ColumnDataType,
    request_type: ColumnDataType,
) -> Result<Option<ValueData>, TraceCoerceError> {
    let Some(v) = value else {
        return Ok(None);
    };

    let Some(value) = coerce_non_null_value(target, request_type, v) else {
        return Err(TraceCoerceError::Unsupported);
    };
    Ok(Some(value))
}

pub fn coerce_non_null_value(
    target: ColumnDataType,
    request_type: ColumnDataType,
    value: &ValueData,
) -> Option<ValueData> {
    match (request_type, target, value) {
        (ColumnDataType::Int64, ColumnDataType::Float64, ValueData::I64Value(n)) => {
            Some(ValueData::F64Value(*n as f64))
        }
        (ColumnDataType::Int64, ColumnDataType::String, ValueData::I64Value(n)) => {
            Some(ValueData::StringValue(n.to_string()))
        }
        (ColumnDataType::Float64, ColumnDataType::String, ValueData::F64Value(n)) => {
            Some(ValueData::StringValue(n.to_string()))
        }
        (ColumnDataType::Boolean, ColumnDataType::String, ValueData::BoolValue(b)) => {
            Some(ValueData::StringValue(b.to_string()))
        }
        (ColumnDataType::String, ColumnDataType::Int64, ValueData::StringValue(s)) => {
            s.parse::<i64>().ok().map(ValueData::I64Value)
        }
        (ColumnDataType::String, ColumnDataType::Float64, ValueData::StringValue(s)) => {
            s.parse::<f64>().ok().map(ValueData::F64Value)
        }
        (ColumnDataType::String, ColumnDataType::Boolean, ValueData::StringValue(s)) => {
            s.parse::<bool>().ok().map(ValueData::BoolValue)
        }
        // Lossless signed -> unsigned casts for built-in fields moving to
        // signed types (durations, counts, flags are always non-negative).
        (ColumnDataType::Int64, ColumnDataType::Uint64, ValueData::I64Value(n)) => {
            Some(ValueData::U64Value(*n as u64))
        }
        (ColumnDataType::Int32, ColumnDataType::Uint32, ValueData::I32Value(n)) => {
            Some(ValueData::U32Value(*n as u32))
        }
        _ => None,
    }
}

pub fn trace_value_datatype(value: &ValueData) -> Option<ColumnDataType> {
    match value {
        ValueData::StringValue(_) => Some(ColumnDataType::String),
        ValueData::BoolValue(_) => Some(ColumnDataType::Boolean),
        ValueData::I64Value(_) => Some(ColumnDataType::Int64),
        ValueData::F64Value(_) => Some(ColumnDataType::Float64),
        ValueData::BinaryValue(_) => Some(ColumnDataType::Binary),
        _ => None,
    }
}

/// Resolves the final datatype for a new trace column when there is no existing
/// table schema to override the request-local observations.
pub fn resolve_new_trace_column_type(
    observed_types: impl IntoIterator<Item = ColumnDataType>,
) -> Result<Option<ColumnDataType>, TraceCoerceError> {
    let mut observed = Vec::new();
    for datatype in observed_types {
        if !observed.contains(&datatype) {
            observed.push(datatype);
        }
    }

    if observed.is_empty() {
        return Ok(None);
    }
    if observed.len() == 1 {
        return Ok(observed.first().copied());
    }

    [
        ColumnDataType::Boolean,
        ColumnDataType::Int64,
        ColumnDataType::Float64,
        ColumnDataType::String,
    ]
    .into_iter()
    .find(|target| {
        observed.contains(target)
            && observed
                .iter()
                .all(|source| source == target || is_supported_trace_coercion(*source, *target))
    })
    .map(Some)
    .ok_or(TraceCoerceError::Unsupported)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_coerce_int64_to_float64() {
        let result = coerce_value_data(
            &Some(ValueData::I64Value(42)),
            ColumnDataType::Float64,
            ColumnDataType::Int64,
        );
        assert_eq!(result, Ok(Some(ValueData::F64Value(42.0))));
    }

    #[test]
    fn test_coerce_string_to_int64() {
        let result = coerce_value_data(
            &Some(ValueData::StringValue("123".to_string())),
            ColumnDataType::Int64,
            ColumnDataType::String,
        );
        assert_eq!(result, Ok(Some(ValueData::I64Value(123))));
    }

    #[test]
    fn test_coerce_int64_to_string() {
        let result = coerce_value_data(
            &Some(ValueData::I64Value(123)),
            ColumnDataType::String,
            ColumnDataType::Int64,
        );
        assert_eq!(result, Ok(Some(ValueData::StringValue("123".to_string()))));
    }

    #[test]
    fn test_coerce_string_to_float64() {
        let result = coerce_value_data(
            &Some(ValueData::StringValue("1.5".to_string())),
            ColumnDataType::Float64,
            ColumnDataType::String,
        );
        assert_eq!(result, Ok(Some(ValueData::F64Value(1.5))));
    }

    #[test]
    fn test_coerce_float64_to_string() {
        let result = coerce_value_data(
            &Some(ValueData::F64Value(1.5)),
            ColumnDataType::String,
            ColumnDataType::Float64,
        );
        assert_eq!(result, Ok(Some(ValueData::StringValue("1.5".to_string()))));
    }

    #[test]
    fn test_coerce_string_to_boolean() {
        let result = coerce_value_data(
            &Some(ValueData::StringValue("true".to_string())),
            ColumnDataType::Boolean,
            ColumnDataType::String,
        );
        assert_eq!(result, Ok(Some(ValueData::BoolValue(true))));

        let result = coerce_value_data(
            &Some(ValueData::StringValue("false".to_string())),
            ColumnDataType::Boolean,
            ColumnDataType::String,
        );
        assert_eq!(result, Ok(Some(ValueData::BoolValue(false))));
    }

    #[test]
    fn test_coerce_boolean_to_string() {
        let result = coerce_value_data(
            &Some(ValueData::BoolValue(true)),
            ColumnDataType::String,
            ColumnDataType::Boolean,
        );
        assert_eq!(result, Ok(Some(ValueData::StringValue("true".to_string()))));
    }

    #[test]
    fn test_coerce_unparsable_string() {
        let result = coerce_value_data(
            &Some(ValueData::StringValue("not_a_number".to_string())),
            ColumnDataType::Int64,
            ColumnDataType::String,
        );
        assert_eq!(result, Err(TraceCoerceError::Unsupported));
    }

    #[test]
    fn test_coerce_float64_to_int64_not_supported() {
        let result = coerce_value_data(
            &Some(ValueData::F64Value(1.5)),
            ColumnDataType::Int64,
            ColumnDataType::Float64,
        );
        assert_eq!(result, Err(TraceCoerceError::Unsupported));
    }

    #[test]
    fn test_coerce_int64_to_uint64() {
        // Non-negative durations coerce losslessly into an existing UInt64
        // column, so built-in fields moving to signed keep accepting writes.
        let result = coerce_value_data(
            &Some(ValueData::I64Value(123)),
            ColumnDataType::Uint64,
            ColumnDataType::Int64,
        );
        assert_eq!(result, Ok(Some(ValueData::U64Value(123))));
    }

    #[test]
    fn test_coerce_int32_to_uint32() {
        let result = coerce_value_data(
            &Some(ValueData::I32Value(7)),
            ColumnDataType::Uint32,
            ColumnDataType::Int32,
        );
        assert_eq!(result, Ok(Some(ValueData::U32Value(7))));
    }

    #[test]
    fn test_coerce_uint_to_int_not_supported() {
        // Only the signed -> unsigned direction is supported (the direction
        // the no-ALTER transition needs); the reverse would be lossy for
        // values above the signed range and is intentionally rejected.
        let result = coerce_value_data(
            &Some(ValueData::U64Value(9)),
            ColumnDataType::Int64,
            ColumnDataType::Uint64,
        );
        assert_eq!(result, Err(TraceCoerceError::Unsupported));
    }

    #[test]
    fn test_coerce_none_value() {
        let result = coerce_value_data(&None, ColumnDataType::Float64, ColumnDataType::Int64);
        assert_eq!(result, Ok(None));
    }

    #[test]
    fn test_is_supported_trace_coercion() {
        assert!(is_supported_trace_coercion(
            ColumnDataType::Int64,
            ColumnDataType::Float64
        ));
        assert!(is_supported_trace_coercion(
            ColumnDataType::Int64,
            ColumnDataType::String
        ));
        assert!(is_supported_trace_coercion(
            ColumnDataType::Float64,
            ColumnDataType::String
        ));
        assert!(is_supported_trace_coercion(
            ColumnDataType::Boolean,
            ColumnDataType::String
        ));
        assert!(is_supported_trace_coercion(
            ColumnDataType::String,
            ColumnDataType::Int64
        ));
        assert!(is_supported_trace_coercion(
            ColumnDataType::String,
            ColumnDataType::Float64
        ));
        assert!(is_supported_trace_coercion(
            ColumnDataType::String,
            ColumnDataType::Boolean
        ));
        assert!(!is_supported_trace_coercion(
            ColumnDataType::Binary,
            ColumnDataType::Json
        ));
        // Signed -> unsigned casts are supported (built-in no-ALTER transition).
        assert!(is_supported_trace_coercion(
            ColumnDataType::Int64,
            ColumnDataType::Uint64
        ));
        assert!(is_supported_trace_coercion(
            ColumnDataType::Int32,
            ColumnDataType::Uint32
        ));
        // The reverse direction is intentionally not supported (lossy).
        assert!(!is_supported_trace_coercion(
            ColumnDataType::Uint64,
            ColumnDataType::Int64
        ));
    }

    #[test]
    fn test_trace_value_datatype() {
        assert_eq!(
            trace_value_datatype(&ValueData::StringValue("x".to_string())),
            Some(ColumnDataType::String)
        );
        assert_eq!(
            trace_value_datatype(&ValueData::BoolValue(true)),
            Some(ColumnDataType::Boolean)
        );
        assert_eq!(
            trace_value_datatype(&ValueData::I64Value(1)),
            Some(ColumnDataType::Int64)
        );
        assert_eq!(
            trace_value_datatype(&ValueData::F64Value(1.0)),
            Some(ColumnDataType::Float64)
        );
        assert_eq!(
            trace_value_datatype(&ValueData::BinaryValue(vec![1_u8])),
            Some(ColumnDataType::Binary)
        );
    }

    #[test]
    fn test_resolve_new_trace_column_type() {
        assert_eq!(
            resolve_new_trace_column_type([ColumnDataType::Int64]),
            Ok(Some(ColumnDataType::Int64))
        );
        assert_eq!(
            resolve_new_trace_column_type([ColumnDataType::String, ColumnDataType::Int64]),
            Ok(Some(ColumnDataType::Int64))
        );
        assert_eq!(
            resolve_new_trace_column_type([ColumnDataType::String, ColumnDataType::Float64]),
            Ok(Some(ColumnDataType::Float64))
        );
        assert_eq!(
            resolve_new_trace_column_type([ColumnDataType::String, ColumnDataType::Boolean]),
            Ok(Some(ColumnDataType::Boolean))
        );
        assert_eq!(
            resolve_new_trace_column_type([ColumnDataType::Int64, ColumnDataType::Float64]),
            Ok(Some(ColumnDataType::Float64))
        );
        assert_eq!(
            resolve_new_trace_column_type([
                ColumnDataType::String,
                ColumnDataType::Int64,
                ColumnDataType::Float64,
            ]),
            Ok(Some(ColumnDataType::Float64))
        );
        assert_eq!(
            resolve_new_trace_column_type([
                ColumnDataType::Float64,
                ColumnDataType::String,
                ColumnDataType::Int64,
            ]),
            Ok(Some(ColumnDataType::Float64))
        );
    }
}
