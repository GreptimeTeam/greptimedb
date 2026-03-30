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
        _ => None,
    }
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
    }
}
