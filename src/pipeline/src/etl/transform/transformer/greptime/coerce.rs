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

use api::v1::column_data_type_extension::TypeExt;
use api::v1::column_def::{options_from_fulltext, options_from_inverted, options_from_skipping};
use api::v1::{ColumnDataTypeExtension, ColumnOptions, JsonTypeExtension};
use datatypes::schema::{FulltextOptions, SkippingIndexOptions};
use greptime_proto::v1::value::ValueData;
use greptime_proto::v1::{ColumnDataType, ColumnSchema, SemanticType};
use snafu::ResultExt;

use crate::error::{
    CoerceIncompatibleTypesSnafu, CoerceJsonTypeToSnafu, CoerceStringToTypeSnafu,
    CoerceTypeToJsonSnafu, CoerceUnsupportedEpochTypeSnafu, CoerceUnsupportedNullTypeSnafu,
    CoerceUnsupportedNullTypeToSnafu, ColumnOptionsSnafu, Error, Result,
};
use crate::etl::transform::index::Index;
use crate::etl::transform::{OnFailure, Transform};
use crate::etl::value::{Timestamp, Value};

impl TryFrom<Value> for ValueData {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self> {
        match value {
            Value::Null => CoerceUnsupportedNullTypeSnafu.fail(),

            Value::Int8(v) => Ok(ValueData::I32Value(v as i32)),
            Value::Int16(v) => Ok(ValueData::I32Value(v as i32)),
            Value::Int32(v) => Ok(ValueData::I32Value(v)),
            Value::Int64(v) => Ok(ValueData::I64Value(v)),

            Value::Uint8(v) => Ok(ValueData::U32Value(v as u32)),
            Value::Uint16(v) => Ok(ValueData::U32Value(v as u32)),
            Value::Uint32(v) => Ok(ValueData::U32Value(v)),
            Value::Uint64(v) => Ok(ValueData::U64Value(v)),

            Value::Float32(v) => Ok(ValueData::F32Value(v)),
            Value::Float64(v) => Ok(ValueData::F64Value(v)),

            Value::Boolean(v) => Ok(ValueData::BoolValue(v)),
            Value::String(v) => Ok(ValueData::StringValue(v)),

            Value::Timestamp(Timestamp::Nanosecond(ns)) => {
                Ok(ValueData::TimestampNanosecondValue(ns))
            }
            Value::Timestamp(Timestamp::Microsecond(us)) => {
                Ok(ValueData::TimestampMicrosecondValue(us))
            }
            Value::Timestamp(Timestamp::Millisecond(ms)) => {
                Ok(ValueData::TimestampMillisecondValue(ms))
            }
            Value::Timestamp(Timestamp::Second(s)) => Ok(ValueData::TimestampSecondValue(s)),

            Value::Array(_) | Value::Map(_) => {
                let data: jsonb::Value = value.into();
                Ok(ValueData::BinaryValue(data.to_vec()))
            }
        }
    }
}

pub(crate) fn coerce_columns(transform: &Transform) -> Result<Vec<ColumnSchema>> {
    let mut columns = Vec::new();

    for field in transform.fields.iter() {
        let column_name = field.target_or_input_field().to_string();

        let (datatype, datatype_extension) = coerce_type(transform)?;

        let semantic_type = coerce_semantic_type(transform) as i32;

        let column = ColumnSchema {
            column_name,
            datatype: datatype as i32,
            semantic_type,
            datatype_extension,
            options: coerce_options(transform)?,
        };
        columns.push(column);
    }

    Ok(columns)
}

fn coerce_semantic_type(transform: &Transform) -> SemanticType {
    if transform.tag {
        return SemanticType::Tag;
    }

    match transform.index {
        Some(Index::Tag) => SemanticType::Tag,
        Some(Index::Time) => SemanticType::Timestamp,
        Some(Index::Fulltext) | Some(Index::Skipping) | Some(Index::Inverted) | None => {
            SemanticType::Field
        }
    }
}

fn coerce_options(transform: &Transform) -> Result<Option<ColumnOptions>> {
    match transform.index {
        Some(Index::Fulltext) => options_from_fulltext(&FulltextOptions {
            enable: true,
            ..Default::default()
        })
        .context(ColumnOptionsSnafu),
        Some(Index::Skipping) => {
            options_from_skipping(&SkippingIndexOptions::default()).context(ColumnOptionsSnafu)
        }
        Some(Index::Inverted) => Ok(Some(options_from_inverted())),
        _ => Ok(None),
    }
}

fn coerce_type(transform: &Transform) -> Result<(ColumnDataType, Option<ColumnDataTypeExtension>)> {
    match transform.type_ {
        Value::Int8(_) => Ok((ColumnDataType::Int8, None)),
        Value::Int16(_) => Ok((ColumnDataType::Int16, None)),
        Value::Int32(_) => Ok((ColumnDataType::Int32, None)),
        Value::Int64(_) => Ok((ColumnDataType::Int64, None)),

        Value::Uint8(_) => Ok((ColumnDataType::Uint8, None)),
        Value::Uint16(_) => Ok((ColumnDataType::Uint16, None)),
        Value::Uint32(_) => Ok((ColumnDataType::Uint32, None)),
        Value::Uint64(_) => Ok((ColumnDataType::Uint64, None)),

        Value::Float32(_) => Ok((ColumnDataType::Float32, None)),
        Value::Float64(_) => Ok((ColumnDataType::Float64, None)),

        Value::Boolean(_) => Ok((ColumnDataType::Boolean, None)),
        Value::String(_) => Ok((ColumnDataType::String, None)),

        Value::Timestamp(Timestamp::Nanosecond(_)) => {
            Ok((ColumnDataType::TimestampNanosecond, None))
        }
        Value::Timestamp(Timestamp::Microsecond(_)) => {
            Ok((ColumnDataType::TimestampMicrosecond, None))
        }
        Value::Timestamp(Timestamp::Millisecond(_)) => {
            Ok((ColumnDataType::TimestampMillisecond, None))
        }
        Value::Timestamp(Timestamp::Second(_)) => Ok((ColumnDataType::TimestampSecond, None)),

        Value::Array(_) | Value::Map(_) => Ok((
            ColumnDataType::Binary,
            Some(ColumnDataTypeExtension {
                type_ext: Some(TypeExt::JsonType(JsonTypeExtension::JsonBinary.into())),
            }),
        )),

        Value::Null => CoerceUnsupportedNullTypeToSnafu {
            ty: transform.type_.to_str_type(),
        }
        .fail(),
    }
}

pub(crate) fn coerce_value(val: &Value, transform: &Transform) -> Result<Option<ValueData>> {
    match val {
        Value::Null => Ok(None),

        Value::Int8(n) => coerce_i64_value(*n as i64, transform),
        Value::Int16(n) => coerce_i64_value(*n as i64, transform),
        Value::Int32(n) => coerce_i64_value(*n as i64, transform),
        Value::Int64(n) => coerce_i64_value(*n, transform),

        Value::Uint8(n) => coerce_u64_value(*n as u64, transform),
        Value::Uint16(n) => coerce_u64_value(*n as u64, transform),
        Value::Uint32(n) => coerce_u64_value(*n as u64, transform),
        Value::Uint64(n) => coerce_u64_value(*n, transform),

        Value::Float32(n) => coerce_f64_value(*n as f64, transform),
        Value::Float64(n) => coerce_f64_value(*n, transform),

        Value::Boolean(b) => coerce_bool_value(*b, transform),
        Value::String(s) => coerce_string_value(s, transform),

        Value::Timestamp(input_timestamp) => match &transform.type_ {
            Value::Timestamp(target_timestamp) => match target_timestamp {
                Timestamp::Nanosecond(_) => Ok(Some(ValueData::TimestampNanosecondValue(
                    input_timestamp.timestamp_nanos(),
                ))),
                Timestamp::Microsecond(_) => Ok(Some(ValueData::TimestampMicrosecondValue(
                    input_timestamp.timestamp_micros(),
                ))),
                Timestamp::Millisecond(_) => Ok(Some(ValueData::TimestampMillisecondValue(
                    input_timestamp.timestamp_millis(),
                ))),
                Timestamp::Second(_) => Ok(Some(ValueData::TimestampSecondValue(
                    input_timestamp.timestamp(),
                ))),
            },
            _ => CoerceIncompatibleTypesSnafu {
                msg: "Timestamp can only be coerced to another type",
            }
            .fail(),
        },

        Value::Array(_) | Value::Map(_) => coerce_json_value(val, transform),
    }
}

fn coerce_bool_value(b: bool, transform: &Transform) -> Result<Option<ValueData>> {
    let val = match transform.type_ {
        Value::Int8(_) => ValueData::I8Value(b as i32),
        Value::Int16(_) => ValueData::I16Value(b as i32),
        Value::Int32(_) => ValueData::I32Value(b as i32),
        Value::Int64(_) => ValueData::I64Value(b as i64),

        Value::Uint8(_) => ValueData::U8Value(b as u32),
        Value::Uint16(_) => ValueData::U16Value(b as u32),
        Value::Uint32(_) => ValueData::U32Value(b as u32),
        Value::Uint64(_) => ValueData::U64Value(b as u64),

        Value::Float32(_) => ValueData::F32Value(if b { 1.0 } else { 0.0 }),
        Value::Float64(_) => ValueData::F64Value(if b { 1.0 } else { 0.0 }),

        Value::Boolean(_) => ValueData::BoolValue(b),
        Value::String(_) => ValueData::StringValue(b.to_string()),

        Value::Timestamp(_) => match transform.on_failure {
            Some(OnFailure::Ignore) => return Ok(None),
            Some(OnFailure::Default) => {
                return CoerceUnsupportedEpochTypeSnafu { ty: "Default" }.fail();
            }
            None => {
                return CoerceUnsupportedEpochTypeSnafu { ty: "Boolean" }.fail();
            }
        },

        Value::Array(_) | Value::Map(_) => {
            return CoerceJsonTypeToSnafu {
                ty: transform.type_.to_str_type(),
            }
            .fail()
        }

        Value::Null => return Ok(None),
    };

    Ok(Some(val))
}

fn coerce_i64_value(n: i64, transform: &Transform) -> Result<Option<ValueData>> {
    let val = match &transform.type_ {
        Value::Int8(_) => ValueData::I8Value(n as i32),
        Value::Int16(_) => ValueData::I16Value(n as i32),
        Value::Int32(_) => ValueData::I32Value(n as i32),
        Value::Int64(_) => ValueData::I64Value(n),

        Value::Uint8(_) => ValueData::U8Value(n as u32),
        Value::Uint16(_) => ValueData::U16Value(n as u32),
        Value::Uint32(_) => ValueData::U32Value(n as u32),
        Value::Uint64(_) => ValueData::U64Value(n as u64),

        Value::Float32(_) => ValueData::F32Value(n as f32),
        Value::Float64(_) => ValueData::F64Value(n as f64),

        Value::Boolean(_) => ValueData::BoolValue(n != 0),
        Value::String(_) => ValueData::StringValue(n.to_string()),

        Value::Timestamp(unit) => match unit {
            Timestamp::Nanosecond(_) => ValueData::TimestampNanosecondValue(n),
            Timestamp::Microsecond(_) => ValueData::TimestampMicrosecondValue(n),
            Timestamp::Millisecond(_) => ValueData::TimestampMillisecondValue(n),
            Timestamp::Second(_) => ValueData::TimestampSecondValue(n),
        },

        Value::Array(_) | Value::Map(_) => {
            return CoerceJsonTypeToSnafu {
                ty: transform.type_.to_str_type(),
            }
            .fail()
        }

        Value::Null => return Ok(None),
    };

    Ok(Some(val))
}

fn coerce_u64_value(n: u64, transform: &Transform) -> Result<Option<ValueData>> {
    let val = match &transform.type_ {
        Value::Int8(_) => ValueData::I8Value(n as i32),
        Value::Int16(_) => ValueData::I16Value(n as i32),
        Value::Int32(_) => ValueData::I32Value(n as i32),
        Value::Int64(_) => ValueData::I64Value(n as i64),

        Value::Uint8(_) => ValueData::U8Value(n as u32),
        Value::Uint16(_) => ValueData::U16Value(n as u32),
        Value::Uint32(_) => ValueData::U32Value(n as u32),
        Value::Uint64(_) => ValueData::U64Value(n),

        Value::Float32(_) => ValueData::F32Value(n as f32),
        Value::Float64(_) => ValueData::F64Value(n as f64),

        Value::Boolean(_) => ValueData::BoolValue(n != 0),
        Value::String(_) => ValueData::StringValue(n.to_string()),

        Value::Timestamp(unit) => match unit {
            Timestamp::Nanosecond(_) => ValueData::TimestampNanosecondValue(n as i64),
            Timestamp::Microsecond(_) => ValueData::TimestampMicrosecondValue(n as i64),
            Timestamp::Millisecond(_) => ValueData::TimestampMillisecondValue(n as i64),
            Timestamp::Second(_) => ValueData::TimestampSecondValue(n as i64),
        },

        Value::Array(_) | Value::Map(_) => {
            return CoerceJsonTypeToSnafu {
                ty: transform.type_.to_str_type(),
            }
            .fail()
        }

        Value::Null => return Ok(None),
    };

    Ok(Some(val))
}

fn coerce_f64_value(n: f64, transform: &Transform) -> Result<Option<ValueData>> {
    let val = match transform.type_ {
        Value::Int8(_) => ValueData::I8Value(n as i32),
        Value::Int16(_) => ValueData::I16Value(n as i32),
        Value::Int32(_) => ValueData::I32Value(n as i32),
        Value::Int64(_) => ValueData::I64Value(n as i64),

        Value::Uint8(_) => ValueData::U8Value(n as u32),
        Value::Uint16(_) => ValueData::U16Value(n as u32),
        Value::Uint32(_) => ValueData::U32Value(n as u32),
        Value::Uint64(_) => ValueData::U64Value(n as u64),

        Value::Float32(_) => ValueData::F32Value(n as f32),
        Value::Float64(_) => ValueData::F64Value(n),

        Value::Boolean(_) => ValueData::BoolValue(n != 0.0),
        Value::String(_) => ValueData::StringValue(n.to_string()),

        Value::Timestamp(_) => match transform.on_failure {
            Some(OnFailure::Ignore) => return Ok(None),
            Some(OnFailure::Default) => {
                return CoerceUnsupportedEpochTypeSnafu { ty: "Default" }.fail();
            }
            None => {
                return CoerceUnsupportedEpochTypeSnafu { ty: "Float" }.fail();
            }
        },

        Value::Array(_) | Value::Map(_) => {
            return CoerceJsonTypeToSnafu {
                ty: transform.type_.to_str_type(),
            }
            .fail()
        }

        Value::Null => return Ok(None),
    };

    Ok(Some(val))
}

macro_rules! coerce_string_value {
    ($s:expr, $transform:expr, $type:ident, $parse:ident) => {
        match $s.parse::<$type>() {
            Ok(v) => Ok(Some(ValueData::$parse(v))),
            Err(_) => match $transform.on_failure {
                Some(OnFailure::Ignore) => Ok(None),
                Some(OnFailure::Default) => match $transform.get_default() {
                    Some(default) => coerce_value(default, $transform),
                    None => coerce_value($transform.get_type_matched_default_val(), $transform),
                },
                None => CoerceStringToTypeSnafu {
                    s: $s,
                    ty: $transform.type_.to_str_type(),
                }
                .fail(),
            },
        }
    };
}

fn coerce_string_value(s: &String, transform: &Transform) -> Result<Option<ValueData>> {
    match transform.type_ {
        Value::Int8(_) => {
            coerce_string_value!(s, transform, i32, I8Value)
        }
        Value::Int16(_) => {
            coerce_string_value!(s, transform, i32, I16Value)
        }
        Value::Int32(_) => {
            coerce_string_value!(s, transform, i32, I32Value)
        }
        Value::Int64(_) => {
            coerce_string_value!(s, transform, i64, I64Value)
        }

        Value::Uint8(_) => {
            coerce_string_value!(s, transform, u32, U8Value)
        }
        Value::Uint16(_) => {
            coerce_string_value!(s, transform, u32, U16Value)
        }
        Value::Uint32(_) => {
            coerce_string_value!(s, transform, u32, U32Value)
        }
        Value::Uint64(_) => {
            coerce_string_value!(s, transform, u64, U64Value)
        }

        Value::Float32(_) => {
            coerce_string_value!(s, transform, f32, F32Value)
        }
        Value::Float64(_) => {
            coerce_string_value!(s, transform, f64, F64Value)
        }

        Value::Boolean(_) => {
            coerce_string_value!(s, transform, bool, BoolValue)
        }

        Value::String(_) => Ok(Some(ValueData::StringValue(s.to_string()))),

        Value::Timestamp(_) => match transform.on_failure {
            Some(OnFailure::Ignore) => Ok(None),
            Some(OnFailure::Default) => CoerceUnsupportedEpochTypeSnafu { ty: "Default" }.fail(),
            None => CoerceUnsupportedEpochTypeSnafu { ty: "String" }.fail(),
        },

        Value::Array(_) | Value::Map(_) => CoerceStringToTypeSnafu {
            s,
            ty: transform.type_.to_str_type(),
        }
        .fail(),

        Value::Null => Ok(None),
    }
}

fn coerce_json_value(v: &Value, transform: &Transform) -> Result<Option<ValueData>> {
    match &transform.type_ {
        Value::Array(_) | Value::Map(_) => (),
        t => {
            return CoerceTypeToJsonSnafu {
                ty: t.to_str_type(),
            }
            .fail();
        }
    }
    match v {
        Value::Map(_) => {
            let data: jsonb::Value = v.into();
            Ok(Some(ValueData::BinaryValue(data.to_vec())))
        }
        Value::Array(_) => {
            let data: jsonb::Value = v.into();
            Ok(Some(ValueData::BinaryValue(data.to_vec())))
        }
        _ => CoerceTypeToJsonSnafu {
            ty: v.to_str_type(),
        }
        .fail(),
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::etl::field::Fields;

    #[test]
    fn test_coerce_string_without_on_failure() {
        let transform = Transform {
            fields: Fields::default(),
            type_: Value::Int32(0),
            default: None,
            index: None,
            on_failure: None,
            tag: false,
        };

        // valid string
        {
            let val = Value::String("123".to_string());
            let result = coerce_value(&val, &transform).unwrap();
            assert_eq!(result, Some(ValueData::I32Value(123)));
        }

        // invalid string
        {
            let val = Value::String("hello".to_string());
            let result = coerce_value(&val, &transform);
            assert!(result.is_err());
        }
    }

    #[test]
    fn test_coerce_string_with_on_failure_ignore() {
        let transform = Transform {
            fields: Fields::default(),
            type_: Value::Int32(0),
            default: None,
            index: None,
            on_failure: Some(OnFailure::Ignore),
            tag: false,
        };

        let val = Value::String("hello".to_string());
        let result = coerce_value(&val, &transform).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_coerce_string_with_on_failure_default() {
        let mut transform = Transform {
            fields: Fields::default(),
            type_: Value::Int32(0),
            default: None,
            index: None,
            on_failure: Some(OnFailure::Default),
            tag: false,
        };

        // with no explicit default value
        {
            let val = Value::String("hello".to_string());
            let result = coerce_value(&val, &transform).unwrap();
            assert_eq!(result, Some(ValueData::I32Value(0)));
        }

        // with explicit default value
        {
            transform.default = Some(Value::Int32(42));
            let val = Value::String("hello".to_string());
            let result = coerce_value(&val, &transform).unwrap();
            assert_eq!(result, Some(ValueData::I32Value(42)));
        }
    }
}
