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
use snafu::{OptionExt, ResultExt};
use vrl::value::Value as VrlValue;

use crate::error::{
    CoerceIncompatibleTypesSnafu, CoerceJsonTypeToSnafu, CoerceStringToTypeSnafu,
    CoerceTypeToJsonSnafu, CoerceUnsupportedEpochTypeSnafu, ColumnOptionsSnafu,
    InvalidTimestampSnafu, Result, UnsupportedTypeInPipelineSnafu, VrlRegexValueSnafu,
};
use crate::etl::transform::index::Index;
use crate::etl::transform::transformer::greptime::vrl_value_to_jsonb_value;
use crate::etl::transform::{OnFailure, Transform};

// impl TryFrom<Value> for ValueData {
//     type Error = Error;

//     fn try_from(value: Value) -> Result<Self> {
//         match value {
//             Value::Null => CoerceUnsupportedNullTypeSnafu.fail(),

//             Value::Int8(v) => Ok(ValueData::I32Value(v as i32)),
//             Value::Int16(v) => Ok(ValueData::I32Value(v as i32)),
//             Value::Int32(v) => Ok(ValueData::I32Value(v)),
//             Value::Int64(v) => Ok(ValueData::I64Value(v)),

//             Value::Uint8(v) => Ok(ValueData::U32Value(v as u32)),
//             Value::Uint16(v) => Ok(ValueData::U32Value(v as u32)),
//             Value::Uint32(v) => Ok(ValueData::U32Value(v)),
//             Value::Uint64(v) => Ok(ValueData::U64Value(v)),

//             Value::Float32(v) => Ok(ValueData::F32Value(v)),
//             Value::Float64(v) => Ok(ValueData::F64Value(v)),

//             Value::Boolean(v) => Ok(ValueData::BoolValue(v)),
//             Value::String(v) => Ok(ValueData::StringValue(v)),

//             Value::Timestamp(Timestamp::Nanosecond(ns)) => {
//                 Ok(ValueData::TimestampNanosecondValue(ns))
//             }
//             Value::Timestamp(Timestamp::Microsecond(us)) => {
//                 Ok(ValueData::TimestampMicrosecondValue(us))
//             }
//             Value::Timestamp(Timestamp::Millisecond(ms)) => {
//                 Ok(ValueData::TimestampMillisecondValue(ms))
//             }
//             Value::Timestamp(Timestamp::Second(s)) => Ok(ValueData::TimestampSecondValue(s)),

//             Value::Array(_) | Value::Map(_) => {
//                 let data: jsonb::Value = value.into();
//                 Ok(ValueData::BinaryValue(data.to_vec()))
//             }
//         }
//     }
// }

pub(crate) fn coerce_columns(transform: &Transform) -> Result<Vec<ColumnSchema>> {
    let mut columns = Vec::new();

    for field in transform.fields.iter() {
        let column_name = field.target_or_input_field().to_string();

        // let (datatype, datatype_extension) = coerce_type(transform)?;
        let ext = if matches!(transform.type_, ColumnDataType::Binary) {
            Some(ColumnDataTypeExtension {
                type_ext: Some(TypeExt::JsonType(JsonTypeExtension::JsonBinary.into())),
            })
        } else {
            None
        };

        let semantic_type = coerce_semantic_type(transform) as i32;

        let column = ColumnSchema {
            column_name,
            datatype: transform.type_ as i32,
            semantic_type,
            datatype_extension: ext,
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

// fn coerce_type(transform: &Transform) -> Result<(ColumnDataType, Option<ColumnDataTypeExtension>)> {
//     match transform.type_ {
//         Value::Int8(_) => Ok((ColumnDataType::Int8, None)),
//         Value::Int16(_) => Ok((ColumnDataType::Int16, None)),
//         Value::Int32(_) => Ok((ColumnDataType::Int32, None)),
//         Value::Int64(_) => Ok((ColumnDataType::Int64, None)),

//         Value::Uint8(_) => Ok((ColumnDataType::Uint8, None)),
//         Value::Uint16(_) => Ok((ColumnDataType::Uint16, None)),
//         Value::Uint32(_) => Ok((ColumnDataType::Uint32, None)),
//         Value::Uint64(_) => Ok((ColumnDataType::Uint64, None)),

//         Value::Float32(_) => Ok((ColumnDataType::Float32, None)),
//         Value::Float64(_) => Ok((ColumnDataType::Float64, None)),

//         Value::Boolean(_) => Ok((ColumnDataType::Boolean, None)),
//         Value::String(_) => Ok((ColumnDataType::String, None)),

//         Value::Timestamp(Timestamp::Nanosecond(_)) => {
//             Ok((ColumnDataType::TimestampNanosecond, None))
//         }
//         Value::Timestamp(Timestamp::Microsecond(_)) => {
//             Ok((ColumnDataType::TimestampMicrosecond, None))
//         }
//         Value::Timestamp(Timestamp::Millisecond(_)) => {
//             Ok((ColumnDataType::TimestampMillisecond, None))
//         }
//         Value::Timestamp(Timestamp::Second(_)) => Ok((ColumnDataType::TimestampSecond, None)),

//         Value::Array(_) | Value::Map(_) => Ok((
//             ColumnDataType::Binary,
//             Some(ColumnDataTypeExtension {
//                 type_ext: Some(TypeExt::JsonType(JsonTypeExtension::JsonBinary.into())),
//             }),
//         )),

//         Value::Null => CoerceUnsupportedNullTypeToSnafu {
//             ty: transform.type_.to_str_type(),
//         }
//         .fail(),
//     }
// }

pub(crate) fn coerce_value(val: &VrlValue, transform: &Transform) -> Result<Option<ValueData>> {
    match val {
        VrlValue::Null => Ok(None),
        VrlValue::Integer(n) => coerce_i64_value(*n, transform),
        VrlValue::Float(n) => coerce_f64_value(n.into_inner(), transform),
        VrlValue::Boolean(b) => coerce_bool_value(*b, transform),
        VrlValue::Bytes(b) => {
            coerce_string_value(&String::from_utf8_lossy(b).to_string(), transform)
        }
        VrlValue::Timestamp(ts) => match transform.type_ {
            ColumnDataType::TimestampNanosecond => Ok(Some(ValueData::TimestampNanosecondValue(
                ts.timestamp_nanos_opt().context(InvalidTimestampSnafu {
                    input: ts.to_rfc3339(),
                })?,
            ))),
            ColumnDataType::TimestampMicrosecond => Ok(Some(ValueData::TimestampMicrosecondValue(
                ts.timestamp_micros(),
            ))),
            ColumnDataType::TimestampMillisecond => Ok(Some(ValueData::TimestampMillisecondValue(
                ts.timestamp_millis(),
            ))),
            ColumnDataType::TimestampSecond => {
                Ok(Some(ValueData::TimestampSecondValue(ts.timestamp())))
            }
            _ => CoerceIncompatibleTypesSnafu {
                msg: "Timestamp can only be coerced to another type",
            }
            .fail(),
        },
        VrlValue::Array(_) | VrlValue::Object(_) => coerce_json_value(val, transform),
        VrlValue::Regex(_) => VrlRegexValueSnafu.fail(),
    }
}

fn coerce_bool_value(b: bool, transform: &Transform) -> Result<Option<ValueData>> {
    let val = match transform.type_ {
        ColumnDataType::Int8 => ValueData::I8Value(b as i32),
        ColumnDataType::Int16 => ValueData::I16Value(b as i32),
        ColumnDataType::Int32 => ValueData::I32Value(b as i32),
        ColumnDataType::Int64 => ValueData::I64Value(b as i64),

        ColumnDataType::Uint8 => ValueData::U8Value(b as u32),
        ColumnDataType::Uint16 => ValueData::U16Value(b as u32),
        ColumnDataType::Uint32 => ValueData::U32Value(b as u32),
        ColumnDataType::Uint64 => ValueData::U64Value(b as u64),

        ColumnDataType::Float32 => ValueData::F32Value(if b { 1.0 } else { 0.0 }),
        ColumnDataType::Float64 => ValueData::F64Value(if b { 1.0 } else { 0.0 }),

        ColumnDataType::Boolean => ValueData::BoolValue(b),
        ColumnDataType::String => ValueData::StringValue(b.to_string()),

        ColumnDataType::TimestampNanosecond
        | ColumnDataType::TimestampMicrosecond
        | ColumnDataType::TimestampMillisecond
        | ColumnDataType::TimestampSecond => match transform.on_failure {
            Some(OnFailure::Ignore) => return Ok(None),
            Some(OnFailure::Default) => {
                return CoerceUnsupportedEpochTypeSnafu { ty: "Default" }.fail();
            }
            None => {
                return CoerceUnsupportedEpochTypeSnafu { ty: "Boolean" }.fail();
            }
        },

        ColumnDataType::Binary => {
            return CoerceJsonTypeToSnafu {
                ty: transform.type_.as_str_name(),
            }
            .fail()
        }

        _ => {
            return UnsupportedTypeInPipelineSnafu {
                ty: transform.type_.as_str_name(),
            }
            .fail()
        }
    };

    Ok(Some(val))
}

fn coerce_i64_value(n: i64, transform: &Transform) -> Result<Option<ValueData>> {
    let val = match &transform.type_ {
        ColumnDataType::Int8 => ValueData::I8Value(n as i32),
        ColumnDataType::Int16 => ValueData::I16Value(n as i32),
        ColumnDataType::Int32 => ValueData::I32Value(n as i32),
        ColumnDataType::Int64 => ValueData::I64Value(n),

        ColumnDataType::Uint8 => ValueData::U8Value(n as u32),
        ColumnDataType::Uint16 => ValueData::U16Value(n as u32),
        ColumnDataType::Uint32 => ValueData::U32Value(n as u32),
        ColumnDataType::Uint64 => ValueData::U64Value(n as u64),

        ColumnDataType::Float32 => ValueData::F32Value(n as f32),
        ColumnDataType::Float64 => ValueData::F64Value(n as f64),

        ColumnDataType::Boolean => ValueData::BoolValue(n != 0),
        ColumnDataType::String => ValueData::StringValue(n.to_string()),

        ColumnDataType::TimestampNanosecond => ValueData::TimestampNanosecondValue(n),
        ColumnDataType::TimestampMicrosecond => ValueData::TimestampMicrosecondValue(n),
        ColumnDataType::TimestampMillisecond => ValueData::TimestampMillisecondValue(n),
        ColumnDataType::TimestampSecond => ValueData::TimestampSecondValue(n),

        ColumnDataType::Binary => {
            return CoerceJsonTypeToSnafu {
                ty: transform.type_.as_str_name(),
            }
            .fail()
        }

        _ => return Ok(None),
    };

    Ok(Some(val))
}

fn coerce_u64_value(n: u64, transform: &Transform) -> Result<Option<ValueData>> {
    let val = match &transform.type_ {
        ColumnDataType::Int8 => ValueData::I8Value(n as i32),
        ColumnDataType::Int16 => ValueData::I16Value(n as i32),
        ColumnDataType::Int32 => ValueData::I32Value(n as i32),
        ColumnDataType::Int64 => ValueData::I64Value(n as i64),

        ColumnDataType::Uint8 => ValueData::U8Value(n as u32),
        ColumnDataType::Uint16 => ValueData::U16Value(n as u32),
        ColumnDataType::Uint32 => ValueData::U32Value(n as u32),
        ColumnDataType::Uint64 => ValueData::U64Value(n),

        ColumnDataType::Float32 => ValueData::F32Value(n as f32),
        ColumnDataType::Float64 => ValueData::F64Value(n as f64),

        ColumnDataType::Boolean => ValueData::BoolValue(n != 0),
        ColumnDataType::String => ValueData::StringValue(n.to_string()),

        ColumnDataType::TimestampNanosecond => ValueData::TimestampNanosecondValue(n as i64),
        ColumnDataType::TimestampMicrosecond => ValueData::TimestampMicrosecondValue(n as i64),
        ColumnDataType::TimestampMillisecond => ValueData::TimestampMillisecondValue(n as i64),
        ColumnDataType::TimestampSecond => ValueData::TimestampSecondValue(n as i64),

        ColumnDataType::Binary => {
            return CoerceJsonTypeToSnafu {
                ty: transform.type_.as_str_name(),
            }
            .fail()
        }

        _ => return Ok(None),
    };

    Ok(Some(val))
}

fn coerce_f64_value(n: f64, transform: &Transform) -> Result<Option<ValueData>> {
    let val = match transform.type_ {
        ColumnDataType::Int8 => ValueData::I8Value(n as i32),
        ColumnDataType::Int16 => ValueData::I16Value(n as i32),
        ColumnDataType::Int32 => ValueData::I32Value(n as i32),
        ColumnDataType::Int64 => ValueData::I64Value(n as i64),

        ColumnDataType::Uint8 => ValueData::U8Value(n as u32),
        ColumnDataType::Uint16 => ValueData::U16Value(n as u32),
        ColumnDataType::Uint32 => ValueData::U32Value(n as u32),
        ColumnDataType::Uint64 => ValueData::U64Value(n as u64),

        ColumnDataType::Float32 => ValueData::F32Value(n as f32),
        ColumnDataType::Float64 => ValueData::F64Value(n),

        ColumnDataType::Boolean => ValueData::BoolValue(n != 0.0),
        ColumnDataType::String => ValueData::StringValue(n.to_string()),

        ColumnDataType::TimestampNanosecond
        | ColumnDataType::TimestampMicrosecond
        | ColumnDataType::TimestampMillisecond
        | ColumnDataType::TimestampSecond => match transform.on_failure {
            Some(OnFailure::Ignore) => return Ok(None),
            Some(OnFailure::Default) => {
                return CoerceUnsupportedEpochTypeSnafu { ty: "Default" }.fail();
            }
            None => {
                return CoerceUnsupportedEpochTypeSnafu { ty: "Float" }.fail();
            }
        },

        ColumnDataType::Binary => {
            return CoerceJsonTypeToSnafu {
                ty: transform.type_.as_str_name(),
            }
            .fail()
        }

        _ => return Ok(None),
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
                    Some(default) => Ok(Some(default.clone())),
                    None => $transform.get_type_matched_default_val().map(Some),
                },
                None => CoerceStringToTypeSnafu {
                    s: $s,
                    ty: $transform.type_.as_str_name(),
                }
                .fail(),
            },
        }
    };
}

fn coerce_string_value(s: &String, transform: &Transform) -> Result<Option<ValueData>> {
    match transform.type_ {
        ColumnDataType::Int8 => {
            coerce_string_value!(s, transform, i32, I8Value)
        }
        ColumnDataType::Int16 => {
            coerce_string_value!(s, transform, i32, I16Value)
        }
        ColumnDataType::Int32 => {
            coerce_string_value!(s, transform, i32, I32Value)
        }
        ColumnDataType::Int64 => {
            coerce_string_value!(s, transform, i64, I64Value)
        }

        ColumnDataType::Uint8 => {
            coerce_string_value!(s, transform, u32, U8Value)
        }
        ColumnDataType::Uint16 => {
            coerce_string_value!(s, transform, u32, U16Value)
        }
        ColumnDataType::Uint32 => {
            coerce_string_value!(s, transform, u32, U32Value)
        }
        ColumnDataType::Uint64 => {
            coerce_string_value!(s, transform, u64, U64Value)
        }

        ColumnDataType::Float32 => {
            coerce_string_value!(s, transform, f32, F32Value)
        }
        ColumnDataType::Float64 => {
            coerce_string_value!(s, transform, f64, F64Value)
        }

        ColumnDataType::Boolean => {
            coerce_string_value!(s, transform, bool, BoolValue)
        }

        ColumnDataType::String => Ok(Some(ValueData::StringValue(s.to_string()))),

        ColumnDataType::TimestampNanosecond
        | ColumnDataType::TimestampMicrosecond
        | ColumnDataType::TimestampMillisecond
        | ColumnDataType::TimestampSecond => match transform.on_failure {
            Some(OnFailure::Ignore) => Ok(None),
            Some(OnFailure::Default) => CoerceUnsupportedEpochTypeSnafu { ty: "Default" }.fail(),
            None => CoerceUnsupportedEpochTypeSnafu { ty: "String" }.fail(),
        },

        ColumnDataType::Binary => CoerceStringToTypeSnafu {
            s,
            ty: transform.type_.as_str_name(),
        }
        .fail(),

        _ => Ok(None),
    }
}

fn coerce_json_value(v: &VrlValue, transform: &Transform) -> Result<Option<ValueData>> {
    match &transform.type_ {
        ColumnDataType::Binary => (),
        t => {
            return CoerceTypeToJsonSnafu {
                ty: t.as_str_name(),
            }
            .fail();
        }
    }
    let data: jsonb::Value = vrl_value_to_jsonb_value(v);
    Ok(Some(ValueData::BinaryValue(data.to_vec())))
}

#[cfg(test)]
mod tests {

    use vrl::prelude::Bytes;

    use super::*;
    use crate::etl::field::Fields;

    #[test]
    fn test_coerce_string_without_on_failure() {
        let transform = Transform {
            fields: Fields::default(),
            type_: ColumnDataType::Int32,
            default: None,
            index: None,
            on_failure: None,
            tag: false,
        };

        // valid string
        {
            let val = VrlValue::Integer(123);
            let result = coerce_value(&val, &transform).unwrap();
            assert_eq!(result, Some(ValueData::I32Value(123)));
        }

        // invalid string
        {
            let val = VrlValue::Bytes(Bytes::from("hello"));
            let result = coerce_value(&val, &transform);
            assert!(result.is_err());
        }
    }

    #[test]
    fn test_coerce_string_with_on_failure_ignore() {
        let transform = Transform {
            fields: Fields::default(),
            type_: ColumnDataType::Int32,
            default: None,
            index: None,
            on_failure: Some(OnFailure::Ignore),
            tag: false,
        };

        let val = VrlValue::Bytes(Bytes::from("hello"));
        let result = coerce_value(&val, &transform).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_coerce_string_with_on_failure_default() {
        let mut transform = Transform {
            fields: Fields::default(),
            type_: ColumnDataType::Int32,
            default: None,
            index: None,
            on_failure: Some(OnFailure::Default),
            tag: false,
        };

        // with no explicit default value
        {
            let val = VrlValue::Bytes(Bytes::from("hello"));
            let result = coerce_value(&val, &transform).unwrap();
            assert_eq!(result, Some(ValueData::I32Value(0)));
        }

        // with explicit default value
        {
            transform.default = Some(ValueData::I32Value(42));
            let val = VrlValue::Bytes(Bytes::from("hello"));
            let result = coerce_value(&val, &transform).unwrap();
            assert_eq!(result, Some(ValueData::I32Value(42)));
        }
    }
}
