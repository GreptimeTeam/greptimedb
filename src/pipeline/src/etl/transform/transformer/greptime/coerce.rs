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

use api::v1::column_def::options_from_fulltext;
use api::v1::ColumnOptions;
use datatypes::schema::FulltextOptions;
use greptime_proto::v1::value::ValueData;
use greptime_proto::v1::{ColumnDataType, ColumnSchema, SemanticType};

use crate::etl::transform::index::Index;
use crate::etl::transform::{OnFailure, Transform};
use crate::etl::value::{Epoch, Time, Value};

impl TryFrom<Value> for ValueData {
    type Error = String;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Null => Err("Null type not supported".to_string()),

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

            Value::Time(Time { nanosecond, .. }) => Ok(ValueData::TimeNanosecondValue(nanosecond)),

            Value::Epoch(Epoch::Nanosecond(ns)) => Ok(ValueData::TimestampNanosecondValue(ns)),
            Value::Epoch(Epoch::Microsecond(us)) => Ok(ValueData::TimestampMicrosecondValue(us)),
            Value::Epoch(Epoch::Millisecond(ms)) => Ok(ValueData::TimestampMillisecondValue(ms)),
            Value::Epoch(Epoch::Second(s)) => Ok(ValueData::TimestampSecondValue(s)),

            Value::Array(_) => unimplemented!("Array type not supported"),
            Value::Map(_) => unimplemented!("Object type not supported"),
        }
    }
}

// TODO(yuanbohan): add fulltext support in datatype_extension
pub(crate) fn coerce_columns(transform: &Transform) -> Result<Vec<ColumnSchema>, String> {
    let mut columns = Vec::new();

    for field in transform.fields.iter() {
        let column_name = field.get_renamed_field().to_string();

        let datatype = coerce_type(transform)? as i32;

        let semantic_type = coerce_semantic_type(transform) as i32;

        let column = ColumnSchema {
            column_name,
            datatype,
            semantic_type,
            datatype_extension: None,
            options: coerce_options(transform)?,
        };
        columns.push(column);
    }

    Ok(columns)
}

fn coerce_semantic_type(transform: &Transform) -> SemanticType {
    match transform.index {
        Some(Index::Tag) => SemanticType::Tag,
        Some(Index::TimeIndex) => SemanticType::Timestamp,
        Some(Index::Fulltext) | None => SemanticType::Field,
    }
}

fn coerce_options(transform: &Transform) -> Result<Option<ColumnOptions>, String> {
    if let Some(Index::Fulltext) = transform.index {
        options_from_fulltext(&FulltextOptions {
            enable: true,
            ..Default::default()
        })
        .map_err(|e| e.to_string())
    } else {
        Ok(None)
    }
}

fn coerce_type(transform: &Transform) -> Result<ColumnDataType, String> {
    match transform.type_ {
        Value::Int8(_) => Ok(ColumnDataType::Int8),
        Value::Int16(_) => Ok(ColumnDataType::Int16),
        Value::Int32(_) => Ok(ColumnDataType::Int32),
        Value::Int64(_) => Ok(ColumnDataType::Int64),

        Value::Uint8(_) => Ok(ColumnDataType::Uint8),
        Value::Uint16(_) => Ok(ColumnDataType::Uint16),
        Value::Uint32(_) => Ok(ColumnDataType::Uint32),
        Value::Uint64(_) => Ok(ColumnDataType::Uint64),

        Value::Float32(_) => Ok(ColumnDataType::Float32),
        Value::Float64(_) => Ok(ColumnDataType::Float64),

        Value::Boolean(_) => Ok(ColumnDataType::Boolean),
        Value::String(_) => Ok(ColumnDataType::String),

        Value::Time(_) => Ok(ColumnDataType::TimestampNanosecond),

        Value::Epoch(Epoch::Nanosecond(_)) => Ok(ColumnDataType::TimestampNanosecond),
        Value::Epoch(Epoch::Microsecond(_)) => Ok(ColumnDataType::TimestampMicrosecond),
        Value::Epoch(Epoch::Millisecond(_)) => Ok(ColumnDataType::TimestampMillisecond),
        Value::Epoch(Epoch::Second(_)) => Ok(ColumnDataType::TimestampSecond),

        Value::Array(_) => unimplemented!("Array"),
        Value::Map(_) => unimplemented!("Object"),

        Value::Null => Err(format!(
            "Null type not supported when to coerce '{}' type",
            transform.fields
        )),
    }
}

pub(crate) fn coerce_value(
    val: &Value,
    transform: &Transform,
) -> Result<Option<ValueData>, String> {
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

        Value::Time(Time { nanosecond, .. }) => {
            Ok(Some(ValueData::TimestampNanosecondValue(*nanosecond)))
        }

        Value::Epoch(Epoch::Nanosecond(ns)) => Ok(Some(ValueData::TimestampNanosecondValue(*ns))),
        Value::Epoch(Epoch::Microsecond(us)) => Ok(Some(ValueData::TimestampMicrosecondValue(*us))),
        Value::Epoch(Epoch::Millisecond(ms)) => Ok(Some(ValueData::TimestampMillisecondValue(*ms))),
        Value::Epoch(Epoch::Second(s)) => Ok(Some(ValueData::TimestampSecondValue(*s))),

        Value::Array(_) => unimplemented!("Array type not supported"),
        Value::Map(_) => unimplemented!("Object type not supported"),
    }
}

fn coerce_bool_value(b: bool, transform: &Transform) -> Result<Option<ValueData>, String> {
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

        Value::Time(_) => match transform.on_failure {
            Some(OnFailure::Ignore) => return Ok(None),
            Some(OnFailure::Default) => {
                return Err("default value not supported for Time".to_string())
            }
            None => return Err("Boolean type not supported for Time".to_string()),
        },
        Value::Epoch(_) => match transform.on_failure {
            Some(OnFailure::Ignore) => return Ok(None),
            Some(OnFailure::Default) => {
                return Err("default value not supported for Epoch".to_string())
            }
            None => return Err("Boolean type not supported for Epoch".to_string()),
        },

        Value::Array(_) => unimplemented!("Array type not supported"),
        Value::Map(_) => unimplemented!("Object type not supported"),

        Value::Null => return Ok(None),
    };

    Ok(Some(val))
}

fn coerce_i64_value(n: i64, transform: &Transform) -> Result<Option<ValueData>, String> {
    let val = match transform.type_ {
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

        Value::Time(_) => match transform.on_failure {
            Some(OnFailure::Ignore) => return Ok(None),
            Some(OnFailure::Default) => {
                return Err("default value not supported for Time".to_string())
            }
            None => return Err("Integer type not supported for Time".to_string()),
        },

        Value::Epoch(_) => match transform.on_failure {
            Some(OnFailure::Ignore) => return Ok(None),
            Some(OnFailure::Default) => {
                return Err("default value not supported for Epoch".to_string())
            }
            None => return Err("Integer type not supported for Epoch".to_string()),
        },

        Value::Array(_) => unimplemented!("Array type not supported"),
        Value::Map(_) => unimplemented!("Object type not supported"),

        Value::Null => return Ok(None),
    };

    Ok(Some(val))
}

fn coerce_u64_value(n: u64, transform: &Transform) -> Result<Option<ValueData>, String> {
    let val = match transform.type_ {
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

        Value::Time(_) => match transform.on_failure {
            Some(OnFailure::Ignore) => return Ok(None),
            Some(OnFailure::Default) => {
                return Err("default value not supported for Time".to_string())
            }
            None => return Err("Integer type not supported for Time".to_string()),
        },

        Value::Epoch(_) => match transform.on_failure {
            Some(OnFailure::Ignore) => return Ok(None),
            Some(OnFailure::Default) => {
                return Err("default value not supported for Epoch".to_string())
            }
            None => return Err("Integer type not supported for Epoch".to_string()),
        },

        Value::Array(_) => unimplemented!("Array type not supported"),
        Value::Map(_) => unimplemented!("Object type not supported"),

        Value::Null => return Ok(None),
    };

    Ok(Some(val))
}

fn coerce_f64_value(n: f64, transform: &Transform) -> Result<Option<ValueData>, String> {
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

        Value::Time(_) => match transform.on_failure {
            Some(OnFailure::Ignore) => return Ok(None),
            Some(OnFailure::Default) => {
                return Err("default value not supported for Time".to_string())
            }
            None => return Err("Float type not supported for Time".to_string()),
        },

        Value::Epoch(_) => match transform.on_failure {
            Some(OnFailure::Ignore) => return Ok(None),
            Some(OnFailure::Default) => {
                return Err("default value not supported for Epoch".to_string())
            }
            None => return Err("Float type not supported for Epoch".to_string()),
        },

        Value::Array(_) => unimplemented!("Array type not supported"),
        Value::Map(_) => unimplemented!("Object type not supported"),

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
                None => Err(format!(
                    "failed to coerce string value '{}' to type '{}'",
                    $s,
                    $transform.type_.to_str_type()
                )),
            },
        }
    };
}

fn coerce_string_value(s: &String, transform: &Transform) -> Result<Option<ValueData>, String> {
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

        Value::Time(_) => match transform.on_failure {
            Some(OnFailure::Ignore) => Ok(None),
            Some(OnFailure::Default) => Err("default value not supported for Time".to_string()),
            None => Err("String type not supported for Time".to_string()),
        },

        Value::Epoch(_) => match transform.on_failure {
            Some(OnFailure::Ignore) => Ok(None),
            Some(OnFailure::Default) => Err("default value not supported for Epoch".to_string()),
            None => Err("String type not supported for Epoch".to_string()),
        },

        Value::Array(_) => unimplemented!("Array type not supported"),
        Value::Map(_) => unimplemented!("Object type not supported"),

        Value::Null => Ok(None),
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
