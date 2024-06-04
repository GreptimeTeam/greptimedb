// Copyright 2024 Greptime Team
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

use greptime_proto::v1::value::ValueData;
use greptime_proto::v1::{ColumnDataType, ColumnSchema, SemanticType};

use crate::etl::transform::index::Index;
use crate::etl::transform::Transform;
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
            Value::String(v) => Ok(ValueData::StringValue(v.clone())),

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
        let column_name = field.get_target_field().to_string();

        let datatype = coerce_type(transform)? as i32;

        let semantic_type = coerce_semantic_type(transform) as i32;

        let column = ColumnSchema {
            column_name,
            datatype,
            semantic_type,
            datatype_extension: None,
        };
        columns.push(column);
    }

    Ok(columns)
}

fn coerce_semantic_type(transform: &Transform) -> SemanticType {
    match transform.index {
        Some(Index::Tag) => SemanticType::Tag,
        Some(Index::Timestamp) => SemanticType::Timestamp,
        Some(Index::Fulltext) => unimplemented!("Fulltext"),
        None => SemanticType::Field,
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

        Value::Time(_) => return Err("Boolean type not supported for Time".to_string()),
        Value::Epoch(_) => return Err("Boolean type not supported for Epoch".to_string()),

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

        Value::Time(_) => return Err("Integer type not supported for Time".to_string()),
        Value::Epoch(_) => return Err("Integer type not supported for Epoch".to_string()),

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

        Value::Time(_) => return Err("Integer type not supported for Time".to_string()),
        Value::Epoch(_) => return Err("Integer type not supported for Epoch".to_string()),

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

        Value::Time(_) => return Err("Float type not supported for Time".to_string()),
        Value::Epoch(_) => return Err("Float type not supported for Epoch".to_string()),

        Value::Array(_) => unimplemented!("Array type not supported"),
        Value::Map(_) => unimplemented!("Object type not supported"),

        Value::Null => return Ok(None),
    };

    Ok(Some(val))
}

fn coerce_string_value(s: &str, transform: &Transform) -> Result<Option<ValueData>, String> {
    let val = match transform.type_ {
        Value::Int8(_) => ValueData::I8Value(s.parse::<i32>().map_err(|e| e.to_string())?),
        Value::Int16(_) => ValueData::I16Value(s.parse::<i32>().map_err(|e| e.to_string())?),
        Value::Int32(_) => ValueData::I32Value(s.parse::<i32>().map_err(|e| e.to_string())?),
        Value::Int64(_) => ValueData::I64Value(s.parse::<i64>().map_err(|e| e.to_string())?),

        Value::Uint8(_) => ValueData::U8Value(s.parse::<u32>().map_err(|e| e.to_string())?),
        Value::Uint16(_) => ValueData::U16Value(s.parse::<u32>().map_err(|e| e.to_string())?),
        Value::Uint32(_) => ValueData::U32Value(s.parse::<u32>().map_err(|e| e.to_string())?),
        Value::Uint64(_) => ValueData::U64Value(s.parse::<u64>().map_err(|e| e.to_string())?),

        Value::Float32(_) => ValueData::F32Value(s.parse::<f32>().map_err(|e| e.to_string())?),
        Value::Float64(_) => ValueData::F64Value(s.parse::<f64>().map_err(|e| e.to_string())?),

        Value::Boolean(_) => ValueData::BoolValue(s.parse::<bool>().map_err(|e| e.to_string())?),
        Value::String(_) => ValueData::StringValue(s.to_string()),

        Value::Time(_) => return Err("String type not supported for Time".to_string()),
        Value::Epoch(_) => return Err("String type not supported for Epoch".to_string()),

        Value::Array(_) => unimplemented!("Array type not supported"),
        Value::Map(_) => unimplemented!("Object type not supported"),

        Value::Null => return Ok(None),
    };

    Ok(Some(val))
}
