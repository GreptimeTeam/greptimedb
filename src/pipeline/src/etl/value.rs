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

pub mod array;
pub mod map;
pub mod time;

use ahash::{HashMap, HashMapExt};
pub use array::Array;
pub use map::Map;
pub use time::{Epoch, Time};

/// Value can be used as type
/// acts as value: the enclosed value is the actual value
/// acts as type: the enclosed value is the default value

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    // as value: null
    // as type: no type specified
    Null,

    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),

    Uint8(u8),
    Uint16(u16),
    Uint32(u32),
    Uint64(u64),

    Float32(f32),
    Float64(f64),

    Boolean(bool),
    String(String),

    Time(Time),
    Epoch(Epoch),

    Array(Array),
    Map(Map),
}

impl Value {
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    pub fn parse_str_type(t: &str) -> Result<Self, String> {
        let mut parts = t.splitn(2, ',');
        let head = parts.next().unwrap_or_default();
        let tail = parts.next().map(|s| s.trim().to_string());
        match head.to_lowercase().as_str() {
            "int8" => Ok(Value::Int8(0)),
            "int16" => Ok(Value::Int16(0)),
            "int32" => Ok(Value::Int32(0)),
            "int64" => Ok(Value::Int64(0)),

            "uint8" => Ok(Value::Uint8(0)),
            "uint16" => Ok(Value::Uint16(0)),
            "uint32" => Ok(Value::Uint32(0)),
            "uint64" => Ok(Value::Uint64(0)),

            "float32" => Ok(Value::Float32(0.0)),
            "float64" => Ok(Value::Float64(0.0)),

            "boolean" => Ok(Value::Boolean(false)),
            "string" => Ok(Value::String("".to_string())),

            "time" => Ok(Value::Time(Time::default())),
            "epoch" => match tail {
                Some(resolution) if !resolution.is_empty() => match resolution.as_str() {
                    time::NANOSECOND_RESOLUTION | time::NANO_RESOLUTION | time::NS_RESOLUTION => {
                        Ok(Value::Epoch(Epoch::Nanosecond(0)))
                    }
                    time::MICROSECOND_RESOLUTION | time::MICRO_RESOLUTION | time::US_RESOLUTION => {
                        Ok(Value::Epoch(Epoch::Microsecond(0)))
                    }
                    time::MILLISECOND_RESOLUTION | time::MILLI_RESOLUTION | time::MS_RESOLUTION => {
                        Ok(Value::Epoch(Epoch::Millisecond(0)))
                    }
                    time::SECOND_RESOLUTION | time::SEC_RESOLUTION | time::S_RESOLUTION => {
                        Ok(Value::Epoch(Epoch::Second(0)))
                    }
                    _ => Err(format!(
                        "invalid resolution: '{resolution}'. Available resolutions: {}",
                        time::VALID_RESOLUTIONS.join(",")
                    )),
                },
                _ => Err(format!(
                    "resolution MUST BE set for epoch type: '{t}'. Available resolutions: {}",
                    time::VALID_RESOLUTIONS.join(", ")
                )),
            },

            "array" => Ok(Value::Array(Array::default())),
            "map" => Ok(Value::Map(Map::default())),

            _ => Err(format!("failed to parse type: '{t}'")),
        }
    }

    /// only support string, bool, number, null
    pub fn parse_str_value(&self, v: &str) -> Result<Self, String> {
        match self {
            Value::Int8(_) => v
                .parse::<i8>()
                .map(Value::Int8)
                .map_err(|e| format!("failed to parse int8: {}", e)),
            Value::Int16(_) => v
                .parse::<i16>()
                .map(Value::Int16)
                .map_err(|e| format!("failed to parse int16: {}", e)),
            Value::Int32(_) => v
                .parse::<i32>()
                .map(Value::Int32)
                .map_err(|e| format!("failed to parse int32: {}", e)),
            Value::Int64(_) => v
                .parse::<i64>()
                .map(Value::Int64)
                .map_err(|e| format!("failed to parse int64: {}", e)),

            Value::Uint8(_) => v
                .parse::<u8>()
                .map(Value::Uint8)
                .map_err(|e| format!("failed to parse uint8: {}", e)),
            Value::Uint16(_) => v
                .parse::<u16>()
                .map(Value::Uint16)
                .map_err(|e| format!("failed to parse uint16: {}", e)),
            Value::Uint32(_) => v
                .parse::<u32>()
                .map(Value::Uint32)
                .map_err(|e| format!("failed to parse uint32: {}", e)),
            Value::Uint64(_) => v
                .parse::<u64>()
                .map(Value::Uint64)
                .map_err(|e| format!("failed to parse uint64: {}", e)),

            Value::Float32(_) => v
                .parse::<f32>()
                .map(Value::Float32)
                .map_err(|e| format!("failed to parse float32: {}", e)),
            Value::Float64(_) => v
                .parse::<f64>()
                .map(Value::Float64)
                .map_err(|e| format!("failed to parse float64: {}", e)),

            Value::Boolean(_) => v
                .parse::<bool>()
                .map(Value::Boolean)
                .map_err(|e| format!("failed to parse bool: {}", e)),
            Value::String(_) => Ok(Value::String(v.to_string())),

            Value::Null => Ok(Value::Null),

            _ => Err(format!("default value not unsupported for type {}", self)),
        }
    }

    /// only support string, bool, number, null
    pub fn to_str_value(&self) -> String {
        match self {
            Value::Int8(v) => format!("{}", v),
            Value::Int16(v) => format!("{}", v),
            Value::Int32(v) => format!("{}", v),
            Value::Int64(v) => format!("{}", v),

            Value::Uint8(v) => format!("{}", v),
            Value::Uint16(v) => format!("{}", v),
            Value::Uint32(v) => format!("{}", v),
            Value::Uint64(v) => format!("{}", v),

            Value::Float32(v) => format!("{}", v),
            Value::Float64(v) => format!("{}", v),

            Value::Boolean(v) => format!("{}", v),
            Value::String(v) => v.to_string(),

            v => v.to_string(),
        }
    }

    pub fn to_str_type(&self) -> &str {
        match self {
            Value::Int8(_) => "int8",
            Value::Int16(_) => "int16",
            Value::Int32(_) => "int32",
            Value::Int64(_) => "int64",

            Value::Uint8(_) => "uint8",
            Value::Uint16(_) => "uint16",
            Value::Uint32(_) => "uint32",
            Value::Uint64(_) => "uint64",

            Value::Float32(_) => "float32",
            Value::Float64(_) => "float64",

            Value::Boolean(_) => "boolean",
            Value::String(_) => "string",

            Value::Time(_) => "time",
            Value::Epoch(_) => "epoch",

            Value::Array(_) => "array",
            Value::Map(_) => "map",

            Value::Null => "null",
        }
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let str = match self {
            Value::Null => "null".to_string(),

            Value::Int8(v) => format!("int8({})", v),
            Value::Int16(v) => format!("int16({})", v),
            Value::Int32(v) => format!("int32({})", v),
            Value::Int64(v) => format!("int64({})", v),

            Value::Uint8(v) => format!("uint8({})", v),
            Value::Uint16(v) => format!("uint16({})", v),
            Value::Uint32(v) => format!("uint32({})", v),
            Value::Uint64(v) => format!("uint64({})", v),

            Value::Float32(v) => format!("float32({})", v),
            Value::Float64(v) => format!("float64({})", v),

            Value::Boolean(v) => format!("boolean({})", v),
            Value::String(v) => format!("string({})", v),

            Value::Time(v) => format!("time({})", v),
            Value::Epoch(v) => format!("epoch({})", v),

            Value::Array(v) => format!("{}", v),
            Value::Map(v) => format!("{}", v),
        };

        write!(f, "{}", str)
    }
}

impl TryFrom<serde_json::Value> for Value {
    type Error = String;

    fn try_from(v: serde_json::Value) -> Result<Self, Self::Error> {
        match v {
            serde_json::Value::Null => Ok(Value::Null),
            serde_json::Value::Bool(v) => Ok(Value::Boolean(v)),
            serde_json::Value::Number(v) => {
                if let Some(v) = v.as_i64() {
                    Ok(Value::Int64(v))
                } else if let Some(v) = v.as_u64() {
                    Ok(Value::Uint64(v))
                } else if let Some(v) = v.as_f64() {
                    Ok(Value::Float64(v))
                } else {
                    Err(format!("unsupported number type: {}", v))
                }
            }
            serde_json::Value::String(v) => Ok(Value::String(v)),
            serde_json::Value::Array(v) => {
                let mut values = Vec::with_capacity(v.len());
                for v in v {
                    values.push(Value::try_from(v)?);
                }
                Ok(Value::Array(Array { values }))
            }
            serde_json::Value::Object(v) => {
                let mut values = HashMap::with_capacity(v.len());
                for (k, v) in v {
                    values.insert(k, Value::try_from(v)?);
                }
                Ok(Value::Map(Map { values }))
            }
        }
    }
}

impl TryFrom<&yaml_rust::Yaml> for Value {
    type Error = String;

    fn try_from(v: &yaml_rust::Yaml) -> Result<Self, Self::Error> {
        match v {
            yaml_rust::Yaml::Null => Ok(Value::Null),
            yaml_rust::Yaml::Boolean(v) => Ok(Value::Boolean(*v)),
            yaml_rust::Yaml::Integer(v) => Ok(Value::Int64(*v)),
            yaml_rust::Yaml::Real(v) => {
                if let Ok(v) = v.parse() {
                    Ok(Value::Float64(v))
                } else {
                    Err(format!("failed to parse float64: {}", v))
                }
            }
            yaml_rust::Yaml::String(v) => Ok(Value::String(v.to_string())),
            yaml_rust::Yaml::Array(arr) => {
                let mut values = vec![];
                for v in arr {
                    values.push(Value::try_from(v)?);
                }
                Ok(Value::Array(Array { values }))
            }
            yaml_rust::Yaml::Hash(v) => {
                let mut values = HashMap::new();
                for (k, v) in v {
                    let key = k
                        .as_str()
                        .ok_or(format!("key in Hash must be a string, but got {v:?}"))?;
                    values.insert(key.to_string(), Value::try_from(v)?);
                }
                Ok(Value::Map(Map { values }))
            }
            _ => Err(format!("unsupported yaml type: {v:?}")),
        }
    }
}
