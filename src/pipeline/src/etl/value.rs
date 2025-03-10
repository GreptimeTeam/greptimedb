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

use std::result::Result as StdResult;

pub use array::Array;
use jsonb::{Number as JsonbNumber, Object as JsonbObject, Value as JsonbValue};
use jsonpath_rust::parser::{parse_json_path, JsonPathIndex};
use jsonpath_rust::path::{JsonLike, Path};
use jsonpath_rust::{jsp_idx, jsp_obj, JsonPath, JsonPathParserError, JsonPathStr};
pub use map::Map;
use regex::Regex;
use snafu::{OptionExt, ResultExt};
pub use time::Timestamp;

use super::error::{
    ValueDefaultValueUnsupportedSnafu, ValueInvalidResolutionSnafu, ValueParseBooleanSnafu,
    ValueParseFloatSnafu, ValueParseIntSnafu, ValueParseTypeSnafu, ValueUnsupportedNumberTypeSnafu,
    ValueUnsupportedYamlTypeSnafu, ValueYamlKeyMustBeStringSnafu,
};
use super::PipelineMap;
use crate::etl::error::{Error, Result};

/// Value can be used as type
/// acts as value: the enclosed value is the actual value
/// acts as type: the enclosed value is the default value

#[derive(Debug, Clone, PartialEq, Default)]
pub enum Value {
    // as value: null
    // as type: no type specified
    #[default]
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

    Timestamp(Timestamp),

    /// We only consider object and array to be json types.
    Array(Array),
    Map(Map),
}

impl Value {
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    pub fn parse_str_type(t: &str) -> Result<Self> {
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

            "timestamp" | "epoch" | "time" => match tail {
                Some(resolution) if !resolution.is_empty() => match resolution.as_str() {
                    time::NANOSECOND_RESOLUTION | time::NANO_RESOLUTION | time::NS_RESOLUTION => {
                        Ok(Value::Timestamp(Timestamp::Nanosecond(0)))
                    }
                    time::MICROSECOND_RESOLUTION | time::MICRO_RESOLUTION | time::US_RESOLUTION => {
                        Ok(Value::Timestamp(Timestamp::Microsecond(0)))
                    }
                    time::MILLISECOND_RESOLUTION | time::MILLI_RESOLUTION | time::MS_RESOLUTION => {
                        Ok(Value::Timestamp(Timestamp::Millisecond(0)))
                    }
                    time::SECOND_RESOLUTION | time::SEC_RESOLUTION | time::S_RESOLUTION => {
                        Ok(Value::Timestamp(Timestamp::Second(0)))
                    }
                    _ => ValueInvalidResolutionSnafu {
                        resolution,
                        valid_resolution: time::VALID_RESOLUTIONS.join(","),
                    }
                    .fail(),
                },
                _ => Ok(Value::Timestamp(Timestamp::Nanosecond(0))),
            },

            // We only consider object and array to be json types. and use Map to represent json
            // TODO(qtang): Needs to be defined with better semantics
            "json" => Ok(Value::Map(Map::default())),

            _ => ValueParseTypeSnafu { t }.fail(),
        }
    }

    /// only support string, bool, number, null
    pub fn parse_str_value(&self, v: &str) -> Result<Self> {
        match self {
            Value::Int8(_) => v
                .parse::<i8>()
                .map(Value::Int8)
                .context(ValueParseIntSnafu { ty: "int8", v }),
            Value::Int16(_) => v
                .parse::<i16>()
                .map(Value::Int16)
                .context(ValueParseIntSnafu { ty: "int16", v }),
            Value::Int32(_) => v
                .parse::<i32>()
                .map(Value::Int32)
                .context(ValueParseIntSnafu { ty: "int32", v }),
            Value::Int64(_) => v
                .parse::<i64>()
                .map(Value::Int64)
                .context(ValueParseIntSnafu { ty: "int64", v }),

            Value::Uint8(_) => v
                .parse::<u8>()
                .map(Value::Uint8)
                .context(ValueParseIntSnafu { ty: "uint8", v }),
            Value::Uint16(_) => v
                .parse::<u16>()
                .map(Value::Uint16)
                .context(ValueParseIntSnafu { ty: "uint16", v }),
            Value::Uint32(_) => v
                .parse::<u32>()
                .map(Value::Uint32)
                .context(ValueParseIntSnafu { ty: "uint32", v }),
            Value::Uint64(_) => v
                .parse::<u64>()
                .map(Value::Uint64)
                .context(ValueParseIntSnafu { ty: "uint64", v }),

            Value::Float32(_) => v
                .parse::<f32>()
                .map(Value::Float32)
                .context(ValueParseFloatSnafu { ty: "float32", v }),
            Value::Float64(_) => v
                .parse::<f64>()
                .map(Value::Float64)
                .context(ValueParseFloatSnafu { ty: "float64", v }),

            Value::Boolean(_) => v
                .parse::<bool>()
                .map(Value::Boolean)
                .context(ValueParseBooleanSnafu { ty: "boolean", v }),
            Value::String(_) => Ok(Value::String(v.to_string())),

            Value::Null => Ok(Value::Null),

            _ => ValueDefaultValueUnsupportedSnafu {
                value: format!("{:?}", self),
            }
            .fail(),
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

            Value::Timestamp(_) => "epoch",

            Value::Array(_) | Value::Map(_) => "json",

            Value::Null => "null",
        }
    }

    pub fn get(&self, key: &str) -> Option<&Self> {
        match self {
            Value::Map(map) => map.get(key),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::String(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Uint32(v) => Some(*v as i64),
            Value::Uint16(v) => Some(*v as i64),
            Value::Uint8(v) => Some(*v as i64),
            Value::Int64(v) => Some(*v),
            Value::Int32(v) => Some(*v as i64),
            Value::Int16(v) => Some(*v as i64),
            Value::Int8(v) => Some(*v as i64),
            _ => None,
        }
    }

    pub fn as_u64(&self) -> Option<u64> {
        match self {
            Value::Uint64(v) => Some(*v),
            Value::Uint32(v) => Some(*v as u64),
            Value::Uint16(v) => Some(*v as u64),
            Value::Uint8(v) => Some(*v as u64),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Float32(v) => Some(*v as f64),
            Value::Float64(v) => Some(*v),
            Value::Uint64(v) => Some(*v as f64),
            Value::Uint32(v) => Some(*v as f64),
            Value::Uint16(v) => Some(*v as f64),
            Value::Uint8(v) => Some(*v as f64),
            Value::Int64(v) => Some(*v as f64),
            Value::Int32(v) => Some(*v as f64),
            Value::Int16(v) => Some(*v as f64),
            Value::Int8(v) => Some(*v as f64),
            _ => None,
        }
    }

    // ref https://github.com/serde-rs/json/blob/master/src/value/mod.rs#L779
    pub fn pointer(&self, pointer: &str) -> Option<&Value> {
        if pointer.is_empty() {
            return Some(self);
        }
        if !pointer.starts_with('/') {
            return None;
        }
        pointer
            .split('/')
            .skip(1)
            .map(|x| x.replace("~1", "/").replace("~0", "~"))
            .try_fold(self, |target, token| match target {
                Value::Map(map) => map.get(&token),
                Value::Array(list) => parse_index(&token).and_then(|x| list.get(x)),
                _ => None,
            })
    }

    // ref https://github.com/serde-rs/json/blob/master/src/value/mod.rs#L834
    pub fn pointer_mut(&mut self, pointer: &str) -> Option<&mut Value> {
        if pointer.is_empty() {
            return Some(self);
        }
        if !pointer.starts_with('/') {
            return None;
        }
        pointer
            .split('/')
            .skip(1)
            .map(|x| x.replace("~1", "/").replace("~0", "~"))
            .try_fold(self, |target, token| match target {
                Value::Map(map) => map.get_mut(&token),
                Value::Array(list) => parse_index(&token).and_then(move |x| list.get_mut(x)),
                _ => None,
            })
    }
}

// ref https://github.com/serde-rs/json/blob/master/src/value/mod.rs#L259
fn parse_index(s: &str) -> Option<usize> {
    if s.starts_with('+') || (s.starts_with('0') && s.len() != 1) {
        return None;
    }
    s.parse().ok()
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

            Value::Timestamp(v) => format!("epoch({})", v),

            Value::Array(v) => format!("{}", v),
            Value::Map(v) => format!("{}", v),
        };

        write!(f, "{}", str)
    }
}

impl TryFrom<serde_json::Value> for Value {
    type Error = Error;

    fn try_from(v: serde_json::Value) -> Result<Self> {
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
                    ValueUnsupportedNumberTypeSnafu { value: v }.fail()
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
                let mut values = PipelineMap::new();
                for (k, v) in v {
                    values.insert(k, Value::try_from(v)?);
                }
                Ok(Value::Map(Map { values }))
            }
        }
    }
}

impl TryFrom<&yaml_rust::Yaml> for Value {
    type Error = Error;

    fn try_from(v: &yaml_rust::Yaml) -> Result<Self> {
        match v {
            yaml_rust::Yaml::Null => Ok(Value::Null),
            yaml_rust::Yaml::Boolean(v) => Ok(Value::Boolean(*v)),
            yaml_rust::Yaml::Integer(v) => Ok(Value::Int64(*v)),
            yaml_rust::Yaml::Real(v) => match v.parse::<f64>() {
                Ok(v) => Ok(Value::Float64(v)),
                Err(e) => Err(e).context(ValueParseFloatSnafu { ty: "float64", v }),
            },
            yaml_rust::Yaml::String(v) => Ok(Value::String(v.to_string())),
            yaml_rust::Yaml::Array(arr) => {
                let mut values = vec![];
                for v in arr {
                    values.push(Value::try_from(v)?);
                }
                Ok(Value::Array(Array { values }))
            }
            yaml_rust::Yaml::Hash(v) => {
                let mut values = PipelineMap::new();
                for (k, v) in v {
                    let key = k
                        .as_str()
                        .with_context(|| ValueYamlKeyMustBeStringSnafu { value: v.clone() })?;
                    values.insert(key.to_string(), Value::try_from(v)?);
                }
                Ok(Value::Map(Map { values }))
            }
            _ => ValueUnsupportedYamlTypeSnafu { value: v.clone() }.fail(),
        }
    }
}

impl From<&Value> for JsonbValue<'_> {
    fn from(value: &Value) -> Self {
        match value {
            Value::Null => JsonbValue::Null,
            Value::Boolean(v) => JsonbValue::Bool(*v),

            Value::Int8(v) => JsonbValue::Number(JsonbNumber::Int64(*v as i64)),
            Value::Int16(v) => JsonbValue::Number(JsonbNumber::Int64(*v as i64)),
            Value::Int32(v) => JsonbValue::Number(JsonbNumber::Int64(*v as i64)),
            Value::Int64(v) => JsonbValue::Number(JsonbNumber::Int64(*v)),

            Value::Uint8(v) => JsonbValue::Number(JsonbNumber::UInt64(*v as u64)),
            Value::Uint16(v) => JsonbValue::Number(JsonbNumber::UInt64(*v as u64)),
            Value::Uint32(v) => JsonbValue::Number(JsonbNumber::UInt64(*v as u64)),
            Value::Uint64(v) => JsonbValue::Number(JsonbNumber::UInt64(*v)),
            Value::Float32(v) => JsonbValue::Number(JsonbNumber::Float64(*v as f64)),
            Value::Float64(v) => JsonbValue::Number(JsonbNumber::Float64(*v)),
            Value::String(v) => JsonbValue::String(v.clone().into()),
            Value::Timestamp(v) => JsonbValue::String(v.to_string().into()),
            Value::Array(arr) => {
                let mut vals: Vec<JsonbValue> = Vec::with_capacity(arr.len());
                for val in arr.iter() {
                    vals.push(val.into());
                }
                JsonbValue::Array(vals)
            }
            Value::Map(obj) => {
                let mut map = JsonbObject::new();
                for (k, v) in obj.iter() {
                    let val: JsonbValue = v.into();
                    map.insert(k.to_string(), val);
                }
                JsonbValue::Object(map)
            }
        }
    }
}

impl From<Value> for JsonbValue<'_> {
    fn from(value: Value) -> Self {
        match value {
            Value::Null => JsonbValue::Null,
            Value::Boolean(v) => JsonbValue::Bool(v),

            Value::Int8(v) => JsonbValue::Number(JsonbNumber::Int64(v as i64)),
            Value::Int16(v) => JsonbValue::Number(JsonbNumber::Int64(v as i64)),
            Value::Int32(v) => JsonbValue::Number(JsonbNumber::Int64(v as i64)),
            Value::Int64(v) => JsonbValue::Number(JsonbNumber::Int64(v)),

            Value::Uint8(v) => JsonbValue::Number(JsonbNumber::UInt64(v as u64)),
            Value::Uint16(v) => JsonbValue::Number(JsonbNumber::UInt64(v as u64)),
            Value::Uint32(v) => JsonbValue::Number(JsonbNumber::UInt64(v as u64)),
            Value::Uint64(v) => JsonbValue::Number(JsonbNumber::UInt64(v)),
            Value::Float32(v) => JsonbValue::Number(JsonbNumber::Float64(v as f64)),
            Value::Float64(v) => JsonbValue::Number(JsonbNumber::Float64(v)),
            Value::String(v) => JsonbValue::String(v.into()),
            Value::Timestamp(v) => JsonbValue::String(v.to_string().into()),
            Value::Array(arr) => {
                let mut vals: Vec<JsonbValue> = Vec::with_capacity(arr.len());
                for val in arr.into_iter() {
                    vals.push(val.into());
                }
                JsonbValue::Array(vals)
            }
            Value::Map(obj) => {
                let mut map = JsonbObject::new();
                for (k, v) in obj.values.into_iter() {
                    let val: JsonbValue = v.into();
                    map.insert(k, val);
                }
                JsonbValue::Object(map)
            }
        }
    }
}

impl From<String> for Value {
    fn from(value: String) -> Self {
        Value::String(value)
    }
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Value::String(value.to_string())
    }
}

impl From<i64> for Value {
    fn from(value: i64) -> Self {
        Value::Int64(value)
    }
}

impl From<f64> for Value {
    fn from(value: f64) -> Self {
        Value::Float64(value)
    }
}

impl From<Vec<String>> for Value {
    fn from(value: Vec<String>) -> Self {
        Value::Array(Array {
            values: value.into_iter().map(Value::String).collect(),
        })
    }
}

impl From<Vec<Self>> for Value {
    fn from(value: Vec<Self>) -> Self {
        Value::Array(Array { values: value })
    }
}

impl From<bool> for Value {
    fn from(value: bool) -> Self {
        Value::Boolean(value)
    }
}

impl JsonLike for Value {
    fn get(&self, key: &str) -> Option<&Self> {
        self.get(key)
    }

    fn itre(&self, pref: String) -> Vec<jsonpath_rust::JsonPathValue<Self>> {
        let res = match self {
            Value::Array(elems) => {
                let mut res = vec![];
                for (idx, el) in elems.iter().enumerate() {
                    res.push(jsonpath_rust::JsonPathValue::Slice(
                        el,
                        jsonpath_rust::jsp_idx(&pref, idx),
                    ));
                }
                res
            }
            Value::Map(elems) => {
                let mut res = vec![];
                for (key, el) in elems.iter() {
                    res.push(jsonpath_rust::JsonPathValue::Slice(
                        el,
                        jsonpath_rust::jsp_obj(&pref, key),
                    ));
                }
                res
            }
            _ => vec![],
        };
        if res.is_empty() {
            vec![jsonpath_rust::JsonPathValue::NoValue]
        } else {
            res
        }
    }

    fn array_len(&self) -> jsonpath_rust::JsonPathValue<'static, Self> {
        match self {
            Value::Array(elems) => {
                jsonpath_rust::JsonPathValue::NewValue(Value::Int64(elems.len() as i64))
            }
            _ => jsonpath_rust::JsonPathValue::NoValue,
        }
    }

    fn init_with_usize(cnt: usize) -> Self {
        Value::Int64(cnt as i64)
    }

    fn deep_flatten(&self, pref: String) -> Vec<(&Self, String)> {
        let mut acc = vec![];
        match self {
            Value::Map(elems) => {
                for (f, v) in elems.iter() {
                    let pref = jsp_obj(&pref, f);
                    acc.push((v, pref.clone()));
                    acc.append(&mut v.deep_flatten(pref));
                }
            }
            Value::Array(elems) => {
                for (i, v) in elems.iter().enumerate() {
                    let pref = jsp_idx(&pref, i);
                    acc.push((v, pref.clone()));
                    acc.append(&mut v.deep_flatten(pref));
                }
            }
            _ => (),
        }
        acc
    }

    fn deep_path_by_key<'a>(
        &'a self,
        key: jsonpath_rust::path::ObjectField<'a, Self>,
        pref: String,
    ) -> Vec<(&'a Self, String)> {
        let mut result: Vec<(&'a Value, String)> = jsonpath_rust::JsonPathValue::vec_as_pair(
            key.find(jsonpath_rust::JsonPathValue::new_slice(self, pref.clone())),
        );
        match self {
            Value::Map(elems) => {
                let mut next_levels: Vec<(&'a Value, String)> = elems
                    .iter()
                    .flat_map(|(k, v)| v.deep_path_by_key(key.clone(), jsp_obj(&pref, k)))
                    .collect();
                result.append(&mut next_levels);
                result
            }
            Value::Array(elems) => {
                let mut next_levels: Vec<(&'a Value, String)> = elems
                    .iter()
                    .enumerate()
                    .flat_map(|(i, v)| v.deep_path_by_key(key.clone(), jsp_idx(&pref, i)))
                    .collect();
                result.append(&mut next_levels);
                result
            }
            _ => result,
        }
    }

    fn as_u64(&self) -> Option<u64> {
        match self {
            Value::Uint64(v) => Some(*v),
            Value::Uint32(v) => Some(*v as u64),
            Value::Uint16(v) => Some(*v as u64),
            Value::Uint8(v) => Some(*v as u64),
            Value::Int64(v) if *v >= 0 => Some(*v as u64),
            Value::Int32(v) if *v >= 0 => Some(*v as u64),
            Value::Int16(v) if *v >= 0 => Some(*v as u64),
            Value::Int8(v) if *v >= 0 => Some(*v as u64),
            Value::Float64(v) if *v >= 0.0 => Some(*v as u64),
            Value::Float32(v) if *v >= 0.0 => Some(*v as u64),
            _ => None,
        }
    }

    fn is_array(&self) -> bool {
        matches!(self, Value::Array(_))
    }

    fn as_array(&self) -> Option<&Vec<Self>> {
        match self {
            Value::Array(arr) => Some(&arr.values),
            _ => None,
        }
    }

    fn size(left: Vec<&Self>, right: Vec<&Self>) -> bool {
        if let Some(v) = right.first() {
            let sz = match v {
                Value::Int64(n) => *n as usize,
                Value::Int32(n) => *n as usize,
                Value::Int16(n) => *n as usize,
                Value::Int8(n) => *n as usize,

                Value::Uint64(n) => *n as usize,
                Value::Uint32(n) => *n as usize,
                Value::Uint16(n) => *n as usize,
                Value::Uint8(n) => *n as usize,
                Value::Float32(n) => *n as usize,
                Value::Float64(n) => *n as usize,
                _ => return false,
            };
            for el in left.iter() {
                match el {
                    Value::String(v) if v.len() == sz => true,
                    Value::Array(elems) if elems.len() == sz => true,
                    Value::Map(fields) if fields.len() == sz => true,
                    _ => return false,
                };
            }
            return true;
        }
        false
    }

    fn sub_set_of(left: Vec<&Self>, right: Vec<&Self>) -> bool {
        if left.is_empty() {
            return true;
        }
        if right.is_empty() {
            return false;
        }

        if let Some(elems) = left.first().and_then(|e| e.as_array()) {
            if let Some(Value::Array(right_elems)) = right.first() {
                if right_elems.is_empty() {
                    return false;
                }

                for el in elems {
                    let mut res = false;

                    for r in right_elems.iter() {
                        if el.eq(r) {
                            res = true
                        }
                    }
                    if !res {
                        return false;
                    }
                }
                return true;
            }
        }
        false
    }

    fn any_of(left: Vec<&Self>, right: Vec<&Self>) -> bool {
        if left.is_empty() {
            return true;
        }
        if right.is_empty() {
            return false;
        }

        if let Some(Value::Array(elems)) = right.first() {
            if elems.is_empty() {
                return false;
            }

            for el in left.iter() {
                if let Some(left_elems) = el.as_array() {
                    for l in left_elems.iter() {
                        for r in elems.iter() {
                            if l.eq(r) {
                                return true;
                            }
                        }
                    }
                } else {
                    for r in elems.iter() {
                        if el.eq(&r) {
                            return true;
                        }
                    }
                }
            }
        }

        false
    }

    fn regex(left: Vec<&Self>, right: Vec<&Self>) -> bool {
        if left.is_empty() || right.is_empty() {
            return false;
        }

        match right.first() {
            Some(Value::String(str)) => {
                if let Ok(regex) = Regex::new(str) {
                    for el in left.iter() {
                        if let Some(v) = el.as_str() {
                            if regex.is_match(v) {
                                return true;
                            }
                        }
                    }
                }
                false
            }
            _ => false,
        }
    }

    fn inside(left: Vec<&Self>, right: Vec<&Self>) -> bool {
        if left.is_empty() {
            return false;
        }

        match right.first() {
            Some(Value::Array(elems)) => {
                for el in left.iter() {
                    if elems.contains(el) {
                        return true;
                    }
                }
                false
            }
            Some(Value::Map(elems)) => {
                for el in left.iter() {
                    for r in elems.values() {
                        if el.eq(&r) {
                            return true;
                        }
                    }
                }
                false
            }
            _ => false,
        }
    }

    fn less(left: Vec<&Self>, right: Vec<&Self>) -> bool {
        if left.len() == 1 && right.len() == 1 {
            match (left.first(), right.first()) {
                (Some(l), Some(r)) => l
                    .as_f64()
                    .and_then(|v1| r.as_f64().map(|v2| v1 < v2))
                    .unwrap_or(false),
                _ => false,
            }
        } else {
            false
        }
    }

    fn eq(left: Vec<&Self>, right: Vec<&Self>) -> bool {
        if left.len() != right.len() {
            false
        } else {
            left.iter().zip(right).all(|(a, b)| a.eq(&b))
        }
    }

    fn array(data: Vec<Self>) -> Self {
        Value::Array(Array { values: data })
    }

    fn null() -> Self {
        Value::Null
    }

    // ref https://github.com/besok/jsonpath-rust/blob/main/src/path/mod.rs#L423
    fn reference<T>(
        &self,
        path: T,
    ) -> std::result::Result<std::option::Option<&Value>, JsonPathParserError>
    where
        T: Into<JsonPathStr>,
    {
        Ok(self.pointer(&path_to_json_path(path.into())?))
    }

    // https://github.com/besok/jsonpath-rust/blob/main/src/path/mod.rs#L430
    fn reference_mut<T>(
        &mut self,
        path: T,
    ) -> std::result::Result<std::option::Option<&mut Value>, JsonPathParserError>
    where
        T: Into<JsonPathStr>,
    {
        Ok(self.pointer_mut(&path_to_json_path(path.into())?))
    }
}

// ref https://github.com/besok/jsonpath-rust/blob/main/src/path/mod.rs#L438
fn path_to_json_path(path: JsonPathStr) -> StdResult<String, JsonPathParserError> {
    convert_part(&parse_json_path(path.as_str())?)
}

// https://github.com/besok/jsonpath-rust/blob/main/src/path/mod.rs#L442
fn convert_part(path: &JsonPath) -> StdResult<String, JsonPathParserError> {
    match path {
        JsonPath::Chain(elems) => elems
            .iter()
            .map(convert_part)
            .collect::<StdResult<String, JsonPathParserError>>(),

        JsonPath::Index(JsonPathIndex::Single(v)) => Ok(format!("/{}", v)),
        JsonPath::Field(e) => Ok(format!("/{}", e)),
        JsonPath::Root => Ok("".to_string()),
        e => Err(JsonPathParserError::InvalidJsonPath(e.to_string())),
    }
}
