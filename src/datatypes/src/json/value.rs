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

use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::{Arc, OnceLock};

use num_traits::ToPrimitive;
use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};
use serde_json::Number;

use crate::data_type::ConcreteDataType;
use crate::types::json_type::JsonNativeType;
use crate::types::{JsonType, StructField, StructType};
use crate::value::{ListValue, ListValueRef, StructValue, StructValueRef, Value, ValueRef};

/// Number in json, can be a positive integer, a negative integer, or a floating number.
/// Each of which is represented as `u64`, `i64` and `f64`.
///
/// This follows how `serde_json` designs number.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum JsonNumber {
    PosInt(u64),
    NegInt(i64),
    Float(OrderedFloat<f64>),
}

impl JsonNumber {
    fn as_u64(&self) -> Option<u64> {
        match self {
            JsonNumber::PosInt(n) => Some(*n),
            JsonNumber::NegInt(n) => (*n >= 0).then_some(*n as u64),
            _ => None,
        }
    }

    fn as_i64(&self) -> Option<i64> {
        match self {
            JsonNumber::PosInt(n) => (*n <= i64::MAX as u64).then_some(*n as i64),
            JsonNumber::NegInt(n) => Some(*n),
            _ => None,
        }
    }

    fn as_f64(&self) -> f64 {
        match self {
            JsonNumber::PosInt(n) => *n as f64,
            JsonNumber::NegInt(n) => *n as f64,
            JsonNumber::Float(n) => n.0,
        }
    }
}

impl From<u64> for JsonNumber {
    fn from(i: u64) -> Self {
        Self::PosInt(i)
    }
}

impl From<i64> for JsonNumber {
    fn from(n: i64) -> Self {
        Self::NegInt(n)
    }
}

impl From<f64> for JsonNumber {
    fn from(i: f64) -> Self {
        Self::Float(i.into())
    }
}

impl Display for JsonNumber {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PosInt(x) => write!(f, "{x}"),
            Self::NegInt(x) => write!(f, "{x}"),
            Self::Float(x) => write!(f, "{x}"),
        }
    }
}

/// Variants of json.
///
/// This follows how [serde_json::Value] designs except that we only choose to use [BTreeMap] to
/// preserve the fields order by their names in the json object. (By default `serde_json` uses
/// [BTreeMap], too. But it additionally supports "IndexMap" which preserves the order by insertion
/// times of fields.)
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum JsonVariant {
    Null,
    Bool(bool),
    Number(JsonNumber),
    String(String),
    Array(Vec<JsonVariant>),
    Object(BTreeMap<String, JsonVariant>),
}

impl JsonVariant {
    fn native_type(&self) -> JsonNativeType {
        match self {
            JsonVariant::Null => JsonNativeType::Null,
            JsonVariant::Bool(_) => JsonNativeType::Bool,
            JsonVariant::Number(n) => match n {
                JsonNumber::PosInt(_) => JsonNativeType::u64(),
                JsonNumber::NegInt(_) => JsonNativeType::i64(),
                JsonNumber::Float(_) => JsonNativeType::f64(),
            },
            JsonVariant::String(_) => JsonNativeType::String,
            JsonVariant::Array(array) => {
                let item_type = if let Some(first) = array.first() {
                    first.native_type()
                } else {
                    JsonNativeType::Null
                };
                JsonNativeType::Array(Box::new(item_type))
            }
            JsonVariant::Object(object) => JsonNativeType::Object(
                object
                    .iter()
                    .map(|(k, v)| (k.clone(), v.native_type()))
                    .collect(),
            ),
        }
    }

    fn json_type(&self) -> JsonType {
        JsonType::new_native(self.native_type())
    }

    fn as_ref(&self) -> JsonVariantRef<'_> {
        match self {
            JsonVariant::Null => JsonVariantRef::Null,
            JsonVariant::Bool(x) => (*x).into(),
            JsonVariant::Number(x) => match x {
                JsonNumber::PosInt(i) => (*i).into(),
                JsonNumber::NegInt(i) => (*i).into(),
                JsonNumber::Float(f) => (f.0).into(),
            },
            JsonVariant::String(x) => x.as_str().into(),
            JsonVariant::Array(array) => {
                JsonVariantRef::Array(array.iter().map(|x| x.as_ref()).collect())
            }
            JsonVariant::Object(object) => JsonVariantRef::Object(
                object
                    .iter()
                    .map(|(k, v)| (k.as_str(), v.as_ref()))
                    .collect(),
            ),
        }
    }
}

impl From<()> for JsonVariant {
    fn from(_: ()) -> Self {
        Self::Null
    }
}

impl From<bool> for JsonVariant {
    fn from(v: bool) -> Self {
        Self::Bool(v)
    }
}

impl<T: Into<JsonNumber>> From<T> for JsonVariant {
    fn from(v: T) -> Self {
        Self::Number(v.into())
    }
}

impl From<&str> for JsonVariant {
    fn from(v: &str) -> Self {
        Self::String(v.to_string())
    }
}

impl From<String> for JsonVariant {
    fn from(v: String) -> Self {
        Self::String(v)
    }
}

impl<const N: usize, T: Into<JsonVariant>> From<[T; N]> for JsonVariant {
    fn from(vs: [T; N]) -> Self {
        Self::Array(vs.into_iter().map(|x| x.into()).collect())
    }
}

impl<K: Into<String>, V: Into<JsonVariant>, const N: usize> From<[(K, V); N]> for JsonVariant {
    fn from(vs: [(K, V); N]) -> Self {
        Self::Object(vs.into_iter().map(|(k, v)| (k.into(), v.into())).collect())
    }
}

impl Display for JsonVariant {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Null => write!(f, "null"),
            Self::Bool(x) => write!(f, "{x}"),
            Self::Number(x) => write!(f, "{x}"),
            Self::String(x) => write!(f, "{x}"),
            Self::Array(array) => write!(
                f,
                "[{}]",
                array
                    .iter()
                    .map(|x| x.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            Self::Object(object) => {
                write!(
                    f,
                    "{{ {} }}",
                    object
                        .iter()
                        .map(|(k, v)| format!("{k}: {v}"))
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }
        }
    }
}

/// Represents any valid JSON value.
#[derive(Debug, Eq, Serialize, Deserialize)]
pub struct JsonValue {
    #[serde(skip)]
    json_type: OnceLock<JsonType>,
    json_variant: JsonVariant,
}

impl JsonValue {
    pub fn null() -> Self {
        ().into()
    }

    pub(crate) fn new(json_variant: JsonVariant) -> Self {
        Self {
            json_type: OnceLock::new(),
            json_variant,
        }
    }

    pub(crate) fn data_type(&self) -> ConcreteDataType {
        ConcreteDataType::Json(self.json_type().clone())
    }

    pub fn json_type(&self) -> &JsonType {
        self.json_type.get_or_init(|| self.json_variant.json_type())
    }

    pub(crate) fn is_null(&self) -> bool {
        matches!(self.json_variant, JsonVariant::Null)
    }

    /// Check if this JSON value is an empty object.
    pub fn is_empty_object(&self) -> bool {
        match &self.json_variant {
            JsonVariant::Object(object) => object.is_empty(),
            _ => false,
        }
    }

    pub(crate) fn as_i64(&self) -> Option<i64> {
        match self.json_variant {
            JsonVariant::Number(n) => n.as_i64(),
            _ => None,
        }
    }

    pub(crate) fn as_u64(&self) -> Option<u64> {
        match self.json_variant {
            JsonVariant::Number(n) => n.as_u64(),
            _ => None,
        }
    }

    pub(crate) fn as_f64(&self) -> Option<f64> {
        match self.json_variant {
            JsonVariant::Number(n) => Some(n.as_f64()),
            _ => None,
        }
    }

    pub(crate) fn as_f64_lossy(&self) -> Option<f64> {
        match self.json_variant {
            JsonVariant::Number(n) => Some(match n {
                JsonNumber::PosInt(i) => i as f64,
                JsonNumber::NegInt(i) => i as f64,
                JsonNumber::Float(f) => f.0,
            }),
            _ => None,
        }
    }

    pub(crate) fn as_bool(&self) -> Option<bool> {
        match self.json_variant {
            JsonVariant::Bool(b) => Some(b),
            _ => None,
        }
    }

    pub fn as_ref(&self) -> JsonValueRef<'_> {
        JsonValueRef {
            json_type: OnceLock::new(),
            json_variant: self.json_variant.as_ref(),
        }
    }

    pub fn into_variant(self) -> JsonVariant {
        self.json_variant
    }

    pub(crate) fn into_value(self) -> Value {
        fn helper(v: JsonVariant) -> Value {
            match v {
                JsonVariant::Null => Value::Null,
                JsonVariant::Bool(x) => Value::Boolean(x),
                JsonVariant::Number(x) => match x {
                    JsonNumber::PosInt(i) => Value::UInt64(i),
                    JsonNumber::NegInt(i) => Value::Int64(i),
                    JsonNumber::Float(f) => Value::Float64(f),
                },
                JsonVariant::String(x) => Value::String(x.into()),
                JsonVariant::Array(array) => {
                    let item_type = if let Some(first) = array.first() {
                        first.native_type()
                    } else {
                        JsonNativeType::Null
                    };
                    Value::List(ListValue::new(
                        array.into_iter().map(helper).collect(),
                        Arc::new((&item_type).into()),
                    ))
                }
                JsonVariant::Object(object) => {
                    let mut fields = Vec::with_capacity(object.len());
                    let mut items = Vec::with_capacity(object.len());
                    for (k, v) in object {
                        fields.push(StructField::new(k, (&v.native_type()).into(), true));
                        items.push(helper(v));
                    }
                    Value::Struct(StructValue::new(items, StructType::new(Arc::new(fields))))
                }
            }
        }
        helper(self.json_variant)
    }
}

impl<T: Into<JsonVariant>> From<T> for JsonValue {
    fn from(v: T) -> Self {
        Self {
            json_type: OnceLock::new(),
            json_variant: v.into(),
        }
    }
}

impl From<JsonValue> for serde_json::Value {
    fn from(v: JsonValue) -> Self {
        fn helper(v: JsonVariant) -> serde_json::Value {
            match v {
                JsonVariant::Null => serde_json::Value::Null,
                JsonVariant::Bool(x) => serde_json::Value::Bool(x),
                JsonVariant::Number(x) => match x {
                    JsonNumber::PosInt(i) => serde_json::Value::Number(i.into()),
                    JsonNumber::NegInt(i) => serde_json::Value::Number(i.into()),
                    JsonNumber::Float(f) => {
                        if let Some(x) = Number::from_f64(f.0) {
                            serde_json::Value::Number(x)
                        } else {
                            serde_json::Value::String("NaN".into())
                        }
                    }
                },
                JsonVariant::String(x) => serde_json::Value::String(x),
                JsonVariant::Array(array) => {
                    serde_json::Value::Array(array.into_iter().map(helper).collect())
                }
                JsonVariant::Object(object) => serde_json::Value::Object(
                    object.into_iter().map(|(k, v)| (k, helper(v))).collect(),
                ),
            }
        }
        helper(v.json_variant)
    }
}

impl Clone for JsonValue {
    fn clone(&self) -> Self {
        let Self {
            json_type: _,
            json_variant,
        } = self;
        Self {
            json_type: OnceLock::new(),
            json_variant: json_variant.clone(),
        }
    }
}

impl PartialEq<JsonValue> for JsonValue {
    fn eq(&self, other: &JsonValue) -> bool {
        let Self {
            json_type: _,
            json_variant,
        } = self;
        json_variant.eq(&other.json_variant)
    }
}

impl Hash for JsonValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let Self {
            json_type: _,
            json_variant,
        } = self;
        json_variant.hash(state);
    }
}

impl Display for JsonValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.json_variant)
    }
}

/// References of variants of json.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum JsonVariantRef<'a> {
    Null,
    Bool(bool),
    Number(JsonNumber),
    String(&'a str),
    Array(Vec<JsonVariantRef<'a>>),
    Object(BTreeMap<&'a str, JsonVariantRef<'a>>),
}

impl JsonVariantRef<'_> {
    fn json_type(&self) -> JsonType {
        fn native_type(v: &JsonVariantRef<'_>) -> JsonNativeType {
            match v {
                JsonVariantRef::Null => JsonNativeType::Null,
                JsonVariantRef::Bool(_) => JsonNativeType::Bool,
                JsonVariantRef::Number(n) => match n {
                    JsonNumber::PosInt(_) => JsonNativeType::u64(),
                    JsonNumber::NegInt(_) => JsonNativeType::i64(),
                    JsonNumber::Float(_) => JsonNativeType::f64(),
                },
                JsonVariantRef::String(_) => JsonNativeType::String,
                JsonVariantRef::Array(array) => {
                    let item_type = if let Some(first) = array.first() {
                        native_type(first)
                    } else {
                        JsonNativeType::Null
                    };
                    JsonNativeType::Array(Box::new(item_type))
                }
                JsonVariantRef::Object(object) => JsonNativeType::Object(
                    object
                        .iter()
                        .map(|(k, v)| (k.to_string(), native_type(v)))
                        .collect(),
                ),
            }
        }
        JsonType::new_native(native_type(self))
    }
}

impl From<()> for JsonVariantRef<'_> {
    fn from(_: ()) -> Self {
        Self::Null
    }
}

impl From<bool> for JsonVariantRef<'_> {
    fn from(v: bool) -> Self {
        Self::Bool(v)
    }
}

impl<T: Into<JsonNumber>> From<T> for JsonVariantRef<'_> {
    fn from(v: T) -> Self {
        Self::Number(v.into())
    }
}

impl<'a> From<&'a str> for JsonVariantRef<'a> {
    fn from(v: &'a str) -> Self {
        Self::String(v)
    }
}

impl<'a, const N: usize, T: Into<JsonVariantRef<'a>>> From<[T; N]> for JsonVariantRef<'a> {
    fn from(vs: [T; N]) -> Self {
        Self::Array(vs.into_iter().map(|x| x.into()).collect())
    }
}

impl<'a, V: Into<JsonVariantRef<'a>>, const N: usize> From<[(&'a str, V); N]>
    for JsonVariantRef<'a>
{
    fn from(vs: [(&'a str, V); N]) -> Self {
        Self::Object(vs.into_iter().map(|(k, v)| (k, v.into())).collect())
    }
}

impl<'a> From<Vec<JsonVariantRef<'a>>> for JsonVariantRef<'a> {
    fn from(v: Vec<JsonVariantRef<'a>>) -> Self {
        Self::Array(v)
    }
}

impl<'a> From<BTreeMap<&'a str, JsonVariantRef<'a>>> for JsonVariantRef<'a> {
    fn from(v: BTreeMap<&'a str, JsonVariantRef<'a>>) -> Self {
        Self::Object(v)
    }
}

impl From<JsonVariantRef<'_>> for JsonVariant {
    fn from(v: JsonVariantRef) -> Self {
        match v {
            JsonVariantRef::Null => Self::Null,
            JsonVariantRef::Bool(x) => Self::Bool(x),
            JsonVariantRef::Number(x) => Self::Number(x),
            JsonVariantRef::String(x) => Self::String(x.to_string()),
            JsonVariantRef::Array(array) => {
                Self::Array(array.into_iter().map(Into::into).collect())
            }
            JsonVariantRef::Object(object) => Self::Object(
                object
                    .into_iter()
                    .map(|(k, v)| (k.to_string(), v.into()))
                    .collect(),
            ),
        }
    }
}

/// Reference to representation of any valid JSON value.
#[derive(Debug, Serialize)]
pub struct JsonValueRef<'a> {
    #[serde(skip)]
    json_type: OnceLock<JsonType>,
    json_variant: JsonVariantRef<'a>,
}

impl<'a> JsonValueRef<'a> {
    pub fn null() -> Self {
        ().into()
    }

    pub(crate) fn data_type(&self) -> ConcreteDataType {
        ConcreteDataType::Json(self.json_type().clone())
    }

    pub(crate) fn json_type(&self) -> &JsonType {
        self.json_type.get_or_init(|| self.json_variant.json_type())
    }

    pub fn into_variant(self) -> JsonVariantRef<'a> {
        self.json_variant
    }

    pub(crate) fn is_null(&self) -> bool {
        matches!(self.json_variant, JsonVariantRef::Null)
    }

    pub fn is_object(&self) -> bool {
        matches!(self.json_variant, JsonVariantRef::Object(_))
    }

    pub(crate) fn as_f32(&self) -> Option<f32> {
        match self.json_variant {
            JsonVariantRef::Number(JsonNumber::Float(f)) => f.to_f32(),
            _ => None,
        }
    }

    pub(crate) fn as_f64(&self) -> Option<f64> {
        match self.json_variant {
            JsonVariantRef::Number(JsonNumber::Float(f)) => Some(f.0),
            _ => None,
        }
    }

    pub fn as_value_ref(&self) -> ValueRef<'_> {
        fn helper<'a>(v: &'a JsonVariantRef) -> ValueRef<'a> {
            match v {
                JsonVariantRef::Null => ValueRef::Null,
                JsonVariantRef::Bool(x) => ValueRef::Boolean(*x),
                JsonVariantRef::Number(x) => match x {
                    JsonNumber::PosInt(i) => ValueRef::UInt64(*i),
                    JsonNumber::NegInt(i) => ValueRef::Int64(*i),
                    JsonNumber::Float(f) => ValueRef::Float64(*f),
                },
                JsonVariantRef::String(x) => ValueRef::String(x),
                JsonVariantRef::Array(array) => {
                    let val = array.iter().map(helper).collect::<Vec<_>>();
                    let item_datatype = if let Some(first) = val.first() {
                        first.data_type()
                    } else {
                        ConcreteDataType::null_datatype()
                    };
                    ValueRef::List(ListValueRef::RefList {
                        val,
                        item_datatype: Arc::new(item_datatype),
                    })
                }
                JsonVariantRef::Object(object) => {
                    let mut fields = Vec::with_capacity(object.len());
                    let mut val = Vec::with_capacity(object.len());
                    for (k, v) in object.iter() {
                        let v = helper(v);
                        fields.push(StructField::new(k.to_string(), v.data_type(), true));
                        val.push(v);
                    }
                    ValueRef::Struct(StructValueRef::RefList {
                        val,
                        fields: StructType::new(Arc::new(fields)),
                    })
                }
            }
        }
        helper(&self.json_variant)
    }

    pub(crate) fn data_size(&self) -> usize {
        size_of_val(self)
    }
}

impl<'a, T: Into<JsonVariantRef<'a>>> From<T> for JsonValueRef<'a> {
    fn from(v: T) -> Self {
        Self {
            json_type: OnceLock::new(),
            json_variant: v.into(),
        }
    }
}

impl From<JsonValueRef<'_>> for JsonValue {
    fn from(v: JsonValueRef<'_>) -> Self {
        Self {
            json_type: OnceLock::new(),
            json_variant: v.json_variant.into(),
        }
    }
}

impl PartialEq for JsonValueRef<'_> {
    fn eq(&self, other: &Self) -> bool {
        let Self {
            json_type: _,
            json_variant,
        } = self;
        json_variant == &other.json_variant
    }
}

impl Eq for JsonValueRef<'_> {}

impl Clone for JsonValueRef<'_> {
    fn clone(&self) -> Self {
        let Self {
            json_type: _,
            json_variant,
        } = self;
        Self {
            json_type: OnceLock::new(),
            json_variant: json_variant.clone(),
        }
    }
}
