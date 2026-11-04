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
use std::fmt::{Debug, Display, Formatter};
use std::sync::Arc;

use arrow::datatypes::DataType as ArrowDataType;
use arrow_schema::{Field, Fields};
use common_base::bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::Error;
use crate::data_type::DataType;
use crate::error::{InvalidJsonSnafu, InvalidJsonbSnafu, Result, UnsupportedArrowTypeSnafu};
use crate::prelude::ConcreteDataType;
use crate::scalars::ScalarVectorBuilder;
use crate::type_id::LogicalTypeId;
use crate::value::Value;
use crate::vectors::json::builder::JsonVectorBuilder;
use crate::vectors::{BinaryVectorBuilder, MutableVector};

pub const JSON_TYPE_NAME: &str = "Json";
const JSON2_TYPE_NAME: &str = "Json2";

pub type JsonObjectType = BTreeMap<String, JsonNativeType>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum JsonNumberType {
    U64,
    I64,
    F64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, Default)]
pub enum JsonNativeType {
    /// JSON null value type.
    ///
    /// This variant may also appear as the initial state while merging inferred
    /// types, but it does not represent an empty object. Empty objects are
    /// represented as `Object({})`.
    #[default]
    Null,
    Bool,
    Number(JsonNumberType),
    String,
    Array(Box<JsonNativeType>),
    Object(JsonObjectType),
    /// A special (not in the JSON official specification) JSON type to indicate the "resolved" or
    /// "lifted" type of two conflicting JSON types. For example, when merging JSON types of "Bool"
    /// and "Number".
    Variant,
}

impl JsonNativeType {
    pub fn is_null(&self) -> bool {
        matches!(self, JsonNativeType::Null)
    }

    pub fn u64() -> Self {
        Self::Number(JsonNumberType::U64)
    }

    pub fn i64() -> Self {
        Self::Number(JsonNumberType::I64)
    }

    pub fn f64() -> Self {
        Self::Number(JsonNumberType::F64)
    }

    pub fn object() -> Self {
        Self::Object(JsonObjectType::new())
    }

    /// Merge other [JsonNativeType] into this.
    /// Conflicting fields will be resolved to the "Variant" type.
    pub fn merge(&mut self, other: &JsonNativeType) {
        if self == other {
            return;
        }

        fn merge_object(this: &mut JsonObjectType, that: &JsonObjectType) {
            // merge "that" into "this" directly:
            for (type_name, that_type) in that {
                if let Some(this_type) = this.get_mut(type_name) {
                    this_type.merge(that_type);
                } else {
                    this.insert(type_name.clone(), that_type.clone());
                }
            }
        }

        let zelf = std::mem::take(self);
        *self = match (zelf, other) {
            (JsonNativeType::Object(mut this), JsonNativeType::Object(that)) => {
                merge_object(&mut this, that);
                JsonNativeType::Object(this)
            }
            (JsonNativeType::Array(mut this), JsonNativeType::Array(that)) => {
                this.merge(that);
                JsonNativeType::Array(this)
            }
            (JsonNativeType::Null, that) => that.clone(),
            (this, JsonNativeType::Null) => this,
            (this, that) if this == *that => this,

            _ => JsonNativeType::Variant,
        };
    }

    pub fn as_arrow_type(&self) -> ArrowDataType {
        match self {
            JsonNativeType::Null => ArrowDataType::Null,
            JsonNativeType::Bool => ArrowDataType::Boolean,
            JsonNativeType::Number(n) => match n {
                JsonNumberType::U64 => ArrowDataType::UInt64,
                JsonNumberType::I64 => ArrowDataType::Int64,
                JsonNumberType::F64 => ArrowDataType::Float64,
            },
            JsonNativeType::String => ArrowDataType::Utf8View,
            JsonNativeType::Array(array) => {
                ArrowDataType::List(Arc::new(Field::new("item", array.as_arrow_type(), true)))
            }
            JsonNativeType::Object(object) => {
                let fields = object
                    .iter()
                    .map(|(k, v)| Arc::new(Field::new(k, v.as_arrow_type(), true)))
                    .collect::<Vec<_>>();
                ArrowDataType::Struct(Fields::from(fields))
            }
            JsonNativeType::Variant => ArrowDataType::Binary,
        }
    }

    /// Returns whether this type is a boolean, number, or string scalar.
    pub fn is_primitive(&self) -> bool {
        matches!(
            self,
            JsonNativeType::Bool | JsonNativeType::Number(_) | JsonNativeType::String
        )
    }
}

impl From<&ConcreteDataType> for JsonNativeType {
    fn from(value: &ConcreteDataType) -> Self {
        match value {
            ConcreteDataType::Null(_) => JsonNativeType::Null,
            ConcreteDataType::Boolean(_) => JsonNativeType::Bool,
            ConcreteDataType::UInt64(_)
            | ConcreteDataType::UInt32(_)
            | ConcreteDataType::UInt16(_)
            | ConcreteDataType::UInt8(_) => JsonNativeType::u64(),
            ConcreteDataType::Int64(_)
            | ConcreteDataType::Int32(_)
            | ConcreteDataType::Int16(_)
            | ConcreteDataType::Int8(_) => JsonNativeType::i64(),
            ConcreteDataType::Float64(_) | ConcreteDataType::Float32(_) => JsonNativeType::f64(),
            ConcreteDataType::String(_) => JsonNativeType::String,
            ConcreteDataType::List(list_type) => {
                JsonNativeType::Array(Box::new(JsonNativeType::from(list_type.item_type())))
            }
            ConcreteDataType::Struct(struct_type) => JsonNativeType::Object(
                struct_type
                    .fields()
                    .iter()
                    .map(|field| (field.name().to_string(), field.data_type().into()))
                    .collect(),
            ),
            ConcreteDataType::Json(json_type) => json_type.native_type().clone(),
            ConcreteDataType::Binary(_) => JsonNativeType::Variant,
            _ => unreachable!(),
        }
    }
}

impl TryFrom<&ArrowDataType> for JsonNativeType {
    type Error = Error;

    fn try_from(t: &ArrowDataType) -> Result<Self> {
        let t = match t {
            ArrowDataType::Null => JsonNativeType::Null,
            ArrowDataType::Boolean => JsonNativeType::Bool,
            ArrowDataType::Int8
            | ArrowDataType::Int16
            | ArrowDataType::Int32
            | ArrowDataType::Int64 => JsonNativeType::i64(),
            ArrowDataType::UInt8
            | ArrowDataType::UInt16
            | ArrowDataType::UInt32
            | ArrowDataType::UInt64 => JsonNativeType::u64(),
            ArrowDataType::Float16 | ArrowDataType::Float32 | ArrowDataType::Float64 => {
                JsonNativeType::f64()
            }
            ArrowDataType::Binary
            | ArrowDataType::FixedSizeBinary(_)
            | ArrowDataType::LargeBinary
            | ArrowDataType::BinaryView => JsonNativeType::Variant,
            ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 | ArrowDataType::Utf8View => {
                JsonNativeType::String
            }
            ArrowDataType::List(field)
            | ArrowDataType::ListView(field)
            | ArrowDataType::FixedSizeList(field, _)
            | ArrowDataType::LargeList(field)
            | ArrowDataType::LargeListView(field) => {
                JsonNativeType::Array(Box::new(Self::try_from(field.data_type())?))
            }
            ArrowDataType::Struct(fields) => {
                let mut object = JsonObjectType::new();
                for field in fields {
                    object.insert(field.name().clone(), Self::try_from(field.data_type())?);
                }
                JsonNativeType::Object(object)
            }
            t => {
                return UnsupportedArrowTypeSnafu {
                    arrow_type: t.clone(),
                }
                .fail();
            }
        };
        Ok(t)
    }
}

impl Display for JsonNativeType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            JsonNativeType::Null => write!(f, r#""<Null>""#),
            JsonNativeType::Bool => write!(f, r#""<Bool>""#),
            JsonNativeType::Number(_) => {
                write!(f, r#""<Number>""#)
            }
            JsonNativeType::String => write!(f, r#""<String>""#),
            JsonNativeType::Array(item_type) => write!(f, "[{}]", item_type),
            JsonNativeType::Object(object) => {
                write!(
                    f,
                    "{{{}}}",
                    object
                        .iter()
                        .map(|(k, v)| format!(r#""{k}":{v}"#))
                        .collect::<Vec<_>>()
                        .join(",")
                )
            }
            JsonNativeType::Variant => write!(f, r#""<Variant>""#),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, Default)]
pub enum JsonFormat {
    #[default]
    Jsonb,
    Json2(Arc<JsonNativeType>),
}

/// JsonType is a data type for JSON data. It is stored as binary data of jsonb format.
/// It utilizes current binary value and vector implementation.
#[derive(Debug, Default, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct JsonType {
    pub format: JsonFormat,
}

impl JsonType {
    pub fn new(format: JsonFormat) -> Self {
        Self { format }
    }

    pub(crate) fn json2(json_type: Arc<JsonNativeType>) -> Self {
        Self {
            format: JsonFormat::Json2(json_type),
        }
    }

    pub fn is_json2(&self) -> bool {
        matches!(self.format, JsonFormat::Json2(_))
    }

    /// Returns the native JSON type represented by this data type.
    pub fn native_type(&self) -> &JsonNativeType {
        match &self.format {
            JsonFormat::Jsonb => &JsonNativeType::String,
            JsonFormat::Json2(x) => x.as_ref(),
        }
    }

    pub fn null() -> Self {
        Self::json2(Arc::new(JsonNativeType::Null))
    }

    /// Check if it includes all fields in `other` json type.
    pub fn is_include(&self, other: &JsonType) -> bool {
        match (&self.format, &other.format) {
            (JsonFormat::Jsonb, JsonFormat::Jsonb) => true,
            (JsonFormat::Json2(this), JsonFormat::Json2(that)) => is_include(this, that),
            _ => false,
        }
    }
}

pub(crate) fn is_include(this: &JsonNativeType, that: &JsonNativeType) -> bool {
    fn is_include_object(this: &JsonObjectType, that: &JsonObjectType) -> bool {
        for (type_name, that_type) in that {
            let Some(this_type) = this.get(type_name) else {
                return false;
            };
            if !is_include(this_type, that_type) {
                return false;
            }
        }
        true
    }

    match (this, that) {
        (this, that) if this == that => true,
        (JsonNativeType::Array(this), JsonNativeType::Array(that)) => is_include(this, that),
        (JsonNativeType::Object(this), JsonNativeType::Object(that)) => {
            is_include_object(this, that)
        }
        (_, JsonNativeType::Null) => true,
        _ => false,
    }
}

impl DataType for JsonType {
    fn name(&self) -> String {
        match &self.format {
            JsonFormat::Jsonb => JSON_TYPE_NAME.to_string(),
            JsonFormat::Json2(ty) => {
                format!("{JSON2_TYPE_NAME}{}", ty)
            }
        }
    }

    fn logical_type_id(&self) -> LogicalTypeId {
        LogicalTypeId::Json
    }

    fn default_value(&self) -> Value {
        Bytes::default().into()
    }

    fn as_arrow_type(&self) -> ArrowDataType {
        match &self.format {
            JsonFormat::Jsonb => ArrowDataType::Binary,
            JsonFormat::Json2(x) => {
                let mut object = JsonNativeType::object();
                object.merge(x.as_ref());
                object.as_arrow_type()
            }
        }
    }

    fn create_mutable_vector(&self, capacity: usize) -> Box<dyn MutableVector> {
        match &self.format {
            JsonFormat::Jsonb => Box::new(BinaryVectorBuilder::with_capacity(capacity)),
            // TODO(LFC): Carry JsonSettings in JsonFormat::Json2 and use with_settings here.
            JsonFormat::Json2(x) => Box::new(JsonVectorBuilder::new(x.as_ref().clone(), capacity)),
        }
    }

    fn try_cast(&self, from: Value) -> Option<Value> {
        match from {
            Value::Binary(v) => Some(Value::Binary(v)),
            _ => None,
        }
    }
}

impl Display for JsonType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

/// Converts a json type value to string
///
/// The binary jsonb is serialized directly with `serde_json`, so both object keys and
/// string values are escaped by `serde_json`. `RawJsonb::to_string()` is deliberately
/// avoided, since it falls back to the lossy raw bytes when serialization fails.
pub fn jsonb_to_string(val: &[u8]) -> Result<String> {
    if val.is_empty() {
        return Ok("".to_string());
    }
    serde_json::to_string(&jsonb::RawJsonb::new(val)).map_err(|e| {
        InvalidJsonSnafu {
            value: e.to_string(),
        }
        .build()
    })
}

/// Converts a json type value to serde_json::Value
pub fn jsonb_to_serde_json(val: &[u8]) -> Result<serde_json::Value> {
    jsonb::from_slice(val)
        .map(Into::into)
        .map_err(|error| InvalidJsonbSnafu { error }.build())
}

/// Parses a string to a json type value
pub fn parse_string_to_jsonb(s: &str) -> Result<Vec<u8>> {
    jsonb::parse_value_standard_mode(s.as_bytes())
        .map_err(|_| InvalidJsonSnafu { value: s }.build())
        .map(|json| json.to_vec())
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_parse_json_rejects_extended_syntax() {
        for input in ["+1", "01", ".5", "[1,,2]", "{'a':1}"] {
            assert!(parse_string_to_jsonb(input).is_err(), "{input}");
        }
    }

    #[test]
    fn test_v2_jsonb_to_serde_json_escaped_keys()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let expected = serde_json::json!({"": 9, "a\"b": {"a\\b": "\u{1f600}\n"}});
        let input = expected.to_string();
        let value = jsonb::parse_value(input.as_bytes()).map_err(|e| e.to_string())?;
        assert_eq!(jsonb_to_serde_json(&value.to_vec())?, expected);
        Ok(())
    }

    #[test]
    fn test_jsonb_to_string_escapes_object_keys()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let expected = serde_json::json!({
            "": 1,
            "a\"b": {"c\\d": ["x", {"e\nf\tg\u{1}": true}], "h": null},
            "plain": "v\"q",
        });
        let input = expected.to_string();
        let value = jsonb::parse_value(input.as_bytes()).map_err(|e| e.to_string())?;
        let text = jsonb_to_string(&value.to_vec())?;
        assert_eq!(serde_json::from_str::<serde_json::Value>(&text)?, expected);
        assert!(text.contains(r#""a\"b":"#), "{text}");
        Ok(())
    }

    #[test]
    fn test_jsonb_to_string_preserves_literal_unicode_escape_key() {
        let input = r#"{"\\u{1f600}":"value"}"#;
        let bytes = parse_string_to_jsonb(input).unwrap();
        let text = jsonb_to_string(&bytes).unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&text).unwrap(),
            serde_json::from_str::<serde_json::Value>(input).unwrap(),
        );

        // A BMP literal key round-trips unchanged as well.
        let input = r#"{"\\u{41}":"value"}"#;
        let bytes = parse_string_to_jsonb(input).unwrap();
        let text = jsonb_to_string(&bytes).unwrap();
        assert_eq!(text, input);
    }

    #[test]
    fn test_jsonb_to_string_escapes_string_values() {
        // Literal `\u{...}` text and a NUL in a value must still render as valid JSON
        // that round-trips to the same value.
        for input in [
            r#"{"k":"\\u{1f600}"}"#,
            r#"{"k":"\\u{41}"}"#,
            r#"{"k":"a\u0000b"}"#,
            r#"["\\u{1f600}","\u0000"]"#,
        ] {
            let bytes = parse_string_to_jsonb(input).unwrap();
            let text = jsonb_to_string(&bytes).unwrap();
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(&text).unwrap(),
                serde_json::from_str::<serde_json::Value>(input).unwrap(),
                "{input} -> {text}"
            );
        }

        // U+2028 is emitted as-is, which is still valid JSON.
        let bytes = parse_string_to_jsonb("{\"k\":\"a\\u2028b\"}").unwrap();
        assert_eq!(jsonb_to_string(&bytes).unwrap(), "{\"k\":\"a\u{2028}b\"}");
    }

    #[test]
    fn test_jsonb_to_string_rejects_invalid_jsonb() {
        assert!(jsonb_to_string(&[1, 2, 3, 4]).is_err());
        assert_eq!(jsonb_to_string(&[]).unwrap(), "");
    }

    #[test]
    fn test_jsonb_to_string_keeps_compact_format() -> Result<()> {
        let value = jsonb::parse_value(br#"{"b": [1, 2.5, "x"], "a": {"c": false}}"#).unwrap();
        assert_eq!(
            jsonb_to_string(&value.to_vec())?,
            r#"{"a":{"c":false},"b":[1,2.5,"x"]}"#
        );
        Ok(())
    }

    use super::*;

    #[test]
    fn test_json_type_include() {
        fn test(this: &JsonNativeType, that: &JsonNativeType, expected: bool) {
            assert_eq!(is_include(this, that), expected, "this={this}, that={that}");
        }

        test(&JsonNativeType::Null, &JsonNativeType::Null, true);
        test(&JsonNativeType::Null, &JsonNativeType::Bool, false);
        test(&JsonNativeType::Bool, &JsonNativeType::Null, true);

        test(&JsonNativeType::Bool, &JsonNativeType::Bool, true);
        test(&JsonNativeType::Bool, &JsonNativeType::u64(), false);

        test(&JsonNativeType::u64(), &JsonNativeType::Null, true);
        test(&JsonNativeType::u64(), &JsonNativeType::u64(), true);
        test(&JsonNativeType::u64(), &JsonNativeType::String, false);

        test(&JsonNativeType::String, &JsonNativeType::Null, true);
        test(&JsonNativeType::String, &JsonNativeType::String, true);
        test(
            &JsonNativeType::String,
            &JsonNativeType::Array(Box::new(JsonNativeType::f64())),
            false,
        );

        test(
            &JsonNativeType::Array(Box::new(JsonNativeType::f64())),
            &JsonNativeType::Null,
            true,
        );
        test(
            &JsonNativeType::Array(Box::new(JsonNativeType::f64())),
            &JsonNativeType::Array(Box::new(JsonNativeType::Null)),
            true,
        );
        test(
            &JsonNativeType::Array(Box::new(JsonNativeType::f64())),
            &JsonNativeType::Array(Box::new(JsonNativeType::f64())),
            true,
        );
        test(
            &JsonNativeType::Array(Box::new(JsonNativeType::f64())),
            &JsonNativeType::String,
            false,
        );
        test(
            &JsonNativeType::Array(Box::new(JsonNativeType::f64())),
            &JsonNativeType::Object(JsonObjectType::new()),
            false,
        );

        let simple_json_object = &JsonNativeType::Object(JsonObjectType::from([(
            "foo".to_string(),
            JsonNativeType::String,
        )]));
        test(simple_json_object, simple_json_object, true);
        test(simple_json_object, &JsonNativeType::i64(), false);
        test(
            simple_json_object,
            &JsonNativeType::Object(JsonObjectType::from([(
                "bar".to_string(),
                JsonNativeType::i64(),
            )])),
            false,
        );

        let complex_json_object = &JsonNativeType::Object(JsonObjectType::from([
            (
                "nested".to_string(),
                JsonNativeType::Object(JsonObjectType::from([(
                    "a".to_string(),
                    JsonNativeType::Object(JsonObjectType::from([(
                        "b".to_string(),
                        JsonNativeType::Object(JsonObjectType::from([(
                            "c".to_string(),
                            JsonNativeType::String,
                        )])),
                    )])),
                )])),
            ),
            ("bar".to_string(), JsonNativeType::i64()),
        ]));
        test(simple_json_object, &JsonNativeType::Null, true);
        test(complex_json_object, &JsonNativeType::String, false);
        test(complex_json_object, complex_json_object, true);
        test(
            complex_json_object,
            &JsonNativeType::Object(JsonObjectType::from([(
                "bar".to_string(),
                JsonNativeType::i64(),
            )])),
            true,
        );
        test(
            complex_json_object,
            &JsonNativeType::Object(JsonObjectType::from([
                (
                    "nested".to_string(),
                    JsonNativeType::Object(JsonObjectType::from([(
                        "a".to_string(),
                        JsonNativeType::Null,
                    )])),
                ),
                ("bar".to_string(), JsonNativeType::i64()),
            ])),
            true,
        );
        test(
            complex_json_object,
            &JsonNativeType::Object(JsonObjectType::from([
                (
                    "nested".to_string(),
                    JsonNativeType::Object(JsonObjectType::from([(
                        "a".to_string(),
                        JsonNativeType::String,
                    )])),
                ),
                ("bar".to_string(), JsonNativeType::i64()),
            ])),
            false,
        );
        test(
            complex_json_object,
            &JsonNativeType::Object(JsonObjectType::from([
                (
                    "nested".to_string(),
                    JsonNativeType::Object(JsonObjectType::from([(
                        "a".to_string(),
                        JsonNativeType::Object(JsonObjectType::from([(
                            "b".to_string(),
                            JsonNativeType::String,
                        )])),
                    )])),
                ),
                ("bar".to_string(), JsonNativeType::i64()),
            ])),
            false,
        );
        test(
            complex_json_object,
            &JsonNativeType::Object(JsonObjectType::from([
                (
                    "nested".to_string(),
                    JsonNativeType::Object(JsonObjectType::from([(
                        "a".to_string(),
                        JsonNativeType::Object(JsonObjectType::from([(
                            "b".to_string(),
                            JsonNativeType::Object(JsonObjectType::from([(
                                "c".to_string(),
                                JsonNativeType::Null,
                            )])),
                        )])),
                    )])),
                ),
                ("bar".to_string(), JsonNativeType::i64()),
            ])),
            true,
        );
        test(
            complex_json_object,
            &JsonNativeType::Object(JsonObjectType::from([
                (
                    "nested".to_string(),
                    JsonNativeType::Object(JsonObjectType::from([(
                        "a".to_string(),
                        JsonNativeType::Object(JsonObjectType::from([(
                            "b".to_string(),
                            JsonNativeType::Object(JsonObjectType::from([(
                                "c".to_string(),
                                JsonNativeType::Bool,
                            )])),
                        )])),
                    )])),
                ),
                ("bar".to_string(), JsonNativeType::i64()),
            ])),
            false,
        );
        test(
            complex_json_object,
            &JsonNativeType::Object(JsonObjectType::from([(
                "nested".to_string(),
                JsonNativeType::Object(JsonObjectType::from([(
                    "a".to_string(),
                    JsonNativeType::Object(JsonObjectType::from([(
                        "b".to_string(),
                        JsonNativeType::Object(JsonObjectType::from([(
                            "c".to_string(),
                            JsonNativeType::String,
                        )])),
                    )])),
                )])),
            )])),
            true,
        );
    }

    #[test]
    fn test_merge_json_type() {
        fn test(other: JsonNativeType, json_type: &mut JsonNativeType, expected: &str) {
            json_type.merge(&other);
            assert_eq!(json_type.to_string(), expected);
        }

        // Null should be absorbed by a concrete scalar type.
        test(
            JsonNativeType::Bool,
            &mut JsonNativeType::Null,
            r#""<Bool>""#,
        );

        // Merging a null value into an existing concrete type should keep the type unchanged.
        test(
            JsonNativeType::Null,
            &mut JsonNativeType::Bool,
            r#""<Bool>""#,
        );

        // Identical number categories should stay as Number.
        test(
            JsonNativeType::i64(),
            &mut JsonNativeType::i64(),
            r#""<Number>""#,
        );

        // Conflicting number categories should be lifted to Variant.
        for (mut this, other) in [
            (JsonNativeType::u64(), JsonNativeType::i64()),
            (JsonNativeType::u64(), JsonNativeType::f64()),
            (JsonNativeType::i64(), JsonNativeType::f64()),
        ] {
            test(other, &mut this, r#""<Variant>""#);
        }

        // Object merge should preserve existing fields and append missing fields.
        test(
            JsonNativeType::Object(JsonObjectType::from([(
                "foo".to_string(),
                JsonNativeType::String,
            )])),
            &mut JsonNativeType::Object(JsonObjectType::from([(
                "bar".to_string(),
                JsonNativeType::i64(),
            )])),
            r#"{"bar":"<Number>","foo":"<String>"}"#,
        );

        // Conflicting object field types should only lift that field to Variant.
        test(
            JsonNativeType::Object(JsonObjectType::from([(
                "foo".to_string(),
                JsonNativeType::i64(),
            )])),
            &mut JsonNativeType::Object(JsonObjectType::from([(
                "foo".to_string(),
                JsonNativeType::Bool,
            )])),
            r#"{"foo":"<Variant>"}"#,
        );

        // Nested objects should merge recursively.
        test(
            JsonNativeType::Object(JsonObjectType::from([(
                "nested".to_string(),
                JsonNativeType::Object(JsonObjectType::from([(
                    "foo".to_string(),
                    JsonNativeType::String,
                )])),
            )])),
            &mut JsonNativeType::Object(JsonObjectType::from([(
                "nested".to_string(),
                JsonNativeType::Object(JsonObjectType::from([(
                    "bar".to_string(),
                    JsonNativeType::Bool,
                )])),
            )])),
            r#"{"nested":{"bar":"<Bool>","foo":"<String>"}}"#,
        );

        // Arrays should merge their element types recursively.
        test(
            JsonNativeType::Array(Box::new(JsonNativeType::String)),
            &mut JsonNativeType::Array(Box::new(JsonNativeType::u64())),
            r#"["<Variant>"]"#,
        );

        // Root-level incompatible types should be lifted to Variant.
        test(
            JsonNativeType::Object(JsonObjectType::from([(
                "foo".to_string(),
                JsonNativeType::String,
            )])),
            &mut JsonNativeType::Bool,
            r#""<Variant>""#,
        );
    }
}
