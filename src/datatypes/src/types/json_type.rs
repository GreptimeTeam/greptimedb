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
use std::str::FromStr;
use std::sync::{Arc, LazyLock};

use arrow::datatypes::DataType as ArrowDataType;
use arrow_schema::Fields;
use common_base::bytes::Bytes;
use regex::{Captures, Regex};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

use crate::data_type::DataType;
use crate::error::{
    DeserializeSnafu, InvalidJsonSnafu, InvalidJsonbSnafu, MergeJsonDatatypeSnafu, Result,
};
use crate::prelude::ConcreteDataType;
use crate::scalars::ScalarVectorBuilder;
use crate::type_id::LogicalTypeId;
use crate::types::{ListType, StructField, StructType};
use crate::value::Value;
use crate::vectors::json::builder::JsonVectorBuilder;
use crate::vectors::{BinaryVectorBuilder, MutableVector};

pub const JSON_TYPE_NAME: &str = "Json";
const JSON2_TYPE_NAME: &str = "Json2";
const JSON_PLAIN_FIELD_NAME: &str = "__json_plain__";
const JSON_PLAIN_FIELD_METADATA_KEY: &str = "is_plain_json";

pub type JsonObjectType = BTreeMap<String, JsonNativeType>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum JsonNumberType {
    U64,
    I64,
    F64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum JsonNativeType {
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
}

impl From<&JsonNativeType> for ConcreteDataType {
    fn from(value: &JsonNativeType) -> Self {
        match value {
            JsonNativeType::Null => ConcreteDataType::null_datatype(),
            JsonNativeType::Bool => ConcreteDataType::boolean_datatype(),
            JsonNativeType::Number(JsonNumberType::U64) => ConcreteDataType::uint64_datatype(),
            JsonNativeType::Number(JsonNumberType::I64) => ConcreteDataType::int64_datatype(),
            JsonNativeType::Number(JsonNumberType::F64) => ConcreteDataType::float64_datatype(),
            JsonNativeType::String => ConcreteDataType::string_datatype(),
            JsonNativeType::Array(item_type) => {
                ConcreteDataType::List(ListType::new(Arc::new(item_type.as_ref().into())))
            }
            JsonNativeType::Object(object) => {
                let fields = object
                    .iter()
                    .map(|(type_name, field_type)| {
                        StructField::new(type_name.clone(), field_type.into(), true)
                    })
                    .collect();
                ConcreteDataType::Struct(StructType::new(Arc::new(fields)))
            }
            JsonNativeType::Variant => ConcreteDataType::binary_datatype(),
        }
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
                JsonNativeType::Array(Box::new(list_type.item_type().into()))
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

impl Display for JsonNativeType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            JsonNativeType::Null => write!(f, r#""<Null>""#),
            JsonNativeType::Bool => write!(f, r#""<Bool>""#),
            JsonNativeType::Number(_) => {
                write!(f, r#""<Number>""#)
            }
            JsonNativeType::String => write!(f, r#""<String>""#),
            JsonNativeType::Array(item_type) => {
                write!(f, "[{}]", item_type)
            }
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
    Json2(Box<JsonNativeType>),
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

    pub(crate) fn new_json2(native: JsonNativeType) -> Self {
        Self {
            format: JsonFormat::Json2(Box::new(native)),
        }
    }

    pub fn is_json2(&self) -> bool {
        matches!(self.format, JsonFormat::Json2(_))
    }

    pub(crate) fn native_type(&self) -> &JsonNativeType {
        match &self.format {
            JsonFormat::Jsonb => &JsonNativeType::String,
            JsonFormat::Json2(x) => x.as_ref(),
        }
    }

    pub fn null() -> Self {
        Self {
            format: JsonFormat::Json2(Box::new(JsonNativeType::Null)),
        }
    }

    /// Make json type a struct type, by:
    /// - if the json is an object, its entries are mapped to struct fields, obviously;
    /// - if not, the json is one of bool, number, string or array, make it a special field
    ///   (see [plain_json_struct_type]).
    pub(crate) fn as_struct_type(&self) -> StructType {
        match &self.format {
            JsonFormat::Jsonb => StructType::default(),
            JsonFormat::Json2(inner) => match ConcreteDataType::from(inner.as_ref()) {
                ConcreteDataType::Struct(t) => t.clone(),
                x => plain_json_struct_type(x),
            },
        }
    }

    /// Try to merge this json type with others, error on datatype conflict.
    pub fn merge(&mut self, other: &JsonType) -> Result<()> {
        match (&self.format, &other.format) {
            (JsonFormat::Jsonb, JsonFormat::Jsonb) => Ok(()),
            (JsonFormat::Json2(this), JsonFormat::Json2(that)) => {
                let merged = merge(this.as_ref(), that.as_ref());
                self.format = JsonFormat::Json2(Box::new(merged));
                Ok(())
            }
            _ => MergeJsonDatatypeSnafu {
                reason: "json format not match",
            }
            .fail(),
        }
    }

    /// Check if it can merge with `other` json type.
    pub(crate) fn is_mergeable(&self, other: &JsonType) -> bool {
        match (&self.format, &other.format) {
            (JsonFormat::Jsonb, JsonFormat::Jsonb) => true,
            (JsonFormat::Json2(this), JsonFormat::Json2(that)) => {
                is_mergeable(this.as_ref(), that.as_ref())
            }
            _ => false,
        }
    }

    /// Check if it includes all fields in `other` json type.
    pub fn is_include(&self, other: &JsonType) -> bool {
        match (&self.format, &other.format) {
            (JsonFormat::Jsonb, JsonFormat::Jsonb) => true,
            (JsonFormat::Json2(this), JsonFormat::Json2(that)) => {
                is_include(this.as_ref(), that.as_ref())
            }
            _ => false,
        }
    }
}

fn is_include(this: &JsonNativeType, that: &JsonNativeType) -> bool {
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
        (JsonNativeType::Array(this), JsonNativeType::Array(that)) => {
            is_include(this.as_ref(), that.as_ref())
        }
        (JsonNativeType::Object(this), JsonNativeType::Object(that)) => {
            is_include_object(this, that)
        }
        (_, JsonNativeType::Null) => true,
        _ => false,
    }
}

/// A special struct type for denoting "plain"(not object) json value. It has only one field, with
/// fixed name [JSON_PLAIN_FIELD_NAME] and with metadata [JSON_PLAIN_FIELD_METADATA_KEY] = `"true"`.
pub(crate) fn plain_json_struct_type(item_type: ConcreteDataType) -> StructType {
    let mut field = StructField::new(JSON_PLAIN_FIELD_NAME.to_string(), item_type, true);
    field.insert_metadata(JSON_PLAIN_FIELD_METADATA_KEY, true);
    StructType::new(Arc::new(vec![field]))
}

fn is_mergeable(this: &JsonNativeType, that: &JsonNativeType) -> bool {
    fn is_mergeable_object(this: &JsonObjectType, that: &JsonObjectType) -> bool {
        for (type_name, that_type) in that {
            if let Some(this_type) = this.get(type_name)
                && !is_mergeable(this_type, that_type)
            {
                return false;
            }
        }
        true
    }

    match (this, that) {
        (this, that) if this == that => true,
        (JsonNativeType::Array(this), JsonNativeType::Array(that)) => {
            is_mergeable(this.as_ref(), that.as_ref())
        }
        (JsonNativeType::Object(this), JsonNativeType::Object(that)) => {
            is_mergeable_object(this, that)
        }
        (JsonNativeType::Null, _) | (_, JsonNativeType::Null) => true,
        _ => false,
    }
}

fn merge(this: &JsonNativeType, that: &JsonNativeType) -> JsonNativeType {
    fn merge_object(this: &JsonObjectType, that: &JsonObjectType) -> JsonObjectType {
        let mut this = this.clone();
        // merge "that" into "this" directly:
        for (type_name, that_type) in that {
            if let Some(this_type) = this.get_mut(type_name) {
                let merged_type = merge(this_type, that_type);
                *this_type = merged_type;
            } else {
                this.insert(type_name.clone(), that_type.clone());
            }
        }
        this
    }

    match (this, that) {
        (this, that) if this == that => this.clone(),
        (JsonNativeType::Array(this), JsonNativeType::Array(that)) => {
            JsonNativeType::Array(Box::new(merge(this.as_ref(), that.as_ref())))
        }
        (JsonNativeType::Object(this), JsonNativeType::Object(that)) => {
            JsonNativeType::Object(merge_object(this, that))
        }
        (JsonNativeType::Null, x) | (x, JsonNativeType::Null) => x.clone(),
        _ => JsonNativeType::Variant,
    }
}

impl DataType for JsonType {
    fn name(&self) -> String {
        match &self.format {
            JsonFormat::Jsonb => JSON_TYPE_NAME.to_string(),
            JsonFormat::Json2(_) => JSON2_TYPE_NAME.to_string(),
        }
    }

    fn logical_type_id(&self) -> LogicalTypeId {
        LogicalTypeId::Json
    }

    fn default_value(&self) -> Value {
        Bytes::default().into()
    }

    fn as_arrow_type(&self) -> ArrowDataType {
        match self.format {
            JsonFormat::Jsonb => ArrowDataType::Binary,
            // "Erase" the JSON struct when converting to Arrow datatype, is a feature (not a bug).
            // The actual Arrow datatype is deduced from parquet data and query schema (a process
            // called "JSON type concretization") from time to time, there's no a universal/global
            // type for JSON2.
            // Same reason for ignoring the struct in the `name` method above.
            JsonFormat::Json2(_) => ArrowDataType::Struct(Fields::empty()),
        }
    }

    fn create_mutable_vector(&self, capacity: usize) -> Box<dyn MutableVector> {
        match &self.format {
            JsonFormat::Jsonb => Box::new(BinaryVectorBuilder::with_capacity(capacity)),
            JsonFormat::Json2(x) => Box::new(JsonVectorBuilder::new(*x.clone(), capacity)),
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
pub fn jsonb_to_string(val: &[u8]) -> Result<String> {
    if val.is_empty() {
        return Ok("".to_string());
    }
    match jsonb::from_slice(val) {
        Ok(jsonb_value) => {
            let serialized = jsonb_value.to_string();
            fix_unicode_point(&serialized)
        }
        Err(e) => InvalidJsonbSnafu { error: e }.fail(),
    }
}

/// Converts a json type value to serde_json::Value
pub fn jsonb_to_serde_json(val: &[u8]) -> Result<serde_json::Value> {
    let json_string = jsonb_to_string(val)?;
    serde_json::Value::from_str(&json_string).context(DeserializeSnafu { json: json_string })
}

/// Normalizes a JSON string by converting Rust-style Unicode escape sequences to JSON-compatible format.
///
/// The input is scanned for Rust-style Unicode code
/// point escapes of the form `\\u{H...}` (a backslash, `u`, an opening brace,
/// followed by 1–6 hexadecimal digits, and a closing brace). Each such escape is
/// converted into JSON-compatible UTF‑16 escape sequences:
///
/// - For code points in the Basic Multilingual Plane (≤ `0xFFFF`), the escape is
///   converted to a single JSON `\\uXXXX` sequence with four uppercase hex digits.
/// - For code points above `0xFFFF` and less than Unicode max code point `0x10FFFF`,
///   the code point is encoded as a UTF‑16 surrogate pair and emitted as two consecutive
///   `\\uXXXX` sequences (as JSON format required).
///
/// After this normalization, the function returns the normalized string
fn fix_unicode_point(json: &str) -> Result<String> {
    static UNICODE_CODE_POINT_PATTERN: LazyLock<Regex> = LazyLock::new(|| {
        // Match literal "\u{...}" sequences, capturing 1–6 (code point range) hex digits
        // inside braces.
        Regex::new(r"\\u\{([0-9a-fA-F]{1,6})}").unwrap_or_else(|e| panic!("{}", e))
    });

    let v = UNICODE_CODE_POINT_PATTERN.replace_all(json, |caps: &Captures| {
        // Extract the hex payload (without braces) and parse to a code point.
        let hex = &caps[1];
        let Ok(code) = u32::from_str_radix(hex, 16) else {
            // On parse failure, leave the original escape sequence unchanged.
            return caps[0].to_string();
        };

        if code <= 0xFFFF {
            // Basic Multilingual Plane: JSON can represent this directly as \uXXXX.
            format!("\\u{:04X}", code)
        } else if code > 0x10FFFF {
            // Beyond max Unicode code point
            caps[0].to_string()
        } else {
            // Supplementary planes: JSON needs UTF-16 surrogate pairs.
            // Convert the code point to a 20-bit value.
            let code = code - 0x10000;

            // High surrogate: top 10 bits, offset by 0xD800.
            let high = 0xD800 + ((code >> 10) & 0x3FF);

            // Low surrogate: bottom 10 bits, offset by 0xDC00.
            let low = 0xDC00 + (code & 0x3FF);

            // Emit two \uXXXX escapes in sequence.
            format!("\\u{:04X}\\u{:04X}", high, low)
        }
    });
    Ok(v.to_string())
}

/// Parses a string to a json type value
pub fn parse_string_to_jsonb(s: &str) -> Result<Vec<u8>> {
    jsonb::parse_value(s.as_bytes())
        .map_err(|_| InvalidJsonSnafu { value: s }.build())
        .map(|json| json.to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::json::JsonStructureSettings;

    #[test]
    fn test_fix_unicode_point() -> Result<()> {
        let valid_cases = vec![
            (r#"{"data": "simple ascii"}"#, r#"{"data": "simple ascii"}"#),
            (
                r#"{"data":"Greek sigma: \u{03a3}"}"#,
                r#"{"data":"Greek sigma: \u03A3"}"#,
            ),
            (
                r#"{"data":"Joker card: \u{1f0df}"}"#,
                r#"{"data":"Joker card: \uD83C\uDCDF"}"#,
            ),
            (
                r#"{"data":"BMP boundary: \u{ffff}"}"#,
                r#"{"data":"BMP boundary: \uFFFF"}"#,
            ),
            (
                r#"{"data":"Supplementary min: \u{10000}"}"#,
                r#"{"data":"Supplementary min: \uD800\uDC00"}"#,
            ),
            (
                r#"{"data":"Supplementary max: \u{10ffff}"}"#,
                r#"{"data":"Supplementary max: \uDBFF\uDFFF"}"#,
            ),
        ];
        for (input, expect) in valid_cases {
            let v = fix_unicode_point(input)?;
            assert_eq!(v, expect);
        }

        let invalid_escape_cases = vec![
            (
                r#"{"data": "Invalid hex: \u{gggg}"}"#,
                r#"{"data": "Invalid hex: \u{gggg}"}"#,
            ),
            (
                r#"{"data": "Empty braces: \u{}"}"#,
                r#"{"data": "Empty braces: \u{}"}"#,
            ),
            (
                r#"{"data": "Out of range: \u{1100000}"}"#,
                r#"{"data": "Out of range: \u{1100000}"}"#,
            ),
        ];
        for (input, expect) in invalid_escape_cases {
            let v = fix_unicode_point(input)?;
            assert_eq!(v, expect);
        }

        Ok(())
    }

    #[test]
    fn test_json_type_include() {
        fn test(this: &JsonNativeType, that: &JsonNativeType, expected: bool) {
            assert_eq!(is_include(this, that), expected);
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
        test(simple_json_object, &JsonNativeType::Null, true);
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
        test(complex_json_object, &JsonNativeType::Null, true);
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
    fn test_merge_json_type() -> Result<()> {
        fn test(
            json: &str,
            json_type: &mut JsonType,
            expected: std::result::Result<&str, &str>,
        ) -> Result<()> {
            let json: serde_json::Value = serde_json::from_str(json).unwrap();

            let settings = JsonStructureSettings::Structured(None);
            let value = settings.encode(json)?;
            let value_type = value.data_type();
            let Some(other) = value_type.as_json() else {
                unreachable!()
            };

            let result = json_type.merge(other);
            match (result, expected) {
                (Ok(()), Ok(expected)) => {
                    assert_eq!(json_type.native_type().to_string(), expected);
                }
                (Err(err), Err(expected)) => {
                    assert_eq!(err.to_string(), expected);
                }
                _ => unreachable!(),
            }
            Ok(())
        }

        // Null should be absorbed by a concrete scalar type.
        test("true", &mut JsonType::null(), Ok(r#""<Bool>""#))?;

        // Merging a null value into an existing concrete type should keep the type unchanged.
        test(
            "null",
            &mut JsonType::new_json2(JsonNativeType::Bool),
            Ok(r#""<Bool>""#),
        )?;

        // Identical number categories should stay as Number.
        test(
            "1",
            &mut JsonType::new_json2(JsonNativeType::i64()),
            Ok(r#""<Number>""#),
        )?;

        // Conflicting number categories should be lifted to Variant.
        test(
            "1.5",
            &mut JsonType::new_json2(JsonNativeType::i64()),
            Ok(r#""<Variant>""#),
        )?;

        // Object merge should preserve existing fields and append missing fields.
        test(
            r#"{"foo":"x"}"#,
            &mut JsonType::new_json2(JsonNativeType::Object(JsonObjectType::from([(
                "bar".to_string(),
                JsonNativeType::i64(),
            )]))),
            Ok(r#"{"bar":"<Number>","foo":"<String>"}"#),
        )?;

        // Conflicting object field types should only lift that field to Variant.
        test(
            r#"{"foo":1}"#,
            &mut JsonType::new_json2(JsonNativeType::Object(JsonObjectType::from([(
                "foo".to_string(),
                JsonNativeType::Bool,
            )]))),
            Ok(r#"{"foo":"<Variant>"}"#),
        )?;

        // Nested objects should merge recursively.
        test(
            r#"{"nested":{"foo":"bar"}}"#,
            &mut JsonType::new_json2(JsonNativeType::Object(JsonObjectType::from([(
                "nested".to_string(),
                JsonNativeType::Object(JsonObjectType::from([(
                    "bar".to_string(),
                    JsonNativeType::Bool,
                )])),
            )]))),
            Ok(r#"{"nested":{"bar":"<Bool>","foo":"<String>"}}"#),
        )?;

        // Arrays should merge their element types recursively.
        test(
            r#"["foo"]"#,
            &mut JsonType::new_json2(JsonNativeType::Array(Box::new(JsonNativeType::u64()))),
            Ok(r#"["<Variant>"]"#),
        )?;

        // Root-level incompatible types should be lifted to Variant.
        test(
            r#"{"foo":"bar"}"#,
            &mut JsonType::new_json2(JsonNativeType::Bool),
            Ok(r#""<Variant>""#),
        )?;

        // Jsonb and Json2 should not be mergeable.
        test(
            "true",
            &mut JsonType::new(JsonFormat::Jsonb),
            Err("Failed to merge JSON datatype: json format not match"),
        )?;

        Ok(())
    }
}
