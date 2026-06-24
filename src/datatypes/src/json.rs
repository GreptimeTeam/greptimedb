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

//! Data conversion between greptime's StructType and Json
//!
//! The idea of this module is to provide utilities to convert serde_json::Value to greptime's StructType and vice versa.
//!
//! The struct will carry all the fields of the Json object. We will not flatten any json object in this implementation.
//!

pub mod value;

use std::collections::BTreeMap;
use std::collections::btree_map::Entry;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value as Json};
use snafu::ResultExt;

use crate::data_type::ConcreteDataType;
use crate::error::{self, Result};
use crate::json::value::{JsonValue, JsonVariant};
use crate::schema::ColumnDefaultConstraint;
use crate::types::json_type::JsonNativeType;
use crate::value::{ListValue, StructValue, Value};

/// JSON2 settings stored in column schema metadata and represented through
/// Arrow extension metadata.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct JsonSettings {
    #[serde(default)]
    pub type_hints: Vec<JsonTypeHint>,
}

/// Declares selected JSON2 subpaths as typed fields.
///
/// These hints let JSON2 encode frequently used subpaths in a typed layout, so
/// queries over those subpaths can get behavior and performance closer to
/// ordinary columns.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JsonTypeHint {
    /// JSON2 subpath for a typed field.
    ///
    /// Each item is one JSON object key. For example, `["user", "age"]`
    /// represents `user.age`.
    ///
    /// Array traversal is not currently supported. For example, a hint cannot
    /// describe `events[0].name` or fields shared by all items in `events[*]`.
    pub path: Vec<String>,
    #[serde(rename = "type")]
    pub data_type: ConcreteDataType,
    pub nullable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_constraint: Option<ColumnDefaultConstraint>,
    pub inverted_index: bool,
}

/// Context for JSON encoding/decoding that tracks the current key path.
#[derive(Clone, Debug)]
pub struct JsonContext<'a> {
    /// Current key path from the JSON2 root.
    pub path: Vec<String>,
    /// Settings for JSON encoding/decoding.
    pub settings: &'a JsonSettings,
}

impl JsonSettings {
    pub fn new(type_hints: Vec<JsonTypeHint>) -> Self {
        Self { type_hints }
    }

    /// Decode an encoded StructValue back into a serde_json::Value.
    pub fn decode(&self, value: Value) -> Result<Json> {
        let context = JsonContext {
            path: Vec::new(),
            settings: self,
        };
        decode_value_with_context(value, &context)
    }

    /// Encode a serde_json::Value into a Value::Json using current settings.
    pub fn encode(&self, json: Json) -> Result<Value> {
        let context = JsonContext {
            path: Vec::new(),
            settings: self,
        };
        encode_json_with_context(json, &context).map(|v| Value::Json(Box::new(v)))
    }
}

impl<'a> JsonContext<'a> {
    /// Create a new context with an updated key path
    pub fn with_key(&self, key: &str) -> JsonContext<'a> {
        let mut path = self.path.clone();
        path.push(key.to_string());
        JsonContext {
            path,
            settings: self.settings,
        }
    }

    fn type_hint(&self) -> Option<&'a JsonTypeHint> {
        self.settings
            .type_hints
            .iter()
            .find(|hint| hint.path == self.path)
    }
}

/// Main encoding function with key path tracking
pub fn encode_json_with_context<'a>(json: Json, context: &JsonContext<'a>) -> Result<JsonValue> {
    match json {
        Json::Object(json_object) => encode_json_object_with_context(json_object, context),
        Json::Array(json_array) => encode_json_array_with_context(json_array, context),
        _ => encode_json_value_with_context(json, context),
    }
}

fn encode_json_object_with_context<'a>(
    json_object: Map<String, Json>,
    context: &JsonContext<'a>,
) -> Result<JsonValue> {
    let mut object = BTreeMap::new();
    for (key, value) in json_object {
        let field_context = context.with_key(&key);

        let value = if let Some(hint) = field_context.type_hint() {
            encode_json_value_with_hint(value, hint, &field_context)?
        } else {
            encode_json_value_with_context(value, &field_context)?
        };

        object.insert(key, value.into_variant());
    }

    apply_missing_type_hints(&mut object, context)?;

    Ok(JsonValue::new(JsonVariant::Object(object)))
}

fn apply_missing_type_hints(
    object: &mut BTreeMap<String, JsonVariant>,
    context: &JsonContext,
) -> Result<()> {
    for hint in &context.settings.type_hints {
        if hint.path.len() > context.path.len() && hint.path.starts_with(&context.path) {
            insert_missing_type_hint(object, context, hint, context.path.len())?;
        }
    }
    Ok(())
}

fn insert_missing_type_hint(
    object: &mut BTreeMap<String, JsonVariant>,
    context: &JsonContext,
    hint: &JsonTypeHint,
    depth: usize,
) -> Result<()> {
    let key = &hint.path[depth];
    let field_context = context.with_key(key);
    let is_leaf = depth + 1 == hint.path.len();

    if is_leaf {
        if !object.contains_key(key) {
            let value = encode_missing_type_hint_value(hint, &field_context)?;
            object.insert(key.clone(), value.into_variant());
        }
        return Ok(());
    }

    match object.entry(key.clone()) {
        Entry::Occupied(mut entry) => match entry.get_mut() {
            JsonVariant::Object(child) => {
                insert_missing_type_hint(child, &field_context, hint, depth + 1)
            }
            _ => error::InvalidJsonSnafu {
                value: format!(
                    "JSON2 type hint path {} expects object at {}",
                    hint.path.join("."),
                    field_context.path.join(".")
                ),
            }
            .fail(),
        },
        Entry::Vacant(entry) => {
            let mut child = BTreeMap::new();
            insert_missing_type_hint(&mut child, &field_context, hint, depth + 1)?;
            entry.insert(JsonVariant::Object(child));
            Ok(())
        }
    }
}

fn encode_missing_type_hint_value(hint: &JsonTypeHint, context: &JsonContext) -> Result<JsonValue> {
    if let Some(default_constraint) = &hint.default_constraint {
        let value = default_constraint.create_default(&hint.data_type, hint.nullable)?;
        let json = decode_primitive_value(value)?;
        return encode_json_value_with_hint(json, hint, context);
    }

    if hint.nullable {
        Ok(JsonValue::null())
    } else {
        error::InvalidJsonSnafu {
            value: format!(
                "missing non-null JSON2 type hint path {}",
                hint.path.join(".")
            ),
        }
        .fail()
    }
}

fn encode_json_value_with_hint(
    json: Json,
    hint: &JsonTypeHint,
    context: &JsonContext,
) -> Result<JsonValue> {
    if json.is_null() {
        return if hint.nullable {
            Ok(JsonValue::null())
        } else {
            error::InvalidJsonSnafu {
                value: format!(
                    "JSON2 type hint path {} is not nullable",
                    context.path.join(".")
                ),
            }
            .fail()
        };
    }

    let invalid_type = || {
        error::InvalidJsonSnafu {
            value: format!(
                "JSON value at {} does not match JSON2 type hint {}",
                context.path.join("."),
                hint.data_type
            ),
        }
        .fail()
    };

    match (&hint.data_type, json) {
        (ConcreteDataType::String(_), Json::String(v)) => Ok(v.into()),
        (
            ConcreteDataType::Int8(_)
            | ConcreteDataType::Int16(_)
            | ConcreteDataType::Int32(_)
            | ConcreteDataType::Int64(_),
            Json::Number(v),
        ) => match v.as_i64() {
            Some(v) => Ok(v.into()),
            None => invalid_type(),
        },
        (
            ConcreteDataType::UInt8(_)
            | ConcreteDataType::UInt16(_)
            | ConcreteDataType::UInt32(_)
            | ConcreteDataType::UInt64(_),
            Json::Number(v),
        ) => match v.as_u64() {
            Some(v) => Ok(v.into()),
            None => invalid_type(),
        },
        (ConcreteDataType::Float32(_) | ConcreteDataType::Float64(_), Json::Number(v)) => {
            match v.as_f64() {
                Some(v) => Ok(v.into()),
                None => invalid_type(),
            }
        }
        (ConcreteDataType::Boolean(_), Json::Bool(v)) => Ok(v.into()),
        _ => invalid_type(),
    }
}

fn encode_json_array_with_context<'a>(
    json_array: Vec<Json>,
    context: &JsonContext<'a>,
) -> Result<JsonValue> {
    let json_array_len = json_array.len();
    let mut items = Vec::with_capacity(json_array_len);

    for (index, value) in json_array.into_iter().enumerate() {
        let array_context = context.with_key(&index.to_string());
        let item_value = encode_json_value_with_context(value, &array_context)?;
        items.push(item_value);
    }

    // In specification, it's valid for a JSON array to have different types of items, for example,
    // ["a string", 1]. However, in implementation, the `JsonValue` will be converted to Arrow list
    // array, which requires all items have exactly the same type. So we merge out the maybe
    // different item types to a unified type, and align all the item values to it.

    let merged_item_type = if let Some((first, rests)) = items.split_first() {
        let mut merged = first.json_type().clone();
        for rest in rests.iter().map(|x| x.json_type()) {
            if matches!(merged.native_type(), JsonNativeType::Variant) {
                break;
            }
            merged.merge(rest)?;
        }
        Some(merged)
    } else {
        None
    };
    let unified_item_type = merged_item_type;
    if let Some(unified_item_type) = unified_item_type {
        for item in &mut items {
            item.try_align(&unified_item_type)?;
        }
    }
    let items = items
        .into_iter()
        .map(|x| x.into_variant())
        .collect::<Vec<_>>();
    Ok(JsonValue::new(JsonVariant::Array(items)))
}

/// Helper function to encode a JSON value to a Value and determine its ConcreteDataType with context
fn encode_json_value_with_context<'a>(json: Json, context: &JsonContext<'a>) -> Result<JsonValue> {
    match json {
        Json::Null => Ok(JsonValue::null()),
        Json::Bool(b) => Ok(b.into()),
        Json::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(i.into())
            } else if let Some(u) = n.as_u64() {
                if u <= i64::MAX as u64 {
                    Ok((u as i64).into())
                } else {
                    Ok(u.into())
                }
            } else if let Some(f) = n.as_f64() {
                Ok(f.into())
            } else {
                // Fallback to string representation
                Ok(n.to_string().into())
            }
        }
        Json::String(s) => Ok(s.into()),
        Json::Array(arr) => encode_json_array_with_context(arr, context),
        Json::Object(obj) => encode_json_object_with_context(obj, context),
    }
}

/// Main decoding function with key path tracking
pub fn decode_value_with_context(value: Value, context: &JsonContext) -> Result<Json> {
    match value {
        Value::Struct(struct_value) => decode_struct_with_context(struct_value, context),
        Value::List(list_value) => decode_list_with_context(list_value, context),
        _ => decode_primitive_value(value),
    }
}

/// Decode a structured value to JSON object
fn decode_struct_with_context<'a>(
    struct_value: StructValue,
    context: &JsonContext<'a>,
) -> Result<Json> {
    let mut json_object = Map::with_capacity(struct_value.len());

    let (items, fields) = struct_value.into_parts();

    for (field, field_value) in fields.fields().iter().zip(items) {
        let field_context = context.with_key(field.name());
        let json_value = decode_value_with_context(field_value, &field_context)?;
        json_object.insert(field.name().to_string(), json_value);
    }

    Ok(Json::Object(json_object))
}

/// Decode a list value to JSON array
fn decode_list_with_context(list_value: ListValue, context: &JsonContext) -> Result<Json> {
    let mut json_array = Vec::with_capacity(list_value.len());

    let data_items = list_value.take_items();

    for (index, item) in data_items.into_iter().enumerate() {
        let array_context = context.with_key(&index.to_string());
        let json_value = decode_value_with_context(item, &array_context)?;
        json_array.push(json_value);
    }

    Ok(Json::Array(json_array))
}

/// Decode primitive value to JSON
fn decode_primitive_value(value: Value) -> Result<Json> {
    match value {
        Value::Null => Ok(Json::Null),
        Value::Boolean(b) => Ok(Json::Bool(b)),
        Value::UInt8(v) => Ok(Json::from(v)),
        Value::UInt16(v) => Ok(Json::from(v)),
        Value::UInt32(v) => Ok(Json::from(v)),
        Value::UInt64(v) => Ok(Json::from(v)),
        Value::Int8(v) => Ok(Json::from(v)),
        Value::Int16(v) => Ok(Json::from(v)),
        Value::Int32(v) => Ok(Json::from(v)),
        Value::Int64(v) => Ok(Json::from(v)),
        Value::Float32(v) => Ok(Json::from(v.0)),
        Value::Float64(v) => Ok(Json::from(v.0)),
        Value::String(s) => Ok(Json::String(s.as_utf8().to_string())),
        Value::Binary(b) => serde_json::to_value(b.as_ref()).context(error::SerializeSnafu),
        Value::Date(v) => Ok(Json::from(v.val())),
        Value::Timestamp(v) => serde_json::to_value(v.value()).context(error::SerializeSnafu),
        Value::Time(v) => serde_json::to_value(v.value()).context(error::SerializeSnafu),
        Value::IntervalYearMonth(v) => {
            serde_json::to_value(v.to_i32()).context(error::SerializeSnafu)
        }
        Value::IntervalDayTime(v) => {
            serde_json::to_value(v.to_i64()).context(error::SerializeSnafu)
        }
        Value::IntervalMonthDayNano(v) => {
            serde_json::to_value(v.to_i128()).context(error::SerializeSnafu)
        }
        Value::Duration(v) => serde_json::to_value(v.value()).context(error::SerializeSnafu),
        Value::Decimal128(v) => serde_json::to_value(v.to_string()).context(error::SerializeSnafu),
        Value::Struct(_) | Value::List(_) | Value::Json(_) => {
            // These should be handled by the context-aware functions
            Err(error::InvalidJsonSnafu {
                value: "Structured values should be handled by context-aware decoding".to_string(),
            }
            .build())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde_json::json;

    use super::*;
    use crate::data_type::ConcreteDataType;
    use crate::types::{ListType, StructField, StructType};

    fn struct_field_value<'a>(struct_value: &'a StructValue, field_name: &str) -> &'a Value {
        let index = struct_value
            .struct_type()
            .fields()
            .iter()
            .position(|field| field.name() == field_name)
            .expect("field exists");
        &struct_value.items()[index]
    }

    #[test]
    fn test_json_settings_ser_de() {
        let settings = JsonSettings::new(vec![
            JsonTypeHint {
                path: vec!["user".to_string(), "age".to_string()],
                data_type: ConcreteDataType::int64_datatype(),
                nullable: false,
                default_constraint: Some(ColumnDefaultConstraint::Value(Value::Int64(18))),
                inverted_index: true,
            },
            JsonTypeHint {
                path: vec!["user".to_string(), "name".to_string()],
                data_type: ConcreteDataType::string_datatype(),
                nullable: true,
                default_constraint: None,
                inverted_index: false,
            },
        ]);

        let serialized = serde_json::to_string(&settings).unwrap();
        let deserialized = serde_json::from_str::<JsonSettings>(&serialized).unwrap();

        assert_eq!(settings, deserialized);
    }

    #[test]
    fn test_encode_json_null() {
        let json = Json::Null;
        let settings = JsonSettings::default();
        let result = settings.encode(json).unwrap().into_json_inner().unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_encode_json_boolean() {
        let json = Json::Bool(true);
        let settings = JsonSettings::default();
        let result = settings.encode(json).unwrap().into_json_inner().unwrap();
        assert_eq!(result, Value::Boolean(true));
    }

    #[test]
    fn test_encode_json_number_integer() {
        let json = Json::from(42);
        let settings = JsonSettings::default();
        let result = settings.encode(json).unwrap().into_json_inner().unwrap();
        assert_eq!(result, Value::Int64(42));
    }

    #[test]
    fn test_encode_json_number_float() {
        let json = Json::from(3.15);
        let settings = JsonSettings::default();
        let result = settings.encode(json).unwrap().into_json_inner().unwrap();
        match result {
            Value::Float64(f) => assert_eq!(f.0, 3.15),
            _ => panic!("Expected Float64"),
        }
    }

    #[test]
    fn test_encode_json_string() {
        let json = Json::String("hello".to_string());
        let settings = JsonSettings::default();
        let result = settings.encode(json).unwrap().into_json_inner().unwrap();
        assert_eq!(result, Value::String("hello".into()));
    }

    #[test]
    fn test_encode_json_array() {
        let json = json!([1, 2, 3]);
        let settings = JsonSettings::default();
        let result = settings.encode(json).unwrap().into_json_inner().unwrap();

        if let Value::List(list_value) = result {
            assert_eq!(list_value.items().len(), 3);
            assert_eq!(list_value.items()[0], Value::Int64(1));
            assert_eq!(list_value.items()[1], Value::Int64(2));
            assert_eq!(list_value.items()[2], Value::Int64(3));
        } else {
            panic!("Expected List value");
        }
    }

    #[test]
    fn test_encode_json_object() {
        let json = json!({
            "name": "John",
            "age": 30,
            "active": true
        });

        let settings = JsonSettings::default();
        let result = settings.encode(json).unwrap().into_json_inner().unwrap();
        let Value::Struct(result) = result else {
            panic!("Expected Struct value");
        };
        assert_eq!(result.items().len(), 3);

        let items = result.items();
        let struct_type = result.struct_type();

        // Check that we have the expected fields
        let fields = struct_type.fields();
        let field_names: Vec<&str> = fields.iter().map(|f| f.name()).collect();
        assert!(field_names.contains(&"name"));
        assert!(field_names.contains(&"age"));
        assert!(field_names.contains(&"active"));

        // Find and check each field
        for (i, field) in struct_type.fields().iter().enumerate() {
            match field.name() {
                "name" => {
                    assert_eq!(items[i], Value::String("John".into()));
                    assert_eq!(field.data_type(), &ConcreteDataType::string_datatype());
                }
                "age" => {
                    assert_eq!(items[i], Value::Int64(30));
                    assert_eq!(field.data_type(), &ConcreteDataType::int64_datatype());
                }
                "active" => {
                    assert_eq!(items[i], Value::Boolean(true));
                    assert_eq!(field.data_type(), &ConcreteDataType::boolean_datatype());
                }
                _ => panic!("Unexpected field: {}", field.name()),
            }
        }
    }

    #[test]
    fn test_encode_json_nested_object() {
        let json = json!({
            "person": {
                "name": "Alice",
                "age": 25
            },
            "scores": [95, 87, 92]
        });

        let settings = JsonSettings::default();
        let result = settings.encode(json).unwrap().into_json_inner().unwrap();
        let Value::Struct(result) = result else {
            panic!("Expected Struct value");
        };
        assert_eq!(result.items().len(), 2);

        let items = result.items();
        let struct_type = result.struct_type();

        // Check person field (nested struct)
        let person_index = struct_type
            .fields()
            .iter()
            .position(|f| f.name() == "person")
            .unwrap();
        if let Value::Struct(person_struct) = &items[person_index] {
            assert_eq!(person_struct.items().len(), 2);
            let fields = person_struct.struct_type().fields();
            let person_fields: Vec<&str> = fields.iter().map(|f| f.name()).collect();
            assert!(person_fields.contains(&"name"));
            assert!(person_fields.contains(&"age"));
        } else {
            panic!("Expected Struct value for person field");
        }

        // Check scores field (list)
        let scores_index = struct_type
            .fields()
            .iter()
            .position(|f| f.name() == "scores")
            .unwrap();
        if let Value::List(scores_list) = &items[scores_index] {
            assert_eq!(scores_list.items().len(), 3);
            assert_eq!(scores_list.items()[0], Value::Int64(95));
            assert_eq!(scores_list.items()[1], Value::Int64(87));
            assert_eq!(scores_list.items()[2], Value::Int64(92));
        } else {
            panic!("Expected List value for scores field");
        }
    }

    #[test]
    fn test_encode_json_array_mixed_types() {
        let json = json!([1, "hello", true, 3.15]);
        let settings = JsonSettings::default();
        let value = settings.encode(json).unwrap();
        assert_eq!(value.data_type().to_string(), r#"Json2["<Variant>"]"#);
    }

    #[test]
    fn test_encode_json_empty_array() {
        let json = json!([]);
        let settings = JsonSettings::default();
        let result = settings.encode(json).unwrap().into_json_inner().unwrap();

        if let Value::List(list_value) = result {
            assert_eq!(list_value.items().len(), 0);
            // Empty arrays default to string type
            assert_eq!(
                list_value.datatype(),
                Arc::new(ConcreteDataType::null_datatype())
            );
        } else {
            panic!("Expected List value");
        }
    }

    #[test]
    fn test_encode_json_structured() {
        let json = json!({
            "name": "Bob",
            "age": 35
        });

        let settings = JsonSettings::default();
        let result = settings.encode(json).unwrap().into_json_inner().unwrap();

        if let Value::Struct(struct_value) = result {
            assert_eq!(struct_value.items().len(), 2);
            let fields = struct_value.struct_type().fields();
            let field_names: Vec<&str> = fields.iter().map(|f| f.name()).collect();
            assert!(field_names.contains(&"name"));
            assert!(field_names.contains(&"age"));
        } else {
            panic!("Expected Struct value");
        }
    }

    #[test]
    fn test_encode_json_respects_type_hint() {
        let settings = JsonSettings::new(vec![JsonTypeHint {
            path: vec!["age".to_string()],
            data_type: ConcreteDataType::int64_datatype(),
            nullable: false,
            default_constraint: None,
            inverted_index: false,
        }]);

        let result = settings
            .encode(json!({
                "name": "Alice",
                "age": 42
            }))
            .unwrap()
            .into_json_inner()
            .unwrap();

        let Value::Struct(struct_value) = result else {
            panic!("Expected Struct value");
        };
        assert_eq!(struct_field_value(&struct_value, "age"), &Value::Int64(42));

        let err = settings
            .encode(json!({
                "age": "42"
            }))
            .unwrap_err();
        assert!(err.to_string().contains("does not match JSON2 type hint"));
    }

    #[test]
    fn test_encode_json_respects_unsigned_type_hint() {
        let settings = JsonSettings::new(vec![JsonTypeHint {
            path: vec!["count".to_string()],
            data_type: ConcreteDataType::uint64_datatype(),
            nullable: false,
            default_constraint: None,
            inverted_index: false,
        }]);

        let result = settings
            .encode(json!({
                "count": u64::MAX
            }))
            .unwrap()
            .into_json_inner()
            .unwrap();

        let Value::Struct(struct_value) = result else {
            panic!("Expected Struct value");
        };
        assert_eq!(
            struct_field_value(&struct_value, "count"),
            &Value::UInt64(u64::MAX)
        );

        let err = settings
            .encode(json!({
                "count": -1
            }))
            .unwrap_err();
        assert!(err.to_string().contains("does not match JSON2 type hint"));
    }

    #[test]
    fn test_encode_json_fills_missing_type_hint_with_default() {
        let settings = JsonSettings::new(vec![JsonTypeHint {
            path: vec!["user".to_string(), "age".to_string()],
            data_type: ConcreteDataType::int64_datatype(),
            nullable: false,
            default_constraint: Some(ColumnDefaultConstraint::Value(Value::Int64(7))),
            inverted_index: false,
        }]);

        let result = settings
            .encode(json!({}))
            .unwrap()
            .into_json_inner()
            .unwrap();

        let Value::Struct(root) = result else {
            panic!("Expected Struct value");
        };
        let Value::Struct(user) = struct_field_value(&root, "user") else {
            panic!("Expected user Struct value");
        };
        assert_eq!(struct_field_value(user, "age"), &Value::Int64(7));
    }

    #[test]
    fn test_encode_json_fills_missing_nullable_type_hint_with_null() {
        let settings = JsonSettings::new(vec![JsonTypeHint {
            path: vec!["user".to_string(), "name".to_string()],
            data_type: ConcreteDataType::string_datatype(),
            nullable: true,
            default_constraint: None,
            inverted_index: false,
        }]);

        let result = settings
            .encode(json!({ "user": {} }))
            .unwrap()
            .into_json_inner()
            .unwrap();

        let Value::Struct(root) = result else {
            panic!("Expected Struct value");
        };
        let Value::Struct(user) = struct_field_value(&root, "user") else {
            panic!("Expected user Struct value");
        };
        assert_eq!(struct_field_value(user, "name"), &Value::Null);
    }

    #[test]
    fn test_encode_json_rejects_missing_non_null_type_hint() {
        let settings = JsonSettings::new(vec![JsonTypeHint {
            path: vec!["user".to_string(), "age".to_string()],
            data_type: ConcreteDataType::int64_datatype(),
            nullable: false,
            default_constraint: None,
            inverted_index: false,
        }]);

        let err = settings.encode(json!({})).unwrap_err();
        assert!(
            err.to_string()
                .contains("missing non-null JSON2 type hint path user.age")
        );
    }

    #[test]
    fn test_encode_json_merges_missing_type_hint_prefix() {
        let settings = JsonSettings::new(vec![
            JsonTypeHint {
                path: vec!["user".to_string(), "age".to_string()],
                data_type: ConcreteDataType::int64_datatype(),
                nullable: false,
                default_constraint: Some(ColumnDefaultConstraint::Value(Value::Int64(7))),
                inverted_index: false,
            },
            JsonTypeHint {
                path: vec!["user".to_string(), "name".to_string()],
                data_type: ConcreteDataType::string_datatype(),
                nullable: false,
                default_constraint: Some(ColumnDefaultConstraint::Value(Value::String(
                    "unknown".into(),
                ))),
                inverted_index: false,
            },
        ]);

        let result = settings
            .encode(json!({}))
            .unwrap()
            .into_json_inner()
            .unwrap();

        let Value::Struct(root) = result else {
            panic!("Expected Struct value");
        };
        let Value::Struct(user) = struct_field_value(&root, "user") else {
            panic!("Expected user Struct value");
        };
        assert_eq!(struct_field_value(user, "age"), &Value::Int64(7));
        assert_eq!(
            struct_field_value(user, "name"),
            &Value::String("unknown".into())
        );
    }

    #[test]
    fn test_json_settings_structured() {
        let json = json!({
            "name": "Eve",
            "score": 95
        });

        let settings = JsonSettings::default();
        let result = settings.encode(json).unwrap().into_json_inner().unwrap();

        if let Value::Struct(struct_value) = result {
            assert_eq!(struct_value.items().len(), 2);
        } else {
            panic!("Expected Struct value");
        }
    }

    #[cfg(test)]
    mod decode_tests {
        use ordered_float::OrderedFloat;
        use serde_json::json;

        use super::*;

        #[test]
        fn test_decode_primitive_values() {
            let settings = JsonSettings::default();

            // Test null
            let result = settings.decode(Value::Null).unwrap();
            assert_eq!(result, Json::Null);

            // Test boolean
            let result = settings.decode(Value::Boolean(true)).unwrap();
            assert_eq!(result, Json::Bool(true));

            // Test integer
            let result = settings.decode(Value::Int64(42)).unwrap();
            assert_eq!(result, Json::from(42));

            // Test float
            let result = settings.decode(Value::Float64(OrderedFloat(3.16))).unwrap();
            assert_eq!(result, Json::from(3.16));

            // Test string
            let result = settings.decode(Value::String("hello".into())).unwrap();
            assert_eq!(result, Json::String("hello".to_string()));
        }

        #[test]
        fn test_decode_struct() {
            let settings = JsonSettings::default();

            let struct_value = StructValue::new(
                vec![
                    Value::String("Alice".into()),
                    Value::Int64(25),
                    Value::Boolean(true),
                ],
                StructType::new(Arc::new(vec![
                    StructField::new(
                        "name".to_string(),
                        ConcreteDataType::string_datatype(),
                        true,
                    ),
                    StructField::new("age".to_string(), ConcreteDataType::int64_datatype(), true),
                    StructField::new(
                        "active".to_string(),
                        ConcreteDataType::boolean_datatype(),
                        true,
                    ),
                ])),
            );

            let result = settings.decode(Value::Struct(struct_value)).unwrap();
            let expected = json!({
                "name": "Alice",
                "age": 25,
                "active": true
            });
            assert_eq!(result, expected);
        }

        #[test]
        fn test_decode_list() {
            let settings = JsonSettings::default();

            let list_value = ListValue::new(
                vec![Value::Int64(1), Value::Int64(2), Value::Int64(3)],
                Arc::new(ConcreteDataType::int64_datatype()),
            );

            let result = settings.decode(Value::List(list_value)).unwrap();
            let expected = json!([1, 2, 3]);
            assert_eq!(result, expected);
        }

        #[test]
        fn test_decode_nested_structure() {
            let settings = JsonSettings::default();

            let inner_struct = StructValue::new(
                vec![Value::String("Alice".into()), Value::Int64(25)],
                StructType::new(Arc::new(vec![
                    StructField::new(
                        "name".to_string(),
                        ConcreteDataType::string_datatype(),
                        true,
                    ),
                    StructField::new("age".to_string(), ConcreteDataType::int64_datatype(), true),
                ])),
            );

            let score_list_item_type = Arc::new(ConcreteDataType::int64_datatype());
            let outer_struct = StructValue::new(
                vec![
                    Value::Struct(inner_struct),
                    Value::List(ListValue::new(
                        vec![Value::Int64(95), Value::Int64(87)],
                        score_list_item_type.clone(),
                    )),
                ],
                StructType::new(Arc::new(vec![
                    StructField::new(
                        "user".to_string(),
                        ConcreteDataType::Struct(StructType::new(Arc::new(vec![
                            StructField::new(
                                "name".to_string(),
                                ConcreteDataType::string_datatype(),
                                true,
                            ),
                            StructField::new(
                                "age".to_string(),
                                ConcreteDataType::int64_datatype(),
                                true,
                            ),
                        ]))),
                        true,
                    ),
                    StructField::new(
                        "scores".to_string(),
                        ConcreteDataType::List(ListType::new(score_list_item_type.clone())),
                        true,
                    ),
                ])),
            );

            let result = settings.decode(Value::Struct(outer_struct)).unwrap();
            let expected = json!({
                "user": {
                    "name": "Alice",
                    "age": 25
                },
                "scores": [95, 87]
            });
            assert_eq!(result, expected);
        }

        #[test]
        fn test_decode_missing_fields() {
            let settings = JsonSettings::default();

            // Struct with missing field (null value)
            let struct_value = StructValue::new(
                vec![
                    Value::String("Bob".into()),
                    Value::Null, // missing age field
                ],
                StructType::new(Arc::new(vec![
                    StructField::new(
                        "name".to_string(),
                        ConcreteDataType::string_datatype(),
                        true,
                    ),
                    StructField::new("age".to_string(), ConcreteDataType::int64_datatype(), true),
                ])),
            );

            let result = settings.decode(Value::Struct(struct_value)).unwrap();
            let expected = json!({
                "name": "Bob",
                "age": null
            });
            assert_eq!(result, expected);
        }
    }

    #[test]
    fn test_encode_json_large_unsigned_integer() {
        // Test unsigned integer that fits in i64
        let json = Json::from(u64::MAX / 2);
        let settings = JsonSettings::default();
        let result = settings.encode(json).unwrap().into_json_inner().unwrap();
        assert_eq!(result, Value::Int64((u64::MAX / 2) as i64));

        // Test unsigned integer that exceeds i64 range
        let json = Json::from(u64::MAX);
        let result = settings.encode(json).unwrap().into_json_inner().unwrap();
        assert_eq!(result, Value::UInt64(u64::MAX));
    }
}
