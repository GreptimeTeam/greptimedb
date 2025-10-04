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
//

//! Data conversion between greptime's StructType and Json
//!
//! The idea of this module is to provide utilities to convert serde_json::Value to greptime's StructType and vice versa.
//!
//! The struct will carry all the fields of the Json object. We will not flatten any json object in this implementation.
//!

use std::collections::HashSet;

use common_base::bytes::StringBytes;
use ordered_float::OrderedFloat;
use serde_json::{Map, Value as Json};

use crate::data_type::{ConcreteDataType, DataType};
use crate::error::{self, Error};
use crate::types::{ListType, StructField, StructType};
use crate::value::{ListValue, StructValue, Value};

#[derive(Debug, Clone)]
pub enum JsonStructureSettings {
    // TODO(sunng87): provide a limit
    Structured(Option<StructType>),
    UnstructuredRaw,
    PartialUnstructuredByKey {
        fields: Option<StructType>,
        unstructured_keys: HashSet<String>,
    },
    // TODO(sung87): add a new setting to flatten nested objects
}

/// Context for JSON encoding that tracks the current key path
#[derive(Clone, Debug)]
pub struct JsonEncodeContext<'a> {
    /// Current key path in dot notation (e.g., "user.profile.name")
    pub key_path: String,
    /// Settings for JSON structure handling
    pub settings: &'a JsonStructureSettings,
}

impl JsonStructureSettings {
    pub fn encode(&self, json: &Json) -> Result<Value, Error> {
        let context = JsonEncodeContext {
            key_path: String::new(),
            settings: self,
        };
        encode_json_with_context(json, None, &context)
    }

    pub fn encode_with_type(
        &self,
        json: &Json,
        data_type: Option<&ConcreteDataType>,
    ) -> Result<Value, Error> {
        let context = JsonEncodeContext {
            key_path: String::new(),
            settings: self,
        };
        encode_json_with_context(json, data_type, &context)
    }
}

impl<'a> JsonEncodeContext<'a> {
    /// Create a new context with an updated key path
    pub fn with_key(&self, key: &str) -> JsonEncodeContext<'a> {
        let new_key_path = if self.key_path.is_empty() {
            key.to_string()
        } else {
            format!("{}.{}", self.key_path, key)
        };
        JsonEncodeContext {
            key_path: new_key_path,
            settings: self.settings,
        }
    }

    /// Check if the current key path should be treated as unstructured
    pub fn is_unstructured_key(&self) -> bool {
        match &self.settings {
            JsonStructureSettings::PartialUnstructuredByKey {
                unstructured_keys, ..
            } => unstructured_keys.contains(&self.key_path),
            _ => false,
        }
    }
}

/// Main encoding function with key path tracking
pub fn encode_json_with_context<'a>(
    json: &Json,
    data_type: Option<&ConcreteDataType>,
    context: &JsonEncodeContext<'a>,
) -> Result<Value, Error> {
    // Check if the entire encoding should be unstructured
    if matches!(context.settings, JsonStructureSettings::UnstructuredRaw) {
        return Ok(Value::String(json.to_string().into()));
    }

    // Check if current key should be treated as unstructured
    if context.is_unstructured_key() {
        return Ok(Value::String(json.to_string().into()));
    }

    match json {
        Json::Object(json_object) => {
            if let Some(ConcreteDataType::Struct(struct_type)) = data_type {
                let struct_value =
                    encode_json_object_with_context(json_object, Some(struct_type), context)?;
                Ok(Value::Struct(struct_value))
            } else if data_type.is_some() {
                Err(error::InvalidJsonSnafu {
                    value: "JSON object can only be encoded to Struct type".to_string(),
                }
                .build())
            } else {
                let struct_value = encode_json_object_with_context(json_object, None, context)?;
                Ok(Value::Struct(struct_value))
            }
        }
        Json::Array(json_array) => {
            let item_type = if let Some(ConcreteDataType::List(list_type)) = data_type {
                Some(list_type.item_type())
            } else {
                None
            };
            let list_value =
                encode_json_array_with_context(json_array.as_ref(), item_type, context)?;
            Ok(Value::List(list_value))
        }
        _ => {
            // For non-collection types, verify type compatibility
            if let Some(expected_type) = data_type {
                let (value, actual_type) =
                    encode_json_value_with_context(json, Some(expected_type), context)?;
                if &actual_type == expected_type {
                    Ok(value)
                } else {
                    Err(error::InvalidJsonSnafu {
                        value: format!(
                            "JSON value type {} does not match expected type {}",
                            actual_type.name(),
                            expected_type.name()
                        ),
                    }
                    .build())
                }
            } else {
                let (value, _) = encode_json_value_with_context(json, None, context)?;
                Ok(value)
            }
        }
    }
}

fn encode_json_object_with_context<'a>(
    json_object: &Map<String, Json>,
    fields: Option<&StructType>,
    context: &JsonEncodeContext<'a>,
) -> Result<StructValue, Error> {
    let mut items = Vec::new();
    let mut struct_fields = Vec::new();
    let mut processed_keys = std::collections::HashSet::new();

    // First, process fields from the provided schema in their original order
    if let Some(fields) = fields {
        for field in fields.fields() {
            let field_name = field.name();
            processed_keys.insert(field_name.to_string());

            if let Some(value) = json_object.get(field_name) {
                let field_context = context.with_key(field_name);
                let (value, data_type) =
                    encode_json_value_with_context(value, Some(field.data_type()), &field_context)?;
                items.push(value);
                struct_fields.push(StructField::new(
                    field_name.to_string(),
                    data_type,
                    true, // JSON fields are always nullable
                ));
            } else {
                // Field exists in schema but not in JSON - add null value
                items.push(Value::Null);
                struct_fields.push(field.clone());
            }
        }
    }

    // Then, process any remaining JSON fields that weren't in the schema
    for (key, value) in json_object {
        if processed_keys.contains(key) {
            continue; // Skip fields already processed from schema
        }

        let field_context = context.with_key(key);

        let (value, data_type) = encode_json_value_with_context(value, None, &field_context)?;
        items.push(value);

        struct_fields.push(StructField::new(
            key.clone(),
            data_type,
            true, // JSON fields are always nullable
        ));
    }

    let struct_type = StructType::new(struct_fields);
    Ok(StructValue::new(items, struct_type))
}

fn encode_json_array_with_context<'a>(
    json_array: &[Json],
    item_type: Option<&ConcreteDataType>,
    context: &JsonEncodeContext<'a>,
) -> Result<ListValue, Error> {
    let mut items = Vec::new();
    let mut element_type = None;

    for (index, value) in json_array.iter().enumerate() {
        let array_context = context.with_key(&index.to_string());
        let (item_value, item_type) =
            encode_json_value_with_context(value, item_type, &array_context)?;
        items.push(item_value);

        // Determine the common type for the list
        if let Some(current_type) = &element_type {
            // For now, we'll use the first non-null type we encounter
            // In a more sophisticated implementation, we might want to find a common supertype
            if *current_type == ConcreteDataType::null_datatype()
                && item_type != ConcreteDataType::null_datatype()
            {
                element_type = Some(item_type);
            }
        } else {
            element_type = Some(item_type);
        }
    }

    // Use provided item_type if available, otherwise determine from elements
    let element_type = if let Some(item_type) = item_type {
        item_type.clone()
    } else {
        element_type.unwrap_or_else(ConcreteDataType::string_datatype)
    };
    let list_type = ListType::new(element_type);

    Ok(ListValue::new(items, ConcreteDataType::List(list_type)))
}

/// Helper function to encode a JSON value to a Value and determine its ConcreteDataType with context
fn encode_json_value_with_context<'a>(
    json: &Json,
    expected_type: Option<&ConcreteDataType>,
    context: &JsonEncodeContext<'a>,
) -> Result<(Value, ConcreteDataType), Error> {
    // Check if current key should be treated as unstructured
    if context.is_unstructured_key() {
        return Ok((
            Value::String(json.to_string().into()),
            ConcreteDataType::string_datatype(),
        ));
    }

    match json {
        Json::Null => Ok((Value::Null, ConcreteDataType::null_datatype())),
        Json::Bool(b) => Ok((Value::Boolean(*b), ConcreteDataType::boolean_datatype())),
        Json::Number(n) => {
            if let Some(i) = n.as_i64() {
                // Use int64 for all integer numbers when possible
                if let Some(expected) = expected_type
                    && let Ok(value) = try_convert_to_expected_type(i, expected)
                {
                    return Ok((value, expected.clone()));
                }
                Ok((Value::Int64(i), ConcreteDataType::int64_datatype()))
            } else if let Some(u) = n.as_u64() {
                // Use int64 for unsigned integers that fit, otherwise use u64
                if let Some(expected) = expected_type
                    && let Ok(value) = try_convert_to_expected_type(u, expected)
                {
                    return Ok((value, expected.clone()));
                }
                if u <= i64::MAX as u64 {
                    Ok((Value::Int64(u as i64), ConcreteDataType::int64_datatype()))
                } else {
                    Ok((Value::UInt64(u), ConcreteDataType::uint64_datatype()))
                }
            } else if let Some(f) = n.as_f64() {
                // Try to use the expected type if provided
                if let Some(expected) = expected_type
                    && let Ok(value) = try_convert_to_expected_type(f, expected)
                {
                    return Ok((value, expected.clone()));
                }

                // Default to f64 for floating point numbers
                Ok((
                    Value::Float64(OrderedFloat(f)),
                    ConcreteDataType::float64_datatype(),
                ))
            } else {
                // Fallback to string representation
                Ok((
                    Value::String(StringBytes::from(n.to_string())),
                    ConcreteDataType::string_datatype(),
                ))
            }
        }
        Json::String(s) => {
            if let Some(expected) = expected_type
                && let Ok(value) = try_convert_to_expected_type(s.as_str(), expected)
            {
                return Ok((value, expected.clone()));
            }
            Ok((
                Value::String(StringBytes::from(s.clone())),
                ConcreteDataType::string_datatype(),
            ))
        }
        Json::Array(arr) => {
            let list_value = encode_json_array_with_context(arr, expected_type, context)?;
            let data_type = list_value.datatype().clone();
            Ok((Value::List(list_value), data_type))
        }
        Json::Object(obj) => {
            let struct_value = encode_json_object_with_context(obj, None, context)?;
            let data_type = ConcreteDataType::Struct(struct_value.struct_type().clone());
            Ok((Value::Struct(struct_value), data_type))
        }
    }
}

/// Helper function to try converting a value to an expected type
fn try_convert_to_expected_type<T>(
    value: T,
    expected_type: &ConcreteDataType,
) -> Result<Value, Error>
where
    T: Into<Value>,
{
    let value = value.into();
    expected_type.try_cast(value.clone()).ok_or_else(|| {
        error::CastTypeSnafu {
            msg: format!(
                "Cannot cast from {} to {}",
                value.data_type().name(),
                expected_type.name()
            ),
        }
        .build()
    })
}

pub fn encode_json_partial_unstructured(
    json: &Json,
    data_type: Option<&ConcreteDataType>,
    keys: &HashSet<String>,
) -> Result<Value, Error> {
    let settings = JsonStructureSettings::PartialUnstructuredByKey {
        fields: None,
        unstructured_keys: keys.clone(),
    };
    let context = JsonEncodeContext {
        key_path: String::new(),
        settings: &settings,
    };
    encode_json_with_context(json, data_type, &context)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn test_encode_json_null() {
        let json = Json::Null;
        let settings = JsonStructureSettings::Structured(None);
        let result = settings.encode_with_type(&json, None).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_encode_json_boolean() {
        let json = Json::Bool(true);
        let settings = JsonStructureSettings::Structured(None);
        let result = settings.encode_with_type(&json, None).unwrap();
        assert_eq!(result, Value::Boolean(true));
    }

    #[test]
    fn test_encode_json_number_integer() {
        let json = Json::from(42);
        let settings = JsonStructureSettings::Structured(None);
        let result = settings.encode_with_type(&json, None).unwrap();
        assert_eq!(result, Value::Int64(42));
    }

    #[test]
    fn test_encode_json_number_float() {
        let json = Json::from(3.15);
        let settings = JsonStructureSettings::Structured(None);
        let result = settings.encode_with_type(&json, None).unwrap();
        match result {
            Value::Float64(f) => assert_eq!(f.0, 3.15),
            _ => panic!("Expected Float64"),
        }
    }

    #[test]
    fn test_encode_json_string() {
        let json = Json::String("hello".to_string());
        let settings = JsonStructureSettings::Structured(None);
        let result = settings.encode_with_type(&json, None).unwrap();
        assert_eq!(result, Value::String("hello".into()));
    }

    #[test]
    fn test_encode_json_array() {
        let json = json!([1, 2, 3]);
        let settings = JsonStructureSettings::Structured(None);
        let result = settings.encode_with_type(&json, None).unwrap();

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

        let settings = JsonStructureSettings::Structured(None);
        let result = settings.encode_with_type(&json, None).unwrap();
        let Value::Struct(result) = result else {
            panic!("Expected Struct value");
        };
        assert_eq!(result.items().len(), 3);

        let items = result.items();
        let struct_type = result.struct_type();

        // Check that we have the expected fields
        let field_names: Vec<&str> = struct_type.fields().iter().map(|f| f.name()).collect();
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

        let settings = JsonStructureSettings::Structured(None);
        let result = settings.encode_with_type(&json, None).unwrap();
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
            let person_fields: Vec<&str> = person_struct
                .struct_type()
                .fields()
                .iter()
                .map(|f| f.name())
                .collect();
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
    fn test_encode_json_with_expected_type() {
        // Test encoding JSON number with expected int8 type
        let json = Json::from(42);
        let settings = JsonStructureSettings::Structured(None);
        let result = settings
            .encode_with_type(&json, Some(&ConcreteDataType::int8_datatype()))
            .unwrap();
        assert_eq!(result, Value::Int8(42));

        // Test with expected string type
        let result = settings
            .encode_with_type(&json, Some(&ConcreteDataType::string_datatype()))
            .unwrap();
        assert_eq!(result, Value::String("42".into()));
    }

    #[test]
    fn test_encode_json_array_mixed_types() {
        let json = json!([1, "hello", true, 3.15]);
        let settings = JsonStructureSettings::Structured(None);
        let result = settings.encode_with_type(&json, None).unwrap();

        if let Value::List(list_value) = result {
            assert_eq!(list_value.items().len(), 4);
            // The first non-null type should determine the list type
            // In this case, it should be string since we can't find a common numeric type
            assert_eq!(
                list_value.datatype(),
                &ConcreteDataType::List(ListType::new(ConcreteDataType::int64_datatype()))
            );
        } else {
            panic!("Expected List value");
        }
    }

    #[test]
    fn test_encode_json_empty_array() {
        let json = json!([]);
        let settings = JsonStructureSettings::Structured(None);
        let result = settings.encode_with_type(&json, None).unwrap();

        if let Value::List(list_value) = result {
            assert_eq!(list_value.items().len(), 0);
            // Empty arrays default to string type
            assert_eq!(
                list_value.datatype(),
                &ConcreteDataType::List(ListType::new(ConcreteDataType::string_datatype()))
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

        let settings = JsonStructureSettings::Structured(None);
        let result = settings.encode(&json).unwrap();

        if let Value::Struct(struct_value) = result {
            assert_eq!(struct_value.items().len(), 2);
            let field_names: Vec<&str> = struct_value
                .struct_type()
                .fields()
                .iter()
                .map(|f| f.name())
                .collect();
            assert!(field_names.contains(&"name"));
            assert!(field_names.contains(&"age"));
        } else {
            panic!("Expected Struct value");
        }
    }

    #[test]
    fn test_encode_json_structured_with_fields() {
        let json = json!({
            "name": "Carol",
            "age": 28
        });

        // Define expected struct type
        let fields = vec![
            StructField::new(
                "name".to_string(),
                ConcreteDataType::string_datatype(),
                true,
            ),
            StructField::new("age".to_string(), ConcreteDataType::int64_datatype(), true),
        ];
        let struct_type = StructType::new(fields);
        let concrete_type = ConcreteDataType::Struct(struct_type);

        let settings = JsonStructureSettings::Structured(None);
        let result = settings
            .encode_with_type(&json, Some(&concrete_type))
            .unwrap();

        if let Value::Struct(struct_value) = result {
            assert_eq!(struct_value.items().len(), 2);
            let struct_fields = struct_value.struct_type().fields();
            assert_eq!(struct_fields[0].name(), "name");
            assert_eq!(
                struct_fields[0].data_type(),
                &ConcreteDataType::string_datatype()
            );
            assert_eq!(struct_fields[1].name(), "age");
            assert_eq!(
                struct_fields[1].data_type(),
                &ConcreteDataType::int64_datatype()
            );
        } else {
            panic!("Expected Struct value");
        }
    }

    #[test]
    fn test_encode_json_object_field_order_preservation() {
        let json = json!({
            "z_field": "last",
            "a_field": "first",
            "m_field": "middle"
        });

        // Define schema with specific field order
        let fields = vec![
            StructField::new(
                "a_field".to_string(),
                ConcreteDataType::string_datatype(),
                true,
            ),
            StructField::new(
                "m_field".to_string(),
                ConcreteDataType::string_datatype(),
                true,
            ),
            StructField::new(
                "z_field".to_string(),
                ConcreteDataType::string_datatype(),
                true,
            ),
        ];
        let struct_type = StructType::new(fields);

        let result = encode_json_object_with_context(
            json.as_object().unwrap(),
            Some(&struct_type),
            &JsonEncodeContext {
                key_path: String::new(),
                settings: &JsonStructureSettings::Structured(None),
            },
        )
        .unwrap();

        // Verify field order is preserved from schema
        let struct_fields = result.struct_type().fields();
        assert_eq!(struct_fields[0].name(), "a_field");
        assert_eq!(struct_fields[1].name(), "m_field");
        assert_eq!(struct_fields[2].name(), "z_field");

        // Verify values are correct
        let items = result.items();
        assert_eq!(items[0], Value::String("first".into()));
        assert_eq!(items[1], Value::String("middle".into()));
        assert_eq!(items[2], Value::String("last".into()));
    }

    #[test]
    fn test_encode_json_object_schema_reuse_with_extra_fields() {
        let json = json!({
            "name": "Alice",
            "age": 25,
            "active": true                 // Extra field not in schema
        });

        // Define schema with only name and age
        let fields = vec![
            StructField::new(
                "name".to_string(),
                ConcreteDataType::string_datatype(),
                true,
            ),
            StructField::new("age".to_string(), ConcreteDataType::int64_datatype(), true),
        ];
        let struct_type = StructType::new(fields);

        let result = encode_json_object_with_context(
            json.as_object().unwrap(),
            Some(&struct_type),
            &JsonEncodeContext {
                key_path: String::new(),
                settings: &JsonStructureSettings::Structured(None),
            },
        )
        .unwrap();

        // Verify schema fields come first in order
        let struct_fields = result.struct_type().fields();
        assert_eq!(struct_fields[0].name(), "name");
        assert_eq!(struct_fields[1].name(), "age");
        assert_eq!(struct_fields[2].name(), "active");

        // Verify values are correct
        let items = result.items();
        assert_eq!(items[0], Value::String("Alice".into()));
        assert_eq!(items[1], Value::Int64(25));
        assert_eq!(items[2], Value::Boolean(true));
    }

    #[test]
    fn test_encode_json_object_missing_schema_fields() {
        let json = json!({
            "name": "Bob"
            // age field is missing from JSON but present in schema
        });

        // Define schema with name and age
        let fields = vec![
            StructField::new(
                "name".to_string(),
                ConcreteDataType::string_datatype(),
                true,
            ),
            StructField::new("age".to_string(), ConcreteDataType::int64_datatype(), true),
        ];
        let struct_type = StructType::new(fields);

        let result = encode_json_object_with_context(
            json.as_object().unwrap(),
            Some(&struct_type),
            &JsonEncodeContext {
                key_path: String::new(),
                settings: &JsonStructureSettings::Structured(None),
            },
        )
        .unwrap();

        // Verify both schema fields are present
        let struct_fields = result.struct_type().fields();
        assert_eq!(struct_fields[0].name(), "name");
        assert_eq!(struct_fields[1].name(), "age");

        // Verify values - name has value, age is null
        let items = result.items();
        assert_eq!(items[0], Value::String("Bob".into()));
        assert_eq!(items[1], Value::Null);
    }

    #[test]
    fn test_encode_json_partial_unstructured() {
        let json = json!({
            "name": "Dave",
            "age": 40
        });

        let result = encode_json_partial_unstructured(&json, None, &HashSet::new()).unwrap();

        if let Value::Struct(s) = result {
            assert_eq!(s.items().len(), 2);
        } else {
            panic!("Expected Struct value");
        }
    }

    #[test]
    fn test_json_structure_settings_structured() {
        let json = json!({
            "name": "Eve",
            "score": 95
        });

        let settings = JsonStructureSettings::Structured(None);
        let result = settings.encode(&json).unwrap();

        if let Value::Struct(struct_value) = result {
            assert_eq!(struct_value.items().len(), 2);
        } else {
            panic!("Expected Struct value");
        }
    }

    #[test]
    fn test_encode_json_array_with_item_type() {
        let json = json!([1, 2, 3]);
        let list_type = ListType::new(ConcreteDataType::int8_datatype());
        let concrete_type = ConcreteDataType::List(list_type);
        let settings = JsonStructureSettings::Structured(None);
        let result = settings
            .encode_with_type(&json, Some(&concrete_type))
            .unwrap();

        if let Value::List(list_value) = result {
            assert_eq!(list_value.items().len(), 3);
            assert_eq!(list_value.items()[0], Value::Int8(1));
            assert_eq!(list_value.items()[1], Value::Int8(2));
            assert_eq!(list_value.items()[2], Value::Int8(3));
            assert_eq!(
                list_value.datatype(),
                &ConcreteDataType::List(ListType::new(ConcreteDataType::int8_datatype()))
            );
        } else {
            panic!("Expected List value");
        }
    }

    #[test]
    fn test_encode_json_array_empty_with_item_type() {
        let json = json!([]);
        let list_type = ListType::new(ConcreteDataType::string_datatype());
        let concrete_type = ConcreteDataType::List(list_type);
        let settings = JsonStructureSettings::Structured(None);
        let result = settings
            .encode_with_type(&json, Some(&concrete_type))
            .unwrap();

        if let Value::List(list_value) = result {
            assert_eq!(list_value.items().len(), 0);
            assert_eq!(
                list_value.datatype(),
                &ConcreteDataType::List(ListType::new(ConcreteDataType::string_datatype()))
            );
        } else {
            panic!("Expected List value");
        }
    }

    #[test]
    fn test_encode_json_with_concrete_type() {
        let settings = JsonStructureSettings::Structured(None);

        // Test encoding JSON number with expected int64 type
        let json = Json::from(42);
        let result = settings
            .encode_with_type(&json, Some(&ConcreteDataType::int64_datatype()))
            .unwrap();
        assert_eq!(result, Value::Int64(42));

        // Test encoding JSON string with expected string type
        let json = Json::String("hello".to_string());
        let result = settings
            .encode_with_type(&json, Some(&ConcreteDataType::string_datatype()))
            .unwrap();
        assert_eq!(result, Value::String("hello".into()));

        // Test encoding JSON boolean with expected boolean type
        let json = Json::Bool(true);
        let result = settings
            .encode_with_type(&json, Some(&ConcreteDataType::boolean_datatype()))
            .unwrap();
        assert_eq!(result, Value::Boolean(true));
    }

    #[test]
    fn test_encode_json_with_mismatched_type() {
        // Test encoding JSON number with mismatched string type
        let json = Json::from(42);
        let settings = JsonStructureSettings::Structured(None);
        let result = settings.encode_with_type(&json, Some(&ConcreteDataType::string_datatype()));
        assert!(result.is_ok()); // Should succeed due to type conversion

        // Test encoding JSON object with mismatched non-struct type
        let json = json!({"name": "test"});
        let result = settings.encode_with_type(&json, Some(&ConcreteDataType::int64_datatype()));
        assert!(result.is_err()); // Should fail - object can't be converted to int64
    }

    #[test]
    fn test_encode_json_array_with_list_type() {
        let json = json!([1, 2, 3]);
        let list_type = ListType::new(ConcreteDataType::int64_datatype());
        let concrete_type = ConcreteDataType::List(list_type);

        let settings = JsonStructureSettings::Structured(None);
        let result = settings
            .encode_with_type(&json, Some(&concrete_type))
            .unwrap();

        if let Value::List(list_value) = result {
            assert_eq!(list_value.items().len(), 3);
            assert_eq!(list_value.items()[0], Value::Int64(1));
            assert_eq!(list_value.items()[1], Value::Int64(2));
            assert_eq!(list_value.items()[2], Value::Int64(3));
            assert_eq!(
                list_value.datatype(),
                &ConcreteDataType::List(ListType::new(ConcreteDataType::int64_datatype()))
            );
        } else {
            panic!("Expected List value");
        }
    }

    #[test]
    fn test_encode_json_non_collection_with_type() {
        // Test null with null type
        let json = Json::Null;
        let settings = JsonStructureSettings::Structured(None);
        let result = settings
            .encode_with_type(&json, Some(&ConcreteDataType::null_datatype()))
            .unwrap();
        assert_eq!(result, Value::Null);

        // Test float with float64 type
        let json = Json::from(3.15);
        let result = settings
            .encode_with_type(&json, Some(&ConcreteDataType::float64_datatype()))
            .unwrap();
        match result {
            Value::Float64(f) => assert_eq!(f.0, 3.15),
            _ => panic!("Expected Float64"),
        }
    }

    #[test]
    fn test_encode_json_large_unsigned_integer() {
        // Test unsigned integer that fits in i64
        let json = Json::from(u64::MAX / 2);
        let settings = JsonStructureSettings::Structured(None);
        let result = settings.encode_with_type(&json, None).unwrap();
        assert_eq!(result, Value::Int64((u64::MAX / 2) as i64));

        // Test unsigned integer that exceeds i64 range
        let json = Json::from(u64::MAX);
        let result = settings.encode_with_type(&json, None).unwrap();
        assert_eq!(result, Value::UInt64(u64::MAX));
    }

    #[test]
    fn test_json_structure_settings_unstructured_raw() {
        let json = json!({
            "name": "Frank",
            "score": 88
        });

        let settings = JsonStructureSettings::UnstructuredRaw;
        let result = settings.encode(&json).unwrap();

        if let Value::String(s) = result {
            let json_str = s.as_utf8();
            assert!(json_str.contains("\"name\":\"Frank\""));
            assert!(json_str.contains("\"score\":88"));
        } else {
            panic!("Expected String value");
        }
    }

    #[test]
    fn test_json_structure_settings_unstructured_raw_with_type() {
        let json = json!({
            "name": "Grace",
            "age": 30,
            "active": true
        });

        let settings = JsonStructureSettings::UnstructuredRaw;

        // Test with encode (no type)
        let result1 = settings.encode(&json).unwrap();
        if let Value::String(s) = result1 {
            let json_str = s.as_utf8();
            assert!(json_str.contains("\"name\":\"Grace\""));
            assert!(json_str.contains("\"age\":30"));
            assert!(json_str.contains("\"active\":true"));
        } else {
            panic!("Expected String value for encode");
        }

        // Test with encode_with_type (with type)
        let struct_type = StructType::new(vec![
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
        ]);
        let concrete_type = ConcreteDataType::Struct(struct_type);

        let result2 = settings
            .encode_with_type(&json, Some(&concrete_type))
            .unwrap();
        if let Value::String(s) = result2 {
            let json_str = s.as_utf8();
            assert!(json_str.contains("\"name\":\"Grace\""));
            assert!(json_str.contains("\"age\":30"));
            assert!(json_str.contains("\"active\":true"));
        } else {
            panic!("Expected String value for encode_with_type");
        }

        // Test with nested objects
        let nested_json = json!({
            "user": {
                "profile": {
                    "name": "Alice",
                    "settings": {"theme": "dark"}
                }
            }
        });

        let result3 = settings.encode(&nested_json).unwrap();
        if let Value::String(s) = result3 {
            let json_str = s.as_utf8();
            assert!(json_str.contains("\"user\""));
            assert!(json_str.contains("\"profile\""));
            assert!(json_str.contains("\"name\":\"Alice\""));
            assert!(json_str.contains("\"settings\""));
            assert!(json_str.contains("\"theme\":\"dark\""));
        } else {
            panic!("Expected String value for nested JSON");
        }

        // Test with arrays
        let array_json = json!([1, "hello", true, 3.15]);
        let result4 = settings.encode(&array_json).unwrap();
        if let Value::String(s) = result4 {
            let json_str = s.as_utf8();
            assert!(json_str.contains("[1,\"hello\",true,3.15]"));
        } else {
            panic!("Expected String value for array JSON");
        }
    }

    #[test]
    fn test_encode_json_with_context_partial_unstructured() {
        let json = json!({
            "user": {
                "name": "Alice",
                "metadata": {
                    "preferences": {"theme": "dark"},
                    "history": [1, 2, 3]
                }
            }
        });

        let mut unstructured_keys = HashSet::new();
        unstructured_keys.insert("user.metadata".to_string());

        let settings = JsonStructureSettings::PartialUnstructuredByKey {
            fields: None,
            unstructured_keys,
        };
        let result = settings.encode(&json).unwrap();

        if let Value::Struct(struct_value) = result {
            let items = struct_value.items();
            let struct_type = struct_value.struct_type();

            // Find user field
            let user_index = struct_type
                .fields()
                .iter()
                .position(|f| f.name() == "user")
                .unwrap();
            if let Value::Struct(user_struct) = &items[user_index] {
                let user_items = user_struct.items();
                let user_fields: Vec<&str> = user_struct
                    .struct_type()
                    .fields()
                    .iter()
                    .map(|f| f.name())
                    .collect();

                // name should be structured
                let name_index = user_fields.iter().position(|&f| f == "name").unwrap();
                assert_eq!(user_items[name_index], Value::String("Alice".into()));

                // metadata should be unstructured (string)
                let metadata_index = user_fields.iter().position(|&f| f == "metadata").unwrap();
                if let Value::String(metadata_str) = &user_items[metadata_index] {
                    let json_str = metadata_str.as_utf8();
                    assert!(json_str.contains("\"preferences\""));
                    assert!(json_str.contains("\"history\""));
                } else {
                    panic!("Expected String value for metadata field");
                }
            } else {
                panic!("Expected Struct value for user field");
            }
        } else {
            panic!("Expected Struct value");
        }
    }
}
