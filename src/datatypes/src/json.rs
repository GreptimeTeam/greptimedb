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

use std::collections::HashSet;

use common_base::bytes::StringBytes;
use ordered_float::OrderedFloat;
use serde_json::{Map, Value as Json};
use snafu::{ResultExt, ensure};

use crate::data_type::{ConcreteDataType, DataType};
use crate::error::{self, Error};
use crate::types::{ListType, StructField, StructType};
use crate::value::{ListValue, StructValue, Value};

/// The configuration of JSON encoding
///
/// The enum describes how we handle JSON encoding to `StructValue` internally.
/// It defines three configurations:
/// - Structured: Encodes JSON objects as StructValue with an optional predefined StructType.
/// - UnstructuredRaw: Encodes JSON data as string and store it in a struct with a field named "_raw".
/// - PartialUnstructuredByKey: Encodes JSON objects as StructValue with an optional predefined StructType
///   and a set of unstructured keys, these keys are provided as flattened names, for example: `a.b.c`.
///
/// We provide a few methods to convert JSON data to StructValue based on the settings. And we also
/// convert them to fully structured StructValue for user-facing APIs: the UI protocol and the UDF interface.
///
/// **Important**: This settings only controls the internal form of JSON encoding.
#[derive(Debug, Clone)]
pub enum JsonStructureSettings {
    // TODO(sunng87): provide a limit
    Structured(Option<StructType>),
    UnstructuredRaw,
    PartialUnstructuredByKey {
        fields: Option<StructType>,
        unstructured_keys: HashSet<String>,
    },
}

/// Context for JSON encoding/decoding that tracks the current key path
#[derive(Clone, Debug)]
pub struct JsonContext<'a> {
    /// Current key path in dot notation (e.g., "user.profile.name")
    pub key_path: String,
    /// Settings for JSON structure handling
    pub settings: &'a JsonStructureSettings,
}

impl JsonStructureSettings {
    pub const RAW_FIELD: &'static str = "_raw";

    /// Decode an encoded StructValue back into a serde_json::Value.
    pub fn decode(&self, value: Value) -> Result<Json, Error> {
        let context = JsonContext {
            key_path: String::new(),
            settings: self,
        };
        decode_value_with_context(value, &context)
    }

    /// Decode a StructValue that was encoded with current settings back into a fully structured StructValue.
    /// This is useful for reconstructing the original structure from encoded data, especially when
    /// unstructured encoding was used for some fields.
    pub fn decode_struct(&self, struct_value: StructValue) -> Result<StructValue, Error> {
        let context = JsonContext {
            key_path: String::new(),
            settings: self,
        };
        decode_struct_with_settings(struct_value, &context)
    }

    /// Encode a serde_json::Value into a StructValue using current settings.
    pub fn encode(&self, json: Json) -> Result<Value, Error> {
        let context = JsonContext {
            key_path: String::new(),
            settings: self,
        };
        encode_json_with_context(json, None, &context)
    }

    pub fn encode_with_type(
        &self,
        json: Json,
        data_type: Option<&ConcreteDataType>,
    ) -> Result<Value, Error> {
        let context = JsonContext {
            key_path: String::new(),
            settings: self,
        };
        encode_json_with_context(json, data_type, &context)
    }
}

impl<'a> JsonContext<'a> {
    /// Create a new context with an updated key path
    pub fn with_key(&self, key: &str) -> JsonContext<'a> {
        let new_key_path = if self.key_path.is_empty() {
            key.to_string()
        } else {
            format!("{}.{}", self.key_path, key)
        };
        JsonContext {
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
    json: Json,
    data_type: Option<&ConcreteDataType>,
    context: &JsonContext<'a>,
) -> Result<Value, Error> {
    // Check if the entire encoding should be unstructured
    if matches!(context.settings, JsonStructureSettings::UnstructuredRaw) {
        let json_string = json.to_string();
        let struct_value = StructValue::try_new(
            vec![Value::String(json_string.into())],
            StructType::new(vec![StructField::new(
                JsonStructureSettings::RAW_FIELD.to_string(),
                ConcreteDataType::string_datatype(),
                true,
            )]),
        )?;
        return Ok(Value::Struct(struct_value));
    }

    // Check if current key should be treated as unstructured
    if context.is_unstructured_key() {
        return Ok(Value::String(json.to_string().into()));
    }

    match json {
        Json::Object(json_object) => {
            ensure!(
                matches!(data_type, Some(ConcreteDataType::Struct(_)) | None),
                error::InvalidJsonSnafu {
                    value: "JSON object can only be encoded to Struct type".to_string(),
                }
            );

            let data_type = data_type.and_then(|x| x.as_struct());
            let struct_value = encode_json_object_with_context(json_object, data_type, context)?;
            Ok(Value::Struct(struct_value))
        }
        Json::Array(json_array) => {
            let item_type = if let Some(ConcreteDataType::List(list_type)) = data_type {
                Some(list_type.item_type())
            } else {
                None
            };
            let list_value = encode_json_array_with_context(json_array, item_type, context)?;
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
    mut json_object: Map<String, Json>,
    fields: Option<&StructType>,
    context: &JsonContext<'a>,
) -> Result<StructValue, Error> {
    let total_json_keys = json_object.len();
    let mut items = Vec::with_capacity(total_json_keys);
    let mut struct_fields = Vec::with_capacity(total_json_keys);
    // First, process fields from the provided schema in their original order
    if let Some(fields) = fields {
        for field in fields.fields() {
            let field_name = field.name();

            if let Some(value) = json_object.remove(field_name) {
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
        let field_context = context.with_key(&key);

        let (value, data_type) = encode_json_value_with_context(value, None, &field_context)?;
        items.push(value);

        struct_fields.push(StructField::new(
            key.clone(),
            data_type,
            true, // JSON fields are always nullable
        ));
    }

    let struct_type = StructType::new(struct_fields);
    StructValue::try_new(items, struct_type)
}

fn encode_json_array_with_context<'a>(
    json_array: Vec<Json>,
    item_type: Option<&ConcreteDataType>,
    context: &JsonContext<'a>,
) -> Result<ListValue, Error> {
    let json_array_len = json_array.len();
    let mut items = Vec::with_capacity(json_array_len);
    let mut element_type = None;

    for (index, value) in json_array.into_iter().enumerate() {
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
    json: Json,
    expected_type: Option<&ConcreteDataType>,
    context: &JsonContext<'a>,
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
        Json::Bool(b) => Ok((Value::Boolean(b), ConcreteDataType::boolean_datatype())),
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

/// Main decoding function with key path tracking
pub fn decode_value_with_context<'a>(
    value: Value,
    context: &JsonContext<'a>,
) -> Result<Json, Error> {
    // Check if the entire decoding should be unstructured
    if matches!(context.settings, JsonStructureSettings::UnstructuredRaw) {
        return decode_unstructured_value(value);
    }

    // Check if current key should be treated as unstructured
    if context.is_unstructured_key() {
        return decode_unstructured_value(value);
    }

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
) -> Result<Json, Error> {
    let mut json_object = Map::with_capacity(struct_value.len());

    let (items, fields) = struct_value.into_parts();

    for (field, field_value) in fields.fields().iter().zip(items.into_iter()) {
        let field_context = context.with_key(field.name());
        let json_value = decode_value_with_context(field_value, &field_context)?;
        json_object.insert(field.name().to_string(), json_value);
    }

    Ok(Json::Object(json_object))
}

/// Decode a list value to JSON array
fn decode_list_with_context<'a>(
    list_value: ListValue,
    context: &JsonContext<'a>,
) -> Result<Json, Error> {
    let mut json_array = Vec::with_capacity(list_value.len());

    let data_items = list_value.take_items();

    for (index, item) in data_items.into_iter().enumerate() {
        let array_context = context.with_key(&index.to_string());
        let json_value = decode_value_with_context(item, &array_context)?;
        json_array.push(json_value);
    }

    Ok(Json::Array(json_array))
}

/// Decode unstructured value (stored as string)
fn decode_unstructured_value(value: Value) -> Result<Json, Error> {
    match value {
        // Handle expected format: StructValue with single _raw field
        Value::Struct(struct_value) => {
            if struct_value.struct_type().fields().len() == 1 {
                let field = &struct_value.struct_type().fields()[0];
                if field.name() == JsonStructureSettings::RAW_FIELD
                    && let Some(Value::String(s)) = struct_value.items().first()
                {
                    let json_str = s.as_utf8();
                    return serde_json::from_str(json_str).with_context(|_| {
                        error::DeserializeSnafu {
                            json: json_str.to_string(),
                        }
                    });
                }
            }
            // Invalid format - expected struct with single _raw field
            Err(error::InvalidJsonSnafu {
                value: "Unstructured value must be stored as struct with single _raw field"
                    .to_string(),
            }
            .build())
        }
        // Handle old format: plain string (for backward compatibility)
        Value::String(s) => {
            let json_str = s.as_utf8();
            serde_json::from_str(json_str).with_context(|_| error::DeserializeSnafu {
                json: json_str.to_string(),
            })
        }
        _ => Err(error::InvalidJsonSnafu {
            value: "Unstructured value must be stored as string or struct with _raw field"
                .to_string(),
        }
        .build()),
    }
}

/// Decode primitive value to JSON
fn decode_primitive_value(value: Value) -> Result<Json, Error> {
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
        Value::Struct(_) | Value::List(_) => {
            // These should be handled by the context-aware functions
            Err(error::InvalidJsonSnafu {
                value: "Structured values should be handled by context-aware decoding".to_string(),
            }
            .build())
        }
    }
}

/// Decode a StructValue that was encoded with current settings back into a fully structured StructValue
fn decode_struct_with_settings<'a>(
    struct_value: StructValue,
    context: &JsonContext<'a>,
) -> Result<StructValue, Error> {
    // Check if we can return the struct directly (Structured case)
    if matches!(context.settings, JsonStructureSettings::Structured(_)) {
        return Ok(struct_value);
    }

    // Check if we can return the struct directly (PartialUnstructuredByKey with no matching keys)
    if let JsonStructureSettings::PartialUnstructuredByKey {
        unstructured_keys, ..
    } = context.settings
        && unstructured_keys.is_empty()
    {
        return Ok(struct_value.clone());
    }

    // Check if the entire decoding should be unstructured (UnstructuredRaw case)
    if matches!(context.settings, JsonStructureSettings::UnstructuredRaw) {
        // For UnstructuredRaw, the entire struct should be reconstructed from _raw field
        return decode_unstructured_raw_struct(struct_value);
    }

    let mut items = Vec::with_capacity(struct_value.len());
    let mut struct_fields = Vec::with_capacity(struct_value.len());

    // Process each field in the struct value
    let (struct_data, fields) = struct_value.into_parts();
    for (field, value) in fields.fields().iter().zip(struct_data.into_iter()) {
        let field_context = context.with_key(field.name());

        // Check if this field should be treated as unstructured
        if field_context.is_unstructured_key() {
            // Decode the unstructured value
            let json_value = decode_unstructured_value(value)?;

            // Re-encode the unstructured value with proper structure using structured context
            let structured_context = JsonContext {
                key_path: field_context.key_path.clone(),
                settings: &JsonStructureSettings::Structured(None),
            };
            let (decoded_value, data_type) = encode_json_value_with_context(
                json_value,
                None, // Don't force a specific type, let it be inferred from JSON
                &structured_context,
            )?;

            items.push(decoded_value);
            struct_fields.push(StructField::new(
                field.name().to_string(),
                data_type,
                true, // JSON fields are always nullable
            ));
        } else {
            // For structured fields, recursively decode if they are structs/lists
            let decoded_value = match value {
                Value::Struct(nested_struct) => {
                    let nested_context = context.with_key(field.name());
                    Value::Struct(decode_struct_with_settings(nested_struct, &nested_context)?)
                }
                Value::List(list_value) => {
                    let list_context = context.with_key(field.name());
                    Value::List(decode_list_with_settings(list_value, &list_context)?)
                }
                _ => value.clone(),
            };

            items.push(decoded_value);
            struct_fields.push(field.clone());
        }
    }

    let struct_type = StructType::new(struct_fields);
    StructValue::try_new(items, struct_type)
}

/// Decode a ListValue that was encoded with current settings back into a fully structured ListValue
fn decode_list_with_settings<'a>(
    list_value: ListValue,
    context: &JsonContext<'a>,
) -> Result<ListValue, Error> {
    let mut items = Vec::with_capacity(list_value.len());

    let (data_items, datatype) = list_value.into_parts();

    for (index, item) in data_items.into_iter().enumerate() {
        let item_context = context.with_key(&index.to_string());

        let decoded_item = match item {
            Value::Struct(nested_struct) => {
                Value::Struct(decode_struct_with_settings(nested_struct, &item_context)?)
            }
            Value::List(nested_list) => {
                Value::List(decode_list_with_settings(nested_list, &item_context)?)
            }
            _ => item.clone(),
        };

        items.push(decoded_item);
    }

    Ok(ListValue::new(items, datatype))
}

/// Helper function to decode a struct that was encoded with UnstructuredRaw settings
fn decode_unstructured_raw_struct(struct_value: StructValue) -> Result<StructValue, Error> {
    // For UnstructuredRaw, the struct must have exactly one field named "_raw"
    if struct_value.struct_type().fields().len() == 1 {
        let field = &struct_value.struct_type().fields()[0];
        if field.name() == JsonStructureSettings::RAW_FIELD
            && let Some(Value::String(s)) = struct_value.items().first()
        {
            let json_str = s.as_utf8();
            let json_value: Json =
                serde_json::from_str(json_str).with_context(|_| error::DeserializeSnafu {
                    json: json_str.to_string(),
                })?;

            // Re-encode the JSON with proper structure
            let context = JsonContext {
                key_path: String::new(),
                settings: &JsonStructureSettings::Structured(None),
            };
            let (decoded_value, data_type) =
                encode_json_value_with_context(json_value, None, &context)?;

            if let Value::Struct(decoded_struct) = decoded_value {
                return Ok(decoded_struct);
            } else {
                // If the decoded value is not a struct, wrap it in a struct
                let struct_type =
                    StructType::new(vec![StructField::new("value".to_string(), data_type, true)]);
                return StructValue::try_new(vec![decoded_value], struct_type);
            }
        }
    }

    // Invalid format - expected struct with single _raw field
    Err(error::InvalidJsonSnafu {
        value: "UnstructuredRaw value must be stored as struct with single _raw field".to_string(),
    }
    .build())
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

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn test_encode_json_null() {
        let json = Json::Null;
        let settings = JsonStructureSettings::Structured(None);
        let result = settings.encode_with_type(json, None).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_encode_json_boolean() {
        let json = Json::Bool(true);
        let settings = JsonStructureSettings::Structured(None);
        let result = settings.encode_with_type(json, None).unwrap();
        assert_eq!(result, Value::Boolean(true));
    }

    #[test]
    fn test_encode_json_number_integer() {
        let json = Json::from(42);
        let settings = JsonStructureSettings::Structured(None);
        let result = settings.encode_with_type(json, None).unwrap();
        assert_eq!(result, Value::Int64(42));
    }

    #[test]
    fn test_encode_json_number_float() {
        let json = Json::from(3.15);
        let settings = JsonStructureSettings::Structured(None);
        let result = settings.encode_with_type(json, None).unwrap();
        match result {
            Value::Float64(f) => assert_eq!(f.0, 3.15),
            _ => panic!("Expected Float64"),
        }
    }

    #[test]
    fn test_encode_json_string() {
        let json = Json::String("hello".to_string());
        let settings = JsonStructureSettings::Structured(None);
        let result = settings.encode_with_type(json, None).unwrap();
        assert_eq!(result, Value::String("hello".into()));
    }

    #[test]
    fn test_encode_json_array() {
        let json = json!([1, 2, 3]);
        let settings = JsonStructureSettings::Structured(None);
        let result = settings.encode_with_type(json, None).unwrap();

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
        let result = settings.encode_with_type(json, None).unwrap();
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
        let result = settings.encode_with_type(json, None).unwrap();
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
            .encode_with_type(json.clone(), Some(&ConcreteDataType::int8_datatype()))
            .unwrap();
        assert_eq!(result, Value::Int8(42));

        // Test with expected string type
        let result = settings
            .encode_with_type(json, Some(&ConcreteDataType::string_datatype()))
            .unwrap();
        assert_eq!(result, Value::String("42".into()));
    }

    #[test]
    fn test_encode_json_array_mixed_types() {
        let json = json!([1, "hello", true, 3.15]);
        let settings = JsonStructureSettings::Structured(None);
        let result = settings.encode_with_type(json, None).unwrap();

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
        let result = settings.encode_with_type(json, None).unwrap();

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
        let result = settings.encode(json).unwrap();

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
            .encode_with_type(json, Some(&concrete_type))
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
            json.as_object().unwrap().clone(),
            Some(&struct_type),
            &JsonContext {
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
            json.as_object().unwrap().clone(),
            Some(&struct_type),
            &JsonContext {
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
            json.as_object().unwrap().clone(),
            Some(&struct_type),
            &JsonContext {
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
    fn test_json_structure_settings_structured() {
        let json = json!({
            "name": "Eve",
            "score": 95
        });

        let settings = JsonStructureSettings::Structured(None);
        let result = settings.encode(json).unwrap();

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
            .encode_with_type(json, Some(&concrete_type))
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
            .encode_with_type(json, Some(&concrete_type))
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

    #[cfg(test)]
    mod decode_tests {
        use serde_json::json;

        use super::*;

        #[test]
        fn test_decode_primitive_values() {
            let settings = JsonStructureSettings::Structured(None);

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
            let settings = JsonStructureSettings::Structured(None);

            let struct_value = StructValue::new(
                vec![
                    Value::String("Alice".into()),
                    Value::Int64(25),
                    Value::Boolean(true),
                ],
                StructType::new(vec![
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
                ]),
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
            let settings = JsonStructureSettings::Structured(None);

            let list_value = ListValue::new(
                vec![Value::Int64(1), Value::Int64(2), Value::Int64(3)],
                ConcreteDataType::List(ListType::new(ConcreteDataType::int64_datatype())),
            );

            let result = settings.decode(Value::List(list_value)).unwrap();
            let expected = json!([1, 2, 3]);
            assert_eq!(result, expected);
        }

        #[test]
        fn test_decode_nested_structure() {
            let settings = JsonStructureSettings::Structured(None);

            let inner_struct = StructValue::new(
                vec![Value::String("Alice".into()), Value::Int64(25)],
                StructType::new(vec![
                    StructField::new(
                        "name".to_string(),
                        ConcreteDataType::string_datatype(),
                        true,
                    ),
                    StructField::new("age".to_string(), ConcreteDataType::int64_datatype(), true),
                ]),
            );

            let outer_struct = StructValue::new(
                vec![
                    Value::Struct(inner_struct),
                    Value::List(ListValue::new(
                        vec![Value::Int64(95), Value::Int64(87)],
                        ConcreteDataType::List(ListType::new(ConcreteDataType::int64_datatype())),
                    )),
                ],
                StructType::new(vec![
                    StructField::new(
                        "user".to_string(),
                        ConcreteDataType::Struct(StructType::new(vec![
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
                        ])),
                        true,
                    ),
                    StructField::new(
                        "scores".to_string(),
                        ConcreteDataType::List(ListType::new(ConcreteDataType::int64_datatype())),
                        true,
                    ),
                ]),
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
        fn test_decode_unstructured_raw() {
            let settings = JsonStructureSettings::UnstructuredRaw;

            let json_str = r#"{"name": "Bob", "age": 30}"#;
            let value = Value::String(json_str.into());

            let result = settings.decode(value).unwrap();
            let expected: Json = serde_json::from_str(json_str).unwrap();
            assert_eq!(result, expected);
        }

        #[test]
        fn test_decode_unstructured_raw_struct_format() {
            let settings = JsonStructureSettings::UnstructuredRaw;

            let json_str = r#"{"name": "Bob", "age": 30}"#;
            let struct_value = StructValue::new(
                vec![Value::String(json_str.into())],
                StructType::new(vec![StructField::new(
                    JsonStructureSettings::RAW_FIELD.to_string(),
                    ConcreteDataType::string_datatype(),
                    true,
                )]),
            );
            let value = Value::Struct(struct_value);

            let result = settings.decode(value).unwrap();
            let expected: Json = serde_json::from_str(json_str).unwrap();
            assert_eq!(result, expected);
        }

        #[test]
        fn test_decode_partial_unstructured() {
            let mut unstructured_keys = HashSet::new();
            unstructured_keys.insert("user.metadata".to_string());

            let settings = JsonStructureSettings::PartialUnstructuredByKey {
                fields: None,
                unstructured_keys,
            };

            let metadata_json = r#"{"preferences": {"theme": "dark"}, "history": [1, 2, 3]}"#;

            let struct_value = StructValue::new(
                vec![
                    Value::String("Alice".into()),
                    Value::String(metadata_json.into()),
                ],
                StructType::new(vec![
                    StructField::new(
                        "name".to_string(),
                        ConcreteDataType::string_datatype(),
                        true,
                    ),
                    StructField::new(
                        "metadata".to_string(),
                        ConcreteDataType::string_datatype(),
                        true,
                    ),
                ]),
            );

            let result = settings.decode(Value::Struct(struct_value)).unwrap();

            if let Json::Object(obj) = result {
                assert_eq!(obj.get("name"), Some(&Json::String("Alice".to_string())));

                if let Some(Json::String(metadata_str)) = obj.get("metadata") {
                    let metadata: Json = serde_json::from_str(metadata_str).unwrap();
                    let expected_metadata: Json = serde_json::from_str(metadata_json).unwrap();
                    assert_eq!(metadata, expected_metadata);
                } else {
                    panic!("Expected metadata to be unstructured string");
                }
            } else {
                panic!("Expected object result");
            }
        }

        #[test]
        fn test_decode_missing_fields() {
            let settings = JsonStructureSettings::Structured(None);

            // Struct with missing field (null value)
            let struct_value = StructValue::new(
                vec![
                    Value::String("Bob".into()),
                    Value::Null, // missing age field
                ],
                StructType::new(vec![
                    StructField::new(
                        "name".to_string(),
                        ConcreteDataType::string_datatype(),
                        true,
                    ),
                    StructField::new("age".to_string(), ConcreteDataType::int64_datatype(), true),
                ]),
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
    fn test_encode_json_with_concrete_type() {
        let settings = JsonStructureSettings::Structured(None);

        // Test encoding JSON number with expected int64 type
        let json = Json::from(42);
        let result = settings
            .encode_with_type(json, Some(&ConcreteDataType::int64_datatype()))
            .unwrap();
        assert_eq!(result, Value::Int64(42));

        // Test encoding JSON string with expected string type
        let json = Json::String("hello".to_string());
        let result = settings
            .encode_with_type(json, Some(&ConcreteDataType::string_datatype()))
            .unwrap();
        assert_eq!(result, Value::String("hello".into()));

        // Test encoding JSON boolean with expected boolean type
        let json = Json::Bool(true);
        let result = settings
            .encode_with_type(json, Some(&ConcreteDataType::boolean_datatype()))
            .unwrap();
        assert_eq!(result, Value::Boolean(true));
    }

    #[test]
    fn test_encode_json_with_mismatched_type() {
        // Test encoding JSON number with mismatched string type
        let json = Json::from(42);
        let settings = JsonStructureSettings::Structured(None);
        let result = settings.encode_with_type(json, Some(&ConcreteDataType::string_datatype()));
        assert!(result.is_ok()); // Should succeed due to type conversion

        // Test encoding JSON object with mismatched non-struct type
        let json = json!({"name": "test"});
        let result = settings.encode_with_type(json, Some(&ConcreteDataType::int64_datatype()));
        assert!(result.is_err()); // Should fail - object can't be converted to int64
    }

    #[test]
    fn test_encode_json_array_with_list_type() {
        let json = json!([1, 2, 3]);
        let list_type = ListType::new(ConcreteDataType::int64_datatype());
        let concrete_type = ConcreteDataType::List(list_type);

        let settings = JsonStructureSettings::Structured(None);
        let result = settings
            .encode_with_type(json, Some(&concrete_type))
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
            .encode_with_type(json.clone(), Some(&ConcreteDataType::null_datatype()))
            .unwrap();
        assert_eq!(result, Value::Null);

        // Test float with float64 type
        let json = Json::from(3.15);
        let result = settings
            .encode_with_type(json, Some(&ConcreteDataType::float64_datatype()))
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
        let result = settings.encode_with_type(json, None).unwrap();
        assert_eq!(result, Value::Int64((u64::MAX / 2) as i64));

        // Test unsigned integer that exceeds i64 range
        let json = Json::from(u64::MAX);
        let result = settings.encode_with_type(json, None).unwrap();
        assert_eq!(result, Value::UInt64(u64::MAX));
    }

    #[test]
    fn test_json_structure_settings_unstructured_raw() {
        let json = json!({
            "name": "Frank",
            "score": 88
        });

        let settings = JsonStructureSettings::UnstructuredRaw;
        let result = settings.encode(json).unwrap();

        if let Value::Struct(struct_value) = result {
            assert_eq!(struct_value.struct_type().fields().len(), 1);
            let field = &struct_value.struct_type().fields()[0];
            assert_eq!(field.name(), JsonStructureSettings::RAW_FIELD);
            assert_eq!(field.data_type(), &ConcreteDataType::string_datatype());

            let items = struct_value.items();
            assert_eq!(items.len(), 1);
            if let Value::String(s) = &items[0] {
                let json_str = s.as_utf8();
                assert!(json_str.contains("\"name\":\"Frank\""));
                assert!(json_str.contains("\"score\":88"));
            } else {
                panic!("Expected String value in _raw field");
            }
        } else {
            panic!("Expected Struct value");
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
        let result = settings.encode(json.clone()).unwrap();
        if let Value::Struct(s) = result {
            if let Value::String(json_str) = &s.items()[0] {
                let json_str = json_str.as_utf8();
                assert!(json_str.contains("\"name\":\"Grace\""));
                assert!(json_str.contains("\"age\":30"));
                assert!(json_str.contains("\"active\":true"));
            } else {
                panic!("Expected String value in _raw field");
            }
        } else {
            panic!("Expected Struct value for encode");
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
            .encode_with_type(json, Some(&concrete_type))
            .unwrap();
        if let Value::Struct(s) = result2 {
            if let Value::String(json_str) = &s.items()[0] {
                let json_str = json_str.as_utf8();
                assert!(json_str.contains("\"name\":\"Grace\""));
                assert!(json_str.contains("\"age\":30"));
                assert!(json_str.contains("\"active\":true"));
            } else {
                panic!("Expected String value for _raw field");
            }
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

        let result3 = settings.encode(nested_json).unwrap();
        if let Value::Struct(s) = result3 {
            if let Value::String(json_str) = &s.items()[0] {
                let json_str = json_str.as_utf8();
                assert!(json_str.contains("\"user\""));
                assert!(json_str.contains("\"profile\""));
                assert!(json_str.contains("\"name\":\"Alice\""));
                assert!(json_str.contains("\"settings\""));
                assert!(json_str.contains("\"theme\":\"dark\""));
            } else {
                panic!("Expected String value for _raw field");
            }
        } else {
            panic!("Expected String value for nested JSON");
        }

        // Test with arrays
        let array_json = json!([1, "hello", true, 3.15]);
        let result4 = settings.encode(array_json).unwrap();
        if let Value::Struct(s) = result4 {
            if let Value::String(json_str) = &s.items()[0] {
                let json_str = json_str.as_utf8();
                assert!(json_str.contains("[1,\"hello\",true,3.15]"));
            } else {
                panic!("Expected String value for _raw field")
            }
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
        let result = settings.encode(json).unwrap();

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

    #[test]
    fn test_decode_struct_structured() {
        // Test decoding a structured struct value - should return the same struct
        let settings = JsonStructureSettings::Structured(None);

        let original_struct = StructValue::new(
            vec![
                Value::String("Alice".into()),
                Value::Int64(25),
                Value::Boolean(true),
            ],
            StructType::new(vec![
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
            ]),
        );

        let decoded_struct = settings.decode_struct(original_struct.clone()).unwrap();

        // With Structured settings, the struct should be returned directly
        assert_eq!(decoded_struct.items(), original_struct.items());
        assert_eq!(decoded_struct.struct_type(), original_struct.struct_type());
    }

    #[test]
    fn test_decode_struct_partial_unstructured_empty_keys() {
        // Test decoding with PartialUnstructuredByKey but empty unstructured_keys
        let settings = JsonStructureSettings::PartialUnstructuredByKey {
            fields: None,
            unstructured_keys: HashSet::new(),
        };

        let original_struct = StructValue::new(
            vec![
                Value::String("Alice".into()),
                Value::Int64(25),
                Value::Boolean(true),
            ],
            StructType::new(vec![
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
            ]),
        );

        let decoded_struct = settings.decode_struct(original_struct.clone()).unwrap();

        // With empty unstructured_keys, the struct should be returned directly
        assert_eq!(decoded_struct.items(), original_struct.items());
        assert_eq!(decoded_struct.struct_type(), original_struct.struct_type());
    }

    #[test]
    fn test_decode_struct_partial_unstructured() {
        // Test decoding a struct with unstructured fields
        let mut unstructured_keys = HashSet::new();
        unstructured_keys.insert("metadata".to_string());

        let settings = JsonStructureSettings::PartialUnstructuredByKey {
            fields: Some(StructType::new(vec![
                StructField::new(
                    "name".to_string(),
                    ConcreteDataType::string_datatype(),
                    true,
                ),
                StructField::new(
                    "metadata".to_string(),
                    ConcreteDataType::string_datatype(),
                    true,
                ),
            ])),
            unstructured_keys,
        };

        // Create a struct where metadata is stored as unstructured JSON string
        let encoded_struct = StructValue::new(
            vec![
                Value::String("Alice".into()),
                Value::String(r#"{"preferences":{"theme":"dark"},"history":[1,2,3]}"#.into()),
            ],
            StructType::new(vec![
                StructField::new(
                    "name".to_string(),
                    ConcreteDataType::string_datatype(),
                    true,
                ),
                StructField::new(
                    "metadata".to_string(),
                    ConcreteDataType::string_datatype(),
                    true,
                ),
            ]),
        );

        let decoded_struct = settings.decode_struct(encoded_struct).unwrap();

        // Verify name field remains the same
        assert_eq!(decoded_struct.items()[0], Value::String("Alice".into()));

        // Verify metadata field is now properly structured
        if let Value::Struct(metadata_struct) = &decoded_struct.items()[1] {
            let metadata_fields: Vec<&str> = metadata_struct
                .struct_type()
                .fields()
                .iter()
                .map(|f| f.name())
                .collect();

            assert!(metadata_fields.contains(&"preferences"));
            assert!(metadata_fields.contains(&"history"));
        } else {
            panic!("Expected metadata to be decoded as structured value");
        }
    }

    #[test]
    fn test_decode_struct_nested_unstructured() {
        // Test decoding nested structures with unstructured fields
        let mut unstructured_keys = HashSet::new();
        unstructured_keys.insert("user.metadata".to_string());

        let settings = JsonStructureSettings::PartialUnstructuredByKey {
            fields: None,
            unstructured_keys,
        };

        // Create a nested struct where user.metadata is stored as unstructured JSON string
        let user_struct = StructValue::new(
            vec![
                Value::String("Alice".into()),
                Value::String(r#"{"preferences":{"theme":"dark"},"history":[1,2,3]}"#.into()),
            ],
            StructType::new(vec![
                StructField::new(
                    "name".to_string(),
                    ConcreteDataType::string_datatype(),
                    true,
                ),
                StructField::new(
                    "metadata".to_string(),
                    ConcreteDataType::string_datatype(),
                    true,
                ),
            ]),
        );

        let encoded_struct = StructValue::new(
            vec![Value::Struct(user_struct)],
            StructType::new(vec![StructField::new(
                "user".to_string(),
                ConcreteDataType::struct_datatype(StructType::new(vec![])),
                true,
            )]),
        );

        let decoded_struct = settings.decode_struct(encoded_struct).unwrap();

        // Verify the nested structure is properly decoded
        if let Value::Struct(decoded_user) = &decoded_struct.items()[0] {
            if let Value::Struct(metadata_struct) = &decoded_user.items()[1] {
                let metadata_fields: Vec<&str> = metadata_struct
                    .struct_type()
                    .fields()
                    .iter()
                    .map(|f| f.name())
                    .collect();

                assert!(metadata_fields.contains(&"preferences"));
                assert!(metadata_fields.contains(&"history"));

                let preference_index = metadata_fields
                    .iter()
                    .position(|&field| field == "preferences")
                    .unwrap();
                let history_index = metadata_fields
                    .iter()
                    .position(|&field| field == "history")
                    .unwrap();

                // Verify the nested structure within preferences
                if let Value::Struct(preferences_struct) =
                    &metadata_struct.items()[preference_index]
                {
                    let pref_fields: Vec<&str> = preferences_struct
                        .struct_type()
                        .fields()
                        .iter()
                        .map(|f| f.name())
                        .collect();
                    assert!(pref_fields.contains(&"theme"));
                } else {
                    panic!("Expected preferences to be decoded as structured value");
                }

                // Verify the array within history
                if let Value::List(history_list) = &metadata_struct.items()[history_index] {
                    assert_eq!(history_list.items().len(), 3);
                } else {
                    panic!("Expected history to be decoded as list value");
                }
            } else {
                panic!("Expected metadata to be decoded as structured value");
            }
        } else {
            panic!("Expected user to be decoded as structured value");
        }
    }

    #[test]
    fn test_decode_struct_unstructured_raw() {
        // Test decoding with UnstructuredRaw setting
        let settings = JsonStructureSettings::UnstructuredRaw;

        // With UnstructuredRaw, the entire JSON is encoded as a struct with _raw field
        let encoded_struct = StructValue::new(
            vec![Value::String(
                r#"{"name":"Alice","age":25,"active":true}"#.into(),
            )],
            StructType::new(vec![StructField::new(
                "_raw".to_string(),
                ConcreteDataType::string_datatype(),
                true,
            )]),
        );

        let decoded_struct = settings.decode_struct(encoded_struct).unwrap();

        // With UnstructuredRaw, the entire struct should be reconstructed from _raw field
        let decoded_fields: Vec<&str> = decoded_struct
            .struct_type()
            .fields()
            .iter()
            .map(|f| f.name())
            .collect();

        assert!(decoded_fields.contains(&"name"));
        assert!(decoded_fields.contains(&"age"));
        assert!(decoded_fields.contains(&"active"));

        // Verify the actual values
        let name_index = decoded_fields.iter().position(|&f| f == "name").unwrap();
        let age_index = decoded_fields.iter().position(|&f| f == "age").unwrap();
        let active_index = decoded_fields.iter().position(|&f| f == "active").unwrap();

        assert_eq!(
            decoded_struct.items()[name_index],
            Value::String("Alice".into())
        );
        assert_eq!(decoded_struct.items()[age_index], Value::Int64(25));
        assert_eq!(decoded_struct.items()[active_index], Value::Boolean(true));
    }

    #[test]
    fn test_decode_struct_unstructured_raw_invalid_format() {
        // Test UnstructuredRaw decoding when the struct doesn't have the expected _raw field format
        let settings = JsonStructureSettings::UnstructuredRaw;

        // Create a struct that doesn't match the expected UnstructuredRaw format
        let invalid_struct = StructValue::new(
            vec![Value::String("Alice".into()), Value::Int64(25)],
            StructType::new(vec![
                StructField::new(
                    "name".to_string(),
                    ConcreteDataType::string_datatype(),
                    true,
                ),
                StructField::new("age".to_string(), ConcreteDataType::int64_datatype(), true),
            ]),
        );

        // Should fail with error since it doesn't match expected UnstructuredRaw format
        let result = settings.decode_struct(invalid_struct);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("UnstructuredRaw value must be stored as struct with single _raw field")
        );
    }

    #[test]
    fn test_decode_struct_unstructured_raw_primitive_value() {
        // Test UnstructuredRaw decoding when the _raw field contains a primitive value
        let settings = JsonStructureSettings::UnstructuredRaw;

        // Test with a string primitive in _raw field
        let string_struct = StructValue::new(
            vec![Value::String("\"hello world\"".into())],
            StructType::new(vec![StructField::new(
                "_raw".to_string(),
                ConcreteDataType::string_datatype(),
                true,
            )]),
        );

        let decoded_struct = settings.decode_struct(string_struct).unwrap();
        let decoded_fields: Vec<&str> = decoded_struct
            .struct_type()
            .fields()
            .iter()
            .map(|f| f.name())
            .collect();
        assert!(decoded_fields.contains(&"value"));
        assert_eq!(
            decoded_struct.items()[0],
            Value::String("hello world".into())
        );

        // Test with a number primitive in _raw field
        let number_struct = StructValue::new(
            vec![Value::String("42".into())],
            StructType::new(vec![StructField::new(
                "_raw".to_string(),
                ConcreteDataType::string_datatype(),
                true,
            )]),
        );

        let decoded_struct = settings.decode_struct(number_struct).unwrap();
        let decoded_fields: Vec<&str> = decoded_struct
            .struct_type()
            .fields()
            .iter()
            .map(|f| f.name())
            .collect();
        assert!(decoded_fields.contains(&"value"));
        assert_eq!(decoded_struct.items()[0], Value::Int64(42));

        // Test with a boolean primitive in _raw field
        let bool_struct = StructValue::new(
            vec![Value::String("true".into())],
            StructType::new(vec![StructField::new(
                "_raw".to_string(),
                ConcreteDataType::string_datatype(),
                true,
            )]),
        );

        let decoded_struct = settings.decode_struct(bool_struct).unwrap();
        let decoded_fields: Vec<&str> = decoded_struct
            .struct_type()
            .fields()
            .iter()
            .map(|f| f.name())
            .collect();
        assert!(decoded_fields.contains(&"value"));
        assert_eq!(decoded_struct.items()[0], Value::Boolean(true));

        // Test with a null primitive in _raw field
        let null_struct = StructValue::new(
            vec![Value::String("null".into())],
            StructType::new(vec![StructField::new(
                "_raw".to_string(),
                ConcreteDataType::string_datatype(),
                true,
            )]),
        );

        let decoded_struct = settings.decode_struct(null_struct).unwrap();
        let decoded_fields: Vec<&str> = decoded_struct
            .struct_type()
            .fields()
            .iter()
            .map(|f| f.name())
            .collect();
        assert!(decoded_fields.contains(&"value"));
        assert_eq!(decoded_struct.items()[0], Value::Null);
    }

    #[test]
    fn test_decode_struct_unstructured_raw_array() {
        // Test UnstructuredRaw decoding when the _raw field contains a JSON array
        let settings = JsonStructureSettings::UnstructuredRaw;

        // Test with an array in _raw field
        let array_struct = StructValue::new(
            vec![Value::String("[1, \"hello\", true, 3.15]".into())],
            StructType::new(vec![StructField::new(
                "_raw".to_string(),
                ConcreteDataType::string_datatype(),
                true,
            )]),
        );

        let decoded_struct = settings.decode_struct(array_struct).unwrap();
        let decoded_fields: Vec<&str> = decoded_struct
            .struct_type()
            .fields()
            .iter()
            .map(|f| f.name())
            .collect();
        assert!(decoded_fields.contains(&"value"));

        if let Value::List(list_value) = &decoded_struct.items()[0] {
            assert_eq!(list_value.items().len(), 4);
            assert_eq!(list_value.items()[0], Value::Int64(1));
            assert_eq!(list_value.items()[1], Value::String("hello".into()));
            assert_eq!(list_value.items()[2], Value::Boolean(true));
            assert_eq!(list_value.items()[3], Value::Float64(OrderedFloat(3.15)));
        } else {
            panic!("Expected array to be decoded as ListValue");
        }
    }

    #[test]
    fn test_decode_struct_comprehensive_flow() {
        // Test the complete flow: encode JSON with partial unstructured settings,
        // then decode the resulting StructValue back to fully structured form
        let mut unstructured_keys = HashSet::new();
        unstructured_keys.insert("metadata".to_string());
        unstructured_keys.insert("user.profile.settings".to_string());

        let settings = JsonStructureSettings::PartialUnstructuredByKey {
            fields: None,
            unstructured_keys,
        };

        // Original JSON with nested structure
        let original_json = json!({
            "name": "Alice",
            "age": 25,
            "metadata": {
                "tags": ["admin", "premium"],
                "preferences": {
                    "theme": "dark",
                    "notifications": true
                }
            },
            "user": {
                "profile": {
                    "name": "Alice Smith",
                    "settings": {
                        "language": "en",
                        "timezone": "UTC"
                    }
                },
                "active": true
            }
        });

        // Encode the JSON with partial unstructured settings
        let encoded_value = settings.encode(original_json).unwrap();

        // Verify encoding worked - metadata and user.profile.settings should be unstructured
        if let Value::Struct(encoded_struct) = encoded_value {
            let fields: Vec<&str> = encoded_struct
                .struct_type()
                .fields()
                .iter()
                .map(|f| f.name())
                .collect();

            assert!(fields.contains(&"name"));
            assert!(fields.contains(&"age"));
            assert!(fields.contains(&"metadata"));
            assert!(fields.contains(&"user"));

            // Check that metadata is stored as string (unstructured)
            let metadata_index = fields.iter().position(|&f| f == "metadata").unwrap();
            if let Value::String(_) = encoded_struct.items()[metadata_index] {
                // Good - metadata is unstructured
            } else {
                panic!("Expected metadata to be encoded as string (unstructured)");
            }

            // Check that user.profile.settings is unstructured
            let user_index = fields.iter().position(|&f| f == "user").unwrap();
            if let Value::Struct(user_struct) = &encoded_struct.items()[user_index] {
                let user_fields: Vec<&str> = user_struct
                    .struct_type()
                    .fields()
                    .iter()
                    .map(|f| f.name())
                    .collect();

                let profile_index = user_fields.iter().position(|&f| f == "profile").unwrap();
                if let Value::Struct(profile_struct) = &user_struct.items()[profile_index] {
                    let profile_fields: Vec<&str> = profile_struct
                        .struct_type()
                        .fields()
                        .iter()
                        .map(|f| f.name())
                        .collect();

                    let settings_index = profile_fields
                        .iter()
                        .position(|&f| f == "settings")
                        .unwrap();
                    if let Value::String(_) = &profile_struct.items()[settings_index] {
                        // Good - settings is unstructured
                    } else {
                        panic!(
                            "Expected user.profile.settings to be encoded as string (unstructured)"
                        );
                    }
                } else {
                    panic!("Expected user.profile to be a struct");
                }
            } else {
                panic!("Expected user to be a struct");
            }

            // Now decode the struct back to fully structured form
            let decoded_struct = settings.decode_struct(encoded_struct).unwrap();

            // Verify the decoded struct has proper structure
            let decoded_fields: Vec<&str> = decoded_struct
                .struct_type()
                .fields()
                .iter()
                .map(|f| f.name())
                .collect();

            assert!(decoded_fields.contains(&"name"));
            assert!(decoded_fields.contains(&"age"));
            assert!(decoded_fields.contains(&"metadata"));
            assert!(decoded_fields.contains(&"user"));

            // Check that metadata is now properly structured
            let metadata_index = decoded_fields
                .iter()
                .position(|&f| f == "metadata")
                .unwrap();
            if let Value::Struct(metadata_struct) = &decoded_struct.items()[metadata_index] {
                let metadata_fields: Vec<&str> = metadata_struct
                    .struct_type()
                    .fields()
                    .iter()
                    .map(|f| f.name())
                    .collect();

                assert!(metadata_fields.contains(&"tags"));
                assert!(metadata_fields.contains(&"preferences"));

                // Check nested structure within metadata
                let preferences_index = metadata_fields
                    .iter()
                    .position(|&f| f == "preferences")
                    .unwrap();
                if let Value::Struct(prefs_struct) = &metadata_struct.items()[preferences_index] {
                    let prefs_fields: Vec<&str> = prefs_struct
                        .struct_type()
                        .fields()
                        .iter()
                        .map(|f| f.name())
                        .collect();

                    assert!(prefs_fields.contains(&"theme"));
                    assert!(prefs_fields.contains(&"notifications"));
                } else {
                    panic!("Expected metadata.preferences to be a struct");
                }
            } else {
                panic!("Expected metadata to be decoded as struct");
            }

            // Check that user.profile.settings is now properly structured
            let user_index = decoded_fields.iter().position(|&f| f == "user").unwrap();
            if let Value::Struct(user_struct) = &decoded_struct.items()[user_index] {
                let user_fields: Vec<&str> = user_struct
                    .struct_type()
                    .fields()
                    .iter()
                    .map(|f| f.name())
                    .collect();

                let profile_index = user_fields.iter().position(|&f| f == "profile").unwrap();
                if let Value::Struct(profile_struct) = &user_struct.items()[profile_index] {
                    let profile_fields: Vec<&str> = profile_struct
                        .struct_type()
                        .fields()
                        .iter()
                        .map(|f| f.name())
                        .collect();

                    let settings_index = profile_fields
                        .iter()
                        .position(|&f| f == "settings")
                        .unwrap();
                    if let Value::Struct(settings_struct) = &profile_struct.items()[settings_index]
                    {
                        let settings_fields: Vec<&str> = settings_struct
                            .struct_type()
                            .fields()
                            .iter()
                            .map(|f| f.name())
                            .collect();

                        assert!(settings_fields.contains(&"language"));
                        assert!(settings_fields.contains(&"timezone"));
                    } else {
                        panic!("Expected user.profile.settings to be decoded as struct");
                    }
                } else {
                    panic!("Expected user.profile to be a struct");
                }
            } else {
                panic!("Expected user to be a struct");
            }
        } else {
            panic!("Expected encoded value to be a struct");
        }
    }
}
