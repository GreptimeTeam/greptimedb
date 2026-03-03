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

use std::any::Any;
use std::borrow::Cow;
use std::sync::LazyLock;

use crate::data_type::ConcreteDataType;
use crate::error::{Result, TryFromValueSnafu, UnsupportedOperationSnafu};
use crate::json::value::{JsonValue, JsonValueRef, JsonVariant};
use crate::prelude::{ValueRef, Vector, VectorRef};
use crate::types::JsonType;
use crate::types::json_type::JsonNativeType;
use crate::vectors::{MutableVector, StructVectorBuilder};

pub(crate) struct Json2VectorBuilder {
    merged_type: JsonType,
    capacity: usize,
    values: Vec<JsonValue>,
}

impl Json2VectorBuilder {
    pub(crate) fn new(json_type: JsonNativeType, capacity: usize) -> Self {
        Self {
            merged_type: JsonType::new_native(json_type),
            capacity,
            values: vec![],
        }
    }

    fn build(&self) -> VectorRef {
        let mut builder = StructVectorBuilder::with_type_and_capacity(
            self.merged_type.as_struct_type(),
            self.capacity,
        );
        for value in self.values.iter() {
            let value = align_json_value_with_type(&self.merged_type, value);
            builder
                .try_push_value_ref(&(*value).as_ref().as_value_ref())
                // Safety: after the `align_json_value_with_type`, the values to push must have
                // the same types with the builder, so it's not expected to meet any errors here.
                .unwrap_or_else(|e| panic!("Failed to push JSON value {value}: {e:?}"));
        }
        builder.to_vector()
    }
}

impl MutableVector for Json2VectorBuilder {
    fn data_type(&self) -> ConcreteDataType {
        ConcreteDataType::Json(self.merged_type.clone())
    }

    fn len(&self) -> usize {
        self.values.len()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn Any {
        self
    }

    fn to_vector(&mut self) -> VectorRef {
        self.build()
    }

    fn to_vector_cloned(&self) -> VectorRef {
        self.build()
    }

    fn try_push_value_ref(&mut self, value: &ValueRef) -> Result<()> {
        let ValueRef::Json(value) = value else {
            return TryFromValueSnafu {
                reason: format!("expected json value, got {value:?}"),
            }
            .fail();
        };
        let json_type = value.json_type();
        self.merged_type.merge_with_lifting(json_type)?;

        let value = JsonValue::from(value.clone().into_variant());
        self.values.push(value);
        Ok(())
    }

    fn push_null(&mut self) {
        static NULL_JSON: LazyLock<ValueRef> =
            LazyLock::new(|| ValueRef::Json(Box::new(JsonValueRef::null())));
        self.try_push_value_ref(&NULL_JSON)
            // Safety: learning from the method "try_push_value_ref", a null json value should be
            // always able to push into any json vectors.
            .unwrap_or_else(|e| panic!("failed to push null json value, error: {e}"));
    }

    fn extend_slice_of(&mut self, _: &dyn Vector, _: usize, _: usize) -> Result<()> {
        UnsupportedOperationSnafu {
            op: "extend_slice_of",
            vector_type: "JsonVector",
        }
        .fail()
    }
}

pub(crate) fn align_json_value_with_type<'a>(
    expected_type: &JsonType,
    value: &'a JsonValue,
) -> Cow<'a, JsonValue> {
    if value.json_type() == expected_type {
        return Cow::Borrowed(value);
    }

    fn helper(expected_type: &JsonNativeType, value: JsonVariant) -> JsonVariant {
        match (expected_type, value) {
            (_, JsonVariant::Null) | (JsonNativeType::Null, _) => JsonVariant::Null,
            (JsonNativeType::Bool, JsonVariant::Bool(v)) => JsonVariant::Bool(v),
            (JsonNativeType::Number(_), JsonVariant::Number(v)) => JsonVariant::Number(v),
            (JsonNativeType::String, JsonVariant::String(v)) => JsonVariant::String(v),

            (JsonNativeType::Array(item_type), JsonVariant::Array(items)) => JsonVariant::Array(
                items
                    .into_iter()
                    .map(|item| helper(item_type.as_ref(), item))
                    .collect(),
            ),

            (JsonNativeType::Object(expected_fields), JsonVariant::Object(object)) => {
                JsonVariant::Object(
                    expected_fields
                        .iter()
                        .map(|(field_name, expected_field_type)| {
                            let value =
                                object.get(field_name).cloned().unwrap_or(JsonVariant::Null);
                            (field_name.clone(), helper(expected_field_type, value))
                        })
                        .collect(),
                )
            }

            (JsonNativeType::String, v) => {
                let json: serde_json::Value = JsonValue::from(v).into();
                JsonVariant::String(json.to_string())
            }

            (t, v) => panic!("unsupported json alignment cast from {v} to {t}"),
        }
    }

    let value = helper(expected_type.native_type(), value.clone().into_variant());
    Cow::Owned(JsonValue::from(value))
}
