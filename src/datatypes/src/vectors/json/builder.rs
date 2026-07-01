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

use crate::data_type::ConcreteDataType;
use crate::error::{Result, TryFromValueSnafu, UnsupportedOperationSnafu};
use crate::json::value::{JsonValue, JsonVariant};
use crate::prelude::{ValueRef, Vector, VectorRef};
use crate::types::JsonType;
use crate::types::json_type::{JsonFormat, JsonNativeType};
use crate::vectors::{MutableVector, StructVectorBuilder};

#[derive(Clone)]
pub(crate) struct JsonVectorBuilder {
    merged_type: JsonType,
    values: Vec<JsonValue>,
}

impl JsonVectorBuilder {
    pub(crate) fn new(initial_native_type: JsonNativeType, capacity: usize) -> Self {
        debug_assert!(matches!(
            initial_native_type,
            JsonNativeType::Object(_) | JsonNativeType::Null
        ));
        Self {
            merged_type: JsonType::new_json2(initial_native_type),
            values: Vec::with_capacity(capacity),
        }
    }

    fn try_build(&mut self) -> Result<VectorRef> {
        let mut builder = StructVectorBuilder::with_type_and_capacity(
            self.merged_type.as_struct_type(),
            self.values.len(),
        );
        for value in self.values.iter_mut() {
            if value.is_null() {
                builder.push_null();
                continue;
            }
            value.try_align(&self.merged_type)?;
            builder.try_push_value_ref(&value.as_ref().as_value_ref())?;
        }
        Ok(builder.to_vector())
    }
}

impl MutableVector for JsonVectorBuilder {
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
        self.try_build().unwrap_or_else(|e| panic!("{}", e))
    }

    fn to_vector_cloned(&self) -> VectorRef {
        self.clone().to_vector()
    }

    fn try_push_value_ref(&mut self, value: &ValueRef) -> Result<()> {
        let ValueRef::Json(value) = value else {
            return TryFromValueSnafu {
                reason: format!("expected json value, got {value:?}"),
            }
            .fail();
        };
        let json_type = value.json_type();
        if !matches!(
            json_type.format,
            JsonFormat::Json2(ref native_type)
                if matches!(native_type.as_ref(), JsonNativeType::Object(_) | JsonNativeType::Null)
        ) {
            return TryFromValueSnafu {
                reason: format!("expected json object value, got {value:?}"),
            }
            .fail();
        }
        if !self.merged_type.is_include(json_type) {
            self.merged_type.merge(json_type)?;
        }

        let value = JsonValue::new_with(
            JsonVariant::from(value.variant().clone()),
            json_type.clone(),
        );
        self.values.push(value);
        Ok(())
    }

    fn push_null(&mut self) {
        self.values.push(JsonValue::null())
    }

    fn extend_slice_of(&mut self, _: &dyn Vector, _: usize, _: usize) -> Result<()> {
        UnsupportedOperationSnafu {
            op: "extend_slice_of",
            vector_type: "JsonVector",
        }
        .fail()
    }
}

#[cfg(test)]
mod tests {
    use common_base::bytes::Bytes;

    use super::*;
    use crate::data_type::ConcreteDataType;
    use crate::types::json_type::JsonObjectType;
    use crate::value::{StructValue, Value, ValueRef};

    #[test]
    fn test_json_vector_builder() -> Result<()> {
        fn parse_json_value(json: &str) -> Value {
            let value: serde_json::Value = serde_json::from_str(json).unwrap();
            Value::Json(Box::new(value.into()))
        }

        // Object inputs should merge into a superset schema, preserve null rows,
        // and align conflicting nested values into Variant payloads.
        let mut builder = JsonVectorBuilder::new(JsonNativeType::Object(Default::default()), 3);
        let first = parse_json_value(r#"{"id":1,"payload":{"name":"foo"}}"#);
        let second = parse_json_value(r#"{"id":2,"extra":true,"payload":"raw"}"#);
        builder.try_push_value_ref(&first.as_value_ref())?;
        builder.push_null();
        builder.try_push_value_ref(&second.as_value_ref())?;

        let merged_type = JsonType::new_json2(JsonNativeType::Object(JsonObjectType::from([
            ("extra".to_string(), JsonNativeType::Bool),
            ("id".to_string(), JsonNativeType::i64()),
            ("payload".to_string(), JsonNativeType::Variant),
        ])));
        assert_eq!(
            builder.data_type(),
            ConcreteDataType::Json(merged_type.clone())
        );

        let merged_struct_type = merged_type.as_struct_type();
        let vector = builder.to_vector();
        assert_eq!(vector.len(), 3);
        assert_eq!(
            vector.get(0),
            Value::Struct(StructValue::new(
                vec![
                    Value::Null,
                    Value::Int64(1),
                    Value::Binary(Bytes::from(br#"{"name":"foo"}"#.to_vec())),
                ],
                merged_struct_type.clone(),
            ))
        );
        assert_eq!(vector.get(1), Value::Null);
        assert_eq!(
            vector.get(2),
            Value::Struct(StructValue::new(
                vec![
                    Value::Boolean(true),
                    Value::Int64(2),
                    Value::Binary(Bytes::from(br#""raw""#.to_vec())),
                ],
                merged_struct_type,
            ))
        );

        // A Null initial type represents an unknown JSON2 runtime type. The first
        // non-null value should set the concrete type instead of aligning all rows to Null.
        let mut inferred_builder = JsonVectorBuilder::new(JsonNativeType::Null, 2);
        let inferred_value = parse_json_value(r#"{"id":3}"#);
        inferred_builder.push_null();
        inferred_builder.try_push_value_ref(&inferred_value.as_value_ref())?;

        let inferred_type = JsonType::new_json2(JsonNativeType::Object(JsonObjectType::from([(
            "id".to_string(),
            JsonNativeType::i64(),
        )])));
        assert_eq!(
            inferred_builder.data_type(),
            ConcreteDataType::Json(inferred_type.clone())
        );

        let inferred_struct_type = inferred_type.as_struct_type();
        let vector = inferred_builder.to_vector();
        assert_eq!(vector.get(0), Value::Null);
        assert_eq!(
            vector.get(1),
            Value::Struct(StructValue::new(
                vec![Value::Int64(3)],
                inferred_struct_type,
            ))
        );

        // Non-object initial types are rejected by the builder invariant.
        let result = std::panic::catch_unwind(|| JsonVectorBuilder::new(JsonNativeType::Bool, 2));
        assert!(result.is_err());

        // Non-object root values should be rejected at push time.
        let mut object_builder =
            JsonVectorBuilder::new(JsonNativeType::Object(Default::default()), 2);
        let object = parse_json_value(r#"{"k":1}"#);
        let boolean = parse_json_value("true");
        let err = object_builder
            .try_push_value_ref(&boolean.as_value_ref())
            .unwrap_err();
        assert!(err.to_string().contains("expected json object value"));
        object_builder.try_push_value_ref(&object.as_value_ref())?;

        // Non-JSON values should be rejected at push time.
        let mut invalid_builder =
            JsonVectorBuilder::new(JsonNativeType::Object(Default::default()), 1);
        let err = invalid_builder
            .try_push_value_ref(&ValueRef::Boolean(true))
            .unwrap_err();
        assert!(err.to_string().contains("expected json value"));

        Ok(())
    }
}
