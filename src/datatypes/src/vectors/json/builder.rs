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
use crate::types::json_type::JsonNativeType;
use crate::vectors::{MutableVector, StructVectorBuilder};

#[derive(Clone)]
pub(crate) struct JsonVectorBuilder {
    merged_type: JsonType,
    values: Vec<JsonValue>,
}

impl JsonVectorBuilder {
    pub(crate) fn new(json_type: JsonNativeType, capacity: usize) -> Self {
        Self {
            merged_type: JsonType::new_json2(json_type),
            values: Vec::with_capacity(capacity),
        }
    }

    fn try_build(&mut self) -> Result<VectorRef> {
        let mut builder = StructVectorBuilder::with_type_and_capacity(
            self.merged_type.as_struct_type(),
            self.values.len(),
        );
        for value in self.values.iter_mut() {
            value.try_align(&self.merged_type)?;

            if value.is_null() {
                builder.push_null();
                continue;
            }

            let value = value.as_ref();
            builder.try_push_value_ref(&value.as_struct_value())?;
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
        self.merged_type.merge(json_type)?;

        let value = JsonValue::new(JsonVariant::from(value.variant().clone()));
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

        // Root-level conflicts should be lifted to a plain Variant field that preserves
        // each original JSON payload.
        let mut variant_builder = JsonVectorBuilder::new(JsonNativeType::Bool, 2);
        let object = parse_json_value(r#"{"k":1}"#);
        let boolean = parse_json_value("true");
        variant_builder.try_push_value_ref(&boolean.as_value_ref())?;
        variant_builder.try_push_value_ref(&object.as_value_ref())?;

        let variant_type = JsonType::new_json2(JsonNativeType::Variant);
        assert_eq!(
            variant_builder.data_type(),
            ConcreteDataType::Json(variant_type.clone())
        );

        let variant_struct_type = variant_type.as_struct_type();
        let vector = variant_builder.to_vector();
        assert_eq!(
            vector.get(0),
            Value::Struct(StructValue::new(
                vec![Value::Binary(Bytes::from(b"true".to_vec()))],
                variant_struct_type.clone(),
            ))
        );
        assert_eq!(
            vector.get(1),
            Value::Struct(StructValue::new(
                vec![Value::Binary(Bytes::from(br#"{"k":1}"#.to_vec()))],
                variant_struct_type,
            ))
        );

        // Non-JSON values should be rejected at push time.
        let mut invalid_builder = JsonVectorBuilder::new(JsonNativeType::Bool, 1);
        let err = invalid_builder
            .try_push_value_ref(&ValueRef::Boolean(true))
            .unwrap_err();
        assert!(err.to_string().contains("expected json value"));

        Ok(())
    }
}
