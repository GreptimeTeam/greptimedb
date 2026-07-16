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
use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_schema::DataType;

use crate::data_type::ConcreteDataType;
use crate::error::{Result, TryFromValueSnafu, UnexpectedSnafu, UnsupportedOperationSnafu};
use crate::json::value::{JsonNumber, JsonVariant, encode_json_variant};
use crate::prelude::{ValueRef, Vector, VectorRef};
use crate::types::StructType;
use crate::types::json_type::{JsonNativeType, is_include};
use crate::value::{ListValue, StructValue, Value};
use crate::vectors::{MutableVector, StructVectorBuilder};

#[derive(Clone)]
pub(crate) struct JsonVectorBuilder {
    merged_type: JsonNativeType,
    values: Vec<JsonVariant>,
}

impl JsonVectorBuilder {
    pub(crate) fn new(initial_native_type: JsonNativeType, capacity: usize) -> Self {
        debug_assert!(matches!(
            initial_native_type,
            JsonNativeType::Object(_) | JsonNativeType::Null
        ));
        Self {
            merged_type: initial_native_type,
            values: Vec::with_capacity(capacity),
        }
    }

    fn try_build(&mut self) -> Result<VectorRef> {
        let DataType::Struct(fields) = self.merged_type.as_arrow_type() else {
            return UnexpectedSnafu {
                reason: "merged JSON2 type must map to Arrow Struct in JsonVectorBuilder",
            }
            .fail();
        };
        // TODO(LFC): Direct use Arrow's Struct datatype here.
        let struct_type = StructType::from(&fields);

        let mut builder =
            StructVectorBuilder::with_type_and_capacity(struct_type.clone(), self.values.len());
        for value in std::mem::take(&mut self.values) {
            match value {
                JsonVariant::Null => builder.push_null(),
                JsonVariant::Object(object) => push_json_object(&mut builder, object)?,
                value => {
                    return TryFromValueSnafu {
                        reason: format!("expected json object value, got {value:?}"),
                    }
                    .fail();
                }
            }
        }
        Ok(builder.to_vector())
    }
}

fn push_json_object(
    builder: &mut StructVectorBuilder,
    object: BTreeMap<String, JsonVariant>,
) -> Result<()> {
    let mut entries = object.into_iter();
    let mut entry = entries.next();
    builder.try_push_row_with(|builder, field_name, field_type| match entry.take() {
        Some((name, value)) if name == field_name => {
            entry = entries.next();
            push_json_variant(builder, value, field_type)
        }
        Some((name, _)) if name.as_str() < field_name => TryFromValueSnafu {
            reason: format!("field {name} is missing from merged JSON type"),
        }
        .fail(),
        next => {
            entry = next;
            builder.push_null();
            Ok(())
        }
    })?;
    if let Some((name, _)) = entry {
        return TryFromValueSnafu {
            reason: format!("field {name} is missing from merged JSON type"),
        }
        .fail();
    }

    Ok(())
}

fn push_json_variant(
    builder: &mut dyn MutableVector,
    value: JsonVariant,
    expected_type: &ConcreteDataType,
) -> Result<()> {
    match (value, expected_type) {
        (JsonVariant::Null, _) | (_, ConcreteDataType::Null(_)) => {
            builder.push_null();
            Ok(())
        }
        (JsonVariant::Object(object), ConcreteDataType::Struct(_)) => {
            let Some(builder) = builder.as_mut_any().downcast_mut::<StructVectorBuilder>() else {
                return UnexpectedSnafu {
                    reason: "JSON object field must use StructVectorBuilder",
                }
                .fail();
            };
            push_json_object(builder, object)
        }
        (value, expected_type) => {
            builder.try_push_value(json_variant_into_value(value, expected_type)?)
        }
    }
}

fn json_variant_into_struct_value(
    value: JsonVariant,
    struct_type: StructType,
) -> Result<StructValue> {
    let JsonVariant::Object(object) = value else {
        return TryFromValueSnafu {
            reason: format!("expected json object value, got {value:?}"),
        }
        .fail();
    };

    let mut entries = object.into_iter();
    let mut entry = entries.next();
    let mut values = Vec::with_capacity(struct_type.fields().len());
    for field in struct_type.fields().iter() {
        let value = match entry.take() {
            Some((name, value)) if name == field.name() => {
                entry = entries.next();
                json_variant_into_value(value, field.data_type())?
            }
            Some((name, _)) if name.as_str() < field.name() => {
                return TryFromValueSnafu {
                    reason: format!("field {name} is missing from merged JSON type"),
                }
                .fail();
            }
            next => {
                entry = next;
                Value::Null
            }
        };
        values.push(value);
    }
    if let Some((name, _)) = entry {
        return TryFromValueSnafu {
            reason: format!("field {name} is missing from merged JSON type"),
        }
        .fail();
    }

    Ok(StructValue::new(values, struct_type))
}

fn json_variant_into_value(value: JsonVariant, expected_type: &ConcreteDataType) -> Result<Value> {
    let value = match (value, expected_type) {
        (JsonVariant::Null, _) | (_, ConcreteDataType::Null(_)) => Value::Null,
        (JsonVariant::Bool(x), ConcreteDataType::Boolean(_)) => Value::Boolean(x),
        (JsonVariant::Number(JsonNumber::PosInt(x)), ConcreteDataType::UInt64(_)) => {
            Value::UInt64(x)
        }
        (JsonVariant::Number(x), ConcreteDataType::Int64(_)) => {
            let x = match x {
                JsonNumber::PosInt(x) => i64::try_from(x).ok(),
                JsonNumber::NegInt(x) => Some(x),
                JsonNumber::Float(_) => None,
            };
            let Some(x) = x else {
                return TryFromValueSnafu {
                    reason: format!("unable to convert {x:?} to Int64"),
                }
                .fail();
            };
            Value::Int64(x)
        }
        (JsonVariant::Number(JsonNumber::PosInt(x)), ConcreteDataType::Float64(_)) => {
            Value::Float64((x as f64).into())
        }
        (JsonVariant::Number(JsonNumber::NegInt(x)), ConcreteDataType::Float64(_)) => {
            Value::Float64((x as f64).into())
        }
        (JsonVariant::Number(JsonNumber::Float(x)), ConcreteDataType::Float64(_)) => {
            Value::Float64(x)
        }
        (JsonVariant::String(x), ConcreteDataType::String(_)) => Value::String(x.into()),
        (JsonVariant::Array(array), ConcreteDataType::List(list_type)) => {
            let item_type = list_type.item_type().clone();
            let values = array
                .into_iter()
                .map(|v| json_variant_into_value(v, &item_type))
                .collect::<Result<Vec<_>>>()?;
            Value::List(ListValue::new(values, Arc::new(item_type)))
        }
        (value @ JsonVariant::Object(_), ConcreteDataType::Struct(struct_type)) => {
            Value::Struct(json_variant_into_struct_value(value, struct_type.clone())?)
        }
        (value, ConcreteDataType::Binary(_)) => Value::from(encode_json_variant(value)?),
        (value, expected_type) => {
            return TryFromValueSnafu {
                reason: format!("unable to convert json value {value:?} to {expected_type}"),
            }
            .fail();
        }
    };
    Ok(value)
}

impl MutableVector for JsonVectorBuilder {
    fn data_type(&self) -> ConcreteDataType {
        ConcreteDataType::json2(self.merged_type.clone())
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
        let json_type = json_type.as_ref();
        if !matches!(json_type, JsonNativeType::Object(_) | JsonNativeType::Null) {
            return TryFromValueSnafu {
                reason: format!("expected json object value, got {value:?}"),
            }
            .fail();
        }
        if !is_include(&self.merged_type, json_type) {
            self.merged_type.merge(json_type);
        }

        self.values.push(JsonVariant::from(value.variant()));
        Ok(())
    }

    fn try_push_value(&mut self, value: Value) -> Result<()> {
        let Value::Json(value) = value else {
            return TryFromValueSnafu {
                reason: format!("expected json value, got {value:?}"),
            }
            .fail();
        };
        let json_type = value.json_type();
        if !matches!(json_type, JsonNativeType::Object(_) | JsonNativeType::Null) {
            return TryFromValueSnafu {
                reason: format!("expected json object value, got {value:?}"),
            }
            .fail();
        }
        if !is_include(&self.merged_type, json_type) {
            self.merged_type.merge(json_type);
        }

        self.values.push(value.into_variant());
        Ok(())
    }

    fn push_null(&mut self) {
        self.values.push(JsonVariant::Null)
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
    use std::sync::Arc;

    use common_base::bytes::Bytes;

    use super::*;
    use crate::data_type::ConcreteDataType;
    use crate::types::StructField;
    use crate::types::json_type::JsonObjectType;
    use crate::value::{ListValue, StructValue, Value, ValueRef};

    #[test]
    fn test_json_vector_builder() -> Result<()> {
        fn parse_json_value(json: &str) -> Value {
            let value: serde_json::Value = serde_json::from_str(json).unwrap();
            Value::Json(Box::new(value.into()))
        }

        fn jsonb_bytes(json: &str) -> Bytes {
            Bytes::from(jsonb::parse_value(json.as_bytes()).unwrap().to_vec())
        }

        // Object inputs should merge into a superset schema, preserve null rows,
        // and project conflicting nested values into Variant payloads.
        let mut builder = JsonVectorBuilder::new(JsonNativeType::Object(Default::default()), 3);
        let first = parse_json_value(r#"{"id":1,"payload":{"name":"foo"}}"#);
        let second = parse_json_value(r#"{"id":2,"extra":true,"payload":"raw"}"#);
        builder.try_push_value_ref(&first.as_value_ref())?;
        builder.push_null();
        builder.try_push_value(second)?;

        let merged_type = JsonNativeType::Object(JsonObjectType::from([
            ("extra".to_string(), JsonNativeType::Bool),
            ("id".to_string(), JsonNativeType::i64()),
            ("payload".to_string(), JsonNativeType::Variant),
        ]));
        assert_eq!(
            builder.data_type(),
            ConcreteDataType::json2(merged_type.clone())
        );

        let DataType::Struct(fields) = merged_type.as_arrow_type() else {
            unreachable!()
        };
        let merged_struct_type = StructType::from(&fields);
        let vector = builder.to_vector();
        assert_eq!(vector.len(), 3);
        assert_eq!(
            vector.get(0),
            Value::Struct(StructValue::new(
                vec![
                    Value::Null,
                    Value::Int64(1),
                    Value::Binary(jsonb_bytes(r#"{"name":"foo"}"#)),
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
                    Value::Binary(jsonb_bytes(r#""raw""#)),
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

        let inferred_type = JsonNativeType::Object(JsonObjectType::from([(
            "id".to_string(),
            JsonNativeType::i64(),
        )]));
        assert_eq!(
            inferred_builder.data_type(),
            ConcreteDataType::json2(inferred_type.clone())
        );

        let DataType::Struct(fields) = inferred_type.as_arrow_type() else {
            unreachable!()
        };
        let inferred_struct_type = StructType::from(&fields);
        let vector = inferred_builder.to_vector();
        assert_eq!(vector.get(0), Value::Null);
        assert_eq!(
            vector.get(1),
            Value::Struct(StructValue::new(
                vec![Value::Int64(3)],
                inferred_struct_type,
            ))
        );

        // Nested objects should be written directly into nested struct builders.
        let mut nested_builder =
            JsonVectorBuilder::new(JsonNativeType::Object(Default::default()), 1);
        nested_builder.try_push_value(parse_json_value(r#"{"payload":{"name":"foo"}}"#))?;
        let value = nested_builder.to_vector().get(0);
        let Value::Struct(root) = value else {
            panic!("expected root struct value");
        };
        let Value::Struct(payload) = &root.items()[0] else {
            panic!("expected nested struct value");
        };
        assert_eq!(payload.items(), &[Value::String("foo".into())]);

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

    #[test]
    fn test_json_variant_into_struct_value() -> Result<()> {
        let item_type =
            ConcreteDataType::struct_datatype(StructType::new(Arc::new(vec![StructField::new(
                "id".to_string(),
                ConcreteDataType::int64_datatype(),
                true,
            )])));
        let struct_type = StructType::new(Arc::new(vec![
            StructField::new(
                "items".to_string(),
                ConcreteDataType::list_datatype(Arc::new(item_type.clone())),
                true,
            ),
            StructField::new(
                "meta".to_string(),
                ConcreteDataType::struct_datatype(StructType::new(Arc::new(vec![
                    StructField::new(
                        "name".to_string(),
                        ConcreteDataType::string_datatype(),
                        true,
                    ),
                ]))),
                true,
            ),
        ]));
        let variant = JsonVariant::from([
            (
                "items",
                JsonVariant::Array(vec![
                    JsonVariant::from([("id", JsonVariant::from(1i64))]),
                    JsonVariant::from([("id", JsonVariant::from(2i64))]),
                ]),
            ),
            (
                "meta",
                JsonVariant::from([("name", JsonVariant::from("foo"))]),
            ),
        ]);
        let value = Value::Struct(json_variant_into_struct_value(
            variant,
            struct_type.clone(),
        )?);

        assert_eq!(
            value,
            Value::Struct(StructValue::new(
                vec![
                    Value::List(ListValue::new(
                        vec![
                            Value::Struct(StructValue::new(
                                vec![Value::Int64(1)],
                                StructType::new(Arc::new(vec![StructField::new(
                                    "id".to_string(),
                                    ConcreteDataType::int64_datatype(),
                                    true,
                                )]))
                            )),
                            Value::Struct(StructValue::new(
                                vec![Value::Int64(2)],
                                StructType::new(Arc::new(vec![StructField::new(
                                    "id".to_string(),
                                    ConcreteDataType::int64_datatype(),
                                    true,
                                )]))
                            )),
                        ],
                        Arc::new(item_type),
                    )),
                    Value::Struct(StructValue::new(
                        vec![Value::String("foo".into())],
                        StructType::new(Arc::new(vec![StructField::new(
                            "name".to_string(),
                            ConcreteDataType::string_datatype(),
                            true,
                        )])),
                    )),
                ],
                struct_type,
            ))
        );
        Ok(())
    }
}
