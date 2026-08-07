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
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, StructArray};
use arrow_schema::DataType;
use snafu::{OptionExt, ResultExt};

use crate::data_type::ConcreteDataType;
#[cfg(test)]
use crate::error::InvalidJson2LayoutSnafu;
use crate::error::{
    Result, SerializeSnafu, TryFromValueSnafu, UnexpectedSnafu, UnsupportedOperationSnafu,
};
use crate::extension::json::JSON2_REMAINDER_FIELD_NAME;
#[cfg(test)]
use crate::json::JsonSettings;
use crate::json::value::{
    JsonNumber, JsonObjectVariant, JsonValue, JsonVariant, encode_json_variant,
};
use crate::prelude::{ValueRef, Vector, VectorRef};
use crate::types::StructType;
#[cfg(test)]
use crate::types::json_type::JsonObjectType;
use crate::types::json_type::{JsonNativeType, is_include};
use crate::value::{ListValue, StructValue, StructValueRef, Value};
use crate::vectors::json::variant::{json_values_to_variant, variant_field};
use crate::vectors::{Helper, MutableVector, StructVectorBuilder};

#[derive(Clone)]
pub(crate) struct JsonVectorBuilder {
    merged_type: JsonNativeType,
    bounded: bool,
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
            bounded: false,
            values: Vec::with_capacity(capacity),
        }
    }

    #[cfg(test)]
    fn with_settings(settings: &JsonSettings, capacity: usize) -> Result<Self> {
        if settings.max_auto_expanded_paths != Some(0) {
            return Ok(Self::new(JsonNativeType::object(), capacity));
        }

        Ok(Self {
            merged_type: type_hint_native_type(settings)?,
            bounded: true,
            values: Vec::with_capacity(capacity),
        })
    }

    fn try_build(&mut self) -> Result<VectorRef> {
        if self.bounded {
            return self.try_build_bounded();
        }

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
            if matches!(&value, JsonVariant::Null) {
                builder.push_null();
                continue;
            }
            let JsonVariant::Object(value) = value else {
                return TryFromValueSnafu {
                    reason: format!("expected json object value, got {value:?}"),
                }
                .fail();
            };
            let value = json_variant_into_struct_value(value, struct_type.clone())?;
            builder.push_struct_value_ref(StructValueRef::Ref(&value))?;
        }
        Ok(builder.to_vector())
    }

    fn try_build_bounded(&mut self) -> Result<VectorRef> {
        let DataType::Struct(fields) = self.merged_type.as_arrow_type() else {
            return UnexpectedSnafu {
                reason: "bounded JSON2 type must map to Arrow Struct in JsonVectorBuilder",
            }
            .fail();
        };
        let struct_type = StructType::from(&fields);
        let mut explicit =
            StructVectorBuilder::with_type_and_capacity(struct_type.clone(), self.values.len());
        let mut remainders = Vec::with_capacity(self.values.len());

        for value in std::mem::take(&mut self.values) {
            if matches!(value, JsonVariant::Null) {
                explicit.push_null();
                remainders.push(None);
                continue;
            }

            let (value, remainder) = split_to_explicit(value, &self.merged_type)?;
            let value = json_variant_into_struct_value(value, struct_type.clone())?;
            explicit.push_struct_value_ref(StructValueRef::Ref(&value))?;
            let remainder =
                serde_json::Value::try_from(JsonValue::from(remainder)).context(SerializeSnafu)?;
            remainders.push(Some(remainder));
        }

        let explicit = explicit.to_vector().to_arrow_array();
        let explicit =
            explicit
                .as_any()
                .downcast_ref::<StructArray>()
                .context(UnexpectedSnafu {
                    reason: "StructVectorBuilder does not produce StructArray",
                })?;
        let remainder = json_values_to_variant(&remainders)?;
        let mut children = explicit
            .fields()
            .iter()
            .cloned()
            .zip(explicit.columns().iter().cloned())
            .collect::<Vec<_>>();
        children.push((
            Arc::new(variant_field(JSON2_REMAINDER_FIELD_NAME, true)),
            remainder,
        ));
        children.sort_unstable_by(|(x, _), (y, _)| x.name().cmp(y.name()));
        let (fields, columns): (Vec<_>, Vec<ArrayRef>) = children.into_iter().unzip();
        let array: ArrayRef = Arc::new(StructArray::new(
            fields.into(),
            columns,
            explicit.nulls().cloned(),
        ));
        Helper::try_into_vector(array)
    }
}

#[cfg(test)]
fn type_hint_native_type(settings: &JsonSettings) -> Result<JsonNativeType> {
    let mut object = JsonObjectType::new();
    for hint in &settings.type_hints {
        insert_type_hint(&mut object, &hint.path, (&hint.data_type).into())?;
    }
    Ok(JsonNativeType::Object(object))
}

#[cfg(test)]
fn insert_type_hint(
    object: &mut JsonObjectType,
    path: &[String],
    data_type: JsonNativeType,
) -> Result<()> {
    let Some((name, path)) = path.split_first() else {
        return InvalidJson2LayoutSnafu {
            reason: "JSON2 type hint path must not be empty".to_string(),
        }
        .fail();
    };

    if path.is_empty() {
        if object.insert(name.clone(), data_type).is_some() {
            return InvalidJson2LayoutSnafu {
                reason: format!("duplicate JSON2 type hint path '{name}'"),
            }
            .fail();
        }
        return Ok(());
    }

    let child = object
        .entry(name.clone())
        .or_insert_with(|| JsonNativeType::Object(JsonObjectType::new()));
    let JsonNativeType::Object(child) = child else {
        return InvalidJson2LayoutSnafu {
            reason: format!("conflicting JSON2 type hint path at '{name}'"),
        }
        .fail();
    };
    insert_type_hint(child, path, data_type)
}

fn split_to_explicit(
    value: JsonVariant,
    explicit_type: &JsonNativeType,
) -> Result<(JsonObjectVariant, JsonObjectVariant)> {
    let JsonVariant::Object(mut remainder) = value else {
        return TryFromValueSnafu {
            reason: "expected json object value".to_string(),
        }
        .fail();
    };
    let JsonNativeType::Object(fields) = explicit_type else {
        return UnexpectedSnafu {
            reason: "bounded JSON2 explicit type must be an object",
        }
        .fail();
    };
    let mut explicit = JsonObjectVariant::new();

    for (name, data_type) in fields {
        let Some(value) = remainder.remove(name) else {
            continue;
        };
        if matches!(data_type, JsonNativeType::Object(_)) {
            let (value, child_remainder) = split_to_explicit(value, data_type)?;
            if !value.is_empty() {
                explicit.insert(name.clone(), JsonVariant::Object(value));
            }
            if !child_remainder.is_empty() {
                remainder.insert(name.clone(), JsonVariant::Object(child_remainder));
            }
        } else {
            explicit.insert(name.clone(), value);
        }
    }

    Ok((explicit, remainder))
}

fn json_variant_into_struct_value(
    object: JsonObjectVariant,
    struct_type: StructType,
) -> Result<StructValue> {
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
        (JsonVariant::Object(object), _) if object.is_empty() => Value::Null,
        (JsonVariant::Bool(x), ConcreteDataType::Boolean(_)) => Value::Boolean(x),
        (JsonVariant::Number(x), ConcreteDataType::UInt64(_)) => {
            let Some(x) = x.as_u64() else {
                return TryFromValueSnafu {
                    reason: format!("unable to convert {x:?} to UInt64"),
                }
                .fail();
            };
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
        (JsonVariant::Object(value), ConcreteDataType::Struct(struct_type)) => {
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
        self.try_build().unwrap_or_else(|e| panic!("{:?}", e))
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
        if !self.bounded && !is_include(&self.merged_type, json_type) {
            self.merged_type.merge(json_type);
        }

        self.values.push(JsonVariant::from(value.variant()));
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

    use arrow_array::cast::AsArray;
    use arrow_schema::Field;
    use common_base::bytes::Bytes;
    use serde_json::json;

    use super::*;
    use crate::data_type::ConcreteDataType;
    use crate::extension::json::{Json2ExtensionType, JsonMetadata};
    use crate::json::JsonTypeHint;
    use crate::json::value::decode_json_variant;
    use crate::types::StructField;
    use crate::types::json_type::JsonObjectType;
    use crate::value::{ListValue, StructValue, Value, ValueRef};
    use crate::vectors::json::array::JsonArray;
    use crate::vectors::json::variant::variant_to_json_values;

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
        builder.try_push_value_ref(&second.as_value_ref())?;

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
    fn test_zero_budget_builder_uses_fixed_schema_and_remainder() -> Result<()> {
        let settings = JsonSettings {
            type_hints: vec![
                JsonTypeHint {
                    path: vec!["kind".to_string()],
                    data_type: ConcreteDataType::string_datatype(),
                    nullable: true,
                    default_constraint: None,
                    inverted_index: false,
                },
                JsonTypeHint {
                    path: vec!["commit".to_string(), "operation".to_string()],
                    data_type: ConcreteDataType::string_datatype(),
                    nullable: true,
                    default_constraint: None,
                    inverted_index: false,
                },
            ],
            max_auto_expanded_paths: Some(0),
        };
        let mut builder = JsonVectorBuilder::with_settings(&settings, 2)?;
        let values = [
            json!({
                "kind": "record",
                "commit": {"operation": "create", "collection": "post"},
                "extra": 1
            }),
            json!({"kind": "other", "dynamic": true}),
        ];
        for value in values.clone() {
            let value = settings.encode(value)?;
            builder.try_push_value_ref(&value.as_value_ref())?;
        }

        let array = builder.to_vector().to_arrow_array();
        let structs = array.as_struct();
        assert_eq!(
            vec![JSON2_REMAINDER_FIELD_NAME, "commit", "kind"],
            structs
                .fields()
                .iter()
                .map(|x| x.name().as_str())
                .collect::<Vec<_>>()
        );
        assert_eq!(
            vec![
                Some(json!({"commit": {"collection": "post"}, "extra": 1})),
                Some(json!({"dynamic": true})),
            ],
            variant_to_json_values(structs.column_by_name(JSON2_REMAINDER_FIELD_NAME).unwrap())?
        );

        let field = Field::new("data", array.data_type().clone(), true).with_extension_type(
            Json2ExtensionType::new(Arc::new(JsonMetadata::new(settings))),
        );
        let reconstructed = JsonArray::from(&array).project_json2(&field, &DataType::Binary)?;
        let reconstructed = reconstructed.as_binary::<i32>();
        assert_eq!(
            values[0],
            decode_json_variant(reconstructed.value(0)).unwrap()
        );
        assert_eq!(
            json!({
                "kind": "other",
                "commit": {"operation": null},
                "dynamic": true
            }),
            decode_json_variant(reconstructed.value(1)).unwrap()
        );

        let settings = JsonSettings {
            max_auto_expanded_paths: Some(0),
            ..Default::default()
        };
        let mut builder = JsonVectorBuilder::with_settings(&settings, 3)?;
        for value in [json!({}), json!({"x": 1})] {
            let value = settings.encode(value)?;
            builder.try_push_value_ref(&value.as_value_ref())?;
        }
        builder.push_null();
        let array = builder.to_vector().to_arrow_array();
        let structs = array.as_struct();
        assert_eq!(1, structs.num_columns());
        assert_eq!(
            vec![Some(json!({})), Some(json!({"x": 1})), None],
            variant_to_json_values(structs.column(0))?
        );

        Ok(())
    }

    #[test]
    fn test_json_variant_into_struct_value() -> Result<()> {
        assert_eq!(
            json_variant_into_value(
                JsonVariant::Object(Default::default()),
                &ConcreteDataType::string_datatype(),
            )?,
            Value::Null
        );

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
        let variant = JsonObjectVariant::from([
            (
                "items".to_string(),
                JsonVariant::Array(vec![
                    JsonVariant::from([("id", JsonVariant::from(1i64))]),
                    JsonVariant::from([("id", JsonVariant::from(2i64))]),
                ]),
            ),
            (
                "meta".to_string(),
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
