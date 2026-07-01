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

use arrow_schema::{DataType as ArrowDataType, Field, FieldRef, Fields};
use parquet::variant::json_to_variant;

use crate::arrow::array::{
    ArrayRef, BooleanArray, Float64Array, Int64Array, ListArray, StringArray, StructArray,
    UInt64Array,
};
use crate::arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use crate::data_type::ConcreteDataType;
use crate::error::{InvalidVectorSnafu, Result, TryFromValueSnafu, UnsupportedOperationSnafu};
use crate::json::value::{JsonNumber, JsonValue, JsonVariant, JsonVariantRef};
use crate::prelude::{ValueRef, Vector, VectorRef};
use crate::types::json_type::{JsonFormat, JsonNativeType, JsonNumberType, JsonObjectType};
use crate::types::{JsonType, StructType};
use crate::value::Value;
use crate::vectors::{Helper, MutableVector, StructVector};

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
        build_json2_struct_vector(&mut self.values, self.merged_type.native_type())
    }
}

fn build_json2_struct_vector(
    vals: &mut [JsonValue],
    native_type: &JsonNativeType,
) -> Result<VectorRef> {
    let json_type = JsonType::new_json2(native_type.clone());
    for val in vals.iter_mut() {
        val.try_align(&json_type)?;
    }

    let variants = vals
        .iter()
        .map(|val| val.as_ref().into_variant())
        .collect::<Vec<_>>();

    let JsonNativeType::Object(_) = native_type else {
        return InvalidVectorSnafu {
            msg: format!("expected json object type, got {native_type}"),
        }
        .fail();
    };

    let (_, array) = build_json_array("", &variants, native_type)?;
    let json_struct_array = array
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| {
            InvalidVectorSnafu {
                msg: format!(
                    "expected JSON2 to be a StructArray, actual array type: {}",
                    array.data_type()
                ),
            }
            .build()
        })?
        .clone();

    let fields = StructType::from(json_struct_array.fields());
    Ok(Arc::new(StructVector::try_new(fields, json_struct_array)?))
}

fn build_json_array(
    name: &str,
    vals: &[JsonVariantRef<'_>],
    native_type: &JsonNativeType,
) -> Result<(FieldRef, ArrayRef)> {
    match native_type {
        JsonNativeType::Variant => build_parquet_variant_array(name, vals),
        JsonNativeType::Object(object_type) => build_json_object_array(name, vals, object_type),
        JsonNativeType::Array(item_type) => build_json_list_array(name, vals, item_type),
        _ => build_plain_json_variant_array(name, vals, native_type),
    }
}

fn build_parquet_variant_array(
    name: &str,
    vals: &[JsonVariantRef<'_>],
) -> Result<(FieldRef, ArrayRef)> {
    let json_vals = vals
        .iter()
        .map(|val| match val {
            JsonVariantRef::Null => Ok(None),
            JsonVariantRef::Variant(val) => Ok(Some(from_utf8(val).map_err(|e| {
                InvalidVectorSnafu {
                    msg: format!("invalid UTF-8 in JSON variant payload: {e}"),
                }
                .build()
            })?)),
            _ => InvalidVectorSnafu {
                msg: format!("expected JSON variant payload, got {val:?}"),
            }
            .fail(),
        })
        .collect::<Result<Vec<_>>>()?;
    let input: ArrayRef = Arc::new(StringArray::from(json_vals));
    let array = json_to_variant(&input).map_err(|e| {
        InvalidVectorSnafu {
            msg: format!("failed to encode JSON payload as parquet variant: {e}"),
        }
        .build()
    })?;
    let field = Arc::new(array.field(name));
    Ok((field, ArrayRef::from(array)))
}

fn build_json_object_array(
    name: &str,
    vals: &[JsonVariantRef<'_>],
    object_type: &JsonObjectType,
) -> Result<(FieldRef, ArrayRef)> {
    let mut fields = Vec::with_capacity(object_type.len());
    let mut arrays = Vec::with_capacity(object_type.len());
    let mut field_vals = Vec::with_capacity(vals.len());
    for (field_name, field_type) in object_type {
        field_vals.clear();
        for value in vals {
            let val = match value {
                JsonVariantRef::Object(object) => object
                    .get(field_name.as_str())
                    .cloned()
                    .unwrap_or(JsonVariantRef::Null),
                _ => JsonVariantRef::Null,
            };
            field_vals.push(val);
        }
        let (field, array) = build_json_array(field_name, &field_vals, field_type)?;
        fields.push(field);
        arrays.push(array);
    }

    let fields = Fields::from(fields);
    let nulls = null_buffer(
        vals.iter()
            .map(|value| matches!(value, JsonVariantRef::Object(_))),
    );
    let array = StructArray::new(fields.clone(), arrays, nulls);
    let field = Arc::new(Field::new(name, ArrowDataType::Struct(fields), true));
    Ok((field, Arc::new(array)))
}

fn build_plain_json_variant_array(
    name: &str,
    vals: &[JsonVariantRef<'_>],
    native_type: &JsonNativeType,
) -> Result<(FieldRef, ArrayRef)> {
    let data_type = native_type.as_arrow_type();
    let field = Arc::new(Field::new(name, data_type.clone(), true));
    let array = match native_type {
        JsonNativeType::Null => crate::arrow::array::new_null_array(&data_type, vals.len()),
        JsonNativeType::Bool => Arc::new(BooleanArray::from_iter(vals.iter().map(
            |value| match value {
                JsonVariantRef::Null => None,
                JsonVariantRef::Bool(value) => Some(*value),
                _ => None,
            },
        ))) as ArrayRef,
        JsonNativeType::Number(JsonNumberType::U64) => Arc::new(UInt64Array::from_iter(
            vals.iter().map(|value| match value {
                JsonVariantRef::Null => None,
                JsonVariantRef::Number(value) => json_number_as_u64(value),
                _ => None,
            }),
        )) as ArrayRef,
        JsonNativeType::Number(JsonNumberType::I64) => Arc::new(Int64Array::from_iter(
            vals.iter().map(|value| match value {
                JsonVariantRef::Null => None,
                JsonVariantRef::Number(value) => json_number_as_i64(value),
                _ => None,
            }),
        )) as ArrayRef,
        JsonNativeType::Number(JsonNumberType::F64) => Arc::new(Float64Array::from_iter(
            vals.iter().map(|value| match value {
                JsonVariantRef::Null => None,
                JsonVariantRef::Number(value) => Some(json_number_as_f64(value)),
                _ => None,
            }),
        )) as ArrayRef,
        JsonNativeType::String => {
            let row_vals = vals
                .iter()
                .map(|value| match value {
                    JsonVariantRef::Null => Value::Null,
                    JsonVariantRef::String(value) => Value::String((*value).into()),
                    _ => Value::Null,
                })
                .collect::<Vec<_>>();
            Helper::try_from_row_into_vector(
                &row_vals,
                &ConcreteDataType::from_arrow_type(&ArrowDataType::Utf8View),
            )?
            .to_arrow_array()
        }
        JsonNativeType::Array(_) | JsonNativeType::Object(_) | JsonNativeType::Variant => {
            unreachable!("complex JSON native types are built by recursive builders")
        }
    };
    Ok((field, array))
}

fn build_json_list_array(
    name: &str,
    vals: &[JsonVariantRef<'_>],
    item_type: &JsonNativeType,
) -> Result<(FieldRef, ArrayRef)> {
    let mut offsets = Vec::with_capacity(vals.len() + 1);
    let mut valid = Vec::with_capacity(vals.len());
    let mut flattened = Vec::new();
    offsets.push(0_i32);
    for value in vals {
        match value {
            JsonVariantRef::Array(items) => {
                flattened.extend(items.iter().cloned());
                valid.push(true);
            }
            JsonVariantRef::Null => valid.push(false),
            _ => {
                return InvalidVectorSnafu {
                    msg: format!("expected JSON array payload, got {value:?}"),
                }
                .fail();
            }
        }
        let offset: i32 = flattened.len().try_into().map_err(|_| {
            InvalidVectorSnafu {
                msg: "flattened list length exceeds i32::MAX",
            }
            .build()
        })?;
        offsets.push(offset);
    }

    let (item_field, item_array) = build_json_array("item", &flattened, item_type)?;
    let nulls = null_buffer(valid);
    let array = ListArray::new(
        item_field.clone(),
        OffsetBuffer::new(ScalarBuffer::from(offsets)),
        item_array,
        nulls,
    );
    let field = Arc::new(Field::new(
        name,
        ArrowDataType::List(item_field.clone()),
        true,
    ));
    Ok((field, Arc::new(array)))
}

fn null_buffer(valid: impl IntoIterator<Item = bool>) -> Option<NullBuffer> {
    let valid = valid.into_iter().collect::<Vec<_>>();
    valid
        .iter()
        .any(|valid| !*valid)
        .then(|| NullBuffer::from(valid))
}

fn json_number_as_u64(value: &JsonNumber) -> Option<u64> {
    match value {
        JsonNumber::PosInt(value) => Some(*value),
        JsonNumber::NegInt(value) => (*value >= 0).then_some(*value as u64),
        JsonNumber::Float(_) => None,
    }
}

fn json_number_as_i64(value: &JsonNumber) -> Option<i64> {
    match value {
        JsonNumber::PosInt(value) => (*value <= i64::MAX as u64).then_some(*value as i64),
        JsonNumber::NegInt(value) => Some(*value),
        JsonNumber::Float(_) => None,
    }
}

fn json_number_as_f64(value: &JsonNumber) -> f64 {
    match value {
        JsonNumber::PosInt(value) => *value as f64,
        JsonNumber::NegInt(value) => *value as f64,
        JsonNumber::Float(value) => value.0,
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
    use parquet::arrow::ArrowWriter;
    use parquet::basic::LogicalType;
    use parquet::schema::types::Type;
    use parquet::variant::{VariantArray, VariantType};

    use super::*;
    use crate::arrow::array::Array;
    use crate::arrow::record_batch::RecordBatch;
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

        let vector = builder.to_vector();
        assert_eq!(vector.len(), 3);
        let vector = vector.as_any().downcast_ref::<StructVector>().unwrap();
        let array = vector.array();
        assert!(array.is_valid(0));
        assert!(array.is_null(1));
        assert!(array.is_valid(2));

        let payload_field = array
            .fields()
            .iter()
            .find(|field| field.name() == "payload")
            .unwrap();
        assert!(payload_field.has_valid_extension_type::<VariantType>());
        assert_eq!(
            payload_field.extension_type_name(),
            Some("arrow.parquet.variant")
        );
        let payload = array.column_by_name("payload").unwrap();
        let payload = VariantArray::try_new(payload.as_ref()).unwrap();
        assert!(payload.is_valid(0));
        assert!(payload.is_null(1));
        assert!(payload.is_valid(2));

        let root_array: ArrayRef = Arc::new(array.clone());
        let schema = Arc::new(arrow_schema::Schema::new(vec![Field::new(
            "j",
            root_array.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![root_array]).unwrap();
        let mut parquet_bytes = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut parquet_bytes, schema, None).unwrap();
        writer.write(&batch).unwrap();
        let metadata = writer.close().unwrap();
        let parquet_schema = metadata.file_metadata().schema_descr().root_schema();
        assert!(has_variant_logical_type(parquet_schema, "payload"));

        let mut nested_builder =
            JsonVectorBuilder::new(JsonNativeType::Object(Default::default()), 2);
        let first = parse_json_value(r#"{"items":[{"name":"foo"}],"name":"a"}"#);
        let second = parse_json_value(r#"{"items":["raw"],"name":"b"}"#);
        nested_builder.try_push_value_ref(&first.as_value_ref())?;
        nested_builder.try_push_value_ref(&second.as_value_ref())?;

        let nested_vector = nested_builder.to_vector();
        let nested_vector = nested_vector
            .as_any()
            .downcast_ref::<StructVector>()
            .unwrap();
        let nested_array = nested_vector.array();
        assert_eq!(
            nested_array.column_by_name("name").unwrap().data_type(),
            &ArrowDataType::Utf8View
        );
        let items = nested_array.column_by_name("items").unwrap();
        let items = items.as_any().downcast_ref::<ListArray>().unwrap();
        let ArrowDataType::List(item_field) = items.data_type() else {
            unreachable!();
        };
        assert!(item_field.has_valid_extension_type::<VariantType>());

        let mut plain_nested_builder =
            JsonVectorBuilder::new(JsonNativeType::Object(Default::default()), 2);
        let first = parse_json_value(r#"{"items":[1,2],"payload":{"name":"foo"}}"#);
        let second = parse_json_value(r#"{"items":[3],"payload":{"name":"bar"}}"#);
        plain_nested_builder.try_push_value_ref(&first.as_value_ref())?;
        plain_nested_builder.try_push_value_ref(&second.as_value_ref())?;

        let plain_nested_vector = plain_nested_builder.to_vector();
        let plain_nested_vector = plain_nested_vector
            .as_any()
            .downcast_ref::<StructVector>()
            .unwrap();
        let plain_nested_array = plain_nested_vector.array();
        assert!(matches!(
            plain_nested_array
                .column_by_name("items")
                .unwrap()
                .data_type(),
            ArrowDataType::List(_)
        ));
        assert!(matches!(
            plain_nested_array
                .column_by_name("payload")
                .unwrap()
                .data_type(),
            ArrowDataType::Struct(_)
        ));

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

    fn has_variant_logical_type(parquet_type: &Type, field_name: &str) -> bool {
        parquet_type.name() == field_name
            && matches!(
                parquet_type.get_basic_info().logical_type_ref(),
                Some(LogicalType::Variant { .. })
            )
            || parquet_type.is_group()
                && parquet_type
                    .get_fields()
                    .iter()
                    .any(|field| has_variant_logical_type(field, field_name))
    }
}
