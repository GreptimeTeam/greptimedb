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

use std::cmp::Ordering;
use std::sync::Arc;

use arrow::compute::{can_cast_types, cast};
use arrow_array::cast::AsArray;
use arrow_array::types::{
    Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type,
    UInt32Type, UInt64Type,
};
use arrow_array::{Array, ArrayRef, GenericListArray, ListArray, StructArray, new_null_array};
use arrow_schema::{DataType, Field, FieldRef};
use common_telemetry::trace;
use serde_json::Value;
use snafu::{OptionExt, ResultExt};

use crate::arrow_array::{MutableBinaryArray, binary_array_value, string_array_value};
use crate::data_type::ConcreteDataType;
use crate::error::{
    AlignJsonArraySnafu, ArrowComputeSnafu, InvalidJsonSnafu, InvalidJsonbSnafu, Result,
};
use crate::extension::json::{JSON2_REMAINDER_FIELD_NAME, json2_remainder_field};
use crate::json::JsonSettings;
use crate::json::value::{decode_json_variant, encode_serde_json_as_jsonb};
use crate::prelude::{DataType as _, Value as GreptimeValue};
use crate::value::{ListValue, StructValue};
use crate::vectors::MutableVector;
use crate::vectors::json::builder::JsonVectorBuilder;
use crate::vectors::json::variant::variant_to_json_values;

pub struct JsonArray<'a> {
    inner: &'a ArrayRef,
}

impl JsonArray<'_> {
    /// Try to get the value (as a [Value]) at the index `i`.
    pub fn try_get_value(&self, i: usize) -> Result<Value> {
        let array = self.inner;
        if array.is_null(i) {
            return Ok(Value::Null);
        }

        let value = match array.data_type() {
            DataType::Null => Value::Null,
            DataType::Boolean => Value::Bool(array.as_boolean().value(i)),
            DataType::Int8 => Value::from(array.as_primitive::<Int8Type>().value(i)),
            DataType::Int16 => Value::from(array.as_primitive::<Int16Type>().value(i)),
            DataType::Int32 => Value::from(array.as_primitive::<Int32Type>().value(i)),
            DataType::Int64 => Value::from(array.as_primitive::<Int64Type>().value(i)),
            DataType::UInt8 => Value::from(array.as_primitive::<UInt8Type>().value(i)),
            DataType::UInt16 => Value::from(array.as_primitive::<UInt16Type>().value(i)),
            DataType::UInt32 => Value::from(array.as_primitive::<UInt32Type>().value(i)),
            DataType::UInt64 => Value::from(array.as_primitive::<UInt64Type>().value(i)),
            DataType::Float32 => Value::from(array.as_primitive::<Float32Type>().value(i)),
            DataType::Float64 => Value::from(array.as_primitive::<Float64Type>().value(i)),
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                Value::String(string_array_value(array, i).to_string())
            }
            DataType::Binary | DataType::LargeBinary | DataType::BinaryView => {
                let bytes = binary_array_value(array, i);
                decode_json_variant(bytes).map_err(|error| InvalidJsonbSnafu { error }.build())?
            }
            DataType::Struct(_) => {
                let structs = array.as_struct();
                let object = structs
                    .fields()
                    .iter()
                    .zip(structs.columns())
                    .map(|(field, column)| {
                        JsonArray::from(column)
                            .try_get_value(i)
                            .map(|v| (field.name().clone(), v))
                    })
                    .collect::<Result<_>>()?;
                Value::Object(object)
            }
            DataType::List(_) => {
                let lists = array.as_list::<i32>();
                let list = lists.value(i);
                let list = JsonArray::from(&list);
                let mut values = Vec::with_capacity(list.inner.len());
                for i in 0..list.inner.len() {
                    values.push(list.try_get_value(i)?);
                }
                Value::Array(values)
            }
            t => {
                return InvalidJsonSnafu {
                    value: format!("unknown JSON type {t}"),
                }
                .fail();
            }
        };
        Ok(value)
    }

    /// Projects a physical JSON2 array to a logical query type.
    ///
    /// TODO(LFC) Supersede `project_to_v2` to `project_to`.
    pub fn project_to_v2(&self, field: &Field, target: &DataType) -> Result<ArrayRef> {
        if json2_remainder_field(field)?.is_some() {
            project_json_values(self.json2_values()?, target)
        } else {
            self.project_to(target)
        }
    }

    /// Rewrites a physical JSON2 array directly into a fixed v2 layout.
    pub fn rewrite_to_v2(
        &self,
        field: &Field,
        logical_settings: &JsonSettings,
        target_layout: &JsonSettings,
    ) -> Result<ArrayRef> {
        let values = if json2_remainder_field(field)?.is_some() {
            self.json2_values()?
        } else {
            (0..self.inner.len())
                .map(|i| self.try_get_value(i))
                .collect::<Result<Vec<_>>>()?
        };
        let mut builder = JsonVectorBuilder::with_settings(target_layout, values.len());
        for value in values {
            if value.is_null() {
                builder.push_null();
            } else {
                let value = logical_settings.encode(value)?;
                builder.try_push_value_ref(&value.as_value_ref())?;
            }
        }
        Ok(builder.to_vector().to_arrow_array())
    }

    fn json2_values(&self) -> Result<Vec<Value>> {
        let structs = self.inner.as_struct_opt().context(AlignJsonArraySnafu {
            reason: "JSON2 layout v2 root array must be a struct",
        })?;
        let remainder = structs.column_by_name(JSON2_REMAINDER_FIELD_NAME);
        let mut remainders = if let Some(remainder) = remainder {
            variant_to_json_values(remainder)?
        } else {
            vec![None; structs.len()]
        };
        let mut values = Vec::with_capacity(structs.len());
        let mut path = Vec::new();

        for (i, remainder) in remainders.iter_mut().enumerate() {
            if structs.is_null(i) {
                values.push(Value::Null);
                continue;
            }

            let mut object = match remainder.take() {
                None => serde_json::Map::new(),
                Some(Value::Object(object)) => object,
                Some(value) => {
                    return InvalidJsonSnafu {
                        value: format!("JSON2 layout v2 remainder must be an object, got {value}"),
                    }
                    .fail();
                }
            };

            for (child, column) in structs.fields().iter().zip(structs.columns()) {
                if child.name() == JSON2_REMAINDER_FIELD_NAME {
                    continue;
                }
                let mut value = JsonArray::from(column).try_get_value(i)?;
                remove_null_object_fields(&mut value);
                if value.is_null() {
                    continue;
                }
                merge_explicit_value(&mut object, child.name().clone(), value, &mut path)?;
            }
            values.push(Value::Object(object));
        }

        Ok(values)
    }

    /// Normalizes a JSON2 array to the wider `expect` data type without losing
    /// information.
    ///
    /// This is mainly used for write/flush-time JSON2 schema alignment:
    /// - fields missing from the source are filled with typed null arrays;
    /// - fields present in the source must also exist in `expect`;
    /// - fields present in both are widened recursively when their types differ.
    ///
    /// Narrowing conversions and any other conversions that may lose information
    /// are rejected.
    pub fn widen_to(&self, expect: &DataType) -> Result<ArrayRef> {
        let data_type = self.inner.data_type();

        if data_type == expect {
            return Ok(self.inner.clone());
        }

        trace!(
            "Try aligning JSON array {} to data type {}",
            data_type, expect
        );

        let struct_array = self.inner.as_struct_opt().context(AlignJsonArraySnafu {
            reason: "expect struct array",
        })?;
        let array_fields = struct_array.fields();
        let array_columns = struct_array.columns();
        let DataType::Struct(expect_fields) = expect else {
            return AlignJsonArraySnafu {
                reason: "expect struct datatype",
            }
            .fail();
        };
        let mut aligned = Vec::with_capacity(expect_fields.len());

        // Compare the fields in the JSON array and the to-be-aligned schema, amending with null
        // arrays on the way. It's very important to note that fields in the JSON array and those
        // in the JSON type are both **SORTED**, which can be guaranteed because the fields in the
        // JSON type implementation are sorted.
        debug_assert!(expect_fields.iter().map(|f| f.name()).is_sorted());
        debug_assert!(array_fields.iter().map(|f| f.name()).is_sorted());

        let mut i = 0; // point to the expect fields
        let mut j = 0; // point to the array fields
        while i < expect_fields.len() && j < array_fields.len() {
            let expect_field = &expect_fields[i];
            let array_field = &array_fields[j];
            match expect_field.name().cmp(array_field.name()) {
                Ordering::Equal => {
                    if expect_field.data_type() == array_field.data_type() {
                        aligned.push(array_columns[j].clone());
                    } else {
                        let expect_type = expect_field.data_type();
                        let array_type = array_field.data_type();
                        let array = match (expect_type, array_type) {
                            (DataType::Struct(_), DataType::Struct(_)) => {
                                JsonArray::from(&array_columns[j]).widen_to(expect_type)?
                            }
                            (DataType::List(expect_item), DataType::List(array_item)) => {
                                let list_array = array_columns[j].as_list::<i32>();
                                widen_list(list_array, array_item, expect_item)?
                            }
                            _ => JsonArray::from(&array_columns[j]).widen_scalar_to(expect_type)?,
                        };
                        aligned.push(array);
                    }
                    i += 1;
                    j += 1;
                }
                Ordering::Less => {
                    aligned.push(new_null_array(expect_field.data_type(), struct_array.len()));
                    i += 1;
                }
                Ordering::Greater => {
                    return AlignJsonArraySnafu {
                        reason: format!(
                            "source field {} does not exist in target schema",
                            array_field.name()
                        ),
                    }
                    .fail();
                }
            }
        }
        if j < array_fields.len() {
            return AlignJsonArraySnafu {
                reason: format!(
                    "source field {} does not exist in target schema",
                    array_fields[j].name()
                ),
            }
            .fail();
        }
        if i < expect_fields.len() {
            for field in &expect_fields[i..] {
                aligned.push(new_null_array(field.data_type(), struct_array.len()));
            }
        }

        let json_array = StructArray::try_new_with_length(
            expect_fields.clone(),
            aligned,
            struct_array.nulls().cloned(),
            struct_array.len(),
        )
        .map_err(|e| {
            AlignJsonArraySnafu {
                reason: e.to_string(),
            }
            .build()
        })?;
        Ok(Arc::new(json_array))
    }

    /// Widens an array to the merged JSON2 physical type without losing information.
    ///
    /// Supported conversions:
    /// - identical types are returned unchanged;
    /// - null arrays become typed null arrays;
    /// - concrete JSON values are encoded as JSONB when the target type is binary.
    ///
    /// All other conversions are rejected.
    fn widen_scalar_to(&self, to_type: &DataType) -> Result<ArrayRef> {
        let from_type = self.inner.data_type();
        if from_type == to_type {
            return Ok(self.inner.clone());
        }

        if from_type == &DataType::Null {
            return Ok(new_null_array(to_type, self.inner.len()));
        }

        if !from_type.is_binary() && to_type.is_binary() {
            return self.encode_variant();
        }

        AlignJsonArraySnafu {
            reason: format!("unable to widen {from_type} to {to_type}"),
        }
        .fail()
    }

    fn encode_variant(&self) -> Result<ArrayRef> {
        let len = self.inner.len();
        let mut encoded = Vec::with_capacity(len);
        let mut total_bytes = 0;

        for i in 0..len {
            let value = self.try_get_value(i)?;
            if value.is_null() {
                encoded.push(None);
            } else {
                let bytes = encode_serde_json_as_jsonb(value);
                total_bytes += bytes.len();
                encoded.push(Some(bytes));
            }
        }

        let mut builder = MutableBinaryArray::with_capacity(len, total_bytes);
        for value in encoded {
            builder.append_option(value);
        }
        Ok(Arc::new(builder.finish()))
    }

    /// Projects this JSON array to `target` for query evaluation.
    ///
    /// Unlike [`Self::widen_to`], projection tolerates lossy conversions:
    /// - source fields not present in `target` are discarded;
    /// - fields missing from the source are filled with typed null arrays;
    /// - values incompatible with the target type become NULL.
    ///
    /// Projection is applied recursively to structs and lists. Input nulls
    /// remain NULL. Errors unrelated to type incompatibility, such as invalid
    /// JSONB, are returned.
    pub fn project_to(&self, target: &DataType) -> Result<ArrayRef> {
        if self.inner.data_type() == target {
            return Ok(self.inner.clone());
        }

        match (self.inner.data_type(), target) {
            (DataType::Struct(_), DataType::Struct(target_fields)) => {
                let struct_array = self.inner.as_struct();
                let mut columns = Vec::with_capacity(target_fields.len());
                for target_field in target_fields {
                    let column = struct_array
                        .column_by_name(target_field.name())
                        .map(|column| JsonArray::from(column).project_to(target_field.data_type()))
                        .transpose()?
                        .unwrap_or_else(|| {
                            new_null_array(target_field.data_type(), self.inner.len())
                        });
                    columns.push(column);
                }
                let projected = StructArray::try_new_with_length(
                    target_fields.clone(),
                    columns,
                    struct_array.nulls().cloned(),
                    struct_array.len(),
                )
                .context(ArrowComputeSnafu)?;
                Ok(Arc::new(projected))
            }
            (DataType::List(_), DataType::List(target_item)) => {
                let list_array = self.inner.as_list::<i32>();
                let item_projected =
                    JsonArray::from(list_array.values()).project_to(target_item.data_type())?;
                Ok(Arc::new(
                    GenericListArray::<i32>::try_new(
                        target_item.clone(),
                        list_array.offsets().clone(),
                        item_projected,
                        list_array.nulls().cloned(),
                    )
                    .context(ArrowComputeSnafu)?,
                ))
            }
            _ => self.project_values_to(target),
        }
    }

    fn project_values_to(&self, to_type: &DataType) -> Result<ArrayRef> {
        let from_type = self.inner.data_type();
        if can_fast_cast_types(from_type, to_type) {
            return cast(self.inner.as_ref(), to_type).context(ArrowComputeSnafu);
        }

        let values = (0..self.inner.len())
            .map(|i| self.try_get_value(i))
            .collect::<Result<Vec<_>>>()?;
        project_json_values(values, to_type)
    }
}

fn merge_explicit_value(
    remainder: &mut serde_json::Map<String, Value>,
    key: String,
    explicit: Value,
    path: &mut Vec<String>,
) -> Result<()> {
    let Some(existing) = remainder.get_mut(&key) else {
        remainder.insert(key, explicit);
        return Ok(());
    };
    path.push(key);

    let (Value::Object(remainder), Value::Object(explicit)) = (existing, explicit) else {
        return InvalidJsonSnafu {
            value: format!(
                "cannot merge '{}' in explicit fields and remainder: not both objects",
                path.join("."),
            ),
        }
        .fail();
    };
    for (key, value) in explicit {
        merge_explicit_value(remainder, key, value, path)?;
    }
    path.pop();
    Ok(())
}

fn remove_null_object_fields(value: &mut Value) {
    let Value::Object(object) = value else {
        return;
    };
    object.retain(|_, value| {
        remove_null_object_fields(value);
        !value.is_null()
    });
}

/// Returns whether Arrow can cast between the types without JSON-aware projection.
/// Binary and nested types require JSONB decoding or recursive projection.
fn can_fast_cast_types(from_type: &DataType, to_type: &DataType) -> bool {
    let is_scalar = |data_type: &DataType| {
        data_type.is_numeric() || data_type.is_string() || data_type == &DataType::Boolean
    };

    is_scalar(from_type) && is_scalar(to_type) && can_cast_types(from_type, to_type)
}

fn project_json_values(values: Vec<Value>, to_type: &DataType) -> Result<ArrayRef> {
    let concrete_type = ConcreteDataType::from_arrow_type(to_type);
    let mut builder = concrete_type.create_mutable_vector(values.len());
    for value in values {
        let value = project_json_value_to_type(value, &concrete_type)?;
        builder.try_push_value_ref(&value.as_value_ref())?;
    }
    Ok(builder.to_vector().to_arrow_array())
}

fn project_json_value_to_type(value: Value, to_type: &ConcreteDataType) -> Result<GreptimeValue> {
    if value.is_null() {
        return Ok(GreptimeValue::Null);
    }

    if to_type.is_string() {
        let value = match value {
            Value::String(value) => value,
            value => value.to_string(),
        };
        return Ok(GreptimeValue::String(value.into()));
    }

    if matches!(to_type, ConcreteDataType::Binary(_)) {
        return Ok(GreptimeValue::Binary(
            encode_serde_json_as_jsonb(value).into(),
        ));
    }

    if let Some(struct_type) = to_type.as_struct() {
        let Value::Object(mut object) = value else {
            return Ok(GreptimeValue::Null);
        };
        let values = struct_type
            .fields()
            .iter()
            .map(|field| {
                object
                    .remove(field.name())
                    .map(|value| project_json_value_to_type(value, field.data_type()))
                    .transpose()
                    .map(|value| value.unwrap_or(GreptimeValue::Null))
            })
            .collect::<Result<Vec<_>>>()?;
        return Ok(GreptimeValue::Struct(StructValue::new(
            values,
            struct_type.clone(),
        )));
    }

    if let Some(list_type) = to_type.as_list() {
        let Value::Array(values) = value else {
            return Ok(GreptimeValue::Null);
        };
        let item_type = list_type.item_type().clone();
        let values = values
            .into_iter()
            .map(|value| project_json_value_to_type(value, &item_type))
            .collect::<Result<Vec<_>>>()?;
        return Ok(GreptimeValue::List(ListValue::new(
            values,
            Arc::new(item_type),
        )));
    }

    let value = match value {
        Value::Bool(value) => GreptimeValue::Boolean(value),
        Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                GreptimeValue::Int64(value)
            } else if let Some(value) = value.as_u64() {
                GreptimeValue::UInt64(value)
            } else if let Some(value) = value.as_f64() {
                GreptimeValue::Float64(value.into())
            } else {
                GreptimeValue::Null
            }
        }
        Value::String(value) => GreptimeValue::String(value.into()),
        Value::Array(_) | Value::Object(_) => GreptimeValue::Null,
        Value::Null => GreptimeValue::Null,
    };
    Ok(to_type.try_cast(value).unwrap_or(GreptimeValue::Null))
}

fn widen_list(list_array: &ListArray, actual: &FieldRef, expected: &FieldRef) -> Result<ArrayRef> {
    let item_aligned = match (actual.data_type(), expected.data_type()) {
        (DataType::Struct(_), DataType::Struct(_)) => {
            JsonArray::from(list_array.values()).widen_to(expected.data_type())?
        }
        (DataType::List(actual), DataType::List(expected)) => {
            let list_array = list_array.values().as_list::<i32>();
            widen_list(list_array, actual, expected)?
        }
        _ => JsonArray::from(list_array.values()).widen_scalar_to(expected.data_type())?,
    };
    Ok(Arc::new(
        GenericListArray::<i32>::try_new(
            expected.clone(),
            list_array.offsets().clone(),
            item_aligned,
            list_array.nulls().cloned(),
        )
        .context(ArrowComputeSnafu)?,
    ))
}

impl<'a> From<&'a ArrayRef> for JsonArray<'a> {
    fn from(inner: &'a ArrayRef) -> Self {
        Self { inner }
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow_array::types::Int64Type;
    use arrow_array::{
        BinaryArray, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
        Int64Array, ListArray, StringArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
    };
    use arrow_schema::{Field, Fields};
    use serde_json::json;

    use super::*;
    use crate::extension::json::{Json2ExtensionType, JsonMetadata};
    use crate::json::JsonSettings;
    use crate::vectors::json::variant::{json_values_to_variant, variant_field};

    #[test]
    fn test_try_get_value() -> Result<()> {
        let nulls = new_null_array(&DataType::Null, 2);
        assert_eq!(JsonArray::from(&nulls).try_get_value(0)?, Value::Null);

        let bools: ArrayRef = Arc::new(BooleanArray::from(vec![Some(true), None]));
        assert_eq!(JsonArray::from(&bools).try_get_value(0)?, json!(true));
        assert_eq!(JsonArray::from(&bools).try_get_value(1)?, Value::Null);

        let ints: ArrayRef = Arc::new(Int64Array::from(vec![Some(-7), None]));
        assert_eq!(JsonArray::from(&ints).try_get_value(0)?, json!(-7));
        assert_eq!(JsonArray::from(&ints).try_get_value(1)?, Value::Null);

        macro_rules! assert_number {
            ($array:expr, $expected:expr) => {{
                let array: ArrayRef = Arc::new($array);
                assert_eq!(JsonArray::from(&array).try_get_value(0)?, json!($expected));
            }};
        }
        assert_number!(Int8Array::from(vec![-8]), -8);
        assert_number!(Int16Array::from(vec![-16]), -16);
        assert_number!(Int32Array::from(vec![-32]), -32);
        assert_number!(UInt8Array::from(vec![8]), 8);
        assert_number!(UInt16Array::from(vec![16]), 16);
        assert_number!(UInt32Array::from(vec![32]), 32);
        assert_number!(Float32Array::from(vec![1.25]), 1.25);

        let floats: ArrayRef = Arc::new(Float64Array::from(vec![Some(1.5)]));
        assert_eq!(JsonArray::from(&floats).try_get_value(0)?, json!(1.5));

        let strings: ArrayRef = Arc::new(StringArray::from(vec![Some("hello"), None]));
        assert_eq!(JsonArray::from(&strings).try_get_value(0)?, json!("hello"));
        assert_eq!(JsonArray::from(&strings).try_get_value(1)?, Value::Null);

        let nested = jsonb::parse_value(br#"{"nested":[1,null,"x"]}"#)
            .unwrap()
            .to_vec();
        let null = jsonb::parse_value(b"null").unwrap().to_vec();
        let binaries: ArrayRef =
            Arc::new(BinaryArray::from(vec![nested.as_slice(), null.as_slice()]));
        assert_eq!(
            JsonArray::from(&binaries).try_get_value(0)?,
            json!({"nested": [1, null, "x"]})
        );
        assert_eq!(JsonArray::from(&binaries).try_get_value(1)?, Value::Null);

        let lists: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
            Some(vec![Some(1), None, Some(3)]),
            None,
        ]));
        assert_eq!(
            JsonArray::from(&lists).try_get_value(0)?,
            json!([1, null, 3])
        );
        assert_eq!(JsonArray::from(&lists).try_get_value(1)?, Value::Null);

        let structs: ArrayRef = Arc::new(StructArray::from(vec![
            (
                Arc::new(Field::new("flag", DataType::Boolean, true)),
                Arc::new(BooleanArray::from(vec![Some(true), None])) as ArrayRef,
            ),
            (
                Arc::new(Field::new_list(
                    "items",
                    Field::new_list_field(DataType::Int64, true),
                    true,
                )),
                Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
                    Some(vec![Some(1), None]),
                    Some(vec![Some(2)]),
                ])) as ArrayRef,
            ),
        ]));
        assert_eq!(
            JsonArray::from(&structs).try_get_value(0)?,
            json!({"flag": true, "items": [1, null]})
        );
        assert_eq!(
            JsonArray::from(&structs).try_get_value(1)?,
            json!({"flag": null, "items": [2]})
        );

        Ok(())
    }

    #[test]
    fn test_cast_variant_to_utf8_view_preserves_json_null() -> Result<()> {
        let encode = |json: &[u8]| jsonb::parse_value(json).unwrap().to_vec();
        let json_null = encode(b"null");
        let object = encode(br#"{"value":1}"#);
        let string = encode(br#""text""#);
        let variants: ArrayRef = Arc::new(BinaryArray::from(vec![
            Some(json_null.as_slice()),
            Some(object.as_slice()),
            Some(string.as_slice()),
            None,
        ]));

        let casted = JsonArray::from(&variants).project_to(&DataType::Utf8View)?;
        let casted = casted.as_string_view();
        assert!(casted.is_null(0));
        assert_eq!(casted.value(1), r#"{"value":1}"#);
        assert_eq!(casted.value(2), "text");
        assert!(casted.is_null(3));

        Ok(())
    }

    #[test]
    fn test_project_plain_scalars() -> Result<()> {
        let integers: ArrayRef = Arc::new(Int64Array::from(vec![Some(42), Some(i64::MAX), None]));
        let projected = JsonArray::from(&integers).project_to(&DataType::Int32)?;
        let expected: ArrayRef = Arc::new(Int32Array::from(vec![Some(42), None, None]));
        assert_eq!(&expected, &projected);

        let booleans: ArrayRef = Arc::new(BooleanArray::from(vec![Some(true), Some(false), None]));
        let projected = JsonArray::from(&booleans).project_to(&DataType::Float64)?;
        let expected: ArrayRef = Arc::new(Float64Array::from(vec![Some(1.0), Some(0.0), None]));
        assert_eq!(&expected, &projected);

        let strings: ArrayRef = Arc::new(StringArray::from(vec![Some("42"), Some("bad"), None]));
        let projected = JsonArray::from(&strings).project_to(&DataType::UInt64)?;
        let expected: ArrayRef = Arc::new(UInt64Array::from(vec![Some(42), None, None]));
        assert_eq!(&expected, &projected);

        Ok(())
    }

    #[test]
    fn test_widen_null_to_any_type() -> Result<()> {
        let nulls = new_null_array(&DataType::Null, 2);
        let target_types = [
            DataType::Boolean,
            DataType::UInt64,
            DataType::Utf8View,
            DataType::Binary,
            DataType::List(Arc::new(Field::new_list_field(DataType::Int64, true))),
            DataType::Struct(Fields::from(vec![Field::new(
                "value",
                DataType::Int64,
                true,
            )])),
        ];

        for target_type in target_types {
            let widened = JsonArray::from(&nulls).widen_scalar_to(&target_type)?;
            assert_eq!(&target_type, widened.data_type());
            assert_eq!(2, widened.len());
            assert_eq!(2, widened.null_count());
        }

        Ok(())
    }

    #[test]
    fn test_widen_non_null_to_utf8_view_fails() {
        let bools: ArrayRef = Arc::new(BooleanArray::from(vec![true]));
        let err = JsonArray::from(&bools)
            .widen_scalar_to(&DataType::Utf8View)
            .unwrap_err();

        assert_eq!(
            "Failed to align JSON array, reason: unable to widen Boolean to Utf8View",
            err.to_string()
        );
    }

    #[test]
    fn test_widen_variant_to_non_binary_fails() {
        let value = jsonb::parse_value(b"true").unwrap().to_vec();
        let variants: ArrayRef = Arc::new(BinaryArray::from(vec![value.as_slice()]));
        let err = JsonArray::from(&variants)
            .widen_scalar_to(&DataType::Boolean)
            .unwrap_err();

        assert_eq!(
            "Failed to align JSON array, reason: unable to widen Binary to Boolean",
            err.to_string()
        );
    }

    #[test]
    fn test_widen_between_number_types_fails() {
        let values: ArrayRef = Arc::new(UInt64Array::from(vec![1]));
        let err = JsonArray::from(&values)
            .widen_scalar_to(&DataType::Int64)
            .unwrap_err();

        assert_eq!(
            "Failed to align JSON array, reason: unable to widen UInt64 to Int64",
            err.to_string()
        );
    }

    #[test]
    fn test_widen_numbers_to_variant_preserves_values() -> Result<()> {
        let cases: [(ArrayRef, Value); 3] = [
            (Arc::new(UInt64Array::from(vec![u64::MAX])), json!(u64::MAX)),
            (Arc::new(Int64Array::from(vec![i64::MIN])), json!(i64::MIN)),
            (Arc::new(Float64Array::from(vec![1.25])), json!(1.25)),
        ];

        for (values, expected) in cases {
            let widened = JsonArray::from(&values).widen_scalar_to(&DataType::Binary)?;
            assert_eq!(&DataType::Binary, widened.data_type());
            assert_eq!(expected, JsonArray::from(&widened).try_get_value(0)?);
        }

        Ok(())
    }

    #[test]
    fn test_align_json_array() -> Result<()> {
        struct TestCase {
            json_array: ArrayRef,
            schema_type: DataType,
            expected: std::result::Result<ArrayRef, String>,
        }

        impl TestCase {
            fn new(
                json_array: StructArray,
                schema_type: Fields,
                expected: std::result::Result<Vec<ArrayRef>, String>,
            ) -> Self {
                Self {
                    json_array: Arc::new(json_array),
                    schema_type: DataType::Struct(schema_type.clone()),
                    expected: expected
                        .map(|x| Arc::new(StructArray::new(schema_type, x, None)) as ArrayRef),
                }
            }

            fn test(self) -> Result<()> {
                let result = JsonArray::from(&self.json_array).widen_to(&self.schema_type);
                match (result, self.expected) {
                    (Ok(json_array), Ok(expected)) => assert_eq!(&json_array, &expected),
                    (Ok(json_array), Err(e)) => {
                        panic!("expecting error {e} but actually get: {json_array:?}")
                    }
                    (Err(e), Err(expected)) => assert_eq!(e.to_string(), expected),
                    (Err(e), Ok(_)) => return Err(e),
                }
                Ok(())
            }
        }

        // Test empty json array can be aligned with a complex json type.
        TestCase::new(
            StructArray::new_empty_fields(2, None),
            Fields::from(vec![
                Field::new("int", DataType::Int64, true),
                Field::new_struct(
                    "nested",
                    vec![Field::new("bool", DataType::Boolean, true)],
                    true,
                ),
                Field::new("string", DataType::Utf8, true),
            ]),
            Ok(vec![
                Arc::new(Int64Array::new_null(2)) as ArrayRef,
                Arc::new(StructArray::new_null(
                    Fields::from(vec![Arc::new(Field::new("bool", DataType::Boolean, true))]),
                    2,
                )),
                Arc::new(StringArray::new_null(2)),
            ]),
        )
        .test()?;

        // Test simple json array alignment.
        TestCase::new(
            StructArray::from(vec![(
                Arc::new(Field::new("float", DataType::Float64, true)),
                Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])) as ArrayRef,
            )]),
            Fields::from(vec![
                Field::new("float", DataType::Float64, true),
                Field::new("string", DataType::Utf8, true),
            ]),
            Ok(vec![
                Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])) as ArrayRef,
                Arc::new(StringArray::new_null(3)),
            ]),
        )
        .test()?;

        // Test complex json array alignment.
        TestCase::new(
            StructArray::from(vec![
                (
                    Arc::new(Field::new_list(
                        "list",
                        Field::new_list_field(DataType::Int64, true),
                        true,
                    )),
                    Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
                        Some(vec![Some(1)]),
                        None,
                        Some(vec![Some(2), Some(3)]),
                    ])) as ArrayRef,
                ),
                (
                    Arc::new(Field::new_struct(
                        "nested",
                        vec![Field::new("int", DataType::Int64, true)],
                        true,
                    )),
                    Arc::new(StructArray::from(vec![(
                        Arc::new(Field::new("int", DataType::Int64, true)),
                        Arc::new(Int64Array::from(vec![-1, -2, -3])) as ArrayRef,
                    )])),
                ),
                (
                    Arc::new(Field::new("string", DataType::Utf8, true)),
                    Arc::new(StringArray::from(vec!["a", "b", "c"])),
                ),
            ]),
            Fields::from(vec![
                Field::new("bool", DataType::Boolean, true),
                Field::new_list("list", Field::new_list_field(DataType::Int64, true), true),
                Field::new_struct(
                    "nested",
                    vec![
                        Field::new("float", DataType::Float64, true),
                        Field::new("int", DataType::Int64, true),
                    ],
                    true,
                ),
                Field::new("string", DataType::Utf8, true),
            ]),
            Ok(vec![
                Arc::new(BooleanArray::new_null(3)) as ArrayRef,
                Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
                    Some(vec![Some(1)]),
                    None,
                    Some(vec![Some(2), Some(3)]),
                ])),
                Arc::new(StructArray::from(vec![
                    (
                        Arc::new(Field::new("float", DataType::Float64, true)),
                        Arc::new(Float64Array::new_null(3)) as ArrayRef,
                    ),
                    (
                        Arc::new(Field::new("int", DataType::Int64, true)),
                        Arc::new(Int64Array::from(vec![-1, -2, -3])),
                    ),
                ])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ]),
        )
        .test()?;

        // Source fields that do not exist in the target schema must not be discarded.
        TestCase::new(
            StructArray::from(vec![(
                Arc::new(Field::new("a", DataType::Boolean, true)),
                Arc::new(BooleanArray::from(vec![true])) as ArrayRef,
            )]),
            Fields::from(vec![Field::new("b", DataType::Boolean, true)]),
            Err(
                "Failed to align JSON array, reason: source field a does not exist in target schema"
                    .to_string(),
            ),
        )
        .test()?;

        // Trailing source fields must also be rejected after all target fields are processed.
        TestCase::new(
            StructArray::from(vec![(
                Arc::new(Field::new("b", DataType::Boolean, true)),
                Arc::new(BooleanArray::from(vec![true])) as ArrayRef,
            )]),
            Fields::from(vec![Field::new("a", DataType::Boolean, true)]),
            Err(
                "Failed to align JSON array, reason: source field b does not exist in target schema"
                    .to_string(),
            ),
        )
        .test()?;

        Ok(())
    }

    #[test]
    fn test_align_variant_to_struct() -> Result<()> {
        let encode = |json: &[u8]| jsonb::parse_value(json).unwrap().to_vec();
        let object =
            encode(br#"{"nested":{"flag":true,"items":[1,2],"raw":{"x":1},"text":42,"value":42}}"#);
        let scalar = encode(b"1");
        let variants: ArrayRef = Arc::new(BinaryArray::from(vec![
            Some(object.as_slice()),
            None,
            Some(scalar.as_slice()),
        ]));
        let expected_type = DataType::Struct(Fields::from(vec![Field::new_struct(
            "nested",
            vec![
                Field::new("flag", DataType::Boolean, true),
                Field::new_list("items", Field::new_list_field(DataType::UInt64, true), true),
                Field::new("raw", DataType::Binary, true),
                Field::new("text", DataType::Utf8View, true),
                Field::new("value", DataType::UInt64, true),
            ],
            true,
        )]));

        let aligned = JsonArray::from(&variants).project_to(&expected_type)?;
        assert_eq!(&expected_type, aligned.data_type());
        assert_eq!(
            json!({
                "nested": {
                    "flag": true,
                    "items": [1, 2],
                    "raw": {"x": 1},
                    "text": "42",
                    "value": 42
                }
            }),
            JsonArray::from(&aligned).try_get_value(0)?
        );
        assert!(aligned.is_null(1));
        assert!(aligned.is_null(2));

        Ok(())
    }

    #[test]
    fn test_align_nested_variant_to_struct() -> Result<()> {
        let object = jsonb::parse_value(br#"{"flag":true,"value":42}"#)
            .unwrap()
            .to_vec();
        let variants: ArrayRef = Arc::new(BinaryArray::from(vec![Some(object.as_slice()), None]));
        let input: ArrayRef = Arc::new(StructArray::from(vec![(
            Arc::new(Field::new("nested", DataType::Binary, true)),
            variants,
        )]));
        let expected_type = DataType::Struct(Fields::from(vec![Field::new_struct(
            "nested",
            vec![
                Field::new("flag", DataType::Boolean, true),
                Field::new("value", DataType::UInt64, true),
            ],
            true,
        )]));

        let aligned = JsonArray::from(&input).project_to(&expected_type)?;
        assert_eq!(&expected_type, aligned.data_type());
        assert_eq!(
            json!({"nested": {"flag": true, "value": 42}}),
            JsonArray::from(&aligned).try_get_value(0)?
        );
        assert_eq!(
            json!({"nested": null}),
            JsonArray::from(&aligned).try_get_value(1)?
        );

        Ok(())
    }

    #[test]
    fn test_reconstruct_json2_v2_value() -> Result<()> {
        let remainders = json_values_to_variant(&[
            Some(json!({"cold": 1, "nested": {"right": true}})),
            Some(json!({"!__remainder__!": "user value"})),
        ])?;
        let remainder = Arc::new(variant_field(JSON2_REMAINDER_FIELD_NAME, true));
        let nested = Arc::new(Field::new_struct(
            "nested",
            [Arc::new(Field::new("left", DataType::Utf8, true))],
            true,
        ));
        let nested_values: ArrayRef = Arc::new(StructArray::from(vec![(
            Arc::new(Field::new("left", DataType::Utf8, true)),
            Arc::new(StringArray::from(vec![Some("value"), None])) as ArrayRef,
        )]));
        let fields = Fields::from(vec![
            remainder,
            Arc::new(Field::new("count", DataType::Int64, true)),
            nested,
        ]);
        let array: ArrayRef = Arc::new(StructArray::new(
            fields.clone(),
            vec![
                remainders,
                Arc::new(Int64Array::from(vec![Some(42), None])),
                nested_values,
            ],
            None,
        ));
        let field = Field::new("data", DataType::Struct(fields), true).with_extension_type(
            Json2ExtensionType::new(Arc::new(JsonMetadata::new(JsonSettings::default()))),
        );

        assert_eq!(
            json!({
                "cold": 1,
                "count": 42,
                "nested": {"left": "value", "right": true}
            }),
            JsonArray::from(&array).json2_values()?[0]
        );
        assert_eq!(
            json!({
                "!__remainder__!": "user value",
                "nested": {}
            }),
            JsonArray::from(&array).json2_values()?[1]
        );
        let target = DataType::Struct(
            vec![
                Arc::new(Field::new("cold", DataType::UInt64, true)),
                Arc::new(Field::new("count", DataType::Int64, true)),
            ]
            .into(),
        );
        let projected = JsonArray::from(&array).project_to_v2(&field, &target)?;
        assert_eq!(
            json!({"cold": 1, "count": 42}),
            JsonArray::from(&projected).try_get_value(0)?
        );
        assert_eq!(
            json!({"cold": null, "count": null}),
            JsonArray::from(&projected).try_get_value(1)?
        );
        Ok(())
    }

    #[test]
    fn test_project_partial_json2_v2_without_remainder() -> Result<()> {
        let fields = Fields::from(vec![Arc::new(Field::new("hot", DataType::Int64, true))]);
        let array: ArrayRef = Arc::new(StructArray::new(
            fields.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2]))],
            None,
        ));
        let field = Field::new("data", DataType::Struct(fields), true).with_extension_type(
            Json2ExtensionType::new(Arc::new(JsonMetadata::new(JsonSettings::default()))),
        );

        let projected = JsonArray::from(&array).project_to_v2(&field, field.data_type())?;
        assert!(Arc::ptr_eq(&array, &projected));
        Ok(())
    }

    #[test]
    fn test_reject_conflict_json2_v2_path() -> Result<()> {
        let remainders = json_values_to_variant(&[Some(json!({"count": 1}))])?;
        let fields = Fields::from(vec![
            Arc::new(variant_field(JSON2_REMAINDER_FIELD_NAME, true)),
            Arc::new(Field::new("count", DataType::Int64, true)),
        ]);
        let array: ArrayRef = Arc::new(StructArray::new(
            fields,
            vec![remainders, Arc::new(Int64Array::from(vec![2]))],
            None,
        ));
        let error = JsonArray::from(&array).json2_values().unwrap_err();
        assert!(
            error.to_string().contains(
                "cannot merge 'count' in explicit fields and remainder: not both objects"
            )
        );

        let Value::Object(mut remainder) = json!({"count": 1}) else {
            unreachable!();
        };
        let error = merge_explicit_value(
            &mut remainder,
            "count".to_string(),
            json!(1),
            &mut Vec::new(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("cannot merge 'count'"));

        let Value::Object(mut remainder) = json!({"nested": {"count": 1}}) else {
            unreachable!();
        };
        let error = merge_explicit_value(
            &mut remainder,
            "nested".to_string(),
            json!({"count": 2}),
            &mut Vec::new(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("cannot merge 'nested.count'"));
        Ok(())
    }
}
