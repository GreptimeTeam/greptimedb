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

use std::sync::Arc;

use arrow::compute::{can_cast_types, cast};
use arrow_array::cast::AsArray;
use arrow_array::types::{
    Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type,
    UInt32Type, UInt64Type,
};
use arrow_array::{Array, ArrayRef, GenericListArray, StructArray, new_null_array};
use arrow_schema::{DataType, Field};
use serde_json::Value;
use snafu::{OptionExt, ResultExt};

use crate::arrow_array::{binary_array_value, string_array_value};
use crate::data_type::{ConcreteDataType, DataType as _};
use crate::error::{
    AlignJsonArraySnafu, ArrowComputeSnafu, InvalidJsonSnafu, InvalidJsonbSnafu, Result,
};
use crate::extension::json::{JSON2_REMAINDER_FIELD_NAME, json2_remainder_field};
use crate::json::value::decode_json_variant;
use crate::json::{JsonSettings, TypeHintMismatchPolicy, coerce_json_value_to_type};
use crate::vectors::MutableVector;
use crate::vectors::json::builder::{JsonVectorBuilder, json2_physical_data_type};
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

    /// Rewrites a JSON2 array from the current physical layout into the specified
    /// v2 physical layout.
    pub fn rewrite_to_v2(
        &self,
        field: &Field,
        logical_settings: &JsonSettings,
        target_layout: &JsonSettings,
    ) -> Result<ArrayRef> {
        self.rewrite_to_v2_with_type_hint_mismatch_policy(
            field,
            logical_settings,
            target_layout,
            TypeHintMismatchPolicy::Reject,
        )
    }

    /// Rewrites a JSON2 array to the specified v2 physical layout using the
    /// given type hint mismatch policy.
    pub fn rewrite_to_v2_with_type_hint_mismatch_policy(
        &self,
        field: &Field,
        logical_settings: &JsonSettings,
        target_layout: &JsonSettings,
        policy: TypeHintMismatchPolicy,
    ) -> Result<ArrayRef> {
        let is_v2 = json2_remainder_field(field)?.is_some();
        if is_v2 && self.inner.data_type() == &json2_physical_data_type(target_layout) {
            return Ok(self.inner.clone());
        }

        let values = if is_v2 {
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
                let value =
                    logical_settings.encode_with_type_hint_mismatch_policy(value, policy)?;
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
                // Arrow child nulls cannot distinguish a missing path from an explicit JSON
                // null. Builders preserve explicit null presence in the remainder, so nulls
                // from the explicit branch must be discarded before merging both branches.
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
        let value = coerce_json_value_to_type(value, &concrete_type);
        builder.try_push_value_ref(&value.as_value_ref())?;
    }
    Ok(builder.to_vector().to_arrow_array())
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
    use crate::json::{JsonSettings, JsonTypeHint};
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
    fn test_rewrite_to_v2_reuses_matching_layout() -> Result<()> {
        let settings = JsonSettings::try_new(
            vec![JsonTypeHint {
                path: vec!["kind".to_string()],
                data_type: ConcreteDataType::string_datatype(),
                inverted_index: false,
            }],
            Some(0),
        )?;
        let value = settings.encode(json!({"kind": "access", "cold": 1}))?;
        let mut builder = JsonVectorBuilder::with_settings(&settings, 1);
        builder.try_push_value_ref(&value.as_value_ref())?;
        let array = builder.to_vector().to_arrow_array();
        let structs = array.as_struct();
        assert!(structs.column_by_name("kind").is_some());
        assert_eq!(
            vec![Some(json!({"cold": 1}))],
            variant_to_json_values(structs.column_by_name(JSON2_REMAINDER_FIELD_NAME).unwrap())?
        );
        let field = Field::new("data", array.data_type().clone(), true).with_extension_type(
            Json2ExtensionType::new(Arc::new(JsonMetadata::new(settings.clone()))),
        );

        let rewritten = JsonArray::from(&array).rewrite_to_v2(&field, &settings, &settings)?;

        assert!(Arc::ptr_eq(&array, &rewritten));
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
