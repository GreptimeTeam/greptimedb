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

use arrow::compute;
use arrow::util::display::{ArrayFormatter, FormatOptions};
use arrow_array::cast::AsArray;
use arrow_array::types::{Float64Type, Int64Type, UInt64Type};
use arrow_array::{Array, ArrayRef, GenericListArray, ListArray, StructArray, new_null_array};
use arrow_schema::{DataType, FieldRef};
use serde_json::Value;
use snafu::{OptionExt, ResultExt, ensure};

use crate::arrow_array::{MutableBinaryArray, StringArray, binary_array_value, string_array_value};
use crate::error::{
    AlignJsonArraySnafu, ArrowComputeSnafu, DeserializeSnafu, InvalidJsonSnafu, Result,
    SerializeSnafu,
};

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
            DataType::Int64 => Value::from(array.as_primitive::<Int64Type>().value(i)),
            DataType::UInt64 => Value::from(array.as_primitive::<UInt64Type>().value(i)),
            DataType::Float64 => Value::from(array.as_primitive::<Float64Type>().value(i)),
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                Value::String(string_array_value(array, i).to_string())
            }
            DataType::Binary | DataType::LargeBinary | DataType::BinaryView => {
                let bytes = binary_array_value(array, i);
                serde_json::from_slice(bytes).with_context(|_| DeserializeSnafu {
                    json: String::from_utf8_lossy(bytes),
                })?
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

    /// Align a JSON array to the `expect` data type. The `expect` data type is often the "largest"
    /// JSON type after some insertions in the table schema, while the JSON array previously
    /// written in the SST could be lagged behind it. So it's important to "align" the JSON array by
    /// setting the missing fields with null arrays, or casting the data.
    ///
    /// It's an error if the to-be-aligned array contains extra fields that are not in the `expect`
    /// data type. Forcing to align that kind of array will result in data loss, something we
    /// generally not wanted.
    pub fn try_align(&self, expect: &DataType) -> Result<ArrayRef> {
        if self.inner.data_type() == expect {
            return Ok(self.inner.clone());
        }

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
                                JsonArray::from(&array_columns[j]).try_align(expect_type)?
                            }
                            (DataType::List(expect_item), DataType::List(array_item)) => {
                                let list_array = array_columns[j].as_list::<i32>();
                                try_align_list(list_array, expect_item, array_item)?
                            }
                            _ => JsonArray::from(&array_columns[j]).try_cast(expect_type)?,
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
                        reason: format!("extra fields are found: [{}]", array_field.name()),
                    }
                    .fail();
                }
            }
        }
        if i < expect_fields.len() {
            for field in &expect_fields[i..] {
                aligned.push(new_null_array(field.data_type(), struct_array.len()));
            }
        }
        ensure!(
            j >= array_fields.len(),
            AlignJsonArraySnafu {
                reason: format!(
                    "extra fields are found: [{}]",
                    array_fields[j..]
                        .iter()
                        .map(|x| x.name().as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            }
        );

        let json_array = StructArray::try_new(
            expect_fields.clone(),
            aligned,
            struct_array.nulls().cloned(),
        )
        .map_err(|e| {
            AlignJsonArraySnafu {
                reason: e.to_string(),
            }
            .build()
        })?;
        Ok(Arc::new(json_array))
    }

    fn try_decode_variant(&self) -> Result<ArrayRef> {
        let json_values = (0..self.inner.len())
            .map(|i| self.try_get_value(i))
            .collect::<Result<Vec<_>>>()?;

        let serialized_values = json_values
            .iter()
            .map(|value| {
                (!value.is_null())
                    .then(|| serde_json::to_vec(value))
                    .transpose()
            })
            .collect::<std::result::Result<Vec<_>, _>>()
            .context(SerializeSnafu)?;
        let total_bytes = serialized_values.iter().flatten().map(Vec::len).sum();

        let mut builder = MutableBinaryArray::with_capacity(self.inner.len(), total_bytes);
        for serialized_value in serialized_values {
            if let Some(bytes) = serialized_value {
                builder.append_value(bytes);
            } else {
                builder.append_null();
            }
        }

        Ok(Arc::new(builder.finish()))
    }

    fn try_cast(&self, to_type: &DataType) -> Result<ArrayRef> {
        if matches!(to_type, DataType::Binary) {
            return self.try_decode_variant();
        }

        if compute::can_cast_types(self.inner.data_type(), to_type) {
            return compute::cast(&self.inner, to_type).context(ArrowComputeSnafu);
        }

        // TODO(LFC): Cast according to `to_type` instead of formatting to String here.
        let formatter = ArrayFormatter::try_new(&self.inner, &FormatOptions::default())
            .context(ArrowComputeSnafu)?;
        let values = (0..self.inner.len())
            .map(|i| {
                self.inner
                    .is_valid(i)
                    .then(|| formatter.value(i).to_string())
            })
            .collect::<Vec<_>>();
        Ok(Arc::new(StringArray::from(values)))
    }
}

fn try_align_list(
    list_array: &ListArray,
    expect_item: &FieldRef,
    array_item: &FieldRef,
) -> Result<ArrayRef> {
    let item_aligned = match (expect_item.data_type(), array_item.data_type()) {
        (DataType::Struct(_), DataType::Struct(_)) => {
            JsonArray::from(list_array.values()).try_align(expect_item.data_type())?
        }
        (DataType::List(expect_item), DataType::List(array_item)) => {
            let list_array = list_array.values().as_list::<i32>();
            try_align_list(list_array, expect_item, array_item)?
        }
        _ => JsonArray::from(list_array.values()).try_cast(expect_item.data_type())?,
    };
    Ok(Arc::new(
        GenericListArray::<i32>::try_new(
            expect_item.clone(),
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
    use arrow_array::types::Int64Type;
    use arrow_array::{BinaryArray, BooleanArray, Float64Array, Int32Array, Int64Array, ListArray};
    use arrow_schema::{Field, Fields};
    use serde_json::json;

    use super::*;

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

        let floats: ArrayRef = Arc::new(Float64Array::from(vec![Some(1.5)]));
        assert_eq!(JsonArray::from(&floats).try_get_value(0)?, json!(1.5));

        let strings: ArrayRef = Arc::new(StringArray::from(vec![Some("hello"), None]));
        assert_eq!(JsonArray::from(&strings).try_get_value(0)?, json!("hello"));
        assert_eq!(JsonArray::from(&strings).try_get_value(1)?, Value::Null);

        let binaries: ArrayRef = Arc::new(BinaryArray::from(vec![
            br#"{"nested":[1,null,"x"]}"#.as_slice(),
            b"null".as_slice(),
        ]));
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

        let unsupported: ArrayRef = Arc::new(Int32Array::from(vec![1]));
        assert_eq!(
            JsonArray::from(&unsupported)
                .try_get_value(0)
                .unwrap_err()
                .to_string(),
            "Invalid JSON: unknown JSON type Int32"
        );

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
                let result = JsonArray::from(&self.json_array).try_align(&self.schema_type);
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

        // Test align failed.
        TestCase::new(
            StructArray::try_from(vec![
                ("i", Arc::new(Int64Array::from(vec![1])) as ArrayRef),
                ("j", Arc::new(Int64Array::from(vec![2])) as ArrayRef),
            ])
            .unwrap(),
            Fields::from(vec![Field::new("i", DataType::Int64, true)]),
            Err("Failed to align JSON array, reason: extra fields are found: [j]".to_string()),
        )
        .test()?;
        Ok(())
    }
}
