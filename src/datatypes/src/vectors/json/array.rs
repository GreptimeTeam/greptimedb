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
use arrow_array::{Array, ArrayRef, StructArray, new_null_array};
use arrow_schema::DataType;
use snafu::ResultExt;

use crate::arrow_array::StringArray;
use crate::error::{AlignJsonArraySnafu, ArrowComputeSnafu, Result};

pub struct JsonArray<'a> {
    inner: &'a ArrayRef,
}

impl JsonArray<'_> {
    /// Align a JSON array to the `expect` data type. The `expect` data type is often the
    /// "largest" JSON type after some insertions in the table schema, while the JSON array previously
    /// written in the SST could be lagged behind it. So it's important to "align" the JSON array by
    /// setting the missing fields with null arrays, or casting the data.
    ///
    /// # Panics
    ///
    /// - The JSON array is not an Arrow [StructArray], or the provided `expect` data type is not
    ///   of Struct type. Both of which shouldn't happen unless we switch our implementation of how
    ///   JSON array is physically stored.
    pub fn try_align(&self, expect: &DataType) -> Result<ArrayRef> {
        let json_type = self.inner.data_type();
        if json_type == expect {
            return Ok(self.inner.clone());
        }

        let struct_array = self.inner.as_struct();
        let array_fields = struct_array.fields();
        let array_columns = struct_array.columns();
        let DataType::Struct(expect_fields) = expect else {
            unreachable!()
        };
        let mut aligned = Vec::with_capacity(expect_fields.len());

        // Compare the fields in the JSON array and the to-be-aligned schema, amending with null arrays
        // on the way. It's very important to note that fields in the JSON array and those in the JSON type
        // are both **SORTED**.
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
                        let array = JsonArray::from(&array_columns[j]);
                        if matches!(expect_field.data_type(), DataType::Struct(_)) {
                            // A `StructArray` in a JSON array must be another JSON array.
                            // (Like a nested JSON object in a JSON value.)
                            aligned.push(array.try_align(expect_field.data_type())?);
                        } else {
                            aligned.push(array.try_cast(expect_field.data_type())?);
                        }
                    }
                    i += 1;
                    j += 1;
                }
                Ordering::Less => {
                    aligned.push(new_null_array(expect_field.data_type(), struct_array.len()));
                    i += 1;
                }
                Ordering::Greater => {
                    j += 1;
                }
            }
        }
        if i < expect_fields.len() {
            for field in &expect_fields[i..] {
                aligned.push(new_null_array(field.data_type(), struct_array.len()));
            }
        }

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

    fn try_cast(&self, to_type: &DataType) -> Result<ArrayRef> {
        if compute::can_cast_types(self.inner.data_type(), to_type) {
            return compute::cast(&self.inner, to_type).context(ArrowComputeSnafu);
        }

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

impl<'a> From<&'a ArrayRef> for JsonArray<'a> {
    fn from(inner: &'a ArrayRef) -> Self {
        Self { inner }
    }
}

#[cfg(test)]
mod test {
    use arrow_array::types::Int64Type;
    use arrow_array::{BooleanArray, Float64Array, Int64Array, ListArray};
    use arrow_schema::{Field, Fields};

    use super::*;

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
            Err(
                r#"Failed to align JSON array, reason: this json array has more fields ["j"]"#
                    .to_string(),
            ),
        )
        .test()?;
        Ok(())
    }
}
