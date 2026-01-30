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

use std::collections::HashMap;
use std::slice;
use std::sync::Arc;

use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion_common::arrow::array::ArrayRef;
use datafusion_common::arrow::compute;
use datafusion_common::arrow::datatypes::{DataType as ArrowDataType, SchemaRef as ArrowSchemaRef};
use datatypes::arrow::array::{Array, AsArray, RecordBatchOptions, StructArray, new_null_array};
use datatypes::extension::json::is_json_extension_type;
use datatypes::prelude::DataType;
use datatypes::schema::SchemaRef;
use datatypes::vectors::{Helper, VectorRef};
use serde::ser::{Error, SerializeStruct};
use serde::{Serialize, Serializer};
use snafu::{OptionExt, ResultExt, ensure};

use crate::DfRecordBatch;
use crate::error::{
    self, AlignJsonArraySnafu, ArrowComputeSnafu, ColumnNotExistsSnafu, DataTypesSnafu,
    NewDfRecordBatchSnafu, ProjectArrowRecordBatchSnafu, Result,
};

/// A two-dimensional batch of column-oriented data with a defined schema.
#[derive(Clone, Debug, PartialEq)]
pub struct RecordBatch {
    pub schema: SchemaRef,
    df_record_batch: DfRecordBatch,
}

impl RecordBatch {
    /// Create a new [`RecordBatch`] from `schema` and `columns`.
    pub fn new<I: IntoIterator<Item = VectorRef>>(
        schema: SchemaRef,
        columns: I,
    ) -> Result<RecordBatch> {
        let columns: Vec<_> = columns.into_iter().collect();
        let arrow_arrays = columns.iter().map(|v| v.to_arrow_array()).collect();

        // Casting the arrays here to match the schema, is a temporary solution to support Arrow's
        // view array types (`StringViewArray` and `BinaryViewArray`).
        // As to "support": the arrays here are created from vectors, which do not have types
        // corresponding to view arrays. What we can do is to only cast them.
        // As to "temporary": we are planing to use Arrow's RecordBatch directly in the read path.
        // the casting here will be removed in the end.
        // TODO(LFC): Remove the casting here once `Batch` is no longer used.
        let arrow_arrays = Self::cast_view_arrays(schema.arrow_schema(), arrow_arrays)?;

        let arrow_arrays = maybe_align_json_array_with_schema(schema.arrow_schema(), arrow_arrays)?;

        let df_record_batch = DfRecordBatch::try_new(schema.arrow_schema().clone(), arrow_arrays)
            .context(error::NewDfRecordBatchSnafu)?;

        Ok(RecordBatch {
            schema,
            df_record_batch,
        })
    }

    pub fn to_df_record_batch<I: IntoIterator<Item = VectorRef>>(
        arrow_schema: ArrowSchemaRef,
        columns: I,
    ) -> Result<DfRecordBatch> {
        let columns: Vec<_> = columns.into_iter().collect();
        let arrow_arrays = columns.iter().map(|v| v.to_arrow_array()).collect();

        // Casting the arrays here to match the schema, is a temporary solution to support Arrow's
        // view array types (`StringViewArray` and `BinaryViewArray`).
        // As to "support": the arrays here are created from vectors, which do not have types
        // corresponding to view arrays. What we can do is to only cast them.
        // As to "temporary": we are planing to use Arrow's RecordBatch directly in the read path.
        // the casting here will be removed in the end.
        // TODO(LFC): Remove the casting here once `Batch` is no longer used.
        let arrow_arrays = Self::cast_view_arrays(&arrow_schema, arrow_arrays)?;

        let arrow_arrays = maybe_align_json_array_with_schema(&arrow_schema, arrow_arrays)?;

        let df_record_batch = DfRecordBatch::try_new(arrow_schema, arrow_arrays)
            .context(error::NewDfRecordBatchSnafu)?;

        Ok(df_record_batch)
    }

    fn cast_view_arrays(
        schema: &ArrowSchemaRef,
        mut arrays: Vec<ArrayRef>,
    ) -> Result<Vec<ArrayRef>> {
        for (f, a) in schema.fields().iter().zip(arrays.iter_mut()) {
            let expected = f.data_type();
            let actual = a.data_type();
            if matches!(
                (expected, actual),
                (ArrowDataType::Utf8View, ArrowDataType::Utf8)
                    | (ArrowDataType::BinaryView, ArrowDataType::Binary)
            ) {
                *a = compute::cast(a, expected).context(ArrowComputeSnafu)?;
            }
        }
        Ok(arrays)
    }

    /// Create an empty [`RecordBatch`] from `schema`.
    pub fn new_empty(schema: SchemaRef) -> RecordBatch {
        let df_record_batch = DfRecordBatch::new_empty(schema.arrow_schema().clone());
        RecordBatch {
            schema,
            df_record_batch,
        }
    }

    /// Create an empty [`RecordBatch`] from `schema` with `num_rows`.
    pub fn new_with_count(schema: SchemaRef, num_rows: usize) -> Result<Self> {
        let df_record_batch = DfRecordBatch::try_new_with_options(
            schema.arrow_schema().clone(),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(num_rows)),
        )
        .context(error::NewDfRecordBatchSnafu)?;
        Ok(RecordBatch {
            schema,
            df_record_batch,
        })
    }

    pub fn try_project(&self, indices: &[usize]) -> Result<Self> {
        let schema = Arc::new(self.schema.try_project(indices).context(DataTypesSnafu)?);
        let df_record_batch = self.df_record_batch.project(indices).with_context(|_| {
            ProjectArrowRecordBatchSnafu {
                schema: self.schema.clone(),
                projection: indices.to_vec(),
            }
        })?;

        Ok(Self {
            schema,
            df_record_batch,
        })
    }

    /// Create a new [`RecordBatch`] from `schema` and `df_record_batch`.
    ///
    /// This method doesn't check the schema.
    pub fn from_df_record_batch(schema: SchemaRef, df_record_batch: DfRecordBatch) -> RecordBatch {
        RecordBatch {
            schema,
            df_record_batch,
        }
    }

    #[inline]
    pub fn df_record_batch(&self) -> &DfRecordBatch {
        &self.df_record_batch
    }

    #[inline]
    pub fn into_df_record_batch(self) -> DfRecordBatch {
        self.df_record_batch
    }

    #[inline]
    pub fn columns(&self) -> &[ArrayRef] {
        self.df_record_batch.columns()
    }

    #[inline]
    pub fn column(&self, idx: usize) -> &ArrayRef {
        self.df_record_batch.column(idx)
    }

    pub fn column_by_name(&self, name: &str) -> Option<&ArrayRef> {
        self.df_record_batch.column_by_name(name)
    }

    #[inline]
    pub fn num_columns(&self) -> usize {
        self.df_record_batch.num_columns()
    }

    #[inline]
    pub fn num_rows(&self) -> usize {
        self.df_record_batch.num_rows()
    }

    pub fn column_vectors(
        &self,
        table_name: &str,
        table_schema: SchemaRef,
    ) -> Result<HashMap<String, VectorRef>> {
        let mut vectors = HashMap::with_capacity(self.num_columns());

        // column schemas in recordbatch must match its vectors, otherwise it's corrupted
        for (field, array) in self
            .df_record_batch
            .schema()
            .fields()
            .iter()
            .zip(self.df_record_batch.columns().iter())
        {
            let column_name = field.name();
            let column_schema =
                table_schema
                    .column_schema_by_name(column_name)
                    .context(ColumnNotExistsSnafu {
                        table_name,
                        column_name,
                    })?;
            let vector = if field.data_type() != &column_schema.data_type.as_arrow_type() {
                let array = compute::cast(array, &column_schema.data_type.as_arrow_type())
                    .context(ArrowComputeSnafu)?;
                Helper::try_into_vector(array).context(DataTypesSnafu)?
            } else {
                Helper::try_into_vector(array).context(DataTypesSnafu)?
            };

            let _ = vectors.insert(column_name.clone(), vector);
        }

        Ok(vectors)
    }

    /// Pretty display this record batch like a table
    pub fn pretty_print(&self) -> String {
        pretty_format_batches(slice::from_ref(&self.df_record_batch))
            .map(|t| t.to_string())
            .unwrap_or("failed to pretty display a record batch".to_string())
    }

    /// Return a slice record batch starts from offset, with len rows
    pub fn slice(&self, offset: usize, len: usize) -> Result<RecordBatch> {
        ensure!(
            offset + len <= self.num_rows(),
            error::RecordBatchSliceIndexOverflowSnafu {
                size: self.num_rows(),
                visit_index: offset + len
            }
        );
        let sliced = self.df_record_batch.slice(offset, len);
        Ok(RecordBatch::from_df_record_batch(
            self.schema.clone(),
            sliced,
        ))
    }

    /// Returns the total number of bytes of memory pointed to by the arrays in this `RecordBatch`.
    ///
    /// The buffers store bytes in the Arrow memory format, and include the data as well as the validity map.
    /// Note that this does not always correspond to the exact memory usage of an array,
    /// since multiple arrays can share the same buffers or slices thereof.
    pub fn buffer_memory_size(&self) -> usize {
        self.df_record_batch
            .columns()
            .iter()
            .map(|array| array.get_buffer_memory_size())
            .sum()
    }

    /// Iterate the values as strings in the column at index `i`.
    ///
    /// Note that if the underlying array is not a valid GreptimeDB vector, an empty iterator is
    /// returned.
    ///
    /// # Panics
    /// if index `i` is out of bound.
    pub fn iter_column_as_string(&self, i: usize) -> Box<dyn Iterator<Item = Option<String>> + '_> {
        macro_rules! iter {
            ($column: ident) => {
                Box::new(
                    (0..$column.len())
                        .map(|i| $column.is_valid(i).then(|| $column.value(i).to_string())),
                )
            };
        }

        let column = self.df_record_batch.column(i);
        match column.data_type() {
            ArrowDataType::Utf8 => {
                let column = column.as_string::<i32>();
                let iter = iter!(column);
                iter as _
            }
            ArrowDataType::LargeUtf8 => {
                let column = column.as_string::<i64>();
                iter!(column)
            }
            ArrowDataType::Utf8View => {
                let column = column.as_string_view();
                iter!(column)
            }
            _ => {
                if let Ok(column) = Helper::try_into_vector(column) {
                    Box::new(
                        (0..column.len())
                            .map(move |i| (!column.is_null(i)).then(|| column.get(i).to_string())),
                    )
                } else {
                    Box::new(std::iter::empty())
                }
            }
        }
    }
}

impl Serialize for RecordBatch {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // TODO(yingwen): arrow and arrow2's schemas have different fields, so
        // it might be better to use our `RawSchema` as serialized field.
        let mut s = serializer.serialize_struct("record", 2)?;
        s.serialize_field("schema", &**self.schema.arrow_schema())?;

        let columns = self.df_record_batch.columns();
        let columns = Helper::try_into_vectors(columns).map_err(Error::custom)?;
        let vec = columns
            .iter()
            .map(|c| c.serialize_to_json())
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(S::Error::custom)?;

        s.serialize_field("columns", &vec)?;
        s.end()
    }
}

/// merge multiple recordbatch into a single
pub fn merge_record_batches(schema: SchemaRef, batches: &[RecordBatch]) -> Result<RecordBatch> {
    let batches_len = batches.len();
    if batches_len == 0 {
        return Ok(RecordBatch::new_empty(schema));
    }

    let record_batch = compute::concat_batches(
        schema.arrow_schema(),
        batches.iter().map(|x| x.df_record_batch()),
    )
    .context(ArrowComputeSnafu)?;

    // Create a new RecordBatch with merged columns
    Ok(RecordBatch::from_df_record_batch(schema, record_batch))
}

/// Align a json array `json_array` to the json type `schema_type`. The `schema_type` is often the
/// "largest" json type after some insertions in the table schema, while the json array previously
/// written in the SST could be lagged behind it. So it's important to "amend" the json array's
/// missing fields with null arrays, to align the array's data type with the provided one.
///
/// # Panics
///
/// - The json array is not an Arrow [StructArray], or the provided data type `schema_type` is not
///   of Struct type. Both of which shouldn't happen unless we switch our implementation of how
///   json array is physically stored.
pub fn align_json_array(json_array: &ArrayRef, schema_type: &ArrowDataType) -> Result<ArrayRef> {
    let json_type = json_array.data_type();
    if json_type == schema_type {
        return Ok(json_array.clone());
    }

    let json_array = json_array.as_struct();
    let array_fields = json_array.fields();
    let array_columns = json_array.columns();
    let ArrowDataType::Struct(schema_fields) = schema_type else {
        unreachable!()
    };
    let mut aligned = Vec::with_capacity(schema_fields.len());

    // Compare the fields in the json array and the to-be-aligned schema, amending with null arrays
    // on the way. It's very important to note that fields in the json array and in the json type
    // are both SORTED.

    let mut i = 0; // point to the schema fields
    let mut j = 0; // point to the array fields
    while i < schema_fields.len() && j < array_fields.len() {
        let schema_field = &schema_fields[i];
        let array_field = &array_fields[j];
        if schema_field.name() == array_field.name() {
            if matches!(schema_field.data_type(), ArrowDataType::Struct(_)) {
                // A `StructArray`s in a json array must be another json array. (Like a nested json
                // object in a json value.)
                aligned.push(align_json_array(
                    &array_columns[j],
                    schema_field.data_type(),
                )?);
            } else {
                aligned.push(array_columns[j].clone());
            }
            j += 1;
        } else {
            aligned.push(new_null_array(schema_field.data_type(), json_array.len()));
        }
        i += 1;
    }
    if i < schema_fields.len() {
        for field in &schema_fields[i..] {
            aligned.push(new_null_array(field.data_type(), json_array.len()));
        }
    }
    ensure!(
        j == array_fields.len(),
        AlignJsonArraySnafu {
            reason: format!(
                "this json array has more fields {:?}",
                array_fields[j..]
                    .iter()
                    .map(|x| x.name())
                    .collect::<Vec<_>>(),
            )
        }
    );

    let json_array =
        StructArray::try_new(schema_fields.clone(), aligned, json_array.nulls().cloned())
            .context(NewDfRecordBatchSnafu)?;
    Ok(Arc::new(json_array))
}

fn maybe_align_json_array_with_schema(
    schema: &ArrowSchemaRef,
    arrays: Vec<ArrayRef>,
) -> Result<Vec<ArrayRef>> {
    if schema.fields().iter().all(|f| !is_json_extension_type(f)) {
        return Ok(arrays);
    }

    let mut aligned = Vec::with_capacity(arrays.len());
    for (field, array) in schema.fields().iter().zip(arrays.into_iter()) {
        if !is_json_extension_type(field) {
            aligned.push(array);
            continue;
        }

        let json_array = align_json_array(&array, field.data_type())?;
        aligned.push(json_array);
    }
    Ok(aligned)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datatypes::arrow::array::{
        AsArray, BooleanArray, Float64Array, Int64Array, ListArray, UInt32Array,
    };
    use datatypes::arrow::datatypes::{
        DataType, Field, Fields, Int64Type, Schema as ArrowSchema, UInt32Type,
    };
    use datatypes::arrow_array::StringArray;
    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, Schema};
    use datatypes::vectors::{StringVector, UInt32Vector};

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
                let result = align_json_array(&self.json_array, &self.schema_type);
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

    #[test]
    fn test_record_batch() {
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("c1", DataType::UInt32, false),
            Field::new("c2", DataType::UInt32, false),
        ]));
        let schema = Arc::new(Schema::try_from(arrow_schema).unwrap());

        let c1 = Arc::new(UInt32Vector::from_slice([1, 2, 3]));
        let c2 = Arc::new(UInt32Vector::from_slice([4, 5, 6]));
        let columns: Vec<VectorRef> = vec![c1, c2];

        let expected = vec![
            Arc::new(UInt32Array::from_iter_values([1, 2, 3])) as ArrayRef,
            Arc::new(UInt32Array::from_iter_values([4, 5, 6])),
        ];

        let batch = RecordBatch::new(schema.clone(), columns.clone()).unwrap();
        assert_eq!(3, batch.num_rows());
        assert_eq!(expected, batch.df_record_batch().columns());
        assert_eq!(schema, batch.schema);

        assert_eq!(&expected[0], batch.column_by_name("c1").unwrap());
        assert_eq!(&expected[1], batch.column_by_name("c2").unwrap());
        assert!(batch.column_by_name("c3").is_none());

        let converted = RecordBatch::from_df_record_batch(schema, batch.df_record_batch().clone());
        assert_eq!(batch, converted);
        assert_eq!(*batch.df_record_batch(), converted.into_df_record_batch());
    }

    #[test]
    pub fn test_serialize_recordbatch() {
        let column_schemas = vec![ColumnSchema::new(
            "number",
            ConcreteDataType::uint32_datatype(),
            false,
        )];
        let schema = Arc::new(Schema::try_new(column_schemas).unwrap());

        let numbers: Vec<u32> = (0..10).collect();
        let columns = vec![Arc::new(UInt32Vector::from_slice(numbers)) as VectorRef];
        let batch = RecordBatch::new(schema, columns).unwrap();

        let output = serde_json::to_string(&batch).unwrap();
        assert_eq!(
            r#"{"schema":{"fields":[{"name":"number","data_type":"UInt32","nullable":false,"dict_id":0,"dict_is_ordered":false,"metadata":{}}],"metadata":{"greptime:version":"0"}},"columns":[[0,1,2,3,4,5,6,7,8,9]]}"#,
            output
        );
    }

    #[test]
    fn test_record_batch_slice() {
        let column_schemas = vec![
            ColumnSchema::new("numbers", ConcreteDataType::uint32_datatype(), false),
            ColumnSchema::new("strings", ConcreteDataType::string_datatype(), true),
        ];
        let schema = Arc::new(Schema::new(column_schemas));
        let columns: Vec<VectorRef> = vec![
            Arc::new(UInt32Vector::from_slice(vec![1, 2, 3, 4])),
            Arc::new(StringVector::from(vec![
                None,
                Some("hello"),
                Some("greptime"),
                None,
            ])),
        ];
        let recordbatch = RecordBatch::new(schema, columns).unwrap();
        let recordbatch = recordbatch.slice(1, 2).expect("recordbatch slice");

        let expected = &UInt32Array::from_iter_values([2u32, 3]);
        let array = recordbatch.column(0);
        let actual = array.as_primitive::<UInt32Type>();
        assert_eq!(expected, actual);

        let expected = &StringArray::from(vec!["hello", "greptime"]);
        let array = recordbatch.column(1);
        let actual = array.as_string::<i32>();
        assert_eq!(expected, actual);

        assert!(recordbatch.slice(1, 5).is_err());
    }

    #[test]
    fn test_merge_record_batch() {
        let column_schemas = vec![
            ColumnSchema::new("numbers", ConcreteDataType::uint32_datatype(), false),
            ColumnSchema::new("strings", ConcreteDataType::string_datatype(), true),
        ];
        let schema = Arc::new(Schema::new(column_schemas));
        let columns: Vec<VectorRef> = vec![
            Arc::new(UInt32Vector::from_slice(vec![1, 2, 3, 4])),
            Arc::new(StringVector::from(vec![
                None,
                Some("hello"),
                Some("greptime"),
                None,
            ])),
        ];
        let recordbatch = RecordBatch::new(schema.clone(), columns).unwrap();

        let columns: Vec<VectorRef> = vec![
            Arc::new(UInt32Vector::from_slice(vec![1, 2, 3, 4])),
            Arc::new(StringVector::from(vec![
                None,
                Some("hello"),
                Some("greptime"),
                None,
            ])),
        ];
        let recordbatch2 = RecordBatch::new(schema.clone(), columns).unwrap();

        let merged = merge_record_batches(schema.clone(), &[recordbatch, recordbatch2])
            .expect("merge recordbatch");
        assert_eq!(merged.num_rows(), 8);
    }
}
