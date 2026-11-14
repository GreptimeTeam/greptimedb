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
use datatypes::arrow::array::{Array, AsArray, RecordBatchOptions};
use datatypes::prelude::DataType;
use datatypes::schema::SchemaRef;
use datatypes::vectors::{Helper, VectorRef};
use serde::ser::{Error, SerializeStruct};
use serde::{Serialize, Serializer};
use snafu::{OptionExt, ResultExt, ensure};

use crate::DfRecordBatch;
use crate::error::{
    self, ArrowComputeSnafu, ColumnNotExistsSnafu, DataTypesSnafu, ProjectArrowRecordBatchSnafu,
    Result,
};

/// What [`RecordBatch::column_vectors`] does with the payloads of JSON columns.
///
/// A jsonb column is exposed as `Binary` in Arrow, so a value written into it can
/// still be JSON text; this policy decides whether the write boundary encodes such
/// payloads (see [`crate::jsonb_payload`]). The bytes of a *key* column are not just
/// a value though: they are what a row is stored and looked up under, so they must
/// not be rewritten.
///
/// This is the canonical explanation of the rule; the call sites reference it
/// instead of repeating it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonPayloadPolicy<'a> {
    /// Encode JSON text payloads, except in the given key columns.
    ///
    /// The columns are named rather than indexed because the COPY adapter applies
    /// the same policy to a projected schema, where table-schema indices do not
    /// survive. A key column keeps whatever bytes the statement provides - rows
    /// written by an older version can hold a text-encoded JSON key, and rewriting
    /// it would move them to a different key - but those bytes are still validated,
    /// so a key column rejects exactly the payloads a non-key column rejects.
    NormalizeExceptKeys { key_columns: &'a [String] },

    /// Keep every payload byte-identical and do not validate it either.
    ///
    /// Used by DELETE: it is planned from the scanned key columns and storage looks
    /// rows up by exactly those bytes, so re-encoding a legacy text-encoded JSON key
    /// would miss the row (or match a canonical sibling). The payloads are values
    /// read back from storage, so they are not re-validated.
    Preserve,
}

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

    /// Converts this batch into the column vectors of the table it is written to.
    ///
    /// This is the insert boundary: every value is converted against the target
    /// column type here, which includes normalizing the payloads of a JSON column
    /// (a jsonb column is exposed as `Binary`, so a value can still be JSON text
    /// when it reaches this point). `json_policy` decides which payloads may be
    /// rewritten; see [`JsonPayloadPolicy`].
    pub fn column_vectors(
        &self,
        table_name: &str,
        table_schema: SchemaRef,
        json_policy: JsonPayloadPolicy<'_>,
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
            let target_type = column_schema.data_type.as_arrow_type();
            let array = if field.data_type() != &target_type {
                compute::cast(array, &target_type).context(ArrowComputeSnafu)?
            } else {
                Arc::clone(array)
            };
            // `Json2` columns use a struct arrow type and never reach this branch.
            let array = if column_schema.data_type.is_json()
                && matches!(array.data_type(), ArrowDataType::Binary)
            {
                match json_policy {
                    JsonPayloadPolicy::NormalizeExceptKeys { key_columns } => {
                        if key_columns.iter().any(|key| key == column_name) {
                            crate::jsonb_payload::validate_jsonb_array(&array)?;
                            array
                        } else {
                            crate::jsonb_payload::normalize_jsonb_array(&array)?
                        }
                    }
                    JsonPayloadPolicy::Preserve => array,
                }
            } else {
                array
            };

            let vector = Helper::try_into_vector(array).context(DataTypesSnafu)?;
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

    /// Returns the logical memory size of this batch's array slices.
    ///
    /// This sums Arrow's logical visible slice buffers rather than the capacity of their shared
    /// backing buffers. View out-of-line payloads and nested custom payloads are not separately
    /// traversed or accounted. It is not an exact measure of live physical memory. If Arrow cannot
    /// calculate a slice's size, the full buffer size is used conservatively.
    ///
    /// Mito's current scan paths do not produce top-level View arrays. If they do in the future,
    /// their out-of-line payload accounting must be reassessed here.
    pub fn logical_slice_memory_size(&self) -> usize {
        self.df_record_batch
            .columns()
            .iter()
            .fold(0, |total, array| {
                let array_size = array
                    .to_data()
                    .get_slice_memory_size()
                    .unwrap_or_else(|_| array.get_buffer_memory_size());
                total.saturating_add(array_size)
            })
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datatypes::arrow::array::{AsArray, StringArray, StringViewArray, UInt32Array};
    use datatypes::arrow::datatypes::{DataType, Field, Schema as ArrowSchema, UInt32Type};
    use datatypes::data_type::ConcreteDataType;
    use datatypes::extension::json::JsonExtensionType;
    use datatypes::schema::{ColumnSchema, Schema};
    use datatypes::value::Value;
    use datatypes::vectors::{BinaryVector, StringVector, UInt32Vector};

    use super::*;

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

    fn json_table_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            ColumnSchema::new("j", ConcreteDataType::json_datatype(), true),
            ColumnSchema::new("b", ConcreteDataType::binary_datatype(), true),
            ColumnSchema::new("v", ConcreteDataType::vector_datatype(3), true),
        ]))
    }

    fn binary_column(payloads: Vec<Option<&[u8]>>) -> ArrayRef {
        Arc::new(datatypes::arrow::array::BinaryArray::from_iter(payloads))
    }

    fn binary_payloads(vector: &VectorRef) -> Vec<Option<Vec<u8>>> {
        (0..vector.len())
            .map(|index| match vector.get(index) {
                Value::Binary(bytes) => Some(bytes.to_vec()),
                Value::Null => None,
                other => panic!("unexpected value: {other:?}"),
            })
            .collect()
    }

    /// JSON text written through the plan-based insert path (MySQL/Postgres
    /// prepared statements and every other SQL insert) must be encoded as jsonb
    /// when it reaches the JSON column.
    #[test]
    fn test_column_vectors_encodes_json_text() {
        use datatypes::types::jsonb_to_string;

        let schema = json_table_schema();
        let json_text = binary_column(vec![
            Some(br#"{"a":"x"}"#),
            Some(b"12345678"),
            Some(b""),
            None,
        ]);
        let blob = binary_column(vec![
            Some(b"12345678"),
            Some(b"12345678"),
            Some(b"12345678"),
            Some(b"12345678"),
        ]);
        let vector_column = binary_column(vec![
            Some(b"12345678"),
            Some(b"12345678"),
            Some(b"12345678"),
            Some(b"12345678"),
        ]);

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("j", DataType::Binary, true),
            Field::new("b", DataType::Binary, true),
            Field::new("v", DataType::Binary, true),
        ]));
        let batch = RecordBatch::from_df_record_batch(
            Arc::new(Schema::try_from(Arc::clone(&arrow_schema)).unwrap()),
            DfRecordBatch::try_new(
                arrow_schema,
                vec![json_text, blob.clone(), vector_column.clone()],
            )
            .unwrap(),
        );

        let vectors = batch
            .column_vectors(
                "t",
                schema,
                JsonPayloadPolicy::NormalizeExceptKeys { key_columns: &[] },
            )
            .unwrap();

        let json = binary_payloads(&vectors["j"]);
        assert_eq!(
            r#"{"a":"x"}"#,
            jsonb_to_string(&json[0].clone().unwrap()).unwrap()
        );
        assert_eq!(
            "12345678",
            jsonb_to_string(&json[1].clone().unwrap()).unwrap()
        );
        // An empty payload (how a NULL JSON value is exported to CSV) stays empty.
        assert_eq!(Some(Vec::new()), json[2]);
        assert_eq!(None, json[3]);

        // Non-JSON binary columns are untouched.
        let expected_binary = vec![Some(b"12345678".to_vec()); 4];
        assert_eq!(expected_binary, binary_payloads(&vectors["b"]));
        assert_eq!(expected_binary, binary_payloads(&vectors["v"]));
    }

    /// A batch can mix native jsonb, JSON text and nulls; normalizing the text must
    /// not disturb the payloads that are already canonical jsonb.
    #[test]
    fn test_column_vectors_normalizes_mixed_json_payloads() {
        use datatypes::types::{jsonb_to_string, parse_string_to_jsonb};

        let schema = json_table_schema();
        let canonical = parse_string_to_jsonb(r#"{"a":"x"}"#).unwrap();
        let canonical_number = parse_string_to_jsonb("12345678").unwrap();
        let json_column = binary_column(vec![
            Some(&canonical),
            Some(br#"{"b":"y"}"#),
            None,
            Some(b"12345678"),
            Some(&canonical_number),
        ]);

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("j", DataType::Binary, true),
            Field::new("b", DataType::Binary, true),
            Field::new("v", DataType::Binary, true),
        ]));
        let batch = RecordBatch::from_df_record_batch(
            Arc::new(Schema::try_from(Arc::clone(&arrow_schema)).unwrap()),
            DfRecordBatch::try_new(
                arrow_schema,
                vec![
                    json_column,
                    binary_column(vec![None; 5]),
                    binary_column(vec![None; 5]),
                ],
            )
            .unwrap(),
        );

        let vectors = batch
            .column_vectors(
                "t",
                schema,
                JsonPayloadPolicy::NormalizeExceptKeys { key_columns: &[] },
            )
            .unwrap();
        let json = binary_payloads(&vectors["j"]);

        // The payloads that were already jsonb stay byte-identical.
        assert_eq!(Some(canonical), json[0]);
        assert_eq!(Some(canonical_number), json[4]);
        // The text payloads are encoded.
        assert_eq!(
            r#"{"b":"y"}"#,
            jsonb_to_string(&json[1].clone().unwrap()).unwrap()
        );
        assert_eq!(
            "12345678",
            jsonb_to_string(&json[3].clone().unwrap()).unwrap()
        );
        // Nulls stay null.
        assert_eq!(None, json[2]);
    }

    fn json_key_table_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            // A JSON1 column that is part of the primary key.
            ColumnSchema::new("k", ConcreteDataType::json_datatype(), false),
            ColumnSchema::new("v", ConcreteDataType::json_datatype(), true),
        ]))
    }

    fn json_key_batch(columns: Vec<ArrayRef>) -> RecordBatch {
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("k", DataType::Binary, true),
            Field::new("v", DataType::Binary, true),
        ]));
        RecordBatch::from_df_record_batch(
            Arc::new(Schema::try_from(Arc::clone(&arrow_schema)).unwrap()),
            DfRecordBatch::try_new(arrow_schema, columns).unwrap(),
        )
    }

    /// A JSON primary key keeps its bytes: a row written with a text-encoded key
    /// must stay findable by that key, and a DELETE looks rows up by exactly the
    /// scanned bytes.
    #[test]
    fn test_column_vectors_keeps_json_primary_key_bytes() {
        use datatypes::types::parse_string_to_jsonb;

        let schema = json_key_table_schema();
        let text_key = br#"{"a":"x"}"#;
        let canonical_key = parse_string_to_jsonb(r#"{"a":"y"}"#).unwrap();
        let batch = json_key_batch(vec![
            binary_column(vec![Some(text_key), Some(&canonical_key)]),
            binary_column(vec![Some(b"12345678"), Some(b"12345678")]),
        ]);

        // INSERT: the key column is a primary key, so it is left alone; the value
        // column is still normalized.
        let vectors = batch
            .column_vectors(
                "t",
                Arc::clone(&schema),
                JsonPayloadPolicy::NormalizeExceptKeys {
                    key_columns: &["k".to_string()],
                },
            )
            .unwrap();
        assert_eq!(
            vec![Some(text_key.to_vec()), Some(canonical_key.clone())],
            binary_payloads(&vectors["k"])
        );
        assert_eq!(
            "12345678",
            datatypes::types::jsonb_to_string(&binary_payloads(&vectors["v"])[0].clone().unwrap())
                .unwrap()
        );

        // DELETE: nothing is normalized, not even a non-key column.
        let vectors = batch
            .column_vectors("t", schema, JsonPayloadPolicy::Preserve)
            .unwrap();
        assert_eq!(
            vec![Some(text_key.to_vec()), Some(canonical_key.clone())],
            binary_payloads(&vectors["k"])
        );
        assert_eq!(
            vec![Some(b"12345678".to_vec()), Some(b"12345678".to_vec())],
            binary_payloads(&vectors["v"])
        );
    }

    /// A JSON key column keeps its bytes, but it still rejects a payload the read
    /// side cannot decode - with the same error a non-key JSON column produces.
    #[test]
    fn test_column_vectors_validates_json_primary_key_bytes() {
        let schema = json_key_table_schema();
        let garbage = br#"{"a":}"#;

        // Garbage in the key column.
        let key_batch = json_key_batch(vec![
            binary_column(vec![Some(garbage)]),
            binary_column(vec![Some(b"12345678")]),
        ]);
        let key_err = key_batch
            .column_vectors(
                "t",
                Arc::clone(&schema),
                JsonPayloadPolicy::NormalizeExceptKeys {
                    key_columns: &["k".to_string()],
                },
            )
            .expect_err("garbage in a JSON key column must be rejected");

        // The same payload in the non-key column.
        let value_batch = json_key_batch(vec![
            binary_column(vec![Some(br#"{"a":"x"}"#)]),
            binary_column(vec![Some(garbage)]),
        ]);
        let value_err = value_batch
            .column_vectors(
                "t",
                Arc::clone(&schema),
                JsonPayloadPolicy::NormalizeExceptKeys {
                    key_columns: &["k".to_string()],
                },
            )
            .expect_err("garbage in a JSON column must be rejected");

        assert!(
            key_err
                .to_string()
                .contains("Invalid value for a JSON column"),
            "unexpected error: {key_err}"
        );
        assert_eq!(
            key_err.to_string(),
            value_err.to_string(),
            "a key column must reject a payload exactly like a non-key column"
        );

        // A legacy text key and a canonical key are both accepted unchanged.
        let text_key = br#"{"a":"x"}"#;
        let canonical_key = datatypes::types::parse_string_to_jsonb(r#"{"a":"y"}"#).unwrap();
        let batch = json_key_batch(vec![
            binary_column(vec![Some(text_key), Some(&canonical_key)]),
            binary_column(vec![Some(b"1"), Some(b"2")]),
        ]);
        let vectors = batch
            .column_vectors(
                "t",
                schema,
                JsonPayloadPolicy::NormalizeExceptKeys {
                    key_columns: &["k".to_string()],
                },
            )
            .unwrap();
        assert_eq!(
            vec![Some(text_key.to_vec()), Some(canonical_key)],
            binary_payloads(&vectors["k"])
        );
    }

    /// Native jsonb reaching the insert boundary is not re-encoded.
    #[test]
    fn test_column_vectors_keeps_native_jsonb() {
        use datatypes::types::parse_string_to_jsonb;

        let schema = json_table_schema();
        let native = parse_string_to_jsonb(r#"{"a":"x"}"#).unwrap();
        let json_column = binary_column(vec![Some(&native)]);

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("j", DataType::Binary, true),
            Field::new("b", DataType::Binary, true),
            Field::new("v", DataType::Binary, true),
        ]));
        let batch = RecordBatch::from_df_record_batch(
            Arc::new(Schema::try_from(Arc::clone(&arrow_schema)).unwrap()),
            DfRecordBatch::try_new(
                arrow_schema,
                vec![
                    json_column,
                    binary_column(vec![None]),
                    binary_column(vec![None]),
                ],
            )
            .unwrap(),
        );

        let vectors = batch
            .column_vectors(
                "t",
                schema,
                JsonPayloadPolicy::NormalizeExceptKeys { key_columns: &[] },
            )
            .unwrap();
        assert_eq!(vec![Some(native)], binary_payloads(&vectors["j"]));
    }

    /// A payload that is neither JSON text nor canonical jsonb fails the write.
    #[test]
    fn test_column_vectors_rejects_invalid_json() {
        let schema = json_table_schema();
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("j", DataType::Binary, true),
            Field::new("b", DataType::Binary, true),
            Field::new("v", DataType::Binary, true),
        ]));
        let batch = RecordBatch::from_df_record_batch(
            Arc::new(Schema::try_from(Arc::clone(&arrow_schema)).unwrap()),
            DfRecordBatch::try_new(
                arrow_schema,
                vec![
                    binary_column(vec![Some(b"{\"a\":}")]),
                    binary_column(vec![None]),
                    binary_column(vec![None]),
                ],
            )
            .unwrap(),
        );

        let err = batch
            .column_vectors(
                "t",
                schema,
                JsonPayloadPolicy::NormalizeExceptKeys { key_columns: &[] },
            )
            .unwrap_err();
        assert!(
            err.to_string().contains("Invalid value for a JSON column"),
            "unexpected error: {err}"
        );
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
    fn test_logical_slice_memory_size_for_visible_primitive_string_binary_slices() {
        let schema = Arc::new(Schema::new(vec![
            ColumnSchema::new("numbers", ConcreteDataType::uint32_datatype(), false),
            ColumnSchema::new("strings", ConcreteDataType::string_datatype(), true),
            ColumnSchema::new("binary", ConcreteDataType::binary_datatype(), true),
        ]));
        let numbers: Vec<_> = (0..1024).collect();
        let strings = (0..1024)
            .map(|value| (value % 3 != 0).then(|| format!("value-{value}")))
            .collect::<Vec<_>>();
        let binary = (0_u32..1024)
            .map(|value| (value % 3 != 0).then(|| value.to_le_bytes().to_vec()))
            .collect::<Vec<_>>();
        let columns: Vec<VectorRef> = vec![
            Arc::new(UInt32Vector::from_slice(numbers)),
            Arc::new(StringVector::from(strings)),
            Arc::new(BinaryVector::from(binary)),
        ];
        let batch = RecordBatch::new(schema, columns).unwrap();
        let slice = batch.slice(511, 3).unwrap();

        assert!(slice.columns().iter().any(|column| column.null_count() > 0));
        assert!(slice.logical_slice_memory_size() < slice.buffer_memory_size());
        assert_eq!(
            slice.logical_slice_memory_size(),
            slice
                .columns()
                .iter()
                .map(|column| column.to_data().get_slice_memory_size().unwrap())
                .sum::<usize>()
        );
    }

    #[test]
    fn test_logical_slice_memory_size_for_many_shared_buffer_slices() {
        let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
            "strings",
            ConcreteDataType::string_datatype(),
            false,
        )]));
        let strings = (0..1024)
            .map(|value| format!("shared-value-{value}"))
            .collect::<Vec<_>>();
        let backing = RecordBatch::new(
            schema,
            vec![Arc::new(StringVector::from(strings)) as VectorRef],
        )
        .unwrap();
        let slices = (0..128)
            .map(|index| backing.slice(index * 4, 2).unwrap())
            .collect::<Vec<_>>();
        let logical_total = slices
            .iter()
            .map(RecordBatch::logical_slice_memory_size)
            .sum::<usize>();
        let buffer_total = slices
            .iter()
            .map(RecordBatch::buffer_memory_size)
            .sum::<usize>();

        assert!(logical_total < buffer_total);
        assert!(slices.iter().all(|slice| {
            slice.logical_slice_memory_size()
                == slice.column(0).to_data().get_slice_memory_size().unwrap()
        }));
    }

    #[test]
    fn test_logical_slice_memory_size_uses_arrow_slice_scope_for_views() {
        let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
            "strings",
            ConcreteDataType::utf8_view_datatype(),
            false,
        )]));
        let columns: Vec<VectorRef> =
            vec![Arc::new(StringVector::from(StringViewArray::from(vec![
                "unrelated backing payload",
                "visible string view payload",
            ])))];
        let batch = RecordBatch::new(schema, columns)
            .unwrap()
            .slice(1, 1)
            .unwrap();

        assert_eq!(
            batch.column(0).to_data().get_slice_memory_size().unwrap(),
            batch.logical_slice_memory_size()
        );
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

    #[test]
    fn test_legacy_json_with_extension_does_not_align_as_structured_json() {
        let field = Field::new("j", DataType::Binary, true).with_extension_type(JsonExtensionType);
        let arrow_schema = Arc::new(ArrowSchema::new(vec![field]));
        let schema = Arc::new(Schema::try_from(arrow_schema).unwrap());
        let columns: Vec<VectorRef> = vec![Arc::new(BinaryVector::from(vec![Some(
            br#"{"a":1}"#.to_vec(),
        )]))];
        let batch = RecordBatch::new(schema, columns).unwrap();
        assert_eq!(batch.column(0).data_type(), &DataType::Binary);
    }
}
