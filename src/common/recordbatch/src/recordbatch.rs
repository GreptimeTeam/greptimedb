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
use std::mem::size_of;
use std::slice;
use std::sync::Arc;

use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion_common::arrow::array::ArrayRef;
use datafusion_common::arrow::compute;
use datafusion_common::arrow::datatypes::{DataType as ArrowDataType, SchemaRef as ArrowSchemaRef};
use datatypes::arrow::array::{Array, AsArray, OffsetSizeTrait, RecordBatchOptions};
use datatypes::arrow::util::bit_util;
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

    /// Returns the logical memory size of this batch's array slices.
    ///
    /// This sums Arrow's logical visible slice buffers rather than the capacity of their shared
    /// backing buffers. View out-of-line payloads and nested custom payloads are not separately
    /// traversed or accounted. It is not an exact measure of live physical memory. If Arrow cannot
    /// calculate a slice's size, the full buffer size is used conservatively.
    ///
    /// Flat columns (fixed-width primitives, timestamps, booleans, and `Utf8`/`LargeUtf8`/
    /// `Binary`/`LargeBinary`) are measured directly from their typed arrays using Arrow's own
    /// formula (see [`flat_slice_memory_size`]), so the scan hot path does not materialize an
    /// `ArrayData` per column per batch. All other types use Arrow's calculation.
    ///
    /// Mito's current scan paths do not produce top-level View arrays. If they do in the future,
    /// their out-of-line payload accounting must be reassessed here.
    pub fn logical_slice_memory_size(&self) -> usize {
        self.df_record_batch
            .columns()
            .iter()
            .fold(0, |total, array| {
                total.saturating_add(logical_column_memory_size(array))
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

/// Returns Arrow's logical slice memory size of a single column.
///
/// This is `array.to_data().get_slice_memory_size()`, with the full buffer size as a conservative
/// fallback when Arrow cannot calculate the slice size. Flat columns are measured from their typed
/// array (see [`flat_slice_memory_size`]) so that no `ArrayData` is materialized for them.
fn logical_column_memory_size(array: &dyn Array) -> usize {
    flat_slice_memory_size(array).unwrap_or_else(|| {
        array
            .to_data()
            .get_slice_memory_size()
            .unwrap_or_else(|_| array.get_buffer_memory_size())
    })
}

/// Computes `array.to_data().get_slice_memory_size()` for flat arrays without building the
/// `ArrayData`.
///
/// The formulas mirror Arrow's `ArrayData::get_slice_memory_size` (arrow-data 59.2.0,
/// `get_slice_memory_size` in `arrow-data/src/data.rs`):
///
/// - a fixed-width value buffer costs `len * byte_width`, where the byte width is the one of the
///   type's buffer layout, as reported by `DataType::primitive_width`,
/// - the value bitmap of a boolean array costs `ceil(len / 8)`,
/// - the offsets buffer of a variable-width array holds `len + 1` boundaries and costs
///   `(len + 1) * offset_width`,
/// - the visible value bytes of a variable-width array cost `offset[len] - offset[0]`,
/// - the validity bitmap costs `ceil(len / 8)`, but only if the slice contains a null: converting
///   an array to `ArrayData` drops a validity bitmap without any null (`ArrayDataBuilder::build`),
///   so Arrow only sees one when `null_count` is non-zero.
///
/// Returns `None` for data types that are left to Arrow's general computation (nested, view,
/// dictionary, union, `FixedSizeBinary`, `Null`, ...) and when the size computation overflows
/// `usize`, which Arrow reports as an error. Callers must then fall back to Arrow.
fn flat_slice_memory_size(array: &dyn Array) -> Option<usize> {
    let len = array.len();
    let validity_size = if array.null_count() != 0 {
        bit_util::ceil(len, 8)
    } else {
        0
    };

    match array.data_type() {
        ArrowDataType::Boolean => bit_util::ceil(len, 8).checked_add(validity_size),
        ArrowDataType::Utf8 => {
            offset_slice_memory_size(len, array.as_string::<i32>().offsets(), validity_size)
        }
        ArrowDataType::LargeUtf8 => {
            offset_slice_memory_size(len, array.as_string::<i64>().offsets(), validity_size)
        }
        ArrowDataType::Binary => {
            offset_slice_memory_size(len, array.as_binary::<i32>().offsets(), validity_size)
        }
        ArrowDataType::LargeBinary => {
            offset_slice_memory_size(len, array.as_binary::<i64>().offsets(), validity_size)
        }
        data_type => data_type
            .primitive_width()
            .and_then(|byte_width| fixed_width_slice_memory_size(len, byte_width, validity_size)),
    }
}

/// Returns `len * byte_width + validity_size`, or `None` on overflow.
#[inline]
fn fixed_width_slice_memory_size(
    len: usize,
    byte_width: usize,
    validity_size: usize,
) -> Option<usize> {
    len.checked_mul(byte_width)?.checked_add(validity_size)
}

/// Returns `(len + 1) * offset_width + (offsets[len] - offsets[0]) + validity_size`, or `None` on
/// overflow.
///
/// `offsets` are the offsets of the visible slice, so `offsets[0]` is the first visible byte and
/// `offsets[len]` is the end of the last visible value. The offsets buffer size is checked before
/// `offsets[len]` is read, so a `len` that cannot come from a valid offsets buffer is reported as
/// an overflow instead of panicking.
#[inline]
fn offset_slice_memory_size<O: OffsetSizeTrait>(
    len: usize,
    offsets: &[O],
    validity_size: usize,
) -> Option<usize> {
    let offsets_size = len.checked_add(1)?.checked_mul(size_of::<O>())?;
    let values_size = (offsets[len] - offsets[0]).as_usize();
    offsets_size
        .checked_add(values_size)?
        .checked_add(validity_size)
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

    use datatypes::arrow::array::{
        ArrayRef, AsArray, BinaryArray, BooleanArray, DictionaryArray, FixedSizeBinaryArray,
        Int32Array, LargeBinaryArray, LargeStringArray, ListArray, PrimitiveArray, StringArray,
        StringViewArray, TimestampMillisecondArray, UInt32Array,
    };
    use datatypes::arrow::buffer::{NullBuffer, ScalarBuffer};
    use datatypes::arrow::datatypes::{
        ArrowNativeType, ArrowPrimitiveType, DataType, Date32Type, Date64Type, Decimal128Type,
        Decimal256Type, DurationMicrosecondType, Field, Float32Type, Float64Type, Int8Type,
        Int16Type, Int32Type, Int64Type, IntervalDayTimeType, IntervalMonthDayNanoType,
        IntervalYearMonthType, Schema as ArrowSchema, Time32MillisecondType, Time64NanosecondType,
        TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
        TimestampSecondType, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
    };
    use datatypes::data_type::ConcreteDataType;
    use datatypes::extension::json::JsonExtensionType;
    use datatypes::schema::{ColumnSchema, Schema};
    use datatypes::vectors::{
        BinaryVector, BooleanVector, StringVector, UInt32Vector, UInt64Vector,
    };

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

    /// Arrow's own computation for a single array, the reference for the fast path.
    fn arrow_slice_memory_size(array: &dyn Array) -> usize {
        array
            .to_data()
            .get_slice_memory_size()
            .unwrap_or_else(|_| array.get_buffer_memory_size())
    }

    /// Slices probed for every array: the whole array, empty slices at both ends, single element
    /// slices, a non byte aligned range, and slices that skip a prefix or a suffix.
    fn probe_ranges(len: usize) -> Vec<(usize, usize)> {
        if len == 0 {
            return vec![(0, 0)];
        }

        let mut ranges = vec![
            (0, 0),
            (len, 0),
            (0, 1),
            (len - 1, 1),
            (1, len.saturating_sub(1)),
            (len / 3, len / 3),
        ];
        if len >= 8 {
            ranges.push((3, 5));
        }
        ranges
    }

    /// Asserts the fast path covers `array` and agrees with Arrow's own formula.
    #[track_caller]
    fn assert_fast_path_matches_arrow(name: &str, array: &dyn Array) {
        let expected = arrow_slice_memory_size(array);
        let Some(actual) = flat_slice_memory_size(array) else {
            panic!("{name}: fast path must cover {:?}", array.data_type());
        };

        assert_eq!(
            expected,
            actual,
            "{name}: fast path disagrees with Arrow for {:?} (len {}, null count {})",
            array.data_type(),
            array.len(),
            array.null_count()
        );
        assert_eq!(
            expected,
            logical_column_memory_size(array),
            "{name}: column size disagrees with Arrow for {:?}",
            array.data_type()
        );
    }

    /// Asserts the fast path agrees with Arrow for `array` and all probe slices of it.
    fn assert_fast_path_matches_arrow_on_slices(name: &str, array: &ArrayRef) {
        assert_fast_path_matches_arrow(name, array.as_ref());

        for (offset, len) in probe_ranges(array.len()) {
            let slice = array.slice(offset, len);
            assert_fast_path_matches_arrow(
                &format!("{name}[{offset}..{}]", offset + len),
                slice.as_ref(),
            );
        }
    }

    /// Returns a full array, an array with a few nulls and an all-null array of `len` values.
    fn primitive_cases<T: ArrowPrimitiveType>(len: usize) -> Vec<ArrayRef> {
        let values = (0..len)
            .map(|index| Some(T::Native::usize_as(index)))
            .collect::<PrimitiveArray<T>>();
        let with_nulls = (0..len)
            .map(|index| (index % 3 != 0).then(|| T::Native::usize_as(index)))
            .collect::<PrimitiveArray<T>>();
        let all_null = (0..len)
            .map(|_| None::<T::Native>)
            .collect::<PrimitiveArray<T>>();

        vec![
            Arc::new(values) as ArrayRef,
            Arc::new(with_nulls),
            Arc::new(all_null),
        ]
    }

    #[test]
    fn test_flat_slice_memory_size_matches_arrow_for_fixed_width_types() {
        macro_rules! check {
            ($($ty:ty),* $(,)?) => {
                $(
                    for (index, array) in primitive_cases::<$ty>(13).into_iter().enumerate() {
                        assert_fast_path_matches_arrow_on_slices(
                            &format!("{}-{index}", stringify!($ty)),
                            &array,
                        );
                    }
                )*
            };
        }

        check!(
            Int8Type,
            Int16Type,
            Int32Type,
            Int64Type,
            UInt8Type,
            UInt16Type,
            UInt32Type,
            UInt64Type,
            Float32Type,
            Float64Type,
            Date32Type,
            Date64Type,
            Time32MillisecondType,
            Time64NanosecondType,
            TimestampSecondType,
            TimestampMillisecondType,
            TimestampMicrosecondType,
            TimestampNanosecondType,
            DurationMicrosecondType,
            IntervalYearMonthType,
            IntervalDayTimeType,
            IntervalMonthDayNanoType,
            Decimal128Type,
            Decimal256Type,
        );

        // Guard against a vacuous differential test: `len` values cost `len * width`, plus one
        // bitmap byte per 8 values when the array is nullable.
        let len = 13;
        let plain = Int32Array::from((0..len as i32).collect::<Vec<_>>());
        assert_eq!(Some(len * 4), flat_slice_memory_size(&plain));
        let nullable = Int32Array::from(
            (0..len)
                .map(|index| (index % 2 == 0).then_some(index as i32))
                .collect::<Vec<_>>(),
        );
        assert_eq!(
            Some(len * 4 + bit_util::ceil(len, 8)),
            flat_slice_memory_size(&nullable)
        );

        // Timestamps with a timezone share the `Int64` layout.
        let with_timezone =
            Arc::new(TimestampMillisecondArray::from(vec![1_i64, 2, 3]).with_timezone_utc())
                as ArrayRef;
        assert_fast_path_matches_arrow_on_slices("timestamp-utc", &with_timezone);
    }

    #[test]
    fn test_flat_slice_memory_size_matches_arrow_for_byte_arrays() {
        // Empty, short (inline) and long (out of line) values, plus nulls.
        let strings: Vec<Option<&str>> = vec![
            Some(""),
            Some("a"),
            Some("greptime"),
            Some("a value that is much longer than twelve bytes"),
            None,
            Some(""),
            Some("z"),
            Some("another long value that exceeds the inline limit"),
            None,
        ];
        let bytes: Vec<Option<&[u8]>> = strings
            .iter()
            .map(|value| value.map(str::as_bytes))
            .collect();

        let cases: Vec<(&str, ArrayRef)> = vec![
            ("utf8", Arc::new(StringArray::from(strings.clone()))),
            (
                "large-utf8",
                Arc::new(LargeStringArray::from(strings.clone())),
            ),
            ("binary", Arc::new(BinaryArray::from(bytes.clone()))),
            ("large-binary", Arc::new(LargeBinaryArray::from(bytes))),
        ];

        for (name, array) in cases {
            assert_fast_path_matches_arrow_on_slices(name, &array);

            // `(len + 1) * offset_width + visible value bytes`, plus the validity bitmap.
            let len = array.len();
            let offset_width = match array.data_type() {
                DataType::Utf8 | DataType::Binary => 4,
                DataType::LargeUtf8 | DataType::LargeBinary => 8,
                other => panic!("unexpected type {other}"),
            };
            let visible: usize = strings.iter().map(|value| value.map_or(0, str::len)).sum();
            let mut expected = (len + 1) * offset_width + visible;
            if array.null_count() != 0 {
                expected += bit_util::ceil(len, 8);
            }
            assert_eq!(
                Some(expected),
                flat_slice_memory_size(array.as_ref()),
                "{name}"
            );
        }

        // Zero rows keep one offset boundary per array, and nothing else.
        let empty = Arc::new(StringArray::from(Vec::<Option<&str>>::new())) as ArrayRef;
        assert_eq!(Some(4), flat_slice_memory_size(empty.as_ref()));
        assert_eq!(4, arrow_slice_memory_size(empty.as_ref()));
        assert_fast_path_matches_arrow("utf8-empty", empty.as_ref());

        let empty_large = Arc::new(LargeStringArray::from(Vec::<Option<&str>>::new())) as ArrayRef;
        assert_eq!(Some(8), flat_slice_memory_size(empty_large.as_ref()));
        assert_fast_path_matches_arrow("large-utf8-empty", empty_large.as_ref());
    }

    #[test]
    fn test_flat_slice_memory_size_matches_arrow_for_boolean_bitmaps() {
        // Lengths around byte boundaries of the value bitmap.
        for len in [0_usize, 1, 7, 8, 9, 15, 16, 17, 33] {
            let plain =
                BooleanArray::from((0..len).map(|index| index % 3 == 0).collect::<Vec<_>>());
            let nullable = BooleanArray::from(
                (0..len)
                    .map(|index| (index % 4 != 0).then_some(index % 3 == 0))
                    .collect::<Vec<_>>(),
            );
            let all_null = BooleanArray::from(vec![None::<bool>; len]);

            let cases: Vec<(&str, ArrayRef)> = vec![
                ("plain", Arc::new(plain)),
                ("nullable", Arc::new(nullable)),
                ("all-null", Arc::new(all_null)),
            ];

            for (kind, array) in cases {
                let name = format!("boolean-{kind}-{len}");
                assert_fast_path_matches_arrow_on_slices(&name, &array);

                // A value bitmap, plus a validity bitmap when the slice contains a null.
                let bitmaps = if array.null_count() != 0 { 2 } else { 1 };
                assert_eq!(
                    Some(bit_util::ceil(len, 8) * bitmaps),
                    flat_slice_memory_size(array.as_ref()),
                    "{name}"
                );
            }
        }
    }

    #[test]
    fn test_flat_slice_memory_size_ignores_null_free_validity_bitmap() {
        // A validity bitmap without any null value is free: `ArrayDataBuilder::build` drops it, so
        // Arrow's formula does not charge for it even though the physical buffer exists.
        let array = Arc::new(Int32Array::new(
            ScalarBuffer::from(vec![1_i32, 2, 3, 4, 5]),
            Some(NullBuffer::new_valid(5)),
        )) as ArrayRef;

        assert_eq!(0, array.null_count());
        assert!(array.nulls().is_some());
        assert_eq!(Some(5 * 4), flat_slice_memory_size(array.as_ref()));
        assert_fast_path_matches_arrow_on_slices("int32-validity-only", &array);
    }

    #[test]
    fn test_flat_slice_memory_size_matches_arrow_for_zero_rows() {
        let schema = Arc::new(Schema::new(vec![
            ColumnSchema::new("numbers", ConcreteDataType::uint64_datatype(), true),
            ColumnSchema::new("flags", ConcreteDataType::boolean_datatype(), true),
            ColumnSchema::new("strings", ConcreteDataType::string_datatype(), true),
            ColumnSchema::new("binary", ConcreteDataType::binary_datatype(), true),
        ]));
        let columns: Vec<VectorRef> = vec![
            Arc::new(UInt64Vector::from_slice([1_u64, 2, 3])),
            Arc::new(BooleanVector::from(vec![Some(true), None, Some(false)])),
            Arc::new(StringVector::from(vec![Some("a"), None, Some("ccc")])),
            Arc::new(BinaryVector::from(vec![
                Some(vec![1_u8]),
                None,
                Some(vec![2_u8, 3]),
            ])),
        ];
        let batch = RecordBatch::new(schema, columns).unwrap();
        let empty = batch.slice(0, 0).unwrap();

        assert_eq!(0, empty.num_rows());
        for (index, column) in empty.columns().iter().enumerate() {
            assert_fast_path_matches_arrow(&format!("empty-column-{index}"), column.as_ref());
        }

        let expected: usize = empty
            .columns()
            .iter()
            .map(|column| arrow_slice_memory_size(column.as_ref()))
            .sum();
        assert_eq!(expected, empty.logical_slice_memory_size());
        // Only the offsets of the string and binary columns survive.
        assert_eq!(4 + 4, empty.logical_slice_memory_size());
    }

    #[test]
    fn test_flat_slice_memory_size_for_many_slices_of_shared_backing() {
        let schema = Arc::new(Schema::new(vec![
            ColumnSchema::new("strings", ConcreteDataType::string_datatype(), false),
            ColumnSchema::new("numbers", ConcreteDataType::uint64_datatype(), false),
            ColumnSchema::new("flags", ConcreteDataType::boolean_datatype(), false),
        ]));
        let strings = (0..1024)
            .map(|value| format!("shared-value-{value}"))
            .collect::<Vec<_>>();
        let numbers = (0_u64..1024).collect::<Vec<_>>();
        let flags = (0..1024).map(|value| value % 3 == 0).collect::<Vec<_>>();
        let backing = RecordBatch::new(
            schema,
            vec![
                Arc::new(StringVector::from(strings)) as VectorRef,
                Arc::new(UInt64Vector::from_slice(numbers)),
                Arc::new(BooleanVector::from(flags)),
            ],
        )
        .unwrap();

        let slices = (0..128)
            .map(|index| backing.slice(index * 4, 3).unwrap())
            .collect::<Vec<_>>();

        for slice in &slices {
            for column in slice.columns() {
                assert_fast_path_matches_arrow("shared-backing", column.as_ref());
            }
            let expected: usize = slice
                .columns()
                .iter()
                .map(|column| arrow_slice_memory_size(column.as_ref()))
                .sum();
            assert_eq!(expected, slice.logical_slice_memory_size());
        }

        let logical_total = slices
            .iter()
            .map(RecordBatch::logical_slice_memory_size)
            .sum::<usize>();
        let buffer_total = slices
            .iter()
            .map(RecordBatch::buffer_memory_size)
            .sum::<usize>();
        assert!(logical_total < buffer_total);
    }

    #[test]
    fn test_logical_slice_memory_size_matches_arrow_for_mixed_slices() {
        let schema = Arc::new(Schema::new(vec![
            ColumnSchema::new("numbers", ConcreteDataType::uint32_datatype(), false),
            ColumnSchema::new("flags", ConcreteDataType::boolean_datatype(), true),
            ColumnSchema::new("strings", ConcreteDataType::string_datatype(), true),
            ColumnSchema::new("binary", ConcreteDataType::binary_datatype(), true),
        ]));
        let numbers: Vec<u32> = (0..512).collect();
        let flags = (0..512)
            .map(|value| (value % 5 != 0).then_some(value % 3 == 0))
            .collect::<Vec<_>>();
        let strings = (0..512)
            .map(|value| (value % 4 != 0).then(|| format!("value-{value}")))
            .collect::<Vec<_>>();
        let binary = (0..512_u32)
            .map(|value| (value % 4 != 0).then(|| value.to_le_bytes().to_vec()))
            .collect::<Vec<_>>();
        let batch = RecordBatch::new(
            schema,
            vec![
                Arc::new(UInt32Vector::from_slice(numbers)) as VectorRef,
                Arc::new(BooleanVector::from(flags)),
                Arc::new(StringVector::from(strings)),
                Arc::new(BinaryVector::from(binary)),
            ],
        )
        .unwrap();

        for (offset, len) in [(0, 512), (0, 0), (511, 1), (1, 511), (3, 5), (256, 3)] {
            let slice = batch.slice(offset, len).unwrap();

            for column in slice.columns() {
                assert_fast_path_matches_arrow("mixed-slice", column.as_ref());
            }
            let expected: usize = slice
                .columns()
                .iter()
                .map(|column| arrow_slice_memory_size(column.as_ref()))
                .sum();
            assert_eq!(
                expected,
                slice.logical_slice_memory_size(),
                "slice({offset}, {len})"
            );
        }
    }

    #[test]
    fn test_flat_slice_memory_size_declines_on_overflow() {
        // `len * byte_width` overflows `usize`, Arrow reports an error for it.
        assert_eq!(
            None,
            fixed_width_slice_memory_size(usize::MAX / 8 + 1, 8, 0)
        );
        // The validity bitmap pushes the total over the edge.
        assert_eq!(None, fixed_width_slice_memory_size(usize::MAX, 1, 8));
        // `(len + 1) * offset_width` overflows `usize`.
        assert_eq!(
            None,
            offset_slice_memory_size::<i32>(usize::MAX, &[0, 0], 0)
        );
        // The visible value bytes push the total over the edge.
        assert_eq!(
            None,
            offset_slice_memory_size::<i32>(1, &[0, i32::MAX], usize::MAX)
        );
        // In range sizes are still computed: `(len + 1) * 4` offsets plus `offsets[2] - offsets[0]`
        // visible value bytes.
        assert_eq!(Some(16), fixed_width_slice_memory_size(2, 8, 0));
        assert_eq!(
            Some(3 * 4 + 3),
            offset_slice_memory_size::<i32>(2, &[7, 9, 10], 0)
        );
    }

    #[test]
    fn test_logical_column_memory_size_falls_back_for_other_types() {
        let view = StringViewArray::from(vec![
            "a short view",
            "a much longer string view value that is stored out of line",
            "",
        ]);
        let dictionary = DictionaryArray::<Int32Type>::from_iter(vec![
            Some("first"),
            None,
            Some("a longer dictionary value"),
        ]);
        let list = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1), Some(2), None]),
            None,
            Some(vec![Some(3)]),
        ]);
        let first: [u8; 3] = [1, 2, 3];
        let second: [u8; 3] = [4, 5, 6];
        let fixed_size_binary = FixedSizeBinaryArray::try_from(vec![
            Some(first.as_slice()),
            None::<&[u8]>,
            Some(second.as_slice()),
        ])
        .unwrap();

        let cases: Vec<(&str, ArrayRef)> = vec![
            ("utf8-view", Arc::new(view)),
            ("dictionary", Arc::new(dictionary)),
            ("list", Arc::new(list)),
            ("fixed-size-binary", Arc::new(fixed_size_binary)),
        ];

        for (name, array) in cases {
            assert_eq!(
                None,
                flat_slice_memory_size(array.as_ref()),
                "{name}: expected Arrow's fallback for {:?}",
                array.data_type()
            );

            for (offset, len) in probe_ranges(array.len()) {
                let slice = array.slice(offset, len);
                assert_eq!(
                    None,
                    flat_slice_memory_size(slice.as_ref()),
                    "{name}[{offset}..{}]: expected Arrow's fallback",
                    offset + len
                );
                assert_eq!(
                    arrow_slice_memory_size(slice.as_ref()),
                    logical_column_memory_size(slice.as_ref()),
                    "{name}[{offset}..{}]: fallback must match Arrow",
                    offset + len
                );
            }
        }
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
