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

//! Format to store in parquet.
//!
//! We store three internal columns in parquet:
//! - `__primary_key`, the primary key of the row (tags). Type: dictionary(uint16, binary)
//! - `__sequence`, the sequence number of a row. Type: uint64
//! - `__op_type`, the op type of the row. Type: uint8
//!
//! The schema of a parquet file is:
//! ```text
//! field 0, field 1, ..., field N, time index, primary key, sequence, op type
//! ```
//!
//! We stores fields in the same order as [RegionMetadata::field_columns()](store_api::metadata::RegionMetadata::field_columns()).

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use api::v1::SemanticType;
use datafusion_common::ScalarValue;
use datatypes::arrow::array::{ArrayRef, BinaryArray, DictionaryArray, UInt16Array, UInt64Array};
use datatypes::arrow::datatypes::{
    DataType as ArrowDataType, Field, FieldRef, Fields, Schema, SchemaRef, UInt16Type,
};
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::prelude::DataType;
use datatypes::vectors::{Helper, Vector};
use parquet::file::metadata::RowGroupMetaData;
use parquet::file::statistics::Statistics;
use snafu::{ensure, OptionExt, ResultExt};
use store_api::metadata::{ColumnMetadata, RegionMetadata, RegionMetadataRef};
use store_api::storage::consts::{
    OP_TYPE_COLUMN_NAME, PRIMARY_KEY_COLUMN_NAME, SEQUENCE_COLUMN_NAME,
};
use store_api::storage::ColumnId;

use crate::error::{
    ConvertVectorSnafu, InvalidBatchSnafu, InvalidRecordBatchSnafu, NewRecordBatchSnafu, Result,
};
use crate::read::{Batch, BatchBuilder, BatchColumn};
use crate::row_converter::{McmpRowCodec, RowCodec, SortField};

/// Number of columns that have fixed positions.
///
/// Contains: time index and internal columns.
const FIXED_POS_COLUMN_NUM: usize = 4;

/// Helper for writing the SST format.
pub(crate) struct WriteFormat {
    metadata: RegionMetadataRef,
    /// SST file schema.
    arrow_schema: SchemaRef,
}

impl WriteFormat {
    /// Creates a new helper.
    pub(crate) fn new(metadata: RegionMetadataRef) -> WriteFormat {
        let arrow_schema = to_sst_arrow_schema(&metadata);
        WriteFormat {
            metadata,
            arrow_schema,
        }
    }

    /// Gets the arrow schema to store in parquet.
    pub(crate) fn arrow_schema(&self) -> SchemaRef {
        self.arrow_schema.clone()
    }

    /// Convert `batch` to a arrow record batch to store in parquet.
    pub(crate) fn convert_batch(&self, batch: &Batch) -> Result<RecordBatch> {
        debug_assert_eq!(
            batch.fields().len() + FIXED_POS_COLUMN_NUM,
            self.arrow_schema.fields().len()
        );
        let mut columns = Vec::with_capacity(batch.fields().len() + FIXED_POS_COLUMN_NUM);
        // Store all fields first.
        for (column, column_metadata) in batch.fields().iter().zip(self.metadata.field_columns()) {
            ensure!(
                column.column_id == column_metadata.column_id,
                InvalidBatchSnafu {
                    reason: format!(
                        "Batch has column {} but metadata has column {}",
                        column.column_id, column_metadata.column_id
                    ),
                }
            );

            columns.push(column.data.to_arrow_array());
        }
        // Add time index column.
        columns.push(batch.timestamps().to_arrow_array());
        // Add internal columns: primary key, sequences, op types.
        columns.push(new_primary_key_array(batch.primary_key(), batch.num_rows()));
        columns.push(batch.sequences().to_arrow_array());
        columns.push(batch.op_types().to_arrow_array());

        RecordBatch::try_new(self.arrow_schema.clone(), columns).context(NewRecordBatchSnafu)
    }
}

/// Helper for reading the SST format.
pub(crate) struct ReadFormat {
    metadata: RegionMetadataRef,
    /// SST file schema.
    arrow_schema: SchemaRef,
    // Field column id to its index in `schema` (SST schema).
    field_id_to_index: HashMap<ColumnId, usize>,
}

impl ReadFormat {
    /// Creates a helper with existing `metadata`.
    pub(crate) fn new(metadata: RegionMetadataRef) -> ReadFormat {
        let field_id_to_index: HashMap<_, _> = metadata
            .field_columns()
            .enumerate()
            .map(|(index, column)| (column.column_id, index))
            .collect();
        let arrow_schema = to_sst_arrow_schema(&metadata);

        ReadFormat {
            metadata,
            arrow_schema,
            field_id_to_index,
        }
    }

    /// Gets the arrow schema of the SST file.
    ///
    /// This schema is computed from the region metadata but should be the same
    /// as the arrow schema decoded from the file metadata.
    pub(crate) fn arrow_schema(&self) -> &SchemaRef {
        &self.arrow_schema
    }

    /// Gets the metadata of the SST.
    pub(crate) fn metadata(&self) -> &RegionMetadataRef {
        &self.metadata
    }

    /// Gets sorted projection indices to read `columns` from parquet files.
    ///
    /// This function ignores columns not in `metadata` to for compatibility between
    /// different schemas.
    pub(crate) fn projection_indices(
        &self,
        columns: impl IntoIterator<Item = ColumnId>,
    ) -> Vec<usize> {
        let mut indices: Vec<_> = columns
            .into_iter()
            .filter_map(|column_id| {
                // Only apply projection to fields.
                self.field_id_to_index.get(&column_id).copied()
            })
            // We need to add all fixed position columns.
            .chain(
                self.arrow_schema.fields.len() - FIXED_POS_COLUMN_NUM
                    ..self.arrow_schema.fields.len(),
            )
            .collect();
        indices.sort_unstable();
        indices
    }

    /// Convert a arrow record batch into `batches`.
    ///
    /// Note that the `record_batch` may only contains a subset of columns if it is projected.
    pub(crate) fn convert_record_batch(
        &self,
        record_batch: &RecordBatch,
        batches: &mut VecDeque<Batch>,
    ) -> Result<()> {
        debug_assert!(batches.is_empty());

        // The record batch must has time index and internal columns.
        ensure!(
            record_batch.num_columns() >= FIXED_POS_COLUMN_NUM,
            InvalidRecordBatchSnafu {
                reason: format!(
                    "record batch only has {} columns",
                    record_batch.num_columns()
                ),
            }
        );

        let mut fixed_pos_columns = record_batch
            .columns()
            .iter()
            .rev()
            .take(FIXED_POS_COLUMN_NUM);
        // Safety: We have checked the column number.
        let op_type_array = fixed_pos_columns.next().unwrap();
        let sequence_array = fixed_pos_columns.next().unwrap();
        let pk_array = fixed_pos_columns.next().unwrap();
        let ts_array = fixed_pos_columns.next().unwrap();
        let field_batch_columns = self.get_field_batch_columns(record_batch)?;

        // Compute primary key offsets.
        let pk_dict_array = pk_array
            .as_any()
            .downcast_ref::<DictionaryArray<UInt16Type>>()
            .with_context(|| InvalidRecordBatchSnafu {
                reason: format!("primary key array should not be {:?}", pk_array.data_type()),
            })?;
        let offsets = primary_key_offsets(pk_dict_array)?;
        if offsets.is_empty() {
            return Ok(());
        }

        // Split record batch according to pk offsets.
        let keys = pk_dict_array.keys();
        let pk_values = pk_dict_array
            .values()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .with_context(|| InvalidRecordBatchSnafu {
                reason: format!(
                    "values of primary key array should not be {:?}",
                    pk_dict_array.values().data_type()
                ),
            })?;
        for (i, start) in offsets[..offsets.len() - 1].iter().enumerate() {
            let end = offsets[i + 1];
            let rows_in_batch = end - start;
            let dict_key = keys.value(*start);
            let primary_key = pk_values.value(dict_key.into()).to_vec();

            let mut builder = BatchBuilder::new(primary_key);
            builder
                .timestamps_array(ts_array.slice(*start, rows_in_batch))?
                .sequences_array(sequence_array.slice(*start, rows_in_batch))?
                .op_types_array(op_type_array.slice(*start, rows_in_batch))?;
            // Push all fields
            for batch_column in &field_batch_columns {
                builder.push_field(BatchColumn {
                    column_id: batch_column.column_id,
                    data: batch_column.data.slice(*start, rows_in_batch),
                });
            }

            let batch = builder.build()?;
            batches.push_back(batch);
        }

        Ok(())
    }

    /// Returns min values of specific column in row groups.
    pub(crate) fn min_values(
        &self,
        row_groups: &[RowGroupMetaData],
        column_id: ColumnId,
    ) -> Option<ArrayRef> {
        let column = self.metadata.column_by_id(column_id)?;
        match column.semantic_type {
            SemanticType::Tag => self.tag_values(row_groups, column, true),
            SemanticType::Field => {
                let index = self.field_id_to_index.get(&column_id)?;
                Self::column_values(row_groups, column, *index, true)
            }
            SemanticType::Timestamp => {
                let index = self.time_index_position();
                Self::column_values(row_groups, column, index, true)
            }
        }
    }

    /// Returns max values of specific column in row groups.
    pub(crate) fn max_values(
        &self,
        row_groups: &[RowGroupMetaData],
        column_id: ColumnId,
    ) -> Option<ArrayRef> {
        let column = self.metadata.column_by_id(column_id)?;
        match column.semantic_type {
            SemanticType::Tag => self.tag_values(row_groups, column, false),
            SemanticType::Field => {
                let index = self.field_id_to_index.get(&column_id)?;
                Self::column_values(row_groups, column, *index, false)
            }
            SemanticType::Timestamp => {
                let index = self.time_index_position();
                Self::column_values(row_groups, column, index, false)
            }
        }
    }

    /// Returns null counts of specific column in row groups.
    pub(crate) fn null_counts(
        &self,
        row_groups: &[RowGroupMetaData],
        column_id: ColumnId,
    ) -> Option<ArrayRef> {
        let column = self.metadata.column_by_id(column_id)?;
        match column.semantic_type {
            SemanticType::Tag => None,
            SemanticType::Field => {
                let index = self.field_id_to_index.get(&column_id)?;
                Self::column_null_counts(row_groups, *index)
            }
            SemanticType::Timestamp => {
                let index = self.time_index_position();
                Self::column_null_counts(row_groups, index)
            }
        }
    }

    /// Get fields from `record_batch`.
    fn get_field_batch_columns(&self, record_batch: &RecordBatch) -> Result<Vec<BatchColumn>> {
        record_batch
            .columns()
            .iter()
            .zip(record_batch.schema().fields())
            .take(record_batch.num_columns() - FIXED_POS_COLUMN_NUM) // Take all field columns.
            .map(|(array, field)| {
                let vector = Helper::try_into_vector(array.clone()).context(ConvertVectorSnafu)?;
                let column = self
                    .metadata
                    .column_by_name(field.name())
                    .with_context(|| InvalidRecordBatchSnafu {
                        reason: format!("column {} not found in metadata", field.name()),
                    })?;

                Ok(BatchColumn {
                    column_id: column.column_id,
                    data: vector,
                })
            })
            .collect()
    }

    /// Returns min/max values of specific tag.
    fn tag_values(
        &self,
        row_groups: &[RowGroupMetaData],
        column: &ColumnMetadata,
        is_min: bool,
    ) -> Option<ArrayRef> {
        let is_first_tag = self
            .metadata
            .primary_key
            .first()
            .map(|id| *id == column.column_id)
            .unwrap_or(false);
        if !is_first_tag {
            // Only the min-max of the first tag is available in the primary key.
            return None;
        }

        let converter =
            McmpRowCodec::new(vec![SortField::new(column.column_schema.data_type.clone())]);
        let values = row_groups.iter().map(|meta| {
            let stats = meta.column(self.primary_key_position()).statistics()?;
            if !stats.has_min_max_set() {
                return None;
            }
            match stats {
                Statistics::Boolean(_) => None,
                Statistics::Int32(_) => None,
                Statistics::Int64(_) => None,
                Statistics::Int96(_) => None,
                Statistics::Float(_) => None,
                Statistics::Double(_) => None,
                Statistics::ByteArray(s) => {
                    let bytes = if is_min { s.min_bytes() } else { s.max_bytes() };
                    let mut values = converter.decode(bytes).ok()?;
                    values.pop()
                }
                Statistics::FixedLenByteArray(_) => None,
            }
        });
        let mut builder = column
            .column_schema
            .data_type
            .create_mutable_vector(row_groups.len());
        for value_opt in values {
            match value_opt {
                // Safety: We use the same data type to create the converter.
                Some(v) => builder.push_value_ref(v.as_value_ref()),
                None => builder.push_null(),
            }
        }
        let vector = builder.to_vector();

        Some(vector.to_arrow_array())
    }

    /// Returns min/max values of specific non-tag columns.
    fn column_values(
        row_groups: &[RowGroupMetaData],
        column: &ColumnMetadata,
        column_index: usize,
        is_min: bool,
    ) -> Option<ArrayRef> {
        let null_scalar: ScalarValue = column
            .column_schema
            .data_type
            .as_arrow_type()
            .try_into()
            .ok()?;
        let scalar_values = row_groups
            .iter()
            .map(|meta| {
                let stats = meta.column(column_index).statistics()?;
                if !stats.has_min_max_set() {
                    return None;
                }
                match stats {
                    Statistics::Boolean(s) => Some(ScalarValue::Boolean(Some(if is_min {
                        *s.min()
                    } else {
                        *s.max()
                    }))),
                    Statistics::Int32(s) => Some(ScalarValue::Int32(Some(if is_min {
                        *s.min()
                    } else {
                        *s.max()
                    }))),
                    Statistics::Int64(s) => Some(ScalarValue::Int64(Some(if is_min {
                        *s.min()
                    } else {
                        *s.max()
                    }))),

                    Statistics::Int96(_) => None,
                    Statistics::Float(s) => Some(ScalarValue::Float32(Some(if is_min {
                        *s.min()
                    } else {
                        *s.max()
                    }))),
                    Statistics::Double(s) => Some(ScalarValue::Float64(Some(if is_min {
                        *s.min()
                    } else {
                        *s.max()
                    }))),
                    Statistics::ByteArray(s) => {
                        let bytes = if is_min { s.min_bytes() } else { s.max_bytes() };
                        let s = String::from_utf8(bytes.to_vec()).ok();
                        Some(ScalarValue::Utf8(s))
                    }

                    Statistics::FixedLenByteArray(_) => None,
                }
            })
            .map(|maybe_scalar| maybe_scalar.unwrap_or_else(|| null_scalar.clone()))
            .collect::<Vec<ScalarValue>>();
        debug_assert_eq!(scalar_values.len(), row_groups.len());
        ScalarValue::iter_to_array(scalar_values).ok()
    }

    /// Returns null counts of specific non-tag columns.
    fn column_null_counts(
        row_groups: &[RowGroupMetaData],
        column_index: usize,
    ) -> Option<ArrayRef> {
        let values = row_groups.iter().map(|meta| {
            let col = meta.column(column_index);
            let stat = col.statistics()?;
            Some(stat.null_count())
        });
        Some(Arc::new(UInt64Array::from_iter(values)))
    }

    /// Field index of the primary key.
    fn primary_key_position(&self) -> usize {
        self.arrow_schema.fields.len() - 3
    }

    /// Field index of the time index.
    fn time_index_position(&self) -> usize {
        self.arrow_schema.fields.len() - FIXED_POS_COLUMN_NUM
    }
}

/// Gets the arrow schema to store in parquet.
fn to_sst_arrow_schema(metadata: &RegionMetadata) -> SchemaRef {
    let fields = Fields::from_iter(
        metadata
            .schema
            .arrow_schema()
            .fields()
            .iter()
            .zip(&metadata.column_metadatas)
            .filter_map(|(field, column_meta)| {
                if column_meta.semantic_type == SemanticType::Field {
                    Some(field.clone())
                } else {
                    // We have fixed positions for tags (primary key) and time index.
                    None
                }
            })
            .chain([metadata.time_index_field()])
            .chain(internal_fields()),
    );

    Arc::new(Schema::new(fields))
}

/// Compute offsets of different primary keys in the array.
fn primary_key_offsets(pk_dict_array: &DictionaryArray<UInt16Type>) -> Result<Vec<usize>> {
    if pk_dict_array.is_empty() {
        return Ok(Vec::new());
    }

    // Init offsets.
    let mut offsets = vec![0];
    let keys = pk_dict_array.keys();
    // We know that primary keys are always not null so we iterate `keys.values()` directly.
    let pk_indices = keys.values();
    for (i, key) in pk_indices.iter().take(keys.len() - 1).enumerate() {
        // Compare each key with next key
        if *key != pk_indices[i + 1] {
            // We meet a new key, push the next index as end of the offset.
            offsets.push(i + 1);
        }
    }
    offsets.push(keys.len());

    Ok(offsets)
}

/// Fields for internal columns.
fn internal_fields() -> [FieldRef; 3] {
    // Internal columns are always not null.
    [
        Arc::new(Field::new_dictionary(
            PRIMARY_KEY_COLUMN_NAME,
            ArrowDataType::UInt16,
            ArrowDataType::Binary,
            false,
        )),
        Arc::new(Field::new(
            SEQUENCE_COLUMN_NAME,
            ArrowDataType::UInt64,
            false,
        )),
        Arc::new(Field::new(OP_TYPE_COLUMN_NAME, ArrowDataType::UInt8, false)),
    ]
}

/// Creates a new array for specific `primary_key`.
fn new_primary_key_array(primary_key: &[u8], num_rows: usize) -> ArrayRef {
    let values = Arc::new(BinaryArray::from_iter_values([primary_key]));
    let keys = UInt16Array::from_value(0, num_rows);

    // Safety: The key index is valid.
    Arc::new(DictionaryArray::new(keys, values))
}

#[cfg(test)]
mod tests {
    use api::v1::OpType;
    use datatypes::arrow::array::{Int64Array, TimestampMillisecondArray, UInt64Array, UInt8Array};
    use datatypes::arrow::datatypes::TimeUnit;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::ColumnSchema;
    use datatypes::vectors::{Int64Vector, TimestampMillisecondVector, UInt64Vector, UInt8Vector};
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};
    use store_api::storage::RegionId;

    use super::*;

    const TEST_SEQUENCE: u64 = 1;
    const TEST_OP_TYPE: u8 = OpType::Put as u8;

    fn build_test_region_metadata() -> RegionMetadataRef {
        let mut builder = RegionMetadataBuilder::new(RegionId::new(1, 1));
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("tag0", ConcreteDataType::int64_datatype(), true),
                semantic_type: SemanticType::Tag,
                column_id: 1,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "field1",
                    ConcreteDataType::int64_datatype(),
                    true,
                ),
                semantic_type: SemanticType::Field,
                column_id: 4, // We change the order of fields columns.
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("tag1", ConcreteDataType::int64_datatype(), true),
                semantic_type: SemanticType::Tag,
                column_id: 3,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "field0",
                    ConcreteDataType::int64_datatype(),
                    true,
                ),
                semantic_type: SemanticType::Field,
                column_id: 2,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "ts",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Timestamp,
                column_id: 5,
            })
            .primary_key(vec![1, 3]);
        Arc::new(builder.build().unwrap())
    }

    fn build_test_arrow_schema() -> SchemaRef {
        let fields = vec![
            Field::new("field1", ArrowDataType::Int64, true),
            Field::new("field0", ArrowDataType::Int64, true),
            Field::new(
                "ts",
                ArrowDataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new(
                "__primary_key",
                ArrowDataType::Dictionary(
                    Box::new(ArrowDataType::UInt16),
                    Box::new(ArrowDataType::Binary),
                ),
                false,
            ),
            Field::new("__sequence", ArrowDataType::UInt64, false),
            Field::new("__op_type", ArrowDataType::UInt8, false),
        ];
        Arc::new(Schema::new(fields))
    }

    fn new_batch(primary_key: &[u8], start_ts: i64, start_field: i64, num_rows: usize) -> Batch {
        let ts_values = (0..num_rows).map(|i| start_ts + i as i64);
        let timestamps = Arc::new(TimestampMillisecondVector::from_values(ts_values));
        let sequences = Arc::new(UInt64Vector::from_vec(vec![TEST_SEQUENCE; num_rows]));
        let op_types = Arc::new(UInt8Vector::from_vec(vec![TEST_OP_TYPE; num_rows]));
        let fields = vec![
            BatchColumn {
                column_id: 4,
                data: Arc::new(Int64Vector::from_vec(vec![start_field; num_rows])),
            }, // field1
            BatchColumn {
                column_id: 2,
                data: Arc::new(Int64Vector::from_vec(vec![start_field + 1; num_rows])),
            }, // field0
        ];

        BatchBuilder::with_required_columns(primary_key.to_vec(), timestamps, sequences, op_types)
            .with_fields(fields)
            .build()
            .unwrap()
    }

    #[test]
    fn test_to_sst_arrow_schema() {
        let metadata = build_test_region_metadata();
        let write_format = WriteFormat::new(metadata);
        assert_eq!(build_test_arrow_schema(), write_format.arrow_schema());
    }

    #[test]
    fn test_new_primary_key_array() {
        let array = new_primary_key_array(b"test", 3);
        let expect = build_test_pk_array(&[(b"test".to_vec(), 3)]) as ArrayRef;
        assert_eq!(&expect, &array);
    }

    fn build_test_pk_array(pk_row_nums: &[(Vec<u8>, usize)]) -> Arc<DictionaryArray<UInt16Type>> {
        let values = Arc::new(BinaryArray::from_iter_values(
            pk_row_nums.iter().map(|v| &v.0),
        ));
        let mut keys = vec![];
        for (index, num_rows) in pk_row_nums.iter().map(|v| v.1).enumerate() {
            keys.extend(std::iter::repeat(index as u16).take(num_rows));
        }
        let keys = UInt16Array::from(keys);
        Arc::new(DictionaryArray::new(keys, values))
    }

    #[test]
    fn test_convert_batch() {
        let metadata = build_test_region_metadata();
        let write_format = WriteFormat::new(metadata);

        let num_rows = 4;
        let batch = new_batch(b"test", 1, 2, num_rows);
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![2; num_rows])), // field1
            Arc::new(Int64Array::from(vec![3; num_rows])), // field0
            Arc::new(TimestampMillisecondArray::from(vec![1, 2, 3, 4])), // ts
            build_test_pk_array(&[(b"test".to_vec(), num_rows)]), // primary key
            Arc::new(UInt64Array::from(vec![TEST_SEQUENCE; num_rows])), // sequence
            Arc::new(UInt8Array::from(vec![TEST_OP_TYPE; num_rows])), // op type
        ];
        let expect_record = RecordBatch::try_new(build_test_arrow_schema(), columns).unwrap();

        let actual = write_format.convert_batch(&batch).unwrap();
        assert_eq!(expect_record, actual);
    }

    #[test]
    fn test_projection_indices() {
        let metadata = build_test_region_metadata();
        let read_format = ReadFormat::new(metadata);
        // Only read tag1
        assert_eq!(vec![2, 3, 4, 5], read_format.projection_indices([3]));
        // Only read field1
        assert_eq!(vec![0, 2, 3, 4, 5], read_format.projection_indices([4]));
        // Only read ts
        assert_eq!(vec![2, 3, 4, 5], read_format.projection_indices([5]));
        // Read field0, tag0, ts
        assert_eq!(
            vec![1, 2, 3, 4, 5],
            read_format.projection_indices([2, 1, 5])
        );
    }

    #[test]
    fn test_empty_primary_key_offsets() {
        let array = build_test_pk_array(&[]);
        assert!(primary_key_offsets(&array).unwrap().is_empty());
    }

    #[test]
    fn test_primary_key_offsets_one_series() {
        let array = build_test_pk_array(&[(b"one".to_vec(), 1)]);
        assert_eq!(vec![0, 1], primary_key_offsets(&array).unwrap());

        let array = build_test_pk_array(&[(b"one".to_vec(), 1), (b"two".to_vec(), 1)]);
        assert_eq!(vec![0, 1, 2], primary_key_offsets(&array).unwrap());

        let array = build_test_pk_array(&[
            (b"one".to_vec(), 1),
            (b"two".to_vec(), 1),
            (b"three".to_vec(), 1),
        ]);
        assert_eq!(vec![0, 1, 2, 3], primary_key_offsets(&array).unwrap());
    }

    #[test]
    fn test_primary_key_offsets_multi_series() {
        let array = build_test_pk_array(&[(b"one".to_vec(), 1), (b"two".to_vec(), 3)]);
        assert_eq!(vec![0, 1, 4], primary_key_offsets(&array).unwrap());

        let array = build_test_pk_array(&[(b"one".to_vec(), 3), (b"two".to_vec(), 1)]);
        assert_eq!(vec![0, 3, 4], primary_key_offsets(&array).unwrap());

        let array = build_test_pk_array(&[(b"one".to_vec(), 3), (b"two".to_vec(), 3)]);
        assert_eq!(vec![0, 3, 6], primary_key_offsets(&array).unwrap());
    }

    #[test]
    fn test_convert_empty_record_batch() {
        let metadata = build_test_region_metadata();
        let arrow_schema = build_test_arrow_schema();
        let read_format = ReadFormat::new(metadata);
        assert_eq!(arrow_schema, *read_format.arrow_schema());

        let record_batch = RecordBatch::new_empty(arrow_schema);
        let mut batches = VecDeque::new();
        read_format
            .convert_record_batch(&record_batch, &mut batches)
            .unwrap();
        assert!(batches.is_empty());
    }

    #[test]
    fn test_convert_record_batch() {
        let metadata = build_test_region_metadata();
        let read_format = ReadFormat::new(metadata);

        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![1, 1, 10, 10])), // field1
            Arc::new(Int64Array::from(vec![2, 2, 11, 11])), // field0
            Arc::new(TimestampMillisecondArray::from(vec![1, 2, 11, 12])), // ts
            build_test_pk_array(&[(b"one".to_vec(), 2), (b"two".to_vec(), 2)]), // primary key
            Arc::new(UInt64Array::from(vec![TEST_SEQUENCE; 4])), // sequence
            Arc::new(UInt8Array::from(vec![TEST_OP_TYPE; 4])), // op type
        ];
        let arrow_schema = build_test_arrow_schema();
        let record_batch = RecordBatch::try_new(arrow_schema, columns).unwrap();
        let mut batches = VecDeque::new();
        read_format
            .convert_record_batch(&record_batch, &mut batches)
            .unwrap();

        assert_eq!(
            vec![new_batch(b"one", 1, 1, 2), new_batch(b"two", 11, 10, 2)],
            batches.into_iter().collect::<Vec<_>>(),
        );
    }
}
