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
//! - `__primary_key`, the primary key of the row (tags). Type: dictionary(uint32, binary)
//! - `__sequence`, the sequence number of a row. Type: uint64
//! - `__op_type`, the op type of the row. Type: uint8
//!
//! The schema of a parquet file is:
//! ```text
//! field 0, field 1, ..., field N, time index, primary key, sequence, op type
//! ```
//!
//! We stores fields in the same order as [RegionMetadata::field_columns()](store_api::metadata::RegionMetadata::field_columns()).

use std::borrow::Borrow;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use api::v1::SemanticType;
use common_time::Timestamp;
use datafusion_common::ScalarValue;
use datatypes::arrow::array::{ArrayRef, BinaryArray, DictionaryArray, UInt32Array, UInt64Array};
use datatypes::arrow::datatypes::{SchemaRef, UInt32Type};
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::prelude::DataType;
use datatypes::vectors::{Helper, Vector};
use mito_codec::row_converter::{build_primary_key_codec_with_fields, SortField};
use parquet::file::metadata::{ParquetMetaData, RowGroupMetaData};
use parquet::file::statistics::Statistics;
use snafu::{ensure, OptionExt, ResultExt};
use store_api::metadata::{ColumnMetadata, RegionMetadataRef};
use store_api::storage::{ColumnId, SequenceNumber};

use crate::error::{
    ConvertVectorSnafu, InvalidBatchSnafu, InvalidRecordBatchSnafu, NewRecordBatchSnafu, Result,
};
use crate::read::{Batch, BatchBuilder, BatchColumn};
use crate::sst::file::{FileMeta, FileTimeRange};
use crate::sst::parquet::flat_format::FlatReadFormat;
use crate::sst::to_sst_arrow_schema;

/// Arrow array type for the primary key dictionary.
pub(crate) type PrimaryKeyArray = DictionaryArray<UInt32Type>;

/// Number of columns that have fixed positions.
///
/// Contains: time index and internal columns.
pub(crate) const FIXED_POS_COLUMN_NUM: usize = 4;

/// Helper for writing the SST format with primary key.
pub(crate) struct PrimaryKeyWriteFormat {
    metadata: RegionMetadataRef,
    /// SST file schema.
    arrow_schema: SchemaRef,
    override_sequence: Option<SequenceNumber>,
}

impl PrimaryKeyWriteFormat {
    /// Creates a new helper.
    pub(crate) fn new(metadata: RegionMetadataRef) -> PrimaryKeyWriteFormat {
        let arrow_schema = to_sst_arrow_schema(&metadata);
        PrimaryKeyWriteFormat {
            metadata,
            arrow_schema,
            override_sequence: None,
        }
    }

    /// Set override sequence.
    pub(crate) fn with_override_sequence(
        mut self,
        override_sequence: Option<SequenceNumber>,
    ) -> Self {
        self.override_sequence = override_sequence;
        self
    }

    /// Gets the arrow schema to store in parquet.
    pub(crate) fn arrow_schema(&self) -> &SchemaRef {
        &self.arrow_schema
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

        if let Some(override_sequence) = self.override_sequence {
            let sequence_array =
                Arc::new(UInt64Array::from(vec![override_sequence; batch.num_rows()]));
            columns.push(sequence_array);
        } else {
            columns.push(batch.sequences().to_arrow_array());
        }
        columns.push(batch.op_types().to_arrow_array());

        RecordBatch::try_new(self.arrow_schema.clone(), columns).context(NewRecordBatchSnafu)
    }
}

/// Helper to read parquet formats.
pub enum ReadFormat {
    PrimaryKey(PrimaryKeyReadFormat),
    Flat(FlatReadFormat),
}

impl ReadFormat {
    // TODO(yingwen): Add a flag to choose format type.
    pub(crate) fn new(
        metadata: RegionMetadataRef,
        column_ids: impl Iterator<Item = ColumnId>,
    ) -> Self {
        Self::new_primary_key(metadata, column_ids)
    }

    /// Creates a helper to read the primary key format.
    pub fn new_primary_key(
        metadata: RegionMetadataRef,
        column_ids: impl Iterator<Item = ColumnId>,
    ) -> Self {
        ReadFormat::PrimaryKey(PrimaryKeyReadFormat::new(metadata, column_ids))
    }

    /// Creates a helper to read the flat format.
    pub fn new_flat(
        metadata: RegionMetadataRef,
        column_ids: impl Iterator<Item = ColumnId>,
    ) -> Self {
        ReadFormat::Flat(FlatReadFormat::new(metadata, column_ids))
    }

    pub(crate) fn as_primary_key(&self) -> Option<&PrimaryKeyReadFormat> {
        match self {
            ReadFormat::PrimaryKey(format) => Some(format),
            _ => None,
        }
    }

    /// Gets the arrow schema of the SST file.
    ///
    /// This schema is computed from the region metadata but should be the same
    /// as the arrow schema decoded from the file metadata.
    pub(crate) fn arrow_schema(&self) -> &SchemaRef {
        match self {
            ReadFormat::PrimaryKey(format) => format.arrow_schema(),
            ReadFormat::Flat(format) => format.arrow_schema(),
        }
    }

    /// Gets the metadata of the SST.
    pub(crate) fn metadata(&self) -> &RegionMetadataRef {
        match self {
            ReadFormat::PrimaryKey(format) => format.metadata(),
            ReadFormat::Flat(format) => format.metadata(),
        }
    }

    /// Gets sorted projection indices to read.
    pub(crate) fn projection_indices(&self) -> &[usize] {
        match self {
            ReadFormat::PrimaryKey(format) => format.projection_indices(),
            ReadFormat::Flat(format) => format.projection_indices(),
        }
    }

    /// Returns min values of specific column in row groups.
    pub fn min_values(
        &self,
        row_groups: &[impl Borrow<RowGroupMetaData>],
        column_id: ColumnId,
    ) -> StatValues {
        match self {
            ReadFormat::PrimaryKey(format) => format.min_values(row_groups, column_id),
            ReadFormat::Flat(format) => format.min_values(row_groups, column_id),
        }
    }

    /// Returns max values of specific column in row groups.
    pub fn max_values(
        &self,
        row_groups: &[impl Borrow<RowGroupMetaData>],
        column_id: ColumnId,
    ) -> StatValues {
        match self {
            ReadFormat::PrimaryKey(format) => format.max_values(row_groups, column_id),
            ReadFormat::Flat(format) => format.max_values(row_groups, column_id),
        }
    }

    /// Returns null counts of specific column in row groups.
    pub fn null_counts(
        &self,
        row_groups: &[impl Borrow<RowGroupMetaData>],
        column_id: ColumnId,
    ) -> StatValues {
        match self {
            ReadFormat::PrimaryKey(format) => format.null_counts(row_groups, column_id),
            ReadFormat::Flat(format) => format.null_counts(row_groups, column_id),
        }
    }

    /// Returns min/max values of specific columns.
    /// Returns None if the column does not have statistics.
    /// The column should not be encoded as a part of a primary key.
    pub(crate) fn column_values(
        row_groups: &[impl Borrow<RowGroupMetaData>],
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
                let stats = meta.borrow().column(column_index).statistics()?;
                match stats {
                    Statistics::Boolean(s) => Some(ScalarValue::Boolean(Some(if is_min {
                        *s.min_opt()?
                    } else {
                        *s.max_opt()?
                    }))),
                    Statistics::Int32(s) => Some(ScalarValue::Int32(Some(if is_min {
                        *s.min_opt()?
                    } else {
                        *s.max_opt()?
                    }))),
                    Statistics::Int64(s) => Some(ScalarValue::Int64(Some(if is_min {
                        *s.min_opt()?
                    } else {
                        *s.max_opt()?
                    }))),

                    Statistics::Int96(_) => None,
                    Statistics::Float(s) => Some(ScalarValue::Float32(Some(if is_min {
                        *s.min_opt()?
                    } else {
                        *s.max_opt()?
                    }))),
                    Statistics::Double(s) => Some(ScalarValue::Float64(Some(if is_min {
                        *s.min_opt()?
                    } else {
                        *s.max_opt()?
                    }))),
                    Statistics::ByteArray(s) => {
                        let bytes = if is_min {
                            s.min_bytes_opt()?
                        } else {
                            s.max_bytes_opt()?
                        };
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

    /// Returns null counts of specific columns.
    /// The column should not be encoded as a part of a primary key.
    pub(crate) fn column_null_counts(
        row_groups: &[impl Borrow<RowGroupMetaData>],
        column_index: usize,
    ) -> Option<ArrayRef> {
        let values = row_groups.iter().map(|meta| {
            let col = meta.borrow().column(column_index);
            let stat = col.statistics()?;
            stat.null_count_opt()
        });
        Some(Arc::new(UInt64Array::from_iter(values)))
    }

    /// Creates a sequence array to override.
    pub(crate) fn new_override_sequence_array(&self, length: usize) -> Option<ArrayRef> {
        match self {
            ReadFormat::PrimaryKey(format) => format.new_override_sequence_array(length),
            ReadFormat::Flat(format) => format.new_override_sequence_array(length),
        }
    }
}

/// Helper for reading the SST format.
pub struct PrimaryKeyReadFormat {
    /// The metadata stored in the SST.
    metadata: RegionMetadataRef,
    /// SST file schema.
    arrow_schema: SchemaRef,
    /// Field column id to its index in `schema` (SST schema).
    /// In SST schema, fields are stored in the front of the schema.
    field_id_to_index: HashMap<ColumnId, usize>,
    /// Indices of columns to read from the SST. It contains all internal columns.
    projection_indices: Vec<usize>,
    /// Field column id to their index in the projected schema (
    /// the schema of [Batch]).
    field_id_to_projected_index: HashMap<ColumnId, usize>,
    /// Sequence number to override the sequence read from the SST.
    override_sequence: Option<SequenceNumber>,
}

impl PrimaryKeyReadFormat {
    /// Creates a helper with existing `metadata` and `column_ids` to read.
    pub fn new(
        metadata: RegionMetadataRef,
        column_ids: impl Iterator<Item = ColumnId>,
    ) -> PrimaryKeyReadFormat {
        let field_id_to_index: HashMap<_, _> = metadata
            .field_columns()
            .enumerate()
            .map(|(index, column)| (column.column_id, index))
            .collect();
        let arrow_schema = to_sst_arrow_schema(&metadata);

        let format_projection = FormatProjection::compute_format_projection(
            &field_id_to_index,
            arrow_schema.fields.len(),
            column_ids,
        );

        PrimaryKeyReadFormat {
            metadata,
            arrow_schema,
            field_id_to_index,
            projection_indices: format_projection.projection_indices,
            field_id_to_projected_index: format_projection.column_id_to_projected_index,
            override_sequence: None,
        }
    }

    /// Sets the sequence number to override.
    pub(crate) fn set_override_sequence(&mut self, sequence: Option<SequenceNumber>) {
        self.override_sequence = sequence;
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

    /// Gets sorted projection indices to read.
    pub(crate) fn projection_indices(&self) -> &[usize] {
        &self.projection_indices
    }

    /// Creates a sequence array to override.
    pub(crate) fn new_override_sequence_array(&self, length: usize) -> Option<ArrayRef> {
        self.override_sequence
            .map(|seq| Arc::new(UInt64Array::from_value(seq, length)) as ArrayRef)
    }

    /// Convert a arrow record batch into `batches`.
    ///
    /// The length of `override_sequence_array` must be larger than the length of the record batch.
    /// Note that the `record_batch` may only contains a subset of columns if it is projected.
    pub fn convert_record_batch(
        &self,
        record_batch: &RecordBatch,
        override_sequence_array: Option<&ArrayRef>,
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
        let mut sequence_array = fixed_pos_columns.next().unwrap().clone();
        let pk_array = fixed_pos_columns.next().unwrap();
        let ts_array = fixed_pos_columns.next().unwrap();
        let field_batch_columns = self.get_field_batch_columns(record_batch)?;

        // Override sequence array if provided.
        if let Some(override_array) = override_sequence_array {
            assert!(override_array.len() >= sequence_array.len());
            // It's fine to assign the override array directly, but we slice it to make
            // sure it matches the length of the original sequence array.
            sequence_array = if override_array.len() > sequence_array.len() {
                override_array.slice(0, sequence_array.len())
            } else {
                override_array.clone()
            };
        }

        // Compute primary key offsets.
        let pk_dict_array = pk_array
            .as_any()
            .downcast_ref::<PrimaryKeyArray>()
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
            let primary_key = pk_values.value(dict_key as usize).to_vec();

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
    pub fn min_values(
        &self,
        row_groups: &[impl Borrow<RowGroupMetaData>],
        column_id: ColumnId,
    ) -> StatValues {
        let Some(column) = self.metadata.column_by_id(column_id) else {
            // No such column in the SST.
            return StatValues::NoColumn;
        };
        match column.semantic_type {
            SemanticType::Tag => self.tag_values(row_groups, column, true),
            SemanticType::Field => {
                // Safety: `field_id_to_index` is initialized by the semantic type.
                let index = self.field_id_to_index.get(&column_id).unwrap();
                let stats = ReadFormat::column_values(row_groups, column, *index, true);
                StatValues::from_stats_opt(stats)
            }
            SemanticType::Timestamp => {
                let index = self.time_index_position();
                let stats = ReadFormat::column_values(row_groups, column, index, true);
                StatValues::from_stats_opt(stats)
            }
        }
    }

    /// Returns max values of specific column in row groups.
    pub fn max_values(
        &self,
        row_groups: &[impl Borrow<RowGroupMetaData>],
        column_id: ColumnId,
    ) -> StatValues {
        let Some(column) = self.metadata.column_by_id(column_id) else {
            // No such column in the SST.
            return StatValues::NoColumn;
        };
        match column.semantic_type {
            SemanticType::Tag => self.tag_values(row_groups, column, false),
            SemanticType::Field => {
                // Safety: `field_id_to_index` is initialized by the semantic type.
                let index = self.field_id_to_index.get(&column_id).unwrap();
                let stats = ReadFormat::column_values(row_groups, column, *index, false);
                StatValues::from_stats_opt(stats)
            }
            SemanticType::Timestamp => {
                let index = self.time_index_position();
                let stats = ReadFormat::column_values(row_groups, column, index, false);
                StatValues::from_stats_opt(stats)
            }
        }
    }

    /// Returns null counts of specific column in row groups.
    pub fn null_counts(
        &self,
        row_groups: &[impl Borrow<RowGroupMetaData>],
        column_id: ColumnId,
    ) -> StatValues {
        let Some(column) = self.metadata.column_by_id(column_id) else {
            // No such column in the SST.
            return StatValues::NoColumn;
        };
        match column.semantic_type {
            SemanticType::Tag => StatValues::NoStats,
            SemanticType::Field => {
                // Safety: `field_id_to_index` is initialized by the semantic type.
                let index = self.field_id_to_index.get(&column_id).unwrap();
                let stats = ReadFormat::column_null_counts(row_groups, *index);
                StatValues::from_stats_opt(stats)
            }
            SemanticType::Timestamp => {
                let index = self.time_index_position();
                let stats = ReadFormat::column_null_counts(row_groups, index);
                StatValues::from_stats_opt(stats)
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
        row_groups: &[impl Borrow<RowGroupMetaData>],
        column: &ColumnMetadata,
        is_min: bool,
    ) -> StatValues {
        let is_first_tag = self
            .metadata
            .primary_key
            .first()
            .map(|id| *id == column.column_id)
            .unwrap_or(false);
        if !is_first_tag {
            // Only the min-max of the first tag is available in the primary key.
            return StatValues::NoStats;
        }

        StatValues::from_stats_opt(self.first_tag_values(row_groups, column, is_min))
    }

    /// Returns min/max values of the first tag.
    /// Returns None if the tag does not have statistics.
    fn first_tag_values(
        &self,
        row_groups: &[impl Borrow<RowGroupMetaData>],
        column: &ColumnMetadata,
        is_min: bool,
    ) -> Option<ArrayRef> {
        debug_assert!(self
            .metadata
            .primary_key
            .first()
            .map(|id| *id == column.column_id)
            .unwrap_or(false));

        let primary_key_encoding = self.metadata.primary_key_encoding;
        let converter = build_primary_key_codec_with_fields(
            primary_key_encoding,
            [(
                column.column_id,
                SortField::new(column.column_schema.data_type.clone()),
            )]
            .into_iter(),
        );

        let values = row_groups.iter().map(|meta| {
            let stats = meta
                .borrow()
                .column(self.primary_key_position())
                .statistics()?;
            match stats {
                Statistics::Boolean(_) => None,
                Statistics::Int32(_) => None,
                Statistics::Int64(_) => None,
                Statistics::Int96(_) => None,
                Statistics::Float(_) => None,
                Statistics::Double(_) => None,
                Statistics::ByteArray(s) => {
                    let bytes = if is_min {
                        s.min_bytes_opt()?
                    } else {
                        s.max_bytes_opt()?
                    };
                    converter.decode_leftmost(bytes).ok()?
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

    /// Index in SST of the primary key.
    fn primary_key_position(&self) -> usize {
        self.arrow_schema.fields.len() - 3
    }

    /// Index in SST of the time index.
    fn time_index_position(&self) -> usize {
        self.arrow_schema.fields.len() - FIXED_POS_COLUMN_NUM
    }

    /// Index of a field column by its column id.
    pub fn field_index_by_id(&self, column_id: ColumnId) -> Option<usize> {
        self.field_id_to_projected_index.get(&column_id).copied()
    }
}

/// Helper to compute the projection for the SST.
pub(crate) struct FormatProjection {
    /// Indices of columns to read from the SST. It contains all internal columns.
    pub(crate) projection_indices: Vec<usize>,
    /// Column id to their index in the projected schema (
    /// the schema after projection).
    pub(crate) column_id_to_projected_index: HashMap<ColumnId, usize>,
}

impl FormatProjection {
    /// Computes the projection.
    ///
    /// `id_to_index` is a mapping from column id to the index of the column in the SST.
    pub(crate) fn compute_format_projection(
        id_to_index: &HashMap<ColumnId, usize>,
        sst_column_num: usize,
        column_ids: impl Iterator<Item = ColumnId>,
    ) -> Self {
        // Maps column id of a projected column to its index in SST.
        // It also ignores columns not in the SST.
        // [(column id, index in SST)]
        let mut projected_schema: Vec<_> = column_ids
            .filter_map(|column_id| {
                id_to_index
                    .get(&column_id)
                    .copied()
                    .map(|index| (column_id, index))
            })
            .collect();
        // Sorts columns by their indices in the SST. SST uses a bitmap for projection.
        // This ensures the schema of `projected_schema` is the same as the batch returned from the SST.
        projected_schema.sort_unstable_by_key(|x| x.1);
        // Dedups the entries to avoid the case that `column_ids` has duplicated columns.
        projected_schema.dedup_by_key(|x| x.1);

        // Collects all projected indices.
        // It contains the positions of all columns we need to read.
        let mut projection_indices: Vec<_> = projected_schema
            .iter()
            .map(|(_column_id, index)| *index)
            // We need to add all fixed position columns.
            .chain(sst_column_num - FIXED_POS_COLUMN_NUM..sst_column_num)
            .collect();
        projection_indices.sort_unstable();
        // Removes duplications.
        projection_indices.dedup();

        // Creates a map from column id to the index of that column in the projected record batch.
        let column_id_to_projected_index = projected_schema
            .into_iter()
            .map(|(column_id, _)| column_id)
            .enumerate()
            .map(|(index, column_id)| (column_id, index))
            .collect();

        Self {
            projection_indices,
            column_id_to_projected_index,
        }
    }
}

/// Values of column statistics of the SST.
///
/// It also distinguishes the case that a column is not found and
/// the column exists but has no statistics.
pub enum StatValues {
    /// Values of each row group.
    Values(ArrayRef),
    /// No such column.
    NoColumn,
    /// Column exists but has no statistics.
    NoStats,
}

impl StatValues {
    /// Creates a new `StatValues` instance from optional statistics.
    pub fn from_stats_opt(stats: Option<ArrayRef>) -> Self {
        match stats {
            Some(stats) => StatValues::Values(stats),
            None => StatValues::NoStats,
        }
    }
}

#[cfg(test)]
impl PrimaryKeyReadFormat {
    /// Creates a helper with existing `metadata` and all columns.
    pub fn new_with_all_columns(metadata: RegionMetadataRef) -> PrimaryKeyReadFormat {
        Self::new(
            Arc::clone(&metadata),
            metadata.column_metadatas.iter().map(|c| c.column_id),
        )
    }
}

/// Compute offsets of different primary keys in the array.
fn primary_key_offsets(pk_dict_array: &PrimaryKeyArray) -> Result<Vec<usize>> {
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

/// Creates a new array for specific `primary_key`.
fn new_primary_key_array(primary_key: &[u8], num_rows: usize) -> ArrayRef {
    let values = Arc::new(BinaryArray::from_iter_values([primary_key]));
    let keys = UInt32Array::from_value(0, num_rows);

    // Safety: The key index is valid.
    Arc::new(DictionaryArray::new(keys, values))
}

/// Gets the min/max time index of the row group from the parquet meta.
/// It assumes the parquet is created by the mito engine.
pub(crate) fn parquet_row_group_time_range(
    file_meta: &FileMeta,
    parquet_meta: &ParquetMetaData,
    row_group_idx: usize,
) -> Option<FileTimeRange> {
    let row_group_meta = parquet_meta.row_group(row_group_idx);
    let num_columns = parquet_meta.file_metadata().schema_descr().num_columns();
    assert!(
        num_columns >= FIXED_POS_COLUMN_NUM,
        "file only has {} columns",
        num_columns
    );
    let time_index_pos = num_columns - FIXED_POS_COLUMN_NUM;

    let stats = row_group_meta.column(time_index_pos).statistics()?;
    // The physical type for the timestamp should be i64.
    let (min, max) = match stats {
        Statistics::Int64(value_stats) => (*value_stats.min_opt()?, *value_stats.max_opt()?),
        Statistics::Int32(_)
        | Statistics::Boolean(_)
        | Statistics::Int96(_)
        | Statistics::Float(_)
        | Statistics::Double(_)
        | Statistics::ByteArray(_)
        | Statistics::FixedLenByteArray(_) => {
            common_telemetry::warn!(
                "Invalid statistics {:?} for time index in parquet in {}",
                stats,
                file_meta.file_id
            );
            return None;
        }
    };

    debug_assert!(min >= file_meta.time_range.0.value() && min <= file_meta.time_range.1.value());
    debug_assert!(max >= file_meta.time_range.0.value() && max <= file_meta.time_range.1.value());
    let unit = file_meta.time_range.0.unit();

    Some((Timestamp::new(min, unit), Timestamp::new(max, unit)))
}

/// Checks if sequence override is needed based on all row groups' statistics.
/// Returns true if ALL row groups have sequence min-max values of 0.
pub(crate) fn need_override_sequence(parquet_meta: &ParquetMetaData) -> bool {
    let num_columns = parquet_meta.file_metadata().schema_descr().num_columns();
    if num_columns < FIXED_POS_COLUMN_NUM {
        return false;
    }

    // The sequence column is the second-to-last column (before op_type)
    let sequence_pos = num_columns - 2;

    // Check all row groups - all must have sequence min-max of 0
    for row_group in parquet_meta.row_groups() {
        if let Some(Statistics::Int64(value_stats)) = row_group.column(sequence_pos).statistics() {
            if let (Some(min_val), Some(max_val)) = (value_stats.min_opt(), value_stats.max_opt()) {
                // If any row group doesn't have min=0 and max=0, return false
                if *min_val != 0 || *max_val != 0 {
                    return false;
                }
            } else {
                // If any row group doesn't have statistics, return false
                return false;
            }
        } else {
            // If any row group doesn't have Int64 statistics, return false
            return false;
        }
    }

    // All row groups have sequence min-max of 0, or there are no row groups
    !parquet_meta.row_groups().is_empty()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::v1::OpType;
    use datatypes::arrow::array::{Int64Array, TimestampMillisecondArray, UInt64Array, UInt8Array};
    use datatypes::arrow::datatypes::{DataType as ArrowDataType, Field, Schema, TimeUnit};
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::ColumnSchema;
    use datatypes::vectors::{Int64Vector, TimestampMillisecondVector, UInt64Vector, UInt8Vector};
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};
    use store_api::storage::RegionId;

    use super::*;
    use crate::sst::parquet::flat_format::FlatWriteFormat;
    use crate::sst::FlatSchemaOptions;

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
                    Box::new(ArrowDataType::UInt32),
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
        new_batch_with_sequence(primary_key, start_ts, start_field, num_rows, TEST_SEQUENCE)
    }

    fn new_batch_with_sequence(
        primary_key: &[u8],
        start_ts: i64,
        start_field: i64,
        num_rows: usize,
        sequence: u64,
    ) -> Batch {
        let ts_values = (0..num_rows).map(|i| start_ts + i as i64);
        let timestamps = Arc::new(TimestampMillisecondVector::from_values(ts_values));
        let sequences = Arc::new(UInt64Vector::from_vec(vec![sequence; num_rows]));
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
        let write_format = PrimaryKeyWriteFormat::new(metadata);
        assert_eq!(&build_test_arrow_schema(), write_format.arrow_schema());
    }

    #[test]
    fn test_new_primary_key_array() {
        let array = new_primary_key_array(b"test", 3);
        let expect = build_test_pk_array(&[(b"test".to_vec(), 3)]) as ArrayRef;
        assert_eq!(&expect, &array);
    }

    fn build_test_pk_array(pk_row_nums: &[(Vec<u8>, usize)]) -> Arc<PrimaryKeyArray> {
        let values = Arc::new(BinaryArray::from_iter_values(
            pk_row_nums.iter().map(|v| &v.0),
        ));
        let mut keys = vec![];
        for (index, num_rows) in pk_row_nums.iter().map(|v| v.1).enumerate() {
            keys.extend(std::iter::repeat_n(index as u32, num_rows));
        }
        let keys = UInt32Array::from(keys);
        Arc::new(DictionaryArray::new(keys, values))
    }

    #[test]
    fn test_convert_batch() {
        let metadata = build_test_region_metadata();
        let write_format = PrimaryKeyWriteFormat::new(metadata);

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
    fn test_convert_batch_with_override_sequence() {
        let metadata = build_test_region_metadata();
        let write_format =
            PrimaryKeyWriteFormat::new(metadata).with_override_sequence(Some(415411));

        let num_rows = 4;
        let batch = new_batch(b"test", 1, 2, num_rows);
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![2; num_rows])), // field1
            Arc::new(Int64Array::from(vec![3; num_rows])), // field0
            Arc::new(TimestampMillisecondArray::from(vec![1, 2, 3, 4])), // ts
            build_test_pk_array(&[(b"test".to_vec(), num_rows)]), // primary key
            Arc::new(UInt64Array::from(vec![415411; num_rows])), // sequence
            Arc::new(UInt8Array::from(vec![TEST_OP_TYPE; num_rows])), // op type
        ];
        let expect_record = RecordBatch::try_new(build_test_arrow_schema(), columns).unwrap();

        let actual = write_format.convert_batch(&batch).unwrap();
        assert_eq!(expect_record, actual);
    }

    #[test]
    fn test_projection_indices() {
        let metadata = build_test_region_metadata();
        // Only read tag1
        let read_format = ReadFormat::new_primary_key(metadata.clone(), [3].iter().copied());
        assert_eq!(&[2, 3, 4, 5], read_format.projection_indices());
        // Only read field1
        let read_format = ReadFormat::new_primary_key(metadata.clone(), [4].iter().copied());
        assert_eq!(&[0, 2, 3, 4, 5], read_format.projection_indices());
        // Only read ts
        let read_format = ReadFormat::new_primary_key(metadata.clone(), [5].iter().copied());
        assert_eq!(&[2, 3, 4, 5], read_format.projection_indices());
        // Read field0, tag0, ts
        let read_format = ReadFormat::new_primary_key(metadata, [2, 1, 5].iter().copied());
        assert_eq!(&[1, 2, 3, 4, 5], read_format.projection_indices());
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
        let column_ids: Vec<_> = metadata
            .column_metadatas
            .iter()
            .map(|col| col.column_id)
            .collect();
        let read_format = PrimaryKeyReadFormat::new(metadata, column_ids.iter().copied());
        assert_eq!(arrow_schema, *read_format.arrow_schema());

        let record_batch = RecordBatch::new_empty(arrow_schema);
        let mut batches = VecDeque::new();
        read_format
            .convert_record_batch(&record_batch, None, &mut batches)
            .unwrap();
        assert!(batches.is_empty());
    }

    #[test]
    fn test_convert_record_batch() {
        let metadata = build_test_region_metadata();
        let column_ids: Vec<_> = metadata
            .column_metadatas
            .iter()
            .map(|col| col.column_id)
            .collect();
        let read_format = PrimaryKeyReadFormat::new(metadata, column_ids.iter().copied());

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
            .convert_record_batch(&record_batch, None, &mut batches)
            .unwrap();

        assert_eq!(
            vec![new_batch(b"one", 1, 1, 2), new_batch(b"two", 11, 10, 2)],
            batches.into_iter().collect::<Vec<_>>(),
        );
    }

    #[test]
    fn test_convert_record_batch_with_override_sequence() {
        let metadata = build_test_region_metadata();
        let column_ids: Vec<_> = metadata
            .column_metadatas
            .iter()
            .map(|col| col.column_id)
            .collect();
        let read_format = ReadFormat::new(metadata, column_ids.iter().copied());

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

        // Create override sequence array with custom values
        let override_sequence: u64 = 12345;
        let override_sequence_array: ArrayRef =
            Arc::new(UInt64Array::from_value(override_sequence, 4));

        let mut batches = VecDeque::new();
        read_format
            .as_primary_key()
            .unwrap()
            .convert_record_batch(&record_batch, Some(&override_sequence_array), &mut batches)
            .unwrap();

        // Create expected batches with override sequence
        let expected_batch1 = new_batch_with_sequence(b"one", 1, 1, 2, override_sequence);
        let expected_batch2 = new_batch_with_sequence(b"two", 11, 10, 2, override_sequence);

        assert_eq!(
            vec![expected_batch1, expected_batch2],
            batches.into_iter().collect::<Vec<_>>(),
        );
    }

    fn build_test_flat_sst_schema() -> SchemaRef {
        let fields = vec![
            Field::new("tag0", ArrowDataType::Int64, true), // primary key columns first
            Field::new("tag1", ArrowDataType::Int64, true),
            Field::new("field1", ArrowDataType::Int64, true), // then field columns
            Field::new("field0", ArrowDataType::Int64, true),
            Field::new(
                "ts",
                ArrowDataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new(
                "__primary_key",
                ArrowDataType::Dictionary(
                    Box::new(ArrowDataType::UInt32),
                    Box::new(ArrowDataType::Binary),
                ),
                false,
            ),
            Field::new("__sequence", ArrowDataType::UInt64, false),
            Field::new("__op_type", ArrowDataType::UInt8, false),
        ];
        Arc::new(Schema::new(fields))
    }

    #[test]
    fn test_flat_to_sst_arrow_schema() {
        let metadata = build_test_region_metadata();
        let format = FlatWriteFormat::new(metadata, &FlatSchemaOptions::default());
        assert_eq!(&build_test_flat_sst_schema(), format.arrow_schema());
    }

    fn input_columns_for_flat_batch(num_rows: usize) -> Vec<ArrayRef> {
        vec![
            Arc::new(Int64Array::from(vec![1; num_rows])), // tag0
            Arc::new(Int64Array::from(vec![1; num_rows])), // tag1
            Arc::new(Int64Array::from(vec![2; num_rows])), // field1
            Arc::new(Int64Array::from(vec![3; num_rows])), // field0
            Arc::new(TimestampMillisecondArray::from(vec![1, 2, 3, 4])), // ts
            build_test_pk_array(&[(b"test".to_vec(), num_rows)]), // __primary_key
            Arc::new(UInt64Array::from(vec![TEST_SEQUENCE; num_rows])), // sequence
            Arc::new(UInt8Array::from(vec![TEST_OP_TYPE; num_rows])), // op type
        ]
    }

    #[test]
    fn test_flat_convert_batch() {
        let metadata = build_test_region_metadata();
        let format = FlatWriteFormat::new(metadata, &FlatSchemaOptions::default());

        let num_rows = 4;
        let columns: Vec<ArrayRef> = input_columns_for_flat_batch(num_rows);
        let batch = RecordBatch::try_new(build_test_flat_sst_schema(), columns.clone()).unwrap();
        let expect_record = RecordBatch::try_new(build_test_flat_sst_schema(), columns).unwrap();

        let actual = format.convert_batch(&batch).unwrap();
        assert_eq!(expect_record, actual);
    }

    #[test]
    fn test_flat_convert_with_override_sequence() {
        let metadata = build_test_region_metadata();
        let format = FlatWriteFormat::new(metadata, &FlatSchemaOptions::default())
            .with_override_sequence(Some(415411));

        let num_rows = 4;
        let columns: Vec<ArrayRef> = input_columns_for_flat_batch(num_rows);
        let batch = RecordBatch::try_new(build_test_flat_sst_schema(), columns).unwrap();

        let expected_columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![1; num_rows])), // tag0
            Arc::new(Int64Array::from(vec![1; num_rows])), // tag1
            Arc::new(Int64Array::from(vec![2; num_rows])), // field1
            Arc::new(Int64Array::from(vec![3; num_rows])), // field0
            Arc::new(TimestampMillisecondArray::from(vec![1, 2, 3, 4])), // ts
            build_test_pk_array(&[(b"test".to_vec(), num_rows)]), // __primary_key
            Arc::new(UInt64Array::from(vec![415411; num_rows])), // overridden sequence
            Arc::new(UInt8Array::from(vec![TEST_OP_TYPE; num_rows])), // op type
        ];
        let expected_record =
            RecordBatch::try_new(build_test_flat_sst_schema(), expected_columns).unwrap();

        let actual = format.convert_batch(&batch).unwrap();
        assert_eq!(expected_record, actual);
    }

    #[test]
    fn test_flat_projection_indices() {
        let metadata = build_test_region_metadata();
        // Based on flat format: tag0(0), tag1(1), field1(2), field0(3), ts(4), __primary_key(5), __sequence(6), __op_type(7)
        // The projection includes all "fixed position" columns: ts(4), __primary_key(5), __sequence(6), __op_type(7)

        // Only read tag1 (column_id=3, index=1) + fixed columns
        let read_format = ReadFormat::new_flat(metadata.clone(), [3].iter().copied());
        assert_eq!(&[1, 4, 5, 6, 7], read_format.projection_indices());

        // Only read field1 (column_id=4, index=2) + fixed columns
        let read_format = ReadFormat::new_flat(metadata.clone(), [4].iter().copied());
        assert_eq!(&[2, 4, 5, 6, 7], read_format.projection_indices());

        // Only read ts (column_id=5, index=4) + fixed columns (ts is already included in fixed)
        let read_format = ReadFormat::new_flat(metadata.clone(), [5].iter().copied());
        assert_eq!(&[4, 5, 6, 7], read_format.projection_indices());

        // Read field0(column_id=2, index=3), tag0(column_id=1, index=0), ts(column_id=5, index=4) + fixed columns
        let read_format = ReadFormat::new_flat(metadata, [2, 1, 5].iter().copied());
        assert_eq!(&[0, 3, 4, 5, 6, 7], read_format.projection_indices());
    }

    #[test]
    fn test_flat_read_format_convert_batch() {
        let metadata = build_test_region_metadata();
        let mut format = FlatReadFormat::new(
            metadata,
            std::iter::once(1), // Just read tag0
        );

        let num_rows = 4;
        let original_sequence = 100u64;
        let override_sequence = 200u64;

        // Create a test record batch
        let columns: Vec<ArrayRef> = input_columns_for_flat_batch(num_rows);
        let mut test_columns = columns.clone();
        // Replace sequence column with original sequence values
        test_columns[6] = Arc::new(UInt64Array::from(vec![original_sequence; num_rows]));
        let record_batch =
            RecordBatch::try_new(format.arrow_schema().clone(), test_columns).unwrap();

        // Test without override sequence - should return clone
        let result = format.convert_batch(&record_batch, None).unwrap();
        let sequence_column = result.column(
            crate::sst::parquet::flat_format::sequence_column_index(result.num_columns()),
        );
        let sequence_array = sequence_column
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();

        let expected_original = UInt64Array::from(vec![original_sequence; num_rows]);
        assert_eq!(sequence_array, &expected_original);

        // Set override sequence and test with new_override_sequence_array
        format.set_override_sequence(Some(override_sequence));
        let override_sequence_array = format.new_override_sequence_array(num_rows).unwrap();
        let result = format
            .convert_batch(&record_batch, Some(&override_sequence_array))
            .unwrap();
        let sequence_column = result.column(
            crate::sst::parquet::flat_format::sequence_column_index(result.num_columns()),
        );
        let sequence_array = sequence_column
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();

        let expected_override = UInt64Array::from(vec![override_sequence; num_rows]);
        assert_eq!(sequence_array, &expected_override);
    }
}
