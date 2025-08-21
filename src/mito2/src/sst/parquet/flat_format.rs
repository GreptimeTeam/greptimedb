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
//! It can store both encoded primary key and raw key columns.
//!
//! We store two additional internal columns at last:
//! - `__primary_key`, the encoded primary key of the row (tags). Type: dictionary(uint32, binary)
//! - `__sequence`, the sequence number of a row. Type: uint64
//! - `__op_type`, the op type of the row. Type: uint8
//!
//! The format is
//! ```text
//! primary key columns, field columns, time index, encoded primary key, __sequence, __op_type.
//!
//! It stores field columns in the same order as [RegionMetadata::field_columns()](store_api::metadata::RegionMetadata::field_columns())
//! and stores primary key columns in the same order as [RegionMetadata::primary_key].

use std::borrow::Borrow;
use std::collections::HashMap;
use std::sync::Arc;

use api::v1::SemanticType;
use datatypes::arrow::array::{
    Array, ArrayRef, BinaryArray, DictionaryArray, UInt32Array, UInt64Array,
};
use datatypes::arrow::compute::kernels::take::take;
use datatypes::arrow::datatypes::{Field, Schema, SchemaRef};
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::prelude::{ConcreteDataType, DataType};
use mito_codec::row_converter::{build_primary_key_codec, CompositeValues, PrimaryKeyCodec};
use parquet::file::metadata::RowGroupMetaData;
use snafu::{ensure, OptionExt, ResultExt};
use store_api::metadata::{RegionMetadata, RegionMetadataRef};
use store_api::storage::{ColumnId, SequenceNumber};

use crate::error::{
    ComputeArrowSnafu, DecodeSnafu, InvalidParquetSnafu, InvalidRecordBatchSnafu,
    NewRecordBatchSnafu, Result,
};
use crate::sst::parquet::format::{
    FormatProjection, PrimaryKeyArray, ReadFormat, StatValues, FIXED_POS_COLUMN_NUM,
};
use crate::sst::{to_flat_sst_arrow_schema, FlatSchemaOptions};

/// Helper for writing the SST format.
#[allow(dead_code)]
pub(crate) struct FlatWriteFormat {
    metadata: RegionMetadataRef,
    /// SST file schema.
    arrow_schema: SchemaRef,
    override_sequence: Option<SequenceNumber>,
}

impl FlatWriteFormat {
    /// Creates a new helper.
    #[allow(dead_code)]
    pub(crate) fn new(metadata: RegionMetadataRef, options: &FlatSchemaOptions) -> FlatWriteFormat {
        let arrow_schema = to_flat_sst_arrow_schema(&metadata, options);
        FlatWriteFormat {
            metadata,
            arrow_schema,
            override_sequence: None,
        }
    }

    /// Set override sequence.
    #[allow(dead_code)]
    pub(crate) fn with_override_sequence(
        mut self,
        override_sequence: Option<SequenceNumber>,
    ) -> Self {
        self.override_sequence = override_sequence;
        self
    }

    /// Gets the arrow schema to store in parquet.
    #[allow(dead_code)]
    pub(crate) fn arrow_schema(&self) -> &SchemaRef {
        &self.arrow_schema
    }

    /// Convert `batch` to a arrow record batch to store in parquet.
    #[allow(dead_code)]
    pub(crate) fn convert_batch(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        debug_assert_eq!(batch.num_columns(), self.arrow_schema.fields().len());

        let Some(override_sequence) = self.override_sequence else {
            return Ok(batch.clone());
        };

        let mut columns = batch.columns().to_vec();
        let sequence_array = Arc::new(UInt64Array::from(vec![override_sequence; batch.num_rows()]));
        columns[sequence_column_index(batch.num_columns())] = sequence_array;

        RecordBatch::try_new(self.arrow_schema.clone(), columns).context(NewRecordBatchSnafu)
    }
}

/// Returns the position of the sequence column.
pub(crate) fn sequence_column_index(num_columns: usize) -> usize {
    num_columns - 2
}

/// Returns the position of the time index column.
pub(crate) fn time_index_column_index(num_columns: usize) -> usize {
    num_columns - 4
}

/// Returns the position of the primary key column.
pub(crate) fn primary_key_column_index(num_columns: usize) -> usize {
    num_columns - 3
}

/// Returns the position of the op type key column.
pub(crate) fn op_type_column_index(num_columns: usize) -> usize {
    num_columns - 1
}

// TODO(yingwen): Add an option to skip reading internal columns.
/// Helper for reading the flat SST format with projection.
///
/// It only supports flat format that stores primary keys additionally.
pub struct FlatReadFormat {
    /// The metadata stored in the SST.
    metadata: RegionMetadataRef,
    /// SST file schema.
    arrow_schema: SchemaRef,
    /// Projection.
    format_projection: FormatProjection,
    /// Column id to index in SST.
    column_id_to_sst_index: HashMap<ColumnId, usize>,
    /// Sequence number to override the sequence read from the SST.
    override_sequence: Option<SequenceNumber>,
    /// Optional format converter for handling flat format conversion.
    convert_format: Option<FlatConvertFormat>,
}

impl FlatReadFormat {
    /// Creates a helper with existing `metadata` and `column_ids` to read.
    pub fn new(
        metadata: RegionMetadataRef,
        column_ids: impl Iterator<Item = ColumnId>,
        convert_to_flat: bool,
    ) -> FlatReadFormat {
        let arrow_schema = to_flat_sst_arrow_schema(&metadata, &FlatSchemaOptions::default());

        // Creates a map to lookup index.
        let id_to_index = sst_column_id_indices(&metadata);

        let format_projection = FormatProjection::compute_format_projection(
            &id_to_index,
            arrow_schema.fields.len(),
            column_ids,
        );

        let convert_format = if convert_to_flat {
            let codec = build_primary_key_codec(&metadata);
            FlatConvertFormat::new(Arc::clone(&metadata), &format_projection, codec)
        } else {
            None
        };

        FlatReadFormat {
            metadata,
            arrow_schema,
            format_projection,
            column_id_to_sst_index: id_to_index,
            override_sequence: None,
            convert_format,
        }
    }

    /// Sets the sequence number to override.
    #[allow(dead_code)]
    pub(crate) fn set_override_sequence(&mut self, sequence: Option<SequenceNumber>) {
        self.override_sequence = sequence;
    }

    /// Index of a column in the projected batch by its column id.
    pub fn projected_index_by_id(&self, column_id: ColumnId) -> Option<usize> {
        self.format_projection
            .column_id_to_projected_index
            .get(&column_id)
            .copied()
    }

    /// Returns min values of specific column in row groups.
    pub fn min_values(
        &self,
        row_groups: &[impl Borrow<RowGroupMetaData>],
        column_id: ColumnId,
    ) -> StatValues {
        self.get_stat_values(row_groups, column_id, true)
    }

    /// Returns max values of specific column in row groups.
    pub fn max_values(
        &self,
        row_groups: &[impl Borrow<RowGroupMetaData>],
        column_id: ColumnId,
    ) -> StatValues {
        self.get_stat_values(row_groups, column_id, false)
    }

    /// Returns null counts of specific column in row groups.
    pub fn null_counts(
        &self,
        row_groups: &[impl Borrow<RowGroupMetaData>],
        column_id: ColumnId,
    ) -> StatValues {
        let Some(index) = self.column_id_to_sst_index.get(&column_id) else {
            // No such column in the SST.
            return StatValues::NoColumn;
        };

        let stats = ReadFormat::column_null_counts(row_groups, *index);
        StatValues::from_stats_opt(stats)
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
        &self.format_projection.projection_indices
    }

    /// Creates a sequence array to override.
    #[allow(dead_code)]
    pub(crate) fn new_override_sequence_array(&self, length: usize) -> Option<ArrayRef> {
        self.override_sequence
            .map(|seq| Arc::new(UInt64Array::from_value(seq, length)) as ArrayRef)
    }

    /// Convert a record batch to apply flat format conversion and override sequence array.
    ///
    /// Returns a new RecordBatch with flat format conversion applied first (if enabled),
    /// then the sequence column replaced by the override sequence array.
    #[allow(dead_code)]
    pub(crate) fn convert_batch(
        &self,
        record_batch: RecordBatch,
        override_sequence_array: Option<&ArrayRef>,
    ) -> Result<RecordBatch> {
        // First, apply flat format conversion if enabled
        let batch = if let Some(ref convert_format) = self.convert_format {
            convert_format.convert(record_batch)?
        } else {
            record_batch
        };

        // Then apply sequence override if provided
        let Some(override_array) = override_sequence_array else {
            return Ok(batch);
        };

        let mut columns = batch.columns().to_vec();
        let sequence_column_idx = sequence_column_index(batch.num_columns());

        // Use the provided override sequence array, slicing if necessary to match batch length
        let sequence_array = if override_array.len() > batch.num_rows() {
            override_array.slice(0, batch.num_rows())
        } else {
            override_array.clone()
        };

        columns[sequence_column_idx] = sequence_array;

        RecordBatch::try_new(batch.schema(), columns).context(NewRecordBatchSnafu)
    }

    /// Checks whether the batch from the parquet file needs to be converted to match the flat format.
    ///
    /// * `file_path` is the path to the parquet file, for error message.
    /// * `num_columns` is the number of columns in the parquet file.
    /// * `metadata` is the region metadata (always assumes flat format).
    #[allow(dead_code)]
    pub(crate) fn need_convert_to_flat(
        file_path: &str,
        num_columns: usize,
        metadata: &RegionMetadata,
    ) -> Result<bool> {
        // For flat format, compute expected column number:
        // all columns + internal columns (pk, sequence, op_type) - 1 (time index already counted)
        // FIXME(yingwen): use INTERNAL_COLUMN_NUM
        let expected_columns = metadata.column_metadatas.len() + FIXED_POS_COLUMN_NUM - 1;

        if expected_columns == num_columns {
            // Same number of columns, no conversion needed
            Ok(false)
        } else {
            ensure!(
                expected_columns >= num_columns,
                InvalidParquetSnafu {
                    file: file_path,
                    reason: format!(
                        "Expected columns {} should be >= actual columns {}",
                        expected_columns, num_columns
                    )
                }
            );

            // Different number of columns, check if the difference matches primary key count
            let column_diff = expected_columns - num_columns;

            ensure!(
                column_diff == metadata.primary_key.len(),
                InvalidParquetSnafu {
                    file: file_path,
                    reason: format!(
                        "Column number difference {} does not match primary key count {}",
                        column_diff,
                        metadata.primary_key.len()
                    )
                }
            );

            Ok(true)
        }
    }

    /// Returns the format projection.
    pub(crate) fn format_projection(&self) -> &FormatProjection {
        &self.format_projection
    }

    fn get_stat_values(
        &self,
        row_groups: &[impl Borrow<RowGroupMetaData>],
        column_id: ColumnId,
        is_min: bool,
    ) -> StatValues {
        let Some(column) = self.metadata.column_by_id(column_id) else {
            // No such column in the SST.
            return StatValues::NoColumn;
        };
        // Safety: `column_id_to_sst_index` is built from `metadata`.
        let index = self.column_id_to_sst_index.get(&column_id).unwrap();

        let stats = ReadFormat::column_values(row_groups, column, *index, is_min);
        StatValues::from_stats_opt(stats)
    }
}

/// Returns a map that the key is the column id and the value is the column position
/// in the SST.
/// It only supports SSTs with raw primary key columns.
pub(crate) fn sst_column_id_indices(metadata: &RegionMetadata) -> HashMap<ColumnId, usize> {
    let mut id_to_index = HashMap::with_capacity(metadata.column_metadatas.len());
    let mut column_index = 0;
    // keys
    for pk_id in &metadata.primary_key {
        id_to_index.insert(*pk_id, column_index);
        column_index += 1;
    }
    // fields
    for column in &metadata.column_metadatas {
        if column.semantic_type == SemanticType::Field {
            id_to_index.insert(column.column_id, column_index);
            column_index += 1;
        }
    }
    // time index
    id_to_index.insert(metadata.time_index_column().column_id, column_index);

    id_to_index
}

/// Converts a batch that doesn't have decoded primary key columns into a batch that has decoded
/// primary key columns in flat format.
pub(crate) struct FlatConvertFormat {
    /// Metadata of the region.
    metadata: RegionMetadataRef,
    /// Primary key codec to decode primary keys.
    codec: Arc<dyn PrimaryKeyCodec>,
    /// Projected primary key column information: (column_id, pk_index, column_index in metadata).
    projected_primary_keys: Vec<(ColumnId, usize, usize)>,
}

impl FlatConvertFormat {
    /// Creates a new `FlatConvertFormat`.
    ///
    /// The `format_projection` is the projection computed in the [FlatReadFormat] with the `metadata`.
    /// The `codec` is the primary key codec of the `metadata`.
    ///
    /// Returns `None` if there is no primary key.
    pub(crate) fn new(
        metadata: RegionMetadataRef,
        format_projection: &FormatProjection,
        codec: Arc<dyn PrimaryKeyCodec>,
    ) -> Option<Self> {
        if metadata.primary_key.is_empty() {
            return None;
        }

        // Builds projected primary keys list maintaining the order of RegionMetadata::primary_key
        let mut projected_primary_keys = Vec::new();
        for (pk_index, &column_id) in metadata.primary_key.iter().enumerate() {
            if format_projection
                .column_id_to_projected_index
                .contains_key(&column_id)
            {
                // We expect the format_projection is built from the metadata.
                let column_index = metadata.column_index_by_id(column_id).unwrap();
                projected_primary_keys.push((column_id, pk_index, column_index));
            }
        }

        Some(Self {
            metadata,
            codec,
            projected_primary_keys,
        })
    }

    /// Converts a batch to have decoded primary key columns in flat format.
    ///
    /// The primary key array in the batch is a dictionary array. We decode each value which is a
    /// primary key and reuse the keys array to build a dictionary array for each tag column.
    /// The decoded columns are inserted in front of other columns.
    pub(crate) fn convert(&self, batch: RecordBatch) -> Result<RecordBatch> {
        if self.projected_primary_keys.is_empty() {
            return Ok(batch);
        }

        let primary_key_index = primary_key_column_index(batch.num_columns());
        let pk_dict_array = batch
            .column(primary_key_index)
            .as_any()
            .downcast_ref::<PrimaryKeyArray>()
            .with_context(|| InvalidRecordBatchSnafu {
                reason: "Primary key column is not a dictionary array".to_string(),
            })?;

        let pk_values_array = pk_dict_array
            .values()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .with_context(|| InvalidRecordBatchSnafu {
                reason: "Primary key values are not binary array".to_string(),
            })?;

        // Decodes all primary key values
        let mut decoded_pk_values = Vec::with_capacity(pk_values_array.len());
        for i in 0..pk_values_array.len() {
            if pk_values_array.is_null(i) {
                decoded_pk_values.push(None);
            } else {
                let pk_bytes = pk_values_array.value(i);
                let decoded = self.codec.decode(pk_bytes).context(DecodeSnafu)?;
                decoded_pk_values.push(Some(decoded));
            }
        }

        // Builds decoded tag column arrays.
        let mut decoded_columns = Vec::new();
        for (column_id, pk_index, column_index) in &self.projected_primary_keys {
            let column_metadata = &self.metadata.column_metadatas[*column_index];
            let tag_column = self.build_primary_key_column(
                *column_id,
                *pk_index,
                &column_metadata.column_schema.data_type,
                pk_dict_array.keys(),
                &decoded_pk_values,
            )?;
            decoded_columns.push(tag_column);
        }

        // Builds new columns: decoded tag columns first, then original columns
        let mut new_columns = Vec::with_capacity(batch.num_columns() + decoded_columns.len());
        new_columns.extend(decoded_columns);
        new_columns.extend_from_slice(batch.columns());

        // Builds new schema
        let mut new_fields =
            Vec::with_capacity(batch.schema().fields().len() + self.projected_primary_keys.len());
        for (_, _, column_index) in &self.projected_primary_keys {
            let column_metadata = &self.metadata.column_metadatas[*column_index];
            let field = Field::new(
                &column_metadata.column_schema.name,
                column_metadata.column_schema.data_type.as_arrow_type(),
                column_metadata.column_schema.is_nullable(),
            );
            new_fields.push(Arc::new(field));
        }
        new_fields.extend(batch.schema().fields().iter().cloned());

        let new_schema = Arc::new(Schema::new(new_fields));
        RecordBatch::try_new(new_schema, new_columns).context(NewRecordBatchSnafu)
    }

    /// Builds an array for a specific tag column.
    ///
    /// It may build a dictionary array if the type is string. Note that the dictionary
    /// array may have null values, although keys are not null.
    fn build_primary_key_column(
        &self,
        column_id: ColumnId,
        pk_index: usize,
        column_type: &ConcreteDataType,
        keys: &UInt32Array,
        decoded_pk_values: &[Option<CompositeValues>],
    ) -> Result<ArrayRef> {
        // Gets values from the primary key.
        let mut builder = column_type.create_mutable_vector(decoded_pk_values.len());
        for decoded_opt in decoded_pk_values {
            match decoded_opt {
                Some(decoded) => {
                    match decoded {
                        CompositeValues::Dense(dense) => {
                            if pk_index < dense.len() {
                                builder.push_value_ref(dense[pk_index].1.as_value_ref());
                            } else {
                                builder.push_null();
                            }
                        }
                        CompositeValues::Sparse(sparse) => {
                            let value = sparse.get_or_null(column_id);
                            builder.push_value_ref(value.as_value_ref());
                        }
                    };
                }
                None => builder.push_null(),
            }
        }

        let values_vector = builder.to_vector();
        let values_array = values_vector.to_arrow_array();

        // Only creates dictionary array for string types, otherwise take values by keys
        if matches!(column_type, ConcreteDataType::String(_)) {
            // Creates dictionary array using the same keys for string types
            // Note that the dictionary values may have nulls.
            let dict_array = DictionaryArray::new(keys.clone(), values_array);
            Ok(Arc::new(dict_array))
        } else {
            // For non-string types, takes values by keys indices to create a regular array
            let taken_array = take(&values_array, keys, None).context(ComputeArrowSnafu)?;
            Ok(taken_array)
        }
    }
}

#[cfg(test)]
impl FlatReadFormat {
    /// Creates a helper with existing `metadata` and all columns.
    pub fn new_with_all_columns(metadata: RegionMetadataRef) -> FlatReadFormat {
        Self::new(
            Arc::clone(&metadata),
            metadata.column_metadatas.iter().map(|c| c.column_id),
            false,
        )
    }
}
