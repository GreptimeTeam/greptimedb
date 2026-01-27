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
use datatypes::arrow::datatypes::{Schema, SchemaRef};
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::prelude::{ConcreteDataType, DataType};
use mito_codec::row_converter::{CompositeValues, PrimaryKeyCodec, build_primary_key_codec};
use parquet::file::metadata::RowGroupMetaData;
use snafu::{OptionExt, ResultExt, ensure};
use store_api::codec::PrimaryKeyEncoding;
use store_api::metadata::{RegionMetadata, RegionMetadataRef};
use store_api::storage::{ColumnId, SequenceNumber};

use crate::error::{
    ComputeArrowSnafu, DecodeSnafu, InvalidParquetSnafu, InvalidRecordBatchSnafu,
    NewRecordBatchSnafu, Result,
};
use crate::sst::parquet::format::{
    FormatProjection, INTERNAL_COLUMN_NUM, PrimaryKeyArray, PrimaryKeyReadFormat, ReadFormat,
    StatValues,
};
use crate::sst::{
    FlatSchemaOptions, flat_sst_arrow_schema_column_num, tag_maybe_to_dictionary_field,
    to_flat_sst_arrow_schema,
};

/// Helper for writing the SST format.
pub(crate) struct FlatWriteFormat {
    /// SST file schema.
    arrow_schema: SchemaRef,
    override_sequence: Option<SequenceNumber>,
}

impl FlatWriteFormat {
    /// Creates a new helper.
    pub(crate) fn new(metadata: RegionMetadataRef, options: &FlatSchemaOptions) -> FlatWriteFormat {
        let arrow_schema = to_flat_sst_arrow_schema(&metadata, options);
        FlatWriteFormat {
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

// TODO(yingwen): Add an option to skip reading internal columns if the region is
// append only and doesn't use sparse encoding (We need to check the table id under
// sparse encoding).
/// Helper for reading the flat SST format with projection.
///
/// It only supports flat format that stores primary keys additionally.
pub struct FlatReadFormat {
    /// Sequence number to override the sequence read from the SST.
    override_sequence: Option<SequenceNumber>,
    /// Parquet format adapter.
    parquet_adapter: ParquetAdapter,
}

impl FlatReadFormat {
    /// Creates a helper with existing `metadata` and `column_ids` to read.
    ///
    /// If `skip_auto_convert` is true, skips auto conversion of format when the encoding is sparse encoding.
    pub fn new(
        metadata: RegionMetadataRef,
        column_ids: impl Iterator<Item = ColumnId>,
        num_columns: Option<usize>,
        file_path: &str,
        skip_auto_convert: bool,
    ) -> Result<FlatReadFormat> {
        let is_legacy = match num_columns {
            Some(num) => Self::is_legacy_format(&metadata, num, file_path)?,
            None => metadata.primary_key_encoding == PrimaryKeyEncoding::Sparse,
        };

        let parquet_adapter = if is_legacy {
            // Safety: is_legacy_format() ensures primary_key is not empty.
            if metadata.primary_key_encoding == PrimaryKeyEncoding::Sparse {
                // Only skip auto convert when the primary key encoding is sparse.
                ParquetAdapter::PrimaryKeyToFlat(ParquetPrimaryKeyToFlat::new(
                    metadata,
                    column_ids,
                    skip_auto_convert,
                ))
            } else {
                ParquetAdapter::PrimaryKeyToFlat(ParquetPrimaryKeyToFlat::new(
                    metadata, column_ids, false,
                ))
            }
        } else {
            ParquetAdapter::Flat(ParquetFlat::new(metadata, column_ids))
        };

        Ok(FlatReadFormat {
            override_sequence: None,
            parquet_adapter,
        })
    }

    /// Sets the sequence number to override.
    pub(crate) fn set_override_sequence(&mut self, sequence: Option<SequenceNumber>) {
        self.override_sequence = sequence;
    }

    /// Index of a column in the projected batch by its column id.
    pub fn projected_index_by_id(&self, column_id: ColumnId) -> Option<usize> {
        self.format_projection()
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
        match &self.parquet_adapter {
            ParquetAdapter::Flat(p) => p.min_values(row_groups, column_id),
            ParquetAdapter::PrimaryKeyToFlat(p) => p.format.min_values(row_groups, column_id),
        }
    }

    /// Returns max values of specific column in row groups.
    pub fn max_values(
        &self,
        row_groups: &[impl Borrow<RowGroupMetaData>],
        column_id: ColumnId,
    ) -> StatValues {
        match &self.parquet_adapter {
            ParquetAdapter::Flat(p) => p.max_values(row_groups, column_id),
            ParquetAdapter::PrimaryKeyToFlat(p) => p.format.max_values(row_groups, column_id),
        }
    }

    /// Returns null counts of specific column in row groups.
    pub fn null_counts(
        &self,
        row_groups: &[impl Borrow<RowGroupMetaData>],
        column_id: ColumnId,
    ) -> StatValues {
        match &self.parquet_adapter {
            ParquetAdapter::Flat(p) => p.null_counts(row_groups, column_id),
            ParquetAdapter::PrimaryKeyToFlat(p) => p.format.null_counts(row_groups, column_id),
        }
    }

    /// Gets the arrow schema of the SST file.
    ///
    /// This schema is computed from the region metadata but should be the same
    /// as the arrow schema decoded from the file metadata.
    pub(crate) fn arrow_schema(&self) -> &SchemaRef {
        match &self.parquet_adapter {
            ParquetAdapter::Flat(p) => &p.arrow_schema,
            ParquetAdapter::PrimaryKeyToFlat(p) => p.format.arrow_schema(),
        }
    }

    /// Gets the metadata of the SST.
    pub(crate) fn metadata(&self) -> &RegionMetadataRef {
        match &self.parquet_adapter {
            ParquetAdapter::Flat(p) => &p.metadata,
            ParquetAdapter::PrimaryKeyToFlat(p) => p.format.metadata(),
        }
    }

    /// Gets sorted projection indices to read from the SST file.
    pub(crate) fn projection_indices(&self) -> &[usize] {
        match &self.parquet_adapter {
            ParquetAdapter::Flat(p) => &p.format_projection.projection_indices,
            ParquetAdapter::PrimaryKeyToFlat(p) => p.format.projection_indices(),
        }
    }

    /// Gets the projection in the flat format.
    pub(crate) fn format_projection(&self) -> &FormatProjection {
        match &self.parquet_adapter {
            ParquetAdapter::Flat(p) => &p.format_projection,
            ParquetAdapter::PrimaryKeyToFlat(p) => p
                .old_format_projection
                .as_ref()
                .unwrap_or(&p.format_projection),
        }
    }

    /// Creates a sequence array to override.
    pub(crate) fn new_override_sequence_array(&self, length: usize) -> Option<ArrayRef> {
        self.override_sequence
            .map(|seq| Arc::new(UInt64Array::from_value(seq, length)) as ArrayRef)
    }

    /// Convert a record batch to apply flat format conversion and override sequence array.
    ///
    /// Returns a new RecordBatch with flat format conversion applied first (if enabled),
    /// then the sequence column replaced by the override sequence array.
    pub(crate) fn convert_batch(
        &self,
        record_batch: RecordBatch,
        override_sequence_array: Option<&ArrayRef>,
    ) -> Result<RecordBatch> {
        // First, apply flat format conversion.
        let batch = match &self.parquet_adapter {
            ParquetAdapter::Flat(_) => record_batch,
            ParquetAdapter::PrimaryKeyToFlat(p) => p.convert_batch(record_batch)?,
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
    /// * `metadata` is the region metadata (always assumes flat format).
    /// * `num_columns` is the number of columns in the parquet file.
    /// * `file_path` is the path to the parquet file, for error message.
    pub(crate) fn is_legacy_format(
        metadata: &RegionMetadata,
        num_columns: usize,
        file_path: &str,
    ) -> Result<bool> {
        if metadata.primary_key.is_empty() {
            return Ok(false);
        }

        // For flat format, compute expected column number:
        // all columns + internal columns (pk, sequence, op_type)
        let expected_columns = metadata.column_metadatas.len() + INTERNAL_COLUMN_NUM;

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
}

/// Wraps the parquet helper for different formats.
enum ParquetAdapter {
    Flat(ParquetFlat),
    PrimaryKeyToFlat(ParquetPrimaryKeyToFlat),
}

/// Helper to reads the parquet from primary key format into the flat format.
struct ParquetPrimaryKeyToFlat {
    /// The primary key format to read the parquet.
    format: PrimaryKeyReadFormat,
    /// Format converter for handling flat format conversion.
    convert_format: Option<FlatConvertFormat>,
    /// Projection computed for the flat format.
    format_projection: FormatProjection,
    /// Whether to skip auto convert.
    old_format_projection: Option<FormatProjection>,
}

impl ParquetPrimaryKeyToFlat {
    /// Creates a helper with existing `metadata` and `column_ids` to read.
    fn new(
        metadata: RegionMetadataRef,
        column_ids: impl Iterator<Item = ColumnId>,
        skip_auto_convert: bool,
    ) -> ParquetPrimaryKeyToFlat {
        assert!(if skip_auto_convert {
            metadata.primary_key_encoding == PrimaryKeyEncoding::Sparse
        } else {
            true
        });

        let column_ids: Vec<_> = column_ids.collect();

        // Creates a map to lookup index based on the new format.
        let id_to_index = sst_column_id_indices(&metadata);
        let sst_column_num =
            flat_sst_arrow_schema_column_num(&metadata, &FlatSchemaOptions::default());

        // Computes the format projection for the new format.
        let format_projection = FormatProjection::compute_format_projection(
            &id_to_index,
            sst_column_num,
            column_ids.iter().copied(),
        );
        let codec = build_primary_key_codec(&metadata);
        let convert_format = if skip_auto_convert {
            None
        } else {
            FlatConvertFormat::new(Arc::clone(&metadata), &format_projection, codec)
        };

        let format = PrimaryKeyReadFormat::new(metadata.clone(), column_ids.iter().copied());
        let old_format_projection = if skip_auto_convert {
            Some(FormatProjection {
                projection_indices: format.projection_indices().to_vec(),
                column_id_to_projected_index: format.field_id_to_projected_index().clone(),
            })
        } else {
            None
        };

        Self {
            format,
            convert_format,
            format_projection,
            old_format_projection,
        }
    }

    fn convert_batch(&self, record_batch: RecordBatch) -> Result<RecordBatch> {
        if let Some(convert_format) = &self.convert_format {
            convert_format.convert(record_batch)
        } else {
            Ok(record_batch)
        }
    }
}

/// Helper to reads the parquet in flat format directly.
struct ParquetFlat {
    /// The metadata stored in the SST.
    metadata: RegionMetadataRef,
    /// SST file schema.
    arrow_schema: SchemaRef,
    /// Projection computed for the flat format.
    format_projection: FormatProjection,
    /// Column id to index in SST.
    column_id_to_sst_index: HashMap<ColumnId, usize>,
}

impl ParquetFlat {
    /// Creates a helper with existing `metadata` and `column_ids` to read.
    fn new(metadata: RegionMetadataRef, column_ids: impl Iterator<Item = ColumnId>) -> ParquetFlat {
        // Creates a map to lookup index.
        let id_to_index = sst_column_id_indices(&metadata);
        let arrow_schema = to_flat_sst_arrow_schema(&metadata, &FlatSchemaOptions::default());
        let sst_column_num =
            flat_sst_arrow_schema_column_num(&metadata, &FlatSchemaOptions::default());
        let format_projection =
            FormatProjection::compute_format_projection(&id_to_index, sst_column_num, column_ids);

        Self {
            metadata,
            arrow_schema,
            format_projection,
            column_id_to_sst_index: id_to_index,
        }
    }

    /// Returns min values of specific column in row groups.
    fn min_values(
        &self,
        row_groups: &[impl Borrow<RowGroupMetaData>],
        column_id: ColumnId,
    ) -> StatValues {
        self.get_stat_values(row_groups, column_id, true)
    }

    /// Returns max values of specific column in row groups.
    fn max_values(
        &self,
        row_groups: &[impl Borrow<RowGroupMetaData>],
        column_id: ColumnId,
    ) -> StatValues {
        self.get_stat_values(row_groups, column_id, false)
    }

    /// Returns null counts of specific column in row groups.
    fn null_counts(
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

/// Decodes primary keys from a batch and returns decoded primary key information.
///
/// The batch must contain a primary key column at the expected index.
pub(crate) fn decode_primary_keys(
    codec: &dyn PrimaryKeyCodec,
    batch: &RecordBatch,
) -> Result<DecodedPrimaryKeys> {
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

    let keys = pk_dict_array.keys();

    // Decodes primary key values by iterating through keys, reusing decoded values for duplicate keys.
    // Maps original key index -> new decoded value index
    let mut key_to_decoded_index = Vec::with_capacity(keys.len());
    let mut decoded_pk_values = Vec::new();
    let mut prev_key: Option<u32> = None;

    // The parquet reader may read the whole dictionary page into the dictionary values, so
    // we may decode many primary keys not in this batch if we decode the values array directly.
    let pk_indices = keys.values();
    for &current_key in pk_indices.iter().take(keys.len()) {
        // Check if current key is the same as previous key
        if let Some(prev) = prev_key
            && prev == current_key
        {
            // Reuse the last decoded index
            key_to_decoded_index.push((decoded_pk_values.len() - 1) as u32);
            continue;
        }

        // New key, decodes the value
        let pk_bytes = pk_values_array.value(current_key as usize);
        let decoded_value = codec.decode(pk_bytes).context(DecodeSnafu)?;

        decoded_pk_values.push(decoded_value);
        key_to_decoded_index.push((decoded_pk_values.len() - 1) as u32);
        prev_key = Some(current_key);
    }

    // Create the keys array from key_to_decoded_index
    let keys_array = UInt32Array::from(key_to_decoded_index);

    Ok(DecodedPrimaryKeys {
        decoded_pk_values,
        keys_array,
    })
}

/// Holds decoded primary key values and their indices.
pub(crate) struct DecodedPrimaryKeys {
    /// Decoded primary key values for unique keys in the dictionary.
    decoded_pk_values: Vec<CompositeValues>,
    /// Prebuilt keys array for creating dictionary arrays.
    keys_array: UInt32Array,
}

impl DecodedPrimaryKeys {
    /// Gets a tag column array by column id and data type.
    ///
    /// For sparse encoding, uses column_id to lookup values.
    /// For dense encoding, uses pk_index to get values.
    pub(crate) fn get_tag_column(
        &self,
        column_id: ColumnId,
        pk_index: Option<usize>,
        column_type: &ConcreteDataType,
    ) -> Result<ArrayRef> {
        // Gets values from the primary key.
        let mut builder = column_type.create_mutable_vector(self.decoded_pk_values.len());
        for decoded in &self.decoded_pk_values {
            match decoded {
                CompositeValues::Dense(dense) => {
                    let pk_idx = pk_index.expect("pk_index required for dense encoding");
                    if pk_idx < dense.len() {
                        builder.push_value_ref(&dense[pk_idx].1.as_value_ref());
                    } else {
                        builder.push_null();
                    }
                }
                CompositeValues::Sparse(sparse) => {
                    let value = sparse.get_or_null(column_id);
                    builder.push_value_ref(&value.as_value_ref());
                }
            };
        }

        let values_vector = builder.to_vector();
        let values_array = values_vector.to_arrow_array();

        // Only creates dictionary array for string types, otherwise take values by keys
        if column_type.is_string() {
            // Creates dictionary array using the same keys for string types
            // Note that the dictionary values may have nulls.
            let dict_array = DictionaryArray::new(self.keys_array.clone(), values_array);
            Ok(Arc::new(dict_array))
        } else {
            // For non-string types, takes values by keys indices to create a regular array
            let taken_array =
                take(&values_array, &self.keys_array, None).context(ComputeArrowSnafu)?;
            Ok(taken_array)
        }
    }
}

/// Converts a batch that doesn't have decoded primary key columns into a batch that has decoded
/// primary key columns in flat format.
#[derive(Debug)]
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
    /// The primary key array in the batch is a dictionary array.
    pub(crate) fn convert(&self, batch: RecordBatch) -> Result<RecordBatch> {
        if self.projected_primary_keys.is_empty() {
            return Ok(batch);
        }

        let decoded_pks = decode_primary_keys(self.codec.as_ref(), &batch)?;

        // Builds decoded tag column arrays.
        let mut decoded_columns = Vec::new();
        for (column_id, pk_index, column_index) in &self.projected_primary_keys {
            let column_metadata = &self.metadata.column_metadatas[*column_index];
            let tag_column = decoded_pks.get_tag_column(
                *column_id,
                Some(*pk_index),
                &column_metadata.column_schema.data_type,
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
            let old_field = &self.metadata.schema.arrow_schema().fields()[*column_index];
            let field =
                tag_maybe_to_dictionary_field(&column_metadata.column_schema.data_type, old_field);
            new_fields.push(field);
        }
        new_fields.extend(batch.schema().fields().iter().cloned());

        let new_schema = Arc::new(Schema::new(new_fields));
        RecordBatch::try_new(new_schema, new_columns).context(NewRecordBatchSnafu)
    }
}

#[cfg(test)]
impl FlatReadFormat {
    /// Creates a helper with existing `metadata` and all columns.
    pub fn new_with_all_columns(metadata: RegionMetadataRef) -> FlatReadFormat {
        Self::new(
            Arc::clone(&metadata),
            metadata.column_metadatas.iter().map(|c| c.column_id),
            None,
            "test",
            false,
        )
        .unwrap()
    }
}
