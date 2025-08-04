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
use datatypes::arrow::array::{ArrayRef, UInt64Array};
use datatypes::arrow::datatypes::SchemaRef;
use datatypes::arrow::record_batch::RecordBatch;
use parquet::file::metadata::RowGroupMetaData;
use snafu::ResultExt;
use store_api::metadata::{RegionMetadata, RegionMetadataRef};
use store_api::storage::{ColumnId, SequenceNumber};

use crate::error::{NewRecordBatchSnafu, Result};
use crate::sst::parquet::format::{FormatProjection, ReadFormat, StatValues};
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

// TODO(yingwen): Add an option to skip reading internal columns.
/// Helper for reading the flat SST format with projection.
///
/// It only supports flat format that stores primary keys additionally.
pub struct FlatReadFormat {
    /// The metadata stored in the SST.
    metadata: RegionMetadataRef,
    /// SST file schema.
    arrow_schema: SchemaRef,
    /// Indices of columns to read from the SST. It contains all internal columns.
    projection_indices: Vec<usize>,
    /// Column id to their index in the projected schema (
    /// the schema after projection).
    column_id_to_projected_index: HashMap<ColumnId, usize>,
    /// Column id to index in SST.
    column_id_to_sst_index: HashMap<ColumnId, usize>,
    /// Sequence number to override the sequence read from the SST.
    override_sequence: Option<SequenceNumber>,
}

impl FlatReadFormat {
    /// Creates a helper with existing `metadata` and `column_ids` to read.
    pub fn new(
        metadata: RegionMetadataRef,
        column_ids: impl Iterator<Item = ColumnId>,
    ) -> FlatReadFormat {
        let arrow_schema = to_flat_sst_arrow_schema(&metadata, &FlatSchemaOptions::default());

        // Creates a map to lookup index.
        let id_to_index = sst_column_id_indices(&metadata);

        let format_projection = FormatProjection::compute_format_projection(
            &id_to_index,
            arrow_schema.fields.len(),
            column_ids,
        );

        FlatReadFormat {
            metadata,
            arrow_schema,
            projection_indices: format_projection.projection_indices,
            column_id_to_projected_index: format_projection.column_id_to_projected_index,
            column_id_to_sst_index: id_to_index,
            override_sequence: None,
        }
    }

    /// Sets the sequence number to override.
    #[allow(dead_code)]
    pub(crate) fn set_override_sequence(&mut self, sequence: Option<SequenceNumber>) {
        self.override_sequence = sequence;
    }

    /// Index of a column in the projected batch by its column id.
    pub fn projected_index_by_id(&self, column_id: ColumnId) -> Option<usize> {
        self.column_id_to_projected_index.get(&column_id).copied()
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
        &self.projection_indices
    }

    /// Creates a sequence array to override.
    #[allow(dead_code)]
    pub(crate) fn new_override_sequence_array(&self, length: usize) -> Option<ArrayRef> {
        self.override_sequence
            .map(|seq| Arc::new(UInt64Array::from_value(seq, length)) as ArrayRef)
    }

    /// Convert a record batch to apply override sequence array.
    ///
    /// Returns a new RecordBatch with the sequence column replaced by the override sequence array.
    #[allow(dead_code)]
    pub(crate) fn convert_batch(
        &self,
        record_batch: &RecordBatch,
        override_sequence_array: Option<&ArrayRef>,
    ) -> Result<RecordBatch> {
        let Some(override_array) = override_sequence_array else {
            return Ok(record_batch.clone());
        };

        let mut columns = record_batch.columns().to_vec();
        let sequence_column_idx = sequence_column_index(record_batch.num_columns());

        // Use the provided override sequence array, slicing if necessary to match batch length
        let sequence_array = if override_array.len() > record_batch.num_rows() {
            override_array.slice(0, record_batch.num_rows())
        } else {
            override_array.clone()
        };

        columns[sequence_column_idx] = sequence_array;

        RecordBatch::try_new(record_batch.schema(), columns).context(NewRecordBatchSnafu)
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
fn sst_column_id_indices(metadata: &RegionMetadata) -> HashMap<ColumnId, usize> {
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

#[cfg(test)]
impl FlatReadFormat {
    /// Creates a helper with existing `metadata` and all columns.
    pub fn new_with_all_columns(metadata: RegionMetadataRef) -> FlatReadFormat {
        Self::new(
            Arc::clone(&metadata),
            metadata.column_metadatas.iter().map(|c| c.column_id),
        )
    }
}
