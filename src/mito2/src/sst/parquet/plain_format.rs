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
//! We store two additional internal columns at last:
//! - `__sequence`, the sequence number of a row. Type: uint64
//! - `__op_type`, the op type of the row. Type: uint8
//!
//! We store other columns in the same order as [RegionMetadata::field_columns()](store_api::metadata::RegionMetadata::field_columns()).
//!

use std::borrow::Borrow;
use std::collections::HashMap;
use std::sync::Arc;

use common_time::Timestamp;
use datafusion_common::ScalarValue;
use datatypes::arrow::array::{ArrayRef, UInt64Array};
use datatypes::arrow::datatypes::SchemaRef;
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::prelude::DataType;
use parquet::file::metadata::{ParquetMetaData, RowGroupMetaData};
use parquet::file::statistics::Statistics;
use snafu::ResultExt;
use store_api::metadata::{ColumnMetadata, RegionMetadata, RegionMetadataRef};
use store_api::storage::{ColumnId, SequenceNumber};

use crate::error::{NewRecordBatchSnafu, Result};
use crate::read::plain_batch::PlainBatch;
use crate::sst::file::{FileId, FileTimeRange};
use crate::sst::parquet::format::StatValues;
use crate::sst::to_plain_sst_arrow_schema;

/// Number of columns that have fixed positions.
///
/// Contains all internal columns.
pub(crate) const PLAIN_FIXED_POS_COLUMN_NUM: usize = 2;

/// Helper for writing the SST format.
pub(crate) struct PlainWriteFormat {
    metadata: RegionMetadataRef,
    /// SST file schema.
    arrow_schema: SchemaRef,
    override_sequence: Option<SequenceNumber>,
}

impl PlainWriteFormat {
    /// Creates a new helper.
    pub(crate) fn new(metadata: RegionMetadataRef) -> PlainWriteFormat {
        let arrow_schema = to_plain_sst_arrow_schema(&metadata);
        PlainWriteFormat {
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
    pub(crate) fn convert_batch(&self, batch: &PlainBatch) -> Result<RecordBatch> {
        debug_assert_eq!(batch.num_columns(), self.arrow_schema.fields().len());

        let Some(override_sequence) = self.override_sequence else {
            return Ok(batch.as_record_batch().clone());
        };

        let mut columns = batch.columns().to_vec();
        let sequence_array = Arc::new(UInt64Array::from(vec![override_sequence; batch.num_rows()]));
        columns[batch.sequence_column_index()] = sequence_array;

        RecordBatch::try_new(self.arrow_schema.clone(), columns).context(NewRecordBatchSnafu)
    }
}

/// Helper for reading the SST format.
pub struct PlainReadFormat {
    /// The metadata stored in the SST.
    metadata: RegionMetadataRef,
    /// SST file schema.
    arrow_schema: SchemaRef,
    /// Indices of columns to read from the SST. It contains all internal columns.
    projection_indices: Vec<usize>,
    /// Column id to their index in the projected schema (
    /// the schema after projection).
    column_id_to_projected_index: HashMap<ColumnId, usize>,
}

impl PlainReadFormat {
    /// Creates a helper with existing `metadata` and `column_ids` to read.
    pub fn new(
        metadata: RegionMetadataRef,
        column_ids: impl Iterator<Item = ColumnId>,
    ) -> PlainReadFormat {
        let arrow_schema = to_plain_sst_arrow_schema(&metadata);

        // Maps column id of a projected column to its index in SST.
        // The metadata and the SST have the same column order.
        // [(column id, index in SST)]
        let mut projected_column_id_index: Vec<_> = column_ids
            .filter_map(|column_id| {
                metadata
                    .column_index_by_id(column_id)
                    .map(|index| (column_id, index))
            })
            .collect();
        // Collect all projected indices.
        let mut projection_indices: Vec<_> = projected_column_id_index
            .iter()
            .map(|(_column_id, index)| *index)
            // We need to add all fixed position columns.
            .chain(
                arrow_schema.fields.len() - PLAIN_FIXED_POS_COLUMN_NUM..arrow_schema.fields.len(),
            )
            .collect();
        projection_indices.sort_unstable();

        // Sort columns by their indices in the SST. Then the output order is their order
        // in the batch returned from the SST.
        projected_column_id_index.sort_unstable_by_key(|x| x.1);
        // Then we map the column id to the index of the column id in `projected_column_id_index`.
        // So we can get the index of the column id in the batch returned from the SST.
        let column_id_to_projected_index = projected_column_id_index
            .into_iter()
            .map(|(column_id, _)| column_id)
            .enumerate()
            .map(|(index, column_id)| (column_id, index))
            .collect();

        PlainReadFormat {
            metadata,
            arrow_schema,
            projection_indices,
            column_id_to_projected_index,
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

    /// Gets sorted projection indices to read.
    pub(crate) fn projection_indices(&self) -> &[usize] {
        &self.projection_indices
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
        let Some(column_index) = self.metadata.column_index_by_id(column_id) else {
            // No such column in the SST.
            return StatValues::NoColumn;
        };
        let column = &self.metadata.column_metadatas[column_index];
        let stats = Self::column_values(row_groups, column, column_index, true);
        StatValues::from_stats_opt(stats)
    }

    /// Returns max values of specific column in row groups.
    pub fn max_values(
        &self,
        row_groups: &[impl Borrow<RowGroupMetaData>],
        column_id: ColumnId,
    ) -> StatValues {
        let Some(column_index) = self.metadata.column_index_by_id(column_id) else {
            // No such column in the SST.
            return StatValues::NoColumn;
        };
        let column = &self.metadata.column_metadatas[column_index];
        let stats = Self::column_values(row_groups, column, column_index, false);
        StatValues::from_stats_opt(stats)
    }

    /// Returns null counts of specific column in row groups.
    pub fn null_counts(
        &self,
        row_groups: &[impl Borrow<RowGroupMetaData>],
        column_id: ColumnId,
    ) -> StatValues {
        let Some(column_index) = self.metadata.column_index_by_id(column_id) else {
            // No such column in the SST.
            return StatValues::NoColumn;
        };
        let stats = Self::column_null_counts(row_groups, column_index);
        StatValues::from_stats_opt(stats)
    }

    /// Returns min/max values of specific column.
    /// Returns None if the column does not have statistics.
    fn column_values(
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

    /// Returns null counts of specific column.
    fn column_null_counts(
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
}

#[cfg(test)]
impl PlainReadFormat {
    /// Creates a helper with existing `metadata` and all columns.
    pub fn new_with_all_columns(metadata: RegionMetadataRef) -> PlainReadFormat {
        Self::new(
            Arc::clone(&metadata),
            metadata.column_metadatas.iter().map(|c| c.column_id),
        )
    }
}

/// Gets the min/max time index of the SST from the parquet meta.
/// It assumes the parquet is created by the mito engine.
pub(crate) fn parquet_time_range(
    file_id: FileId,
    parquet_meta: &ParquetMetaData,
    metadata: &RegionMetadata,
) -> Option<FileTimeRange> {
    let time_index_pos = metadata.time_index_column_pos();
    // Safety: The time index column is valid.
    let time_unit = metadata
        .time_index_column()
        .column_schema
        .data_type
        .as_timestamp()
        .unwrap()
        .unit();
    let mut time_range: Option<FileTimeRange> = None;
    for row_group_meta in parquet_meta.row_groups() {
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
                    file_id
                );
                return None;
            }
        };
        let (min_in_rg, max_in_rg) = (
            Timestamp::new(min, time_unit),
            Timestamp::new(max, time_unit),
        );
        if let Some(range) = &mut time_range {
            range.0 = range.0.min(min_in_rg);
            range.1 = range.1.max(max_in_rg);
        } else {
            time_range = Some((min_in_rg, max_in_rg));
        }
    }

    time_range
}
