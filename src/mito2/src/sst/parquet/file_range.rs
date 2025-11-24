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

//! Structs and functions for reading ranges from a parquet file. A file range
//! is usually a row group in a parquet file.

use std::collections::HashMap;
use std::ops::BitAnd;
use std::sync::Arc;

use api::v1::{OpType, SemanticType};
use common_telemetry::error;
use datatypes::arrow::array::{ArrayRef, BooleanArray};
use datatypes::arrow::buffer::BooleanBuffer;
use datatypes::arrow::record_batch::RecordBatch;
use mito_codec::row_converter::{CompositeValues, PrimaryKeyCodec};
use parquet::arrow::arrow_reader::RowSelection;
use parquet::file::metadata::ParquetMetaData;
use snafu::{OptionExt, ResultExt};
use store_api::codec::PrimaryKeyEncoding;
use store_api::storage::{ColumnId, TimeSeriesRowSelector};

use crate::error::{
    ComputeArrowSnafu, DataTypeMismatchSnafu, DecodeSnafu, DecodeStatsSnafu, RecordBatchSnafu,
    Result, StatsNotPresentSnafu,
};
use crate::read::Batch;
use crate::read::compat::CompatBatch;
use crate::read::last_row::RowGroupLastRowCachedReader;
use crate::read::prune::{FlatPruneReader, PruneReader};
use crate::sst::file::FileHandle;
use crate::sst::parquet::flat_format::{DecodedPrimaryKeys, decode_primary_keys};
use crate::sst::parquet::format::ReadFormat;
use crate::sst::parquet::reader::{
    FlatRowGroupReader, MaybeFilter, RowGroupReader, RowGroupReaderBuilder, SimpleFilterContext,
};
use crate::sst::parquet::row_group::ParquetFetchMetrics;

/// Checks if a row group contains delete operations by examining the min value of op_type column.
///
/// Returns `Ok(true)` if the row group contains delete operations, `Ok(false)` if it doesn't,
/// or an error if the statistics are not present or cannot be decoded.
pub(crate) fn row_group_contains_delete(
    parquet_meta: &ParquetMetaData,
    row_group_index: usize,
    file_path: &str,
) -> Result<bool> {
    let row_group_metadata = &parquet_meta.row_groups()[row_group_index];

    // safety: The last column of SST must be op_type
    let column_metadata = &row_group_metadata.columns().last().unwrap();
    let stats = column_metadata
        .statistics()
        .context(StatsNotPresentSnafu { file_path })?;
    stats
        .min_bytes_opt()
        .context(StatsNotPresentSnafu { file_path })?
        .try_into()
        .map(i32::from_le_bytes)
        .map(|min_op_type| min_op_type == OpType::Delete as i32)
        .ok()
        .context(DecodeStatsSnafu { file_path })
}

/// A range of a parquet SST. Now it is a row group.
/// We can read different file ranges in parallel.
#[derive(Clone)]
pub struct FileRange {
    /// Shared context.
    context: FileRangeContextRef,
    /// Index of the row group in the SST.
    row_group_idx: usize,
    /// Row selection for the row group. `None` means all rows.
    row_selection: Option<RowSelection>,
}

impl FileRange {
    /// Creates a new [FileRange].
    pub(crate) fn new(
        context: FileRangeContextRef,
        row_group_idx: usize,
        row_selection: Option<RowSelection>,
    ) -> Self {
        Self {
            context,
            row_group_idx,
            row_selection,
        }
    }

    /// Returns true if [FileRange] selects all rows in row group.
    fn select_all(&self) -> bool {
        let rows_in_group = self
            .context
            .reader_builder
            .parquet_metadata()
            .row_group(self.row_group_idx)
            .num_rows();

        let Some(row_selection) = &self.row_selection else {
            return true;
        };
        row_selection.row_count() == rows_in_group as usize
    }

    /// Returns a reader to read the [FileRange].
    pub(crate) async fn reader(
        &self,
        selector: Option<TimeSeriesRowSelector>,
        fetch_metrics: &ParquetFetchMetrics,
    ) -> Result<PruneReader> {
        let parquet_reader = self
            .context
            .reader_builder
            .build(
                self.row_group_idx,
                self.row_selection.clone(),
                fetch_metrics,
            )
            .await?;

        let use_last_row_reader = if selector
            .map(|s| s == TimeSeriesRowSelector::LastRow)
            .unwrap_or(false)
        {
            // Only use LastRowReader if row group does not contain DELETE
            // and all rows are selected.
            let put_only = !self
                .context
                .contains_delete(self.row_group_idx)
                .inspect_err(|e| {
                    error!(e; "Failed to decode min value of op_type, fallback to RowGroupReader");
                })
                .unwrap_or(true);
            put_only && self.select_all()
        } else {
            // No selector provided, use RowGroupReader
            false
        };

        // Compute skip_fields once for this row group
        let skip_fields = self.context.should_skip_fields(self.row_group_idx);

        let prune_reader = if use_last_row_reader {
            // Row group is PUT only, use LastRowReader to skip unnecessary rows.
            let reader = RowGroupLastRowCachedReader::new(
                self.file_handle().file_id().file_id(),
                self.row_group_idx,
                self.context.reader_builder.cache_strategy().clone(),
                RowGroupReader::new(self.context.clone(), parquet_reader),
            );
            PruneReader::new_with_last_row_reader(self.context.clone(), reader, skip_fields)
        } else {
            // Row group contains DELETE, fallback to default reader.
            PruneReader::new_with_row_group_reader(
                self.context.clone(),
                RowGroupReader::new(self.context.clone(), parquet_reader),
                skip_fields,
            )
        };

        Ok(prune_reader)
    }

    /// Creates a flat reader that returns RecordBatch.
    pub(crate) async fn flat_reader(
        &self,
        fetch_metrics: &ParquetFetchMetrics,
    ) -> Result<FlatPruneReader> {
        let parquet_reader = self
            .context
            .reader_builder
            .build(
                self.row_group_idx,
                self.row_selection.clone(),
                fetch_metrics,
            )
            .await?;

        // Compute skip_fields once for this row group
        let skip_fields = self.context.should_skip_fields(self.row_group_idx);

        let flat_row_group_reader = FlatRowGroupReader::new(self.context.clone(), parquet_reader);
        let flat_prune_reader = FlatPruneReader::new_with_row_group_reader(
            self.context.clone(),
            flat_row_group_reader,
            skip_fields,
        );

        Ok(flat_prune_reader)
    }

    /// Returns the helper to compat batches.
    pub(crate) fn compat_batch(&self) -> Option<&CompatBatch> {
        self.context.compat_batch()
    }

    /// Returns the file handle of the file range.
    pub(crate) fn file_handle(&self) -> &FileHandle {
        self.context.reader_builder.file_handle()
    }
}

/// Context shared by ranges of the same parquet SST.
pub(crate) struct FileRangeContext {
    /// Row group reader builder for the file.
    reader_builder: RowGroupReaderBuilder,
    /// Base of the context.
    base: RangeBase,
}

pub(crate) type FileRangeContextRef = Arc<FileRangeContext>;

impl FileRangeContext {
    /// Creates a new [FileRangeContext].
    pub(crate) fn new(
        reader_builder: RowGroupReaderBuilder,
        filters: Vec<SimpleFilterContext>,
        read_format: ReadFormat,
        codec: Arc<dyn PrimaryKeyCodec>,
        pre_filter_mode: PreFilterMode,
    ) -> Self {
        Self {
            reader_builder,
            base: RangeBase {
                filters,
                read_format,
                codec,
                compat_batch: None,
                pre_filter_mode,
            },
        }
    }

    /// Returns the path of the file to read.
    pub(crate) fn file_path(&self) -> &str {
        self.reader_builder.file_path()
    }

    /// Returns filters pushed down.
    pub(crate) fn filters(&self) -> &[SimpleFilterContext] {
        &self.base.filters
    }

    /// Returns the format helper.
    pub(crate) fn read_format(&self) -> &ReadFormat {
        &self.base.read_format
    }

    /// Returns the reader builder.
    pub(crate) fn reader_builder(&self) -> &RowGroupReaderBuilder {
        &self.reader_builder
    }

    /// Returns the helper to compat batches.
    pub(crate) fn compat_batch(&self) -> Option<&CompatBatch> {
        self.base.compat_batch.as_ref()
    }

    /// Sets the `CompatBatch` to the context.
    pub(crate) fn set_compat_batch(&mut self, compat: Option<CompatBatch>) {
        self.base.compat_batch = compat;
    }

    /// TRY THE BEST to perform pushed down predicate precisely on the input batch.
    /// Return the filtered batch. If the entire batch is filtered out, return None.
    pub(crate) fn precise_filter(&self, input: Batch, skip_fields: bool) -> Result<Option<Batch>> {
        self.base.precise_filter(input, skip_fields)
    }

    /// Filters the input RecordBatch by the pushed down predicate and returns RecordBatch.
    pub(crate) fn precise_filter_flat(
        &self,
        input: RecordBatch,
        skip_fields: bool,
    ) -> Result<Option<RecordBatch>> {
        self.base.precise_filter_flat(input, skip_fields)
    }

    /// Determines whether to skip field filters based on PreFilterMode and row group delete status.
    pub(crate) fn should_skip_fields(&self, row_group_idx: usize) -> bool {
        match self.base.pre_filter_mode {
            PreFilterMode::All => false,
            PreFilterMode::SkipFields => true,
            PreFilterMode::SkipFieldsOnDelete => {
                // Check if this specific row group contains delete op
                self.contains_delete(row_group_idx).unwrap_or(true)
            }
        }
    }

    //// Decodes parquet metadata and finds if row group contains delete op.
    pub(crate) fn contains_delete(&self, row_group_index: usize) -> Result<bool> {
        let metadata = self.reader_builder.parquet_metadata();
        row_group_contains_delete(metadata, row_group_index, self.reader_builder.file_path())
    }
}

/// Mode to pre-filter columns in a range.
#[derive(Debug, Clone, Copy)]
pub enum PreFilterMode {
    /// Filters all columns.
    All,
    /// If the range doesn't contain delete op or doesn't have statistics, filters all columns.
    /// Otherwise, skips filtering fields.
    SkipFieldsOnDelete,
    /// Always skip fields.
    SkipFields,
}

/// Common fields for a range to read and filter batches.
pub(crate) struct RangeBase {
    /// Filters pushed down.
    pub(crate) filters: Vec<SimpleFilterContext>,
    /// Helper to read the SST.
    pub(crate) read_format: ReadFormat,
    /// Decoder for primary keys
    pub(crate) codec: Arc<dyn PrimaryKeyCodec>,
    /// Optional helper to compat batches.
    pub(crate) compat_batch: Option<CompatBatch>,
    /// Mode to pre-filter columns.
    pub(crate) pre_filter_mode: PreFilterMode,
}

impl RangeBase {
    /// TRY THE BEST to perform pushed down predicate precisely on the input batch.
    /// Return the filtered batch. If the entire batch is filtered out, return None.
    ///
    /// Supported filter expr type is defined in [SimpleFilterEvaluator].
    ///
    /// When a filter is referencing primary key column, this method will decode
    /// the primary key and put it into the batch.
    ///
    /// # Arguments
    /// * `input` - The batch to filter
    /// * `skip_fields` - Whether to skip field filters based on PreFilterMode and row group delete status
    pub(crate) fn precise_filter(
        &self,
        mut input: Batch,
        skip_fields: bool,
    ) -> Result<Option<Batch>> {
        let mut mask = BooleanBuffer::new_set(input.num_rows());

        // Run filter one by one and combine them result
        // TODO(ruihang): run primary key filter first. It may short circuit other filters
        for filter_ctx in &self.filters {
            let filter = match filter_ctx.filter() {
                MaybeFilter::Filter(f) => f,
                // Column matches.
                MaybeFilter::Matched => continue,
                // Column doesn't match, filter the entire batch.
                MaybeFilter::Pruned => return Ok(None),
            };
            let result = match filter_ctx.semantic_type() {
                SemanticType::Tag => {
                    let pk_values = if let Some(pk_values) = input.pk_values() {
                        pk_values
                    } else {
                        input.set_pk_values(
                            self.codec
                                .decode(input.primary_key())
                                .context(DecodeSnafu)?,
                        );
                        input.pk_values().unwrap()
                    };
                    let pk_value = match pk_values {
                        CompositeValues::Dense(v) => {
                            // Safety: this is a primary key
                            let pk_index = self
                                .read_format
                                .metadata()
                                .primary_key_index(filter_ctx.column_id())
                                .unwrap();
                            v[pk_index]
                                .1
                                .try_to_scalar_value(filter_ctx.data_type())
                                .context(DataTypeMismatchSnafu)?
                        }
                        CompositeValues::Sparse(v) => {
                            let v = v.get_or_null(filter_ctx.column_id());
                            v.try_to_scalar_value(filter_ctx.data_type())
                                .context(DataTypeMismatchSnafu)?
                        }
                    };
                    if filter
                        .evaluate_scalar(&pk_value)
                        .context(RecordBatchSnafu)?
                    {
                        continue;
                    } else {
                        // PK not match means the entire batch is filtered out.
                        return Ok(None);
                    }
                }
                SemanticType::Field => {
                    // Skip field filters if skip_fields is true
                    if skip_fields {
                        continue;
                    }
                    // Safety: Input is Batch so we are using primary key format.
                    let Some(field_index) = self
                        .read_format
                        .as_primary_key()
                        .unwrap()
                        .field_index_by_id(filter_ctx.column_id())
                    else {
                        continue;
                    };
                    let field_col = &input.fields()[field_index].data;
                    filter
                        .evaluate_vector(field_col)
                        .context(RecordBatchSnafu)?
                }
                SemanticType::Timestamp => filter
                    .evaluate_vector(input.timestamps())
                    .context(RecordBatchSnafu)?,
            };

            mask = mask.bitand(&result);
        }

        input.filter(&BooleanArray::from(mask).into())?;

        Ok(Some(input))
    }

    /// Filters the input RecordBatch by the pushed down predicate and returns RecordBatch.
    ///
    /// It assumes all necessary tags are already decoded from the primary key.
    ///
    /// # Arguments
    /// * `input` - The RecordBatch to filter
    /// * `skip_fields` - Whether to skip field filters based on PreFilterMode and row group delete status
    pub(crate) fn precise_filter_flat(
        &self,
        input: RecordBatch,
        skip_fields: bool,
    ) -> Result<Option<RecordBatch>> {
        let mask = self.compute_filter_mask_flat(&input, skip_fields)?;

        // If mask is None, the entire batch is filtered out
        let Some(mask) = mask else {
            return Ok(None);
        };

        let filtered_batch =
            datatypes::arrow::compute::filter_record_batch(&input, &BooleanArray::from(mask))
                .context(ComputeArrowSnafu)?;

        if filtered_batch.num_rows() > 0 {
            Ok(Some(filtered_batch))
        } else {
            Ok(None)
        }
    }

    /// Computes the filter mask for the input RecordBatch based on pushed down predicates.
    ///
    /// Returns `None` if the entire batch is filtered out, otherwise returns the boolean mask.
    ///
    /// # Arguments
    /// * `input` - The RecordBatch to compute mask for
    /// * `skip_fields` - Whether to skip field filters based on PreFilterMode and row group delete status
    pub(crate) fn compute_filter_mask_flat(
        &self,
        input: &RecordBatch,
        skip_fields: bool,
    ) -> Result<Option<BooleanBuffer>> {
        let mut mask = BooleanBuffer::new_set(input.num_rows());

        let flat_format = self
            .read_format
            .as_flat()
            .context(crate::error::UnexpectedSnafu {
                reason: "Expected flat format for precise_filter_flat",
            })?;

        // Decodes primary keys once if we have any tag filters not in projection
        let mut decoded_pks: Option<DecodedPrimaryKeys> = None;
        // Cache decoded tag arrays by column id to avoid redundant decoding
        let mut decoded_tag_cache: HashMap<ColumnId, ArrayRef> = HashMap::new();

        // Run filter one by one and combine them result
        for filter_ctx in &self.filters {
            let filter = match filter_ctx.filter() {
                MaybeFilter::Filter(f) => f,
                // Column matches.
                MaybeFilter::Matched => continue,
                // Column doesn't match, filter the entire batch.
                MaybeFilter::Pruned => return Ok(None),
            };

            // Skip field filters if skip_fields is true
            if skip_fields && filter_ctx.semantic_type() == SemanticType::Field {
                continue;
            }

            // Get the column directly by its projected index
            let column_idx = flat_format.projected_index_by_id(filter_ctx.column_id());
            if let Some(idx) = column_idx {
                let column = &input.columns()[idx];
                let result = filter.evaluate_array(column).context(RecordBatchSnafu)?;
                mask = mask.bitand(&result);
            } else if filter_ctx.semantic_type() == SemanticType::Tag {
                // Column not found in projection, it may be a tag column.
                // Decodes primary keys if not already decoded.
                if decoded_pks.is_none() {
                    decoded_pks = Some(decode_primary_keys(self.codec.as_ref(), input)?);
                }

                let metadata = flat_format.metadata();
                let column_id = filter_ctx.column_id();

                // Check cache first
                let tag_column = if let Some(cached_column) = decoded_tag_cache.get(&column_id) {
                    cached_column.clone()
                } else {
                    // For dense encoding, we need pk_index. For sparse encoding, pk_index is None.
                    let pk_index = if self.codec.encoding() == PrimaryKeyEncoding::Sparse {
                        None
                    } else {
                        metadata.primary_key_index(column_id)
                    };
                    let column_index = metadata.column_index_by_id(column_id);

                    if let (Some(column_index), Some(decoded)) =
                        (column_index, decoded_pks.as_ref())
                    {
                        let column_metadata = &metadata.column_metadatas[column_index];
                        let tag_column = decoded.get_tag_column(
                            column_id,
                            pk_index,
                            &column_metadata.column_schema.data_type,
                        )?;
                        // Cache the decoded tag column
                        decoded_tag_cache.insert(column_id, tag_column.clone());
                        tag_column
                    } else {
                        continue;
                    }
                };

                let result = filter
                    .evaluate_array(&tag_column)
                    .context(RecordBatchSnafu)?;
                mask = mask.bitand(&result);
            }
            // Non-tag column not found in projection.
        }

        Ok(Some(mask))
    }
}
