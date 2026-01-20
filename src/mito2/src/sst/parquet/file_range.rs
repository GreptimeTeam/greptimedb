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
use datafusion::physical_plan::PhysicalExpr;
use datafusion::physical_plan::expressions::DynamicFilterPhysicalExpr;
use datatypes::arrow::array::{ArrayRef, BooleanArray};
use datatypes::arrow::buffer::BooleanBuffer;
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::Schema;
use mito_codec::row_converter::{CompositeValues, PrimaryKeyCodec};
use parquet::arrow::arrow_reader::RowSelection;
use parquet::file::metadata::ParquetMetaData;
use snafu::{OptionExt, ResultExt};
use store_api::codec::PrimaryKeyEncoding;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::{ColumnId, TimeSeriesRowSelector};
use table::predicate::Predicate;

use crate::error::{
    ComputeArrowSnafu, DataTypeMismatchSnafu, DecodeSnafu, DecodeStatsSnafu,
    EvalPartitionFilterSnafu, NewRecordBatchSnafu, RecordBatchSnafu, Result, StatsNotPresentSnafu,
    UnexpectedSnafu,
};
use crate::read::Batch;
use crate::read::compat::CompatBatch;
use crate::read::last_row::RowGroupLastRowCachedReader;
use crate::read::prune::{FlatPruneReader, PruneReader};
use crate::sst::file::FileHandle;
use crate::sst::parquet::flat_format::{
    DecodedPrimaryKeys, decode_primary_keys, time_index_column_index,
};
use crate::sst::parquet::format::ReadFormat;
use crate::sst::parquet::reader::{
    FlatRowGroupReader, MaybeFilter, RowGroupReader, RowGroupReaderBuilder, SimpleFilterContext,
};
use crate::sst::parquet::row_group::ParquetFetchMetrics;
use crate::sst::parquet::stats::RowGroupPruningStats;

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

    /// Performs pruning before reading the [FileRange].
    /// It use latest dynamic filters with row group statistics to prune the range.
    ///
    /// Returns false if the entire range is pruned and can be skipped.
    fn in_dynamic_filter_range(&self) -> bool {
        if self.context.base.dyn_filters.is_empty() {
            return true;
        }
        let curr_row_group = self
            .context
            .reader_builder
            .parquet_metadata()
            .row_group(self.row_group_idx);
        let read_format = self.context.read_format();
        let prune_schema = &self.context.base.prune_schema;
        let stats = RowGroupPruningStats::new(
            std::slice::from_ref(curr_row_group),
            read_format,
            self.context.base.expected_metadata.clone(),
            self.compute_skip_fields(),
        );

        // not costly to create a predicate here since dynamic filters are wrapped in Arc
        let pred = Predicate::new(vec![]).with_dyn_filters(self.context.base.dyn_filters.clone());

        pred.prune_with_stats(&stats, prune_schema.arrow_schema())
            .first()
            .cloned()
            .unwrap_or(true) // unexpected, not skip just in case
    }

    fn compute_skip_fields(&self) -> bool {
        match self.context.base.pre_filter_mode {
            PreFilterMode::All => false,
            PreFilterMode::SkipFields => true,
            PreFilterMode::SkipFieldsOnDelete => {
                // Check if this specific row group contains delete op
                row_group_contains_delete(
                    self.context.reader_builder.parquet_metadata(),
                    self.row_group_idx,
                    self.context.reader_builder.file_path(),
                )
                .unwrap_or(true)
            }
        }
    }

    /// Returns a reader to read the [FileRange].
    pub(crate) async fn reader(
        &self,
        selector: Option<TimeSeriesRowSelector>,
        fetch_metrics: Option<&ParquetFetchMetrics>,
    ) -> Result<Option<PruneReader>> {
        if !self.in_dynamic_filter_range() {
            return Ok(None);
        }
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

        Ok(Some(prune_reader))
    }

    /// Creates a flat reader that returns RecordBatch.
    pub(crate) async fn flat_reader(
        &self,
        fetch_metrics: Option<&ParquetFetchMetrics>,
    ) -> Result<Option<FlatPruneReader>> {
        if !self.in_dynamic_filter_range() {
            return Ok(None);
        }
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

        Ok(Some(flat_prune_reader))
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
    pub(crate) fn new(reader_builder: RowGroupReaderBuilder, base: RangeBase) -> Self {
        Self {
            reader_builder,
            base,
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

    /// Returns true if a partition filter is configured.
    pub(crate) fn has_partition_filter(&self) -> bool {
        self.base.partition_filter.is_some()
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
    /// If a partition expr filter is configured, it is also applied.
    pub(crate) fn precise_filter(&self, input: Batch, skip_fields: bool) -> Result<Option<Batch>> {
        self.base.precise_filter(input, skip_fields)
    }

    /// Filters the input RecordBatch by the pushed down predicate and returns RecordBatch.
    /// If a partition expr filter is configured, it is also applied.
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

/// Context for partition expression filtering.
pub(crate) struct PartitionFilterContext {
    pub(crate) region_partition_physical_expr: Arc<dyn PhysicalExpr>,
    /// Schema containing only columns referenced by the partition expression.
    /// This is used to build a minimal RecordBatch for partition filter evaluation.
    pub(crate) partition_schema: Arc<Schema>,
}

/// Common fields for a range to read and filter batches.
pub(crate) struct RangeBase {
    /// Filters pushed down.
    pub(crate) filters: Vec<SimpleFilterContext>,
    /// Dynamic filter physical exprs.
    pub(crate) dyn_filters: Arc<Vec<DynamicFilterPhysicalExpr>>,
    /// Helper to read the SST.
    pub(crate) read_format: ReadFormat,
    pub(crate) expected_metadata: Option<RegionMetadataRef>,
    /// Schema used for pruning with dynamic filters.
    pub(crate) prune_schema: Arc<Schema>,
    /// Decoder for primary keys
    pub(crate) codec: Arc<dyn PrimaryKeyCodec>,
    /// Optional helper to compat batches.
    pub(crate) compat_batch: Option<CompatBatch>,
    /// Mode to pre-filter columns.
    pub(crate) pre_filter_mode: PreFilterMode,
    /// Partition filter.
    pub(crate) partition_filter: Option<PartitionFilterContext>,
}

pub(crate) struct TagDecodeState {
    decoded_pks: Option<DecodedPrimaryKeys>,
    decoded_tag_cache: HashMap<ColumnId, ArrayRef>,
}

impl TagDecodeState {
    pub(crate) fn new() -> Self {
        Self {
            decoded_pks: None,
            decoded_tag_cache: HashMap::new(),
        }
    }
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

        if mask.count_set_bits() == 0 {
            return Ok(None);
        }

        // Apply partition filter
        if let Some(partition_filter) = &self.partition_filter {
            let record_batch = self
                .build_record_batch_for_pruning(&mut input, &partition_filter.partition_schema)?;
            let partition_mask = self.evaluate_partition_filter(&record_batch, partition_filter)?;
            mask = mask.bitand(&partition_mask);
        }

        if mask.count_set_bits() == 0 {
            Ok(None)
        } else {
            input.filter(&BooleanArray::from(mask).into())?;
            Ok(Some(input))
        }
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
        let mut tag_decode_state = TagDecodeState::new();
        let mask = self.compute_filter_mask_flat(&input, skip_fields, &mut tag_decode_state)?;

        // If mask is None, the entire batch is filtered out
        let Some(mut mask) = mask else {
            return Ok(None);
        };

        // Apply partition filter
        if let Some(partition_filter) = &self.partition_filter {
            let record_batch = self.project_record_batch_for_pruning_flat(
                &input,
                &partition_filter.partition_schema,
                &mut tag_decode_state,
            )?;
            let partition_mask = self.evaluate_partition_filter(&record_batch, partition_filter)?;
            mask = mask.bitand(&partition_mask);
        }

        if mask.count_set_bits() == 0 {
            return Ok(None);
        }

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
    /// If a partition expr filter is configured, it is applied later in `precise_filter_flat` but **NOT** in this function.
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
        tag_decode_state: &mut TagDecodeState,
    ) -> Result<Option<BooleanBuffer>> {
        let mut mask = BooleanBuffer::new_set(input.num_rows());

        let flat_format = self
            .read_format
            .as_flat()
            .context(crate::error::UnexpectedSnafu {
                reason: "Expected flat format for precise_filter_flat",
            })?;
        let metadata = flat_format.metadata();

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
                let column_id = filter_ctx.column_id();

                if let Some(tag_column) =
                    self.maybe_decode_tag_column(metadata, column_id, input, tag_decode_state)?
                {
                    let result = filter
                        .evaluate_array(&tag_column)
                        .context(RecordBatchSnafu)?;
                    mask = mask.bitand(&result);
                }
            } else if filter_ctx.semantic_type() == SemanticType::Timestamp {
                let time_index_pos = time_index_column_index(input.num_columns());
                let column = &input.columns()[time_index_pos];
                let result = filter.evaluate_array(column).context(RecordBatchSnafu)?;
                mask = mask.bitand(&result);
            }
            // Non-tag column not found in projection.
        }

        Ok(Some(mask))
    }

    /// Returns the decoded tag column for `column_id`, or `None` if it's not a tag.
    fn maybe_decode_tag_column(
        &self,
        metadata: &RegionMetadataRef,
        column_id: ColumnId,
        input: &RecordBatch,
        tag_decode_state: &mut TagDecodeState,
    ) -> Result<Option<ArrayRef>> {
        let Some(pk_index) = metadata.primary_key_index(column_id) else {
            return Ok(None);
        };

        if let Some(cached_column) = tag_decode_state.decoded_tag_cache.get(&column_id) {
            return Ok(Some(cached_column.clone()));
        }

        if tag_decode_state.decoded_pks.is_none() {
            tag_decode_state.decoded_pks = Some(decode_primary_keys(self.codec.as_ref(), input)?);
        }

        let pk_index = if self.codec.encoding() == PrimaryKeyEncoding::Sparse {
            None
        } else {
            Some(pk_index)
        };
        let Some(column_index) = metadata.column_index_by_id(column_id) else {
            return Ok(None);
        };
        let Some(decoded) = tag_decode_state.decoded_pks.as_ref() else {
            return Ok(None);
        };

        let column_metadata = &metadata.column_metadatas[column_index];
        let tag_column = decoded.get_tag_column(
            column_id,
            pk_index,
            &column_metadata.column_schema.data_type,
        )?;
        tag_decode_state
            .decoded_tag_cache
            .insert(column_id, tag_column.clone());

        Ok(Some(tag_column))
    }

    /// Evaluates the partition filter against the input `RecordBatch`.
    fn evaluate_partition_filter(
        &self,
        record_batch: &RecordBatch,
        partition_filter: &PartitionFilterContext,
    ) -> Result<BooleanBuffer> {
        let columnar_value = partition_filter
            .region_partition_physical_expr
            .evaluate(record_batch)
            .context(EvalPartitionFilterSnafu)?;
        let array = columnar_value
            .into_array(record_batch.num_rows())
            .context(EvalPartitionFilterSnafu)?;
        let boolean_array =
            array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .context(UnexpectedSnafu {
                    reason: "Failed to downcast to BooleanArray".to_string(),
                })?;

        Ok(boolean_array.values().clone())
    }

    /// Builds a `RecordBatch` from the input `Batch` matching the given schema.
    ///
    /// This is used for partition expression evaluation. The schema should only contain
    /// the columns referenced by the partition expression to minimize overhead.
    fn build_record_batch_for_pruning(
        &self,
        input: &mut Batch,
        schema: &Arc<Schema>,
    ) -> Result<RecordBatch> {
        let arrow_schema = schema.arrow_schema();
        let mut columns = Vec::with_capacity(arrow_schema.fields().len());

        // Decode primary key if necessary.
        if input.pk_values().is_none() {
            input.set_pk_values(
                self.codec
                    .decode(input.primary_key())
                    .context(DecodeSnafu)?,
            );
        }

        for field in arrow_schema.fields() {
            let metadata = self.read_format.metadata();
            let column_id = metadata.column_by_name(field.name()).map(|c| c.column_id);

            // Partition pruning schema should be a subset of the input batch schema.
            let Some(column_id) = column_id else {
                return UnexpectedSnafu {
                    reason: format!(
                        "Partition pruning schema expects column '{}' but it is missing in \
                         region metadata",
                        field.name()
                    ),
                }
                .fail();
            };

            // 1. Check if it's a tag.
            if let Some(pk_index) = metadata.primary_key_index(column_id) {
                let pk_values = input.pk_values().unwrap();
                let value = match pk_values {
                    CompositeValues::Dense(v) => &v[pk_index].1,
                    CompositeValues::Sparse(v) => v.get_or_null(column_id),
                };
                let concrete_type = ConcreteDataType::from_arrow_type(field.data_type());
                let arrow_scalar = value
                    .try_to_scalar_value(&concrete_type)
                    .context(DataTypeMismatchSnafu)?;
                let array = arrow_scalar
                    .to_array_of_size(input.num_rows())
                    .context(EvalPartitionFilterSnafu)?;
                columns.push(array);
            } else if metadata.time_index_column().column_id == column_id {
                // 2. Check if it's the timestamp column.
                columns.push(input.timestamps().to_arrow_array());
            } else if let Some(field_index) = self
                .read_format
                .as_primary_key()
                .and_then(|f| f.field_index_by_id(column_id))
            {
                // 3. Check if it's a field column.
                columns.push(input.fields()[field_index].data.to_arrow_array());
            } else {
                return UnexpectedSnafu {
                    reason: format!(
                        "Partition pruning schema expects column '{}' (id {}) but it is not \
                         present in input batch",
                        field.name(),
                        column_id
                    ),
                }
                .fail();
            }
        }

        RecordBatch::try_new(arrow_schema.clone(), columns).context(NewRecordBatchSnafu)
    }

    /// Projects the input `RecordBatch` to match the given schema.
    ///
    /// This is used for partition expression evaluation. The schema should only contain
    /// the columns referenced by the partition expression to minimize overhead.
    fn project_record_batch_for_pruning_flat(
        &self,
        input: &RecordBatch,
        schema: &Arc<Schema>,
        tag_decode_state: &mut TagDecodeState,
    ) -> Result<RecordBatch> {
        let arrow_schema = schema.arrow_schema();
        let mut columns = Vec::with_capacity(arrow_schema.fields().len());

        let flat_format = self
            .read_format
            .as_flat()
            .context(crate::error::UnexpectedSnafu {
                reason: "Expected flat format for precise_filter_flat",
            })?;
        let metadata = flat_format.metadata();

        for field in arrow_schema.fields() {
            let column_id = metadata.column_by_name(field.name()).map(|c| c.column_id);

            let Some(column_id) = column_id else {
                return UnexpectedSnafu {
                    reason: format!(
                        "Partition pruning schema expects column '{}' but it is missing in \
                         region metadata",
                        field.name()
                    ),
                }
                .fail();
            };

            if let Some(idx) = flat_format.projected_index_by_id(column_id) {
                columns.push(input.column(idx).clone());
                continue;
            }

            if metadata.time_index_column().column_id == column_id {
                let time_index_pos = time_index_column_index(input.num_columns());
                columns.push(input.column(time_index_pos).clone());
                continue;
            }

            if let Some(tag_column) =
                self.maybe_decode_tag_column(metadata, column_id, input, tag_decode_state)?
            {
                columns.push(tag_column);
                continue;
            }

            return UnexpectedSnafu {
                reason: format!(
                    "Partition pruning schema expects column '{}' (id {}) but it is not \
                     present in projected record batch",
                    field.name(),
                    column_id
                ),
            }
            .fail();
        }

        RecordBatch::try_new(arrow_schema.clone(), columns).context(NewRecordBatchSnafu)
    }
}
