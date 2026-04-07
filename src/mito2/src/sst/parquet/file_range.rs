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
use datatypes::arrow::array::{Array as _, ArrayRef, BooleanArray};
use datatypes::arrow::buffer::BooleanBuffer;
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::schema::Schema;
use mito_codec::row_converter::PrimaryKeyCodec;
use parquet::arrow::arrow_reader::RowSelection;
use parquet::file::metadata::ParquetMetaData;
use snafu::{OptionExt, ResultExt};
use store_api::codec::PrimaryKeyEncoding;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::{ColumnId, TimeSeriesRowSelector};
use table::predicate::Predicate;

use crate::cache::CacheStrategy;
use crate::error::{
    ComputeArrowSnafu, DecodeStatsSnafu, EvalPartitionFilterSnafu, NewRecordBatchSnafu,
    RecordBatchSnafu, Result, StatsNotPresentSnafu, UnexpectedSnafu,
};
use crate::read::compat::FlatCompatBatch;
use crate::read::flat_projection::CompactionProjectionMapper;
use crate::read::last_row::FlatRowGroupLastRowCachedReader;
use crate::read::prune::FlatPruneReader;
use crate::sst::file::FileHandle;
use crate::sst::parquet::flat_format::{
    DecodedPrimaryKeys, FlatReadFormat, decode_primary_keys, time_index_column_index,
};
use crate::sst::parquet::reader::{
    FlatRowGroupReader, MaybeFilter, MaybePhysicalFilter, PhysicalFilterContext,
    RowGroupBuildContext, RowGroupReaderBuilder, SimpleFilterContext,
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
            self.context.base.pre_filter_mode.skip_fields(),
        );

        // not costly to create a predicate here since dynamic filters are wrapped in Arc
        let pred = Predicate::with_dyn_filters(vec![], self.context.base.dyn_filters.clone());

        pred.prune_with_stats(&stats, prune_schema.arrow_schema())
            .first()
            .cloned()
            .unwrap_or(true) // unexpected, not skip just in case
    }

    /// Creates a flat reader that returns RecordBatch.
    pub(crate) async fn flat_reader(
        &self,
        selector: Option<TimeSeriesRowSelector>,
        fetch_metrics: Option<&ParquetFetchMetrics>,
    ) -> Result<Option<FlatPruneReader>> {
        if !self.in_dynamic_filter_range() {
            return Ok(None);
        }
        // Compute skip_fields once for this row group
        let skip_fields = self.context.base.pre_filter_mode.skip_fields();
        let parquet_reader = self
            .context
            .reader_builder
            .build(self.context.build_context(
                self.row_group_idx,
                self.row_selection.clone(),
                fetch_metrics,
                skip_fields,
            ))
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
                    error!(e; "Failed to decode min value of op_type, fallback to FlatRowGroupReader");
                })
                .unwrap_or(true);
            put_only && self.select_all()
        } else {
            false
        };

        let flat_prune_reader = if use_last_row_reader {
            let flat_row_group_reader =
                FlatRowGroupReader::new(self.context.clone(), parquet_reader);
            // Flat PK prefilter makes the input stream predicate-dependent, so cached
            // selector results are not reusable across queries with different filters.
            let cache_strategy = if self.context.reader_builder.has_predicate_prefilter() {
                CacheStrategy::Disabled
            } else {
                self.context.reader_builder.cache_strategy().clone()
            };
            let reader = FlatRowGroupLastRowCachedReader::new(
                self.file_handle().file_id().file_id(),
                self.row_group_idx,
                cache_strategy,
                self.context.read_format().projection_indices(),
                flat_row_group_reader,
            );
            FlatPruneReader::new_with_last_row_reader(self.context.clone(), reader, skip_fields)
        } else {
            let flat_row_group_reader =
                FlatRowGroupReader::new(self.context.clone(), parquet_reader);
            FlatPruneReader::new_with_row_group_reader(
                self.context.clone(),
                flat_row_group_reader,
                skip_fields,
            )
        };

        Ok(Some(flat_prune_reader))
    }

    /// Returns the helper to compat batches.
    pub(crate) fn compat_batch(&self) -> Option<&FlatCompatBatch> {
        self.context.compat_batch()
    }

    /// Returns the helper to project batches.
    pub(crate) fn compaction_projection_mapper(&self) -> Option<&CompactionProjectionMapper> {
        self.context.compaction_projection_mapper()
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

    /// Returns filters pushed down.
    pub(crate) fn filters(&self) -> &[SimpleFilterContext] {
        &self.base.filters
    }

    /// Returns true if a partition filter is configured.
    pub(crate) fn has_partition_filter(&self) -> bool {
        self.base.partition_filter.is_some()
    }

    /// Returns the format helper.
    pub(crate) fn read_format(&self) -> &FlatReadFormat {
        &self.base.read_format
    }

    /// Returns the reader builder.
    pub(crate) fn reader_builder(&self) -> &RowGroupReaderBuilder {
        &self.reader_builder
    }

    /// Returns the helper to compat batches.
    pub(crate) fn compat_batch(&self) -> Option<&FlatCompatBatch> {
        self.base.compat_batch.as_ref()
    }

    /// Returns the helper to project batches.
    pub(crate) fn compaction_projection_mapper(&self) -> Option<&CompactionProjectionMapper> {
        self.base.compaction_projection_mapper.as_ref()
    }

    /// Sets the compat helper to the context.
    pub(crate) fn set_compat_batch(&mut self, compat: Option<FlatCompatBatch>) {
        self.base.compat_batch = compat;
    }

    /// Filters the input RecordBatch by the pushed down predicate and returns RecordBatch.
    /// If a partition expr filter is configured, it is also applied.
    pub(crate) fn precise_filter_flat(
        &self,
        input: RecordBatch,
        skip_fields: bool,
    ) -> Result<Option<RecordBatch>> {
        self.base.precise_filter_flat(
            input,
            skip_fields,
            self.reader_builder.has_predicate_prefilter(),
        )
    }

    pub(crate) fn pre_filter_mode(&self) -> PreFilterMode {
        self.base.pre_filter_mode
    }

    //// Decodes parquet metadata and finds if row group contains delete op.
    pub(crate) fn contains_delete(&self, row_group_index: usize) -> Result<bool> {
        let metadata = self.reader_builder.parquet_metadata();
        row_group_contains_delete(metadata, row_group_index, self.reader_builder.file_path())
    }

    /// Creates a [RowGroupBuildContext] for building row group readers with prefiltering.
    pub(crate) fn build_context<'a>(
        &'a self,
        row_group_idx: usize,
        row_selection: Option<RowSelection>,
        fetch_metrics: Option<&'a ParquetFetchMetrics>,
        skip_fields: bool,
    ) -> RowGroupBuildContext<'a> {
        RowGroupBuildContext {
            filters: &self.base.filters,
            physical_filters: &self.base.physical_filters,
            row_group_idx,
            row_selection,
            fetch_metrics,
            skip_fields,
        }
    }

    /// Returns the estimated memory size of this context.
    /// Mainly accounts for the parquet metadata size.
    pub(crate) fn memory_size(&self) -> usize {
        crate::cache::cache_size::parquet_meta_size(self.reader_builder.parquet_metadata())
    }
}

/// Mode to pre-filter columns in a range.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PreFilterMode {
    /// Filters all columns.
    All,
    /// Always skip fields.
    SkipFields,
}

impl PreFilterMode {
    pub(crate) fn skip_fields(self) -> bool {
        matches!(self, Self::SkipFields)
    }
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
    /// Physical filters pushed down.
    pub(crate) physical_filters: Vec<PhysicalFilterContext>,
    /// Dynamic filter physical exprs.
    pub(crate) dyn_filters: Vec<Arc<DynamicFilterPhysicalExpr>>,
    /// Helper to read the SST.
    pub(crate) read_format: FlatReadFormat,
    pub(crate) expected_metadata: Option<RegionMetadataRef>,
    /// Schema used for pruning with dynamic filters.
    pub(crate) prune_schema: Arc<Schema>,
    /// Decoder for primary keys
    pub(crate) codec: Arc<dyn PrimaryKeyCodec>,
    /// Optional helper to compat batches.
    pub(crate) compat_batch: Option<FlatCompatBatch>,
    /// Optional helper to project batches.
    pub(crate) compaction_projection_mapper: Option<CompactionProjectionMapper>,
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
    /// Filters the input RecordBatch by the pushed down predicate and returns RecordBatch.
    ///
    /// It assumes all necessary tags are already decoded from the primary key.
    ///
    /// # Arguments
    /// * `input` - The RecordBatch to filter
    /// * `skip_fields` - Whether to skip field filters based on PreFilterMode
    pub(crate) fn precise_filter_flat(
        &self,
        input: RecordBatch,
        skip_fields: bool,
        skip_prefiltered_filters: bool,
    ) -> Result<Option<RecordBatch>> {
        let mut tag_decode_state = TagDecodeState::new();
        let mask = self.compute_filter_mask_flat(
            &input,
            skip_fields,
            skip_prefiltered_filters,
            &mut tag_decode_state,
        )?;

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
    /// * `skip_fields` - Whether to skip field filters based on PreFilterMode
    pub(crate) fn compute_filter_mask_flat(
        &self,
        input: &RecordBatch,
        skip_fields: bool,
        skip_prefiltered_filters: bool,
        tag_decode_state: &mut TagDecodeState,
    ) -> Result<Option<BooleanBuffer>> {
        let mut mask = BooleanBuffer::new_set(input.num_rows());

        let metadata = self.read_format.metadata();

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

            // Flat parquet PK prefiltering already applied these tag predicates while refining
            // row selection, so skip them here to avoid decoding/evaluating the same condition twice.
            if should_skip_simple_filter_after_prefilter(
                skip_prefiltered_filters,
                skip_fields,
                filter_ctx,
            ) {
                continue;
            }

            // Get the column directly by its projected index.
            // If the column is missing and it's not a tag/time column, this filter is skipped.
            // Assumes the projection indices align with the input batch schema.
            let column_idx = self
                .read_format
                .projected_index_by_id(filter_ctx.column_id());
            if let Some(idx) = column_idx {
                let column = &input.columns().get(idx).unwrap();
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

        for filter_ctx in &self.physical_filters {
            let filter = match filter_ctx.filter() {
                MaybePhysicalFilter::Filter(filter) => filter,
                MaybePhysicalFilter::Matched => continue,
                MaybePhysicalFilter::Pruned => return Ok(None),
            };

            if skip_fields && filter_ctx.semantic_type() == SemanticType::Field {
                continue;
            }

            if should_skip_physical_filter_after_prefilter(
                skip_prefiltered_filters,
                skip_fields,
                filter_ctx,
            ) {
                continue;
            }

            let column =
                if let Some((idx, _)) = input.schema().column_with_name(filter_ctx.column_name()) {
                    Some(input.column(idx).clone())
                } else if filter_ctx.semantic_type() == SemanticType::Tag {
                    self.maybe_decode_tag_column(
                        metadata,
                        filter_ctx.column_id(),
                        input,
                        tag_decode_state,
                    )?
                } else {
                    None
                };

            let Some(column) = column else {
                continue;
            };

            let record_batch = RecordBatch::try_new(filter_ctx.schema().clone(), vec![column])
                .context(NewRecordBatchSnafu)?;
            let evaluated = filter
                .evaluate(&record_batch)
                .context(EvalPartitionFilterSnafu)?;
            let array = evaluated
                .into_array(record_batch.num_rows())
                .context(EvalPartitionFilterSnafu)?;
            let boolean_array =
                array
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .context(UnexpectedSnafu {
                        reason: "Failed to downcast physical filter result to BooleanArray",
                    })?;
            mask = mask.bitand(boolean_array.values());
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

        // also need to consider nulls in the partition filter result. If a value is null, it should be treated as false (filtered out).
        let mut mask = boolean_array.values().clone();
        if let Some(nulls) = boolean_array.nulls() {
            mask = mask.bitand(nulls.inner());
        }

        Ok(mask)
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

        let metadata = self.read_format.metadata();

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

            if let Some(idx) = self.read_format.projected_index_by_id(column_id) {
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

fn should_skip_simple_filter_after_prefilter(
    skip_prefiltered_filters: bool,
    skip_fields: bool,
    filter_ctx: &SimpleFilterContext,
) -> bool {
    if !skip_prefiltered_filters {
        return false;
    }

    match filter_ctx.semantic_type() {
        SemanticType::Field => !skip_fields,
        SemanticType::Tag | SemanticType::Timestamp => true,
    }
}

fn should_skip_physical_filter_after_prefilter(
    skip_prefiltered_filters: bool,
    skip_fields: bool,
    filter_ctx: &PhysicalFilterContext,
) -> bool {
    if !skip_prefiltered_filters {
        return false;
    }

    match filter_ctx.semantic_type() {
        SemanticType::Field => !skip_fields,
        SemanticType::Tag | SemanticType::Timestamp => true,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion_expr::{col, lit};

    use super::*;
    use crate::sst::parquet::flat_format::FlatReadFormat;
    use crate::test_util::sst_util::{new_record_batch_with_custom_sequence, sst_region_metadata};

    fn new_test_range_base(
        filters: Vec<SimpleFilterContext>,
        physical_filters: Vec<PhysicalFilterContext>,
    ) -> RangeBase {
        let metadata: RegionMetadataRef = Arc::new(sst_region_metadata());
        let read_format = FlatReadFormat::new(
            metadata.clone(),
            metadata.column_metadatas.iter().map(|c| c.column_id),
            None,
            "test",
            true,
        )
        .unwrap();

        RangeBase {
            filters,
            physical_filters,
            dyn_filters: vec![],
            read_format,
            expected_metadata: None,
            prune_schema: metadata.schema.clone(),
            codec: mito_codec::row_converter::build_primary_key_codec(metadata.as_ref()),
            compat_batch: None,
            compaction_projection_mapper: None,
            pre_filter_mode: PreFilterMode::All,
            partition_filter: None,
        }
    }

    #[test]
    fn test_compute_filter_mask_flat_skips_prefiltered_pk_filters() {
        let metadata: RegionMetadataRef = Arc::new(sst_region_metadata());
        let filters = vec![
            SimpleFilterContext::new_opt(&metadata, None, &col("tag_0").eq(lit("a"))).unwrap(),
            SimpleFilterContext::new_opt(&metadata, None, &col("field_0").gt(lit(1_u64))).unwrap(),
        ];
        let base = new_test_range_base(filters, vec![]);
        let batch = new_record_batch_with_custom_sequence(&["b", "x"], 0, 4, 1);

        let mask_without_skip = base
            .compute_filter_mask_flat(&batch, false, false, &mut TagDecodeState::new())
            .unwrap()
            .unwrap();
        assert_eq!(mask_without_skip.count_set_bits(), 0);

        let mask_with_skip = base
            .compute_filter_mask_flat(&batch, false, true, &mut TagDecodeState::new())
            .unwrap()
            .unwrap();
        assert_eq!(mask_with_skip.count_set_bits(), 4);
    }

    #[test]
    fn test_compute_filter_mask_flat_skips_prefiltered_physical_filters() {
        let metadata: RegionMetadataRef = Arc::new(sst_region_metadata());
        let read_format = ReadFormat::new_flat(
            metadata.clone(),
            metadata.column_metadatas.iter().map(|c| c.column_id),
            None,
            "test",
            true,
        )
        .unwrap();
        let physical_filters = vec![
            PhysicalFilterContext::new_opt(
                &metadata,
                None,
                &read_format,
                &((col("field_0") + lit(1_u64)).gt(lit(2_u64))),
            )
            .unwrap(),
        ];
        let base = new_test_range_base(vec![], physical_filters);
        let batch = new_record_batch_with_custom_sequence(&["b", "x"], 0, 4, 1);

        let mask_without_skip = base
            .compute_filter_mask_flat(&batch, false, false, &mut TagDecodeState::new())
            .unwrap()
            .unwrap();
        assert_eq!(mask_without_skip.count_set_bits(), 2);

        let mask_with_skip = base
            .compute_filter_mask_flat(&batch, false, true, &mut TagDecodeState::new())
            .unwrap()
            .unwrap();
        assert_eq!(mask_with_skip.count_set_bits(), 4);
    }
}
