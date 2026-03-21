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
use std::ops::{BitAnd, Range};
use std::sync::Arc;

use api::v1::{OpType, SemanticType};
use common_recordbatch::filter::SimpleFilterEvaluator;
use common_telemetry::error;
use datafusion::physical_plan::PhysicalExpr;
use datafusion::physical_plan::expressions::DynamicFilterPhysicalExpr;
use datatypes::arrow::array::{Array as _, ArrayRef, BooleanArray};
use datatypes::arrow::buffer::BooleanBuffer;
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::Schema;
use mito_codec::primary_key_filter::is_partition_column;
use mito_codec::row_converter::{CompositeValues, PrimaryKeyCodec, PrimaryKeyFilter};
use parquet::arrow::arrow_reader::RowSelection;
use parquet::file::metadata::ParquetMetaData;
use snafu::{OptionExt, ResultExt};
use store_api::codec::PrimaryKeyEncoding;
use store_api::metadata::{RegionMetadata, RegionMetadataRef};
use store_api::storage::{ColumnId, TimeSeriesRowSelector};
use table::predicate::Predicate;

use crate::error::{
    ArrowReaderSnafu, ComputeArrowSnafu, DataTypeMismatchSnafu, DecodeSnafu, DecodeStatsSnafu,
    EvalPartitionFilterSnafu, NewRecordBatchSnafu, RecordBatchSnafu, Result, StatsNotPresentSnafu,
    UnexpectedSnafu,
};
use crate::read::Batch;
use crate::read::compat::CompatBatch;
use crate::read::flat_projection::CompactionProjectionMapper;
use crate::read::last_row::{FlatRowGroupLastRowCachedReader, RowGroupLastRowCachedReader};
use crate::read::prune::{FlatPruneReader, PruneReader};
use crate::sst::file::FileHandle;
use crate::sst::parquet::flat_format::{
    DecodedPrimaryKeys, decode_primary_keys, primary_key_column_index, time_index_column_index,
};
use crate::sst::parquet::format::{PrimaryKeyArray, ReadFormat};
use crate::sst::parquet::reader::{
    FlatRowGroupReader, MaybeFilter, RowGroupReader, RowGroupReaderBuilder, SimpleFilterContext,
};
use crate::sst::parquet::row_group::{InMemoryRowGroup, ParquetFetchMetrics};
use crate::sst::parquet::row_selection::row_selection_from_row_ranges;
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
            self.context.should_skip_fields(self.row_group_idx),
        );

        // not costly to create a predicate here since dynamic filters are wrapped in Arc
        let pred = Predicate::with_dyn_filters(vec![], self.context.base.dyn_filters.clone());

        pred.prune_with_stats(&stats, prune_schema.arrow_schema())
            .first()
            .cloned()
            .unwrap_or(true) // unexpected, not skip just in case
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
        selector: Option<TimeSeriesRowSelector>,
        fetch_metrics: Option<&ParquetFetchMetrics>,
    ) -> Result<Option<FlatPruneReader>> {
        if !self.in_dynamic_filter_range() {
            return Ok(None);
        }

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

        // Compute skip_fields once for this row group
        let skip_fields = self.context.should_skip_fields(self.row_group_idx);

        let flat_prune_reader = if use_last_row_reader {
            let row_selection = self.row_selection.clone();
            if row_selection
                .as_ref()
                .is_some_and(|selection| selection.row_count() == 0)
            {
                return Ok(None);
            }

            let flat_row_group_reader = FlatRowGroupReader::new(
                self.context.clone(),
                self.context
                    .reader_builder
                    .build(self.row_group_idx, row_selection, fetch_metrics)
                    .await?,
            );
            let reader = FlatRowGroupLastRowCachedReader::new(
                self.file_handle().file_id().file_id(),
                self.row_group_idx,
                self.context.reader_builder.cache_strategy().clone(),
                self.context.read_format().projection_indices(),
                flat_row_group_reader,
            );
            FlatPruneReader::new_with_last_row_reader(self.context.clone(), reader, skip_fields)
        } else {
            match self.prefiltered_flat_reader_input(fetch_metrics).await? {
                PrefilteredFlatReaderInput::Selection(row_selection) => {
                    if row_selection
                        .as_ref()
                        .is_some_and(|selection| selection.row_count() == 0)
                    {
                        return Ok(None);
                    }

                    let flat_row_group_reader = FlatRowGroupReader::new(
                        self.context.clone(),
                        self.context
                            .reader_builder
                            .build(self.row_group_idx, row_selection, fetch_metrics)
                            .await?,
                    );
                    FlatPruneReader::new_with_row_group_reader(
                        self.context.clone(),
                        flat_row_group_reader,
                        skip_fields,
                    )
                }
                PrefilteredFlatReaderInput::Prefetched(mut row_group) => {
                    let flat_row_group_reader = FlatRowGroupReader::new(
                        self.context.clone(),
                        self.context
                            .reader_builder
                            .build_on_row_group(
                                &mut row_group,
                                self.row_selection.clone(),
                                fetch_metrics,
                            )
                            .await?,
                    );
                    FlatPruneReader::new_with_row_group_reader(
                        self.context.clone(),
                        flat_row_group_reader,
                        skip_fields,
                    )
                }
            }
        };

        Ok(Some(flat_prune_reader))
    }

    async fn prefiltered_flat_reader_input<'a>(
        &'a self,
        fetch_metrics: Option<&ParquetFetchMetrics>,
    ) -> Result<PrefilteredFlatReaderInput<'a>> {
        if !self.select_all() {
            return Ok(PrefilteredFlatReaderInput::Selection(
                self.row_selection.clone(),
            ));
        }

        let Some(mut primary_key_filter) = self.context.new_primary_key_filter() else {
            return Ok(PrefilteredFlatReaderInput::Selection(
                self.row_selection.clone(),
            ));
        };

        let read_format = ReadFormat::new_flat(
            self.context.read_format().metadata().clone(),
            std::iter::empty::<ColumnId>(),
            Some(
                self.context
                    .reader_builder
                    .parquet_metadata()
                    .file_metadata()
                    .schema_descr()
                    .num_columns(),
            ),
            self.context.file_path(),
            false,
        )?;
        let mut row_group = self
            .context
            .reader_builder
            .new_in_memory_row_group(self.row_group_idx);
        let reader = self
            .context
            .reader_builder
            .build_on_row_group_with_read_format(
                &mut row_group,
                self.row_selection.clone(),
                fetch_metrics,
                &read_format,
            )
            .await?;

        let rows_in_group = self
            .context
            .reader_builder
            .parquet_metadata()
            .row_group(self.row_group_idx)
            .num_rows() as usize;
        let mut matched_row_ranges: Vec<Range<usize>> = Vec::new();
        let mut row_offset = 0;
        for batch_result in reader {
            let batch = batch_result.context(ArrowReaderSnafu {
                path: self.context.file_path(),
            })?;
            let batch_num_rows = batch.num_rows();
            matched_row_ranges.extend(
                self.context
                    .base
                    .matching_row_ranges_by_primary_key(&batch, primary_key_filter.as_mut())?
                    .into_iter()
                    .map(|range| (range.start + row_offset)..(range.end + row_offset)),
            );
            row_offset += batch_num_rows;
        }

        if matched_row_ranges.is_empty() {
            return Ok(PrefilteredFlatReaderInput::Selection(Some(
                RowSelection::from(vec![]),
            )));
        }

        if matched_row_ranges.len() == 1
            && matched_row_ranges[0].start == 0
            && matched_row_ranges[0].end == rows_in_group
        {
            return Ok(PrefilteredFlatReaderInput::Prefetched(row_group));
        }

        Ok(PrefilteredFlatReaderInput::Selection(Some(
            row_selection_from_row_ranges(matched_row_ranges.into_iter(), rows_in_group),
        )))
    }

    /// Returns the helper to compat batches.
    pub(crate) fn compat_batch(&self) -> Option<&CompatBatch> {
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

enum PrefilteredFlatReaderInput<'a> {
    Selection(Option<RowSelection>),
    Prefetched(InMemoryRowGroup<'a>),
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

    /// Builds an encoded primary-key filter for flat scan pre-filtering.
    pub(crate) fn new_primary_key_filter(&self) -> Option<Box<dyn PrimaryKeyFilter>> {
        self.base.new_primary_key_filter()
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

    /// Returns the helper to project batches.
    pub(crate) fn compaction_projection_mapper(&self) -> Option<&CompactionProjectionMapper> {
        self.base.compaction_projection_mapper.as_ref()
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

    /// Applies an encoded primary-key prefilter to the input `RecordBatch`.
    pub(crate) fn prefilter_flat_batch_by_primary_key(
        &self,
        input: RecordBatch,
        primary_key_filter: &mut dyn PrimaryKeyFilter,
    ) -> Result<Option<RecordBatch>> {
        self.base
            .prefilter_flat_batch_by_primary_key(input, primary_key_filter)
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

    /// Returns the estimated memory size of this context.
    /// Mainly accounts for the parquet metadata size.
    pub(crate) fn memory_size(&self) -> usize {
        crate::cache::cache_size::parquet_meta_size(self.reader_builder.parquet_metadata())
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
    /// Simple filters that can be compiled into encoded primary-key checks.
    ///
    /// This set is pre-validated against the SST/expected metadata and only contains
    /// tag filters on primary-key columns (excluding partition columns).
    pub(crate) primary_key_filters: Option<Arc<Vec<SimpleFilterEvaluator>>>,
    /// Dynamic filter physical exprs.
    pub(crate) dyn_filters: Vec<Arc<DynamicFilterPhysicalExpr>>,
    /// Helper to read the SST.
    pub(crate) read_format: ReadFormat,
    pub(crate) expected_metadata: Option<RegionMetadataRef>,
    /// Schema used for pruning with dynamic filters.
    pub(crate) prune_schema: Arc<Schema>,
    /// Decoder for primary keys
    pub(crate) codec: Arc<dyn PrimaryKeyCodec>,
    /// Optional helper to compat batches.
    pub(crate) compat_batch: Option<CompatBatch>,
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
    pub(crate) fn retain_usable_primary_key_filters(
        sst_metadata: &RegionMetadataRef,
        expected_metadata: Option<&RegionMetadata>,
        filters: &mut Vec<SimpleFilterEvaluator>,
    ) {
        filters.retain(|filter| {
            Self::is_usable_primary_key_filter(sst_metadata, expected_metadata, filter)
        });
    }

    fn is_usable_primary_key_filter(
        sst_metadata: &RegionMetadataRef,
        expected_metadata: Option<&RegionMetadata>,
        filter: &SimpleFilterEvaluator,
    ) -> bool {
        if is_partition_column(filter.column_name()) {
            return false;
        }

        let sst_column = match expected_metadata {
            Some(expected_metadata) => {
                let Some(expected_column) = expected_metadata.column_by_name(filter.column_name())
                else {
                    return false;
                };
                let Some(sst_column) = sst_metadata.column_by_id(expected_column.column_id) else {
                    return false;
                };

                if sst_column.column_schema.name != expected_column.column_schema.name
                    || sst_column.semantic_type != expected_column.semantic_type
                    || sst_column.column_schema.data_type != expected_column.column_schema.data_type
                {
                    return false;
                }

                sst_column
            }
            None => {
                let Some(sst_column) = sst_metadata.column_by_name(filter.column_name()) else {
                    return false;
                };
                sst_column
            }
        };

        sst_column.semantic_type == SemanticType::Tag
            && sst_metadata
                .primary_key_index(sst_column.column_id)
                .is_some()
    }

    /// Builds an encoded primary-key filter for flat scan pre-filtering.
    pub(crate) fn new_primary_key_filter(&self) -> Option<Box<dyn PrimaryKeyFilter>> {
        if self.read_format.metadata().primary_key.is_empty()
            || !self
                .read_format
                .as_flat()
                .is_some_and(|format| format.raw_batch_has_primary_key_dictionary())
        {
            return None;
        }
        let filters = self.primary_key_filters.as_ref()?;
        if filters.is_empty() {
            return None;
        }
        let filters = Arc::clone(filters);

        Some(
            self.codec
                .primary_key_filter(self.read_format.metadata(), filters),
        )
    }

    /// Applies an encoded primary-key prefilter before flat-row materialization.
    ///
    /// This only prunes rows that are guaranteed to fail simple primary-key predicates.
    /// The normal precise filter still runs after flat conversion.
    pub(crate) fn prefilter_flat_batch_by_primary_key(
        &self,
        input: RecordBatch,
        primary_key_filter: &mut dyn PrimaryKeyFilter,
    ) -> Result<Option<RecordBatch>> {
        if input.num_rows() == 0 {
            return Ok(Some(input));
        }

        let matched_row_ranges =
            self.matching_row_ranges_by_primary_key(&input, primary_key_filter)?;
        if matched_row_ranges.is_empty() {
            return Ok(None);
        }

        if matched_row_ranges.len() == 1
            && matched_row_ranges[0].start == 0
            && matched_row_ranges[0].end == input.num_rows()
        {
            return Ok(Some(input));
        }

        if matched_row_ranges.len() == 1 {
            let span = &matched_row_ranges[0];
            return Ok(Some(input.slice(span.start, span.end - span.start)));
        }

        let mut mask = vec![false; input.num_rows()];
        for span in matched_row_ranges {
            mask[span].fill(true);
        }

        let filtered =
            datatypes::arrow::compute::filter_record_batch(&input, &BooleanArray::from(mask))
                .context(ComputeArrowSnafu)?;
        if filtered.num_rows() == 0 {
            Ok(None)
        } else {
            Ok(Some(filtered))
        }
    }

    fn matching_row_ranges_by_primary_key(
        &self,
        input: &RecordBatch,
        primary_key_filter: &mut dyn PrimaryKeyFilter,
    ) -> Result<Vec<Range<usize>>> {
        let primary_key_index = primary_key_column_index(input.num_columns());
        let pk_dict_array = input
            .column(primary_key_index)
            .as_any()
            .downcast_ref::<PrimaryKeyArray>()
            .context(UnexpectedSnafu {
                reason: "Primary key column is not a dictionary array".to_string(),
            })?;
        let pk_values = pk_dict_array
            .values()
            .as_any()
            .downcast_ref::<datatypes::arrow::array::BinaryArray>()
            .context(UnexpectedSnafu {
                reason: "Primary key values are not binary array".to_string(),
            })?;
        let keys = pk_dict_array.keys();
        let key_values = keys.values();

        if key_values.is_empty() {
            return Ok(std::iter::once(0..input.num_rows()).collect());
        }

        let mut matched_row_ranges: Vec<Range<usize>> = Vec::new();
        let mut start = 0;
        while start < key_values.len() {
            let key = key_values[start];
            let mut end = start + 1;
            while end < key_values.len() && key_values[end] == key {
                end += 1;
            }

            if primary_key_filter.matches(pk_values.value(key as usize)) {
                if let Some(last) = matched_row_ranges.last_mut()
                    && last.end == start
                {
                    last.end = end;
                } else {
                    matched_row_ranges.push(start..end);
                }
            }

            start = end;
        }

        Ok(matched_row_ranges)
    }

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

            // Get the column directly by its projected index.
            // If the column is missing and it's not a tag/time column, this filter is skipped.
            // Assumes the projection indices align with the input batch schema.
            let column_idx = flat_format.projected_index_by_id(filter_ctx.column_id());
            if let Some(idx) = column_idx {
                let column = input.column(idx);
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

        // also need to consider nulls in the partition filter result. If a value is null, it should be treated as false (filtered out).
        let mut mask = boolean_array.values().clone();
        if let Some(nulls) = boolean_array.nulls() {
            mask = mask.bitand(nulls.inner());
        }

        Ok(mask)
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

#[cfg(test)]
mod tests {
    use std::fmt::{Debug, Formatter};
    use std::sync::Arc;

    use async_trait::async_trait;
    use datafusion_expr::{Expr, col, lit};
    use datatypes::arrow::array::{
        Array, ArrayRef, BinaryArray, DictionaryArray, TimestampMillisecondArray, UInt8Array,
        UInt32Array, UInt64Array,
    };
    use datatypes::arrow::datatypes::{Schema, UInt32Type};
    use datatypes::schema::ColumnSchema;
    use mito_codec::row_converter::build_primary_key_codec;
    use object_store::services::Memory;
    use store_api::codec::PrimaryKeyEncoding;
    use store_api::metadata::{ColumnMetadata, RegionMetadata, RegionMetadataBuilder};
    use store_api::region_request::PathType;
    use table::predicate::Predicate;

    use super::*;
    use crate::access_layer::{FilePathProvider, Metrics, WriteType};
    use crate::cache::CacheStrategy;
    use crate::config::IndexConfig;
    use crate::sst::file::{RegionFileId, RegionIndexId};
    use crate::sst::index::{Indexer, IndexerBuilder};
    use crate::sst::parquet::WriteOptions;
    use crate::sst::parquet::reader::{ParquetReaderBuilder, ReaderMetrics};
    use crate::sst::parquet::row_group::ParquetFetchMetricsData;
    use crate::sst::parquet::writer::ParquetWriter;
    use crate::sst::{internal_fields, location};
    use crate::test_util::sst_util::{
        new_flat_source_from_record_batches, new_primary_key, new_record_batch_by_range,
        sst_file_handle, sst_region_metadata, sst_region_metadata_with_encoding,
    };

    const FILE_DIR: &str = "/";

    #[derive(Clone)]
    struct FixedPathProvider {
        region_file_id: RegionFileId,
    }

    impl FilePathProvider for FixedPathProvider {
        fn build_index_file_path(&self, _file_id: RegionFileId) -> String {
            location::index_file_path_legacy(FILE_DIR, self.region_file_id, PathType::Bare)
        }

        fn build_index_file_path_with_version(&self, index_id: RegionIndexId) -> String {
            location::index_file_path(FILE_DIR, index_id, PathType::Bare)
        }

        fn build_sst_file_path(&self, _file_id: RegionFileId) -> String {
            location::sst_file_path(FILE_DIR, self.region_file_id, PathType::Bare)
        }
    }

    struct NoopIndexBuilder;

    impl Debug for NoopIndexBuilder {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            f.write_str("NoopIndexBuilder")
        }
    }

    #[async_trait]
    impl IndexerBuilder for NoopIndexBuilder {
        async fn build(
            &self,
            _file_id: store_api::storage::FileId,
            _index_version: u64,
        ) -> Indexer {
            Indexer::default()
        }
    }

    fn flat_file_num_columns(metadata: &RegionMetadata) -> usize {
        metadata.column_metadatas.len() + 3
    }

    fn new_test_range_base_with_metadata(
        metadata: Arc<RegionMetadata>,
        expected_metadata: Option<Arc<RegionMetadata>>,
        exprs: &[Expr],
    ) -> RangeBase {
        let read_format = ReadFormat::new_flat(
            metadata.clone(),
            metadata.column_metadatas.iter().map(|c| c.column_id),
            Some(flat_file_num_columns(&metadata)),
            "test",
            false,
        )
        .unwrap();
        let mut primary_key_filters = exprs
            .iter()
            .filter_map(SimpleFilterEvaluator::try_new)
            .collect::<Vec<_>>();
        RangeBase::retain_usable_primary_key_filters(
            &metadata,
            expected_metadata.as_deref(),
            &mut primary_key_filters,
        );
        let primary_key_filters =
            (!primary_key_filters.is_empty()).then_some(Arc::new(primary_key_filters));

        RangeBase {
            filters: vec![],
            primary_key_filters,
            dyn_filters: vec![],
            read_format,
            expected_metadata,
            prune_schema: metadata.schema.clone(),
            codec: build_primary_key_codec(metadata.as_ref()),
            compat_batch: None,
            compaction_projection_mapper: None,
            pre_filter_mode: PreFilterMode::All,
            partition_filter: None,
        }
    }

    fn new_test_range_base(exprs: &[Expr]) -> RangeBase {
        new_test_range_base_with_metadata(Arc::new(sst_region_metadata()), None, exprs)
    }

    fn new_test_range_base_with_expected_metadata(
        metadata: Arc<RegionMetadata>,
        expected_metadata: Arc<RegionMetadata>,
        exprs: &[Expr],
    ) -> RangeBase {
        new_test_range_base_with_metadata(metadata, Some(expected_metadata), exprs)
    }

    fn expected_metadata_with_reused_tag_name(
        old_metadata: &RegionMetadata,
    ) -> Arc<RegionMetadata> {
        let mut builder = RegionMetadataBuilder::new(old_metadata.region_id);
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "tag_0".to_string(),
                    ConcreteDataType::string_datatype(),
                    true,
                ),
                semantic_type: SemanticType::Tag,
                column_id: 10,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "tag_1".to_string(),
                    ConcreteDataType::string_datatype(),
                    true,
                ),
                semantic_type: SemanticType::Tag,
                column_id: 1,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "field_0".to_string(),
                    ConcreteDataType::uint64_datatype(),
                    true,
                ),
                semantic_type: SemanticType::Field,
                column_id: 2,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "ts".to_string(),
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Timestamp,
                column_id: 3,
            })
            .primary_key(vec![10, 1]);

        Arc::new(builder.build().unwrap())
    }

    fn new_raw_batch_with_metadata(
        metadata: Arc<RegionMetadata>,
        primary_keys: &[&[u8]],
        field_values: &[u64],
    ) -> RecordBatch {
        assert_eq!(primary_keys.len(), field_values.len());

        let arrow_schema = metadata.schema.arrow_schema();
        let field_column = arrow_schema
            .field(arrow_schema.index_of("field_0").unwrap())
            .clone();
        let time_index_column = arrow_schema
            .field(arrow_schema.index_of("ts").unwrap())
            .clone();
        let mut fields = vec![field_column, time_index_column];
        fields.extend(
            internal_fields()
                .into_iter()
                .map(|field| field.as_ref().clone()),
        );
        let schema = Arc::new(Schema::new(fields));

        let mut dict_values = Vec::new();
        let mut keys = Vec::with_capacity(primary_keys.len());
        for pk in primary_keys {
            let key = dict_values
                .iter()
                .position(|existing: &&[u8]| existing == pk)
                .unwrap_or_else(|| {
                    dict_values.push(*pk);
                    dict_values.len() - 1
                });
            keys.push(key as u32);
        }

        let pk_array: ArrayRef = Arc::new(DictionaryArray::<UInt32Type>::new(
            UInt32Array::from(keys),
            Arc::new(BinaryArray::from_iter_values(dict_values.iter().copied())),
        ));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt64Array::from(field_values.to_vec())),
                Arc::new(TimestampMillisecondArray::from_iter_values(
                    0..primary_keys.len() as i64,
                )),
                pk_array,
                Arc::new(UInt64Array::from(vec![1; primary_keys.len()])),
                Arc::new(UInt8Array::from(vec![1; primary_keys.len()])),
            ],
        )
        .unwrap()
    }

    fn new_raw_batch(primary_keys: &[&[u8]], field_values: &[u64]) -> RecordBatch {
        new_raw_batch_with_metadata(Arc::new(sst_region_metadata()), primary_keys, field_values)
    }

    fn field_values(batch: &RecordBatch) -> Vec<u64> {
        batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .values()
            .to_vec()
    }

    #[test]
    fn test_new_primary_key_filter_skips_non_tag_filters() {
        let base = new_test_range_base(&[col("field_0").eq(lit(1_u64)), col("ts").gt(lit(0_i64))]);

        assert!(base.new_primary_key_filter().is_none());
    }

    #[test]
    fn test_new_primary_key_filter_skips_reused_expected_tag_name() {
        let metadata = Arc::new(sst_region_metadata());
        let expected_metadata = expected_metadata_with_reused_tag_name(&metadata);
        let base = new_test_range_base_with_expected_metadata(
            metadata,
            expected_metadata,
            &[col("tag_0").eq(lit("b"))],
        );

        assert!(base.new_primary_key_filter().is_none());
    }

    #[test]
    fn test_new_primary_key_filter_skips_legacy_primary_key_batches() {
        let metadata = Arc::new(sst_region_metadata_with_encoding(
            PrimaryKeyEncoding::Sparse,
        ));
        let read_format = ReadFormat::new_flat(
            metadata.clone(),
            metadata.column_metadatas.iter().map(|c| c.column_id),
            None,
            "test",
            true,
        )
        .unwrap();
        let primary_key_filters =
            vec![SimpleFilterEvaluator::try_new(&col("tag_0").eq(lit("b"))).unwrap()];
        let base = RangeBase {
            filters: vec![],
            primary_key_filters: Some(Arc::new(primary_key_filters)),
            dyn_filters: vec![],
            read_format,
            expected_metadata: None,
            prune_schema: metadata.schema.clone(),
            codec: build_primary_key_codec(metadata.as_ref()),
            compat_batch: None,
            compaction_projection_mapper: None,
            pre_filter_mode: PreFilterMode::All,
            partition_filter: None,
        };

        assert!(base.new_primary_key_filter().is_none());
    }

    #[test]
    fn test_prefilter_primary_key_drops_single_dictionary_batch() {
        let pk_a = new_primary_key(&["a", "x"]);
        let batch = new_raw_batch(&[pk_a.as_slice(), pk_a.as_slice()], &[10, 11]);
        let base = new_test_range_base(&[col("tag_0").eq(lit("b"))]);
        let mut primary_key_filter = base.new_primary_key_filter().unwrap();

        let filtered = base
            .prefilter_flat_batch_by_primary_key(batch, primary_key_filter.as_mut())
            .unwrap();

        assert!(filtered.is_none());
    }

    #[test]
    fn test_prefilter_primary_key_builds_mask_for_fragmented_matches() {
        let pk_a = new_primary_key(&["a", "x"]);
        let pk_b = new_primary_key(&["b", "x"]);
        let pk_c = new_primary_key(&["c", "x"]);
        let pk_d = new_primary_key(&["d", "x"]);
        let batch = new_raw_batch(
            &[
                pk_a.as_slice(),
                pk_a.as_slice(),
                pk_b.as_slice(),
                pk_b.as_slice(),
                pk_c.as_slice(),
                pk_c.as_slice(),
                pk_d.as_slice(),
                pk_d.as_slice(),
            ],
            &[10, 11, 12, 13, 14, 15, 16, 17],
        );
        let base = new_test_range_base(&[col("tag_0").eq(lit("a")).or(col("tag_0").eq(lit("c")))]);
        let mut primary_key_filter = base.new_primary_key_filter().unwrap();

        let filtered = base
            .prefilter_flat_batch_by_primary_key(batch, primary_key_filter.as_mut())
            .unwrap()
            .unwrap();

        assert_eq!(filtered.num_rows(), 4);
        assert_eq!(field_values(&filtered), vec![10, 11, 14, 15]);
    }

    async fn fetch_metrics_for_predicate(predicate: Option<Predicate>) -> ParquetFetchMetricsData {
        let object_store = object_store::ObjectStore::new(Memory::default())
            .unwrap()
            .finish();
        let handle = sst_file_handle(0, 1000);
        let metadata = Arc::new(sst_region_metadata());
        let source = new_flat_source_from_record_batches(vec![new_record_batch_by_range(
            &["a", "d"],
            0,
            60,
        )]);
        let write_opts = WriteOptions {
            row_group_size: 128,
            ..Default::default()
        };

        let mut write_metrics = Metrics::new(WriteType::Flush);
        let mut writer = ParquetWriter::new_with_object_store(
            object_store.clone(),
            metadata,
            IndexConfig::default(),
            NoopIndexBuilder,
            FixedPathProvider {
                region_file_id: handle.file_id(),
            },
            &mut write_metrics,
        )
        .await;
        writer
            .write_all_flat_as_primary_key(source, None, &write_opts)
            .await
            .unwrap();

        let builder =
            ParquetReaderBuilder::new(FILE_DIR.to_string(), PathType::Bare, handle, object_store)
                .cache(CacheStrategy::Disabled)
                .flat_format(true)
                .predicate(predicate);

        let mut reader_metrics = ReaderMetrics::default();
        let (context, mut selection) = builder
            .build_reader_input(&mut reader_metrics)
            .await
            .unwrap()
            .unwrap();
        let (row_group_idx, row_selection) = selection.pop_first().unwrap();
        assert!(selection.pop_first().is_none());

        let fetch_metrics = ParquetFetchMetrics::default();
        let mut reader = FileRange::new(Arc::new(context), row_group_idx, Some(row_selection))
            .flat_reader(None, Some(&fetch_metrics))
            .await
            .unwrap()
            .unwrap();

        let mut row_count = 0;
        while let Some(batch) = reader.next_batch().unwrap() {
            row_count += batch.num_rows();
        }
        assert_eq!(row_count, 60);

        fetch_metrics.data.lock().unwrap().clone()
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_flat_reader_full_match_primary_key_filter_does_not_refetch_row_group() {
        let baseline = fetch_metrics_for_predicate(None).await;
        let full_match =
            fetch_metrics_for_predicate(Some(Predicate::new(vec![col("tag_0").eq(lit("a"))])))
                .await;

        assert_eq!(
            full_match.pages_to_fetch_store,
            baseline.pages_to_fetch_store
        );
        assert_eq!(
            full_match.page_size_to_fetch_store,
            baseline.page_size_to_fetch_store
        );
        assert_eq!(full_match.page_size_needed, baseline.page_size_needed);
    }
}
