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

//! Prefilter framework for parquet reader.
//!
//! Prefilter optimization reduces I/O by reading only a subset of columns first
//! (the prefilter phase), applying filters to compute a refined row selection,
//! then reading the remaining columns with the refined selection.

use std::collections::HashSet;
use std::ops::{BitAnd, Range};
use std::sync::Arc;

use api::v1::SemanticType;
use common_recordbatch::filter::SimpleFilterEvaluator;
use datatypes::arrow::array::{Array, BinaryArray, BooleanArray, BooleanBufferBuilder};
use datatypes::arrow::buffer::BooleanBuffer;
use datatypes::arrow::datatypes::SchemaRef;
use datatypes::arrow::record_batch::RecordBatch;
use futures::StreamExt;
use mito_codec::row_converter::{PrimaryKeyCodec, PrimaryKeyFilter};
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::RowSelection;
use parquet::schema::types::SchemaDescriptor;
use smallvec::{SmallVec, smallvec};
use snafu::{OptionExt, ResultExt};
use store_api::metadata::{RegionMetadata, RegionMetadataRef};
use store_api::storage::consts::PRIMARY_KEY_COLUMN_NAME;
use table::predicate::Predicate;

use crate::cache::PrefilterKey;
use crate::error::{
    ComputeArrowSnafu, DecodeSnafu, EvalPartitionFilterSnafu, NewRecordBatchSnafu,
    RecordBatchSnafu, Result, UnexpectedSnafu,
};
use crate::sst::parquet::file_range::PreFilterMode;
use crate::sst::parquet::flat_format::FlatReadFormat;
use crate::sst::parquet::format::PrimaryKeyArray;
use crate::sst::parquet::reader::{
    MaybeFilter, PhysicalFilterContext, RowGroupBuildContext, RowGroupReaderBuilder,
    SimpleFilterContext,
};

pub(crate) fn matching_row_ranges_by_primary_key(
    input: &RecordBatch,
    pk_column_index: usize,
    pk_filter: &mut dyn PrimaryKeyFilter,
) -> Result<Vec<Range<usize>>> {
    let pk_column = input.column(pk_column_index);
    if let Some(pk_dict_array) = pk_column.as_any().downcast_ref::<PrimaryKeyArray>() {
        matching_row_ranges_from_dict(pk_dict_array, input.num_rows(), pk_filter)
    } else if let Some(pk_binary_array) = pk_column.as_any().downcast_ref::<BinaryArray>() {
        matching_row_ranges_from_binary(pk_binary_array, input.num_rows(), pk_filter)
    } else {
        UnexpectedSnafu {
            reason: format!(
                "Primary key column is neither a dictionary nor a binary array, got {:?}",
                pk_column.data_type()
            ),
        }
        .fail()
    }
}

/// If `pk_filter` matches `pk`, records the row range `start..end`, coalescing
/// it with the previous range when adjacent.
fn push_matched_range(
    matched_row_ranges: &mut Vec<Range<usize>>,
    pk_filter: &mut dyn PrimaryKeyFilter,
    pk: &[u8],
    start: usize,
    end: usize,
) -> Result<()> {
    if pk_filter.matches(pk).context(DecodeSnafu)? {
        if let Some(last) = matched_row_ranges.last_mut()
            && last.end == start
        {
            last.end = end;
        } else {
            matched_row_ranges.push(start..end);
        }
    }
    Ok(())
}

/// Computes matched row ranges from a dictionary-encoded `__primary_key` column.
fn matching_row_ranges_from_dict(
    pk_dict_array: &PrimaryKeyArray,
    num_rows: usize,
    pk_filter: &mut dyn PrimaryKeyFilter,
) -> Result<Vec<Range<usize>>> {
    let pk_values = pk_dict_array
        .values()
        .as_any()
        .downcast_ref::<BinaryArray>()
        .context(UnexpectedSnafu {
            reason: "Primary key values are not binary array",
        })?;
    let keys = pk_dict_array.keys();
    let key_values = keys.values();

    if key_values.is_empty() {
        return Ok(std::iter::once(0..num_rows).collect());
    }

    let mut matched_row_ranges: Vec<Range<usize>> = Vec::new();
    let mut start = 0;
    while start < key_values.len() {
        let key = key_values[start];
        let mut end = start + 1;
        while end < key_values.len() && key_values[end] == key {
            end += 1;
        }

        push_matched_range(
            &mut matched_row_ranges,
            pk_filter,
            pk_values.value(key as usize),
            start,
            end,
        )?;

        start = end;
    }

    Ok(matched_row_ranges)
}

/// Computes matched row ranges from a plain binary `__primary_key` column.
///
/// The writer falls back to plain binary encoding when the `__primary_key`
/// chunk exceeds the dictionary page size limit (see `should_read_pk_as_binary`
/// in the parquet reader), so the prefilter pass must handle this form too.
fn matching_row_ranges_from_binary(
    pk_array: &BinaryArray,
    num_rows: usize,
    pk_filter: &mut dyn PrimaryKeyFilter,
) -> Result<Vec<Range<usize>>> {
    if pk_array.is_empty() {
        return Ok(std::iter::once(0..num_rows).collect());
    }

    let mut matched_row_ranges: Vec<Range<usize>> = Vec::new();
    let mut start = 0;
    while start < pk_array.len() {
        let value = pk_array.value(start);
        let mut end = start + 1;
        while end < pk_array.len() && pk_array.value(end) == value {
            end += 1;
        }

        push_matched_range(&mut matched_row_ranges, pk_filter, value, start, end)?;

        start = end;
    }

    Ok(matched_row_ranges)
}

/// Filters a flat-format record batch by primary key, returning only rows whose
/// primary key matches the filter. Returns `None` if all rows are filtered out.
pub(crate) fn prefilter_flat_batch_by_primary_key(
    input: RecordBatch,
    pk_column_index: usize,
    pk_filter: &mut dyn PrimaryKeyFilter,
) -> Result<Option<RecordBatch>> {
    if input.num_rows() == 0 {
        return Ok(Some(input));
    }

    let matched_row_ranges =
        matching_row_ranges_by_primary_key(&input, pk_column_index, pk_filter)?;
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

    let mut builder = BooleanBufferBuilder::new(input.num_rows());
    builder.append_n(input.num_rows(), false);
    for span in matched_row_ranges {
        for i in span {
            builder.set_bit(i, true);
        }
    }

    let filtered = datatypes::arrow::compute::filter_record_batch(
        &input,
        &BooleanArray::new(builder.finish(), None),
    )
    .context(ComputeArrowSnafu)?;
    if filtered.num_rows() == 0 {
        Ok(None)
    } else {
        Ok(Some(filtered))
    }
}

pub(crate) struct CachedPrimaryKeyFilter {
    inner: Box<dyn PrimaryKeyFilter>,
    last_primary_key: Vec<u8>,
    last_match: Option<bool>,
}

impl CachedPrimaryKeyFilter {
    pub(crate) fn new(inner: Box<dyn PrimaryKeyFilter>) -> Self {
        Self {
            inner,
            last_primary_key: Vec::new(),
            last_match: None,
        }
    }
}

impl PrimaryKeyFilter for CachedPrimaryKeyFilter {
    fn matches(&mut self, pk: &[u8]) -> mito_codec::error::Result<bool> {
        if let Some(last_match) = self.last_match
            && self.last_primary_key == pk
        {
            return Ok(last_match);
        }

        let matched = self.inner.matches(pk)?;
        self.last_primary_key.clear();
        self.last_primary_key.extend_from_slice(pk);
        self.last_match = Some(matched);
        Ok(matched)
    }
}

/// How the bulk-memtable read should apply each predicate.
///
/// Unlike the parquet reader, the bulk path has no prefilter pass; predicates
/// either run row-wise inside the iterator or are pushed down to encoded-PK
/// matching when the batch still carries the primary-key column.
pub(crate) struct BulkFilterPlan {
    /// Simple filters the iterator still has to evaluate row-wise on each batch.
    pub(crate) remaining_simple_filters: Vec<SimpleFilterContext>,
    /// Tag predicates lowered to encoded-PK filters. `None` when the batch
    /// already exposes raw tag columns or there are no tag predicates.
    pub(crate) pk_filters: Option<Arc<Vec<SimpleFilterEvaluator>>>,
}

/// How the parquet reader should apply each predicate.
///
/// The reader runs in two phases. Predicates routed into `prefilter_builder`
/// execute on a reduced column set first to compute a refined row selection;
/// `remaining_simple_filters` execute alongside the full projection on the
/// normal read path. The contract for what is precise vs best-effort is
/// documented on [`build_reader_filter_plan`].
pub(crate) struct ReaderFilterPlan {
    /// Simple filters that must run on the normal read path: predicates with
    /// `Matched` / `Pruned` outcomes (which carry expected-metadata
    /// compatibility decisions later phases rely on), and predicates whose
    /// column cannot be read directly during the prefilter pass.
    pub(crate) remaining_simple_filters: Vec<SimpleFilterContext>,
    /// Pre-built state for the prefilter pass, or `None` when prefiltering is
    /// not worthwhile (no prefilter columns selected, or the prefilter
    /// projection would cover nearly the full read).
    pub(crate) prefilter_builder: Option<PrefilterContextBuilder>,
}

pub(crate) fn build_bulk_filter_plan(
    read_format: &FlatReadFormat,
    predicate: Option<&Predicate>,
) -> BulkFilterPlan {
    let metadata = read_format.metadata();
    // Bulk memtable only needs simple binary filters here. Any filter that
    // cannot be reduced to a SimpleFilterContext stays out of this fast path.
    let simple_filters: Vec<SimpleFilterContext> = predicate
        .into_iter()
        .flat_map(|predicate| {
            predicate
                .exprs()
                .iter()
                .filter_map(|expr| SimpleFilterContext::new_opt(metadata, None, expr))
        })
        .collect();

    // PK prefilter only works when flat batches still carry the encoded PK
    // column. If tags have already been expanded to raw columns, the iterator
    // can apply those filters directly and there is nothing to extract here.
    if read_format.batch_has_raw_pk_columns() || metadata.primary_key.is_empty() {
        return BulkFilterPlan {
            remaining_simple_filters: simple_filters,
            pk_filters: None,
        };
    }

    let mut remaining_simple_filters = Vec::new();
    let mut pk_filters = Vec::new();

    for filter_ctx in simple_filters {
        // Split tag predicates that can be evaluated against the encoded PK
        // from filters that still need normal row-wise evaluation later.
        let pk_filter = filter_ctx.filter().as_filter().and_then(|filter| {
            (filter_ctx.semantic_type() == SemanticType::Tag).then(|| filter.clone())
        });

        if let Some(pk_filter) = pk_filter {
            pk_filters.push(pk_filter);
        } else {
            remaining_simple_filters.push(filter_ctx);
        }
    }

    BulkFilterPlan {
        remaining_simple_filters,
        pk_filters: (!pk_filters.is_empty()).then_some(Arc::new(pk_filters)),
    }
}

/// Splits a query [`Predicate`] into a [`ReaderFilterPlan`]: predicates that can run
/// during the prefilter pass (on a reduced projection, to compute a refined row
/// selection) versus predicates that must run on the normal read path (alongside the
/// full projection).
///
/// The prefilter pass is *best-effort pruning*: a physical-filter predicate is silently
/// dropped when [`PhysicalFilterContext::new_opt`] returns `None` (column not in the
/// projected arrow schema). This is safe because the DataFusion `FilterExec` above the
/// reader always re-applies the original predicate, so the prefilter pass is purely a
/// pruning hint.
///
/// Tag and timestamp predicates that lower to [`SimpleFilterEvaluator`] are an
/// exception — the engine enforces them precisely, so the prefilter pass is the only
/// place they execute. They are never silently dropped.
pub(crate) fn build_reader_filter_plan(
    predicate: Option<&Predicate>,
    expected_metadata: Option<&RegionMetadata>,
    pre_filter_mode: PreFilterMode,
    read_format: &FlatReadFormat,
    codec: &Arc<dyn PrimaryKeyCodec>,
) -> ReaderFilterPlan {
    let Some(predicate) = predicate else {
        return ReaderFilterPlan {
            remaining_simple_filters: Vec::new(),
            prefilter_builder: None,
        };
    };

    let metadata = read_format.metadata();
    let mut prefilter_simple_filters = Vec::new();
    let mut remaining_simple_filters = Vec::new();
    let mut prefilter_physical_filters = Vec::new();
    let mut primary_key_filters = Vec::new();
    let mut pk_filter_contexts = Vec::new();

    // `SkipFields` keeps field predicates in the normal read path to avoid a
    // second read of projected field columns, while tags/timestamp can still
    // participate in prefiltering.
    let field_prefilter_enabled = pre_filter_mode == PreFilterMode::All;
    // When true, tag columns are encoded in the primary key column and are NOT
    // stored as separate parquet columns. Tag predicates must go through PK
    // decoding rather than direct column reads.
    let need_pk_prefilter = !read_format.batch_has_raw_pk_columns();

    // Whether a column can be read directly from parquet for prefiltering,
    // based on its semantic type and the current mode/format.
    let can_direct_prefilter = |semantic_type: SemanticType| -> bool {
        match semantic_type {
            SemanticType::Tag => !need_pk_prefilter,
            SemanticType::Field => field_prefilter_enabled,
            SemanticType::Timestamp => true,
        }
    };

    for expr in predicate.exprs() {
        // Prefer cheap simple filters first. They also preserve `Matched` /
        // `Pruned` states for columns that only exist in expected metadata.
        if let Some(filter_ctx) = SimpleFilterContext::new_opt(metadata, expected_metadata, expr) {
            // `Matched` and `Pruned` come from expected-metadata compatibility
            // and must stay in the main filter list so later phases keep that
            // outcome.
            let Some(filter) = filter_ctx.filter().as_filter() else {
                remaining_simple_filters.push(filter_ctx);
                continue;
            };

            // If the column is stored as a separate parquet column and is already projected in the main read,
            // we can evaluate the simple filter directly during prefilter.
            let direct_prefilter = can_direct_prefilter(filter_ctx.semantic_type());
            if direct_prefilter {
                assert!(
                    read_format
                        .arrow_schema()
                        .column_with_name(filter.column_name())
                        .is_some(),
                    "Column '{}' is not present in the arrow schema {:?}",
                    filter.column_name(),
                    read_format.arrow_schema(),
                );
                prefilter_simple_filters.push(filter_ctx);
                continue;
            }

            // Otherwise try to filter through encoded-PK matching.
            if need_pk_prefilter && filter_ctx.semantic_type() == SemanticType::Tag {
                primary_key_filters.push(filter.clone());
                pk_filter_contexts.push(filter_ctx);
            } else {
                remaining_simple_filters.push(filter_ctx);
            }
            continue;
        }

        // Best-effort physical-filter prefilter (see fn-level doc): `new_opt`
        // returning `None` means the column is not in the projected arrow
        // schema, and dropping the predicate is safe because the upper
        // `FilterExec` re-applies it.
        if let Some(filter) =
            PhysicalFilterContext::new_opt(metadata, expected_metadata, read_format, expr)
            && can_direct_prefilter(filter.semantic_type())
        {
            prefilter_physical_filters.push(filter);
        }
    }

    let pk_filter_expr_strs = (!pk_filter_contexts.is_empty()).then(|| {
        let mut expr_strs = pk_filter_contexts
            .iter()
            .map(|filter_ctx| filter_ctx.expr_str().to_string())
            .collect::<Vec<_>>();
        expr_strs.sort();
        SmallVec::from_vec(expr_strs)
    });
    let pk_filter_exprs =
        (!primary_key_filters.is_empty()).then_some(Arc::new(primary_key_filters));
    let schema_version = expected_metadata
        .map(|metadata| metadata.schema_version)
        .unwrap_or_else(|| read_format.metadata().schema_version);
    let prefilter_builder = PrefilterContextBuilder::new(
        read_format,
        codec,
        pk_filter_exprs,
        pk_filter_expr_strs,
        prefilter_simple_filters.clone(),
        prefilter_physical_filters,
        schema_version,
    );

    if prefilter_builder.is_some() {
        ReaderFilterPlan {
            remaining_simple_filters,
            prefilter_builder,
        }
    } else {
        // If prefilter setup is not worthwhile, keep the original simple
        // filters on the normal path so behavior is unchanged.
        remaining_simple_filters.extend(prefilter_simple_filters);
        remaining_simple_filters.extend(pk_filter_contexts);
        ReaderFilterPlan {
            remaining_simple_filters,
            prefilter_builder: None,
        }
    }
}

/// Context for prefiltering a row group.
pub(crate) struct PrefilterContext {
    /// Optional PK filter for legacy primary-key-format parquet.
    pk_filter: Option<Box<dyn PrimaryKeyFilter>>,
    /// Simple filters that can be evaluated directly from the prefilter batch.
    filters: Vec<SimpleFilterContext>,
    /// Physical filters that can be evaluated directly from the prefilter batch.
    /// Physical expressions are only applied in the prefilter phase.
    physical_filters: Vec<PhysicalFilterContext>,
    /// Region schema version used in per-filter cache keys.
    schema_version: u64,
    /// Sorted expression strings for the encoded-PK filter group.
    pk_filter_expr_strs: Option<SmallVec<[String; 1]>>,
    /// Arrow schema used to build narrowed prefilter projections.
    arrow_schema: SchemaRef,
}

/// Pre-built state for constructing [PrefilterContext] per row group.
///
/// Fields invariant across row groups (projection mask, codec, metadata, filters)
/// are computed once. A fresh [PrefilterContext] with its own mutable PK filter
/// is created via [PrefilterContextBuilder::build()] for each row group.
pub(crate) struct PrefilterContextBuilder {
    pk_filters: Option<Arc<Vec<SimpleFilterEvaluator>>>,
    pk_filter_expr_strs: Option<SmallVec<[String; 1]>>,
    filters: Vec<SimpleFilterContext>,
    physical_filters: Vec<PhysicalFilterContext>,
    codec: Arc<dyn PrimaryKeyCodec>,
    metadata: RegionMetadataRef,
    schema_version: u64,
    arrow_schema: SchemaRef,
}

impl PrefilterContextBuilder {
    /// Creates a builder if prefiltering is applicable.
    ///
    /// Returns `None` if:
    /// - The read format doesn't use flat layout
    /// - No prefilter columns are selected
    /// - Prefilter would read the full projection without any PK filter
    pub(crate) fn new(
        read_format: &FlatReadFormat,
        codec: &Arc<dyn PrimaryKeyCodec>,
        primary_key_filters: Option<Arc<Vec<SimpleFilterEvaluator>>>,
        primary_key_filter_expr_strs: Option<SmallVec<[String; 1]>>,
        filters: Vec<SimpleFilterContext>,
        physical_filters: Vec<PhysicalFilterContext>,
        schema_version: u64,
    ) -> Option<Self> {
        let metadata = read_format.metadata();
        let use_raw_tag_columns = read_format.batch_has_raw_pk_columns();
        let pk_filters = (!use_raw_tag_columns)
            .then_some(primary_key_filters)
            .flatten()
            .filter(|filters| !filters.is_empty());
        let pk_filter_expr_strs = pk_filters
            .is_some()
            .then_some(primary_key_filter_expr_strs)
            .flatten();

        let mut prefilter_column_names = HashSet::new();
        for filter_ctx in &filters {
            if let MaybeFilter::Filter(filter) = filter_ctx.filter() {
                prefilter_column_names.insert(filter.column_name().to_string());
            }
        }

        if pk_filters.is_some() {
            prefilter_column_names.insert(PRIMARY_KEY_COLUMN_NAME.to_string());
        }

        for filter_ctx in &physical_filters {
            prefilter_column_names.insert(filter_ctx.column_name().to_string());
        }

        let prefilter_count =
            compute_projection_count(&prefilter_column_names, read_format.arrow_schema());

        if prefilter_count == 0 {
            return None;
        }

        let total_count = read_format.parquet_read_columns().root_indices().len();
        let remaining_count = total_count.saturating_sub(prefilter_count);
        if pk_filters.is_none() && prefilter_count >= total_count {
            return None;
        }

        if pk_filters.is_none()
            && !should_use_prefilter(prefilter_count, remaining_count, total_count)
        {
            return None;
        }

        Some(Self {
            pk_filters,
            pk_filter_expr_strs,
            filters,
            physical_filters,
            codec: Arc::clone(codec),
            metadata: metadata.clone(),
            schema_version,
            arrow_schema: read_format.arrow_schema().clone(),
        })
    }

    /// Builds a [PrefilterContext] for a specific row group.
    pub(crate) fn build(&self) -> PrefilterContext {
        let pk_filter = self.pk_filters.as_ref().map(|pk_filters| {
            let pk_filter = self
                .codec
                .primary_key_filter(&self.metadata, Arc::clone(pk_filters));
            Box::new(CachedPrimaryKeyFilter::new(pk_filter)) as Box<dyn PrimaryKeyFilter>
        });
        PrefilterContext {
            pk_filter,
            filters: self.filters.clone(),
            physical_filters: self.physical_filters.clone(),
            schema_version: self.schema_version,
            pk_filter_expr_strs: self.pk_filter_expr_strs.clone(),
            arrow_schema: self.arrow_schema.clone(),
        }
    }
}

const PREFILTER_COLUMN_RATIO_THRESHOLD: f64 = 0.5;
const PREFILTER_MIN_REMAINING_COLUMNS: usize = 2;

/// Result of prefiltering a row group.
pub(crate) struct PrefilterResult {
    /// Refined row selection after prefiltering.
    pub(crate) refined_selection: RowSelection,
    /// Number of rows filtered out by prefiltering.
    pub(crate) filtered_rows: usize,
}

/// Executes prefiltering on a row group.
///
/// Reads only the prefilter columns (currently the PK dictionary column),
/// applies filters, and returns a refined [RowSelection].
fn compute_projection_mask(
    column_names: &HashSet<String>,
    arrow_schema: &datatypes::arrow::datatypes::SchemaRef,
    parquet_schema: &SchemaDescriptor,
) -> ProjectionMask {
    ProjectionMask::roots(
        parquet_schema,
        projection_indices(column_names, arrow_schema),
    )
}

fn compute_projection_count(
    column_names: &HashSet<String>,
    arrow_schema: &datatypes::arrow::datatypes::SchemaRef,
) -> usize {
    projection_indices(column_names, arrow_schema).len()
}

fn projection_indices(
    column_names: &HashSet<String>,
    arrow_schema: &datatypes::arrow::datatypes::SchemaRef,
) -> Vec<usize> {
    let mut projection_indices: Vec<usize> = column_names
        .iter()
        .filter_map(|name| arrow_schema.column_with_name(name).map(|(index, _)| index))
        .collect();
    projection_indices.sort_unstable();
    projection_indices.dedup();
    projection_indices
}

fn should_use_prefilter(
    prefilter_count: usize,
    remaining_count: usize,
    total_count: usize,
) -> bool {
    if remaining_count == 0 {
        return false;
    }

    if remaining_count < PREFILTER_MIN_REMAINING_COLUMNS {
        return false;
    }

    let ratio = prefilter_count as f64 / total_count as f64;
    ratio <= PREFILTER_COLUMN_RATIO_THRESHOLD
}

pub(crate) async fn execute_prefilter(
    prefilter_ctx: &mut PrefilterContext,
    reader_builder: &RowGroupReaderBuilder,
    build_ctx: &RowGroupBuildContext<'_>,
) -> Result<PrefilterResult> {
    let entries = build_prefilter_cache_entries(prefilter_ctx, reader_builder, build_ctx);

    if entries.is_empty() {
        return execute_prefilter_by_reading_columns(prefilter_ctx, reader_builder, build_ctx)
            .await;
    }

    execute_prefilter_with_result_cache(prefilter_ctx, reader_builder, build_ctx, entries).await
}

async fn execute_prefilter_with_result_cache(
    prefilter_ctx: &mut PrefilterContext,
    reader_builder: &RowGroupReaderBuilder,
    build_ctx: &RowGroupBuildContext<'_>,
    entries: Vec<PrefilterEntry>,
) -> Result<PrefilterResult> {
    let non_cacheable_physical = non_cacheable_physical_filters(prefilter_ctx);
    let mut hit_mask: Option<BooleanBuffer> = None;
    let mut misses = Vec::new();
    for entry in entries {
        let Some(key) = &entry.key else {
            misses.push(entry);
            continue;
        };

        if let Some(mask) = reader_builder.cache_strategy().get_prefilter_result(key) {
            hit_mask = Some(match hit_mask {
                Some(hit_mask) => hit_mask.bitand(mask.as_ref()),
                None => mask.as_ref().clone(),
            });
        } else {
            misses.push(entry);
        }
    }

    if misses.is_empty() && non_cacheable_physical.is_empty() {
        let combined_mask = hit_mask.unwrap_or_else(|| BooleanBuffer::new_set(0));
        let refined_selection =
            refined_selection_from_mask(&combined_mask, &build_ctx.row_selection);
        let rows_before_filter = rows_before_filter(reader_builder, build_ctx);
        let filtered_rows = rows_before_filter.saturating_sub(refined_selection.row_count());
        return Ok(PrefilterResult {
            refined_selection,
            filtered_rows,
        });
    }

    let mut uncached_entries = misses;
    uncached_entries.extend(
        non_cacheable_physical
            .iter()
            .copied()
            .map(|idx| PrefilterEntry::without_cache(PrefilterEntryKind::Physical(idx))),
    );
    let (uncached_mask, read_rows) =
        build_prefilter_masks(prefilter_ctx, reader_builder, build_ctx, &uncached_entries).await?;

    let final_mask = match (hit_mask, uncached_mask) {
        (Some(hit_mask), Some(uncached_mask)) => hit_mask.bitand(&uncached_mask),
        (Some(hit_mask), None) => hit_mask,
        (None, Some(uncached_mask)) => uncached_mask,
        (None, None) => BooleanBuffer::new_set(read_rows),
    };
    debug_assert_eq!(final_mask.len(), read_rows);
    let rows_selected = final_mask.count_set_bits();
    let filtered_rows = read_rows.saturating_sub(rows_selected);
    let refined_selection = refined_selection_from_mask(&final_mask, &build_ctx.row_selection);

    Ok(PrefilterResult {
        refined_selection,
        filtered_rows,
    })
}

fn non_cacheable_physical_filters(prefilter_ctx: &PrefilterContext) -> Vec<usize> {
    prefilter_ctx
        .physical_filters
        .iter()
        .enumerate()
        .filter_map(|(idx, filter)| (!filter.is_immutable()).then_some(idx))
        .collect()
}

async fn build_prefilter_masks(
    prefilter_ctx: &mut PrefilterContext,
    reader_builder: &RowGroupReaderBuilder,
    build_ctx: &RowGroupBuildContext<'_>,
    entries: &[PrefilterEntry],
) -> Result<(Option<BooleanBuffer>, usize)> {
    let prefilter_column_names = prefilter_column_names_for_entries(prefilter_ctx, entries);
    let parquet_schema = reader_builder
        .parquet_metadata()
        .file_metadata()
        .schema_descr();
    let projection = compute_projection_mask(
        &prefilter_column_names,
        &prefilter_ctx.arrow_schema,
        parquet_schema,
    );

    let mut stream = reader_builder
        .build_with_projection(
            build_ctx.row_group_idx,
            build_ctx.row_selection.clone(),
            projection,
            build_ctx.fetch_metrics,
        )
        .await?;

    let mut cache_builders = entries
        .iter()
        .map(|entry| entry.key.is_some().then(|| BooleanBufferBuilder::new(0)))
        .collect::<Vec<_>>();
    let mut combined_builder = (!entries.is_empty()).then(|| BooleanBufferBuilder::new(0));
    let mut rows_before_filter = 0usize;

    while let Some(batch_result) = stream.next().await {
        let batch = batch_result?;
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            continue;
        }
        rows_before_filter += num_rows;

        let mut batch_mask = BooleanBuffer::new_set(num_rows);
        for (idx, entry) in entries.iter().enumerate() {
            let mask = eval_entry_mask(
                &batch,
                prefilter_ctx,
                entry.kind,
                reader_builder.file_path(),
            )?;
            batch_mask = batch_mask.bitand(&mask);
            if let Some(Some(builder)) = cache_builders.get_mut(idx) {
                builder.append_buffer(&mask);
            }
        }
        if let Some(builder) = &mut combined_builder {
            builder.append_buffer(&batch_mask);
        }
    }

    for (entry, builder) in entries.iter().zip(cache_builders) {
        if let (Some(key), Some(mut builder)) = (&entry.key, builder) {
            reader_builder
                .cache_strategy()
                .put_prefilter_result(key.clone(), Arc::new(builder.finish()));
        }
    }

    Ok((
        combined_builder.map(|mut builder| builder.finish()),
        rows_before_filter,
    ))
}

fn prefilter_column_names_for_entries(
    prefilter_ctx: &PrefilterContext,
    entries: &[PrefilterEntry],
) -> HashSet<String> {
    let mut prefilter_column_names = HashSet::new();
    for entry in entries {
        match entry.kind {
            PrefilterEntryKind::Simple(idx) => {
                if let MaybeFilter::Filter(filter) = prefilter_ctx.filters[idx].filter() {
                    prefilter_column_names.insert(filter.column_name().to_string());
                }
            }
            PrefilterEntryKind::Physical(idx) => {
                prefilter_column_names.insert(
                    prefilter_ctx.physical_filters[idx]
                        .column_name()
                        .to_string(),
                );
            }
            PrefilterEntryKind::PkGroup => {
                prefilter_column_names.insert(PRIMARY_KEY_COLUMN_NAME.to_string());
            }
        }
    }
    prefilter_column_names
}

async fn execute_prefilter_by_reading_columns(
    prefilter_ctx: &mut PrefilterContext,
    reader_builder: &RowGroupReaderBuilder,
    build_ctx: &RowGroupBuildContext<'_>,
) -> Result<PrefilterResult> {
    let entries = all_prefilter_entries(prefilter_ctx);
    let (mask, rows_before_filter) =
        build_prefilter_masks(prefilter_ctx, reader_builder, build_ctx, &entries).await?;

    let final_mask = mask.unwrap_or_else(|| BooleanBuffer::new_set(rows_before_filter));
    let rows_selected = final_mask.count_set_bits();
    let filtered_rows = rows_before_filter.saturating_sub(rows_selected);
    let refined_selection = refined_selection_from_mask(&final_mask, &build_ctx.row_selection);

    Ok(PrefilterResult {
        refined_selection,
        filtered_rows,
    })
}

fn all_prefilter_entries(prefilter_ctx: &PrefilterContext) -> Vec<PrefilterEntry> {
    let mut entries = Vec::new();
    if prefilter_ctx.pk_filter.is_some() {
        entries.push(PrefilterEntry::without_cache(PrefilterEntryKind::PkGroup));
    }
    entries.extend(
        prefilter_ctx
            .filters
            .iter()
            .enumerate()
            .map(|(idx, _)| PrefilterEntry::without_cache(PrefilterEntryKind::Simple(idx))),
    );
    entries.extend(
        prefilter_ctx
            .physical_filters
            .iter()
            .enumerate()
            .map(|(idx, _)| PrefilterEntry::without_cache(PrefilterEntryKind::Physical(idx))),
    );
    entries
}

#[derive(Clone, Copy)]
enum PrefilterEntryKind {
    Simple(usize),
    Physical(usize),
    PkGroup,
}

struct PrefilterEntry {
    kind: PrefilterEntryKind,
    key: Option<PrefilterKey>,
}

impl PrefilterEntry {
    fn without_cache(kind: PrefilterEntryKind) -> Self {
        Self { kind, key: None }
    }
}

fn build_prefilter_cache_entries(
    prefilter_ctx: &PrefilterContext,
    reader_builder: &RowGroupReaderBuilder,
    build_ctx: &RowGroupBuildContext<'_>,
) -> Vec<PrefilterEntry> {
    let row_selection = PrefilterKey::row_selection_snapshot(build_ctx.row_selection.as_ref());
    let file_id = reader_builder.file_handle().file_id().file_id();
    let row_group_idx = build_ctx.row_group_idx as u32;
    let mut entries = Vec::new();

    for (idx, filter_ctx) in prefilter_ctx.filters.iter().enumerate() {
        entries.push(PrefilterEntry {
            kind: PrefilterEntryKind::Simple(idx),
            key: Some(PrefilterKey::new(
                file_id,
                row_group_idx,
                row_selection.clone(),
                prefilter_ctx.schema_version,
                smallvec![filter_ctx.expr_str().to_string()],
            )),
        });
    }

    for (idx, filter_ctx) in prefilter_ctx.physical_filters.iter().enumerate() {
        if !filter_ctx.is_immutable() {
            continue;
        }
        entries.push(PrefilterEntry {
            kind: PrefilterEntryKind::Physical(idx),
            key: Some(PrefilterKey::new(
                file_id,
                row_group_idx,
                row_selection.clone(),
                prefilter_ctx.schema_version,
                smallvec![filter_ctx.expr_str().to_string()],
            )),
        });
    }

    if prefilter_ctx.pk_filter.is_some()
        && let Some(exprs) = &prefilter_ctx.pk_filter_expr_strs
    {
        entries.push(PrefilterEntry {
            kind: PrefilterEntryKind::PkGroup,
            key: Some(PrefilterKey::new(
                file_id,
                row_group_idx,
                row_selection,
                prefilter_ctx.schema_version,
                exprs.clone(),
            )),
        });
    }

    entries
}

fn rows_before_filter(
    reader_builder: &RowGroupReaderBuilder,
    build_ctx: &RowGroupBuildContext<'_>,
) -> usize {
    build_ctx.row_selection.as_ref().map_or_else(
        || {
            reader_builder
                .parquet_metadata()
                .row_group(build_ctx.row_group_idx)
                .num_rows() as usize
        },
        RowSelection::row_count,
    )
}

fn refined_selection_from_mask(
    mask: &BooleanBuffer,
    original_selection: &Option<RowSelection>,
) -> RowSelection {
    if mask.is_empty() || mask.count_set_bits() == 0 {
        return RowSelection::from(vec![]);
    }

    let prefilter_selection = RowSelection::from_filters(&[BooleanArray::from(mask.clone())]);
    match original_selection {
        Some(original) => original.and_then(&prefilter_selection),
        None => prefilter_selection,
    }
}

fn eval_entry_mask(
    batch: &RecordBatch,
    prefilter_ctx: &mut PrefilterContext,
    kind: PrefilterEntryKind,
    file_path: &str,
) -> Result<BooleanBuffer> {
    match kind {
        PrefilterEntryKind::Simple(idx) => {
            eval_simple_filter_mask(batch, &prefilter_ctx.filters[idx], file_path)
        }
        PrefilterEntryKind::Physical(idx) => {
            eval_physical_filter_mask(batch, &prefilter_ctx.physical_filters[idx], file_path)
        }
        PrefilterEntryKind::PkGroup => {
            let pk_filter = prefilter_ctx.pk_filter.as_mut().context(UnexpectedSnafu {
                reason: "Missing primary key filter for prefilter cache entry",
            })?;
            eval_pk_group_mask(batch, pk_filter.as_mut())
        }
    }
}

fn eval_pk_group_mask(
    batch: &RecordBatch,
    pk_filter: &mut dyn PrimaryKeyFilter,
) -> Result<BooleanBuffer> {
    let (pk_column_index, _) = batch
        .schema()
        .column_with_name(PRIMARY_KEY_COLUMN_NAME)
        .context(UnexpectedSnafu {
            reason: "Primary key column not found in prefilter batch",
        })?;
    let matched_row_ranges = matching_row_ranges_by_primary_key(batch, pk_column_index, pk_filter)?;
    let mut builder = BooleanBufferBuilder::new(batch.num_rows());
    builder.append_n(batch.num_rows(), false);
    for range in matched_row_ranges {
        for row in range {
            builder.set_bit(row, true);
        }
    }
    Ok(builder.finish())
}

fn eval_simple_filter_mask(
    batch: &RecordBatch,
    filter_ctx: &SimpleFilterContext,
    file_path: &str,
) -> Result<BooleanBuffer> {
    let filter = match filter_ctx.filter() {
        MaybeFilter::Filter(filter) => filter,
        MaybeFilter::Matched => return Ok(BooleanBuffer::new_set(batch.num_rows())),
        MaybeFilter::Pruned => return Ok(BooleanBuffer::new_unset(batch.num_rows())),
    };

    let (idx, _) = batch
        .schema()
        .column_with_name(filter.column_name())
        .with_context(|| UnexpectedSnafu {
            reason: format!(
                "Prefilter column '{}' (id {}) not found in batch for file {}",
                filter.column_name(),
                filter_ctx.column_id(),
                file_path
            ),
        })?;
    let column = batch.column(idx).clone();
    filter.evaluate_array(&column).context(RecordBatchSnafu)
}

fn eval_physical_filter_mask(
    batch: &RecordBatch,
    filter_ctx: &PhysicalFilterContext,
    file_path: &str,
) -> Result<BooleanBuffer> {
    let filter = filter_ctx.filter();

    let (idx, _) = batch
        .schema()
        .column_with_name(filter_ctx.column_name())
        .with_context(|| UnexpectedSnafu {
            reason: format!(
                "Prefilter physical column '{}' (id {}) not found in batch for file {}",
                filter_ctx.column_name(),
                filter_ctx.column_id(),
                file_path
            ),
        })?;
    let column = batch.column(idx).clone();

    let record_batch = RecordBatch::try_new(filter_ctx.schema().clone(), vec![column])
        .context(NewRecordBatchSnafu)?;
    let evaluated = filter
        .evaluate(&record_batch)
        .context(EvalPartitionFilterSnafu)?;
    let array = evaluated
        .into_array(record_batch.num_rows())
        .context(EvalPartitionFilterSnafu)?;
    let boolean_array = array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .context(UnexpectedSnafu {
            reason: "Failed to downcast physical filter result to BooleanArray",
        })?;
    // Treat null results as false (filtered out); value bits are not guaranteed
    // to be false for invalid entries.
    let mut result = boolean_array.values().clone();
    if let Some(nulls) = boolean_array.nulls() {
        result = result.bitand(nulls.inner());
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use common_recordbatch::filter::SimpleFilterEvaluator;
    use datafusion_expr::{col, lit};
    use datatypes::arrow::array::{
        ArrayRef, DictionaryArray, TimestampMillisecondArray, UInt8Array, UInt32Array, UInt64Array,
    };
    use datatypes::arrow::datatypes::{DataType, Field, Schema, UInt32Type};
    use mito_codec::row_converter::{PrimaryKeyFilter, build_primary_key_codec};
    use store_api::codec::PrimaryKeyEncoding;

    use super::*;
    use crate::read::read_columns::ReadColumns;
    use crate::sst::internal_fields;
    use crate::sst::parquet::flat_format::{FlatReadFormat, primary_key_column_index};
    use crate::test_util::sst_util::{
        new_primary_key, new_record_batch_with_custom_sequence, sst_region_metadata,
        sst_region_metadata_with_encoding,
    };

    struct CountingPrimaryKeyFilter {
        hits: Arc<AtomicUsize>,
        expected: Vec<u8>,
    }

    impl PrimaryKeyFilter for CountingPrimaryKeyFilter {
        fn matches(&mut self, pk: &[u8]) -> mito_codec::error::Result<bool> {
            self.hits.fetch_add(1, Ordering::Relaxed);
            Ok(pk == self.expected.as_slice())
        }
    }

    #[test]
    fn test_cached_primary_key_filter_reuses_previous_result() {
        let expected = new_primary_key(&["a", "x"]);
        let hits = Arc::new(AtomicUsize::new(0));
        let mut filter = CachedPrimaryKeyFilter::new(Box::new(CountingPrimaryKeyFilter {
            hits: Arc::clone(&hits),
            expected: expected.clone(),
        }));

        assert!(filter.matches(expected.as_slice()).unwrap());
        assert!(filter.matches(expected.as_slice()).unwrap());
        assert!(
            !filter
                .matches(new_primary_key(&["b", "x"]).as_slice())
                .unwrap()
        );

        assert_eq!(hits.load(Ordering::Relaxed), 2);
    }

    fn new_test_filters(exprs: &[datafusion_expr::Expr]) -> Vec<SimpleFilterEvaluator> {
        exprs
            .iter()
            .filter_map(SimpleFilterEvaluator::try_new)
            .collect()
    }

    fn new_simple_filter_contexts(
        metadata: &RegionMetadataRef,
        exprs: &[datafusion_expr::Expr],
    ) -> Vec<SimpleFilterContext> {
        exprs
            .iter()
            .filter_map(|expr| SimpleFilterContext::new_opt(metadata, None, expr))
            .collect()
    }

    fn new_physical_filter_contexts(
        metadata: &RegionMetadataRef,
        read_format: &FlatReadFormat,
        exprs: &[datafusion_expr::Expr],
    ) -> Vec<PhysicalFilterContext> {
        exprs
            .iter()
            .filter_map(|expr| PhysicalFilterContext::new_opt(metadata, None, read_format, expr))
            .collect()
    }

    fn new_raw_batch(primary_keys: &[&[u8]], field_values: &[u64]) -> RecordBatch {
        assert_eq!(primary_keys.len(), field_values.len());

        let metadata = Arc::new(sst_region_metadata());
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

    fn new_prefilter_batch(primary_keys: &[&[u8]], field_values: &[u64]) -> RecordBatch {
        assert_eq!(primary_keys.len(), field_values.len());

        let metadata = Arc::new(sst_region_metadata());
        let arrow_schema = metadata.schema.arrow_schema();
        let field_column = arrow_schema
            .field(arrow_schema.index_of("field_0").unwrap())
            .clone();
        let time_index_column = arrow_schema
            .field(arrow_schema.index_of("ts").unwrap())
            .clone();
        let schema = Arc::new(Schema::new(vec![
            field_column,
            time_index_column,
            internal_fields()[0].as_ref().clone(),
        ]));

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
            ],
        )
        .unwrap()
    }

    fn new_prefilter_batch_binary_pk(primary_keys: &[&[u8]], field_values: &[u64]) -> RecordBatch {
        assert_eq!(primary_keys.len(), field_values.len());

        let metadata = Arc::new(sst_region_metadata());
        let arrow_schema = metadata.schema.arrow_schema();
        let field_column = arrow_schema
            .field(arrow_schema.index_of("field_0").unwrap())
            .clone();
        let time_index_column = arrow_schema
            .field(arrow_schema.index_of("ts").unwrap())
            .clone();
        let schema = Arc::new(Schema::new(vec![
            field_column,
            time_index_column,
            Field::new(PRIMARY_KEY_COLUMN_NAME, DataType::Binary, false),
        ]));

        let pk_array: ArrayRef =
            Arc::new(BinaryArray::from_iter_values(primary_keys.iter().copied()));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt64Array::from(field_values.to_vec())),
                Arc::new(TimestampMillisecondArray::from_iter_values(
                    0..primary_keys.len() as i64,
                )),
                pk_array,
            ],
        )
        .unwrap()
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

    fn remaining_simple_filter_columns(filters: &[SimpleFilterContext]) -> Vec<&str> {
        filters
            .iter()
            .map(|filter_ctx| filter_ctx.filter().as_filter().unwrap().column_name())
            .collect()
    }

    #[test]
    fn test_prefilter_primary_key_drops_single_dictionary_batch() {
        let metadata = Arc::new(sst_region_metadata());
        let filters = Arc::new(new_test_filters(&[col("tag_0").eq(lit("b"))]));
        let mut primary_key_filter =
            build_primary_key_codec(metadata.as_ref()).primary_key_filter(&metadata, filters);
        let pk_a = new_primary_key(&["a", "x"]);
        let batch = new_raw_batch(&[pk_a.as_slice(), pk_a.as_slice()], &[10, 11]);
        let pk_col_idx = primary_key_column_index(batch.num_columns());

        let filtered =
            prefilter_flat_batch_by_primary_key(batch, pk_col_idx, primary_key_filter.as_mut())
                .unwrap();

        assert!(filtered.is_none());
    }

    #[test]
    fn test_prefilter_primary_key_builds_mask_for_fragmented_matches() {
        let metadata = Arc::new(sst_region_metadata());
        let filters = Arc::new(new_test_filters(&[col("tag_0")
            .eq(lit("a"))
            .or(col("tag_0").eq(lit("c")))]));
        let mut primary_key_filter =
            build_primary_key_codec(metadata.as_ref()).primary_key_filter(&metadata, filters);
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
        let pk_col_idx = primary_key_column_index(batch.num_columns());

        let filtered =
            prefilter_flat_batch_by_primary_key(batch, pk_col_idx, primary_key_filter.as_mut())
                .unwrap()
                .unwrap();

        assert_eq!(filtered.num_rows(), 4);
        assert_eq!(field_values(&filtered), vec![10, 11, 14, 15]);
    }

    #[test]
    fn test_prefilter_builder_returns_none_without_selected_filters() {
        let metadata: RegionMetadataRef =
            Arc::new(sst_region_metadata_with_encoding(PrimaryKeyEncoding::Dense));
        let read_format = FlatReadFormat::new(
            metadata.clone(),
            ReadColumns::from_deduped_column_ids(
                metadata.column_metadatas.iter().map(|c| c.column_id),
            ),
            None,
            "test",
            false,
        )
        .unwrap();
        let codec = build_primary_key_codec(metadata.as_ref());

        let builder = PrefilterContextBuilder::new(
            &read_format,
            &codec,
            None,
            None,
            Vec::new(),
            Vec::new(),
            metadata.schema_version,
        );
        assert!(builder.is_none());
    }

    #[test]
    fn test_should_use_prefilter() {
        assert!(should_use_prefilter(1, 5, 6));
        assert!(!should_use_prefilter(1, 0, 1));
        assert!(!should_use_prefilter(1, 1, 2));
        assert!(!should_use_prefilter(4, 3, 7));
        assert!(should_use_prefilter(3, 3, 6));
    }

    #[test]
    fn test_build_bulk_filter_plan_classifies_filters_across_read_paths() {
        let metadata: RegionMetadataRef = Arc::new(sst_region_metadata_with_encoding(
            PrimaryKeyEncoding::Sparse,
        ));
        let legacy_read_format = FlatReadFormat::new(
            metadata.clone(),
            ReadColumns::from_deduped_column_ids(
                metadata.column_metadatas.iter().map(|c| c.column_id),
            ),
            None,
            "memtable",
            false,
        )
        .unwrap();
        assert!(!legacy_read_format.batch_has_raw_pk_columns());

        let plan = build_bulk_filter_plan(
            &legacy_read_format,
            Some(&Predicate::new(vec![
                col("tag_0").eq(lit("a")),
                col("field_0").gt(lit(1_u64)),
            ])),
        );
        assert_eq!(
            plan.pk_filters.as_ref().map(|filters| filters.len()),
            Some(1)
        );
        assert_eq!(
            remaining_simple_filter_columns(&plan.remaining_simple_filters),
            vec!["field_0"]
        );

        let metadata: RegionMetadataRef = Arc::new(sst_region_metadata());
        let raw_pk_read_format = FlatReadFormat::new(
            metadata.clone(),
            ReadColumns::from_deduped_column_ids(
                metadata.column_metadatas.iter().map(|c| c.column_id),
            ),
            None,
            "memtable",
            true,
        )
        .unwrap();
        assert!(raw_pk_read_format.batch_has_raw_pk_columns());

        let tag_only_plan = build_bulk_filter_plan(
            &raw_pk_read_format,
            Some(&Predicate::new(vec![col("tag_0").eq(lit("a"))])),
        );
        assert!(tag_only_plan.pk_filters.is_none());
        assert_eq!(
            remaining_simple_filter_columns(&tag_only_plan.remaining_simple_filters),
            vec!["tag_0"]
        );

        let field_only_plan = build_bulk_filter_plan(
            &raw_pk_read_format,
            Some(&Predicate::new(vec![col("field_0").gt(lit(1_u64))])),
        );
        assert!(field_only_plan.pk_filters.is_none());
        assert_eq!(
            remaining_simple_filter_columns(&field_only_plan.remaining_simple_filters),
            vec!["field_0"]
        );
    }

    #[test]
    fn test_build_reader_filter_plan_classifies_filters_for_prefilter_modes() {
        let metadata: RegionMetadataRef = Arc::new(sst_region_metadata());
        let full_read_format = FlatReadFormat::new(
            metadata.clone(),
            ReadColumns::from_deduped_column_ids(
                metadata.column_metadatas.iter().map(|c| c.column_id),
            ),
            None,
            "test",
            true,
        )
        .unwrap();
        let codec = build_primary_key_codec(metadata.as_ref());

        let skip_fields_plan = build_reader_filter_plan(
            Some(&Predicate::new(vec![
                col("tag_0").eq(lit("a")),
                col("field_0").gt(lit(1_u64)),
            ])),
            None,
            PreFilterMode::SkipFields,
            &full_read_format,
            &codec,
        );
        assert!(skip_fields_plan.prefilter_builder.is_some());
        assert_eq!(
            remaining_simple_filter_columns(&skip_fields_plan.remaining_simple_filters),
            vec!["field_0"]
        );

        let field_0 = metadata.column_by_name("field_0").unwrap().column_id;
        let ts = metadata.time_index_column().column_id;
        let projected_read_format = FlatReadFormat::new(
            metadata.clone(),
            ReadColumns::from_deduped_column_ids([field_0, ts]),
            None,
            "test",
            true,
        )
        .unwrap();
        let pk_prefilter_plan = build_reader_filter_plan(
            Some(&Predicate::new(vec![col("tag_0").eq(lit("a"))])),
            None,
            PreFilterMode::All,
            &projected_read_format,
            &codec,
        );
        assert!(pk_prefilter_plan.prefilter_builder.is_some());
        assert!(pk_prefilter_plan.remaining_simple_filters.is_empty());
    }

    #[test]
    fn test_pk_filter_expr_strings_are_stable_under_expr_order() {
        let metadata: RegionMetadataRef = Arc::new(sst_region_metadata_with_encoding(
            PrimaryKeyEncoding::Sparse,
        ));
        let read_format = FlatReadFormat::new(
            metadata.clone(),
            ReadColumns::from_deduped_column_ids(
                metadata.column_metadatas.iter().map(|c| c.column_id),
            ),
            None,
            "test",
            false,
        )
        .unwrap();
        let codec = build_primary_key_codec(metadata.as_ref());

        let expr_a = col("tag_0").eq(lit("a"));
        let expr_b = col("tag_1").eq(lit("x"));
        let plan_ab = build_reader_filter_plan(
            Some(&Predicate::new(vec![expr_a.clone(), expr_b.clone()])),
            None,
            PreFilterMode::All,
            &read_format,
            &codec,
        );
        let plan_b_a = build_reader_filter_plan(
            Some(&Predicate::new(vec![expr_b, expr_a])),
            None,
            PreFilterMode::All,
            &read_format,
            &codec,
        );

        let exprs_ab = plan_ab.prefilter_builder.unwrap().pk_filter_expr_strs;
        let exprs_b_a = plan_b_a.prefilter_builder.unwrap().pk_filter_expr_strs;
        assert!(exprs_ab.is_some());
        assert_eq!(exprs_ab, exprs_b_a);
    }

    #[test]
    fn test_simple_and_physical_contexts_preserve_expr_strings() {
        let metadata: RegionMetadataRef = Arc::new(sst_region_metadata());
        let read_format = FlatReadFormat::new(
            metadata.clone(),
            ReadColumns::from_deduped_column_ids(
                metadata.column_metadatas.iter().map(|c| c.column_id),
            ),
            None,
            "test",
            true,
        )
        .unwrap();

        let simple_expr = col("tag_0").eq(lit("a"));
        let simple = SimpleFilterContext::new_opt(&metadata, None, &simple_expr).unwrap();
        assert_eq!(simple.expr_str(), format!("{simple_expr:?}"));

        let physical_expr = col("field_0").in_list(vec![lit(1_u64), lit(2_u64)], false);
        let physical =
            PhysicalFilterContext::new_opt(&metadata, None, &read_format, &physical_expr).unwrap();
        assert_eq!(physical.expr_str(), format!("{physical_expr:?}"));
    }

    #[test]
    fn test_eval_simple_filter_mask_uses_flat_tag_columns_directly() {
        let metadata: RegionMetadataRef = Arc::new(sst_region_metadata());
        let filters = new_simple_filter_contexts(&metadata, &[col("tag_0").eq(lit("a"))]);
        let batch = new_record_batch_with_custom_sequence(&["a", "x"], 0, 4, 1);

        let mask = eval_simple_filter_mask(&batch, &filters[0], "test").unwrap();
        assert_eq!(mask.count_set_bits(), 4);
    }

    #[test]
    fn test_eval_simple_filter_mask_errors_on_missing_selected_column() {
        let metadata: RegionMetadataRef = Arc::new(sst_region_metadata());
        let filters = new_simple_filter_contexts(&metadata, &[col("tag_0").eq(lit("a"))]);
        let pk = new_primary_key(&["a", "x"]);
        let batch = new_raw_batch(&[pk.as_slice()], &[10]);

        let err = eval_simple_filter_mask(&batch, &filters[0], "test").unwrap_err();
        let err = err.to_string();
        assert!(err.contains("Prefilter column"));
        assert!(err.contains("tag_0"));
    }

    #[test]
    fn test_eval_physical_filter_mask_evaluates_physical_filters() {
        let metadata: RegionMetadataRef =
            Arc::new(sst_region_metadata_with_encoding(PrimaryKeyEncoding::Dense));
        let read_format = FlatReadFormat::new(
            metadata.clone(),
            ReadColumns::from_deduped_column_ids(
                metadata.column_metadatas.iter().map(|c| c.column_id),
            ),
            None,
            "test",
            false,
        )
        .unwrap();
        let expr = col("field_0").in_list(vec![lit(11_u64)], false);
        let physical_filters = new_physical_filter_contexts(&metadata, &read_format, &[expr]);
        let pk = new_primary_key(&["a", "x"]);
        let batch = new_raw_batch(&[pk.as_slice(), pk.as_slice(), pk.as_slice()], &[9, 10, 11]);

        let mask = eval_physical_filter_mask(&batch, &physical_filters[0], "test").unwrap();
        assert_eq!(mask.count_set_bits(), 1);
    }

    #[test]
    fn test_eval_pk_group_mask_finds_pk_column_by_name() {
        let metadata = Arc::new(sst_region_metadata());
        let filters = Arc::new(new_test_filters(&[col("tag_0").eq(lit("a"))]));
        let mut pk_filter = Some(Box::new(CachedPrimaryKeyFilter::new(
            build_primary_key_codec(metadata.as_ref()).primary_key_filter(&metadata, filters),
        )) as Box<dyn PrimaryKeyFilter>);
        let pk_a = new_primary_key(&["a", "x"]);
        let pk_b = new_primary_key(&["b", "x"]);
        let batch = new_prefilter_batch(
            &[
                pk_a.as_slice(),
                pk_a.as_slice(),
                pk_b.as_slice(),
                pk_b.as_slice(),
            ],
            &[10, 11, 12, 13],
        );

        let mask = eval_pk_group_mask(&batch, pk_filter.as_mut().unwrap().as_mut()).unwrap();

        assert_eq!(mask.count_set_bits(), 2);
    }

    #[test]
    fn test_eval_pk_group_mask_handles_binary_pk_column() {
        let metadata = Arc::new(sst_region_metadata());
        let filters = Arc::new(new_test_filters(&[col("tag_0").eq(lit("a"))]));
        let mut pk_filter = Some(Box::new(CachedPrimaryKeyFilter::new(
            build_primary_key_codec(metadata.as_ref()).primary_key_filter(&metadata, filters),
        )) as Box<dyn PrimaryKeyFilter>);
        let pk_a = new_primary_key(&["a", "x"]);
        let pk_b = new_primary_key(&["b", "x"]);
        let batch = new_prefilter_batch_binary_pk(
            &[
                pk_a.as_slice(),
                pk_a.as_slice(),
                pk_b.as_slice(),
                pk_b.as_slice(),
            ],
            &[10, 11, 12, 13],
        );

        let mask = eval_pk_group_mask(&batch, pk_filter.as_mut().unwrap().as_mut()).unwrap();

        assert_eq!(mask.count_set_bits(), 2);
    }
}
