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
use datafusion_common::ScalarValue;
use datafusion_expr::Expr;
use datatypes::arrow::array::{Array, BinaryArray, BooleanArray, BooleanBufferBuilder};
use datatypes::arrow::buffer::BooleanBuffer;
use datatypes::arrow::datatypes::SchemaRef;
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::prelude::ConcreteDataType;
use datatypes::value::Value;
use futures::StreamExt;
use mito_codec::row_converter::{PrimaryKeyCodec, PrimaryKeyFilter, build_primary_key_codec};
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{RowSelection, RowSelector};
use parquet::file::metadata::ParquetMetaData;
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
use crate::sst::parquet::format::{PrimaryKeyArray, StatValues};
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

/// Builds an encoded-primary-key filter from the supported tag predicates.
///
/// Predicates on fields, timestamps, or unsupported expression shapes are intentionally
/// omitted. Callers use this as a pruning filter and must preserve the full predicate for
/// authoritative filtering later in the scan.
pub(crate) fn build_primary_key_filter(
    sst_metadata: &RegionMetadataRef,
    expected_metadata: Option<&RegionMetadata>,
    predicate: Option<&Predicate>,
) -> Option<CachedPrimaryKeyFilter> {
    let filters = simple_tag_filters(sst_metadata, expected_metadata, predicate)
        .into_iter()
        .map(|(_, filter)| filter)
        .collect::<Vec<_>>();
    if filters.is_empty() {
        return None;
    }

    let codec = build_primary_key_codec(sst_metadata.as_ref());
    let filter = codec.primary_key_filter(sst_metadata, Arc::new(filters));
    Some(CachedPrimaryKeyFilter::new(filter))
}

/// Extracts simple tag filters that can be applied to encoded primary keys or series indexes.
pub(crate) fn simple_tag_filters(
    sst_metadata: &RegionMetadataRef,
    expected_metadata: Option<&RegionMetadata>,
    predicate: Option<&Predicate>,
) -> Vec<(Expr, SimpleFilterEvaluator)> {
    predicate
        .into_iter()
        .flat_map(|predicate| predicate.exprs())
        .filter_map(|expr| {
            SimpleFilterContext::new_opt(sst_metadata, expected_metadata, expr)
                .map(|filter_ctx| (expr, filter_ctx))
        })
        .filter_map(|(expr, filter_ctx)| {
            (filter_ctx.semantic_type() == SemanticType::Tag)
                .then(|| {
                    filter_ctx
                        .filter()
                        .as_filter()
                        .cloned()
                        .map(|filter| (expr.clone(), filter))
                })
                .flatten()
        })
        .collect()
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
/// With predicate prefiltering enabled, tag and timestamp predicates that lower to
/// [`SimpleFilterEvaluator`] are an exception — the engine enforces them precisely in
/// the prefilter pass. A caller can postpone simple timestamp filters to the normal
/// precise-filter path when the scan time range covers the SST. When predicate
/// prefiltering is disabled, all simple filters remain on the normal path instead.
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_reader_filter_plan(
    predicate: Option<&Predicate>,
    expected_metadata: Option<&RegionMetadata>,
    pre_filter_mode: PreFilterMode,
    enable_predicate_prefilter: bool,
    postpone_time_index_filter: bool,
    read_format: &FlatReadFormat,
    codec: &Arc<dyn PrimaryKeyCodec>,
    parquet_metadata: &ParquetMetaData,
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
            if !enable_predicate_prefilter {
                remaining_simple_filters.push(filter_ctx);
                continue;
            }

            // `Matched` and `Pruned` come from expected-metadata compatibility
            // and must stay in the main filter list so later phases keep that
            // outcome.
            let Some(filter) = filter_ctx.filter().as_filter() else {
                remaining_simple_filters.push(filter_ctx);
                continue;
            };

            if postpone_time_index_filter && filter_ctx.semantic_type() == SemanticType::Timestamp {
                remaining_simple_filters.push(filter_ctx);
                continue;
            }

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

        if !enable_predicate_prefilter {
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

    if !enable_predicate_prefilter {
        return ReaderFilterPlan {
            remaining_simple_filters,
            prefilter_builder: None,
        };
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
        parquet_metadata,
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
    /// Simple filters already proven SQL-true by this row group's statistics.
    proven_simple_filters: Vec<bool>,
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
    /// Per-row-group simple filters already proven SQL-true by column statistics.
    proven_simple_filters: Vec<Vec<bool>>,
}

impl PrefilterContextBuilder {
    /// Creates a builder if prefiltering is applicable.
    ///
    /// Returns `None` if:
    /// - The read format doesn't use flat layout
    /// - No prefilter columns are selected
    /// - Prefilter would read the full projection without any PK filter
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        read_format: &FlatReadFormat,
        codec: &Arc<dyn PrimaryKeyCodec>,
        primary_key_filters: Option<Arc<Vec<SimpleFilterEvaluator>>>,
        primary_key_filter_expr_strs: Option<SmallVec<[String; 1]>>,
        filters: Vec<SimpleFilterContext>,
        physical_filters: Vec<PhysicalFilterContext>,
        schema_version: u64,
        parquet_metadata: &ParquetMetaData,
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

        let proven_simple_filters =
            simple_filter_stats_proofs(read_format, parquet_metadata.row_groups(), &filters);

        Some(Self {
            pk_filters,
            pk_filter_expr_strs,
            filters,
            physical_filters,
            codec: Arc::clone(codec),
            metadata: metadata.clone(),
            schema_version,
            arrow_schema: read_format.arrow_schema().clone(),
            proven_simple_filters,
        })
    }

    /// Builds a [PrefilterContext] for a specific row group.
    pub(crate) fn build(&self, row_group_idx: usize) -> PrefilterContext {
        let pk_filter = self
            .build_primary_key_filter()
            .map(|filter| Box::new(filter) as Box<dyn PrimaryKeyFilter>);
        PrefilterContext {
            pk_filter,
            filters: self.filters.clone(),
            physical_filters: self.physical_filters.clone(),
            schema_version: self.schema_version,
            pk_filter_expr_strs: self.pk_filter_expr_strs.clone(),
            arrow_schema: self.arrow_schema.clone(),
            proven_simple_filters: self
                .proven_simple_filters
                .get(row_group_idx)
                .cloned()
                .unwrap_or_else(|| vec![false; self.filters.len()]),
        }
    }

    /// Builds a fresh encoded-primary-key filter selected by this plan.
    pub(crate) fn build_primary_key_filter(&self) -> Option<CachedPrimaryKeyFilter> {
        self.pk_filters.as_ref().map(|pk_filters| {
            let filter = self
                .codec
                .primary_key_filter(&self.metadata, Arc::clone(pk_filters));
            CachedPrimaryKeyFilter::new(filter)
        })
    }
}

const PREFILTER_COLUMN_RATIO_THRESHOLD: f64 = 0.5;
const PREFILTER_MIN_REMAINING_COLUMNS: usize = 2;

/// Returns row-group-major proof bits for simple filters. Statistics are
/// extracted once per eligible filter across all row groups.
fn simple_filter_stats_proofs(
    read_format: &FlatReadFormat,
    row_groups: &[parquet::file::metadata::RowGroupMetaData],
    filters: &[SimpleFilterContext],
) -> Vec<Vec<bool>> {
    let mut proofs = vec![vec![false; filters.len()]; row_groups.len()];
    for (filter_idx, filter_ctx) in filters.iter().enumerate() {
        let Some((filter, literal)) = eligible_simple_filter(read_format, filter_ctx) else {
            continue;
        };
        let (StatValues::Values(mins), StatValues::Values(maxs), StatValues::Values(null_counts)) = (
            read_format.min_values(row_groups, filter_ctx.column_id()),
            read_format.max_values(row_groups, filter_ctx.column_id()),
            read_format.null_counts(row_groups, filter_ctx.column_id()),
        ) else {
            continue;
        };
        for (row_group_idx, proof) in proofs.iter_mut().enumerate() {
            proof[filter_idx] = simple_filter_is_true_by_values(
                filter,
                &literal,
                stat_value_at(&mins, row_group_idx),
                stat_value_at(&maxs, row_group_idx),
                stat_value_at(&null_counts, row_group_idx),
            );
        }
    }
    proofs
}

fn eligible_simple_filter<'a>(
    read_format: &FlatReadFormat,
    filter_ctx: &'a SimpleFilterContext,
) -> Option<(&'a SimpleFilterEvaluator, Value)> {
    if filter_ctx.semantic_type() != SemanticType::Field {
        return None;
    }
    let filter = filter_ctx.filter().as_filter()?;
    let literal = filter.literal_value()?;
    let column = read_format
        .metadata()
        .column_by_id(filter_ctx.column_id())?;
    column_type_matches_literal(&column.column_schema.data_type, &literal)
        .then_some((filter, literal))
}

fn simple_filter_is_true_by_values(
    filter: &SimpleFilterEvaluator,
    literal: &Value,
    min: Option<Value>,
    max: Option<Value>,
    null_count: Option<Value>,
) -> bool {
    let (Some(min), Some(max), Some(null_count)) = (min, max, null_count) else {
        return false;
    };
    if null_count != Value::UInt64(0)
        || !same_supported_value_type(&min, literal)
        || !same_supported_value_type(&max, literal)
        || min > max
    {
        return false;
    }

    if filter.is_gt() {
        min > *literal
    } else if filter.is_gt_eq() {
        min >= *literal
    } else if filter.is_lt() {
        max < *literal
    } else if filter.is_lt_eq() {
        max <= *literal
    } else if filter.is_eq() {
        min == *literal && max == *literal
    } else if filter.is_not_eq() {
        max < *literal || min > *literal
    } else {
        false
    }
}

fn column_type_matches_literal(data_type: &ConcreteDataType, literal: &Value) -> bool {
    matches!(
        (data_type, literal),
        (ConcreteDataType::Int32(_), Value::Int32(_))
            | (ConcreteDataType::UInt32(_), Value::UInt32(_))
            | (ConcreteDataType::Int64(_), Value::Int64(_))
            | (ConcreteDataType::UInt64(_), Value::UInt64(_))
    )
}

fn same_supported_value_type(left: &Value, right: &Value) -> bool {
    matches!(
        (left, right),
        (Value::Int32(_), Value::Int32(_))
            | (Value::UInt32(_), Value::UInt32(_))
            | (Value::Int64(_), Value::Int64(_))
            | (Value::UInt64(_), Value::UInt64(_))
    )
}

fn stat_value_at(values: &datatypes::arrow::array::ArrayRef, index: usize) -> Option<Value> {
    let scalar = ScalarValue::try_from_array(values, index).ok()?;
    Value::try_from(scalar).ok()
}

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
    if entries.is_empty() {
        return Ok(identity_prefilter_result(reader_builder, build_ctx));
    }
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
            .filter(|(idx, _)| {
                !prefilter_ctx
                    .proven_simple_filters
                    .get(*idx)
                    .copied()
                    .unwrap_or(false)
            })
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
        if prefilter_ctx
            .proven_simple_filters
            .get(idx)
            .copied()
            .unwrap_or(false)
        {
            continue;
        }
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

fn identity_prefilter_result(
    reader_builder: &RowGroupReaderBuilder,
    build_ctx: &RowGroupBuildContext<'_>,
) -> PrefilterResult {
    let row_count = reader_builder
        .parquet_metadata()
        .row_group(build_ctx.row_group_idx)
        .num_rows() as usize;
    PrefilterResult {
        refined_selection: identity_row_selection(&build_ctx.row_selection, row_count),
        filtered_rows: 0,
    }
}

fn identity_row_selection(
    original_selection: &Option<RowSelection>,
    row_count: usize,
) -> RowSelection {
    original_selection
        .clone()
        .unwrap_or_else(|| RowSelection::from(vec![RowSelector::select(row_count)]))
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
            primary_key_filter_mask(batch, pk_filter.as_mut())
        }
    }
}

/// Evaluates an encoded-primary-key filter against a primary-key batch.
pub(crate) fn primary_key_filter_mask(
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

    use bytes::Bytes;
    use common_recordbatch::filter::SimpleFilterEvaluator;
    use datafusion_common::ScalarValue;
    use datafusion_expr::{col, lit};
    use datatypes::arrow::array::{
        ArrayRef, DictionaryArray, Int32Array, TimestampMillisecondArray, UInt8Array, UInt32Array,
        UInt64Array,
    };
    use datatypes::arrow::datatypes::{DataType, Field, Schema, UInt32Type};
    use datatypes::arrow::record_batch::RecordBatch;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::value::Value;
    use mito_codec::row_converter::{PrimaryKeyFilter, build_primary_key_codec};
    use parquet::arrow::ArrowWriter;
    use parquet::arrow::arrow_reader::{ParquetRecordBatchReaderBuilder, RowSelector};
    use store_api::codec::PrimaryKeyEncoding;
    use store_api::metadata::RegionMetadataBuilder;
    use store_api::region_request::{AlterKind, ModifyColumnType};

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

    fn stats_metadata_with_options(
        values: &[Vec<u64>],
        nulls: Option<&[bool]>,
        writer_options: Option<parquet::file::properties::WriterProperties>,
    ) -> Arc<ParquetMetaData> {
        let first = new_record_batch_with_custom_sequence(&["a", "x"], 0, values[0].len(), 1);
        let mut bytes = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut bytes, first.schema(), writer_options).unwrap();
        for (idx, values) in values.iter().enumerate() {
            let has_null = nulls.and_then(|nulls| nulls.get(idx)).copied() == Some(true);
            let batch = new_record_batch_with_custom_sequence(
                &["a", "x"],
                0,
                if has_null {
                    values.len().max(2)
                } else {
                    values.len()
                },
                1,
            );
            let mut columns = batch.columns().to_vec();
            columns[2] = Arc::new(if has_null {
                UInt64Array::from(vec![Some(values[0]), None])
            } else {
                UInt64Array::from_iter_values(values.iter().copied())
            });
            writer
                .write(&RecordBatch::try_new(batch.schema(), columns).unwrap())
                .unwrap();
            writer.flush().unwrap();
        }
        writer.close().unwrap();
        ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes))
            .unwrap()
            .metadata()
            .clone()
    }

    fn stats_metadata(values: &[Vec<u64>], nulls: Option<&[bool]>) -> Arc<ParquetMetaData> {
        stats_metadata_with_options(values, nulls, None)
    }

    fn int32_stats_metadata(values: &[i32]) -> Arc<ParquetMetaData> {
        let batch = new_record_batch_with_custom_sequence(&["a", "x"], 0, values.len(), 1);
        let mut fields = batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .collect::<Vec<_>>();
        fields[2].set_data_type(DataType::Int32);
        let mut columns = batch.columns().to_vec();
        columns[2] = Arc::new(Int32Array::from(values.to_vec()));
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
        let mut bytes = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut bytes, batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes))
            .unwrap()
            .metadata()
            .clone()
    }

    fn metadata_with_field_type(data_type: ConcreteDataType) -> RegionMetadataRef {
        let mut builder = RegionMetadataBuilder::from_existing(sst_region_metadata());
        builder
            .alter(AlterKind::ModifyColumnTypes {
                columns: vec![ModifyColumnType {
                    column_name: "field_0".to_string(),
                    target_type: data_type,
                }],
            })
            .unwrap();
        Arc::new(builder.build().unwrap())
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
            ReadColumns::new(metadata.column_metadatas.iter().map(|c| c.column_id)),
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
            &stats_metadata(&[vec![1]], None),
        );
        assert!(builder.is_none());
    }

    #[test]
    fn test_simple_filter_stats_uses_real_metadata() {
        let metadata: RegionMetadataRef = Arc::new(sst_region_metadata());
        let read_format = FlatReadFormat::new(
            metadata.clone(),
            ReadColumns::new(
                metadata
                    .column_metadatas
                    .iter()
                    .map(|column| column.column_id),
            ),
            None,
            "test",
            true,
        )
        .unwrap();
        let parquet_metadata = stats_metadata(&[vec![i64::MAX as u64 + 2]], None);
        let filters = new_simple_filter_contexts(
            &metadata,
            &[
                col("field_0").gt(lit(i64::MAX as u64 + 1)),
                col("field_0").gt(lit(i64::MAX as u64 + 2)),
                lit(i64::MAX as u64 + 1).lt(col("field_0")),
            ],
        );

        let proofs =
            simple_filter_stats_proofs(&read_format, parquet_metadata.row_groups(), &filters);
        assert_eq!(proofs, vec![vec![true, false, true]]);
    }

    #[test]
    fn test_simple_filter_stats_retain_narrow_or_incomplete_metadata() {
        let parquet_metadata = stats_metadata(&[vec![2]], Some(&[true]));
        let metadata: RegionMetadataRef = Arc::new(sst_region_metadata());
        let read_format = FlatReadFormat::new(
            metadata.clone(),
            ReadColumns::new(
                metadata
                    .column_metadatas
                    .iter()
                    .map(|column| column.column_id),
            ),
            None,
            "test",
            true,
        )
        .unwrap();
        let filter = new_simple_filter_contexts(&metadata, &[col("field_0").gt(lit(1_u64))]);
        assert_eq!(
            simple_filter_stats_proofs(&read_format, parquet_metadata.row_groups(), &filter),
            vec![vec![false]],
        );
        let missing_stats = stats_metadata_with_options(
            &[vec![2]],
            None,
            Some(
                parquet::file::properties::WriterProperties::builder()
                    .set_statistics_enabled(parquet::file::properties::EnabledStatistics::None)
                    .build(),
            ),
        );
        assert_eq!(
            simple_filter_stats_proofs(&read_format, missing_stats.row_groups(), &filter),
            vec![vec![false]],
        );

        // Parquet INT32 stats decode as Value::Int32; without the actual SST
        // type gate this Int8 field would be incorrectly proven true.
        let narrow_metadata = metadata_with_field_type(ConcreteDataType::int8_datatype());
        let narrow_read_format = FlatReadFormat::new(
            narrow_metadata.clone(),
            ReadColumns::new(
                narrow_metadata
                    .column_metadatas
                    .iter()
                    .map(|column| column.column_id),
            ),
            None,
            "test",
            true,
        )
        .unwrap();
        let narrow_filter =
            new_simple_filter_contexts(&narrow_metadata, &[col("field_0").gt(lit(1_i32))]);
        let narrow_stats = int32_stats_metadata(&[2]);
        assert_eq!(
            simple_filter_stats_proofs(
                &narrow_read_format,
                narrow_stats.row_groups(),
                &narrow_filter,
            ),
            vec![vec![false]],
        );
    }

    #[test]
    fn test_prefilter_builder_uses_row_group_proofs_by_index() {
        let metadata: RegionMetadataRef = Arc::new(sst_region_metadata());
        let read_format = FlatReadFormat::new(
            metadata.clone(),
            ReadColumns::new(
                metadata
                    .column_metadatas
                    .iter()
                    .map(|column| column.column_id),
            ),
            None,
            "test",
            true,
        )
        .unwrap();
        let codec = build_primary_key_codec(metadata.as_ref());
        let builder = PrefilterContextBuilder::new(
            &read_format,
            &codec,
            None,
            None,
            new_simple_filter_contexts(&metadata, &[col("field_0").gt(lit(1_u64))]),
            Vec::new(),
            metadata.schema_version,
            &stats_metadata(&[vec![2], vec![1]], None),
        )
        .unwrap();

        assert!(builder.build(0).proven_simple_filters[0]);
        assert!(!builder.build(1).proven_simple_filters[0]);
    }

    #[test]
    fn test_simple_filter_stats_prove_integer_predicates() {
        macro_rules! assert_all_operators {
            ($literal:expr, $base:expr, $below:expr, $above:expr) => {{
                for (expr, min, max) in [
                    (col("x").gt(lit($literal)), $above.clone(), $above.clone()),
                    (col("x").gt_eq(lit($literal)), $base.clone(), $above.clone()),
                    (col("x").lt(lit($literal)), $below.clone(), $below.clone()),
                    (col("x").lt_eq(lit($literal)), $below.clone(), $base.clone()),
                    (col("x").eq(lit($literal)), $base.clone(), $base.clone()),
                    (
                        col("x").not_eq(lit($literal)),
                        $below.clone(),
                        $below.clone(),
                    ),
                ] {
                    let filter = SimpleFilterEvaluator::try_new(&expr).unwrap();
                    assert!(simple_filter_is_true_by_values(
                        &filter,
                        &$base,
                        Some(min),
                        Some(max),
                        Some(Value::UInt64(0)),
                    ));
                }
            }};
        }

        assert_all_operators!(5_i32, Value::Int32(5), Value::Int32(-1), Value::Int32(6));
        assert_all_operators!(5_u32, Value::UInt32(5), Value::UInt32(4), Value::UInt32(6));
        assert_all_operators!(5_i64, Value::Int64(5), Value::Int64(-1), Value::Int64(6));
        assert_all_operators!(
            i64::MAX as u64 + 2,
            Value::UInt64(i64::MAX as u64 + 2),
            Value::UInt64(i64::MAX as u64 + 1),
            Value::UInt64(i64::MAX as u64 + 3)
        );

        // Boundary cases must remain unproven: the min/max interval still
        // admits a row that violates the predicate.
        for (expr, min, max) in [
            (col("x").lt(lit(5_i64)), 1, 5),
            (col("x").gt(lit(5_i64)), 5, 9),
            (col("x").lt_eq(lit(5_i64)), 1, 6),
            (col("x").gt_eq(lit(5_i64)), 4, 9),
            (col("x").eq(lit(5_i64)), 5, 6),
            (col("x").not_eq(lit(5_i64)), 5, 9),
            (col("x").not_eq(lit(5_i64)), 1, 5),
            (col("x").not_eq(lit(5_i64)), 1, 9),
        ] {
            let filter = SimpleFilterEvaluator::try_new(&expr).unwrap();
            assert!(
                !simple_filter_is_true_by_values(
                    &filter,
                    &Value::Int64(5),
                    Some(Value::Int64(min)),
                    Some(Value::Int64(max)),
                    Some(Value::UInt64(0)),
                ),
                "{expr:?} must not be proven by stats {min}..={max}",
            );
        }
    }

    #[test]
    fn test_simple_filter_stats_retain_unknown_or_unsupported_values() {
        let filter = SimpleFilterEvaluator::try_new(&col("x").gt(lit(1_i32))).unwrap();
        let literal = Value::Int32(1);
        for (min, max, null_count) in [
            (
                Some(Value::Int32(2)),
                Some(Value::Int32(3)),
                Some(Value::UInt64(1)),
            ),
            (Some(Value::Int32(2)), Some(Value::Int32(3)), None),
            (
                Some(Value::Null),
                Some(Value::Int32(3)),
                Some(Value::UInt64(0)),
            ),
            (None, Some(Value::Int32(3)), Some(Value::UInt64(0))),
            (Some(Value::Int32(2)), None, Some(Value::UInt64(0))),
            (
                Some(Value::Int64(2)),
                Some(Value::Int64(3)),
                Some(Value::UInt64(0)),
            ),
            (
                Some(Value::Int32(3)),
                Some(Value::Int32(2)),
                Some(Value::UInt64(0)),
            ),
        ] {
            assert!(!simple_filter_is_true_by_values(
                &filter, &literal, min, max, null_count
            ));
        }

        let float_filter = SimpleFilterEvaluator::try_new(&col("x").gt(lit(1.0_f64))).unwrap();
        assert!(!simple_filter_is_true_by_values(
            &float_filter,
            &Value::Float64(1.0.into()),
            Some(Value::Float64(2.0.into())),
            Some(Value::Float64(3.0.into())),
            Some(Value::UInt64(0)),
        ));
    }

    #[test]
    fn test_prefilter_entries_keep_only_unproven_simple_filter_indices() {
        let metadata: RegionMetadataRef = Arc::new(sst_region_metadata());
        let filters = new_simple_filter_contexts(
            &metadata,
            &[col("field_0").gt(lit(1_u64)), col("field_0").lt(lit(9_u64))],
        );
        let context = PrefilterContext {
            pk_filter: None,
            filters,
            physical_filters: Vec::new(),
            schema_version: metadata.schema_version,
            pk_filter_expr_strs: None,
            arrow_schema: metadata.schema.arrow_schema().clone(),
            proven_simple_filters: vec![true, false],
        };

        let entries = all_prefilter_entries(&context);
        assert!(matches!(
            entries.as_slice(),
            [PrefilterEntry {
                kind: PrefilterEntryKind::Simple(1),
                ..
            }]
        ));
    }

    #[test]
    fn test_identity_row_selection_preserves_input() {
        let sparse = RowSelection::from(vec![
            RowSelector::skip(2),
            RowSelector::select(3),
            RowSelector::skip(1),
        ]);
        assert_eq!(identity_row_selection(&Some(sparse.clone()), 6), sparse);

        let empty = RowSelection::from(vec![]);
        assert_eq!(identity_row_selection(&Some(empty.clone()), 6), empty);
        assert_eq!(
            identity_row_selection(&None, 6),
            RowSelection::from(vec![RowSelector::select(6)])
        );
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
            ReadColumns::new(metadata.column_metadatas.iter().map(|c| c.column_id)),
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
            ReadColumns::new(metadata.column_metadatas.iter().map(|c| c.column_id)),
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
            ReadColumns::new(metadata.column_metadatas.iter().map(|c| c.column_id)),
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
            true,
            false,
            &full_read_format,
            &codec,
            &stats_metadata(&[vec![1]], None),
        );
        assert!(skip_fields_plan.prefilter_builder.is_some());
        assert_eq!(
            remaining_simple_filter_columns(&skip_fields_plan.remaining_simple_filters),
            vec!["field_0"]
        );

        let postponed_time_plan = build_reader_filter_plan(
            Some(&Predicate::new(vec![
                col("tag_0").eq(lit("a")),
                col("field_0").gt(lit(1_u64)),
                col("ts").gt_eq(lit(ScalarValue::TimestampMillisecond(Some(1), None))),
            ])),
            None,
            PreFilterMode::SkipFields,
            true,
            true,
            &full_read_format,
            &codec,
            &stats_metadata(&[vec![1]], None),
        );
        assert!(postponed_time_plan.prefilter_builder.is_some());
        assert_eq!(
            remaining_simple_filter_columns(&postponed_time_plan.remaining_simple_filters),
            vec!["field_0", "ts"]
        );

        let postponed_time_only_plan = build_reader_filter_plan(
            Some(&Predicate::new(vec![col("ts").gt_eq(lit(
                ScalarValue::TimestampMillisecond(Some(1), None),
            ))])),
            None,
            PreFilterMode::All,
            true,
            true,
            &full_read_format,
            &codec,
            &stats_metadata(&[vec![1]], None),
        );
        assert!(postponed_time_only_plan.prefilter_builder.is_none());
        assert_eq!(
            remaining_simple_filter_columns(&postponed_time_only_plan.remaining_simple_filters),
            vec!["ts"]
        );

        let metric_metadata: RegionMetadataRef = Arc::new(sst_region_metadata_with_encoding(
            PrimaryKeyEncoding::Sparse,
        ));
        let field_0 = metric_metadata.column_by_name("field_0").unwrap().column_id;
        let ts = metric_metadata.time_index_column().column_id;
        let projected_read_format = FlatReadFormat::new(
            metric_metadata.clone(),
            ReadColumns::new([field_0, ts]),
            None,
            "test",
            true,
        )
        .unwrap();
        let metric_codec = build_primary_key_codec(metric_metadata.as_ref());
        let pk_prefilter_plan = build_reader_filter_plan(
            Some(&Predicate::new(vec![col("tag_0").eq(lit("a"))])),
            None,
            PreFilterMode::All,
            true,
            false,
            &projected_read_format,
            &metric_codec,
            &stats_metadata(&[vec![1]], None),
        );
        assert!(pk_prefilter_plan.prefilter_builder.is_some());
        assert!(
            pk_prefilter_plan
                .prefilter_builder
                .as_ref()
                .unwrap()
                .build_primary_key_filter()
                .is_some()
        );
        assert!(pk_prefilter_plan.remaining_simple_filters.is_empty());

        let disabled_plan = build_reader_filter_plan(
            Some(&Predicate::new(vec![
                col("tag_0").eq(lit("a")),
                col("field_0").gt(lit(1_u64)),
                col("ts").gt_eq(lit(ScalarValue::TimestampMillisecond(Some(1), None))),
            ])),
            None,
            PreFilterMode::All,
            false,
            true,
            &projected_read_format,
            &metric_codec,
            &stats_metadata(&[vec![1]], None),
        );
        assert!(disabled_plan.prefilter_builder.is_none());
        assert_eq!(
            remaining_simple_filter_columns(&disabled_plan.remaining_simple_filters),
            vec!["tag_0", "field_0", "ts"]
        );
    }

    #[test]
    fn test_pk_filter_expr_strings_are_stable_under_expr_order() {
        let metadata: RegionMetadataRef = Arc::new(sst_region_metadata_with_encoding(
            PrimaryKeyEncoding::Sparse,
        ));
        let read_format = FlatReadFormat::new(
            metadata.clone(),
            ReadColumns::new(metadata.column_metadatas.iter().map(|c| c.column_id)),
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
            true,
            false,
            &read_format,
            &codec,
            &stats_metadata(&[vec![1]], None),
        );
        let plan_b_a = build_reader_filter_plan(
            Some(&Predicate::new(vec![expr_b, expr_a])),
            None,
            PreFilterMode::All,
            true,
            false,
            &read_format,
            &codec,
            &stats_metadata(&[vec![1]], None),
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
            ReadColumns::new(metadata.column_metadatas.iter().map(|c| c.column_id)),
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
            ReadColumns::new(metadata.column_metadatas.iter().map(|c| c.column_id)),
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

        let mask = primary_key_filter_mask(&batch, pk_filter.as_mut().unwrap().as_mut()).unwrap();

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

        let mask = primary_key_filter_mask(&batch, pk_filter.as_mut().unwrap().as_mut()).unwrap();

        assert_eq!(mask.count_set_bits(), 2);
    }
}
