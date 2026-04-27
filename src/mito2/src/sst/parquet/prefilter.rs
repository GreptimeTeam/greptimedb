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
use datatypes::arrow::record_batch::RecordBatch;
use futures::StreamExt;
use mito_codec::row_converter::{PrimaryKeyCodec, PrimaryKeyFilter};
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::RowSelection;
use parquet::schema::types::SchemaDescriptor;
use snafu::{OptionExt, ResultExt};
use store_api::metadata::{RegionMetadata, RegionMetadataRef};
use store_api::storage::consts::PRIMARY_KEY_COLUMN_NAME;
use table::predicate::Predicate;

use crate::error::{
    ComputeArrowSnafu, DecodeSnafu, EvalPartitionFilterSnafu, NewRecordBatchSnafu,
    ReadParquetSnafu, RecordBatchSnafu, Result, UnexpectedSnafu,
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
    let pk_dict_array = input
        .column(pk_column_index)
        .as_any()
        .downcast_ref::<PrimaryKeyArray>()
        .context(UnexpectedSnafu {
            reason: "Primary key column is not a dictionary array",
        })?;
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

        if pk_filter
            .matches(pk_values.value(key as usize))
            .context(DecodeSnafu)?
        {
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
    parquet_schema: &SchemaDescriptor,
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

    let pk_filter_exprs =
        (!primary_key_filters.is_empty()).then_some(Arc::new(primary_key_filters));
    let prefilter_builder = PrefilterContextBuilder::new(
        read_format,
        codec,
        pk_filter_exprs,
        prefilter_simple_filters.clone(),
        prefilter_physical_filters,
        parquet_schema,
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
    /// Projection mask for reading prefilter columns.
    projection: ProjectionMask,
    /// Optional PK filter for legacy primary-key-format parquet.
    pk_filter: Option<Box<dyn PrimaryKeyFilter>>,
    /// Simple filters that can be evaluated directly from the prefilter batch.
    filters: Vec<SimpleFilterContext>,
    /// Physical filters that can be evaluated directly from the prefilter batch.
    /// Physical expressions are only applied in the prefilter phase.
    physical_filters: Vec<PhysicalFilterContext>,
}

/// Pre-built state for constructing [PrefilterContext] per row group.
///
/// Fields invariant across row groups (projection mask, codec, metadata, filters)
/// are computed once. A fresh [PrefilterContext] with its own mutable PK filter
/// is created via [PrefilterContextBuilder::build()] for each row group.
pub(crate) struct PrefilterContextBuilder {
    projection: ProjectionMask,
    pk_filters: Option<Arc<Vec<SimpleFilterEvaluator>>>,
    filters: Vec<SimpleFilterContext>,
    physical_filters: Vec<PhysicalFilterContext>,
    codec: Arc<dyn PrimaryKeyCodec>,
    metadata: RegionMetadataRef,
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
        filters: Vec<SimpleFilterContext>,
        physical_filters: Vec<PhysicalFilterContext>,
        parquet_schema: &SchemaDescriptor,
    ) -> Option<Self> {
        let metadata = read_format.metadata();
        let use_raw_tag_columns = read_format.batch_has_raw_pk_columns();
        let pk_filters = (!use_raw_tag_columns)
            .then_some(primary_key_filters)
            .flatten()
            .filter(|filters| !filters.is_empty());

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

        let (projection, prefilter_count) = compute_projection_mask(
            &prefilter_column_names,
            read_format.arrow_schema(),
            parquet_schema,
        );

        if prefilter_count == 0 {
            return None;
        }

        let total_count = read_format.projection_indices().len();
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
            projection,
            pk_filters,
            filters,
            physical_filters,
            codec: Arc::clone(codec),
            metadata: metadata.clone(),
        })
    }

    /// Builds a [PrefilterContext] for a specific row group.
    pub(crate) fn build(&self) -> PrefilterContext {
        let pk_filter = self.pk_filters.as_ref().map(|pk_filters| {
            let pk_filter =
                self.codec
                    .primary_key_filter(&self.metadata, Arc::clone(pk_filters), false);
            Box::new(CachedPrimaryKeyFilter::new(pk_filter)) as Box<dyn PrimaryKeyFilter>
        });
        PrefilterContext {
            projection: self.projection.clone(),
            pk_filter,
            filters: self.filters.clone(),
            physical_filters: self.physical_filters.clone(),
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
) -> (ProjectionMask, usize) {
    let mut projection_indices: Vec<usize> = column_names
        .iter()
        .filter_map(|name| arrow_schema.column_with_name(name).map(|(index, _)| index))
        .collect();
    projection_indices.sort_unstable();
    projection_indices.dedup();
    let count = projection_indices.len();
    (
        ProjectionMask::roots(parquet_schema, projection_indices.iter().copied()),
        count,
    )
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
    let mut stream = reader_builder
        .build_with_projection(
            build_ctx.row_group_idx,
            build_ctx.row_selection.clone(),
            prefilter_ctx.projection.clone(),
            build_ctx.fetch_metrics,
        )
        .await?;

    let mut filter_arrays = Vec::new();
    let mut rows_before_filter = 0usize;
    let mut rows_selected = 0usize;

    while let Some(batch_result) = stream.next().await {
        let batch = batch_result.context(ReadParquetSnafu {
            path: reader_builder.file_path(),
        })?;
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            continue;
        }
        rows_before_filter += num_rows;

        let batch_mask = match apply_filters_to_batch(
            &batch,
            &mut prefilter_ctx.pk_filter,
            &prefilter_ctx.filters,
            &prefilter_ctx.physical_filters,
            reader_builder.file_path(),
        )? {
            Some(mask) => mask,
            None => BooleanBuffer::new_unset(num_rows),
        };
        rows_selected += batch_mask.count_set_bits();
        filter_arrays.push(BooleanArray::from(batch_mask));
    }

    let filtered_rows = rows_before_filter.saturating_sub(rows_selected);
    let refined_selection = if filter_arrays.is_empty() || rows_selected == 0 {
        RowSelection::from(vec![])
    } else {
        let prefilter_selection = RowSelection::from_filters(&filter_arrays);
        match &build_ctx.row_selection {
            Some(original) => original.and_then(&prefilter_selection),
            None => prefilter_selection,
        }
    };

    Ok(PrefilterResult {
        refined_selection,
        filtered_rows,
    })
}

fn apply_filters_to_batch(
    batch: &RecordBatch,
    pk_filter: &mut Option<Box<dyn PrimaryKeyFilter>>,
    filters: &[SimpleFilterContext],
    physical_filters: &[PhysicalFilterContext],
    file_path: &str,
) -> Result<Option<BooleanBuffer>> {
    let mut mask = BooleanBuffer::new_set(batch.num_rows());

    if let Some(pk_filter) = pk_filter.as_mut() {
        // Prefilter reads a reduced projection. For PK prefilter, the encoded
        // primary key column is always appended as the last projected column,
        // while `__sequence` and `__op_type` are not read.
        let pk_column_index = batch.num_columns() - 1;
        let matched_row_ranges =
            matching_row_ranges_by_primary_key(batch, pk_column_index, pk_filter.as_mut())?;
        let mut builder = BooleanBufferBuilder::new(batch.num_rows());
        builder.append_n(batch.num_rows(), false);
        for range in matched_row_ranges {
            for row in range {
                builder.set_bit(row, true);
            }
        }
        mask = mask.bitand(&builder.finish());
    }

    for filter_ctx in filters {
        let filter = match filter_ctx.filter() {
            MaybeFilter::Filter(filter) => filter,
            MaybeFilter::Matched => continue,
            MaybeFilter::Pruned => return Ok(None),
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
        let result = filter.evaluate_array(&column).context(RecordBatchSnafu)?;
        mask = mask.bitand(&result);
    }

    for filter_ctx in physical_filters {
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
        let boolean_array =
            array
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
        mask = mask.bitand(&result);
    }

    if mask.count_set_bits() == 0 {
        Ok(None)
    } else {
        Ok(Some(mask))
    }
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
    use datatypes::arrow::datatypes::{Schema, UInt32Type};
    use mito_codec::row_converter::{PrimaryKeyFilter, build_primary_key_codec};
    use parquet::arrow::ArrowSchemaConverter;
    use store_api::codec::PrimaryKeyEncoding;

    use super::*;
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

    fn parquet_schema(read_format: &FlatReadFormat) -> SchemaDescriptor {
        ArrowSchemaConverter::new()
            .convert(read_format.arrow_schema())
            .unwrap()
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
        let mut primary_key_filter = build_primary_key_codec(metadata.as_ref())
            .primary_key_filter(&metadata, filters, false);
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
        let mut primary_key_filter = build_primary_key_codec(metadata.as_ref())
            .primary_key_filter(&metadata, filters, false);
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
            metadata.column_metadatas.iter().map(|c| c.column_id),
            None,
            "test",
            false,
        )
        .unwrap();
        let codec = build_primary_key_codec(metadata.as_ref());
        let parquet_schema = parquet_schema(&read_format);

        let builder = PrefilterContextBuilder::new(
            &read_format,
            &codec,
            None,
            Vec::new(),
            Vec::new(),
            &parquet_schema,
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
            metadata.column_metadatas.iter().map(|c| c.column_id),
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
            metadata.column_metadatas.iter().map(|c| c.column_id),
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
            metadata.column_metadatas.iter().map(|c| c.column_id),
            None,
            "test",
            true,
        )
        .unwrap();
        let full_parquet_schema = parquet_schema(&full_read_format);
        let codec = build_primary_key_codec(metadata.as_ref());

        let skip_fields_plan = build_reader_filter_plan(
            Some(&Predicate::new(vec![
                col("tag_0").eq(lit("a")),
                col("field_0").gt(lit(1_u64)),
            ])),
            None,
            PreFilterMode::SkipFields,
            &full_read_format,
            &full_parquet_schema,
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
            [field_0, ts].into_iter(),
            None,
            "test",
            true,
        )
        .unwrap();
        let projected_parquet_schema = parquet_schema(&projected_read_format);
        let pk_prefilter_plan = build_reader_filter_plan(
            Some(&Predicate::new(vec![col("tag_0").eq(lit("a"))])),
            None,
            PreFilterMode::All,
            &projected_read_format,
            &projected_parquet_schema,
            &codec,
        );
        assert!(pk_prefilter_plan.prefilter_builder.is_some());
        assert!(pk_prefilter_plan.remaining_simple_filters.is_empty());
    }

    #[test]
    fn test_apply_filters_to_batch_uses_flat_tag_columns_directly() {
        let metadata: RegionMetadataRef = Arc::new(sst_region_metadata());
        let filters = new_simple_filter_contexts(&metadata, &[col("tag_0").eq(lit("a"))]);
        let batch = new_record_batch_with_custom_sequence(&["a", "x"], 0, 4, 1);

        let mut no_pk_filter = None;
        let mask = apply_filters_to_batch(&batch, &mut no_pk_filter, &filters, &[], "test")
            .unwrap()
            .unwrap();
        assert_eq!(mask.count_set_bits(), 4);
    }

    #[test]
    fn test_apply_filters_to_batch_errors_on_missing_selected_column() {
        let metadata: RegionMetadataRef = Arc::new(sst_region_metadata());
        let filters = new_simple_filter_contexts(&metadata, &[col("tag_0").eq(lit("a"))]);
        let pk = new_primary_key(&["a", "x"]);
        let batch = new_raw_batch(&[pk.as_slice()], &[10]);

        let mut no_pk_filter = None;
        let err =
            apply_filters_to_batch(&batch, &mut no_pk_filter, &filters, &[], "test").unwrap_err();
        let err = err.to_string();
        assert!(err.contains("Prefilter column"));
        assert!(err.contains("tag_0"));
    }

    #[test]
    fn test_apply_filters_to_batch_evaluates_physical_filters() {
        let metadata: RegionMetadataRef =
            Arc::new(sst_region_metadata_with_encoding(PrimaryKeyEncoding::Dense));
        let read_format = FlatReadFormat::new(
            metadata.clone(),
            metadata.column_metadatas.iter().map(|c| c.column_id),
            None,
            "test",
            false,
        )
        .unwrap();
        let expr = col("field_0").in_list(vec![lit(11_u64)], false);
        let physical_filters = new_physical_filter_contexts(&metadata, &read_format, &[expr]);
        let pk = new_primary_key(&["a", "x"]);
        let batch = new_raw_batch(&[pk.as_slice(), pk.as_slice(), pk.as_slice()], &[9, 10, 11]);

        let mut no_pk_filter = None;
        let mask =
            apply_filters_to_batch(&batch, &mut no_pk_filter, &[], &physical_filters, "test")
                .unwrap()
                .unwrap();
        assert_eq!(mask.count_set_bits(), 1);
    }

    #[test]
    fn test_apply_filters_to_batch_uses_last_projected_column_for_pk_prefilter() {
        let metadata = Arc::new(sst_region_metadata());
        let filters = Arc::new(new_test_filters(&[col("tag_0").eq(lit("a"))]));
        let mut pk_filter = Some(Box::new(CachedPrimaryKeyFilter::new(
            build_primary_key_codec(metadata.as_ref())
                .primary_key_filter(&metadata, filters, false),
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

        let mask = apply_filters_to_batch(&batch, &mut pk_filter, &[], &[], "test")
            .unwrap()
            .unwrap();

        assert_eq!(mask.count_set_bits(), 2);
    }
}
