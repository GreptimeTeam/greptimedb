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

use std::ops::Range;
use std::sync::Arc;

use api::v1::SemanticType;
use common_recordbatch::filter::SimpleFilterEvaluator;
use datatypes::arrow::array::{BinaryArray, BooleanArray};
use datatypes::arrow::record_batch::RecordBatch;
use futures::StreamExt;
use mito_codec::row_converter::{PrimaryKeyCodec, PrimaryKeyFilter};
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::RowSelection;
use parquet::schema::types::SchemaDescriptor;
use snafu::{OptionExt, ResultExt};
use store_api::metadata::{RegionMetadata, RegionMetadataRef};

use crate::error::{ComputeArrowSnafu, DecodeSnafu, ReadParquetSnafu, Result, UnexpectedSnafu};
use crate::sst::parquet::flat_format::primary_key_column_index;
use crate::sst::parquet::format::{PrimaryKeyArray, ReadFormat};
use crate::sst::parquet::reader::{RowGroupBuildContext, RowGroupReaderBuilder};
use crate::sst::parquet::row_selection::row_selection_from_row_ranges_exact;

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

/// Returns whether a filter can be applied by parquet primary-key prefiltering.
///
/// Unlike `PartitionTreeMemtable`, parquet prefilter always supports predicates
/// on the partition column.
pub(crate) fn is_usable_primary_key_filter(
    sst_metadata: &RegionMetadataRef,
    expected_metadata: Option<&RegionMetadata>,
    filter: &SimpleFilterEvaluator,
) -> bool {
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

/// Context for prefiltering a row group.
///
/// Currently supports primary key (PK) filtering only.
/// Will be extended with simple column filters and physical filters in the future.
pub(crate) struct PrefilterContext {
    /// PK filter instance.
    pk_filter: Box<dyn PrimaryKeyFilter>,
    /// Projection mask for reading only the PK column.
    pk_projection: ProjectionMask,
    /// Index of the PK column within the prefilter projection batch.
    /// This is 0 when we project only the PK column.
    pk_column_index: usize,
}

/// Pre-built state for constructing [PrefilterContext] per row group.
///
/// Fields invariant across row groups (projection mask, codec, metadata, filters)
/// are computed once. A fresh [PrefilterContext] with its own mutable PK filter
/// is created via [PrefilterContextBuilder::build()] for each row group.
pub(crate) struct PrefilterContextBuilder {
    pk_projection: ProjectionMask,
    pk_column_index: usize,
    codec: Arc<dyn PrimaryKeyCodec>,
    metadata: RegionMetadataRef,
    pk_filters: Arc<Vec<SimpleFilterEvaluator>>,
}

impl PrefilterContextBuilder {
    /// Creates a builder if prefiltering is applicable.
    ///
    /// Returns `None` if:
    /// - No primary key filters are available
    /// - The read format doesn't use flat layout with dictionary-encoded PKs
    /// - The primary key is empty
    pub(crate) fn new(
        read_format: &ReadFormat,
        codec: &Arc<dyn PrimaryKeyCodec>,
        primary_key_filters: Option<&Arc<Vec<SimpleFilterEvaluator>>>,
        parquet_schema: &SchemaDescriptor,
    ) -> Option<Self> {
        let pk_filters = primary_key_filters?;
        if pk_filters.is_empty() {
            return None;
        }

        let metadata = read_format.metadata();
        if metadata.primary_key.is_empty() {
            return None;
        }

        // Only perform PK prefiltering for primary-key-to-flat conversion path.
        let flat_format = read_format.as_flat()?;
        if flat_format.batch_has_raw_pk_columns() {
            return None;
        }

        // Compute PK-only projection mask.
        let num_parquet_columns = parquet_schema.num_columns();
        let pk_index = primary_key_column_index(num_parquet_columns);
        let pk_projection = ProjectionMask::roots(parquet_schema, [pk_index]);

        // The PK column is the only column in the projection, so its index is 0.
        let pk_column_index = 0;

        Some(Self {
            pk_projection,
            pk_column_index,
            codec: Arc::clone(codec),
            metadata: metadata.clone(),
            pk_filters: Arc::clone(pk_filters),
        })
    }

    /// Builds a [PrefilterContext] for a specific row group.
    pub(crate) fn build(&self) -> PrefilterContext {
        // Parquet PK prefilter always supports the partition column. Only
        // PartitionTreeMemtable skips it after partition pruning.
        let pk_filter =
            self.codec
                .primary_key_filter(&self.metadata, Arc::clone(&self.pk_filters), false);
        let pk_filter = Box::new(CachedPrimaryKeyFilter::new(pk_filter));
        PrefilterContext {
            pk_filter,
            pk_projection: self.pk_projection.clone(),
            pk_column_index: self.pk_column_index,
        }
    }
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
pub(crate) async fn execute_prefilter(
    prefilter_ctx: &mut PrefilterContext,
    reader_builder: &RowGroupReaderBuilder,
    build_ctx: &RowGroupBuildContext<'_>,
) -> Result<PrefilterResult> {
    // Reads PK column only.
    let mut pk_stream = reader_builder
        .build_with_projection(
            build_ctx.row_group_idx,
            build_ctx.row_selection.clone(),
            prefilter_ctx.pk_projection.clone(),
            build_ctx.fetch_metrics,
        )
        .await?;

    // Applies PK filter to each batch and collect matching row ranges.
    let mut matched_row_ranges: Vec<Range<usize>> = Vec::new();
    let mut row_offset = 0;
    let mut rows_before_filter = 0usize;

    while let Some(batch_result) = pk_stream.next().await {
        let batch = batch_result.context(ReadParquetSnafu {
            path: reader_builder.file_path(),
        })?;
        let batch_num_rows = batch.num_rows();
        if batch_num_rows == 0 {
            continue;
        }
        rows_before_filter += batch_num_rows;

        let ranges = matching_row_ranges_by_primary_key(
            &batch,
            prefilter_ctx.pk_column_index,
            prefilter_ctx.pk_filter.as_mut(),
        )?;
        matched_row_ranges.extend(
            ranges
                .into_iter()
                .map(|range| (range.start + row_offset)..(range.end + row_offset)),
        );
        row_offset += batch_num_rows;
    }

    // Converts matched ranges to RowSelection.
    let rows_selected: usize = matched_row_ranges.iter().map(|r| r.end - r.start).sum();
    let filtered_rows = rows_before_filter.saturating_sub(rows_selected);

    let refined_selection = if rows_selected == 0 {
        RowSelection::from(vec![])
    } else {
        // Build the prefilter selection relative to the yielded rows
        // (not total_rows), since matched_row_ranges are offsets within
        // the rows actually read from the stream.
        let prefilter_selection =
            row_selection_from_row_ranges_exact(matched_row_ranges.into_iter(), rows_before_filter);

        // Use and_then to apply prefilter selection within the context
        // of the original selection, since prefilter offsets are relative
        // to the original selection's selected rows.
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
    use store_api::codec::PrimaryKeyEncoding;

    use super::*;
    use crate::sst::internal_fields;
    use crate::sst::parquet::format::ReadFormat;
    use crate::test_util::sst_util::{
        new_primary_key, sst_region_metadata, sst_region_metadata_with_encoding,
    };

    #[test]
    fn test_is_usable_primary_key_filter_skips_legacy_primary_key_batches() {
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
        assert!(read_format.as_flat().is_some());

        let filter = SimpleFilterEvaluator::try_new(&col("tag_0").eq(lit("b"))).unwrap();
        assert!(is_usable_primary_key_filter(&metadata, None, &filter));
    }

    #[test]
    fn test_is_usable_primary_key_filter_supports_partition_column_by_default() {
        let metadata = Arc::new(sst_region_metadata_with_encoding(
            PrimaryKeyEncoding::Sparse,
        ));
        let filter = SimpleFilterEvaluator::try_new(
            &col(store_api::metric_engine_consts::DATA_SCHEMA_TABLE_ID_COLUMN_NAME).eq(lit(1_u32)),
        )
        .unwrap();

        assert!(is_usable_primary_key_filter(&metadata, None, &filter));
    }

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
}
