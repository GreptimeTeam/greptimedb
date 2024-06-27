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

//! Parquet reader.

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use api::v1::SemanticType;
use async_trait::async_trait;
use common_recordbatch::filter::SimpleFilterEvaluator;
use common_telemetry::{debug, warn};
use common_time::range::TimestampRange;
use common_time::timestamp::TimeUnit;
use common_time::Timestamp;
use datafusion_common::ScalarValue;
use datafusion_expr::{Expr, Operator};
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::data_type::ConcreteDataType;
use itertools::Itertools;
use object_store::ObjectStore;
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, RowSelection};
use parquet::arrow::{parquet_to_arrow_field_levels, FieldLevels, ProjectionMask};
use parquet::file::metadata::ParquetMetaData;
use parquet::format::KeyValue;
use snafu::{OptionExt, ResultExt};
use store_api::metadata::{RegionMetadata, RegionMetadataRef};
use store_api::storage::ColumnId;
use table::predicate::Predicate;

use crate::cache::CacheManagerRef;
use crate::error;
use crate::error::{
    ArrowReaderSnafu, InvalidMetadataSnafu, InvalidParquetSnafu, ReadParquetSnafu, Result,
};
use crate::metrics::{
    PRECISE_FILTER_ROWS_TOTAL, READ_ROWS_IN_ROW_GROUP_TOTAL, READ_ROWS_TOTAL,
    READ_ROW_GROUPS_TOTAL, READ_STAGE_ELAPSED,
};
use crate::read::{Batch, BatchReader};
use crate::row_converter::{McmpRowCodec, SortField};
use crate::sst::file::FileHandle;
use crate::sst::index::applier::SstIndexApplierRef;
use crate::sst::parquet::file_range::{FileRangeContext, FileRangeContextRef};
use crate::sst::parquet::format::ReadFormat;
use crate::sst::parquet::metadata::MetadataLoader;
use crate::sst::parquet::row_group::InMemoryRowGroup;
use crate::sst::parquet::row_selection::row_selection_from_row_ranges;
use crate::sst::parquet::stats::RowGroupPruningStats;
use crate::sst::parquet::{DEFAULT_READ_BATCH_SIZE, PARQUET_METADATA_KEY};

/// Parquet SST reader builder.
pub struct ParquetReaderBuilder {
    /// SST directory.
    file_dir: String,
    file_handle: FileHandle,
    object_store: ObjectStore,
    /// Predicate to push down.
    predicate: Option<Predicate>,
    /// Time range to filter.
    time_range: Option<TimestampRange>,
    /// Metadata of columns to read.
    ///
    /// `None` reads all columns. Due to schema change, the projection
    /// can contain columns not in the parquet file.
    projection: Option<Vec<ColumnId>>,
    /// Manager that caches SST data.
    cache_manager: Option<CacheManagerRef>,
    /// Index applier.
    index_applier: Option<SstIndexApplierRef>,
    /// Expected metadata of the region while reading the SST.
    /// This is usually the latest metadata of the region. The reader use
    /// it get the correct column id of a column by name.
    expected_metadata: Option<RegionMetadataRef>,
}

impl ParquetReaderBuilder {
    /// Returns a new [ParquetReaderBuilder] to read specific SST.
    pub fn new(
        file_dir: String,
        file_handle: FileHandle,
        object_store: ObjectStore,
    ) -> ParquetReaderBuilder {
        ParquetReaderBuilder {
            file_dir,
            file_handle,
            object_store,
            predicate: None,
            time_range: None,
            projection: None,
            cache_manager: None,
            index_applier: None,
            expected_metadata: None,
        }
    }

    /// Attaches the predicate to the builder.
    #[must_use]
    pub fn predicate(mut self, predicate: Option<Predicate>) -> ParquetReaderBuilder {
        self.predicate = predicate;
        self
    }

    /// Attaches the time range to the builder.
    #[must_use]
    pub fn time_range(mut self, time_range: Option<TimestampRange>) -> ParquetReaderBuilder {
        self.time_range = time_range;
        self
    }

    /// Attaches the projection to the builder.
    ///
    /// The reader only applies the projection to fields.
    #[must_use]
    pub fn projection(mut self, projection: Option<Vec<ColumnId>>) -> ParquetReaderBuilder {
        self.projection = projection;
        self
    }

    /// Attaches the cache to the builder.
    #[must_use]
    pub fn cache(mut self, cache: Option<CacheManagerRef>) -> ParquetReaderBuilder {
        self.cache_manager = cache;
        self
    }

    /// Attaches the index applier to the builder.
    #[must_use]
    pub(crate) fn index_applier(mut self, index_applier: Option<SstIndexApplierRef>) -> Self {
        self.index_applier = index_applier;
        self
    }

    /// Attaches the expected metadata to the builder.
    #[must_use]
    pub fn expected_metadata(mut self, expected_metadata: Option<RegionMetadataRef>) -> Self {
        self.expected_metadata = expected_metadata;
        self
    }

    /// Builds a [ParquetReader].
    ///
    /// This needs to perform IO operation.
    pub async fn build(&self) -> Result<ParquetReader> {
        let (context, row_groups) = self.build_reader_input().await?;
        ParquetReader::new(Arc::new(context), row_groups).await
    }

    /// Builds a [FileRangeContext] and collects row groups to read.
    ///
    /// This needs to perform IO operation.
    pub(crate) async fn build_reader_input(&self) -> Result<(FileRangeContext, RowGroupMap)> {
        let start = Instant::now();

        let file_path = self.file_handle.file_path(&self.file_dir);
        let file_size = self.file_handle.meta_ref().file_size;
        // Loads parquet metadata of the file.
        let parquet_meta = self.read_parquet_metadata(&file_path, file_size).await?;
        // Decodes region metadata.
        let key_value_meta = parquet_meta.file_metadata().key_value_metadata();
        // Gets the metadata stored in the SST.
        let region_meta = Arc::new(Self::get_region_metadata(&file_path, key_value_meta)?);
        let read_format = if let Some(column_ids) = &self.projection {
            ReadFormat::new(region_meta.clone(), column_ids.iter().copied())
        } else {
            // Lists all column ids to read, we always use the expected metadata if possible.
            let expected_meta = self.expected_metadata.as_ref().unwrap_or(&region_meta);
            ReadFormat::new(
                region_meta.clone(),
                expected_meta
                    .column_metadatas
                    .iter()
                    .map(|col| col.column_id),
            )
        };

        // Computes the projection mask.
        let parquet_schema_desc = parquet_meta.file_metadata().schema_descr();
        let indices = read_format.projection_indices();
        // Now we assumes we don't have nested schemas.
        // TODO(yingwen): Revisit this if we introduce nested types such as JSON type.
        let projection_mask = ProjectionMask::roots(parquet_schema_desc, indices.iter().copied());

        // Computes the field levels.
        let hint = Some(read_format.arrow_schema().fields());
        let field_levels =
            parquet_to_arrow_field_levels(parquet_schema_desc, projection_mask.clone(), hint)
                .context(ReadParquetSnafu { path: &file_path })?;

        let mut metrics = ReaderMetrics::default();

        let row_groups = self
            .row_groups_to_read(&read_format, &parquet_meta, &mut metrics)
            .await;

        let reader_builder = RowGroupReaderBuilder {
            file_handle: self.file_handle.clone(),
            file_path,
            parquet_meta,
            object_store: self.object_store.clone(),
            projection: projection_mask,
            field_levels,
            cache_manager: self.cache_manager.clone(),
        };

        metrics.build_cost = start.elapsed();

        let mut filters = if let Some(predicate) = &self.predicate {
            predicate
                .exprs()
                .iter()
                .filter_map(|expr| {
                    SimpleFilterContext::new_opt(
                        &region_meta,
                        self.expected_metadata.as_deref(),
                        expr,
                    )
                })
                .collect::<Vec<_>>()
        } else {
            vec![]
        };

        if let Some(time_range) = &self.time_range {
            filters.extend(time_range_to_predicate(*time_range, &region_meta)?);
        }

        let codec = McmpRowCodec::new(
            read_format
                .metadata()
                .primary_key_columns()
                .map(|c| SortField::new(c.column_schema.data_type.clone()))
                .collect(),
        );

        let context = FileRangeContext::new(reader_builder, filters, read_format, codec);
        Ok((context, row_groups))
    }

    /// Decodes region metadata from key value.
    fn get_region_metadata(
        file_path: &str,
        key_value_meta: Option<&Vec<KeyValue>>,
    ) -> Result<RegionMetadata> {
        let key_values = key_value_meta.context(InvalidParquetSnafu {
            file: file_path,
            reason: "missing key value meta",
        })?;
        let meta_value = key_values
            .iter()
            .find(|kv| kv.key == PARQUET_METADATA_KEY)
            .with_context(|| InvalidParquetSnafu {
                file: file_path,
                reason: format!("key {} not found", PARQUET_METADATA_KEY),
            })?;
        let json = meta_value
            .value
            .as_ref()
            .with_context(|| InvalidParquetSnafu {
                file: file_path,
                reason: format!("No value for key {}", PARQUET_METADATA_KEY),
            })?;

        RegionMetadata::from_json(json).context(InvalidMetadataSnafu)
    }

    /// Reads parquet metadata of specific file.
    async fn read_parquet_metadata(
        &self,
        file_path: &str,
        file_size: u64,
    ) -> Result<Arc<ParquetMetaData>> {
        let region_id = self.file_handle.region_id();
        let file_id = self.file_handle.file_id();
        // Tries to get from global cache.
        if let Some(manager) = &self.cache_manager {
            if let Some(metadata) = manager.get_parquet_meta_data(region_id, file_id).await {
                return Ok(metadata);
            }
        }

        // Cache miss, load metadata directly.
        let metadata_loader = MetadataLoader::new(self.object_store.clone(), file_path, file_size);
        let metadata = metadata_loader.load().await?;
        let metadata = Arc::new(metadata);
        // Cache the metadata.
        if let Some(cache) = &self.cache_manager {
            cache.put_parquet_meta_data(
                self.file_handle.region_id(),
                self.file_handle.file_id(),
                metadata.clone(),
            );
        }

        Ok(metadata)
    }

    /// Computes row groups to read, along with their respective row selections.
    async fn row_groups_to_read(
        &self,
        read_format: &ReadFormat,
        parquet_meta: &ParquetMetaData,
        metrics: &mut ReaderMetrics,
    ) -> BTreeMap<usize, Option<RowSelection>> {
        let num_row_groups = parquet_meta.num_row_groups();
        if num_row_groups == 0 {
            return BTreeMap::default();
        }
        metrics.num_row_groups_before_filtering += num_row_groups;

        self.prune_row_groups_by_inverted_index(parquet_meta, metrics)
            .await
            .or_else(|| self.prune_row_groups_by_minmax(read_format, parquet_meta, metrics))
            .unwrap_or_else(|| (0..num_row_groups).map(|i| (i, None)).collect())
    }

    /// Applies index to prune row groups.
    ///
    /// TODO(zhongzc): Devise a mechanism to enforce the non-use of indices
    /// as an escape route in case of index issues, and it can be used to test
    /// the correctness of the index.
    async fn prune_row_groups_by_inverted_index(
        &self,
        parquet_meta: &ParquetMetaData,
        metrics: &mut ReaderMetrics,
    ) -> Option<BTreeMap<usize, Option<RowSelection>>> {
        let Some(index_applier) = &self.index_applier else {
            return None;
        };

        if !self.file_handle.meta_ref().inverted_index_available() {
            return None;
        }

        let output = match index_applier.apply(self.file_handle.file_id()).await {
            Ok(output) => output,
            Err(err) => {
                if cfg!(any(test, feature = "test")) {
                    panic!(
                        "Failed to apply index, region_id: {}, file_id: {}, err: {}",
                        self.file_handle.region_id(),
                        self.file_handle.file_id(),
                        err
                    );
                } else {
                    warn!(
                        err; "Failed to apply index, region_id: {}, file_id: {}",
                        self.file_handle.region_id(), self.file_handle.file_id()
                    );
                }

                return None;
            }
        };

        // Let's assume that the number of rows in the first row group
        // can represent the `row_group_size` of the Parquet file.
        //
        // If the file contains only one row group, i.e. the number of rows
        // less than the `row_group_size`, the calculation of `row_group_id`
        // and `rg_begin_row_id` is still correct.
        let row_group_size = parquet_meta.row_group(0).num_rows() as usize;
        if row_group_size == 0 {
            return None;
        }

        let segment_row_count = output.segment_row_count;
        let row_groups = output
            .matched_segment_ids
            .iter_ones()
            .map(|seg_id| {
                let begin_row_id = seg_id * segment_row_count;
                let row_group_id = begin_row_id / row_group_size;

                let rg_begin_row_id = begin_row_id % row_group_size;
                let rg_end_row_id = rg_begin_row_id + segment_row_count;

                (row_group_id, rg_begin_row_id..rg_end_row_id)
            })
            .group_by(|(row_group_id, _)| *row_group_id)
            .into_iter()
            .map(|(row_group_id, group)| {
                let row_ranges = group.map(|(_, range)| range);

                let total_row_count = parquet_meta.row_group(row_group_id).num_rows() as usize;
                let (row_selection, skipped) =
                    row_selection_from_row_ranges(row_ranges, total_row_count);

                metrics.num_rows_in_row_group_before_filtering += total_row_count;
                metrics.num_rows_in_row_group_inverted_index_filtered += skipped;

                (row_group_id, Some(row_selection))
            })
            .collect::<BTreeMap<_, _>>();

        let filtered = parquet_meta.num_row_groups() - row_groups.len();
        metrics.num_row_groups_inverted_index_filtered += filtered;

        Some(row_groups)
    }

    /// Prunes row groups by min-max index.
    fn prune_row_groups_by_minmax(
        &self,
        read_format: &ReadFormat,
        parquet_meta: &ParquetMetaData,
        metrics: &mut ReaderMetrics,
    ) -> Option<BTreeMap<usize, Option<RowSelection>>> {
        let Some(predicate) = &self.predicate else {
            return None;
        };

        let num_row_groups = parquet_meta.num_row_groups();

        let region_meta = read_format.metadata();
        let row_groups = parquet_meta.row_groups();
        let stats =
            RowGroupPruningStats::new(row_groups, read_format, self.expected_metadata.clone());
        // Here we use the schema of the SST to build the physical expression. If the column
        // in the SST doesn't have the same column id as the column in the expected metadata,
        // we will get a None statistics for that column.
        let row_groups = predicate
            .prune_with_stats(&stats, region_meta.schema.arrow_schema())
            .iter()
            .zip(0..num_row_groups)
            .filter(|&(mask, _)| *mask)
            .map(|(_, id)| (id, None))
            .collect::<BTreeMap<_, _>>();

        let filtered = num_row_groups - row_groups.len();
        metrics.num_row_groups_min_max_filtered += filtered;

        Some(row_groups)
    }
}

/// Transforms time range into [SimpleFilterEvaluator].
fn time_range_to_predicate(
    time_range: TimestampRange,
    metadata: &RegionMetadataRef,
) -> Result<Vec<SimpleFilterContext>> {
    let ts_col = metadata.time_index_column();
    let ts_col_id = ts_col.column_id;

    let ts_to_filter = |op: Operator, timestamp: &Timestamp| {
        let value = match timestamp.unit() {
            TimeUnit::Second => ScalarValue::TimestampSecond(Some(timestamp.value()), None),
            TimeUnit::Millisecond => {
                ScalarValue::TimestampMillisecond(Some(timestamp.value()), None)
            }
            TimeUnit::Microsecond => {
                ScalarValue::TimestampMicrosecond(Some(timestamp.value()), None)
            }
            TimeUnit::Nanosecond => ScalarValue::TimestampNanosecond(Some(timestamp.value()), None),
        };
        let evaluator = SimpleFilterEvaluator::new(ts_col.column_schema.name.clone(), value, op)
            .context(error::BuildTimeRangeFilterSnafu {
                timestamp: *timestamp,
            })?;
        Ok(SimpleFilterContext::new(
            evaluator,
            ts_col_id,
            SemanticType::Timestamp,
            ts_col.column_schema.data_type.clone(),
        ))
    };

    let predicates = match (time_range.start(), time_range.end()) {
        (Some(start), Some(end)) => {
            vec![
                ts_to_filter(Operator::GtEq, start)?,
                ts_to_filter(Operator::Lt, end)?,
            ]
        }

        (Some(start), None) => {
            vec![ts_to_filter(Operator::GtEq, start)?]
        }

        (None, Some(end)) => {
            vec![ts_to_filter(Operator::Lt, end)?]
        }
        (None, None) => {
            vec![]
        }
    };
    Ok(predicates)
}

/// Parquet reader metrics.
#[derive(Debug, Default)]
pub(crate) struct ReaderMetrics {
    /// Number of row groups before filtering.
    num_row_groups_before_filtering: usize,
    /// Number of row groups filtered by inverted index.
    num_row_groups_inverted_index_filtered: usize,
    /// Number of row groups filtered by min-max index.
    num_row_groups_min_max_filtered: usize,
    /// Number of rows filtered by precise filter.
    num_rows_precise_filtered: usize,
    /// Number of rows in row group before filtering.
    num_rows_in_row_group_before_filtering: usize,
    /// Number of rows in row group filtered by inverted index.
    num_rows_in_row_group_inverted_index_filtered: usize,
    /// Duration to build the parquet reader.
    build_cost: Duration,
    /// Duration to scan the reader.
    scan_cost: Duration,
    /// Number of record batches read.
    num_record_batches: usize,
    /// Number of batches decoded.
    num_batches: usize,
    /// Number of rows read.
    num_rows: usize,
}

impl ReaderMetrics {
    /// Adds `other` metrics to this metrics.
    pub(crate) fn merge_from(&mut self, other: &ReaderMetrics) {
        self.num_row_groups_before_filtering += other.num_row_groups_before_filtering;
        self.num_row_groups_inverted_index_filtered += other.num_row_groups_inverted_index_filtered;
        self.num_row_groups_min_max_filtered += other.num_row_groups_min_max_filtered;
        self.num_rows_precise_filtered += other.num_rows_precise_filtered;
        self.num_rows_in_row_group_before_filtering += other.num_rows_in_row_group_before_filtering;
        self.num_rows_in_row_group_inverted_index_filtered +=
            other.num_rows_in_row_group_inverted_index_filtered;
        self.build_cost += other.build_cost;
        self.scan_cost += other.scan_cost;
        self.num_record_batches += other.num_record_batches;
        self.num_batches += other.num_batches;
        self.num_rows += other.num_rows;
    }
}

/// Builder to build a [ParquetRecordBatchReader] for a row group.
pub(crate) struct RowGroupReaderBuilder {
    /// SST file to read.
    ///
    /// Holds the file handle to avoid the file purge purge it.
    file_handle: FileHandle,
    /// Path of the file.
    file_path: String,
    /// Metadata of the parquet file.
    parquet_meta: Arc<ParquetMetaData>,
    /// Object store as an Operator.
    object_store: ObjectStore,
    /// Projection mask.
    projection: ProjectionMask,
    /// Field levels to read.
    field_levels: FieldLevels,
    /// Cache.
    cache_manager: Option<CacheManagerRef>,
}

impl RowGroupReaderBuilder {
    /// Path of the file to read.
    pub(crate) fn file_path(&self) -> &str {
        &self.file_path
    }

    /// Handle of the file to read.
    pub(crate) fn file_handle(&self) -> &FileHandle {
        &self.file_handle
    }

    /// Builds a [ParquetRecordBatchReader] to read the row group at `row_group_idx`.
    pub(crate) async fn build(
        &self,
        row_group_idx: usize,
        row_selection: Option<RowSelection>,
    ) -> Result<ParquetRecordBatchReader> {
        let mut row_group = InMemoryRowGroup::create(
            self.file_handle.region_id(),
            self.file_handle.file_id(),
            &self.parquet_meta,
            row_group_idx,
            self.cache_manager.clone(),
            &self.file_path,
            self.object_store.clone(),
        );
        // Fetches data into memory.
        row_group
            .fetch(&self.projection, row_selection.as_ref())
            .await
            .context(ReadParquetSnafu {
                path: &self.file_path,
            })?;

        // Builds the parquet reader.
        // Now the row selection is None.
        ParquetRecordBatchReader::try_new_with_row_groups(
            &self.field_levels,
            &row_group,
            DEFAULT_READ_BATCH_SIZE,
            row_selection,
        )
        .context(ReadParquetSnafu {
            path: &self.file_path,
        })
    }
}

/// The state of a [ParquetReader].
enum ReaderState {
    /// The reader is reading a row group.
    Readable(RowGroupReader),
    /// The reader is exhausted.
    Exhausted(ReaderMetrics),
}

impl ReaderState {
    /// Returns the metrics of the reader.
    fn metrics(&self) -> &ReaderMetrics {
        match self {
            ReaderState::Readable(reader) => &reader.metrics,
            ReaderState::Exhausted(m) => m,
        }
    }
}

/// Context to evaluate the column filter.
pub(crate) struct SimpleFilterContext {
    /// Filter to evaluate.
    filter: SimpleFilterEvaluator,
    /// Id of the column to evaluate.
    column_id: ColumnId,
    /// Semantic type of the column.
    semantic_type: SemanticType,
    /// The data type of the column.
    data_type: ConcreteDataType,
}

impl SimpleFilterContext {
    fn new(
        filter: SimpleFilterEvaluator,
        column_id: ColumnId,
        semantic_type: SemanticType,
        data_type: ConcreteDataType,
    ) -> Self {
        Self {
            filter,
            column_id,
            semantic_type,
            data_type,
        }
    }

    /// Creates a context for the `expr`.
    ///
    /// Returns None if the column to filter doesn't exist in the SST metadata or the
    /// expected metadata.
    fn new_opt(
        sst_meta: &RegionMetadataRef,
        expected_meta: Option<&RegionMetadata>,
        expr: &Expr,
    ) -> Option<Self> {
        let filter = SimpleFilterEvaluator::try_new(expr)?;
        let column_metadata = match expected_meta {
            Some(meta) => {
                // Gets the column metadata from the expected metadata.
                let column = meta.column_by_name(filter.column_name())?;
                // Checks if the column is present in the SST metadata. We still uses the
                // column from the expected metadata.
                let sst_column = sst_meta.column_by_id(column.column_id)?;
                debug_assert_eq!(column.semantic_type, sst_column.semantic_type);

                column
            }
            None => sst_meta.column_by_name(filter.column_name())?,
        };

        Some(Self {
            filter,
            column_id: column_metadata.column_id,
            semantic_type: column_metadata.semantic_type,
            data_type: column_metadata.column_schema.data_type.clone(),
        })
    }

    /// Returns the filter to evaluate.
    pub(crate) fn filter(&self) -> &SimpleFilterEvaluator {
        &self.filter
    }

    /// Returns the column id.
    pub(crate) fn column_id(&self) -> ColumnId {
        self.column_id
    }

    /// Returns the semantic type of the column.
    pub(crate) fn semantic_type(&self) -> SemanticType {
        self.semantic_type
    }

    /// Returns the data type of the column.
    pub(crate) fn data_type(&self) -> &ConcreteDataType {
        &self.data_type
    }
}

type RowGroupMap = BTreeMap<usize, Option<RowSelection>>;

/// Parquet batch reader to read our SST format.
pub struct ParquetReader {
    /// File range context.
    context: FileRangeContextRef,
    /// Indices of row groups to read, along with their respective row selections.
    row_groups: RowGroupMap,
    /// Reader of current row group.
    reader_state: ReaderState,
}

#[async_trait]
impl BatchReader for ParquetReader {
    async fn next_batch(&mut self) -> Result<Option<Batch>> {
        let ReaderState::Readable(reader) = &mut self.reader_state else {
            return Ok(None);
        };

        // We don't collect the elapsed time if the reader returns an error.
        if let Some(batch) = reader.next_batch().await? {
            return Ok(Some(batch));
        }

        // No more items in current row group, reads next row group.
        while let Some((row_group_idx, row_selection)) = self.row_groups.pop_first() {
            let parquet_reader = self
                .context
                .reader_builder()
                .build(row_group_idx, row_selection)
                .await?;
            // Resets the parquet reader.
            reader.reset_reader(parquet_reader);
            if let Some(batch) = reader.next_batch().await? {
                return Ok(Some(batch));
            }
        }

        // The reader is exhausted.
        self.reader_state = ReaderState::Exhausted(std::mem::take(&mut reader.metrics));
        Ok(None)
    }
}

impl Drop for ParquetReader {
    fn drop(&mut self) {
        let metrics = self.reader_state.metrics();
        debug!(
            "Read parquet {} {}, range: {:?}, {}/{} row groups, metrics: {:?}",
            self.context.reader_builder().file_handle.region_id(),
            self.context.reader_builder().file_handle.file_id(),
            self.context.reader_builder().file_handle.time_range(),
            metrics.num_row_groups_before_filtering
                - metrics.num_row_groups_inverted_index_filtered
                - metrics.num_row_groups_min_max_filtered,
            metrics.num_row_groups_before_filtering,
            metrics
        );

        // Report metrics.
        READ_STAGE_ELAPSED
            .with_label_values(&["build_parquet_reader"])
            .observe(metrics.build_cost.as_secs_f64());
        READ_STAGE_ELAPSED
            .with_label_values(&["scan_row_groups"])
            .observe(metrics.scan_cost.as_secs_f64());
        READ_ROWS_TOTAL
            .with_label_values(&["parquet"])
            .inc_by(metrics.num_rows as u64);
        READ_ROW_GROUPS_TOTAL
            .with_label_values(&["before_filtering"])
            .inc_by(metrics.num_row_groups_before_filtering as u64);
        READ_ROW_GROUPS_TOTAL
            .with_label_values(&["inverted_index_filtered"])
            .inc_by(metrics.num_row_groups_inverted_index_filtered as u64);
        READ_ROW_GROUPS_TOTAL
            .with_label_values(&["minmax_index_filtered"])
            .inc_by(metrics.num_row_groups_min_max_filtered as u64);
        PRECISE_FILTER_ROWS_TOTAL
            .with_label_values(&["parquet"])
            .inc_by(metrics.num_rows_precise_filtered as u64);
        READ_ROWS_IN_ROW_GROUP_TOTAL
            .with_label_values(&["before_filtering"])
            .inc_by(metrics.num_rows_in_row_group_before_filtering as u64);
        READ_ROWS_IN_ROW_GROUP_TOTAL
            .with_label_values(&["inverted_index_filtered"])
            .inc_by(metrics.num_rows_in_row_group_inverted_index_filtered as u64);
    }
}

impl ParquetReader {
    /// Creates a new reader.
    async fn new(
        context: FileRangeContextRef,
        mut row_groups: BTreeMap<usize, Option<RowSelection>>,
    ) -> Result<Self> {
        // No more items in current row group, reads next row group.
        let reader_state = if let Some((row_group_idx, row_selection)) = row_groups.pop_first() {
            let parquet_reader = context
                .reader_builder()
                .build(row_group_idx, row_selection)
                .await?;
            ReaderState::Readable(RowGroupReader::new(context.clone(), parquet_reader))
        } else {
            ReaderState::Exhausted(ReaderMetrics::default())
        };

        Ok(ParquetReader {
            context,
            row_groups,
            reader_state,
        })
    }

    /// Returns the metadata of the SST.
    pub fn metadata(&self) -> &RegionMetadataRef {
        self.context.read_format().metadata()
    }

    #[cfg(test)]
    pub fn parquet_metadata(&self) -> Arc<ParquetMetaData> {
        self.context.reader_builder().parquet_meta.clone()
    }
}

/// Reader to read a row group of a parquet file.
pub struct RowGroupReader {
    /// Context for file ranges.
    context: FileRangeContextRef,
    /// Inner parquet reader.
    reader: ParquetRecordBatchReader,
    /// Buffered batches to return.
    batches: VecDeque<Batch>,
    /// Local scan metrics.
    metrics: ReaderMetrics,
}

impl RowGroupReader {
    /// Creates a new reader.
    pub(crate) fn new(context: FileRangeContextRef, reader: ParquetRecordBatchReader) -> Self {
        Self {
            context,
            reader,
            batches: VecDeque::new(),
            metrics: ReaderMetrics::default(),
        }
    }

    /// Gets the metrics.
    pub(crate) fn metrics(&self) -> &ReaderMetrics {
        &self.metrics
    }

    /// Resets the parquet reader.
    fn reset_reader(&mut self, reader: ParquetRecordBatchReader) {
        self.reader = reader;
    }

    /// Tries to fetch next [Batch] from the reader.
    pub(crate) async fn next_batch(&mut self) -> Result<Option<Batch>> {
        let scan_start = Instant::now();
        if let Some(batch) = self.batches.pop_front() {
            self.metrics.num_rows += batch.num_rows();
            self.metrics.scan_cost += scan_start.elapsed();
            return Ok(Some(batch));
        }

        // We need to fetch next record batch and convert it to batches.
        while self.batches.is_empty() {
            let Some(record_batch) = self.fetch_next_record_batch()? else {
                self.metrics.scan_cost += scan_start.elapsed();
                return Ok(None);
            };
            self.metrics.num_record_batches += 1;

            self.context
                .read_format()
                .convert_record_batch(&record_batch, &mut self.batches)?;
            self.prune_batches()?;
            self.metrics.num_batches += self.batches.len();
        }
        let batch = self.batches.pop_front();
        self.metrics.num_rows += batch.as_ref().map(|b| b.num_rows()).unwrap_or(0);
        self.metrics.scan_cost += scan_start.elapsed();
        Ok(batch)
    }

    /// Tries to fetch next [RecordBatch] from the reader.
    ///
    /// If the reader is exhausted, reads next row group.
    fn fetch_next_record_batch(&mut self) -> Result<Option<RecordBatch>> {
        self.reader.next().transpose().context(ArrowReaderSnafu {
            path: self.context.file_path(),
        })
    }

    /// Prunes batches by the pushed down predicate.
    fn prune_batches(&mut self) -> Result<()> {
        // fast path
        if self.context.filters().is_empty() {
            return Ok(());
        }

        let mut new_batches = VecDeque::new();
        let batches = std::mem::take(&mut self.batches);
        for batch in batches {
            let num_rows_before_filter = batch.num_rows();
            let Some(batch_filtered) = self.context.precise_filter(batch)? else {
                // the entire batch is filtered out
                self.metrics.num_rows_precise_filtered += num_rows_before_filter;
                continue;
            };

            // update metric
            let filtered_rows = num_rows_before_filter - batch_filtered.num_rows();
            self.metrics.num_rows_precise_filtered += filtered_rows;

            if !batch_filtered.is_empty() {
                new_batches.push_back(batch_filtered);
            }
        }
        self.batches = new_batches;

        Ok(())
    }
}

impl Drop for RowGroupReader {
    fn drop(&mut self) {
        debug!(
            "Row group reader finish, file_id: {}, metrics: {:?}",
            self.context.file_path(),
            self.metrics
        );
    }
}
