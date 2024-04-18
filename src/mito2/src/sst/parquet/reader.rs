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
use std::ops::BitAnd;
use std::sync::Arc;
use std::time::{Duration, Instant};

use api::v1::SemanticType;
use async_trait::async_trait;
use common_recordbatch::filter::SimpleFilterEvaluator;
use common_telemetry::{debug, warn};
use common_time::range::TimestampRange;
use datafusion_common::arrow::array::BooleanArray;
use datafusion_common::arrow::buffer::BooleanBuffer;
use datatypes::arrow::record_batch::RecordBatch;
use itertools::Itertools;
use object_store::ObjectStore;
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, RowSelection, RowSelector};
use parquet::arrow::{parquet_to_arrow_field_levels, FieldLevels, ProjectionMask};
use parquet::file::metadata::ParquetMetaData;
use parquet::format::KeyValue;
use snafu::{OptionExt, ResultExt};
use store_api::metadata::{RegionMetadata, RegionMetadataRef};
use store_api::storage::ColumnId;
use table::predicate::Predicate;

use crate::cache::CacheManagerRef;
use crate::error::{
    ArrowReaderSnafu, FieldTypeMismatchSnafu, FilterRecordBatchSnafu, InvalidMetadataSnafu,
    InvalidParquetSnafu, ReadParquetSnafu, Result,
};
use crate::metrics::{
    PRECISE_FILTER_ROWS_TOTAL, READ_ROWS_IN_ROW_GROUP_TOTAL, READ_ROWS_TOTAL,
    READ_ROW_GROUPS_TOTAL, READ_STAGE_ELAPSED,
};
use crate::read::{Batch, BatchReader};
use crate::row_converter::{McmpRowCodec, RowCodec, SortField};
use crate::sst::file::FileHandle;
use crate::sst::index::applier::{FullTextIndexApplier, SstIndexApplierRef};
use crate::sst::parquet::format::ReadFormat;
use crate::sst::parquet::metadata::MetadataLoader;
use crate::sst::parquet::row_group::InMemoryRowGroup;
use crate::sst::parquet::row_selection::row_selection_from_row_ranges;
use crate::sst::parquet::stats::RowGroupPruningStats;
use crate::sst::parquet::{DEFAULT_READ_BATCH_SIZE, PARQUET_METADATA_KEY};

/// Parquet SST reader builder.
pub(crate) struct ParquetReaderBuilder {
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

    full_text_index_applier: Option<FullTextIndexApplier>,
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
            full_text_index_applier: None,
        }
    }

    /// Attaches the predicate to the builder.
    pub fn predicate(mut self, predicate: Option<Predicate>) -> ParquetReaderBuilder {
        self.predicate = predicate;
        self
    }

    /// Attaches the time range to the builder.
    pub fn time_range(mut self, time_range: Option<TimestampRange>) -> ParquetReaderBuilder {
        self.time_range = time_range;
        self
    }

    /// Attaches the projection to the builder.
    ///
    /// The reader only applies the projection to fields.
    pub fn projection(mut self, projection: Option<Vec<ColumnId>>) -> ParquetReaderBuilder {
        self.projection = projection;
        self
    }

    /// Attaches the cache to the builder.
    pub fn cache(mut self, cache: Option<CacheManagerRef>) -> ParquetReaderBuilder {
        self.cache_manager = cache;
        self
    }

    /// Attaches the index applier to the builder.
    #[must_use]
    pub fn index_applier(mut self, index_applier: Option<SstIndexApplierRef>) -> Self {
        self.index_applier = index_applier;
        self
    }

    #[must_use]
    pub fn full_text_index_applier(
        mut self,
        full_text_index_applier: Option<FullTextIndexApplier>,
    ) -> Self {
        self.full_text_index_applier = full_text_index_applier;
        self
    }

    /// Builds and initializes a [ParquetReader].
    ///
    /// This needs to perform IO operation.
    pub async fn build(&self) -> Result<ParquetReader> {
        let start = Instant::now();

        let file_path = self.file_handle.file_path(&self.file_dir);
        let file_size = self.file_handle.meta().file_size;
        // Loads parquet metadata of the file.
        let parquet_meta = self.read_parquet_metadata(&file_path, file_size).await?;
        // Decodes region metadata.
        let key_value_meta = parquet_meta.file_metadata().key_value_metadata();
        let region_meta = Self::get_region_metadata(&file_path, key_value_meta)?;
        let read_format = ReadFormat::new(Arc::new(region_meta));

        // Computes the projection mask.
        let parquet_schema_desc = parquet_meta.file_metadata().schema_descr();
        let projection_mask = if let Some(column_ids) = self.projection.as_ref() {
            let indices = read_format.projection_indices(column_ids.iter().copied());
            // Now we assumes we don't have nested schemas.
            ProjectionMask::roots(parquet_schema_desc, indices)
        } else {
            ProjectionMask::all()
        };

        // Computes the field levels.
        let hint = Some(read_format.arrow_schema().fields());
        let field_levels =
            parquet_to_arrow_field_levels(parquet_schema_desc, projection_mask.clone(), hint)
                .context(ReadParquetSnafu { path: &file_path })?;

        let mut metrics = Metrics::default();

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

        let predicate = if let Some(p) = &self.predicate {
            p.exprs()
                .iter()
                .filter_map(|expr| SimpleFilterEvaluator::try_new(expr.df_expr()))
                .collect()
        } else {
            vec![]
        };

        let codec = McmpRowCodec::new(
            read_format
                .metadata()
                .primary_key_columns()
                .map(|c| SortField::new(c.column_schema.data_type.clone()))
                .collect(),
        );

        Ok(ParquetReader {
            row_groups,
            read_format,
            reader_builder,
            predicate,
            current_reader: None,
            batches: VecDeque::new(),
            codec,
            metrics,
        })
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
        metrics: &mut Metrics,
    ) -> BTreeMap<usize, Option<RowSelection>> {
        let num_row_groups = parquet_meta.num_row_groups();
        if num_row_groups == 0 {
            return BTreeMap::default();
        }
        metrics.num_row_groups_before_filtering += num_row_groups;

        if let Some(full_text_index_result) = self.prune_row_groups_by_full_text_index(parquet_meta)
        {
            return full_text_index_result;
        }

        self.prune_row_groups_by_inverted_index(parquet_meta, metrics)
            .await
            .or_else(|| self.prune_row_groups_by_minmax(read_format, parquet_meta, metrics))
            .unwrap_or_else(|| (0..num_row_groups).map(|i| (i, None)).collect())

        // todo: change here
    }

    /// Applies index to prune row groups.
    ///
    /// TODO(zhongzc): Devise a mechanism to enforce the non-use of indices
    /// as an escape route in case of index issues, and it can be used to test
    /// the correctness of the index.
    async fn prune_row_groups_by_inverted_index(
        &self,
        parquet_meta: &ParquetMetaData,
        metrics: &mut Metrics,
    ) -> Option<BTreeMap<usize, Option<RowSelection>>> {
        let Some(index_applier) = &self.index_applier else {
            return None;
        };

        if !self.file_handle.meta().inverted_index_available() {
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
        metrics: &mut Metrics,
    ) -> Option<BTreeMap<usize, Option<RowSelection>>> {
        let Some(predicate) = &self.predicate else {
            return None;
        };

        let num_row_groups = parquet_meta.num_row_groups();

        let region_meta = read_format.metadata();
        let column_ids = region_meta
            .column_metadatas
            .iter()
            .map(|c| c.column_id)
            .collect();

        let row_groups = parquet_meta.row_groups();
        let stats = RowGroupPruningStats::new(row_groups, read_format, column_ids);
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

    fn prune_row_groups_by_full_text_index(
        &self,
        parquet_meta: &ParquetMetaData,
    ) -> Option<BTreeMap<usize, Option<RowSelection>>> {
        let applier = self.full_text_index_applier.as_ref()?;
        let file_id = self.file_handle.file_id();
        let mut selected_row = applier.apply(file_id).unwrap();

        common_telemetry::info!("[DEBUG] selected_row: {:?}", selected_row.len());

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

        // translate `selected_row` into row groups selection
        selected_row.sort_unstable();
        let mut row_groups_selected = BTreeMap::new();
        for row_id in selected_row.iter() {
            let row_group_id = row_id / row_group_size;
            let rg_row_id = row_id % row_group_size;

            row_groups_selected
                .entry(row_group_id)
                .or_insert_with(Vec::new)
                .push(rg_row_id);
        }
        let row_group = row_groups_selected
            .into_iter()
            .map(|(row_group_id, row_ids)| {
                let mut current_row = 0;
                let mut selection = vec![];
                for row_id in row_ids {
                    selection.push(RowSelector::skip(row_id - current_row));
                    selection.push(RowSelector::select(1));
                    current_row = row_id + 1;
                }

                (row_group_id, Some(RowSelection::from(selection)))
            })
            .collect();

        // common_telemetry::info!("[DEBUG] row_group: {:?}", row_group);

        Some(row_group)
    }
}

/// Parquet reader metrics.
#[derive(Debug, Default)]
struct Metrics {
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

/// Builder to build a [ParquetRecordBatchReader] for a row group.
struct RowGroupReaderBuilder {
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
    fn file_path(&self) -> &str {
        &self.file_path
    }

    /// Builds a [ParquetRecordBatchReader] to read the row group at `row_group_idx`.
    async fn build(
        &mut self,
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

/// Parquet batch reader to read our SST format.
pub struct ParquetReader {
    /// Indices of row groups to read, along with their respective row selections.
    row_groups: BTreeMap<usize, Option<RowSelection>>,
    /// Helper to read record batches.
    ///
    /// Not `None` if [ParquetReader::stream] is not `None`.
    read_format: ReadFormat,
    /// Builder to build row group readers.
    ///
    /// The builder contains the file handle, so don't drop the builder while using
    /// the [ParquetReader].
    reader_builder: RowGroupReaderBuilder,
    /// Predicate pushed down to this reader.
    predicate: Vec<SimpleFilterEvaluator>,
    /// Reader of current row group.
    current_reader: Option<ParquetRecordBatchReader>,
    /// Buffered batches to return.
    batches: VecDeque<Batch>,
    /// Decoder for primary keys
    codec: McmpRowCodec,
    /// Local metrics.
    metrics: Metrics,
}

#[async_trait]
impl BatchReader for ParquetReader {
    async fn next_batch(&mut self) -> Result<Option<Batch>> {
        let start = Instant::now();
        if let Some(batch) = self.batches.pop_front() {
            self.metrics.scan_cost += start.elapsed();
            self.metrics.num_rows += batch.num_rows();
            return Ok(Some(batch));
        }

        // We need to fetch next record batch and convert it to batches.
        while self.batches.is_empty() {
            let Some(record_batch) = self.fetch_next_record_batch().await? else {
                self.metrics.scan_cost += start.elapsed();
                return Ok(None);
            };
            self.metrics.num_record_batches += 1;

            self.read_format
                .convert_record_batch(&record_batch, &mut self.batches)?;
            self.prune_batches()?;
            self.metrics.num_batches += self.batches.len();
        }
        let batch = self.batches.pop_front();
        self.metrics.scan_cost += start.elapsed();
        self.metrics.num_rows += batch.as_ref().map(|b| b.num_rows()).unwrap_or(0);
        Ok(batch)
    }
}

impl Drop for ParquetReader {
    fn drop(&mut self) {
        debug!(
            "Read parquet {} {}, range: {:?}, {}/{} row groups, metrics: {:?}",
            self.reader_builder.file_handle.region_id(),
            self.reader_builder.file_handle.file_id(),
            self.reader_builder.file_handle.time_range(),
            self.metrics.num_row_groups_before_filtering
                - self.metrics.num_row_groups_inverted_index_filtered
                - self.metrics.num_row_groups_min_max_filtered,
            self.metrics.num_row_groups_before_filtering,
            self.metrics
        );

        // Report metrics.
        READ_STAGE_ELAPSED
            .with_label_values(&["build_parquet_reader"])
            .observe(self.metrics.build_cost.as_secs_f64());
        READ_STAGE_ELAPSED
            .with_label_values(&["scan_row_groups"])
            .observe(self.metrics.scan_cost.as_secs_f64());
        READ_ROWS_TOTAL
            .with_label_values(&["parquet"])
            .inc_by(self.metrics.num_rows as u64);
        READ_ROW_GROUPS_TOTAL
            .with_label_values(&["before_filtering"])
            .inc_by(self.metrics.num_row_groups_before_filtering as u64);
        READ_ROW_GROUPS_TOTAL
            .with_label_values(&["inverted_index_filtered"])
            .inc_by(self.metrics.num_row_groups_inverted_index_filtered as u64);
        READ_ROW_GROUPS_TOTAL
            .with_label_values(&["minmax_index_filtered"])
            .inc_by(self.metrics.num_row_groups_min_max_filtered as u64);
        PRECISE_FILTER_ROWS_TOTAL
            .with_label_values(&["parquet"])
            .inc_by(self.metrics.num_rows_precise_filtered as u64);
        READ_ROWS_IN_ROW_GROUP_TOTAL
            .with_label_values(&["before_filtering"])
            .inc_by(self.metrics.num_rows_in_row_group_before_filtering as u64);
        READ_ROWS_IN_ROW_GROUP_TOTAL
            .with_label_values(&["inverted_index_filtered"])
            .inc_by(self.metrics.num_rows_in_row_group_inverted_index_filtered as u64);
    }
}

impl ParquetReader {
    /// Returns the metadata of the SST.
    pub fn metadata(&self) -> &RegionMetadataRef {
        self.read_format.metadata()
    }

    /// Tries to fetch next [RecordBatch] from the reader.
    ///
    /// If the reader is exhausted, reads next row group.
    async fn fetch_next_record_batch(&mut self) -> Result<Option<RecordBatch>> {
        if let Some(row_group_reader) = &mut self.current_reader {
            if let Some(record_batch) =
                row_group_reader
                    .next()
                    .transpose()
                    .context(ArrowReaderSnafu {
                        path: self.reader_builder.file_path(),
                    })?
            {
                return Ok(Some(record_batch));
            }
        }

        // No more items in current row group, reads next row group.
        while let Some((row_group_idx, row_selection)) = self.row_groups.pop_first() {
            let mut row_group_reader = self
                .reader_builder
                .build(row_group_idx, row_selection)
                .await?;
            let Some(record_batch) =
                row_group_reader
                    .next()
                    .transpose()
                    .context(ArrowReaderSnafu {
                        path: self.reader_builder.file_path(),
                    })?
            else {
                continue;
            };

            // Sets current reader to this reader.
            self.current_reader = Some(row_group_reader);
            return Ok(Some(record_batch));
        }

        Ok(None)
    }

    /// Prunes batches by the pushed down predicate.
    fn prune_batches(&mut self) -> Result<()> {
        // fast path
        if self.predicate.is_empty() {
            return Ok(());
        }

        let mut new_batches = VecDeque::new();
        let batches = std::mem::take(&mut self.batches);
        for batch in batches {
            let num_rows_before_filter = batch.num_rows();
            let Some(batch_filtered) = self.precise_filter(batch)? else {
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

    /// TRY THE BEST to perform pushed down predicate precisely on the input batch.
    /// Return the filtered batch. If the entire batch is filtered out, return None.
    ///
    /// Supported filter expr type is defined in [SimpleFilterEvaluator].
    ///
    /// When a filter is referencing primary key column, this method will decode
    /// the primary key and put it into the batch.
    fn precise_filter(&self, mut input: Batch) -> Result<Option<Batch>> {
        let mut mask = BooleanBuffer::new_set(input.num_rows());

        // Run filter one by one and combine them result
        // TODO(ruihang): run primary key filter first. It may short circuit other filters
        for filter in &self.predicate {
            let column_name = filter.column_name();
            let Some(column_metadata) = self.read_format.metadata().column_by_name(column_name)
            else {
                // column not found, skip
                // in situation like an column is added later
                continue;
            };
            let result = match column_metadata.semantic_type {
                SemanticType::Tag => {
                    let pk_values = if let Some(pk_values) = input.pk_values() {
                        pk_values
                    } else {
                        input.set_pk_values(self.codec.decode(input.primary_key())?);
                        input.pk_values().unwrap()
                    };
                    // Safety: this is a primary key
                    let pk_index = self
                        .read_format
                        .metadata()
                        .primary_key_index(column_metadata.column_id)
                        .unwrap();
                    let pk_value = pk_values[pk_index]
                        .try_to_scalar_value(&column_metadata.column_schema.data_type)
                        .context(FieldTypeMismatchSnafu)?;
                    if filter
                        .evaluate_scalar(&pk_value)
                        .context(FilterRecordBatchSnafu)?
                    {
                        continue;
                    } else {
                        // PK not match means the entire batch is filtered out.
                        return Ok(None);
                    }
                }
                SemanticType::Field => {
                    let Some(field_index) = self
                        .read_format
                        .field_index_by_id(column_metadata.column_id)
                    else {
                        continue;
                    };
                    let field_col = &input.fields()[field_index].data;
                    filter
                        .evaluate_vector(field_col)
                        .context(FilterRecordBatchSnafu)?
                }
                SemanticType::Timestamp => filter
                    .evaluate_vector(input.timestamps())
                    .context(FilterRecordBatchSnafu)?,
            };

            mask = mask.bitand(&result);
        }

        input.filter(&BooleanArray::from(mask).into())?;

        Ok(Some(input))
    }

    #[cfg(test)]
    pub fn parquet_metadata(&self) -> Arc<ParquetMetaData> {
        self.reader_builder.parquet_meta.clone()
    }
}
