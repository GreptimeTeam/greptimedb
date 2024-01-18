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

use std::collections::{BTreeSet, VecDeque};
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
use object_store::ObjectStore;
use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
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
    InvalidParquetSnafu, OpenDalSnafu, ReadParquetSnafu, Result,
};
use crate::metrics::{
    PRECISE_FILTER_ROWS_TOTAL, READ_ROWS_TOTAL, READ_ROW_GROUPS_TOTAL, READ_STAGE_ELAPSED,
};
use crate::read::{Batch, BatchReader};
use crate::row_converter::{McmpRowCodec, RowCodec, SortField};
use crate::sst::file::FileHandle;
use crate::sst::index::applier::SstIndexApplierRef;
use crate::sst::parquet::format::ReadFormat;
use crate::sst::parquet::metadata::MetadataLoader;
use crate::sst::parquet::row_group::InMemoryRowGroup;
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

    /// Builds and initializes a [ParquetReader].
    ///
    /// This needs to perform IO operation.
    pub async fn build(&self) -> Result<ParquetReader> {
        let start = Instant::now();

        let file_path = self.file_handle.file_path(&self.file_dir);
        let file_size = self.file_handle.file_size();
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

        // Computes row groups to read.
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

        let metrics = Metrics {
            build_cost: start.elapsed(),
            ..Default::default()
        };

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
        // Tries to get from global cache.
        if let Some(metadata) = self.cache_manager.as_ref().and_then(|cache| {
            cache.get_parquet_meta_data(self.file_handle.region_id(), self.file_handle.file_id())
        }) {
            return Ok(metadata);
        }

        // Cache miss, read directly.
        let metadata_loader =
            MetadataLoader::new(self.object_store.clone(), file_path, Some(file_size));
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

    /// Computes row groups to read.
    async fn row_groups_to_read(
        &self,
        read_format: &ReadFormat,
        parquet_meta: &ParquetMetaData,
        metrics: &mut Metrics,
    ) -> BTreeSet<usize> {
        let mut row_group_ids: BTreeSet<_> = (0..parquet_meta.num_row_groups()).collect();
        metrics.num_row_groups_unfiltered += row_group_ids.len();

        // Applies index to prune row groups.
        //
        // TODO(zhongzc): Devise a mechanism to enforce the non-use of indices
        // as an escape route in case of index issues, and it can be used to test
        // the correctness of the index.
        if let Some(index_applier) = &self.index_applier {
            if self.file_handle.meta().inverted_index_available() {
                match index_applier.apply(self.file_handle.file_id()).await {
                    Ok(row_groups) => row_group_ids = row_groups,
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
                    }
                }
            }
        }
        metrics.num_row_groups_inverted_index_selected += row_group_ids.len();

        if row_group_ids.is_empty() {
            return row_group_ids;
        }

        // Prunes row groups by min-max index.
        if let Some(predicate) = &self.predicate {
            let region_meta = read_format.metadata();
            let column_ids = match &self.projection {
                Some(ids) => ids.iter().cloned().collect(),
                None => region_meta
                    .column_metadatas
                    .iter()
                    .map(|c| c.column_id)
                    .collect(),
            };

            let row_groups = row_group_ids
                .iter()
                .map(|id| parquet_meta.row_group(*id))
                .collect::<Vec<_>>();
            let stats = RowGroupPruningStats::new(&row_groups, read_format, column_ids);
            let mut mask = predicate
                .prune_with_stats(&stats, region_meta.schema.arrow_schema())
                .into_iter();

            row_group_ids.retain(|_| mask.next().unwrap_or(false));
        };
        metrics.num_row_groups_min_max_selected += row_group_ids.len();

        row_group_ids
    }
}

/// Parquet reader metrics.
#[derive(Debug, Default)]
struct Metrics {
    /// Number of unfiltered row groups.
    num_row_groups_unfiltered: usize,
    /// Number of row groups to read after filtering by inverted index.
    num_row_groups_inverted_index_selected: usize,
    /// Number of row groups to read after filtering by min-max index.
    num_row_groups_min_max_selected: usize,
    num_rows_precise_filtered: usize,
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
    async fn build(&mut self, row_group_idx: usize) -> Result<ParquetRecordBatchReader> {
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
            .fetch(&self.projection, None)
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
            None,
        )
        .context(ReadParquetSnafu {
            path: &self.file_path,
        })
    }
}

/// Parquet batch reader to read our SST format.
pub struct ParquetReader {
    /// Indices of row groups to read.
    row_groups: BTreeSet<usize>,
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
            self.metrics.num_row_groups_min_max_selected,
            self.metrics.num_row_groups_unfiltered,
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
            .with_label_values(&["unfiltered"])
            .inc_by(self.metrics.num_row_groups_unfiltered as u64);
        READ_ROW_GROUPS_TOTAL
            .with_label_values(&["inverted_index_selected"])
            .inc_by(self.metrics.num_row_groups_inverted_index_selected as u64);
        READ_ROW_GROUPS_TOTAL
            .with_label_values(&["min_max_index_selected"])
            .inc_by(self.metrics.num_row_groups_min_max_selected as u64);
        PRECISE_FILTER_ROWS_TOTAL
            .with_label_values(&["parquet"])
            .inc_by(self.metrics.num_rows_precise_filtered as u64);
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
        while let Some(row_group_idx) = self.row_groups.pop_first() {
            let mut row_group_reader = self.reader_builder.build(row_group_idx).await?;
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
                    let pk_values = self.codec.decode(input.primary_key())?;
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
                        input.set_pk_values(pk_values);
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
