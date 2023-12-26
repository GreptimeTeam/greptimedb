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

use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use common_telemetry::debug;
use common_time::range::TimestampRange;
use datatypes::arrow::record_batch::RecordBatch;
use object_store::ObjectStore;
use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::{parquet_to_arrow_field_levels, FieldLevels, ProjectionMask};
use parquet::file::metadata::ParquetMetaData;
use parquet::format::KeyValue;
use snafu::{OptionExt, ResultExt};
use store_api::metadata::{RegionMetadata, RegionMetadataRef};
use store_api::storage::ColumnId;
use table::predicate::Predicate;
use tokio::io::BufReader;

use crate::cache::CacheManagerRef;
use crate::error::{
    ArrowReaderSnafu, InvalidMetadataSnafu, InvalidParquetSnafu, OpenDalSnafu, ReadParquetSnafu,
    Result,
};
use crate::metrics::{READ_ROWS_TOTAL, READ_STAGE_ELAPSED};
use crate::read::{Batch, BatchReader};
use crate::sst::file::FileHandle;
use crate::sst::parquet::format::ReadFormat;
use crate::sst::parquet::row_group::InMemoryRowGroup;
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

    /// Builds and initializes a [ParquetReader].
    ///
    /// This needs to perform IO operation.
    pub async fn build(&self) -> Result<ParquetReader> {
        let start = Instant::now();

        let file_path = self.file_handle.file_path(&self.file_dir);
        // Now we create a reader to read the whole file.
        let reader = self
            .object_store
            .reader(&file_path)
            .await
            .context(OpenDalSnafu)?;
        let mut reader = BufReader::new(reader);
        // Loads parquet metadata of the file.
        let parquet_meta = self.read_parquet_metadata(&mut reader, &file_path).await?;
        // Decodes region metadata.
        let key_value_meta = parquet_meta.file_metadata().key_value_metadata();
        let region_meta = Self::get_region_metadata(&file_path, key_value_meta)?;
        // Computes column ids to read.
        let column_ids: HashSet<_> = self
            .projection
            .as_ref()
            .map(|p| p.iter().cloned().collect())
            .unwrap_or_else(|| {
                region_meta
                    .column_metadatas
                    .iter()
                    .map(|c| c.column_id)
                    .collect()
            });
        let read_format = ReadFormat::new(Arc::new(region_meta));

        // Prunes row groups by metadata.
        let row_groups: VecDeque<_> = if let Some(predicate) = &self.predicate {
            let stats =
                RowGroupPruningStats::new(parquet_meta.row_groups(), &read_format, column_ids);

            predicate
                .prune_with_stats(&stats, read_format.metadata().schema.arrow_schema())
                .into_iter()
                .enumerate()
                .filter_map(|(idx, valid)| if valid { Some(idx) } else { None })
                .collect()
        } else {
            (0..parquet_meta.num_row_groups()).collect()
        };

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
            read_row_groups: row_groups.len(),
            build_cost: start.elapsed(),
            ..Default::default()
        };

        Ok(ParquetReader {
            row_groups,
            read_format,
            reader_builder,
            current_reader: None,
            batches: VecDeque::new(),
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
        reader: &mut impl AsyncFileReader,
        file_path: &str,
    ) -> Result<Arc<ParquetMetaData>> {
        // Tries to get from global cache.
        if let Some(metadata) = self.cache_manager.as_ref().and_then(|cache| {
            cache.get_parquet_meta_data(self.file_handle.region_id(), self.file_handle.file_id())
        }) {
            return Ok(metadata);
        }

        // Cache miss, get from the reader.
        let metadata = reader
            .get_metadata()
            .await
            .context(ReadParquetSnafu { path: file_path })?;
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
}

/// Parquet reader metrics.
#[derive(Debug, Default)]
struct Metrics {
    /// Number of row groups to read.
    read_row_groups: usize,
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
    row_groups: VecDeque<usize>,
    /// Helper to read record batches.
    ///
    /// Not `None` if [ParquetReader::stream] is not `None`.
    read_format: ReadFormat,
    /// Builder to build row group readers.
    ///
    /// The builder contains the file handle, so don't drop the builder while using
    /// the [ParquetReader].
    reader_builder: RowGroupReaderBuilder,
    /// Reader of current row group.
    current_reader: Option<ParquetRecordBatchReader>,
    /// Buffered batches to return.
    batches: VecDeque<Batch>,
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
        let Some(record_batch) = self.fetch_next_record_batch().await? else {
            self.metrics.scan_cost += start.elapsed();
            return Ok(None);
        };
        self.metrics.num_record_batches += 1;

        self.read_format
            .convert_record_batch(&record_batch, &mut self.batches)?;
        self.metrics.num_batches += self.batches.len();

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
            self.metrics.read_row_groups,
            self.reader_builder.parquet_meta.num_row_groups(),
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
        while let Some(row_group_idx) = self.row_groups.pop_front() {
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

    #[cfg(test)]
    pub fn parquet_metadata(&self) -> Arc<ParquetMetaData> {
        self.reader_builder.parquet_meta.clone()
    }
}
