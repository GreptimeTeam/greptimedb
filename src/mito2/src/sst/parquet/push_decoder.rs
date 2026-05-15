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

//! Push decoder stream implementation for SST parquet files.

use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use datatypes::arrow::record_batch::RecordBatch;
use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::ObjectStore;
use parquet::DecodeResult;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, RowSelection};
use parquet::arrow::push_decoder::ParquetPushDecoderBuilder;
use snafu::ResultExt;

use crate::cache::file_cache::{FileType, IndexKey};
use crate::cache::{CacheStrategy, PageKey, PageValue};
use crate::error::{OpenDalSnafu, ReadParquetSnafu, Result};
use crate::metrics::{READ_STAGE_ELAPSED, READ_STAGE_FETCH_PAGES};
use crate::sst::file::RegionFileId;
use crate::sst::parquet::DEFAULT_READ_BATCH_SIZE;
use crate::sst::parquet::helper::fetch_byte_ranges;
use crate::sst::parquet::row_group::{ParquetFetchMetrics, compute_total_range_size};

/// Fetches parquet byte ranges through Greptime's cache hierarchy.
///
/// The push decoder decides which ranges are required for decoding, while this
/// fetcher keeps cache lookup, local write-cache reads, and remote I/O explicit
/// in Greptime code.
pub(crate) struct SstParquetRangeFetcher {
    /// Region file ID for cache key.
    region_file_id: RegionFileId,
    /// Path to the parquet file in object storage.
    file_path: String,
    /// Object store for reading data.
    object_store: ObjectStore,
    /// Cache strategy for reading pages.
    cache_strategy: CacheStrategy,
    /// Row group index for cache key.
    row_group_idx: usize,
    /// Optional metrics for tracking fetch operations.
    fetch_metrics: Option<ParquetFetchMetrics>,
}

impl SstParquetRangeFetcher {
    /// Creates a new [SstParquetRangeFetcher].
    pub(crate) fn new(
        region_file_id: RegionFileId,
        file_path: String,
        object_store: ObjectStore,
        cache_strategy: CacheStrategy,
        row_group_idx: usize,
        fetch_metrics: Option<ParquetFetchMetrics>,
    ) -> Self {
        Self {
            region_file_id,
            file_path,
            object_store,
            cache_strategy,
            row_group_idx,
            fetch_metrics,
        }
    }

    /// Fetches byte ranges from page cache, write cache, or object store.
    async fn fetch_bytes_with_cache(&self, ranges: Vec<Range<u64>>) -> Result<Vec<Bytes>> {
        let fetch_start = self
            .fetch_metrics
            .as_ref()
            .map(|_| std::time::Instant::now());
        let _timer = READ_STAGE_FETCH_PAGES.start_timer();

        let page_key = PageKey::new(
            self.region_file_id.file_id(),
            self.row_group_idx,
            ranges.clone(),
        );

        // Check page cache first.
        if let Some(pages) = self.cache_strategy.get_pages(&page_key) {
            if let Some(metrics) = &self.fetch_metrics {
                let total_size: u64 = ranges.iter().map(|r| r.end - r.start).sum();
                let mut metrics_data = metrics.data.lock().unwrap();
                metrics_data.page_cache_hit += 1;
                metrics_data.pages_to_fetch_mem += ranges.len();
                metrics_data.page_size_to_fetch_mem += total_size;
                metrics_data.page_size_needed += total_size;
                if let Some(start) = fetch_start {
                    metrics_data.total_fetch_elapsed += start.elapsed();
                }
            }
            return Ok(pages.compressed.clone());
        }

        // Calculate total range size for metrics.
        let (total_range_size, unaligned_size) = compute_total_range_size(&ranges);

        // Check write cache.
        let key = IndexKey::new(
            self.region_file_id.region_id(),
            self.region_file_id.file_id(),
            FileType::Parquet,
        );
        let fetch_write_cache_start = self
            .fetch_metrics
            .as_ref()
            .map(|_| std::time::Instant::now());
        let write_cache_result = match self.cache_strategy.write_cache() {
            Some(cache) => cache.file_cache().read_ranges(key, &ranges).await,
            None => None,
        };

        let pages = match write_cache_result {
            Some(data) => {
                if let Some(metrics) = &self.fetch_metrics {
                    let elapsed = fetch_write_cache_start
                        .map(|start| start.elapsed())
                        .unwrap_or_default();
                    let range_size_needed: u64 = ranges.iter().map(|r| r.end - r.start).sum();
                    let mut metrics_data = metrics.data.lock().unwrap();
                    metrics_data.write_cache_fetch_elapsed += elapsed;
                    metrics_data.write_cache_hit += 1;
                    metrics_data.pages_to_fetch_write_cache += ranges.len();
                    metrics_data.page_size_to_fetch_write_cache += unaligned_size;
                    metrics_data.page_size_needed += range_size_needed;
                }
                data
            }
            None => {
                // Fetch data from object store.
                let _timer = READ_STAGE_ELAPSED
                    .with_label_values(&["cache_miss_read"])
                    .start_timer();

                let start = self
                    .fetch_metrics
                    .as_ref()
                    .map(|_| std::time::Instant::now());
                let data = fetch_byte_ranges(&self.file_path, self.object_store.clone(), &ranges)
                    .await
                    .context(OpenDalSnafu)?;

                if let Some(metrics) = &self.fetch_metrics {
                    let elapsed = start.map(|start| start.elapsed()).unwrap_or_default();
                    let range_size_needed: u64 = ranges.iter().map(|r| r.end - r.start).sum();
                    let mut metrics_data = metrics.data.lock().unwrap();
                    metrics_data.store_fetch_elapsed += elapsed;
                    metrics_data.cache_miss += 1;
                    metrics_data.pages_to_fetch_store += ranges.len();
                    metrics_data.page_size_to_fetch_store += unaligned_size;
                    metrics_data.page_size_needed += range_size_needed;
                }
                data
            }
        };

        // Put pages back to the cache.
        let page_value = PageValue::new(pages.clone(), total_range_size);
        self.cache_strategy
            .put_pages(page_key, Arc::new(page_value));

        if let (Some(metrics), Some(start)) = (&self.fetch_metrics, fetch_start) {
            metrics.data.lock().unwrap().total_fetch_elapsed += start.elapsed();
        }

        Ok(pages)
    }
}

/// Builds a parquet record batch stream driven directly by [ParquetPushDecoderBuilder].
pub(crate) fn build_sst_parquet_record_batch_stream(
    arrow_metadata: ArrowReaderMetadata,
    row_group_idx: usize,
    row_selection: Option<RowSelection>,
    projection: ProjectionMask,
    fetcher: SstParquetRangeFetcher,
    file_path: String,
) -> Result<BoxStream<'static, Result<RecordBatch>>> {
    let mut builder = ParquetPushDecoderBuilder::new_with_metadata(arrow_metadata)
        .with_row_groups(vec![row_group_idx])
        .with_projection(projection)
        .with_batch_size(DEFAULT_READ_BATCH_SIZE);

    if let Some(selection) = row_selection {
        builder = builder.with_row_selection(selection);
    }

    let mut decoder = builder
        .build()
        .context(ReadParquetSnafu { path: &file_path })?;

    Ok(async_stream::try_stream! {
        loop {
            match decoder.try_decode().context(ReadParquetSnafu { path: &file_path })? {
                DecodeResult::NeedsData(ranges) => {
                    let data = fetcher.fetch_bytes_with_cache(ranges.clone()).await?;
                    decoder
                        .push_ranges(ranges, data)
                        .context(ReadParquetSnafu { path: &file_path })?;
                }
                DecodeResult::Data(batch) => yield batch,
                DecodeResult::Finished => break,
            }
        }
    }
    .boxed())
}
