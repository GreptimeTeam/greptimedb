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

//! Async file reader implementation for SST parquet files.

use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use futures::FutureExt;
use futures::future::BoxFuture;
use object_store::ObjectStore;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::errors::{ParquetError, Result as ParquetResult};
use parquet::file::metadata::ParquetMetaData;
use store_api::storage::{FileId, RegionId};

use crate::cache::file_cache::{FileType, IndexKey};
use crate::cache::{CacheStrategy, PageKey, PageValue};
use crate::metrics::{READ_STAGE_ELAPSED, READ_STAGE_FETCH_PAGES};
use crate::sst::parquet::helper::fetch_byte_ranges;
use crate::sst::parquet::row_group::{ParquetFetchMetrics, compute_total_range_size};

/// An [AsyncFileReader] implementation for SST parquet files.
///
/// This reader provides async byte access to parquet data in object storage,
/// with caching support (page cache and write cache).
pub struct SstAsyncFileReader {
    /// Region ID for cache key.
    region_id: RegionId,
    /// File ID for cache key.
    file_id: FileId,
    /// Path to the parquet file in object storage.
    file_path: String,
    /// Object store for reading data.
    object_store: ObjectStore,
    /// Cache strategy for reading pages.
    cache_strategy: CacheStrategy,
    /// Cached parquet metadata.
    metadata: Arc<ParquetMetaData>,
    /// Row group index for cache key (set before reading a row group).
    row_group_idx: usize,
    /// Optional metrics for tracking fetch operations.
    fetch_metrics: Option<ParquetFetchMetrics>,
}

impl SstAsyncFileReader {
    /// Creates a new [SstAsyncFileReader].
    pub fn new(
        region_id: RegionId,
        file_id: FileId,
        file_path: String,
        object_store: ObjectStore,
        cache_strategy: CacheStrategy,
        metadata: Arc<ParquetMetaData>,
    ) -> Self {
        Self {
            region_id,
            file_id,
            file_path,
            object_store,
            cache_strategy,
            metadata,
            row_group_idx: 0,
            fetch_metrics: None,
        }
    }

    /// Sets the row group index for cache key.
    pub fn with_row_group_idx(mut self, row_group_idx: usize) -> Self {
        self.row_group_idx = row_group_idx;
        self
    }

    /// Sets the fetch metrics.
    pub fn with_fetch_metrics(mut self, metrics: Option<ParquetFetchMetrics>) -> Self {
        self.fetch_metrics = metrics;
        self
    }

    /// Fetches byte ranges with caching support.
    ///
    /// This method:
    /// 1. Checks page cache first
    /// 2. Falls back to write cache if available
    /// 3. Falls back to object store
    /// 4. Stores fetched data in page cache
    async fn fetch_bytes_with_cache(&self, ranges: Vec<Range<u64>>) -> ParquetResult<Vec<Bytes>> {
        let _timer = READ_STAGE_FETCH_PAGES.start_timer();

        let page_key = PageKey::new(self.file_id, self.row_group_idx, ranges.clone());

        // Check page cache first.
        if let Some(pages) = self.cache_strategy.get_pages(&page_key) {
            if let Some(metrics) = &self.fetch_metrics {
                let total_size: u64 = ranges.iter().map(|r| r.end - r.start).sum();
                let mut metrics_data = metrics.data.lock().unwrap();
                metrics_data.page_cache_hit += 1;
                metrics_data.pages_to_fetch_mem += ranges.len();
                metrics_data.page_size_to_fetch_mem += total_size;
                metrics_data.page_size_needed += total_size;
            }
            return Ok(pages.compressed.clone());
        }

        // Calculate total range size for metrics.
        let (total_range_size, unaligned_size) = compute_total_range_size(&ranges);

        // Check write cache.
        let key = IndexKey::new(self.region_id, self.file_id, FileType::Parquet);
        let fetch_write_cache_start = self
            .fetch_metrics
            .as_ref()
            .map(|_| std::time::Instant::now());
        let write_cache_result = self.fetch_ranges_from_write_cache(key, &ranges).await;

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
                    .map_err(|e| ParquetError::External(Box::new(e)))?;

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

        Ok(pages)
    }

    /// Fetches data from write cache.
    /// Returns `None` if the data is not in the cache.
    async fn fetch_ranges_from_write_cache(
        &self,
        key: IndexKey,
        ranges: &[Range<u64>],
    ) -> Option<Vec<Bytes>> {
        if let Some(cache) = self.cache_strategy.write_cache() {
            return cache.file_cache().read_ranges(key, ranges).await;
        }
        None
    }
}

impl AsyncFileReader for SstAsyncFileReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, ParquetResult<Bytes>> {
        async move {
            let mut result = self.fetch_bytes_with_cache(vec![range]).await?;
            Ok(result.pop().unwrap_or_default())
        }
        .boxed()
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, ParquetResult<Vec<Bytes>>> {
        async move { self.fetch_bytes_with_cache(ranges).await }.boxed()
    }

    fn get_metadata(
        &mut self,
        _options: Option<&parquet::arrow::arrow_reader::ArrowReaderOptions>,
    ) -> BoxFuture<'_, ParquetResult<Arc<ParquetMetaData>>> {
        // Metadata is already cached, return it immediately.
        std::future::ready(Ok(self.metadata.clone())).boxed()
    }
}
