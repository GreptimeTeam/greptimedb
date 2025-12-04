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

//! Ports private structs from [parquet crate](https://github.com/apache/arrow-rs/blob/7e134f4d277c0b62c27529fc15a4739de3ad0afd/parquet/src/arrow/async_reader/mod.rs#L644-L650).

use std::ops::Range;
use std::sync::Arc;

use bytes::{Buf, Bytes};
use object_store::ObjectStore;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{RowGroups, RowSelection};
use parquet::column::page::{PageIterator, PageReader};
use parquet::errors::{ParquetError, Result};
use parquet::file::metadata::{ParquetMetaData, RowGroupMetaData};
use parquet::file::page_index::offset_index::OffsetIndexMetaData;
use parquet::file::reader::{ChunkReader, Length};
use parquet::file::serialized_reader::SerializedPageReader;
use store_api::storage::{FileId, RegionId};
use tokio::task::yield_now;

use crate::cache::file_cache::{FileType, IndexKey};
use crate::cache::{CacheStrategy, PageKey, PageValue};
use crate::metrics::{READ_STAGE_ELAPSED, READ_STAGE_FETCH_PAGES};
use crate::sst::parquet::helper::{MERGE_GAP, fetch_byte_ranges};

/// Inner data for ParquetFetchMetrics.
#[derive(Default, Debug, Clone)]
pub struct ParquetFetchMetricsData {
    /// Number of page cache hits.
    pub page_cache_hit: usize,
    /// Number of write cache hits.
    pub write_cache_hit: usize,
    /// Number of cache misses.
    pub cache_miss: usize,
    /// Number of pages to fetch from mem cache.
    pub pages_to_fetch_mem: usize,
    /// Total size in bytes of pages to fetch from mem cache.
    pub page_size_to_fetch_mem: u64,
    /// Number of pages to fetch from write cache.
    pub pages_to_fetch_write_cache: usize,
    /// Total size in bytes of pages to fetch from write cache.
    pub page_size_to_fetch_write_cache: u64,
    /// Number of pages to fetch from store.
    pub pages_to_fetch_store: usize,
    /// Total size in bytes of pages to fetch from store.
    pub page_size_to_fetch_store: u64,
    /// Total size in bytes of pages actually returned.
    pub page_size_needed: u64,
    /// Elapsed time in microseconds fetching from write cache.
    pub write_cache_fetch_elapsed: u64,
    /// Elapsed time in microseconds fetching from object store.
    pub store_fetch_elapsed: u64,
    /// Total elapsed time in microseconds for fetching row groups.
    pub total_fetch_elapsed: u64,
}

impl ParquetFetchMetricsData {
    /// Returns true if the metrics are empty (contain no meaningful data).
    fn is_empty(&self) -> bool {
        self.total_fetch_elapsed == 0
    }
}

/// Metrics for tracking page/row group fetch operations.
#[derive(Default)]
pub struct ParquetFetchMetrics {
    pub data: std::sync::Mutex<ParquetFetchMetricsData>,
}

impl std::fmt::Debug for ParquetFetchMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let data = self.data.lock().unwrap();
        if data.is_empty() {
            return write!(f, "{{}}");
        }

        let ParquetFetchMetricsData {
            page_cache_hit,
            write_cache_hit,
            cache_miss,
            pages_to_fetch_mem,
            page_size_to_fetch_mem,
            pages_to_fetch_write_cache,
            page_size_to_fetch_write_cache,
            pages_to_fetch_store,
            page_size_to_fetch_store,
            page_size_needed,
            write_cache_fetch_elapsed,
            store_fetch_elapsed,
            total_fetch_elapsed,
        } = *data;

        write!(f, "{{")?;

        write!(
            f,
            "\"total_fetch_elapsed\":\"{:?}\"",
            std::time::Duration::from_micros(total_fetch_elapsed)
        )?;

        if page_cache_hit > 0 {
            write!(f, ", \"page_cache_hit\":{}", page_cache_hit)?;
        }
        if write_cache_hit > 0 {
            write!(f, ", \"write_cache_hit\":{}", write_cache_hit)?;
        }
        if cache_miss > 0 {
            write!(f, ", \"cache_miss\":{}", cache_miss)?;
        }
        if pages_to_fetch_mem > 0 {
            write!(f, ", \"pages_to_fetch_mem\":{}", pages_to_fetch_mem)?;
        }
        if page_size_to_fetch_mem > 0 {
            write!(f, ", \"page_size_to_fetch_mem\":{}", page_size_to_fetch_mem)?;
        }
        if pages_to_fetch_write_cache > 0 {
            write!(
                f,
                ", \"pages_to_fetch_write_cache\":{}",
                pages_to_fetch_write_cache
            )?;
        }
        if page_size_to_fetch_write_cache > 0 {
            write!(
                f,
                ", \"page_size_to_fetch_write_cache\":{}",
                page_size_to_fetch_write_cache
            )?;
        }
        if pages_to_fetch_store > 0 {
            write!(f, ", \"pages_to_fetch_store\":{}", pages_to_fetch_store)?;
        }
        if page_size_to_fetch_store > 0 {
            write!(
                f,
                ", \"page_size_to_fetch_store\":{}",
                page_size_to_fetch_store
            )?;
        }
        if page_size_needed > 0 {
            write!(f, ", \"page_size_needed\":{}", page_size_needed)?;
        }
        if write_cache_fetch_elapsed > 0 {
            write!(
                f,
                ", \"write_cache_fetch_elapsed\":\"{:?}\"",
                std::time::Duration::from_micros(write_cache_fetch_elapsed)
            )?;
        }
        if store_fetch_elapsed > 0 {
            write!(
                f,
                ", \"store_fetch_elapsed\":\"{:?}\"",
                std::time::Duration::from_micros(store_fetch_elapsed)
            )?;
        }

        write!(f, "}}")
    }
}

impl ParquetFetchMetrics {
    /// Returns true if the metrics are empty (contain no meaningful data).
    pub fn is_empty(&self) -> bool {
        self.data.lock().unwrap().is_empty()
    }

    /// Merges metrics from another [ParquetFetchMetrics].
    pub fn merge_from(&self, other: &ParquetFetchMetrics) {
        let ParquetFetchMetricsData {
            page_cache_hit,
            write_cache_hit,
            cache_miss,
            pages_to_fetch_mem,
            page_size_to_fetch_mem,
            pages_to_fetch_write_cache,
            page_size_to_fetch_write_cache,
            pages_to_fetch_store,
            page_size_to_fetch_store,
            page_size_needed,
            write_cache_fetch_elapsed,
            store_fetch_elapsed,
            total_fetch_elapsed,
        } = *other.data.lock().unwrap();

        let mut data = self.data.lock().unwrap();
        data.page_cache_hit += page_cache_hit;
        data.write_cache_hit += write_cache_hit;
        data.cache_miss += cache_miss;
        data.pages_to_fetch_mem += pages_to_fetch_mem;
        data.page_size_to_fetch_mem += page_size_to_fetch_mem;
        data.pages_to_fetch_write_cache += pages_to_fetch_write_cache;
        data.page_size_to_fetch_write_cache += page_size_to_fetch_write_cache;
        data.pages_to_fetch_store += pages_to_fetch_store;
        data.page_size_to_fetch_store += page_size_to_fetch_store;
        data.page_size_needed += page_size_needed;
        data.write_cache_fetch_elapsed += write_cache_fetch_elapsed;
        data.store_fetch_elapsed += store_fetch_elapsed;
        data.total_fetch_elapsed += total_fetch_elapsed;
    }
}

pub(crate) struct RowGroupBase<'a> {
    metadata: &'a RowGroupMetaData,
    pub(crate) offset_index: Option<&'a [OffsetIndexMetaData]>,
    /// Compressed page of each column.
    column_chunks: Vec<Option<Arc<ColumnChunkData>>>,
    pub(crate) row_count: usize,
}

impl<'a> RowGroupBase<'a> {
    pub(crate) fn new(parquet_meta: &'a ParquetMetaData, row_group_idx: usize) -> Self {
        let metadata = parquet_meta.row_group(row_group_idx);
        // `offset_index` is always `None` if we don't set
        // [with_page_index()](https://docs.rs/parquet/latest/parquet/arrow/arrow_reader/struct.ArrowReaderOptions.html#method.with_page_index)
        // to `true`.
        let offset_index = parquet_meta
            .offset_index()
            // filter out empty offset indexes (old versions specified Some(vec![]) when no present)
            .filter(|index| !index.is_empty())
            .map(|x| x[row_group_idx].as_slice());

        Self {
            metadata,
            offset_index,
            column_chunks: vec![None; metadata.columns().len()],
            row_count: metadata.num_rows() as usize,
        }
    }

    pub(crate) fn calc_sparse_read_ranges(
        &self,
        projection: &ProjectionMask,
        offset_index: &[OffsetIndexMetaData],
        selection: &RowSelection,
    ) -> (Vec<Range<u64>>, Vec<Vec<usize>>) {
        // If we have a `RowSelection` and an `OffsetIndex` then only fetch pages required for the
        // `RowSelection`
        let mut page_start_offsets: Vec<Vec<usize>> = vec![];
        let ranges = self
            .column_chunks
            .iter()
            .zip(self.metadata.columns())
            .enumerate()
            .filter(|&(idx, (chunk, _chunk_meta))| chunk.is_none() && projection.leaf_included(idx))
            .flat_map(|(idx, (_chunk, chunk_meta))| {
                // If the first page does not start at the beginning of the column,
                // then we need to also fetch a dictionary page.
                let mut ranges = vec![];
                let (start, _len) = chunk_meta.byte_range();
                match offset_index[idx].page_locations.first() {
                    Some(first) if first.offset as u64 != start => {
                        ranges.push(start..first.offset as u64);
                    }
                    _ => (),
                }

                ranges.extend(
                    selection
                        .scan_ranges(&offset_index[idx].page_locations)
                        .iter()
                        .map(|range| range.start..range.end),
                );
                page_start_offsets.push(ranges.iter().map(|range| range.start as usize).collect());

                ranges
            })
            .collect::<Vec<_>>();
        (ranges, page_start_offsets)
    }

    pub(crate) fn assign_sparse_chunk(
        &mut self,
        projection: &ProjectionMask,
        data: Vec<Bytes>,
        page_start_offsets: Vec<Vec<usize>>,
    ) {
        let mut page_start_offsets = page_start_offsets.into_iter();
        let mut chunk_data = data.into_iter();

        for (idx, chunk) in self.column_chunks.iter_mut().enumerate() {
            if chunk.is_some() || !projection.leaf_included(idx) {
                continue;
            }

            if let Some(offsets) = page_start_offsets.next() {
                let mut chunks = Vec::with_capacity(offsets.len());
                for _ in 0..offsets.len() {
                    chunks.push(chunk_data.next().unwrap());
                }

                *chunk = Some(Arc::new(ColumnChunkData::Sparse {
                    length: self.metadata.column(idx).byte_range().1 as usize,
                    data: offsets.into_iter().zip(chunks).collect(),
                }))
            }
        }
    }

    pub(crate) fn calc_dense_read_ranges(&self, projection: &ProjectionMask) -> Vec<Range<u64>> {
        self.column_chunks
            .iter()
            .enumerate()
            .filter(|&(idx, chunk)| chunk.is_none() && projection.leaf_included(idx))
            .map(|(idx, _chunk)| {
                let column = self.metadata.column(idx);
                let (start, length) = column.byte_range();
                start..(start + length)
            })
            .collect::<Vec<_>>()
    }

    /// Assigns compressed chunk binary data to [RowGroupBase::column_chunks]
    /// and returns the chunk offset and binary data assigned.
    pub(crate) fn assign_dense_chunk(
        &mut self,
        projection: &ProjectionMask,
        chunk_data: Vec<Bytes>,
    ) {
        let mut chunk_data = chunk_data.into_iter();

        for (idx, chunk) in self.column_chunks.iter_mut().enumerate() {
            if chunk.is_some() || !projection.leaf_included(idx) {
                continue;
            }

            // Get the fetched page.
            let Some(data) = chunk_data.next() else {
                continue;
            };

            let column = self.metadata.column(idx);
            *chunk = Some(Arc::new(ColumnChunkData::Dense {
                offset: column.byte_range().0 as usize,
                data,
            }));
        }
    }

    /// Create [PageReader] from [RowGroupBase::column_chunks]
    pub(crate) fn column_reader(
        &self,
        col_idx: usize,
    ) -> Result<SerializedPageReader<ColumnChunkData>> {
        let page_reader = match &self.column_chunks[col_idx] {
            None => {
                return Err(ParquetError::General(format!(
                    "Invalid column index {col_idx}, column was not fetched"
                )));
            }
            Some(data) => {
                let page_locations = self
                    .offset_index
                    // filter out empty offset indexes (old versions specified Some(vec![]) when no present)
                    .filter(|index| !index.is_empty())
                    .map(|index| index[col_idx].page_locations.clone());
                SerializedPageReader::new(
                    data.clone(),
                    self.metadata.column(col_idx),
                    self.row_count,
                    page_locations,
                )?
            }
        };

        Ok(page_reader)
    }
}

/// An in-memory collection of column chunks
pub struct InMemoryRowGroup<'a> {
    region_id: RegionId,
    file_id: FileId,
    row_group_idx: usize,
    cache_strategy: CacheStrategy,
    file_path: &'a str,
    /// Object store.
    object_store: ObjectStore,
    base: RowGroupBase<'a>,
}

impl<'a> InMemoryRowGroup<'a> {
    /// Creates a new [InMemoryRowGroup] by `row_group_idx`.
    ///
    /// # Panics
    /// Panics if the `row_group_idx` is invalid.
    pub fn create(
        region_id: RegionId,
        file_id: FileId,
        parquet_meta: &'a ParquetMetaData,
        row_group_idx: usize,
        cache_strategy: CacheStrategy,
        file_path: &'a str,
        object_store: ObjectStore,
    ) -> Self {
        Self {
            region_id,
            file_id,
            row_group_idx,
            cache_strategy,
            file_path,
            object_store,
            base: RowGroupBase::new(parquet_meta, row_group_idx),
        }
    }

    /// Fetches the necessary column data into memory
    pub async fn fetch(
        &mut self,
        projection: &ProjectionMask,
        selection: Option<&RowSelection>,
        metrics: Option<&ParquetFetchMetrics>,
    ) -> Result<()> {
        if let Some((selection, offset_index)) = selection.zip(self.base.offset_index) {
            let (fetch_ranges, page_start_offsets) =
                self.base
                    .calc_sparse_read_ranges(projection, offset_index, selection);

            let chunk_data = self.fetch_bytes(&fetch_ranges, metrics).await?;
            // Assign sparse chunk data to base.
            self.base
                .assign_sparse_chunk(projection, chunk_data, page_start_offsets);
        } else {
            // Release the CPU to avoid blocking the runtime. Since `fetch_pages_from_cache`
            // is a synchronous, CPU-bound operation.
            yield_now().await;

            // Calculate ranges to read.
            let fetch_ranges = self.base.calc_dense_read_ranges(projection);

            if fetch_ranges.is_empty() {
                // Nothing to fetch.
                return Ok(());
            }

            // Fetch data with ranges
            let chunk_data = self.fetch_bytes(&fetch_ranges, metrics).await?;

            // Assigns fetched data to base.
            self.base.assign_dense_chunk(projection, chunk_data);
        }

        Ok(())
    }

    /// Try to fetch data from the memory cache or the WriteCache,
    /// if not in WriteCache, fetch data from object store directly.
    async fn fetch_bytes(
        &self,
        ranges: &[Range<u64>],
        metrics: Option<&ParquetFetchMetrics>,
    ) -> Result<Vec<Bytes>> {
        // Now fetch page timer includes the whole time to read pages.
        let _timer = READ_STAGE_FETCH_PAGES.start_timer();

        let page_key = PageKey::new(self.file_id, self.row_group_idx, ranges.to_vec());
        if let Some(pages) = self.cache_strategy.get_pages(&page_key) {
            if let Some(metrics) = metrics {
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
        let (total_range_size, unaligned_size) = compute_total_range_size(ranges);

        let key = IndexKey::new(self.region_id, self.file_id, FileType::Parquet);
        let fetch_write_cache_start = metrics.map(|_| std::time::Instant::now());
        let write_cache_result = self.fetch_ranges_from_write_cache(key, ranges).await;
        let pages = match write_cache_result {
            Some(data) => {
                if let Some(metrics) = metrics {
                    let elapsed = fetch_write_cache_start
                        .map(|start| start.elapsed().as_micros() as u64)
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

                let start = metrics.map(|_| std::time::Instant::now());
                let data = fetch_byte_ranges(self.file_path, self.object_store.clone(), ranges)
                    .await
                    .map_err(|e| ParquetError::External(Box::new(e)))?;
                if let Some(metrics) = metrics {
                    let elapsed = start
                        .map(|start| start.elapsed().as_micros() as u64)
                        .unwrap_or_default();
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

/// Computes the max possible buffer size to read the given `ranges`.
/// Returns (aligned_size, unaligned_size) where:
/// - aligned_size: total size aligned to pooled buffer size
/// - unaligned_size: actual total size without alignment
// See https://github.com/apache/opendal/blob/v0.54.0/core/src/types/read/reader.rs#L166-L192
fn compute_total_range_size(ranges: &[Range<u64>]) -> (u64, u64) {
    if ranges.is_empty() {
        return (0, 0);
    }

    let gap = MERGE_GAP as u64;
    let mut sorted_ranges = ranges.to_vec();
    sorted_ranges.sort_unstable_by(|a, b| a.start.cmp(&b.start));

    let mut total_size_aligned = 0;
    let mut total_size_unaligned = 0;
    let mut cur = sorted_ranges[0].clone();

    for range in sorted_ranges.into_iter().skip(1) {
        if range.start <= cur.end + gap {
            // There is an overlap or the gap is small enough to merge
            cur.end = cur.end.max(range.end);
        } else {
            // No overlap and the gap is too large, add current range to total and start a new one
            let range_size = cur.end - cur.start;
            total_size_aligned += align_to_pooled_buf_size(range_size);
            total_size_unaligned += range_size;
            cur = range;
        }
    }

    // Add the last range
    let range_size = cur.end - cur.start;
    total_size_aligned += align_to_pooled_buf_size(range_size);
    total_size_unaligned += range_size;

    (total_size_aligned, total_size_unaligned)
}

/// Aligns the given size to the multiple of the pooled buffer size.
// See:
// - https://github.com/apache/opendal/blob/v0.54.0/core/src/services/fs/backend.rs#L178
// - https://github.com/apache/opendal/blob/v0.54.0/core/src/services/fs/reader.rs#L36-L46
fn align_to_pooled_buf_size(size: u64) -> u64 {
    const POOLED_BUF_SIZE: u64 = 2 * 1024 * 1024;
    size.div_ceil(POOLED_BUF_SIZE) * POOLED_BUF_SIZE
}

impl RowGroups for InMemoryRowGroup<'_> {
    fn num_rows(&self) -> usize {
        self.base.row_count
    }

    fn column_chunks(&self, i: usize) -> Result<Box<dyn PageIterator>> {
        // Creates a page reader to read column at `i`.
        let page_reader = self.base.column_reader(i)?;

        Ok(Box::new(ColumnChunkIterator {
            reader: Some(Ok(Box::new(page_reader))),
        }))
    }
}

/// An in-memory column chunk
#[derive(Clone)]
pub(crate) enum ColumnChunkData {
    /// Column chunk data representing only a subset of data pages
    Sparse {
        /// Length of the full column chunk
        length: usize,
        /// Set of data pages included in this sparse chunk. Each element is a tuple
        /// of (page offset, page data)
        data: Vec<(usize, Bytes)>,
    },
    /// Full column chunk and its offset
    Dense { offset: usize, data: Bytes },
}

impl ColumnChunkData {
    fn get(&self, start: u64) -> Result<Bytes> {
        match &self {
            ColumnChunkData::Sparse { data, .. } => data
                .binary_search_by_key(&start, |(offset, _)| *offset as u64)
                .map(|idx| data[idx].1.clone())
                .map_err(|_| {
                    ParquetError::General(format!(
                        "Invalid offset in sparse column chunk data: {start}"
                    ))
                }),
            ColumnChunkData::Dense { offset, data } => {
                let start = start as usize - *offset;
                Ok(data.slice(start..))
            }
        }
    }
}

impl Length for ColumnChunkData {
    fn len(&self) -> u64 {
        match &self {
            ColumnChunkData::Sparse { length, .. } => *length as u64,
            ColumnChunkData::Dense { data, .. } => data.len() as u64,
        }
    }
}

impl ChunkReader for ColumnChunkData {
    type T = bytes::buf::Reader<Bytes>;

    fn get_read(&self, start: u64) -> Result<Self::T> {
        Ok(self.get(start)?.reader())
    }

    fn get_bytes(&self, start: u64, length: usize) -> Result<Bytes> {
        Ok(self.get(start)?.slice(..length))
    }
}

/// Implements [`PageIterator`] for a single column chunk, yielding a single [`PageReader`]
pub(crate) struct ColumnChunkIterator {
    pub(crate) reader: Option<Result<Box<dyn PageReader>>>,
}

impl Iterator for ColumnChunkIterator {
    type Item = Result<Box<dyn PageReader>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.reader.take()
    }
}

impl PageIterator for ColumnChunkIterator {}
