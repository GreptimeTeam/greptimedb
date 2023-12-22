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
use parquet::arrow::arrow_reader::{RowGroups, RowSelection};
use parquet::arrow::ProjectionMask;
use parquet::column::page::{PageIterator, PageReader};
use parquet::errors::{ParquetError, Result};
use parquet::file::metadata::{ParquetMetaData, RowGroupMetaData};
use parquet::file::reader::{ChunkReader, Length};
use parquet::file::serialized_reader::SerializedPageReader;
use parquet::format::PageLocation;
use store_api::storage::RegionId;

use crate::cache::{CacheManagerRef, PageKey, PageValue};
use crate::sst::file::FileId;
use crate::sst::parquet::page_reader::CachedPageReader;

/// An in-memory collection of column chunks
pub struct InMemoryRowGroup<'a> {
    metadata: &'a RowGroupMetaData,
    page_locations: Option<&'a [Vec<PageLocation>]>,
    column_chunks: Vec<Option<Arc<ColumnChunkData>>>,
    row_count: usize,
    region_id: RegionId,
    file_id: FileId,
    row_group_idx: usize,
    cache_manager: Option<CacheManagerRef>,
    /// Cached pages for each column.
    ///
    /// `column_cached_pages.len()` equals to `column_chunks.len()`.
    column_cached_pages: Vec<Option<Arc<PageValue>>>,
    file_path: &'a str,
    /// Object store.
    object_store: ObjectStore,
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
        cache_manager: Option<CacheManagerRef>,
        file_path: &'a str,
        object_store: ObjectStore,
    ) -> Self {
        let metadata = parquet_meta.row_group(row_group_idx);
        // `page_locations` is always `None` if we don't set
        // [with_page_index()](https://docs.rs/parquet/latest/parquet/arrow/arrow_reader/struct.ArrowReaderOptions.html#method.with_page_index)
        // to `true`.
        let page_locations = parquet_meta
            .offset_index()
            .map(|x| x[row_group_idx].as_slice());

        Self {
            metadata,
            row_count: metadata.num_rows() as usize,
            column_chunks: vec![None; metadata.columns().len()],
            page_locations,
            region_id,
            file_id,
            row_group_idx,
            cache_manager,
            column_cached_pages: vec![None; metadata.columns().len()],
            file_path,
            object_store,
        }
    }

    /// Fetches the necessary column data into memory
    pub async fn fetch(
        &mut self,
        projection: &ProjectionMask,
        selection: Option<&RowSelection>,
    ) -> Result<()> {
        if let Some((selection, page_locations)) = selection.zip(self.page_locations) {
            // If we have a `RowSelection` and an `OffsetIndex` then only fetch pages required for the
            // `RowSelection`
            let mut page_start_offsets: Vec<Vec<usize>> = vec![];

            let fetch_ranges: Vec<_> = self
                .column_chunks
                .iter()
                .zip(self.metadata.columns())
                .enumerate()
                .filter(|&(idx, (chunk, _chunk_meta))| {
                    chunk.is_none() && projection.leaf_included(idx)
                })
                .flat_map(|(idx, (_chunk, chunk_meta))| {
                    // If the first page does not start at the beginning of the column,
                    // then we need to also fetch a dictionary page.
                    let mut ranges = vec![];
                    let (start, _len) = chunk_meta.byte_range();
                    match page_locations[idx].first() {
                        Some(first) if first.offset as u64 != start => {
                            ranges.push(start as usize..first.offset as usize);
                        }
                        _ => (),
                    }

                    ranges.extend(selection.scan_ranges(&page_locations[idx]));
                    page_start_offsets.push(ranges.iter().map(|range| range.start).collect());

                    ranges
                })
                .collect();
            let mut chunk_data =
                fetch_byte_ranges(self.file_path, self.object_store.clone(), fetch_ranges)
                    .await?
                    .into_iter();

            let mut page_start_offsets = page_start_offsets.into_iter();

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
        } else {
            // Now we only use cache in dense chunk data.
            self.fetch_pages_from_cache(projection);

            let fetch_ranges: Vec<_> = self
                .column_chunks
                .iter()
                .zip(&self.column_cached_pages)
                .enumerate()
                // Don't need to fetch column data if we already cache the column's pages.
                .filter(|&(idx, (chunk, cached_pages))| {
                    chunk.is_none() && projection.leaf_included(idx) && cached_pages.is_none()
                })
                .map(|(idx, (_chunk, _cached_pages))| {
                    let column = self.metadata.column(idx);
                    let (start, length) = column.byte_range();
                    start as usize..(start + length) as usize
                })
                .collect();

            if fetch_ranges.is_empty() {
                // Nothing to fetch.
                return Ok(());
            }

            let mut chunk_data =
                fetch_byte_ranges(self.file_path, self.object_store.clone(), fetch_ranges)
                    .await?
                    .into_iter();

            for (idx, (chunk, cached_pages)) in self
                .column_chunks
                .iter_mut()
                .zip(&self.column_cached_pages)
                .enumerate()
            {
                if chunk.is_some() || !projection.leaf_included(idx) || cached_pages.is_some() {
                    continue;
                }

                if let Some(data) = chunk_data.next() {
                    *chunk = Some(Arc::new(ColumnChunkData::Dense {
                        offset: self.metadata.column(idx).byte_range().0 as usize,
                        data,
                    }));
                }
            }
        }

        Ok(())
    }

    /// Fetches pages for columns if cache is enabled.
    fn fetch_pages_from_cache(&mut self, projection: &ProjectionMask) {
        self.column_chunks
            .iter()
            .enumerate()
            .filter(|&(idx, chunk)| chunk.is_none() && projection.leaf_included(idx))
            .for_each(|(idx, _chunk)| {
                if let Some(cache) = &self.cache_manager {
                    let page_key = PageKey {
                        region_id: self.region_id,
                        file_id: self.file_id,
                        row_group_idx: self.row_group_idx,
                        column_idx: idx,
                    };
                    self.column_cached_pages[idx] = cache.get_pages(&page_key);
                }
            });
    }

    /// Creates a page reader to read column at `i`.
    fn column_page_reader(&self, i: usize) -> Result<Box<dyn PageReader>> {
        if let Some(cached_pages) = &self.column_cached_pages[i] {
            // Already in cache.
            return Ok(Box::new(CachedPageReader::new(&cached_pages.pages)));
        }

        // Cache miss.
        let page_reader = match &self.column_chunks[i] {
            None => {
                return Err(ParquetError::General(format!(
                    "Invalid column index {i}, column was not fetched"
                )))
            }
            Some(data) => {
                let page_locations = self.page_locations.map(|index| index[i].clone());
                SerializedPageReader::new(
                    data.clone(),
                    self.metadata.column(i),
                    self.row_count,
                    page_locations,
                )?
            }
        };

        let Some(cache) = &self.cache_manager else {
            // Cache is disabled.
            return Ok(Box::new(page_reader));
        };

        // We collect all pages and put them into the cache.
        let pages = page_reader.collect::<Result<Vec<_>>>()?;
        let page_value = Arc::new(PageValue::new(pages));
        let page_key = PageKey {
            region_id: self.region_id,
            file_id: self.file_id,
            row_group_idx: self.row_group_idx,
            column_idx: i,
        };
        cache.put_pages(page_key, page_value.clone());

        Ok(Box::new(CachedPageReader::new(&page_value.pages)))
    }
}

impl<'a> RowGroups for InMemoryRowGroup<'a> {
    fn num_rows(&self) -> usize {
        self.row_count
    }

    fn column_chunks(&self, i: usize) -> Result<Box<dyn PageIterator>> {
        let page_reader = self.column_page_reader(i)?;

        Ok(Box::new(ColumnChunkIterator {
            reader: Some(Ok(page_reader)),
        }))
    }
}

/// An in-memory column chunk
#[derive(Clone)]
enum ColumnChunkData {
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
struct ColumnChunkIterator {
    reader: Option<Result<Box<dyn PageReader>>>,
}

impl Iterator for ColumnChunkIterator {
    type Item = Result<Box<dyn PageReader>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.reader.take()
    }
}

impl PageIterator for ColumnChunkIterator {}

/// Fetches data from object store.
/// If the object store supports blocking, use sequence blocking read.
/// Otherwise, use concurrent read.
async fn fetch_byte_ranges(
    file_path: &str,
    object_store: ObjectStore,
    ranges: Vec<Range<usize>>,
) -> Result<Vec<Bytes>> {
    let ranges: Vec<_> = ranges
        .iter()
        .map(|range| range.start as u64..range.end as u64)
        .collect();
    if object_store.info().full_capability().blocking {
        fetch_ranges_seq(file_path, object_store, ranges).await
    } else {
        fetch_ranges_concurrent(file_path, object_store, ranges).await
    }
}

/// Fetches data from object store sequentially
async fn fetch_ranges_seq(
    file_path: &str,
    object_store: ObjectStore,
    ranges: Vec<Range<u64>>,
) -> Result<Vec<Bytes>> {
    let block_object_store = object_store.blocking();
    let file_path = file_path.to_string();

    let f = move || -> Result<Vec<Bytes>> {
        ranges
            .into_iter()
            .map(|range| {
                let data = block_object_store
                    .read_with(&file_path)
                    .range(range)
                    .call()
                    .map_err(|e| ParquetError::External(Box::new(e)))?;
                Ok::<_, ParquetError>(Bytes::from(data))
            })
            .collect::<Result<Vec<_>>>()
    };

    maybe_spawn_blocking(f).await
}

/// Fetches data from object store concurrently.
async fn fetch_ranges_concurrent(
    file_path: &str,
    object_store: ObjectStore,
    ranges: Vec<Range<u64>>,
) -> Result<Vec<Bytes>> {
    // TODO(QuenKar): may merge small ranges to a bigger range to optimize.
    let mut handles = Vec::with_capacity(ranges.len());
    for range in ranges {
        let future_read = object_store.read_with(file_path);
        handles.push(async move {
            let data = future_read
                .range(range.start..range.end)
                .await
                .map_err(|e| ParquetError::External(Box::new(e)))?;
            Ok::<_, ParquetError>(Bytes::from(data))
        });
    }
    let results = futures::future::try_join_all(handles).await?;
    Ok(results)
}

//  Port from https://github.com/apache/arrow-rs/blob/802ed428f87051fdca31180430ddb0ecb2f60e8b/object_store/src/util.rs#L74-L83
/// Takes a function and spawns it to a tokio blocking pool if available
pub async fn maybe_spawn_blocking<F, T>(f: F) -> Result<T>
where
    F: FnOnce() -> Result<T> + Send + 'static,
    T: Send + 'static,
{
    match tokio::runtime::Handle::try_current() {
        Ok(runtime) => runtime
            .spawn_blocking(f)
            .await
            .map_err(|e| ParquetError::External(Box::new(e)))?,
        Err(_) => f(),
    }
}
