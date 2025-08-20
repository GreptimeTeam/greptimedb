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
use parquet::file::metadata::{ColumnChunkMetaData, ParquetMetaData, RowGroupMetaData};
use parquet::file::page_index::offset_index::OffsetIndexMetaData;
use parquet::file::properties::DEFAULT_PAGE_SIZE;
use parquet::file::reader::{ChunkReader, Length};
use parquet::file::serialized_reader::SerializedPageReader;
use store_api::storage::RegionId;
use tokio::task::yield_now;

use crate::cache::file_cache::{FileType, IndexKey};
use crate::cache::{CacheStrategy, PageKey, PageValue};
use crate::metrics::{READ_STAGE_ELAPSED, READ_STAGE_FETCH_PAGES};
use crate::sst::file::FileId;
use crate::sst::parquet::helper::fetch_byte_ranges;
use crate::sst::parquet::page_reader::RowGroupCachedReader;

pub(crate) struct RowGroupBase<'a> {
    metadata: &'a RowGroupMetaData,
    pub(crate) offset_index: Option<&'a [OffsetIndexMetaData]>,
    /// Compressed page of each column.
    column_chunks: Vec<Option<Arc<ColumnChunkData>>>,
    pub(crate) row_count: usize,
    /// Row group level cached pages for each column.
    ///
    /// These pages are uncompressed pages of a row group.
    /// `column_uncompressed_pages.len()` equals to `column_chunks.len()`.
    column_uncompressed_pages: Vec<Option<Arc<PageValue>>>,
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
            column_uncompressed_pages: vec![None; metadata.columns().len()],
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
            .zip(&self.column_uncompressed_pages)
            .enumerate()
            .filter(|&(idx, (chunk, uncompressed_pages))| {
                // Don't need to fetch column data if we already cache the column's pages.
                chunk.is_none() && projection.leaf_included(idx) && uncompressed_pages.is_none()
            })
            .map(|(idx, (_chunk, _pages))| {
                let column = self.metadata.column(idx);
                let (start, length) = column.byte_range();
                start..(start + length)
            })
            .collect::<Vec<_>>()
    }

    /// Assigns uncompressed chunk binary data to [RowGroupBase::column_chunks]
    /// and returns the chunk offset and binary data assigned.
    pub(crate) fn assign_dense_chunk(
        &mut self,
        projection: &ProjectionMask,
        chunk_data: Vec<Bytes>,
    ) -> Vec<(usize, Bytes)> {
        let mut chunk_data = chunk_data.into_iter();
        let mut res = vec![];

        for (idx, (chunk, row_group_pages)) in self
            .column_chunks
            .iter_mut()
            .zip(&self.column_uncompressed_pages)
            .enumerate()
        {
            if chunk.is_some() || !projection.leaf_included(idx) || row_group_pages.is_some() {
                continue;
            }

            // Get the fetched page.
            let Some(data) = chunk_data.next() else {
                continue;
            };

            let column = self.metadata.column(idx);
            res.push((idx, data.clone()));
            *chunk = Some(Arc::new(ColumnChunkData::Dense {
                offset: column.byte_range().0 as usize,
                data,
            }));
        }
        res
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
                )))
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

        // This column don't cache uncompressed pages.
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
    ) -> Result<()> {
        if let Some((selection, offset_index)) = selection.zip(self.base.offset_index) {
            let (fetch_ranges, page_start_offsets) =
                self.base
                    .calc_sparse_read_ranges(projection, offset_index, selection);

            let chunk_data = self.fetch_bytes(&fetch_ranges).await?;
            // Assign sparse chunk data to base.
            self.base
                .assign_sparse_chunk(projection, chunk_data, page_start_offsets);
        } else {
            // Now we only use cache in dense chunk data.
            self.fetch_pages_from_cache(projection);

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
            let chunk_data = self.fetch_bytes(&fetch_ranges).await?;

            // Assigns fetched data to base.
            let assigned_columns = self.base.assign_dense_chunk(projection, chunk_data);

            // Put fetched data to cache if necessary.
            for (col_idx, data) in assigned_columns {
                let column = self.base.metadata.column(col_idx);
                if !cache_uncompressed_pages(column) {
                    // For columns that have multiple uncompressed pages, we only cache the compressed page
                    // to save memory.
                    let page_key = PageKey::new_compressed(
                        self.region_id,
                        self.file_id,
                        self.row_group_idx,
                        col_idx,
                    );
                    self.cache_strategy
                        .put_pages(page_key, Arc::new(PageValue::new_compressed(data.clone())));
                }
            }
        }

        Ok(())
    }

    /// Fetches pages for columns if cache is enabled.
    /// If the page is in the cache, sets the column chunk or `column_uncompressed_pages` for the column.
    fn fetch_pages_from_cache(&mut self, projection: &ProjectionMask) {
        let _timer = READ_STAGE_FETCH_PAGES.start_timer();
        self.base
            .column_chunks
            .iter_mut()
            .enumerate()
            .filter(|(idx, chunk)| chunk.is_none() && projection.leaf_included(*idx))
            .for_each(|(idx, chunk)| {
                let column = self.base.metadata.column(idx);
                if cache_uncompressed_pages(column) {
                    // Fetches uncompressed pages for the row group.
                    let page_key = PageKey::new_uncompressed(
                        self.region_id,
                        self.file_id,
                        self.row_group_idx,
                        idx,
                    );
                    self.base.column_uncompressed_pages[idx] =
                        self.cache_strategy.get_pages(&page_key);
                } else {
                    // Fetches the compressed page from the cache.
                    let page_key = PageKey::new_compressed(
                        self.region_id,
                        self.file_id,
                        self.row_group_idx,
                        idx,
                    );

                    *chunk = self.cache_strategy.get_pages(&page_key).map(|page_value| {
                        Arc::new(ColumnChunkData::Dense {
                            offset: column.byte_range().0 as usize,
                            data: page_value.compressed.clone(),
                        })
                    });
                }
            });
    }

    /// Try to fetch data from WriteCache,
    /// if not in WriteCache, fetch data from object store directly.
    async fn fetch_bytes(&self, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        let key = IndexKey::new(self.region_id, self.file_id, FileType::Parquet);
        match self.fetch_ranges_from_write_cache(key, ranges).await {
            Some(data) => Ok(data),
            None => {
                // Fetch data from object store.
                let _timer = READ_STAGE_ELAPSED
                    .with_label_values(&["cache_miss_read"])
                    .start_timer();
                let data = fetch_byte_ranges(self.file_path, self.object_store.clone(), ranges)
                    .await
                    .map_err(|e| ParquetError::External(Box::new(e)))?;
                Ok(data)
            }
        }
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

    /// Creates a page reader to read column at `i`.
    fn column_page_reader(&self, i: usize) -> Result<Box<dyn PageReader>> {
        if let Some(cached_pages) = &self.base.column_uncompressed_pages[i] {
            debug_assert!(!cached_pages.row_group.is_empty());
            // Hits the row group level page cache.
            return Ok(Box::new(RowGroupCachedReader::new(&cached_pages.row_group)));
        }

        let page_reader = self.base.column_reader(i)?;

        let column = self.base.metadata.column(i);
        if cache_uncompressed_pages(column) {
            // This column use row group level page cache.
            // We collect all pages and put them into the cache.
            let pages = page_reader.collect::<Result<Vec<_>>>()?;
            let page_value = Arc::new(PageValue::new_row_group(pages));
            let page_key =
                PageKey::new_uncompressed(self.region_id, self.file_id, self.row_group_idx, i);
            self.cache_strategy.put_pages(page_key, page_value.clone());

            return Ok(Box::new(RowGroupCachedReader::new(&page_value.row_group)));
        }

        // This column don't cache uncompressed pages.
        Ok(Box::new(page_reader))
    }
}

/// Returns whether we cache uncompressed pages for the column.
fn cache_uncompressed_pages(column: &ColumnChunkMetaData) -> bool {
    // If the row group only has a data page, cache the whole row group as
    // it might be faster than caching a compressed page.
    column.uncompressed_size() as usize <= DEFAULT_PAGE_SIZE
}

impl RowGroups for InMemoryRowGroup<'_> {
    fn num_rows(&self) -> usize {
        self.base.row_count
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
