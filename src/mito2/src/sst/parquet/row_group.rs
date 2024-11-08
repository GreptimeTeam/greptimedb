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
use parquet::arrow::arrow_reader::{RowGroups, RowSelection};
use parquet::arrow::ProjectionMask;
use parquet::column::page::{PageIterator, PageReader};
use parquet::errors::{ParquetError, Result};
use parquet::file::metadata::{ColumnChunkMetaData, ParquetMetaData, RowGroupMetaData};
use parquet::file::properties::DEFAULT_PAGE_SIZE;
use parquet::file::reader::{ChunkReader, Length};
use parquet::file::serialized_reader::SerializedPageReader;
use parquet::format::PageLocation;
use tokio::task::yield_now;

use crate::cache::{PageKey, PageValue};
use crate::metrics::READ_STAGE_FETCH_PAGES;
use crate::sst::parquet::page_reader::RowGroupCachedReader;
use crate::sst::parquet::reader::Location;

/// Location of row group.
struct RowGroupLocation<'a> {
    file_location: &'a Location,
    row_group_idx: usize,
}

impl<'a> RowGroupLocation<'a> {
    /// Fetches bytes from given ranges.
    async fn fetch_bytes(&self, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        self.file_location.fetch_bytes(ranges).await
    }

    /// Returns true if cache manager is enabled for given [RowGroupLocation].
    fn has_cache_manager(&self) -> bool {
        self.file_location.cache_manager().is_some()
    }

    /// Gets page value of given column index in row group.
    fn get_cache(&self, col_idx: usize, compressed: bool) -> Option<Arc<PageValue>> {
        let Location::Sst(sst) = &self.file_location else {
            return None;
        };
        let cache = sst.cache_manager.as_ref()?;
        let file_id = sst.file_handle.file_id();
        let region_id = sst.file_handle.region_id();
        let page_key = if compressed {
            PageKey::new_compressed(region_id, file_id, self.row_group_idx, col_idx)
        } else {
            PageKey::new_uncompressed(region_id, file_id, self.row_group_idx, col_idx)
        };
        cache.get_pages(&page_key)
    }

    /// Puts compressed pages to page cache.
    fn put_compressed_to_cache(&self, col_idx: usize, data: Bytes) {
        let Location::Sst(sst) = &self.file_location else {
            return;
        };
        let Some(cache) = &sst.cache_manager else {
            return;
        };
        // For columns that have multiple uncompressed pages, we only cache the compressed page
        // to save memory.
        cache.put_pages(
            PageKey::new_compressed(
                sst.file_handle.region_id(),
                sst.file_handle.file_id(),
                self.row_group_idx,
                col_idx,
            ),
            Arc::new(PageValue::new_compressed(data.clone())),
        );
    }

    /// Puts uncompressed pages to page cache.
    fn put_uncompressed_to_cache(&self, col_idx: usize, data: Arc<PageValue>) {
        let Location::Sst(sst) = &self.file_location else {
            return;
        };
        let Some(cache) = &sst.cache_manager else {
            return;
        };
        cache.put_pages(
            PageKey::new_uncompressed(
                sst.file_handle.region_id(),
                sst.file_handle.file_id(),
                self.row_group_idx,
                col_idx,
            ),
            data,
        );
    }
}

/// An in-memory collection of column chunks
pub struct InMemoryRowGroup<'a> {
    metadata: &'a RowGroupMetaData,
    page_locations: Option<&'a [Vec<PageLocation>]>,
    /// Compressed page of each column.
    column_chunks: Vec<Option<Arc<ColumnChunkData>>>,
    row_count: usize,
    /// Row group level cached pages for each column.
    ///
    /// These pages are uncompressed pages of a row group.
    /// `column_uncompressed_pages.len()` equals to `column_chunks.len()`.
    column_uncompressed_pages: Vec<Option<Arc<PageValue>>>,
    location: RowGroupLocation<'a>,
}

impl<'a> InMemoryRowGroup<'a> {
    /// Creates a new [InMemoryRowGroup] with given file location.
    pub fn create(
        row_group_idx: usize,
        parquet_meta: &'a ParquetMetaData,
        file_location: &'a Location,
    ) -> Self {
        let metadata = parquet_meta.row_group(row_group_idx);
        let row_count = metadata.num_rows() as usize;
        let page_locations = parquet_meta
            .offset_index()
            .map(|x| x[row_group_idx].as_slice());

        Self {
            metadata,
            page_locations,
            column_chunks: vec![None; metadata.columns().len()],
            row_count,
            column_uncompressed_pages: vec![None; metadata.columns().len()],
            location: RowGroupLocation {
                row_group_idx,
                file_location,
            },
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

            let fetch_ranges = self
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
                            ranges.push(start..first.offset as u64);
                        }
                        _ => (),
                    }

                    ranges.extend(
                        selection
                            .scan_ranges(&page_locations[idx])
                            .iter()
                            .map(|range| range.start as u64..range.end as u64),
                    );
                    page_start_offsets
                        .push(ranges.iter().map(|range| range.start as usize).collect());

                    ranges
                })
                .collect::<Vec<_>>();

            let mut chunk_data = self.location.fetch_bytes(&fetch_ranges).await?.into_iter();
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
            self.maybe_fetch_pages_from_cache(projection);

            // Release the CPU to avoid blocking the runtime. Since `fetch_pages_from_cache`
            // is a synchronous, CPU-bound operation.
            yield_now().await;

            let fetch_ranges = self
                .column_chunks
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
                .collect::<Vec<_>>();

            if fetch_ranges.is_empty() {
                // Nothing to fetch.
                return Ok(());
            }

            let mut chunk_data = self.location.fetch_bytes(&fetch_ranges).await?.into_iter();

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
                if self.location.has_cache_manager() && !cache_uncompressed_pages(column) {
                    self.location.put_compressed_to_cache(idx, data.clone());
                }

                *chunk = Some(Arc::new(ColumnChunkData::Dense {
                    offset: column.byte_range().0 as usize,
                    data,
                }));
            }
        }

        Ok(())
    }

    /// Fetches pages for columns if cache is enabled.
    /// If the page is in the cache, sets the column chunk or `column_uncompressed_pages` for the column.
    fn maybe_fetch_pages_from_cache(&mut self, projection: &ProjectionMask) {
        if !self.location.has_cache_manager() {
            return;
        };

        let _timer = READ_STAGE_FETCH_PAGES.start_timer();
        self.column_chunks
            .iter_mut()
            .enumerate()
            .filter(|(idx, chunk)| chunk.is_none() && projection.leaf_included(*idx))
            .for_each(|(idx, chunk)| {
                let column = self.metadata.column(idx);
                if cache_uncompressed_pages(column) {
                    // Fetches uncompressed pages for the row group.
                    self.column_uncompressed_pages[idx] = self.location.get_cache(idx, false);
                } else {
                    // Fetches the compressed page from the cache.
                    *chunk = self.location.get_cache(idx, true).map(|page_value| {
                        Arc::new(ColumnChunkData::Dense {
                            offset: column.byte_range().0 as usize,
                            data: page_value.compressed.clone(),
                        })
                    });
                }
            });
    }

    /// Creates a page reader to read column at `i`.
    fn column_page_reader(&self, i: usize) -> Result<Box<dyn PageReader>> {
        if let Some(cached_pages) = &self.column_uncompressed_pages[i] {
            debug_assert!(!cached_pages.row_group.is_empty());
            // Hits the row group level page cache.
            return Ok(Box::new(RowGroupCachedReader::new(&cached_pages.row_group)));
        }

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

        if !self.location.has_cache_manager() {
            return Ok(Box::new(page_reader));
        };

        let column = self.metadata.column(i);
        if cache_uncompressed_pages(column) {
            // This column use row group level page cache.
            // We collect all pages and put them into the cache.
            let pages = page_reader.collect::<Result<Vec<_>>>()?;
            let page_value = Arc::new(PageValue::new_row_group(pages));
            self.location
                .put_uncompressed_to_cache(i, page_value.clone());
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::arrow::ProjectionMask;

    use crate::sst::index::Indexer;
    use crate::sst::parquet::reader::{Location, Memory};
    use crate::sst::parquet::row_group::InMemoryRowGroup;
    use crate::sst::parquet::writer::ParquetWriter;
    use crate::sst::parquet::WriteOptions;
    use crate::test_util::sst_util::{
        new_batch_by_range, new_source, sst_file_handle, sst_region_metadata,
    };
    use crate::test_util::TestEnv;

    #[tokio::test]
    async fn test_read_with_field_filter() {
        let mut env = TestEnv::new();
        let object_store = env.init_object_store_manager();
        let handle = sst_file_handle(0, 1000);
        let file_path = handle.file_path("test");
        let metadata = Arc::new(sst_region_metadata());
        let source = new_source(&[
            new_batch_by_range(&["a", "d"], 0, 60),
            new_batch_by_range(&["b", "f"], 0, 40),
            new_batch_by_range(&["b", "h"], 100, 200),
        ]);
        // Use a small row group size for test.
        let write_opts = WriteOptions {
            row_group_size: 50,
            ..Default::default()
        };
        // Prepare data.
        let mut writer = ParquetWriter::new_with_object_store(
            object_store.clone(),
            file_path.clone(),
            metadata.clone(),
            Indexer::default(),
        );

        writer
            .write_all(source, &write_opts)
            .await
            .unwrap()
            .unwrap();

        let path = env.data_home().to_path_buf().join("data").join(file_path);

        let data = Bytes::from(tokio::fs::read(&path).await.unwrap());
        let builder = ParquetRecordBatchReaderBuilder::try_new(data.clone()).unwrap();
        let parquet_metadata = builder.metadata();
        let location = Location::Memory(Memory {
            region_id: handle.region_id(),
            data,
        });
        let mut group = InMemoryRowGroup::create(0, parquet_metadata, &location);
        group.fetch(&ProjectionMask::all(), None).await.unwrap();
        for col_idx in 0..group.metadata.num_columns() {
            assert!(
                group.column_chunks[col_idx].is_some()
                    || group.column_uncompressed_pages[col_idx].is_some()
            );
        }
    }
}
