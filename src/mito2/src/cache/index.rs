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

use std::ops::Range;
use std::sync::Arc;

use api::v1::index::InvertedIndexMetas;
use async_trait::async_trait;
use common_base::BitVec;
use index::inverted_index::error::DecodeFstSnafu;
use index::inverted_index::format::reader::InvertedIndexReader;
use index::inverted_index::FstMap;
use prost::Message;
use snafu::ResultExt;

use super::PAGE_SIZE;
use crate::metrics::{CACHE_BYTES, CACHE_HIT, CACHE_MISS};
use crate::sst::file::FileId;

/// Metrics for index metadata.
const INDEX_METADATA_TYPE: &str = "index_metadata";
/// Metrics for index content.
const INDEX_CONTENT_TYPE: &str = "index_content";

/// Inverted index blob reader with cache.
pub struct CachedInvertedIndexBlobReader<R> {
    file_id: FileId,
    inner: R,
    cache: InvertedIndexCacheRef,
}

impl<R> CachedInvertedIndexBlobReader<R> {
    pub fn new(file_id: FileId, inner: R, cache: InvertedIndexCacheRef) -> Self {
        Self {
            file_id,
            inner,
            cache,
        }
    }
}

impl<R> CachedInvertedIndexBlobReader<R>
where
    R: InvertedIndexReader,
{
    /// Gets given range of index data from cache, and loads from source if the file
    /// is not already cached.
    async fn get_or_load(
        &mut self,
        offset: u64,
        size: u32,
    ) -> index::inverted_index::error::Result<Vec<u8>> {
        let indexes = IndexKey::index(self.file_id, offset, size);
        let mut data = Vec::with_capacity(size as usize);
        let first_page_id = indexes[0].page_id;
        let last_page_id = indexes.last().unwrap().page_id;
        for index in indexes {
            if let Some(cached) = self.cache.get_index(index.clone()) {
                CACHE_HIT.with_label_values(&[INDEX_CONTENT_TYPE]).inc();
                data.extend_from_slice(&cached);
            } else {
                CACHE_MISS.with_label_values(&[INDEX_CONTENT_TYPE]).inc();
                let buf = self
                    .inner
                    .seek_read(index.page_id * (PAGE_SIZE as u64), PAGE_SIZE as u32)
                    .await?;
                let first = IndexKey::offset_to_first_range(offset, size);
                let last = IndexKey::offset_to_last_range(offset, size);
                if index.page_id == first_page_id {
                    data.extend_from_slice(&buf[first.clone()]);
                } else if index.page_id == last_page_id {
                    data.extend_from_slice(&buf[last.clone()]);
                } else {
                    data.extend_from_slice(&buf);
                }
                self.cache.put_index(index, Arc::new(buf));
            }
        }
        Ok(data)
    }
}

#[async_trait]
impl<R: InvertedIndexReader> InvertedIndexReader for CachedInvertedIndexBlobReader<R> {
    async fn read_all(
        &mut self,
        dest: &mut Vec<u8>,
    ) -> index::inverted_index::error::Result<usize> {
        self.inner.read_all(dest).await
    }

    async fn seek_read(
        &mut self,
        offset: u64,
        size: u32,
    ) -> index::inverted_index::error::Result<Vec<u8>> {
        self.inner.seek_read(offset, size).await
    }

    async fn metadata(&mut self) -> index::inverted_index::error::Result<Arc<InvertedIndexMetas>> {
        if let Some(cached) = self.cache.get_index_metadata(self.file_id) {
            CACHE_HIT.with_label_values(&[INDEX_METADATA_TYPE]).inc();
            Ok(cached)
        } else {
            let meta = self.inner.metadata().await?;
            self.cache.put_index_metadata(self.file_id, meta.clone());
            CACHE_MISS.with_label_values(&[INDEX_METADATA_TYPE]).inc();
            Ok(meta)
        }
    }

    async fn fst(
        &mut self,
        offset: u64,
        size: u32,
    ) -> index::inverted_index::error::Result<FstMap> {
        self.get_or_load(offset, size)
            .await
            .and_then(|r| FstMap::new(r).context(DecodeFstSnafu))
    }

    async fn bitmap(
        &mut self,
        offset: u64,
        size: u32,
    ) -> index::inverted_index::error::Result<BitVec> {
        self.get_or_load(offset, size).await.map(BitVec::from_vec)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IndexKey {
    file_id: FileId,
    page_id: u64,
}

impl IndexKey {
    fn offset_to_page_id(offset: u64) -> u64 {
        (offset / (PAGE_SIZE as u64)) as u64
    }

    fn size_to_page_num(size: u32) -> u32 {
        size / (PAGE_SIZE as u32)
    }

    /// Ranges of first page.
    /// For example, if offset is 1000 and size is 2000, then the first page is 1000..4096.
    fn offset_to_first_range(offset: u64, _size: u32) -> Range<usize> {
        let first = (offset % (PAGE_SIZE as u64)) as usize..PAGE_SIZE;
        first
    }

    /// Ranges of last page.
    /// For example, if offset is 1000 and size is 2000, then the last page is 0..904.
    fn offset_to_last_range(_offset: u64, size: u32) -> Range<usize> {
        let last = 0..(size % (PAGE_SIZE as u32)) as usize;
        last
    }

    pub fn index(file_id: FileId, offset: u64, size: u32) -> Vec<Self> {
        let page_id = Self::offset_to_page_id(offset);
        let page_num = Self::size_to_page_num(size);
        (0..page_num)
            .map(|i| Self {
                file_id,
                page_id: page_id + i as u64,
            })
            .collect()
    }
}

pub type InvertedIndexCacheRef = Arc<InvertedIndexCache>;

pub struct InvertedIndexCache {
    /// Cache for inverted index metadata
    index_metadata: moka::sync::Cache<IndexKey, Arc<InvertedIndexMetas>>,
    /// Cache for inverted index content.
    index: moka::sync::Cache<IndexKey, Arc<Vec<u8>>>,
}

impl InvertedIndexCache {
    /// Creates `InvertedIndexCache` with provided `index_metadata_cap` and `index_content_cap`.
    pub fn new(index_metadata_cap: u64, index_content_cap: u64) -> Self {
        common_telemetry::debug!("Building InvertedIndexCache with metadata size: {index_metadata_cap}, content size: {index_content_cap}");
        let index_metadata = moka::sync::CacheBuilder::new(index_metadata_cap)
            .name("inverted_index_metadata")
            .weigher(index_metadata_weight)
            .eviction_listener(|k, v, _cause| {
                let size = index_metadata_weight(&k, &v);
                CACHE_BYTES
                    .with_label_values(&[INDEX_METADATA_TYPE])
                    .sub(size.into());
            })
            .build();
        let index_cache = moka::sync::CacheBuilder::new(index_content_cap)
            .name("inverted_index_content")
            .weigher(index_content_weight)
            .eviction_listener(|k, v, _cause| {
                let size = index_content_weight(&k, &v);
                CACHE_BYTES
                    .with_label_values(&[INDEX_CONTENT_TYPE])
                    .sub(size.into());
            })
            .build();
        Self {
            index_metadata,
            index: index_cache,
        }
    }
}

impl InvertedIndexCache {
    pub fn get_index_metadata(&self, file_id: FileId) -> Option<Arc<InvertedIndexMetas>> {
        self.index_metadata.get(&IndexKey {
            file_id,
            page_id: 0,
        })
    }

    pub fn put_index_metadata(&self, file_id: FileId, metadata: Arc<InvertedIndexMetas>) {
        let key = IndexKey {
            file_id,
            page_id: 0,
        };
        CACHE_BYTES
            .with_label_values(&[INDEX_METADATA_TYPE])
            .add(index_metadata_weight(&key, &metadata).into());
        self.index_metadata.insert(key, metadata)
    }

    // todo(hl): align index file content to pages with size like 4096 bytes.
    pub fn get_index(&self, key: IndexKey) -> Option<Arc<Vec<u8>>> {
        self.index.get(&key)
    }

    pub fn put_index(&self, key: IndexKey, value: Arc<Vec<u8>>) {
        CACHE_BYTES
            .with_label_values(&[INDEX_CONTENT_TYPE])
            .add(index_content_weight(&key, &value).into());
        self.index.insert(key, value);
    }
}

/// Calculates weight for index metadata.
fn index_metadata_weight(k: &IndexKey, v: &Arc<InvertedIndexMetas>) -> u32 {
    (k.file_id.as_bytes().len() + v.encoded_len()) as u32
}

/// Calculates weight for index content.
fn index_content_weight(k: &IndexKey, v: &Arc<Vec<u8>>) -> u32 {
    (k.file_id.as_bytes().len() + v.len()) as u32
}
