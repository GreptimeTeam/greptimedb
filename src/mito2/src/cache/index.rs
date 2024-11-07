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

use std::sync::Arc;

use api::v1::index::InvertedIndexMetas;
use async_trait::async_trait;
use common_base::BitVec;
use index::inverted_index::error::DecodeFstSnafu;
use index::inverted_index::format::reader::InvertedIndexReader;
use index::inverted_index::FstMap;
use prost::Message;
use snafu::ResultExt;

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
        let range = offset as usize..(offset + size as u64) as usize;
        if let Some(cached) = self.cache.get_index(IndexKey {
            file_id: self.file_id,
        }) {
            CACHE_HIT.with_label_values(&[INDEX_CONTENT_TYPE]).inc();
            Ok(cached[range].to_vec())
        } else {
            let mut all_data = Vec::with_capacity(1024 * 1024);
            self.inner.read_all(&mut all_data).await?;
            let result = all_data[range].to_vec();
            self.cache.put_index(
                IndexKey {
                    file_id: self.file_id,
                },
                Arc::new(all_data),
            );
            CACHE_MISS.with_label_values(&[INDEX_CONTENT_TYPE]).inc();
            Ok(result)
        }
    }
}

#[async_trait]
impl<R: InvertedIndexReader> InvertedIndexReader for CachedInvertedIndexBlobReader<R> {
    async fn read_all(
        &mut self,
        dest: &mut Vec<u8>,
    ) -> index::inverted_index::error::Result<usize> {
        common_telemetry::debug!(
            "Inverted index reader read_all start, file_id: {}",
            self.file_id,
        );
        let res = self.inner.read_all(dest).await;
        common_telemetry::debug!(
            "Inverted index reader read_all end, file_id: {}",
            self.file_id,
        );
        res
    }

    async fn seek_read(
        &mut self,
        offset: u64,
        size: u32,
    ) -> index::inverted_index::error::Result<Vec<u8>> {
        common_telemetry::debug!(
            "Inverted index reader seek_read start, file_id: {}, offset: {}, size: {}",
            self.file_id,
            offset,
            size,
        );
        let res = self.inner.seek_read(offset, size).await;
        common_telemetry::debug!(
            "Inverted index reader seek_read end, file_id: {}, offset: {}, size: {}",
            self.file_id,
            offset,
            size,
        );
        res
    }

    async fn metadata(&mut self) -> index::inverted_index::error::Result<Arc<InvertedIndexMetas>> {
        if let Some(cached) = self.cache.get_index_metadata(self.file_id) {
            CACHE_HIT.with_label_values(&[INDEX_METADATA_TYPE]).inc();
            Ok(cached)
        } else {
            common_telemetry::debug!(
                "Inverted index reader get metadata start, file_id: {}",
                self.file_id,
            );
            let meta = self.inner.metadata().await?;
            self.cache.put_index_metadata(self.file_id, meta.clone());
            common_telemetry::debug!(
                "Inverted index reader get metadata end, file_id: {}",
                self.file_id,
            );
            CACHE_MISS.with_label_values(&[INDEX_METADATA_TYPE]).inc();
            Ok(meta)
        }
    }

    async fn fst(
        &mut self,
        offset: u64,
        size: u32,
    ) -> index::inverted_index::error::Result<FstMap> {
        common_telemetry::debug!(
            "Inverted index reader fst start, file_id: {}, offset: {}, size: {}",
            self.file_id,
            offset,
            size,
        );
        let res = self
            .get_or_load(offset, size)
            .await
            .and_then(|r| FstMap::new(r).context(DecodeFstSnafu));
        common_telemetry::debug!(
            "Inverted index reader fst end, file_id: {}, offset: {}, size: {}",
            self.file_id,
            offset,
            size,
        );
        res
    }

    async fn bitmap(
        &mut self,
        offset: u64,
        size: u32,
    ) -> index::inverted_index::error::Result<BitVec> {
        common_telemetry::debug!(
            "Inverted index reader bitmap start, file_id: {}, offset: {}, size: {}",
            self.file_id,
            offset,
            size,
        );
        let res = self.get_or_load(offset, size).await.map(BitVec::from_vec);
        common_telemetry::debug!(
            "Inverted index reader bitmap end, file_id: {}, offset: {}, size: {}",
            self.file_id,
            offset,
            size,
        );
        res
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IndexKey {
    file_id: FileId,
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
        self.index_metadata.get(&IndexKey { file_id })
    }

    pub fn put_index_metadata(&self, file_id: FileId, metadata: Arc<InvertedIndexMetas>) {
        let key = IndexKey { file_id };
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
