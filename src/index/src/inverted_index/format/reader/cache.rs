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

use async_trait::async_trait;
use common_base::BitVec;
use greptime_proto::v1::index::InvertedIndexMetas;
use puffin::file_metadata::FileMetadata;
use snafu::ResultExt;
use uuid::Uuid;

use crate::inverted_index::error::DecodeFstSnafu;
use crate::inverted_index::format::reader::InvertedIndexReader;
use crate::inverted_index::metrics::INDEX_CACHE_STATS;
use crate::inverted_index::FstMap;

pub struct CachedInvertedIndexBlobReader<R> {
    file_id: Uuid,
    inner: R,
    cache: Arc<InvertedIndexCache>,
}

impl<R> CachedInvertedIndexBlobReader<R> {
    pub fn new(file_id: Uuid, inner: R, cache: Arc<InvertedIndexCache>) -> Self {
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
    async fn get_or_load(
        &mut self,
        offset: u64,
        size: u32,
    ) -> crate::inverted_index::error::Result<Vec<u8>> {
        let range = offset as usize..(offset + size as u64) as usize;
        if let Some(cached) = self.cache.get_index(IndexKey {
            file_id: self.file_id,
        }) {
            INDEX_CACHE_STATS.with_label_values(&["index", "hit"]).inc();
            Ok(cached[range].to_vec())
        } else {
            INDEX_CACHE_STATS
                .with_label_values(&["index", "miss"])
                .inc();

            let mut all_data = Vec::with_capacity(1024 * 1024);
            self.inner.read_all(&mut all_data).await?;
            let res = all_data[range].to_vec();
            self.cache.put_index(
                IndexKey {
                    file_id: self.file_id,
                },
                all_data,
            );
            Ok(res)
        }
    }
}

#[async_trait]
impl<R: InvertedIndexReader> InvertedIndexReader for CachedInvertedIndexBlobReader<R> {
    async fn read_all(
        &mut self,
        dest: &mut Vec<u8>,
    ) -> crate::inverted_index::error::Result<usize> {
        self.inner.read_all(dest).await
    }

    async fn seek_read(
        &mut self,
        offset: u64,
        size: u32,
    ) -> crate::inverted_index::error::Result<Vec<u8>> {
        self.inner.seek_read(offset, size).await
    }

    async fn metadata(&mut self) -> crate::inverted_index::error::Result<Arc<InvertedIndexMetas>> {
        if let Some(cached) = self.cache.get_index_metadata(self.file_id) {
            INDEX_CACHE_STATS
                .with_label_values(&["index_metadata", "hit"])
                .inc();
            Ok(cached)
        } else {
            INDEX_CACHE_STATS
                .with_label_values(&["index_metadata", "miss"])
                .inc();
            let meta = self.inner.metadata().await?;
            self.cache.put_index_metadata(self.file_id, meta.clone());
            Ok(meta)
        }
    }

    async fn fst(
        &mut self,
        offset: u64,
        size: u32,
    ) -> crate::inverted_index::error::Result<FstMap> {
        self.get_or_load(offset, size)
            .await
            .and_then(|r| FstMap::new(r).context(DecodeFstSnafu))
    }

    async fn bitmap(
        &mut self,
        offset: u64,
        size: u32,
    ) -> crate::inverted_index::error::Result<BitVec> {
        self.get_or_load(offset, size).await.map(BitVec::from_vec)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MetadataKey {
    file_id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IndexKey {
    file_id: Uuid,
}

pub struct InvertedIndexCache {
    index_metadata: moka::sync::Cache<MetadataKey, Arc<InvertedIndexMetas>>,
    file_metadata: moka::sync::Cache<MetadataKey, Arc<FileMetadata>>,
    index: moka::sync::Cache<IndexKey, Arc<Vec<u8>>>,
}

impl InvertedIndexCache {
    pub fn new(file_metadata_cap: u64, index_metadata_cap: u64, index_cap: u64) -> Self {
        let file_metadata = moka::sync::CacheBuilder::new(file_metadata_cap)
            .name("inverted_index_file_metadata")
            .build();
        let index_metadata = moka::sync::CacheBuilder::new(index_metadata_cap)
            .name("inverted_index_metadata")
            .build();
        let index_cache = moka::sync::CacheBuilder::new(index_cap)
            .name("inverted_index_content")
            .build();
        Self {
            index_metadata,
            file_metadata,
            index: index_cache,
        }
    }
}

impl InvertedIndexCache {
    pub fn get_file_metadata(&self, file_id: Uuid) -> Option<Arc<FileMetadata>> {
        self.file_metadata.get(&MetadataKey { file_id })
    }

    pub fn put_file_metadata(&self, file_id: Uuid, metadata: Arc<FileMetadata>) {
        self.file_metadata.insert(MetadataKey { file_id }, metadata)
    }

    pub fn get_index_metadata(&self, file_id: Uuid) -> Option<Arc<InvertedIndexMetas>> {
        self.index_metadata.get(&MetadataKey { file_id })
    }

    pub fn put_index_metadata(&self, file_id: Uuid, metadata: Arc<InvertedIndexMetas>) {
        self.index_metadata
            .insert(MetadataKey { file_id }, metadata)
    }

    // todo(hl): align index file content to pages with size like 4096 bytes so that we won't
    // have too many cache entries.
    pub fn get_index(&self, key: IndexKey) -> Option<Arc<Vec<u8>>> {
        self.index.get(&key)
    }

    pub fn put_index(&self, key: IndexKey, value: Vec<u8>) {
        self.index.insert(key, Arc::new(value));
    }
}
