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
use greptime_proto::v1::index::{InvertedIndexMeta, InvertedIndexMetas};
use snafu::ResultExt;
use uuid::Uuid;

use crate::inverted_index::error::DecodeFstSnafu;
use crate::inverted_index::format::reader::InvertedIndexReader;
use crate::inverted_index::FstMap;

pub struct CachedInvertedIndexBlobReader<R> {
    file_id: Uuid,
    inner: R,
    cache: InvertedIndexCacheRef,
}

impl<R> CachedInvertedIndexBlobReader<R> {
    pub fn new(file_id: Uuid, inner: R, cache: InvertedIndexCacheRef) -> Self {
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
        if let Some(cached) = self.cache.get_index(IndexKey {
            file_id: self.file_id,
            offset,
            size,
        }) {
            Ok(cached)
        } else {
            let data = self.inner.seek_read(offset, size).await?;
            self.cache.put_index(
                IndexKey {
                    file_id: self.file_id,
                    offset,
                    size,
                },
                data.clone(),
            );
            Ok(data)
        }
    }
}

#[async_trait]
impl<R: InvertedIndexReader> InvertedIndexReader for CachedInvertedIndexBlobReader<R> {
    async fn seek_read(
        &mut self,
        offset: u64,
        size: u32,
    ) -> crate::inverted_index::error::Result<Vec<u8>> {
        self.inner.seek_read(offset, size).await
    }

    async fn metadata(&mut self) -> crate::inverted_index::error::Result<InvertedIndexMetas> {
        self.inner.metadata().await
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
    offset: u64,
    size: u32,
}

pub type InvertedIndexCacheRef = Arc<InvertedIndexCache>;

pub struct InvertedIndexCache {
    /// Cache for inverted index metadata
    metadata: moka::sync::Cache<MetadataKey, Arc<InvertedIndexMeta>>,
    /// Cache for inverted index content.
    index: moka::sync::Cache<IndexKey, Vec<u8>>,
}

impl InvertedIndexCache {
    pub fn new(metadata_cap: u64, index_cap: u64) -> Self {
        let metadata_cache = moka::sync::CacheBuilder::new(metadata_cap)
            .name("inverted_index_metadata")
            .build();
        let index_cache = moka::sync::CacheBuilder::new(index_cap)
            .name("inverted_index_content")
            .build();
        Self {
            metadata: metadata_cache,
            index: index_cache,
        }
    }
}

impl InvertedIndexCache {
    pub fn get_metadata(&self, file_id: Uuid) -> Option<Arc<InvertedIndexMeta>> {
        self.metadata.get(&MetadataKey { file_id })
    }

    pub fn put_metadata(&self, file_id: Uuid, metadata: Arc<InvertedIndexMeta>) {
        self.metadata.insert(MetadataKey { file_id }, metadata)
    }

    // todo(hl): align index file content to pages with size like 4096 bytes so that we won't
    // have too many cache entries.
    pub fn get_index(&self, key: IndexKey) -> Option<Vec<u8>> {
        self.index.get(&key)
    }

    pub fn put_index(&self, key: IndexKey, value: Vec<u8>) {
        self.index.insert(key, value);
    }
}
