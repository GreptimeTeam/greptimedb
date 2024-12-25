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

use async_trait::async_trait;
use bytes::Bytes;
use index::bloom_filter::error::Result;
use index::bloom_filter::reader::BloomFilterReader;
use index::bloom_filter::BloomFilterMeta;

use crate::cache::index::{IndexCache, PageKey, INDEX_METADATA_TYPE};
use crate::metrics::{CACHE_HIT, CACHE_MISS};
use crate::sst::file::FileId;

const INDEX_TYPE_BLOOM_FILTER_INDEX: &str = "bloom_filter_index";

/// Cache for bloom filter index.
pub type BloomFilterIndexCache = IndexCache<FileId, BloomFilterMeta>;
pub type BloomFilterIndexCacheRef = Arc<BloomFilterIndexCache>;

impl BloomFilterIndexCache {
    /// Creates a new bloom filter index cache.
    pub fn new(index_metadata_cap: u64, index_content_cap: u64, page_size: u64) -> Self {
        Self::new_with_weighter(
            index_metadata_cap,
            index_content_cap,
            page_size,
            INDEX_TYPE_BLOOM_FILTER_INDEX,
            bloom_filter_index_metadata_weight,
            bloom_filter_index_content_weight,
        )
    }
}

/// Calculates weight for bloom filter index metadata.
fn bloom_filter_index_metadata_weight(k: &FileId, _: &Arc<BloomFilterMeta>) -> u32 {
    (k.as_bytes().len() + std::mem::size_of::<BloomFilterMeta>()) as u32
}

/// Calculates weight for bloom filter index content.
fn bloom_filter_index_content_weight((k, _): &(FileId, PageKey), v: &Bytes) -> u32 {
    (k.as_bytes().len() + v.len()) as u32
}

/// Bloom filter index blob reader with cache.
pub struct CachedBloomFilterIndexBlobReader<R> {
    file_id: FileId,
    file_size: u64,
    inner: R,
    cache: BloomFilterIndexCacheRef,
}

impl<R> CachedBloomFilterIndexBlobReader<R> {
    /// Creates a new bloom filter index blob reader with cache.
    pub fn new(file_id: FileId, file_size: u64, inner: R, cache: BloomFilterIndexCacheRef) -> Self {
        Self {
            file_id,
            file_size,
            inner,
            cache,
        }
    }
}

#[async_trait]
impl<R: BloomFilterReader + Send> BloomFilterReader for CachedBloomFilterIndexBlobReader<R> {
    async fn range_read(&mut self, offset: u64, size: u32) -> Result<Bytes> {
        let inner = &mut self.inner;
        self.cache
            .get_or_load(
                self.file_id,
                self.file_size,
                offset,
                size,
                move |ranges| async move { inner.read_vec(&ranges).await },
            )
            .await
            .map(|b| b.into())
    }

    /// Reads bunch of ranges from the file.
    async fn read_vec(&mut self, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        let mut results = Vec::with_capacity(ranges.len());
        for range in ranges {
            let size = (range.end - range.start) as u32;
            let data = self.range_read(range.start, size).await?;
            results.push(data);
        }
        Ok(results)
    }

    /// Reads the meta information of the bloom filter.
    async fn metadata(&mut self) -> Result<BloomFilterMeta> {
        if let Some(cached) = self.cache.get_metadata(self.file_id) {
            CACHE_HIT.with_label_values(&[INDEX_METADATA_TYPE]).inc();
            Ok((*cached).clone())
        } else {
            let meta = self.inner.metadata().await?;
            self.cache
                .put_metadata(self.file_id, Arc::new(meta.clone()));
            CACHE_MISS.with_label_values(&[INDEX_METADATA_TYPE]).inc();
            Ok(meta)
        }
    }
}
