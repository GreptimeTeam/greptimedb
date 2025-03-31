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

use api::v1::index::BloomFilterMeta;
use async_trait::async_trait;
use bytes::Bytes;
use futures::future::try_join_all;
use index::bloom_filter::error::Result;
use index::bloom_filter::reader::BloomFilterReader;
use store_api::storage::ColumnId;

use crate::cache::index::{IndexCache, PageKey, INDEX_METADATA_TYPE};
use crate::metrics::{CACHE_HIT, CACHE_MISS};
use crate::sst::file::FileId;

const INDEX_TYPE_BLOOM_FILTER_INDEX: &str = "bloom_filter_index";

/// Cache for bloom filter index.
pub type BloomFilterIndexCache = IndexCache<(FileId, ColumnId), BloomFilterMeta>;
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
fn bloom_filter_index_metadata_weight(k: &(FileId, ColumnId), _: &Arc<BloomFilterMeta>) -> u32 {
    (k.0.as_bytes().len()
        + std::mem::size_of::<ColumnId>()
        + std::mem::size_of::<BloomFilterMeta>()) as u32
}

/// Calculates weight for bloom filter index content.
fn bloom_filter_index_content_weight((k, _): &((FileId, ColumnId), PageKey), v: &Bytes) -> u32 {
    (k.0.as_bytes().len() + std::mem::size_of::<ColumnId>() + v.len()) as u32
}

/// Bloom filter index blob reader with cache.
pub struct CachedBloomFilterIndexBlobReader<R> {
    file_id: FileId,
    column_id: ColumnId,
    blob_size: u64,
    inner: R,
    cache: BloomFilterIndexCacheRef,
}

impl<R> CachedBloomFilterIndexBlobReader<R> {
    /// Creates a new bloom filter index blob reader with cache.
    pub fn new(
        file_id: FileId,
        column_id: ColumnId,
        blob_size: u64,
        inner: R,
        cache: BloomFilterIndexCacheRef,
    ) -> Self {
        Self {
            file_id,
            column_id,
            blob_size,
            inner,
            cache,
        }
    }
}

#[async_trait]
impl<R: BloomFilterReader + Send> BloomFilterReader for CachedBloomFilterIndexBlobReader<R> {
    async fn range_read(&self, offset: u64, size: u32) -> Result<Bytes> {
        let inner = &self.inner;
        self.cache
            .get_or_load(
                (self.file_id, self.column_id),
                self.blob_size,
                offset,
                size,
                move |ranges| async move { inner.read_vec(&ranges).await },
            )
            .await
            .map(|b| b.into())
    }

    async fn read_vec(&self, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        let fetch = ranges.iter().map(|range| {
            let inner = &self.inner;
            self.cache.get_or_load(
                (self.file_id, self.column_id),
                self.blob_size,
                range.start,
                (range.end - range.start) as u32,
                move |ranges| async move { inner.read_vec(&ranges).await },
            )
        });
        Ok(try_join_all(fetch)
            .await?
            .into_iter()
            .map(Bytes::from)
            .collect::<Vec<_>>())
    }

    /// Reads the meta information of the bloom filter.
    async fn metadata(&self) -> Result<BloomFilterMeta> {
        if let Some(cached) = self.cache.get_metadata((self.file_id, self.column_id)) {
            CACHE_HIT.with_label_values(&[INDEX_METADATA_TYPE]).inc();
            Ok((*cached).clone())
        } else {
            let meta = self.inner.metadata().await?;
            self.cache
                .put_metadata((self.file_id, self.column_id), Arc::new(meta.clone()));
            CACHE_MISS.with_label_values(&[INDEX_METADATA_TYPE]).inc();
            Ok(meta)
        }
    }
}

#[cfg(test)]
mod test {
    use rand::{Rng, RngCore};

    use super::*;

    const FUZZ_REPEAT_TIMES: usize = 100;

    #[test]
    fn fuzz_index_calculation() {
        let mut rng = rand::rng();
        let mut data = vec![0u8; 1024 * 1024];
        rng.fill_bytes(&mut data);

        for _ in 0..FUZZ_REPEAT_TIMES {
            let offset = rng.random_range(0..data.len() as u64);
            let size = rng.random_range(0..data.len() as u32 - offset as u32);
            let page_size: usize = rng.random_range(1..1024);

            let indexes =
                PageKey::generate_page_keys(offset, size, page_size as u64).collect::<Vec<_>>();
            let page_num = indexes.len();
            let mut read = Vec::with_capacity(size as usize);
            for key in indexes.into_iter() {
                let start = key.page_id as usize * page_size;
                let page = if start + page_size < data.len() {
                    &data[start..start + page_size]
                } else {
                    &data[start..]
                };
                read.extend_from_slice(page);
            }
            let expected_range = offset as usize..(offset + size as u64 as u64) as usize;
            let read = read[PageKey::calculate_range(offset, size, page_size as u64)].to_vec();
            assert_eq!(
                read,
                data.get(expected_range).unwrap(),
                "fuzz_read_index failed, offset: {}, size: {}, page_size: {}\nread len: {}, expected len: {}\nrange: {:?}, page num: {}",
                offset,
                size,
                page_size,
                read.len(),
                size as usize,
                PageKey::calculate_range(offset, size, page_size as u64),
                page_num
            );
        }
    }
}
