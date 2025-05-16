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

pub mod bloom_filter_index;
pub mod inverted_index;
pub mod result_cache;

use std::future::Future;
use std::hash::Hash;
use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use object_store::Buffer;

use crate::metrics::{CACHE_BYTES, CACHE_HIT, CACHE_MISS};

/// Metrics for index metadata.
const INDEX_METADATA_TYPE: &str = "index_metadata";
/// Metrics for index content.
const INDEX_CONTENT_TYPE: &str = "index_content";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PageKey {
    page_id: u64,
}

impl PageKey {
    /// Converts an offset to a page ID based on the page size.
    fn calculate_page_id(offset: u64, page_size: u64) -> u64 {
        offset / page_size
    }

    /// Calculates the total number of pages that a given size spans, starting from a specific offset.
    fn calculate_page_count(offset: u64, size: u32, page_size: u64) -> u32 {
        let start_page = Self::calculate_page_id(offset, page_size);
        let end_page = Self::calculate_page_id(offset + (size as u64) - 1, page_size);
        (end_page + 1 - start_page) as u32
    }

    /// Calculates the byte range for data retrieval based on the specified offset and size.
    ///
    /// This function determines the starting and ending byte positions required for reading data.
    /// For example, with an offset of 5000 and a size of 5000, using a PAGE_SIZE of 4096,
    /// the resulting byte range will be 904..5904. This indicates that:
    /// - The reader will first access fixed-size pages [4096, 8192) and [8192, 12288).
    /// - To read the range [5000..10000), it only needs to fetch bytes within the range [904, 5904) across two pages.
    fn calculate_range(offset: u64, size: u32, page_size: u64) -> Range<usize> {
        let start = (offset % page_size) as usize;
        let end = start + size as usize;
        start..end
    }

    /// Generates a iterator of `IndexKey` for the pages that a given offset and size span.
    fn generate_page_keys(offset: u64, size: u32, page_size: u64) -> impl Iterator<Item = Self> {
        let start_page = Self::calculate_page_id(offset, page_size);
        let total_pages = Self::calculate_page_count(offset, size, page_size);
        (0..total_pages).map(move |i| Self {
            page_id: start_page + i as u64,
        })
    }
}

/// Cache for index metadata and content.
pub struct IndexCache<K, M> {
    /// Cache for index metadata
    index_metadata: moka::sync::Cache<K, Arc<M>>,
    /// Cache for index content.
    index: moka::sync::Cache<(K, PageKey), Bytes>,
    // Page size for index content.
    page_size: u64,

    /// Weighter for metadata.
    weight_of_metadata: fn(&K, &Arc<M>) -> u32,
    /// Weighter for content.
    weight_of_content: fn(&(K, PageKey), &Bytes) -> u32,
}

impl<K, M> IndexCache<K, M>
where
    K: Hash + Eq + Send + Sync + 'static,
    M: Send + Sync + 'static,
{
    pub fn new_with_weighter(
        index_metadata_cap: u64,
        index_content_cap: u64,
        page_size: u64,
        index_type: &'static str,
        weight_of_metadata: fn(&K, &Arc<M>) -> u32,
        weight_of_content: fn(&(K, PageKey), &Bytes) -> u32,
    ) -> Self {
        common_telemetry::debug!("Building IndexCache with metadata size: {index_metadata_cap}, content size: {index_content_cap}, page size: {page_size}, index type: {index_type}");
        let index_metadata = moka::sync::CacheBuilder::new(index_metadata_cap)
            .name(&format!("index_metadata_{}", index_type))
            .weigher(weight_of_metadata)
            .eviction_listener(move |k, v, _cause| {
                let size = weight_of_metadata(&k, &v);
                CACHE_BYTES
                    .with_label_values(&[INDEX_METADATA_TYPE])
                    .sub(size.into());
            })
            .build();
        let index_cache = moka::sync::CacheBuilder::new(index_content_cap)
            .name(&format!("index_content_{}", index_type))
            .weigher(weight_of_content)
            .eviction_listener(move |k, v, _cause| {
                let size = weight_of_content(&k, &v);
                CACHE_BYTES
                    .with_label_values(&[INDEX_CONTENT_TYPE])
                    .sub(size.into());
            })
            .build();
        Self {
            index_metadata,
            index: index_cache,
            page_size,
            weight_of_content,
            weight_of_metadata,
        }
    }
}

impl<K, M> IndexCache<K, M>
where
    K: Hash + Eq + Clone + Copy + Send + Sync + 'static,
    M: Send + Sync + 'static,
{
    pub fn get_metadata(&self, key: K) -> Option<Arc<M>> {
        self.index_metadata.get(&key)
    }

    pub fn put_metadata(&self, key: K, metadata: Arc<M>) {
        CACHE_BYTES
            .with_label_values(&[INDEX_METADATA_TYPE])
            .add((self.weight_of_metadata)(&key, &metadata).into());
        self.index_metadata.insert(key, metadata)
    }

    /// Gets given range of index data from cache, and loads from source if the file
    /// is not already cached.
    async fn get_or_load<F, Fut, E>(
        &self,
        key: K,
        file_size: u64,
        offset: u64,
        size: u32,
        load: F,
    ) -> Result<Vec<u8>, E>
    where
        F: Fn(Vec<Range<u64>>) -> Fut,
        Fut: Future<Output = Result<Vec<Bytes>, E>>,
        E: std::error::Error,
    {
        let page_keys =
            PageKey::generate_page_keys(offset, size, self.page_size).collect::<Vec<_>>();
        // Size is 0, return empty data.
        if page_keys.is_empty() {
            return Ok(Vec::new());
        }
        let mut data = Vec::with_capacity(page_keys.len());
        data.resize(page_keys.len(), Bytes::new());
        let mut cache_miss_range = vec![];
        let mut cache_miss_idx = vec![];
        let last_index = page_keys.len() - 1;
        // TODO: Avoid copy as much as possible.
        for (i, page_key) in page_keys.iter().enumerate() {
            match self.get_page(key, *page_key) {
                Some(page) => {
                    CACHE_HIT.with_label_values(&[INDEX_CONTENT_TYPE]).inc();
                    data[i] = page;
                }
                None => {
                    CACHE_MISS.with_label_values(&[INDEX_CONTENT_TYPE]).inc();
                    let base_offset = page_key.page_id * self.page_size;
                    let pruned_size = if i == last_index {
                        prune_size(page_keys.iter(), file_size, self.page_size)
                    } else {
                        self.page_size
                    };
                    cache_miss_range.push(base_offset..base_offset + pruned_size);
                    cache_miss_idx.push(i);
                }
            }
        }
        if !cache_miss_range.is_empty() {
            let pages = load(cache_miss_range).await?;
            for (i, page) in cache_miss_idx.into_iter().zip(pages.into_iter()) {
                let page_key = page_keys[i];
                data[i] = page.clone();
                self.put_page(key, page_key, page.clone());
            }
        }
        let buffer = Buffer::from_iter(data.into_iter());
        Ok(buffer
            .slice(PageKey::calculate_range(offset, size, self.page_size))
            .to_vec())
    }

    fn get_page(&self, key: K, page_key: PageKey) -> Option<Bytes> {
        self.index.get(&(key, page_key))
    }

    fn put_page(&self, key: K, page_key: PageKey, value: Bytes) {
        CACHE_BYTES
            .with_label_values(&[INDEX_CONTENT_TYPE])
            .add((self.weight_of_content)(&(key, page_key), &value).into());
        self.index.insert((key, page_key), value);
    }
}

/// Prunes the size of the last page based on the indexes.
/// We have following cases:
/// 1. The rest file size is less than the page size, read to the end of the file.
/// 2. Otherwise, read the page size.
fn prune_size<'a>(
    indexes: impl Iterator<Item = &'a PageKey>,
    file_size: u64,
    page_size: u64,
) -> u64 {
    let last_page_start = indexes.last().map(|i| i.page_id * page_size).unwrap_or(0);
    page_size.min(file_size - last_page_start)
}
