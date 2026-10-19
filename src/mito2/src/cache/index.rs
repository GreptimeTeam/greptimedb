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

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::hash::Hash;
use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use object_store::Buffer;

use crate::cache::load::{CacheLoad, CacheLoads};
use crate::metrics::{CACHE_BYTES, CACHE_HIT, CACHE_LOAD_BYTES, CACHE_LOAD_EVENTS, CACHE_MISS};

/// Metrics for index metadata.
const INDEX_METADATA_TYPE: &str = "index_metadata";
/// Metrics for index content.
const INDEX_CONTENT_TYPE: &str = "index_content";

/// Metrics collected from IndexCache operations.
#[derive(Debug, Default, Clone)]
pub struct IndexCacheMetrics {
    /// Number of cache hits.
    pub cache_hit: usize,
    /// Number of cache misses.
    pub cache_miss: usize,
    /// Number of pages accessed.
    pub num_pages: usize,
    /// Total bytes from pages.
    pub page_bytes: u64,
}

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
    /// Pending page reads shared by single-range and batched readers.
    page_loads: Arc<CacheLoads<(K, PageKey), Bytes>>,
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
        common_telemetry::debug!(
            "Building IndexCache with metadata size: {index_metadata_cap}, content size: {index_content_cap}, page size: {page_size}, index type: {index_type}"
        );
        let index_metadata = moka::sync::CacheBuilder::new(index_metadata_cap)
            .name(&format!("index_metadata_{}", index_type))
            .weigher(weight_of_metadata)
            .eviction_listener(move |k, v, _cause| {
                let size = weight_of_metadata(&k, &v);
                CACHE_BYTES
                    .with_label_values(&[INDEX_METADATA_TYPE])
                    .sub(size.into());
            })
            .support_invalidation_closures()
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
            .support_invalidation_closures()
            .build();
        Self {
            index_metadata,
            index: index_cache,
            page_loads: Arc::default(),
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
    ) -> Result<(Vec<u8>, IndexCacheMetrics), E>
    where
        F: Fn(Vec<Range<u64>>) -> Fut,
        Fut: Future<Output = Result<Vec<Bytes>, E>>,
        E: std::error::Error,
    {
        let range = offset..offset + u64::from(size);
        let (pages, metrics) = self
            .get_or_load_vec(key, file_size, std::slice::from_ref(&range), load)
            .await?;
        Ok((pages[0].to_vec(), metrics))
    }

    /// Like [`IndexCache::get_or_load`] for several ranges, but loads the missing pages of
    /// all ranges with a single `load` call so a cold read costs one round trip instead of
    /// one per range.
    async fn get_or_load_vec<F, Fut, E>(
        &self,
        key: K,
        file_size: u64,
        ranges: &[Range<u64>],
        load: F,
    ) -> Result<(Vec<Bytes>, IndexCacheMetrics), E>
    where
        F: Fn(Vec<Range<u64>>) -> Fut,
        Fut: Future<Output = Result<Vec<Bytes>, E>>,
        E: std::error::Error,
    {
        let mut metrics = IndexCacheMetrics::default();
        let mut pages: HashMap<u64, Bytes> = HashMap::new();
        let mut missing: Vec<u64> = Vec::new();
        let mut seen = HashSet::new();
        for range in ranges.iter().filter(|r| r.end > r.start) {
            let size = (range.end - range.start) as u32;
            for page_key in PageKey::generate_page_keys(range.start, size, self.page_size) {
                if !seen.insert(page_key.page_id) {
                    continue;
                }
                metrics.num_pages += 1;
                match self.get_page(key, page_key) {
                    Some(page) => {
                        CACHE_HIT.with_label_values(&[INDEX_CONTENT_TYPE]).inc();
                        metrics.cache_hit += 1;
                        metrics.page_bytes += page.len() as u64;
                        pages.insert(page_key.page_id, page);
                    }
                    None => {
                        CACHE_MISS.with_label_values(&[INDEX_CONTENT_TYPE]).inc();
                        metrics.cache_miss += 1;
                        missing.push(page_key.page_id);
                    }
                }
            }
        }

        if !missing.is_empty() {
            missing.sort_unstable();
            let mut owned = Vec::new();
            let mut waiting = Vec::new();
            for page_id in missing {
                let page_key = PageKey { page_id };
                match self.page_loads.start((key, page_key), INDEX_CONTENT_TYPE) {
                    CacheLoad::Load(guard) => {
                        // Close the race between the cache lookup and registration.
                        if let Some(page) = self.get_page(key, page_key) {
                            guard.finish(page.clone());
                            metrics.page_bytes += page.len() as u64;
                            pages.insert(page_id, page);
                        } else {
                            owned.push((page_id, guard));
                        }
                    }
                    CacheLoad::Wait(waiter) => waiting.push((page_id, waiter)),
                }
            }
            // Publish owned pages before awaiting others to avoid circular waits
            // between overlapping batches. Keep owned reads in a single batch.
            if !owned.is_empty() {
                let ranges = owned
                    .iter()
                    .map(|(id, _)| self.page_range(*id, file_size))
                    .collect();
                CACHE_LOAD_EVENTS
                    .with_label_values(&[INDEX_CONTENT_TYPE, "read_page"])
                    .inc_by(owned.len() as u64);
                CACHE_LOAD_EVENTS
                    .with_label_values(&[INDEX_CONTENT_TYPE, "read_batch"])
                    .inc();
                let loaded = load(ranges).await?;
                debug_assert_eq!(loaded.len(), owned.len());
                for ((page_id, guard), page) in owned.into_iter().zip(loaded) {
                    CACHE_LOAD_BYTES
                        .with_label_values(&[INDEX_CONTENT_TYPE])
                        .inc_by(page.len() as u64);
                    metrics.page_bytes += page.len() as u64;
                    let page = self.put_page(key, PageKey { page_id }, page);
                    guard.finish(page.clone());
                    pages.insert(page_id, page);
                }
            }
            let mut retry = Vec::new();
            for (page_id, waiter) in waiting {
                if let Some(page) = waiter.wait().await {
                    metrics.page_bytes += page.len() as u64;
                    pages.insert(page_id, page);
                } else {
                    retry.push(page_id);
                }
            }
            // Retry cancelled or failed producers as one batch without joining
            // another generation of pending reads.
            if !retry.is_empty() {
                let ranges = retry
                    .iter()
                    .map(|id| self.page_range(*id, file_size))
                    .collect();
                CACHE_LOAD_EVENTS
                    .with_label_values(&[INDEX_CONTENT_TYPE, "read_page"])
                    .inc_by(retry.len() as u64);
                CACHE_LOAD_EVENTS
                    .with_label_values(&[INDEX_CONTENT_TYPE, "read_batch"])
                    .inc();
                let loaded = load(ranges).await?;
                debug_assert_eq!(loaded.len(), retry.len());
                for (page_id, page) in retry.into_iter().zip(loaded) {
                    CACHE_LOAD_BYTES
                        .with_label_values(&[INDEX_CONTENT_TYPE])
                        .inc_by(page.len() as u64);
                    metrics.page_bytes += page.len() as u64;
                    let page = self.put_page(key, PageKey { page_id }, page);
                    pages.insert(page_id, page);
                }
            }
        }

        let result = ranges
            .iter()
            .map(|range| {
                let size = (range.end - range.start) as u32;
                if size == 0 {
                    return Bytes::new();
                }
                let buffer = Buffer::from_iter(
                    PageKey::generate_page_keys(range.start, size, self.page_size)
                        .map(|p| pages[&p.page_id].clone()),
                );
                buffer
                    .slice(PageKey::calculate_range(range.start, size, self.page_size))
                    .to_bytes()
            })
            .collect();
        Ok((result, metrics))
    }

    fn page_range(&self, page_id: u64, file_size: u64) -> Range<u64> {
        let start = page_id * self.page_size;
        start..(start + self.page_size).min(file_size)
    }

    fn get_page(&self, key: K, page_key: PageKey) -> Option<Bytes> {
        self.index.get(&(key, page_key))
    }

    fn put_page(&self, key: K, page_key: PageKey, value: Bytes) -> Bytes {
        // Clones the value to ensure it doesn't reference a larger buffer.
        let value = Bytes::from(value.to_vec());
        CACHE_BYTES
            .with_label_values(&[INDEX_CONTENT_TYPE])
            .add((self.weight_of_content)(&(key, page_key), &value).into());
        self.index.insert((key, page_key), value.clone());
        value
    }

    /// Invalidates all cache entries whose keys satisfy `predicate`.
    pub fn invalidate_if<F>(&self, predicate: F)
    where
        F: Fn(&K) -> bool + Send + Sync + 'static,
    {
        let predicate = Arc::new(predicate);
        let metadata_predicate = Arc::clone(&predicate);

        self.index_metadata
            .invalidate_entries_if(move |key, _| metadata_predicate(key))
            .expect("cache should support invalidation closures");

        self.index
            .invalidate_entries_if(move |(key, _), _| predicate(key))
            .expect("cache should support invalidation closures");
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use tokio::sync::Notify;

    use super::*;

    fn read_pages(ranges: Vec<Range<u64>>) -> Vec<Bytes> {
        ranges
            .into_iter()
            .map(|range| Bytes::from(range.map(|byte| byte as u8).collect::<Vec<_>>()))
            .collect()
    }

    #[tokio::test]
    async fn overlapping_reads_publish_owned_pages_before_waiting() {
        for capacity in [0, 1024] {
            for outcome in ["complete", "error", "cancel"] {
                let cache = IndexCache::<u64, ()>::new_with_weighter(
                    1024,
                    capacity,
                    4,
                    "test",
                    |_, _| 1,
                    |_, page| page.len() as u32,
                );
                let release = Notify::new();
                let mut producer = Box::pin(cache.get_or_load(1, 16, 0, 8, |ranges| {
                    let release = &release;
                    async move {
                        assert_eq!(ranges, vec![0..4, 4..8]);
                        release.notified().await;
                        if outcome == "error" {
                            return Err(std::io::Error::other("injected failure"));
                        }
                        Ok(read_pages(ranges))
                    }
                }));
                assert!(futures::poll!(&mut producer).is_pending());
                let reads = Mutex::new(Vec::new());
                let requested = 4..12;
                let mut consumer = Box::pin(cache.get_or_load_vec(
                    1,
                    16,
                    std::slice::from_ref(&requested),
                    |ranges| {
                        reads.lock().unwrap().push(ranges.clone());
                        std::future::ready(Ok::<_, std::io::Error>(read_pages(ranges)))
                    },
                ));
                assert!(futures::poll!(&mut consumer).is_pending());
                // It reads its own page before waiting for the shared page.
                assert_eq!(*reads.lock().unwrap(), vec![vec![8..12]]);
                if outcome == "cancel" {
                    drop(producer);
                } else {
                    release.notify_one();
                    let result = producer.await;
                    assert_eq!(result.is_err(), outcome == "error");
                    if let Ok((data, _)) = result {
                        assert_eq!(data, (0..8).collect::<Vec<u8>>());
                    }
                }
                let (data, _) = tokio::time::timeout(std::time::Duration::from_secs(5), consumer)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(data, vec![Bytes::from((4..12).collect::<Vec<u8>>())]);
                let expected = if outcome == "complete" {
                    vec![vec![8..12]]
                } else {
                    vec![vec![8..12], vec![4..8]]
                };
                assert_eq!(*reads.lock().unwrap(), expected);
            }
        }
    }
}
