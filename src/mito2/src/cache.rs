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

//! Cache for the engine.

mod cache_size;

pub(crate) mod file_cache;
pub(crate) mod index;
pub(crate) mod manifest_cache;
#[cfg(test)]
pub(crate) mod test_util;
pub(crate) mod write_cache;

use std::mem;
use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use datatypes::value::Value;
use datatypes::vectors::VectorRef;
use index::bloom_filter_index::{BloomFilterIndexCache, BloomFilterIndexCacheRef};
use index::result_cache::IndexResultCache;
use moka::notification::RemovalCause;
use moka::sync::Cache;
use object_store::ObjectStore;
use parquet::file::metadata::ParquetMetaData;
use puffin::puffin_manager::cache::{PuffinMetadataCache, PuffinMetadataCacheRef};
use store_api::storage::{ConcreteDataType, FileId, RegionId, TimeSeriesRowSelector};

use crate::cache::cache_size::parquet_meta_size;
use crate::cache::file_cache::{FileType, IndexKey};
use crate::cache::index::inverted_index::{InvertedIndexCache, InvertedIndexCacheRef};
use crate::cache::write_cache::WriteCacheRef;
use crate::metrics::{CACHE_BYTES, CACHE_EVICTION, CACHE_HIT, CACHE_MISS};
use crate::read::Batch;
use crate::sst::file::{RegionFileId, RegionIndexId};
use crate::sst::parquet::reader::MetadataCacheMetrics;

/// Metrics type key for sst meta.
const SST_META_TYPE: &str = "sst_meta";
/// Metrics type key for vector.
const VECTOR_TYPE: &str = "vector";
/// Metrics type key for pages.
const PAGE_TYPE: &str = "page";
/// Metrics type key for files on the local store.
const FILE_TYPE: &str = "file";
/// Metrics type key for index files (puffin) on the local store.
const INDEX_TYPE: &str = "index";
/// Metrics type key for selector result cache.
const SELECTOR_RESULT_TYPE: &str = "selector_result";

/// Cache strategies that may only enable a subset of caches.
#[derive(Clone)]
pub enum CacheStrategy {
    /// Strategy for normal operations.
    /// Doesn't disable any cache.
    EnableAll(CacheManagerRef),
    /// Strategy for compaction.
    /// Disables some caches during compaction to avoid affecting queries.
    /// Enables the write cache so that the compaction can read files cached
    /// in the write cache and write the compacted files back to the write cache.
    Compaction(CacheManagerRef),
    /// Do not use any cache.
    Disabled,
}

impl CacheStrategy {
    /// Gets parquet metadata with cache metrics tracking.
    /// Returns the metadata and updates the provided metrics.
    pub(crate) async fn get_parquet_meta_data(
        &self,
        file_id: RegionFileId,
        metrics: &mut MetadataCacheMetrics,
    ) -> Option<Arc<ParquetMetaData>> {
        match self {
            CacheStrategy::EnableAll(cache_manager) => {
                cache_manager.get_parquet_meta_data(file_id, metrics).await
            }
            CacheStrategy::Compaction(cache_manager) => {
                cache_manager.get_parquet_meta_data(file_id, metrics).await
            }
            CacheStrategy::Disabled => {
                metrics.cache_miss += 1;
                None
            }
        }
    }

    /// Calls [CacheManager::get_parquet_meta_data_from_mem_cache()].
    pub fn get_parquet_meta_data_from_mem_cache(
        &self,
        file_id: RegionFileId,
    ) -> Option<Arc<ParquetMetaData>> {
        match self {
            CacheStrategy::EnableAll(cache_manager) => {
                cache_manager.get_parquet_meta_data_from_mem_cache(file_id)
            }
            CacheStrategy::Compaction(cache_manager) => {
                cache_manager.get_parquet_meta_data_from_mem_cache(file_id)
            }
            CacheStrategy::Disabled => None,
        }
    }

    /// Calls [CacheManager::put_parquet_meta_data()].
    pub fn put_parquet_meta_data(&self, file_id: RegionFileId, metadata: Arc<ParquetMetaData>) {
        match self {
            CacheStrategy::EnableAll(cache_manager) => {
                cache_manager.put_parquet_meta_data(file_id, metadata);
            }
            CacheStrategy::Compaction(cache_manager) => {
                cache_manager.put_parquet_meta_data(file_id, metadata);
            }
            CacheStrategy::Disabled => {}
        }
    }

    /// Calls [CacheManager::remove_parquet_meta_data()].
    pub fn remove_parquet_meta_data(&self, file_id: RegionFileId) {
        match self {
            CacheStrategy::EnableAll(cache_manager) => {
                cache_manager.remove_parquet_meta_data(file_id);
            }
            CacheStrategy::Compaction(cache_manager) => {
                cache_manager.remove_parquet_meta_data(file_id);
            }
            CacheStrategy::Disabled => {}
        }
    }

    /// Calls [CacheManager::get_repeated_vector()].
    /// It returns None if the strategy is [CacheStrategy::Compaction] or [CacheStrategy::Disabled].
    pub fn get_repeated_vector(
        &self,
        data_type: &ConcreteDataType,
        value: &Value,
    ) -> Option<VectorRef> {
        match self {
            CacheStrategy::EnableAll(cache_manager) => {
                cache_manager.get_repeated_vector(data_type, value)
            }
            CacheStrategy::Compaction(_) | CacheStrategy::Disabled => None,
        }
    }

    /// Calls [CacheManager::put_repeated_vector()].
    /// It does nothing if the strategy isn't [CacheStrategy::EnableAll].
    pub fn put_repeated_vector(&self, value: Value, vector: VectorRef) {
        if let CacheStrategy::EnableAll(cache_manager) = self {
            cache_manager.put_repeated_vector(value, vector);
        }
    }

    /// Calls [CacheManager::get_pages()].
    /// It returns None if the strategy is [CacheStrategy::Compaction] or [CacheStrategy::Disabled].
    pub fn get_pages(&self, page_key: &PageKey) -> Option<Arc<PageValue>> {
        match self {
            CacheStrategy::EnableAll(cache_manager) => cache_manager.get_pages(page_key),
            CacheStrategy::Compaction(_) | CacheStrategy::Disabled => None,
        }
    }

    /// Calls [CacheManager::put_pages()].
    /// It does nothing if the strategy isn't [CacheStrategy::EnableAll].
    pub fn put_pages(&self, page_key: PageKey, pages: Arc<PageValue>) {
        if let CacheStrategy::EnableAll(cache_manager) = self {
            cache_manager.put_pages(page_key, pages);
        }
    }

    /// Calls [CacheManager::evict_puffin_cache()].
    pub async fn evict_puffin_cache(&self, file_id: RegionIndexId) {
        match self {
            CacheStrategy::EnableAll(cache_manager) => {
                cache_manager.evict_puffin_cache(file_id).await
            }
            CacheStrategy::Compaction(cache_manager) => {
                cache_manager.evict_puffin_cache(file_id).await
            }
            CacheStrategy::Disabled => {}
        }
    }

    /// Calls [CacheManager::get_selector_result()].
    /// It returns None if the strategy is [CacheStrategy::Compaction] or [CacheStrategy::Disabled].
    pub fn get_selector_result(
        &self,
        selector_key: &SelectorResultKey,
    ) -> Option<Arc<SelectorResultValue>> {
        match self {
            CacheStrategy::EnableAll(cache_manager) => {
                cache_manager.get_selector_result(selector_key)
            }
            CacheStrategy::Compaction(_) | CacheStrategy::Disabled => None,
        }
    }

    /// Calls [CacheManager::put_selector_result()].
    /// It does nothing if the strategy isn't [CacheStrategy::EnableAll].
    pub fn put_selector_result(
        &self,
        selector_key: SelectorResultKey,
        result: Arc<SelectorResultValue>,
    ) {
        if let CacheStrategy::EnableAll(cache_manager) = self {
            cache_manager.put_selector_result(selector_key, result);
        }
    }

    /// Calls [CacheManager::write_cache()].
    /// It returns None if the strategy is [CacheStrategy::Disabled].
    pub fn write_cache(&self) -> Option<&WriteCacheRef> {
        match self {
            CacheStrategy::EnableAll(cache_manager) => cache_manager.write_cache(),
            CacheStrategy::Compaction(cache_manager) => cache_manager.write_cache(),
            CacheStrategy::Disabled => None,
        }
    }

    /// Calls [CacheManager::index_cache()].
    /// It returns None if the strategy is [CacheStrategy::Compaction] or [CacheStrategy::Disabled].
    pub fn inverted_index_cache(&self) -> Option<&InvertedIndexCacheRef> {
        match self {
            CacheStrategy::EnableAll(cache_manager) => cache_manager.inverted_index_cache(),
            CacheStrategy::Compaction(_) | CacheStrategy::Disabled => None,
        }
    }

    /// Calls [CacheManager::bloom_filter_index_cache()].
    /// It returns None if the strategy is [CacheStrategy::Compaction] or [CacheStrategy::Disabled].
    pub fn bloom_filter_index_cache(&self) -> Option<&BloomFilterIndexCacheRef> {
        match self {
            CacheStrategy::EnableAll(cache_manager) => cache_manager.bloom_filter_index_cache(),
            CacheStrategy::Compaction(_) | CacheStrategy::Disabled => None,
        }
    }

    /// Calls [CacheManager::puffin_metadata_cache()].
    /// It returns None if the strategy is [CacheStrategy::Compaction] or [CacheStrategy::Disabled].
    pub fn puffin_metadata_cache(&self) -> Option<&PuffinMetadataCacheRef> {
        match self {
            CacheStrategy::EnableAll(cache_manager) => cache_manager.puffin_metadata_cache(),
            CacheStrategy::Compaction(_) | CacheStrategy::Disabled => None,
        }
    }

    /// Calls [CacheManager::index_result_cache()].
    /// It returns None if the strategy is [CacheStrategy::Compaction] or [CacheStrategy::Disabled].
    pub fn index_result_cache(&self) -> Option<&IndexResultCache> {
        match self {
            CacheStrategy::EnableAll(cache_manager) => cache_manager.index_result_cache(),
            CacheStrategy::Compaction(_) | CacheStrategy::Disabled => None,
        }
    }

    /// Triggers download if the strategy is [CacheStrategy::EnableAll] and write cache is available.
    pub fn maybe_download_background(
        &self,
        index_key: IndexKey,
        remote_path: String,
        remote_store: ObjectStore,
        file_size: u64,
    ) {
        if let CacheStrategy::EnableAll(cache_manager) = self
            && let Some(write_cache) = cache_manager.write_cache()
        {
            write_cache.file_cache().maybe_download_background(
                index_key,
                remote_path,
                remote_store,
                file_size,
            );
        }
    }
}

/// Manages cached data for the engine.
///
/// All caches are disabled by default.
#[derive(Default)]
pub struct CacheManager {
    /// Cache for SST metadata.
    sst_meta_cache: Option<SstMetaCache>,
    /// Cache for vectors.
    vector_cache: Option<VectorCache>,
    /// Cache for SST pages.
    page_cache: Option<PageCache>,
    /// A Cache for writing files to object stores.
    write_cache: Option<WriteCacheRef>,
    /// Cache for inverted index.
    inverted_index_cache: Option<InvertedIndexCacheRef>,
    /// Cache for bloom filter index.
    bloom_filter_index_cache: Option<BloomFilterIndexCacheRef>,
    /// Puffin metadata cache.
    puffin_metadata_cache: Option<PuffinMetadataCacheRef>,
    /// Cache for time series selectors.
    selector_result_cache: Option<SelectorResultCache>,
    /// Cache for index result.
    index_result_cache: Option<IndexResultCache>,
}

pub type CacheManagerRef = Arc<CacheManager>;

impl CacheManager {
    /// Returns a builder to build the cache.
    pub fn builder() -> CacheManagerBuilder {
        CacheManagerBuilder::default()
    }

    /// Gets cached [ParquetMetaData] with metrics tracking.
    /// Tries in-memory cache first, then file cache, updating metrics accordingly.
    pub(crate) async fn get_parquet_meta_data(
        &self,
        file_id: RegionFileId,
        metrics: &mut MetadataCacheMetrics,
    ) -> Option<Arc<ParquetMetaData>> {
        // Try to get metadata from sst meta cache
        if let Some(metadata) = self.get_parquet_meta_data_from_mem_cache(file_id) {
            metrics.mem_cache_hit += 1;
            return Some(metadata);
        }

        // Try to get metadata from write cache
        let key = IndexKey::new(file_id.region_id(), file_id.file_id(), FileType::Parquet);
        if let Some(write_cache) = &self.write_cache
            && let Some(metadata) = write_cache.file_cache().get_parquet_meta_data(key).await
        {
            metrics.file_cache_hit += 1;
            let metadata = Arc::new(metadata);
            // Put metadata into sst meta cache
            self.put_parquet_meta_data(file_id, metadata.clone());
            return Some(metadata);
        };
        metrics.cache_miss += 1;

        None
    }

    /// Gets cached [ParquetMetaData] from in-memory cache.
    /// This method does not perform I/O.
    pub fn get_parquet_meta_data_from_mem_cache(
        &self,
        file_id: RegionFileId,
    ) -> Option<Arc<ParquetMetaData>> {
        // Try to get metadata from sst meta cache
        self.sst_meta_cache.as_ref().and_then(|sst_meta_cache| {
            let value = sst_meta_cache.get(&SstMetaKey(file_id.region_id(), file_id.file_id()));
            update_hit_miss(value, SST_META_TYPE)
        })
    }

    /// Puts [ParquetMetaData] into the cache.
    pub fn put_parquet_meta_data(&self, file_id: RegionFileId, metadata: Arc<ParquetMetaData>) {
        if let Some(cache) = &self.sst_meta_cache {
            let key = SstMetaKey(file_id.region_id(), file_id.file_id());
            CACHE_BYTES
                .with_label_values(&[SST_META_TYPE])
                .add(meta_cache_weight(&key, &metadata).into());
            cache.insert(key, metadata);
        }
    }

    /// Removes [ParquetMetaData] from the cache.
    pub fn remove_parquet_meta_data(&self, file_id: RegionFileId) {
        if let Some(cache) = &self.sst_meta_cache {
            cache.remove(&SstMetaKey(file_id.region_id(), file_id.file_id()));
        }
    }

    /// Gets a vector with repeated value for specific `key`.
    pub fn get_repeated_vector(
        &self,
        data_type: &ConcreteDataType,
        value: &Value,
    ) -> Option<VectorRef> {
        self.vector_cache.as_ref().and_then(|vector_cache| {
            let value = vector_cache.get(&(data_type.clone(), value.clone()));
            update_hit_miss(value, VECTOR_TYPE)
        })
    }

    /// Puts a vector with repeated value into the cache.
    pub fn put_repeated_vector(&self, value: Value, vector: VectorRef) {
        if let Some(cache) = &self.vector_cache {
            let key = (vector.data_type(), value);
            CACHE_BYTES
                .with_label_values(&[VECTOR_TYPE])
                .add(vector_cache_weight(&key, &vector).into());
            cache.insert(key, vector);
        }
    }

    /// Gets pages for the row group.
    pub fn get_pages(&self, page_key: &PageKey) -> Option<Arc<PageValue>> {
        self.page_cache.as_ref().and_then(|page_cache| {
            let value = page_cache.get(page_key);
            update_hit_miss(value, PAGE_TYPE)
        })
    }

    /// Puts pages of the row group into the cache.
    pub fn put_pages(&self, page_key: PageKey, pages: Arc<PageValue>) {
        if let Some(cache) = &self.page_cache {
            CACHE_BYTES
                .with_label_values(&[PAGE_TYPE])
                .add(page_cache_weight(&page_key, &pages).into());
            cache.insert(page_key, pages);
        }
    }

    /// Evicts every puffin-related cache entry for the given file.
    pub async fn evict_puffin_cache(&self, file_id: RegionIndexId) {
        if let Some(cache) = &self.bloom_filter_index_cache {
            cache.invalidate_file(file_id.file_id());
        }

        if let Some(cache) = &self.inverted_index_cache {
            cache.invalidate_file(file_id.file_id());
        }

        if let Some(cache) = &self.index_result_cache {
            cache.invalidate_file(file_id.file_id());
        }

        if let Some(cache) = &self.puffin_metadata_cache {
            cache.remove(&file_id.to_string());
        }

        if let Some(write_cache) = &self.write_cache {
            write_cache
                .remove(IndexKey::new(
                    file_id.region_id(),
                    file_id.file_id(),
                    FileType::Puffin(file_id.version),
                ))
                .await;
        }
    }

    /// Gets result of for the selector.
    pub fn get_selector_result(
        &self,
        selector_key: &SelectorResultKey,
    ) -> Option<Arc<SelectorResultValue>> {
        self.selector_result_cache
            .as_ref()
            .and_then(|selector_result_cache| selector_result_cache.get(selector_key))
    }

    /// Puts result of the selector into the cache.
    pub fn put_selector_result(
        &self,
        selector_key: SelectorResultKey,
        result: Arc<SelectorResultValue>,
    ) {
        if let Some(cache) = &self.selector_result_cache {
            CACHE_BYTES
                .with_label_values(&[SELECTOR_RESULT_TYPE])
                .add(selector_result_cache_weight(&selector_key, &result).into());
            cache.insert(selector_key, result);
        }
    }

    /// Gets the write cache.
    pub(crate) fn write_cache(&self) -> Option<&WriteCacheRef> {
        self.write_cache.as_ref()
    }

    pub(crate) fn inverted_index_cache(&self) -> Option<&InvertedIndexCacheRef> {
        self.inverted_index_cache.as_ref()
    }

    pub(crate) fn bloom_filter_index_cache(&self) -> Option<&BloomFilterIndexCacheRef> {
        self.bloom_filter_index_cache.as_ref()
    }

    pub(crate) fn puffin_metadata_cache(&self) -> Option<&PuffinMetadataCacheRef> {
        self.puffin_metadata_cache.as_ref()
    }

    pub(crate) fn index_result_cache(&self) -> Option<&IndexResultCache> {
        self.index_result_cache.as_ref()
    }
}

/// Increases selector cache miss metrics.
pub fn selector_result_cache_miss() {
    CACHE_MISS.with_label_values(&[SELECTOR_RESULT_TYPE]).inc()
}

/// Increases selector cache hit metrics.
pub fn selector_result_cache_hit() {
    CACHE_HIT.with_label_values(&[SELECTOR_RESULT_TYPE]).inc()
}

/// Builder to construct a [CacheManager].
#[derive(Default)]
pub struct CacheManagerBuilder {
    sst_meta_cache_size: u64,
    vector_cache_size: u64,
    page_cache_size: u64,
    index_metadata_size: u64,
    index_content_size: u64,
    index_content_page_size: u64,
    index_result_cache_size: u64,
    puffin_metadata_size: u64,
    write_cache: Option<WriteCacheRef>,
    selector_result_cache_size: u64,
}

impl CacheManagerBuilder {
    /// Sets meta cache size.
    pub fn sst_meta_cache_size(mut self, bytes: u64) -> Self {
        self.sst_meta_cache_size = bytes;
        self
    }

    /// Sets vector cache size.
    pub fn vector_cache_size(mut self, bytes: u64) -> Self {
        self.vector_cache_size = bytes;
        self
    }

    /// Sets page cache size.
    pub fn page_cache_size(mut self, bytes: u64) -> Self {
        self.page_cache_size = bytes;
        self
    }

    /// Sets write cache.
    pub fn write_cache(mut self, cache: Option<WriteCacheRef>) -> Self {
        self.write_cache = cache;
        self
    }

    /// Sets cache size for index metadata.
    pub fn index_metadata_size(mut self, bytes: u64) -> Self {
        self.index_metadata_size = bytes;
        self
    }

    /// Sets cache size for index content.
    pub fn index_content_size(mut self, bytes: u64) -> Self {
        self.index_content_size = bytes;
        self
    }

    /// Sets page size for index content.
    pub fn index_content_page_size(mut self, bytes: u64) -> Self {
        self.index_content_page_size = bytes;
        self
    }

    /// Sets cache size for index result.
    pub fn index_result_cache_size(mut self, bytes: u64) -> Self {
        self.index_result_cache_size = bytes;
        self
    }

    /// Sets cache size for puffin metadata.
    pub fn puffin_metadata_size(mut self, bytes: u64) -> Self {
        self.puffin_metadata_size = bytes;
        self
    }

    /// Sets selector result cache size.
    pub fn selector_result_cache_size(mut self, bytes: u64) -> Self {
        self.selector_result_cache_size = bytes;
        self
    }

    /// Builds the [CacheManager].
    pub fn build(self) -> CacheManager {
        fn to_str(cause: RemovalCause) -> &'static str {
            match cause {
                RemovalCause::Expired => "expired",
                RemovalCause::Explicit => "explicit",
                RemovalCause::Replaced => "replaced",
                RemovalCause::Size => "size",
            }
        }

        let sst_meta_cache = (self.sst_meta_cache_size != 0).then(|| {
            Cache::builder()
                .max_capacity(self.sst_meta_cache_size)
                .weigher(meta_cache_weight)
                .eviction_listener(|k, v, cause| {
                    let size = meta_cache_weight(&k, &v);
                    CACHE_BYTES
                        .with_label_values(&[SST_META_TYPE])
                        .sub(size.into());
                    CACHE_EVICTION
                        .with_label_values(&[SST_META_TYPE, to_str(cause)])
                        .inc();
                })
                .build()
        });
        let vector_cache = (self.vector_cache_size != 0).then(|| {
            Cache::builder()
                .max_capacity(self.vector_cache_size)
                .weigher(vector_cache_weight)
                .eviction_listener(|k, v, cause| {
                    let size = vector_cache_weight(&k, &v);
                    CACHE_BYTES
                        .with_label_values(&[VECTOR_TYPE])
                        .sub(size.into());
                    CACHE_EVICTION
                        .with_label_values(&[VECTOR_TYPE, to_str(cause)])
                        .inc();
                })
                .build()
        });
        let page_cache = (self.page_cache_size != 0).then(|| {
            Cache::builder()
                .max_capacity(self.page_cache_size)
                .weigher(page_cache_weight)
                .eviction_listener(|k, v, cause| {
                    let size = page_cache_weight(&k, &v);
                    CACHE_BYTES.with_label_values(&[PAGE_TYPE]).sub(size.into());
                    CACHE_EVICTION
                        .with_label_values(&[PAGE_TYPE, to_str(cause)])
                        .inc();
                })
                .build()
        });
        let inverted_index_cache = InvertedIndexCache::new(
            self.index_metadata_size,
            self.index_content_size,
            self.index_content_page_size,
        );
        // TODO(ruihang): check if it's ok to reuse the same param with inverted index
        let bloom_filter_index_cache = BloomFilterIndexCache::new(
            self.index_metadata_size,
            self.index_content_size,
            self.index_content_page_size,
        );
        let index_result_cache = (self.index_result_cache_size != 0)
            .then(|| IndexResultCache::new(self.index_result_cache_size));
        let puffin_metadata_cache =
            PuffinMetadataCache::new(self.puffin_metadata_size, &CACHE_BYTES);
        let selector_result_cache = (self.selector_result_cache_size != 0).then(|| {
            Cache::builder()
                .max_capacity(self.selector_result_cache_size)
                .weigher(selector_result_cache_weight)
                .eviction_listener(|k, v, cause| {
                    let size = selector_result_cache_weight(&k, &v);
                    CACHE_BYTES
                        .with_label_values(&[SELECTOR_RESULT_TYPE])
                        .sub(size.into());
                    CACHE_EVICTION
                        .with_label_values(&[SELECTOR_RESULT_TYPE, to_str(cause)])
                        .inc();
                })
                .build()
        });
        CacheManager {
            sst_meta_cache,
            vector_cache,
            page_cache,
            write_cache: self.write_cache,
            inverted_index_cache: Some(Arc::new(inverted_index_cache)),
            bloom_filter_index_cache: Some(Arc::new(bloom_filter_index_cache)),
            puffin_metadata_cache: Some(Arc::new(puffin_metadata_cache)),
            selector_result_cache,
            index_result_cache,
        }
    }
}

fn meta_cache_weight(k: &SstMetaKey, v: &Arc<ParquetMetaData>) -> u32 {
    // We ignore the size of `Arc`.
    (k.estimated_size() + parquet_meta_size(v)) as u32
}

fn vector_cache_weight(_k: &(ConcreteDataType, Value), v: &VectorRef) -> u32 {
    // We ignore the heap size of `Value`.
    (mem::size_of::<ConcreteDataType>() + mem::size_of::<Value>() + v.memory_size()) as u32
}

fn page_cache_weight(k: &PageKey, v: &Arc<PageValue>) -> u32 {
    (k.estimated_size() + v.estimated_size()) as u32
}

fn selector_result_cache_weight(k: &SelectorResultKey, v: &Arc<SelectorResultValue>) -> u32 {
    (mem::size_of_val(k) + v.estimated_size()) as u32
}

/// Updates cache hit/miss metrics.
fn update_hit_miss<T>(value: Option<T>, cache_type: &str) -> Option<T> {
    if value.is_some() {
        CACHE_HIT.with_label_values(&[cache_type]).inc();
    } else {
        CACHE_MISS.with_label_values(&[cache_type]).inc();
    }
    value
}

/// Cache key (region id, file id) for SST meta.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SstMetaKey(RegionId, FileId);

impl SstMetaKey {
    /// Returns memory used by the key (estimated).
    fn estimated_size(&self) -> usize {
        mem::size_of::<Self>()
    }
}

/// Path to column pages in the SST file.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ColumnPagePath {
    /// Region id of the SST file to cache.
    region_id: RegionId,
    /// Id of the SST file to cache.
    file_id: FileId,
    /// Index of the row group.
    row_group_idx: usize,
    /// Index of the column in the row group.
    column_idx: usize,
}

/// Cache key to pages in a row group (after projection).
///
/// Different projections will have different cache keys.
/// We cache all ranges together because they may refer to the same `Bytes`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PageKey {
    /// Id of the SST file to cache.
    file_id: FileId,
    /// Index of the row group.
    row_group_idx: usize,
    /// Byte ranges of the pages to cache.
    ranges: Vec<Range<u64>>,
}

impl PageKey {
    /// Creates a key for a list of pages.
    pub fn new(file_id: FileId, row_group_idx: usize, ranges: Vec<Range<u64>>) -> PageKey {
        PageKey {
            file_id,
            row_group_idx,
            ranges,
        }
    }

    /// Returns memory used by the key (estimated).
    fn estimated_size(&self) -> usize {
        mem::size_of::<Self>() + mem::size_of_val(self.ranges.as_slice())
    }
}

/// Cached row group pages for a column.
// We don't use enum here to make it easier to mock and use the struct.
#[derive(Default)]
pub struct PageValue {
    /// Compressed page in the row group.
    pub compressed: Vec<Bytes>,
    /// Total size of the pages (may be larger than sum of compressed bytes due to gaps).
    pub page_size: u64,
}

impl PageValue {
    /// Creates a new value from a range of compressed pages.
    pub fn new(bytes: Vec<Bytes>, page_size: u64) -> PageValue {
        PageValue {
            compressed: bytes,
            page_size,
        }
    }

    /// Returns memory used by the value (estimated).
    fn estimated_size(&self) -> usize {
        mem::size_of::<Self>()
            + self.page_size as usize
            + self.compressed.iter().map(mem::size_of_val).sum::<usize>()
    }
}

/// Cache key for time series row selector result.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SelectorResultKey {
    /// Id of the SST file.
    pub file_id: FileId,
    /// Index of the row group.
    pub row_group_idx: usize,
    /// Time series row selector.
    pub selector: TimeSeriesRowSelector,
}

/// Cached result for time series row selector.
pub struct SelectorResultValue {
    /// Batches of rows selected by the selector.
    pub result: Vec<Batch>,
    /// Projection of rows.
    pub projection: Vec<usize>,
}

impl SelectorResultValue {
    /// Creates a new selector result value.
    pub fn new(result: Vec<Batch>, projection: Vec<usize>) -> SelectorResultValue {
        SelectorResultValue { result, projection }
    }

    /// Returns memory used by the value (estimated).
    fn estimated_size(&self) -> usize {
        // We only consider heap size of all batches.
        self.result.iter().map(|batch| batch.memory_size()).sum()
    }
}

/// Maps (region id, file id) to [ParquetMetaData].
type SstMetaCache = Cache<SstMetaKey, Arc<ParquetMetaData>>;
/// Maps [Value] to a vector that holds this value repeatedly.
///
/// e.g. `"hello" => ["hello", "hello", "hello"]`
type VectorCache = Cache<(ConcreteDataType, Value), VectorRef>;
/// Maps (region, file, row group, column) to [PageValue].
type PageCache = Cache<PageKey, Arc<PageValue>>;
/// Maps (file id, row group id, time series row selector) to [SelectorResultValue].
type SelectorResultCache = Cache<SelectorResultKey, Arc<SelectorResultValue>>;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::v1::index::{BloomFilterMeta, InvertedIndexMetas};
    use datatypes::vectors::Int64Vector;
    use puffin::file_metadata::FileMetadata;
    use store_api::storage::ColumnId;

    use super::*;
    use crate::cache::index::bloom_filter_index::Tag;
    use crate::cache::index::result_cache::PredicateKey;
    use crate::cache::test_util::parquet_meta;
    use crate::sst::parquet::row_selection::RowGroupSelection;

    #[tokio::test]
    async fn test_disable_cache() {
        let cache = CacheManager::default();
        assert!(cache.sst_meta_cache.is_none());
        assert!(cache.vector_cache.is_none());
        assert!(cache.page_cache.is_none());

        let region_id = RegionId::new(1, 1);
        let file_id = RegionFileId::new(region_id, FileId::random());
        let metadata = parquet_meta();
        let mut metrics = MetadataCacheMetrics::default();
        cache.put_parquet_meta_data(file_id, metadata);
        assert!(
            cache
                .get_parquet_meta_data(file_id, &mut metrics)
                .await
                .is_none()
        );

        let value = Value::Int64(10);
        let vector: VectorRef = Arc::new(Int64Vector::from_slice([10, 10, 10, 10]));
        cache.put_repeated_vector(value.clone(), vector.clone());
        assert!(
            cache
                .get_repeated_vector(&ConcreteDataType::int64_datatype(), &value)
                .is_none()
        );

        let key = PageKey::new(file_id.file_id(), 1, vec![Range { start: 0, end: 5 }]);
        let pages = Arc::new(PageValue::default());
        cache.put_pages(key.clone(), pages);
        assert!(cache.get_pages(&key).is_none());

        assert!(cache.write_cache().is_none());
    }

    #[tokio::test]
    async fn test_parquet_meta_cache() {
        let cache = CacheManager::builder().sst_meta_cache_size(2000).build();
        let mut metrics = MetadataCacheMetrics::default();
        let region_id = RegionId::new(1, 1);
        let file_id = RegionFileId::new(region_id, FileId::random());
        assert!(
            cache
                .get_parquet_meta_data(file_id, &mut metrics)
                .await
                .is_none()
        );
        let metadata = parquet_meta();
        cache.put_parquet_meta_data(file_id, metadata);
        assert!(
            cache
                .get_parquet_meta_data(file_id, &mut metrics)
                .await
                .is_some()
        );
        cache.remove_parquet_meta_data(file_id);
        assert!(
            cache
                .get_parquet_meta_data(file_id, &mut metrics)
                .await
                .is_none()
        );
    }

    #[test]
    fn test_repeated_vector_cache() {
        let cache = CacheManager::builder().vector_cache_size(4096).build();
        let value = Value::Int64(10);
        assert!(
            cache
                .get_repeated_vector(&ConcreteDataType::int64_datatype(), &value)
                .is_none()
        );
        let vector: VectorRef = Arc::new(Int64Vector::from_slice([10, 10, 10, 10]));
        cache.put_repeated_vector(value.clone(), vector.clone());
        let cached = cache
            .get_repeated_vector(&ConcreteDataType::int64_datatype(), &value)
            .unwrap();
        assert_eq!(vector, cached);
    }

    #[test]
    fn test_page_cache() {
        let cache = CacheManager::builder().page_cache_size(1000).build();
        let file_id = FileId::random();
        let key = PageKey::new(file_id, 0, vec![(0..10), (10..20)]);
        assert!(cache.get_pages(&key).is_none());
        let pages = Arc::new(PageValue::default());
        cache.put_pages(key.clone(), pages);
        assert!(cache.get_pages(&key).is_some());
    }

    #[test]
    fn test_selector_result_cache() {
        let cache = CacheManager::builder()
            .selector_result_cache_size(1000)
            .build();
        let file_id = FileId::random();
        let key = SelectorResultKey {
            file_id,
            row_group_idx: 0,
            selector: TimeSeriesRowSelector::LastRow,
        };
        assert!(cache.get_selector_result(&key).is_none());
        let result = Arc::new(SelectorResultValue::new(Vec::new(), Vec::new()));
        cache.put_selector_result(key, result);
        assert!(cache.get_selector_result(&key).is_some());
    }

    #[tokio::test]
    async fn test_evict_puffin_cache_clears_all_entries() {
        use std::collections::{BTreeMap, HashMap};

        let cache = CacheManager::builder()
            .index_metadata_size(128)
            .index_content_size(128)
            .index_content_page_size(64)
            .index_result_cache_size(128)
            .puffin_metadata_size(128)
            .build();
        let cache = Arc::new(cache);

        let region_id = RegionId::new(1, 1);
        let index_id = RegionIndexId::new(RegionFileId::new(region_id, FileId::random()), 0);
        let column_id: ColumnId = 1;

        let bloom_cache = cache.bloom_filter_index_cache().unwrap().clone();
        let inverted_cache = cache.inverted_index_cache().unwrap().clone();
        let result_cache = cache.index_result_cache().unwrap();
        let puffin_metadata_cache = cache.puffin_metadata_cache().unwrap().clone();

        let bloom_key = (
            index_id.file_id(),
            index_id.version,
            column_id,
            Tag::Skipping,
        );
        bloom_cache.put_metadata(bloom_key, Arc::new(BloomFilterMeta::default()));
        inverted_cache.put_metadata(
            (index_id.file_id(), index_id.version),
            Arc::new(InvertedIndexMetas::default()),
        );
        let predicate = PredicateKey::new_bloom(Arc::new(BTreeMap::new()));
        let selection = Arc::new(RowGroupSelection::default());
        result_cache.put(predicate.clone(), index_id.file_id(), selection);
        let file_id_str = index_id.to_string();
        let metadata = Arc::new(FileMetadata {
            blobs: Vec::new(),
            properties: HashMap::new(),
        });
        puffin_metadata_cache.put_metadata(file_id_str.clone(), metadata);

        assert!(bloom_cache.get_metadata(bloom_key).is_some());
        assert!(
            inverted_cache
                .get_metadata((index_id.file_id(), index_id.version))
                .is_some()
        );
        assert!(result_cache.get(&predicate, index_id.file_id()).is_some());
        assert!(puffin_metadata_cache.get_metadata(&file_id_str).is_some());

        cache.evict_puffin_cache(index_id).await;

        assert!(bloom_cache.get_metadata(bloom_key).is_none());
        assert!(
            inverted_cache
                .get_metadata((index_id.file_id(), index_id.version))
                .is_none()
        );
        assert!(result_cache.get(&predicate, index_id.file_id()).is_none());
        assert!(puffin_metadata_cache.get_metadata(&file_id_str).is_none());

        // Refill caches and evict via CacheStrategy to ensure delegation works.
        bloom_cache.put_metadata(bloom_key, Arc::new(BloomFilterMeta::default()));
        inverted_cache.put_metadata(
            (index_id.file_id(), index_id.version),
            Arc::new(InvertedIndexMetas::default()),
        );
        result_cache.put(
            predicate.clone(),
            index_id.file_id(),
            Arc::new(RowGroupSelection::default()),
        );
        puffin_metadata_cache.put_metadata(
            file_id_str.clone(),
            Arc::new(FileMetadata {
                blobs: Vec::new(),
                properties: HashMap::new(),
            }),
        );

        let strategy = CacheStrategy::EnableAll(cache.clone());
        strategy.evict_puffin_cache(index_id).await;

        assert!(bloom_cache.get_metadata(bloom_key).is_none());
        assert!(
            inverted_cache
                .get_metadata((index_id.file_id(), index_id.version))
                .is_none()
        );
        assert!(result_cache.get(&predicate, index_id.file_id()).is_none());
        assert!(puffin_metadata_cache.get_metadata(&file_id_str).is_none());
    }
}
