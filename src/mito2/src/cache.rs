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

pub(crate) mod cache_size;

pub(crate) mod file_cache;
pub(crate) mod index;
pub(crate) mod manifest_cache;
#[cfg(test)]
pub(crate) mod test_util;
pub(crate) mod write_cache;

use std::collections::BTreeMap;
use std::mem;
use std::ops::Range;
use std::sync::{Arc, RwLock};

use bytes::Bytes;
use common_base::readable_size::ReadableSize;
use common_telemetry::warn;
use dashmap::DashMap;
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::value::Value;
use datatypes::vectors::VectorRef;
use index::bloom_filter_index::{BloomFilterIndexCache, BloomFilterIndexCacheRef};
use index::result_cache::IndexResultCache;
use moka::notification::RemovalCause;
use moka::sync::Cache;
use object_store::ObjectStore;
use parquet::file::metadata::{FileMetaData, PageIndexPolicy, ParquetMetaData};
use puffin::puffin_manager::cache::{PuffinMetadataCache, PuffinMetadataCacheRef};
use snafu::{OptionExt, ResultExt};
use store_api::metadata::RegionMetadataRef;
use store_api::storage::{ConcreteDataType, FileId, RegionId, TimeSeriesRowSelector};

use crate::cache::cache_size::parquet_meta_size;
use crate::cache::file_cache::{FileType, IndexKey};
use crate::cache::index::inverted_index::{InvertedIndexCache, InvertedIndexCacheRef};
#[cfg(feature = "vector_index")]
use crate::cache::index::vector_index::{VectorIndexCache, VectorIndexCacheRef};
use crate::cache::write_cache::WriteCacheRef;
use crate::error::{InvalidMetadataSnafu, InvalidParquetSnafu, Result, UnexpectedSnafu};
use crate::memtable::record_batch_estimated_size;
use crate::metrics::{CACHE_BYTES, CACHE_EVICTION, CACHE_HIT, CACHE_MISS};
use crate::read::Batch;
use crate::read::range_cache::{RangeScanCacheKey, RangeScanCacheValue};
use crate::sst::file::{RegionFileId, RegionIndexId};
use crate::sst::parquet::PARQUET_METADATA_KEY;
use crate::sst::parquet::read_columns::ParquetReadColumns;
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
/// Metrics type key for range scan result cache.
const RANGE_RESULT_TYPE: &str = "range_result";
const RANGE_RESULT_CONCAT_MEMORY_LIMIT: ReadableSize = ReadableSize::mb(512);
const RANGE_RESULT_CONCAT_MEMORY_PERMIT: ReadableSize = ReadableSize::kb(1);

#[derive(Debug)]
pub(crate) struct RangeResultMemoryLimiter {
    semaphore: Arc<tokio::sync::Semaphore>,
    permit_bytes: usize,
    total_permits: usize,
}

impl Default for RangeResultMemoryLimiter {
    fn default() -> Self {
        Self::new(
            RANGE_RESULT_CONCAT_MEMORY_LIMIT.as_bytes() as usize,
            RANGE_RESULT_CONCAT_MEMORY_PERMIT.as_bytes() as usize,
        )
    }
}

impl RangeResultMemoryLimiter {
    pub(crate) fn new(limit_bytes: usize, permit_bytes: usize) -> Self {
        let permit_bytes = permit_bytes.max(1);
        let total_permits = limit_bytes
            .div_ceil(permit_bytes)
            .clamp(1, tokio::sync::Semaphore::MAX_PERMITS);
        Self {
            semaphore: Arc::new(tokio::sync::Semaphore::new(total_permits)),
            permit_bytes,
            total_permits,
        }
    }

    #[cfg(test)]
    pub(crate) fn permit_bytes(&self) -> usize {
        self.permit_bytes
    }

    #[cfg(test)]
    pub(crate) fn available_permits(&self) -> usize {
        self.semaphore.available_permits()
    }

    pub(crate) async fn acquire(&self, bytes: usize) -> Result<tokio::sync::SemaphorePermit<'_>> {
        let permits = bytes.div_ceil(self.permit_bytes).max(1);
        if permits > self.total_permits {
            return UnexpectedSnafu {
                reason: format!(
                    "range result memory request of {bytes} bytes exceeds limiter capacity of {} bytes",
                    self.total_permits.saturating_mul(self.permit_bytes)
                ),
            }
            .fail();
        }
        self.semaphore
            .acquire_many(permits as u32)
            .await
            .map_err(|_| {
                UnexpectedSnafu {
                    reason: "range result memory limiter is unexpectedly closed",
                }
                .build()
            })
    }
}

/// Cached SST metadata combines the parquet footer with the decoded region metadata.
///
/// The cached parquet footer strips the `greptime:metadata` JSON payload and stores the decoded
/// [RegionMetadata] separately so readers can skip repeated deserialization work.
#[derive(Debug)]
pub(crate) struct CachedSstMeta {
    parquet_metadata: Arc<ParquetMetaData>,
    region_metadata: RegionMetadataRef,
    region_metadata_weight: usize,
}

impl CachedSstMeta {
    pub(crate) fn try_new(file_path: &str, parquet_metadata: ParquetMetaData) -> Result<Self> {
        Self::try_new_with_region_metadata(file_path, parquet_metadata, None)
    }

    pub(crate) fn try_new_with_region_metadata(
        file_path: &str,
        parquet_metadata: ParquetMetaData,
        region_metadata: Option<RegionMetadataRef>,
    ) -> Result<Self> {
        let file_metadata = parquet_metadata.file_metadata();
        let key_values = file_metadata
            .key_value_metadata()
            .context(InvalidParquetSnafu {
                file: file_path,
                reason: "missing key value meta",
            })?;
        let meta_value = key_values
            .iter()
            .find(|kv| kv.key == PARQUET_METADATA_KEY)
            .with_context(|| InvalidParquetSnafu {
                file: file_path,
                reason: format!("key {} not found", PARQUET_METADATA_KEY),
            })?;
        let json = meta_value
            .value
            .as_ref()
            .with_context(|| InvalidParquetSnafu {
                file: file_path,
                reason: format!("No value for key {}", PARQUET_METADATA_KEY),
            })?;
        let region_metadata = match region_metadata {
            Some(region_metadata) => region_metadata,
            None => Arc::new(
                store_api::metadata::RegionMetadata::from_json(json)
                    .context(InvalidMetadataSnafu)?,
            ),
        };
        // Keep the previous JSON-byte floor and charge the decoded structures as well.
        let region_metadata_weight = region_metadata.estimated_size().max(json.len());
        let parquet_metadata = Arc::new(strip_region_metadata_from_parquet(parquet_metadata));

        Ok(Self {
            parquet_metadata,
            region_metadata,
            region_metadata_weight,
        })
    }

    pub(crate) fn parquet_metadata(&self) -> Arc<ParquetMetaData> {
        self.parquet_metadata.clone()
    }

    pub(crate) fn region_metadata(&self) -> RegionMetadataRef {
        self.region_metadata.clone()
    }
}

fn strip_region_metadata_from_parquet(parquet_metadata: ParquetMetaData) -> ParquetMetaData {
    let file_metadata = parquet_metadata.file_metadata();
    let filtered_key_values = file_metadata.key_value_metadata().and_then(|key_values| {
        let filtered = key_values
            .iter()
            .filter(|kv| kv.key != PARQUET_METADATA_KEY)
            .cloned()
            .collect::<Vec<_>>();
        (!filtered.is_empty()).then_some(filtered)
    });
    let stripped_file_metadata = FileMetaData::new(
        file_metadata.version(),
        file_metadata.num_rows(),
        file_metadata.created_by().map(ToString::to_string),
        filtered_key_values,
        file_metadata.schema_descr_ptr(),
        file_metadata.column_orders().cloned(),
    );

    let mut builder = parquet_metadata.into_builder();
    let row_groups = builder.take_row_groups();
    let column_index = builder.take_column_index();
    let offset_index = builder.take_offset_index();

    parquet::file::metadata::ParquetMetaDataBuilder::new(stripped_file_metadata)
        .set_row_groups(row_groups)
        .set_column_index(column_index)
        .set_offset_index(offset_index)
        .build()
}

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
    /// Gets fused SST metadata with cache metrics tracking.
    pub(crate) async fn get_sst_meta_data(
        &self,
        file_id: RegionFileId,
        metrics: &mut MetadataCacheMetrics,
        page_index_policy: PageIndexPolicy,
    ) -> Option<Arc<CachedSstMeta>> {
        match self {
            CacheStrategy::EnableAll(cache_manager) | CacheStrategy::Compaction(cache_manager) => {
                cache_manager
                    .get_sst_meta_data(file_id, metrics, page_index_policy)
                    .await
            }
            CacheStrategy::Disabled => {
                metrics.cache_miss += 1;
                None
            }
        }
    }

    /// Calls [CacheManager::get_sst_meta_data_from_mem_cache()].
    pub(crate) fn get_sst_meta_data_from_mem_cache(
        &self,
        file_id: RegionFileId,
    ) -> Option<Arc<CachedSstMeta>> {
        match self {
            CacheStrategy::EnableAll(cache_manager) | CacheStrategy::Compaction(cache_manager) => {
                cache_manager.get_sst_meta_data_from_mem_cache(file_id)
            }
            CacheStrategy::Disabled => None,
        }
    }

    /// Calls [CacheManager::get_parquet_meta_data_from_mem_cache()].
    pub fn get_parquet_meta_data_from_mem_cache(
        &self,
        file_id: RegionFileId,
    ) -> Option<Arc<ParquetMetaData>> {
        self.get_sst_meta_data_from_mem_cache(file_id)
            .map(|metadata| metadata.parquet_metadata())
    }

    /// Calls [CacheManager::put_sst_meta_data()].
    pub(crate) fn put_sst_meta_data(&self, file_id: RegionFileId, metadata: Arc<CachedSstMeta>) {
        match self {
            CacheStrategy::EnableAll(cache_manager) | CacheStrategy::Compaction(cache_manager) => {
                cache_manager.put_sst_meta_data(file_id, metadata);
            }
            CacheStrategy::Disabled => {}
        }
    }

    /// Calls [CacheManager::put_parquet_meta_data()].
    pub fn put_parquet_meta_data(
        &self,
        file_id: RegionFileId,
        metadata: Arc<ParquetMetaData>,
        region_metadata: Option<RegionMetadataRef>,
    ) {
        match self {
            CacheStrategy::EnableAll(cache_manager) | CacheStrategy::Compaction(cache_manager) => {
                cache_manager.put_parquet_meta_data(file_id, metadata, region_metadata);
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

    /// Calls [CacheManager::get_page_ranges()].
    /// It returns None if the strategy is [CacheStrategy::Compaction] or [CacheStrategy::Disabled].
    pub fn get_page_ranges(
        &self,
        file_id: FileId,
        row_group_idx: usize,
        ranges: &[Range<u64>],
    ) -> Option<PageRangeLookup> {
        match self {
            CacheStrategy::EnableAll(cache_manager) => {
                cache_manager.get_page_ranges(file_id, row_group_idx, ranges)
            }
            CacheStrategy::Compaction(_) | CacheStrategy::Disabled => None,
        }
    }

    /// Calls [CacheManager::put_page_ranges()].
    /// It does nothing if the strategy isn't [CacheStrategy::EnableAll].
    pub fn put_page_ranges(
        &self,
        file_id: FileId,
        row_group_idx: usize,
        ranges: &[Range<u64>],
        pages: &[Bytes],
    ) {
        if let CacheStrategy::EnableAll(cache_manager) = self {
            cache_manager.put_page_ranges(file_id, row_group_idx, ranges, pages);
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

    /// Calls [CacheManager::get_range_result()].
    /// It returns None if the strategy is [CacheStrategy::Compaction] or [CacheStrategy::Disabled].
    #[allow(dead_code)]
    pub(crate) fn get_range_result(
        &self,
        key: &RangeScanCacheKey,
    ) -> Option<Arc<RangeScanCacheValue>> {
        match self {
            CacheStrategy::EnableAll(cache_manager) => cache_manager.get_range_result(key),
            CacheStrategy::Compaction(_) | CacheStrategy::Disabled => None,
        }
    }

    /// Calls [CacheManager::put_range_result()].
    /// It does nothing if the strategy isn't [CacheStrategy::EnableAll].
    pub(crate) fn put_range_result(
        &self,
        key: RangeScanCacheKey,
        result: Arc<RangeScanCacheValue>,
    ) {
        if let CacheStrategy::EnableAll(cache_manager) = self {
            cache_manager.put_range_result(key, result);
        }
    }

    /// Returns true if the range result cache is enabled.
    pub(crate) fn has_range_result_cache(&self) -> bool {
        match self {
            CacheStrategy::EnableAll(cache_manager) => cache_manager.has_range_result_cache(),
            CacheStrategy::Compaction(_) | CacheStrategy::Disabled => false,
        }
    }

    pub(crate) fn range_result_memory_limiter(&self) -> Option<&Arc<RangeResultMemoryLimiter>> {
        match self {
            CacheStrategy::EnableAll(cache_manager) => {
                Some(cache_manager.range_result_memory_limiter())
            }
            CacheStrategy::Compaction(_) | CacheStrategy::Disabled => None,
        }
    }

    pub(crate) fn range_result_cache_size(&self) -> Option<usize> {
        match self {
            CacheStrategy::EnableAll(cache_manager) => {
                Some(cache_manager.range_result_cache_size())
            }
            CacheStrategy::Compaction(_) | CacheStrategy::Disabled => None,
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

    /// Calls [CacheManager::vector_index_cache()].
    /// It returns None if the strategy is [CacheStrategy::Compaction] or [CacheStrategy::Disabled].
    #[cfg(feature = "vector_index")]
    pub fn vector_index_cache(&self) -> Option<&VectorIndexCacheRef> {
        match self {
            CacheStrategy::EnableAll(cache_manager) => cache_manager.vector_index_cache(),
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
    /// Cache for SST byte ranges.
    page_cache: Option<Arc<PageRangeCache>>,
    /// A Cache for writing files to object stores.
    write_cache: Option<WriteCacheRef>,
    /// Cache for inverted index.
    inverted_index_cache: Option<InvertedIndexCacheRef>,
    /// Cache for bloom filter index.
    bloom_filter_index_cache: Option<BloomFilterIndexCacheRef>,
    /// Cache for vector index.
    #[cfg(feature = "vector_index")]
    vector_index_cache: Option<VectorIndexCacheRef>,
    /// Puffin metadata cache.
    puffin_metadata_cache: Option<PuffinMetadataCacheRef>,
    /// Cache for time series selectors.
    selector_result_cache: Option<SelectorResultCache>,
    /// Cache for range scan outputs in flat format.
    range_result_cache: Option<RangeResultCache>,
    /// Configured capacity for range scan outputs in flat format.
    range_result_cache_size: u64,
    /// Shared memory limiter for async range-result cache tasks.
    range_result_memory_limiter: Arc<RangeResultMemoryLimiter>,
    /// Cache for index result.
    index_result_cache: Option<IndexResultCache>,
}

pub type CacheManagerRef = Arc<CacheManager>;

impl CacheManager {
    /// Returns a builder to build the cache.
    pub fn builder() -> CacheManagerBuilder {
        CacheManagerBuilder::default()
    }

    /// Gets fused SST metadata with metrics tracking.
    /// Tries in-memory cache first, then file cache, updating metrics accordingly.
    pub(crate) async fn get_sst_meta_data(
        &self,
        file_id: RegionFileId,
        metrics: &mut MetadataCacheMetrics,
        page_index_policy: PageIndexPolicy,
    ) -> Option<Arc<CachedSstMeta>> {
        if let Some(metadata) = self.get_sst_meta_data_from_mem_cache(file_id) {
            metrics.mem_cache_hit += 1;
            return Some(metadata);
        }

        let key = IndexKey::new(file_id.region_id(), file_id.file_id(), FileType::Parquet);
        if let Some(write_cache) = &self.write_cache
            && let Some(metadata) = write_cache
                .file_cache()
                .get_sst_meta_data(key, metrics, page_index_policy)
                .await
        {
            metrics.file_cache_hit += 1;
            self.put_sst_meta_data(file_id, metadata.clone());
            return Some(metadata);
        }

        metrics.cache_miss += 1;
        None
    }

    /// Gets cached [ParquetMetaData] with metrics tracking.
    /// Tries in-memory cache first, then file cache, updating metrics accordingly.
    pub(crate) async fn get_parquet_meta_data(
        &self,
        file_id: RegionFileId,
        metrics: &mut MetadataCacheMetrics,
        page_index_policy: PageIndexPolicy,
    ) -> Option<Arc<ParquetMetaData>> {
        self.get_sst_meta_data(file_id, metrics, page_index_policy)
            .await
            .map(|metadata| metadata.parquet_metadata())
    }

    /// Gets cached fused SST metadata from in-memory cache.
    /// This method does not perform I/O.
    pub(crate) fn get_sst_meta_data_from_mem_cache(
        &self,
        file_id: RegionFileId,
    ) -> Option<Arc<CachedSstMeta>> {
        self.sst_meta_cache.as_ref().and_then(|sst_meta_cache| {
            let value = sst_meta_cache.get(&SstMetaKey(file_id.region_id(), file_id.file_id()));
            update_hit_miss(value, SST_META_TYPE)
        })
    }

    /// Gets cached [ParquetMetaData] from in-memory cache.
    /// This method does not perform I/O.
    pub fn get_parquet_meta_data_from_mem_cache(
        &self,
        file_id: RegionFileId,
    ) -> Option<Arc<ParquetMetaData>> {
        self.get_sst_meta_data_from_mem_cache(file_id)
            .map(|metadata| metadata.parquet_metadata())
    }

    /// Puts fused SST metadata into the cache.
    pub(crate) fn put_sst_meta_data(&self, file_id: RegionFileId, metadata: Arc<CachedSstMeta>) {
        if let Some(cache) = &self.sst_meta_cache {
            let key = SstMetaKey(file_id.region_id(), file_id.file_id());
            CACHE_BYTES
                .with_label_values(&[SST_META_TYPE])
                .add(meta_cache_weight(&key, &metadata).into());
            cache.insert(key, metadata);
        }
    }

    /// Puts [ParquetMetaData] into the cache.
    pub fn put_parquet_meta_data(
        &self,
        file_id: RegionFileId,
        metadata: Arc<ParquetMetaData>,
        region_metadata: Option<RegionMetadataRef>,
    ) {
        if self.sst_meta_cache.is_some() {
            let file_path = format!(
                "region_id={}, file_id={}",
                file_id.region_id(),
                file_id.file_id()
            );
            match CachedSstMeta::try_new_with_region_metadata(
                &file_path,
                Arc::unwrap_or_clone(metadata),
                region_metadata,
            ) {
                Ok(metadata) => self.put_sst_meta_data(file_id, Arc::new(metadata)),
                Err(err) => warn!(
                    err; "Failed to decode region metadata while caching parquet metadata, region_id: {}, file_id: {}",
                    file_id.region_id(),
                    file_id.file_id()
                ),
            }
        }
    }

    /// Removes [ParquetMetaData] from the cache.
    pub fn remove_parquet_meta_data(&self, file_id: RegionFileId) {
        if let Some(cache) = &self.sst_meta_cache {
            cache.remove(&SstMetaKey(file_id.region_id(), file_id.file_id()));
        }
    }

    /// Returns the total weighted size of the in-memory SST meta cache.
    pub(crate) fn sst_meta_cache_weighted_size(&self) -> u64 {
        self.sst_meta_cache
            .as_ref()
            .map(|cache| cache.weighted_size())
            .unwrap_or(0)
    }

    /// Returns true if the in-memory SST meta cache is enabled.
    pub(crate) fn sst_meta_cache_enabled(&self) -> bool {
        self.sst_meta_cache.is_some()
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

    /// Gets cached byte fragments for the requested ranges.
    pub fn get_page_ranges(
        &self,
        file_id: FileId,
        row_group_idx: usize,
        ranges: &[Range<u64>],
    ) -> Option<PageRangeLookup> {
        self.page_cache.as_ref().map(|page_cache| {
            let lookup = page_cache.lookup(file_id, row_group_idx, ranges);
            if lookup.cached_bytes > 0 {
                CACHE_HIT.with_label_values(&[PAGE_TYPE]).inc();
            }
            if !lookup.missing_ranges.is_empty() {
                CACHE_MISS.with_label_values(&[PAGE_TYPE]).inc();
            }
            lookup
        })
    }

    /// Puts byte fragments into the page cache.
    pub fn put_page_ranges(
        &self,
        file_id: FileId,
        row_group_idx: usize,
        ranges: &[Range<u64>],
        pages: &[Bytes],
    ) {
        if let Some(cache) = &self.page_cache {
            cache.insert_ranges(file_id, row_group_idx, ranges, pages);
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

        #[cfg(feature = "vector_index")]
        if let Some(cache) = &self.vector_index_cache {
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

    /// Gets cached result for range scan.
    #[allow(dead_code)]
    pub(crate) fn get_range_result(
        &self,
        key: &RangeScanCacheKey,
    ) -> Option<Arc<RangeScanCacheValue>> {
        self.range_result_cache
            .as_ref()
            .and_then(|cache| update_hit_miss(cache.get(key), RANGE_RESULT_TYPE))
    }

    /// Puts range scan result into cache.
    pub(crate) fn put_range_result(
        &self,
        key: RangeScanCacheKey,
        result: Arc<RangeScanCacheValue>,
    ) {
        if let Some(cache) = &self.range_result_cache {
            CACHE_BYTES
                .with_label_values(&[RANGE_RESULT_TYPE])
                .add(range_result_cache_weight(&key, &result).into());
            cache.insert(key, result);
        }
    }

    /// Returns true if the range result cache is enabled.
    pub(crate) fn has_range_result_cache(&self) -> bool {
        self.range_result_cache.is_some()
    }

    pub(crate) fn range_result_memory_limiter(&self) -> &Arc<RangeResultMemoryLimiter> {
        &self.range_result_memory_limiter
    }

    pub(crate) fn range_result_cache_size(&self) -> usize {
        self.range_result_cache_size as usize
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

    #[cfg(feature = "vector_index")]
    pub(crate) fn vector_index_cache(&self) -> Option<&VectorIndexCacheRef> {
        self.vector_index_cache.as_ref()
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
    range_result_cache_size: u64,
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

    /// Sets range result cache size.
    pub fn range_result_cache_size(mut self, bytes: u64) -> Self {
        self.range_result_cache_size = bytes;
        self
    }

    /// Builds the [CacheManager].
    pub fn build(self) -> CacheManager {
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
                        .with_label_values(&[SST_META_TYPE, removal_cause_to_str(cause)])
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
                        .with_label_values(&[VECTOR_TYPE, removal_cause_to_str(cause)])
                        .inc();
                })
                .build()
        });
        let page_cache =
            (self.page_cache_size != 0).then(|| PageRangeCache::new(self.page_cache_size));
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
        #[cfg(feature = "vector_index")]
        let vector_index_cache = (self.index_content_size != 0)
            .then(|| Arc::new(VectorIndexCache::new(self.index_content_size)));
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
                        .with_label_values(&[SELECTOR_RESULT_TYPE, removal_cause_to_str(cause)])
                        .inc();
                })
                .build()
        });
        let range_result_cache = (self.range_result_cache_size != 0).then(|| {
            Cache::builder()
                .max_capacity(self.range_result_cache_size)
                .weigher(range_result_cache_weight)
                .eviction_listener(move |k, v, cause| {
                    let size = range_result_cache_weight(&k, &v);
                    CACHE_BYTES
                        .with_label_values(&[RANGE_RESULT_TYPE])
                        .sub(size.into());
                    CACHE_EVICTION
                        .with_label_values(&[RANGE_RESULT_TYPE, removal_cause_to_str(cause)])
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
            #[cfg(feature = "vector_index")]
            vector_index_cache,
            puffin_metadata_cache: Some(Arc::new(puffin_metadata_cache)),
            selector_result_cache,
            range_result_cache,
            range_result_cache_size: self.range_result_cache_size,
            range_result_memory_limiter: Arc::new(RangeResultMemoryLimiter::new(
                self.range_result_cache_size as usize,
                RANGE_RESULT_CONCAT_MEMORY_PERMIT.as_bytes() as usize,
            )),
            index_result_cache,
        }
    }
}

fn meta_cache_weight(k: &SstMetaKey, v: &Arc<CachedSstMeta>) -> u32 {
    // We ignore the size of `Arc`.
    (k.estimated_size() + parquet_meta_size(&v.parquet_metadata) + v.region_metadata_weight) as u32
}

fn vector_cache_weight(_k: &(ConcreteDataType, Value), v: &VectorRef) -> u32 {
    // We ignore the heap size of `Value`.
    (mem::size_of::<ConcreteDataType>() + mem::size_of::<Value>() + v.memory_size()) as u32
}

fn page_cache_weight(k: &PageFragmentKey, v: &Bytes) -> u32 {
    (k.estimated_size() + mem::size_of::<Bytes>() + v.len()) as u32
}

fn selector_result_cache_weight(k: &SelectorResultKey, v: &Arc<SelectorResultValue>) -> u32 {
    (mem::size_of_val(k) + v.estimated_size()) as u32
}

fn range_result_cache_weight(k: &RangeScanCacheKey, v: &Arc<RangeScanCacheValue>) -> u32 {
    (k.estimated_size() + v.estimated_size()) as u32
}

fn removal_cause_to_str(cause: RemovalCause) -> &'static str {
    match cause {
        RemovalCause::Expired => "expired",
        RemovalCause::Explicit => "explicit",
        RemovalCause::Replaced => "replaced",
        RemovalCause::Size => "size",
    }
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct PageFragmentGroupKey {
    file_id: FileId,
    row_group_idx: usize,
}

/// Cache key for one byte fragment in an SST row group.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PageFragmentKey {
    /// Id of the SST file.
    file_id: FileId,
    /// Index of the row group.
    row_group_idx: usize,
    /// Start offset of the cached byte fragment.
    start: u64,
    /// End offset of the cached byte fragment.
    end: u64,
}

impl PageFragmentKey {
    fn new(file_id: FileId, row_group_idx: usize, range: &Range<u64>) -> PageFragmentKey {
        PageFragmentKey {
            file_id,
            row_group_idx,
            start: range.start,
            end: range.end,
        }
    }

    fn group_key(&self) -> PageFragmentGroupKey {
        PageFragmentGroupKey {
            file_id: self.file_id,
            row_group_idx: self.row_group_idx,
        }
    }

    /// Returns memory used by the key (estimated).
    fn estimated_size(&self) -> usize {
        mem::size_of::<Self>()
    }
}

/// One cached byte fragment that overlaps a requested range.
#[derive(Clone)]
pub struct PageRangePart {
    /// Range covered by `bytes`.
    pub range: Range<u64>,
    /// Bytes for `range`.
    pub bytes: Bytes,
}

/// Result of looking up request ranges in the page range cache.
pub struct PageRangeLookup {
    /// Cached fragments grouped by the original requested range index.
    pub cached_parts: Vec<Vec<PageRangePart>>,
    /// Ranges that are not covered by cached fragments and need fetching.
    pub missing_ranges: Vec<Range<u64>>,
    /// Number of cached fragments used.
    pub cached_range_count: usize,
    /// Number of requested bytes served from cached fragments.
    pub cached_bytes: u64,
}

impl PageRangeLookup {
    pub fn is_fully_cached(&self) -> bool {
        self.missing_ranges.is_empty()
    }
}

type PageFragmentRangeMap = RwLock<BTreeMap<(u64, u64), PageFragmentKey>>;
type PageFragmentIndex = DashMap<PageFragmentGroupKey, Arc<PageFragmentRangeMap>>;

/// Byte-fragment cache for Parquet row-group reads.
///
/// Moka owns capacity and eviction. The side index only makes overlap lookup possible.
pub struct PageRangeCache {
    cache: Cache<PageFragmentKey, Bytes>,
    index: Arc<PageFragmentIndex>,
}

impl PageRangeCache {
    fn new(capacity: u64) -> Arc<PageRangeCache> {
        let index: Arc<PageFragmentIndex> = Arc::new(DashMap::new());
        let eviction_index = index.clone();
        let cache = Cache::builder()
            .max_capacity(capacity)
            .weigher(page_cache_weight)
            .eviction_listener(move |k: Arc<PageFragmentKey>, v: Bytes, cause| {
                let key = *k;
                let size = page_cache_weight(&key, &v);
                CACHE_BYTES.with_label_values(&[PAGE_TYPE]).sub(size.into());
                CACHE_EVICTION
                    .with_label_values(&[PAGE_TYPE, removal_cause_to_str(cause)])
                    .inc();

                let group_key = key.group_key();
                if let Some(group) = eviction_index
                    .get(&group_key)
                    .map(|group| Arc::clone(group.value()))
                {
                    let mut ranges = group.write().unwrap();
                    ranges.remove(&(key.start, key.end));
                    if ranges.is_empty() {
                        eviction_index
                            .remove_if(&group_key, |_, current| Arc::ptr_eq(current, &group));
                    }
                }
            })
            .build();

        Arc::new(PageRangeCache { cache, index })
    }

    fn lookup(
        &self,
        file_id: FileId,
        row_group_idx: usize,
        ranges: &[Range<u64>],
    ) -> PageRangeLookup {
        let mut cached_parts = Vec::with_capacity(ranges.len());
        let mut missing_ranges = Vec::new();
        let mut cached_range_count = 0;
        let mut cached_bytes = 0;
        let group_key = PageFragmentGroupKey {
            file_id,
            row_group_idx,
        };

        let group = self
            .index
            .get(&group_key)
            .map(|group| Arc::clone(group.value()));
        for range in ranges {
            if range.start >= range.end {
                cached_parts.push(Vec::new());
                continue;
            }

            let mut parts = Vec::new();
            if let Some(group) = &group {
                let index = group.read().unwrap();
                // A simple first-stage interval lookup: inspect fragments whose start is before
                // the requested end and keep those that overlap the requested range.
                for (_, fragment_key) in index.range(..(range.end, 0)) {
                    if fragment_key.end <= range.start {
                        continue;
                    }

                    if let Some(bytes) = self.cache.get(fragment_key) {
                        let start = range.start.max(fragment_key.start);
                        let end = range.end.min(fragment_key.end);
                        if start < end {
                            let slice_start = (start - fragment_key.start) as usize;
                            let slice_end = (end - fragment_key.start) as usize;
                            parts.push(PageRangePart {
                                range: start..end,
                                bytes: bytes.slice(slice_start..slice_end),
                            });
                        }
                    }
                }
            }

            parts.sort_unstable_by_key(|part| part.range.start);
            let mut cursor = range.start;
            let mut compacted_parts: Vec<PageRangePart> = Vec::with_capacity(parts.len());
            for part in parts {
                if part.range.end <= cursor {
                    continue;
                }

                let part = if part.range.start < cursor {
                    let offset = (cursor - part.range.start) as usize;
                    PageRangePart {
                        range: cursor..part.range.end,
                        bytes: part.bytes.slice(offset..),
                    }
                } else {
                    part
                };

                if cursor < part.range.start {
                    missing_ranges.push(cursor..part.range.start);
                }
                cached_bytes += part.range.end - part.range.start;
                cached_range_count += 1;
                cursor = part.range.end;
                compacted_parts.push(part);

                if cursor >= range.end {
                    break;
                }
            }

            if cursor < range.end {
                missing_ranges.push(cursor..range.end);
            }
            cached_parts.push(compacted_parts);
        }

        PageRangeLookup {
            cached_parts,
            missing_ranges,
            cached_range_count,
            cached_bytes,
        }
    }

    fn insert_ranges(
        &self,
        file_id: FileId,
        row_group_idx: usize,
        ranges: &[Range<u64>],
        pages: &[Bytes],
    ) {
        for (range, bytes) in ranges.iter().zip(pages) {
            if range.start >= range.end || bytes.len() as u64 != range.end - range.start {
                continue;
            }

            let key = PageFragmentKey::new(file_id, row_group_idx, range);
            let bytes = Bytes::copy_from_slice(bytes);
            let size = page_cache_weight(&key, &bytes);
            CACHE_BYTES.with_label_values(&[PAGE_TYPE]).add(size.into());
            self.cache.insert(key, bytes);

            self.insert_index_entry(key);
        }
    }

    fn insert_index_entry(&self, key: PageFragmentKey) {
        let group_key = key.group_key();
        loop {
            let group = self
                .index
                .entry(group_key)
                .or_insert_with(|| Arc::new(PageFragmentRangeMap::new(BTreeMap::new())))
                .clone();
            let mut ranges = group.write().unwrap();
            if self
                .index
                .get(&group_key)
                .is_some_and(|current| Arc::ptr_eq(current.value(), &group))
            {
                ranges.insert((key.start, key.end), key);
                return;
            }
        }
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

/// Result stored in the selector result cache.
pub enum SelectorResult {
    /// Batches in the primary key format.
    PrimaryKey(Vec<Batch>),
    /// Record batches in the flat format.
    Flat(Vec<RecordBatch>),
}

/// Cached result for time series row selector.
pub struct SelectorResultValue {
    /// Batches of rows selected by the selector.
    pub result: SelectorResult,
    /// The read columns of rows.
    pub read_cols: ParquetReadColumns,
}

impl SelectorResultValue {
    /// Creates a new selector result value with primary key format.
    pub fn new(result: Vec<Batch>, read_cols: ParquetReadColumns) -> SelectorResultValue {
        SelectorResultValue {
            result: SelectorResult::PrimaryKey(result),
            read_cols,
        }
    }

    /// Creates a new selector result value with flat format.
    pub fn new_flat(
        result: Vec<RecordBatch>,
        read_cols: ParquetReadColumns,
    ) -> SelectorResultValue {
        SelectorResultValue {
            result: SelectorResult::Flat(result),
            read_cols,
        }
    }

    /// Returns memory used by the value (estimated).
    fn estimated_size(&self) -> usize {
        match &self.result {
            SelectorResult::PrimaryKey(batches) => {
                batches.iter().map(|batch| batch.memory_size()).sum()
            }
            SelectorResult::Flat(batches) => batches.iter().map(record_batch_estimated_size).sum(),
        }
    }
}

/// Maps (region id, file id) to fused SST metadata.
type SstMetaCache = Cache<SstMetaKey, Arc<CachedSstMeta>>;
/// Maps [Value] to a vector that holds this value repeatedly.
///
/// e.g. `"hello" => ["hello", "hello", "hello"]`
type VectorCache = Cache<(ConcreteDataType, Value), VectorRef>;
/// Maps (file id, row group id, time series row selector) to [SelectorResultValue].
type SelectorResultCache = Cache<SelectorResultKey, Arc<SelectorResultValue>>;
/// Maps partition-range scan key to cached flat batches.
type RangeResultCache = Cache<RangeScanCacheKey, Arc<RangeScanCacheValue>>;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::v1::SemanticType;
    use api::v1::index::{BloomFilterMeta, InvertedIndexMetas};
    use datatypes::schema::ColumnSchema;
    use datatypes::vectors::Int64Vector;
    use puffin::file_metadata::FileMetadata;
    use store_api::metadata::{ColumnMetadata, RegionMetadata, RegionMetadataBuilder};
    use store_api::storage::ColumnId;

    use super::*;
    use crate::cache::index::bloom_filter_index::Tag;
    use crate::cache::index::result_cache::PredicateKey;
    use crate::cache::test_util::{
        parquet_meta, sst_parquet_meta, sst_parquet_meta_with_region_metadata,
    };
    use crate::read::range_cache::{
        RangeScanCacheKey, RangeScanCacheValue, ScanRequestFingerprintBuilder,
    };
    use crate::read::read_columns::ReadColumns;
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
        cache.put_parquet_meta_data(file_id, metadata, None);
        assert!(
            cache
                .get_parquet_meta_data(file_id, &mut metrics, Default::default())
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

        cache.put_page_ranges(
            file_id.file_id(),
            1,
            &[Range { start: 0, end: 5 }],
            &[Bytes::from_static(b"abcde")],
        );
        assert!(
            cache
                .get_page_ranges(file_id.file_id(), 1, &[Range { start: 0, end: 5 }])
                .is_none()
        );

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
                .get_parquet_meta_data(file_id, &mut metrics, Default::default())
                .await
                .is_none()
        );
        let (metadata, region_metadata) = sst_parquet_meta();
        cache.put_parquet_meta_data(file_id, metadata, None);
        let cached = cache
            .get_sst_meta_data(file_id, &mut metrics, Default::default())
            .await
            .unwrap();
        assert_eq!(region_metadata, cached.region_metadata());
        assert!(
            cached
                .parquet_metadata()
                .file_metadata()
                .key_value_metadata()
                .is_none_or(|key_values| {
                    key_values
                        .iter()
                        .all(|key_value| key_value.key != PARQUET_METADATA_KEY)
                })
        );
        cache.remove_parquet_meta_data(file_id);
        assert!(
            cache
                .get_parquet_meta_data(file_id, &mut metrics, Default::default())
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_parquet_meta_cache_with_provided_region_metadata() {
        let cache = CacheManager::builder().sst_meta_cache_size(2000).build();
        let mut metrics = MetadataCacheMetrics::default();
        let region_id = RegionId::new(1, 1);
        let file_id = RegionFileId::new(region_id, FileId::random());
        let (metadata, region_metadata) = sst_parquet_meta();

        cache.put_parquet_meta_data(file_id, metadata, Some(region_metadata.clone()));

        let cached = cache
            .get_sst_meta_data(file_id, &mut metrics, Default::default())
            .await
            .unwrap();
        assert!(Arc::ptr_eq(&region_metadata, &cached.region_metadata()));
    }

    #[test]
    fn test_meta_cache_weight_accounts_for_decoded_region_metadata() {
        let region_metadata = Arc::new(wide_region_metadata(128));
        let json_len = region_metadata.to_json().unwrap().len();
        let metadata = sst_parquet_meta_with_region_metadata(region_metadata.clone());
        let cached = Arc::new(
            CachedSstMeta::try_new("test.parquet", Arc::unwrap_or_clone(metadata)).unwrap(),
        );
        let key = SstMetaKey(region_metadata.region_id, FileId::random());

        assert!(cached.region_metadata_weight > json_len);
        assert_eq!(
            meta_cache_weight(&key, &cached) as usize,
            key.estimated_size()
                + parquet_meta_size(&cached.parquet_metadata)
                + cached.region_metadata_weight
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
        let uncached = 0..10;
        assert_eq!(
            vec![0..10],
            cache
                .get_page_ranges(file_id, 0, std::slice::from_ref(&uncached))
                .unwrap()
                .missing_ranges
        );

        let cached = 100..500;
        cache.put_page_ranges(
            file_id,
            0,
            std::slice::from_ref(&cached),
            &[Bytes::from(vec![7; 400])],
        );

        let subrange = 200..300;
        let lookup = cache
            .get_page_ranges(file_id, 0, std::slice::from_ref(&subrange))
            .unwrap();
        assert!(lookup.is_fully_cached());
        assert_eq!(100, lookup.cached_bytes);
        assert_eq!(1, lookup.cached_parts.len());
        assert_eq!(200..300, lookup.cached_parts[0][0].range);
        assert_eq!(100, lookup.cached_parts[0][0].bytes.len());

        let overlapping = 400..600;
        let lookup = cache
            .get_page_ranges(file_id, 0, std::slice::from_ref(&overlapping))
            .unwrap();
        assert!(!lookup.is_fully_cached());
        assert_eq!(100, lookup.cached_bytes);
        assert_eq!(vec![500..600], lookup.missing_ranges);
        assert_eq!(400..500, lookup.cached_parts[0][0].range);
    }

    #[test]
    fn test_page_cache_detaches_fragment_bytes() {
        let cache = PageRangeCache::new(1000);
        let file_id = FileId::random();
        let backing = Bytes::from(vec![1; 1024]);
        let page = backing.slice(512..522);
        let page_ptr = page.as_ptr();
        let range = 0..10;

        cache.insert_ranges(
            file_id,
            0,
            std::slice::from_ref(&range),
            std::slice::from_ref(&page),
        );

        let lookup = cache.lookup(file_id, 0, std::slice::from_ref(&range));
        assert!(lookup.is_fully_cached());
        assert_eq!(1, lookup.cached_parts[0].len());
        assert_eq!(&page[..], &lookup.cached_parts[0][0].bytes[..]);
        assert_ne!(page_ptr, lookup.cached_parts[0][0].bytes.as_ptr());
    }

    #[test]
    fn test_page_cache_removes_empty_index_group_on_cache_removal() {
        let cache = PageRangeCache::new(1000);
        let file_id = FileId::random();
        let group_key = PageFragmentGroupKey {
            file_id,
            row_group_idx: 0,
        };
        let range1 = 0..10;
        let range2 = 20..30;
        let key1 = PageFragmentKey::new(file_id, 0, &range1);
        let key2 = PageFragmentKey::new(file_id, 0, &range2);

        cache.insert_ranges(
            file_id,
            0,
            &[range1, range2],
            &[Bytes::from(vec![1; 10]), Bytes::from(vec![2; 10])],
        );
        assert_eq!(
            2,
            cache.index.get(&group_key).unwrap().read().unwrap().len()
        );

        cache.cache.invalidate(&key1);
        cache.cache.run_pending_tasks();
        assert_eq!(
            1,
            cache.index.get(&group_key).unwrap().read().unwrap().len()
        );

        cache.cache.invalidate(&key2);
        cache.cache.run_pending_tasks();
        assert!(!cache.index.contains_key(&group_key));
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
        let result = Arc::new(SelectorResultValue::new(
            Vec::new(),
            ParquetReadColumns::from_deduped(Vec::new()),
        ));
        cache.put_selector_result(key, result);
        assert!(cache.get_selector_result(&key).is_some());
    }

    #[test]
    fn test_range_result_cache() {
        let cache = Arc::new(
            CacheManager::builder()
                .range_result_cache_size(1024 * 1024)
                .build(),
        );

        let key = RangeScanCacheKey {
            region_id: RegionId::new(1, 1),
            row_groups: vec![(FileId::random(), 0)],
            scan: ScanRequestFingerprintBuilder {
                read_columns: ReadColumns::from_deduped_column_ids(std::iter::empty()),
                read_column_types: vec![],
                filters: vec!["tag_0 = 1".to_string()],
                time_filters: vec![],
                series_row_selector: None,
                append_mode: false,
                filter_deleted: true,
                merge_mode: crate::region::options::MergeMode::LastRow,
                partition_expr_version: 0,
            }
            .build(),
        };
        let value = Arc::new(RangeScanCacheValue::new(Vec::new(), 0));

        assert!(cache.get_range_result(&key).is_none());
        cache.put_range_result(key.clone(), value.clone());
        assert!(cache.get_range_result(&key).is_some());

        let enable_all = CacheStrategy::EnableAll(cache.clone());
        assert!(enable_all.get_range_result(&key).is_some());

        let compaction = CacheStrategy::Compaction(cache.clone());
        assert!(compaction.get_range_result(&key).is_none());
        compaction.put_range_result(key.clone(), value.clone());
        assert!(cache.get_range_result(&key).is_some());

        let disabled = CacheStrategy::Disabled;
        assert!(disabled.get_range_result(&key).is_none());
        disabled.put_range_result(key.clone(), value);
        assert!(cache.get_range_result(&key).is_some());
    }

    #[test]
    fn test_range_result_cache_size_configures_limiter() {
        let cache_size = 3 * 1024_u64;
        let cache = CacheManager::builder()
            .range_result_cache_size(cache_size)
            .build();

        assert_eq!(cache.range_result_cache_size(), cache_size as usize);
        assert_eq!(
            cache.range_result_memory_limiter().permit_bytes(),
            RANGE_RESULT_CONCAT_MEMORY_PERMIT.as_bytes() as usize
        );
        assert_eq!(
            cache.range_result_memory_limiter().available_permits(),
            (cache_size as usize).div_ceil(RANGE_RESULT_CONCAT_MEMORY_PERMIT.as_bytes() as usize)
        );
    }

    #[tokio::test]
    async fn range_result_memory_limiter_rejects_oversized_request() {
        let limiter = RangeResultMemoryLimiter::new(2 * 1024, 1024);
        assert_eq!(limiter.available_permits(), 2);

        let err = limiter.acquire(10 * 1024).await.unwrap_err();
        assert!(
            err.to_string().contains("exceeds limiter capacity"),
            "unexpected error: {err}"
        );
        assert_eq!(limiter.available_permits(), 2);
    }

    #[tokio::test]
    async fn range_result_memory_limiter_allows_request_up_to_capacity() {
        let limiter = RangeResultMemoryLimiter::new(2 * 1024, 1024);
        let permit = limiter.acquire(2 * 1024).await.unwrap();
        assert_eq!(limiter.available_permits(), 0);
        drop(permit);
        assert_eq!(limiter.available_permits(), 2);
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

    fn wide_region_metadata(column_count: u32) -> RegionMetadata {
        let region_id = RegionId::new(1024, 7);
        let mut builder = RegionMetadataBuilder::new(region_id);
        let mut primary_key = Vec::new();

        for column_id in 0..column_count {
            let semantic_type = if column_id < 32 {
                primary_key.push(column_id);
                SemanticType::Tag
            } else {
                SemanticType::Field
            };
            let mut column_schema = ColumnSchema::new(
                format!("wide_column_{column_id}"),
                ConcreteDataType::string_datatype(),
                true,
            );
            column_schema
                .mut_metadata()
                .insert(format!("cache_key_{column_id}"), "cache_value".repeat(4));
            builder.push_column_metadata(ColumnMetadata {
                column_schema,
                semantic_type,
                column_id,
            });
        }

        builder.push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
            semantic_type: SemanticType::Timestamp,
            column_id: column_count,
        });
        builder.primary_key(primary_key);

        builder.build().unwrap()
    }
}
