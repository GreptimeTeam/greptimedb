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
// TODO(yingwen): Remove this after the write cache is ready.
#[allow(unused)]
pub(crate) mod file_cache;
#[cfg(test)]
pub(crate) mod test_util;
#[allow(unused)]
pub(crate) mod write_cache;

use std::mem;
use std::sync::Arc;

use datatypes::value::Value;
use datatypes::vectors::VectorRef;
use moka::sync::Cache;
use parquet::column::page::Page;
use parquet::file::metadata::ParquetMetaData;
use store_api::storage::RegionId;

use crate::cache::cache_size::parquet_meta_size;
use crate::cache::write_cache::WriteCacheRef;
use crate::metrics::{CACHE_BYTES, CACHE_HIT, CACHE_MISS};
use crate::sst::file::FileId;

// Metrics type key for sst meta.
const SST_META_TYPE: &str = "sst_meta";
// Metrics type key for vector.
const VECTOR_TYPE: &str = "vector";
// Metrics type key for pages.
const PAGE_TYPE: &str = "page";
// Metrics type key for files on the local store.
const FILE_TYPE: &str = "file";

// TODO(yingwen): Builder for cache manager.

/// Manages cached data for the engine.
pub struct CacheManager {
    /// Cache for SST metadata.
    sst_meta_cache: Option<SstMetaCache>,
    /// Cache for vectors.
    vector_cache: Option<VectorCache>,
    /// Cache for SST pages.
    page_cache: Option<PageCache>,
    /// A Cache for writing files to object stores.
    // TODO(yingwen): Remove this once the cache is ready.
    #[allow(unused)]
    write_cache: Option<WriteCacheRef>,
}

pub type CacheManagerRef = Arc<CacheManager>;

impl CacheManager {
    /// Creates a new manager with specific cache size in bytes.
    pub fn new(
        sst_meta_cache_size: u64,
        vector_cache_size: u64,
        page_cache_size: u64,
    ) -> CacheManager {
        let sst_meta_cache = if sst_meta_cache_size == 0 {
            None
        } else {
            let cache = Cache::builder()
                .max_capacity(sst_meta_cache_size)
                .weigher(meta_cache_weight)
                .eviction_listener(|k, v, _cause| {
                    let size = meta_cache_weight(&k, &v);
                    CACHE_BYTES
                        .with_label_values(&[SST_META_TYPE])
                        .sub(size.into());
                })
                .build();
            Some(cache)
        };
        let vector_cache = if vector_cache_size == 0 {
            None
        } else {
            let cache = Cache::builder()
                .max_capacity(vector_cache_size)
                .weigher(vector_cache_weight)
                .eviction_listener(|k, v, _cause| {
                    let size = vector_cache_weight(&k, &v);
                    CACHE_BYTES
                        .with_label_values(&[VECTOR_TYPE])
                        .sub(size.into());
                })
                .build();
            Some(cache)
        };
        let page_cache = if page_cache_size == 0 {
            None
        } else {
            let cache = Cache::builder()
                .max_capacity(page_cache_size)
                .weigher(page_cache_weight)
                .eviction_listener(|k, v, _cause| {
                    let size = page_cache_weight(&k, &v);
                    CACHE_BYTES.with_label_values(&[PAGE_TYPE]).sub(size.into());
                })
                .build();
            Some(cache)
        };

        CacheManager {
            sst_meta_cache,
            vector_cache,
            page_cache,
            write_cache: None,
        }
    }

    /// Gets cached [ParquetMetaData].
    pub fn get_parquet_meta_data(
        &self,
        region_id: RegionId,
        file_id: FileId,
    ) -> Option<Arc<ParquetMetaData>> {
        self.sst_meta_cache.as_ref().and_then(|sst_meta_cache| {
            let value = sst_meta_cache.get(&SstMetaKey(region_id, file_id));
            update_hit_miss(value, SST_META_TYPE)
        })
    }

    /// Puts [ParquetMetaData] into the cache.
    pub fn put_parquet_meta_data(
        &self,
        region_id: RegionId,
        file_id: FileId,
        metadata: Arc<ParquetMetaData>,
    ) {
        if let Some(cache) = &self.sst_meta_cache {
            let key = SstMetaKey(region_id, file_id);
            CACHE_BYTES
                .with_label_values(&[SST_META_TYPE])
                .add(meta_cache_weight(&key, &metadata).into());
            cache.insert(key, metadata);
        }
    }

    /// Removes [ParquetMetaData] from the cache.
    pub fn remove_parquet_meta_data(&self, region_id: RegionId, file_id: FileId) {
        if let Some(cache) = &self.sst_meta_cache {
            cache.remove(&SstMetaKey(region_id, file_id));
        }
    }

    /// Gets a vector with repeated value for specific `key`.
    pub fn get_repeated_vector(&self, key: &Value) -> Option<VectorRef> {
        self.vector_cache.as_ref().and_then(|vector_cache| {
            let value = vector_cache.get(key);
            update_hit_miss(value, VECTOR_TYPE)
        })
    }

    /// Puts a vector with repeated value into the cache.
    pub fn put_repeated_vector(&self, key: Value, vector: VectorRef) {
        if let Some(cache) = &self.vector_cache {
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

    /// Gets the the write cache.
    pub(crate) fn write_cache(&self) -> Option<&WriteCacheRef> {
        self.write_cache.as_ref()
    }
}

fn meta_cache_weight(k: &SstMetaKey, v: &Arc<ParquetMetaData>) -> u32 {
    // We ignore the size of `Arc`.
    (k.estimated_size() + parquet_meta_size(v)) as u32
}

fn vector_cache_weight(_k: &Value, v: &VectorRef) -> u32 {
    // We ignore the heap size of `Value`.
    (mem::size_of::<Value>() + v.memory_size()) as u32
}

fn page_cache_weight(k: &PageKey, v: &Arc<PageValue>) -> u32 {
    (k.estimated_size() + v.estimated_size()) as u32
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

/// Cache key for pages of a SST row group.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PageKey {
    /// Region id of the SST file to cache.
    pub region_id: RegionId,
    /// Id of the SST file to cache.
    pub file_id: FileId,
    /// Index of the row group.
    pub row_group_idx: usize,
    /// Index of the column in the row group.
    pub column_idx: usize,
}

impl PageKey {
    /// Returns memory used by the key (estimated).
    fn estimated_size(&self) -> usize {
        mem::size_of::<Self>()
    }
}

/// Cached row group pages for a column.
pub struct PageValue {
    /// All pages of the column in the row group.
    pub pages: Vec<Page>,
}

impl PageValue {
    /// Creates a new page value.
    pub fn new(pages: Vec<Page>) -> PageValue {
        PageValue { pages }
    }

    /// Returns memory used by the value (estimated).
    fn estimated_size(&self) -> usize {
        // We only consider heap size of all pages.
        self.pages.iter().map(|page| page.buffer().len()).sum()
    }
}

/// Maps (region id, file id) to [ParquetMetaData].
type SstMetaCache = Cache<SstMetaKey, Arc<ParquetMetaData>>;
/// Maps [Value] to a vector that holds this value repeatedly.
///
/// e.g. `"hello" => ["hello", "hello", "hello"]`
type VectorCache = Cache<Value, VectorRef>;
/// Maps (region, file, row group, column) to [PageValue].
type PageCache = Cache<PageKey, Arc<PageValue>>;

#[cfg(test)]
mod tests {
    use datatypes::vectors::Int64Vector;

    use super::*;
    use crate::cache::test_util::parquet_meta;

    #[test]
    fn test_disable_cache() {
        let cache = CacheManager::new(0, 0, 0);
        assert!(cache.sst_meta_cache.is_none());
        assert!(cache.vector_cache.is_none());
        assert!(cache.page_cache.is_none());

        let region_id = RegionId::new(1, 1);
        let file_id = FileId::random();
        let metadata = parquet_meta();
        cache.put_parquet_meta_data(region_id, file_id, metadata);
        assert!(cache.get_parquet_meta_data(region_id, file_id).is_none());

        let value = Value::Int64(10);
        let vector: VectorRef = Arc::new(Int64Vector::from_slice([10, 10, 10, 10]));
        cache.put_repeated_vector(value.clone(), vector.clone());
        assert!(cache.get_repeated_vector(&value).is_none());

        let key = PageKey {
            region_id,
            file_id,
            row_group_idx: 0,
            column_idx: 0,
        };
        let pages = Arc::new(PageValue::new(Vec::new()));
        cache.put_pages(key.clone(), pages);
        assert!(cache.get_pages(&key).is_none());
    }

    #[test]
    fn test_parquet_meta_cache() {
        let cache = CacheManager::new(2000, 0, 0);
        let region_id = RegionId::new(1, 1);
        let file_id = FileId::random();
        assert!(cache.get_parquet_meta_data(region_id, file_id).is_none());
        let metadata = parquet_meta();
        cache.put_parquet_meta_data(region_id, file_id, metadata);
        assert!(cache.get_parquet_meta_data(region_id, file_id).is_some());
        cache.remove_parquet_meta_data(region_id, file_id);
        assert!(cache.get_parquet_meta_data(region_id, file_id).is_none());
    }

    #[test]
    fn test_repeated_vector_cache() {
        let cache = CacheManager::new(0, 4096, 0);
        let value = Value::Int64(10);
        assert!(cache.get_repeated_vector(&value).is_none());
        let vector: VectorRef = Arc::new(Int64Vector::from_slice([10, 10, 10, 10]));
        cache.put_repeated_vector(value.clone(), vector.clone());
        let cached = cache.get_repeated_vector(&value).unwrap();
        assert_eq!(vector, cached);
    }

    #[test]
    fn test_page_cache() {
        let cache = CacheManager::new(0, 0, 1000);
        let region_id = RegionId::new(1, 1);
        let file_id = FileId::random();
        let key = PageKey {
            region_id,
            file_id,
            row_group_idx: 0,
            column_idx: 0,
        };
        assert!(cache.get_pages(&key).is_none());
        let pages = Arc::new(PageValue::new(Vec::new()));
        cache.put_pages(key.clone(), pages);
        assert!(cache.get_pages(&key).is_some());
    }
}
