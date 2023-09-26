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
#[cfg(test)]
pub(crate) mod test_util;

use std::mem;
use std::sync::Arc;

use moka::sync::Cache;
use parquet::file::metadata::ParquetMetaData;
use store_api::storage::RegionId;

use crate::cache::cache_size::parquet_meta_size;
use crate::sst::file::FileId;

/// Manages cached data for the engine.
pub struct CacheManager {
    /// Cache for SST metadata.
    sst_meta_cache: Option<SstMetaCache>,
}

pub type CacheManagerRef = Arc<CacheManager>;

impl CacheManager {
    /// Creates a new manager with specific cache size in bytes.
    pub fn new(sst_meta_cache_size: u64) -> CacheManager {
        let sst_meta_cache = if sst_meta_cache_size == 0 {
            None
        } else {
            let cache = Cache::builder()
                .max_capacity(sst_meta_cache_size)
                .weigher(|k: &SstMetaKey, v: &Arc<ParquetMetaData>| {
                    // We ignore the size of `Arc`.
                    (k.estimated_size() + parquet_meta_size(v)) as u32
                })
                .build();
            Some(cache)
        };

        CacheManager { sst_meta_cache }
    }

    /// Gets cached [ParquetMetaData].
    pub fn get_parquet_meta_data(
        &self,
        region_id: RegionId,
        file_id: FileId,
    ) -> Option<Arc<ParquetMetaData>> {
        self.sst_meta_cache
            .as_ref()
            .and_then(|sst_meta_cache| sst_meta_cache.get(&SstMetaKey(region_id, file_id)))
    }

    /// Puts [ParquetMetaData] into the cache.
    pub fn put_parquet_meta_data(
        &self,
        region_id: RegionId,
        file_id: FileId,
        metadata: Arc<ParquetMetaData>,
    ) {
        if let Some(cache) = &self.sst_meta_cache {
            cache.insert(SstMetaKey(region_id, file_id), metadata);
        }
    }

    /// Removes [ParquetMetaData] from the cache.
    pub fn remove_parquet_meta_data(&self, region_id: RegionId, file_id: FileId) {
        if let Some(cache) = &self.sst_meta_cache {
            cache.remove(&SstMetaKey(region_id, file_id));
        }
    }
}

/// Cache key for SST meta.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SstMetaKey(RegionId, FileId);

impl SstMetaKey {
    /// Returns memory used by the key (estimated).
    fn estimated_size(&self) -> usize {
        mem::size_of::<SstMetaKey>()
    }
}

type SstMetaCache = Cache<SstMetaKey, Arc<ParquetMetaData>>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::test_util::parquet_meta;

    #[test]
    fn test_disable_meta_cache() {
        let cache = CacheManager::new(0);
        assert!(cache.sst_meta_cache.is_none());

        let region_id = RegionId::new(1, 1);
        let file_id = FileId::random();
        let metadata = parquet_meta();
        cache.put_parquet_meta_data(region_id, file_id, metadata);
        assert!(cache.get_parquet_meta_data(region_id, file_id).is_none());
    }

    #[test]
    fn test_parquet_meta_cache() {
        let cache = CacheManager::new(2000);
        let region_id = RegionId::new(1, 1);
        let file_id = FileId::random();
        assert!(cache.get_parquet_meta_data(region_id, file_id).is_none());
        let metadata = parquet_meta();
        cache.put_parquet_meta_data(region_id, file_id, metadata);
        assert!(cache.get_parquet_meta_data(region_id, file_id).is_some());
        cache.remove_parquet_meta_data(region_id, file_id);
        assert!(cache.get_parquet_meta_data(region_id, file_id).is_none());
    }
}
