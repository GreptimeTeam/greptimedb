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
    cache: Cache<CacheKey, CacheValue>,
}

pub type CacheManagerRef = Arc<CacheManager>;

impl CacheManager {
    /// Creates a new manager with specific cache capacity in bytes.
    /// Returns `None` if `capacity` is 0.
    pub fn new(capacity: u64) -> Option<CacheManager> {
        if capacity == 0 {
            None
        } else {
            let cache = Cache::builder()
                .max_capacity(capacity)
                .weigher(|k: &CacheKey, v: &CacheValue| {
                    (k.estimated_size() + v.estimated_size()) as u32
                })
                .build();
            Some(CacheManager { cache })
        }
    }

    /// Gets cached [ParquetMetaData].
    pub fn get_parquet_meta_data(
        &self,
        region_id: RegionId,
        file_id: FileId,
    ) -> Option<Arc<ParquetMetaData>> {
        self.cache
            .get(&CacheKey::ParquetMeta(region_id, file_id))
            .map(|v| {
                // Safety: key and value have the same type.
                v.into_parquet_meta().unwrap()
            })
    }

    /// Puts [ParquetMetaData] into the cache.
    pub fn put_parquet_meta_data(
        &self,
        region_id: RegionId,
        file_id: FileId,
        metadata: Arc<ParquetMetaData>,
    ) {
        self.cache.insert(
            CacheKey::ParquetMeta(region_id, file_id),
            CacheValue::ParquetMeta(metadata),
        );
    }
}

/// Cache key.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum CacheKey {
    /// Parquet meta data.
    ParquetMeta(RegionId, FileId),
}

impl CacheKey {
    /// Returns memory used by the key (estimated).
    fn estimated_size(&self) -> usize {
        mem::size_of::<CacheKey>()
    }
}

/// Cached value.
/// It can hold different kinds of data.
#[derive(Clone)]
enum CacheValue {
    /// Parquet meta data.
    ParquetMeta(Arc<ParquetMetaData>),
}

impl CacheValue {
    /// Returns memory used by the value (estimated).
    fn estimated_size(&self) -> usize {
        let inner_size = match self {
            CacheValue::ParquetMeta(meta) => parquet_meta_size(&meta),
        };
        inner_size + mem::size_of::<CacheValue>()
    }

    /// Convert to parquet meta.
    fn into_parquet_meta(self) -> Option<Arc<ParquetMetaData>> {
        match self {
            CacheValue::ParquetMeta(meta) => Some(meta),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::test_util::parquet_meta;

    #[test]
    fn test_capacity_zero() {
        assert!(CacheManager::new(0).is_none());
    }

    #[test]
    fn test_parquet_meta_cache() {
        let cache = CacheManager::new(2000).unwrap();
        let region_id = RegionId::new(1, 1);
        let file_id = FileId::random();
        assert!(cache.get_parquet_meta_data(region_id, file_id).is_none());
        let metadata = parquet_meta();
        cache.put_parquet_meta_data(region_id, file_id, metadata);
        assert!(cache.get_parquet_meta_data(region_id, file_id).is_some());
    }
}
