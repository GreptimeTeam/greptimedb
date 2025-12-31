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

//! Cache for vector indexes.
//!
//! Unlike other indexes that use page-based caching, vector indexes cache
//! the entire loaded vector index since it needs to be loaded as a whole
//! for KNN search operations.

use std::sync::Arc;

use roaring::RoaringBitmap;
use store_api::storage::{ColumnId, FileId, VectorIndexEngine};

use crate::metrics::CACHE_BYTES;

const INDEX_TYPE_VECTOR_INDEX: &str = "vector_index";

/// Key for vector index cache entries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct VectorIndexCacheKey {
    /// The file ID of the SST file.
    pub file_id: FileId,
    /// The column ID of the vector column.
    pub column_id: ColumnId,
}

impl VectorIndexCacheKey {
    pub fn new(file_id: FileId, column_id: ColumnId) -> Self {
        Self { file_id, column_id }
    }
}

/// Cached vector index entry containing the loaded vector index and NULL bitmap.
pub struct CachedVectorIndex {
    /// The loaded vector index engine.
    pub engine: Box<dyn VectorIndexEngine>,
    /// Bitmap tracking which row offsets have NULL vectors.
    pub null_bitmap: RoaringBitmap,
    /// The size of this cache entry in bytes (for cache eviction).
    pub size_bytes: usize,
}

impl CachedVectorIndex {
    /// Creates a new cached vector index.
    pub fn new(engine: Box<dyn VectorIndexEngine>, null_bitmap: RoaringBitmap) -> Self {
        // Estimate size: engine memory usage + RoaringBitmap size + struct overhead
        let size_bytes =
            engine.memory_usage() + null_bitmap.serialized_size() + std::mem::size_of::<Self>();
        Self {
            engine,
            null_bitmap,
            size_bytes,
        }
    }

    /// Returns the number of vectors in the index.
    pub fn size(&self) -> usize {
        self.engine.size()
    }
}

/// Cache for loaded vector indexes.
pub type VectorIndexCache = moka::sync::Cache<VectorIndexCacheKey, Arc<CachedVectorIndex>>;
pub type VectorIndexCacheRef = Arc<VectorIndexCache>;

/// Creates a new vector index cache with the given capacity in bytes.
pub fn new_vector_index_cache(capacity_bytes: u64) -> VectorIndexCache {
    moka::sync::CacheBuilder::new(capacity_bytes)
        .name("vector_index")
        .weigher(|_key, value: &Arc<CachedVectorIndex>| value.size_bytes as u32)
        .eviction_listener(|_key, value, _cause| {
            CACHE_BYTES
                .with_label_values(&[INDEX_TYPE_VECTOR_INDEX])
                .sub(value.size_bytes as i64);
        })
        .build()
}

/// Extension trait for VectorIndexCache.
pub trait VectorIndexCacheExt {
    /// Gets or loads a vector index from the cache.
    ///
    /// If the index is not in the cache, the `loader` function is called to load it,
    /// and the result is stored in the cache.
    ///
    /// This method is thread-safe and uses atomic get-or-insert semantics to avoid
    /// loading the same index multiple times when concurrent requests arrive.
    #[allow(dead_code)]
    fn get_or_try_insert_with<E, F>(
        &self,
        key: VectorIndexCacheKey,
        loader: F,
    ) -> Result<Arc<CachedVectorIndex>, E>
    where
        E: Clone + Send + Sync + 'static,
        F: FnOnce() -> Result<CachedVectorIndex, E>;

    /// Invalidates all cache entries for the given file ID.
    fn invalidate_file(&self, file_id: FileId);
}

impl VectorIndexCacheExt for VectorIndexCache {
    fn get_or_try_insert_with<E, F>(
        &self,
        key: VectorIndexCacheKey,
        loader: F,
    ) -> Result<Arc<CachedVectorIndex>, E>
    where
        E: Clone + Send + Sync + 'static,
        F: FnOnce() -> Result<CachedVectorIndex, E>,
    {
        // Use moka's try_get_with for atomic get-or-insert to avoid race conditions.
        // This ensures that only one thread loads the index even if multiple threads
        // request the same key concurrently.
        self.try_get_with(key, || {
            let loaded = loader()?;
            let size = loaded.size_bytes;
            CACHE_BYTES
                .with_label_values(&[INDEX_TYPE_VECTOR_INDEX])
                .add(size as i64);
            Ok(Arc::new(loaded))
        })
        .map_err(|e: Arc<E>| Arc::try_unwrap(e).unwrap_or_else(|arc| (*arc).clone()))
    }

    fn invalidate_file(&self, file_id: FileId) {
        let _ = self.invalidate_entries_if(move |key, _value| key.file_id == file_id);
    }
}

#[cfg(test)]
mod tests {
    use index::vector::VectorIndexOptions;
    use store_api::storage::VectorIndexEngineType;

    use super::*;
    use crate::sst::index::vector_index::creator::VectorIndexConfig;
    use crate::sst::index::vector_index::engine;

    #[test]
    fn test_vector_index_cache_key() {
        let key1 = VectorIndexCacheKey::new(FileId::random(), 1);
        let key2 = VectorIndexCacheKey::new(key1.file_id, 1);
        let key3 = VectorIndexCacheKey::new(key1.file_id, 2);

        assert_eq!(key1, key2);
        assert_ne!(key1, key3);
    }

    #[test]
    fn test_cached_vector_index_size() {
        let options = VectorIndexOptions::default();
        let config = VectorIndexConfig::new(4, &options);
        let engine_instance =
            engine::create_engine(VectorIndexEngineType::Usearch, &config).unwrap();

        let null_bitmap = RoaringBitmap::new();
        let cached = CachedVectorIndex::new(engine_instance, null_bitmap);

        assert!(cached.size_bytes > 0);
        assert_eq!(cached.size(), 0);
    }

    #[test]
    fn test_vector_index_cache() {
        let cache = new_vector_index_cache(1024 * 1024); // 1MB
        let key = VectorIndexCacheKey::new(FileId::random(), 1);

        // Cache miss - load
        let result = cache.get_or_try_insert_with(key, || {
            let options = VectorIndexOptions::default();
            let config = VectorIndexConfig::new(4, &options);
            let engine_instance = engine::create_engine(VectorIndexEngineType::Usearch, &config)
                .map_err(|e| e.to_string())?;
            Ok::<_, String>(CachedVectorIndex::new(
                engine_instance,
                RoaringBitmap::new(),
            ))
        });

        assert!(result.is_ok());

        // Cache hit
        let cached = cache.get(&key);
        assert!(cached.is_some());
    }
}
