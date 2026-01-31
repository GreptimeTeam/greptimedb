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

use std::sync::Arc;

use index::vector::apply::IndexApplier;
use moka::notification::RemovalCause;
use moka::sync::Cache;
use store_api::storage::{ColumnId, FileId, IndexVersion};

use crate::metrics::{CACHE_BYTES, CACHE_EVICTION};

const VECTOR_INDEX_CACHE_TYPE: &str = "vector_index";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct VectorIndexCacheKey {
    file_id: FileId,
    index_version: IndexVersion,
    column_id: ColumnId,
}

impl VectorIndexCacheKey {
    pub fn new(file_id: FileId, index_version: IndexVersion, column_id: ColumnId) -> Self {
        Self {
            file_id,
            index_version,
            column_id,
        }
    }
}

/// Cached vector index wrapping an `IndexApplier` for efficient searches.
pub struct CachedVectorIndex {
    /// The underlying vector index applier.
    applier: Box<dyn IndexApplier>,
    /// Size in bytes for cache weighing.
    size_bytes: usize,
}

impl CachedVectorIndex {
    /// Creates a new `CachedVectorIndex` from an `IndexApplier`.
    pub fn new(applier: Box<dyn IndexApplier>) -> Self {
        let size_bytes = applier.memory_usage();
        Self {
            applier,
            size_bytes,
        }
    }

    /// Returns the underlying applier for searching.
    pub fn applier(&self) -> &dyn IndexApplier {
        self.applier.as_ref()
    }

    /// Returns the cache size in bytes.
    pub fn size_bytes(&self) -> usize {
        self.size_bytes
    }
}

impl std::fmt::Debug for CachedVectorIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CachedVectorIndex")
            .field("size_bytes", &self.size_bytes)
            .field("dimensions", &self.applier.dimensions())
            .field("metric", &self.applier.metric())
            .field("total_rows", &self.applier.total_rows())
            .field("indexed_rows", &self.applier.indexed_rows())
            .finish()
    }
}

pub struct VectorIndexCache {
    inner: Cache<VectorIndexCacheKey, Arc<CachedVectorIndex>>,
}

pub type VectorIndexCacheRef = Arc<VectorIndexCache>;

impl VectorIndexCache {
    pub fn new(capacity: u64) -> Self {
        fn to_str(cause: RemovalCause) -> &'static str {
            match cause {
                RemovalCause::Expired => "expired",
                RemovalCause::Explicit => "explicit",
                RemovalCause::Replaced => "replaced",
                RemovalCause::Size => "size",
            }
        }

        let inner = Cache::builder()
            .max_capacity(capacity)
            .weigher(|_k, v: &Arc<CachedVectorIndex>| v.size_bytes() as u32)
            .eviction_listener(|_k, v, cause| {
                CACHE_BYTES
                    .with_label_values(&[VECTOR_INDEX_CACHE_TYPE])
                    .sub(v.size_bytes() as i64);
                CACHE_EVICTION
                    .with_label_values(&[VECTOR_INDEX_CACHE_TYPE, to_str(cause)])
                    .inc();
            })
            .build();
        Self { inner }
    }

    pub fn get(&self, key: &VectorIndexCacheKey) -> Option<Arc<CachedVectorIndex>> {
        self.inner.get(key)
    }

    pub fn insert(&self, key: VectorIndexCacheKey, value: Arc<CachedVectorIndex>) {
        CACHE_BYTES
            .with_label_values(&[VECTOR_INDEX_CACHE_TYPE])
            .add(value.size_bytes() as i64);
        self.inner.insert(key, value);
    }

    pub fn invalidate_file(&self, file_id: FileId) {
        let _ = self
            .inner
            .invalidate_entries_if(move |k, _| k.file_id == file_id);
    }
}
