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

use opendal::raw::oio::Reader;
use opendal::raw::{
    Access, Layer, LayeredAccess, OpList, OpRead, OpWrite, RpDelete, RpList, RpRead, RpWrite,
};
use opendal::Result;
mod read_cache;
use std::time::Instant;

use common_telemetry::{error, info};
use read_cache::ReadCache;

use crate::layers::lru_cache::read_cache::CacheAwareDeleter;

/// An opendal layer with local LRU file cache supporting.
pub struct LruCacheLayer<C: Access> {
    // The read cache
    read_cache: ReadCache<C>,
}

impl<C: Access> Clone for LruCacheLayer<C> {
    fn clone(&self) -> Self {
        Self {
            read_cache: self.read_cache.clone(),
        }
    }
}

impl<C: Access> LruCacheLayer<C> {
    /// Create a [`LruCacheLayer`] with local file cache and capacity in bytes.
    pub fn new(file_cache: Arc<C>, capacity: usize) -> Result<Self> {
        let read_cache = ReadCache::new(file_cache, capacity);
        Ok(Self { read_cache })
    }

    /// Recovers cache
    pub async fn recover_cache(&self, sync: bool) {
        let now = Instant::now();
        let moved_read_cache = self.read_cache.clone();
        let handle = tokio::spawn(async move {
            match moved_read_cache.recover_cache().await {
                Ok((entries, bytes)) => info!(
                    "Recovered {} entries and total size {} in bytes for LruCacheLayer, cost: {:?}",
                    entries,
                    bytes,
                    now.elapsed()
                ),
                Err(err) => error!(err; "Failed to recover file cache."),
            }
        });
        if sync {
            let _ = handle.await;
        }
    }

    /// Returns true when the local cache contains the specific file
    pub async fn contains_file(&self, path: &str) -> bool {
        self.read_cache.contains_file(path).await
    }

    /// Returns the read cache statistics info `(EntryCount, SizeInBytes)`.
    pub async fn read_cache_stat(&self) -> (u64, u64) {
        self.read_cache.cache_stat().await
    }
}

impl<I: Access, C: Access> Layer<I> for LruCacheLayer<C> {
    type LayeredAccess = LruCacheAccess<I, C>;

    fn layer(&self, inner: I) -> Self::LayeredAccess {
        LruCacheAccess {
            inner,
            read_cache: self.read_cache.clone(),
        }
    }
}

#[derive(Debug)]
pub struct LruCacheAccess<I, C> {
    inner: I,
    read_cache: ReadCache<C>,
}

impl<I: Access, C: Access> LayeredAccess for LruCacheAccess<I, C> {
    type Inner = I;
    type Reader = Reader;
    type BlockingReader = I::BlockingReader;
    type Writer = I::Writer;
    type BlockingWriter = I::BlockingWriter;
    type Lister = I::Lister;
    type BlockingLister = I::BlockingLister;
    type Deleter = CacheAwareDeleter<C, I::Deleter>;
    type BlockingDeleter = I::BlockingDeleter;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        self.read_cache
            .read_from_cache(&self.inner, path, args)
            .await
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let result = self.inner.write(path, args).await;

        self.read_cache.invalidate_entries_with_prefix(path);

        result
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        self.inner
            .delete()
            .await
            .map(|(rp, deleter)| (rp, CacheAwareDeleter::new(self.read_cache.clone(), deleter)))
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        self.inner.list(path, args).await
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        // TODO(dennis): support blocking read cache
        self.inner.blocking_read(path, args)
    }

    fn blocking_write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::BlockingWriter)> {
        let result = self.inner.blocking_write(path, args);

        self.read_cache.invalidate_entries_with_prefix(path);

        result
    }

    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, Self::BlockingLister)> {
        self.inner.blocking_list(path, args)
    }

    fn blocking_delete(&self) -> Result<(RpDelete, Self::BlockingDeleter)> {
        self.inner.blocking_delete()
    }
}
