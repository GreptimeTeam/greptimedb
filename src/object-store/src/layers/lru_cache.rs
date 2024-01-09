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

use async_trait::async_trait;
use opendal::raw::oio::Read;
use opendal::raw::{
    Accessor, Layer, LayeredAccessor, OpDelete, OpList, OpRead, OpWrite, RpDelete, RpList, RpRead,
    RpWrite,
};
use opendal::Result;
mod read_cache;
use common_telemetry::logging::info;
use read_cache::ReadCache;

/// An opendal layer with local LRU file cache supporting.
#[derive(Clone)]
pub struct LruCacheLayer<C: Clone> {
    // The read cache
    read_cache: ReadCache<C>,
}

impl<C: Accessor + Clone> LruCacheLayer<C> {
    /// Create a `[LruCacheLayer]` with local file cache and capacity in bytes.
    pub async fn new(file_cache: Arc<C>, capacity: usize) -> Result<Self> {
        let read_cache = ReadCache::new(file_cache, capacity);
        let (entries, bytes) = read_cache.recover_cache().await?;

        info!(
            "Recovered {} entries and total size {} in bytes for LruCacheLayer",
            entries, bytes
        );

        Ok(Self { read_cache })
    }

    /// Returns true when the local cache contains the specific file
    pub async fn contains_file(&self, path: &str) -> bool {
        self.read_cache.contains_file(path).await
    }

    /// Returns the read cache statistics info `(EntryCount, SizeInBytes)`.
    pub async fn read_cache_stat(&self) -> (u64, u64) {
        self.read_cache.stat().await
    }
}

impl<I: Accessor, C: Accessor + Clone> Layer<I> for LruCacheLayer<C> {
    type LayeredAccessor = LruCacheAccessor<I, C>;

    fn layer(&self, inner: I) -> Self::LayeredAccessor {
        LruCacheAccessor {
            inner,
            read_cache: self.read_cache.clone(),
        }
    }
}

#[derive(Debug)]
pub struct LruCacheAccessor<I, C: Clone> {
    inner: I,
    read_cache: ReadCache<C>,
}

#[async_trait]
impl<I: Accessor, C: Accessor + Clone> LayeredAccessor for LruCacheAccessor<I, C> {
    type Inner = I;
    type Reader = Box<dyn Read>;
    type BlockingReader = I::BlockingReader;
    type Writer = I::Writer;
    type BlockingWriter = I::BlockingWriter;
    type Lister = I::Lister;
    type BlockingLister = I::BlockingLister;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        self.read_cache.read(&self.inner, path, args).await
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let result = self.inner.write(path, args).await;

        self.read_cache
            .invalidate_entries_with_prefix(format!("{:x}", md5::compute(path)))
            .await;

        result
    }

    async fn delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        let result = self.inner.delete(path, args).await;

        self.read_cache
            .invalidate_entries_with_prefix(format!("{:x}", md5::compute(path)))
            .await;

        result
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

        self.read_cache
            .blocking_invalidate_entries_with_prefix(format!("{:x}", md5::compute(path)));

        result
    }

    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, Self::BlockingLister)> {
        self.inner.blocking_list(path, args)
    }
}
