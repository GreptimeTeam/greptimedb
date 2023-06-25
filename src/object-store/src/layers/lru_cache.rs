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

use std::num::NonZeroUsize;
use std::ops::DerefMut;
use std::sync::Arc;

use async_trait::async_trait;
use lru::LruCache;
use metrics::increment_counter;
use opendal::raw::oio::{Page, Read, Reader, Write};
use opendal::raw::{
    Accessor, Layer, LayeredAccessor, OpAppend, OpDelete, OpList, OpRead, OpWrite, RpAppend,
    RpDelete, RpList, RpRead, RpWrite,
};
use opendal::{ErrorKind, Result};
use tokio::sync::Mutex;

use crate::metrics::{
    OBJECT_STORE_LRU_CACHE_ERROR, OBJECT_STORE_LRU_CACHE_ERROR_KIND, OBJECT_STORE_LRU_CACHE_HIT,
    OBJECT_STORE_LRU_CACHE_MISS,
};

#[derive(Clone)]
pub struct LruCacheLayer<C> {
    cache: Arc<C>,
    lru_cache: Arc<Mutex<LruCache<String, ()>>>,
}

impl<C: Accessor + Clone> LruCacheLayer<C> {
    pub async fn new(cache: Arc<C>, capacity: usize) -> Result<Self> {
        let layer = Self {
            cache,
            lru_cache: Arc::new(Mutex::new(LruCache::new(
                NonZeroUsize::new(capacity).unwrap(),
            ))),
        };
        layer.recover_keys().await?;

        Ok(layer)
    }

    /// Recover existing keys from `cache` to `lru_cache`.
    async fn recover_keys(&self) -> Result<()> {
        let (_, mut pager) = self.cache.list("/", OpList::default()).await?;

        let mut lru_cache = self.lru_cache.lock().await;
        while let Some(entries) = pager.next().await? {
            for entry in entries {
                let _ = lru_cache.push(entry.path().to_string(), ());
            }
        }

        Ok(())
    }

    pub async fn lru_contains_key(&self, key: &str) -> bool {
        self.lru_cache.lock().await.contains(key)
    }
}

impl<I: Accessor, C: Accessor> Layer<I> for LruCacheLayer<C> {
    type LayeredAccessor = LruCacheAccessor<I, C>;

    fn layer(&self, inner: I) -> Self::LayeredAccessor {
        LruCacheAccessor {
            inner,
            cache: self.cache.clone(),
            lru_cache: self.lru_cache.clone(),
        }
    }
}

#[derive(Debug)]
pub struct LruCacheAccessor<I, C> {
    inner: I,
    cache: Arc<C>,
    lru_cache: Arc<Mutex<LruCache<String, ()>>>,
}

impl<I, C> LruCacheAccessor<I, C> {
    fn cache_path(&self, path: &str, args: &OpRead) -> String {
        format!(
            "{:x}.cache-{}",
            md5::compute(path),
            args.range().to_header()
        )
    }
}

use opendal::raw::oio::ReadExt;

#[async_trait]
impl<I: Accessor, C: Accessor> LayeredAccessor for LruCacheAccessor<I, C> {
    type Inner = I;
    type Reader = Box<dyn Read>;
    type BlockingReader = I::BlockingReader;
    type Writer = I::Writer;
    type BlockingWriter = I::BlockingWriter;
    type Pager = I::Pager;
    type BlockingPager = I::BlockingPager;
    type Appender = I::Appender;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let path = path.to_string();
        let cache_path = self.cache_path(&path, &args);
        let lru_cache = &self.lru_cache;

        // the args is already in the cache path, so we must create a new OpRead.
        match self.cache.read(&cache_path, OpRead::default()).await {
            Ok((rp, r)) => {
                increment_counter!(OBJECT_STORE_LRU_CACHE_HIT);

                // update lru when cache hit
                let mut lru_cache = lru_cache.lock().await;
                let _ = lru_cache.get_or_insert(cache_path.clone(), || ());
                Ok(to_output_reader((rp, r)))
            }
            Err(err) if err.kind() == ErrorKind::NotFound => {
                increment_counter!(OBJECT_STORE_LRU_CACHE_MISS);

                let (_, mut reader) = self.inner.read(&path, args.clone()).await?;
                let (_, mut writer) = self.cache.write(&cache_path, OpWrite::new()).await?;

                while let Some(bytes) = reader.next().await {
                    writer.write(bytes?).await?;
                }

                writer.close().await?;

                match self.cache.read(&cache_path, OpRead::default()).await {
                    Ok((rp, reader)) => {
                        let r = {
                            // push new cache file name to lru
                            let mut lru_cache = lru_cache.lock().await;
                            lru_cache.push(cache_path.clone(), ())
                        };
                        // delete the evicted cache file
                        if let Some((k, _v)) = r {
                            let _ = self.cache.delete(&k, OpDelete::new()).await;
                        }
                        return Ok(to_output_reader((rp, reader)));
                    }
                    Err(_) => return self.inner.read(&path, args).await.map(to_output_reader),
                }
            }
            Err(err) => {
                increment_counter!(OBJECT_STORE_LRU_CACHE_ERROR, OBJECT_STORE_LRU_CACHE_ERROR_KIND => format!("{}", err.kind()));
                return self.inner.read(&path, args).await.map(to_output_reader);
            }
        }
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        self.inner.write(path, args).await
    }

    async fn append(&self, path: &str, args: OpAppend) -> Result<(RpAppend, Self::Appender)> {
        self.inner.append(path, args).await
    }

    async fn delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        let cache_path = md5::compute(path);
        let lru_cache = &self.lru_cache;

        let cache_files: Vec<String> = {
            let mut guard = lru_cache.lock().await;
            let lru = guard.deref_mut();
            let cache_files = lru
                .iter()
                .filter(|(k, _v)| k.starts_with(format!("{:x}.cache-", cache_path).as_str()))
                .map(|(k, _v)| k.clone())
                .collect::<Vec<_>>();
            for k in &cache_files {
                let _ = lru.pop(k);
            }
            cache_files
        };
        for file in cache_files {
            let _ = self.cache.delete(&file, OpDelete::new()).await;
        }
        self.inner.delete(path, args).await
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Pager)> {
        self.inner.list(path, args).await
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        self.inner.blocking_read(path, args)
    }

    fn blocking_write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::BlockingWriter)> {
        self.inner.blocking_write(path, args)
    }

    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, Self::BlockingPager)> {
        self.inner.blocking_list(path, args)
    }
}

#[inline]
fn to_output_reader<R: Read + 'static>(input: (RpRead, R)) -> (RpRead, Reader) {
    (input.0, Box::new(input.1))
}
