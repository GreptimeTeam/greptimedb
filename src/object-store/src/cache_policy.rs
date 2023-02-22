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

use std::io;
use std::num::NonZeroUsize;
use std::ops::DerefMut;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_trait::async_trait;
use futures::AsyncRead;
use lru::LruCache;
use opendal::ops::*;
use opendal::raw::*;
use opendal::{ErrorKind, Result};
use tokio::sync::Mutex;

pub struct LruCacheLayer<C> {
    cache: Arc<C>,
    lru_cache: Arc<Mutex<LruCache<String, ()>>>,
}

impl<C: Accessor> LruCacheLayer<C> {
    pub fn new(cache: Arc<C>, capacity: usize) -> Self {
        Self {
            cache,
            lru_cache: Arc::new(Mutex::new(LruCache::new(
                NonZeroUsize::new(capacity).unwrap(),
            ))),
        }
    }
}

impl<I: Accessor, C: Accessor> Layer<I> for LruCacheLayer<C> {
    type LayeredAccessor = LruCacheAccessor<I, C>;

    fn layer(&self, inner: I) -> Self::LayeredAccessor {
        LruCacheAccessor {
            inner: Arc::new(inner),
            cache: self.cache.clone(),
            lru_cache: self.lru_cache.clone(),
        }
    }
}

#[derive(Debug)]
pub struct LruCacheAccessor<I, C> {
    inner: Arc<I>,
    cache: Arc<C>,
    lru_cache: Arc<Mutex<LruCache<String, ()>>>,
}

impl<I, C> LruCacheAccessor<I, C> {
    fn cache_path(&self, path: &str, args: &OpRead) -> String {
        format!("{}.cache-{}", path, args.range().to_header())
    }
}

#[async_trait]
impl<I: Accessor, C: Accessor> LayeredAccessor for LruCacheAccessor<I, C> {
    type Inner = I;
    type Reader = output::Reader;
    type BlockingReader = I::BlockingReader;
    type Pager = I::Pager;
    type BlockingPager = I::BlockingPager;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let path = path.to_string();
        let cache_path = self.cache_path(&path, &args);
        let lru_cache = self.lru_cache.clone();

        match self.cache.read(&cache_path, OpRead::default()).await {
            Ok((rp, r)) => {
                // update lru when cache hit
                let mut lru_cache = lru_cache.lock().await;
                lru_cache.get_or_insert(cache_path.clone(), || ());
                Ok((rp, Box::new(r) as output::Reader))
            }
            Err(err) if err.kind() == ErrorKind::ObjectNotFound => {
                let (rp, reader) = self.inner.read(&path, args.clone()).await?;
                let size = rp.clone().into_metadata().content_length();
                let _ = self
                    .cache
                    .write(
                        &cache_path,
                        OpWrite::new(size),
                        Box::new(ReadWrppaer(reader)),
                    )
                    .await?;
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
                        Ok((rp, Box::new(reader) as output::Reader))
                    }
                    Err(_) => {
                        return self
                            .inner
                            .read(&path, args)
                            .await
                            .map(|(rp, r)| (rp, Box::new(r) as output::Reader))
                    }
                }
            }
            Err(_) => {
                return self
                    .inner
                    .read(&path, args)
                    .await
                    .map(|(rp, r)| (rp, Box::new(r) as output::Reader))
            }
        }
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        self.inner.blocking_read(path, args)
    }

    async fn delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        let path = path.to_string();
        let lru_cache = self.lru_cache.clone();

        let cache_files: Vec<String> = {
            let mut guard = lru_cache.lock().await;
            let lru = guard.deref_mut();
            let cache_files = lru
                .iter()
                .filter(|(k, _v)| k.starts_with(format!("{path}.cache-").as_str()))
                .map(|(k, _v)| k.clone())
                .collect::<Vec<_>>();
            for k in &cache_files {
                lru.pop(k);
            }
            cache_files
        };
        for file in cache_files {
            let _ = self.cache.delete(&file, OpDelete::new()).await;
        }
        return self.inner.delete(&path, args).await;
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Pager)> {
        self.inner.list(path, args).await
    }

    async fn scan(&self, path: &str, args: OpScan) -> Result<(RpScan, Self::Pager)> {
        self.inner.scan(path, args).await
    }

    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, Self::BlockingPager)> {
        self.inner.blocking_list(path, args)
    }

    fn blocking_scan(&self, path: &str, args: OpScan) -> Result<(RpScan, Self::BlockingPager)> {
        self.inner.blocking_scan(path, args)
    }
}

/// Workaround for output::Read doesn't implement input::Read
///
/// Should be remove after opendal fixed it.
struct ReadWrppaer<R>(R);

impl<R: output::Read> AsyncRead for ReadWrppaer<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        self.0.poll_read(cx, buf)
    }
}
