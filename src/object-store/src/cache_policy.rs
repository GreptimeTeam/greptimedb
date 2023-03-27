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
use bytes::Bytes;
use lru::LruCache;
use opendal::ops::{OpDelete, OpList, OpRead, OpScan, OpWrite};
use opendal::raw::oio::{Read, Reader, Write};
use opendal::raw::{Accessor, Layer, LayeredAccessor, RpDelete, RpList, RpRead, RpScan, RpWrite};
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
                Ok(to_output_reader((rp, r)))
            }
            Err(err) if err.kind() == ErrorKind::NotFound => {
                let (rp, mut reader) = self.inner.read(&path, args.clone()).await?;
                let size = rp.clone().into_metadata().content_length();
                let (_, mut writer) = self.cache.write(&cache_path, OpWrite::new()).await?;

                // TODO(hl): We can use [Writer::append](https://docs.rs/opendal/0.30.4/opendal/struct.Writer.html#method.append)
                // here to avoid loading whole file into memory once all our backend supports `Writer`.
                let mut buf = vec![0; size as usize];
                reader.read(&mut buf).await?;
                writer.write(Bytes::from(buf)).await?;
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
            Err(_) => return self.inner.read(&path, args).await.map(to_output_reader),
        }
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        self.inner.write(path, args).await
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

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        self.inner.blocking_read(path, args)
    }

    fn blocking_write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::BlockingWriter)> {
        self.inner.blocking_write(path, args)
    }

    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, Self::BlockingPager)> {
        self.inner.blocking_list(path, args)
    }

    fn blocking_scan(&self, path: &str, args: OpScan) -> Result<(RpScan, Self::BlockingPager)> {
        self.inner.blocking_scan(path, args)
    }
}

#[inline]
fn to_output_reader<R: Read + 'static>(input: (RpRead, R)) -> (RpRead, Reader) {
    (input.0, Box::new(input.1))
}
