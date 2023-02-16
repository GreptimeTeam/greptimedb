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
use futures::future::BoxFuture;
use lru::LruCache;
use opendal::layers::CachePolicy;
use opendal::raw::output::Reader;
use opendal::raw::{Accessor, RpDelete, RpRead};
use opendal::{ErrorKind, OpDelete, OpRead, OpWrite, Result};
use tokio::sync::Mutex;

#[derive(Debug)]
pub struct LruCachePolicy {
    lru_cache: Arc<Mutex<LruCache<String, ()>>>,
}

impl LruCachePolicy {
    pub fn new(capacity: usize) -> Self {
        Self {
            lru_cache: Arc::new(Mutex::new(LruCache::new(
                NonZeroUsize::new(capacity).unwrap(),
            ))),
        }
    }

    fn cache_path(&self, path: &str, args: &OpRead) -> String {
        format!("{}.cache-{}", path, args.range().to_header())
    }
}

#[async_trait]
impl CachePolicy for LruCachePolicy {
    fn on_read(
        &self,
        inner: Arc<dyn Accessor>,
        cache: Arc<dyn Accessor>,
        path: &str,
        args: OpRead,
    ) -> BoxFuture<'static, Result<(RpRead, Reader)>> {
        let path = path.to_string();
        let cache_path = self.cache_path(&path, &args);
        let lru_cache = self.lru_cache.clone();
        Box::pin(async move {
            match cache.read(&cache_path, OpRead::default()).await {
                Ok(v) => {
                    // update lru when cache hit
                    let mut lru_cache = lru_cache.lock().await;
                    lru_cache.get_or_insert(cache_path.clone(), || ());
                    Ok(v)
                }
                Err(err) if err.kind() == ErrorKind::ObjectNotFound => {
                    let (rp, reader) = inner.read(&path, args.clone()).await?;
                    let size = rp.clone().into_metadata().content_length();
                    let _ = cache
                        .write(&cache_path, OpWrite::new(size), Box::new(reader))
                        .await?;
                    match cache.read(&cache_path, OpRead::default()).await {
                        Ok(v) => {
                            let r = {
                                // push new cache file name to lru
                                let mut lru_cache = lru_cache.lock().await;
                                lru_cache.push(cache_path.clone(), ())
                            };
                            // delete the evicted cache file
                            if let Some((k, _v)) = r {
                                let _ = cache.delete(&k, OpDelete::new()).await;
                            }
                            Ok(v)
                        }
                        Err(_) => inner.read(&path, args).await,
                    }
                }
                Err(_) => inner.read(&path, args).await,
            }
        })
    }

    fn on_delete(
        &self,
        inner: Arc<dyn Accessor>,
        cache: Arc<dyn Accessor>,
        path: &str,
        args: OpDelete,
    ) -> BoxFuture<'static, Result<RpDelete>> {
        let path = path.to_string();
        let lru_cache = self.lru_cache.clone();
        Box::pin(async move {
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
                let _ = cache.delete(&file, OpDelete::new()).await;
            }
            inner.delete(&path, args).await
        })
    }
}
