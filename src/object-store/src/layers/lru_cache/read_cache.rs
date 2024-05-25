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

use common_telemetry::debug;
use futures::{FutureExt, StreamExt};
use moka::future::Cache;
use moka::notification::ListenerFuture;
use opendal::raw::oio::{Read, ReadDyn, Reader};
use opendal::raw::{Access, BytesRange, OpRead, RpRead};
use opendal::{Buffer, Error as OpendalError, ErrorKind, Operator, Result};

use crate::metrics::{
    OBJECT_STORE_LRU_CACHE_BYTES, OBJECT_STORE_LRU_CACHE_ENTRIES, OBJECT_STORE_LRU_CACHE_HIT,
    OBJECT_STORE_LRU_CACHE_MISS, OBJECT_STORE_READ_ERROR,
};

/// Cache value for read file
#[derive(Debug, Clone, PartialEq, Eq, Copy)]
enum ReadResult {
    // Read success with size
    Success(u32),
    // File not found
    NotFound,
}

impl ReadResult {
    fn size_bytes(&self) -> u32 {
        match self {
            ReadResult::NotFound => 0,
            ReadResult::Success(size) => *size,
        }
    }
}

/// Returns true when the path of the file can be cached.
fn can_cache(path: &str) -> bool {
    // TODO(dennis): find a better way
    !path.ends_with("_last_checkpoint")
}

/// Generate an unique cache key for the read path and range.
fn read_cache_key(path: &str, range: BytesRange) -> String {
    format!("{:x}.cache-{}", md5::compute(path), range.to_header())
}

/// Local read cache for files in object storage
#[derive(Clone, Debug)]
pub(crate) struct ReadCache {
    /// Local file cache backend
    file_cache: Operator,
    /// Local memory cache to track local cache files
    mem_cache: Cache<String, ReadResult>,
}

impl ReadCache {
    /// Create a [`ReadCache`] with capacity in bytes.
    pub(crate) fn new(file_cache: Operator, capacity: usize) -> Self {
        let file_cache_cloned = file_cache.clone();
        let eviction_listener =
            move |read_key: Arc<String>, read_result: ReadResult, cause| -> ListenerFuture {
                // Delete the file from local file cache when it's purged from mem_cache.
                OBJECT_STORE_LRU_CACHE_ENTRIES.dec();
                let file_cache_cloned = file_cache_cloned.clone();

                async move {
                    if let ReadResult::Success(size) = read_result {
                        OBJECT_STORE_LRU_CACHE_BYTES.sub(size as i64);

                        let result = file_cache_cloned.delete(&read_key).await;
                        debug!(
                            "Deleted local cache file `{}`, result: {:?}, cause: {:?}.",
                            read_key, result, cause
                        );
                    }
                }
                .boxed()
            };

        Self {
            file_cache,
            mem_cache: Cache::builder()
                .max_capacity(capacity as u64)
                .weigher(|_key, value: &ReadResult| -> u32 {
                    // TODO(dennis): add key's length to weight?
                    value.size_bytes()
                })
                .async_eviction_listener(eviction_listener)
                .support_invalidation_closures()
                .build(),
        }
    }

    /// Returns the cache's entry count and total approximate entry size in bytes.
    pub(crate) async fn stat(&self) -> (u64, u64) {
        self.mem_cache.run_pending_tasks().await;

        (self.mem_cache.entry_count(), self.mem_cache.weighted_size())
    }

    /// Invalidate all cache items which key starts with `prefix`.
    pub(crate) async fn invalidate_entries_with_prefix(&self, prefix: String) {
        // Safety: always ok when building cache with `support_invalidation_closures`.
        self.mem_cache
            .invalidate_entries_if(move |k: &String, &_v| k.starts_with(&prefix))
            .ok();
    }

    /// Blocking version of `invalidate_entries_with_prefix`.
    pub(crate) fn blocking_invalidate_entries_with_prefix(&self, prefix: String) {
        // Safety: always ok when building cache with `support_invalidation_closures`.
        self.mem_cache
            .invalidate_entries_if(move |k: &String, &_v| k.starts_with(&prefix))
            .ok();
    }

    /// Recover existing cache items from `file_cache` to `mem_cache`.
    /// Return entry count and total approximate entry size in bytes.
    pub(crate) async fn recover_cache(&self) -> Result<(u64, u64)> {
        let mut pager = self.file_cache.lister("/").await?;

        while let Some(entry) = pager.next().await.transpose()? {
            let read_key = entry.path();

            // We can't retrieve the metadata from `[opendal::raw::oio::Entry]` directly,
            // because it's private field.
            let size = {
                let stat = self.file_cache.stat(read_key).await?;

                stat.content_length()
            };

            OBJECT_STORE_LRU_CACHE_ENTRIES.inc();
            OBJECT_STORE_LRU_CACHE_BYTES.add(size as i64);
            self.mem_cache
                .insert(read_key.to_string(), ReadResult::Success(size as u32))
                .await;
        }

        Ok(self.stat().await)
    }

    /// Returns true when the read cache contains the specific file.
    pub(crate) async fn contains_file(&self, path: &str) -> bool {
        self.mem_cache.run_pending_tasks().await;
        self.mem_cache.contains_key(path) && self.file_cache.stat(path).await.is_ok()
    }

    /// Read from a specific path using the OpRead operation.
    /// It will attempt to retrieve the data from the local cache.
    /// If the data is not found in the local cache,
    /// it will fallback to retrieving it from remote object storage
    /// and cache the result locally.
    pub(crate) async fn read<I>(
        &self,
        inner: &I,
        path: &str,
        args: OpRead,
    ) -> Result<(RpRead, Arc<dyn ReadDyn>)>
    where
        I: Access,
    {
        if !can_cache(path) {
            return inner.read(path, args).await.map(to_output_reader);
        }

        let (rp, reader) = inner.read(path, args).await?;
        let reader: ReadCacheReader<I> = ReadCacheReader {
            path: Arc::new(path.to_string()),
            inner_reader: reader,
            file_cache: self.file_cache.clone(),
            mem_cache: self.mem_cache.clone(),
        };
        Ok((rp, Arc::new(reader)))
    }
}

pub struct ReadCacheReader<I: Access> {
    /// Path of the file
    path: Arc<String>,
    /// Remote file reader.
    inner_reader: I::Reader,
    /// Local file cache backend
    file_cache: Operator,
    /// Local memory cache to track local cache files
    mem_cache: Cache<String, ReadResult>,
}

impl<I: Access> ReadCacheReader<I> {
    /// TODO: we can return the Buffer directly to avoid another read from cache.
    async fn read_remote(&self, offset: u64, limit: usize) -> Result<ReadResult> {
        OBJECT_STORE_LRU_CACHE_MISS.inc();

        let buf = self.inner_reader.read_at(offset, limit).await?;
        let result = self.try_write_cache(buf, offset, limit).await;

        match result {
            Ok(read_bytes) => {
                OBJECT_STORE_LRU_CACHE_ENTRIES.inc();
                OBJECT_STORE_LRU_CACHE_BYTES.add(read_bytes as i64);

                Ok(ReadResult::Success(read_bytes as u32))
            }

            Err(e) if e.kind() == ErrorKind::NotFound => {
                OBJECT_STORE_READ_ERROR
                    .with_label_values(&[e.kind().to_string().as_str()])
                    .inc();
                OBJECT_STORE_LRU_CACHE_ENTRIES.inc();

                Ok(ReadResult::NotFound)
            }

            Err(e) => {
                OBJECT_STORE_READ_ERROR
                    .with_label_values(&[e.kind().to_string().as_str()])
                    .inc();
                Err(e)
            }
        }
    }

    async fn try_write_cache(&self, buf: Buffer, offset: u64, limit: usize) -> Result<usize> {
        let size = buf.len();
        let read_key = read_cache_key(&self.path, BytesRange::new(offset, Some(limit as _)));
        self.file_cache.write(&read_key, buf).await?;
        Ok(size)
    }
}

impl<I: Access> Read for ReadCacheReader<I> {
    async fn read_at(&self, offset: u64, limit: usize) -> Result<Buffer> {
        let read_key = read_cache_key(&self.path, BytesRange::new(offset, Some(limit as _)));

        let read_result = self
            .mem_cache
            .try_get_with(read_key.clone(), self.read_remote(offset, limit))
            .await
            .map_err(|e| OpendalError::new(e.kind(), &e.to_string()))?;

        match read_result {
            ReadResult::Success(_) => {
                // There is a concurrent issue here, the local cache may be purged
                // while reading, we have to fallback to remote read
                match self.file_cache.read(&read_key).await {
                    Ok(ret) => {
                        OBJECT_STORE_LRU_CACHE_HIT
                            .with_label_values(&["success"])
                            .inc();
                        Ok(ret)
                    }
                    Err(_) => {
                        OBJECT_STORE_LRU_CACHE_MISS.inc();
                        self.inner_reader.read_at(offset, limit).await
                    }
                }
            }
            ReadResult::NotFound => {
                OBJECT_STORE_LRU_CACHE_HIT
                    .with_label_values(&["not_found"])
                    .inc();

                Err(OpendalError::new(
                    ErrorKind::NotFound,
                    &format!("File not found: {}", self.path),
                ))
            }
        }
    }
}

fn to_output_reader<R: Read + 'static>(input: (RpRead, R)) -> (RpRead, Reader) {
    (input.0, Arc::new(input.1))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_can_cache() {
        assert!(can_cache("test"));
        assert!(can_cache("a/b/c.parquet"));
        assert!(can_cache("1.json"));
        assert!(can_cache("100.checkpoint"));
        assert!(can_cache("test/last_checkpoint"));
        assert!(!can_cache("test/__last_checkpoint"));
        assert!(!can_cache("a/b/c/__last_checkpoint"));
    }
}
