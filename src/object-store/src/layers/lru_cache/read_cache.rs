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

use common_telemetry::logging::debug;
use futures::FutureExt;
use moka::future::Cache;
use moka::notification::ListenerFuture;
use opendal::raw::oio::{ListExt, Read, ReadExt, Reader, WriteExt};
use opendal::raw::{Accessor, OpDelete, OpList, OpRead, OpStat, OpWrite, RpRead};
use opendal::{Error as OpendalError, ErrorKind, Result};

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
fn read_cache_key(path: &str, args: &OpRead) -> String {
    format!(
        "{:x}.cache-{}",
        md5::compute(path),
        args.range().to_header()
    )
}

/// Local read cache for files in object storage
#[derive(Clone, Debug)]
pub(crate) struct ReadCache<C: Clone> {
    /// Local file cache backend
    file_cache: Arc<C>,
    /// Local memory cache to track local cache files
    mem_cache: Cache<String, ReadResult>,
}

impl<C: Accessor + Clone> ReadCache<C> {
    /// Create a [`ReadCache`] with capacity in bytes.
    pub(crate) fn new(file_cache: Arc<C>, capacity: usize) -> Self {
        let file_cache_cloned = file_cache.clone();
        let eviction_listener =
            move |read_key: Arc<String>, read_result: ReadResult, cause| -> ListenerFuture {
                // Delete the file from local file cache when it's purged from mem_cache.
                OBJECT_STORE_LRU_CACHE_ENTRIES.dec();
                let file_cache_cloned = file_cache_cloned.clone();

                async move {
                    if let ReadResult::Success(size) = read_result {
                        OBJECT_STORE_LRU_CACHE_BYTES.sub(size as i64);

                        let result = file_cache_cloned.delete(&read_key, OpDelete::new()).await;
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
        let (_, mut pager) = self.file_cache.list("/", OpList::default()).await?;

        while let Some(entry) = pager.next().await? {
            let read_key = entry.path();

            // We can't retrieve the metadata from `[opendal::raw::oio::Entry]` directly,
            // because it's private field.
            let size = {
                let stat = self.file_cache.stat(read_key, OpStat::default()).await?;

                stat.into_metadata().content_length()
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
        self.mem_cache.contains_key(path)
            && self.file_cache.stat(path, OpStat::default()).await.is_ok()
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
    ) -> Result<(RpRead, Box<dyn Read>)>
    where
        I: Accessor,
    {
        if !can_cache(path) {
            return inner.read(path, args).await.map(to_output_reader);
        }

        let read_key = read_cache_key(path, &args);

        let read_result = self
            .mem_cache
            .try_get_with(
                read_key.clone(),
                self.read_remote(inner, &read_key, path, args.clone()),
            )
            .await
            .map_err(|e| OpendalError::new(e.kind(), &e.to_string()))?;

        match read_result {
            ReadResult::Success(_) => {
                // There is a concurrent issue here, the local cache may be purged
                // while reading, we have to fallback to remote read
                match self.file_cache.read(&read_key, OpRead::default()).await {
                    Ok(ret) => {
                        OBJECT_STORE_LRU_CACHE_HIT
                            .with_label_values(&["success"])
                            .inc();
                        Ok(to_output_reader(ret))
                    }
                    Err(_) => {
                        OBJECT_STORE_LRU_CACHE_MISS.inc();
                        inner.read(path, args).await.map(to_output_reader)
                    }
                }
            }
            ReadResult::NotFound => {
                OBJECT_STORE_LRU_CACHE_HIT
                    .with_label_values(&["not_found"])
                    .inc();

                Err(OpendalError::new(
                    ErrorKind::NotFound,
                    &format!("File not found: {path}"),
                ))
            }
        }
    }

    async fn try_write_cache<I>(&self, mut reader: I::Reader, read_key: &str) -> Result<usize>
    where
        I: Accessor,
    {
        let (_, mut writer) = self.file_cache.write(read_key, OpWrite::new()).await?;
        let mut total = 0;
        while let Some(bytes) = reader.next().await {
            let bytes = &bytes?;
            total += bytes.len();
            writer.write(bytes).await?;
        }
        // Call `close` to ensure data is written.
        writer.close().await?;
        Ok(total)
    }

    /// Read the file from remote storage. If success, write the content into local cache.
    async fn read_remote<I>(
        &self,
        inner: &I,
        read_key: &str,
        path: &str,
        args: OpRead,
    ) -> Result<ReadResult>
    where
        I: Accessor,
    {
        OBJECT_STORE_LRU_CACHE_MISS.inc();

        let (_, reader) = inner.read(path, args).await?;
        let result = self.try_write_cache::<I>(reader, read_key).await;

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
}

fn to_output_reader<R: Read + 'static>(input: (RpRead, R)) -> (RpRead, Reader) {
    (input.0, Box::new(input.1))
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
