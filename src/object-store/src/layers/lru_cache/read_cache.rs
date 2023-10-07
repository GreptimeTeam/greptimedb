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
use std::pin::Pin;
use std::sync::Arc;

use common_telemetry::logging::debug;
use futures::{Future, FutureExt};
use metrics::{decrement_gauge, increment_counter, increment_gauge};
use moka::future::Cache;
use moka::notification::ListenerFuture;
use opendal::raw::oio::{Page, Read, ReadExt, Reader, WriteExt};
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
                decrement_gauge!(OBJECT_STORE_LRU_CACHE_ENTRIES, 1.0);
                let file_cache_cloned = file_cache_cloned.clone();

                async move {
                    if let ReadResult::Success(size) = read_result {
                        decrement_gauge!(OBJECT_STORE_LRU_CACHE_BYTES, size as f64);

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

    /// Invalidte all cache items which key starts with `prefix`.
    pub(crate) async fn invalidate_entries_with_prefix(&self, prefix: String) {
        // Safety: always ok when building cache with `support_invalidation_closures`.
        self.mem_cache
            .invalidate_entries_if(move |k: &String, &_v| k.starts_with(&prefix))
            .ok();
        self.mem_cache.run_pending_tasks().await;
    }

    /// Blocking version of `invalidate_entries_with_prefix`.
    pub(crate) fn blocking_invalidate_entries_with_prefix(&self, prefix: String) {
        // Safety: always ok when building cache with `support_invalidation_closures`.
        self.mem_cache
            .invalidate_entries_if(move |k: &String, &_v| k.starts_with(&prefix))
            .ok();
        common_runtime::block_on_bg(async {
            self.mem_cache.run_pending_tasks().await;
        });
    }

    /// Recover existing cache items from `file_cache` to `mem_cache`.
    /// Return entry count and total approximate entry size in bytes.
    pub(crate) async fn recover_cache(&self) -> Result<(u64, u64)> {
        let (_, mut pager) = self.file_cache.list("/", OpList::default()).await?;

        while let Some(entries) = pager.next().await? {
            for entry in entries {
                let read_key = entry.path().to_string();

                // We can't retrieve the metadata from `[opendal::raw::oio::Entry]` directly,
                // because it's private field.
                let size = {
                    let stat = self.file_cache.stat(&read_key, OpStat::default()).await?;

                    stat.into_metadata().content_length()
                };

                increment_gauge!(OBJECT_STORE_LRU_CACHE_ENTRIES, 1.0);
                increment_gauge!(OBJECT_STORE_LRU_CACHE_BYTES, size as f64);
                self.mem_cache
                    .insert(read_key, ReadResult::Success(size as u32))
                    .await;
            }
        }

        Ok(self.stat().await)
    }

    /// Returns true when the read cache contains the specific file.
    pub(crate) async fn contains_file(&self, path: &str) -> bool {
        self.mem_cache.contains_key(path)
            && self.file_cache.stat(path, OpStat::default()).await.is_ok()
    }

    /// Read from a specific path using the OpRead operation.
    /// It will attempt to retrieve the data from the local cache.
    /// If the data is not found in the local cache,
    /// it will fallback to retrieving it from remote object storage
    /// and cache the result locally.
    pub(crate) async fn read<'life0, 'life1, 'async_trait, I, F>(
        &'life0 self,
        path: &'life1 str,
        args: OpRead,
        inner_read: F,
    ) -> Result<(RpRead, Box<dyn Read>)>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        I: Accessor,
        F: FnOnce(
                &'life1 str,
                OpRead,
            ) -> Pin<
                Box<dyn Future<Output = Result<(RpRead, I::Reader)>> + Send + 'async_trait>,
            > + Clone,
    {
        let read_key = read_cache_key(path, &args);

        let read_result = self
            .mem_cache
            .try_get_with(
                read_key.clone(),
                self.read_remote::<I, _>(&read_key, path, args.clone(), inner_read.clone()),
            )
            .await
            .map_err(|e| OpendalError::new(e.kind(), &e.to_string()))?;

        let cache_result = match read_result {
            ReadResult::Success(_) => {
                // There is a concurrent issue here, the local cache may be purged
                // while reading, we have to fallback to remote read
                match self.file_cache.read(&read_key, OpRead::default()).await {
                    Ok(ret) => {
                        increment_counter!(OBJECT_STORE_LRU_CACHE_HIT, "result" => "success");
                        Ok(to_output_reader(ret))
                    }
                    Err(_) => {
                        increment_counter!(OBJECT_STORE_LRU_CACHE_MISS);
                        inner_read(path, args).await.map(to_output_reader)
                    }
                }
            }
            ReadResult::NotFound => {
                increment_counter!(OBJECT_STORE_LRU_CACHE_HIT, "result" => "not_found");

                Err(OpendalError::new(
                    ErrorKind::NotFound,
                    &format!("File not found: {path}"),
                ))
            }
        };
        self.mem_cache.run_pending_tasks().await;

        cache_result
    }

    /// Read the file from remote storage. If success, write the content into local cache.
    async fn read_remote<'life0, 'life1, 'async_trait, I, F>(
        &'life0 self,
        read_key: &str,
        path: &'life1 str,
        args: OpRead,
        inner_read: F,
    ) -> Result<ReadResult>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        I: Accessor,
        F: FnOnce(
            &'life1 str,
            OpRead,
        ) -> Pin<
            Box<dyn Future<Output = Result<(RpRead, I::Reader)>> + Send + 'async_trait>,
        >,
    {
        increment_counter!(OBJECT_STORE_LRU_CACHE_MISS);

        let inner_result = inner_read(path, args).await;

        match inner_result {
            Ok((rp, mut reader)) => {
                let (_, mut writer) = self.file_cache.write(read_key, OpWrite::new()).await?;

                while let Some(bytes) = reader.next().await {
                    writer.write(&bytes?).await?;
                }

                // Call `close` to ensure data is written.
                writer.close().await?;

                let read_bytes = rp.metadata().content_length() as u32;
                increment_gauge!(OBJECT_STORE_LRU_CACHE_ENTRIES, 1.0);
                increment_gauge!(OBJECT_STORE_LRU_CACHE_BYTES, read_bytes as f64);

                Ok(ReadResult::Success(read_bytes))
            }

            Err(e) if e.kind() == ErrorKind::NotFound => {
                increment_counter!(OBJECT_STORE_READ_ERROR, "kind" => format!("{}", e.kind()));
                increment_gauge!(OBJECT_STORE_LRU_CACHE_ENTRIES, 1.0);

                Ok(ReadResult::NotFound)
            }

            Err(e) => {
                increment_counter!(OBJECT_STORE_READ_ERROR, "kind" => format!("{}", e.kind()));

                Err(e)
            }
        }
    }
}

fn to_output_reader<R: Read + 'static>(input: (RpRead, R)) -> (RpRead, Reader) {
    (input.0, Box::new(input.1))
}
