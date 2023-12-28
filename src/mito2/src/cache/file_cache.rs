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

//! A cache for files.

use std::time::Instant;

use common_base::readable_size::ReadableSize;
use common_telemetry::{info, warn};
use futures::{FutureExt, TryStreamExt};
use moka::future::Cache;
use moka::notification::RemovalCause;
use object_store::util::{join_dir, join_path};
use object_store::{Metakey, ObjectStore};
use snafu::ResultExt;
use store_api::storage::RegionId;

use crate::cache::FILE_TYPE;
use crate::error::{OpenDalSnafu, Result};
use crate::metrics::CACHE_BYTES;
use crate::sst::file::FileId;

/// Subdirectory of cached files.
const FILE_DIR: &str = "files";

/// A file cache manages files on local store and evict files based
/// on size.
#[derive(Debug)]
pub(crate) struct FileCache {
    /// Local store to cache files.
    local_store: ObjectStore,
    /// Cached file directory under cache home.
    file_dir: String,
    /// Index to track cached files.
    ///
    /// File id is enough to identity a file uniquely.
    memory_index: Cache<IndexKey, IndexValue>,
}

impl FileCache {
    /// Creates a new file cache.
    pub(crate) fn new(
        local_store: ObjectStore,
        cache_home: String,
        capacity: ReadableSize,
    ) -> FileCache {
        // Stores files under `cache_home/{FILE_DIR}`.
        let file_dir = cache_file_dir(&cache_home);
        let cache_store = local_store.clone();
        let cache_file_dir = file_dir.clone();
        let memory_index = Cache::builder().max_capacity(capacity.as_bytes()).weigher(|_key, value: &IndexValue| -> u32 {
                // We only measure space on local store.
                value.file_size
            })
            .async_eviction_listener(move |key, value, cause| {
                let store = cache_store.clone();
                let file_path = cache_file_path(&cache_file_dir, *key);
                async move {
                    if let RemovalCause::Replaced = cause {
                        // The cache is replaced by another file. This is unexpected, we don't remove the same
                        // file but updates the metrics as the file is already replaced by users.
                        CACHE_BYTES.with_label_values(&[FILE_TYPE]).sub(value.file_size.into());
                        warn!("Replace existing cache {} for region {} unexpectedly", file_path, key.0);
                        return;
                    }

                    match store.delete(&file_path).await {
                        Ok(()) => {
                            CACHE_BYTES.with_label_values(&[FILE_TYPE]).sub(value.file_size.into());
                        }
                        Err(e) => {
                            warn!(e; "Failed to delete cached file {} for region {}", file_path, key.0);
                        }
                    }
                }
                .boxed()
            })
            .build();
        FileCache {
            local_store,
            file_dir,
            memory_index,
        }
    }

    /// Puts a file into the cache.
    ///
    /// Callers should ensure the file is in the correct path.
    pub(crate) async fn put(&self, key: IndexKey, value: IndexValue) {
        CACHE_BYTES
            .with_label_values(&[FILE_TYPE])
            .add(value.file_size.into());
        self.memory_index.insert(key, value).await;
    }

    /// Recovers the index from local store.
    pub(crate) async fn recover(&self) -> Result<()> {
        let now = Instant::now();

        let mut lister = self
            .local_store
            .lister_with(&self.file_dir)
            .metakey(Metakey::ContentLength)
            .await
            .context(OpenDalSnafu)?;
        let (mut total_size, mut total_keys) = (0, 0);
        while let Some(entry) = lister.try_next().await.context(OpenDalSnafu)? {
            let meta = entry.metadata();
            if !meta.is_file() {
                continue;
            }
            let Some(key) = parse_index_key(entry.name()) else {
                continue;
            };
            let file_size = meta.content_length() as u32;
            self.memory_index
                .insert(key, IndexValue { file_size })
                .await;
            total_size += file_size;
            total_keys += 1;
        }
        // The metrics is a signed int gauge so we can updates it finally.
        CACHE_BYTES
            .with_label_values(&[FILE_TYPE])
            .add(total_size.into());

        info!(
            "Recovered file cache, num_keys: {}, num_bytes: {}, cost: {:?}",
            total_keys,
            total_size,
            now.elapsed()
        );

        Ok(())
    }
}

/// Key of file cache index.
pub(crate) type IndexKey = (RegionId, FileId);

/// An entity that describes the file in the file cache.
///
/// It should only keep minimal information needed by the cache.
#[derive(Debug, Clone)]
pub(crate) struct IndexValue {
    /// Size of the file in bytes.
    file_size: u32,
}

/// Returns the directory to store files.
fn cache_file_dir(cache_home: &str) -> String {
    join_dir(cache_home, FILE_DIR)
}

/// Generates the path to the cached file.
///
/// The file name format is `{region_id}.{file_id}`
fn cache_file_path(cache_file_dir: &str, key: IndexKey) -> String {
    join_path(cache_file_dir, &format!("{}.{}", key.0.as_u64(), key.1))
}

/// Parse index key from the file name.
fn parse_index_key(name: &str) -> Option<IndexKey> {
    let mut splited = name.split('.');
    let region_id = splited.next().and_then(|s| {
        let id = s.parse::<u64>().ok()?;
        Some(RegionId::from_u64(id))
    })?;
    let file_id = splited.next().and_then(|s| FileId::parse_str(s).ok())?;

    Some((region_id, file_id))
}
