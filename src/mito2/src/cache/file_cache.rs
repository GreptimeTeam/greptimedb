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

use std::sync::Arc;
use std::time::Instant;

use common_base::readable_size::ReadableSize;
use common_telemetry::{info, warn};
use futures::{FutureExt, TryStreamExt};
use moka::future::Cache;
use moka::notification::RemovalCause;
use object_store::util::join_path;
use object_store::{ErrorKind, Metakey, ObjectStore, Reader};
use snafu::ResultExt;
use store_api::storage::RegionId;

use crate::cache::FILE_TYPE;
use crate::error::{OpenDalSnafu, Result};
use crate::metrics::{CACHE_BYTES, CACHE_HIT, CACHE_MISS};
use crate::sst::file::FileId;

/// Subdirectory of cached files.
const FILE_DIR: &str = "files/";

/// A file cache manages files on local store and evict files based
/// on size.
#[derive(Debug)]
pub(crate) struct FileCache {
    /// Local store to cache files.
    local_store: ObjectStore,
    /// Index to track cached files.
    ///
    /// File id is enough to identity a file uniquely.
    memory_index: Cache<IndexKey, IndexValue>,
}

pub(crate) type FileCacheRef = Arc<FileCache>;

impl FileCache {
    /// Creates a new file cache.
    pub(crate) fn new(local_store: ObjectStore, capacity: ReadableSize) -> FileCache {
        let cache_store = local_store.clone();
        let memory_index = Cache::builder()
            .weigher(|_key, value: &IndexValue| -> u32 {
                // We only measure space on local store.
                value.file_size
            })
            .max_capacity(capacity.as_bytes())
            .async_eviction_listener(move |key, value, cause| {
                let store = cache_store.clone();
                // Stores files under FILE_DIR.
                let file_path = cache_file_path(FILE_DIR, *key);
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
            memory_index,
        }
    }

    /// Puts a file into the cache index.
    ///
    /// The `WriteCache` should ensure the file is in the correct path.
    pub(crate) async fn put(&self, key: IndexKey, value: IndexValue) {
        CACHE_BYTES
            .with_label_values(&[FILE_TYPE])
            .add(value.file_size.into());
        self.memory_index.insert(key, value).await;
    }

    async fn get_reader(&self, file_path: &str) -> object_store::Result<Option<Reader>> {
        if self.local_store.is_exist(file_path).await? {
            Ok(Some(self.local_store.reader(file_path).await?))
        } else {
            Ok(None)
        }
    }

    /// Reads a file from the cache.
    pub(crate) async fn reader(&self, key: IndexKey) -> Option<Reader> {
        if !self.memory_index.contains_key(&key) {
            CACHE_MISS.with_label_values(&[FILE_TYPE]).inc();
            return None;
        }

        let file_path = self.cache_file_path(key);
        match self.get_reader(&file_path).await {
            Ok(Some(reader)) => {
                CACHE_HIT.with_label_values(&[FILE_TYPE]).inc();
                return Some(reader);
            }
            Err(e) => {
                if e.kind() != ErrorKind::NotFound {
                    warn!("Failed to get file for key {:?}, err: {}", key, e);
                }
            }
            Ok(None) => {}
        }

        // We removes the file from the index.
        self.memory_index.remove(&key).await;
        CACHE_MISS.with_label_values(&[FILE_TYPE]).inc();
        None
    }

    /// Removes a file from the cache explicitly.
    pub(crate) async fn remove(&self, key: IndexKey) {
        let file_path = self.cache_file_path(key);
        self.memory_index.remove(&key).await;
        if let Err(e) = self.local_store.delete(&file_path).await {
            warn!(e; "Failed to delete a cached file {}", file_path);
        }
    }

    /// Recovers the index from local store.
    pub(crate) async fn recover(&self) -> Result<()> {
        let now = Instant::now();

        let mut lister = self
            .local_store
            .lister_with(FILE_DIR)
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

    /// Returns the cache file path for the key.
    pub(crate) fn cache_file_path(&self, key: IndexKey) -> String {
        cache_file_path(FILE_DIR, key)
    }

    /// Returns the local store of the file cache.
    pub(crate) fn local_store(&self) -> ObjectStore {
        self.local_store.clone()
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

/// Generates the path to the cached file.
///
/// The file name format is `{region_id}.{file_id}`
fn cache_file_path(cache_file_dir: &str, key: IndexKey) -> String {
    join_path(cache_file_dir, &format!("{}.{}", key.0.as_u64(), key.1))
}

/// Parse index key from the file name.
fn parse_index_key(name: &str) -> Option<IndexKey> {
    let mut splited = name.splitn(2, '.');
    let region_id = splited.next().and_then(|s| {
        let id = s.parse::<u64>().ok()?;
        Some(RegionId::from_u64(id))
    })?;
    let file_id = splited.next().and_then(|s| FileId::parse_str(s).ok())?;

    Some((region_id, file_id))
}

#[cfg(test)]
mod tests {
    use common_test_util::temp_dir::create_temp_dir;
    use futures::AsyncReadExt;
    use object_store::services::Fs;

    use super::*;

    fn new_fs_store(path: &str) -> ObjectStore {
        let mut builder = Fs::default();
        builder.root(path);
        ObjectStore::new(builder).unwrap().finish()
    }

    #[tokio::test]
    async fn test_file_cache_basic() {
        let dir = create_temp_dir("");
        let local_store = new_fs_store(dir.path().to_str().unwrap());

        let cache = FileCache::new(local_store.clone(), ReadableSize::mb(10));
        let region_id = RegionId::new(2000, 0);
        let file_id = FileId::random();
        let key = (region_id, file_id);
        let file_path = cache.cache_file_path(key);

        // Get an empty file.
        assert!(cache.reader(key).await.is_none());

        // Write a file.
        local_store
            .write(&file_path, b"hello".as_slice())
            .await
            .unwrap();
        // Add to the cache.
        cache
            .put((region_id, file_id), IndexValue { file_size: 5 })
            .await;

        // Read file content.
        let mut reader = cache.reader(key).await.unwrap();
        let mut buf = String::new();
        reader.read_to_string(&mut buf).await.unwrap();
        assert_eq!("hello", buf);

        // Remove the file.
        cache.remove(key).await;
        assert!(cache.reader(key).await.is_none());

        // Ensure all pending tasks of the moka cache is done before assertion.
        cache.memory_index.run_pending_tasks().await;

        // The file also not exists.
        assert!(!local_store.is_exist(&file_path).await.unwrap());
    }

    #[tokio::test]
    async fn test_file_cache_file_removed() {
        let dir = create_temp_dir("");
        let local_store = new_fs_store(dir.path().to_str().unwrap());

        let cache = FileCache::new(local_store.clone(), ReadableSize::mb(10));
        let region_id = RegionId::new(2000, 0);
        let file_id = FileId::random();
        let key = (region_id, file_id);
        let file_path = cache.cache_file_path(key);

        // Write a file.
        local_store
            .write(&file_path, b"hello".as_slice())
            .await
            .unwrap();
        // Add to the cache.
        cache
            .put((region_id, file_id), IndexValue { file_size: 5 })
            .await;

        // Remove the file but keep the index.
        local_store.delete(&file_path).await.unwrap();

        // Reader is none.
        assert!(cache.reader(key).await.is_none());
        // Key is removed.
        assert!(!cache.memory_index.contains_key(&key));
    }

    #[tokio::test]
    async fn test_file_cache_recover() {
        let dir = create_temp_dir("");
        let local_store = new_fs_store(dir.path().to_str().unwrap());
        let cache = FileCache::new(local_store.clone(), ReadableSize::mb(10));

        let region_id = RegionId::new(2000, 0);
        // Write N files.
        let file_ids: Vec<_> = (0..10).map(|_| FileId::random()).collect();
        for (i, file_id) in file_ids.iter().enumerate() {
            let key = (region_id, *file_id);
            let file_path = cache.cache_file_path(key);
            let bytes = i.to_string().into_bytes();
            local_store.write(&file_path, bytes.clone()).await.unwrap();

            // Add to the cache.
            cache
                .put(
                    (region_id, *file_id),
                    IndexValue {
                        file_size: bytes.len() as u32,
                    },
                )
                .await;
        }

        // Recover the cache.
        let cache = FileCache::new(local_store.clone(), ReadableSize::mb(10));
        // No entry before recovery.
        assert!(cache.reader((region_id, file_ids[0])).await.is_none());
        cache.recover().await.unwrap();

        for (i, file_id) in file_ids.iter().enumerate() {
            let key = (region_id, *file_id);
            let mut reader = cache.reader(key).await.unwrap();
            let mut buf = String::new();
            reader.read_to_string(&mut buf).await.unwrap();
            assert_eq!(i.to_string(), buf);
        }
    }

    #[test]
    fn test_cache_file_path() {
        let file_id = FileId::parse_str("3368731b-a556-42b8-a5df-9c31ce155095").unwrap();
        assert_eq!(
            "test_dir/5299989643269.3368731b-a556-42b8-a5df-9c31ce155095",
            cache_file_path("test_dir", (RegionId::new(1234, 5), file_id))
        );
        assert_eq!(
            "test_dir/5299989643269.3368731b-a556-42b8-a5df-9c31ce155095",
            cache_file_path("test_dir/", (RegionId::new(1234, 5), file_id))
        );
    }

    #[test]
    fn test_parse_file_name() {
        let file_id = FileId::parse_str("3368731b-a556-42b8-a5df-9c31ce155095").unwrap();
        let region_id = RegionId::new(1234, 5);
        assert_eq!(
            (region_id, file_id),
            parse_index_key("5299989643269.3368731b-a556-42b8-a5df-9c31ce155095").unwrap()
        );
        assert!(parse_index_key("").is_none());
        assert!(parse_index_key(".").is_none());
        assert!(parse_index_key("5299989643269").is_none());
        assert!(parse_index_key("5299989643269.").is_none());
        assert!(parse_index_key(".5299989643269").is_none());
        assert!(parse_index_key("5299989643269.").is_none());
        assert!(parse_index_key("5299989643269.3368731b-a556-42b8-a5df").is_none());
        assert!(
            parse_index_key("5299989643269.3368731b-a556-42b8-a5df-9c31ce155095.parquet").is_none()
        );
    }
}
