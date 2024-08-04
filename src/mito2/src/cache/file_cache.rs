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

use std::ops::Range;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use common_base::readable_size::ReadableSize;
use common_telemetry::{info, warn};
use futures::{FutureExt, TryStreamExt};
use moka::future::Cache;
use moka::notification::RemovalCause;
use object_store::util::join_path;
use object_store::{ErrorKind, Metakey, ObjectStore, Reader};
use parquet::file::metadata::ParquetMetaData;
use snafu::ResultExt;
use store_api::storage::RegionId;

use crate::cache::FILE_TYPE;
use crate::error::{OpenDalSnafu, Result};
use crate::metrics::{CACHE_BYTES, CACHE_HIT, CACHE_MISS};
use crate::sst::file::FileId;
use crate::sst::parquet::helper::fetch_byte_ranges;
use crate::sst::parquet::metadata::MetadataLoader;

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
    pub(crate) fn new(
        local_store: ObjectStore,
        capacity: ReadableSize,
        ttl: Option<Duration>,
    ) -> FileCache {
        let cache_store = local_store.clone();
        let mut builder = Cache::builder()
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
                        warn!("Replace existing cache {} for region {} unexpectedly", file_path, key.region_id);
                        return;
                    }

                    match store.delete(&file_path).await {
                        Ok(()) => {
                            CACHE_BYTES.with_label_values(&[FILE_TYPE]).sub(value.file_size.into());
                        }
                        Err(e) => {
                            warn!(e; "Failed to delete cached file {} for region {}", file_path, key.region_id);
                        }
                    }
                }
                .boxed()
            });
        if let Some(ttl) = ttl {
            builder = builder.time_to_idle(ttl);
        }
        let memory_index = builder.build();
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

    pub(crate) async fn get(&self, key: IndexKey) -> Option<IndexValue> {
        self.memory_index.get(&key).await
    }

    /// Reads a file from the cache.
    #[allow(unused)]
    pub(crate) async fn reader(&self, key: IndexKey) -> Option<Reader> {
        // We must use `get()` to update the estimator of the cache.
        // See https://docs.rs/moka/latest/moka/future/struct.Cache.html#method.contains_key
        if self.memory_index.get(&key).await.is_none() {
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
                    warn!(e; "Failed to get file for key {:?}", key);
                }
            }
            Ok(None) => {}
        }

        // We removes the file from the index.
        self.memory_index.remove(&key).await;
        CACHE_MISS.with_label_values(&[FILE_TYPE]).inc();
        None
    }

    /// Reads ranges from the cache.
    pub(crate) async fn read_ranges(
        &self,
        key: IndexKey,
        ranges: &[Range<u64>],
    ) -> Option<Vec<Bytes>> {
        if self.memory_index.get(&key).await.is_none() {
            CACHE_MISS.with_label_values(&[FILE_TYPE]).inc();
            return None;
        }

        let file_path = self.cache_file_path(key);
        // In most cases, it will use blocking read,
        // because FileCache is normally based on local file system, which supports blocking read.
        let bytes_result = fetch_byte_ranges(&file_path, self.local_store.clone(), ranges).await;
        match bytes_result {
            Ok(bytes) => {
                CACHE_HIT.with_label_values(&[FILE_TYPE]).inc();
                Some(bytes)
            }
            Err(e) => {
                if e.kind() != ErrorKind::NotFound {
                    warn!(e; "Failed to get file for key {:?}", key);
                }

                // We removes the file from the index.
                self.memory_index.remove(&key).await;
                CACHE_MISS.with_label_values(&[FILE_TYPE]).inc();
                None
            }
        }
    }

    #[allow(unused)]
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
        // Use i64 for total_size to reduce the risk of overflow.
        // It is possible that the total size of the cache is larger than i32::MAX.
        let (mut total_size, mut total_keys) = (0i64, 0);
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
            total_size += i64::from(file_size);
            total_keys += 1;
        }
        // The metrics is a signed int gauge so we can updates it finally.
        CACHE_BYTES.with_label_values(&[FILE_TYPE]).add(total_size);

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

    /// Get the parquet metadata in file cache.
    /// If the file is not in the cache or fail to load metadata, return None.
    pub(crate) async fn get_parquet_meta_data(&self, key: IndexKey) -> Option<ParquetMetaData> {
        // Check if file cache contains the key
        if let Some(index_value) = self.memory_index.get(&key).await {
            // Load metadata from file cache
            let local_store = self.local_store();
            let file_path = self.cache_file_path(key);
            let file_size = index_value.file_size as u64;
            let metadata_loader = MetadataLoader::new(local_store, &file_path, file_size);

            match metadata_loader.load().await {
                Ok(metadata) => {
                    CACHE_HIT.with_label_values(&[FILE_TYPE]).inc();
                    Some(metadata)
                }
                Err(e) => {
                    if !e.is_object_not_found() {
                        warn!(
                            e; "Failed to get parquet metadata for key {:?}",
                            key
                        );
                    }
                    // We removes the file from the index.
                    self.memory_index.remove(&key).await;
                    CACHE_MISS.with_label_values(&[FILE_TYPE]).inc();
                    None
                }
            }
        } else {
            CACHE_MISS.with_label_values(&[FILE_TYPE]).inc();
            None
        }
    }

    async fn get_reader(&self, file_path: &str) -> object_store::Result<Option<Reader>> {
        if self.local_store.is_exist(file_path).await? {
            Ok(Some(self.local_store.reader(file_path).await?))
        } else {
            Ok(None)
        }
    }

    /// Checks if the key is in the file cache.
    #[cfg(test)]
    pub(crate) fn contains_key(&self, key: &IndexKey) -> bool {
        self.memory_index.contains_key(key)
    }
}

/// Key of file cache index.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct IndexKey {
    pub region_id: RegionId,
    pub file_id: FileId,
    pub file_type: FileType,
}

impl IndexKey {
    /// Creates a new index key.
    pub fn new(region_id: RegionId, file_id: FileId, file_type: FileType) -> IndexKey {
        IndexKey {
            region_id,
            file_id,
            file_type,
        }
    }
}

/// Type of the file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FileType {
    /// Parquet file.
    Parquet,
    /// Puffin file.
    Puffin,
}

impl FileType {
    /// Parses the file type from string.
    fn parse(s: &str) -> Option<FileType> {
        match s {
            "parquet" => Some(FileType::Parquet),
            "puffin" => Some(FileType::Puffin),
            _ => None,
        }
    }

    /// Converts the file type to string.
    fn as_str(&self) -> &'static str {
        match self {
            FileType::Parquet => "parquet",
            FileType::Puffin => "puffin",
        }
    }
}

/// An entity that describes the file in the file cache.
///
/// It should only keep minimal information needed by the cache.
#[derive(Debug, Clone)]
pub(crate) struct IndexValue {
    /// Size of the file in bytes.
    pub(crate) file_size: u32,
}

/// Generates the path to the cached file.
///
/// The file name format is `{region_id}.{file_id}.{file_type}`
fn cache_file_path(cache_file_dir: &str, key: IndexKey) -> String {
    join_path(
        cache_file_dir,
        &format!(
            "{}.{}.{}",
            key.region_id.as_u64(),
            key.file_id,
            key.file_type.as_str()
        ),
    )
}

/// Parse index key from the file name.
fn parse_index_key(name: &str) -> Option<IndexKey> {
    let mut split = name.splitn(3, '.');
    let region_id = split.next().and_then(|s| {
        let id = s.parse::<u64>().ok()?;
        Some(RegionId::from_u64(id))
    })?;
    let file_id = split.next().and_then(|s| FileId::parse_str(s).ok())?;
    let file_type = split.next().and_then(FileType::parse)?;

    Some(IndexKey::new(region_id, file_id, file_type))
}

#[cfg(test)]
mod tests {
    use common_test_util::temp_dir::create_temp_dir;
    use object_store::services::Fs;

    use super::*;

    fn new_fs_store(path: &str) -> ObjectStore {
        let builder = Fs::default().root(path);
        ObjectStore::new(builder).unwrap().finish()
    }

    #[tokio::test]
    async fn test_file_cache_ttl() {
        let dir = create_temp_dir("");
        let local_store = new_fs_store(dir.path().to_str().unwrap());

        let cache = FileCache::new(
            local_store.clone(),
            ReadableSize::mb(10),
            Some(Duration::from_millis(5)),
        );
        let region_id = RegionId::new(2000, 0);
        let file_id = FileId::random();
        let key = IndexKey::new(region_id, file_id, FileType::Parquet);
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
            .put(
                IndexKey::new(region_id, file_id, FileType::Parquet),
                IndexValue { file_size: 5 },
            )
            .await;

        let exist = cache.reader(key).await;
        assert!(exist.is_some());
        tokio::time::sleep(Duration::from_millis(10)).await;
        cache.memory_index.run_pending_tasks().await;
        let non = cache.reader(key).await;
        assert!(non.is_none());
    }

    #[tokio::test]
    async fn test_file_cache_basic() {
        let dir = create_temp_dir("");
        let local_store = new_fs_store(dir.path().to_str().unwrap());

        let cache = FileCache::new(local_store.clone(), ReadableSize::mb(10), None);
        let region_id = RegionId::new(2000, 0);
        let file_id = FileId::random();
        let key = IndexKey::new(region_id, file_id, FileType::Parquet);
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
            .put(
                IndexKey::new(region_id, file_id, FileType::Parquet),
                IndexValue { file_size: 5 },
            )
            .await;

        // Read file content.
        let reader = cache.reader(key).await.unwrap();
        let buf = reader.read(..).await.unwrap().to_vec();
        assert_eq!("hello", String::from_utf8(buf).unwrap());

        // Get weighted size.
        cache.memory_index.run_pending_tasks().await;
        assert_eq!(5, cache.memory_index.weighted_size());

        // Remove the file.
        cache.remove(key).await;
        assert!(cache.reader(key).await.is_none());

        // Ensure all pending tasks of the moka cache is done before assertion.
        cache.memory_index.run_pending_tasks().await;

        // The file also not exists.
        assert!(!local_store.is_exist(&file_path).await.unwrap());
        assert_eq!(0, cache.memory_index.weighted_size());
    }

    #[tokio::test]
    async fn test_file_cache_file_removed() {
        let dir = create_temp_dir("");
        let local_store = new_fs_store(dir.path().to_str().unwrap());

        let cache = FileCache::new(local_store.clone(), ReadableSize::mb(10), None);
        let region_id = RegionId::new(2000, 0);
        let file_id = FileId::random();
        let key = IndexKey::new(region_id, file_id, FileType::Parquet);
        let file_path = cache.cache_file_path(key);

        // Write a file.
        local_store
            .write(&file_path, b"hello".as_slice())
            .await
            .unwrap();
        // Add to the cache.
        cache
            .put(
                IndexKey::new(region_id, file_id, FileType::Parquet),
                IndexValue { file_size: 5 },
            )
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
        let cache = FileCache::new(local_store.clone(), ReadableSize::mb(10), None);

        let region_id = RegionId::new(2000, 0);
        let file_type = FileType::Parquet;
        // Write N files.
        let file_ids: Vec<_> = (0..10).map(|_| FileId::random()).collect();
        let mut total_size = 0;
        for (i, file_id) in file_ids.iter().enumerate() {
            let key = IndexKey::new(region_id, *file_id, file_type);
            let file_path = cache.cache_file_path(key);
            let bytes = i.to_string().into_bytes();
            local_store.write(&file_path, bytes.clone()).await.unwrap();

            // Add to the cache.
            cache
                .put(
                    IndexKey::new(region_id, *file_id, file_type),
                    IndexValue {
                        file_size: bytes.len() as u32,
                    },
                )
                .await;
            total_size += bytes.len();
        }

        // Recover the cache.
        let cache = FileCache::new(local_store.clone(), ReadableSize::mb(10), None);
        // No entry before recovery.
        assert!(cache
            .reader(IndexKey::new(region_id, file_ids[0], file_type))
            .await
            .is_none());
        cache.recover().await.unwrap();

        // Check size.
        cache.memory_index.run_pending_tasks().await;
        assert_eq!(total_size, cache.memory_index.weighted_size() as usize);

        for (i, file_id) in file_ids.iter().enumerate() {
            let key = IndexKey::new(region_id, *file_id, file_type);
            let reader = cache.reader(key).await.unwrap();
            let buf = reader.read(..).await.unwrap().to_vec();
            assert_eq!(i.to_string(), String::from_utf8(buf).unwrap());
        }
    }

    #[tokio::test]
    async fn test_file_cache_read_ranges() {
        let dir = create_temp_dir("");
        let local_store = new_fs_store(dir.path().to_str().unwrap());
        let file_cache = FileCache::new(local_store.clone(), ReadableSize::mb(10), None);
        let region_id = RegionId::new(2000, 0);
        let file_id = FileId::random();
        let key = IndexKey::new(region_id, file_id, FileType::Parquet);
        let file_path = file_cache.cache_file_path(key);
        // Write a file.
        let data = b"hello greptime database";
        local_store
            .write(&file_path, data.as_slice())
            .await
            .unwrap();
        // Add to the cache.
        file_cache.put(key, IndexValue { file_size: 5 }).await;
        // Ranges
        let ranges = vec![0..5, 6..10, 15..19, 0..data.len() as u64];
        let bytes = file_cache.read_ranges(key, &ranges).await.unwrap();

        assert_eq!(4, bytes.len());
        assert_eq!(b"hello", bytes[0].as_ref());
        assert_eq!(b"grep", bytes[1].as_ref());
        assert_eq!(b"data", bytes[2].as_ref());
        assert_eq!(data, bytes[3].as_ref());
    }

    #[test]
    fn test_cache_file_path() {
        let file_id = FileId::parse_str("3368731b-a556-42b8-a5df-9c31ce155095").unwrap();
        assert_eq!(
            "test_dir/5299989643269.3368731b-a556-42b8-a5df-9c31ce155095.parquet",
            cache_file_path(
                "test_dir",
                IndexKey::new(RegionId::new(1234, 5), file_id, FileType::Parquet)
            )
        );
        assert_eq!(
            "test_dir/5299989643269.3368731b-a556-42b8-a5df-9c31ce155095.parquet",
            cache_file_path(
                "test_dir/",
                IndexKey::new(RegionId::new(1234, 5), file_id, FileType::Parquet)
            )
        );
    }

    #[test]
    fn test_parse_file_name() {
        let file_id = FileId::parse_str("3368731b-a556-42b8-a5df-9c31ce155095").unwrap();
        let region_id = RegionId::new(1234, 5);
        assert_eq!(
            IndexKey::new(region_id, file_id, FileType::Parquet),
            parse_index_key("5299989643269.3368731b-a556-42b8-a5df-9c31ce155095.parquet").unwrap()
        );
        assert!(parse_index_key("").is_none());
        assert!(parse_index_key(".").is_none());
        assert!(parse_index_key("5299989643269").is_none());
        assert!(parse_index_key("5299989643269.").is_none());
        assert!(parse_index_key(".5299989643269").is_none());
        assert!(parse_index_key("5299989643269.").is_none());
        assert!(parse_index_key("5299989643269.3368731b-a556-42b8-a5df").is_none());
        assert!(parse_index_key("5299989643269.3368731b-a556-42b8-a5df-9c31ce155095").is_none());
        assert!(
            parse_index_key("5299989643269.3368731b-a556-42b8-a5df-9c31ce155095.parque").is_none()
        );
        assert!(parse_index_key(
            "5299989643269.3368731b-a556-42b8-a5df-9c31ce155095.parquet.puffin"
        )
        .is_none());
    }
}
