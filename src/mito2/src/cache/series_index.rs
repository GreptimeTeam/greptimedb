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

//! Disposable series-index files. Remote objects remain authoritative.

use std::ops::Range;
use std::sync::Arc;

use common_telemetry::debug;
use moka::future::Cache;
use object_store::{Buffer, ObjectStore, Reader};
use tokio::sync::Semaphore;

/// A node-wide disk budget shared by series, samples, and range indexes.
/// Readers pin files independently of Moka's admission and eviction decisions.
#[derive(Debug, Clone)]
pub(crate) struct SeriesIndexFileCache {
    storage: String,
    local: ObjectStore,
    files: Cache<String, Arc<CachedFile>>,
    downloads: Arc<Semaphore>,
    capacity: u64,
}

impl SeriesIndexFileCache {
    /// Creates an empty cache over the local filesystem store.
    pub(crate) fn new(local: ObjectStore, capacity: u64) -> Arc<Self> {
        Arc::new(Self {
            storage: "default".to_string(),
            local,
            files: Cache::builder()
                .max_capacity(capacity)
                .eviction_policy(moka::policy::EvictionPolicy::lru())
                .weigher(|_: &String, file: &Arc<CachedFile>| file.size as u32)
                .build(),
            downloads: Arc::new(Semaphore::new(2)),
            capacity,
        })
    }

    /// Separates named backends while sharing the node-wide capacity and downloads.
    pub(crate) fn for_storage(&self, storage: Option<&str>) -> Arc<Self> {
        let mut cache = self.clone();
        cache.storage = storage.map_or_else(
            || "default".to_string(),
            |name| format!("named:{}", name.to_lowercase()),
        );
        Arc::new(cache)
    }

    async fn get(&self, remote: &ObjectStore, path: &str, size: u64) -> Option<Arc<CachedFile>> {
        // Moka's per-entry weights are u32. Larger objects remain readable remotely.
        if size > self.capacity || size > u32::MAX as u64 {
            return None;
        }
        let info = remote.info();
        let key = format!(
            "{}:{}:{}:{}:{path}:{size}",
            self.storage,
            info.scheme(),
            info.name(),
            info.root()
        );
        if let Some(file) = self.files.get(&key).await {
            if self
                .local
                .stat(&file.path)
                .await
                .is_ok_and(|meta| meta.content_length() == size)
            {
                return Some(file);
            }
            self.files.invalidate(&key).await;
        }
        let result = self
            .files
            .try_get_with(key, async {
                let _permit = self.downloads.acquire().await;
                // The guard also removes incomplete downloads on errors or cancellation.
                let file = Arc::new(CachedFile {
                    store: self.local.clone(),
                    path: format!("objects/{}", uuid::Uuid::new_v4()),
                    size,
                });
                let reader = remote.reader(path).await?;
                let mut writer = self.local.writer(&file.path).await?;
                const CHUNK: u64 = 8 * 1024 * 1024;
                let mut offset = 0;
                while offset < size {
                    let end = (offset + CHUNK).min(size);
                    let bytes = reader.read(offset..end).await?;
                    if bytes.len() as u64 != end - offset {
                        return Err(object_store::Error::new(
                            object_store::ErrorKind::Unexpected,
                            "incomplete index cache download",
                        ));
                    }
                    writer.write(bytes).await?;
                    offset = end;
                }
                writer.close().await?;
                Ok::<_, object_store::Error>(file)
            })
            .await;
        self.files.run_pending_tasks().await;
        match result {
            Ok(file) => Some(file),
            Err(error) => {
                debug!("Unable to cache series-index object {path}: {error}");
                None
            }
        }
    }
}

/// Owns a local file until both the cache and all active readers release it.
#[derive(Debug)]
struct CachedFile {
    store: ObjectStore,
    path: String,
    size: u64,
}

impl Drop for CachedFile {
    fn drop(&mut self) {
        let store = self.store.clone();
        let path = self.path.clone();
        common_runtime::spawn_global(async move {
            if let Err(error) = store.delete(&path).await {
                debug!("Unable to remove cached series-index file {path}: {error}");
            }
        });
    }
}

/// A filesystem reader with a file guard, or a remote reader on cache bypass.
/// Remote fallback also covers local-file loss after the reader was opened.
#[derive(Clone)]
pub(crate) struct IndexReader {
    remote: Reader,
    local: Option<(Reader, Arc<CachedFile>)>,
}

impl IndexReader {
    /// Opens a cached filesystem reader, falling back to the remote object.
    pub(crate) async fn open(
        store: &ObjectStore,
        path: &str,
        size: u64,
        cache: Option<&Arc<SeriesIndexFileCache>>,
    ) -> object_store::Result<Self> {
        let remote = store.reader_with(path).concurrent(8).gap(0).await?;
        let local = if let Some(cache) = cache {
            if let Some(file) = cache.get(store, path, size).await {
                file.store
                    .reader_with(&file.path)
                    .concurrent(8)
                    .gap(0)
                    .await
                    .ok()
                    .map(|reader| (reader, file))
            } else {
                None
            }
        } else {
            None
        };
        Ok(Self { remote, local })
    }

    /// Reads a range, retrying remotely if the local copy is unavailable.
    pub(crate) async fn read(&self, range: Range<u64>) -> object_store::Result<Buffer> {
        if let Some((reader, _)) = &self.local
            && let Ok(bytes) = reader.read(range.clone()).await
            && bytes.len() as u64 == range.end - range.start
        {
            return Ok(bytes);
        }
        self.remote.read(range).await
    }

    /// Fetches ranges with the same local-file lifetime and fallback as `read`.
    pub(crate) async fn fetch(&self, ranges: Vec<Range<u64>>) -> object_store::Result<Vec<Buffer>> {
        if let Some((reader, _)) = &self.local
            && let Ok(buffers) = reader.fetch(ranges.clone()).await
            && buffers.len() == ranges.len()
            && buffers
                .iter()
                .zip(&ranges)
                .all(|(buf, range)| buf.len() as u64 == range.end - range.start)
        {
            return Ok(buffers);
        }
        self.remote.fetch(ranges).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn eviction_pins_active_readers_and_local_loss_falls_back() {
        let temp = common_test_util::temp_dir::create_temp_dir("series-file-cache");
        let local = crate::access_layer::new_fs_cache_store(temp.path().to_str().unwrap())
            .await
            .unwrap();
        let remote = ObjectStore::new(object_store::services::Memory::default()).unwrap();
        remote.write("first", "aaaa").await.unwrap();
        remote.write("second", "bbbb").await.unwrap();
        let cache = SeriesIndexFileCache::new(local.clone(), 4);
        let first = IndexReader::open(&remote, "first", 4, Some(&cache))
            .await
            .unwrap();
        let first_path = first.local.as_ref().unwrap().1.path.clone();
        let second = IndexReader::open(&remote, "second", 4, Some(&cache))
            .await
            .unwrap();
        assert_eq!(cache.files.weighted_size(), 4);
        assert!(!cache.files.iter().any(|(_, file)| file.path == first_path));
        assert!(local.exists(&first_path).await.unwrap());
        // The first entry has been evicted, but its reader still pins the local file.
        remote.delete("first").await.unwrap();
        assert_eq!(first.read(0..4).await.unwrap().to_bytes().as_ref(), b"aaaa");
        drop(first);
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while local.exists(&first_path).await.unwrap() {
                // Moka defers reclaiming removed entries until maintenance runs.
                cache.files.run_pending_tasks().await;
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        // Disk loss after open also falls back without affecting the durable copy.
        let second_path = second.local.as_ref().unwrap().1.path.clone();
        local.delete(&second_path).await.unwrap();
        assert_eq!(
            second.read(0..4).await.unwrap().to_bytes().as_ref(),
            b"bbbb"
        );
    }
}
