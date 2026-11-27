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

//! Immutable Narthex reader metadata shares the decoded SST reservation.

use std::any::{Any, TypeId};
use std::sync::Arc;

use snafu::IntoError;
use store_api::storage::FileId;

use crate::cache::file_cache::{FileCacheRef, FileType, IndexKey};
use crate::cache::write_cache::WriteCacheRef;
use crate::cache::{
    CacheManagerRef, CacheStrategy, MetaValue, NarthexPrefilterBlock, PrefilterKey,
    SstDecodedMetaKey,
};
use crate::error::Result;
use crate::metrics::{CACHE_HIT, CACHE_MISS};

pub(crate) const COLUMNAR_META: &str = "columnar_metadata";

/// Identity of one immutable Narthex reader structure.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub(crate) struct DecodedKey {
    namespace: FileId,
    identity: String,
    representation: TypeId,
}

#[derive(Clone)]
pub(crate) struct DecodedValue {
    value: Arc<dyn Any + Send + Sync>,
    pub(crate) weight: u32,
}
/// Cache context for immutable Narthex objects.
#[derive(Clone)]
pub(crate) struct MetadataCache {
    memory: Option<CacheManagerRef>,
    disk: Option<WriteCacheRef>,
    namespace: FileId,
    download_on_miss: bool,
}
impl CacheStrategy {
    pub(crate) fn columnar_metadata(&self, namespace: FileId) -> Option<MetadataCache> {
        match self {
            Self::EnableAll(manager) => Some(MetadataCache {
                memory: Some(manager.clone()),
                disk: manager.write_cache().cloned(),
                namespace,
                download_on_miss: true,
            }),
            Self::Compaction(manager) => {
                MetadataCache::maintenance(manager.write_cache(), namespace)
            }
            Self::Disabled => None,
        }
    }
}

impl MetadataCache {
    pub(crate) fn maintenance(disk: Option<&WriteCacheRef>, namespace: FileId) -> Option<Self> {
        Some(Self {
            memory: None,
            disk: Some(disk?.clone()),
            namespace,
            download_on_miss: false,
        })
    }

    pub(crate) fn preload(manager: &CacheManagerRef, namespace: FileId) -> Option<Self> {
        let cache = manager.sst_decoded_meta_cache.as_ref()?;
        if manager.columnar_load_metrics.is_none() || cache.weighted_size() >= cache.capacity() {
            return None;
        }
        Some(Self {
            memory: Some(manager.clone()),
            disk: manager.write_cache().cloned(),
            namespace,
            download_on_miss: false,
        })
    }
}

/// Reads each immutable object locally when resident. The remote reader retains
/// its coalescing options and serves the same ranges on eviction.
#[derive(Clone)]
pub(crate) struct ObjectReader {
    remote: object_store::Reader,
    refill: Option<Arc<NarthexRefill>>,
    local: Option<(FileCacheRef, IndexKey)>,
}

struct NarthexRefill {
    store: object_store::ObjectStore,
    path: String,
    size: u64,
    pin: crate::sst::file::FileHandle,
    queued: std::sync::atomic::AtomicBool,
}

impl ObjectReader {
    pub(crate) async fn open(
        cache: Option<&MetadataCache>,
        file: Option<&crate::sst::file::FileHandle>,
        kind: FileType,
        store: &object_store::ObjectStore,
        path: &str,
        size: u64,
    ) -> Result<Self> {
        use snafu::ResultExt;
        let download = cache.is_some_and(|c| c.download_on_miss);
        let local = cache
            .and_then(|c| c.disk.as_ref())
            .zip(file)
            .map(|(cache, file)| {
                let disk = cache.file_cache();
                let key = IndexKey::new(file.region_id(), file.file_id().file_id(), kind);
                (disk, key)
            });
        let refill = file.filter(|_| download && local.is_some()).map(|file| {
            Arc::new(NarthexRefill {
                store: store.clone(),
                path: path.to_owned(),
                size,
                pin: file.clone(),
                queued: false.into(),
            })
        });
        let gap = match kind {
            FileType::Series(_) => crate::sst::parquet::helper::MERGE_GAP,
            _ => 0,
        };
        let remote = store
            .reader_with(path)
            .concurrent(8)
            .chunk(256 * 1024)
            .gap(gap)
            .await
            .context(crate::error::OpenDalSnafu)?;
        Ok(Self {
            remote,
            local,
            refill,
        })
    }

    /// Decode metadata before trusting a local copy. A failed validation gets
    /// one read from the authoritative object without touching the disk cache.
    pub(crate) async fn validated<T>(
        &self,
        ranges: Vec<std::ops::Range<u64>>,
        decode: impl Fn(Vec<bytes::Bytes>) -> Result<T>,
    ) -> Result<T> {
        use snafu::ResultExt;
        if let Some((cache, key)) = &self.local
            && let Some(bytes) = cache.read_ranges(*key, &ranges).await
        {
            match decode(bytes) {
                Ok(value) => return Ok(value),
                Err(_) => cache.remove(*key).await,
            }
        }
        let buffers = self
            .remote
            .fetch(ranges)
            .await
            .context(crate::error::OpenDalSnafu)?;
        let value = decode(buffers.into_iter().map(|b| b.to_bytes()).collect())?;
        if self.local.as_ref().is_some_and(|(_, key)| {
            matches!(
                key.file_type,
                FileType::Samples(None) | FileType::Series(None)
            )
        }) {
            self.queue_refill();
        }
        Ok(value)
    }

    fn queue_refill(&self) {
        if let (Some(refill), Some((disk, key))) = (&self.refill, &self.local)
            && !refill
                .queued
                .swap(true, std::sync::atomic::Ordering::Relaxed)
        {
            disk.queue_download(
                *key,
                refill.path.clone(),
                refill.store.clone(),
                refill.size,
                Some(refill.pin.clone()),
            );
        }
    }

    pub(crate) async fn fetch(
        &self,
        ranges: Vec<std::ops::Range<u64>>,
    ) -> object_store::Result<Vec<bytes::Bytes>> {
        if let Some((cache, key)) = &self.local
            && let Some(bytes) = cache.read_ranges(*key, &ranges).await
        {
            return Ok(bytes);
        }
        self.queue_refill();
        self.remote
            .fetch(ranges)
            .await
            .map(|buffers| buffers.into_iter().map(|b| b.to_bytes()).collect())
    }

    pub(crate) async fn read(
        &self,
        range: std::ops::Range<u64>,
    ) -> object_store::Result<bytes::Bytes> {
        if let Some((cache, key)) = &self.local
            && let Some(mut bytes) = cache.read_ranges(*key, std::slice::from_ref(&range)).await
        {
            return Ok(bytes.pop().unwrap());
        }
        self.queue_refill();
        self.remote.read(range).await.map(|b| b.to_bytes())
    }
}

/// Byte ranges are relative to one validated payload or key area of an immutable part.
#[derive(Clone)]
pub(crate) struct PartArea {
    pub(crate) reader: ObjectReader,
    cache: Option<MetadataCache>,
    generation: FileId,
    part_id: u32,
    bounds: std::ops::Range<u64>,
    pool: Option<Arc<dyn datafusion::execution::memory_pool::MemoryPool>>,
}

impl PartArea {
    pub(crate) fn new(
        reader: ObjectReader,
        cache: Option<MetadataCache>,
        generation: FileId,
        part_id: u32,
        bounds: std::ops::Range<u64>,
        pool: Option<Arc<dyn datafusion::execution::memory_pool::MemoryPool>>,
    ) -> Self {
        Self {
            reader,
            cache,
            generation,
            part_id,
            bounds,
            pool,
        }
    }

    pub(crate) fn prefilter(
        &self,
        block_offset: u64,
        exprs: &smallvec::SmallVec<[String; 1]>,
    ) -> Option<(&CacheManagerRef, PrefilterKey)> {
        let context = self.cache.as_ref()?;
        let manager = context.memory.as_ref()?;
        manager.prefilter_result_cache.as_ref()?;
        let mut key = PrefilterKey::new(self.generation, 0, None, 0, exprs.clone());
        key.narthex_block = Some(NarthexPrefilterBlock {
            part_id: self.part_id,
            offset: block_offset,
        });
        Some((manager, key))
    }

    pub(crate) async fn cached_ranges(
        &self,
        ranges: &[std::ops::Range<u64>],
    ) -> Result<Option<(Vec<bytes::Bytes>, common_cache::paged::PageFetchStats)>> {
        let Some((manager, pages)) = self.cache.as_ref().and_then(|c| {
            let manager = c.memory.as_ref()?;
            Some((manager, manager.page_cache.as_ref()?))
        }) else {
            return Ok(None);
        };
        let relative = ranges
            .iter()
            .map(|r| r.start - self.bounds.start..r.end - self.bounds.start)
            .collect::<Vec<_>>();
        let mut reservation = self.pool.as_ref().map(|pool| {
            datafusion::execution::memory_pool::MemoryConsumer::new("NarthexPages").register(pool)
        });
        if let Some(memory) = &mut reservation {
            let size = pages.page_size();
            let area_size = self.bounds.end - self.bounds.start;
            let aligned = relative
                .iter()
                .filter(|r| !r.is_empty())
                .map(|r| r.start / size * size..(r.end.div_ceil(size) * size).min(area_size))
                .collect::<Vec<_>>();
            let bytes = 2 * common_cache::paged::unique_requested_bytes(&aligned)
                + relative.iter().map(|r| r.end - r.start).sum::<u64>();
            if memory.try_grow(bytes as usize).is_err() {
                return Ok(None);
            }
        }
        let pooled = reservation.is_some();
        let reader = Arc::new(NarthexPageReader {
            reader: self.reader.clone(),
            bounds: self.bounds.clone(),
            _reservation: reservation,
        });
        pages
            .fetch_narthex(
                manager.narthex_load_pool(),
                self.generation,
                self.part_id,
                self.bounds.clone(),
                &relative,
                reader.clone(),
            )
            .await
            .map(|(bytes, stats)| {
                // Pooled readers retain only the bytes charged to their block after this reservation drops.
                let bytes = if pooled {
                    bytes
                        .iter()
                        .map(|slice| bytes::Bytes::copy_from_slice(slice))
                        .collect()
                } else {
                    bytes
                };
                Some((bytes, stats))
            })
            .map_err(|error| {
                if let Some(source) = super::find_shared_page_source::<crate::error::Error>(&error)
                {
                    crate::error::ScanSeriesSnafu.into_error(source)
                } else {
                    crate::series_index::columnar::invalid(error.to_string())
                }
            })
    }
}

/// Keeps the read budget alive until a worker completes or drops this request.
struct NarthexPageReader {
    reader: ObjectReader,
    bounds: std::ops::Range<u64>,
    _reservation: Option<datafusion::execution::memory_pool::MemoryReservation>,
}

#[async_trait::async_trait]
impl common_base::range_read::RangeReader for NarthexPageReader {
    async fn metadata(&self) -> std::io::Result<common_base::range_read::Metadata> {
        Ok(common_base::range_read::Metadata {
            content_length: self.bounds.end - self.bounds.start,
        })
    }
    async fn read(&self, range: std::ops::Range<u64>) -> std::io::Result<bytes::Bytes> {
        self.reader
            .read(self.bounds.start + range.start..self.bounds.start + range.end)
            .await
            .map_err(narthex_page_io_error)
    }
    async fn read_vec(
        &self,
        ranges: &[std::ops::Range<u64>],
    ) -> std::io::Result<Vec<bytes::Bytes>> {
        let ranges = ranges
            .iter()
            .map(|r| self.bounds.start + r.start..self.bounds.start + r.end)
            .collect::<Vec<_>>();
        self.reader
            .fetch(ranges)
            .await
            .map_err(narthex_page_io_error)
    }
}

fn narthex_page_io_error(error: object_store::Error) -> std::io::Error {
    std::io::Error::other(Arc::new(crate::error::OpenDalSnafu.into_error(error)))
}

// Moka shares initializer errors with current waiters without caching them.
// Oversized values also take this path so current readers can still share them.
enum Uncached<T> {
    Value(Arc<T>),
    Error(Arc<crate::error::Error>),
}

/// Loads only validated immutable structures. Identities include the generation path,
/// object length, and the complete reference/codec context used to validate the value.
/// Oversized entries bypass admission; disabled caches perform the same validation.
pub(crate) async fn load<T, F>(
    cache: Option<&MetadataCache>,
    identity: String,
    weight: impl FnOnce(&T) -> usize + Send + 'static,
    loader: impl FnOnce() -> F + Clone + Send + 'static,
) -> Result<Arc<T>>
where
    T: Send + Sync + 'static,
    F: Future<Output = Result<T>> + Send + 'static,
{
    let Some((context, manager)) = cache.and_then(|c| Some((c, c.memory.as_ref()?))) else {
        return loader().await.map(Arc::new);
    };
    let (Some(decoded), Some(metrics)) = (
        &manager.sst_decoded_meta_cache,
        &manager.columnar_load_metrics,
    ) else {
        return loader().await.map(Arc::new);
    };
    let overhead = identity.capacity().saturating_add(
        std::mem::size_of::<SstDecodedMetaKey>()
            + std::mem::size_of::<MetaValue>()
            + 2 * std::mem::size_of::<usize>(),
    );
    let key = SstDecodedMetaKey::Narthex(DecodedKey {
        namespace: context.namespace,
        identity,
        representation: TypeId::of::<T>(),
    });
    if let Some(MetaValue::Narthex(hit)) = decoded.get(&key).await {
        CACHE_HIT.with_label_values(&[COLUMNAR_META]).inc();
        return Ok(hit.value.downcast::<T>().expect("type is part of the key"));
    }
    CACHE_MISS.with_label_values(&[COLUMNAR_META]).inc();
    let cache = decoded.clone();
    let worker_metrics = metrics.clone();
    let worker_load = loader.clone();
    let job = async move {
        let result = cache
            .try_get_with_unobserved(key, async {
                let value = Arc::new(
                    worker_metrics
                        .loads
                        .observe(worker_load())
                        .await
                        .map_err(|e| Uncached::Error(Arc::new(e)))?,
                );
                let retained = weight(&value).saturating_add(overhead);
                if let Ok(weight) = u32::try_from(retained)
                    && u64::from(weight) <= cache.capacity()
                {
                    Ok(MetaValue::Narthex(DecodedValue { value, weight }))
                } else {
                    Err(Uncached::Value(value))
                }
            })
            .await;
        match result {
            Ok(MetaValue::Narthex(entry)) => Ok(entry
                .value
                .downcast::<T>()
                .expect("type is part of the key")),
            Ok(MetaValue::Parquet(_)) => unreachable!("Narthex key contains Parquet metadata"),
            Err(result) => match result.as_ref() {
                Uncached::Value(value) => Ok(value.clone()),
                Uncached::Error(error) => {
                    Err(crate::error::LoadSstMetaSnafu.into_error(error.clone()))
                }
            },
        }
    };
    match manager.narthex_load_pool().run(job).await {
        Some(result) => result,
        None => metrics.loads.observe(loader()).await.map(Arc::new),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::CacheManager;

    #[tokio::test]
    async fn oversized_metadata_is_returned_without_retention() {
        let strategy = CacheStrategy::EnableAll(Arc::new(
            CacheManager::builder().sst_meta_cache_size(8192).build(),
        ));
        let cache = strategy.columnar_metadata(FileId::random()).unwrap();
        for expected in [1, 2] {
            let value = load(
                Some(&cache),
                "oversized".into(),
                |_| usize::MAX,
                move || async move { Ok(expected) },
            )
            .await
            .unwrap();
            assert_eq!(*value, expected);
        }
    }

    #[tokio::test]
    async fn lost_worker_reply_retries_metadata_without_caching() {
        let strategy = CacheStrategy::EnableAll(Arc::new(
            CacheManager::builder().sst_meta_cache_size(8192).build(),
        ));
        let cache = strategy.columnar_metadata(FileId::random()).unwrap();
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let loader = {
            let calls = calls.clone();
            move || async move {
                let attempt = calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                assert_ne!(attempt, 0, "worker lost during metadata read");
                Ok(attempt)
            }
        };
        let read = || load(Some(&cache), "key".into(), |_| 1, loader.clone());
        assert_eq!(*read().await.unwrap(), 1);
        assert_eq!(*read().await.unwrap(), 2);
        assert_eq!(*read().await.unwrap(), 2);
    }

    #[tokio::test]
    async fn paused_reader_shares_the_workers_load() {
        let strategy = CacheStrategy::EnableAll(Arc::new(
            CacheManager::builder().sst_meta_cache_size(8192).build(),
        ));
        let cache = strategy.columnar_metadata(FileId::random()).unwrap();
        let started = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let loader = {
            let started = started.clone();
            let release = release.clone();
            move || async move {
                started.notify_one();
                release.notified().await;
                Ok(vec![1u8])
            }
        };
        let mut first = Box::pin(load(Some(&cache), "shared".into(), |_| 1, loader));
        assert!(futures::poll!(&mut first).is_pending());
        started.notified().await;
        let mut second = Box::pin(load(
            Some(&cache),
            "shared".into(),
            |_| 1,
            || async {
                Err::<Vec<u8>, _>(crate::series_index::columnar::invalid(
                    "duplicate storage load",
                ))
            },
        ));
        assert!(futures::poll!(&mut second).is_pending());
        release.notify_one();
        let other = tokio::time::timeout(std::time::Duration::from_secs(5), second)
            .await
            .unwrap()
            .unwrap();
        assert!(Arc::ptr_eq(&first.await.unwrap(), &other));
    }
}
