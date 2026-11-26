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

//! Immutable native-reader metadata shares the decoded SST reservation.

use std::any::{Any, TypeId};
use std::sync::Arc;

use snafu::IntoError;
use store_api::storage::FileId;

use crate::cache::{CacheManagerRef, CacheStrategy, MetaValue, SstDecodedMetaKey};
use crate::error::Result;
use crate::metrics::{CACHE_HIT, CACHE_MISS};

pub(super) const COLUMNAR_META: &str = "columnar_metadata";

/// Identity of one immutable native-reader structure.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub(crate) struct DecodedKey {
    namespace: FileId,
    identity: String,
    representation: TypeId,
}

#[derive(Clone)]
pub(crate) struct DecodedValue {
    value: Arc<dyn Any + Send + Sync>,
    pub(super) weight: u32,
}
/// Contains no artifact handle or live object-store reader. Loads run independently;
/// admission uses a synchronous, atomic cache entry after loading.
#[derive(Clone)]
pub(crate) struct MetadataCache {
    manager: CacheManagerRef,
    namespace: FileId,
    download_on_miss: bool,
}
impl CacheStrategy {
    pub(crate) fn columnar_metadata(&self, namespace: FileId) -> Option<MetadataCache> {
        let Self::EnableAll(manager) = self else {
            return None;
        };
        Some(MetadataCache {
            manager: manager.clone(),
            namespace,
            download_on_miss: true,
        })
    }
}

impl MetadataCache {
    pub(crate) fn preload(manager: &CacheManagerRef, namespace: FileId) -> Option<Self> {
        let cache = manager.sst_decoded_meta_cache.as_ref()?;
        if manager.columnar_load_metrics.is_none() || cache.weighted_size() >= cache.capacity() {
            return None;
        }
        Some(Self {
            manager: manager.clone(),
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
    local: Option<(super::file_cache::FileCacheRef, super::file_cache::IndexKey)>,
}

impl ObjectReader {
    pub(crate) async fn open(
        cache: Option<&MetadataCache>,
        file: Option<&crate::sst::file::FileHandle>,
        kind: super::file_cache::FileType,
        store: &object_store::ObjectStore,
        path: &str,
        size: u64,
    ) -> Result<Self> {
        use snafu::ResultExt;
        let download = cache.is_some_and(|c| c.download_on_miss);
        let local = cache
            .and_then(|c| c.manager.write_cache())
            .zip(file)
            .map(|(cache, file)| {
                let disk = cache.file_cache();
                let key = super::file_cache::IndexKey::new(
                    file.region_id(),
                    file.file_id().file_id(),
                    kind,
                );
                if download && !disk.contains_key(&key) {
                    disk.queue_download(
                        key,
                        path.to_owned(),
                        store.clone(),
                        size,
                        Some(file.clone()),
                    );
                }
                (disk, key)
            });
        let remote = store
            .reader_with(path)
            .concurrent(8)
            .chunk(256 * 1024)
            .gap(0)
            .await
            .context(crate::error::OpenDalSnafu)?;
        Ok(Self { remote, local })
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
        self.remote.read(range).await.map(|b| b.to_bytes())
    }
}

/// Byte ranges are relative to one validated payload or key area of an immutable part.
#[derive(Clone)]
pub(crate) struct PartArea {
    pub(crate) reader: ObjectReader,
    cache: Option<MetadataCache>,
    path: Arc<str>,
    bounds: std::ops::Range<u64>,
    pool: Option<Arc<dyn datafusion::execution::memory_pool::MemoryPool>>,
}

impl PartArea {
    pub(crate) fn new(
        reader: ObjectReader,
        cache: Option<MetadataCache>,
        path: Arc<str>,
        bounds: std::ops::Range<u64>,
        pool: Option<Arc<dyn datafusion::execution::memory_pool::MemoryPool>>,
    ) -> Self {
        Self {
            reader,
            cache,
            path,
            bounds,
            pool,
        }
    }

    pub(crate) fn prefilter(
        &self,
        block_offset: u64,
        exprs: &smallvec::SmallVec<[String; 1]>,
    ) -> Option<(&CacheManagerRef, super::PrefilterKey)> {
        let context = self.cache.as_ref()?;
        context.manager.prefilter_result_cache.as_ref()?;
        let mut key = super::PrefilterKey::new(context.namespace, 0, None, 0, exprs.clone());
        key.mem_usage += self.path.len();
        key.native_block = Some((self.path.clone(), block_offset));
        Some((&context.manager, key))
    }

    pub(crate) async fn cached_ranges(
        &self,
        ranges: &[std::ops::Range<u64>],
    ) -> Result<Option<(Vec<bytes::Bytes>, common_cache::paged::PageFetchStats)>> {
        let Some((context, pages)) = self
            .cache
            .as_ref()
            .and_then(|c| Some((c, c.manager.page_cache.as_ref()?)))
        else {
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
            let bytes = relative
                .iter()
                .filter(|r| !r.is_empty())
                .map(|r| {
                    let aligned = ((r.end.div_ceil(size) * size)
                        .min(self.bounds.end - self.bounds.start))
                        - r.start / size * size;
                    2 * aligned + r.end - r.start
                })
                .sum::<u64>();
            if memory.try_grow(bytes as usize).is_err() {
                return Ok(None);
            }
        }
        pages
            .fetch_native(
                context.namespace,
                self.path.clone(),
                self.bounds.clone(),
                &relative,
                self,
            )
            .await
            .map(|(bytes, stats)| {
                // Pooled readers retain only the bytes charged to their block after this reservation drops.
                let bytes = if reservation.is_some() {
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
                // Independent I/O errors have one owner. Preserve storage error kinds for
                // discovery fallback and retry classification.
                let error = Arc::try_unwrap(error).unwrap_or_else(std::io::Error::other);
                match error.downcast::<object_store::Error>() {
                    Ok(source) => crate::error::OpenDalSnafu.into_error(source),
                    Err(error) => crate::series_index::columnar::invalid(error.to_string()),
                }
            })
    }
}

#[async_trait::async_trait]
impl common_base::range_read::RangeReader for PartArea {
    async fn metadata(&self) -> std::io::Result<common_base::range_read::Metadata> {
        Ok(common_base::range_read::Metadata {
            content_length: self.bounds.end - self.bounds.start,
        })
    }
    async fn read(&self, range: std::ops::Range<u64>) -> std::io::Result<bytes::Bytes> {
        self.reader
            .read(self.bounds.start + range.start..self.bounds.start + range.end)
            .await
            .map_err(std::io::Error::other)
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
            .map_err(std::io::Error::other)
    }
}

/// Loads only validated immutable structures. Identities include the generation path,
/// object length, and the complete reference/codec context used to validate the value.
/// Oversized entries bypass admission; disabled caches perform the same validation.
pub(crate) async fn load<T: Send + Sync + 'static>(
    cache: Option<&MetadataCache>,
    identity: String,
    weight: impl FnOnce(&T) -> usize,
    loader: impl Future<Output = Result<T>>,
) -> Result<Arc<T>> {
    let Some(context) = cache else {
        return loader.await.map(Arc::new);
    };
    let (Some(decoded), Some(metrics)) = (
        &context.manager.sst_decoded_meta_cache,
        &context.manager.columnar_load_metrics,
    ) else {
        return loader.await.map(Arc::new);
    };
    let key = DecodedKey {
        namespace: context.namespace,
        identity,
        representation: TypeId::of::<T>(),
    };
    let identity_size = key.identity.capacity();
    let key = SstDecodedMetaKey::Native(key);
    if let Some(MetaValue::Native(value)) = decoded.get(&key).await {
        let value = value
            .value
            .downcast::<T>()
            .expect("type is part of the key");
        CACHE_HIT.with_label_values(&[COLUMNAR_META]).inc();
        return Ok(value);
    }
    CACHE_MISS.with_label_values(&[COLUMNAR_META]).inc();
    let value = Arc::new(metrics.loads.observe(loader).await?);
    // A caller may flush buffered output while its loader is Pending, then stop
    // polling it under downstream backpressure. Never hold a shared lock across
    // that await: another stream may need the lock to finish the current series.
    let retained = weight(&value).saturating_add(identity_size).saturating_add(
        std::mem::size_of::<SstDecodedMetaKey>()
            + std::mem::size_of::<MetaValue>()
            + 2 * std::mem::size_of::<usize>(),
    );
    if let Ok(weight) = u32::try_from(retained)
        && u64::from(weight) <= decoded.capacity()
    {
        let admitted = decoded
            .get_or_insert(
                key,
                MetaValue::Native(DecodedValue {
                    value: value.clone(),
                    weight,
                }),
            )
            .await;
        if let MetaValue::Native(entry) = admitted {
            return Ok(entry
                .value
                .downcast::<T>()
                .expect("type is part of the key"));
        }
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::CacheManager;
    use std::hash::{DefaultHasher, Hash, Hasher};

    #[rstest::rstest]
    #[case::same_key(false)]
    #[case::stripe_collision(true)]
    #[tokio::test]
    async fn backpressured_load_does_not_block_other_readers(#[case] different_key: bool) {
        let manager = CacheManager::builder().sst_meta_cache_size(8192).build();
        let strategy = CacheStrategy::EnableAll(Arc::new(manager));
        let cache = strategy.columnar_metadata(FileId::random()).unwrap();
        let identity = "generation/part/page";
        // Keep the former stripe collision as a regression case for the old loader.
        let stripe = |identity: &str| {
            let key = DecodedKey {
                namespace: cache.namespace,
                identity: identity.to_string(),
                representation: TypeId::of::<Vec<u8>>(),
            };
            let mut hash = DefaultHasher::new();
            key.hash(&mut hash);
            hash.finish() as usize % 32
        };
        let other_identity = if different_key {
            (0..)
                .map(|i| format!("other/{i}"))
                .find(|other| stripe(other) == stripe(identity))
                .unwrap()
        } else {
            identity.to_string()
        };
        let (complete, pending) = tokio::sync::oneshot::channel();
        let mut owner = Box::pin(load(
            Some(&cache),
            "generation/part/page".into(),
            |_| 64,
            async {
                pending.await.unwrap();
                Ok(vec![1u8, 2, 3])
            },
        ));
        assert!(futures::poll!(&mut owner).is_pending());
        // Deliver the I/O wake, but deliberately never poll its owner again until
        // the second reader completes. This models a stream paused after a flush.
        complete.send(()).unwrap();
        let other = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            load(Some(&cache), other_identity, |_| 64, async {
                Ok(vec![1u8, 2, 3])
            }),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(other.as_slice(), &[1, 2, 3]);
        let first = owner.await.unwrap();
        if !different_key {
            assert!(Arc::ptr_eq(&first, &other));
        }
        let cached = load(Some(&cache), "generation/part/page".into(), |_| 64, async {
            panic!("must reuse the admitted entry")
        })
        .await
        .unwrap();
        assert!(Arc::ptr_eq(&first, &cached));

        let mut canceled = Box::pin(load(
            Some(&cache),
            "canceled".into(),
            |_| 64,
            std::future::pending::<Result<Vec<u8>>>(),
        ));
        assert!(futures::poll!(&mut canceled).is_pending());
        drop(canceled);
        let retry = load(Some(&cache), "canceled".into(), |_| 64, async {
            Ok(vec![4u8])
        })
        .await
        .unwrap();
        assert_eq!(&[4], retry.as_slice());
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_misses_share_the_admitted_value() {
        let strategy = CacheStrategy::EnableAll(Arc::new(
            CacheManager::builder().sst_meta_cache_size(8192).build(),
        ));
        let cache = strategy.columnar_metadata(FileId::random()).unwrap();
        let barrier = Arc::new(tokio::sync::Barrier::new(64));
        let tasks = (0..64)
            .map(|_| {
                let cache = cache.clone();
                let barrier = barrier.clone();
                tokio::spawn(async move {
                    load(Some(&cache), "shared-generation".into(), |_| 64, async {
                        barrier.wait().await;
                        Ok(vec![1_u8; 64])
                    })
                    .await
                    .unwrap()
                })
            })
            .collect::<Vec<_>>();
        let values = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            futures::future::try_join_all(tasks),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(values.iter().all(|value| Arc::ptr_eq(value, &values[0])));
    }
}
