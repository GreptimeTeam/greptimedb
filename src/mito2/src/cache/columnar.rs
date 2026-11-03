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
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;

use store_api::storage::FileId;

use crate::cache::{
    CacheStrategy, CachedSstMeta, SST_META_DECODED_TYPE, SstDecodedMetaCache, SstMetaKey,
    decoded_meta_cache_weight,
};
use crate::error::Result;
use crate::metrics::{CACHE_BYTES, CACHE_HIT, CACHE_LOAD, CACHE_MISS};

const COLUMNAR_META: &str = "columnar_metadata";

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub(crate) enum DecodedKey {
    Sst(SstMetaKey),
    Columnar {
        namespace: FileId,
        identity: String,
        representation: TypeId,
    },
}

#[derive(Clone)]
pub(crate) enum DecodedValue {
    Sst(Arc<CachedSstMeta>),
    Columnar {
        value: Arc<dyn Any + Send + Sync>,
        weight: u32,
    },
}
impl DecodedValue {
    pub(crate) fn label(&self) -> &'static str {
        match self {
            Self::Sst(_) => SST_META_DECODED_TYPE,
            Self::Columnar { .. } => COLUMNAR_META,
        }
    }
}
pub(crate) fn shared_weight(key: &DecodedKey, value: &DecodedValue) -> u32 {
    match (key, value) {
        (DecodedKey::Sst(key), DecodedValue::Sst(value)) => decoded_meta_cache_weight(key, value),
        (_, DecodedValue::Columnar { weight, .. }) => *weight,
        _ => unreachable!("decoded metadata key/value mismatch"),
    }
}

/// Contains no artifact handle or live object-store reader. Fixed lock striping bounds
/// coordination memory; cancellation drops the guard and permits a later retry.
#[derive(Clone)]
pub(crate) struct MetadataCache {
    cache: SstDecodedMetaCache,
    locks: Arc<[tokio::sync::Mutex<()>; 32]>,
    namespace: FileId,
}
impl CacheStrategy {
    pub(crate) fn columnar_metadata(&self, namespace: FileId) -> Option<MetadataCache> {
        let Self::EnableAll(manager) = self else {
            return None;
        };
        Some(MetadataCache {
            cache: manager.sst_decoded_meta_cache.clone()?,
            locks: manager.columnar_load_locks.clone(),
            namespace,
        })
    }
}

/// Native payload blocks share the page-cache budget with Parquet. Keys never
/// retain a file handle, so cached bytes do not delay artifact purging.
#[derive(Clone)]
pub(crate) struct PayloadCache {
    cache: Arc<crate::cache::PageRangeCache>,
    namespace: FileId,
}

impl CacheStrategy {
    pub(crate) fn columnar_payload(&self, namespace: FileId) -> Option<PayloadCache> {
        let Self::EnableAll(manager) = self else {
            return None;
        };
        Some(PayloadCache {
            cache: manager.page_cache.clone()?,
            namespace,
        })
    }
}

impl PayloadCache {
    pub(crate) fn part(&self, generation: FileId, part: u32, file_size: u64) -> PayloadPartCache {
        PayloadPartCache {
            cache: self.cache.clone(),
            group: crate::cache::PageFragmentGroupKey::Columnar {
                namespace: self.namespace,
                generation,
                part,
                file_size,
            },
        }
    }
}

/// Exact aligned-block lookup avoids scanning the Parquet range index.
pub(crate) struct PayloadPartCache {
    cache: Arc<crate::cache::PageRangeCache>,
    group: crate::cache::PageFragmentGroupKey,
}

impl PayloadPartCache {
    fn key(&self, range: &std::ops::Range<u64>) -> crate::cache::PageFragmentKey {
        crate::cache::PageFragmentKey {
            group: self.group,
            start: range.start,
            end: range.end,
        }
    }

    pub(crate) fn get(&self, range: &std::ops::Range<u64>) -> Option<bytes::Bytes> {
        let value = self.cache.cache.get(&self.key(range));
        if value.is_some() {
            CACHE_HIT.with_label_values(&["columnar_payload"]).inc();
        } else {
            CACHE_MISS.with_label_values(&["columnar_payload"]).inc();
        }
        value
    }

    pub(crate) fn insert(&self, range: &std::ops::Range<u64>, bytes: &bytes::Bytes) {
        let key = self.key(range);
        let weight = crate::cache::page_cache_weight(&key, bytes);
        if u64::from(weight) > self.cache.cache.policy().max_capacity().unwrap_or(0) {
            return;
        }
        // Detach from the merged read allocation: eviction of one block must
        // release its bytes even while another block from that read is cached.
        let bytes = bytes::Bytes::copy_from_slice(bytes);
        CACHE_BYTES
            .with_label_values(&[crate::cache::PAGE_TYPE])
            .add(i64::from(weight));
        self.cache.cache.insert(key, bytes);
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
    let key = DecodedKey::Columnar {
        namespace: context.namespace,
        identity,
        representation: TypeId::of::<T>(),
    };
    let get = || match context.cache.get(&key) {
        Some(DecodedValue::Columnar { value, .. }) => value.downcast::<T>().ok(),
        _ => None,
    };
    if let Some(value) = get() {
        CACHE_HIT.with_label_values(&[COLUMNAR_META]).inc();
        return Ok(value);
    }
    CACHE_MISS.with_label_values(&[COLUMNAR_META]).inc();
    let mut hash = DefaultHasher::new();
    key.hash(&mut hash);
    let _guard = context.locks[hash.finish() as usize % context.locks.len()]
        .lock()
        .await;
    if let Some(value) = get() {
        return Ok(value);
    }
    CACHE_LOAD.with_label_values(&[COLUMNAR_META]).inc();
    let value = Arc::new(loader.await?);
    let identity_size = match &key {
        DecodedKey::Columnar { identity, .. } => identity.capacity(),
        _ => 0,
    };
    let retained = weight(&value).saturating_add(identity_size).saturating_add(
        std::mem::size_of::<DecodedKey>()
            + std::mem::size_of::<DecodedValue>()
            + 2 * std::mem::size_of::<usize>(),
    );
    if let Ok(weight) = u32::try_from(retained)
        && u64::from(weight) <= context.cache.policy().max_capacity().unwrap_or(0)
    {
        CACHE_BYTES
            .with_label_values(&[COLUMNAR_META])
            .add(i64::from(weight));
        context.cache.insert(
            key,
            DecodedValue::Columnar {
                value: value.clone(),
                weight,
            },
        );
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::cache::CacheManager;

    #[test]
    fn payload_cache_isolates_objects_and_bounds_memory() {
        let manager = Arc::new(CacheManager::builder().page_cache_size(4096).build());
        let strategy = CacheStrategy::EnableAll(manager.clone());
        let namespace = FileId::random();
        let generation = FileId::random();
        let cache = strategy.columnar_payload(namespace).unwrap();
        let part = cache.part(generation, 0, 100);
        let range = 0..10;
        part.insert(&range, &bytes::Bytes::from_static(b"0123456789"));
        assert_eq!(&part.get(&range).unwrap()[..], b"0123456789");
        assert!(cache.part(FileId::random(), 0, 100).get(&range).is_none());
        assert!(cache.part(generation, 1, 100).get(&range).is_none());
        assert!(cache.part(generation, 0, 101).get(&range).is_none());
        assert!(
            strategy
                .columnar_payload(FileId::random())
                .unwrap()
                .part(generation, 0, 100)
                .get(&range)
                .is_none()
        );
        assert!(
            !manager
                .get_page_ranges(generation, 0, std::slice::from_ref(&range))
                .unwrap()
                .is_fully_cached()
        );
        // A slice is detached from the much larger source allocation.
        let source = bytes::Bytes::from(vec![7; 8192]);
        part.insert(&range, &source.slice(..10));
        assert_ne!(source.as_ptr(), part.get(&range).unwrap().as_ptr());
        part.insert(&(0..8192), &source);
        assert!(part.get(&(0..8192)).is_none());
        part.cache.cache.invalidate(&part.key(&range));
        part.cache.cache.run_pending_tasks();
        assert!(part.get(&range).is_none());
        assert!(
            CacheStrategy::EnableAll(Arc::new(CacheManager::default()))
                .columnar_payload(namespace)
                .is_none()
        );
    }

    #[tokio::test]
    async fn concurrent_loads_coalesce_and_cancellation_allows_retry() {
        let manager = CacheManager::builder().sst_meta_cache_size(8192).build();
        let strategy = CacheStrategy::EnableAll(Arc::new(manager));
        let cache = strategy.columnar_metadata(FileId::random()).unwrap();
        let calls = AtomicUsize::new(0);
        let request = || {
            load(Some(&cache), "generation/part/page".into(), |_| 64, async {
                calls.fetch_add(1, Ordering::Relaxed);
                tokio::task::yield_now().await;
                Ok(vec![1u8, 2, 3])
            })
        };
        let (left, right) = tokio::join!(request(), request());
        assert!(Arc::ptr_eq(&left.unwrap(), &right.unwrap()));
        assert_eq!(1, calls.load(Ordering::Relaxed));

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
}
