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

use store_api::storage::FileId;

use crate::cache::{CacheStrategy, ColumnarMetaCache};
use crate::error::Result;
use crate::metrics::{CACHE_BYTES, CACHE_HIT, CACHE_LOAD, CACHE_MISS};

const COLUMNAR_META: &str = "columnar_metadata";

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
    weight: u32,
}
impl DecodedValue {
    pub(crate) fn label(&self) -> &'static str {
        COLUMNAR_META
    }
}
/// Weigher for the native-reader metadata tier; the value carries its own admitted weight.
pub(crate) fn columnar_meta_weight(_key: &DecodedKey, value: &DecodedValue) -> u32 {
    value.weight
}

/// Contains no artifact handle or live object-store reader. Loads run independently;
/// admission uses a synchronous, atomic cache entry after loading.
#[derive(Clone)]
pub(crate) struct MetadataCache {
    cache: ColumnarMetaCache,
    namespace: FileId,
}
impl CacheStrategy {
    pub(crate) fn columnar_metadata(&self, namespace: FileId) -> Option<MetadataCache> {
        let Self::EnableAll(manager) = self else {
            return None;
        };
        Some(MetadataCache {
            cache: manager.columnar_meta_cache.clone()?,
            namespace,
        })
    }
}

// Native payload blocks used to share the page-cache budget through the shared
// `PageRangeCache`, which main replaced with `ParquetPageCache`. That cache serves
// uniform-size blocks fetched through a reader, so it cannot hold these aligned blocks
// clipped to the payload area.
// TODO(narthex-merge): re-attach native payload byte caching to a dedicated page-cache
// budget in `CacheManager`; until then payload reads stay uncached.
/// Placeholder for the disabled native payload byte cache.
#[allow(dead_code)]
#[derive(Clone)]
pub(crate) struct PayloadCache {
    namespace: FileId,
}

impl CacheStrategy {
    pub(crate) fn columnar_payload(&self, _namespace: FileId) -> Option<PayloadCache> {
        None
    }
}

/// Placeholder for one cached payload block; lookups always miss.
#[allow(dead_code)]
pub(crate) struct PayloadPartCache {
    namespace: FileId,
    generation: FileId,
    part: u32,
    file_size: u64,
}

impl PayloadCache {
    pub(crate) fn part(&self, generation: FileId, part: u32, file_size: u64) -> PayloadPartCache {
        PayloadPartCache {
            namespace: self.namespace,
            generation,
            part,
            file_size,
        }
    }
}

impl PayloadPartCache {
    pub(crate) fn get(&self, _range: &std::ops::Range<u64>) -> Option<bytes::Bytes> {
        CACHE_MISS.with_label_values(&["columnar_payload"]).inc();
        None
    }

    pub(crate) fn insert(&self, _range: &std::ops::Range<u64>, _bytes: &bytes::Bytes) {}
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
    let key = DecodedKey {
        namespace: context.namespace,
        identity,
        representation: TypeId::of::<T>(),
    };
    let get = || {
        context
            .cache
            .get(&key)
            .and_then(|value| value.value.downcast::<T>().ok())
    };
    if let Some(value) = get() {
        CACHE_HIT.with_label_values(&[COLUMNAR_META]).inc();
        return Ok(value);
    }
    CACHE_MISS.with_label_values(&[COLUMNAR_META]).inc();
    CACHE_LOAD.with_label_values(&[COLUMNAR_META]).inc();
    let value = Arc::new(loader.await?);
    // A caller may flush buffered output while its loader is Pending, then stop
    // polling it under downstream backpressure. Never hold a shared lock across
    // that await: another stream may need the lock to finish the current series.
    let identity_size = key.identity.capacity();
    let retained = weight(&value).saturating_add(identity_size).saturating_add(
        std::mem::size_of::<DecodedKey>()
            + std::mem::size_of::<DecodedValue>()
            + 2 * std::mem::size_of::<usize>(),
    );
    if let Ok(weight) = u32::try_from(retained)
        && u64::from(weight) <= context.cache.policy().max_capacity().unwrap_or(0)
    {
        let entry = context.cache.entry(key).or_insert_with(|| DecodedValue {
            value: value.clone(),
            weight,
        });
        if entry.is_fresh() {
            CACHE_BYTES
                .with_label_values(&[COLUMNAR_META])
                .add(i64::from(weight));
        }
        let admitted = entry.into_value().value;
        if let Ok(admitted) = admitted.downcast::<T>() {
            return Ok(admitted);
        }
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::CacheManager;
    use std::hash::{DefaultHasher, Hash, Hasher};

    // TODO(narthex-merge): restore payload-block assertions once the byte cache gets a budget.
    #[test]
    fn payload_cache_stays_disabled_without_a_page_budget() {
        let manager = Arc::new(CacheManager::builder().page_cache_size(4096).build());
        let strategy = CacheStrategy::EnableAll(manager);
        assert!(strategy.columnar_payload(FileId::random()).is_none());
        assert!(
            CacheStrategy::EnableAll(Arc::new(CacheManager::default()))
                .columnar_payload(FileId::random())
                .is_none()
        );
        assert!(
            CacheStrategy::Disabled
                .columnar_payload(FileId::random())
                .is_none()
        );
    }

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
