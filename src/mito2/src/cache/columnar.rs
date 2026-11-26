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

use crate::cache::{CacheStrategy, SstDecodedMetaCache, SstDecodedMetaKey, MetaValue};
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
    cache: Arc<SstDecodedMetaCache>,
    metrics: Arc<common_cache::metrics::CacheMetrics>,
    namespace: FileId,
}
impl CacheStrategy {
    pub(crate) fn columnar_metadata(&self, namespace: FileId) -> Option<MetadataCache> {
        let Self::EnableAll(manager) = self else {
            return None;
        };
        Some(MetadataCache {
            cache: manager.sst_decoded_meta_cache.clone()?,
            metrics: manager.columnar_load_metrics.clone()?,
            namespace,
        })
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
    let key = DecodedKey {
        namespace: context.namespace,
        identity,
        representation: TypeId::of::<T>(),
    };
    let identity_size = key.identity.capacity();
    let key = SstDecodedMetaKey::Native(key);
    if let Some(MetaValue::Native(value)) = context.cache.get(&key).await {
        let value = value.value.downcast::<T>().expect("type is part of the key");
        CACHE_HIT.with_label_values(&[COLUMNAR_META]).inc();
        return Ok(value);
    }
    CACHE_MISS.with_label_values(&[COLUMNAR_META]).inc();
    let value = Arc::new(context.metrics.loads.observe(loader).await?);
    // A caller may flush buffered output while its loader is Pending, then stop
    // polling it under downstream backpressure. Never hold a shared lock across
    // that await: another stream may need the lock to finish the current series.
    let retained = weight(&value).saturating_add(identity_size).saturating_add(
        std::mem::size_of::<SstDecodedMetaKey>()
            + std::mem::size_of::<MetaValue>()
            + 2 * std::mem::size_of::<usize>(),
    );
    if let Ok(weight) = u32::try_from(retained)
        && u64::from(weight) <= context.cache.capacity()
    {
        let admitted = context.cache.get_or_insert(key, MetaValue::Native(DecodedValue {
            value: value.clone(),
            weight,
        })).await;
        if let MetaValue::Native(entry) = admitted {
            return Ok(entry.value.downcast::<T>().expect("type is part of the key"));
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
        let manager = CacheManager::builder()
            .sst_meta_cache_size(8192)
            .enable_columnar_meta_cache(true)
            .build();
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
            CacheManager::builder()
                .sst_meta_cache_size(8192)
                .enable_columnar_meta_cache(true)
                .build(),
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
