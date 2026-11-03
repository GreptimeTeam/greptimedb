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
/// coordination memory; locks serialize only the post-load recheck-and-insert decision.
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
    let stripe = hash.finish() as usize % context.locks.len();
    // The load itself never holds the stripe: a stream that is holding it can be paused by
    // backpressure from a consumer, and a second stream waiting for that stripe would then wait
    // for a task nothing polls again. Two loads of one identity can therefore both run, which
    // costs duplicated reads but removes the dependency on polling a paused loader to release its stripe.
    CACHE_LOAD.with_label_values(&[COLUMNAR_META]).inc();
    let value = Arc::new(loader.await?);
    // Admission is the only critical section left, and it does not wait: a stripe another load
    // still holds means this value is simply returned without being cached.
    let Ok(_guard) = context.locks[stripe].try_lock() else {
        return Ok(value);
    };
    if let Some(existing) = get() {
        return Ok(existing);
    }
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

    /// Builds a cache on one stripe set, plus the helper that names an identity.
    fn cache() -> MetadataCache {
        let manager = CacheManager::builder().sst_meta_cache_size(8192).build();
        let strategy = CacheStrategy::EnableAll(Arc::new(manager));
        strategy.columnar_metadata(FileId::random()).unwrap()
    }

    /// The stripe an identity lands on, computed exactly as `load` computes it.
    fn stripe_of(cache: &MetadataCache, identity: &str) -> usize {
        let key = DecodedKey::Columnar {
            namespace: cache.namespace,
            identity: identity.to_string(),
            representation: TypeId::of::<Vec<u8>>(),
        };
        let mut hash = DefaultHasher::new();
        key.hash(&mut hash);
        hash.finish() as usize % cache.locks.len()
    }

    /// Two identities that share one stripe, found rather than assumed: with 32 stripes a
    /// handful of names collides.
    fn colliding_pair(cache: &MetadataCache) -> (String, String) {
        let mut seen = std::collections::HashMap::new();
        for index in 0..1024 {
            let identity = format!("generation/part/page-{index}");
            let stripe = stripe_of(cache, &identity);
            if let Some(previous) = seen.insert(stripe, identity.clone()) {
                return (previous, identity);
            }
        }
        panic!("no two identities shared a stripe in 1024 tries");
    }

    /// Polls `future` to completion without a wall clock, so a load that waits for a stripe a
    /// paused loader holds fails the assertion instead of hanging the test.
    async fn complete_promptly<T>(future: impl std::future::Future<Output = T>) -> Option<T> {
        let mut future = Box::pin(future);
        for _ in 0..256 {
            if let std::task::Poll::Ready(value) = futures::poll!(&mut future) {
                return Some(value);
            }
            tokio::task::yield_now().await;
        }
        None
    }

    /// A load must not wait for a stripe another, paused load holds.
    ///
    /// This is the shape of the reported stall: a stream takes the stripe, its loader awaits and
    /// the consumer that would poll it again is blocked, so nothing advances it. A second load of
    /// the same identity must still finish.
    #[tokio::test]
    async fn a_paused_load_does_not_block_the_same_identity() {
        let cache = cache();
        let identity = "generation/part/page".to_string();
        let mut paused = Box::pin(load(
            Some(&cache),
            identity.clone(),
            |_| 64,
            std::future::pending::<Result<Vec<u8>>>(),
        ));
        // One poll takes the stripe (under the old code) and parks inside the loader.
        assert!(futures::poll!(&mut paused).is_pending());

        let finished = complete_promptly(load(Some(&cache), identity, |_| 64, async {
            Ok(vec![7u8])
        }))
        .await;
        let value = finished.expect("a second load must not wait for a paused one on its stripe");
        assert_eq!(&[7], value.unwrap().as_slice());
    }

    /// When two loads of one identity both miss, the later finisher adopts the value admitted first.
    #[tokio::test]
    async fn a_load_that_finishes_later_adopts_the_value_admitted_first() {
        let cache = cache();
        let identity = "generation/part/racing-page".to_string();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let mut load_a = Box::pin(load(Some(&cache), identity.clone(), |_| 64, async move {
            Ok(receiver.await.expect("load A payload sender remains alive"))
        }));
        assert!(futures::poll!(&mut load_a).is_pending());

        let value_b = complete_promptly(load(Some(&cache), identity.clone(), |_| 64, async {
            Ok(vec![1u8])
        }))
        .await
        .expect("load B must finish while load A is parked")
        .unwrap();
        assert_eq!(&[1], value_b.as_slice());

        sender.send(vec![2u8]).expect("load A is still waiting");
        let value_a = complete_promptly(&mut load_a)
            .await
            .expect("load A must finish promptly")
            .unwrap();
        assert!(
            Arc::ptr_eq(&value_b, &value_a),
            "later-finishing load must adopt the value admitted by load B"
        );
        assert_eq!(&[1], value_a.as_slice());

        let cached = load(Some(&cache), identity, |_| 64, async {
            panic!("cached value should avoid invoking the loader")
        })
        .await
        .unwrap();
        assert!(Arc::ptr_eq(&value_b, &cached));
        assert_eq!(&[1], cached.as_slice());
    }

    /// The same, for two identities that share one stripe.
    #[tokio::test]
    async fn a_paused_load_does_not_block_a_colliding_identity() {
        let cache = cache();
        let (paused_identity, other) = colliding_pair(&cache);
        assert_eq!(
            stripe_of(&cache, &paused_identity),
            stripe_of(&cache, &other)
        );

        let mut paused = Box::pin(load(
            Some(&cache),
            paused_identity,
            |_| 64,
            std::future::pending::<Result<Vec<u8>>>(),
        ));
        assert!(futures::poll!(&mut paused).is_pending());

        let finished =
            complete_promptly(load(Some(&cache), other, |_| 64, async { Ok(vec![9u8]) })).await;
        let value = finished.expect("a load must not wait for a paused load on its stripe");
        assert_eq!(&[9], value.unwrap().as_slice());
    }

    /// A contended admission returns promptly without waiting, then can be cached on a later load.
    #[tokio::test]
    async fn contended_admission_does_not_wait_and_later_load_is_cached() {
        let cache = cache();
        let identity = "generation/part/admission".to_string();
        let stripe = stripe_of(&cache, &identity);
        let guard = cache.locks[stripe].lock().await;

        let finished = complete_promptly(load(Some(&cache), identity.clone(), |_| 64, async {
            Ok(vec![5u8, 6])
        }))
        .await;
        let value = finished
            .expect("load must return while its admission stripe is held")
            .unwrap();
        assert_eq!(&[5, 6], value.as_slice());
        drop(guard);

        let calls = AtomicUsize::new(0);
        let admitted = load(Some(&cache), identity.clone(), |_| 64, async {
            calls.fetch_add(1, Ordering::Relaxed);
            Ok(vec![5u8, 6])
        })
        .await
        .unwrap();
        assert_eq!(&[5, 6], admitted.as_slice());
        let cached = load(Some(&cache), identity, |_| 64, async {
            calls.fetch_add(1, Ordering::Relaxed);
            Ok(vec![5u8, 6])
        })
        .await
        .unwrap();
        assert!(Arc::ptr_eq(&admitted, &cached));
        assert_eq!(1, calls.load(Ordering::Relaxed), "the retry was admitted");
    }

    /// A finished load is reused, and a contended admission remains available to a later load.
    #[tokio::test]
    async fn loads_are_reused_and_a_canceled_load_allows_a_retry() {
        let cache = cache();
        let calls = AtomicUsize::new(0);
        let request = || {
            load(Some(&cache), "generation/part/page".into(), |_| 64, async {
                calls.fetch_add(1, Ordering::Relaxed);
                tokio::task::yield_now().await;
                Ok(vec![1u8, 2, 3])
            })
        };
        let first = request().await.unwrap();
        let second = request().await.unwrap();
        assert!(
            Arc::ptr_eq(&first, &second),
            "a completed load must be served from the cache"
        );
        assert_eq!(1, calls.load(Ordering::Relaxed), "the loader ran once");

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
