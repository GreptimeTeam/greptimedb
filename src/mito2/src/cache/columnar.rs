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

/// Contains no artifact handle or live object-store reader. Loads run independently;
/// admission uses a synchronous, atomic cache entry after loading.
#[derive(Clone)]
pub(crate) struct MetadataCache {
    cache: SstDecodedMetaCache,
    namespace: FileId,
}
impl CacheStrategy {
    pub(crate) fn columnar_metadata(&self, namespace: FileId) -> Option<MetadataCache> {
        let Self::EnableAll(manager) = self else {
            return None;
        };
        Some(MetadataCache {
            cache: manager.sst_decoded_meta_cache.clone()?,
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
    CACHE_LOAD.with_label_values(&[COLUMNAR_META]).inc();
    let value = Arc::new(loader.await?);
    // A caller may flush buffered output while its loader is Pending, then stop
    // polling it under downstream backpressure. Never hold a shared lock across
    // that await: another stream may need the lock to finish the current series.
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
        let entry = context
            .cache
            .entry(key)
            .or_insert_with(|| DecodedValue::Columnar {
                value: value.clone(),
                weight,
            });
        if entry.is_fresh() {
            CACHE_BYTES
                .with_label_values(&[COLUMNAR_META])
                .add(i64::from(weight));
        }
        if let DecodedValue::Columnar {
            value: admitted, ..
        } = entry.into_value()
            && let Ok(admitted) = admitted.downcast::<T>()
        {
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
            let key = DecodedKey::Columnar {
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
}
