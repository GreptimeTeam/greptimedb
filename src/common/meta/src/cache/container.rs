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

use std::borrow::Borrow;
use std::hash::Hash;
use std::sync::Arc;

use futures::future::BoxFuture;
use moka::future::Cache;
use snafu::{OptionExt, ResultExt};

use crate::cache_invalidator::{CacheInvalidator, Context};
use crate::error::{self, Error, Result};
use crate::instruction::CacheIdent;

/// Filters out unused [CacheToken]s
pub type TokenFilter<CacheToken> = Box<dyn Fn(&CacheToken) -> bool + Send + Sync>;

/// Invalidates cached values by [CacheToken]s.
pub type Invalidator<K, V, CacheToken> =
    Box<dyn for<'a> Fn(&'a Cache<K, V>, &'a CacheToken) -> BoxFuture<'a, Result<()>> + Send + Sync>;

/// Initializes value (i.e., fetches from remote).
pub type Initializer<K, V> = Arc<dyn Fn(&'_ K) -> BoxFuture<'_, Result<Option<V>>> + Send + Sync>;

/// [CacheContainer] provides ability to:
/// - Cache value loaded by [Initializer].
/// - Invalidate caches by [Invalidator].
pub struct CacheContainer<K, V, CacheToken> {
    cache: Cache<K, V>,
    invalidator: Invalidator<K, V, CacheToken>,
    initializer: Initializer<K, V>,
    token_filter: TokenFilter<CacheToken>,
}

impl<K, V, CacheToken> CacheContainer<K, V, CacheToken>
where
    K: Send + Sync,
    V: Send + Sync,
    CacheToken: Send + Sync,
{
    /// Constructs an [CacheContainer].
    pub fn new(
        cache: Cache<K, V>,
        invalidator: Invalidator<K, V, CacheToken>,
        initializer: Initializer<K, V>,
        token_filter: TokenFilter<CacheToken>,
    ) -> Self {
        Self {
            cache,
            invalidator,
            initializer,
            token_filter,
        }
    }
}

#[async_trait::async_trait]
impl<K, V> CacheInvalidator for CacheContainer<K, V, CacheIdent>
where
    K: Send + Sync,
    V: Send + Sync,
{
    async fn invalidate(&self, _ctx: &Context, caches: Vec<CacheIdent>) -> Result<()> {
        for token in caches
            .into_iter()
            .filter(|token| (self.token_filter)(token))
        {
            (self.invalidator)(&self.cache, &token).await?;
        }
        Ok(())
    }
}

impl<K, V, CacheToken> CacheContainer<K, V, CacheToken>
where
    K: Copy + Hash + Eq + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    /// Returns a _clone_ of the value corresponding to the key.
    pub async fn get(&self, key: K) -> Result<Option<V>> {
        let moved_init = self.initializer.clone();
        let moved_key = key;
        let init = async move {
            moved_init(&moved_key)
                .await
                .transpose()
                .context(error::ValueNotExistSnafu)?
        };

        match self.cache.try_get_with(key, init).await {
            Ok(value) => Ok(Some(value)),
            Err(err) => match err.as_ref() {
                Error::ValueNotExist { .. } => Ok(None),
                _ => Err(err).context(error::GetCacheSnafu),
            },
        }
    }
}

impl<K, V, CacheToken> CacheContainer<K, V, CacheToken>
where
    K: Hash + Eq + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    /// Invalidates cache by [CacheToken].
    pub async fn invalidate(&self, caches: &[CacheToken]) -> Result<()> {
        for token in caches.iter().filter(|token| (self.token_filter)(token)) {
            (self.invalidator)(&self.cache, token).await?;
        }
        Ok(())
    }

    /// Returns a _clone_ of the value corresponding to the key.
    pub async fn get_by_ref<Q>(&self, key: &Q) -> Result<Option<V>>
    where
        K: Borrow<Q>,
        Q: ToOwned<Owned = K> + Hash + Eq + ?Sized,
    {
        let moved_init = self.initializer.clone();
        let moved_key = key.to_owned();
        let init = async move {
            moved_init(&moved_key)
                .await
                .transpose()
                .context(error::ValueNotExistSnafu)?
        };

        match self.cache.try_get_with_by_ref(key, init).await {
            Ok(value) => Ok(Some(value)),
            Err(err) => match err.as_ref() {
                Error::ValueNotExist { .. } => Ok(None),
                _ => Err(err).context(error::GetCacheSnafu),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicI32, Ordering};
    use std::sync::Arc;

    use moka::future::{Cache, CacheBuilder};

    use super::*;

    #[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
    struct NameKey<'a> {
        name: &'a str,
    }

    #[tokio::test]
    async fn test_get() {
        let cache: Cache<NameKey, String> = CacheBuilder::new(128).build();
        let filter: TokenFilter<String> = Box::new(|_| true);
        let counter = Arc::new(AtomicI32::new(0));
        let moved_counter = counter.clone();
        let init: Initializer<NameKey, String> = Arc::new(move |_| {
            moved_counter.fetch_add(1, Ordering::Relaxed);
            Box::pin(async { Ok(Some("hi".to_string())) })
        });
        let invalidator: Invalidator<NameKey, String, String> =
            Box::new(|_, _| Box::pin(async { Ok(()) }));

        let adv_cache = CacheContainer::new(cache, invalidator, init, filter);
        let key = NameKey { name: "key" };
        let value = adv_cache.get(key).await.unwrap().unwrap();
        assert_eq!(value, "hi");
    }

    #[tokio::test]
    async fn test_get_by_ref() {
        let cache: Cache<String, String> = CacheBuilder::new(128).build();
        let filter: TokenFilter<String> = Box::new(|_| true);
        let counter = Arc::new(AtomicI32::new(0));
        let moved_counter = counter.clone();
        let init: Initializer<String, String> = Arc::new(move |_| {
            moved_counter.fetch_add(1, Ordering::Relaxed);
            Box::pin(async { Ok(Some("hi".to_string())) })
        });
        let invalidator: Invalidator<String, String, String> =
            Box::new(|_, _| Box::pin(async { Ok(()) }));

        let adv_cache = CacheContainer::new(cache, invalidator, init, filter);
        let value = adv_cache.get_by_ref("foo").await.unwrap().unwrap();
        assert_eq!(value, "hi");
        let value = adv_cache.get_by_ref("foo").await.unwrap().unwrap();
        assert_eq!(value, "hi");
        assert_eq!(counter.load(Ordering::Relaxed), 1);
        let value = adv_cache.get_by_ref("bar").await.unwrap().unwrap();
        assert_eq!(value, "hi");
        assert_eq!(counter.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn test_invalidate() {
        let cache: Cache<String, String> = CacheBuilder::new(128).build();
        let filter: TokenFilter<String> = Box::new(|_| true);
        let counter = Arc::new(AtomicI32::new(0));
        let moved_counter = counter.clone();
        let init: Initializer<String, String> = Arc::new(move |_| {
            moved_counter.fetch_add(1, Ordering::Relaxed);
            Box::pin(async { Ok(Some("hi".to_string())) })
        });
        let invalidator: Invalidator<String, String, String> = Box::new(|cache, key| {
            Box::pin(async move {
                cache.invalidate(key).await;
                Ok(())
            })
        });

        let adv_cache = CacheContainer::new(cache, invalidator, init, filter);
        let value = adv_cache.get_by_ref("foo").await.unwrap().unwrap();
        assert_eq!(value, "hi");
        let value = adv_cache.get_by_ref("foo").await.unwrap().unwrap();
        assert_eq!(value, "hi");
        assert_eq!(counter.load(Ordering::Relaxed), 1);
        adv_cache
            .invalidate(&["foo".to_string(), "bar".to_string()])
            .await
            .unwrap();
        let value = adv_cache.get_by_ref("foo").await.unwrap().unwrap();
        assert_eq!(value, "hi");
        assert_eq!(counter.load(Ordering::Relaxed), 2);
    }
}
