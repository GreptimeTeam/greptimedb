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
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use backon::{BackoffBuilder, ExponentialBuilder};
use futures::future::BoxFuture;
use moka::future::Cache;
use snafu::{OptionExt, ResultExt};
use tokio::time::sleep;

use crate::cache_invalidator::{CacheInvalidator, Context};
use crate::error::{self, Error, Result};
use crate::instruction::CacheIdent;
use crate::metrics;

/// Filters out unused [CacheToken]s
pub type TokenFilter<CacheToken> = Box<dyn Fn(&CacheToken) -> bool + Send + Sync>;

/// Invalidates cached values by [CacheToken]s.
pub type Invalidator<K, V, CacheToken> = Box<
    dyn for<'a> Fn(&'a Cache<K, V>, &'a [&CacheToken]) -> BoxFuture<'a, Result<()>> + Send + Sync,
>;

/// Initializes value (i.e., fetches from remote).
pub type Initializer<K, V> = Arc<dyn Fn(&'_ K) -> BoxFuture<'_, Result<Option<V>>> + Send + Sync>;

#[derive(Debug, Clone, Copy)]
/// Initialization strategy for cache-miss loading.
///
/// This strategy is selected when building [CacheContainer] and remains immutable
/// for the lifetime of the container instance.
pub enum InitStrategy {
    /// Fast path: load once without version conflict retry.
    ///
    /// Under concurrent invalidation, callers may observe stale/dirty value.
    Unchecked,
    /// Strict path: retry load when version changes during initialization.
    ///
    /// This avoids returning dirty value under invalidate/load races.
    VersionChecked,
}

/// [CacheContainer] provides ability to:
/// - Cache value loaded by [Initializer].
/// - Invalidate caches by [Invalidator].
pub struct CacheContainer<K, V, CacheToken> {
    name: String,
    cache: Cache<K, V>,
    invalidator: Invalidator<K, V, CacheToken>,
    initializer: Initializer<K, V>,
    token_filter: fn(&CacheToken) -> bool,
    version: Arc<AtomicUsize>,
    init_strategy: InitStrategy,
}

fn latest_get_backoff() -> impl Iterator<Item = Duration> {
    ExponentialBuilder::default()
        .with_min_delay(Duration::from_millis(100))
        .with_max_delay(Duration::from_millis(1000))
        .with_max_times(3)
        .build()
}

impl<K, V, CacheToken> CacheContainer<K, V, CacheToken>
where
    K: Send + Sync,
    V: Send + Sync,
    CacheToken: Send + Sync,
{
    /// Constructs an [CacheContainer] with [InitStrategy::Unchecked].
    ///
    /// This keeps the historical behavior and can return stale/dirty value under
    /// concurrent invalidation.
    pub fn new(
        name: String,
        cache: Cache<K, V>,
        invalidator: Invalidator<K, V, CacheToken>,
        initializer: Initializer<K, V>,
        token_filter: fn(&CacheToken) -> bool,
    ) -> Self {
        Self::with_strategy(
            name,
            cache,
            invalidator,
            initializer,
            token_filter,
            InitStrategy::Unchecked,
        )
    }

    /// Constructs an [CacheContainer] with explicit [InitStrategy].
    ///
    /// The strategy is fixed at construction time and cannot be changed later.
    pub fn with_strategy(
        name: String,
        cache: Cache<K, V>,
        invalidator: Invalidator<K, V, CacheToken>,
        initializer: Initializer<K, V>,
        token_filter: fn(&CacheToken) -> bool,
        init_strategy: InitStrategy,
    ) -> Self {
        Self {
            name,
            cache,
            invalidator,
            initializer,
            token_filter,
            version: Arc::new(AtomicUsize::new(0)),
            init_strategy,
        }
    }

    /// Returns the `name`.
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl<K, V, CacheToken> CacheContainer<K, V, CacheToken> {
    fn inc_version(&self) {
        self.version.fetch_add(1, Ordering::Relaxed);
    }
}

async fn init<'a, K, V>(init: Initializer<K, V>, key: K, cache_name: &'a str) -> Result<V>
where
    K: Send + Sync + 'a,
    V: Send + 'a,
{
    metrics::CACHE_CONTAINER_CACHE_MISS
        .with_label_values(&[cache_name])
        .inc();
    let _timer = metrics::CACHE_CONTAINER_LOAD_CACHE
        .with_label_values(&[cache_name])
        .start_timer();
    init(&key)
        .await
        .transpose()
        .context(error::ValueNotExistSnafu)?
}

async fn init_with_retry<'a, K, V>(
    init: Initializer<K, V>,
    key: K,
    mut backoff: impl Iterator<Item = Duration> + 'a,
    version: Arc<AtomicUsize>,
    cache_name: &'a str,
) -> Result<V>
where
    K: Send + Sync + 'a,
    V: Send + 'a,
{
    let mut attempts = 1usize;
    loop {
        let pre_version = version.load(Ordering::Relaxed);
        metrics::CACHE_CONTAINER_CACHE_MISS
            .with_label_values(&[cache_name])
            .inc();
        let _timer = metrics::CACHE_CONTAINER_LOAD_CACHE
            .with_label_values(&[cache_name])
            .start_timer();
        let value = init(&key)
            .await
            .transpose()
            .context(error::ValueNotExistSnafu)??;

        if pre_version == version.load(Ordering::Relaxed) {
            return Ok(value);
        }

        if let Some(duration) = backoff.next() {
            sleep(duration).await;
            attempts += 1;
        } else {
            return error::GetLatestCacheRetryExceededSnafu { attempts }.fail();
        }
    }
}

#[async_trait::async_trait]
impl<K, V> CacheInvalidator for CacheContainer<K, V, CacheIdent>
where
    K: Send + Sync,
    V: Send + Sync,
{
    async fn invalidate(&self, _ctx: &Context, caches: &[CacheIdent]) -> Result<()> {
        let idents = caches
            .iter()
            .filter(|token| (self.token_filter)(token))
            .collect::<Vec<_>>();
        if !idents.is_empty() {
            self.inc_version();
            (self.invalidator)(&self.cache, &idents).await?;
        }

        Ok(())
    }
}

impl<K, V, CacheToken> CacheContainer<K, V, CacheToken>
where
    K: Copy + Hash + Eq + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    /// Returns a value from cache for copyable keys.
    ///
    /// With [InitStrategy::Unchecked], this method prioritizes latency and may
    /// return stale/dirty value. With [InitStrategy::VersionChecked], this method
    /// retries initialization on version change and avoids dirty returns.
    pub async fn get(&self, key: K) -> Result<Option<V>> {
        metrics::CACHE_CONTAINER_CACHE_GET
            .with_label_values(&[&self.name])
            .inc();

        let result = match self.init_strategy {
            InitStrategy::Unchecked => {
                self.cache
                    .try_get_with(key, init(self.initializer.clone(), key, &self.name))
                    .await
            }
            InitStrategy::VersionChecked => {
                self.cache
                    .try_get_with(
                        key,
                        init_with_retry(
                            self.initializer.clone(),
                            key,
                            latest_get_backoff(),
                            self.version.clone(),
                            &self.name,
                        ),
                    )
                    .await
            }
        };

        match result {
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
        let idents = caches
            .iter()
            .filter(|token| (self.token_filter)(token))
            .collect::<Vec<_>>();
        if !idents.is_empty() {
            self.inc_version();
            (self.invalidator)(&self.cache, &idents).await?;
        }

        Ok(())
    }

    /// Returns true if the cache contains a value for the key.
    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.cache.contains_key(key)
    }

    /// Returns a value from cache by key reference.
    ///
    /// With [InitStrategy::Unchecked], this method prioritizes latency and may
    /// return stale/dirty value. With [InitStrategy::VersionChecked], this method
    /// retries initialization on version change and avoids dirty returns.
    pub async fn get_by_ref<Q>(&self, key: &Q) -> Result<Option<V>>
    where
        K: Borrow<Q>,
        Q: ToOwned<Owned = K> + Hash + Eq + ?Sized,
    {
        metrics::CACHE_CONTAINER_CACHE_GET
            .with_label_values(&[&self.name])
            .inc();
        let result = match self.init_strategy {
            InitStrategy::Unchecked => {
                self.cache
                    .try_get_with_by_ref(
                        key,
                        init(self.initializer.clone(), key.to_owned(), &self.name),
                    )
                    .await
            }
            InitStrategy::VersionChecked => {
                self.cache
                    .try_get_with_by_ref(
                        key,
                        init_with_retry(
                            self.initializer.clone(),
                            key.to_owned(),
                            latest_get_backoff(),
                            self.version.clone(),
                            &self.name,
                        ),
                    )
                    .await
            }
        };

        match result {
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
    use std::sync::Arc;
    use std::sync::atomic::{AtomicI32, Ordering};

    use moka::future::{Cache, CacheBuilder};

    use super::*;

    #[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
    struct NameKey<'a> {
        name: &'a str,
    }

    fn always_true_filter(_: &String) -> bool {
        true
    }

    #[tokio::test]
    async fn test_get() {
        let cache: Cache<NameKey, String> = CacheBuilder::new(128).build();
        let counter = Arc::new(AtomicI32::new(0));
        let moved_counter = counter.clone();
        let init: Initializer<NameKey, String> = Arc::new(move |_| {
            moved_counter.fetch_add(1, Ordering::Relaxed);
            Box::pin(async { Ok(Some("hi".to_string())) })
        });
        let invalidator: Invalidator<NameKey, String, String> =
            Box::new(|_, _| Box::pin(async { Ok(()) }));

        let adv_cache = CacheContainer::new(
            "test".to_string(),
            cache,
            invalidator,
            init,
            always_true_filter,
        );
        let key = NameKey { name: "key" };
        let value = adv_cache.get(key).await.unwrap().unwrap();
        assert_eq!(value, "hi");
        assert_eq!(counter.load(Ordering::Relaxed), 1);
        let key = NameKey { name: "key" };
        let value = adv_cache.get(key).await.unwrap().unwrap();
        assert_eq!(value, "hi");
        assert_eq!(counter.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn test_get_by_ref() {
        let cache: Cache<String, String> = CacheBuilder::new(128).build();
        let counter = Arc::new(AtomicI32::new(0));
        let moved_counter = counter.clone();
        let init: Initializer<String, String> = Arc::new(move |_| {
            moved_counter.fetch_add(1, Ordering::Relaxed);
            Box::pin(async { Ok(Some("hi".to_string())) })
        });
        let invalidator: Invalidator<String, String, String> =
            Box::new(|_, _| Box::pin(async { Ok(()) }));

        let adv_cache = CacheContainer::new(
            "test".to_string(),
            cache,
            invalidator,
            init,
            always_true_filter,
        );
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
    async fn test_get_value_not_exits() {
        let cache: Cache<String, String> = CacheBuilder::new(128).build();
        let init: Initializer<String, String> =
            Arc::new(move |_| Box::pin(async { error::ValueNotExistSnafu {}.fail() }));
        let invalidator: Invalidator<String, String, String> =
            Box::new(|_, _| Box::pin(async { Ok(()) }));

        let adv_cache = CacheContainer::new(
            "test".to_string(),
            cache,
            invalidator,
            init,
            always_true_filter,
        );
        let value = adv_cache.get_by_ref("foo").await.unwrap();
        assert!(value.is_none());
    }

    #[tokio::test]
    async fn test_invalidate() {
        let cache: Cache<String, String> = CacheBuilder::new(128).build();
        let counter = Arc::new(AtomicI32::new(0));
        let moved_counter = counter.clone();
        let init: Initializer<String, String> = Arc::new(move |_| {
            moved_counter.fetch_add(1, Ordering::Relaxed);
            Box::pin(async { Ok(Some("hi".to_string())) })
        });
        let invalidator: Invalidator<String, String, String> = Box::new(|cache, keys| {
            Box::pin(async move {
                for key in keys {
                    cache.invalidate(*key).await;
                }
                Ok(())
            })
        });

        let adv_cache = CacheContainer::new(
            "test".to_string(),
            cache,
            invalidator,
            init,
            always_true_filter,
        );
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

    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_by_ref_returns_fresh_value_after_invalidate() {
        let cache: Cache<String, String> = CacheBuilder::new(128).build();
        let counter = Arc::new(AtomicI32::new(0));
        let moved_counter = counter.clone();
        let init: Initializer<String, String> = Arc::new(move |_| {
            let counter = moved_counter.clone();
            Box::pin(async move {
                let n = counter.fetch_add(1, Ordering::Relaxed) + 1;
                sleep(Duration::from_millis(100)).await;
                Ok(Some(format!("v{n}")))
            })
        });
        let invalidator: Invalidator<String, String, String> = Box::new(|cache, keys| {
            Box::pin(async move {
                for key in keys {
                    cache.invalidate(*key).await;
                }
                Ok(())
            })
        });

        let adv_cache = Arc::new(CacheContainer::with_strategy(
            "test".to_string(),
            cache,
            invalidator,
            init,
            always_true_filter,
            InitStrategy::VersionChecked,
        ));

        let moved_cache = adv_cache.clone();
        let get_task = tokio::spawn(async move { moved_cache.get_by_ref("foo").await });

        sleep(Duration::from_millis(50)).await;
        adv_cache.invalidate(&["foo".to_string()]).await.unwrap();

        let value = get_task.await.unwrap().unwrap().unwrap();
        assert_eq!(value, "v2");
        assert_eq!(counter.load(Ordering::Relaxed), 2);
    }
}
