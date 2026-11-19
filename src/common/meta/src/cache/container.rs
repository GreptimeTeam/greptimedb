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
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use backon::{BackoffBuilder, ExponentialBuilder};
use futures::FutureExt;
use futures::future::{BoxFuture, try_join_all};
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

/// Loads missing keys in one backend request. Absent keys are omitted from the result.
pub type BatchInitializer<K, V> =
    Arc<dyn Fn(Vec<K>) -> BoxFuture<'static, Result<HashMap<K, V>>> + Send + Sync>;

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
    batch_initializer: Option<BatchInitializer<K, V>>,
    token_filter: fn(&CacheToken) -> bool,
    version: Arc<AtomicUsize>,
    init_strategy: InitStrategy,
}

fn latest_get_backoff() -> impl Iterator<Item = Duration> {
    ExponentialBuilder::default()
        .with_min_delay(Duration::from_millis(10))
        .with_max_delay(Duration::from_millis(100))
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
            batch_initializer: None,
            token_filter,
            version: Arc::new(AtomicUsize::new(0)),
            init_strategy,
        }
    }

    /// Uses the supplied loader for explicit batch reads.
    pub fn with_batch_initializer(mut self, initializer: BatchInitializer<K, V>) -> Self {
        self.batch_initializer = Some(initializer);
        self
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
    K: Hash + Eq + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
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

    fn invalidate_all(&self) -> Result<()> {
        self.inc_version();
        self.cache.invalidate_all();
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
        self.get_by_ref_with(key, self.initializer.clone()).await
    }

    async fn get_by_ref_with<Q>(&self, key: &Q, initializer: Initializer<K, V>) -> Result<Option<V>>
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
                    .try_get_with_by_ref(key, init(initializer.clone(), key.to_owned(), &self.name))
                    .await
            }
            InitStrategy::VersionChecked => {
                self.cache
                    .try_get_with_by_ref(
                        key,
                        init_with_retry(
                            initializer.clone(),
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

impl<K, V, CacheToken> CacheContainer<K, V, CacheToken>
where
    K: Clone + Hash + Eq + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
    CacheToken: Send + Sync,
{
    /// Returns values in input order, preserving duplicates and missing keys.
    ///
    /// Cache hits do not reach the batch loader. Misses use the same Moka
    /// initialization entries as single-key reads. Invalidation during a batch
    /// load falls back to the normal initializer for affected pending reads.
    pub async fn get_many(&self, keys: &[K]) -> Result<Vec<Option<V>>> {
        let Some(batch_initializer) = &self.batch_initializer else {
            return try_join_all(keys.iter().map(|key| self.get_by_ref(key))).await;
        };
        let mut values = Vec::with_capacity(keys.len());
        let mut misses = HashMap::new();
        for key in keys {
            let value = self.cache.get(key).await;
            if value.is_none() {
                misses.entry(key.clone()).or_insert(());
            }
            values.push(value);
        }
        if misses.is_empty() {
            metrics::CACHE_CONTAINER_CACHE_GET
                .with_label_values(&[&self.name])
                .inc_by(keys.len() as u64);
            return Ok(values);
        }
        let batch_initializer = batch_initializer.clone();
        let version = self.version.clone();
        let batch = async move {
            let generation = version.load(Ordering::Relaxed);
            let values = batch_initializer(misses.into_keys().collect()).await;
            (generation, values.map(Arc::new).map_err(Arc::new))
        }
        .boxed()
        .shared();
        let version = self.version.clone();
        let fallback = self.initializer.clone();
        let initializer: Initializer<K, V> = Arc::new(move |key| {
            let batch = batch.clone();
            let version = version.clone();
            let fallback = fallback.clone();
            Box::pin(async move {
                let (generation, values) = batch.await;
                if generation != version.load(Ordering::Relaxed) {
                    return fallback(key).await;
                }
                Ok(values.context(error::GetCacheSnafu)?.get(key).cloned())
            })
        });
        try_join_all(keys.iter().zip(values).map(|(key, value)| {
            let initializer = initializer.clone();
            async move {
                if value.is_some() {
                    metrics::CACHE_CONTAINER_CACHE_GET
                        .with_label_values(&[&self.name])
                        .inc();
                    Ok(value)
                } else {
                    self.get_by_ref_with(key, initializer).await
                }
            }
        }))
        .await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicI32, Ordering};

    use moka::future::{Cache, CacheBuilder};

    use super::*;

    fn batch_test_cache(
        initializer: Initializer<u32, u32>,
    ) -> CacheContainer<u32, u32, CacheIdent> {
        CacheContainer::with_strategy(
            "batch_test".to_string(),
            Cache::new(64),
            Box::new(|cache, tokens| {
                Box::pin(async move {
                    for token in tokens {
                        if let CacheIdent::TableId(id) = token {
                            cache.invalidate(id).await;
                        }
                    }
                    Ok(())
                })
            }),
            initializer,
            |token| matches!(token, CacheIdent::TableId(_)),
            InitStrategy::VersionChecked,
        )
    }

    #[tokio::test]
    async fn test_get_many_hits_missing_duplicates_and_invalidation() {
        let batches = Arc::new(std::sync::Mutex::new(Vec::new()));
        let observed = batches.clone();
        let cache = batch_test_cache(Arc::new(|_| {
            Box::pin(async { panic!("unexpected single load") })
        }))
        .with_batch_initializer(Arc::new(move |mut keys| {
            let observed = observed.clone();
            Box::pin(async move {
                keys.sort_unstable();
                observed.lock().unwrap().push(keys.clone());
                Ok(keys
                    .into_iter()
                    .filter(|&k| k != 3)
                    .map(|k| (k, k * 10))
                    .collect())
            })
        }));
        cache.cache.insert(1, 10).await;
        assert_eq!(
            cache.get_many(&[2, 1, 2, 3]).await.unwrap(),
            vec![Some(20), Some(10), Some(20), None]
        );
        assert_eq!(*batches.lock().unwrap(), vec![vec![2, 3]]);
        assert_eq!(
            cache.get_many(&[2, 1]).await.unwrap(),
            vec![Some(20), Some(10)]
        );
        assert!(cache.get_many(&[]).await.unwrap().is_empty());
        assert_eq!(batches.lock().unwrap().len(), 1);
        cache.invalidate(&[CacheIdent::TableId(1)]).await.unwrap();
        assert_eq!(
            cache.get_many(&[1, 2]).await.unwrap(),
            vec![Some(10), Some(20)]
        );
        assert_eq!(*batches.lock().unwrap(), vec![vec![2, 3], vec![1]]);
        // Missing values are batch-local, so a subsequent request loads them again.
        assert_eq!(cache.get_many(&[3]).await.unwrap(), vec![None]);
        assert_eq!(batches.lock().unwrap().last().unwrap(), &vec![3]);
    }

    #[tokio::test]
    async fn test_get_many_invalidated_load_uses_current_value() {
        let started = Arc::new(tokio::sync::Notify::new());
        let resume = Arc::new(tokio::sync::Notify::new());
        let value = Arc::new(AtomicUsize::new(10));
        let current = value.clone();
        let cache = batch_test_cache(Arc::new(move |_| {
            let current = current.clone();
            Box::pin(async move { Ok(Some(current.load(Ordering::SeqCst) as u32)) })
        }))
        .with_batch_initializer(Arc::new({
            let started = started.clone();
            let resume = resume.clone();
            move |_| {
                let started = started.clone();
                let resume = resume.clone();
                Box::pin(async move {
                    started.notify_one();
                    resume.notified().await;
                    Ok(HashMap::from([(1, 10)]))
                })
            }
        }));
        let read = cache.get_many(&[1]);
        let invalidate = async {
            started.notified().await;
            value.store(20, Ordering::SeqCst);
            cache.invalidate(&[CacheIdent::TableId(1)]).await.unwrap();
            resume.notify_one();
        };
        let (result, ()) = tokio::join!(read, invalidate);
        assert_eq!(result.unwrap(), vec![Some(20)]);
        assert_eq!(cache.get(1).await.unwrap(), Some(20));
    }

    #[tokio::test]
    async fn test_get_many_coalesces_with_single_key_load() {
        let started = Arc::new(tokio::sync::Notify::new());
        let resume = Arc::new(tokio::sync::Notify::new());
        let cache = Arc::new(
            batch_test_cache(Arc::new({
                let started = started.clone();
                let resume = resume.clone();
                move |_| {
                    let started = started.clone();
                    let resume = resume.clone();
                    Box::pin(async move {
                        started.notify_one();
                        resume.notified().await;
                        Ok(Some(10))
                    })
                }
            }))
            .with_batch_initializer(Arc::new(|_| {
                Box::pin(async { panic!("single-key load owns this miss") })
            })),
        );
        let single = tokio::spawn({
            let cache = cache.clone();
            async move { cache.get(1).await }
        });
        started.notified().await;
        let mut batch = Box::pin(cache.get_many(&[1]));
        assert!(futures::poll!(&mut batch).is_pending());
        resume.notify_one();
        assert_eq!(batch.await.unwrap(), vec![Some(10)]);
        assert_eq!(single.await.unwrap().unwrap(), Some(10));
    }

    #[tokio::test]
    async fn test_get_many_retries_after_backend_error() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let loads = attempts.clone();
        let cache = batch_test_cache(Arc::new(|_| {
            Box::pin(async { panic!("unexpected single load") })
        }))
        .with_batch_initializer(Arc::new(move |keys| {
            let loads = loads.clone();
            Box::pin(async move {
                if loads.fetch_add(1, Ordering::SeqCst) == 0 {
                    return error::GetLatestCacheRetryExceededSnafu { attempts: 1usize }.fail();
                }
                Ok(keys.into_iter().map(|k| (k, k * 10)).collect())
            })
        }));
        assert!(cache.get_many(&[1, 2]).await.is_err());
        assert!(!cache.contains_key(&1));
        assert_eq!(
            cache.get_many(&[1, 2]).await.unwrap(),
            vec![Some(10), Some(20)]
        );
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_cancelled_batch_releases_single_key_waiter() {
        let started = Arc::new(tokio::sync::Notify::new());
        let cache = Arc::new(
            batch_test_cache(Arc::new(|_| Box::pin(async { Ok(Some(20)) })))
                .with_batch_initializer(Arc::new({
                    let started = started.clone();
                    move |_| {
                        let started = started.clone();
                        Box::pin(async move {
                            started.notify_one();
                            std::future::pending().await
                        })
                    }
                })),
        );
        let batch = tokio::spawn({
            let cache = cache.clone();
            async move { cache.get_many(&[1, 2]).await }
        });
        started.notified().await;
        let mut single = Box::pin(cache.get(1));
        assert!(futures::poll!(&mut single).is_pending());
        batch.abort();
        assert!(batch.await.unwrap_err().is_cancelled());
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), single)
                .await
                .unwrap()
                .unwrap(),
            Some(20)
        );
    }

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
