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

use common_cache::batch;
use futures::future::{BoxFuture, try_join_all};
use moka::future::Cache;
use moka::ops::compute::Op;
use snafu::{OptionExt, ResultExt};

use crate::cache_invalidator::{CacheInvalidator, Context};
use crate::error::{self, Error, Result};
use crate::instruction::CacheIdent;
use crate::metrics;

/// Initializes value (i.e., fetches from remote).
pub type Initializer<K, V> = Arc<dyn Fn(&'_ K) -> BoxFuture<'_, Result<Option<V>>> + Send + Sync>;

/// Loads missing keys in one backend request. Absent keys are omitted from the result.
pub type BatchInitializer<K, V> =
    Arc<dyn Fn(Vec<K>) -> BoxFuture<'static, Result<HashMap<K, V>>> + Send + Sync>;

pub(crate) type EventHandler<K, V, T> =
    Box<dyn for<'a> Fn(CacheEntries<'a, K, V>, &'a [T]) -> BoxFuture<'a, ()> + Send + Sync>;

pub(crate) enum Invalidation<K, V, T> {
    Key(fn(&T) -> Option<&K>),
    Events(EventHandler<K, V, T>),
}

/// Event handlers use the same epoch and per-key compute lock as cache loads.
pub(crate) struct CacheEntries<'a, K, V> {
    cache: &'a Cache<(usize, K), V>,
    epoch: &'a AtomicUsize,
}

impl<K, V> CacheEntries<'_, K, V>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    pub(crate) async fn compute(&self, key: &K, update: impl FnOnce(Option<V>) -> Op<V>) {
        let key = (self.epoch.load(Ordering::Acquire), key.clone());
        self.cache
            .entry(key.clone())
            .and_compute_with(|entry| async move {
                match update(entry.map(|entry| entry.into_value())) {
                    Op::Put(value) => self.cache.insert(key, value).await,
                    Op::Remove => self.cache.invalidate(&key).await,
                    Op::Nop => {}
                }
                Op::Nop
            })
            .await;
    }

    pub(crate) fn invalidate_all(&self) {
        self.epoch.fetch_add(1, Ordering::AcqRel);
        self.cache.invalidate_all();
    }
}

/// Caches metadata and coalesces backend reads for concurrent callers.
pub struct CacheContainer<K, V, CacheToken> {
    name: String,
    cache: Cache<(usize, K), V>,
    invalidation: Invalidation<K, V, CacheToken>,
    initializer: Initializer<K, V>,
    batch_initializer: Option<BatchInitializer<K, V>>,
    epoch: AtomicUsize,
}

impl<K, V, CacheToken> CacheContainer<K, V, CacheToken>
where
    K: Clone + Hash + Eq + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
    CacheToken: Send + Sync,
{
    /// Coordinates misses and targeted invalidation through Moka's per-key compute lock.
    /// Invalidation can wait for a backend read. An overlapping reader may return its
    /// old result, but a read started after invalidation returns cannot hit that result.
    /// A synchronous full clear advances the epoch, isolating any late publication.
    pub fn new(
        name: String,
        cache: Cache<(usize, K), V>,
        initializer: Initializer<K, V>,
        key: fn(&CacheToken) -> Option<&K>,
    ) -> Self {
        Self::with_invalidation(name, cache, initializer, Invalidation::Key(key))
    }

    pub(crate) fn with_invalidation(
        name: String,
        cache: Cache<(usize, K), V>,
        initializer: Initializer<K, V>,
        invalidation: Invalidation<K, V, CacheToken>,
    ) -> Self {
        Self {
            name,
            cache,
            invalidation,
            initializer,
            batch_initializer: None,
            epoch: AtomicUsize::default(),
        }
    }

    /// Uses the supplied loader for [`get_many`](Self::get_many).
    pub fn with_batch_initializer(mut self, initializer: BatchInitializer<K, V>) -> Self {
        self.batch_initializer = Some(initializer);
        self
    }

    /// Returns the `name`.
    pub fn name(&self) -> &str {
        &self.name
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

#[async_trait::async_trait]
impl<K, V> CacheInvalidator for CacheContainer<K, V, CacheIdent>
where
    K: Clone + Hash + Eq + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    async fn invalidate(&self, _ctx: &Context, caches: &[CacheIdent]) -> Result<()> {
        self.invalidate(caches).await
    }

    fn invalidate_all(&self) -> Result<()> {
        self.entries().invalidate_all();
        Ok(())
    }
}

impl<K, V, CacheToken> CacheContainer<K, V, CacheToken>
where
    K: Clone + Hash + Eq + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
    CacheToken: Send + Sync,
{
    fn entries(&self) -> CacheEntries<'_, K, V> {
        CacheEntries {
            cache: &self.cache,
            epoch: &self.epoch,
        }
    }

    /// Applies matching invalidations, waiting for active loads of the affected keys.
    pub async fn invalidate(&self, tokens: &[CacheToken]) -> Result<()> {
        let entries = self.entries();
        match &self.invalidation {
            Invalidation::Key(key_of) => {
                for key in tokens.iter().filter_map(key_of) {
                    entries.compute(key, |_| Op::Remove).await;
                }
            }
            Invalidation::Events(handler) => handler(entries, tokens).await,
        }
        Ok(())
    }

    /// Tests membership in the current cache epoch.
    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: ToOwned<Owned = K> + Hash + Eq + ?Sized,
    {
        self.cache
            .contains_key(&(self.epoch.load(Ordering::Acquire), key.to_owned()))
    }

    /// Returns the cached value or initializes it.
    pub async fn get(&self, key: K) -> Result<Option<V>> {
        self.get_by_ref(&key).await
    }

    /// Looks up a borrowed key with the same loading semantics as [Self::get].
    pub async fn get_by_ref<Q>(&self, key: &Q) -> Result<Option<V>>
    where
        K: Borrow<Q>,
        Q: ToOwned<Owned = K> + Hash + Eq + ?Sized,
    {
        self.get_by_ref_with(key, self.initializer.clone()).await
    }

    async fn cached<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: ToOwned<Owned = K> + Hash + Eq + ?Sized,
    {
        self.cache
            .get(&(self.epoch.load(Ordering::Acquire), key.to_owned()))
            .await
    }

    async fn get_by_ref_with<Q>(&self, key: &Q, initializer: Initializer<K, V>) -> Result<Option<V>>
    where
        K: Borrow<Q>,
        Q: ToOwned<Owned = K> + Hash + Eq + ?Sized,
    {
        metrics::CACHE_CONTAINER_CACHE_GET
            .with_label_values(&[&self.name])
            .inc();
        let cache = &self.cache;
        let storage_key = (self.epoch.load(Ordering::Acquire), key.to_owned());
        if let Some(value) = cache.get(&storage_key).await {
            return Ok(Some(value));
        }
        let load = init(initializer, key.to_owned(), &self.name);
        let mut loaded = None;
        let output = &mut loaded;
        let result = cache
            .entry(storage_key.clone())
            .and_try_compute_with(|entry| async move {
                if entry.is_none() {
                    let value = load.await?;
                    // Moka 0.12.10 releases the compute lock before applying Op::Put
                    // or Op::Remove. Publish here, while removals still wait.
                    cache.insert(storage_key, value.clone()).await;
                    *output = Some(value);
                }
                Ok(Op::Nop)
            })
            .await;
        match result {
            Ok(result) => Ok(loaded.or_else(|| result.into_entry().map(|e| e.into_value()))),
            Err(Error::ValueNotExist { .. }) => Ok(None),
            Err(err) => Err(Arc::new(err)).context(error::GetCacheSnafu),
        }
    }

    /// Returns values in input order, preserving duplicates and missing keys.
    ///
    /// A key omitted by the loader returns `None` and is not cached, so a
    /// subsequent lookup loads it again. Backend errors are propagated.
    /// Cache hits do not reach the batch loader. Misses use the same Moka
    /// initialization entries as single-key reads. Only the keys whose loaders
    /// this call owns are submitted to its batch loader. Results do not form a multi-key
    /// snapshot. A waiter may retry a failed or missing load.
    pub async fn get_many(&self, keys: &[K]) -> Result<Vec<Option<V>>> {
        let Some(batch_initializer) = &self.batch_initializer else {
            return try_join_all(keys.iter().map(|key| self.get_by_ref(key))).await;
        };
        let mut values = Vec::with_capacity(keys.len());
        let mut misses = HashMap::new();
        let mut duplicates = Vec::new();
        for (index, key) in keys.iter().enumerate() {
            let value = self.cached(key).await;
            if value.is_none() {
                let first = *misses.entry(key).or_insert(index);
                if first != index {
                    duplicates.push((index, first));
                }
            }
            values.push(value);
        }
        metrics::CACHE_CONTAINER_CACHE_GET
            .with_label_values(&[&self.name])
            .inc_by((keys.len() - misses.len() - duplicates.len()) as u64);
        if misses.is_empty() {
            return Ok(values);
        }

        let loads = |requests| {
            let initializer = Self::queued_initializer(requests);
            misses.into_values().map(move |index| {
                let read = self.get_by_ref_with(&keys[index], initializer.clone());
                async move { read.await.map(|value| (index, value)) }
            })
        };
        batch::load(
            loads,
            |keys, replies| async move {
                let values = batch_initializer(keys)
                    .await
                    .map(Arc::new)
                    .map_err(Arc::new);
                for reply in replies {
                    let _ = reply.send(values.clone());
                }
            },
            |(index, value)| values[index] = value,
        )
        .await?;
        for (index, first) in duplicates {
            values[index] = values[first].clone();
        }
        Ok(values)
    }

    fn queued_initializer(requests: batch::BatchSender<K, BatchResult<K, V>>) -> Initializer<K, V> {
        Arc::new(move |key| {
            let requests = requests.clone();
            Box::pin(async move {
                let values =
                    requests
                        .request(key.clone())
                        .await
                        .context(error::UnexpectedSnafu {
                            err_msg: "Cache batch reply dropped",
                        })?;
                Ok(values.context(error::GetCacheSnafu)?.get(key).cloned())
            })
        })
    }
}

type BatchResult<K, V> = std::result::Result<Arc<HashMap<K, V>>, Arc<Error>>;

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicI32, Ordering};

    use super::*;

    use std::time::Duration;

    fn batch_test_cache(
        initializer: Initializer<u32, u32>,
    ) -> CacheContainer<u32, u32, CacheIdent> {
        CacheContainer::new(
            "batch_test".into(),
            Cache::new(64),
            initializer,
            CacheIdent::table_id_key,
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
        assert_eq!(cache.get_many(&[1]).await.unwrap(), [Some(10)]);
        batches.lock().unwrap().clear();
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
    async fn test_get_many_without_batch_initializer() {
        let cache = batch_test_cache(Arc::new(|&key| {
            Box::pin(async move { Ok((key != 3).then_some(key * 10)) })
        }));
        assert_eq!(cache.get(1).await.unwrap(), Some(10));
        assert_eq!(
            cache.get_many(&[2, 1, 3, 2]).await.unwrap(),
            [Some(20), Some(10), None, Some(20)]
        );
    }

    type BatchGate = (Vec<u32>, tokio::sync::oneshot::Sender<Result<()>>);

    fn gated_batch_cache() -> (
        CacheContainer<u32, u32, CacheIdent>,
        tokio::sync::mpsc::UnboundedReceiver<BatchGate>,
    ) {
        let (observed, batches) = tokio::sync::mpsc::unbounded_channel();
        let init: Initializer<u32, u32> =
            Arc::new(|_| Box::pin(async { panic!("unexpected single load") }));
        let cache = batch_test_cache(init).with_batch_initializer(Arc::new(move |mut keys| {
            let (resume, gate) = tokio::sync::oneshot::channel();
            keys.sort_unstable();
            observed.send((keys.clone(), resume)).unwrap();
            Box::pin(async move {
                gate.await.unwrap()?;
                Ok(keys.into_iter().map(|key| (key, key)).collect())
            })
        }));
        (cache, batches)
    }

    #[tokio::test]
    async fn test_overlapping_batches_load_each_key_once() {
        let (cache, mut batches) = gated_batch_cache();
        let mut first = Box::pin(cache.get_many(&[1, 2]));
        assert!(futures::poll!(&mut first).is_pending());
        let (keys, resume_first) = batches.try_recv().unwrap();
        assert_eq!(keys, [1, 2]);

        let mut second = Box::pin(cache.get_many(&[2, 3]));
        assert!(futures::poll!(&mut second).is_pending());
        let (keys, resume_second) = batches.try_recv().unwrap();
        assert_eq!(keys, [3]);

        resume_first.send(Ok(())).unwrap();
        resume_second.send(Ok(())).unwrap();
        let (first, second) = futures::try_join!(first, second).unwrap();
        assert_eq!(first, [Some(1), Some(2)]);
        assert_eq!(second, [Some(2), Some(3)]);
    }

    #[tokio::test]
    async fn test_batch_waiter_promoted_after_cancellation() {
        let (cache, mut batches) = gated_batch_cache();
        let mut owner = Box::pin(cache.get_many(&[1]));
        assert!(futures::poll!(&mut owner).is_pending());
        let (keys, cancelled) = batches.try_recv().unwrap();
        assert_eq!(keys, [1]);

        let mut waiter = Box::pin(cache.get_many(&[1, 2]));
        assert!(futures::poll!(&mut waiter).is_pending());
        let (keys, resume) = batches.try_recv().unwrap();
        assert_eq!(keys, [2]);
        drop(owner);
        assert!(cancelled.send(Ok(())).is_err());
        resume.send(Ok(())).unwrap();

        let promoted = async {
            let (keys, resume) = batches.recv().await.unwrap();
            assert_eq!(keys, [1]);
            resume.send(Ok(())).unwrap();
        };
        let (result, ()) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(waiter, promoted)
        })
        .await
        .unwrap();
        assert_eq!(result.unwrap(), [Some(1), Some(2)]);
    }

    #[tokio::test]
    async fn test_waiter_reloads_after_owner_error() {
        let (cache, mut batches) = gated_batch_cache();
        let mut owner = Box::pin(cache.get_many(&[1]));
        assert!(futures::poll!(&mut owner).is_pending());
        let (_, fail) = batches.try_recv().unwrap();
        let mut waiter = Box::pin(cache.get_many(&[1]));
        assert!(futures::poll!(&mut waiter).is_pending());
        assert!(batches.try_recv().is_err());

        fail.send(error::GetLatestCacheRetryExceededSnafu { attempts: 1usize }.fail())
            .unwrap();
        assert!(owner.await.is_err());
        assert!(futures::poll!(&mut waiter).is_pending());
        let (keys, resume) = batches.try_recv().unwrap();
        assert_eq!(keys, [1]);
        resume.send(Ok(())).unwrap();
        assert_eq!(waiter.await.unwrap(), [Some(1)]);
    }

    #[tokio::test]
    async fn test_large_cold_batch_loads_ready_keys_together() {
        let (cache, mut batches) = gated_batch_cache();
        let keys: Vec<_> = (0..128).collect();
        let mut read = Box::pin(cache.get_many(&keys));
        assert!(futures::poll!(&mut read).is_pending());
        let (loaded, resume) = batches.try_recv().unwrap();
        assert_eq!(loaded, keys);
        resume.send(Ok(())).unwrap();
        assert_eq!(
            read.await.unwrap(),
            keys.into_iter().map(Some).collect::<Vec<_>>()
        );
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
            .with_batch_initializer(Arc::new(|keys| {
                assert_eq!(keys, [2]);
                Box::pin(async { Ok(HashMap::from([(2, 20)])) })
            })),
        );
        let single = tokio::spawn({
            let cache = cache.clone();
            async move { cache.get(1).await }
        });
        started.notified().await;
        let mut batch = Box::pin(cache.get_many(&[1, 2]));
        assert!(futures::poll!(&mut batch).is_pending());
        resume.notify_one();
        assert_eq!(batch.await.unwrap(), vec![Some(10), Some(20)]);
        assert_eq!(single.await.unwrap().unwrap(), Some(10));
    }

    #[tokio::test]
    async fn test_get_many_reloads_after_backend_error() {
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

    #[tokio::test]
    async fn test_get() {
        let cache = Cache::new(128);
        let counter = Arc::new(AtomicI32::new(0));
        let moved_counter = counter.clone();
        let init: Initializer<NameKey, String> = Arc::new(move |_| {
            moved_counter.fetch_add(1, Ordering::Relaxed);
            Box::pin(async { Ok(Some("hi".to_string())) })
        });
        let cache = CacheContainer::new("test".to_string(), cache, init, |_: &String| None);
        let key = NameKey { name: "key" };
        let value = cache.get(key).await.unwrap().unwrap();
        assert_eq!(value, "hi");
        assert_eq!(counter.load(Ordering::Relaxed), 1);
        let key = NameKey { name: "key" };
        let value = cache.get(key).await.unwrap().unwrap();
        assert_eq!(value, "hi");
        assert_eq!(counter.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn test_get_by_ref() {
        let cache = Cache::new(128);
        let counter = Arc::new(AtomicI32::new(0));
        let moved_counter = counter.clone();
        let init: Initializer<String, String> = Arc::new(move |_| {
            moved_counter.fetch_add(1, Ordering::Relaxed);
            Box::pin(async { Ok(Some("hi".to_string())) })
        });
        let cache = CacheContainer::new("test".to_string(), cache, init, |_: &String| None);
        let value = cache.get_by_ref("foo").await.unwrap().unwrap();
        assert_eq!(value, "hi");
        let value = cache.get_by_ref("foo").await.unwrap().unwrap();
        assert_eq!(value, "hi");
        assert_eq!(counter.load(Ordering::Relaxed), 1);
        let value = cache.get_by_ref("bar").await.unwrap().unwrap();
        assert_eq!(value, "hi");
        assert_eq!(counter.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn test_get_value_not_exits() {
        let cache = Cache::new(128);
        let init: Initializer<String, String> =
            Arc::new(move |_| Box::pin(async { error::ValueNotExistSnafu {}.fail() }));
        let cache = CacheContainer::new("test".to_string(), cache, init, |_: &String| None);
        let value = cache.get_by_ref("foo").await.unwrap();
        assert!(value.is_none());
    }

    #[tokio::test]
    async fn test_invalidate_with_borrowed_keys() {
        let foo = String::from("foo");
        let bar = String::from("bar");
        let cache = Cache::new(128);
        let counter = Arc::new(AtomicI32::new(0));
        let moved_counter = counter.clone();
        let init: Initializer<String, String> = Arc::new(move |_| {
            moved_counter.fetch_add(1, Ordering::Relaxed);
            Box::pin(async { Ok(Some("hi".to_string())) })
        });
        let cache =
            CacheContainer::new("test".to_string(), cache, init, |key: &&String| Some(*key));
        let value = cache.get_by_ref("foo").await.unwrap().unwrap();
        assert_eq!(value, "hi");
        let value = cache.get_by_ref("foo").await.unwrap().unwrap();
        assert_eq!(value, "hi");
        assert_eq!(counter.load(Ordering::Relaxed), 1);
        cache.invalidate(&[&foo, &bar]).await.unwrap();
        let value = cache.get_by_ref("foo").await.unwrap().unwrap();
        assert_eq!(value, "hi");
        assert_eq!(counter.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn removal_waits_for_publication() {
        let (cache, mut batches) = gated_batch_cache();
        let mut reader = Box::pin(cache.get_many(&[1]));
        assert!(futures::poll!(&mut reader).is_pending());
        let (_, resume) = batches.try_recv().unwrap();

        cache.invalidate(&[CacheIdent::TableId(2)]).await.unwrap();
        let tokens = [CacheIdent::TableId(1)];
        let mut removal = Box::pin(cache.invalidate(&tokens));
        assert!(futures::poll!(&mut removal).is_pending());
        resume.send(Ok(())).unwrap();
        assert_eq!(reader.await.unwrap(), [Some(1)]);
        removal.await.unwrap();
        assert!(!cache.contains_key(&1));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn invalidation_during_publication_removes_old_value() {
        use std::sync::{Mutex, mpsc};

        struct Value {
            id: i32,
            in_batch_reply: bool,
            before_publish: Arc<Mutex<Option<mpsc::Sender<()>>>>,
            resume: Arc<Mutex<mpsc::Receiver<()>>>,
        }
        impl Clone for Value {
            fn clone(&self) -> Self {
                // Batch replies first clone the value out of the result map.
                if !self.in_batch_reply
                    && let Some(started) = self.before_publish.lock().unwrap().take()
                {
                    started.send(()).unwrap();
                    self.resume
                        .lock()
                        .unwrap()
                        .recv_timeout(Duration::from_secs(5))
                        .unwrap();
                }
                Self {
                    id: self.id,
                    in_batch_reply: false,
                    before_publish: self.before_publish.clone(),
                    resume: self.resume.clone(),
                }
            }
        }

        for batch in [false, true] {
            let (started, publishing) = mpsc::channel();
            let (resume, gate) = mpsc::channel();
            let before_publish = Arc::new(Mutex::new(Some(started)));
            let gate = Arc::new(Mutex::new(gate));
            let backend = Arc::new(AtomicI32::new(10));
            let current = backend.clone();
            let read = move |in_batch_reply| Value {
                id: current.load(Ordering::SeqCst),
                in_batch_reply,
                before_publish: before_publish.clone(),
                resume: gate.clone(),
            };
            let single = read.clone();
            let cache = Arc::new(
                CacheContainer::new(
                    "test".into(),
                    Cache::new(8),
                    Arc::new(move |_| {
                        let value = single(false);
                        Box::pin(async move { Ok(Some(value)) })
                    }),
                    CacheIdent::table_id_key,
                )
                .with_batch_initializer(Arc::new(move |keys| {
                    let values = keys.into_iter().map(|k| (k, read(true))).collect();
                    Box::pin(async move { Ok(values) })
                })),
            );
            let reader = tokio::spawn({
                let cache = cache.clone();
                async move {
                    if batch {
                        cache.get_many(&[1]).await.unwrap()[0].as_ref().unwrap().id
                    } else {
                        cache.get(1).await.unwrap().unwrap().id
                    }
                }
            });
            publishing.recv_timeout(Duration::from_secs(5)).unwrap();
            backend.store(20, Ordering::SeqCst);
            let tokens = [CacheIdent::TableId(1)];
            let mut removal = Box::pin(cache.invalidate(&tokens));
            let result = futures::poll!(&mut removal);
            resume.send(()).unwrap();
            assert!(result.is_pending());
            removal.await.unwrap();
            assert_eq!(reader.await.unwrap(), 10);
            assert_eq!(cache.get(1).await.unwrap().unwrap().id, 20);
        }
    }

    #[tokio::test]
    async fn full_clear_isolates_an_inflight_batch_load() {
        let (cache, mut batches) = gated_batch_cache();
        let mut old = Box::pin(cache.get_many(&[1]));
        assert!(futures::poll!(&mut old).is_pending());
        let (_, resume) = batches.try_recv().unwrap();
        cache.invalidate_all().unwrap();
        resume.send(Ok(())).unwrap();
        assert_eq!(old.await.unwrap(), [Some(1)]);
        assert!(!cache.contains_key(&1));
    }

    #[tokio::test]
    async fn full_clear_isolates_an_inflight_single_load() {
        let (send, mut recv) = tokio::sync::mpsc::unbounded_channel();
        let cache = batch_test_cache(Arc::new(move |_| {
            let (resume, gate) = tokio::sync::oneshot::channel();
            send.send(resume).unwrap();
            Box::pin(async move { Ok(Some(gate.await.unwrap())) })
        }));
        let mut old = Box::pin(cache.get_by_ref(&1));
        assert!(futures::poll!(&mut old).is_pending());
        let first = recv.try_recv().unwrap();
        cache.invalidate_all().unwrap();
        let mut new = Box::pin(cache.get(1));
        assert!(futures::poll!(&mut new).is_pending());
        recv.try_recv().unwrap().send(20).unwrap();
        assert_eq!(new.await.unwrap(), Some(20));
        first.send(10).unwrap();
        assert_eq!(old.await.unwrap(), Some(10));
        assert_eq!(cache.get_many(&[1]).await.unwrap(), [Some(20)]);
    }
}
