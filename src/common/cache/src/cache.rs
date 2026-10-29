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

use crate::metrics::{CacheMetrics, CacheSizeMetric, CacheSizeRegistration};

/// Reason for releasing an entry from the cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemovalCause {
    /// The entry reached its expiration time.
    Expired,
    /// The entry was explicitly invalidated.
    Explicit,
    /// A new value replaced the entry.
    Replaced,
    /// The entry was removed to satisfy the capacity policy.
    Size,
}

impl RemovalCause {
    /// Returns the established eviction metric label.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Expired => "expired",
            Self::Explicit => "explicit",
            Self::Replaced => "replaced",
            Self::Size => "size",
        }
    }
}

impl From<moka::notification::RemovalCause> for RemovalCause {
    fn from(value: moka::notification::RemovalCause) -> Self {
        match value {
            moka::notification::RemovalCause::Expired => Self::Expired,
            moka::notification::RemovalCause::Explicit => Self::Explicit,
            moka::notification::RemovalCause::Replaced => Self::Replaced,
            moka::notification::RemovalCause::Size => Self::Size,
        }
    }
}

/// A weighted memory cache with backend-provided asynchronous load deduplication.
/// Values should be inexpensive to clone, such as `Arc<T>` or `Bytes`.
/// Capacity is maintained asynchronously; insertion does not guarantee retention.
#[derive(Clone)]
pub struct Cache<K, V> {
    inner: moka::future::Cache<K, V>,
    capacity: u64,
    size_registration: Option<CacheSizeRegistration>,
    metrics: Option<Arc<CacheMetrics>>,
}

impl<K, V> Cache<K, V>
where
    K: Eq + Hash + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    /// Creates a cache with the caller's weight and eviction accounting.
    pub fn new<W, L>(name: &str, capacity: u64, weigher: W, listener: L) -> Self
    where
        W: Fn(&K, &V) -> u32 + Send + Sync + 'static,
        L: Fn(Arc<K>, V, RemovalCause) + Send + Sync + 'static,
    {
        Self {
            inner: moka::future::Cache::builder()
                .name(name)
                .max_capacity(capacity)
                .support_invalidation_closures()
                .weigher(weigher)
                .eviction_listener(move |key, value, cause| listener(key, value, cause.into()))
                .build(),
            capacity,
            size_registration: None,
            metrics: None,
        }
    }

    /// Attaches caller-bound metrics without changing existing hit/miss accounting.
    pub fn with_metrics(mut self, metrics: Arc<CacheMetrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Samples this cache's retained weight when the caller collects byte metrics.
    pub fn with_size_metric(mut self, metric: CacheSizeMetric) -> Self {
        let cache = self.inner.clone();
        self.size_registration = Some(metric.register(move || cache.weighted_size()));
        self
    }

    /// Gets a completed value without starting a load.
    pub fn get<'a, Q>(&'a self, key: &'a Q) -> impl Future<Output = Option<V>> + Send + 'a
    where
        K: Borrow<Q>,
        Q: Hash + Eq + Sync + ?Sized,
    {
        self.inner.get(key)
    }

    /// Shares a successful load among concurrent callers with the same key and error type.
    /// Errors are shared with current waiters but are not retained as cached values.
    /// Cancelling the active loader lets a remaining caller execute its own loader.
    pub async fn try_get_with<Q, F, E>(&self, key: &Q, load: F) -> Result<V, Arc<E>>
    where
        K: Borrow<Q>,
        Q: ToOwned<Owned = K> + Hash + Eq + ?Sized,
        F: Future<Output = Result<V, E>>,
        E: Send + Sync + 'static,
    {
        let request = self
            .metrics
            .as_ref()
            .map(|metrics| metrics.requests.start());
        let result = self
            .inner
            .try_get_with_by_ref(key, async {
                // Only the backend-selected loader contributes physical load metrics.
                let operation = self.metrics.as_ref().map(|metrics| metrics.loads.start());
                let result = load.await;
                if let Some(operation) = operation {
                    operation.finish(&result);
                }
                result
            })
            .await;
        if let Some(request) = request {
            request.finish(&result);
        }
        result
    }

    /// Submits a completed value to the cache's admission and eviction policy.
    pub async fn insert(&self, key: K, value: V) {
        self.inner.insert(key, value).await;
    }

    /// Invalidates a completed entry. This does not cancel a concurrent loader.
    pub async fn invalidate<Q>(&self, key: &Q)
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.inner.invalidate(key).await;
    }

    /// Invalidates completed entries matching a predicate. Pending loaders are not fenced.
    /// Physical removal and eviction accounting run asynchronously.
    pub fn invalidate_if<F>(&self, predicate: F) -> std::io::Result<()>
    where
        F: Fn(&K, &V) -> bool + Send + Sync + 'static,
    {
        self.inner
            .invalidate_entries_if(predicate)
            .map(|_| ())
            .map_err(std::io::Error::other)
    }

    /// Returns the configured weight capacity.
    pub fn capacity(&self) -> u64 {
        self.capacity
    }

    /// Returns the approximate number of retained entries.
    pub fn entry_count(&self) -> u64 {
        self.inner.entry_count()
    }

    /// Returns the approximate total retained weight.
    pub fn weighted_size(&self) -> u64 {
        self.inner.weighted_size()
    }

    /// Applies pending admission, eviction, and accounting work.
    pub async fn run_pending_tasks(&self) {
        self.inner.run_pending_tasks().await;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use tokio::sync::Notify;

    use super::*;

    // This is also a compile-time contract for generic callers crossing async-trait boundaries.
    fn send_lookup<'a, K, V, Q>(
        cache: &'a Cache<K, V>,
        key: &'a Q,
    ) -> impl Future<Output = Option<V>> + Send + 'a
    where
        K: Borrow<Q> + Eq + Hash + Send + Sync + 'static,
        V: Clone + Send + Sync + 'static,
        Q: Hash + Eq + Sync + ?Sized,
    {
        cache.get(key)
    }

    #[tokio::test]
    async fn test_borrowed_lookup_has_a_send_future() {
        let cache = cache(1024);
        cache
            .insert("key".to_string(), Arc::new("value".to_string()))
            .await;
        let future: futures::future::BoxFuture<'_, Option<Arc<String>>> =
            Box::pin(send_lookup(&cache, "key"));
        assert_eq!(future.await.as_deref().map(String::as_str), Some("value"));
    }

    #[tokio::test]
    async fn test_predicate_invalidation_preserves_other_entries() {
        let cache = cache(1024);
        cache
            .insert("old".to_string(), Arc::new("a".to_string()))
            .await;
        cache
            .insert("keep".to_string(), Arc::new("b".to_string()))
            .await;
        cache.run_pending_tasks().await;
        cache.invalidate_if(|key, _| key == "old").unwrap();
        assert!(cache.get("old").await.is_none());
        assert_eq!(
            cache.get("keep").await.as_deref().map(String::as_str),
            Some("b")
        );
    }

    fn cache(capacity: u64) -> Cache<String, Arc<String>> {
        Cache::new(
            "test",
            capacity,
            |_, value: &Arc<String>| value.len() as u32,
            |_, _, _| {},
        )
    }

    fn operation_metrics(name: &str) -> crate::metrics::OperationMetrics {
        use prometheus::{Histogram, HistogramOpts, IntCounter, IntGauge};

        crate::metrics::OperationMetrics {
            started: IntCounter::new(format!("{name}_started"), "test").unwrap(),
            succeeded: IntCounter::new(format!("{name}_succeeded"), "test").unwrap(),
            failed: IntCounter::new(format!("{name}_failed"), "test").unwrap(),
            cancelled: IntCounter::new(format!("{name}_cancelled"), "test").unwrap(),
            active: IntGauge::new(format!("{name}_active"), "test").unwrap(),
            elapsed: Histogram::with_opts(HistogramOpts::new(format!("{name}_seconds"), "test"))
                .unwrap(),
        }
    }

    fn instrumented_cache() -> (Cache<String, Arc<String>>, Arc<CacheMetrics>) {
        let metrics = Arc::new(CacheMetrics {
            requests: operation_metrics("requests"),
            loads: operation_metrics("loads"),
        });
        (cache(1024).with_metrics(metrics.clone()), metrics)
    }

    #[tokio::test]
    async fn test_metrics_distinguish_requests_from_executed_loads() {
        let (cache, metrics) = instrumented_cache();
        // Merely constructing an async call does not start an operation.
        drop(cache.try_get_with("unused", async { Ok::<_, ()>(Arc::new(String::new())) }));
        assert_eq!(0, metrics.requests.started.get());
        let gate = Notify::new();
        let mut first = Box::pin(cache.try_get_with("key", async {
            gate.notified().await;
            Ok::<_, ()>(Arc::new("value".to_string()))
        }));
        let mut second = Box::pin(cache.try_get_with("key", async { Err::<Arc<String>, _>(()) }));
        assert!(futures::poll!(&mut first).is_pending());
        assert!(futures::poll!(&mut second).is_pending());
        assert_eq!(2, metrics.requests.active.get());
        assert_eq!(1, metrics.loads.active.get());
        gate.notify_waiters();
        let (first, second) = tokio::join!(first, second);
        assert!(Arc::ptr_eq(&first.unwrap(), &second.unwrap()));
        cache
            .try_get_with("key", async { Err::<Arc<String>, _>(()) })
            .await
            .unwrap();
        assert_eq!(3, metrics.requests.started.get());
        assert_eq!(3, metrics.requests.succeeded.get());
        assert_eq!(1, metrics.loads.started.get());
        assert_eq!(1, metrics.loads.succeeded.get());
        assert_eq!(0, metrics.requests.active.get());
        assert_eq!(0, metrics.loads.active.get());
        assert_eq!(3, metrics.requests.elapsed.get_sample_count());
        assert_eq!(1, metrics.loads.elapsed.get_sample_count());
        assert_eq!(0, metrics.requests.failed.get());
        assert_eq!(0, metrics.requests.cancelled.get());
    }

    #[tokio::test]
    async fn test_metrics_release_active_work_on_cancellation_and_error() {
        let (cache, metrics) = instrumented_cache();
        let mut producer =
            Box::pin(cache.try_get_with("key", std::future::pending::<Result<Arc<String>, ()>>()));
        let mut waiter =
            Box::pin(cache.try_get_with("key", std::future::pending::<Result<Arc<String>, ()>>()));
        assert!(futures::poll!(&mut producer).is_pending());
        assert!(futures::poll!(&mut waiter).is_pending());
        drop(waiter);
        assert_eq!(1, metrics.requests.cancelled.get());
        assert_eq!(0, metrics.loads.cancelled.get());
        drop(producer);
        assert_eq!(2, metrics.requests.cancelled.get());
        assert_eq!(1, metrics.loads.cancelled.get());
        let result = cache
            .try_get_with("key", async { Err::<Arc<String>, _>(()) })
            .await;
        assert!(result.is_err());
        assert_eq!(1, metrics.requests.failed.get());
        assert_eq!(1, metrics.loads.failed.get());
        assert_eq!(0, metrics.requests.active.get());
        assert_eq!(0, metrics.loads.active.get());
        assert_eq!(3, metrics.requests.elapsed.get_sample_count());
        assert_eq!(2, metrics.loads.elapsed.get_sample_count());
    }

    #[tokio::test]
    async fn test_instrumented_loading_future_is_send() {
        fn assert_send<T: Send>(value: T) -> T {
            value
        }

        let (cache, _) = instrumented_cache();
        assert_send(
            cache.try_get_with("key", async { Ok::<_, ()>(Arc::new("value".to_string())) }),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_shares_load_and_bypasses_loader_on_hit() {
        let cache = cache(1024);
        let calls = AtomicUsize::new(0);
        let gate = Notify::new();
        let load = || async {
            calls.fetch_add(1, Ordering::Relaxed);
            gate.notified().await;
            Ok::<_, ()>(Arc::new("value".to_string()))
        };
        let mut first = Box::pin(cache.try_get_with("key", load()));
        let mut second = Box::pin(cache.try_get_with("key", load()));
        assert!(futures::poll!(&mut first).is_pending());
        assert!(futures::poll!(&mut second).is_pending());
        assert_eq!(1, calls.load(Ordering::Relaxed));
        gate.notify_waiters();
        let (first, second) = tokio::join!(first, second);
        let first = first.unwrap();
        assert!(Arc::ptr_eq(&first, &second.unwrap()));
        let hit = cache
            .try_get_with("key", async { Err::<Arc<String>, _>(()) })
            .await
            .unwrap();
        assert!(Arc::ptr_eq(&first, &hit));
    }

    #[tokio::test]
    async fn test_remaining_waiter_loads_after_producer_cancellation() {
        let (cache, metrics) = instrumented_cache();
        let mut first =
            Box::pin(cache.try_get_with("key", std::future::pending::<Result<Arc<String>, ()>>()));
        assert!(futures::poll!(&mut first).is_pending());
        let mut second = Box::pin(
            cache.try_get_with("key", async { Ok::<_, ()>(Arc::new("retry".to_string())) }),
        );
        assert!(futures::poll!(&mut second).is_pending());
        drop(first);
        assert_eq!("retry", second.await.unwrap().as_str());
        assert_eq!(2, metrics.requests.started.get());
        assert_eq!(1, metrics.requests.cancelled.get());
        assert_eq!(1, metrics.requests.succeeded.get());
        assert_eq!(2, metrics.loads.started.get());
        assert_eq!(1, metrics.loads.cancelled.get());
        assert_eq!(1, metrics.loads.succeeded.get());
        assert_eq!(0, metrics.requests.active.get());
        assert_eq!(0, metrics.loads.active.get());
    }

    #[tokio::test]
    async fn test_errors_are_shared_but_not_cached() {
        let (cache, metrics) = instrumented_cache();
        let calls = AtomicUsize::new(0);
        let gate = Notify::new();
        let load = || async {
            calls.fetch_add(1, Ordering::Relaxed);
            gate.notified().await;
            Err::<Arc<String>, _>("read failed")
        };
        let mut first = Box::pin(cache.try_get_with("key", load()));
        let mut second = Box::pin(cache.try_get_with("key", load()));
        assert!(futures::poll!(&mut first).is_pending());
        assert!(futures::poll!(&mut second).is_pending());
        gate.notify_waiters();
        let (first, second) = tokio::join!(first, second);
        assert!(Arc::ptr_eq(&first.unwrap_err(), &second.unwrap_err()));
        assert_eq!(2, metrics.requests.failed.get());
        assert_eq!(1, metrics.loads.failed.get());
        assert_eq!(0, metrics.requests.active.get());
        assert_eq!(0, metrics.loads.active.get());
        assert_eq!(1, calls.load(Ordering::Relaxed));
        assert!(cache.get("key").await.is_none());
        assert_eq!(
            "retry",
            cache
                .try_get_with("key", async {
                    Ok::<_, &'static str>(Arc::new("retry".to_string()))
                })
                .await
                .unwrap()
                .as_str()
        );
    }

    #[tokio::test]
    async fn test_sampled_size_sums_instances_and_preserves_manual_bytes() {
        use crate::metrics::CacheSizeMetrics;
        use prometheus::Opts;
        use prometheus::core::Collector;

        let metrics =
            CacheSizeMetrics::new(Opts::new("test_cache_bytes", "test"), &["type"]).unwrap();
        let manual = metrics.with_label_values(&["shared"]);
        manual.set(3);
        let make_cache = || {
            Cache::new("sampled", 100, |_: &u64, _: &Arc<String>| 7, |_, _, _| {})
                .with_size_metric(metrics.bind(&["shared"]))
        };
        let a = make_cache();
        let b = make_cache();
        let sample = || {
            metrics.collect()[0].get_metric()[0]
                .get_gauge()
                .as_ref()
                .map_or(0.0, |gauge| gauge.value())
        };
        a.insert(1, Arc::new("one".into())).await;
        b.insert(2, Arc::new("two".into())).await;
        a.run_pending_tasks().await;
        b.run_pending_tasks().await;
        assert_eq!(17.0, sample());
        assert_eq!(3, manual.get());
        let consumer = a.get(&1).await.unwrap();
        a.invalidate(&1).await;
        a.run_pending_tasks().await;
        assert_eq!(10.0, sample());
        drop(consumer);
        let cloned = b.clone();
        drop(b);
        assert_eq!(10.0, sample());
        drop(cloned);
        assert_eq!(3.0, sample());
    }
}
