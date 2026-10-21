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

pub mod bloom_filter_index;
pub mod inverted_index;
pub mod result_cache;

use std::future::Future;
use std::hash::Hash;
use std::io;
use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use common_base::range_read::{Metadata, RangeReader};
use common_cache::accounting::CacheAccounting;
use common_cache::paged::PagedCache;

use crate::metrics::CACHE_BYTES;

/// Metrics for index metadata.
const INDEX_METADATA_TYPE: &str = "index_metadata";
/// Metrics for index content.
const INDEX_CONTENT_TYPE: &str = "index_content";

/// Metrics collected from IndexCache operations.
#[derive(Debug, Default, Clone)]
pub struct IndexCacheMetrics {
    /// Number of cache hits.
    pub cache_hit: usize,
    /// Number of cache misses.
    pub cache_miss: usize,
    /// Number of pages accessed.
    pub num_pages: usize,
    /// Total bytes from pages.
    pub page_bytes: u64,
}

/// Cache for index metadata and content.
pub struct IndexCache<K, M> {
    /// Cache for index metadata
    index_metadata: common_cache::Cache<K, Arc<IndexMetadata<M>>>,
    /// Cache for index content.
    index: PagedCache<K>,
    index_type: &'static str,

    /// Weighter for metadata.
    weight_of_metadata: fn(&K, &Arc<M>) -> u32,
}

/// Metadata charge is released on eviction even while readers retain the value.
struct IndexMetadata<M> {
    metadata: Arc<M>,
    accounting: CacheAccounting,
}

impl<K, M> IndexCache<K, M>
where
    K: Hash + Eq + Copy + Send + Sync + 'static,
    M: Send + Sync + 'static,
{
    pub fn new_with_weighter(
        index_metadata_cap: u64,
        index_content_cap: u64,
        page_size: u64,
        index_type: &'static str,
        metadata_type: &'static str,
        weight_of_metadata: fn(&K, &Arc<M>) -> u32,
        weight_of_content: fn(&(K, u64), &Bytes) -> u32,
    ) -> Self {
        common_telemetry::debug!(
            "Building IndexCache with metadata size: {index_metadata_cap}, content size: {index_content_cap}, page size: {page_size}, index type: {index_type}"
        );
        let index_metadata = common_cache::Cache::new(
            &format!("index_metadata_{index_type}"),
            index_metadata_cap,
            move |key: &K, value: &Arc<IndexMetadata<M>>| weight_of_metadata(key, &value.metadata),
            |_, value, _| value.accounting.release(),
        )
        .with_metrics(crate::metrics::cache_loading_metrics(metadata_type));
        let index_cache = PagedCache::new(
            &format!("index_content_{index_type}"),
            index_content_cap,
            page_size,
            weight_of_content,
            crate::metrics::cache_page_observer(index_type, INDEX_CONTENT_TYPE, |hits, misses| {
                (hits, misses)
            }),
        );
        Self {
            index_metadata,
            index: index_cache,
            index_type,
            weight_of_metadata,
        }
    }
}

impl<K, M> IndexCache<K, M>
where
    K: Hash + Eq + Clone + Copy + Send + Sync + 'static,
    M: Send + Sync + 'static,
{
    /// Looks up completed metadata without starting or waiting for a load.
    pub async fn get_metadata(&self, key: K) -> Option<Arc<M>> {
        self.index_metadata
            .get(&key)
            .await
            .map(|value| value.metadata.clone())
    }

    fn build_metadata_value(&self, key: &K, metadata: Arc<M>) -> Arc<IndexMetadata<M>> {
        let accounting = CacheAccounting::new(
            (self.weight_of_metadata)(key, &metadata),
            CACHE_BYTES.with_label_values(&[INDEX_METADATA_TYPE]),
        );
        Arc::new(IndexMetadata {
            metadata,
            accounting,
        })
    }

    /// Inserts completed metadata for cache eviction and accounting tests.
    #[cfg(test)]
    pub async fn put_metadata(&self, key: K, metadata: Arc<M>) {
        self.index_metadata
            .insert(key, self.build_metadata_value(&key, metadata))
            .await;
    }

    /// Shares loads after the caller's initial lookup missed. Native request counts
    /// describe these submissions, including waiters, rather than completed hits.
    async fn get_or_load_metadata<F, E>(&self, key: K, load: F) -> Result<Arc<M>, Arc<E>>
    where
        F: Future<Output = Result<Arc<M>, E>>,
        E: Send + Sync + 'static,
    {
        if self.index_metadata.capacity() == 0 {
            return load.await.map_err(Arc::new);
        }
        self.index_metadata
            .try_get_with(&key, async {
                let metadata = load.await?;
                Ok(self.build_metadata_value(&key, metadata))
            })
            .await
            .map(|value| value.metadata.clone())
    }

    /// Fetches one byte range through fixed-size pages.
    async fn get_or_load<F, Fut, E>(
        &self,
        key: K,
        file_size: u64,
        offset: u64,
        size: u32,
        load: F,
    ) -> Result<(Bytes, IndexCacheMetrics), Arc<io::Error>>
    where
        F: Fn(Vec<Range<u64>>) -> Fut + Send + Sync,
        Fut: Future<Output = Result<Vec<Bytes>, E>> + Send,
        E: std::error::Error + Send + Sync + 'static,
    {
        let end = offset.checked_add(u64::from(size)).ok_or_else(|| {
            Arc::new(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Index range overflow",
            ))
        })?;
        let (pages, metrics) = self
            .get_or_load_vec(key, file_size, std::slice::from_ref(&(offset..end)), load)
            .await?;
        Ok((pages.into_iter().next().unwrap_or_default(), metrics))
    }

    /// Shares missing page loads while retaining legacy unique-page metrics.
    async fn get_or_load_vec<F, Fut, E>(
        &self,
        key: K,
        file_size: u64,
        ranges: &[Range<u64>],
        load: F,
    ) -> Result<(Vec<Bytes>, IndexCacheMetrics), Arc<io::Error>>
    where
        F: Fn(Vec<Range<u64>>) -> Fut + Send + Sync,
        Fut: Future<Output = Result<Vec<Bytes>, E>> + Send,
        E: std::error::Error + Send + Sync + 'static,
    {
        let reader = IndexPageReader {
            load: &load,
            file_size,
        };
        let (pages, stats) = self.index.fetch(key, file_size, ranges, &reader).await?;
        crate::metrics::cache_page_fetch_metrics(self.index_type, &stats);
        Ok((
            pages,
            IndexCacheMetrics {
                cache_hit: stats.cache_hit,
                cache_miss: stats.cache_miss,
                num_pages: stats.cache_hit + stats.cache_miss,
                page_bytes: stats.aligned_bytes,
            },
        ))
    }

    /// Invalidates all cache entries whose keys satisfy `predicate`.
    pub fn invalidate_if<F>(&self, predicate: F)
    where
        F: Fn(&K) -> bool + Send + Sync + 'static,
    {
        let predicate = Arc::new(predicate);
        let metadata_predicate = Arc::clone(&predicate);

        self.index_metadata
            .invalidate_if(move |key, _| metadata_predicate(key))
            .expect("cache should support invalidation closures");

        self.index
            .invalidate_if(move |key| predicate(key))
            .expect("cache should support invalidation closures");
    }
}

/// Adapts borrowed index readers to the common range-reader interface.
struct IndexPageReader<'a, F> {
    load: &'a F,
    file_size: u64,
}

#[async_trait]
impl<F, Fut, E> RangeReader for IndexPageReader<'_, F>
where
    F: Fn(Vec<Range<u64>>) -> Fut + Send + Sync,
    Fut: Future<Output = Result<Vec<Bytes>, E>> + Send,
    E: std::error::Error + Send + Sync + 'static,
{
    async fn metadata(&self) -> io::Result<Metadata> {
        Ok(Metadata {
            content_length: self.file_size,
        })
    }

    async fn read(&self, range: Range<u64>) -> io::Result<Bytes> {
        let mut reads = self.read_vec(&[range]).await?;
        if reads.len() != 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Index reader returned an invalid result count",
            ));
        }
        Ok(reads.remove(0))
    }

    async fn read_vec(&self, ranges: &[Range<u64>]) -> io::Result<Vec<Bytes>> {
        (self.load)(ranges.to_vec())
            .await
            .map_err(|error| io::Error::other(Arc::new(error)))
    }
}

/// Finds a typed shared error through the range-reader I/O wrappers.
fn find_shared_page_source<E: std::error::Error + Send + Sync + 'static>(
    mut error: &io::Error,
) -> Option<Arc<E>> {
    loop {
        let source = error.get_ref()?;
        if let Some(source) = source.downcast_ref::<Arc<E>>() {
            return Some(source.clone());
        }
        if let Some(nested) = source.downcast_ref::<Arc<io::Error>>() {
            error = nested;
        } else {
            error = source.downcast_ref::<io::Error>()?;
        }
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::Notify;

    use super::*;

    fn metadata_cache() -> IndexCache<u64, u64> {
        IndexCache::new_with_weighter(
            1024,
            1024,
            4,
            "test",
            "test_index_metadata",
            |_, _| 8,
            |_, bytes| bytes.len() as u32,
        )
    }

    #[tokio::test]
    async fn metadata_shares_same_key_without_blocking_other_keys() {
        let cache = metadata_cache();
        let release = Notify::new();
        let mut owner = Box::pin(cache.get_or_load_metadata(1, async {
            release.notified().await;
            Ok::<_, std::io::Error>(Arc::new(11))
        }));
        assert!(futures::poll!(&mut owner).is_pending());
        let mut waiter = Box::pin(cache.get_or_load_metadata(1, async {
            Err::<Arc<u64>, _>(std::io::Error::other("unexpected duplicate load"))
        }));
        assert!(futures::poll!(&mut waiter).is_pending());
        let other = cache
            .get_or_load_metadata(2, async { Ok::<_, std::io::Error>(Arc::new(33)) })
            .await
            .unwrap();
        assert_eq!(*other, 33);
        release.notify_one();
        let owner = owner.await.unwrap();
        let waiter = waiter.await.unwrap();
        assert!(Arc::ptr_eq(&owner, &waiter));
        assert_eq!(*cache.get_metadata(1).await.unwrap(), 11);
    }

    #[tokio::test]
    async fn metadata_failure_is_shared_but_not_cached() {
        let cache = metadata_cache();
        let release = Notify::new();
        let mut owner = Box::pin(cache.get_or_load_metadata(1, async {
            release.notified().await;
            Err::<Arc<u64>, _>(std::io::Error::other("metadata read failed"))
        }));
        assert!(futures::poll!(&mut owner).is_pending());
        let mut waiter = Box::pin(
            cache.get_or_load_metadata(1, async { Ok::<_, std::io::Error>(Arc::new(22)) }),
        );
        assert!(futures::poll!(&mut waiter).is_pending());
        release.notify_one();
        let error = owner.await.unwrap_err();
        let shared = waiter.await.unwrap_err();
        assert!(Arc::ptr_eq(&error, &shared));
        assert!(cache.get_metadata(1).await.is_none());
        let retried = cache
            .get_or_load_metadata(1, async { Ok::<_, std::io::Error>(Arc::new(33)) })
            .await
            .unwrap();
        assert_eq!(*retried, 33);
    }

    #[tokio::test]
    async fn cancelled_metadata_loader_allows_waiter_to_take_over() {
        let cache = metadata_cache();
        let mut owner = Box::pin(cache.get_or_load_metadata(
            1,
            std::future::pending::<Result<Arc<u64>, std::io::Error>>(),
        ));
        assert!(futures::poll!(&mut owner).is_pending());
        let mut waiter = Box::pin(
            cache.get_or_load_metadata(1, async { Ok::<_, std::io::Error>(Arc::new(22)) }),
        );
        assert!(futures::poll!(&mut waiter).is_pending());
        drop(owner);
        let value = tokio::time::timeout(std::time::Duration::from_secs(1), waiter)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(*value, 22);
    }

    #[tokio::test]
    async fn metadata_invalidation_releases_charge_with_live_reader() {
        let cache = metadata_cache();
        // Use a private gauge so parallel cache tests cannot change this assertion.
        let gauge = prometheus::IntGauge::new("test_index_metadata_charge", "test").unwrap();
        let value = Arc::new(IndexMetadata {
            metadata: Arc::new(11),
            accounting: CacheAccounting::new(8, gauge.clone()),
        });
        cache.index_metadata.insert(1, value.clone()).await;
        cache.put_metadata(2, Arc::new(22)).await;
        cache.index_metadata.run_pending_tasks().await;
        assert_eq!(gauge.get(), 8);
        cache.invalidate_if(|key| *key == 1);
        assert!(cache.get_metadata(1).await.is_none());
        assert_eq!(*cache.get_metadata(2).await.unwrap(), 22);
        cache.index_metadata.run_pending_tasks().await;
        assert_eq!(gauge.get(), 0);
        assert_eq!(*value.metadata, 11);
        drop(value);
        assert_eq!(gauge.get(), 0);
    }

    #[tokio::test]
    async fn disabled_metadata_cache_does_not_share_loads() {
        let cache = IndexCache::new_with_weighter(
            0,
            1024,
            4,
            "test_disabled",
            "test_disabled_index_metadata",
            |_: &u64, _: &Arc<u64>| 8,
            |_, bytes| bytes.len() as u32,
        );
        let mut owner = Box::pin(cache.get_or_load_metadata(
            1,
            std::future::pending::<Result<Arc<u64>, std::io::Error>>(),
        ));
        assert!(futures::poll!(&mut owner).is_pending());
        let mut independent = Box::pin(
            cache.get_or_load_metadata(1, async { Ok::<_, std::io::Error>(Arc::new(22)) }),
        );
        assert!(futures::poll!(&mut independent).is_ready());
    }

    #[tokio::test]
    async fn metadata_metrics_distinguish_requests_from_loads() {
        let metrics = crate::metrics::cache_loading_metrics("test_index_metadata_metrics");
        let mut cache = metadata_cache();
        cache.index_metadata = cache.index_metadata.with_metrics(metrics.clone());
        let release = Notify::new();
        let mut owner = Box::pin(cache.get_or_load_metadata(1, async {
            release.notified().await;
            Ok::<_, std::io::Error>(Arc::new(11))
        }));
        let mut waiter = Box::pin(cache.get_or_load_metadata(1, async {
            Err::<Arc<u64>, _>(std::io::Error::other("duplicate metadata load"))
        }));
        assert!(futures::poll!(&mut owner).is_pending());
        assert!(futures::poll!(&mut waiter).is_pending());
        assert_eq!(metrics.requests.active.get(), 2);
        assert_eq!(metrics.loads.active.get(), 1);
        release.notify_one();
        let (owner, waiter) = tokio::join!(owner, waiter);
        assert!(Arc::ptr_eq(&owner.unwrap(), &waiter.unwrap()));
        assert_eq!(metrics.requests.succeeded.get(), 2);
        assert_eq!(metrics.loads.succeeded.get(), 1);
        assert_eq!(metrics.requests.active.get(), 0);
        assert_eq!(metrics.loads.active.get(), 0);
    }
    #[tokio::test]
    async fn overlapping_page_requests_share_bytes_and_preserve_legacy_metrics() {
        let cache = metadata_cache();
        let release = Notify::new();
        let reads = std::sync::Mutex::new(Vec::new());
        let load = |ranges: Vec<Range<u64>>| {
            reads.lock().unwrap().push(ranges.clone());
            let release = &release;
            async move {
                release.notified().await;
                Ok::<_, io::Error>(
                    ranges
                        .iter()
                        .map(|range| {
                            Bytes::from(range.clone().map(|v| v as u8).collect::<Vec<_>>())
                        })
                        .collect(),
                )
            }
        };
        let first_ranges = std::slice::from_ref(&(0..8));
        let second_ranges = std::slice::from_ref(&(4..12));
        let mut first = Box::pin(cache.get_or_load_vec(1, 12, first_ranges, &load));
        assert!(futures::poll!(&mut first).is_pending());
        let mut second = Box::pin(cache.get_or_load_vec(1, 12, second_ranges, &load));
        assert!(futures::poll!(&mut second).is_pending());
        release.notify_waiters();
        let (first, second) = tokio::join!(first, second);
        let (first, first_metrics) = first.unwrap();
        let (second, second_metrics) = second.unwrap();
        assert_eq!(first, vec![Bytes::from_static(&[0, 1, 2, 3, 4, 5, 6, 7])]);
        assert_eq!(
            second,
            vec![Bytes::from_static(&[4, 5, 6, 7, 8, 9, 10, 11])]
        );
        for metrics in [first_metrics, second_metrics] {
            assert_eq!(
                (
                    metrics.cache_hit,
                    metrics.cache_miss,
                    metrics.num_pages,
                    metrics.page_bytes
                ),
                (0, 2, 2, 8)
            );
        }
        let batches = reads.lock().unwrap().clone();
        assert_eq!(batches, vec![vec![0..4, 4..8], vec![8..12]]);
        let mut physical = batches.into_iter().flatten().collect::<Vec<_>>();
        physical.sort_by_key(|range| range.start);
        assert!(physical.windows(2).all(|pair| pair[0].end <= pair[1].start));
        assert_eq!(
            physical
                .iter()
                .map(|range| range.end - range.start)
                .sum::<u64>(),
            12
        );
        let (hot, metrics) = cache
            .get_or_load_vec(1, 12, &[1..6, 4..12], |_| async {
                Err::<Vec<Bytes>, _>(io::Error::other("unexpected hot read"))
            })
            .await
            .unwrap();
        assert_eq!(hot[0], Bytes::from_static(&[1, 2, 3, 4, 5]));
        assert_eq!(
            (
                metrics.cache_hit,
                metrics.cache_miss,
                metrics.num_pages,
                metrics.page_bytes
            ),
            (3, 0, 3, 12)
        );
        cache.invalidate_if(|key| *key == 1);
        assert!(!cache.index.contains(&1, 0).await);
    }

    #[tokio::test]
    async fn index_pages_preserve_shared_typed_read_errors_and_retry() {
        use common_error::ext::ErrorExt;
        use index::inverted_index::error::{Error, IndexNotFoundSnafu};
        let cache = metadata_cache();
        let error = cache
            .get_or_load_vec(1, 8, std::slice::from_ref(&(0..8)), |_| async {
                Err::<Vec<Bytes>, _>(IndexNotFoundSnafu { name: "missing" }.build())
            })
            .await
            .unwrap_err();
        let retained = find_shared_page_source::<Error>(&error).unwrap();
        assert_eq!(
            retained.status_code(),
            common_error::status_code::StatusCode::InvalidArguments
        );
        assert!(!cache.index.contains(&1, 0).await);
        let (pages, _) = cache
            .get_or_load_vec(1, 8, std::slice::from_ref(&(0..8)), |ranges| async move {
                Ok::<_, io::Error>(
                    ranges
                        .iter()
                        .map(|r| Bytes::from(vec![7; (r.end - r.start) as usize]))
                        .collect(),
                )
            })
            .await
            .unwrap();
        assert_eq!(pages, vec![Bytes::from(vec![7; 8])]);
    }
    #[tokio::test]
    async fn single_page_range_retains_shared_bytes() {
        let cache = metadata_cache();
        let (first, _) = cache
            .get_or_load(1, 8, 1, 2, |ranges| async move {
                Ok::<_, io::Error>(
                    ranges
                        .into_iter()
                        .map(|range| Bytes::from(range.map(|n| n as u8).collect::<Vec<_>>()))
                        .collect(),
                )
            })
            .await
            .unwrap();
        let (second, metrics) = cache
            .get_or_load(1, 8, 1, 2, |_| async {
                Err::<Vec<Bytes>, _>(io::Error::other("unexpected read on cache hit"))
            })
            .await
            .unwrap();
        assert_eq!(&first[..], &[1, 2]);
        assert_eq!(first, second);
        assert_eq!(first.as_ptr(), second.as_ptr());
        assert_eq!(metrics.cache_hit, 1);
        assert_eq!(metrics.cache_miss, 0);
    }
}
