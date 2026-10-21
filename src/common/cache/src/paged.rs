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

//! Immutable byte pages backed by native cache loading and direct range reads.

#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::hash::Hash;
use std::io;
use std::num::NonZeroU64;
use std::ops::Range;
use std::sync::Arc;
use std::task::Poll;

use bytes::{Bytes, BytesMut};
use common_base::range_read::RangeReader;
use futures::channel::{mpsc, oneshot};
use futures::{StreamExt, future};
use prometheus::IntGauge;

use crate::accounting::CacheAccounting;
use crate::metrics::CacheMetrics;
use crate::page::PageLayout;
use crate::{Cache, RemovalCause};

/// Business-level events; callers retain their existing hit/miss metric units.
pub enum PageEvent {
    /// Unique logical page lookups in one request, before any missing loads execute.
    Lookup { hits: usize, misses: usize },
    /// One successful native page loader, excluding waiters and hits.
    Loaded { bytes: u64 },
    /// Backend weight, not process RSS; sampled after fetch/maintenance.
    Residency { usage: u64, capacity: u64 },
    /// Backend eviction, excluding unadmitted values released by their owners.
    Evicted { cause: RemovalCause },
}

/// Caller-bound metrics without storage-engine dependencies or global registrations.
#[derive(Clone)]
pub struct PageCacheObserver {
    pub bytes: IntGauge,
    pub loads: Option<Arc<CacheMetrics>>,
    pub events: Arc<dyn Fn(PageEvent) + Send + Sync>,
}

/// Byte accounting for one successful fetch. Hits are initial lookups, not waiters.
#[derive(Debug, Default)]
pub struct PageFetchStats {
    pub cache_hit: usize,
    pub cache_miss: usize,
    /// Requested intersections already cached; overlapping requests count repeatedly.
    pub cached_bytes: u64,
    pub cached_fragments: usize,
    /// Original request lengths, including duplicates and overlaps.
    pub requested_bytes: u64,
    /// Sum of unique logical page lengths, clipped at EOF, including cache hits.
    pub aligned_bytes: u64,
    /// Union of original request byte ranges, excluding overlaps.
    pub unique_requested_bytes: u64,
}

/// Caches fixed-size byte pages of immutable content identified by `K`.
/// File identity must include any version needed to distinguish different contents.
/// Missing pages elected by this fetch are submitted through [`RangeReader::read_vec`].
pub struct PagedCache<K> {
    cache: Cache<(K, u64), Arc<CachedPage>>,
    page_size: u64,
    weigher: PageWeigher<K>,
    observer: PageCacheObserver,
}

impl<K: Hash + Eq + Clone + Send + Sync + 'static> PagedCache<K> {
    /// Creates an independent cache budget. Zero page size is rejected by fetch.
    pub fn new(
        name: &str,
        capacity: u64,
        page_size: u64,
        weigher: impl Fn(&(K, u64), &Bytes) -> u32 + Send + Sync + 'static,
        observer: PageCacheObserver,
    ) -> Self {
        let weigher: PageWeigher<K> = Arc::new(weigher);
        let weight = weigher.clone();
        let events = observer.events.clone();
        let mut cache = Cache::new(
            name,
            capacity,
            move |key, value: &Arc<CachedPage>| weight(key, &value.bytes),
            move |_, value, cause| {
                value.accounting.release();
                events(PageEvent::Evicted { cause });
            },
        );
        if let Some(metrics) = &observer.loads {
            cache = cache.with_metrics(metrics.clone());
        }
        Self {
            cache,
            page_size,
            weigher,
            observer,
        }
    }

    /// Invalidates completed pages. Concurrent loads are not fenced.
    pub fn invalidate_if(
        &self,
        predicate: impl Fn(&K) -> bool + Send + Sync + 'static,
    ) -> io::Result<()> {
        self.cache
            .invalidate_if(move |(file, _), _| predicate(file))
    }

    /// Looks up a completed page without updating business lookup counters.
    pub async fn contains(&self, file: &K, page_id: u64) -> bool {
        self.cache.get(&(file.clone(), page_id)).await.is_some()
    }

    /// Recovers immutable file size from a requested page already in memory.
    pub async fn known_file_size(&self, file: &K, ranges: &[Range<u64>]) -> Option<u64> {
        let layout = PageLayout::new(NonZeroU64::new(self.page_size)?, u64::MAX);
        for range in ranges {
            for id in layout.page_ids(range.clone()).ok()? {
                if let Some(page) = self.cache.get(&(file.clone(), id)).await {
                    return Some(page.file_size);
                }
            }
        }
        None
    }

    /// Applies pending cache admission and eviction work.
    pub async fn run_pending_tasks(&self) {
        self.cache.run_pending_tasks().await;
        self.observe_residency();
    }

    /// Fetches bytes in input order. The reader must address the same immutable
    /// content as `file`. Each successful loader reads one complete missing page.
    pub async fn fetch<R: RangeReader + ?Sized>(
        &self,
        file: K,
        file_size: u64,
        ranges: &[Range<u64>],
        reader: &R,
    ) -> Result<(Vec<Bytes>, PageFetchStats), Arc<io::Error>> {
        if ranges.iter().any(|r| r.start > r.end || r.end > file_size) {
            return Err(invalid_request());
        }
        let page_size = NonZeroU64::new(self.page_size).ok_or_else(invalid_request)?;
        let layout = PageLayout::new(page_size, file_size);
        let mut stats = PageFetchStats::default();
        let mut ids = BTreeSet::new();
        for range in ranges {
            stats.requested_bytes = stats
                .requested_bytes
                .checked_add(range.end - range.start)
                .ok_or_else(invalid_request)?;
            ids.extend(layout.page_ids(range.clone()).map_err(Arc::new)?);
        }
        stats.unique_requested_bytes = unique_requested_bytes(ranges);
        let mut pages = BTreeMap::new();
        let mut missing = Vec::new();
        for id in ids {
            let range = layout.page_range(id).ok_or_else(invalid_request)?;
            stats.aligned_bytes += range.end - range.start;
            if let Some(page) = self.cache.get(&(file.clone(), id)).await {
                check_file_size(&page, file_size)?;
                pages.insert(id, page.bytes.clone());
            } else {
                missing.push(id);
            }
        }
        for range in ranges {
            for id in layout.page_ids(range.clone()).map_err(Arc::new)? {
                if pages.contains_key(&id) {
                    let slice = layout
                        .request_slice(id, range.clone())
                        .map_err(Arc::new)?
                        .ok_or_else(invalid_request)?;
                    stats.cached_bytes += slice.len() as u64;
                    stats.cached_fragments += 1;
                }
            }
        }
        stats.cache_hit = pages.len();
        stats.cache_miss = missing.len();
        (self.observer.events)(PageEvent::Lookup {
            hits: stats.cache_hit,
            misses: stats.cache_miss,
        });
        if !missing.is_empty() {
            let (requests, receiver) = mpsc::unbounded();
            let batch_size = missing.len().max(1);
            let mut loads = missing
                .into_iter()
                .map(|id| {
                    let requests = requests.clone();
                    let key = (file.clone(), id);
                    Box::pin(future::maybe_done(async move {
                        let range = layout.page_range(id).ok_or_else(invalid_request)?;
                        let page = self
                            .cache
                            .try_get_with(&key, async {
                                let (send, receive) =
                                    oneshot::channel::<Result<Bytes, Arc<io::Error>>>();
                                requests
                                    .unbounded_send((range.clone(), send))
                                    .map_err(|_| io::Error::other("Page read batch closed"))?;
                                let bytes = receive
                                    .await
                                    .map_err(|_| io::Error::other("Page read batch cancelled"))?
                                    .map_err(|error: Arc<io::Error>| {
                                        io::Error::new(error.kind(), error)
                                    })?;
                                if bytes.len() as u64 != range.end - range.start {
                                    return Err(io::Error::new(
                                        io::ErrorKind::UnexpectedEof,
                                        "Invalid page response length",
                                    ));
                                }
                                // Detach readers' shared backing buffers from cache accounting.
                                Ok(self.create_cached_page(
                                    &key,
                                    file_size,
                                    Bytes::copy_from_slice(&bytes),
                                ))
                            })
                            .await?;
                        check_file_size(&page, file_size)?;
                        Ok::<_, Arc<io::Error>>((id, page.bytes.clone()))
                    }))
                })
                .collect::<Vec<_>>();
            // Poll every page before driving I/O, so executor fairness limits do not
            // split one fetch's immediately available misses into separate batches.
            let loads = future::poll_fn(move |cx| {
                let mut complete = true;
                for load in &mut loads {
                    complete &= load.as_mut().poll(cx).is_ready();
                    if let Some(Err(error)) = load.as_mut().output_mut() {
                        return Poll::Ready(Err(error.clone()));
                    }
                }
                if !complete {
                    return Poll::Pending;
                }
                Poll::Ready(
                    loads
                        .iter_mut()
                        .map(|load| load.as_mut().take_output().ok_or_else(invalid_request)?)
                        .collect::<Result<Vec<_>, _>>(),
                )
            });
            // The channel contains only loaders elected by Moka for this fetch. Waiters
            // enqueue nothing; a waiter promoted after cancellation can enqueue later.
            drop(requests);
            let read_batches = async {
                let mut batches = receiver.ready_chunks(batch_size);
                while let Some(batch) = batches.next().await {
                    let (ranges, replies): (Vec<_>, Vec<_>) = batch.into_iter().unzip();
                    let result = reader.read_vec(&ranges).await.and_then(|bytes| {
                        if bytes.len() == ranges.len() {
                            Ok(bytes)
                        } else {
                            Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "Invalid page response count",
                            ))
                        }
                    });
                    match result {
                        Ok(bytes) => {
                            for (reply, bytes) in replies.into_iter().zip(bytes) {
                                let _ = reply.send(Ok(bytes));
                            }
                        }
                        Err(error) => {
                            let error = Arc::new(error);
                            for reply in replies {
                                let _ = reply.send(Err(error.clone()));
                            }
                        }
                    }
                }
                Ok::<(), Arc<io::Error>>(())
            };
            let (loaded, ()) = future::try_join(loads, read_batches).await?;
            pages.extend(loaded);
        }
        let mut output = Vec::with_capacity(ranges.len());
        for range in ranges {
            if range.is_empty() {
                output.push(Bytes::new());
                continue;
            }
            let ids = layout.page_ids(range.clone()).map_err(Arc::new)?;
            if ids.end - ids.start == 1 {
                let slice = layout
                    .request_slice(ids.start, range.clone())
                    .map_err(Arc::new)?
                    .ok_or_else(invalid_request)?;
                let page = pages.get(&ids.start).ok_or_else(invalid_request)?;
                if slice.end > page.len() {
                    return Err(invalid_request());
                }
                output.push(page.slice(slice));
                continue;
            }
            let len = usize::try_from(range.end - range.start).map_err(|_| invalid_request())?;
            let mut assembled = BytesMut::with_capacity(len);
            for id in ids {
                let slice = layout
                    .request_slice(id, range.clone())
                    .map_err(Arc::new)?
                    .ok_or_else(invalid_request)?;
                let page = pages.get(&id).ok_or_else(invalid_request)?;
                if slice.end > page.len() {
                    return Err(invalid_request());
                }
                assembled.extend_from_slice(&page[slice]);
            }
            if assembled.len() != len {
                return Err(invalid_request());
            }
            output.push(assembled.freeze());
        }
        self.observe_residency();
        Ok((output, stats))
    }

    fn create_cached_page(&self, key: &(K, u64), file_size: u64, bytes: Bytes) -> Arc<CachedPage> {
        (self.observer.events)(PageEvent::Loaded {
            bytes: bytes.len() as u64,
        });
        let accounting =
            CacheAccounting::new((self.weigher)(key, &bytes), self.observer.bytes.clone());
        Arc::new(CachedPage {
            file_size,
            bytes,
            accounting,
        })
    }

    fn observe_residency(&self) {
        (self.observer.events)(PageEvent::Residency {
            usage: self.cache.weighted_size(),
            capacity: self.cache.capacity(),
        });
    }
}

struct CachedPage {
    file_size: u64,
    bytes: Bytes,
    accounting: CacheAccounting,
}

type PageWeigher<K> = Arc<dyn Fn(&(K, u64), &Bytes) -> u32 + Send + Sync>;

fn invalid_request() -> Arc<io::Error> {
    Arc::new(io::Error::new(
        io::ErrorKind::InvalidInput,
        "Invalid cached page range",
    ))
}

fn check_file_size(page: &CachedPage, expected: u64) -> Result<(), Arc<io::Error>> {
    if page.file_size != expected {
        return Err(Arc::new(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Cached content identity used with inconsistent file sizes",
        )));
    }
    Ok(())
}

/// Counts the union of request ranges already validated by fetch.
fn unique_requested_bytes(ranges: &[Range<u64>]) -> u64 {
    let mut ranges = ranges.to_vec();
    ranges.sort_unstable_by_key(|range| range.start);
    let mut end = 0;
    let mut bytes = 0;
    for range in ranges {
        bytes += range.end.saturating_sub(range.start.max(end));
        end = end.max(range.end);
    }
    bytes
}
