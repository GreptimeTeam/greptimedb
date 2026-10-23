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

//! Coordinates range-result cache loads and SST retirement. No I/O or decoding
//! runs under the coordinator lock; loaders use the shared query runtime.

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex, mpsc};

use moka::sync::Cache;
use store_api::storage::FileId;
use tokio::sync::watch;

use super::{
    RANGE_RESULT_TYPE, RangeResultMemoryLimiter, range_result_cache_weight, removal_cause_str,
};
use crate::metrics::{CACHE_BYTES, CACHE_EVICTION, RANGE_CACHE_WORKER_EVENTS};
use crate::read::range_cache::{RangeScanCacheKey, RangeScanCacheValue};
use crate::sst::file::FileHandle;

/// Admission, not a queue: excess distinct misses keep using ordinary scans.
const MAX_IN_FLIGHT_RANGES: usize = 32;

type SharedResult = Option<Arc<RangeScanCacheValue>>;

pub(crate) enum RangeCacheLookup {
    Hit(Arc<RangeScanCacheValue>),
    Loading(watch::Receiver<SharedResult>),
    Bypass,
}

#[derive(Clone)]
struct Entry {
    generation: u64,
    value: Arc<RangeScanCacheValue>,
}

struct Flight {
    generation: u64,
    sender: watch::Sender<SharedResult>,
    invalidated: bool,
}

#[derive(Default)]
struct State {
    generation: u64,
    flights: HashMap<RangeScanCacheKey, Flight>,
    /// File IDs are globally unique. Index by physical file, not query region:
    /// a copied region can read an SST whose original region differs from its own.
    files: HashMap<FileId, HashMap<Arc<RangeScanCacheKey>, u64>>,
}

impl State {
    fn next_generation(&mut self) -> u64 {
        self.generation += 1;
        self.generation
    }

    fn forget(&mut self, key: &RangeScanCacheKey, generation: u64) {
        for (file, _) in &key.row_groups {
            if let Some(keys) = self.files.get_mut(file) {
                // Eviction callbacks may arrive after the same key is refilled.
                if keys.get(key) == Some(&generation) {
                    keys.remove(key);
                }
                if keys.is_empty() {
                    self.files.remove(file);
                }
            }
        }
    }
}

pub(crate) struct RangeCacheWorker {
    cache: Cache<RangeScanCacheKey, Entry>,
    state: Mutex<State>,
    /// Moka can call listeners during a cache operation. Queue only cleanup
    /// metadata instead of reentering `state` from its eviction listener.
    evictions: Mutex<mpsc::Receiver<(Arc<RangeScanCacheKey>, u64)>>,
    /// Held across a load, separately from the temporary concat-memory limiter.
    pub(crate) load_memory: RangeResultMemoryLimiter,
}

impl RangeCacheWorker {
    pub(crate) fn new(capacity: u64) -> Arc<Self> {
        let (tx, rx) = mpsc::channel();
        let cache = Cache::builder()
            .max_capacity(capacity)
            .weigher(|k: &RangeScanCacheKey, v: &Entry| range_result_cache_weight(k, &v.value))
            .eviction_listener(move |key, entry: Entry, cause| {
                CACHE_BYTES
                    .with_label_values(&[RANGE_RESULT_TYPE])
                    .sub(range_result_cache_weight(&key, &entry.value).into());
                CACHE_EVICTION
                    .with_label_values(&[RANGE_RESULT_TYPE, removal_cause_str(cause)])
                    .inc();
                let _ = tx.send((key, entry.generation));
            })
            .build();
        Arc::new(Self {
            cache,
            state: Mutex::new(State::default()),
            evictions: Mutex::new(rx),
            load_memory: RangeResultMemoryLimiter::new(capacity as usize, 1024),
        })
    }

    fn drain_evictions(&self, state: &mut State) {
        for (key, generation) in self.evictions.lock().unwrap().try_iter() {
            state.forget(&key, generation);
        }
    }

    pub(crate) fn get(&self, key: &RangeScanCacheKey) -> SharedResult {
        self.cache.get(key).map(|entry| entry.value)
    }

    /// For test/benchmark producers that do not hold live SST handles.
    #[cfg(any(test, feature = "test"))]
    pub(crate) fn insert(&self, key: RangeScanCacheKey, value: Arc<RangeScanCacheValue>) {
        let mut state = self.state.lock().unwrap();
        self.drain_evictions(&mut state);
        self.publish(&mut state, key, value);
    }

    fn publish(&self, state: &mut State, key: RangeScanCacheKey, value: Arc<RangeScanCacheValue>) {
        let generation = state.next_generation();
        let indexed_key = Arc::new(key.clone());
        for (file, _) in &key.row_groups {
            state
                .files
                .entry(*file)
                .or_default()
                .insert(indexed_key.clone(), generation);
        }
        CACHE_BYTES
            .with_label_values(&[RANGE_RESULT_TYPE])
            .add(range_result_cache_weight(&key, &value).into());
        self.cache.insert(key, Entry { generation, value });
        self.drain_evictions(state);
    }

    /// Called at logical retirement, before the last snapshot releases the SST.
    /// Existing readers may finish, but their results must not refill this cache.
    pub(crate) fn invalidate_file(&self, file: FileId) {
        RANGE_CACHE_WORKER_EVENTS
            .with_label_values(&["invalidate"])
            .inc();
        let mut state = self.state.lock().unwrap();
        self.drain_evictions(&mut state);
        if let Some(keys) = state.files.remove(&file) {
            for (key, generation) in keys {
                self.cache.invalidate(key.as_ref());
                state.forget(&key, generation);
            }
        }
        for (key, flight) in &mut state.flights {
            if key.row_groups.iter().any(|(id, _)| *id == file) {
                flight.invalidated = true;
            }
        }
        self.drain_evictions(&mut state);
    }

    pub(crate) fn get_or_load<F>(
        self: &Arc<Self>,
        key: RangeScanCacheKey,
        files: Vec<FileHandle>,
        load: F,
    ) -> RangeCacheLookup
    where
        F: Future<Output = SharedResult> + Send + 'static,
    {
        if files.iter().any(FileHandle::is_deleted) {
            RANGE_CACHE_WORKER_EVENTS
                .with_label_values(&["bypass_retired"])
                .inc();
            return RangeCacheLookup::Bypass;
        }
        if let Some(value) = super::update_hit_miss(self.get(&key), RANGE_RESULT_TYPE) {
            return RangeCacheLookup::Hit(value);
        }
        let mut state = self.state.lock().unwrap();
        self.drain_evictions(&mut state);
        // Close the miss/register race with completion and invalidation.
        if files.iter().any(FileHandle::is_deleted) {
            RANGE_CACHE_WORKER_EVENTS
                .with_label_values(&["bypass_retired"])
                .inc();
            return RangeCacheLookup::Bypass;
        }
        if let Some(value) = self.get(&key) {
            return RangeCacheLookup::Hit(value);
        }
        if let Some(flight) = state.flights.get(&key) {
            RANGE_CACHE_WORKER_EVENTS
                .with_label_values(&["coalesced"])
                .inc();
            return RangeCacheLookup::Loading(flight.sender.subscribe());
        }
        if state.flights.len() >= MAX_IN_FLIGHT_RANGES {
            RANGE_CACHE_WORKER_EVENTS
                .with_label_values(&["bypass_capacity"])
                .inc();
            return RangeCacheLookup::Bypass;
        }
        let generation = state.next_generation();
        let (sender, receiver) = watch::channel(None);
        state.flights.insert(
            key.clone(),
            Flight {
                generation,
                sender: sender.clone(),
                invalidated: false,
            },
        );
        drop(state);

        RANGE_CACHE_WORKER_EVENTS
            .with_label_values(&["started"])
            .inc();
        let guard = LoadGuard {
            worker: self.clone(),
            key,
            generation,
        };
        common_runtime::spawn_query(async move {
            // Losing the first requester does not cancel other subscribers.
            // Losing every requester drops the loader and unregisters the flight.
            let value = tokio::select! {
                biased;
                _ = sender.closed() => return,
                value = load => value,
            };
            guard.complete(value, &files);
        });
        RangeCacheLookup::Loading(receiver)
    }
}

/// Also unregisters a cancelled or panicking load. Generation matching keeps a
/// stale task from unregistering a later retry of the same key.
struct LoadGuard {
    worker: Arc<RangeCacheWorker>,
    key: RangeScanCacheKey,
    generation: u64,
}

impl LoadGuard {
    fn complete(&self, value: SharedResult, files: &[FileHandle]) {
        let mut state = self.worker.state.lock().unwrap();
        self.worker.drain_evictions(&mut state);
        let Some(flight) = state.flights.get(&self.key) else {
            return;
        };
        if flight.generation != self.generation {
            return;
        }
        let flight = state.flights.remove(&self.key).unwrap();
        let event = if let Some(value) = &value {
            if !flight.invalidated && !files.iter().any(FileHandle::is_deleted) {
                self.worker
                    .publish(&mut state, self.key.clone(), value.clone());
                "published"
            } else {
                "late_fill_skipped"
            }
        } else {
            "load_skipped"
        };
        RANGE_CACHE_WORKER_EVENTS.with_label_values(&[event]).inc();
        // Fan out independently of Moka admission, including to old snapshots.
        // Do not hold the coordinator lock while notifying readers.
        drop(state);
        let _ = flight.sender.send(value);
    }
}

impl Drop for LoadGuard {
    fn drop(&mut self) {
        let mut state = self.worker.state.lock().unwrap();
        if state
            .flights
            .get(&self.key)
            .is_some_and(|flight| flight.generation == self.generation)
        {
            state.flights.remove(&self.key);
            RANGE_CACHE_WORKER_EVENTS
                .with_label_values(&["abandoned"])
                .inc();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use tokio::sync::oneshot;

    use super::*;
    use crate::cache::test_util::range_cache_key;
    use crate::sst::file::FileMeta;
    use crate::sst::file_purger::NoopFilePurger;

    fn empty_result() -> Arc<RangeScanCacheValue> {
        Arc::new(RangeScanCacheValue::new(vec![], 0))
    }

    fn receiver(lookup: RangeCacheLookup) -> watch::Receiver<SharedResult> {
        match lookup {
            RangeCacheLookup::Loading(receiver) => receiver,
            _ => panic!("expected a shared load"),
        }
    }

    async fn loaded(mut receiver: watch::Receiver<SharedResult>) -> SharedResult {
        tokio::time::timeout(Duration::from_secs(5), async {
            receiver
                .wait_for(|value| value.is_some())
                .await
                .ok()
                .and_then(|value| value.clone())
        })
        .await
        .expect("load must finish or cancel")
    }

    #[tokio::test]
    async fn same_key_misses_share_one_load_even_after_first_reader_leaves() {
        let worker = RangeCacheWorker::new(1024 * 1024);
        let key = range_cache_key(&[FileId::random()]);
        let count = Arc::new(AtomicUsize::new(0));
        let (release, gate) = oneshot::channel();
        let first_count = count.clone();
        let first = receiver(worker.get_or_load(key.clone(), vec![], async move {
            first_count.fetch_add(1, Ordering::Relaxed);
            gate.await.unwrap();
            Some(empty_result())
        }));
        let mut followers = Vec::new();
        for _ in 0..16 {
            let count = count.clone();
            followers.push(receiver(worker.get_or_load(
                key.clone(),
                vec![],
                async move {
                    count.fetch_add(1, Ordering::Relaxed);
                    Some(empty_result())
                },
            )));
        }
        drop(first);
        release.send(()).unwrap();
        let mut results = Vec::new();
        for follower in followers {
            results.push(loaded(follower).await.unwrap());
        }
        assert_eq!(count.load(Ordering::Relaxed), 1);
        assert!(results.iter().all(|value| Arc::ptr_eq(value, &results[0])));
        assert!(Arc::ptr_eq(&worker.get(&key).unwrap(), &results[0]));
    }

    #[test]
    fn retirement_removes_all_dependent_keys_but_preserves_other_files() {
        let worker = RangeCacheWorker::new(1024 * 1024);
        let a = FileId::random();
        let b = FileId::random();
        let combined = range_cache_key(&[a, b, a]);
        let mut copied = range_cache_key(&[a]);
        copied.region_id = store_api::storage::RegionId::new(2, 1);
        let unaffected = range_cache_key(&[b]);
        for key in [&combined, &copied, &unaffected] {
            worker.insert(key.clone(), empty_result());
        }
        worker.invalidate_file(a);
        worker.invalidate_file(a);
        assert!(worker.get(&combined).is_none());
        assert!(worker.get(&copied).is_none());
        assert!(worker.get(&unaffected).is_some());
        worker.invalidate_file(b);
        assert!(worker.get(&unaffected).is_none());
        assert!(worker.state.lock().unwrap().files.is_empty());
    }

    #[test]
    fn delayed_replacement_eviction_cannot_hide_a_new_entry_from_invalidation() {
        let worker = RangeCacheWorker::new(1024 * 1024);
        let file = FileId::random();
        let key = range_cache_key(&[file]);
        worker.insert(key.clone(), empty_result());
        worker.cache.run_pending_tasks();
        let replacement = empty_result();
        worker.insert(key.clone(), replacement.clone());
        worker.cache.run_pending_tasks();
        assert!(Arc::ptr_eq(&worker.get(&key).unwrap(), &replacement));
        worker.invalidate_file(file);
        assert!(worker.get(&key).is_none());
    }

    #[tokio::test]
    async fn retired_inflight_result_serves_old_readers_without_refilling() {
        let worker = RangeCacheWorker::new(1024 * 1024);
        let file = FileId::random();
        let key = range_cache_key(&[file]);
        let (release, gate) = oneshot::channel();
        let pending = receiver(worker.get_or_load(key.clone(), vec![], async move {
            gate.await.unwrap();
            Some(empty_result())
        }));
        worker.invalidate_file(file);
        release.send(()).unwrap();
        assert!(loaded(pending).await.is_some());
        assert!(worker.get(&key).is_none());
    }

    #[test]
    fn old_snapshot_cannot_start_a_fill_after_retirement() {
        let worker = RangeCacheWorker::new(1024 * 1024);
        let file = FileHandle::new(FileMeta::default(), Arc::new(NoopFilePurger));
        file.mark_deleted();
        let key = range_cache_key(&[file.file_id().file_id()]);
        assert!(matches!(
            worker.get_or_load(key, vec![file], async { Some(empty_result()) }),
            RangeCacheLookup::Bypass
        ));
    }

    #[tokio::test]
    async fn admission_rejection_does_not_discard_the_shared_result() {
        let worker = RangeCacheWorker::new(1);
        let key = range_cache_key(&[FileId::random()]);
        let pending =
            receiver(worker.get_or_load(key.clone(), vec![], async { Some(empty_result()) }));
        assert!(loaded(pending).await.is_some());
        worker.cache.run_pending_tasks();
        assert!(worker.get(&key).is_none());
        let mut state = worker.state.lock().unwrap();
        worker.drain_evictions(&mut state);
        assert!(state.files.is_empty());
    }

    #[tokio::test]
    async fn failed_load_releases_followers_and_allows_a_retry() {
        let worker = RangeCacheWorker::new(1024 * 1024);
        let key = range_cache_key(&[FileId::random()]);
        let failed = receiver(worker.get_or_load(key.clone(), vec![], async { None }));
        assert!(loaded(failed).await.is_none());
        let retry = receiver(worker.get_or_load(key, vec![], async { Some(empty_result()) }));
        assert!(loaded(retry).await.is_some());
    }

    #[tokio::test]
    async fn abandoning_all_subscribers_cancels_and_unregisters_the_load() {
        let worker = RangeCacheWorker::new(1024 * 1024);
        let key = range_cache_key(&[FileId::random()]);
        let (started, start_rx) = oneshot::channel();
        let marker = Arc::new(());
        let weak_marker = Arc::downgrade(&marker);
        let pending = receiver(worker.get_or_load(key.clone(), vec![], async move {
            let _marker = marker;
            started.send(()).unwrap();
            std::future::pending().await
        }));
        start_rx.await.unwrap();
        drop(pending);
        tokio::time::timeout(Duration::from_secs(5), async {
            while weak_marker.upgrade().is_some()
                || !worker.state.lock().unwrap().flights.is_empty()
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cancelled loaders must release resources and the flight key");
        assert!(worker.get(&key).is_none());
        let retry = receiver(worker.get_or_load(key, vec![], async { Some(empty_result()) }));
        assert!(loaded(retry).await.is_some());
    }

    #[tokio::test]
    async fn distinct_loads_are_bounded_without_queueing_misses() {
        let worker = RangeCacheWorker::new(1024 * 1024);
        let mut subscribers = Vec::new();
        for _ in 0..MAX_IN_FLIGHT_RANGES {
            subscribers.push(receiver(worker.get_or_load(
                range_cache_key(&[FileId::random()]),
                vec![],
                std::future::pending(),
            )));
        }
        assert!(matches!(
            worker.get_or_load(range_cache_key(&[FileId::random()]), vec![], async {
                Some(empty_result())
            }),
            RangeCacheLookup::Bypass
        ));
        drop(subscribers);
    }
}
