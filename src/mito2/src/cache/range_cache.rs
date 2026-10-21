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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak, mpsc};

use moka::sync::Cache;
use store_api::storage::FileId;
use tokio::sync::{OnceCell, OwnedSemaphorePermit};

use crate::cache::{
    RANGE_RESULT_TYPE, RangeResultMemoryLimiter, range_result_cache_weight, removal_cause_str,
};
use crate::metrics::{CACHE_BYTES, CACHE_EVICTION, RANGE_CACHE_WORKER_EVENTS};
use crate::read::range_cache::shared_range::SharedRange;
use crate::read::range_cache::{RangeScanCacheKey, RangeScanCacheValue};
use crate::sst::file::FileHandle;

/// Admission, not a queue: excess distinct misses keep using ordinary scans.
const MAX_IN_FLIGHT_RANGES: usize = 32;

type SharedResult = Option<Arc<RangeCacheDelivery>>;

/// Keeps completed results charged while subscribers or replay streams own them.
/// Cache entries retain only a weak reference: after the last reader leaves,
/// the resident value is covered solely by the cache's capacity accounting.
pub(crate) struct RangeCacheDelivery {
    value: Arc<RangeScanCacheValue>,
    _permit: OwnedSemaphorePermit,
}

impl RangeCacheDelivery {
    /// Shares a decoded result together with its memory reservation.
    pub(crate) fn new(value: Arc<RangeScanCacheValue>, permit: OwnedSemaphorePermit) -> Arc<Self> {
        Arc::new(Self {
            value,
            _permit: permit,
        })
    }

    /// Borrows the result without detaching it from the delivery reservation.
    pub(crate) fn value(&self) -> &RangeScanCacheValue {
        &self.value
    }
}

pub(crate) enum RangeCacheLookup {
    Hit(Arc<RangeCacheDelivery>),
    Loading(Arc<RangeLoad>),
    Bypass,
}

struct Entry {
    generation: u64,
    value: Arc<RangeScanCacheValue>,
    delivery: Mutex<Weak<RangeCacheDelivery>>,
}

impl Entry {
    fn deliver(&self, memory: &RangeResultMemoryLimiter) -> SharedResult {
        // Serialize cache-hit reservations so readers of the same result share
        // one charge, including when the last previous reader just left.
        let mut shared_delivery = self
            .delivery
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(delivery) = shared_delivery.upgrade() {
            return Some(delivery);
        }
        let permit = memory.try_resize(None, self.value.estimated_size())?;
        let delivery = RangeCacheDelivery::new(self.value.clone(), permit);
        *shared_delivery = Arc::downgrade(&delivery);
        Some(delivery)
    }
}

type FlightKey = (RangeScanCacheKey, usize);

/// Readers own the scan; the coordinator only retains a weak registration.
/// Query-pool identity isolates active reservations, not completed cache values.
pub(crate) struct RangeLoad {
    pub(crate) result: OnceCell<(Arc<SharedRange>, usize)>,
    key: FlightKey,
    generation: u64,
    files: Vec<FileHandle>,
    invalidated: AtomicBool,
}

#[derive(Default)]
struct State {
    generation: u64,
    flights: HashMap<FlightKey, Weak<RangeLoad>>,
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
    cache: Cache<RangeScanCacheKey, Arc<Entry>>,
    state: Mutex<State>,
    /// Moka can call listeners during a cache operation. Queue only cleanup
    /// metadata instead of reentering `state` from its eviction listener.
    evictions: Mutex<mpsc::Receiver<(Arc<RangeScanCacheKey>, u64)>>,
    /// Covers loads and completed deliveries, separately from cache residency
    /// and the temporary concat-memory limiter. Never wait for this budget.
    pub(crate) load_memory: Arc<RangeResultMemoryLimiter>,
}

impl RangeCacheWorker {
    pub(crate) fn new(capacity: u64) -> Arc<Self> {
        let (tx, rx) = mpsc::channel();
        let cache = Cache::builder()
            .max_capacity(capacity)
            .weigher(|k: &RangeScanCacheKey, v: &Arc<Entry>| range_result_cache_weight(k, &v.value))
            .eviction_listener(move |key, entry: Arc<Entry>, cause| {
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
            load_memory: Arc::new(RangeResultMemoryLimiter::new(capacity as usize, 1024)),
        })
    }

    fn drain_evictions(&self, state: &mut State) {
        for (key, generation) in self.evictions.lock().unwrap().try_iter() {
            state.forget(&key, generation);
        }
    }

    #[cfg(test)]
    pub(crate) fn get(&self, key: &RangeScanCacheKey) -> Option<Arc<RangeScanCacheValue>> {
        self.cache.get(key).map(|entry| entry.value.clone())
    }

    fn get_for_delivery(&self, key: &RangeScanCacheKey) -> Option<RangeCacheLookup> {
        let entry = self.cache.get(key)?;
        // A hit without delivery budget must bypass, not start another load.
        Some(match entry.deliver(&self.load_memory) {
            Some(delivery) => RangeCacheLookup::Hit(delivery),
            None => {
                RANGE_CACHE_WORKER_EVENTS
                    .with_label_values(&["bypass_memory"])
                    .inc();
                RangeCacheLookup::Bypass
            }
        })
    }

    /// For test/benchmark producers that do not hold live SST handles.
    #[cfg(test)]
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
        self.cache.insert(
            key,
            Arc::new(Entry {
                generation,
                value,
                delivery: Mutex::new(Weak::new()),
            }),
        );
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
        state.flights.retain(|(key, _), flight| {
            let Some(flight) = flight.upgrade() else {
                return false;
            };
            if key.row_groups.iter().any(|(id, _)| *id == file) {
                flight.invalidated.store(true, Ordering::Relaxed);
            }
            true
        });
        self.drain_evictions(&mut state);
    }

    /// Looks up a retained result or registers one reader-owned streaming scan.
    pub(crate) fn get_or_load(
        self: &Arc<Self>,
        key: RangeScanCacheKey,
        files: Vec<FileHandle>,
        memory_pool_id: usize,
    ) -> RangeCacheLookup {
        let mut state = self.state.lock().unwrap();
        self.drain_evictions(&mut state);
        if files.iter().any(FileHandle::is_deleted) {
            RANGE_CACHE_WORKER_EVENTS
                .with_label_values(&["bypass_retired"])
                .inc();
            return RangeCacheLookup::Bypass;
        }
        if let Some(lookup) =
            crate::cache::update_hit_miss(self.get_for_delivery(&key), RANGE_RESULT_TYPE)
        {
            return lookup;
        }
        // Weak entries never keep readers alive. Prune before applying admission.
        state.flights.retain(|_, load| load.strong_count() != 0);
        let scoped_key = (key, memory_pool_id);
        if let Some(load) = state.flights.get(&scoped_key).and_then(Weak::upgrade) {
            RANGE_CACHE_WORKER_EVENTS
                .with_label_values(&["coalesced"])
                .inc();
            return RangeCacheLookup::Loading(load);
        }
        if state.flights.len() >= MAX_IN_FLIGHT_RANGES {
            RANGE_CACHE_WORKER_EVENTS
                .with_label_values(&["bypass_capacity"])
                .inc();
            return RangeCacheLookup::Bypass;
        }
        let load = Arc::new(RangeLoad {
            result: OnceCell::new(),
            key: scoped_key.clone(),
            generation: state.next_generation(),
            files,
            invalidated: AtomicBool::new(false),
        });
        state.flights.insert(scoped_key, Arc::downgrade(&load));
        RANGE_CACHE_WORKER_EVENTS
            .with_label_values(&["started"])
            .inc();
        RangeCacheLookup::Loading(load)
    }

    /// Publishes only the current, successful scan of live files. Existing readers
    /// may still finish an invalidated scan, but it cannot refill the cache.
    pub(crate) fn complete(&self, load: &RangeLoad, value: Arc<RangeScanCacheValue>) -> bool {
        // Streaming readers already own their buffer reservations. Cache-only
        // residency needs no delivery reservation; cache hits acquire one later.
        let mut state = self.state.lock().unwrap();
        self.drain_evictions(&mut state);
        let current = state.flights.get(&load.key).and_then(Weak::upgrade);
        if !current.is_some_and(|current| current.generation == load.generation) {
            return false;
        }
        if load.invalidated.load(Ordering::Relaxed) || load.files.iter().any(FileHandle::is_deleted)
        {
            RANGE_CACHE_WORKER_EVENTS
                .with_label_values(&["late_fill_skipped"])
                .inc();
            return false;
        }
        self.publish(&mut state, load.key.0.clone(), value);
        RANGE_CACHE_WORKER_EVENTS
            .with_label_values(&["published"])
            .inc();
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::test_util::range_cache_key;
    use crate::sst::file::FileMeta;
    use crate::sst::file_purger::NoopFilePurger;

    fn empty_result() -> Arc<RangeScanCacheValue> {
        Arc::new(RangeScanCacheValue::new(vec![], 0))
    }

    fn loading(lookup: RangeCacheLookup) -> Arc<RangeLoad> {
        let RangeCacheLookup::Loading(load) = lookup else {
            panic!("expected active scan")
        };
        load
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

    #[test]
    fn cache_hits_share_delivery_budget_until_the_last_reader_leaves() {
        let worker = RangeCacheWorker::new(1024);
        let file = FileId::random();
        let key = range_cache_key(&[file]);
        worker.insert(key.clone(), empty_result());
        assert_eq!(worker.load_memory.available_permits(), 1);

        let blocker = worker.load_memory.try_acquire(1024).unwrap();
        assert!(matches!(
            worker.get_or_load(key.clone(), vec![], 0),
            RangeCacheLookup::Bypass
        ));
        assert!(worker.get(&key).is_some());
        drop(blocker);

        let RangeCacheLookup::Hit(first) = worker.get_or_load(key.clone(), vec![], 0) else {
            panic!("expected an accounted cache hit");
        };
        let RangeCacheLookup::Hit(second) = worker.get_or_load(key.clone(), vec![], 0) else {
            panic!("readers of the same value must share its reservation");
        };
        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(worker.load_memory.available_permits(), 0);

        worker.invalidate_file(file);
        worker.cache.run_pending_tasks();
        assert!(worker.get(&key).is_none());
        drop(first);
        assert_eq!(worker.load_memory.available_permits(), 0);
        drop(second);
        assert_eq!(worker.load_memory.available_permits(), 1);
    }

    #[test]
    fn streaming_registration_is_scoped_and_reader_owned() {
        let worker = RangeCacheWorker::new(1024 * 1024);
        let key = range_cache_key(&[FileId::random()]);
        let first = loading(worker.get_or_load(key.clone(), vec![], 1));
        let second = loading(worker.get_or_load(key.clone(), vec![], 1));
        assert!(Arc::ptr_eq(&first, &second));
        let other_pool = loading(worker.get_or_load(key.clone(), vec![], 2));
        assert!(!Arc::ptr_eq(&first, &other_pool));
        let weak = Arc::downgrade(&first);
        drop(first);
        assert!(weak.upgrade().is_some());
        drop(second);
        assert!(weak.upgrade().is_none());
        let retry = loading(worker.get_or_load(key.clone(), vec![], 1));
        // Publication must not reserve the active source's buffer twice.
        let source_permit = worker.load_memory.try_acquire(1024 * 1024).unwrap();
        assert!(worker.complete(&retry, empty_result()));
        assert!(worker.get(&key).is_some());
        assert!(matches!(
            worker.get_or_load(key.clone(), vec![], 3),
            RangeCacheLookup::Bypass
        ));
        drop(source_permit);
        assert!(matches!(
            worker.get_or_load(key, vec![], 3),
            RangeCacheLookup::Hit(_)
        ));
    }

    #[test]
    fn retirement_blocks_late_publication_and_old_snapshot_registration() {
        for mark_handle in [false, true] {
            let worker = RangeCacheWorker::new(1024 * 1024);
            let file = FileHandle::new(FileMeta::default(), Arc::new(NoopFilePurger));
            let key = range_cache_key(&[file.file_id().file_id()]);
            let load = loading(worker.get_or_load(key.clone(), vec![file.clone()], 0));
            if mark_handle {
                file.mark_deleted();
            } else {
                worker.invalidate_file(file.file_id().file_id());
            }
            worker.complete(&load, empty_result());
            assert!(worker.get(&key).is_none());
            if mark_handle {
                assert!(matches!(
                    worker.get_or_load(key, vec![file], 0),
                    RangeCacheLookup::Bypass
                ));
            }
        }
    }

    #[test]
    fn active_scan_limit_prunes_abandoned_registrations() {
        let worker = RangeCacheWorker::new(1024 * 1024);
        let loads: Vec<_> = (0..MAX_IN_FLIGHT_RANGES)
            .map(|_| loading(worker.get_or_load(range_cache_key(&[FileId::random()]), vec![], 0)))
            .collect();
        let key = range_cache_key(&[FileId::random()]);
        assert!(matches!(
            worker.get_or_load(key.clone(), vec![], 0),
            RangeCacheLookup::Bypass
        ));
        drop(loads);
        assert!(matches!(
            worker.get_or_load(key, vec![], 0),
            RangeCacheLookup::Loading(_)
        ));
    }
    #[rstest::rstest]
    #[tokio::test]
    async fn index_replacement_retirement_invalidates_and_blocks_old_reader_fills(
        #[values(false, true)] gc_enabled: bool,
    ) {
        use common_base::readable_size::ReadableSize;
        use store_api::region_engine::RegionEngine;
        use store_api::region_request::RegionRequest;
        use store_api::storage::RegionId;

        use crate::config::MitoConfig;
        use crate::gc::GcConfig;
        use crate::sst::file::IndexType;
        use crate::sst::version::SstVersion;
        use crate::test_util::{CreateRequestBuilder, TestEnv};

        let mut env = TestEnv::new().await;
        let engine = env
            .create_engine(MitoConfig {
                range_result_cache_size: ReadableSize::mb(1),
                gc: GcConfig {
                    enable: gc_enabled,
                    ..Default::default()
                },
                ..Default::default()
            })
            .await;
        let region_id = RegionId::new(1, 1);
        engine
            .handle_request(
                region_id,
                RegionRequest::Create(CreateRequestBuilder::new().build()),
            )
            .await
            .unwrap();
        let region = engine.get_region(region_id).unwrap();
        let cache = engine.cache_manager();
        let worker = cache.range_result_cache.as_ref().unwrap();
        let meta = FileMeta {
            region_id,
            file_id: FileId::random(),
            ..Default::default()
        };
        let mut version = SstVersion::new(crate::test_util::memtable_util::metadata_for_test());
        version.add_files(region.file_purger.clone(), [meta.clone()].into_iter());
        let old_reader = version.levels()[0].files[&meta.file_id].clone();
        let key = range_cache_key(&[meta.file_id]);
        worker.insert(key.clone(), empty_result());
        assert!(worker.get(&key).is_some());
        let loading_key = key.clone().for_cross_range_merge();
        let pending = loading(worker.get_or_load(loading_key.clone(), vec![old_reader.clone()], 0));
        let updated = FileMeta {
            index_version: 1,
            available_indexes: smallvec::smallvec![IndexType::InvertedIndex],
            ..meta
        };
        version.add_files(region.file_purger.clone(), [updated.clone()].into_iter());
        version.remove_files([updated].into_iter());

        // Neither an existing scan nor a new request through the old
        // index handle may refill a range after the current handle is retired.
        assert!(old_reader.is_deleted());
        assert!(worker.get(&key).is_none());
        assert!(matches!(
            worker.get_or_load(key, vec![old_reader], 0),
            RangeCacheLookup::Bypass
        ));
        assert!(!worker.complete(&pending, empty_result()));
        assert!(worker.get(&loading_key).is_none());
    }
}
