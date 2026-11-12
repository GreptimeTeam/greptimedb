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

mod shared_range;

use std::collections::HashMap;
use std::future::Future;
use std::mem;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak, mpsc};

use async_stream::try_stream;
use common_cache::SyncCache;
use common_telemetry::warn;
use datafusion::execution::memory_pool::MemoryPool;
use datatypes::arrow::compute::concat_batches;
use datatypes::arrow::record_batch::RecordBatch;
use snafu::{IntoError, ResultExt};
use store_api::storage::FileId;
use tokio::sync::{OnceCell, OwnedSemaphorePermit};

use crate::cache::range_cache::shared_range::{RangeRead, SharedRange};
use crate::cache::{RANGE_RESULT_TYPE, RangeResultMemoryLimiter, range_result_cache_weight};
use crate::error::{ComputeArrowSnafu, Result};
use crate::metrics::{CACHE_BYTES, CACHE_EVICTION, RANGE_CACHE_WORKER_EVENTS};
use crate::read::BoxedRecordBatchStream;
use crate::read::range_cache::RangeScanCacheKey;
use crate::sst::file::FileHandle;
use crate::sst::parquet::DEFAULT_READ_BATCH_SIZE;

pub(crate) const RANGE_CACHE_COMPACT_THRESHOLD_BYTES: usize = 8 * 1024 * 1024;

/// Separates active scans with different query memory reservations.
/// Completed results are reusable across scopes. The default scope is for
/// sources without a query-specific memory pool.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub(crate) struct RangeCacheScope(usize);

impl RangeCacheScope {
    /// Uses the identity of the pool retained by the source scan.
    pub(crate) fn for_memory_pool(pool: &Arc<dyn MemoryPool>) -> Self {
        Self(Arc::as_ptr(pool) as *const () as usize)
    }
}

/// Reports cache activity to the query that owns the reader or publication.
pub(crate) trait RangeCacheMetricsReport: Send + Sync {
    /// Records a completed-result hit for this reader.
    fn inc_range_cache_hit(&self);
    /// Records a reader without a deliverable completed result.
    fn inc_range_cache_miss(&self);
    /// Records bytes published by the reader that finishes the shared scan.
    fn inc_range_cache_size(&self, bytes: usize);
}

/// Compacted batches with their original stream boundaries and memory weight.
pub(crate) struct RangeScanCacheValue {
    cached_batches: Vec<CachedBatchSlice>,
    estimated_batch_size: usize,
    /// Precomputed size of all compacted batches.
    estimated_batches_size: usize,
}

impl RangeScanCacheValue {
    #[cfg(test)]
    pub(crate) fn empty() -> Self {
        Self::new(Vec::new(), 0)
    }

    #[cfg(test)]
    pub(crate) fn cached_batches(&self) -> impl Iterator<Item = (&RecordBatch, &[usize])> {
        self.cached_batches
            .iter()
            .map(|batch| (&batch.batch, batch.slice_lengths.as_slice()))
    }

    #[cfg(test)]
    pub(crate) fn estimated_batches_size(&self) -> usize {
        self.estimated_batches_size
    }

    fn new(cached_batches: Vec<CachedBatchSlice>, estimated_batches_size: usize) -> Self {
        Self {
            cached_batches,
            estimated_batch_size: DEFAULT_READ_BATCH_SIZE,
            estimated_batches_size,
        }
    }

    pub(crate) fn estimated_size(&self) -> usize {
        mem::size_of::<Self>()
            + self.cached_batches.capacity() * mem::size_of::<CachedBatchSlice>()
            + self
                .cached_batches
                .iter()
                .map(CachedBatchSlice::metadata_size)
                .sum::<usize>()
            + self.estimated_batches_size
    }
}

/// Stores completed range results and coordinates reader-owned shared scans.
pub(crate) struct RangeResultCache {
    cache: SyncCache<RangeScanCacheKey, Arc<Entry>>,
    state: Mutex<State>,
    /// Eviction callbacks and the last scan owner can run while `state` is locked.
    /// Queue their cleanup instead of reentering the coordinator lock.
    cleanup_tx: mpsc::Sender<RangeCacheCleanup>,
    cleanup_rx: Mutex<mpsc::Receiver<RangeCacheCleanup>>,
    /// Covers loads and completed deliveries, separately from cache residency
    /// and the temporary concat-memory limiter. Never wait for this budget.
    retention_memory: Arc<RangeResultMemoryLimiter>,
    /// Bounds temporary allocations while compacting a completed result.
    concat_memory: Arc<RangeResultMemoryLimiter>,
}

impl RangeResultCache {
    #[cfg(test)]
    pub(crate) fn retention_memory(&self) -> &Arc<RangeResultMemoryLimiter> {
        &self.retention_memory
    }

    #[cfg(test)]
    pub(crate) fn concat_memory(&self) -> &Arc<RangeResultMemoryLimiter> {
        &self.concat_memory
    }

    /// Replays a cached result or subscribes to one reader-owned streaming scan.
    #[tracing::instrument(name = "range_result_lookup", skip_all)]
    pub(crate) async fn read<F, Fut>(
        self: &Arc<Self>,
        key: RangeScanCacheKey,
        part_metrics: Arc<dyn RangeCacheMetricsReport>,
        files: Vec<FileHandle>,
        scope: RangeCacheScope,
        build: F,
    ) -> Result<(BoxedRecordBatchStream, usize)>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<(BoxedRecordBatchStream, usize)>>,
    {
        let cache = self;
        let load = match cache.get_or_register(key, files, scope) {
            RangeCacheLookup::Hit(value) => {
                part_metrics.inc_range_cache_hit();
                let batch_size = value.value().estimated_batch_size;
                return Ok((cached_flat_range_stream(value), batch_size));
            }
            RangeCacheLookup::Loading(load) => load,
            RangeCacheLookup::Bypass => {
                part_metrics.inc_range_cache_miss();
                return build().await;
            }
        };
        part_metrics.inc_range_cache_miss();
        let cache = cache.clone();
        let limiter = cache.retention_memory.clone();
        let mut build = Some(build);
        let mut owner_cursor = None;
        let (shared, batch_size) = load
            .shared_range
            .get_or_try_init(|| async {
                let builder = build.take().ok_or_else(|| {
                    crate::error::UnexpectedSnafu {
                        reason: "range scan builder already consumed",
                    }
                    .build()
                })?;
                let (source, batch_size) = Box::pin(builder()).await?;
                let shared = SharedRange::new(source, limiter.clone());
                owner_cursor = shared.subscribe().await;
                Ok::<_, crate::error::Error>((shared, batch_size))
            })
            .await?;
        let shared = shared.clone();
        let batch_size = *batch_size;
        let cursor = match owner_cursor {
            Some(cursor) => Some(cursor),
            None => shared.subscribe().await,
        };
        let Some(mut cursor) = cursor else {
            // An oversized range has closed admission. This caller has not emitted
            // any rows, so its original ordinary stream remains a safe fallback.
            let builder = build.ok_or_else(|| {
                crate::error::UnexpectedSnafu {
                    reason: "range scan owner lost its initial cursor",
                }
                .build()
            })?;
            return builder().await;
        };
        let metrics = part_metrics;
        let stream = Box::pin(try_stream! {
            // Keep the weak registry entry alive until this reader finishes.
            let load = load;
            loop {
                match shared.read(&mut cursor).await {
                    RangeRead::Batch(batch) => yield batch,
                    RangeRead::Failed(error) => {
                        Err(crate::error::LoadRangeResultSnafu.into_error(error))?;
                    }
                    RangeRead::Finished => {
                        if let Some(batches) = shared.take_complete_batches().await {
                            // Finalization owns the registration until publication, so
                            // a new caller can still join during background compaction.
                            common_runtime::spawn_query(cache.finish_scan(load, batches, batch_size, metrics));
                        }
                        break;
                    }
                }
            }
        });
        Ok((stream, batch_size))
    }

    /// Creates independent retention and compaction budgets with the cache capacity.
    pub(crate) fn new(capacity: u64) -> Arc<Self> {
        let (tx, rx) = mpsc::channel();
        let eviction_tx = tx.clone();
        let cache = SyncCache::new(
            RANGE_RESULT_TYPE,
            capacity,
            |k: &RangeScanCacheKey, v: &Arc<Entry>| range_result_cache_weight(k, &v.value),
            move |key, entry: Arc<Entry>, cause| {
                CACHE_BYTES
                    .with_label_values(&[RANGE_RESULT_TYPE])
                    .sub(range_result_cache_weight(&key, &entry.value).into());
                CACHE_EVICTION
                    .with_label_values(&[RANGE_RESULT_TYPE, cause.as_str()])
                    .inc();
                let _ = eviction_tx.send(RangeCacheCleanup::Evicted {
                    key,
                    generation: entry.generation,
                });
            },
        );
        Arc::new(Self {
            cache,
            state: Mutex::new(State::default()),
            cleanup_tx: tx,
            cleanup_rx: Mutex::new(rx),
            retention_memory: Arc::new(RangeResultMemoryLimiter::new(capacity as usize, 1024)),
            concat_memory: Arc::new(RangeResultMemoryLimiter::new(
                capacity as usize,
                crate::cache::RANGE_RESULT_CONCAT_MEMORY_PERMIT.as_bytes() as usize,
            )),
        })
    }

    /// Compacts a completed scan, then publishes it under the retirement fence.
    async fn finish_scan(
        self: Arc<Self>,
        load: Arc<RangeLoad>,
        batches: Vec<CachedBatchSlice>,
        batch_size: usize,
        metrics: Arc<dyn RangeCacheMetricsReport>,
    ) {
        let bytes = batches
            .iter()
            .map(|cached| cached.batch.get_array_memory_size())
            .sum();
        let value = RangeScanCacheValue::new(batches, bytes);
        let mut compacted = CacheConcatState::default();
        if let Err(error) = compacted.compact_range(&value, &self.concat_memory).await {
            warn!(error; "Failed to compact shared range result");
            return;
        }
        drop(value);
        let mut value = compacted.finish();
        value.estimated_batch_size = batch_size;
        let bytes = load.key.0.estimated_size() + value.estimated_size();
        if self.complete(&load, Arc::new(value)) {
            metrics.inc_range_cache_size(bytes);
        }
    }

    fn drain_cleanup(&self, state: &mut State) {
        for cleanup in self.cleanup_rx.lock().unwrap().try_iter() {
            match cleanup {
                RangeCacheCleanup::Evicted { key, generation } => state.forget(&key, generation),
                RangeCacheCleanup::Released { key, generation } => {
                    if state
                        .flights
                        .get(&key)
                        .is_some_and(|entry| entry.generation == generation)
                    {
                        state.flights.remove(&key);
                    }
                }
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn get(&self, key: &RangeScanCacheKey) -> Option<Arc<RangeScanCacheValue>> {
        self.cache.get(key).map(|entry| entry.value.clone())
    }

    fn get_for_delivery(&self, key: &RangeScanCacheKey) -> Option<RangeCacheLookup> {
        let entry = self.cache.get(key)?;
        // A hit without delivery budget must bypass, not start another load.
        Some(match entry.deliver(&self.retention_memory) {
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
        self.drain_cleanup(&mut state);
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
        self.drain_cleanup(state);
    }

    /// Called at logical retirement, before the last snapshot releases the SST.
    /// Existing readers may finish, but their results must not refill this cache.
    pub(crate) fn invalidate_file(&self, file: FileId) {
        RANGE_CACHE_WORKER_EVENTS
            .with_label_values(&["invalidate"])
            .inc();
        let mut state = self.state.lock().unwrap();
        self.drain_cleanup(&mut state);
        if let Some(keys) = state.files.remove(&file) {
            for (key, generation) in keys {
                self.cache.invalidate(key.as_ref());
                state.forget(&key, generation);
            }
        }
        state.flights.retain(|(key, _), flight| {
            let Some(flight) = flight.load.upgrade() else {
                return false;
            };
            if key.row_groups.iter().any(|(id, _)| *id == file) {
                flight.invalidated.store(true, Ordering::Relaxed);
            }
            true
        });
        self.drain_cleanup(&mut state);
    }

    /// Looks up a retained result or registers one reader-owned streaming scan.
    fn get_or_register(
        self: &Arc<Self>,
        key: RangeScanCacheKey,
        files: Vec<FileHandle>,
        scope: RangeCacheScope,
    ) -> RangeCacheLookup {
        let mut state = self.state.lock().unwrap();
        self.drain_cleanup(&mut state);
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
        let scoped_key = (key, scope);
        if let Some(load) = state
            .flights
            .get(&scoped_key)
            .and_then(|entry| entry.load.upgrade())
        {
            RANGE_CACHE_WORKER_EVENTS
                .with_label_values(&["coalesced"])
                .inc();
            return RangeCacheLookup::Loading(load);
        }
        let load = Arc::new(RangeLoad {
            shared_range: OnceCell::new(),
            key: scoped_key.clone(),
            generation: state.next_generation(),
            files,
            invalidated: AtomicBool::new(false),
            cleanup_tx: self.cleanup_tx.clone(),
        });
        state.flights.insert(
            scoped_key,
            RangeRegistration {
                generation: load.generation,
                load: Arc::downgrade(&load),
            },
        );
        RANGE_CACHE_WORKER_EVENTS
            .with_label_values(&["started"])
            .inc();
        RangeCacheLookup::Loading(load)
    }

    /// Publishes only the current, successful scan of live files. Existing readers
    /// may still finish an invalidated scan, but it cannot refill the cache.
    fn complete(&self, load: &RangeLoad, value: Arc<RangeScanCacheValue>) -> bool {
        // Streaming readers already own their buffer reservations. Cache-only
        // residency needs no delivery reservation; cache hits acquire one later.
        let mut state = self.state.lock().unwrap();
        self.drain_cleanup(&mut state);
        let current = state
            .flights
            .get(&load.key)
            .and_then(|entry| entry.load.upgrade());
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

/// Cached result for one range scan.
#[derive(Debug, Clone)]
struct CachedBatchSlice {
    batch: RecordBatch,
    slice_lengths: Vec<usize>,
}

impl CachedBatchSlice {
    fn metadata_size(&self) -> usize {
        self.slice_lengths.capacity() * mem::size_of::<usize>()
    }
}

/// Keeps completed results charged while subscribers or replay streams own them.
/// Cache entries retain only a weak reference: after the last reader leaves,
/// the resident value is covered solely by the cache's capacity accounting.
struct RangeCacheDelivery {
    value: Arc<RangeScanCacheValue>,
    _permit: OwnedSemaphorePermit,
}

impl RangeCacheDelivery {
    /// Shares a decoded result together with its memory reservation.
    fn new(value: Arc<RangeScanCacheValue>, permit: OwnedSemaphorePermit) -> Arc<Self> {
        Arc::new(Self {
            value,
            _permit: permit,
        })
    }

    /// Borrows the result without detaching it from the delivery reservation.
    fn value(&self) -> &RangeScanCacheValue {
        &self.value
    }
}

enum RangeCacheLookup {
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
    fn deliver(&self, memory: &RangeResultMemoryLimiter) -> Option<Arc<RangeCacheDelivery>> {
        // Serialize cache-hit reservations so readers of the same result share
        // one charge, including when the last previous reader just left.
        let mut shared_delivery = self
            .delivery
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(delivery) = shared_delivery.upgrade() {
            return Some(delivery);
        }
        let permit = memory.try_acquire(self.value.estimated_size())?;
        let delivery = RangeCacheDelivery::new(self.value.clone(), permit);
        *shared_delivery = Arc::downgrade(&delivery);
        Some(delivery)
    }
}

type FlightKey = (RangeScanCacheKey, RangeCacheScope);

/// Readers own the scan; the coordinator only retains a weak registration.
/// Query-pool identity isolates active reservations, not completed cache values.
struct RangeLoad {
    shared_range: OnceCell<(Arc<SharedRange>, usize)>,
    key: FlightKey,
    generation: u64,
    files: Vec<FileHandle>,
    invalidated: AtomicBool,
    cleanup_tx: mpsc::Sender<RangeCacheCleanup>,
}

impl Drop for RangeLoad {
    fn drop(&mut self) {
        let _ = self.cleanup_tx.send(RangeCacheCleanup::Released {
            key: self.key.clone(),
            generation: self.generation,
        });
    }
}

struct RangeRegistration {
    generation: u64,
    load: Weak<RangeLoad>,
}

enum RangeCacheCleanup {
    Evicted {
        key: Arc<RangeScanCacheKey>,
        generation: u64,
    },
    Released {
        key: FlightKey,
        generation: u64,
    },
}

#[derive(Default)]
struct State {
    generation: u64,
    flights: HashMap<FlightKey, RangeRegistration>,
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

/// Returns a stream that replays cached record batches.
fn cached_flat_range_stream(delivery: Arc<RangeCacheDelivery>) -> BoxedRecordBatchStream {
    Box::pin(try_stream! {
        // Borrow through the delivery guard so it survives every yield. Batches
        // handed to downstream operators are then owned by the query, not by
        // this completed-result backlog.
        for cached_batch in &delivery.value().cached_batches {
            let mut offset = 0;
            for &len in &cached_batch.slice_lengths {
                yield cached_batch.batch.slice(offset, len);
                offset += len;
            }
        }
    })
}

#[derive(Default)]
struct CacheConcatState {
    cached_batches: Vec<CachedBatchSlice>,
    estimated_size: usize,
}

impl CacheConcatState {
    async fn compact(
        &mut self,
        batches: Vec<RecordBatch>,
        limiter: &crate::cache::RangeResultMemoryLimiter,
    ) -> Result<()> {
        if batches.is_empty() {
            return Ok(());
        }

        let input_size = batches
            .iter()
            .map(RecordBatch::get_array_memory_size)
            .sum::<usize>();
        let _permit = limiter.acquire(input_size).await?;

        let compacted = compact_record_batches(batches)?;
        self.estimated_size += compacted.batch.get_array_memory_size();
        self.cached_batches.push(compacted);
        Ok(())
    }

    async fn compact_range(
        &mut self,
        value: &RangeScanCacheValue,
        limiter: &crate::cache::RangeResultMemoryLimiter,
    ) -> Result<()> {
        let mut pending = Vec::new();
        let mut rows = 0;
        let mut bytes = 0;
        for cached in &value.cached_batches {
            if cached.slice_lengths.len() > 1 {
                self.compact(mem::take(&mut pending), limiter).await?;
                self.estimated_size += cached.batch.get_array_memory_size();
                self.cached_batches.push(cached.clone());
                rows = 0;
                bytes = 0;
                continue;
            }
            rows += cached.batch.num_rows();
            bytes += cached.batch.get_array_memory_size();
            pending.push(cached.batch.clone());
            if should_compact_batches(pending.len(), rows, bytes) {
                self.compact(mem::take(&mut pending), limiter).await?;
                rows = 0;
                bytes = 0;
            }
        }
        self.compact(pending, limiter).await
    }

    fn finish(self) -> RangeScanCacheValue {
        RangeScanCacheValue::new(self.cached_batches, self.estimated_size)
    }
}

fn should_compact_batches(batches: usize, rows: usize, bytes: usize) -> bool {
    batches > 1 && (rows > DEFAULT_READ_BATCH_SIZE || bytes > RANGE_CACHE_COMPACT_THRESHOLD_BYTES)
}

fn compact_record_batches(mut batches: Vec<RecordBatch>) -> Result<CachedBatchSlice> {
    debug_assert!(!batches.is_empty());
    let slice_lengths = batches.iter().map(RecordBatch::num_rows).collect();
    if batches.len() == 1 {
        // Arrow's singleton concat aliases its input; a second empty batch lets
        // the concat kernel copy sliced data without adding a replay boundary.
        batches.push(batches[0].slice(0, 0));
    }
    let batch = concat_batches(&batches[0].schema(), &batches).context(ComputeArrowSnafu)?;
    Ok(CachedBatchSlice {
        batch,
        slice_lengths,
    })
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
    fn test_retirement_removes_all_dependent_keys_but_preserves_other_files() {
        let worker = RangeResultCache::new(1024 * 1024);
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
    fn test_delayed_replacement_eviction_cannot_hide_a_new_entry_from_invalidation() {
        let worker = RangeResultCache::new(1024 * 1024);
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
    fn test_cache_hits_share_delivery_budget_until_the_last_reader_leaves() {
        let worker = RangeResultCache::new(1024);
        let file = FileId::random();
        let key = range_cache_key(&[file]);
        worker.insert(key.clone(), empty_result());
        assert_eq!(worker.retention_memory.available_permits(), 1);

        let blocker = worker.retention_memory.try_acquire(1024).unwrap();
        assert!(matches!(
            worker.get_or_register(key.clone(), vec![], RangeCacheScope(0)),
            RangeCacheLookup::Bypass
        ));
        assert!(worker.get(&key).is_some());
        drop(blocker);

        let RangeCacheLookup::Hit(first) =
            worker.get_or_register(key.clone(), vec![], RangeCacheScope(0))
        else {
            panic!("expected an accounted cache hit");
        };
        let RangeCacheLookup::Hit(second) =
            worker.get_or_register(key.clone(), vec![], RangeCacheScope(0))
        else {
            panic!("readers of the same value must share its reservation");
        };
        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(worker.retention_memory.available_permits(), 0);

        worker.invalidate_file(file);
        worker.cache.run_pending_tasks();
        assert!(worker.get(&key).is_none());
        drop(first);
        assert_eq!(worker.retention_memory.available_permits(), 0);
        drop(second);
        assert_eq!(worker.retention_memory.available_permits(), 1);
    }

    #[test]
    fn test_streaming_registration_is_scoped_and_reader_owned() {
        let worker = RangeResultCache::new(1024 * 1024);
        let key = range_cache_key(&[FileId::random()]);
        let first = loading(worker.get_or_register(key.clone(), vec![], RangeCacheScope(1)));
        let second = loading(worker.get_or_register(key.clone(), vec![], RangeCacheScope(1)));
        assert!(Arc::ptr_eq(&first, &second));
        let other_pool = loading(worker.get_or_register(key.clone(), vec![], RangeCacheScope(2)));
        assert!(!Arc::ptr_eq(&first, &other_pool));
        let weak = Arc::downgrade(&first);
        drop(first);
        assert!(weak.upgrade().is_some());
        drop(second);
        assert!(weak.upgrade().is_none());
        let retry = loading(worker.get_or_register(key.clone(), vec![], RangeCacheScope(1)));
        // Publication must not reserve the active source's buffer twice.
        let source_permit = worker.retention_memory.try_acquire(1024 * 1024).unwrap();
        assert!(worker.complete(&retry, empty_result()));
        assert!(worker.get(&key).is_some());
        assert!(matches!(
            worker.get_or_register(key.clone(), vec![], RangeCacheScope(3)),
            RangeCacheLookup::Bypass
        ));
        drop(source_permit);
        assert!(matches!(
            worker.get_or_register(key, vec![], RangeCacheScope(3)),
            RangeCacheLookup::Hit(_)
        ));
    }

    #[test]
    fn test_retirement_blocks_late_publication_and_old_snapshot_registration() {
        for mark_handle in [false, true] {
            let worker = RangeResultCache::new(1024 * 1024);
            let file = FileHandle::new(FileMeta::default(), Arc::new(NoopFilePurger));
            let key = range_cache_key(&[file.file_id().file_id()]);
            let load = loading(worker.get_or_register(
                key.clone(),
                vec![file.clone()],
                RangeCacheScope(0),
            ));
            if mark_handle {
                file.mark_deleted();
            } else {
                worker.invalidate_file(file.file_id().file_id());
            }
            worker.complete(&load, empty_result());
            assert!(worker.get(&key).is_none());
            if mark_handle {
                assert!(matches!(
                    worker.get_or_register(key, vec![file], RangeCacheScope(0)),
                    RangeCacheLookup::Bypass
                ));
            }
        }
    }

    #[test]
    fn test_scan_release_cleans_only_the_last_owner_under_the_state_lock() {
        let cache = RangeResultCache::new(1024 * 1024);
        let key = range_cache_key(&[FileId::random()]);
        let first = loading(cache.get_or_register(key.clone(), vec![], RangeCacheScope(0)));
        let second = loading(cache.get_or_register(key.clone(), vec![], RangeCacheScope(0)));
        let weak = Arc::downgrade(&first);
        let mut state = cache.state.lock().unwrap();
        drop(first);
        cache.drain_cleanup(&mut state);
        assert_eq!(state.flights.len(), 1);
        // Last-owner release can happen while invalidation holds this lock.
        drop(second);
        assert!(weak.upgrade().is_none());
        cache.drain_cleanup(&mut state);
        assert!(state.flights.is_empty());
    }

    #[test]
    fn test_delayed_scan_release_keeps_a_replacement_registration() {
        let cache = RangeResultCache::new(1024 * 1024);
        let key = range_cache_key(&[FileId::random()]);
        let old = loading(cache.get_or_register(key.clone(), vec![], RangeCacheScope(0)));
        drop(old);
        let delayed = cache.cleanup_rx.lock().unwrap().try_recv().unwrap();
        // Reproduce release arriving after another caller has replaced the dead Weak.
        let new = loading(cache.get_or_register(key.clone(), vec![], RangeCacheScope(0)));
        cache.cleanup_tx.send(delayed).unwrap();
        let shared = loading(cache.get_or_register(key, vec![], RangeCacheScope(0)));
        assert!(Arc::ptr_eq(&new, &shared));
        drop(new);
        drop(shared);
        let mut state = cache.state.lock().unwrap();
        cache.drain_cleanup(&mut state);
        assert!(state.flights.is_empty());
    }

    #[rstest::rstest]
    fn test_distinct_active_scans_share_loads_and_prune_abandoned_registrations(
        #[values(512, 2048, 8192)] count: usize,
    ) {
        let worker = RangeResultCache::new(1024 * 1024);
        let keys: Vec<_> = (0..count)
            .map(|i| range_cache_key(&[FileId::from_bytes((i as u128).to_le_bytes())]))
            .collect();
        let loads: Vec<_> = keys
            .iter()
            .map(|key| loading(worker.get_or_register(key.clone(), vec![], RangeCacheScope(0))))
            .collect();
        for (key, load) in keys.iter().zip(&loads) {
            let shared = loading(worker.get_or_register(key.clone(), vec![], RangeCacheScope(0)));
            assert!(Arc::ptr_eq(load, &shared));
        }
        let abandoned: Vec<_> = loads.iter().map(Arc::downgrade).collect();
        drop(loads);
        assert!(abandoned.iter().all(|load| load.upgrade().is_none()));
        let next = loading(worker.get_or_register(keys[0].clone(), vec![], RangeCacheScope(0)));
        assert_eq!(worker.state.lock().unwrap().flights.len(), 1);
        let shared = loading(worker.get_or_register(keys[0].clone(), vec![], RangeCacheScope(0)));
        assert!(Arc::ptr_eq(&next, &shared));
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn test_index_replacement_retirement_invalidates_and_blocks_old_reader_fills(
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
        let pending = loading(worker.get_or_register(
            loading_key.clone(),
            vec![old_reader.clone()],
            RangeCacheScope(0),
        ));
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
            worker.get_or_register(key, vec![old_reader], RangeCacheScope(0)),
            RangeCacheLookup::Bypass
        ));
        assert!(!worker.complete(&pending, empty_result()));
        assert!(worker.get(&loading_key).is_none());
    }
}
