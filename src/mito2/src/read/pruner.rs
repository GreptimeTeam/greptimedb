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

//! Pruner for parallel file pruning across scanner partitions.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use common_telemetry::debug;
use smallvec::SmallVec;
use snafu::ResultExt;
use store_api::region_engine::PartitionRange;
use store_api::storage::FileId;
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

use crate::error::{PruneFileSnafu, Result};
use crate::metrics::PRUNER_ACTIVE_BUILDERS;
use crate::read::range::{FileRangeBuilder, RowGroupIndex};
use crate::read::scan_region::StreamContext;
use crate::read::scan_util::{FileScanMetrics, PartitionMetrics, new_filter_metrics};
use crate::sst::parquet::file_range::{FileRange, PreFilterMode};
use crate::sst::parquet::reader::ReaderMetrics;

/// Number of files to pre-fetch ahead of the current position.
const PREFETCH_COUNT: usize = 8;

/// Local pruner in a partition that supports prefetching files to prune.
pub struct PartitionPruner {
    pruner: Arc<Pruner>,
    /// Files to prune, in the order to scan.
    file_indices: Vec<usize>,
    /// Per-file pre-filter mode lookup indexed by file_index.
    pre_filter_modes: Vec<PreFilterMode>,
    /// Positive manifest-prune cache indexed by file_index.
    ///
    /// This is scoped to a scan partition. We only cache positive decisions
    /// (file => manifest-pruned) and never cache negative decisions, so a later
    /// dynamic-filter tightening can still make a previously unpruned file
    /// prunable.
    ///
    /// SAFETY: dynamic-filter based pruning relies on filters being monotonic
    /// within one scan: updates may keep or tighten the filter, but must not
    /// relax it after consumers have already pruned data. Under that invariant,
    /// a file once proven empty by manifest-level pruning remains empty for
    /// later, stricter predicate snapshots. If a future dynamic filter can relax,
    /// this cache must be invalidated or disabled for that filter.
    manifest_pruned_files: Vec<AtomicBool>,
    /// Current position for tracking pre-fetch progress.
    current_position: AtomicUsize,
}

impl PartitionPruner {
    /// Creates a new `PartitionPruner` for the given partition ranges.
    pub fn new(pruner: Arc<Pruner>, partition_ranges: &[PartitionRange]) -> Self {
        let num_files = pruner.inner.stream_ctx.input.num_files();
        let mut file_indices = Vec::with_capacity(num_files);
        let mut pre_filter_modes = vec![PreFilterMode::SkipFields; num_files];
        let mut dedup_set = HashSet::with_capacity(pruner.inner.stream_ctx.input.num_files());

        let num_memtables = pruner.inner.stream_ctx.input.num_memtables();
        for part_range in partition_ranges {
            let range_meta = &pruner.inner.stream_ctx.ranges[part_range.identifier];
            let pre_filter_mode = pruner.inner.stream_ctx.range_pre_filter_mode(part_range);
            for row_group_index in &range_meta.row_group_indices {
                if pruner
                    .inner
                    .stream_ctx
                    .is_file_range_index(*row_group_index)
                {
                    let file_index = row_group_index.index - num_memtables;
                    if dedup_set.contains(&file_index) {
                        continue;
                    } else {
                        file_indices.push(file_index);
                        pre_filter_modes[file_index] = pre_filter_mode;
                        dedup_set.insert(file_index);
                    }
                }
            }
        }

        Self {
            pruner,
            file_indices,
            pre_filter_modes,
            manifest_pruned_files: (0..num_files).map(|_| AtomicBool::new(false)).collect(),
            current_position: AtomicUsize::new(0),
        }
    }

    /// Gets or creates the FileRangeBuilder for a file.
    ///
    /// This method also triggers pre-fetching of upcoming files in the background
    /// to improve performance by overlapping I/O with computation.
    pub async fn build_file_ranges(
        &self,
        index: RowGroupIndex,
        partition_metrics: &PartitionMetrics,
        reader_metrics: &mut ReaderMetrics,
    ) -> Result<SmallVec<[FileRange; 2]>> {
        let file_index = index.index - self.pruner.inner.stream_ctx.input.num_memtables();
        let pre_filter_mode = self.pre_filter_mode(file_index);

        // Delegate to underlying Pruner
        let ranges = self
            .pruner
            .build_file_ranges(index, pre_filter_mode, partition_metrics, reader_metrics)
            .await?;

        // Find position and trigger pre-fetch for upcoming files
        if let Some(pos) = self.file_indices.iter().position(|&idx| idx == file_index) {
            let prev_pos = self.current_position.fetch_max(pos, Ordering::Relaxed);
            if pos > prev_pos || prev_pos == 0 {
                self.prefetch_upcoming_files(pos, partition_metrics);
            }
        }

        Ok(ranges)
    }

    /// Skips a file range that was pruned before entering [`Pruner::build_file_ranges`].
    pub fn skip_file_range(&self, index: RowGroupIndex, reader_metrics: &mut ReaderMetrics) {
        self.pruner.skip_file_range(index, reader_metrics);
    }

    /// Returns true if this file range was already proven manifest-pruned in
    /// this scan partition.
    pub fn is_manifest_pruned_file_range(&self, index: RowGroupIndex) -> bool {
        self.file_index(index)
            .and_then(|file_index| self.manifest_pruned_files.get(file_index))
            .is_some_and(|pruned| pruned.load(Ordering::Relaxed))
    }

    /// Skips a file range that was proven definitively pruned by manifest-level
    /// time-range pruning. Records `files_time_range_pruned` in `reader_metrics`
    /// and balances the pruner's per-file reference count.
    pub fn skip_manifest_pruned_file_range(
        &self,
        index: RowGroupIndex,
        reader_metrics: &mut ReaderMetrics,
    ) {
        if let Some(file_index) = self.file_index(index)
            && let Some(pruned) = self.manifest_pruned_files.get(file_index)
            && pruned
                .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
        {
            reader_metrics.filter_metrics.files_time_range_pruned += 1;
        }
        self.skip_file_range(index, reader_metrics);
    }

    /// Pre-fetches upcoming files starting from the given position.
    fn prefetch_upcoming_files(&self, current_pos: usize, partition_metrics: &PartitionMetrics) {
        let start = current_pos + 1;
        let end = (start + PREFETCH_COUNT).min(self.file_indices.len());

        for i in start..end {
            let file_index = self.file_indices[i];
            let pre_filter_mode = self.pre_filter_mode(file_index);
            self.pruner.get_file_builder_background(
                file_index,
                pre_filter_mode,
                Some(partition_metrics.clone()),
            );
        }
    }

    fn pre_filter_mode(&self, file_index: usize) -> PreFilterMode {
        self.pre_filter_modes
            .get(file_index)
            .copied()
            .unwrap_or(PreFilterMode::SkipFields)
    }

    fn file_index(&self, index: RowGroupIndex) -> Option<usize> {
        self.pruner
            .inner
            .stream_ctx
            .is_file_range_index(index)
            .then(|| index.index - self.pruner.inner.stream_ctx.input.num_memtables())
    }
}

/// A pruner that prunes files for all partitions of a scanner.
pub struct Pruner {
    /// Channels to send requests to workers.
    worker_senders: Vec<mpsc::Sender<PruneRequest>>,
    inner: Arc<PrunerInner>,
}

struct PrunerInner {
    /// Number of worker tasks.
    num_workers: usize,
    /// Per-file state (indexed by file_index).
    file_entries: Vec<Mutex<FileBuilderEntry>>,
    /// StreamContext containing all context needed for pruning.
    stream_ctx: Arc<StreamContext>,
}

/// Per-file state tracking.
struct FileBuilderEntry {
    /// Cached builder after pruning. None if not yet built or already cleared.
    builder: Option<Arc<FileRangeBuilder>>,
    /// Number of remaining ranges to scan for this file.
    /// When this reaches 0, the builder is dropped for memory cleanup.
    remaining_ranges: usize,
    /// Waiters when pruning is in-progress.
    waiters: Vec<oneshot::Sender<Result<Arc<FileRangeBuilder>>>>,
}

/// Request to prune a file.
struct PruneRequest {
    /// Index of the file in ScanInput.files.
    file_index: usize,
    /// Pre-filter mode to use for the file.
    pre_filter_mode: PreFilterMode,
    /// Oneshot channel to send back the result.
    response_tx: Option<oneshot::Sender<Result<Arc<FileRangeBuilder>>>>,
    /// Partition metrics for merging reader metrics.
    partition_metrics: Option<PartitionMetrics>,
}

impl Pruner {
    /// Creates a new Pruner with N worker tasks.
    ///
    /// Initially all file_entries have `remaining_ranges = 0`.
    /// Call `add_partition_ranges()` to initialize ref counts.
    pub fn new(stream_ctx: Arc<StreamContext>, num_workers: usize) -> Self {
        let num_files = stream_ctx.input.num_files();
        let file_entries: Vec<_> = (0..num_files)
            .map(|_| {
                Mutex::new(FileBuilderEntry {
                    builder: None,
                    remaining_ranges: 0,
                    waiters: Vec::new(),
                })
            })
            .collect();
        // Create channels and collect senders
        let mut worker_senders = Vec::with_capacity(num_workers);
        let mut receivers = Vec::with_capacity(num_workers);
        for _ in 0..num_workers {
            let (tx, rx) = mpsc::channel::<PruneRequest>(64);
            worker_senders.push(tx);
            receivers.push(rx);
        }

        let inner = Arc::new(PrunerInner {
            num_workers,
            file_entries,
            stream_ctx,
        });

        // Spawn worker tasks with their receivers
        for (worker_id, rx) in receivers.into_iter().enumerate() {
            let inner_clone = inner.clone();
            common_runtime::spawn_query(async move {
                Self::worker_loop(worker_id, rx, inner_clone).await;
            });
        }

        Self {
            worker_senders,
            inner,
        }
    }

    /// Adds reference counts for all partitions' ranges.
    pub fn add_partition_ranges(&self, partition_ranges: &[PartitionRange]) {
        // Add reference counts for each partition range
        let num_memtables = self.inner.stream_ctx.input.num_memtables();
        for part_range in partition_ranges {
            let range_meta = &self.inner.stream_ctx.ranges[part_range.identifier];
            for row_group_index in &range_meta.row_group_indices {
                if self.inner.stream_ctx.is_file_range_index(*row_group_index) {
                    let file_index = row_group_index.index - num_memtables;
                    if file_index < self.inner.file_entries.len() {
                        let mut entry = self.inner.file_entries[file_index].lock().unwrap();
                        entry.remaining_ranges += 1;
                    }
                }
            }
        }
    }

    /// Gets or creates the FileRangeBuilder for a file, builds ranges,
    /// and decrements ref count (cleans up if zero).
    ///
    /// Callers should invoke [add_partition_ranges()](Pruner::add_partition_ranges()) to initialize the
    /// file entries and ref counts.
    pub async fn build_file_ranges(
        &self,
        index: RowGroupIndex,
        pre_filter_mode: PreFilterMode,
        partition_metrics: &PartitionMetrics,
        reader_metrics: &mut ReaderMetrics,
    ) -> Result<SmallVec<[FileRange; 2]>> {
        let file_index = index.index - self.inner.stream_ctx.input.num_memtables();

        // Get builder (from cache or by pruning)
        let builder = self
            .get_file_builder(
                file_index,
                pre_filter_mode,
                partition_metrics,
                reader_metrics,
            )
            .await?;

        // Build ranges
        let mut ranges = SmallVec::new();
        builder.build_ranges(index.row_group_index, &mut ranges);

        // Decrement ref count and cleanup if needed
        self.decrement_and_maybe_clear(file_index, reader_metrics);

        Ok(ranges)
    }

    /// Skips a file range that has been pruned before entering the file pruner.
    ///
    /// This keeps the pruner's per-file reference counts balanced with
    /// `add_partition_ranges()`. It may also clear a cached builder when this was the
    /// last remaining range for the file.
    pub fn skip_file_range(&self, index: RowGroupIndex, reader_metrics: &mut ReaderMetrics) {
        let file_index = index.index - self.inner.stream_ctx.input.num_memtables();
        self.decrement_and_maybe_clear(file_index, reader_metrics);
    }

    /// Gets or creates the FileRangeBuilder for a file.
    async fn get_file_builder(
        &self,
        file_index: usize,
        pre_filter_mode: PreFilterMode,
        partition_metrics: &PartitionMetrics,
        reader_metrics: &mut ReaderMetrics,
    ) -> Result<Arc<FileRangeBuilder>> {
        // Fast path: checks cache
        {
            let entry = self.inner.file_entries[file_index].lock().unwrap();
            if let Some(builder) = &entry.builder {
                reader_metrics.filter_metrics.pruner_cache_hit += 1;
                return Ok(builder.clone());
            }
        }

        reader_metrics.filter_metrics.pruner_cache_miss += 1;
        let prune_start = Instant::now();
        let file = &self.inner.stream_ctx.input.files[file_index];
        let file_id = file.file_id().file_id();
        let worker_idx = self.get_worker_idx(file_id);

        let (response_tx, response_rx) = oneshot::channel();
        let request = PruneRequest {
            file_index,
            pre_filter_mode,
            response_tx: Some(response_tx),
            partition_metrics: Some(partition_metrics.clone()),
        };

        let result = if self.worker_senders[worker_idx].send(request).await.is_err() {
            common_telemetry::warn!("Worker channel closed, falling back to direct pruning");
            // Worker channel closed, falls back to direct pruning
            self.prune_file_directly(file_index, pre_filter_mode, reader_metrics)
                .await
        } else {
            // Waits for response
            match response_rx.await {
                Ok(result) => result,
                Err(_) => {
                    common_telemetry::warn!(
                        "Response channel closed, falling back to direct pruning"
                    );
                    // Channel closed, falls back to direct pruning
                    self.prune_file_directly(file_index, pre_filter_mode, reader_metrics)
                        .await
                }
            }
        };
        reader_metrics.filter_metrics.pruner_prune_cost += prune_start.elapsed();
        result
    }

    /// Gets or creates the FileRangeBuilder for a file.
    pub fn get_file_builder_background(
        &self,
        file_index: usize,
        pre_filter_mode: PreFilterMode,
        partition_metrics: Option<PartitionMetrics>,
    ) {
        // Fast path: checks cache
        {
            let entry = self.inner.file_entries[file_index].lock().unwrap();
            if entry.builder.is_some() {
                return;
            }
        }

        let file = &self.inner.stream_ctx.input.files[file_index];
        let file_id = file.file_id().file_id();
        let worker_idx = self.get_worker_idx(file_id);

        let request = PruneRequest {
            file_index,
            pre_filter_mode,
            response_tx: None,
            partition_metrics,
        };

        // Sends request to worker
        let _ = self.worker_senders[worker_idx].try_send(request);
    }

    fn get_worker_idx(&self, file_id: FileId) -> usize {
        let file_id_hash = Uuid::from(file_id).as_u128() as usize;
        file_id_hash % self.inner.num_workers
    }

    /// Prunes a file directly without going through a worker.
    /// Used as fallback when worker channels are closed.
    async fn prune_file_directly(
        &self,
        file_index: usize,
        pre_filter_mode: PreFilterMode,
        reader_metrics: &mut ReaderMetrics,
    ) -> Result<Arc<FileRangeBuilder>> {
        let file = &self.inner.stream_ctx.input.files[file_index];
        let builder = self
            .inner
            .stream_ctx
            .input
            .prune_file(file, pre_filter_mode, reader_metrics)
            .await?;

        let arc_builder = Arc::new(builder);

        // Caches the builder only if the file still has remaining ranges.
        // `skip_file_range` may have already consumed all ranges for this file.
        {
            let mut entry = self.inner.file_entries[file_index].lock().unwrap();
            cache_builder_if_needed(&mut entry, &arc_builder, reader_metrics);
        }

        Ok(arc_builder)
    }

    /// Decrements ref count and clears builder if no longer needed.
    fn decrement_and_maybe_clear(&self, file_index: usize, reader_metrics: &mut ReaderMetrics) {
        let mut entry = self.inner.file_entries[file_index].lock().unwrap();
        entry.remaining_ranges = entry.remaining_ranges.saturating_sub(1);

        if entry.remaining_ranges == 0
            && let Some(builder) = entry.builder.take()
        {
            PRUNER_ACTIVE_BUILDERS.dec();
            reader_metrics.metadata_mem_size -= builder.memory_size() as isize;
            reader_metrics.num_range_builders -= 1;
        }
    }

    /// Worker loop that processes prune requests.
    async fn worker_loop(
        worker_id: usize,
        mut rx: mpsc::Receiver<PruneRequest>,
        inner: Arc<PrunerInner>,
    ) {
        let mut worker_cache_hit = 0;
        let mut worker_cache_miss = 0;
        let mut pruned_files = Vec::new();

        while let Some(request) = rx.recv().await {
            let PruneRequest {
                file_index,
                pre_filter_mode,
                response_tx,
                partition_metrics,
            } = request;

            // Check if already cached or in-progress
            {
                let entry = inner.file_entries[file_index].lock().unwrap();
                if let Some(builder) = &entry.builder {
                    // Cache hit - send immediately
                    if let Some(response_tx) = response_tx {
                        let _ = response_tx.send(Ok(builder.clone()));
                    }
                    worker_cache_hit += 1;
                    continue;
                }
            }
            worker_cache_miss += 1;

            // Do the actual pruning (outside lock)
            let file = &inner.stream_ctx.input.files[file_index];
            pruned_files.push(file.file_id().file_id());
            let explain_verbose = partition_metrics
                .as_ref()
                .map(|m| m.explain_verbose())
                .unwrap_or(false);
            let mut metrics = ReaderMetrics {
                filter_metrics: new_filter_metrics(explain_verbose),
                ..Default::default()
            };
            let result = inner
                .stream_ctx
                .input
                .prune_file(file, pre_filter_mode, &mut metrics)
                .await;

            // Update state and notify waiters
            let mut entry = inner.file_entries[file_index].lock().unwrap();
            match result {
                Ok(builder) => {
                    let arc_builder = Arc::new(builder);
                    let is_background = response_tx.is_none();

                    // Only cache the builder if the file still has remaining ranges.
                    // If remaining_ranges == 0, a concurrent `skip_file_range` (e.g. from a
                    // dynamic filter tightening via manifest-prune fast-skip) already consumed
                    // all ranges and may have cleared a previously cached builder.
                    let did_cache = cache_builder_if_needed(&mut entry, &arc_builder, &mut metrics);

                    // Notify all waiters
                    for waiter in entry.waiters.drain(..) {
                        let _ = waiter.send(Ok(arc_builder.clone()));
                    }
                    // Always respond to foreground caller, even if we did not cache.
                    if let Some(response_tx) = response_tx {
                        let _ = response_tx.send(Ok(arc_builder));
                    }

                    debug!(
                        "Pruner worker {} pruned file_index: {}, file: {:?}, metrics: {:?}",
                        worker_id,
                        file_index,
                        file.file_id(),
                        metrics
                    );

                    // Merge metrics if this is a foreground request, or if the builder
                    // was cached. Skip stale per-file metrics
                    // for background requests that completed after the file was already
                    // fully skipped.
                    if (!is_background || did_cache)
                        && let Some(part_metrics) = &partition_metrics
                    {
                        let per_file_metrics = if part_metrics.explain_verbose() {
                            let file_id = file.file_id();
                            let mut map = HashMap::new();
                            map.insert(
                                file_id,
                                FileScanMetrics {
                                    build_part_cost: metrics.build_cost,
                                    ..Default::default()
                                },
                            );
                            Some(map)
                        } else {
                            None
                        };
                        part_metrics.merge_reader_metrics(&metrics, per_file_metrics.as_ref());
                    }
                }
                Err(e) => {
                    let arc_error = Arc::new(e);
                    for waiter in entry.waiters.drain(..) {
                        let _ = waiter.send(Err(arc_error.clone()).context(PruneFileSnafu));
                    }
                    if let Some(response_tx) = response_tx {
                        let _ = response_tx.send(Err(arc_error).context(PruneFileSnafu));
                    }
                }
            }
        }

        common_telemetry::debug!(
            "Pruner worker {} finished, cache_hit: {}, cache_miss: {}, files: {:?}",
            worker_id,
            worker_cache_hit,
            worker_cache_miss,
            pruned_files,
        );
    }
}

#[cfg(test)]
impl Pruner {
    /// Returns the remaining range count for a file (test-only).
    fn test_remaining_ranges(&self, file_index: usize) -> usize {
        self.inner.file_entries[file_index]
            .lock()
            .unwrap()
            .remaining_ranges
    }

    /// Returns whether a cached builder exists for a file (test-only).
    fn test_has_builder(&self, file_index: usize) -> bool {
        self.inner.file_entries[file_index]
            .lock()
            .unwrap()
            .builder
            .is_some()
    }

    /// Clears a cached builder for a file, simulating stale cleanup (test-only).
    #[allow(dead_code)]
    fn test_clear_builder(&self, file_index: usize) {
        let mut entry = self.inner.file_entries[file_index].lock().unwrap();
        if entry.builder.take().is_some() {
            PRUNER_ACTIVE_BUILDERS.dec();
        }
    }
}

/// Returns true if a freshly pruned builder should be cached for this file.
fn should_cache_builder(entry: &FileBuilderEntry) -> bool {
    entry.builder.is_none() && entry.remaining_ranges > 0
}

/// Caches a freshly pruned builder if the file still has remaining ranges, and
/// records the corresponding builder memory/count deltas for verbose metrics.
fn cache_builder_if_needed(
    entry: &mut FileBuilderEntry,
    builder: &Arc<FileRangeBuilder>,
    reader_metrics: &mut ReaderMetrics,
) -> bool {
    if should_cache_builder(entry) {
        reader_metrics.metadata_mem_size += builder.memory_size() as isize;
        reader_metrics.num_range_builders += 1;
        entry.builder = Some(builder.clone());
        PRUNER_ACTIVE_BUILDERS.inc();
        true
    } else {
        false
    }
}

#[cfg(test)]
mod tests {
    use common_time::Timestamp;
    use store_api::region_engine::PartitionRange;
    use store_api::storage::{FileId, RegionId};

    use super::*;
    use crate::read::flat_projection::FlatProjectionMapper;
    use crate::read::range::RowGroupIndex;
    use crate::read::scan_region::ScanInput;
    use crate::sst::file::{FileHandle, FileMeta};
    use crate::sst::parquet::reader::ReaderMetrics;
    use crate::test_util::memtable_util::metadata_with_primary_key;
    use crate::test_util::new_noop_file_purger;
    use crate::test_util::scheduler_util::SchedulerEnv;

    async fn make_test_pruner(num_files: usize) -> (SchedulerEnv, Arc<Pruner>) {
        let env = SchedulerEnv::new().await;
        let metadata = Arc::new(metadata_with_primary_key(vec![0, 1], false));
        let mapper = FlatProjectionMapper::new(&metadata, [0, 2, 3]).unwrap();

        let files: Vec<FileHandle> = (0..num_files)
            .map(|_| {
                let meta = FileMeta {
                    region_id: RegionId::new(123, 456),
                    file_id: FileId::random(),
                    time_range: (
                        Timestamp::new_millisecond(0),
                        Timestamp::new_millisecond(1000),
                    ),
                    num_row_groups: 1,
                    num_rows: 1024,
                    level: 0,
                    ..Default::default()
                };
                FileHandle::new(meta, new_noop_file_purger())
            })
            .collect();

        let input = ScanInput::new(env.access_layer.clone(), mapper)
            .with_files(files)
            .with_append_mode(true);
        let stream_ctx = Arc::new(StreamContext::unordered_scan_ctx(input));
        let pruner = Arc::new(Pruner::new(stream_ctx, 1));
        (env, pruner)
    }

    /// Builds a minimal `PartitionRange` that references `file_index`.
    /// `add_partition_ranges` will look up `stream_ctx.ranges[identifier]`
    /// and find `row_group_indices[0] == RowGroupIndex { index: file_index,
    /// row_group_index: 0 }` because `unordered_scan_ranges` with
    /// `num_row_groups=1` produces one range per file.
    fn file_partition_range(file_index: usize) -> PartitionRange {
        PartitionRange {
            start: Timestamp::new_millisecond(0),
            end: Timestamp::new_millisecond(1001),
            num_rows: 1024,
            identifier: file_index,
        }
    }

    #[test]
    fn should_cache_builder_when_ranges_remain() {
        let entry = FileBuilderEntry {
            builder: None,
            remaining_ranges: 3,
            waiters: Vec::new(),
        };
        assert!(should_cache_builder(&entry));
    }

    #[test]
    fn should_not_cache_builder_when_no_ranges_remain() {
        let entry = FileBuilderEntry {
            builder: None,
            remaining_ranges: 0,
            waiters: Vec::new(),
        };
        assert!(!should_cache_builder(&entry));
    }

    #[test]
    fn should_not_cache_builder_when_already_cached() {
        let entry = FileBuilderEntry {
            builder: Some(Arc::new(FileRangeBuilder::default())),
            remaining_ranges: 1,
            waiters: Vec::new(),
        };
        assert!(!should_cache_builder(&entry));
    }

    #[test]
    fn cache_builder_records_metrics() {
        let mut entry = FileBuilderEntry {
            builder: None,
            remaining_ranges: 1,
            waiters: Vec::new(),
        };
        let builder = Arc::new(FileRangeBuilder::default());
        let mut reader_metrics = ReaderMetrics::default();

        assert!(cache_builder_if_needed(
            &mut entry,
            &builder,
            &mut reader_metrics
        ));
        assert!(entry.builder.is_some());
        assert_eq!(
            reader_metrics.metadata_mem_size,
            builder.memory_size() as isize
        );
        assert_eq!(reader_metrics.num_range_builders, 1);

        if entry.builder.take().is_some() {
            PRUNER_ACTIVE_BUILDERS.dec();
        }
    }

    #[tokio::test]
    async fn skip_file_range_decrements_and_clears_builder() {
        let (_env, pruner) = make_test_pruner(1).await;

        // Simulate 3 partition ranges for file 0.
        let ranges: Vec<PartitionRange> = (0..3).map(|_| file_partition_range(0)).collect();
        pruner.add_partition_ranges(&ranges);
        assert_eq!(pruner.test_remaining_ranges(0), 3);

        // Manually set a cached builder (simulating a previous cache hit).
        {
            let mut entry = pruner.inner.file_entries[0].lock().unwrap();
            entry.builder = Some(Arc::new(FileRangeBuilder::default()));
            PRUNER_ACTIVE_BUILDERS.inc();
        }
        assert!(pruner.test_has_builder(0));

        // Skip all 3 ranges; the third should clear the builder.
        let mut reader_metrics = ReaderMetrics::default();
        for i in 0..3 {
            let index = RowGroupIndex {
                index: 0,
                row_group_index: i as i64,
            };
            pruner.skip_file_range(index, &mut reader_metrics);
        }

        assert_eq!(pruner.test_remaining_ranges(0), 0);
        assert!(!pruner.test_has_builder(0));
    }

    #[tokio::test]
    async fn worker_does_not_cache_after_skip_file_range_consumed_all() {
        let (_env, pruner) = make_test_pruner(1).await;

        // Simulate one range for file 0.
        let ranges = vec![file_partition_range(0)];
        pruner.add_partition_ranges(&ranges);
        assert_eq!(pruner.test_remaining_ranges(0), 1);

        // Simulate skip_file_range consuming the last range BEFORE the
        // background worker finishes. This mirrors the race: a dynamic filter
        // tightens and manifest-prune fast-skip zeros out remaining_ranges.
        let mut reader_metrics = ReaderMetrics::default();
        let index = RowGroupIndex {
            index: 0,
            row_group_index: 0,
        };
        pruner.skip_file_range(index, &mut reader_metrics);
        assert_eq!(pruner.test_remaining_ranges(0), 0);
        assert!(!pruner.test_has_builder(0));

        // Now simulate the worker completing: check the caching guard.
        let entry = pruner.inner.file_entries[0].lock().unwrap();
        let should_cache = should_cache_builder(&entry);
        drop(entry);

        assert!(!should_cache);

        // Ensure the gauge was not incremented for a stale builder.
        // (skip_file_range already decremented it if there was one, but here
        // there was none, so the gauge should be at baseline.)
    }

    #[tokio::test]
    async fn worker_caches_when_ranges_remain() {
        let (_env, pruner) = make_test_pruner(1).await;

        // Simulate 2 ranges for file 0.
        let ranges: Vec<PartitionRange> = (0..2).map(|_| file_partition_range(0)).collect();
        pruner.add_partition_ranges(&ranges);
        assert_eq!(pruner.test_remaining_ranges(0), 2);

        // Consume only 1 range.
        let mut reader_metrics = ReaderMetrics::default();
        let index = RowGroupIndex {
            index: 0,
            row_group_index: 0,
        };
        pruner.skip_file_range(index, &mut reader_metrics);
        assert_eq!(pruner.test_remaining_ranges(0), 1);

        // The worker should still cache because remaining_ranges > 0.
        let entry = pruner.inner.file_entries[0].lock().unwrap();
        assert!(should_cache_builder(&entry));
    }
}
