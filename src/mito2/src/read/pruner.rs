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
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use common_telemetry::debug;
use smallvec::SmallVec;
use snafu::ResultExt;
use store_api::region_engine::PartitionRange;
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

use crate::error::{PruneFileSnafu, Result};
use crate::metrics::PRUNER_ACTIVE_BUILDERS;
use crate::read::range::{FileRangeBuilder, RowGroupIndex};
use crate::read::scan_region::StreamContext;
use crate::read::scan_util::{FileScanMetrics, PartitionMetrics};
use crate::sst::parquet::file_range::FileRange;
use crate::sst::parquet::reader::ReaderMetrics;

/// Number of files to pre-fetch ahead of the current position.
const PREFETCH_COUNT: usize = 8;

pub struct PartitionPruner {
    pruner: Arc<Pruner>,
    /// Files to prune, in the order to scan.
    file_indices: Vec<usize>,
    /// Current position for tracking pre-fetch progress.
    current_position: AtomicUsize,
}

impl PartitionPruner {
    /// Creates a new `PartitionPruner` for the given partition ranges.
    pub fn new(pruner: Arc<Pruner>, partition_ranges: &[PartitionRange]) -> Self {
        let mut file_indices = Vec::with_capacity(pruner.inner.stream_ctx.input.num_files());
        let mut dedup_set = HashSet::with_capacity(pruner.inner.stream_ctx.input.num_files());

        let num_memtables = pruner.inner.stream_ctx.input.num_memtables();
        for part_range in partition_ranges {
            let range_meta = &pruner.inner.stream_ctx.ranges[part_range.identifier];
            for row_group_index in &range_meta.row_group_indices {
                if row_group_index.index >= num_memtables {
                    let file_index = row_group_index.index - num_memtables;
                    if dedup_set.contains(&file_index) {
                        continue;
                    } else {
                        file_indices.push(file_index);
                        dedup_set.insert(file_index);
                    }
                }
            }
        }

        Self {
            pruner,
            file_indices,
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

        // Delegate to underlying Pruner
        let ranges = self
            .pruner
            .build_file_ranges(index, partition_metrics, reader_metrics)
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

    /// Pre-fetches upcoming files starting from the given position.
    fn prefetch_upcoming_files(&self, current_pos: usize, partition_metrics: &PartitionMetrics) {
        let start = current_pos + 1;
        let end = (start + PREFETCH_COUNT).min(self.file_indices.len());

        for i in start..end {
            self.pruner
                .get_file_builder_background(self.file_indices[i], Some(partition_metrics.clone()));
        }
    }
}

/// A pruner that prunes files for all partitions of a scanner.
///
/// The pruner:
/// - Is created once and shared across all partitions
/// - Spawns N parallel worker tasks on creation
/// - Routes file pruning requests by `file_id % N` to workers
/// - Maintains FileBuilderEntry with reference counting
/// - Tracks remaining usage count per file for cleanup
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
    /// Status of pruning.
    status: PruneStatus,
    /// Waiters when pruning is in-progress.
    waiters: Vec<oneshot::Sender<Result<Arc<FileRangeBuilder>>>>,
}

/// Status of file pruning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PruneStatus {
    /// Not yet started pruning.
    Pending,
    /// Currently being pruned.
    InProgress,
    /// Pruning completed.
    Complete,
}

/// Request to prune a file.
struct PruneRequest {
    /// Index of the file in ScanInput.files.
    file_index: usize,
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
                    status: PruneStatus::Pending,
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
            common_runtime::spawn_global(async move {
                Self::worker_loop(worker_id, rx, inner_clone).await;
            });
        }

        Self {
            worker_senders,
            inner,
        }
    }

    /// Adds reference counts for all partitions' ranges.
    /// Called from scanner's `prepare()` method.
    ///
    /// This method clears all existing builders first, then adds reference counts
    /// based on all partition ranges.
    pub fn add_partition_ranges(&self, partition_ranges: &[PartitionRange]) {
        // Add reference counts for each partition range
        let num_memtables = self.inner.stream_ctx.input.num_memtables();
        for part_range in partition_ranges {
            let range_meta = &self.inner.stream_ctx.ranges[part_range.identifier];
            for row_group_index in &range_meta.row_group_indices {
                if row_group_index.index >= num_memtables {
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
    pub async fn build_file_ranges(
        &self,
        index: RowGroupIndex,
        partition_metrics: &PartitionMetrics,
        reader_metrics: &mut ReaderMetrics,
    ) -> Result<SmallVec<[FileRange; 2]>> {
        let file_index = index.index - self.inner.stream_ctx.input.num_memtables();

        // Get builder (from cache or by pruning)
        let builder = self
            .get_file_builder(file_index, partition_metrics, reader_metrics)
            .await?;

        // Build ranges
        let mut ranges = SmallVec::new();
        builder.build_ranges(index.row_group_index, &mut ranges);

        // Decrement ref count and cleanup if needed
        self.decrement_and_maybe_clear(file_index, reader_metrics);

        Ok(ranges)
    }

    /// Gets or creates the FileRangeBuilder for a file.
    async fn get_file_builder(
        &self,
        file_index: usize,
        partition_metrics: &PartitionMetrics,
        reader_metrics: &mut ReaderMetrics,
    ) -> Result<Arc<FileRangeBuilder>> {
        // Fast path: check cache
        {
            let entry = self.inner.file_entries[file_index].lock().unwrap();
            if let Some(builder) = &entry.builder {
                reader_metrics.filter_metrics.pruner_cache_hit += 1;
                return Ok(builder.clone());
            }
        }

        // Slow path: route to worker by file_id % num_workers
        reader_metrics.filter_metrics.pruner_cache_miss += 1;
        let prune_start = Instant::now();
        let file = &self.inner.stream_ctx.input.files[file_index];
        let file_id = file.file_id().file_id(); // RegionFileId -> FileId
        let file_id_hash = Uuid::from(file_id).as_u128() as usize;
        let worker_idx = file_id_hash % self.inner.num_workers;

        let (response_tx, response_rx) = oneshot::channel();
        let request = PruneRequest {
            file_index,
            response_tx: Some(response_tx),
            partition_metrics: Some(partition_metrics.clone()),
        };

        // Send request to worker
        let result = if self.worker_senders[worker_idx].send(request).await.is_err() {
            common_telemetry::warn!("Worker channel closed, falling back to direct pruning");
            // Worker channel closed, fall back to direct pruning
            self.prune_file_directly(file_index, reader_metrics).await
        } else {
            // Wait for response
            match response_rx.await {
                Ok(result) => result,
                Err(_) => {
                    common_telemetry::warn!(
                        "Response channel closed, falling back to direct pruning"
                    );
                    // Channel closed, fall back to direct pruning
                    self.prune_file_directly(file_index, reader_metrics).await
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
        partition_metrics: Option<PartitionMetrics>,
    ) {
        // Fast path: check cache
        {
            let entry = self.inner.file_entries[file_index].lock().unwrap();
            if entry.builder.is_some() {
                return;
            }
        }

        // Slow path: route to worker by file_id % num_workers
        let file = &self.inner.stream_ctx.input.files[file_index];
        let file_id = file.file_id().file_id(); // RegionFileId -> FileId
        let file_id_hash = Uuid::from(file_id).as_u128() as usize;
        let worker_idx = file_id_hash % self.inner.num_workers;

        let request = PruneRequest {
            file_index,
            response_tx: None,
            partition_metrics,
        };

        // Send request to worker
        let _ = self.worker_senders[worker_idx].try_send(request);
    }

    /// Prunes a file directly without going through a worker.
    /// Used as fallback when worker channels are closed.
    async fn prune_file_directly(
        &self,
        file_index: usize,
        reader_metrics: &mut ReaderMetrics,
    ) -> Result<Arc<FileRangeBuilder>> {
        let file = &self.inner.stream_ctx.input.files[file_index];
        let builder = self
            .inner
            .stream_ctx
            .input
            .prune_file(file, reader_metrics)
            .await?;

        let arc_builder = Arc::new(builder);

        // Cache the builder
        {
            let mut entry = self.inner.file_entries[file_index].lock().unwrap();
            if entry.builder.is_none() {
                reader_metrics.metadata_mem_size += arc_builder.memory_size() as isize;
                reader_metrics.num_range_builders += 1;
                entry.builder = Some(arc_builder.clone());
                PRUNER_ACTIVE_BUILDERS.inc();
                entry.status = PruneStatus::Complete;
            }
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
                response_tx,
                partition_metrics,
            } = request;

            // Check if already cached or in-progress
            {
                let mut entry = inner.file_entries[file_index].lock().unwrap();

                if let Some(builder) = &entry.builder {
                    // Cache hit - send immediately
                    if let Some(response_tx) = response_tx {
                        let _ = response_tx.send(Ok(builder.clone()));
                    }
                    worker_cache_hit += 1;
                    continue;
                }

                // Mark as in-progress
                entry.status = PruneStatus::InProgress;
            }
            worker_cache_miss += 1;

            // Do the actual pruning (outside lock)
            let file = &inner.stream_ctx.input.files[file_index];
            pruned_files.push(file.file_id().file_id());
            let mut metrics = ReaderMetrics::default();
            let result = inner.stream_ctx.input.prune_file(file, &mut metrics).await;

            // Update state and notify waiters
            let mut entry = inner.file_entries[file_index].lock().unwrap();
            match result {
                Ok(builder) => {
                    let arc_builder = Arc::new(builder);
                    entry.builder = Some(arc_builder.clone());
                    PRUNER_ACTIVE_BUILDERS.inc();
                    entry.status = PruneStatus::Complete;

                    // Notify all waiters
                    for waiter in entry.waiters.drain(..) {
                        let _ = waiter.send(Ok(arc_builder.clone()));
                    }
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

                    // Merge metrics to partition if provided
                    if let Some(part_metrics) = &partition_metrics {
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
                    entry.status = PruneStatus::Pending; // Reset status on error
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
