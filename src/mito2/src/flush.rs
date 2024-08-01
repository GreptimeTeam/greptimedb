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

//! Flush related utilities and structs.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use common_telemetry::{debug, error, info};
use smallvec::SmallVec;
use snafu::ResultExt;
use store_api::storage::RegionId;
use strum::IntoStaticStr;
use tokio::sync::{mpsc, watch};

use crate::access_layer::{AccessLayerRef, OperationType, SstWriteRequest};
use crate::cache::CacheManagerRef;
use crate::config::MitoConfig;
use crate::error::{
    Error, FlushRegionSnafu, RegionClosedSnafu, RegionDroppedSnafu, RegionTruncatedSnafu, Result,
};
use crate::manifest::action::{RegionEdit, RegionMetaAction, RegionMetaActionList};
use crate::metrics::{FLUSH_BYTES_TOTAL, FLUSH_ELAPSED, FLUSH_ERRORS_TOTAL, FLUSH_REQUESTS_TOTAL};
use crate::read::Source;
use crate::region::options::IndexOptions;
use crate::region::version::{VersionControlData, VersionControlRef};
use crate::region::{ManifestContextRef, RegionState};
use crate::request::{
    BackgroundNotify, FlushFailed, FlushFinished, OptionOutputTx, OutputTx, SenderDdlRequest,
    SenderWriteRequest, WorkerRequest,
};
use crate::schedule::scheduler::{Job, SchedulerRef};
use crate::sst::file::{FileId, FileMeta, IndexType};
use crate::sst::parquet::WriteOptions;
use crate::worker::WorkerListener;

/// Global write buffer (memtable) manager.
///
/// Tracks write buffer (memtable) usages and decide whether the engine needs to flush.
pub trait WriteBufferManager: Send + Sync + std::fmt::Debug {
    /// Returns whether to trigger the engine.
    fn should_flush_engine(&self) -> bool;

    /// Returns whether to stall write requests.
    fn should_stall(&self) -> bool;

    /// Reserves `mem` bytes.
    fn reserve_mem(&self, mem: usize);

    /// Tells the manager we are freeing `mem` bytes.
    ///
    /// We are in the process of freeing `mem` bytes, so it is not considered
    /// when checking the soft limit.
    fn schedule_free_mem(&self, mem: usize);

    /// We have freed `mem` bytes.
    fn free_mem(&self, mem: usize);

    /// Returns the total memory used by memtables.
    fn memory_usage(&self) -> usize;
}

pub type WriteBufferManagerRef = Arc<dyn WriteBufferManager>;

/// Default [WriteBufferManager] implementation.
///
/// Inspired by RocksDB's WriteBufferManager.
/// <https://github.com/facebook/rocksdb/blob/main/include/rocksdb/write_buffer_manager.h>
#[derive(Debug)]
pub struct WriteBufferManagerImpl {
    /// Write buffer size for the engine.
    global_write_buffer_size: usize,
    /// Mutable memtable memory size limit.
    mutable_limit: usize,
    /// Memory in used (e.g. used by mutable and immutable memtables).
    memory_used: AtomicUsize,
    /// Memory that hasn't been scheduled to free (e.g. used by mutable memtables).
    memory_active: AtomicUsize,
    /// Optional notifier.
    /// The manager can wake up the worker once we free the write buffer.
    notifier: Option<watch::Sender<()>>,
}

impl WriteBufferManagerImpl {
    /// Returns a new manager with specific `global_write_buffer_size`.
    pub fn new(global_write_buffer_size: usize) -> Self {
        Self {
            global_write_buffer_size,
            mutable_limit: Self::get_mutable_limit(global_write_buffer_size),
            memory_used: AtomicUsize::new(0),
            memory_active: AtomicUsize::new(0),
            notifier: None,
        }
    }

    /// Attaches a notifier to the manager.
    pub fn with_notifier(mut self, notifier: watch::Sender<()>) -> Self {
        self.notifier = Some(notifier);
        self
    }

    /// Returns memory usage of mutable memtables.
    pub fn mutable_usage(&self) -> usize {
        self.memory_active.load(Ordering::Relaxed)
    }

    /// Returns the size limit for mutable memtables.
    fn get_mutable_limit(global_write_buffer_size: usize) -> usize {
        // Reserves half of the write buffer for mutable memtable.
        global_write_buffer_size / 2
    }
}

impl WriteBufferManager for WriteBufferManagerImpl {
    fn should_flush_engine(&self) -> bool {
        let mutable_memtable_memory_usage = self.memory_active.load(Ordering::Relaxed);
        if mutable_memtable_memory_usage > self.mutable_limit {
            debug!(
                "Engine should flush (over mutable limit), mutable_usage: {}, memory_usage: {}, mutable_limit: {}, global_limit: {}",
                mutable_memtable_memory_usage, self.memory_usage(), self.mutable_limit, self.global_write_buffer_size,
            );
            return true;
        }

        let memory_usage = self.memory_used.load(Ordering::Relaxed);
        // If the memory exceeds the buffer size, we trigger more aggressive
        // flush. But if already more than half memory is being flushed,
        // triggering more flush may not help. We will hold it instead.
        if memory_usage >= self.global_write_buffer_size
            && mutable_memtable_memory_usage >= self.global_write_buffer_size / 2
        {
            debug!(
                "Engine should flush (over total limit), memory_usage: {}, global_write_buffer_size: {}, \
                 mutable_usage: {}.",
                memory_usage,
                self.global_write_buffer_size,
                mutable_memtable_memory_usage,
            );
            return true;
        }

        false
    }

    fn should_stall(&self) -> bool {
        self.memory_usage() >= self.global_write_buffer_size
    }

    fn reserve_mem(&self, mem: usize) {
        self.memory_used.fetch_add(mem, Ordering::Relaxed);
        self.memory_active.fetch_add(mem, Ordering::Relaxed);
    }

    fn schedule_free_mem(&self, mem: usize) {
        self.memory_active.fetch_sub(mem, Ordering::Relaxed);
    }

    fn free_mem(&self, mem: usize) {
        self.memory_used.fetch_sub(mem, Ordering::Relaxed);
        if let Some(notifier) = &self.notifier {
            // Notifies the worker after the memory usage is decreased. When we drop the memtable
            // outside of the worker, the worker may still stall requests because the memory usage
            // is not updated. So we need to notify the worker to handle stalled requests again.
            let _ = notifier.send(());
        }
    }

    fn memory_usage(&self) -> usize {
        self.memory_used.load(Ordering::Relaxed)
    }
}

/// Reason of a flush task.
#[derive(Debug, IntoStaticStr)]
pub enum FlushReason {
    /// Other reasons.
    Others,
    /// Engine reaches flush threshold.
    EngineFull,
    /// Manual flush.
    Manual,
    /// Flush to alter table.
    Alter,
    /// Flush periodically.
    Periodically,
}

impl FlushReason {
    /// Get flush reason as static str.
    fn as_str(&self) -> &'static str {
        self.into()
    }
}

/// Task to flush a region.
pub(crate) struct RegionFlushTask {
    /// Region to flush.
    pub(crate) region_id: RegionId,
    /// Reason to flush.
    pub(crate) reason: FlushReason,
    /// Flush result senders.
    pub(crate) senders: Vec<OutputTx>,
    /// Request sender to notify the worker.
    pub(crate) request_sender: mpsc::Sender<WorkerRequest>,

    pub(crate) access_layer: AccessLayerRef,
    pub(crate) listener: WorkerListener,
    pub(crate) engine_config: Arc<MitoConfig>,
    pub(crate) row_group_size: Option<usize>,
    pub(crate) cache_manager: CacheManagerRef,
    pub(crate) manifest_ctx: ManifestContextRef,

    /// Index options for the region.
    pub(crate) index_options: IndexOptions,
}

impl RegionFlushTask {
    /// Push the sender if it is not none.
    pub(crate) fn push_sender(&mut self, mut sender: OptionOutputTx) {
        if let Some(sender) = sender.take_inner() {
            self.senders.push(sender);
        }
    }

    /// Consumes the task and notify the sender the job is success.
    fn on_success(self) {
        for sender in self.senders {
            sender.send(Ok(0));
        }
    }

    /// Send flush error to waiter.
    fn on_failure(&mut self, err: Arc<Error>) {
        for sender in self.senders.drain(..) {
            sender.send(Err(err.clone()).context(FlushRegionSnafu {
                region_id: self.region_id,
            }));
        }
    }

    /// Converts the flush task into a background job.
    ///
    /// We must call this in the region worker.
    fn into_flush_job(mut self, version_control: &VersionControlRef) -> Job {
        // Get a version of this region before creating a job to get current
        // wal entry id, sequence and immutable memtables.
        let version_data = version_control.current();

        Box::pin(async move {
            self.do_flush(version_data).await;
        })
    }

    /// Runs the flush task.
    async fn do_flush(&mut self, version_data: VersionControlData) {
        let timer = FLUSH_ELAPSED.with_label_values(&["total"]).start_timer();
        self.listener.on_flush_begin(self.region_id).await;

        let worker_request = match self.flush_memtables(&version_data).await {
            Ok(edit) => {
                let memtables_to_remove = version_data
                    .version
                    .memtables
                    .immutables()
                    .iter()
                    .map(|m| m.id())
                    .collect();
                let flush_finished = FlushFinished {
                    region_id: self.region_id,
                    // The last entry has been flushed.
                    flushed_entry_id: version_data.last_entry_id,
                    senders: std::mem::take(&mut self.senders),
                    _timer: timer,
                    edit,
                    memtables_to_remove,
                };
                WorkerRequest::Background {
                    region_id: self.region_id,
                    notify: BackgroundNotify::FlushFinished(flush_finished),
                }
            }
            Err(e) => {
                error!(e; "Failed to flush region {}", self.region_id);
                // Discard the timer.
                timer.stop_and_discard();

                let err = Arc::new(e);
                self.on_failure(err.clone());
                WorkerRequest::Background {
                    region_id: self.region_id,
                    notify: BackgroundNotify::FlushFailed(FlushFailed { err }),
                }
            }
        };
        self.send_worker_request(worker_request).await;
    }

    /// Flushes memtables to level 0 SSTs and updates the manifest.
    /// Returns the [RegionEdit] to apply.
    async fn flush_memtables(&self, version_data: &VersionControlData) -> Result<RegionEdit> {
        // We must use the immutable memtables list and entry ids from the `version_data`
        // for consistency as others might already modify the version in the `version_control`.
        let version = &version_data.version;
        let timer = FLUSH_ELAPSED
            .with_label_values(&["flush_memtables"])
            .start_timer();

        let mut write_opts = WriteOptions {
            write_buffer_size: self.engine_config.sst_write_buffer_size,
            ..Default::default()
        };
        if let Some(row_group_size) = self.row_group_size {
            write_opts.row_group_size = row_group_size;
        }

        let memtables = version.memtables.immutables();
        let mut file_metas = Vec::with_capacity(memtables.len());
        let mut flushed_bytes = 0;
        for mem in memtables {
            if mem.is_empty() {
                // Skip empty memtables.
                continue;
            }

            let file_id = FileId::random();
            let iter = mem.iter(None, None)?;
            let source = Source::Iter(iter);

            // Flush to level 0.
            let write_request = SstWriteRequest {
                op_type: OperationType::Flush,
                file_id,
                metadata: version.metadata.clone(),
                source,
                cache_manager: self.cache_manager.clone(),
                storage: version.options.storage.clone(),
                index_options: self.index_options.clone(),
                inverted_index_config: self.engine_config.inverted_index.clone(),
                fulltext_index_config: self.engine_config.fulltext_index.clone(),
            };
            let Some(sst_info) = self
                .access_layer
                .write_sst(write_request, &write_opts)
                .await?
            else {
                // No data written.
                continue;
            };

            flushed_bytes += sst_info.file_size;
            let file_meta = FileMeta {
                region_id: self.region_id,
                file_id,
                time_range: sst_info.time_range,
                level: 0,
                file_size: sst_info.file_size,
                available_indexes: {
                    let mut indexes = SmallVec::new();
                    if sst_info.index_metadata.inverted_index.is_available() {
                        indexes.push(IndexType::InvertedIndex);
                    }
                    if sst_info.index_metadata.fulltext_index.is_available() {
                        indexes.push(IndexType::FulltextIndex);
                    }
                    indexes
                },
                index_file_size: sst_info.index_metadata.file_size,
                num_rows: sst_info.num_rows as u64,
                num_row_groups: sst_info.num_row_groups,
            };
            file_metas.push(file_meta);
        }

        if !file_metas.is_empty() {
            FLUSH_BYTES_TOTAL.inc_by(flushed_bytes);
        }

        let file_ids: Vec<_> = file_metas.iter().map(|f| f.file_id).collect();
        info!(
            "Successfully flush memtables, region: {}, reason: {}, files: {:?}, cost: {:?}s",
            self.region_id,
            self.reason.as_str(),
            file_ids,
            timer.stop_and_record(),
        );

        let edit = RegionEdit {
            files_to_add: file_metas,
            files_to_remove: Vec::new(),
            compaction_time_window: None,
            // The last entry has been flushed.
            flushed_entry_id: Some(version_data.last_entry_id),
            flushed_sequence: Some(version_data.committed_sequence),
        };
        info!("Applying {edit:?} to region {}", self.region_id);

        let action_list = RegionMetaActionList::with_action(RegionMetaAction::Edit(edit.clone()));
        // We will leak files if the manifest update fails, but we ignore them for simplicity. We can
        // add a cleanup job to remove them later.
        self.manifest_ctx
            .update_manifest(RegionState::Writable, action_list)
            .await?;

        Ok(edit)
    }

    /// Notify flush job status.
    async fn send_worker_request(&self, request: WorkerRequest) {
        if let Err(e) = self.request_sender.send(request).await {
            error!(
                "Failed to notify flush job status for region {}, request: {:?}",
                self.region_id, e.0
            );
        }
    }

    /// Merge two flush tasks.
    fn merge(&mut self, mut other: RegionFlushTask) {
        assert_eq!(self.region_id, other.region_id);
        // Now we only merge senders. They share the same flush reason.
        self.senders.append(&mut other.senders);
    }
}

/// Manages background flushes of a worker.
pub(crate) struct FlushScheduler {
    /// Tracks regions need to flush.
    region_status: HashMap<RegionId, FlushStatus>,
    /// Background job scheduler.
    scheduler: SchedulerRef,
}

impl FlushScheduler {
    /// Creates a new flush scheduler.
    pub(crate) fn new(scheduler: SchedulerRef) -> FlushScheduler {
        FlushScheduler {
            region_status: HashMap::new(),
            scheduler,
        }
    }

    /// Returns true if the region already requested flush.
    pub(crate) fn is_flush_requested(&self, region_id: RegionId) -> bool {
        self.region_status.contains_key(&region_id)
    }

    /// Schedules a flush `task` for specific `region`.
    pub(crate) fn schedule_flush(
        &mut self,
        region_id: RegionId,
        version_control: &VersionControlRef,
        task: RegionFlushTask,
    ) -> Result<()> {
        debug_assert_eq!(region_id, task.region_id);

        let version = version_control.current().version;
        if version.memtables.is_empty() {
            debug_assert!(!self.region_status.contains_key(&region_id));
            // The region has nothing to flush.
            task.on_success();
            return Ok(());
        }

        // Don't increase the counter if a region has nothing to flush.
        FLUSH_REQUESTS_TOTAL
            .with_label_values(&[task.reason.as_str()])
            .inc();

        // Add this region to status map.
        let flush_status = self
            .region_status
            .entry(region_id)
            .or_insert_with(|| FlushStatus::new(region_id, version_control.clone()));
        // Checks whether we can flush the region now.
        if flush_status.flushing {
            // There is already a flush job running.
            flush_status.merge_task(task);
            return Ok(());
        }

        // TODO(yingwen): We can merge with pending and execute directly.
        // If there are pending tasks, then we should push it to pending list.
        if flush_status.pending_task.is_some() {
            flush_status.merge_task(task);
            return Ok(());
        }

        // Now we can flush the region directly.
        if let Err(e) = version_control.freeze_mutable() {
            error!(e; "Failed to freeze the mutable memtable for region {}", region_id);

            // Remove from region status if we can't freeze the mutable memtable.
            self.region_status.remove(&region_id);
            return Err(e);
        }
        // Submit a flush job.
        let job = task.into_flush_job(version_control);
        if let Err(e) = self.scheduler.schedule(job) {
            // If scheduler returns error, senders in the job will be dropped and waiters
            // can get recv errors.
            error!(e; "Failed to schedule flush job for region {}", region_id);

            // Remove from region status if we can't submit the task.
            self.region_status.remove(&region_id);
            return Err(e);
        }
        flush_status.flushing = true;

        Ok(())
    }

    /// Notifies the scheduler that the flush job is finished.
    ///
    /// Returns all pending requests if the region doesn't need to flush again.
    pub(crate) fn on_flush_success(
        &mut self,
        region_id: RegionId,
    ) -> Option<(Vec<SenderDdlRequest>, Vec<SenderWriteRequest>)> {
        let flush_status = self.region_status.get_mut(&region_id)?;

        // This region doesn't have running flush job.
        flush_status.flushing = false;

        let pending_requests = if flush_status.pending_task.is_none() {
            // The region doesn't have any pending flush task.
            // Safety: The flush status must exist.
            let flush_status = self.region_status.remove(&region_id).unwrap();
            Some((flush_status.pending_ddls, flush_status.pending_writes))
        } else {
            let version_data = flush_status.version_control.current();
            if version_data.version.memtables.is_empty() {
                // The region has nothing to flush, we also need to remove it from the status.
                // Safety: The pending task is not None.
                let task = flush_status.pending_task.take().unwrap();
                // The region has nothing to flush. We can notify pending task.
                task.on_success();
                // `schedule_next_flush()` may pick up the same region to flush, so we must remove
                // it from the status to avoid leaking pending requests.
                // Safety: The flush status must exist.
                let flush_status = self.region_status.remove(&region_id).unwrap();
                Some((flush_status.pending_ddls, flush_status.pending_writes))
            } else {
                // We can flush the region again, keep it in the region status.
                None
            }
        };

        // Schedule next flush job.
        if let Err(e) = self.schedule_next_flush() {
            error!(e; "Flush of region {} is successful, but failed to schedule next flush", region_id);
        }

        pending_requests
    }

    /// Notifies the scheduler that the flush job is failed.
    pub(crate) fn on_flush_failed(&mut self, region_id: RegionId, err: Arc<Error>) {
        error!(err; "Region {} failed to flush, cancel all pending tasks", region_id);

        FLUSH_ERRORS_TOTAL.inc();

        // Remove this region.
        let Some(flush_status) = self.region_status.remove(&region_id) else {
            return;
        };

        // Fast fail: cancels all pending tasks and sends error to their waiters.
        flush_status.on_failure(err);

        // Still tries to schedule a new flush.
        if let Err(e) = self.schedule_next_flush() {
            error!(e; "Failed to schedule next flush after region {} flush is failed", region_id);
        }
    }

    /// Notifies the scheduler that the region is dropped.
    pub(crate) fn on_region_dropped(&mut self, region_id: RegionId) {
        self.remove_region_on_failure(
            region_id,
            Arc::new(RegionDroppedSnafu { region_id }.build()),
        );
    }

    /// Notifies the scheduler that the region is closed.
    pub(crate) fn on_region_closed(&mut self, region_id: RegionId) {
        self.remove_region_on_failure(region_id, Arc::new(RegionClosedSnafu { region_id }.build()));
    }

    /// Notifies the scheduler that the region is truncated.
    pub(crate) fn on_region_truncated(&mut self, region_id: RegionId) {
        self.remove_region_on_failure(
            region_id,
            Arc::new(RegionTruncatedSnafu { region_id }.build()),
        );
    }

    fn remove_region_on_failure(&mut self, region_id: RegionId, err: Arc<Error>) {
        // Remove this region.
        let Some(flush_status) = self.region_status.remove(&region_id) else {
            return;
        };

        // Notifies all pending tasks.
        flush_status.on_failure(err);
    }

    /// Add ddl request to pending queue.
    ///
    /// # Panics
    /// Panics if region didn't request flush.
    pub(crate) fn add_ddl_request_to_pending(&mut self, request: SenderDdlRequest) {
        let status = self.region_status.get_mut(&request.region_id).unwrap();
        status.pending_ddls.push(request);
    }

    /// Add write request to pending queue.
    ///
    /// # Panics
    /// Panics if region didn't request flush.
    pub(crate) fn add_write_request_to_pending(&mut self, request: SenderWriteRequest) {
        let status = self
            .region_status
            .get_mut(&request.request.region_id)
            .unwrap();
        status.pending_writes.push(request);
    }

    /// Returns true if the region has pending DDLs.
    pub(crate) fn has_pending_ddls(&self, region_id: RegionId) -> bool {
        self.region_status
            .get(&region_id)
            .map(|status| !status.pending_ddls.is_empty())
            .unwrap_or(false)
    }

    /// Schedules a new flush task when the scheduler can submit next task.
    pub(crate) fn schedule_next_flush(&mut self) -> Result<()> {
        debug_assert!(self
            .region_status
            .values()
            .all(|status| status.flushing || status.pending_task.is_some()));

        // Get the first region from status map.
        let Some(flush_status) = self
            .region_status
            .values_mut()
            .find(|status| status.pending_task.is_some())
        else {
            return Ok(());
        };
        debug_assert!(!flush_status.flushing);
        let task = flush_status.pending_task.take().unwrap();
        let region_id = flush_status.region_id;
        let version_control = flush_status.version_control.clone();

        self.schedule_flush(region_id, &version_control, task)
    }
}

impl Drop for FlushScheduler {
    fn drop(&mut self) {
        for (region_id, flush_status) in self.region_status.drain() {
            // We are shutting down so notify all pending tasks.
            flush_status.on_failure(Arc::new(RegionClosedSnafu { region_id }.build()));
        }
    }
}

/// Flush status of a region scheduled by the [FlushScheduler].
///
/// Tracks running and pending flush tasks and all pending requests of a region.
struct FlushStatus {
    /// Current region.
    region_id: RegionId,
    /// Version control of the region.
    version_control: VersionControlRef,
    /// There is a flush task running.
    ///
    /// It is possible that a region is not flushing but has pending task if the scheduler
    /// doesn't schedules this region.
    flushing: bool,
    /// Task waiting for next flush.
    pending_task: Option<RegionFlushTask>,
    /// Pending ddl requests.
    pending_ddls: Vec<SenderDdlRequest>,
    /// Requests waiting to write after altering the region.
    pending_writes: Vec<SenderWriteRequest>,
}

impl FlushStatus {
    fn new(region_id: RegionId, version_control: VersionControlRef) -> FlushStatus {
        FlushStatus {
            region_id,
            version_control,
            flushing: false,
            pending_task: None,
            pending_ddls: Vec::new(),
            pending_writes: Vec::new(),
        }
    }

    /// Merges the task to pending task.
    fn merge_task(&mut self, task: RegionFlushTask) {
        if let Some(pending) = &mut self.pending_task {
            pending.merge(task);
        } else {
            self.pending_task = Some(task);
        }
    }

    fn on_failure(self, err: Arc<Error>) {
        if let Some(mut task) = self.pending_task {
            task.on_failure(err.clone());
        }
        for ddl in self.pending_ddls {
            ddl.sender.send(Err(err.clone()).context(FlushRegionSnafu {
                region_id: self.region_id,
            }));
        }
        for write_req in self.pending_writes {
            write_req
                .sender
                .send(Err(err.clone()).context(FlushRegionSnafu {
                    region_id: self.region_id,
                }));
        }
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::oneshot;

    use super::*;
    use crate::cache::CacheManager;
    use crate::memtable::time_series::TimeSeriesMemtableBuilder;
    use crate::test_util::scheduler_util::{SchedulerEnv, VecScheduler};
    use crate::test_util::version_util::{write_rows_to_version, VersionControlBuilder};

    #[test]
    fn test_get_mutable_limit() {
        assert_eq!(4, WriteBufferManagerImpl::get_mutable_limit(8));
        assert_eq!(5, WriteBufferManagerImpl::get_mutable_limit(10));
        assert_eq!(32, WriteBufferManagerImpl::get_mutable_limit(64));
        assert_eq!(0, WriteBufferManagerImpl::get_mutable_limit(0));
    }

    #[test]
    fn test_over_mutable_limit() {
        // Mutable limit is 500.
        let manager = WriteBufferManagerImpl::new(1000);
        manager.reserve_mem(400);
        assert!(!manager.should_flush_engine());
        assert!(!manager.should_stall());

        // More than mutable limit.
        manager.reserve_mem(400);
        assert!(manager.should_flush_engine());

        // Freezes mutable.
        manager.schedule_free_mem(400);
        assert!(!manager.should_flush_engine());
        assert_eq!(800, manager.memory_used.load(Ordering::Relaxed));
        assert_eq!(400, manager.memory_active.load(Ordering::Relaxed));

        // Releases immutable.
        manager.free_mem(400);
        assert_eq!(400, manager.memory_used.load(Ordering::Relaxed));
        assert_eq!(400, manager.memory_active.load(Ordering::Relaxed));
    }

    #[test]
    fn test_over_global() {
        // Mutable limit is 500.
        let manager = WriteBufferManagerImpl::new(1000);
        manager.reserve_mem(1100);
        assert!(manager.should_stall());
        // Global usage is still 1100.
        manager.schedule_free_mem(200);
        assert!(manager.should_flush_engine());

        // More than global limit, but mutable (1100-200-450=450) is not enough (< 500).
        manager.schedule_free_mem(450);
        assert!(!manager.should_flush_engine());

        // Now mutable is enough.
        manager.reserve_mem(50);
        assert!(manager.should_flush_engine());
        manager.reserve_mem(100);
        assert!(manager.should_flush_engine());
    }

    #[test]
    fn test_manager_notify() {
        let (sender, receiver) = watch::channel(());
        let manager = WriteBufferManagerImpl::new(1000).with_notifier(sender);
        manager.reserve_mem(500);
        assert!(!receiver.has_changed().unwrap());
        manager.schedule_free_mem(500);
        assert!(!receiver.has_changed().unwrap());
        manager.free_mem(500);
        assert!(receiver.has_changed().unwrap());
    }

    #[tokio::test]
    async fn test_schedule_empty() {
        let env = SchedulerEnv::new().await;
        let (tx, _rx) = mpsc::channel(4);
        let mut scheduler = env.mock_flush_scheduler();
        let builder = VersionControlBuilder::new();

        let version_control = Arc::new(builder.build());
        let (output_tx, output_rx) = oneshot::channel();
        let mut task = RegionFlushTask {
            region_id: builder.region_id(),
            reason: FlushReason::Others,
            senders: Vec::new(),
            request_sender: tx,
            access_layer: env.access_layer.clone(),
            listener: WorkerListener::default(),
            engine_config: Arc::new(MitoConfig::default()),
            row_group_size: None,
            cache_manager: Arc::new(CacheManager::default()),
            manifest_ctx: env
                .mock_manifest_context(version_control.current().version.metadata.clone())
                .await,
            index_options: IndexOptions::default(),
        };
        task.push_sender(OptionOutputTx::from(output_tx));
        scheduler
            .schedule_flush(builder.region_id(), &version_control, task)
            .unwrap();
        assert!(scheduler.region_status.is_empty());
        let output = output_rx.await.unwrap().unwrap();
        assert_eq!(output, 0);
        assert!(scheduler.region_status.is_empty());
    }

    #[tokio::test]
    async fn test_schedule_pending_request() {
        let job_scheduler = Arc::new(VecScheduler::default());
        let env = SchedulerEnv::new().await.scheduler(job_scheduler.clone());
        let (tx, _rx) = mpsc::channel(4);
        let mut scheduler = env.mock_flush_scheduler();
        let mut builder = VersionControlBuilder::new();
        // Overwrites the empty memtable builder.
        builder.set_memtable_builder(Arc::new(TimeSeriesMemtableBuilder::default()));
        let version_control = Arc::new(builder.build());
        // Writes data to the memtable so it is not empty.
        let version_data = version_control.current();
        write_rows_to_version(&version_data.version, "host0", 0, 10);
        let manifest_ctx = env
            .mock_manifest_context(version_data.version.metadata.clone())
            .await;
        // Creates 3 tasks.
        let mut tasks: Vec<_> = (0..3)
            .map(|_| RegionFlushTask {
                region_id: builder.region_id(),
                reason: FlushReason::Others,
                senders: Vec::new(),
                request_sender: tx.clone(),
                access_layer: env.access_layer.clone(),
                listener: WorkerListener::default(),
                engine_config: Arc::new(MitoConfig::default()),
                row_group_size: None,
                cache_manager: Arc::new(CacheManager::default()),
                manifest_ctx: manifest_ctx.clone(),
                index_options: IndexOptions::default(),
            })
            .collect();
        // Schedule first task.
        let task = tasks.pop().unwrap();
        scheduler
            .schedule_flush(builder.region_id(), &version_control, task)
            .unwrap();
        // Should schedule 1 flush.
        assert_eq!(1, scheduler.region_status.len());
        assert_eq!(1, job_scheduler.num_jobs());
        // Check the new version.
        let version_data = version_control.current();
        assert_eq!(0, version_data.version.memtables.immutables()[0].id());
        // Schedule remaining tasks.
        let output_rxs: Vec<_> = tasks
            .into_iter()
            .map(|mut task| {
                let (output_tx, output_rx) = oneshot::channel();
                task.push_sender(OptionOutputTx::from(output_tx));
                scheduler
                    .schedule_flush(builder.region_id(), &version_control, task)
                    .unwrap();
                output_rx
            })
            .collect();
        // Assumes the flush job is finished.
        version_control.apply_edit(
            RegionEdit {
                files_to_add: Vec::new(),
                files_to_remove: Vec::new(),
                compaction_time_window: None,
                flushed_entry_id: None,
                flushed_sequence: None,
            },
            &[0],
            builder.file_purger(),
        );
        scheduler.on_flush_success(builder.region_id());
        // No new flush task.
        assert_eq!(1, job_scheduler.num_jobs());
        // The flush status is cleared.
        assert!(scheduler.region_status.is_empty());
        for output_rx in output_rxs {
            let output = output_rx.await.unwrap().unwrap();
            assert_eq!(output, 0);
        }
    }
}
