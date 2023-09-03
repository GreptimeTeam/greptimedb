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
use std::sync::Arc;

use common_query::Output;
use common_telemetry::{error, info};
use snafu::ResultExt;
use store_api::storage::RegionId;
use tokio::sync::{mpsc, oneshot};

use crate::access_layer::AccessLayerRef;
use crate::error::{Error, FlushRegionSnafu, RegionClosedSnafu, RegionDroppedSnafu, Result};
use crate::memtable::MemtableBuilderRef;
use crate::read::Source;
use crate::region::version::{VersionControlData, VersionRef};
use crate::region::MitoRegionRef;
use crate::request::{
    BackgroundNotify, FlushFailed, FlushFinished, SenderDdlRequest, WorkerRequest,
};
use crate::schedule::scheduler::{Job, SchedulerRef};
use crate::sst::file::{FileId, FileMeta};
use crate::sst::file_purger::FilePurgerRef;
use crate::sst::parquet::WriteOptions;

/// Global write buffer (memtable) manager.
///
/// Tracks write buffer (memtable) usages and decide whether the engine needs to flush.
pub trait WriteBufferManager: Send + Sync + std::fmt::Debug {
    /// Returns whether to trigger the engine.
    fn should_flush_engine(&self) -> bool;

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

// TODO(yingwen): Implements the manager.
#[derive(Debug)]
pub struct WriteBufferManagerImpl {}

impl WriteBufferManager for WriteBufferManagerImpl {
    fn should_flush_engine(&self) -> bool {
        false
    }

    fn reserve_mem(&self, _mem: usize) {}

    fn schedule_free_mem(&self, _mem: usize) {}

    fn free_mem(&self, _mem: usize) {}

    fn memory_usage(&self) -> usize {
        0
    }
}

/// Reason of a flush task.
pub enum FlushReason {
    /// Other reasons.
    Others,
    /// Engine reaches flush threshold.
    EngineFull,
    /// Manual flush.
    Manual,
    // TODO(yingwen): Alter.
}

/// Task to flush a region.
pub(crate) struct RegionFlushTask {
    /// Region to flush.
    pub(crate) region_id: RegionId,
    /// Reason to flush.
    pub(crate) reason: FlushReason,
    /// Flush result senders.
    pub(crate) senders: Vec<oneshot::Sender<Result<Output>>>,
    /// Request sender to notify the worker.
    pub(crate) request_sender: mpsc::Sender<WorkerRequest>,

    pub(crate) access_layer: AccessLayerRef,
    pub(crate) memtable_builder: MemtableBuilderRef,
    pub(crate) file_purger: FilePurgerRef,
}

impl RegionFlushTask {
    /// Consumes the task and notify the sender the job is success.
    fn on_success(self) {
        for sender in self.senders {
            let _ = sender.send(Ok(Output::AffectedRows(0)));
        }
    }

    /// Send flush error to waiter.
    fn on_failure(&mut self, err: Arc<Error>) {
        for sender in self.senders.drain(..) {
            // Ignore send result.
            let _ = sender.send(Err(err.clone()).context(FlushRegionSnafu {
                region_id: self.region_id,
            }));
        }
    }

    /// Converts the flush task into a background job.
    fn into_flush_job(mut self, region: &MitoRegionRef) -> Job {
        // Get a version of this region before creating a job so we
        // always have a consistent memtable list.
        let version_data = region.version_control.current();

        Box::pin(async move {
            self.do_flush(version_data).await;
        })
    }

    /// Runs the flush task.
    async fn do_flush(&mut self, version_data: VersionControlData) {
        let worker_request = match self.flush_memtables(&version_data.version).await {
            Ok(file_metas) => {
                let memtables_to_remove = version_data
                    .version
                    .memtables
                    .immutables()
                    .iter()
                    .map(|m| m.id())
                    .collect();
                let flush_finished = FlushFinished {
                    region_id: self.region_id,
                    file_metas,
                    // The last entry has been flushed.
                    flushed_entry_id: version_data.last_entry_id,
                    memtables_to_remove,
                    senders: std::mem::take(&mut self.senders),
                    file_purger: self.file_purger.clone(),
                };
                WorkerRequest::Background {
                    region_id: self.region_id,
                    notify: BackgroundNotify::FlushFinished(flush_finished),
                }
            }
            Err(e) => {
                error!(e; "Failed to flush region {}", self.region_id);
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

    /// Flushes memtables to level 0 SSTs.
    async fn flush_memtables(&self, version: &VersionRef) -> Result<Vec<FileMeta>> {
        // TODO(yingwen): Make it configurable.
        let write_opts = WriteOptions::default();
        let memtables = version.memtables.immutables();
        let mut file_metas = Vec::with_capacity(memtables.len());

        for mem in memtables {
            if mem.is_empty() {
                // Skip empty memtables.
                continue;
            }

            let file_id = FileId::random();
            let iter = mem.iter(None, &[]);
            let source = Source::Iter(iter);
            let mut writer = self
                .access_layer
                .write_sst(file_id, version.metadata.clone(), source);
            let Some(sst_info) = writer.write_all(&write_opts).await? else {
                // No data written.
                continue;
            };

            file_metas.push(FileMeta {
                region_id: version.metadata.region_id,
                file_id,
                time_range: sst_info.time_range,
                level: 0,
                file_size: sst_info.file_size,
            });
        }

        let file_ids: Vec<_> = file_metas.iter().map(|f| f.file_id).collect();
        info!(
            "Successfully flush memtables, region: {}, files: {:?}",
            version.metadata.region_id, file_ids
        );

        Ok(file_metas)
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
        region: &MitoRegionRef,
        task: RegionFlushTask,
    ) -> Result<()> {
        debug_assert_eq!(region.region_id, task.region_id);

        let version = region.version_control.current().version;
        if version.memtables.mutable.is_empty() && version.memtables.immutables().is_empty() {
            debug_assert!(!self.region_status.contains_key(&region.region_id));
            // The region has nothing to flush.
            task.on_success();
            return Ok(());
        }

        // Add this region to status map.
        let flush_status = self
            .region_status
            .entry(region.region_id)
            .or_insert_with(|| FlushStatus::new(region.clone()));
        // Checks whether we can flush the region now.
        if flush_status.flushing {
            // There is already a flush job running.
            flush_status.push_task(task);
            return Ok(());
        }

        // If there are pending tasks, then we should push it to pending list.
        if flush_status.pending_task.is_some() {
            flush_status.push_task(task);
            return Ok(());
        }

        // Now we can flush the region directly.
        region
            .version_control
            .freeze_mutable(&task.memtable_builder);
        // Submit a flush job.
        let job = task.into_flush_job(region);
        if let Err(e) = self.scheduler.schedule(job) {
            // If scheduler returns error, senders in the job will be dropped and waiters
            // can get recv errors.
            error!(e; "Failed to schedule flush job for region {}", region.region_id);

            // Remove from region status if we can't submit the task.
            self.region_status.remove(&region.region_id);
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
    ) -> Option<Vec<SenderDdlRequest>> {
        let Some(flush_status) = self.region_status.get_mut(&region_id) else {
            return None;
        };

        // This region doesn't have running flush job.
        flush_status.flushing = false;

        let pending_ddls = if flush_status.pending_task.is_none() {
            // The region doesn't have any pending flush task.
            // Safety: The flush status exists.
            let flush_status = self.region_status.remove(&region_id).unwrap();
            Some(flush_status.pending_ddls)
        } else {
            None
        };

        // Schedule next flush job.
        if let Err(e) = self.schedule_next_flush() {
            error!(e; "Flush of region {} is successful, but failed to schedule next flush", region_id);
        }

        pending_ddls
    }

    /// Notifies the scheduler that the flush job is finished.
    pub(crate) fn on_flush_failed(&mut self, region_id: RegionId, err: Arc<Error>) {
        error!(err; "Region {} failed to flush, cancel all pending tasks", region_id);

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
        // Remove this region.
        let Some(flush_status) = self.region_status.remove(&region_id) else {
            return;
        };

        // Notifies all pending tasks.
        flush_status.on_failure(Arc::new(RegionDroppedSnafu { region_id }.build()));
    }

    /// Notifies the scheduler that the region is closed.
    pub(crate) fn on_region_closed(&mut self, region_id: RegionId) {
        // Remove this region.
        let Some(flush_status) = self.region_status.remove(&region_id) else {
            return;
        };

        // Notifies all pending tasks.
        flush_status.on_failure(Arc::new(RegionClosedSnafu { region_id }.build()));
    }

    /// Add ddl request to pending queue.
    ///
    /// Returns error if region doesn't request flush.
    pub(crate) fn add_ddl_request_to_pending(
        &mut self,
        request: SenderDdlRequest,
    ) -> Result<(), SenderDdlRequest> {
        if let Some(status) = self.region_status.get_mut(&request.region_id) {
            status.pending_ddls.push(request);
            return Ok(());
        }

        Err(request)
    }

    /// Schedules a new flush task when the scheduler can submit next task.
    pub(crate) fn schedule_next_flush(&mut self) -> Result<()> {
        debug_assert!(self
            .region_status
            .values()
            .all(|status| !status.flushing && status.pending_task.is_some()));

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
        let region = flush_status.region.clone();

        self.schedule_flush(&region, task)
    }
}

/// Flush status of a region scheduled by the [FlushScheduler].
///
/// Tracks running and pending flush tasks and all pending requests of a region.
struct FlushStatus {
    /// Current region.
    region: MitoRegionRef,
    /// There is a flush task running.
    flushing: bool,
    /// Task waiting for next flush.
    pending_task: Option<RegionFlushTask>,
    /// Pending ddl requests.
    pending_ddls: Vec<SenderDdlRequest>,
}

impl FlushStatus {
    fn new(region: MitoRegionRef) -> FlushStatus {
        FlushStatus {
            region,
            flushing: false,
            pending_task: None,
            pending_ddls: Vec::new(),
        }
    }

    fn push_task(&mut self, task: RegionFlushTask) {
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
            if let Some(sender) = ddl.sender {
                let _ = sender.send(Err(err.clone()).context(FlushRegionSnafu {
                    region_id: self.region.region_id,
                }));
            }
        }
    }
}
