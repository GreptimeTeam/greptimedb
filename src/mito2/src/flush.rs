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

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use object_store::ObjectStore;
use store_api::storage::RegionId;
use tokio::sync::{mpsc, oneshot};

use crate::error::Result;
use crate::memtable::MemtableBuilderRef;
use crate::region::MitoRegionRef;
use crate::request::{SenderDdlRequest, SenderWriteRequest, WorkerRequest};
use crate::schedule::scheduler::SchedulerRef;

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
    /// Memtable is full.
    MemtableFull,
    /// Engine reaches flush threshold.
    EngineFull,
    // TODO(yingwen): Alter, manually.
}

/// Task to flush a region.
pub(crate) struct RegionFlushTask {
    /// Region to flush.
    pub(crate) region_id: RegionId,
    /// Reason to flush.
    pub(crate) reason: FlushReason,
    /// Flush result sender.
    pub(crate) sender: Option<oneshot::Sender<Result<()>>>,
    /// Request sender to notify the worker.
    pub(crate) request_sender: mpsc::Sender<WorkerRequest>,

    pub(crate) object_store: ObjectStore,
    pub(crate) memtable_builder: MemtableBuilderRef,
}

impl RegionFlushTask {
    /// Consumes the task and notify the sender the job is success.
    fn on_success(self) {
        if let Some(sender) = self.sender {
            let _ = sender.send(Ok(()));
        }
    }
}

/// Manages background flushes of a worker.
pub(crate) struct FlushScheduler {
    /// Pending flush tasks.
    queue: VecDeque<RegionFlushTask>,
    /// Tracks regions need to flush.
    region_status: HashMap<RegionId, FlushStatus>,
    // TODO(yingwen): Support global flush concurrency control. We can implement a global permits for this.
    /// Has running flush job.
    has_flush_running: bool,
    /// Background job scheduler.
    scheduler: SchedulerRef,
}

impl FlushScheduler {
    /// Creates a new flush scheduler.
    pub(crate) fn new(scheduler: SchedulerRef) -> FlushScheduler {
        FlushScheduler {
            queue: VecDeque::new(),
            region_status: HashMap::new(),
            has_flush_running: false,
            scheduler,
        }
    }

    /// Returns true if the region is stalling.
    pub(crate) fn is_stalling(&self, region_id: RegionId) -> bool {
        self.region_status
            .get(&region_id)
            .map(|status| status.stalling)
            .unwrap_or(false)
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
        if version.memtables.mutable.is_empty() && version.memtables.immutable.is_none() {
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
        if flush_status.flushing_task.is_some() {
            // There is already a flush job running, mark as stalling.
            flush_status.stalling = true;
            self.queue.push_back(task);
            return Ok(());
        }

        // Checks flush job limit.
        if !self.queue.is_empty() || self.has_flush_running {
            debug_assert!(self.has_flush_running);
            // We reach job limit.
            self.queue.push_back(task);
            return Ok(());
        }

        // Now we can flush the region, try to freeze the mutable memtable.
        region.version_control.maybe_freeze_mutable(&task.memtable_builder);
        // Flush the immutable memtable.

        self.has_flush_running = true;

        todo!()
    }

    /// Add write `request` to pending queue.
    ///
    /// Returns error if region is not stalling.
    pub(crate) fn add_write_request_to_pending(
        &mut self,
        request: SenderWriteRequest,
    ) -> Result<(), SenderWriteRequest> {
        if let Some(status) = self.region_status.get_mut(&request.request.region_id) {
            if status.stalling {
                status.pending_writes.push(request);
                return Ok(());
            }
        }

        Err(request)
    }

    /// Add ddl request to pending queue.
    ///
    /// Returns error if region is not stalling.
    pub(crate) fn add_ddl_request_to_pending(
        &mut self,
        request: SenderDdlRequest,
    ) -> Result<(), SenderDdlRequest> {
        if let Some(status) = self.region_status.get_mut(&request.region_id) {
            if status.stalling {
                status.pending_ddls.push(request);
                return Ok(());
            }
        }

        Err(request)
    }
}

/// Flush status of a region scheduled by the [FlushScheduler].
///
/// Tracks running and pending flusht tasks and all pending requests of a region.
struct FlushStatus {
    /// Current region.
    region: MitoRegionRef,
    /// Current running flush task.
    flushing_task: Option<RegionFlushTask>,
    /// The number of flush requests waiting in queue.
    num_queueing: usize,
    /// The region is stalling.
    stalling: bool,
    /// Pending write requests.
    pending_writes: Vec<SenderWriteRequest>,
    /// Pending ddl requests.
    pending_ddls: Vec<SenderDdlRequest>,
}

impl FlushStatus {
    fn new(region: MitoRegionRef) -> FlushStatus {
        FlushStatus {
            region,
            flushing_task: None,
            num_queueing: 0,
            stalling: false,
            pending_writes: Vec::new(),
            pending_ddls: Vec::new(),
        }
    }
}
