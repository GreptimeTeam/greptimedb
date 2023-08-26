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

use store_api::storage::{RegionId, SequenceNumber};
use tokio::sync::oneshot::Sender;

use crate::error::Result;
use crate::memtable::MemtableId;
use crate::region::MitoRegionRef;
use crate::request::{RegionTask, SenderWriteRequest};

/// Global write buffer (memtable) manager.
///
/// Tracks write buffer (memtable) usages and decide whether the engine needs to flush.
pub trait WriteBufferManager: Send + Sync + std::fmt::Debug {
    /// Returns whether to trigger the engine.
    fn should_flush_engine(&self) -> bool;

    /// Returns whether the mutable memtable of this region needs to flush.
    fn should_flush_region(&self, stats: RegionMemtableStats) -> bool;

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

/// Statistics of a region's memtable.
#[derive(Debug)]
pub struct RegionMemtableStats {
    /// Size of the mutable memtable.
    pub bytes_mutable: usize,
    /// Write buffer size of the region.
    pub write_buffer_size: usize,
}

// TODO(yingwen): Implements the manager.
#[derive(Debug)]
pub struct WriteBufferManagerImpl {}

impl WriteBufferManager for WriteBufferManagerImpl {
    fn should_flush_engine(&self) -> bool {
        false
    }

    fn should_flush_region(&self, _stats: RegionMemtableStats) -> bool {
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
    pub(crate) sender: Option<Sender<Result<()>>>,
}

/// Manages background flushes of a worker.
#[derive(Default)]
pub(crate) struct FlushScheduler {
    /// Pending flush tasks.
    queue: VecDeque<RegionFlushTask>,
    region_status: HashMap<RegionId, FlushStatus>,
    /// Number of running flush jobs.
    num_flush_running: usize,
}

impl FlushScheduler {
    /// Returns true if the region is stalling.
    pub(crate) fn is_stalling(&self, region_id: RegionId) -> bool {
        todo!()
    }

    /// Schedules a flush `task` for specific `region`.
    pub(crate) fn schedule_flush(&self, region: &MitoRegionRef, task: RegionFlushTask) {
        todo!()
    }

    /// Add write `request` to pending queue.
    pub(crate) fn add_write_request_to_pending(&mut self, request: SenderWriteRequest) {
        todo!()
    }

    /// Add ddl `task` to pending queue.
    pub(crate) fn add_ddl_request_to_pending(&mut self, region_id: RegionId, task: RegionTask) {
        todo!()
    }
}

/// Flush status of a region.
struct FlushStatus {
    /// Current region.
    region: MitoRegionRef,
    /// Current running flush task.
    flushing: Option<RegionFlushTask>,
    /// The number of flush requests waiting in queue.
    num_queueing: usize,
    /// The region is stalling.
    stalling: bool,
    /// Pending write requests.
    pending_writes: Vec<SenderWriteRequest>,
    /// Pending ddl tasks.
    pending_ddls: Vec<RegionTask>,
}
