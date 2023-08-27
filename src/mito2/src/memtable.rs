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

//! Memtables are write buffers for regions.

pub mod time_series;

pub mod key_values;
pub(crate) mod version;

use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::sync::Arc;

use common_time::Timestamp;
use metrics::{decrement_gauge, increment_gauge};
use store_api::metadata::RegionMetadataRef;
use store_api::storage::ScanRequest;

use crate::error::Result;
pub use crate::memtable::key_values::KeyValues;
use crate::metrics::WRITE_BUFFER_BYTES;
use crate::read::Batch;

/// Id for memtables.
///
/// Should be unique under the same region.
pub type MemtableId = u32;

#[derive(Debug, Default)]
pub struct MemtableStats {
    /// The  estimated bytes allocated by this memtable from heap.
    pub estimated_bytes: usize,
    /// The max timestamp that this memtable contains.
    pub max_timestamp: Timestamp,
    /// The min timestamp that this memtable contains.
    pub min_timestamp: Timestamp,
}

impl MemtableStats {
    pub fn bytes_allocated(&self) -> usize {
        self.estimated_bytes
    }
}

pub type BoxedBatchIterator = Box<dyn Iterator<Item = Result<Batch>> + Send + Sync>;

/// In memory write buffer.
pub trait Memtable: Send + Sync + fmt::Debug {
    /// Returns the id of this memtable.
    fn id(&self) -> MemtableId;

    /// Write key values into the memtable.
    fn write(&self, kvs: &KeyValues) -> Result<()>;

    /// Scans the memtable for `req`.
    fn iter(&self, req: ScanRequest) -> BoxedBatchIterator;

    /// Returns true if the memtable is empty.
    fn is_empty(&self) -> bool;

    /// Mark the memtable as immutable.
    fn mark_immutable(&self);

    /// Returns the [MemtableStats] info of Memtable.
    fn stats(&self) -> MemtableStats;
}

pub type MemtableRef = Arc<dyn Memtable>;

/// Builder to build a new [Memtable].
pub trait MemtableBuilder: Send + Sync + fmt::Debug {
    /// Builds a new memtable instance.
    fn build(&self, metadata: &RegionMetadataRef) -> MemtableRef;
}

pub type MemtableBuilderRef = Arc<dyn MemtableBuilder>;

// TODO(yingwen): Remove it once we port the memtable.
/// Empty memtable for test.
#[derive(Debug, Default)]
pub(crate) struct EmptyMemtable {
    /// Id of this memtable.
    id: MemtableId,
}

impl EmptyMemtable {
    /// Returns a new memtable with specific `id`.
    pub(crate) fn new(id: MemtableId) -> EmptyMemtable {
        EmptyMemtable { id }
    }
}

impl Memtable for EmptyMemtable {
    fn id(&self) -> MemtableId {
        self.id
    }

    fn write(&self, _kvs: &KeyValues) -> Result<()> {
        Ok(())
    }

    fn iter(&self, _req: ScanRequest) -> BoxedBatchIterator {
        Box::new(std::iter::empty())
    }

    fn is_empty(&self) -> bool {
        true
    }

    fn mark_immutable(&self) {}

    fn stats(&self) -> MemtableStats {
        MemtableStats::default()
    }
}

/// Memtable memory allocation tracker.
#[derive(Default)]
pub struct AllocTracker {
    // TODO(hl): add FlushStrategy so that we can reserve memory on allocation.
    /// Bytes allocated by the tracker.
    bytes_allocated: AtomicUsize,
    /// Whether allocating is done.
    is_done_allocating: AtomicBool,
}

impl fmt::Debug for AllocTracker {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("AllocTracker")
            .field("bytes_allocated", &self.bytes_allocated)
            .field("is_done_allocating", &self.is_done_allocating)
            .finish()
    }
}

impl AllocTracker {
    /// Returns a new [AllocTracker].
    pub fn new() -> AllocTracker {
        AllocTracker {
            bytes_allocated: AtomicUsize::new(0),
            is_done_allocating: AtomicBool::new(false),
        }
    }

    /// Tracks `bytes` memory is allocated.
    pub(crate) fn on_allocation(&self, bytes: usize) {
        let _ = self.bytes_allocated.fetch_add(bytes, Ordering::Relaxed);
        increment_gauge!(WRITE_BUFFER_BYTES, bytes as f64);
        // if let Some(flush_strategy) = &self.flush_strategy {
        //     flush_strategy.reserve_mem(bytes);
        // }
    }

    /// Marks we have finished allocating memory so we can free it from
    /// the write buffer's limit.
    ///
    /// The region MUST ensure that it calls this method inside the region writer's write lock.
    pub(crate) fn done_allocating(&self) {
        // if let Some(flush_strategy) = &self.flush_strategy {
        //     if self
        //         .is_done_allocating
        //         .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
        //         .is_ok()
        //     {
        //         flush_strategy.schedule_free_mem(self.bytes_allocated.load(Ordering::Relaxed));
        //     }
        // }
    }

    /// Returns bytes allocated.
    pub(crate) fn bytes_allocated(&self) -> usize {
        self.bytes_allocated.load(Ordering::Relaxed)
    }
}

impl Drop for AllocTracker {
    fn drop(&mut self) {
        if !self.is_done_allocating.load(Ordering::Relaxed) {
            self.done_allocating();
        }

        let bytes_allocated = self.bytes_allocated.load(Ordering::Relaxed);
        decrement_gauge!(WRITE_BUFFER_BYTES, bytes_allocated as f64);

        // Memory tracked by this tracker is freed.
        // if let Some(flush_strategy) = &self.flush_strategy {
        //     flush_strategy.free_mem(bytes_allocated);
        // }
    }
}

/// Default memtable builder.
#[derive(Debug, Default)]
pub(crate) struct DefaultMemtableBuilder {
    /// Next memtable id.
    next_id: AtomicU32,
}

impl MemtableBuilder for DefaultMemtableBuilder {
    fn build(&self, _metadata: &RegionMetadataRef) -> MemtableRef {
        Arc::new(EmptyMemtable::new(
            self.next_id.fetch_add(1, Ordering::Relaxed),
        ))
    }
}
