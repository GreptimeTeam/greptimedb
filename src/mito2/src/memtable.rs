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

use common_query::logical_plan::Expr;
use common_time::Timestamp;
use metrics::{decrement_gauge, increment_gauge};
use store_api::metadata::RegionMetadataRef;
use store_api::storage::ColumnId;

use crate::error::Result;
use crate::flush::WriteBufferManagerRef;
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
    estimated_bytes: usize,
    /// The time range that this memtable contains.
    time_range: Option<(Timestamp, Timestamp)>,
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

    /// Scans the memtable.
    /// `projection` selects columns to read, `None` means reading all columns.
    /// `filters` are the predicates to be pushed down to memtable.
    fn iter(&self, projection: Option<&[ColumnId]>, filters: &[Expr]) -> BoxedBatchIterator;

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

    fn iter(&self, _projection: Option<&[ColumnId]>, _filters: &[Expr]) -> BoxedBatchIterator {
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
    write_buffer_manager: Option<WriteBufferManagerRef>,
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
    pub fn new(write_buffer_manager: Option<WriteBufferManagerRef>) -> AllocTracker {
        AllocTracker {
            write_buffer_manager,
            bytes_allocated: AtomicUsize::new(0),
            is_done_allocating: AtomicBool::new(false),
        }
    }

    /// Tracks `bytes` memory is allocated.
    pub(crate) fn on_allocation(&self, bytes: usize) {
        self.bytes_allocated.fetch_add(bytes, Ordering::Relaxed);
        increment_gauge!(WRITE_BUFFER_BYTES, bytes as f64);
        if let Some(write_buffer_manager) = &self.write_buffer_manager {
            write_buffer_manager.reserve_mem(bytes);
        }
    }

    /// Marks we have finished allocating memory so we can free it from
    /// the write buffer's limit.
    ///
    /// The region MUST ensure that it calls this method inside the region writer's write lock.
    pub(crate) fn done_allocating(&self) {
        if let Some(write_buffer_manager) = &self.write_buffer_manager {
            if self
                .is_done_allocating
                .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                write_buffer_manager
                    .schedule_free_mem(self.bytes_allocated.load(Ordering::Relaxed));
            }
        }
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
        if let Some(write_buffer_manager) = &self.write_buffer_manager {
            write_buffer_manager.free_mem(bytes_allocated);
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::flush::{WriteBufferManager, WriteBufferManagerImpl};

    #[test]
    fn test_alloc_tracker_without_manager() {
        let tracker = AllocTracker::new(None);
        assert_eq!(0, tracker.bytes_allocated());
        tracker.on_allocation(100);
        assert_eq!(100, tracker.bytes_allocated());
        tracker.on_allocation(200);
        assert_eq!(300, tracker.bytes_allocated());

        tracker.done_allocating();
        assert_eq!(300, tracker.bytes_allocated());
    }

    #[test]
    fn test_alloc_tracker_with_manager() {
        let manager = Arc::new(WriteBufferManagerImpl::new(1000));
        {
            let tracker = AllocTracker::new(Some(manager.clone() as WriteBufferManagerRef));

            tracker.on_allocation(100);
            assert_eq!(100, tracker.bytes_allocated());
            assert_eq!(100, manager.memory_usage());
            assert_eq!(100, manager.mutable_usage());

            for _ in 0..2 {
                // Done allocating won't free the same memory multiple times.
                tracker.done_allocating();
                assert_eq!(100, manager.memory_usage());
                assert_eq!(0, manager.mutable_usage());
            }
        }

        assert_eq!(0, manager.memory_usage());
        assert_eq!(0, manager.mutable_usage());
    }

    #[test]
    fn test_alloc_tracker_without_done_allocating() {
        let manager = Arc::new(WriteBufferManagerImpl::new(1000));
        {
            let tracker = AllocTracker::new(Some(manager.clone() as WriteBufferManagerRef));

            tracker.on_allocation(100);
            assert_eq!(100, tracker.bytes_allocated());
            assert_eq!(100, manager.memory_usage());
            assert_eq!(100, manager.mutable_usage());
        }

        assert_eq!(0, manager.memory_usage());
        assert_eq!(0, manager.mutable_usage());
    }
}
