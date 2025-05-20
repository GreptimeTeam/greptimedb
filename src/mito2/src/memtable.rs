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

use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

pub use bulk::part::EncodedBulkPart;
use common_time::Timestamp;
use serde::{Deserialize, Serialize};
use store_api::metadata::RegionMetadataRef;
use store_api::storage::{ColumnId, SequenceNumber};
use table::predicate::Predicate;

use crate::config::MitoConfig;
use crate::error::Result;
use crate::flush::WriteBufferManagerRef;
use crate::memtable::key_values::KeyValue;
pub use crate::memtable::key_values::KeyValues;
use crate::memtable::partition_tree::{PartitionTreeConfig, PartitionTreeMemtableBuilder};
use crate::memtable::time_series::TimeSeriesMemtableBuilder;
use crate::metrics::WRITE_BUFFER_BYTES;
use crate::read::prune::PruneTimeIterator;
use crate::read::scan_region::PredicateGroup;
use crate::read::Batch;
use crate::region::options::{MemtableOptions, MergeMode};
use crate::sst::file::FileTimeRange;

mod builder;
pub mod bulk;
pub mod key_values;
pub mod partition_tree;
mod simple_bulk_memtable;
mod stats;
pub mod time_partition;
pub mod time_series;
pub(crate) mod version;

#[cfg(any(test, feature = "test"))]
pub use bulk::part::BulkPart;
#[cfg(any(test, feature = "test"))]
pub use time_partition::filter_record_batch;

/// Id for memtables.
///
/// Should be unique under the same region.
pub type MemtableId = u32;

/// Config for memtables.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MemtableConfig {
    PartitionTree(PartitionTreeConfig),
    TimeSeries,
}

impl Default for MemtableConfig {
    fn default() -> Self {
        Self::TimeSeries
    }
}

#[derive(Debug, Default)]
pub struct MemtableStats {
    /// The estimated bytes allocated by this memtable from heap.
    estimated_bytes: usize,
    /// The inclusive time range that this memtable contains. It is None if
    /// and only if the memtable is empty.
    time_range: Option<(Timestamp, Timestamp)>,
    /// Total rows in memtable
    num_rows: usize,
    /// Total number of ranges in the memtable.
    num_ranges: usize,
    /// The maximum sequence number in the memtable.
    max_sequence: SequenceNumber,
}

impl MemtableStats {
    /// Attaches the time range to the stats.
    #[cfg(any(test, feature = "test"))]
    pub(crate) fn with_time_range(mut self, time_range: Option<(Timestamp, Timestamp)>) -> Self {
        self.time_range = time_range;
        self
    }

    /// Returns the estimated bytes allocated by this memtable.
    pub fn bytes_allocated(&self) -> usize {
        self.estimated_bytes
    }

    /// Returns the time range of the memtable.
    pub fn time_range(&self) -> Option<(Timestamp, Timestamp)> {
        self.time_range
    }

    /// Returns the num of total rows in memtable.
    pub fn num_rows(&self) -> usize {
        self.num_rows
    }

    /// Returns the number of ranges in the memtable.
    pub fn num_ranges(&self) -> usize {
        self.num_ranges
    }

    /// Returns the maximum sequence number in the memtable.
    pub fn max_sequence(&self) -> SequenceNumber {
        self.max_sequence
    }
}

pub type BoxedBatchIterator = Box<dyn Iterator<Item = Result<Batch>> + Send>;

/// Ranges in a memtable.
#[derive(Default)]
pub struct MemtableRanges {
    /// Range IDs and ranges.
    pub ranges: BTreeMap<usize, MemtableRange>,
    /// Statistics of the memtable at the query time.
    pub stats: MemtableStats,
}

/// In memory write buffer.
pub trait Memtable: Send + Sync + fmt::Debug {
    /// Returns the id of this memtable.
    fn id(&self) -> MemtableId;

    /// Writes key values into the memtable.
    fn write(&self, kvs: &KeyValues) -> Result<()>;

    /// Writes one key value pair into the memtable.
    fn write_one(&self, key_value: KeyValue) -> Result<()>;

    /// Writes an encoded batch of into memtable.
    fn write_bulk(&self, part: crate::memtable::bulk::part::BulkPart) -> Result<()>;

    /// Scans the memtable.
    /// `projection` selects columns to read, `None` means reading all columns.
    /// `filters` are the predicates to be pushed down to memtable.
    fn iter(
        &self,
        projection: Option<&[ColumnId]>,
        predicate: Option<Predicate>,
        sequence: Option<SequenceNumber>,
    ) -> Result<BoxedBatchIterator>;

    /// Returns the ranges in the memtable.
    /// The returned map contains the range id and the range after applying the predicate.
    fn ranges(
        &self,
        projection: Option<&[ColumnId]>,
        predicate: PredicateGroup,
        sequence: Option<SequenceNumber>,
    ) -> Result<MemtableRanges>;

    /// Returns true if the memtable is empty.
    fn is_empty(&self) -> bool;

    /// Turns a mutable memtable into an immutable memtable.
    fn freeze(&self) -> Result<()>;

    /// Returns the [MemtableStats] info of Memtable.
    fn stats(&self) -> MemtableStats;

    /// Forks this (immutable) memtable and returns a new mutable memtable with specific memtable `id`.
    ///
    /// A region must freeze the memtable before invoking this method.
    fn fork(&self, id: MemtableId, metadata: &RegionMetadataRef) -> MemtableRef;
}

pub type MemtableRef = Arc<dyn Memtable>;

/// Builder to build a new [Memtable].
pub trait MemtableBuilder: Send + Sync + fmt::Debug {
    /// Builds a new memtable instance.
    fn build(&self, id: MemtableId, metadata: &RegionMetadataRef) -> MemtableRef;
}

pub type MemtableBuilderRef = Arc<dyn MemtableBuilder>;

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
        WRITE_BUFFER_BYTES.add(bytes as i64);
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

    /// Returns the write buffer manager.
    pub(crate) fn write_buffer_manager(&self) -> Option<WriteBufferManagerRef> {
        self.write_buffer_manager.clone()
    }
}

impl Drop for AllocTracker {
    fn drop(&mut self) {
        if !self.is_done_allocating.load(Ordering::Relaxed) {
            self.done_allocating();
        }

        let bytes_allocated = self.bytes_allocated.load(Ordering::Relaxed);
        WRITE_BUFFER_BYTES.sub(bytes_allocated as i64);

        // Memory tracked by this tracker is freed.
        if let Some(write_buffer_manager) = &self.write_buffer_manager {
            write_buffer_manager.free_mem(bytes_allocated);
        }
    }
}

/// Provider of memtable builders for regions.
#[derive(Clone)]
pub(crate) struct MemtableBuilderProvider {
    write_buffer_manager: Option<WriteBufferManagerRef>,
    config: Arc<MitoConfig>,
}

impl MemtableBuilderProvider {
    pub(crate) fn new(
        write_buffer_manager: Option<WriteBufferManagerRef>,
        config: Arc<MitoConfig>,
    ) -> Self {
        Self {
            write_buffer_manager,
            config,
        }
    }

    pub(crate) fn builder_for_options(
        &self,
        options: Option<&MemtableOptions>,
        dedup: bool,
        merge_mode: MergeMode,
    ) -> MemtableBuilderRef {
        match options {
            Some(MemtableOptions::TimeSeries) => Arc::new(TimeSeriesMemtableBuilder::new(
                self.write_buffer_manager.clone(),
                dedup,
                merge_mode,
            )),
            Some(MemtableOptions::PartitionTree(opts)) => {
                Arc::new(PartitionTreeMemtableBuilder::new(
                    PartitionTreeConfig {
                        index_max_keys_per_shard: opts.index_max_keys_per_shard,
                        data_freeze_threshold: opts.data_freeze_threshold,
                        fork_dictionary_bytes: opts.fork_dictionary_bytes,
                        dedup,
                        merge_mode,
                    },
                    self.write_buffer_manager.clone(),
                ))
            }
            None => self.default_memtable_builder(dedup, merge_mode),
        }
    }

    fn default_memtable_builder(&self, dedup: bool, merge_mode: MergeMode) -> MemtableBuilderRef {
        match &self.config.memtable {
            MemtableConfig::PartitionTree(config) => {
                let mut config = config.clone();
                config.dedup = dedup;
                Arc::new(PartitionTreeMemtableBuilder::new(
                    config,
                    self.write_buffer_manager.clone(),
                ))
            }
            MemtableConfig::TimeSeries => Arc::new(TimeSeriesMemtableBuilder::new(
                self.write_buffer_manager.clone(),
                dedup,
                merge_mode,
            )),
        }
    }
}

/// Builder to build an iterator to read the range.
/// The builder should know the projection and the predicate to build the iterator.
pub trait IterBuilder: Send + Sync {
    /// Returns the iterator to read the range.
    fn build(&self) -> Result<BoxedBatchIterator>;
}

pub type BoxedIterBuilder = Box<dyn IterBuilder>;

/// Context shared by ranges of the same memtable.
pub struct MemtableRangeContext {
    /// Id of the memtable.
    id: MemtableId,
    /// Iterator builder.
    builder: BoxedIterBuilder,
    /// All filters.
    predicate: PredicateGroup,
}

pub type MemtableRangeContextRef = Arc<MemtableRangeContext>;

impl MemtableRangeContext {
    /// Creates a new [MemtableRangeContext].
    pub fn new(id: MemtableId, builder: BoxedIterBuilder, predicate: PredicateGroup) -> Self {
        Self {
            id,
            builder,
            predicate,
        }
    }
}

/// A range in the memtable.
#[derive(Clone)]
pub struct MemtableRange {
    /// Shared context.
    context: MemtableRangeContextRef,
}

impl MemtableRange {
    /// Creates a new range from context.
    pub fn new(context: MemtableRangeContextRef) -> Self {
        Self { context }
    }

    /// Returns the id of the memtable to read.
    pub fn id(&self) -> MemtableId {
        self.context.id
    }

    /// Builds an iterator to read the range.
    /// Filters the result by the specific time range, this ensures memtable won't return
    /// rows out of the time range when new rows are inserted.
    pub fn build_iter(&self, time_range: FileTimeRange) -> Result<BoxedBatchIterator> {
        let iter = self.context.builder.build()?;
        let time_filters = self.context.predicate.time_filters();
        Ok(Box::new(PruneTimeIterator::new(
            iter,
            time_range,
            time_filters,
        )))
    }
}

#[cfg(test)]
mod tests {
    use common_base::readable_size::ReadableSize;

    use super::*;
    use crate::flush::{WriteBufferManager, WriteBufferManagerImpl};

    #[test]
    fn test_deserialize_memtable_config() {
        let s = r#"
type = "partition_tree"
index_max_keys_per_shard = 8192
data_freeze_threshold = 1024
dedup = true
fork_dictionary_bytes = "512MiB"
"#;
        let config: MemtableConfig = toml::from_str(s).unwrap();
        let MemtableConfig::PartitionTree(memtable_config) = config else {
            unreachable!()
        };
        assert!(memtable_config.dedup);
        assert_eq!(8192, memtable_config.index_max_keys_per_shard);
        assert_eq!(1024, memtable_config.data_freeze_threshold);
        assert_eq!(ReadableSize::mb(512), memtable_config.fork_dictionary_bytes);
    }

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
