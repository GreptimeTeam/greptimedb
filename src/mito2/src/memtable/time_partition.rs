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

//! Partitions memtables by time.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use common_time::Timestamp;
use smallvec::SmallVec;
use store_api::metadata::RegionMetadataRef;

use crate::error::Result;
use crate::memtable::version::SmallMemtableVec;
use crate::memtable::{KeyValues, MemtableBuilderRef, MemtableId, MemtableRef};

/// A partition holds rows with timestamps between `[min, max)`.
#[derive(Debug, Clone)]
pub struct TimePartition {
    /// Memtable of the partition.
    memtable: MemtableRef,
    /// Time range of the partition. `None` means there is no time range.
    time_range: Option<PartTimeRange>,
}

type PartitionVec = SmallVec<[TimePartition; 2]>;

/// Partitions.
#[derive(Debug)]
pub struct TimePartitions {
    /// Mutable data of partitions.
    inner: Mutex<PartitionsInner>,
    /// Duration of a partition.
    ///
    /// `None` means there is only one partition.
    part_duration: Option<Duration>,
    /// Metadata of the region.
    metadata: RegionMetadataRef,
    /// Builder of memtables.
    builder: MemtableBuilderRef,
}

pub type TimePartitionsRef = Arc<TimePartitions>;

impl TimePartitions {
    /// Returns a new empty partition list with optional duration.
    pub fn new(
        metadata: RegionMetadataRef,
        builder: MemtableBuilderRef,
        next_memtable_id: MemtableId,
        part_duration: Option<Duration>,
    ) -> Self {
        Self {
            inner: Mutex::new(PartitionsInner::new(next_memtable_id)),
            part_duration,
            metadata,
            builder,
        }
    }

    /// Write key values to memtables.
    ///
    /// It creates new partitions if necessary.
    pub fn write(&self, kvs: &KeyValues) -> Result<()> {
        unimplemented!()
    }

    /// Append memtables in partitions to `memtables`.
    pub fn list_memtables(&self, memtables: &mut Vec<MemtableRef>) {
        unimplemented!()
    }

    /// Returns the number of partitions.
    pub fn num_partitions(&self) -> usize {
        unimplemented!()
    }

    /// Returns true if all memtables are empty.
    pub fn is_empty(&self) -> bool {
        unimplemented!()
    }

    /// Freezes all memtables.
    pub fn freeze(&self) -> Result<()> {
        unimplemented!()
    }

    /// Forks latest partition.
    pub fn fork(&self, metadata: &RegionMetadataRef) -> Self {
        unimplemented!()
    }

    /// Returns partition duration.
    pub(crate) fn part_duration(&self) -> Option<Duration> {
        unimplemented!()
    }

    /// Returns memory usage.
    pub(crate) fn memory_usage(&self) -> usize {
        unimplemented!()
    }

    /// Append memtables in partitions to small vec.
    pub(crate) fn list_memtables_to_small_vec(&self, memtables: &mut SmallMemtableVec) {
        unimplemented!()
    }

    /// Returns the next memtable id.
    pub(crate) fn next_memtable_id(&self) -> MemtableId {
        unimplemented!()
    }
}

#[derive(Debug)]
struct PartitionsInner {
    /// All partitions.
    parts: PartitionVec,
    /// Next memtable id.
    next_memtable_id: MemtableId,
}

impl PartitionsInner {
    fn new(next_memtable_id: MemtableId) -> Self {
        unimplemented!()
    }
}

/// Time range of a partition.
#[derive(Debug, Clone)]
struct PartTimeRange {
    /// Inclusive min timestamp of rows in the partition.
    min_timestamp: Timestamp,
    /// Exclusive max timestamp of rows in the partition.
    max_timestamp: Timestamp,
}
