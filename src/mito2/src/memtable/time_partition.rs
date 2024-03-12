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
use smallvec::{smallvec, SmallVec};
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
        let inner = self.inner.lock().unwrap();
        memtables.extend(inner.parts.iter().map(|part| part.memtable.clone()));
    }

    /// Returns the number of partitions.
    pub fn num_partitions(&self) -> usize {
        let inner = self.inner.lock().unwrap();
        inner.parts.len()
    }

    /// Returns true if all memtables are empty.
    pub fn is_empty(&self) -> bool {
        let inner = self.inner.lock().unwrap();
        inner.parts.is_empty()
    }

    /// Freezes all memtables.
    pub fn freeze(&self) -> Result<()> {
        let inner = self.inner.lock().unwrap();
        for part in &*inner.parts {
            part.memtable.freeze()?;
        }
        Ok(())
    }

    /// Forks latest partition.
    pub fn fork(&self, metadata: &RegionMetadataRef) -> Self {
        let mut inner = self.inner.lock().unwrap();
        let latest_part = inner
            .parts
            .iter()
            .max_by_key(|part| part.time_range.map(|range| range.min_timestamp))
            .cloned();

        let Some(old_part) = latest_part else {
            return Self::new(
                metadata.clone(),
                self.builder.clone(),
                inner.next_memtable_id,
                self.part_duration,
            );
        };
        let memtable = old_part.memtable.fork(inner.alloc_memtable_id(), metadata);
        let new_part = TimePartition {
            memtable,
            time_range: old_part.time_range,
        };
        Self {
            inner: Mutex::new(PartitionsInner::with_partition(
                new_part,
                inner.next_memtable_id,
            )),
            part_duration: self.part_duration,
            metadata: metadata.clone(),
            builder: self.builder.clone(),
        }
    }

    /// Returns partition duration.
    pub(crate) fn part_duration(&self) -> Option<Duration> {
        self.part_duration
    }

    /// Returns memory usage.
    pub(crate) fn memory_usage(&self) -> usize {
        let inner = self.inner.lock().unwrap();
        inner
            .parts
            .iter()
            .map(|part| part.memtable.stats().estimated_bytes)
            .sum()
    }

    /// Append memtables in partitions to small vec.
    pub(crate) fn list_memtables_to_small_vec(&self, memtables: &mut SmallMemtableVec) {
        let inner = self.inner.lock().unwrap();
        memtables.extend(inner.parts.iter().map(|part| part.memtable.clone()));
    }

    /// Returns the next memtable id.
    pub(crate) fn next_memtable_id(&self) -> MemtableId {
        let inner = self.inner.lock().unwrap();
        inner.next_memtable_id
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
        Self {
            parts: Default::default(),
            next_memtable_id,
        }
    }

    fn with_partition(part: TimePartition, next_memtable_id: MemtableId) -> Self {
        Self {
            parts: smallvec![part],
            next_memtable_id,
        }
    }

    fn alloc_memtable_id(&mut self) -> MemtableId {
        let id = self.next_memtable_id;
        self.next_memtable_id += 1;
        id
    }
}

/// Time range of a partition.
#[derive(Debug, Clone, Copy)]
struct PartTimeRange {
    /// Inclusive min timestamp of rows in the partition.
    min_timestamp: Timestamp,
    /// Exclusive max timestamp of rows in the partition.
    max_timestamp: Timestamp,
}
