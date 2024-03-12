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

use std::{time::Duration, sync::Mutex};

use common_time::Timestamp;
use smallvec::SmallVec;

use crate::memtable::{MemtableRef, KeyValues, MemtableId};
use crate::error::Result;

/// A partition holds rows with timestamps between `[min, max)`.
#[derive(Debug, Clone)]
pub struct TimePartition {
    /// Memtable of the partition.
    memtable: MemtableRef,
    /// Time range of the partition. `None` means there is no time range.
    time_range: Option<PartTimeRange>
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
}

impl TimePartitions {
    fn write(&self, kvs: &KeyValues) -> Result<()> {
        unimplemented!()
    }

    /// Append memtables in partitions to `memtables`.
    fn list_memtables(&self, memtables: &mut Vec<MemtableRef>) {
        unimplemented!()
    }

    /// Returns true if all memtables are empty.
    fn is_empty(&self) -> bool {
        unimplemented!()
    }

    /// Freezes all memtables.
    fn freeze(&self) -> Result<()> {
        unimplemented!()
    }

    /// Forks latest partition.
    fn fork(&self) -> Self {
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

/// Time range of a partition.
#[derive(Debug, Clone)]
struct PartTimeRange {
    /// Inclusive min timestamp of rows in the partition.
    min_timestamp: Timestamp,
    /// Exclusive max timestamp of rows in the partition.
    max_timestamp: Timestamp,
}
