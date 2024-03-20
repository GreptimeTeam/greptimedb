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

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common_telemetry::debug;
use common_time::timestamp::TimeUnit;
use common_time::timestamp_millis::BucketAligned;
use common_time::Timestamp;
use smallvec::{smallvec, SmallVec};
use snafu::OptionExt;
use store_api::metadata::RegionMetadataRef;

use crate::error::{InvalidRequestSnafu, Result};
use crate::memtable::key_values::KeyValue;
use crate::memtable::version::SmallMemtableVec;
use crate::memtable::{KeyValues, MemtableBuilderRef, MemtableId, MemtableRef};

/// A partition holds rows with timestamps between `[min, max)`.
#[derive(Debug, Clone)]
pub struct TimePartition {
    /// Memtable of the partition.
    memtable: MemtableRef,
    /// Time range of the partition. `None` means there is no time range. The time
    /// range is `None` if and only if the [TimePartitions::part_duration] is `None`.
    time_range: Option<PartTimeRange>,
}

impl TimePartition {
    /// Returns whether the `ts` belongs to the partition.
    fn contains_timestamp(&self, ts: Timestamp) -> bool {
        let Some(range) = self.time_range else {
            return true;
        };

        range.contains_timestamp(ts)
    }

    /// Write rows to the part.
    fn write(&self, kvs: &KeyValues) -> Result<()> {
        self.memtable.write(kvs)
    }
}

type PartitionVec = SmallVec<[TimePartition; 2]>;

/// Partitions.
#[derive(Debug)]
pub struct TimePartitions {
    /// Mutable data of partitions.
    inner: Mutex<PartitionsInner>,
    /// Duration of a partition.
    ///
    /// `None` means there is only one partition and the [TimePartition::time_range] is
    /// also `None`.
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
        let mut inner = PartitionsInner::new(next_memtable_id);
        if part_duration.is_none() {
            // If `part_duration` is None, then we create a partition with `None` time
            // range so we will write all rows to that partition.
            let memtable = builder.build(inner.alloc_memtable_id(), &metadata);
            debug!(
                "Creates a time partition for all timestamps, region: {}, memtable_id: {}",
                metadata.region_id,
                memtable.id(),
            );
            let part = TimePartition {
                memtable,
                time_range: None,
            };
            inner.parts.push(part);
        }

        Self {
            inner: Mutex::new(inner),
            part_duration,
            metadata,
            builder,
        }
    }

    /// Write key values to memtables.
    ///
    /// It creates new partitions if necessary.
    pub fn write(&self, kvs: &KeyValues) -> Result<()> {
        // Get all parts.
        let parts = self.list_partitions();

        // Checks whether all rows belongs to a single part. Checks in reverse order as we usually
        // put to latest part.
        for part in parts.iter().rev() {
            let mut all_in_partition = true;
            for kv in kvs.iter() {
                // Safety: We checked the schema in the write request.
                let ts = kv.timestamp().as_timestamp().unwrap().unwrap();
                if !part.contains_timestamp(ts) {
                    all_in_partition = false;
                    break;
                }
            }
            if !all_in_partition {
                continue;
            }

            // We can write all rows to this part.
            return part.write(kvs);
        }

        // Slow path: We have to split kvs by partitions.
        self.write_multi_parts(kvs, &parts)
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
        inner.parts.iter().all(|part| part.memtable.is_empty())
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

    /// Returns all partitions.
    fn list_partitions(&self) -> PartitionVec {
        let inner = self.inner.lock().unwrap();
        inner.parts.clone()
    }

    /// Write to multiple partitions.
    fn write_multi_parts(&self, kvs: &KeyValues, parts: &PartitionVec) -> Result<()> {
        // If part duration is `None` then there is always one partition and all rows
        // will be put in that partition before invoking this method.
        debug_assert!(self.part_duration.is_some());

        let mut parts_to_write = HashMap::new();
        let mut missing_parts = HashMap::new();
        for kv in kvs.iter() {
            let mut part_found = false;
            // Safety: We used the timestamp before.
            let ts = kv.timestamp().as_timestamp().unwrap().unwrap();
            for part in parts {
                if part.contains_timestamp(ts) {
                    // Safety: Since part duration is `Some` so all time range should be `Some`.
                    parts_to_write
                        .entry(part.time_range.unwrap().min_timestamp)
                        .or_insert_with(|| PartitionToWrite {
                            partition: part.clone(),
                            key_values: Vec::new(),
                        })
                        .key_values
                        .push(kv);
                    part_found = true;
                    break;
                }
            }

            if !part_found {
                // We need to write it to a new part.
                // Safety: `new()` ensures duration is always Some if we do to this method.
                let part_duration = self.part_duration.unwrap();
                let part_start =
                    partition_start_timestamp(ts, part_duration).with_context(|| {
                        InvalidRequestSnafu {
                            region_id: self.metadata.region_id,
                            reason: format!(
                                "timestamp {ts:?} and bucket {part_duration:?} are out of range"
                            ),
                        }
                    })?;
                missing_parts
                    .entry(part_start)
                    .or_insert_with(Vec::new)
                    .push(kv);
            }
        }

        // Writes rows to existing parts.
        for part_to_write in parts_to_write.into_values() {
            for kv in part_to_write.key_values {
                part_to_write.partition.memtable.write_one(kv)?;
            }
        }

        let part_duration = self.part_duration.unwrap();
        // Creates new parts and writes to them. Acquires the lock to avoid others create
        // the same partition.
        let mut inner = self.inner.lock().unwrap();
        for (part_start, key_values) in missing_parts {
            let part_pos = match inner
                .parts
                .iter()
                .position(|part| part.time_range.unwrap().min_timestamp == part_start)
            {
                Some(pos) => pos,
                None => {
                    let range = PartTimeRange::from_start_duration(part_start, part_duration)
                        .with_context(|| InvalidRequestSnafu {
                            region_id: self.metadata.region_id,
                            reason: format!(
                                "Partition time range for {part_start:?} is out of bound, bucket size: {part_duration:?}",
                            ),
                        })?;
                    let memtable = self
                        .builder
                        .build(inner.alloc_memtable_id(), &self.metadata);
                    debug!(
                        "Create time partition {:?} for region {}, duration: {:?}, memtable_id: {}, parts_total: {}",
                        range,
                        self.metadata.region_id,
                        part_duration,
                        memtable.id(),
                        inner.parts.len() + 1
                    );
                    let pos = inner.parts.len();
                    inner.parts.push(TimePartition {
                        memtable,
                        time_range: Some(range),
                    });
                    pos
                }
            };

            let memtable = &inner.parts[part_pos].memtable;
            for kv in key_values {
                memtable.write_one(kv)?;
            }
        }

        Ok(())
    }
}

/// Computes the start timestamp of the partition for `ts`.
///
/// It always use bucket size in seconds which should fit all timestamp resolution.
fn partition_start_timestamp(ts: Timestamp, bucket: Duration) -> Option<Timestamp> {
    // Safety: We convert it to seconds so it never returns `None`.
    let ts_sec = ts.convert_to(TimeUnit::Second).unwrap();
    let bucket_sec: i64 = bucket.as_secs().try_into().ok()?;
    let start_sec = ts_sec.align_by_bucket(bucket_sec)?;
    start_sec.convert_to(ts.unit())
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

impl PartTimeRange {
    fn from_start_duration(start: Timestamp, duration: Duration) -> Option<Self> {
        let start_sec = start.convert_to(TimeUnit::Second)?;
        let end_sec = start_sec.add_duration(duration).ok()?;
        let min_timestamp = start_sec.convert_to(start.unit())?;
        let max_timestamp = end_sec.convert_to(start.unit())?;

        Some(Self {
            min_timestamp,
            max_timestamp,
        })
    }

    /// Returns whether the `ts` belongs to the partition.
    fn contains_timestamp(&self, ts: Timestamp) -> bool {
        self.min_timestamp <= ts && ts < self.max_timestamp
    }
}

struct PartitionToWrite<'a> {
    partition: TimePartition,
    key_values: Vec<KeyValue<'a>>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memtable::partition_tree::PartitionTreeMemtableBuilder;
    use crate::test_util::memtable_util::{self, collect_iter_timestamps};

    #[test]
    fn test_no_duration() {
        let metadata = memtable_util::metadata_for_test();
        let builder = Arc::new(PartitionTreeMemtableBuilder::default());
        let partitions = TimePartitions::new(metadata.clone(), builder, 0, None);
        assert_eq!(1, partitions.num_partitions());
        assert!(partitions.is_empty());

        let kvs = memtable_util::build_key_values(
            &metadata,
            "hello".to_string(),
            0,
            &[1000, 3000, 7000, 5000, 6000],
            0, // sequence 0, 1, 2, 3, 4
        );
        partitions.write(&kvs).unwrap();

        assert_eq!(1, partitions.num_partitions());
        assert!(!partitions.is_empty());
        assert!(!partitions.is_empty());
        let mut memtables = Vec::new();
        partitions.list_memtables(&mut memtables);

        let iter = memtables[0].iter(None, None).unwrap();
        let timestamps = collect_iter_timestamps(iter);
        assert_eq!(&[1000, 3000, 5000, 6000, 7000], &timestamps[..]);
    }

    #[test]
    fn test_write_single_part() {
        let metadata = memtable_util::metadata_for_test();
        let builder = Arc::new(PartitionTreeMemtableBuilder::default());
        let partitions =
            TimePartitions::new(metadata.clone(), builder, 0, Some(Duration::from_secs(10)));
        assert_eq!(0, partitions.num_partitions());

        let kvs = memtable_util::build_key_values(
            &metadata,
            "hello".to_string(),
            0,
            &[5000, 2000, 0],
            0, // sequence 0, 1, 2
        );
        // It should creates a new partition.
        partitions.write(&kvs).unwrap();
        assert_eq!(1, partitions.num_partitions());
        assert!(!partitions.is_empty());

        let kvs = memtable_util::build_key_values(
            &metadata,
            "hello".to_string(),
            0,
            &[3000, 7000, 4000],
            3, // sequence 3, 4, 5
        );
        // Still writes to the same partition.
        partitions.write(&kvs).unwrap();
        assert_eq!(1, partitions.num_partitions());

        let mut memtables = Vec::new();
        partitions.list_memtables(&mut memtables);
        let iter = memtables[0].iter(None, None).unwrap();
        let timestamps = collect_iter_timestamps(iter);
        assert_eq!(&[0, 2000, 3000, 4000, 5000, 7000], &timestamps[..]);
        let parts = partitions.list_partitions();
        assert_eq!(
            Timestamp::new_millisecond(0),
            parts[0].time_range.unwrap().min_timestamp
        );
        assert_eq!(
            Timestamp::new_millisecond(10000),
            parts[0].time_range.unwrap().max_timestamp
        );
    }

    #[test]
    fn test_write_multi_parts() {
        let metadata = memtable_util::metadata_for_test();
        let builder = Arc::new(PartitionTreeMemtableBuilder::default());
        let partitions =
            TimePartitions::new(metadata.clone(), builder, 0, Some(Duration::from_secs(5)));
        assert_eq!(0, partitions.num_partitions());

        let kvs = memtable_util::build_key_values(
            &metadata,
            "hello".to_string(),
            0,
            &[2000, 0],
            0, // sequence 0, 1
        );
        // It should creates a new partition.
        partitions.write(&kvs).unwrap();
        assert_eq!(1, partitions.num_partitions());
        assert!(!partitions.is_empty());

        let kvs = memtable_util::build_key_values(
            &metadata,
            "hello".to_string(),
            0,
            &[3000, 7000, 4000, 5000],
            2, // sequence 2, 3, 4, 5
        );
        // Writes 2 rows to the old partition and 1 row to a new partition.
        partitions.write(&kvs).unwrap();
        assert_eq!(2, partitions.num_partitions());

        let parts = partitions.list_partitions();
        let iter = parts[0].memtable.iter(None, None).unwrap();
        let timestamps = collect_iter_timestamps(iter);
        assert_eq!(
            Timestamp::new_millisecond(0),
            parts[0].time_range.unwrap().min_timestamp
        );
        assert_eq!(
            Timestamp::new_millisecond(5000),
            parts[0].time_range.unwrap().max_timestamp
        );
        assert_eq!(&[0, 2000, 3000, 4000], &timestamps[..]);
        let iter = parts[1].memtable.iter(None, None).unwrap();
        let timestamps = collect_iter_timestamps(iter);
        assert_eq!(&[5000, 7000], &timestamps[..]);
        assert_eq!(
            Timestamp::new_millisecond(5000),
            parts[1].time_range.unwrap().min_timestamp
        );
        assert_eq!(
            Timestamp::new_millisecond(10000),
            parts[1].time_range.unwrap().max_timestamp
        );
    }
}
