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

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use common_telemetry::debug;
use common_time::timestamp::TimeUnit;
use common_time::timestamp_millis::BucketAligned;
use common_time::Timestamp;
use datatypes::arrow;
use datatypes::arrow::array::{
    ArrayRef, BooleanArray, RecordBatch, RecordBatchOptions, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
};
use datatypes::arrow::buffer::{BooleanBuffer, MutableBuffer};
use datatypes::arrow::datatypes::{DataType, Int64Type};
use smallvec::{smallvec, SmallVec};
use snafu::{OptionExt, ResultExt};
use store_api::metadata::RegionMetadataRef;

use crate::error;
use crate::error::{InvalidRequestSnafu, Result};
use crate::memtable::bulk::part::BulkPart;
use crate::memtable::key_values::KeyValue;
use crate::memtable::version::SmallMemtableVec;
use crate::memtable::{KeyValues, MemtableBuilderRef, MemtableId, MemtableRef};

/// A partition holds rows with timestamps between `[min, max)`.
#[derive(Debug, Clone)]
pub struct TimePartition {
    /// Memtable of the partition.
    memtable: MemtableRef,
    /// Time range of the partition. `min` is inclusive and `max` is exclusive.
    /// `None` means there is no time range. The time
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

    /// Writes a record batch to memtable.
    fn write_record_batch(&self, rb: BulkPart) -> Result<()> {
        self.memtable.write_bulk(rb)
    }

    /// Write a partial [BulkPart] according to [TimePartition::time_range].
    fn write_record_batch_partial(&self, part: &BulkPart) -> error::Result<()> {
        let Some(range) = self.time_range else {
            unreachable!("TimePartition must have explicit time range when a bulk request involves multiple time partition")
        };
        let Some(filtered) = filter_record_batch(
            part,
            range.min_timestamp.value(),
            range.max_timestamp.value(),
        )?
        else {
            return Ok(());
        };
        self.write_record_batch(filtered)
    }
}

macro_rules! create_filter_buffer {
    ($ts_array:expr, $min:expr, $max:expr) => {{
        let len = $ts_array.len();
        let mut buffer = MutableBuffer::new(len.div_ceil(64) * 8);

        let f = |idx: usize| -> bool {
            // SAFETY: we only iterate the array within index bound.
            unsafe {
                let val = $ts_array.value_unchecked(idx);
                val >= $min && val < $max
            }
        };

        let chunks = len / 64;
        let remainder = len % 64;

        for chunk in 0..chunks {
            let mut packed = 0;
            for bit_idx in 0..64 {
                let i = bit_idx + chunk * 64;
                packed |= (f(i) as u64) << bit_idx;
            }
            // SAFETY: Already allocated sufficient capacity
            unsafe { buffer.push_unchecked(packed) }
        }

        if remainder != 0 {
            let mut packed = 0;
            for bit_idx in 0..remainder {
                let i = bit_idx + chunks * 64;
                packed |= (f(i) as u64) << bit_idx;
            }
            // SAFETY: Already allocated sufficient capacity
            unsafe { buffer.push_unchecked(packed) }
        }

        BooleanArray::new(BooleanBuffer::new(buffer.into(), 0, len), None)
    }};
}

macro_rules! handle_timestamp_array {
    ($ts_array:expr, $array_type:ty, $min:expr, $max:expr) => {{
        let ts_array = $ts_array.as_any().downcast_ref::<$array_type>().unwrap();
        let filter = create_filter_buffer!(ts_array, $min, $max);

        let res = arrow::compute::filter(ts_array, &filter).context(error::ComputeArrowSnafu)?;
        if res.is_empty() {
            return Ok(None);
        }

        let i64array = res.as_any().downcast_ref::<$array_type>().unwrap();
        // safety: we've checked res is not empty
        let max_ts = arrow::compute::max(i64array).unwrap();
        let min_ts = arrow::compute::min(i64array).unwrap();

        (res, filter, min_ts, max_ts)
    }};
}

/// Filters the given part according to min (inclusive) and max (exclusive) timestamp range.
/// Returns [None] if no matching rows.
pub fn filter_record_batch(part: &BulkPart, min: i64, max: i64) -> Result<Option<BulkPart>> {
    let ts_array = part.timestamps();
    let (ts_array, filter, min_ts, max_ts) = match ts_array.data_type() {
        DataType::Timestamp(unit, _) => match unit {
            arrow::datatypes::TimeUnit::Second => {
                handle_timestamp_array!(ts_array, TimestampSecondArray, min, max)
            }
            arrow::datatypes::TimeUnit::Millisecond => {
                handle_timestamp_array!(ts_array, TimestampMillisecondArray, min, max)
            }
            arrow::datatypes::TimeUnit::Microsecond => {
                handle_timestamp_array!(ts_array, TimestampMicrosecondArray, min, max)
            }
            arrow::datatypes::TimeUnit::Nanosecond => {
                handle_timestamp_array!(ts_array, TimestampNanosecondArray, min, max)
            }
        },
        _ => {
            unreachable!("Got data type: {:?}", ts_array.data_type());
        }
    };

    let num_rows = ts_array.len();
    let arrays = part
        .batch
        .columns()
        .iter()
        .enumerate()
        .map(|(index, array)| {
            if index == part.timestamp_index {
                Ok(ts_array.clone())
            } else {
                arrow::compute::filter(&array, &filter).context(error::ComputeArrowSnafu)
            }
        })
        .collect::<Result<Vec<_>>>()?;
    let batch = RecordBatch::try_new_with_options(
        part.batch.schema(),
        arrays,
        &RecordBatchOptions::default().with_row_count(Some(num_rows)),
    )
    .context(error::NewRecordBatchSnafu)?;
    Ok(Some(BulkPart {
        batch,
        num_rows,
        max_ts,
        min_ts,
        sequence: part.sequence,
        timestamp_index: part.timestamp_index,
    }))
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

    pub fn write_bulk(&self, part: BulkPart) -> Result<()> {
        let time_type = self
            .metadata
            .time_index_column()
            .column_schema
            .data_type
            .as_timestamp()
            .unwrap();

        // Get all parts.
        let parts = self.list_partitions();
        let (matching_parts, missing_parts) = self.find_partitions_by_time_range(
            part.timestamps(),
            &parts,
            time_type.create_timestamp(part.min_ts),
            time_type.create_timestamp(part.max_ts),
        )?;

        if matching_parts.len() == 1 && missing_parts.is_empty() {
            // fast path: all timestamps fall in one time partition.
            return matching_parts[0].write_record_batch(part);
        }

        for matching in matching_parts {
            matching.write_record_batch_partial(&part)?
        }

        for missing in missing_parts {
            let new_part = {
                let mut inner = self.inner.lock().unwrap();
                self.get_or_create_time_partition(missing, &mut inner)?
            };
            new_part.write_record_batch_partial(&part)?;
        }
        Ok(())
    }

    // Creates or gets parts with given start timestamp.
    // Acquires the lock to avoid others create the same partition.
    fn get_or_create_time_partition(
        &self,
        part_start: Timestamp,
        inner: &mut MutexGuard<PartitionsInner>,
    ) -> Result<TimePartition> {
        let part_duration = self.part_duration.unwrap();
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
        Ok(inner.parts[part_pos].clone())
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

    /// Forks latest partition and updates the partition duration if `part_duration` is Some.
    pub fn fork(&self, metadata: &RegionMetadataRef, part_duration: Option<Duration>) -> Self {
        // Fall back to the existing partition duration.
        let part_duration = part_duration.or(self.part_duration);

        let mut inner = self.inner.lock().unwrap();
        let latest_part = inner
            .parts
            .iter()
            .max_by_key(|part| part.time_range.map(|range| range.min_timestamp))
            .cloned();

        let Some(old_part) = latest_part else {
            // If there is no partition, then we create a new partition with the new duration.
            return Self::new(
                metadata.clone(),
                self.builder.clone(),
                inner.next_memtable_id,
                part_duration,
            );
        };

        let old_stats = old_part.memtable.stats();
        // Use the max timestamp to compute the new time range for the memtable.
        // If `part_duration` is None, the new range will be None.
        let new_time_range =
            old_stats
                .time_range()
                .zip(part_duration)
                .and_then(|(range, bucket)| {
                    partition_start_timestamp(range.1, bucket)
                        .and_then(|start| PartTimeRange::from_start_duration(start, bucket))
                });
        // Forks the latest partition, but compute the time range based on the new duration.
        let memtable = old_part.memtable.fork(inner.alloc_memtable_id(), metadata);
        let new_part = TimePartition {
            memtable,
            time_range: new_time_range,
        };

        Self {
            inner: Mutex::new(PartitionsInner::with_partition(
                new_part,
                inner.next_memtable_id,
            )),
            part_duration,
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

    /// Returns the number of rows.
    pub(crate) fn num_rows(&self) -> u64 {
        let inner = self.inner.lock().unwrap();
        inner
            .parts
            .iter()
            .map(|part| part.memtable.stats().num_rows as u64)
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

    /// Creates a new empty partition list from this list and a `part_duration`.
    /// It falls back to the old partition duration if `part_duration` is `None`.
    pub(crate) fn new_with_part_duration(&self, part_duration: Option<Duration>) -> Self {
        debug_assert!(self.is_empty());

        Self::new(
            self.metadata.clone(),
            self.builder.clone(),
            self.next_memtable_id(),
            part_duration.or(self.part_duration),
        )
    }

    /// Returns all partitions.
    fn list_partitions(&self) -> PartitionVec {
        let inner = self.inner.lock().unwrap();
        inner.parts.clone()
    }

    /// Find existing partitions that match the bulk data's time range and identify
    /// any new partitions that need to be created
    fn find_partitions_by_time_range<'a>(
        &self,
        ts_array: &ArrayRef,
        existing_parts: &'a [TimePartition],
        min: Timestamp,
        max: Timestamp,
    ) -> Result<(Vec<&'a TimePartition>, Vec<Timestamp>)> {
        let mut matching = Vec::new();

        let mut present = HashSet::new();
        // First find any existing partitions that overlap
        for part in existing_parts {
            let Some(part_time_range) = part.time_range.as_ref() else {
                matching.push(part);
                return Ok((matching, Vec::new()));
            };

            if !(max < part_time_range.min_timestamp || min >= part_time_range.max_timestamp) {
                matching.push(part);
                present.insert(part_time_range.min_timestamp.value());
            }
        }

        // safety: self.part_duration can only be present when reach here.
        let part_duration = self.part_duration.unwrap();
        let timestamp_unit = self.metadata.time_index_type().unit();

        let part_duration_sec = part_duration.as_secs() as i64;
        // SAFETY: Timestamps won't overflow when converting to Second.
        let start_bucket = min
            .convert_to(TimeUnit::Second)
            .unwrap()
            .value()
            .div_euclid(part_duration_sec);
        let end_bucket = max
            .convert_to(TimeUnit::Second)
            .unwrap()
            .value()
            .div_euclid(part_duration_sec);
        let bucket_num = (end_bucket - start_bucket + 1) as usize;

        let num_timestamps = ts_array.len();
        let missing = if bucket_num <= num_timestamps {
            (start_bucket..=end_bucket)
                .filter_map(|start_sec| {
                    let Some(timestamp) = Timestamp::new_second(start_sec * part_duration_sec)
                        .convert_to(timestamp_unit)
                    else {
                        return Some(
                            InvalidRequestSnafu {
                                region_id: self.metadata.region_id,
                                reason: format!("Timestamp out of range: {}", start_sec),
                            }
                            .fail(),
                        );
                    };
                    if present.insert(timestamp.value()) {
                        Some(Ok(timestamp))
                    } else {
                        None
                    }
                })
                .collect::<Result<Vec<_>>>()?
        } else {
            let ts_primitive = match ts_array.data_type() {
                DataType::Timestamp(unit, _) => match unit {
                    arrow::datatypes::TimeUnit::Second => ts_array
                        .as_any()
                        .downcast_ref::<TimestampSecondArray>()
                        .unwrap()
                        .reinterpret_cast::<Int64Type>(),
                    arrow::datatypes::TimeUnit::Millisecond => ts_array
                        .as_any()
                        .downcast_ref::<TimestampMillisecondArray>()
                        .unwrap()
                        .reinterpret_cast::<Int64Type>(),
                    arrow::datatypes::TimeUnit::Microsecond => ts_array
                        .as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .unwrap()
                        .reinterpret_cast::<Int64Type>(),
                    arrow::datatypes::TimeUnit::Nanosecond => ts_array
                        .as_any()
                        .downcast_ref::<TimestampNanosecondArray>()
                        .unwrap()
                        .reinterpret_cast::<Int64Type>(),
                },
                _ => unreachable!(),
            };

            ts_primitive
                .values()
                .iter()
                .filter_map(|ts| {
                    let ts = self.metadata.time_index_type().create_timestamp(*ts);
                    let Some(bucket_start) = ts
                        .convert_to(TimeUnit::Second)
                        .and_then(|ts| ts.align_by_bucket(part_duration_sec))
                        .and_then(|ts| ts.convert_to(timestamp_unit))
                    else {
                        return Some(
                            InvalidRequestSnafu {
                                region_id: self.metadata.region_id,
                                reason: format!("Timestamp out of range: {:?}", ts),
                            }
                            .fail(),
                        );
                    };
                    if present.insert(bucket_start.value()) {
                        Some(Ok(bucket_start))
                    } else {
                        None
                    }
                })
                .collect::<Result<Vec<_>>>()?
        };
        Ok((matching, missing))
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

        // Creates new parts and writes to them. Acquires the lock to avoid others create
        // the same partition.
        let mut inner = self.inner.lock().unwrap();
        for (part_start, key_values) in missing_parts {
            let partition = self.get_or_create_time_partition(part_start, &mut inner)?;
            for kv in key_values {
                partition.memtable.write_one(kv)?;
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
    use std::sync::Arc;

    use api::v1::SemanticType;
    use datatypes::arrow::array::{ArrayRef, StringArray, TimestampMillisecondArray};
    use datatypes::arrow::datatypes::{DataType, Field, Schema};
    use datatypes::arrow::record_batch::RecordBatch;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::ColumnSchema;
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};
    use store_api::storage::SequenceNumber;

    use super::*;
    use crate::memtable::partition_tree::PartitionTreeMemtableBuilder;
    use crate::memtable::time_series::TimeSeriesMemtableBuilder;
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
        let mut memtables = Vec::new();
        partitions.list_memtables(&mut memtables);
        assert_eq!(0, memtables[0].id());

        let iter = memtables[0].iter(None, None, None).unwrap();
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
        let iter = memtables[0].iter(None, None, None).unwrap();
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

    fn new_multi_partitions(metadata: &RegionMetadataRef) -> TimePartitions {
        let builder = Arc::new(PartitionTreeMemtableBuilder::default());
        let partitions =
            TimePartitions::new(metadata.clone(), builder, 0, Some(Duration::from_secs(5)));
        assert_eq!(0, partitions.num_partitions());

        let kvs = memtable_util::build_key_values(
            metadata,
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
            metadata,
            "hello".to_string(),
            0,
            &[3000, 7000, 4000, 5000],
            2, // sequence 2, 3, 4, 5
        );
        // Writes 2 rows to the old partition and 1 row to a new partition.
        partitions.write(&kvs).unwrap();
        assert_eq!(2, partitions.num_partitions());

        partitions
    }

    #[test]
    fn test_write_multi_parts() {
        let metadata = memtable_util::metadata_for_test();
        let partitions = new_multi_partitions(&metadata);

        let parts = partitions.list_partitions();
        let iter = parts[0].memtable.iter(None, None, None).unwrap();
        let timestamps = collect_iter_timestamps(iter);
        assert_eq!(0, parts[0].memtable.id());
        assert_eq!(
            Timestamp::new_millisecond(0),
            parts[0].time_range.unwrap().min_timestamp
        );
        assert_eq!(
            Timestamp::new_millisecond(5000),
            parts[0].time_range.unwrap().max_timestamp
        );
        assert_eq!(&[0, 2000, 3000, 4000], &timestamps[..]);
        let iter = parts[1].memtable.iter(None, None, None).unwrap();
        assert_eq!(1, parts[1].memtable.id());
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

    #[test]
    fn test_new_with_part_duration() {
        let metadata = memtable_util::metadata_for_test();
        let builder = Arc::new(PartitionTreeMemtableBuilder::default());
        let partitions = TimePartitions::new(metadata.clone(), builder.clone(), 0, None);

        let new_parts = partitions.new_with_part_duration(Some(Duration::from_secs(5)));
        assert_eq!(Duration::from_secs(5), new_parts.part_duration().unwrap());
        assert_eq!(1, new_parts.next_memtable_id());

        // Won't update the duration if it's None.
        let new_parts = new_parts.new_with_part_duration(None);
        assert_eq!(Duration::from_secs(5), new_parts.part_duration().unwrap());
        // Don't need to create new memtables.
        assert_eq!(1, new_parts.next_memtable_id());

        let new_parts = new_parts.new_with_part_duration(Some(Duration::from_secs(10)));
        assert_eq!(Duration::from_secs(10), new_parts.part_duration().unwrap());
        // Don't need to create new memtables.
        assert_eq!(1, new_parts.next_memtable_id());

        let builder = Arc::new(PartitionTreeMemtableBuilder::default());
        let partitions = TimePartitions::new(metadata.clone(), builder.clone(), 0, None);
        // Need to build a new memtable as duration is still None.
        let new_parts = partitions.new_with_part_duration(None);
        assert!(new_parts.part_duration().is_none());
        assert_eq!(2, new_parts.next_memtable_id());
    }

    #[test]
    fn test_fork_empty() {
        let metadata = memtable_util::metadata_for_test();
        let builder = Arc::new(PartitionTreeMemtableBuilder::default());
        let partitions = TimePartitions::new(metadata.clone(), builder, 0, None);
        partitions.freeze().unwrap();
        let new_parts = partitions.fork(&metadata, None);
        assert!(new_parts.part_duration().is_none());
        assert_eq!(1, new_parts.list_partitions()[0].memtable.id());
        assert_eq!(2, new_parts.next_memtable_id());

        new_parts.freeze().unwrap();
        let new_parts = new_parts.fork(&metadata, Some(Duration::from_secs(5)));
        assert_eq!(Duration::from_secs(5), new_parts.part_duration().unwrap());
        assert_eq!(2, new_parts.list_partitions()[0].memtable.id());
        assert_eq!(3, new_parts.next_memtable_id());

        new_parts.freeze().unwrap();
        let new_parts = new_parts.fork(&metadata, None);
        // Won't update the duration.
        assert_eq!(Duration::from_secs(5), new_parts.part_duration().unwrap());
        assert_eq!(3, new_parts.list_partitions()[0].memtable.id());
        assert_eq!(4, new_parts.next_memtable_id());

        new_parts.freeze().unwrap();
        let new_parts = new_parts.fork(&metadata, Some(Duration::from_secs(10)));
        assert_eq!(Duration::from_secs(10), new_parts.part_duration().unwrap());
        assert_eq!(4, new_parts.list_partitions()[0].memtable.id());
        assert_eq!(5, new_parts.next_memtable_id());
    }

    #[test]
    fn test_fork_non_empty_none() {
        let metadata = memtable_util::metadata_for_test();
        let partitions = new_multi_partitions(&metadata);
        partitions.freeze().unwrap();

        // Won't update the duration.
        let new_parts = partitions.fork(&metadata, None);
        assert!(new_parts.is_empty());
        assert_eq!(Duration::from_secs(5), new_parts.part_duration().unwrap());
        assert_eq!(2, new_parts.list_partitions()[0].memtable.id());
        assert_eq!(3, new_parts.next_memtable_id());

        // Although we don't fork a memtable multiple times, we still add a test for it.
        let new_parts = partitions.fork(&metadata, Some(Duration::from_secs(10)));
        assert!(new_parts.is_empty());
        assert_eq!(Duration::from_secs(10), new_parts.part_duration().unwrap());
        assert_eq!(3, new_parts.list_partitions()[0].memtable.id());
        assert_eq!(4, new_parts.next_memtable_id());
    }

    #[test]
    fn test_find_partitions_by_time_range() {
        let metadata = memtable_util::metadata_for_test();
        let builder = Arc::new(PartitionTreeMemtableBuilder::default());

        // Case 1: No time range partitioning
        let partitions = TimePartitions::new(metadata.clone(), builder.clone(), 0, None);
        let parts = partitions.list_partitions();
        let (matching, missing) = partitions
            .find_partitions_by_time_range(
                &(Arc::new(TimestampMillisecondArray::from_iter_values(1000..=2000)) as ArrayRef),
                &parts,
                Timestamp::new_millisecond(1000),
                Timestamp::new_millisecond(2000),
            )
            .unwrap();
        assert_eq!(matching.len(), 1);
        assert!(missing.is_empty());
        assert!(matching[0].time_range.is_none());

        // Case 2: With time range partitioning
        let partitions = TimePartitions::new(
            metadata.clone(),
            builder.clone(),
            0,
            Some(Duration::from_secs(5)),
        );

        // Create two existing partitions: [0, 5000) and [5000, 10000)
        let kvs =
            memtable_util::build_key_values(&metadata, "hello".to_string(), 0, &[2000, 4000], 0);
        partitions.write(&kvs).unwrap();
        let kvs =
            memtable_util::build_key_values(&metadata, "hello".to_string(), 0, &[7000, 8000], 2);
        partitions.write(&kvs).unwrap();

        let parts = partitions.list_partitions();
        assert_eq!(2, parts.len());

        // Test case 2a: Query fully within existing partition
        let (matching, missing) = partitions
            .find_partitions_by_time_range(
                &(Arc::new(TimestampMillisecondArray::from_iter_values(2000..=4000)) as ArrayRef),
                &parts,
                Timestamp::new_millisecond(2000),
                Timestamp::new_millisecond(4000),
            )
            .unwrap();
        assert_eq!(matching.len(), 1);
        assert!(missing.is_empty());
        assert_eq!(matching[0].time_range.unwrap().min_timestamp.value(), 0);

        // Test case 2b: Query spanning multiple existing partitions
        let (matching, missing) = partitions
            .find_partitions_by_time_range(
                &(Arc::new(TimestampMillisecondArray::from_iter_values(3000..=8000)) as ArrayRef),
                &parts,
                Timestamp::new_millisecond(3000),
                Timestamp::new_millisecond(8000),
            )
            .unwrap();
        assert_eq!(matching.len(), 2);
        assert!(missing.is_empty());
        assert_eq!(matching[0].time_range.unwrap().min_timestamp.value(), 0);
        assert_eq!(matching[1].time_range.unwrap().min_timestamp.value(), 5000);

        // Test case 2c: Query requiring new partition
        let (matching, missing) = partitions
            .find_partitions_by_time_range(
                &(Arc::new(TimestampMillisecondArray::from_iter_values(12000..=13000)) as ArrayRef),
                &parts,
                Timestamp::new_millisecond(12000),
                Timestamp::new_millisecond(13000),
            )
            .unwrap();
        assert!(matching.is_empty());
        assert_eq!(missing.len(), 1);
        assert_eq!(missing[0].value(), 10000);

        // Test case 2d: Query partially overlapping existing partition
        let (matching, missing) = partitions
            .find_partitions_by_time_range(
                &(Arc::new(TimestampMillisecondArray::from_iter_values(4000..=6000)) as ArrayRef),
                &parts,
                Timestamp::new_millisecond(4000),
                Timestamp::new_millisecond(6000),
            )
            .unwrap();
        assert_eq!(matching.len(), 2);
        assert!(missing.is_empty());
        assert_eq!(matching[0].time_range.unwrap().min_timestamp.value(), 0);
        assert_eq!(matching[1].time_range.unwrap().min_timestamp.value(), 5000);

        // Test case 2e: Corner case
        let (matching, missing) = partitions
            .find_partitions_by_time_range(
                &(Arc::new(TimestampMillisecondArray::from_iter_values(4999..=5000)) as ArrayRef),
                &parts,
                Timestamp::new_millisecond(4999),
                Timestamp::new_millisecond(5000),
            )
            .unwrap();
        assert_eq!(matching.len(), 2);
        assert!(missing.is_empty());
        assert_eq!(matching[0].time_range.unwrap().min_timestamp.value(), 0);
        assert_eq!(matching[1].time_range.unwrap().min_timestamp.value(), 5000);

        // Test case 2f: Corner case with
        let (matching, missing) = partitions
            .find_partitions_by_time_range(
                &(Arc::new(TimestampMillisecondArray::from_iter_values(9999..=10000)) as ArrayRef),
                &parts,
                Timestamp::new_millisecond(9999),
                Timestamp::new_millisecond(10000),
            )
            .unwrap();
        assert_eq!(matching.len(), 1);
        assert_eq!(1, missing.len());
        assert_eq!(matching[0].time_range.unwrap().min_timestamp.value(), 5000);
        assert_eq!(missing[0].value(), 10000);

        // Test case 2g: Cross 0
        let (matching, missing) = partitions
            .find_partitions_by_time_range(
                &(Arc::new(TimestampMillisecondArray::from_iter_values(-1000..=1000)) as ArrayRef),
                &parts,
                Timestamp::new_millisecond(-1000),
                Timestamp::new_millisecond(1000),
            )
            .unwrap();
        assert_eq!(matching.len(), 1);
        assert_eq!(matching[0].time_range.unwrap().min_timestamp.value(), 0);
        assert_eq!(1, missing.len());
        assert_eq!(missing[0].value(), -5000);

        // Test case 3: sparse data
        let (matching, missing) = partitions
            .find_partitions_by_time_range(
                &(Arc::new(TimestampMillisecondArray::from(vec![
                    -100000000000,
                    0,
                    100000000000,
                ])) as ArrayRef),
                &parts,
                Timestamp::new_millisecond(-100000000000),
                Timestamp::new_millisecond(100000000000),
            )
            .unwrap();
        assert_eq!(2, matching.len());
        assert_eq!(matching[0].time_range.unwrap().min_timestamp.value(), 0);
        assert_eq!(matching[1].time_range.unwrap().min_timestamp.value(), 5000);
        assert_eq!(2, missing.len());
        assert_eq!(missing[0].value(), -100000000000);
        assert_eq!(missing[1].value(), 100000000000);
    }

    fn build_part(ts: &[i64], sequence: SequenceNumber) -> BulkPart {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "ts",
                arrow::datatypes::DataType::Timestamp(
                    arrow::datatypes::TimeUnit::Millisecond,
                    None,
                ),
                false,
            ),
            Field::new("val", DataType::Utf8, true),
        ]));
        let ts_data = ts.to_vec();
        let ts_array = Arc::new(TimestampMillisecondArray::from(ts_data));
        let val_array = Arc::new(StringArray::from_iter_values(
            ts.iter().map(|v| v.to_string()),
        ));
        let batch = RecordBatch::try_new(
            schema,
            vec![ts_array.clone() as ArrayRef, val_array.clone() as ArrayRef],
        )
        .unwrap();
        let max_ts = ts.iter().max().copied().unwrap();
        let min_ts = ts.iter().min().copied().unwrap();
        BulkPart {
            batch,
            num_rows: ts.len(),
            max_ts,
            min_ts,
            sequence,
            timestamp_index: 0,
        }
    }

    #[test]
    fn test_write_bulk() {
        let mut metadata_builder = RegionMetadataBuilder::new(0.into());
        metadata_builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "ts",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Timestamp,
                column_id: 0,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("val", ConcreteDataType::string_datatype(), false),
                semantic_type: SemanticType::Field,
                column_id: 1,
            })
            .primary_key(vec![]);
        let metadata = Arc::new(metadata_builder.build().unwrap());

        let builder = Arc::new(TimeSeriesMemtableBuilder::default());
        let partitions = TimePartitions::new(
            metadata.clone(),
            builder.clone(),
            0,
            Some(Duration::from_secs(5)),
        );

        // Test case 1: Write to single partition
        partitions
            .write_bulk(build_part(&[1000, 2000, 3000], 0))
            .unwrap();

        let parts = partitions.list_partitions();
        assert_eq!(1, parts.len());
        let iter = parts[0].memtable.iter(None, None, None).unwrap();
        let timestamps = collect_iter_timestamps(iter);
        assert_eq!(&[1000, 2000, 3000], &timestamps[..]);

        // Test case 2: Write across multiple existing partitions
        partitions
            .write_bulk(build_part(&[4000, 5000, 6000], 1))
            .unwrap();
        let parts = partitions.list_partitions();
        assert_eq!(2, parts.len());
        // Check first partition [0, 5000)
        let iter = parts[0].memtable.iter(None, None, None).unwrap();
        let timestamps = collect_iter_timestamps(iter);
        assert_eq!(&[1000, 2000, 3000, 4000], &timestamps[..]);
        // Check second partition [5000, 10000)
        let iter = parts[1].memtable.iter(None, None, None).unwrap();
        let timestamps = collect_iter_timestamps(iter);
        assert_eq!(&[5000, 6000], &timestamps[..]);

        // Test case 3: Write requiring new partition
        partitions
            .write_bulk(build_part(&[11000, 12000], 3))
            .unwrap();

        let parts = partitions.list_partitions();
        assert_eq!(3, parts.len());

        // Check new partition [10000, 15000)
        let iter = parts[2].memtable.iter(None, None, None).unwrap();
        let timestamps = collect_iter_timestamps(iter);
        assert_eq!(&[11000, 12000], &timestamps[..]);

        // Test case 4: Write with no time range partitioning
        let partitions = TimePartitions::new(metadata.clone(), builder, 3, None);

        partitions
            .write_bulk(build_part(&[1000, 5000, 9000], 4))
            .unwrap();

        let parts = partitions.list_partitions();
        assert_eq!(1, parts.len());
        let iter = parts[0].memtable.iter(None, None, None).unwrap();
        let timestamps = collect_iter_timestamps(iter);
        assert_eq!(&[1000, 5000, 9000], &timestamps[..]);
    }

    #[test]
    fn test_split_record_batch() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond.as_arrow_time_unit(), None),
                false,
            ),
            Field::new("val", DataType::Utf8, true),
        ]));

        let ts_array = Arc::new(TimestampMillisecondArray::from(vec![
            1000, 2000, 5000, 7000, 8000,
        ]));
        let val_array = Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![ts_array as ArrayRef, val_array as ArrayRef],
        )
        .unwrap();

        let part = BulkPart {
            batch,
            num_rows: 5,
            max_ts: 8000,
            min_ts: 1000,
            sequence: 0,
            timestamp_index: 0,
        };

        let result = filter_record_batch(&part, 1000, 2000).unwrap();
        assert!(result.is_some());
        let filtered = result.unwrap();
        assert_eq!(filtered.num_rows, 1);
        assert_eq!(filtered.min_ts, 1000);
        assert_eq!(filtered.max_ts, 1000);

        // Test splitting with range [3000, 6000)
        let result = filter_record_batch(&part, 3000, 6000).unwrap();
        assert!(result.is_some());
        let filtered = result.unwrap();
        assert_eq!(filtered.num_rows, 1);
        assert_eq!(filtered.min_ts, 5000);
        assert_eq!(filtered.max_ts, 5000);

        // Test splitting with range that includes no points
        let result = filter_record_batch(&part, 3000, 4000).unwrap();
        assert!(result.is_none());

        // Test splitting with range that includes all points
        let result = filter_record_batch(&part, 0, 9000).unwrap();
        assert!(result.is_some());
        let filtered = result.unwrap();
        assert_eq!(filtered.num_rows, 5);
        assert_eq!(filtered.min_ts, 1000);
        assert_eq!(filtered.max_ts, 8000);
    }
}
