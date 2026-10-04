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

//! Memtable version.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use common_time::Timestamp;
use smallvec::SmallVec;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::SequenceNumber;

use crate::error::Result;
use crate::memtable::bulk::part::BulkPart;
use crate::memtable::time_partition::TimePartitions;
use crate::memtable::{KeyValues, MemtableBuilderRef, MemtableId, MemtableRef};

pub(crate) type SmallMemtableVec = SmallVec<[MemtableRef; 2]>;

/// A version of current memtables in a region.
#[derive(Debug, Clone)]
pub(crate) struct MemtableVersion {
    /// Mutable memtable.
    pub(crate) mutable: MutableMemtablesRef,
    /// Immutable memtables.
    ///
    /// We only allow one flush job per region but if a flush job failed, then we
    /// might need to store more than one immutable memtable on the next time we
    /// flush the region.
    immutables: SmallMemtableVec,
    /// Whether immutable memtables may contain writes not protected by WAL.
    /// Flushes are serialized per region and cover all immutable memtables.
    immutable_has_unlogged_writes: bool,
}

pub(crate) type MemtableVersionRef = Arc<MemtableVersion>;

impl MemtableVersion {
    /// Returns a new [MemtableVersion] with specific mutable memtable.
    pub(crate) fn new(mutable: TimePartitions) -> MemtableVersion {
        MemtableVersion {
            mutable: Arc::new(MutableMemtables::new(mutable)),
            immutables: SmallVec::new(),
            immutable_has_unlogged_writes: false,
        }
    }

    /// Returns whether current memtables may contain writes not protected by WAL.
    pub(crate) fn has_unlogged_writes(&self) -> bool {
        self.mutable.has_unlogged_writes() || self.immutable_has_unlogged_writes
    }

    /// Immutable memtables.
    pub(crate) fn immutables(&self) -> &[MemtableRef] {
        &self.immutables
    }

    /// Lists mutable and immutable memtables.
    pub(crate) fn list_memtables(&self) -> Vec<MemtableRef> {
        let mut mems = Vec::with_capacity(self.immutables.len() + self.mutable.num_partitions());
        self.mutable.list_memtables(&mut mems);
        mems.extend_from_slice(&self.immutables);
        mems
    }

    /// Returns a sequence lower bound covering mutable and immutable memtables.
    /// Empty memtables impose no compaction barrier, including newly forked ones.
    pub(crate) fn min_sequence(&self) -> Option<SequenceNumber> {
        self.list_memtables()
            .iter()
            .filter(|mem| !mem.is_empty())
            .map(|mem| mem.min_sequence())
            .min()
    }

    /// Returns a new [MemtableVersion] which switches the old mutable memtable to immutable
    /// memtable.
    ///
    /// It will switch to use the `time_window` provided.
    ///
    /// Returns `None` if the mutable memtable is empty.
    pub(crate) fn freeze_mutable(
        &self,
        metadata: &RegionMetadataRef,
        time_window: Option<Duration>,
    ) -> Result<Option<MemtableVersion>> {
        if self.mutable.is_empty() {
            // No need to freeze the mutable memtable, but we need to check the time window.
            if Some(self.mutable.part_duration()) == time_window {
                // If the time window is the same, we don't need to update it.
                return Ok(None);
            }

            // Update the time window.
            let mutable = self
                .mutable
                .partitions
                .new_with_part_duration(time_window, None);
            common_telemetry::debug!(
                "Freeze empty memtable, update partition duration from {:?} to {:?}",
                self.mutable.part_duration(),
                time_window
            );
            return Ok(Some(MemtableVersion {
                mutable: Arc::new(MutableMemtables::new(mutable)),
                immutables: self.immutables.clone(),
                immutable_has_unlogged_writes: self.immutable_has_unlogged_writes,
            }));
        }

        // Marks the mutable memtable as immutable so it can free the memory usage from our
        // soft limit.
        self.mutable.partitions.freeze()?;
        // Fork the memtable.
        if Some(self.mutable.part_duration()) != time_window {
            common_telemetry::debug!(
                "Fork memtable, update partition duration from {:?}, to {:?}",
                self.mutable.part_duration(),
                time_window
            );
        }
        let mutable = Arc::new(MutableMemtables::new(
            self.mutable.partitions.fork(metadata, time_window),
        ));

        let mut immutables =
            SmallVec::with_capacity(self.immutables.len() + self.mutable.num_partitions());
        immutables.extend(self.immutables.iter().cloned());
        // Pushes the mutable memtable to immutable list.
        self.mutable
            .partitions
            .list_memtables_to_small_vec(&mut immutables);

        Ok(Some(MemtableVersion {
            mutable,
            immutables,
            immutable_has_unlogged_writes: self.immutable_has_unlogged_writes
                || self.mutable.has_unlogged_writes(),
        }))
    }

    /// Removes memtables by ids from immutable memtables.
    pub(crate) fn remove_memtables(&mut self, ids: &[MemtableId]) {
        self.immutables = self
            .immutables
            .iter()
            .filter(|mem| !ids.contains(&mem.id()))
            .cloned()
            .collect();
        if self.immutables.is_empty() {
            self.immutable_has_unlogged_writes = false;
        }
    }

    /// Returns the memory usage of the mutable memtable.
    pub(crate) fn mutable_usage(&self) -> usize {
        self.mutable.memory_usage()
    }

    /// Returns the memory usage of the immutable memtables.
    pub(crate) fn immutables_usage(&self) -> usize {
        self.immutables
            .iter()
            .map(|mem| mem.stats().estimated_bytes)
            .sum()
    }

    /// Returns the number of rows in memtables.
    pub(crate) fn num_rows(&self) -> u64 {
        self.immutables
            .iter()
            .map(|mem| mem.stats().num_rows as u64)
            .sum::<u64>()
            + self.mutable.num_rows()
    }

    /// Returns the time range covered by the memtables, if any hold data.
    pub(crate) fn time_range(&self) -> Option<(Timestamp, Timestamp)> {
        let mut mutables = Vec::new();
        self.mutable.list_memtables(&mut mutables);
        self.immutables
            .iter()
            .chain(mutables.iter())
            .filter_map(|mem| mem.stats().time_range())
            .reduce(|(min_a, max_a), (min_b, max_b)| (min_a.min(min_b), max_a.max(max_b)))
    }

    /// Returns true if the memtable version is empty.
    ///
    /// The version is empty when mutable memtable is empty and there is no
    /// immutable memtables.
    pub(crate) fn is_empty(&self) -> bool {
        self.mutable.is_empty() && self.immutables.is_empty()
    }
}

/// Mutable time partitions and their shared write state.
///
/// Version clones share this object. Freezing creates a new object so writes to
/// the new mutable memtables cannot change the old version's state.
#[derive(Debug)]
pub(crate) struct MutableMemtables {
    partitions: TimePartitions,
    /// Set before installation because failed writes may install some rows.
    has_unlogged_writes: AtomicBool,
}

pub(crate) type MutableMemtablesRef = Arc<MutableMemtables>;

impl MutableMemtables {
    fn new(partitions: TimePartitions) -> Self {
        Self {
            partitions,
            has_unlogged_writes: AtomicBool::new(false),
        }
    }

    /// Marks that these memtables may contain writes not protected by WAL.
    pub(crate) fn mark_unlogged_writes(&self) {
        self.has_unlogged_writes.store(true, Ordering::Relaxed);
    }

    fn has_unlogged_writes(&self) -> bool {
        self.has_unlogged_writes.load(Ordering::Relaxed)
    }

    /// Writes key values to the mutable time partitions.
    pub(crate) fn write(&self, kvs: &KeyValues) -> Result<()> {
        self.partitions.write(kvs)
    }

    /// Writes a bulk part to the mutable time partitions.
    pub(crate) fn write_bulk(&self, part: BulkPart) -> Result<()> {
        self.partitions.write_bulk(part)
    }

    /// Returns whether all mutable time partitions are empty.
    pub(crate) fn is_empty(&self) -> bool {
        self.partitions.is_empty()
    }

    /// Returns the memtable builder.
    pub(crate) fn memtable_builder(&self) -> &MemtableBuilderRef {
        self.partitions.memtable_builder()
    }

    /// Returns the next memtable ID.
    pub(crate) fn next_memtable_id(&self) -> MemtableId {
        self.partitions.next_memtable_id()
    }

    /// Returns the time partition duration.
    pub(crate) fn part_duration(&self) -> Duration {
        self.partitions.part_duration()
    }

    /// Creates empty time partitions with an optional new duration and builder.
    /// The returned partitions do not share data or write state with this object.
    pub(crate) fn new_with_part_duration(
        &self,
        part_duration: Option<Duration>,
        memtable_builder: Option<MemtableBuilderRef>,
    ) -> TimePartitions {
        self.partitions
            .new_with_part_duration(part_duration, memtable_builder)
    }

    /// Returns the mutable memory usage.
    pub(crate) fn memory_usage(&self) -> usize {
        self.partitions.memory_usage()
    }

    /// Returns the number of time series in mutable time partitions.
    pub(crate) fn series_count(&self) -> usize {
        self.partitions.series_count()
    }

    fn num_rows(&self) -> u64 {
        self.partitions.num_rows()
    }

    fn num_partitions(&self) -> usize {
        self.partitions.num_partitions()
    }

    fn list_memtables(&self, memtables: &mut Vec<MemtableRef>) {
        self.partitions.list_memtables(memtables);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memtable::time_series::TimeSeriesMemtableBuilder;
    use crate::test_util::memtable_util;

    #[test]
    fn test_unlogged_writes_follow_memtable_lifecycle() {
        for new_unlogged_writes in [false, true] {
            let metadata = memtable_util::metadata_for_test();
            let partitions = TimePartitions::new(
                metadata.clone(),
                Arc::new(TimeSeriesMemtableBuilder::default()),
                0,
                Some(Duration::from_secs(5)),
            );
            let version = MemtableVersion::new(partitions);
            let snapshot = version.clone();
            assert!(!version.has_unlogged_writes());

            let kvs = memtable_util::build_key_values(
                &metadata,
                "hello".to_string(),
                0,
                &[1000, 7000],
                1,
            );
            version.mutable.mark_unlogged_writes();
            version.mutable.write(&kvs).unwrap();
            assert!(snapshot.has_unlogged_writes());

            let mut frozen = version
                .freeze_mutable(&metadata, Some(Duration::from_secs(5)))
                .unwrap()
                .unwrap();
            assert_eq!(2, frozen.immutables().len());
            assert!(frozen.has_unlogged_writes());
            assert!(!frozen.mutable.has_unlogged_writes());
            assert!(!Arc::ptr_eq(&version.mutable, &frozen.mutable));

            let during_flush = frozen.clone();
            if new_unlogged_writes {
                frozen.mutable.mark_unlogged_writes();
            }
            frozen.mutable.write(&kvs).unwrap();
            let ids: Vec<_> = frozen.immutables().iter().map(|mem| mem.id()).collect();
            frozen.remove_memtables(&ids);
            assert_eq!(new_unlogged_writes, frozen.has_unlogged_writes());
            assert_eq!(
                new_unlogged_writes,
                during_flush.mutable.has_unlogged_writes()
            );
            // Removing flushed memtables does not mutate the old flush snapshot.
            assert_eq!(2, during_flush.immutables().len());
            assert!(during_flush.has_unlogged_writes());

            let mut next_flush = frozen
                .freeze_mutable(&metadata, Some(Duration::from_secs(5)))
                .unwrap()
                .unwrap();
            let ids: Vec<_> = next_flush.immutables().iter().map(|mem| mem.id()).collect();
            next_flush.remove_memtables(&ids);
            assert!(!next_flush.has_unlogged_writes());
        }
    }

    #[test]
    fn test_unlogged_writes_survive_flush_retry() {
        let metadata = memtable_util::metadata_for_test();
        let partitions = TimePartitions::new(
            metadata.clone(),
            Arc::new(TimeSeriesMemtableBuilder::default()),
            0,
            Some(Duration::from_secs(5)),
        );
        let version = MemtableVersion::new(partitions);
        let kvs = memtable_util::build_key_values(&metadata, "hello".to_string(), 0, &[1000], 1);
        version.mutable.mark_unlogged_writes();
        version.mutable.write(&kvs).unwrap();
        let frozen = version
            .freeze_mutable(&metadata, Some(Duration::from_secs(5)))
            .unwrap()
            .unwrap();

        // A failed flush leaves immutable memtables installed. Changing the
        // empty mutable's partition duration must retain their write state.
        let frozen = frozen
            .freeze_mutable(&metadata, Some(Duration::from_secs(10)))
            .unwrap()
            .unwrap();
        assert!(frozen.has_unlogged_writes());
        assert!(!frozen.mutable.has_unlogged_writes());

        // A retry includes the old immutable memtables and new logged writes.
        frozen.mutable.write(&kvs).unwrap();
        let mut retried = frozen
            .freeze_mutable(&metadata, Some(Duration::from_secs(10)))
            .unwrap()
            .unwrap();
        assert_eq!(2, retried.immutables().len());
        assert!(retried.has_unlogged_writes());
        let ids: Vec<_> = retried.immutables().iter().map(|mem| mem.id()).collect();
        retried.remove_memtables(&ids);
        assert!(!retried.has_unlogged_writes());

        let replacement = MemtableVersion::new(TimePartitions::new(
            metadata,
            frozen.mutable.memtable_builder().clone(),
            frozen.mutable.next_memtable_id(),
            None,
        ));
        assert!(!replacement.has_unlogged_writes());
        assert!(frozen.has_unlogged_writes());
    }
}
