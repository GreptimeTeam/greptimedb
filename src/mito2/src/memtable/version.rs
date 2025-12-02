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
use std::time::Duration;

use smallvec::SmallVec;
use store_api::metadata::RegionMetadataRef;

use crate::error::Result;
use crate::memtable::time_partition::TimePartitionsRef;
use crate::memtable::{MemtableId, MemtableRef};

pub(crate) type SmallMemtableVec = SmallVec<[MemtableRef; 2]>;

/// A version of current memtables in a region.
#[derive(Debug, Clone)]
pub(crate) struct MemtableVersion {
    /// Mutable memtable.
    pub(crate) mutable: TimePartitionsRef,
    /// Immutable memtables.
    ///
    /// We only allow one flush job per region but if a flush job failed, then we
    /// might need to store more than one immutable memtable on the next time we
    /// flush the region.
    immutables: SmallMemtableVec,
}

pub(crate) type MemtableVersionRef = Arc<MemtableVersion>;

impl MemtableVersion {
    /// Returns a new [MemtableVersion] with specific mutable memtable.
    pub(crate) fn new(mutable: TimePartitionsRef) -> MemtableVersion {
        MemtableVersion {
            mutable,
            immutables: SmallVec::new(),
        }
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
            let mutable = self.mutable.new_with_part_duration(time_window, None);
            common_telemetry::debug!(
                "Freeze empty memtable, update partition duration from {:?} to {:?}",
                self.mutable.part_duration(),
                time_window
            );
            return Ok(Some(MemtableVersion {
                mutable: Arc::new(mutable),
                immutables: self.immutables.clone(),
            }));
        }

        // Marks the mutable memtable as immutable so it can free the memory usage from our
        // soft limit.
        self.mutable.freeze()?;
        // Fork the memtable.
        if Some(self.mutable.part_duration()) != time_window {
            common_telemetry::debug!(
                "Fork memtable, update partition duration from {:?}, to {:?}",
                self.mutable.part_duration(),
                time_window
            );
        }
        let mutable = Arc::new(self.mutable.fork(metadata, time_window));

        let mut immutables =
            SmallVec::with_capacity(self.immutables.len() + self.mutable.num_partitions());
        immutables.extend(self.immutables.iter().cloned());
        // Pushes the mutable memtable to immutable list.
        self.mutable.list_memtables_to_small_vec(&mut immutables);

        Ok(Some(MemtableVersion {
            mutable,
            immutables,
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

    /// Returns true if the memtable version is empty.
    ///
    /// The version is empty when mutable memtable is empty and there is no
    /// immutable memtables.
    pub(crate) fn is_empty(&self) -> bool {
        self.mutable.is_empty() && self.immutables.is_empty()
    }
}
