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

use smallvec::SmallVec;

use crate::memtable::{MemtableId, MemtableRef};

/// A version of current memtables in a region.
#[derive(Debug, Clone)]
pub(crate) struct MemtableVersion {
    /// Mutable memtable.
    pub(crate) mutable: MemtableRef,
    /// Immutable memtables.
    ///
    /// We only allow one flush job per region but if a flush job failed, then we
    /// might need to store more than one immutable memtable on the next time we
    /// flush the region.
    immutables: SmallVec<[MemtableRef; 2]>,
}

pub(crate) type MemtableVersionRef = Arc<MemtableVersion>;

impl MemtableVersion {
    /// Returns a new [MemtableVersion] with specific mutable memtable.
    pub(crate) fn new(mutable: MemtableRef) -> MemtableVersion {
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
        let mut mems = Vec::with_capacity(self.immutables.len() + 1);
        mems.push(self.mutable.clone());
        mems.extend_from_slice(&self.immutables);
        mems
    }

    /// Returns a new [MemtableVersion] which switches the old mutable memtable to immutable
    /// memtable.
    ///
    /// Returns `None` if the mutable memtable is empty.
    #[must_use]
    pub(crate) fn freeze_mutable(&self, mutable: MemtableRef) -> Option<MemtableVersion> {
        debug_assert!(mutable.is_empty());
        if self.mutable.is_empty() {
            // No need to freeze the mutable memtable.
            return None;
        }

        // Marks the mutable memtable as immutable so it can free the memory usage from our
        // soft limit.
        self.mutable.mark_immutable();
        // Pushes the mutable memtable to immutable list.
        let immutables = self
            .immutables
            .iter()
            .cloned()
            .chain([self.mutable.clone()])
            .collect();
        Some(MemtableVersion {
            mutable,
            immutables,
        })
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
        self.mutable.stats().estimated_bytes
    }

    /// Returns the memory usage of the immutable memtables.
    pub(crate) fn immutables_usage(&self) -> usize {
        self.immutables
            .iter()
            .map(|mem| mem.stats().estimated_bytes)
            .sum()
    }

    /// Returns true if the memtable version is empty.
    ///
    /// The version is empty when mutable memtable is empty and there is no
    /// immutable memtables.
    pub(crate) fn is_empty(&self) -> bool {
        self.mutable.is_empty() && self.immutables.is_empty()
    }
}
