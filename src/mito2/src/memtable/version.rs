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

use crate::memtable::MemtableRef;

/// A version of current memtables in a region.
#[derive(Debug)]
pub(crate) struct MemtableVersion {
    /// Mutable memtable.
    pub(crate) mutable: MemtableRef,
    /// Immutable memtable.
    pub(crate) immutable: Option<MemtableRef>,
}

pub(crate) type MemtableVersionRef = Arc<MemtableVersion>;

impl MemtableVersion {
    /// Returns a new [MemtableVersion] with specific mutable memtable.
    pub(crate) fn new(mutable: MemtableRef) -> MemtableVersion {
        MemtableVersion {
            mutable,
            immutable: None,
        }
    }

    /// Lists mutable and immutable memtables.
    pub(crate) fn list_memtables(&self) -> Vec<MemtableRef> {
        if let Some(immutable) = &self.immutable {
            vec![self.mutable.clone(), immutable.clone()]
        } else {
            vec![self.mutable.clone()]
        }
    }

    /// Returns a new [MemtableVersion] which switches the old mutable memtable to immutable
    /// memtable.
    ///
    /// Returns `None` if immutable memtable is `Some`.
    #[must_use]
    pub(crate) fn freeze_mutable(&self, mutable: MemtableRef) -> Option<MemtableVersion> {
        debug_assert!(self.mutable.is_empty());
        if self.immutable.is_some() {
            // There is already an immutable memtable.
            return None;
        }

        // Marks the mutable memtable as immutable so it can free the memory usage from our
        // soft limit.
        self.mutable.mark_immutable();

        Some(MemtableVersion {
            mutable,
            immutable: Some(self.mutable.clone()),
        })
    }

    /// Returns the memory usage of the mutable memtable.
    pub(crate) fn mutable_bytes_usage(&self) -> usize {
        // TODO(yingwen): Get memtable usage.
        0
    }
}
