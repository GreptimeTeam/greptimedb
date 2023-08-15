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
    mutable: MemtableRef,
    /// Immutable memtables.
    immutables: Vec<MemtableRef>,
}

pub(crate) type MemtableVersionRef = Arc<MemtableVersion>;

impl MemtableVersion {
    /// Returns a new [MemtableVersion] with specific mutable memtable.
    pub(crate) fn new(mutable: MemtableRef) -> MemtableVersion {
        MemtableVersion {
            mutable,
            immutables: vec![],
        }
    }

    /// Returns the mutable memtable.
    pub(crate) fn mutable(&self) -> &MemtableRef {
        &self.mutable
    }
}
