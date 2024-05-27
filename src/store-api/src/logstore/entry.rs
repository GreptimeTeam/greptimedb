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

use crate::storage::RegionId;

/// An entry's id.
/// Different log store implementations may interpret the id to different meanings.
pub type Id = u64;

/// The raw Wal entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawEntry {
    pub region_id: RegionId,
    pub entry_id: Id,
    pub data: Vec<u8>,
}

impl Entry for RawEntry {
    fn into_raw_entry(self) -> RawEntry {
        self
    }

    fn data(&self) -> &[u8] {
        &self.data
    }

    fn id(&self) -> Id {
        self.entry_id
    }

    fn region_id(&self) -> RegionId {
        self.region_id
    }

    fn estimated_size(&self) -> usize {
        std::mem::size_of_val(self)
    }
}

/// Entry is the minimal data storage unit through which users interact with the log store.
/// The log store implementation may have larger or smaller data storage unit than an entry.
pub trait Entry: Send + Sync {
    /// Consumes [Entry] and converts to [RawEntry].
    fn into_raw_entry(self) -> RawEntry;

    /// Returns the contained data of the entry.
    fn data(&self) -> &[u8];

    /// Returns the id of the entry.
    /// Usually the namespace id is identical with the region id.
    fn id(&self) -> Id;

    /// Returns the [RegionId]
    fn region_id(&self) -> RegionId;

    /// Computes the estimated encoded size.
    fn estimated_size(&self) -> usize;
}
