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

//! Version control of mito engine.
//!
//! Version is an immutable snapshot of region's metadata.
//!
//! To read latest data from `VersionControl`, we should
//! 1. Acquire `Version` from `VersionControl`.
//! 2. Then acquire last sequence.
//!
//! Reason: data may be flushed/compacted and some data with old sequence may be removed
//! and became invisible between step 1 and 2, so need to acquire version at first.

use std::sync::{Arc, RwLock};

use store_api::storage::SequenceNumber;

use crate::memtable::version::{MemtableVersion, MemtableVersionRef};
use crate::memtable::MemtableRef;
use crate::metadata::RegionMetadataRef;
use crate::sst::version::{SstVersion, SstVersionRef};
use crate::wal::EntryId;

/// Controls metadata and sequence numbers for a region.
///
/// It manages metadata in a copy-on-write fashion. Any modification to a region's metadata
/// will generate a new [Version].
#[derive(Debug)]
pub(crate) struct VersionControl {
    /// Latest version.
    version: VersionRef,
    /// Sequence number of last committed data.
    committed_sequence: SequenceNumber,
    /// Last WAL entry Id.
    last_entry_id: EntryId,
}

impl VersionControl {
    /// Returns a new [VersionControl] with specific `version`.
    pub(crate) fn new(version: Version) -> VersionControl {
        VersionControl {
            version: Arc::new(version),
            committed_sequence: 0,
            last_entry_id: 0,
        }
    }

    /// Returns current [Version].
    pub(crate) fn version(&self) -> VersionRef {
        self.version.clone()
    }

    /// Returns last committed sequence.
    pub(crate) fn committed_sequence(&self) -> SequenceNumber {
        self.committed_sequence
    }

    /// Returns last entry id.
    pub(crate) fn last_entry_id(&self) -> EntryId {
        self.last_entry_id
    }
}

pub(crate) type VersionControlRef = Arc<RwLock<VersionControl>>;

/// Static metadata of a region.
#[derive(Clone, Debug)]
pub(crate) struct Version {
    /// Metadata of the region.
    ///
    /// Altering metadata isn't frequent, storing metadata in Arc to allow sharing
    /// metadata and reuse metadata when creating a new `Version`.
    pub(crate) metadata: RegionMetadataRef,
    /// Mutable and immutable memtables.
    ///
    /// Wrapped in Arc to make clone of `Version` much cheaper.
    pub(crate) memtables: MemtableVersionRef,
    /// SSTs of the region.
    pub(crate) ssts: SstVersionRef,
    /// Inclusive max sequence of flushed data.
    pub(crate) flushed_sequence: SequenceNumber,
    // TODO(yingwen): RegionOptions.
}

pub(crate) type VersionRef = Arc<Version>;

/// Version builder.
pub(crate) struct VersionBuilder {
    metadata: RegionMetadataRef,
    /// Mutable memtable.
    mutable: MemtableRef,
}

impl VersionBuilder {
    /// Returns a new builder.
    pub(crate) fn new(metadata: RegionMetadataRef, mutable: MemtableRef) -> VersionBuilder {
        VersionBuilder { metadata, mutable }
    }

    /// Builds a new [Version] from the builder.
    pub(crate) fn build(self) -> Version {
        Version {
            metadata: self.metadata,
            memtables: Arc::new(MemtableVersion::new(self.mutable)),
            ssts: Arc::new(SstVersion::new()),
            flushed_sequence: 0,
        }
    }
}
