//! Version control of storage.
//!
//! To read latest data from `VersionControl`, we need to
//! 1. Acquire `Version` from `VersionControl`.
//! 2. Then acquire last sequence.
//!
//! Reason: data may be flushed/compacted and some data with old sequence may be removed
//! and became invisible between step 1 and 2, so need to acquire version at first.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use store_api::storage::{SchemaRef, SequenceNumber};

use crate::memtable::{MemtableRef, MemtableSet};
use crate::metadata::{RegionMetadata, RegionMetadataRef};
use crate::sync::CowCell;

/// Controls version of in memory state for a region.
pub struct VersionControl {
    version: CowCell<Version>,
    /// Latest sequence that is committed and visible to user.
    committed_sequence: AtomicU64,
}

impl VersionControl {
    /// Construct a new version control from `metadata`.
    pub fn new(metadata: RegionMetadata, memtables: MemtableSet) -> VersionControl {
        VersionControl {
            version: CowCell::new(Version::new(metadata, memtables)),
            committed_sequence: AtomicU64::new(0),
        }
    }

    /// Returns current version.
    #[inline]
    pub fn current(&self) -> VersionRef {
        self.version.get()
    }

    /// Metadata of current version.
    pub fn metadata(&self) -> RegionMetadataRef {
        let version = self.current();
        version.metadata.clone()
    }

    #[inline]
    pub fn committed_sequence(&self) -> SequenceNumber {
        self.committed_sequence.load(Ordering::Acquire)
    }

    /// Set committed sequence to `value`.
    ///
    /// External synchronization is required to ensure only one thread can update the
    /// last sequence.
    #[inline]
    pub fn set_committed_sequence(&self, value: SequenceNumber) {
        // Release ordering should be enough to guarantee sequence is updated at last.
        self.committed_sequence.store(value, Ordering::Release);
    }
}

pub type VersionControlRef = Arc<VersionControl>;
pub type VersionRef = Arc<Version>;

// Get data from version, need to
// 1. acquire version first
// 2. acquire sequence later
//
// Reason: data may flush and some data with old sequence may be removed, so need
// to acquire version at first.

/// Version contains metadata and state of region.
pub struct Version {
    /// Metadata of the region. Altering metadata isn't frequent, storing metadata
    /// in Arc to allow sharing metadata and reuse metadata when creating a new
    /// `Version`.
    metadata: RegionMetadataRef,
    memtables: MemtableSet,
    // TODO(yingwen): Also need to store last sequence to this version when switching
    // version, so we can know the newest data can read from this version.
}

impl Version {
    pub fn new(metadata: RegionMetadata, memtables: MemtableSet) -> Version {
        Version {
            metadata: Arc::new(metadata),
            memtables,
        }
    }

    #[inline]
    pub fn schema(&self) -> &SchemaRef {
        &self.metadata.schema
    }

    #[inline]
    pub fn mutable_memtable(&self) -> &MemtableRef {
        self.memtables.mutable_memtable()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memtable::{DefaultMemtableBuilder, MemtableBuilder, MemtableSchema};
    use crate::test_util::descriptor_util::RegionDescBuilder;

    fn new_version_control() -> VersionControl {
        let desc = RegionDescBuilder::new("version-test")
            .enable_version_column(false)
            .build();
        let metadata: RegionMetadata = desc.try_into().unwrap();

        let schema = MemtableSchema::new(metadata.columns_row_key.clone());
        let memtable = DefaultMemtableBuilder {}.build(schema);
        let memtable_set = MemtableSet::new(memtable);

        VersionControl::new(metadata, memtable_set)
    }

    #[test]
    fn test_version_control() {
        let version_control = new_version_control();

        assert_eq!(0, version_control.committed_sequence());
        version_control.set_committed_sequence(12345);
        assert_eq!(12345, version_control.committed_sequence());
    }
}
