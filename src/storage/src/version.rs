use std::sync::Arc;

use crate::memtable::MemtableSet;
use crate::metadata::{RegionMetadata, RegionMetadataRef};
use crate::sync::CowCell;

/// Controls version of in memory state for a region.
pub struct VersionControl {
    version: CowCell<Version>,
}

impl VersionControl {
    /// Construct a new version control from `metadata`.
    pub fn new(metadata: RegionMetadata, memtables: MemtableSet) -> VersionControl {
        VersionControl {
            version: CowCell::new(Version::new(metadata, memtables)),
        }
    }

    /// Returns current version.
    pub fn current(&self) -> VersionRef {
        self.version.get()
    }

    /// Metadata of current version.
    pub fn metadata(&self) -> RegionMetadataRef {
        let version = self.current();
        version.metadata.clone()
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
    pub memtables: MemtableSet,
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
}
