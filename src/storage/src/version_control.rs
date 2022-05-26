use std::sync::Arc;

use crate::metadata::{RegionMetadata, RegionMetadataRef};
use crate::sync::CowCell;

/// Controls version of region in memory state.
pub struct VersionControl {
    /// Metadata of the region.
    metadata: CowCell<RegionMetadata>,
}

impl VersionControl {
    /// Construct a new version control from `metadata`.
    pub fn new(metadata: RegionMetadata) -> VersionControl {
        VersionControl {
            metadata: CowCell::new(metadata),
        }
    }

    pub fn metadata(&self) -> RegionMetadataRef {
        self.metadata.get()
    }
}

pub type VersionControlRef = Arc<VersionControl>;
