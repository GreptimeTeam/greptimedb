use crate::metadata::RegionMetadataRef;

/// A version of region in memory state.
pub struct Version {
    pub metadata: RegionMetadataRef,
}
