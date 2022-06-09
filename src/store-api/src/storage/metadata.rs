use crate::storage::SchemaRef;

/// Metadata of a region.
pub trait RegionMeta: Send + Sync {
    /// Returns the schema of the region.
    fn schema(&self) -> &SchemaRef;
}
