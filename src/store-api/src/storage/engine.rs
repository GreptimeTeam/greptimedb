//! Storage Engine traits.
//!
//! [`StorageEngine`] is the abstraction over a multi-regions, schematized data storage system,
//! a [`StorageEngine`] instance manages a bunch of storage unit called [`Region`], which holds
//! chunks of rows, support operations like PUT/DELETE/SCAN.

use common_error::ext::ErrorExt;

use crate::storage::descriptors::RegionDescriptor;
use crate::storage::region::Region;

/// Storage engine provides primitive operations to store and access data.
pub trait StorageEngine: Send + Sync + Clone {
    type Error: ErrorExt + Send + Sync;
    type Region: Region;

    /// Open an existing region.
    fn open_region(&self, ctx: &EngineContext, name: &str) -> Result<Self::Region, Self::Error>;

    /// Close given region.
    fn close_region(&self, ctx: &EngineContext, region: Self::Region) -> Result<(), Self::Error>;

    /// Create and return a new region.
    fn create_region(
        &self,
        ctx: &EngineContext,
        descriptor: RegionDescriptor,
    ) -> Result<Self::Region, Self::Error>;

    /// Drop given region.
    fn drop_region(&self, ctx: &EngineContext, region: Self::Region) -> Result<(), Self::Error>;

    /// Return the opened region with given name.
    fn get_region(
        &self,
        ctx: &EngineContext,
        name: &str,
    ) -> Result<Option<Self::Region>, Self::Error>;
}

/// Storage engine context.
#[derive(Debug, Clone)]
pub struct EngineContext {}
