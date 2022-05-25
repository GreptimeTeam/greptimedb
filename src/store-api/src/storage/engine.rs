//! Storage Engine traits.
//!
//! [`StorageEngine`] is the abstraction over a multi-regions, schematized data storage system,
//! a [`StorageEngine`] instance manages a bunch of storage unit called [`Region`], which holds
//! chunks of rows, support operations like PUT/DELETE/SCAN.

use async_trait::async_trait;
use common_error::ext::ErrorExt;

use crate::storage::descriptors::RegionDescriptor;
use crate::storage::region::Region;

/// Storage engine provides primitive operations to store and access data.
#[async_trait]
pub trait StorageEngine: Send + Sync + Clone {
    type Error: ErrorExt + Send + Sync;
    type Region: Region;

    /// Opens an existing region.
    async fn open_region(
        &self,
        ctx: &EngineContext,
        name: &str,
    ) -> Result<Self::Region, Self::Error>;

    /// Closes given region.
    async fn close_region(
        &self,
        ctx: &EngineContext,
        region: Self::Region,
    ) -> Result<(), Self::Error>;

    /// Creates and returns the created region.
    ///
    /// Returns exsiting region if region with same name already exists.
    async fn create_region(
        &self,
        ctx: &EngineContext,
        descriptor: RegionDescriptor,
    ) -> Result<Self::Region, Self::Error>;

    /// Drops given region.
    async fn drop_region(
        &self,
        ctx: &EngineContext,
        region: Self::Region,
    ) -> Result<(), Self::Error>;

    /// Returns the opened region with given name.
    fn get_region(
        &self,
        ctx: &EngineContext,
        name: &str,
    ) -> Result<Option<Self::Region>, Self::Error>;
}

/// Storage engine context.
#[derive(Debug, Clone)]
pub struct EngineContext {}
