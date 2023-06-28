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

//! Storage Engine traits.
//!
//! [`StorageEngine`] is the abstraction over a multi-regions, schematized data storage system,
//! a [`StorageEngine`] instance manages a bunch of storage unit called [`Region`], which holds
//! chunks of rows, support operations like PUT/DELETE/SCAN.

use std::time::Duration;

use async_trait::async_trait;
use common_error::ext::ErrorExt;

use crate::storage::descriptors::RegionDescriptor;
use crate::storage::region::Region;

/// Storage engine provides primitive operations to store and access data.
#[async_trait]
pub trait StorageEngine: Send + Sync + Clone + 'static {
    type Error: ErrorExt + Send + Sync;
    type Region: Region;

    /// Opens an existing region. Returns `Ok(None)` if region does not exists.
    async fn open_region(
        &self,
        ctx: &EngineContext,
        name: &str,
        opts: &OpenOptions,
    ) -> Result<Option<Self::Region>, Self::Error>;

    /// Closes given region.
    async fn close_region(
        &self,
        ctx: &EngineContext,
        name: &str,
        opts: &CloseOptions,
    ) -> Result<(), Self::Error>;

    /// Creates and returns the created region.
    ///
    /// Returns existing region if region with same name already exists. The region will
    /// be opened before returning.
    async fn create_region(
        &self,
        ctx: &EngineContext,
        descriptor: RegionDescriptor,
        opts: &CreateOptions,
    ) -> Result<Self::Region, Self::Error>;

    /// Drops given region.
    ///
    /// The region will be closed before dropping.
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

    /// Close the engine.
    async fn close(&self, ctx: &EngineContext) -> Result<(), Self::Error>;
}

/// Storage engine context.
#[derive(Debug, Clone, Default)]
pub struct EngineContext {}

/// Options to create a region.
#[derive(Debug, Clone, Default)]
pub struct CreateOptions {
    /// Region parent directory
    pub parent_dir: String,
    /// Region memtable max size in bytes
    pub write_buffer_size: Option<usize>,
    /// Region SST files TTL
    pub ttl: Option<Duration>,
}

/// Options to open a region.
#[derive(Debug, Clone, Default)]
pub struct OpenOptions {
    /// Region parent directory
    pub parent_dir: String,
    /// Region memtable max size in bytes
    pub write_buffer_size: Option<usize>,
    /// Region SST files TTL
    pub ttl: Option<Duration>,
}

/// Options to close a region.
#[derive(Debug, Clone, Default)]
pub struct CloseOptions {
    /// Flush region
    pub flush: bool,
}
