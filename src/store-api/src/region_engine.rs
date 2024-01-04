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

//! Region Engine's definition

use std::any::Any;
use std::fmt::Display;
use std::sync::Arc;

use api::greptime_proto::v1::meta::{GrantedRegion as PbGrantedRegion, RegionRole as PbRegionRole};
use async_trait::async_trait;
use common_error::ext::BoxedError;
use common_recordbatch::SendableRecordBatchStream;
use serde::{Deserialize, Serialize};

use crate::logstore::entry;
use crate::metadata::RegionMetadataRef;
use crate::region_request::{AffectedRows, RegionRequest};
use crate::storage::{RegionId, ScanRequest};

/// The result of setting readonly for the region.
#[derive(Debug, PartialEq, Eq)]
pub enum SetReadonlyResponse {
    Success {
        /// Returns `last_entry_id` of the region if available(e.g., It's not available in file engine).
        last_entry_id: Option<entry::Id>,
    },
    NotFound,
}

impl SetReadonlyResponse {
    /// Returns a [SetReadonlyResponse::Success] with the `last_entry_id`.
    pub fn success(last_entry_id: Option<entry::Id>) -> Self {
        Self::Success { last_entry_id }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GrantedRegion {
    pub region_id: RegionId,
    pub region_role: RegionRole,
}
impl GrantedRegion {
    pub fn new(region_id: RegionId, region_role: RegionRole) -> Self {
        Self {
            region_id,
            region_role,
        }
    }
}

impl From<GrantedRegion> for PbGrantedRegion {
    fn from(value: GrantedRegion) -> Self {
        PbGrantedRegion {
            region_id: value.region_id.as_u64(),
            role: PbRegionRole::from(value.region_role).into(),
        }
    }
}

impl From<PbGrantedRegion> for GrantedRegion {
    fn from(value: PbGrantedRegion) -> Self {
        GrantedRegion {
            region_id: RegionId::from_u64(value.region_id),
            region_role: value.role().into(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RegionRole {
    // Readonly region(mito2)
    Follower,
    // Writable region(mito2), Readonly region(file).
    Leader,
}

impl Display for RegionRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RegionRole::Follower => write!(f, "Follower"),
            RegionRole::Leader => write!(f, "Leader"),
        }
    }
}

impl RegionRole {
    pub fn writable(&self) -> bool {
        matches!(self, RegionRole::Leader)
    }
}

impl From<RegionRole> for PbRegionRole {
    fn from(value: RegionRole) -> Self {
        match value {
            RegionRole::Follower => PbRegionRole::Follower,
            RegionRole::Leader => PbRegionRole::Leader,
        }
    }
}

impl From<PbRegionRole> for RegionRole {
    fn from(value: PbRegionRole) -> Self {
        match value {
            PbRegionRole::Leader => RegionRole::Leader,
            PbRegionRole::Follower => RegionRole::Follower,
        }
    }
}

#[async_trait]
pub trait RegionEngine: Send + Sync {
    /// Name of this engine
    fn name(&self) -> &str;

    /// Handles non-query request to the region. Returns the count of affected rows.
    async fn handle_request(
        &self,
        region_id: RegionId,
        request: RegionRequest,
    ) -> Result<AffectedRows, BoxedError>;

    /// Handles substrait query and return a stream of record batches
    async fn handle_query(
        &self,
        region_id: RegionId,
        request: ScanRequest,
    ) -> Result<SendableRecordBatchStream, BoxedError>;

    /// Retrieves region's metadata.
    async fn get_metadata(&self, region_id: RegionId) -> Result<RegionMetadataRef, BoxedError>;

    /// Retrieves region's disk usage.
    async fn region_disk_usage(&self, region_id: RegionId) -> Option<i64>;

    /// Stops the engine
    async fn stop(&self) -> Result<(), BoxedError>;

    /// Sets writable mode for a region.
    ///
    /// The engine checks whether the region is writable before writing to the region. Setting
    /// the region as readonly doesn't guarantee that write operations in progress will not
    /// take effect.
    fn set_writable(&self, region_id: RegionId, writable: bool) -> Result<(), BoxedError>;

    /// Sets readonly for a region gracefully.
    ///
    /// After the call returns, the engine ensures no more write operations will succeed in the region.
    async fn set_readonly_gracefully(
        &self,
        region_id: RegionId,
    ) -> Result<SetReadonlyResponse, BoxedError>;

    /// Indicates region role.
    ///
    /// Returns the `None` if the region is not found.
    fn role(&self, region_id: RegionId) -> Option<RegionRole>;

    fn as_any(&self) -> &dyn Any;
}

pub type RegionEngineRef = Arc<dyn RegionEngine>;
