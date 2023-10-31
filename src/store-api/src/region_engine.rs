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

use std::sync::Arc;

use async_trait::async_trait;
use common_error::ext::BoxedError;
use common_query::Output;
use common_recordbatch::SendableRecordBatchStream;

use crate::metadata::RegionMetadataRef;
use crate::region_request::RegionRequest;
use crate::storage::{RegionId, ScanRequest};

#[async_trait]
pub trait RegionEngine: Send + Sync {
    /// Name of this engine
    fn name(&self) -> &str;

    /// Handles request to the region.
    ///
    /// Only query is not included, which is handled in `handle_query`
    async fn handle_request(
        &self,
        region_id: RegionId,
        request: RegionRequest,
    ) -> Result<Output, BoxedError>;

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
}

pub type RegionEngineRef = Arc<dyn RegionEngine>;
