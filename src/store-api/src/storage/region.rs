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

//! Region holds chunks of rows stored in the storage engine, but does not require that
//! rows must have continuous primary key range, which is implementation specific.
//!
//! Regions support operations like PUT/DELETE/SCAN that most key-value stores provide.
//! However, unlike key-value store, data stored in region has data model like:
//!
//! ```text
//! colk-1, ..., colk-m, timestamp, version -> colv-1, ..., colv-n
//! ```
//!
//! The data model require each row
//! - has 0 ~ m key column, parts of row key columns;
//! - **MUST** has a timestamp column, part of row key columns;
//! - has a version column, part of row key columns;
//! - has 0 ~ n value column.
//!
//! Each row is identified by (value of key columns, timestamp, version), which forms
//! a row key. Note that the implementation may allow multiple rows have same row
//! key (like ClickHouse), which is useful in analytic scenario.

use async_trait::async_trait;
use common_error::ext::ErrorExt;

use crate::storage::engine::OpenOptions;
use crate::storage::requests::{AlterRequest, WriteRequest};
use crate::storage::responses::WriteResponse;
use crate::storage::RegionId;

/// Chunks of rows in storage engine.
#[async_trait]
pub trait Region: Send + Sync + Clone + std::fmt::Debug + 'static {
    type Error: ErrorExt + Send + Sync;
    type WriteRequest: WriteRequest;

    fn id(&self) -> RegionId;

    /// Returns name of the region.
    fn name(&self) -> &str;

    /// Write updates to region.
    async fn write(
        &self,
        ctx: &WriteContext,
        request: Self::WriteRequest,
    ) -> Result<WriteResponse, Self::Error>;

    /// Create write request
    fn write_request(&self) -> Self::WriteRequest;

    async fn alter(&self, request: AlterRequest) -> Result<(), Self::Error>;

    async fn drop_region(&self) -> Result<(), Self::Error>;

    fn disk_usage_bytes(&self) -> u64;

    fn region_stat(&self) -> RegionStat {
        RegionStat {
            region_id: self.id().into(),
            disk_usage_bytes: self.disk_usage_bytes(),
        }
    }

    /// Flush memtable of the region to disk.
    async fn flush(&self, ctx: &FlushContext) -> Result<(), Self::Error>;

    async fn compact(&self, ctx: &CompactContext) -> Result<(), Self::Error>;

    async fn truncate(&self) -> Result<(), Self::Error>;
}

#[derive(Default, Debug)]
pub struct RegionStat {
    pub region_id: u64,
    pub disk_usage_bytes: u64,
}

/// Context for write operations.
#[derive(Debug, Clone, Default)]
pub struct WriteContext {}

impl From<&OpenOptions> for WriteContext {
    fn from(_opts: &OpenOptions) -> WriteContext {
        WriteContext::default()
    }
}

#[derive(Debug, Clone, Default)]
pub struct CloseContext {
    /// If true, flush the closing region.
    pub flush: bool,
}

/// Context for flush operations.
#[derive(Debug, Clone)]
pub struct FlushContext {
    /// If true, the flush will wait until the flush is done.
    /// Default: true
    pub wait: bool,
    /// Flush reason.
    pub reason: FlushReason,
    /// If true, allows to flush a closed region
    pub force: bool,
}

impl Default for FlushContext {
    fn default() -> FlushContext {
        FlushContext {
            wait: true,
            reason: FlushReason::Others,
            force: false,
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub struct CompactContext {
    /// Whether to wait the compaction result.
    pub wait: bool,
}

impl Default for CompactContext {
    fn default() -> CompactContext {
        CompactContext { wait: true }
    }
}

/// Reason of flush operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushReason {
    /// Other reasons.
    Others,
    /// Memtable is full.
    MemtableFull,
    /// Flush manually.
    Manually,
    /// Auto flush periodically.
    Periodically,
    /// Global write buffer is full.
    GlobalBufferFull,
}

impl FlushReason {
    /// Returns reason as `str`.
    pub fn as_str(&self) -> &'static str {
        match self {
            FlushReason::Others => "others",
            FlushReason::MemtableFull => "memtable_full",
            FlushReason::Manually => "manually",
            FlushReason::Periodically => "periodically",
            FlushReason::GlobalBufferFull => "global_buffer_full",
        }
    }
}
