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
use std::fmt::{Debug, Display};
use std::sync::{Arc, Mutex};

use api::greptime_proto::v1::meta::{GrantedRegion as PbGrantedRegion, RegionRole as PbRegionRole};
use api::region::RegionResponse;
use async_trait::async_trait;
use common_error::ext::BoxedError;
use common_query::error::ExecuteRepeatedlySnafu;
use common_recordbatch::SendableRecordBatchStream;
use datafusion_physical_plan::{DisplayAs, DisplayFormatType};
use datatypes::schema::SchemaRef;
use serde::{Deserialize, Serialize};
use snafu::OptionExt;

use crate::logstore::entry;
use crate::metadata::RegionMetadataRef;
use crate::region_request::RegionRequest;
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

/// Output partition properties of the [RegionScanner].
#[derive(Debug)]
pub enum ScannerPartitioning {
    /// Unknown partitioning scheme with a known number of partitions
    Unknown(usize),
}

impl ScannerPartitioning {
    /// Returns the number of partitions.
    pub fn num_partitions(&self) -> usize {
        match self {
            ScannerPartitioning::Unknown(num_partitions) => *num_partitions,
        }
    }
}

/// Properties of the [RegionScanner].
#[derive(Debug)]
pub struct ScannerProperties {
    /// Partitions to scan.
    partitioning: ScannerPartitioning,
}

impl ScannerProperties {
    /// Creates a new [ScannerProperties] with the given partitioning.
    pub fn new(partitioning: ScannerPartitioning) -> Self {
        Self { partitioning }
    }

    /// Returns properties of partitions to scan.
    pub fn partitioning(&self) -> &ScannerPartitioning {
        &self.partitioning
    }
}

/// A scanner that provides a way to scan the region concurrently.
/// The scanner splits the region into partitions so that each partition can be scanned concurrently.
/// You can use this trait to implement an [ExecutionPlan](datafusion_physical_plan::ExecutionPlan).
pub trait RegionScanner: Debug + DisplayAs + Send + Sync {
    /// Returns the properties of the scanner.
    fn properties(&self) -> &ScannerProperties;

    /// Returns the schema of the record batches.
    fn schema(&self) -> SchemaRef;

    /// Scans the partition and returns a stream of record batches.
    fn scan_partition(&self, partition: usize) -> Result<SendableRecordBatchStream, BoxedError>;
}

pub type RegionScannerRef = Arc<dyn RegionScanner>;

#[async_trait]
pub trait RegionEngine: Send + Sync {
    /// Name of this engine
    fn name(&self) -> &str;

    /// Handles non-query request to the region. Returns the count of affected rows.
    async fn handle_request(
        &self,
        region_id: RegionId,
        request: RegionRequest,
    ) -> Result<RegionResponse, BoxedError>;

    /// Handles query and return a scanner that can be used to scan the region concurrently.
    async fn handle_query(
        &self,
        region_id: RegionId,
        request: ScanRequest,
    ) -> Result<RegionScannerRef, BoxedError>;

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

/// A [RegionScanner] that only scans a single partition.
pub struct SinglePartitionScanner {
    stream: Mutex<Option<SendableRecordBatchStream>>,
    schema: SchemaRef,
    properties: ScannerProperties,
}

impl SinglePartitionScanner {
    /// Creates a new [SinglePartitionScanner] with the given stream.
    pub fn new(stream: SendableRecordBatchStream) -> Self {
        let schema = stream.schema();
        Self {
            stream: Mutex::new(Some(stream)),
            schema,
            properties: ScannerProperties::new(ScannerPartitioning::Unknown(1)),
        }
    }
}

impl Debug for SinglePartitionScanner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SinglePartitionScanner: <SendableRecordBatchStream>")
    }
}

impl RegionScanner for SinglePartitionScanner {
    fn properties(&self) -> &ScannerProperties {
        &self.properties
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn scan_partition(&self, _partition: usize) -> Result<SendableRecordBatchStream, BoxedError> {
        let mut stream = self.stream.lock().unwrap();
        stream
            .take()
            .context(ExecuteRepeatedlySnafu)
            .map_err(BoxedError::new)
    }
}

impl DisplayAs for SinglePartitionScanner {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}
