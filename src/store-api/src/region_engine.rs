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
use bitflags::bitflags;
use common_error::ext::{BoxedError, PlainError};
use common_error::status_code::StatusCode;
use common_recordbatch::SendableRecordBatchStream;
use common_time::Timestamp;
use datafusion_physical_plan::{DisplayAs, DisplayFormatType};
use datatypes::schema::SchemaRef;
use futures::future::join_all;
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;

use crate::logstore::entry;
use crate::metadata::RegionMetadataRef;
use crate::region_request::{RegionOpenRequest, RegionRequest};
use crate::storage::{RegionId, ScanRequest};

bitflags! {
    #[derive(Debug, Clone, Copy)]
    pub struct WriteHint: u8 {
        const PRIMARY_KEY_ENCODED = 1;
        const SPARSE_KEY_ENCODING = 1 << 1;
    }
}

/// The settable region role state.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum SettableRegionRoleState {
    Follower,
    DowngradingLeader,
}

impl From<SettableRegionRoleState> for RegionRole {
    fn from(value: SettableRegionRoleState) -> Self {
        match value {
            SettableRegionRoleState::Follower => RegionRole::Follower,
            SettableRegionRoleState::DowngradingLeader => RegionRole::DowngradingLeader,
        }
    }
}

/// The request to set region role state.
#[derive(Debug, PartialEq, Eq)]
pub struct SetRegionRoleStateRequest {
    region_id: RegionId,
    region_role_state: SettableRegionRoleState,
}

/// The response of setting region role state.
#[derive(Debug, PartialEq, Eq)]
pub enum SetRegionRoleStateResponse {
    Success {
        /// Returns `last_entry_id` of the region if available(e.g., It's not available in file engine).
        last_entry_id: Option<entry::Id>,
    },
    NotFound,
}

impl SetRegionRoleStateResponse {
    /// Returns a [SetRegionRoleStateResponse::Success] with the `last_entry_id`.
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

/// The role of the region.
/// TODO(weny): rename it to `RegionRoleState`
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RegionRole {
    // Readonly region(mito2)
    Follower,
    // Writable region(mito2), Readonly region(file).
    Leader,
    // Leader is downgrading to follower.
    //
    // This state is used to prevent new write requests.
    DowngradingLeader,
}

impl Display for RegionRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RegionRole::Follower => write!(f, "Follower"),
            RegionRole::Leader => write!(f, "Leader"),
            RegionRole::DowngradingLeader => write!(f, "Leader(Downgrading)"),
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
            RegionRole::DowngradingLeader => PbRegionRole::DowngradingLeader,
        }
    }
}

impl From<PbRegionRole> for RegionRole {
    fn from(value: PbRegionRole) -> Self {
        match value {
            PbRegionRole::Leader => RegionRole::Leader,
            PbRegionRole::Follower => RegionRole::Follower,
            PbRegionRole::DowngradingLeader => RegionRole::DowngradingLeader,
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

/// Represents one data range within a partition
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartitionRange {
    /// Start time of time index column. Inclusive.
    pub start: Timestamp,
    /// End time of time index column. Exclusive.
    pub end: Timestamp,
    /// Number of rows in this range. Is used to balance ranges between partitions.
    pub num_rows: usize,
    /// Identifier to this range. Assigned by storage engine.
    pub identifier: usize,
}

/// Properties of the [RegionScanner].
#[derive(Debug, Default)]
pub struct ScannerProperties {
    /// A 2-dim partition ranges.
    ///
    /// The first dim vector's length represents the output partition number. The second
    /// dim is ranges within one partition.
    pub partitions: Vec<Vec<PartitionRange>>,

    /// Whether scanner is in append-only mode.
    append_mode: bool,

    /// Total rows that **may** return by scanner. This field is only read iff
    /// [ScannerProperties::append_mode] is true.
    total_rows: usize,

    /// Whether to yield an empty batch to distinguish partition ranges.
    pub distinguish_partition_range: bool,

    /// The target partitions of the scanner. 0 indicates using the number of partitions as target partitions.
    target_partitions: usize,
}

impl ScannerProperties {
    /// Sets append mode for scanner.
    pub fn with_append_mode(mut self, append_mode: bool) -> Self {
        self.append_mode = append_mode;
        self
    }

    /// Sets total rows for scanner.
    pub fn with_total_rows(mut self, total_rows: usize) -> Self {
        self.total_rows = total_rows;
        self
    }

    /// Creates a new [`ScannerProperties`] with the given partitioning.
    pub fn new(partitions: Vec<Vec<PartitionRange>>, append_mode: bool, total_rows: usize) -> Self {
        Self {
            partitions,
            append_mode,
            total_rows,
            distinguish_partition_range: false,
            target_partitions: 0,
        }
    }

    /// Updates the properties with the given [PrepareRequest].
    pub fn prepare(&mut self, request: PrepareRequest) {
        if let Some(ranges) = request.ranges {
            self.partitions = ranges;
        }
        if let Some(distinguish_partition_range) = request.distinguish_partition_range {
            self.distinguish_partition_range = distinguish_partition_range;
        }
        if let Some(target_partitions) = request.target_partitions {
            self.target_partitions = target_partitions;
        }
    }

    /// Returns the number of actual partitions.
    pub fn num_partitions(&self) -> usize {
        self.partitions.len()
    }

    pub fn append_mode(&self) -> bool {
        self.append_mode
    }

    pub fn total_rows(&self) -> usize {
        self.total_rows
    }

    /// Returns the target partitions of the scanner. If it is not set, returns the number of partitions.
    pub fn target_partitions(&self) -> usize {
        if self.target_partitions == 0 {
            self.num_partitions()
        } else {
            self.target_partitions
        }
    }
}

/// Request to override the scanner properties.
#[derive(Default)]
pub struct PrepareRequest {
    /// Assigned partition ranges.
    pub ranges: Option<Vec<Vec<PartitionRange>>>,
    /// Distringuishes partition range by empty batches.
    pub distinguish_partition_range: Option<bool>,
    /// The expected number of target partitions.
    pub target_partitions: Option<usize>,
}

impl PrepareRequest {
    /// Sets the ranges.
    pub fn with_ranges(mut self, ranges: Vec<Vec<PartitionRange>>) -> Self {
        self.ranges = Some(ranges);
        self
    }

    /// Sets the distinguish partition range flag.
    pub fn with_distinguish_partition_range(mut self, distinguish_partition_range: bool) -> Self {
        self.distinguish_partition_range = Some(distinguish_partition_range);
        self
    }

    /// Sets the target partitions.
    pub fn with_target_partitions(mut self, target_partitions: usize) -> Self {
        self.target_partitions = Some(target_partitions);
        self
    }
}

/// A scanner that provides a way to scan the region concurrently.
///
/// The scanner splits the region into partitions so that each partition can be scanned concurrently.
/// You can use this trait to implement an [`ExecutionPlan`](datafusion_physical_plan::ExecutionPlan).
pub trait RegionScanner: Debug + DisplayAs + Send {
    /// Returns the properties of the scanner.
    fn properties(&self) -> &ScannerProperties;

    /// Returns the schema of the record batches.
    fn schema(&self) -> SchemaRef;

    /// Returns the metadata of the region.
    fn metadata(&self) -> RegionMetadataRef;

    /// Prepares the scanner with the given partition ranges.
    ///
    /// This method is for the planner to adjust the scanner's behavior based on the partition ranges.
    fn prepare(&mut self, request: PrepareRequest) -> Result<(), BoxedError>;

    /// Scans the partition and returns a stream of record batches.
    ///
    /// # Panics
    /// Panics if the `partition` is out of bound.
    fn scan_partition(&self, partition: usize) -> Result<SendableRecordBatchStream, BoxedError>;

    /// Check if there is any predicate that may be executed in this scanner.
    fn has_predicate(&self) -> bool;
}

pub type RegionScannerRef = Box<dyn RegionScanner>;

pub type BatchResponses = Vec<(RegionId, Result<RegionResponse, BoxedError>)>;

/// Represents the statistics of a region.
#[derive(Debug, Deserialize, Serialize, Default)]
pub struct RegionStatistic {
    /// The number of rows
    #[serde(default)]
    pub num_rows: u64,
    /// The size of memtable in bytes.
    pub memtable_size: u64,
    /// The size of WAL in bytes.
    pub wal_size: u64,
    /// The size of manifest in bytes.
    pub manifest_size: u64,
    /// The size of SST data files in bytes.
    pub sst_size: u64,
    /// The size of SST index files in bytes.
    #[serde(default)]
    pub index_size: u64,
}

impl RegionStatistic {
    /// Deserializes the region statistic to a byte array.
    ///
    /// Returns None if the deserialization fails.
    pub fn deserialize_from_slice(value: &[u8]) -> Option<RegionStatistic> {
        serde_json::from_slice(value).ok()
    }

    /// Serializes the region statistic to a byte array.
    ///
    /// Returns None if the serialization fails.
    pub fn serialize_to_vec(&self) -> Option<Vec<u8>> {
        serde_json::to_vec(self).ok()
    }
}

impl RegionStatistic {
    /// Returns the estimated disk size of the region.
    pub fn estimated_disk_size(&self) -> u64 {
        self.wal_size + self.sst_size + self.manifest_size + self.index_size
    }
}

#[async_trait]
pub trait RegionEngine: Send + Sync {
    /// Name of this engine
    fn name(&self) -> &str;

    /// Handles batch open region requests.
    async fn handle_batch_open_requests(
        &self,
        parallelism: usize,
        requests: Vec<(RegionId, RegionOpenRequest)>,
    ) -> Result<BatchResponses, BoxedError> {
        let semaphore = Arc::new(Semaphore::new(parallelism));
        let mut tasks = Vec::with_capacity(requests.len());

        for (region_id, request) in requests {
            let semaphore_moved = semaphore.clone();

            tasks.push(async move {
                // Safety: semaphore must exist
                let _permit = semaphore_moved.acquire().await.unwrap();
                let result = self
                    .handle_request(region_id, RegionRequest::Open(request))
                    .await;
                (region_id, result)
            });
        }

        Ok(join_all(tasks).await)
    }

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

    /// Retrieves region's statistic.
    fn region_statistic(&self, region_id: RegionId) -> Option<RegionStatistic>;

    /// Stops the engine
    async fn stop(&self) -> Result<(), BoxedError>;

    /// Sets [RegionRole] for a region.
    ///
    /// The engine checks whether the region is writable before writing to the region. Setting
    /// the region as readonly doesn't guarantee that write operations in progress will not
    /// take effect.
    fn set_region_role(&self, region_id: RegionId, role: RegionRole) -> Result<(), BoxedError>;

    /// Sets region role state gracefully.
    ///
    /// After the call returns, the engine ensures no more write operations will succeed in the region.
    async fn set_region_role_state_gracefully(
        &self,
        region_id: RegionId,
        region_role_state: SettableRegionRoleState,
    ) -> Result<SetRegionRoleStateResponse, BoxedError>;

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
    metadata: RegionMetadataRef,
}

impl SinglePartitionScanner {
    /// Creates a new [SinglePartitionScanner] with the given stream and metadata.
    pub fn new(
        stream: SendableRecordBatchStream,
        append_mode: bool,
        metadata: RegionMetadataRef,
    ) -> Self {
        let schema = stream.schema();
        Self {
            stream: Mutex::new(Some(stream)),
            schema,
            properties: ScannerProperties::default().with_append_mode(append_mode),
            metadata,
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

    fn prepare(&mut self, request: PrepareRequest) -> Result<(), BoxedError> {
        self.properties.prepare(request);
        Ok(())
    }

    fn scan_partition(&self, _partition: usize) -> Result<SendableRecordBatchStream, BoxedError> {
        let mut stream = self.stream.lock().unwrap();
        stream.take().ok_or_else(|| {
            BoxedError::new(PlainError::new(
                "Not expected to run ExecutionPlan more than once".to_string(),
                StatusCode::Unexpected,
            ))
        })
    }

    fn has_predicate(&self) -> bool {
        false
    }

    fn metadata(&self) -> RegionMetadataRef {
        self.metadata.clone()
    }
}

impl DisplayAs for SinglePartitionScanner {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}
