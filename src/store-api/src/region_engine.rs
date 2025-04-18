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
use std::collections::HashMap;
use std::fmt::{Debug, Display};
use std::sync::{Arc, Mutex};

use api::greptime_proto::v1::meta::{GrantedRegion as PbGrantedRegion, RegionRole as PbRegionRole};
use api::region::RegionResponse;
use async_trait::async_trait;
use common_error::ext::{BoxedError, PlainError};
use common_error::status_code::StatusCode;
use common_recordbatch::SendableRecordBatchStream;
use common_time::Timestamp;
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion_physical_plan::{DisplayAs, DisplayFormatType};
use datatypes::schema::SchemaRef;
use futures::future::join_all;
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;

use crate::logstore::entry;
use crate::metadata::RegionMetadataRef;
use crate::region_request::{
    BatchRegionDdlRequest, RegionOpenRequest, RegionRequest, RegionSequencesRequest,
};
use crate::storage::{RegionId, ScanRequest, SequenceNumber};

/// The settable region role state.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum SettableRegionRoleState {
    Follower,
    DowngradingLeader,
}

impl Display for SettableRegionRoleState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SettableRegionRoleState::Follower => write!(f, "Follower"),
            SettableRegionRoleState::DowngradingLeader => write!(f, "Leader(Downgrading)"),
        }
    }
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

/// The success response of setting region role state.
#[derive(Debug, PartialEq, Eq)]
pub enum SetRegionRoleStateSuccess {
    File,
    Mito {
        last_entry_id: entry::Id,
    },
    Metric {
        last_entry_id: entry::Id,
        metadata_last_entry_id: entry::Id,
    },
}

impl SetRegionRoleStateSuccess {
    /// Returns a [SetRegionRoleStateSuccess::File].
    pub fn file() -> Self {
        Self::File
    }

    /// Returns a [SetRegionRoleStateSuccess::Mito] with the `last_entry_id`.
    pub fn mito(last_entry_id: entry::Id) -> Self {
        SetRegionRoleStateSuccess::Mito { last_entry_id }
    }

    /// Returns a [SetRegionRoleStateSuccess::Metric] with the `last_entry_id` and `metadata_last_entry_id`.
    pub fn metric(last_entry_id: entry::Id, metadata_last_entry_id: entry::Id) -> Self {
        SetRegionRoleStateSuccess::Metric {
            last_entry_id,
            metadata_last_entry_id,
        }
    }
}

impl SetRegionRoleStateSuccess {
    /// Returns the last entry id of the region.
    pub fn last_entry_id(&self) -> Option<entry::Id> {
        match self {
            SetRegionRoleStateSuccess::File => None,
            SetRegionRoleStateSuccess::Mito { last_entry_id } => Some(*last_entry_id),
            SetRegionRoleStateSuccess::Metric { last_entry_id, .. } => Some(*last_entry_id),
        }
    }

    /// Returns the last entry id of the metadata of the region.
    pub fn metadata_last_entry_id(&self) -> Option<entry::Id> {
        match self {
            SetRegionRoleStateSuccess::File => None,
            SetRegionRoleStateSuccess::Mito { .. } => None,
            SetRegionRoleStateSuccess::Metric {
                metadata_last_entry_id,
                ..
            } => Some(*metadata_last_entry_id),
        }
    }
}

/// The response of setting region role state.
#[derive(Debug, PartialEq, Eq)]
pub enum SetRegionRoleStateResponse {
    Success(SetRegionRoleStateSuccess),
    NotFound,
}

impl SetRegionRoleStateResponse {
    /// Returns a [SetRegionRoleStateResponse::Success] with the `File` success.
    pub fn success(success: SetRegionRoleStateSuccess) -> Self {
        Self::Success(success)
    }

    /// Returns true if the response is a [SetRegionRoleStateResponse::NotFound].
    pub fn is_not_found(&self) -> bool {
        matches!(self, SetRegionRoleStateResponse::NotFound)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrantedRegion {
    pub region_id: RegionId,
    pub region_role: RegionRole,
    pub extensions: HashMap<String, Vec<u8>>,
}

impl GrantedRegion {
    pub fn new(region_id: RegionId, region_role: RegionRole) -> Self {
        Self {
            region_id,
            region_role,
            extensions: HashMap::new(),
        }
    }
}

impl From<GrantedRegion> for PbGrantedRegion {
    fn from(value: GrantedRegion) -> Self {
        PbGrantedRegion {
            region_id: value.region_id.as_u64(),
            role: PbRegionRole::from(value.region_role).into(),
            extensions: value.extensions,
        }
    }
}

impl From<PbGrantedRegion> for GrantedRegion {
    fn from(value: PbGrantedRegion) -> Self {
        GrantedRegion {
            region_id: RegionId::from_u64(value.region_id),
            region_role: value.role().into(),
            extensions: value.extensions,
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

    /// Whether the scanner is scanning a logical region.
    logical_region: bool,
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
            logical_region: false,
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

    /// Returns whether the scanner is scanning a logical region.
    pub fn is_logical_region(&self) -> bool {
        self.logical_region
    }

    /// Returns the target partitions of the scanner. If it is not set, returns the number of partitions.
    pub fn target_partitions(&self) -> usize {
        if self.target_partitions == 0 {
            self.num_partitions()
        } else {
            self.target_partitions
        }
    }

    /// Sets whether the scanner is reading a logical region.
    pub fn set_logical_region(&mut self, logical_region: bool) {
        self.logical_region = logical_region;
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
    fn scan_partition(
        &self,
        metrics_set: &ExecutionPlanMetricsSet,
        partition: usize,
    ) -> Result<SendableRecordBatchStream, BoxedError>;

    /// Check if there is any predicate that may be executed in this scanner.
    fn has_predicate(&self) -> bool;

    /// Sets whether the scanner is reading a logical region.
    fn set_logical_region(&mut self, logical_region: bool);
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
    /// The details of the region.
    #[serde(default)]
    pub manifest: RegionManifestInfo,
    /// The latest entry id of the region's remote WAL since last flush.
    /// For metric engine, there're two latest entry ids, one for data and one for metadata.
    /// TODO(weny): remove this two fields and use single instead.
    #[serde(default)]
    pub data_topic_latest_entry_id: u64,
    #[serde(default)]
    pub metadata_topic_latest_entry_id: u64,
}

/// The manifest info of a region.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RegionManifestInfo {
    Mito {
        manifest_version: u64,
        flushed_entry_id: u64,
    },
    Metric {
        data_manifest_version: u64,
        data_flushed_entry_id: u64,
        metadata_manifest_version: u64,
        metadata_flushed_entry_id: u64,
    },
}

impl RegionManifestInfo {
    /// Creates a new [RegionManifestInfo] for mito2 engine.
    pub fn mito(manifest_version: u64, flushed_entry_id: u64) -> Self {
        Self::Mito {
            manifest_version,
            flushed_entry_id,
        }
    }

    /// Creates a new [RegionManifestInfo] for metric engine.
    pub fn metric(
        data_manifest_version: u64,
        data_flushed_entry_id: u64,
        metadata_manifest_version: u64,
        metadata_flushed_entry_id: u64,
    ) -> Self {
        Self::Metric {
            data_manifest_version,
            data_flushed_entry_id,
            metadata_manifest_version,
            metadata_flushed_entry_id,
        }
    }

    /// Returns true if the region is a mito2 region.
    pub fn is_mito(&self) -> bool {
        matches!(self, RegionManifestInfo::Mito { .. })
    }

    /// Returns true if the region is a metric region.
    pub fn is_metric(&self) -> bool {
        matches!(self, RegionManifestInfo::Metric { .. })
    }

    /// Returns the flushed entry id of the data region.
    pub fn data_flushed_entry_id(&self) -> u64 {
        match self {
            RegionManifestInfo::Mito {
                flushed_entry_id, ..
            } => *flushed_entry_id,
            RegionManifestInfo::Metric {
                data_flushed_entry_id,
                ..
            } => *data_flushed_entry_id,
        }
    }

    /// Returns the manifest version of the data region.
    pub fn data_manifest_version(&self) -> u64 {
        match self {
            RegionManifestInfo::Mito {
                manifest_version, ..
            } => *manifest_version,
            RegionManifestInfo::Metric {
                data_manifest_version,
                ..
            } => *data_manifest_version,
        }
    }

    /// Returns the manifest version of the metadata region.
    pub fn metadata_manifest_version(&self) -> Option<u64> {
        match self {
            RegionManifestInfo::Mito { .. } => None,
            RegionManifestInfo::Metric {
                metadata_manifest_version,
                ..
            } => Some(*metadata_manifest_version),
        }
    }

    /// Returns the flushed entry id of the metadata region.
    pub fn metadata_flushed_entry_id(&self) -> Option<u64> {
        match self {
            RegionManifestInfo::Mito { .. } => None,
            RegionManifestInfo::Metric {
                metadata_flushed_entry_id,
                ..
            } => Some(*metadata_flushed_entry_id),
        }
    }

    /// Encodes a list of ([RegionId], [RegionManifestInfo]) to a byte array.
    pub fn encode_list(manifest_infos: &[(RegionId, Self)]) -> serde_json::Result<Vec<u8>> {
        serde_json::to_vec(manifest_infos)
    }

    /// Decodes a list of ([RegionId], [RegionManifestInfo]) from a byte array.
    pub fn decode_list(value: &[u8]) -> serde_json::Result<Vec<(RegionId, Self)>> {
        serde_json::from_slice(value)
    }
}

impl Default for RegionManifestInfo {
    fn default() -> Self {
        Self::Mito {
            manifest_version: 0,
            flushed_entry_id: 0,
        }
    }
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

/// The response of syncing the manifest.
#[derive(Debug)]
pub enum SyncManifestResponse {
    NotSupported,
    Mito {
        /// Indicates if the data region was synced.
        synced: bool,
    },
    Metric {
        /// Indicates if the metadata region was synced.
        metadata_synced: bool,
        /// Indicates if the data region was synced.
        data_synced: bool,
        /// The logical regions that were newly opened during the sync operation.
        /// This only occurs after the metadata region has been successfully synced.
        new_opened_logical_region_ids: Vec<RegionId>,
    },
}

impl SyncManifestResponse {
    /// Returns true if data region is synced.
    pub fn is_data_synced(&self) -> bool {
        match self {
            SyncManifestResponse::NotSupported => false,
            SyncManifestResponse::Mito { synced } => *synced,
            SyncManifestResponse::Metric { data_synced, .. } => *data_synced,
        }
    }

    /// Returns true if the engine is supported the sync operation.
    pub fn is_supported(&self) -> bool {
        matches!(self, SyncManifestResponse::NotSupported)
    }

    /// Returns true if the engine is a mito2 engine.
    pub fn is_mito(&self) -> bool {
        matches!(self, SyncManifestResponse::Mito { .. })
    }

    /// Returns true if the engine is a metric engine.
    pub fn is_metric(&self) -> bool {
        matches!(self, SyncManifestResponse::Metric { .. })
    }

    /// Returns the new opened logical region ids.
    pub fn new_opened_logical_region_ids(self) -> Option<Vec<RegionId>> {
        match self {
            SyncManifestResponse::Metric {
                new_opened_logical_region_ids,
                ..
            } => Some(new_opened_logical_region_ids),
            _ => None,
        }
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

    async fn handle_batch_ddl_requests(
        &self,
        request: BatchRegionDdlRequest,
    ) -> Result<RegionResponse, BoxedError> {
        let requests = request.into_region_requests();

        let mut affected_rows = 0;
        let mut extensions = HashMap::new();

        for (region_id, request) in requests {
            let result = self.handle_request(region_id, request).await?;
            affected_rows += result.affected_rows;
            extensions.extend(result.extensions);
        }

        Ok(RegionResponse {
            affected_rows,
            extensions,
        })
    }

    /// Handles non-query request to the region. Returns the count of affected rows.
    async fn handle_request(
        &self,
        region_id: RegionId,
        request: RegionRequest,
    ) -> Result<RegionResponse, BoxedError>;

    /// Returns the last sequence number of the region.
    async fn get_last_seq_num(
        &self,
        region_id: RegionId,
    ) -> Result<Option<SequenceNumber>, BoxedError>;

    async fn get_region_sequences(
        &self,
        seqs: RegionSequencesRequest,
    ) -> Result<HashMap<u64, u64>, BoxedError> {
        let mut results = HashMap::with_capacity(seqs.region_ids.len());

        for region_id in seqs.region_ids {
            let seq = self.get_last_seq_num(region_id).await?.unwrap_or_default();
            results.insert(region_id.as_u64(), seq);
        }

        Ok(results)
    }

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

    /// Syncs the region manifest to the given manifest version.
    async fn sync_region(
        &self,
        region_id: RegionId,
        manifest_info: RegionManifestInfo,
    ) -> Result<SyncManifestResponse, BoxedError>;

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

    fn scan_partition(
        &self,
        _metrics_set: &ExecutionPlanMetricsSet,
        _partition: usize,
    ) -> Result<SendableRecordBatchStream, BoxedError> {
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

    fn set_logical_region(&mut self, logical_region: bool) {
        self.properties.set_logical_region(logical_region);
    }
}

impl DisplayAs for SinglePartitionScanner {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}
