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

use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use store_api::storage::{RegionId, RegionNumber};
use strum::Display;
use table::metadata::TableId;
use table::table_name::TableName;

use crate::flow_name::FlowName;
use crate::key::schema_name::SchemaName;
use crate::key::{FlowId, FlowPartitionId};
use crate::peer::Peer;
use crate::{DatanodeId, FlownodeId};

#[derive(Eq, Hash, PartialEq, Clone, Debug, Serialize, Deserialize)]
pub struct RegionIdent {
    pub datanode_id: DatanodeId,
    pub table_id: TableId,
    pub region_number: RegionNumber,
    pub engine: String,
}

impl RegionIdent {
    pub fn get_region_id(&self) -> RegionId {
        RegionId::new(self.table_id, self.region_number)
    }
}

impl Display for RegionIdent {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RegionIdent(datanode_id='{}', table_id={}, region_number={}, engine = {})",
            self.datanode_id, self.table_id, self.region_number, self.engine
        )
    }
}

/// The result of downgrade leader region.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct DowngradeRegionReply {
    /// Returns the `last_entry_id` if available.
    pub last_entry_id: Option<u64>,
    /// Returns the `metadata_last_entry_id` if available (Only available for metric engine).
    pub metadata_last_entry_id: Option<u64>,
    /// Indicates whether the region exists.
    pub exists: bool,
    /// Return error if any during the operation.
    pub error: Option<String>,
}

impl Display for DowngradeRegionReply {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "(last_entry_id={:?}, exists={}, error={:?})",
            self.last_entry_id, self.exists, self.error
        )
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct SimpleReply {
    pub result: bool,
    pub error: Option<String>,
}

/// Reply for flush region operations with support for batch results.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct FlushRegionReply {
    /// Results for each region that was attempted to be flushed.
    /// For single region flushes, this will contain one result.
    /// For batch flushes, this contains results for all attempted regions.
    pub results: Vec<(RegionId, Result<(), String>)>,
    /// Overall success: true if all regions were flushed successfully.
    pub overall_success: bool,
}

impl FlushRegionReply {
    /// Create a successful single region reply.
    pub fn success_single(region_id: RegionId) -> Self {
        Self {
            results: vec![(region_id, Ok(()))],
            overall_success: true,
        }
    }

    /// Create a failed single region reply.
    pub fn error_single(region_id: RegionId, error: String) -> Self {
        Self {
            results: vec![(region_id, Err(error))],
            overall_success: false,
        }
    }

    /// Create a batch reply from individual results.
    pub fn from_results(results: Vec<(RegionId, Result<(), String>)>) -> Self {
        let overall_success = results.iter().all(|(_, result)| result.is_ok());
        Self {
            results,
            overall_success,
        }
    }

    /// Convert to SimpleReply for backward compatibility.
    pub fn to_simple_reply(&self) -> SimpleReply {
        if self.overall_success {
            SimpleReply {
                result: true,
                error: None,
            }
        } else {
            let errors: Vec<String> = self
                .results
                .iter()
                .filter_map(|(region_id, result)| {
                    result
                        .as_ref()
                        .err()
                        .map(|err| format!("{}: {}", region_id, err))
                })
                .collect();
            SimpleReply {
                result: false,
                error: Some(errors.join("; ")),
            }
        }
    }
}

impl Display for SimpleReply {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "(result={}, error={:?})", self.result, self.error)
    }
}

impl Display for FlushRegionReply {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let results_str = self
            .results
            .iter()
            .map(|(region_id, result)| match result {
                Ok(()) => format!("{}:OK", region_id),
                Err(err) => format!("{}:ERR({})", region_id, err),
            })
            .collect::<Vec<_>>()
            .join(", ");
        write!(
            f,
            "(overall_success={}, results=[{}])",
            self.overall_success, results_str
        )
    }
}

impl Display for OpenRegion {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "OpenRegion(region_ident={}, region_storage_path={})",
            self.region_ident, self.region_storage_path
        )
    }
}

#[serde_with::serde_as]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OpenRegion {
    pub region_ident: RegionIdent,
    pub region_storage_path: String,
    pub region_options: HashMap<String, String>,
    #[serde(default)]
    #[serde_as(as = "HashMap<serde_with::DisplayFromStr, _>")]
    pub region_wal_options: HashMap<RegionNumber, String>,
    #[serde(default)]
    pub skip_wal_replay: bool,
}

impl OpenRegion {
    pub fn new(
        region_ident: RegionIdent,
        path: &str,
        region_options: HashMap<String, String>,
        region_wal_options: HashMap<RegionNumber, String>,
        skip_wal_replay: bool,
    ) -> Self {
        Self {
            region_ident,
            region_storage_path: path.to_string(),
            region_options,
            region_wal_options,
            skip_wal_replay,
        }
    }
}

/// The instruction of downgrading leader region.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DowngradeRegion {
    /// The [RegionId].
    pub region_id: RegionId,
    /// The timeout of waiting for flush the region.
    ///
    /// `None` stands for don't flush before downgrading the region.
    #[serde(default)]
    pub flush_timeout: Option<Duration>,
}

impl Display for DowngradeRegion {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "DowngradeRegion(region_id={}, flush_timeout={:?})",
            self.region_id, self.flush_timeout,
        )
    }
}

/// Upgrades a follower region to leader region.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct UpgradeRegion {
    /// The [RegionId].
    pub region_id: RegionId,
    /// The `last_entry_id` of old leader region.
    pub last_entry_id: Option<u64>,
    /// The `last_entry_id` of old leader metadata region (Only used for metric engine).
    pub metadata_last_entry_id: Option<u64>,
    /// The timeout of waiting for a wal replay.
    ///
    /// `None` stands for no wait,
    /// it's helpful to verify whether the leader region is ready.
    #[serde(with = "humantime_serde")]
    pub replay_timeout: Option<Duration>,
    /// The hint for replaying memtable.
    #[serde(default)]
    pub location_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replay_entry_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata_replay_entry_id: Option<u64>,
}

impl UpgradeRegion {
    /// Sets the replay entry id.
    pub fn with_replay_entry_id(mut self, replay_entry_id: Option<u64>) -> Self {
        self.replay_entry_id = replay_entry_id;
        self
    }

    /// Sets the metadata replay entry id.
    pub fn with_metadata_replay_entry_id(mut self, metadata_replay_entry_id: Option<u64>) -> Self {
        self.metadata_replay_entry_id = metadata_replay_entry_id;
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
/// The identifier of cache.
pub enum CacheIdent {
    FlowId(FlowId),
    /// Indicate change of address of flownode.
    FlowNodeAddressChange(u64),
    FlowName(FlowName),
    TableId(TableId),
    TableName(TableName),
    SchemaName(SchemaName),
    CreateFlow(CreateFlow),
    DropFlow(DropFlow),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CreateFlow {
    /// The unique identifier for the flow.
    pub flow_id: FlowId,
    pub source_table_ids: Vec<TableId>,
    /// Mapping of flow partition to peer information
    pub partition_to_peer_mapping: Vec<(FlowPartitionId, Peer)>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DropFlow {
    pub flow_id: FlowId,
    pub source_table_ids: Vec<TableId>,
    /// Mapping of flow partition to flownode id
    pub flow_part2node_id: Vec<(FlowPartitionId, FlownodeId)>,
}

/// Strategy for executing flush operations.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub enum FlushStrategy {
    /// Synchronous operation that waits for completion and expects a reply
    #[default]
    Sync,
    /// Asynchronous hint operation (fire-and-forget, no reply expected)
    Async,
}

/// Error handling strategy for batch flush operations.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub enum FlushErrorStrategy {
    /// Abort on first error (fail-fast)
    #[default]
    FailFast,
    /// Attempt to flush all regions and collect all errors
    TryAll,
}

/// Unified flush instruction supporting both single and batch operations
/// with configurable execution strategies and error handling.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FlushRegions {
    /// List of region IDs to flush. Can contain a single region or multiple regions.
    pub region_ids: Vec<RegionId>,
    /// Execution strategy: Sync (expects reply) or Async (fire-and-forget hint).
    #[serde(default)]
    pub strategy: FlushStrategy,
    /// Error handling strategy for batch operations (only applies when multiple regions and sync strategy).
    #[serde(default)]
    pub error_strategy: FlushErrorStrategy,
}

impl FlushRegions {
    /// Create synchronous single-region flush
    pub fn sync_single(region_id: RegionId) -> Self {
        Self {
            region_ids: vec![region_id],
            strategy: FlushStrategy::Sync,
            error_strategy: FlushErrorStrategy::FailFast,
        }
    }

    /// Create asynchronous batch flush (fire-and-forget)
    pub fn async_batch(region_ids: Vec<RegionId>) -> Self {
        Self {
            region_ids,
            strategy: FlushStrategy::Async,
            error_strategy: FlushErrorStrategy::TryAll,
        }
    }

    /// Create synchronous batch flush with error strategy
    pub fn sync_batch(region_ids: Vec<RegionId>, error_strategy: FlushErrorStrategy) -> Self {
        Self {
            region_ids,
            strategy: FlushStrategy::Sync,
            error_strategy,
        }
    }

    /// Check if this is a single region flush.
    pub fn is_single_region(&self) -> bool {
        self.region_ids.len() == 1
    }

    /// Get the single region ID if this is a single region flush.
    pub fn single_region_id(&self) -> Option<RegionId> {
        if self.is_single_region() {
            self.region_ids.first().copied()
        } else {
            None
        }
    }

    /// Check if this is a hint (asynchronous) operation.
    pub fn is_hint(&self) -> bool {
        matches!(self.strategy, FlushStrategy::Async)
    }

    /// Check if this is a synchronous operation.
    pub fn is_sync(&self) -> bool {
        matches!(self.strategy, FlushStrategy::Sync)
    }
}

impl From<RegionId> for FlushRegions {
    fn from(region_id: RegionId) -> Self {
        Self::sync_single(region_id)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Display, PartialEq)]
pub enum Instruction {
    /// Opens a region.
    ///
    /// - Returns true if a specified region exists.
    OpenRegion(OpenRegion),
    /// Closes a region.
    ///
    /// - Returns true if a specified region does not exist.
    CloseRegion(RegionIdent),
    /// Upgrades a region.
    UpgradeRegion(UpgradeRegion),
    /// Downgrades a region.
    DowngradeRegion(DowngradeRegion),
    /// Invalidates batch cache.
    InvalidateCaches(Vec<CacheIdent>),
    /// Flushes regions.
    FlushRegions(FlushRegions),
}

/// The reply of [UpgradeRegion].
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct UpgradeRegionReply {
    /// Returns true if `last_entry_id` has been replayed to the latest.
    pub ready: bool,
    /// Indicates whether the region exists.
    pub exists: bool,
    /// Returns error if any.
    pub error: Option<String>,
}

impl Display for UpgradeRegionReply {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "(ready={}, exists={}, error={:?})",
            self.ready, self.exists, self.error
        )
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum InstructionReply {
    OpenRegion(SimpleReply),
    CloseRegion(SimpleReply),
    UpgradeRegion(UpgradeRegionReply),
    DowngradeRegion(DowngradeRegionReply),
    FlushRegions(FlushRegionReply),
}

impl Display for InstructionReply {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OpenRegion(reply) => write!(f, "InstructionReply::OpenRegion({})", reply),
            Self::CloseRegion(reply) => write!(f, "InstructionReply::CloseRegion({})", reply),
            Self::UpgradeRegion(reply) => write!(f, "InstructionReply::UpgradeRegion({})", reply),
            Self::DowngradeRegion(reply) => {
                write!(f, "InstructionReply::DowngradeRegion({})", reply)
            }
            Self::FlushRegions(reply) => write!(f, "InstructionReply::FlushRegions({})", reply),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialize_instruction() {
        let open_region = Instruction::OpenRegion(OpenRegion::new(
            RegionIdent {
                datanode_id: 2,
                table_id: 1024,
                region_number: 1,
                engine: "mito2".to_string(),
            },
            "test/foo",
            HashMap::new(),
            HashMap::new(),
            false,
        ));

        let serialized = serde_json::to_string(&open_region).unwrap();

        assert_eq!(
            r#"{"OpenRegion":{"region_ident":{"datanode_id":2,"table_id":1024,"region_number":1,"engine":"mito2"},"region_storage_path":"test/foo","region_options":{},"region_wal_options":{},"skip_wal_replay":false}}"#,
            serialized
        );

        let close_region = Instruction::CloseRegion(RegionIdent {
            datanode_id: 2,
            table_id: 1024,
            region_number: 1,
            engine: "mito2".to_string(),
        });

        let serialized = serde_json::to_string(&close_region).unwrap();

        assert_eq!(
            r#"{"CloseRegion":{"datanode_id":2,"table_id":1024,"region_number":1,"engine":"mito2"}}"#,
            serialized
        );
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct LegacyOpenRegion {
        region_ident: RegionIdent,
        region_storage_path: String,
        region_options: HashMap<String, String>,
    }

    #[test]
    fn test_compatible_serialize_open_region() {
        let region_ident = RegionIdent {
            datanode_id: 2,
            table_id: 1024,
            region_number: 1,
            engine: "mito2".to_string(),
        };
        let region_storage_path = "test/foo".to_string();
        let region_options = HashMap::from([
            ("a".to_string(), "aa".to_string()),
            ("b".to_string(), "bb".to_string()),
        ]);

        // Serialize a legacy OpenRegion.
        let legacy_open_region = LegacyOpenRegion {
            region_ident: region_ident.clone(),
            region_storage_path: region_storage_path.clone(),
            region_options: region_options.clone(),
        };
        let serialized = serde_json::to_string(&legacy_open_region).unwrap();

        // Deserialize to OpenRegion.
        let deserialized = serde_json::from_str(&serialized).unwrap();
        let expected = OpenRegion {
            region_ident,
            region_storage_path,
            region_options,
            region_wal_options: HashMap::new(),
            skip_wal_replay: false,
        };
        assert_eq!(expected, deserialized);
    }

    #[test]
    fn test_flush_regions_creation() {
        let region_id = RegionId::new(1024, 1);

        // Single region sync flush
        let single_sync = FlushRegions::sync_single(region_id);
        assert_eq!(single_sync.region_ids, vec![region_id]);
        assert_eq!(single_sync.strategy, FlushStrategy::Sync);
        assert!(!single_sync.is_hint());
        assert!(single_sync.is_sync());
        assert_eq!(single_sync.error_strategy, FlushErrorStrategy::FailFast);
        assert!(single_sync.is_single_region());
        assert_eq!(single_sync.single_region_id(), Some(region_id));

        // Batch async flush (hint)
        let region_ids = vec![RegionId::new(1024, 1), RegionId::new(1024, 2)];
        let batch_async = FlushRegions::async_batch(region_ids.clone());
        assert_eq!(batch_async.region_ids, region_ids);
        assert_eq!(batch_async.strategy, FlushStrategy::Async);
        assert!(batch_async.is_hint());
        assert!(!batch_async.is_sync());
        assert_eq!(batch_async.error_strategy, FlushErrorStrategy::TryAll);
        assert!(!batch_async.is_single_region());
        assert_eq!(batch_async.single_region_id(), None);

        // Batch sync flush
        let batch_sync = FlushRegions::sync_batch(region_ids.clone(), FlushErrorStrategy::FailFast);
        assert_eq!(batch_sync.region_ids, region_ids);
        assert_eq!(batch_sync.strategy, FlushStrategy::Sync);
        assert!(!batch_sync.is_hint());
        assert!(batch_sync.is_sync());
        assert_eq!(batch_sync.error_strategy, FlushErrorStrategy::FailFast);
    }

    #[test]
    fn test_flush_regions_conversion() {
        let region_id = RegionId::new(1024, 1);

        let from_region_id: FlushRegions = region_id.into();
        assert_eq!(from_region_id.region_ids, vec![region_id]);
        assert_eq!(from_region_id.strategy, FlushStrategy::Sync);
        assert!(!from_region_id.is_hint());
        assert!(from_region_id.is_sync());

        // Test default construction
        let flush_regions = FlushRegions {
            region_ids: vec![region_id],
            strategy: FlushStrategy::Async,
            error_strategy: FlushErrorStrategy::TryAll,
        };
        assert_eq!(flush_regions.region_ids, vec![region_id]);
        assert_eq!(flush_regions.strategy, FlushStrategy::Async);
        assert!(flush_regions.is_hint());
        assert!(!flush_regions.is_sync());
    }

    #[test]
    fn test_flush_region_reply() {
        let region_id = RegionId::new(1024, 1);

        // Successful single region reply
        let success_reply = FlushRegionReply::success_single(region_id);
        assert!(success_reply.overall_success);
        assert_eq!(success_reply.results.len(), 1);
        assert_eq!(success_reply.results[0].0, region_id);
        assert!(success_reply.results[0].1.is_ok());

        // Failed single region reply
        let error_reply = FlushRegionReply::error_single(region_id, "test error".to_string());
        assert!(!error_reply.overall_success);
        assert_eq!(error_reply.results.len(), 1);
        assert_eq!(error_reply.results[0].0, region_id);
        assert!(error_reply.results[0].1.is_err());

        // Batch reply
        let region_id2 = RegionId::new(1024, 2);
        let results = vec![
            (region_id, Ok(())),
            (region_id2, Err("flush failed".to_string())),
        ];
        let batch_reply = FlushRegionReply::from_results(results);
        assert!(!batch_reply.overall_success);
        assert_eq!(batch_reply.results.len(), 2);

        // Conversion to SimpleReply
        let simple_reply = batch_reply.to_simple_reply();
        assert!(!simple_reply.result);
        assert!(simple_reply.error.is_some());
        assert!(simple_reply.error.unwrap().contains("flush failed"));
    }

    #[test]
    fn test_serialize_flush_regions_instruction() {
        let region_id = RegionId::new(1024, 1);
        let flush_regions = FlushRegions::sync_single(region_id);
        let instruction = Instruction::FlushRegions(flush_regions.clone());

        let serialized = serde_json::to_string(&instruction).unwrap();
        let deserialized: Instruction = serde_json::from_str(&serialized).unwrap();

        match deserialized {
            Instruction::FlushRegions(fr) => {
                assert_eq!(fr.region_ids, vec![region_id]);
                assert_eq!(fr.strategy, FlushStrategy::Sync);
                assert_eq!(fr.error_strategy, FlushErrorStrategy::FailFast);
            }
            _ => panic!("Expected FlushRegions instruction"),
        }
    }

    #[test]
    fn test_serialize_flush_regions_batch_instruction() {
        let region_ids = vec![RegionId::new(1024, 1), RegionId::new(1024, 2)];
        let flush_regions =
            FlushRegions::sync_batch(region_ids.clone(), FlushErrorStrategy::TryAll);
        let instruction = Instruction::FlushRegions(flush_regions);

        let serialized = serde_json::to_string(&instruction).unwrap();
        let deserialized: Instruction = serde_json::from_str(&serialized).unwrap();

        match deserialized {
            Instruction::FlushRegions(fr) => {
                assert_eq!(fr.region_ids, region_ids);
                assert_eq!(fr.strategy, FlushStrategy::Sync);
                assert!(!fr.is_hint());
                assert!(fr.is_sync());
                assert_eq!(fr.error_strategy, FlushErrorStrategy::TryAll);
            }
            _ => panic!("Expected FlushRegions instruction"),
        }
    }
}
