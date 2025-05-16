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
use crate::key::FlowId;
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

impl Display for SimpleReply {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "(result={}, error={:?})", self.result, self.error)
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
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
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
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
/// The identifier of cache.
pub enum CacheIdent {
    FlowId(FlowId),
    FlowName(FlowName),
    TableId(TableId),
    TableName(TableName),
    SchemaName(SchemaName),
    CreateFlow(CreateFlow),
    DropFlow(DropFlow),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CreateFlow {
    pub source_table_ids: Vec<TableId>,
    pub flownodes: Vec<Peer>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DropFlow {
    pub source_table_ids: Vec<TableId>,
    pub flownode_ids: Vec<FlownodeId>,
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
    /// Flushes one or more regions with specified configuration.
    FlushRegion(FlushRegionConfig),
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
    FlushRegion(FlushRegionReply),
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
            Self::FlushRegion(reply) => write!(f, "InstructionReply::FlushRegion({})", reply),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Display, PartialEq, Eq)]
pub enum FlushTarget {
    All,
    Regions(Vec<RegionId>),
    Table(TableId),
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Display, PartialEq, Eq)]
pub enum FlushErrorStrategy {
    /// Stop on first error and return immediately
    FailFast,
    /// Try all regions and collect results, overall success if any region succeeds
    TryAll,
    /// Try all regions and collect results, overall success only if all regions succeed
    TryAllStrict,
}

impl Default for FlushErrorStrategy {
    fn default() -> Self {
        Self::TryAll
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FlushRegionConfig {
    pub target: FlushTarget,
    pub is_hint: bool,
    #[serde(with = "humantime_serde")]
    pub timeout: Option<Duration>,
    #[serde(default)]
    pub error_strategy: FlushErrorStrategy,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct FlushResult {
    pub success: bool,
    pub error: Option<String>,
    #[serde(default)]
    pub skipped: bool,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct FlushRegionReply {
    pub success: bool,
    pub results: HashMap<RegionId, FlushResult>,
    pub error_strategy: FlushErrorStrategy,
}

impl Display for FlushRegionReply {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let total = self.results.len();
        let succeeded = self.results.values().filter(|r| r.success).count();
        let failed = self
            .results
            .values()
            .filter(|r| !r.success && !r.skipped)
            .count();
        let skipped = self.results.values().filter(|r| r.skipped).count();

        write!(
            f,
            "(success={}, total={}, succeeded={}, failed={}, skipped={}, strategy={:?})",
            self.success, total, succeeded, failed, skipped, self.error_strategy
        )
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

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
    fn test_flush_region_config() {
        let config = FlushRegionConfig {
            target: FlushTarget::All,
            is_hint: true,
            timeout: Some(Duration::from_secs(30)),
            error_strategy: FlushErrorStrategy::TryAllStrict,
        };
        let json = serde_json::to_string(&config).unwrap();
        let decoded: FlushRegionConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(config, decoded);

        let config = FlushRegionConfig {
            target: FlushTarget::Regions(vec![RegionId::new(1, 1), RegionId::new(2, 1)]),
            is_hint: false,
            timeout: None,
            error_strategy: FlushErrorStrategy::FailFast,
        };
        let json = serde_json::to_string(&config).unwrap();
        let decoded: FlushRegionConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(config, decoded);

        let config = FlushRegionConfig {
            target: FlushTarget::Table(TableId::from(42_u32)),
            is_hint: true,
            timeout: Some(Duration::from_secs(60)),
            error_strategy: FlushErrorStrategy::default(),
        };
        let json = serde_json::to_string(&config).unwrap();
        let decoded: FlushRegionConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(config, decoded);
    }

    #[test]
    fn test_flush_region_reply() {
        let mut results = HashMap::new();
        // Successful flush
        results.insert(
            RegionId::new(1, 1),
            FlushResult {
                success: true,
                error: None,
                skipped: false,
            },
        );
        // Failed flush
        results.insert(
            RegionId::new(2, 1),
            FlushResult {
                success: false,
                error: Some("timeout".to_string()),
                skipped: false,
            },
        );
        // Skipped flush
        results.insert(
            RegionId::new(3, 1),
            FlushResult {
                success: false,
                error: None,
                skipped: true,
            },
        );

        let reply = FlushRegionReply {
            success: true,
            results,
            error_strategy: FlushErrorStrategy::TryAll,
        };
        let json = serde_json::to_string(&reply).unwrap();
        let decoded: FlushRegionReply = serde_json::from_str(&json).unwrap();
        assert_eq!(reply, decoded);

        // Test Display implementation
        let display = format!("{}", reply);
        assert!(display.contains("total=3"));
        assert!(display.contains("succeeded=1"));
        assert!(display.contains("failed=1"));
        assert!(display.contains("skipped=1"));
    }

    #[test]
    fn test_flush_error_strategies() {
        let mut results = HashMap::new();
        results.insert(
            RegionId::new(1, 1),
            FlushResult {
                success: true,
                error: None,
                skipped: false,
            },
        );
        results.insert(
            RegionId::new(2, 1),
            FlushResult {
                success: false,
                error: Some("error".to_string()),
                skipped: false,
            },
        );

        // TryAll: success if any region succeeds
        let reply = FlushRegionReply {
            success: true,
            results: results.clone(),
            error_strategy: FlushErrorStrategy::TryAll,
        };
        assert!(reply.success);

        // TryAllStrict: success only if all regions succeed
        let reply = FlushRegionReply {
            success: false,
            results: results.clone(),
            error_strategy: FlushErrorStrategy::TryAllStrict,
        };
        assert!(!reply.success);

        // FailFast: should have only one result
        let mut fail_fast_results = HashMap::new();
        fail_fast_results.insert(
            RegionId::new(2, 1),
            FlushResult {
                success: false,
                error: Some("error".to_string()),
                skipped: false,
            },
        );
        let reply = FlushRegionReply {
            success: false,
            results: fail_fast_results,
            error_strategy: FlushErrorStrategy::FailFast,
        };
        assert!(!reply.success);
        assert_eq!(reply.results.len(), 1);
    }
}
