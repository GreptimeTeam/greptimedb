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

use crate::table_name::TableName;
use crate::{ClusterId, DatanodeId};

#[derive(Eq, Hash, PartialEq, Clone, Debug, Serialize, Deserialize)]
pub struct RegionIdent {
    pub cluster_id: ClusterId,
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
            "RegionIdent(datanode_id='{}.{}', table_id={}, region_number={}, engine = {})",
            self.cluster_id, self.datanode_id, self.table_id, self.region_number, self.engine
        )
    }
}

/// The result of downgrade leader region.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct DowngradeRegionReply {
    /// Returns the `last_entry_id` if available.
    pub last_entry_id: Option<u64>,
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DowngradeRegion {
    /// The [RegionId].
    pub region_id: RegionId,
}

impl Display for DowngradeRegion {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "DowngradeRegion(region_id={})", self.region_id)
    }
}

/// Upgrades a follower region to leader region.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpgradeRegion {
    /// The [RegionId].
    pub region_id: RegionId,
    /// The `last_entry_id` of old leader region.
    pub last_entry_id: Option<u64>,
    /// The timeout of waiting for a wal replay.
    ///
    /// `None` stands for no wait,
    /// it's helpful to verify whether the leader region is ready.
    #[serde(with = "humantime_serde")]
    pub wait_for_replay_timeout: Option<Duration>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Display)]
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
    /// Invalidates a specified table cache.
    InvalidateTableIdCache(TableId),
    /// Invalidates a specified table name index cache.
    InvalidateTableNameCache(TableName),
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
    InvalidateTableCache(SimpleReply),
    DowngradeRegion(DowngradeRegionReply),
}

impl Display for InstructionReply {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OpenRegion(reply) => write!(f, "InstructionReply::OpenRegion({})", reply),
            Self::CloseRegion(reply) => write!(f, "InstructionReply::CloseRegion({})", reply),
            Self::UpgradeRegion(reply) => write!(f, "InstructionReply::UpgradeRegion({})", reply),
            Self::InvalidateTableCache(reply) => {
                write!(f, "InstructionReply::Invalidate({})", reply)
            }
            Self::DowngradeRegion(reply) => {
                write!(f, "InstructionReply::DowngradeRegion({})", reply)
            }
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
                cluster_id: 1,
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
            r#"{"OpenRegion":{"region_ident":{"cluster_id":1,"datanode_id":2,"table_id":1024,"region_number":1,"engine":"mito2"},"region_storage_path":"test/foo","region_options":{},"region_wal_options":{},"skip_wal_replay":false}}"#,
            serialized
        );

        let close_region = Instruction::CloseRegion(RegionIdent {
            cluster_id: 1,
            datanode_id: 2,
            table_id: 1024,
            region_number: 1,
            engine: "mito2".to_string(),
        });

        let serialized = serde_json::to_string(&close_region).unwrap();

        assert_eq!(
            r#"{"CloseRegion":{"cluster_id":1,"datanode_id":2,"table_id":1024,"region_number":1,"engine":"mito2"}}"#,
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
            cluster_id: 1,
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
}
