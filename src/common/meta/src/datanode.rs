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

use std::collections::HashSet;
use std::str::FromStr;

use api::v1::meta::{DatanodeWorkloads, HeartbeatRequest, RequestHeader};
use common_time::util as time_util;
use lazy_static::lazy_static;
use regex::Regex;
use serde::{Deserialize, Serialize};
use snafu::{ensure, OptionExt, ResultExt};
use store_api::region_engine::{RegionRole, RegionStatistic};
use store_api::storage::RegionId;
use table::metadata::TableId;

use crate::error;
use crate::error::Result;
use crate::heartbeat::utils::get_datanode_workloads;

pub(crate) const DATANODE_LEASE_PREFIX: &str = "__meta_datanode_lease";
const INACTIVE_REGION_PREFIX: &str = "__meta_inactive_region";

const DATANODE_STAT_PREFIX: &str = "__meta_datanode_stat";

pub const REGION_STATISTIC_KEY: &str = "__region_statistic";

lazy_static! {
    pub(crate) static ref DATANODE_LEASE_KEY_PATTERN: Regex =
        Regex::new(&format!("^{DATANODE_LEASE_PREFIX}-([0-9]+)-([0-9]+)$")).unwrap();
    static ref DATANODE_STAT_KEY_PATTERN: Regex =
        Regex::new(&format!("^{DATANODE_STAT_PREFIX}-([0-9]+)-([0-9]+)$")).unwrap();
    static ref INACTIVE_REGION_KEY_PATTERN: Regex = Regex::new(&format!(
        "^{INACTIVE_REGION_PREFIX}-([0-9]+)-([0-9]+)-([0-9]+)$"
    ))
    .unwrap();
}

/// The key of the datanode stat in the storage.
///
/// The format is `__meta_datanode_stat-0-{node_id}`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Stat {
    pub timestamp_millis: i64,
    // The datanode Id.
    pub id: u64,
    // The datanode address.
    pub addr: String,
    /// The read capacity units during this period
    pub rcus: i64,
    /// The write capacity units during this period
    pub wcus: i64,
    /// How many regions on this node
    pub region_num: u64,
    pub region_stats: Vec<RegionStat>,
    // The node epoch is used to check whether the node has restarted or redeployed.
    pub node_epoch: u64,
    /// The datanode workloads.
    pub datanode_workloads: DatanodeWorkloads,
}

/// The statistics of a region.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegionStat {
    /// The region_id.
    pub id: RegionId,
    /// The read capacity units during this period
    pub rcus: i64,
    /// The write capacity units during this period
    pub wcus: i64,
    /// Approximate disk bytes of this region, including sst, index, manifest and wal
    pub approximate_bytes: u64,
    /// The engine name.
    pub engine: String,
    /// The region role.
    pub role: RegionRole,
    /// The number of rows
    pub num_rows: u64,
    /// The size of the memtable in bytes.
    pub memtable_size: u64,
    /// The size of the manifest in bytes.
    pub manifest_size: u64,
    /// The size of the SST data files in bytes.
    pub sst_size: u64,
    /// The size of the SST index files in bytes.
    pub index_size: u64,
    /// The manifest infoof the region.
    pub region_manifest: RegionManifestInfo,
    /// The latest entry id of topic used by data.
    /// **Only used by remote WAL prune.**
    pub data_topic_latest_entry_id: u64,
    /// The latest entry id of topic used by metadata.
    /// **Only used by remote WAL prune.**
    /// In mito engine, this is the same as `data_topic_latest_entry_id`.
    pub metadata_topic_latest_entry_id: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
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

impl Stat {
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.region_stats.is_empty()
    }

    pub fn stat_key(&self) -> DatanodeStatKey {
        DatanodeStatKey { node_id: self.id }
    }

    /// Returns a tuple array containing [RegionId] and [RegionRole].
    pub fn regions(&self) -> Vec<(RegionId, RegionRole)> {
        self.region_stats.iter().map(|s| (s.id, s.role)).collect()
    }

    /// Returns all table ids in the region stats.
    pub fn table_ids(&self) -> HashSet<TableId> {
        self.region_stats.iter().map(|s| s.id.table_id()).collect()
    }

    /// Retains the active region stats and updates the rcus, wcus, and region_num.
    pub fn retain_active_region_stats(&mut self, inactive_region_ids: &HashSet<RegionId>) {
        if inactive_region_ids.is_empty() {
            return;
        }

        self.region_stats
            .retain(|r| !inactive_region_ids.contains(&r.id));
        self.rcus = self.region_stats.iter().map(|s| s.rcus).sum();
        self.wcus = self.region_stats.iter().map(|s| s.wcus).sum();
        self.region_num = self.region_stats.len() as u64;
    }

    pub fn memory_size(&self) -> usize {
        // timestamp_millis, rcus, wcus
        std::mem::size_of::<i64>() * 3 +
        // id, region_num, node_epoch
        std::mem::size_of::<u64>() * 3 +
        // addr
        std::mem::size_of::<String>() + self.addr.capacity() +
        // region_stats
        self.region_stats.iter().map(|s| s.memory_size()).sum::<usize>()
    }
}

impl RegionStat {
    pub fn memory_size(&self) -> usize {
        // role
        std::mem::size_of::<RegionRole>() +
        // id
        std::mem::size_of::<RegionId>() +
        // rcus, wcus, approximate_bytes, num_rows
        std::mem::size_of::<i64>() * 4 +
        // memtable_size, manifest_size, sst_size, index_size
        std::mem::size_of::<u64>() * 4 +
        // engine
        std::mem::size_of::<String>() + self.engine.capacity() +
        // region_manifest
        self.region_manifest.memory_size()
    }
}

impl RegionManifestInfo {
    pub fn memory_size(&self) -> usize {
        match self {
            RegionManifestInfo::Mito { .. } => std::mem::size_of::<u64>() * 2,
            RegionManifestInfo::Metric { .. } => std::mem::size_of::<u64>() * 4,
        }
    }
}

impl TryFrom<&HeartbeatRequest> for Stat {
    type Error = Option<RequestHeader>;

    fn try_from(value: &HeartbeatRequest) -> std::result::Result<Self, Self::Error> {
        let HeartbeatRequest {
            header,
            peer,
            region_stats,
            node_epoch,
            node_workloads,
            ..
        } = value;

        match (header, peer) {
            (Some(_header), Some(peer)) => {
                let region_stats = region_stats
                    .iter()
                    .map(RegionStat::from)
                    .collect::<Vec<_>>();

                let datanode_workloads = get_datanode_workloads(node_workloads.as_ref());
                Ok(Self {
                    timestamp_millis: time_util::current_time_millis(),
                    // datanode id
                    id: peer.id,
                    // datanode address
                    addr: peer.addr.clone(),
                    rcus: region_stats.iter().map(|s| s.rcus).sum(),
                    wcus: region_stats.iter().map(|s| s.wcus).sum(),
                    region_num: region_stats.len() as u64,
                    region_stats,
                    node_epoch: *node_epoch,
                    datanode_workloads,
                })
            }
            (header, _) => Err(header.clone()),
        }
    }
}

impl From<store_api::region_engine::RegionManifestInfo> for RegionManifestInfo {
    fn from(value: store_api::region_engine::RegionManifestInfo) -> Self {
        match value {
            store_api::region_engine::RegionManifestInfo::Mito {
                manifest_version,
                flushed_entry_id,
            } => RegionManifestInfo::Mito {
                manifest_version,
                flushed_entry_id,
            },
            store_api::region_engine::RegionManifestInfo::Metric {
                data_manifest_version,
                data_flushed_entry_id,
                metadata_manifest_version,
                metadata_flushed_entry_id,
            } => RegionManifestInfo::Metric {
                data_manifest_version,
                data_flushed_entry_id,
                metadata_manifest_version,
                metadata_flushed_entry_id,
            },
        }
    }
}

impl From<&api::v1::meta::RegionStat> for RegionStat {
    fn from(value: &api::v1::meta::RegionStat) -> Self {
        let region_stat = value
            .extensions
            .get(REGION_STATISTIC_KEY)
            .and_then(|value| RegionStatistic::deserialize_from_slice(value))
            .unwrap_or_default();

        Self {
            id: RegionId::from_u64(value.region_id),
            rcus: value.rcus,
            wcus: value.wcus,
            approximate_bytes: value.approximate_bytes as u64,
            engine: value.engine.to_string(),
            role: RegionRole::from(value.role()),
            num_rows: region_stat.num_rows,
            memtable_size: region_stat.memtable_size,
            manifest_size: region_stat.manifest_size,
            sst_size: region_stat.sst_size,
            index_size: region_stat.index_size,
            region_manifest: region_stat.manifest.into(),
            data_topic_latest_entry_id: region_stat.data_topic_latest_entry_id,
            metadata_topic_latest_entry_id: region_stat.metadata_topic_latest_entry_id,
        }
    }
}

/// The key of the datanode stat in the memory store.
///
/// The format is `__meta_datanode_stat-0-{node_id}`.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct DatanodeStatKey {
    pub node_id: u64,
}

impl DatanodeStatKey {
    /// The key prefix.
    pub fn prefix_key() -> Vec<u8> {
        // todo(hl): remove cluster id in prefix
        format!("{DATANODE_STAT_PREFIX}-0-").into_bytes()
    }
}

impl From<DatanodeStatKey> for Vec<u8> {
    fn from(value: DatanodeStatKey) -> Self {
        // todo(hl): remove cluster id in prefix
        format!("{}-0-{}", DATANODE_STAT_PREFIX, value.node_id).into_bytes()
    }
}

impl FromStr for DatanodeStatKey {
    type Err = error::Error;

    fn from_str(key: &str) -> Result<Self> {
        let caps = DATANODE_STAT_KEY_PATTERN
            .captures(key)
            .context(error::InvalidStatKeySnafu { key })?;

        ensure!(caps.len() == 3, error::InvalidStatKeySnafu { key });
        let node_id = caps[2].to_string();
        let node_id: u64 = node_id.parse().context(error::ParseNumSnafu {
            err_msg: format!("invalid node_id: {node_id}"),
        })?;

        Ok(Self { node_id })
    }
}

impl TryFrom<Vec<u8>> for DatanodeStatKey {
    type Error = error::Error;

    fn try_from(bytes: Vec<u8>) -> Result<Self> {
        String::from_utf8(bytes)
            .context(error::FromUtf8Snafu {
                name: "DatanodeStatKey",
            })
            .map(|x| x.parse())?
    }
}

/// The value of the datanode stat in the memory store.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(transparent)]
pub struct DatanodeStatValue {
    pub stats: Vec<Stat>,
}

impl DatanodeStatValue {
    /// Get the latest number of regions.
    pub fn region_num(&self) -> Option<u64> {
        self.stats.last().map(|x| x.region_num)
    }

    /// Get the latest node addr.
    pub fn node_addr(&self) -> Option<String> {
        self.stats.last().map(|x| x.addr.clone())
    }
}

impl TryFrom<DatanodeStatValue> for Vec<u8> {
    type Error = error::Error;

    fn try_from(stats: DatanodeStatValue) -> Result<Self> {
        Ok(serde_json::to_string(&stats)
            .context(error::SerializeToJsonSnafu {
                input: format!("{stats:?}"),
            })?
            .into_bytes())
    }
}

impl FromStr for DatanodeStatValue {
    type Err = error::Error;

    fn from_str(value: &str) -> Result<Self> {
        serde_json::from_str(value).context(error::DeserializeFromJsonSnafu { input: value })
    }
}

impl TryFrom<Vec<u8>> for DatanodeStatValue {
    type Error = error::Error;

    fn try_from(value: Vec<u8>) -> Result<Self> {
        String::from_utf8(value)
            .context(error::FromUtf8Snafu {
                name: "DatanodeStatValue",
            })
            .map(|x| x.parse())?
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stat_key() {
        let stat = Stat {
            id: 101,
            region_num: 10,
            ..Default::default()
        };

        let stat_key = stat.stat_key();

        assert_eq!(101, stat_key.node_id);
    }

    #[test]
    fn test_stat_val_round_trip() {
        let stat = Stat {
            id: 101,
            region_num: 100,
            ..Default::default()
        };

        let stat_val = DatanodeStatValue { stats: vec![stat] };

        let bytes: Vec<u8> = stat_val.try_into().unwrap();
        let stat_val: DatanodeStatValue = bytes.try_into().unwrap();
        let stats = stat_val.stats;

        assert_eq!(1, stats.len());

        let stat = stats.first().unwrap();
        assert_eq!(101, stat.id);
        assert_eq!(100, stat.region_num);
    }

    #[test]
    fn test_get_addr_from_stat_val() {
        let empty = DatanodeStatValue { stats: vec![] };
        let addr = empty.node_addr();
        assert!(addr.is_none());

        let stat_val = DatanodeStatValue {
            stats: vec![
                Stat {
                    addr: "1".to_string(),
                    ..Default::default()
                },
                Stat {
                    addr: "2".to_string(),
                    ..Default::default()
                },
                Stat {
                    addr: "3".to_string(),
                    ..Default::default()
                },
            ],
        };
        let addr = stat_val.node_addr().unwrap();
        assert_eq!("3", addr);
    }

    #[test]
    fn test_get_region_num_from_stat_val() {
        let empty = DatanodeStatValue { stats: vec![] };
        let region_num = empty.region_num();
        assert!(region_num.is_none());

        let wrong = DatanodeStatValue {
            stats: vec![Stat {
                region_num: 0,
                ..Default::default()
            }],
        };
        let right = wrong.region_num();
        assert_eq!(Some(0), right);

        let stat_val = DatanodeStatValue {
            stats: vec![
                Stat {
                    region_num: 1,
                    ..Default::default()
                },
                Stat {
                    region_num: 0,
                    ..Default::default()
                },
                Stat {
                    region_num: 2,
                    ..Default::default()
                },
            ],
        };
        let region_num = stat_val.region_num().unwrap();
        assert_eq!(2, region_num);
    }
}
