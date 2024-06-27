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

use std::str::FromStr;

use common_meta::ClusterId;
use lazy_static::lazy_static;
use regex::Regex;
use serde::{Deserialize, Serialize};
use snafu::{ensure, OptionExt, ResultExt};

use crate::error;
use crate::error::Result;
use crate::handler::node_stat::Stat;

pub(crate) const DATANODE_LEASE_PREFIX: &str = "__meta_datanode_lease";
const INACTIVE_REGION_PREFIX: &str = "__meta_inactive_region";

const DATANODE_STAT_PREFIX: &str = "__meta_datanode_stat";

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

#[derive(Debug, Clone, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct DatanodeLeaseKey {
    pub cluster_id: ClusterId,
    pub node_id: u64,
}

impl DatanodeLeaseKey {
    pub fn prefix_key_by_cluster(cluster_id: ClusterId) -> Vec<u8> {
        format!("{DATANODE_LEASE_PREFIX}-{cluster_id}-").into_bytes()
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct DatanodeStatKey {
    pub cluster_id: ClusterId,
    pub node_id: u64,
}

impl DatanodeStatKey {
    pub fn prefix_key() -> Vec<u8> {
        format!("{DATANODE_STAT_PREFIX}-").into_bytes()
    }
}

impl From<&DatanodeLeaseKey> for DatanodeStatKey {
    fn from(lease_key: &DatanodeLeaseKey) -> Self {
        DatanodeStatKey {
            cluster_id: lease_key.cluster_id,
            node_id: lease_key.node_id,
        }
    }
}

impl From<DatanodeStatKey> for Vec<u8> {
    fn from(value: DatanodeStatKey) -> Self {
        format!(
            "{}-{}-{}",
            DATANODE_STAT_PREFIX, value.cluster_id, value.node_id
        )
        .into_bytes()
    }
}

impl FromStr for DatanodeStatKey {
    type Err = error::Error;

    fn from_str(key: &str) -> Result<Self> {
        let caps = DATANODE_STAT_KEY_PATTERN
            .captures(key)
            .context(error::InvalidStatKeySnafu { key })?;

        ensure!(caps.len() == 3, error::InvalidStatKeySnafu { key });

        let cluster_id = caps[1].to_string();
        let node_id = caps[2].to_string();
        let cluster_id: u64 = cluster_id.parse().context(error::ParseNumSnafu {
            err_msg: format!("invalid cluster_id: {cluster_id}"),
        })?;
        let node_id: u64 = node_id.parse().context(error::ParseNumSnafu {
            err_msg: format!("invalid node_id: {node_id}"),
        })?;

        Ok(Self {
            cluster_id,
            node_id,
        })
    }
}

impl TryFrom<Vec<u8>> for DatanodeStatKey {
    type Error = error::Error;

    fn try_from(bytes: Vec<u8>) -> Result<Self> {
        String::from_utf8(bytes)
            .context(error::StatKeyFromUtf8Snafu {})
            .map(|x| x.parse())?
    }
}

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
            .context(error::StatValueFromUtf8Snafu {})
            .map(|x| x.parse())?
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct InactiveRegionKey {
    pub cluster_id: ClusterId,
    pub node_id: u64,
    pub region_id: u64,
}

impl InactiveRegionKey {
    pub fn get_prefix_by_cluster(cluster_id: u64) -> Vec<u8> {
        format!("{}-{}-", INACTIVE_REGION_PREFIX, cluster_id).into_bytes()
    }
}

impl From<InactiveRegionKey> for Vec<u8> {
    fn from(value: InactiveRegionKey) -> Self {
        format!(
            "{}-{}-{}-{}",
            INACTIVE_REGION_PREFIX, value.cluster_id, value.node_id, value.region_id
        )
        .into_bytes()
    }
}

impl FromStr for InactiveRegionKey {
    type Err = error::Error;

    fn from_str(key: &str) -> Result<Self> {
        let caps = INACTIVE_REGION_KEY_PATTERN
            .captures(key)
            .context(error::InvalidInactiveRegionKeySnafu { key })?;

        ensure!(
            caps.len() == 4,
            error::InvalidInactiveRegionKeySnafu { key }
        );

        let cluster_id = caps[1].to_string();
        let node_id = caps[2].to_string();
        let region_id = caps[3].to_string();

        let cluster_id: u64 = cluster_id.parse().context(error::ParseNumSnafu {
            err_msg: format!("invalid cluster_id: {cluster_id}"),
        })?;
        let node_id: u64 = node_id.parse().context(error::ParseNumSnafu {
            err_msg: format!("invalid node_id: {node_id}"),
        })?;
        let region_id: u64 = region_id.parse().context(error::ParseNumSnafu {
            err_msg: format!("invalid region_id: {region_id}"),
        })?;

        Ok(Self {
            cluster_id,
            node_id,
            region_id,
        })
    }
}

impl TryFrom<Vec<u8>> for InactiveRegionKey {
    type Error = error::Error;

    fn try_from(bytes: Vec<u8>) -> Result<Self> {
        String::from_utf8(bytes)
            .context(error::InvalidRegionKeyFromUtf8Snafu {})
            .map(|x| x.parse())?
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stat_key_round_trip() {
        let key = DatanodeStatKey {
            cluster_id: 0,
            node_id: 1,
        };

        let key_bytes: Vec<u8> = key.into();
        let new_key: DatanodeStatKey = key_bytes.try_into().unwrap();

        assert_eq!(0, new_key.cluster_id);
        assert_eq!(1, new_key.node_id);
    }

    #[test]
    fn test_stat_val_round_trip() {
        let stat = Stat {
            cluster_id: 0,
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
        assert_eq!(0, stat.cluster_id);
        assert_eq!(101, stat.id);
        assert_eq!(100, stat.region_num);
    }

    #[test]
    fn test_lease_key_round_trip() {
        let key = DatanodeLeaseKey {
            cluster_id: 0,
            node_id: 1,
        };

        let key_bytes: Vec<u8> = key.clone().try_into().unwrap();
        let new_key: DatanodeLeaseKey = key_bytes.try_into().unwrap();

        assert_eq!(new_key, key);
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

    #[test]
    fn test_lease_key_to_stat_key() {
        let lease_key = DatanodeLeaseKey {
            cluster_id: 1,
            node_id: 101,
        };

        let stat_key: DatanodeStatKey = (&lease_key).into();

        assert_eq!(1, stat_key.cluster_id);
        assert_eq!(101, stat_key.node_id);
    }

    #[test]
    fn test_inactive_region_key_round_trip() {
        let key = InactiveRegionKey {
            cluster_id: 0,
            node_id: 1,
            region_id: 2,
        };

        let key_bytes: Vec<u8> = key.into();
        let new_key: InactiveRegionKey = key_bytes.try_into().unwrap();

        assert_eq!(new_key, key);
    }
}
