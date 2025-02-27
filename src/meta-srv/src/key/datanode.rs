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

use common_meta::datanode::DatanodeStatKey;
use lazy_static::lazy_static;
use regex::Regex;
use serde::{Deserialize, Serialize};
use snafu::{ensure, OptionExt, ResultExt};

use crate::error;
use crate::error::Result;

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
    pub node_id: u64,
}

impl DatanodeLeaseKey {
    pub fn prefix_key_by_cluster() -> Vec<u8> {
        format!("{DATANODE_LEASE_PREFIX}-0-").into_bytes()
    }
}

impl From<&DatanodeLeaseKey> for DatanodeStatKey {
    fn from(lease_key: &DatanodeLeaseKey) -> Self {
        DatanodeStatKey {
            node_id: lease_key.node_id,
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct InactiveRegionKey {
    pub node_id: u64,
    pub region_id: u64,
}

impl InactiveRegionKey {
    pub fn get_prefix_by_cluster() -> Vec<u8> {
        format!("{}-0-", INACTIVE_REGION_PREFIX).into_bytes()
    }
}

impl From<InactiveRegionKey> for Vec<u8> {
    fn from(value: InactiveRegionKey) -> Self {
        format!(
            "{}-0-{}-{}",
            INACTIVE_REGION_PREFIX, value.node_id, value.region_id
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

        let _cluster_id: u64 = cluster_id.parse().context(error::ParseNumSnafu {
            err_msg: format!("invalid cluster_id: {cluster_id}"),
        })?;
        let node_id: u64 = node_id.parse().context(error::ParseNumSnafu {
            err_msg: format!("invalid node_id: {node_id}"),
        })?;
        let region_id: u64 = region_id.parse().context(error::ParseNumSnafu {
            err_msg: format!("invalid region_id: {region_id}"),
        })?;

        Ok(Self { node_id, region_id })
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
        let key = DatanodeStatKey { node_id: 1 };

        let key_bytes: Vec<u8> = key.into();
        let new_key: DatanodeStatKey = key_bytes.try_into().unwrap();

        assert_eq!(1, new_key.node_id);
    }

    #[test]
    fn test_lease_key_round_trip() {
        let key = DatanodeLeaseKey { node_id: 1 };

        let key_bytes: Vec<u8> = key.clone().try_into().unwrap();
        let new_key: DatanodeLeaseKey = key_bytes.try_into().unwrap();

        assert_eq!(new_key, key);
    }

    // add test for DatanodeLeaseKey compatibility when cluster_id is not present in serialized data,   AI!


    #[test]
    fn test_lease_key_to_stat_key() {
        let lease_key = DatanodeLeaseKey { node_id: 101 };

        let stat_key: DatanodeStatKey = (&lease_key).into();

        assert_eq!(101, stat_key.node_id);
    }

    #[test]
    fn test_inactive_region_key_round_trip() {
        let key = InactiveRegionKey {
            node_id: 1,
            region_id: 2,
        };

        let key_bytes: Vec<u8> = key.into();
        let new_key: InactiveRegionKey = key_bytes.try_into().unwrap();

        assert_eq!(new_key, key);
    }
}
