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

const FLOWNODE_LEASE_PREFIX: &str = "__meta_flownode_lease";

lazy_static! {
    static ref FLOWNODE_LEASE_KEY_PATTERN: Regex =
        Regex::new(&format!("^{FLOWNODE_LEASE_PREFIX}-([0-9]+)-([0-9]+)$")).unwrap();
}

#[derive(Debug, Clone, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct FlownodeLeaseKey {
    pub cluster_id: ClusterId,
    pub node_id: u64,
}

impl FlownodeLeaseKey {
    pub fn prefix_key_by_cluster(cluster_id: ClusterId) -> Vec<u8> {
        format!("{FLOWNODE_LEASE_PREFIX}-{cluster_id}-").into_bytes()
    }
}

impl FromStr for FlownodeLeaseKey {
    type Err = error::Error;

    fn from_str(key: &str) -> Result<Self> {
        let caps = FLOWNODE_LEASE_KEY_PATTERN
            .captures(key)
            .context(error::InvalidLeaseKeySnafu { key })?;

        ensure!(caps.len() == 3, error::InvalidLeaseKeySnafu { key });

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

impl TryFrom<Vec<u8>> for FlownodeLeaseKey {
    type Error = error::Error;

    fn try_from(bytes: Vec<u8>) -> Result<Self> {
        String::from_utf8(bytes)
            .context(error::LeaseKeyFromUtf8Snafu {})
            .map(|x| x.parse())?
    }
}

impl TryFrom<FlownodeLeaseKey> for Vec<u8> {
    type Error = error::Error;

    fn try_from(dn_key: FlownodeLeaseKey) -> Result<Self> {
        Ok(format!(
            "{}-{}-{}",
            FLOWNODE_LEASE_PREFIX, dn_key.cluster_id, dn_key.node_id
        )
        .into_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lease_key_round_trip() {
        let key = FlownodeLeaseKey {
            cluster_id: 0,
            node_id: 1,
        };

        let key_bytes: Vec<u8> = key.clone().try_into().unwrap();
        let new_key: FlownodeLeaseKey = key_bytes.try_into().unwrap();

        assert_eq!(new_key, key);
    }
}
