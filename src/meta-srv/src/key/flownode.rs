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

use common_meta::ClusterId;
use lazy_static::lazy_static;
use regex::Regex;
use serde::{Deserialize, Serialize};

pub(crate) const FLOWNODE_LEASE_PREFIX: &str = "__meta_flownode_lease";

lazy_static! {
    pub(crate) static ref FLOWNODE_LEASE_KEY_PATTERN: Regex =
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
