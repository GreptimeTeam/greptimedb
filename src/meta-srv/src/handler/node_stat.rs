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

use std::collections::{HashMap, HashSet};

use api::v1::meta::{HeartbeatRequest, RequestHeader};
use common_meta::ClusterId;
use common_time::util as time_util;
use serde::{Deserialize, Serialize};
use store_api::region_engine::RegionRole;
use store_api::storage::RegionId;
use table::metadata::TableId;

use crate::key::DatanodeStatKey;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Stat {
    pub timestamp_millis: i64,
    pub cluster_id: ClusterId,
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegionStat {
    /// The region_id.
    pub id: RegionId,
    /// The read capacity units during this period
    pub rcus: i64,
    /// The write capacity units during this period
    pub wcus: i64,
    /// Approximate bytes of this region
    pub approximate_bytes: i64,
    /// The engine name.
    pub engine: String,
    /// The region role.
    pub role: RegionRole,
    /// The extension info of this region
    pub extensions: HashMap<String, Vec<u8>>,
}

impl Stat {
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.region_stats.is_empty()
    }

    pub fn stat_key(&self) -> DatanodeStatKey {
        DatanodeStatKey {
            cluster_id: self.cluster_id,
            node_id: self.id,
        }
    }

    /// Returns a tuple array containing [RegionId] and [RegionRole].
    pub fn regions(&self) -> Vec<(RegionId, RegionRole)> {
        self.region_stats.iter().map(|s| (s.id, s.role)).collect()
    }

    /// Returns all table ids in the region stats.
    pub fn table_ids(&self) -> HashSet<TableId> {
        self.region_stats.iter().map(|s| s.id.table_id()).collect()
    }

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
}

impl TryFrom<&HeartbeatRequest> for Stat {
    type Error = Option<RequestHeader>;

    fn try_from(value: &HeartbeatRequest) -> Result<Self, Self::Error> {
        let HeartbeatRequest {
            header,
            peer,
            region_stats,
            node_epoch,
            ..
        } = value;

        match (header, peer) {
            (Some(header), Some(peer)) => {
                let region_stats = region_stats
                    .iter()
                    .map(RegionStat::from)
                    .collect::<Vec<_>>();

                Ok(Self {
                    timestamp_millis: time_util::current_time_millis(),
                    cluster_id: header.cluster_id,
                    // datanode id
                    id: peer.id,
                    // datanode address
                    addr: peer.addr.clone(),
                    rcus: region_stats.iter().map(|s| s.rcus).sum(),
                    wcus: region_stats.iter().map(|s| s.wcus).sum(),
                    region_num: region_stats.len() as u64,
                    region_stats,
                    node_epoch: *node_epoch,
                })
            }
            (header, _) => Err(header.clone()),
        }
    }
}

impl From<&api::v1::meta::RegionStat> for RegionStat {
    fn from(value: &api::v1::meta::RegionStat) -> Self {
        Self {
            id: RegionId::from_u64(value.region_id),
            rcus: value.rcus,
            wcus: value.wcus,
            approximate_bytes: value.approximate_bytes,
            engine: value.engine.to_string(),
            role: RegionRole::from(value.role()),
            extensions: value.extensions.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::handler::node_stat::Stat;

    #[test]
    fn test_stat_key() {
        let stat = Stat {
            cluster_id: 3,
            id: 101,
            region_num: 10,
            ..Default::default()
        };

        let stat_key = stat.stat_key();

        assert_eq!(3, stat_key.cluster_id);
        assert_eq!(101, stat_key.node_id);
    }
}
