// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use api::v1::meta::HeartbeatRequest;
use common_meta::ident::TableIdent;
use common_time::util as time_util;
use serde::{Deserialize, Serialize};

use crate::keys::StatKey;

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Stat {
    pub timestamp_millis: i64,
    pub cluster_id: u64,
    pub id: u64,
    pub addr: String,
    /// The read capacity units during this period
    pub rcus: i64,
    /// The write capacity units during this period
    pub wcus: i64,
    /// How many tables on this node
    pub table_num: i64,
    /// How many regions on this node
    pub region_num: Option<u64>,
    pub region_stats: Vec<RegionStat>,
    // The node epoch is used to check whether the node has restarted or redeployed.
    pub node_epoch: u64,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct RegionStat {
    pub id: u64,
    pub table_ident: TableIdent,
    /// The read capacity units during this period
    pub rcus: i64,
    /// The write capacity units during this period
    pub wcus: i64,
    /// Approximate bytes of this region
    pub approximate_bytes: i64,
    /// Approximate number of rows in this region
    pub approximate_rows: i64,
}

impl Stat {
    pub fn stat_key(&self) -> StatKey {
        StatKey {
            cluster_id: self.cluster_id,
            node_id: self.id,
        }
    }
}

impl TryFrom<HeartbeatRequest> for Stat {
    type Error = ();

    fn try_from(value: HeartbeatRequest) -> Result<Self, Self::Error> {
        let HeartbeatRequest {
            header,
            peer,
            node_stat,
            region_stats,
            node_epoch,
            ..
        } = value;

        match (header, peer, node_stat) {
            (Some(header), Some(peer), Some(node_stat)) => {
                let region_num = if node_stat.region_num >= 0 {
                    Some(node_stat.region_num as u64)
                } else {
                    None
                };
                let region_stats = region_stats
                    .into_iter()
                    .map(RegionStat::try_from)
                    .collect::<Result<Vec<_>, ()>>();
                let Ok(region_stats) = region_stats else { return Err(()) };

                Ok(Self {
                    timestamp_millis: time_util::current_time_millis(),
                    cluster_id: header.cluster_id,
                    id: peer.id,
                    addr: peer.addr,
                    rcus: node_stat.rcus,
                    wcus: node_stat.wcus,
                    table_num: node_stat.table_num,
                    region_num,
                    region_stats,
                    node_epoch,
                })
            }
            _ => Err(()),
        }
    }
}

impl TryFrom<api::v1::meta::RegionStat> for RegionStat {
    type Error = ();

    fn try_from(value: api::v1::meta::RegionStat) -> Result<Self, Self::Error> {
        let Some(table_ident) = value.table_ident else {
            return Err(())
        };
        let Ok(table_ident) = TableIdent::try_from(table_ident) else {
            return Err(())
        };
        Ok(Self {
            id: value.region_id,
            table_ident,
            rcus: value.rcus,
            wcus: value.wcus,
            approximate_bytes: value.approximate_bytes,
            approximate_rows: value.approximate_rows,
        })
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
            region_num: Some(10),
            ..Default::default()
        };

        let stat_key = stat.stat_key();

        assert_eq!(3, stat_key.cluster_id);
        assert_eq!(101, stat_key.node_id);
    }
}
