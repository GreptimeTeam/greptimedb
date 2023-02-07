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

use api::v1::meta::Peer;
use common_time::util as time_util;

use crate::cluster::MetaPeerClient;
use crate::error::Result;
use crate::keys::{LeaseKey, LeaseValue, StatKey};
use crate::lease;
use crate::metasrv::Context;
use crate::selector::{Namespace, Selector};

pub struct LoadBasedSelector {
    pub meta_peer_client: MetaPeerClient,
}

#[async_trait::async_trait]
impl Selector for LoadBasedSelector {
    type Context = Context;
    type Output = Vec<Peer>;

    async fn select(&self, ns: Namespace, ctx: &Self::Context) -> Result<Self::Output> {
        // get alive datanodes
        let lease_filter = |_: &LeaseKey, v: &LeaseValue| {
            time_util::current_time_millis() - v.timestamp_millis < ctx.datanode_lease_secs * 1000
        };
        let lease_kvs: HashMap<LeaseKey, LeaseValue> =
            lease::alive_datanodes(ns, &ctx.kv_store, lease_filter)
                .await?
                .into_iter()
                .collect();

        // get stats of alive datanodes
        let stat_keys: Vec<StatKey> = lease_kvs
            .keys()
            .map(|k| StatKey {
                cluster_id: k.cluster_id,
                node_id: k.node_id,
            })
            .collect();
        let stat_kvs = self.meta_peer_client.get_dn_stat_kvs(stat_keys).await?;

        // aggregate lease and stat information
        let mut tuples: Vec<(LeaseKey, LeaseValue, u64)> = stat_kvs
            .into_iter()
            .filter_map(|(stat_key, stat_val)| {
                let lease_key = to_lease_key(&stat_key);
                match (lease_kvs.get(&lease_key), stat_val.region_num()) {
                    (Some(lease_val), Some(region_num)) => {
                        Some((lease_key, lease_val.clone(), region_num))
                    }
                    _ => None,
                }
            })
            .collect();

        // sort the datanodes according to the number of regions
        tuples.sort_by(|a, b| a.2.cmp(&b.2));

        Ok(tuples
            .into_iter()
            .map(|(stat_key, lease_val, _)| Peer {
                id: stat_key.node_id,
                addr: lease_val.node_addr,
            })
            .collect())
    }
}

fn to_lease_key(k: &StatKey) -> LeaseKey {
    LeaseKey {
        cluster_id: k.cluster_id,
        node_id: k.node_id,
    }
}

#[cfg(test)]
mod tests {
    use super::to_lease_key;
    use crate::keys::StatKey;

    #[test]
    fn test_to_lease_key() {
        let statkey = StatKey {
            cluster_id: 1,
            node_id: 101,
        };
        let lease_key = to_lease_key(&statkey);
        assert_eq!(1, lease_key.cluster_id);
        assert_eq!(101, lease_key.node_id);
    }
}
