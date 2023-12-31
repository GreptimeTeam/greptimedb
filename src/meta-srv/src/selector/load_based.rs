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

use common_meta::key::TableMetadataManager;
use common_meta::peer::Peer;
use common_meta::rpc::router::find_leaders;
use common_telemetry::{debug, info};
use parking_lot::RwLock;
use snafu::ResultExt;
use table::metadata::TableId;

use crate::error::{self, Result};
use crate::keys::{LeaseKey, LeaseValue, StatKey, StatValue};
use crate::lease;
use crate::metasrv::SelectorContext;
use crate::selector::common::choose_peers;
use crate::selector::weight_compute::{RegionNumsBasedWeightCompute, WeightCompute};
use crate::selector::weighted_choose::{RandomWeightedChoose, WeightedChoose};
use crate::selector::{Namespace, Selector, SelectorOptions};

pub struct LoadBasedSelector<W, C> {
    weighted_choose: RwLock<W>,
    weight_compute: C,
}

impl<W, C> LoadBasedSelector<W, C> {
    pub fn new(weighted_choose: W, weight_compute: C) -> Self {
        Self {
            weighted_choose: RwLock::new(weighted_choose),
            weight_compute,
        }
    }
}

impl Default for LoadBasedSelector<RandomWeightedChoose<Peer>, RegionNumsBasedWeightCompute> {
    fn default() -> Self {
        Self {
            weighted_choose: RwLock::new(RandomWeightedChoose::default()),
            weight_compute: RegionNumsBasedWeightCompute,
        }
    }
}

#[async_trait::async_trait]
impl<W, C> Selector for LoadBasedSelector<W, C>
where
    W: WeightedChoose<Peer>,
    C: WeightCompute<Source = HashMap<StatKey, StatValue>>,
{
    type Context = SelectorContext;
    type Output = Vec<Peer>;

    async fn select(
        &self,
        ns: Namespace,
        ctx: &Self::Context,
        opts: SelectorOptions,
    ) -> Result<Self::Output> {
        // 1. get alive datanodes.
        let lease_kvs =
            lease::alive_datanodes(ns, &ctx.meta_peer_client, ctx.datanode_lease_secs).await?;

        // 2. get stat kvs and filter out expired datanodes.
        let stat_keys = lease_kvs.keys().map(|k| k.into()).collect();
        let stat_kvs = filter_out_expired_datanode(
            ctx.meta_peer_client.get_dn_stat_kvs(stat_keys).await?,
            &lease_kvs,
        );

        // 3. try to make the regions of a table distributed on different datanodes as much as possible.
        let stat_kvs = if let Some(table_id) = ctx.table_id {
            let table_metadata_manager = TableMetadataManager::new(ctx.kv_backend.clone());
            let leader_peer_ids = get_leader_peer_ids(&table_metadata_manager, table_id).await?;
            let filter_result = filter_out_datanode_by_table(&stat_kvs, &leader_peer_ids);
            if filter_result.is_empty() {
                info!("The regions of the table cannot be allocated to completely different datanodes, table id: {}.", table_id);
                stat_kvs
            } else {
                filter_result
            }
        } else {
            stat_kvs
        };

        // 4. compute weight array.
        let weight_array = self.weight_compute.compute(&stat_kvs);

        // 5. choose peers by weight_array.
        let mut weighted_choose = self.weighted_choose.write();
        let selected = choose_peers(weight_array, &opts, &mut *weighted_choose)?;

        debug!(
            "LoadBasedSelector select peers: {:?}, namespace: {}, opts: {:?}.",
            selected, ns, opts,
        );

        Ok(selected)
    }
}

fn filter_out_expired_datanode(
    mut stat_kvs: HashMap<StatKey, StatValue>,
    lease_kvs: &HashMap<LeaseKey, LeaseValue>,
) -> HashMap<StatKey, StatValue> {
    lease_kvs
        .iter()
        .filter_map(|(lease_k, _)| stat_kvs.remove_entry(&lease_k.into()))
        .collect()
}

fn filter_out_datanode_by_table(
    stat_kvs: &HashMap<StatKey, StatValue>,
    leader_peer_ids: &[u64],
) -> HashMap<StatKey, StatValue> {
    stat_kvs
        .iter()
        .filter(|(stat_k, _)| leader_peer_ids.contains(&stat_k.node_id))
        .map(|(stat_k, stat_v)| (*stat_k, stat_v.clone()))
        .collect()
}

async fn get_leader_peer_ids(
    table_metadata_manager: &TableMetadataManager,
    table_id: TableId,
) -> Result<Vec<u64>> {
    table_metadata_manager
        .table_route_manager()
        .get(table_id)
        .await
        .context(error::TableMetadataManagerSnafu)
        .map(|route| {
            route.map_or_else(
                || Ok(Vec::new()),
                |route| {
                    let region_routes = route
                        .region_routes()
                        .context(error::UnexpectedLogicalRouteTableSnafu { err_msg: "" })?;
                    Ok(find_leaders(region_routes)
                        .into_iter()
                        .map(|peer| peer.id)
                        .collect())
                },
            )
        })?
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::keys::{LeaseKey, LeaseValue, StatKey, StatValue};
    use crate::selector::load_based::filter_out_expired_datanode;

    #[test]
    fn test_filter_out_expired_datanode() {
        let mut stat_kvs = HashMap::new();
        stat_kvs.insert(
            StatKey {
                cluster_id: 1,
                node_id: 0,
            },
            StatValue { stats: vec![] },
        );
        stat_kvs.insert(
            StatKey {
                cluster_id: 1,
                node_id: 1,
            },
            StatValue { stats: vec![] },
        );
        stat_kvs.insert(
            StatKey {
                cluster_id: 1,
                node_id: 2,
            },
            StatValue { stats: vec![] },
        );

        let mut lease_kvs = HashMap::new();
        lease_kvs.insert(
            LeaseKey {
                cluster_id: 1,
                node_id: 1,
            },
            LeaseValue {
                timestamp_millis: 0,
                node_addr: "127.0.0.1:3002".to_string(),
            },
        );

        let alive_stat_kvs = filter_out_expired_datanode(stat_kvs, &lease_kvs);

        assert_eq!(1, alive_stat_kvs.len());
        assert!(alive_stat_kvs
            .get(&StatKey {
                cluster_id: 1,
                node_id: 1
            })
            .is_some());
    }
}
