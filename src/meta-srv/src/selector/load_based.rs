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
use std::sync::Arc;

use common_meta::datanode::{DatanodeStatKey, DatanodeStatValue};
use common_meta::key::TableMetadataManager;
use common_meta::peer::Peer;
use common_meta::rpc::router::find_leaders;
use common_telemetry::{debug, info};
use snafu::ResultExt;
use table::metadata::TableId;

use crate::error::{self, Result};
use crate::key::{DatanodeLeaseKey, LeaseValue};
use crate::lease;
use crate::metasrv::SelectorContext;
use crate::node_excluder::NodeExcluderRef;
use crate::selector::common::{choose_items, filter_out_excluded_peers};
use crate::selector::weight_compute::{RegionNumsBasedWeightCompute, WeightCompute};
use crate::selector::weighted_choose::RandomWeightedChoose;
use crate::selector::{Selector, SelectorOptions};

pub struct LoadBasedSelector<C> {
    weight_compute: C,
    node_excluder: NodeExcluderRef,
}

impl<C> LoadBasedSelector<C> {
    pub fn new(weight_compute: C, node_excluder: NodeExcluderRef) -> Self {
        Self {
            weight_compute,
            node_excluder,
        }
    }
}

impl Default for LoadBasedSelector<RegionNumsBasedWeightCompute> {
    fn default() -> Self {
        Self {
            weight_compute: RegionNumsBasedWeightCompute,
            node_excluder: Arc::new(Vec::new()),
        }
    }
}

#[async_trait::async_trait]
impl<C> Selector for LoadBasedSelector<C>
where
    C: WeightCompute<Source = HashMap<DatanodeStatKey, DatanodeStatValue>>,
{
    type Context = SelectorContext;
    type Output = Vec<Peer>;

    async fn select(&self, ctx: &Self::Context, opts: SelectorOptions) -> Result<Self::Output> {
        // 1. get alive datanodes.
        let lease_kvs = lease::alive_datanodes(&ctx.meta_peer_client, ctx.datanode_lease_secs)
            .with_condition(lease::is_datanode_accept_ingest_workload)
            .await?;

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
        let mut weight_array = self.weight_compute.compute(&stat_kvs);

        // 5. choose peers by weight_array.
        let mut exclude_peer_ids = self
            .node_excluder
            .excluded_datanode_ids()
            .iter()
            .cloned()
            .collect::<HashSet<_>>();
        exclude_peer_ids.extend(opts.exclude_peer_ids.iter());
        filter_out_excluded_peers(&mut weight_array, &exclude_peer_ids);
        let mut weighted_choose = RandomWeightedChoose::new(weight_array);
        let selected = choose_items(&opts, &mut weighted_choose)?;

        debug!(
            "LoadBasedSelector select peers: {:?}, opts: {:?}.",
            selected, opts,
        );

        Ok(selected)
    }
}

fn filter_out_expired_datanode(
    mut stat_kvs: HashMap<DatanodeStatKey, DatanodeStatValue>,
    lease_kvs: &HashMap<DatanodeLeaseKey, LeaseValue>,
) -> HashMap<DatanodeStatKey, DatanodeStatValue> {
    lease_kvs
        .iter()
        .filter_map(|(lease_k, _)| stat_kvs.remove_entry(&lease_k.into()))
        .collect()
}

fn filter_out_datanode_by_table(
    stat_kvs: &HashMap<DatanodeStatKey, DatanodeStatValue>,
    leader_peer_ids: &[u64],
) -> HashMap<DatanodeStatKey, DatanodeStatValue> {
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
        .table_route_storage()
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

    use api::v1::meta::heartbeat_request::NodeWorkloads;
    use api::v1::meta::DatanodeWorkloads;
    use common_meta::datanode::{DatanodeStatKey, DatanodeStatValue};
    use common_workload::DatanodeWorkloadType;

    use crate::key::{DatanodeLeaseKey, LeaseValue};
    use crate::selector::load_based::filter_out_expired_datanode;

    #[test]
    fn test_filter_out_expired_datanode() {
        let mut stat_kvs = HashMap::new();
        stat_kvs.insert(
            DatanodeStatKey { node_id: 0 },
            DatanodeStatValue { stats: vec![] },
        );
        stat_kvs.insert(
            DatanodeStatKey { node_id: 1 },
            DatanodeStatValue { stats: vec![] },
        );
        stat_kvs.insert(
            DatanodeStatKey { node_id: 2 },
            DatanodeStatValue { stats: vec![] },
        );

        let mut lease_kvs = HashMap::new();
        lease_kvs.insert(
            DatanodeLeaseKey { node_id: 1 },
            LeaseValue {
                timestamp_millis: 0,
                node_addr: "127.0.0.1:3002".to_string(),
                workloads: NodeWorkloads::Datanode(DatanodeWorkloads {
                    types: vec![DatanodeWorkloadType::Hybrid.to_i32()],
                }),
            },
        );

        let alive_stat_kvs = filter_out_expired_datanode(stat_kvs, &lease_kvs);

        assert_eq!(1, alive_stat_kvs.len());
        assert!(alive_stat_kvs.contains_key(&DatanodeStatKey { node_id: 1 }));
    }
}
