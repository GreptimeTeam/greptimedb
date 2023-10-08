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

use api::v1::meta::Peer;
use common_meta::key::TableMetadataManager;
use common_meta::rpc::router::find_leaders;
use common_telemetry::warn;
use snafu::ResultExt;
use table::metadata::TableId;

use crate::error::{self, Result};
use crate::keys::{LeaseKey, LeaseValue, StatKey};
use crate::lease;
use crate::metasrv::SelectorContext;
use crate::selector::{Namespace, Selector};
use crate::service::store::kv::KvBackendAdapter;

const MAX_REGION_NUMBER: u64 = u64::MAX;

pub struct LoadBasedSelector;

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
            route.map_or_else(Vec::new, |route| {
                find_leaders(&route.inner.region_routes)
                    .into_iter()
                    .map(|peer| peer.id)
                    .collect()
            })
        })
}

#[async_trait::async_trait]
impl Selector for LoadBasedSelector {
    type Context = SelectorContext;
    type Output = Vec<Peer>;

    async fn select(&self, ns: Namespace, ctx: &Self::Context) -> Result<Self::Output> {
        // get alive datanodes
        let lease_kvs =
            lease::alive_datanodes(ns, &ctx.meta_peer_client, ctx.datanode_lease_secs).await?;
        if lease_kvs.is_empty() {
            return Ok(vec![]);
        }

        let stat_keys: Vec<StatKey> = lease_kvs.keys().map(|k| k.into()).collect();
        let stat_kvs = ctx.meta_peer_client.get_dn_stat_kvs(stat_keys).await?;

        let leader_peer_ids = if let Some(table_id) = ctx.table_id {
            let table_metadata_manager =
                TableMetadataManager::new(KvBackendAdapter::wrap(ctx.kv_store.clone()));

            get_leader_peer_ids(&table_metadata_manager, table_id).await?
        } else {
            Vec::new()
        };

        let mut tuples: Vec<(LeaseKey, LeaseValue, u64)> = lease_kvs
            .into_iter()
            .filter(|(lease_k, _)| !leader_peer_ids.contains(&lease_k.node_id))
            .map(|(lease_k, lease_v)| {
                let stat_key: StatKey = (&lease_k).into();
                let region_num = match stat_kvs
                    .get(&stat_key)
                    .and_then(|stat_val| stat_val.region_num())
                {
                    Some(region_num) => region_num,
                    None => {
                        warn!("Failed to get stat_val by stat_key {:?}", stat_key);
                        MAX_REGION_NUMBER
                    }
                };

                (lease_k, lease_v, region_num)
            })
            .collect();

        // sort the datanodes according to the number of regions
        tuples.sort_by(|a, b| a.2.cmp(&b.2));

        Ok(tuples
            .into_iter()
            .map(|(lease_key, lease_val, _)| Peer {
                id: lease_key.node_id,
                addr: lease_val.node_addr,
            })
            .collect())
    }
}
