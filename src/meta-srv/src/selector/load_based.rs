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

use common_meta::datanode::{DatanodeStatKey, DatanodeStatValue};
use common_meta::peer::Peer;
use common_telemetry::debug;
use snafu::ResultExt;

use crate::cluster::MetaPeerClientRef;
use crate::error::{ListActiveDatanodesSnafu, Result};
use crate::metasrv::SelectorContext;
use crate::selector::common::{choose_items, filter_out_excluded_peers};
use crate::selector::weight_compute::WeightCompute;
use crate::selector::weighted_choose::RandomWeightedChoose;
use crate::selector::{Selector, SelectorOptions};

pub struct LoadBasedSelector<C> {
    weight_compute: C,
    meta_peer_client: MetaPeerClientRef,
}

impl<C> LoadBasedSelector<C> {
    pub fn new(weight_compute: C, meta_peer_client: MetaPeerClientRef) -> Self {
        Self {
            weight_compute,
            meta_peer_client,
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
        let alive_datanodes = ctx
            .peer_discovery
            .active_datanodes(opts.workload_filter)
            .await
            .context(ListActiveDatanodesSnafu)?;

        // 2. get stat kvs and filter out expired datanodes.
        let stat_keys = alive_datanodes
            .iter()
            .map(|k| DatanodeStatKey { node_id: k.id })
            .collect();
        let stat_kvs = filter_out_expired_datanode(
            self.meta_peer_client.get_dn_stat_kvs(stat_keys).await?,
            &alive_datanodes,
        );

        // 3. compute weight array.
        let mut weight_array = self.weight_compute.compute(&stat_kvs);

        // 4. filter out excluded peers.
        filter_out_excluded_peers(&mut weight_array, &opts.exclude_peer_ids);
        // 5. choose peers by weight_array.
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
    datanodes: &[Peer],
) -> HashMap<DatanodeStatKey, DatanodeStatValue> {
    datanodes
        .iter()
        .filter_map(|p| stat_kvs.remove_entry(&DatanodeStatKey { node_id: p.id }))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use common_meta::datanode::{DatanodeStatKey, DatanodeStatValue};
    use common_meta::peer::Peer;

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

        let lease_kvs = vec![Peer::new(1, "127.0.0.1:3002".to_string())];
        let alive_stat_kvs = filter_out_expired_datanode(stat_kvs, &lease_kvs);

        assert_eq!(1, alive_stat_kvs.len());
        assert!(alive_stat_kvs.contains_key(&DatanodeStatKey { node_id: 1 }));
    }
}
