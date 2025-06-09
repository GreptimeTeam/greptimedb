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

use std::collections::HashSet;
use std::sync::Arc;

use common_meta::peer::Peer;

use crate::error::Result;
use crate::lease;
use crate::metasrv::SelectorContext;
use crate::node_excluder::NodeExcluderRef;
use crate::selector::common::{choose_items, filter_out_excluded_peers};
use crate::selector::weighted_choose::{RandomWeightedChoose, WeightedItem};
use crate::selector::{Selector, SelectorOptions};

/// Select all alive datanodes based using a random weighted choose.
pub struct LeaseBasedSelector {
    node_excluder: NodeExcluderRef,
}

impl LeaseBasedSelector {
    pub fn new(node_excluder: NodeExcluderRef) -> Self {
        Self { node_excluder }
    }
}

impl Default for LeaseBasedSelector {
    fn default() -> Self {
        Self {
            node_excluder: Arc::new(Vec::new()),
        }
    }
}

#[async_trait::async_trait]
impl Selector for LeaseBasedSelector {
    type Context = SelectorContext;
    type Output = Vec<Peer>;

    async fn select(&self, ctx: &Self::Context, opts: SelectorOptions) -> Result<Self::Output> {
        // 1. get alive datanodes.
        let lease_kvs = lease::alive_datanodes(&ctx.meta_peer_client, ctx.datanode_lease_secs)
            .with_condition(lease::is_datanode_accept_ingest_workload)
            .await?;

        // 2. compute weight array, but the weight of each item is the same.
        let mut weight_array = lease_kvs
            .into_iter()
            .map(|(k, v)| WeightedItem {
                item: Peer {
                    id: k.node_id,
                    addr: v.node_addr.clone(),
                },
                weight: 1.0,
            })
            .collect();

        // 3. choose peers by weight_array.
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

        Ok(selected)
    }
}
