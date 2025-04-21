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

use common_meta::peer::Peer;

use crate::error::Result;
use crate::lease;
use crate::metasrv::SelectorContext;
use crate::selector::common::choose_items;
use crate::selector::weighted_choose::{RandomWeightedChoose, WeightedItem};
use crate::selector::{Selector, SelectorOptions};

/// Select all alive datanodes based using a random weighted choose.
pub struct LeaseBasedSelector;

#[async_trait::async_trait]
impl Selector for LeaseBasedSelector {
    type Context = SelectorContext;
    type Output = Vec<Peer>;

    async fn select(&self, ctx: &Self::Context, opts: SelectorOptions) -> Result<Self::Output> {
        // 1. get alive datanodes.
        let lease_kvs =
            lease::alive_datanodes(&ctx.meta_peer_client, ctx.datanode_lease_secs).await?;

        // 2. compute weight array, but the weight of each item is the same.
        let weight_array = lease_kvs
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
        let mut weighted_choose = RandomWeightedChoose::new(weight_array);
        let selected = choose_items(&opts, &mut weighted_choose)?;

        Ok(selected)
    }
}
