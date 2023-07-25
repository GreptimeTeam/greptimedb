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
use rand::seq::SliceRandom;
use rand::thread_rng;

use crate::error::Result;
use crate::lease;
use crate::metasrv::SelectorContext;
use crate::selector::{Namespace, Selector};

pub struct LeaseBasedSelector;

#[async_trait::async_trait]
impl Selector for LeaseBasedSelector {
    type Context = SelectorContext;
    type Output = Vec<Peer>;

    async fn select(&self, ns: Namespace, ctx: &Self::Context) -> Result<Self::Output> {
        // filter out the nodes out lease
        let mut lease_kvs: Vec<_> =
            lease::alive_datanodes(ns, &ctx.meta_peer_client, ctx.datanode_lease_secs)
                .await?
                .into_iter()
                .collect();

        lease_kvs.shuffle(&mut thread_rng());

        let peers = lease_kvs
            .into_iter()
            .map(|(k, v)| Peer {
                id: k.node_id,
                addr: v.node_addr,
            })
            .collect::<Vec<_>>();

        Ok(peers)
    }
}
