// Copyright 2022 Greptime Team
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

use api::v1::meta::Peer;
use common_time::util as time_util;

use crate::error::Result;
use crate::keys::{LeaseKey, LeaseValue};
use crate::lease;
use crate::metasrv::Context;
use crate::selector::{Namespace, Selector};

pub struct LeaseBasedSelector;

#[async_trait::async_trait]
impl Selector for LeaseBasedSelector {
    type Context = Context;
    type Output = Vec<Peer>;

    async fn select(&self, ns: Namespace, ctx: &Self::Context) -> Result<Self::Output> {
        // filter out the nodes out lease
        let lease_filter = |_: &LeaseKey, v: &LeaseValue| {
            time_util::current_time_millis() - v.timestamp_millis < ctx.datanode_lease_secs * 1000
        };
        let mut lease_kvs = lease::alive_datanodes(ns, &ctx.kv_store, lease_filter).await?;
        // TODO(jiachun): At the moment we are just pushing the latest to the forefront,
        // and it is better to use load-based strategies in the future.
        lease_kvs.sort_by(|a, b| b.1.timestamp_millis.cmp(&a.1.timestamp_millis));

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
