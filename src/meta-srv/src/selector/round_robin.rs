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

use std::sync::atomic::AtomicUsize;

use common_meta::peer::Peer;

use crate::error::Result;
use crate::lease;
use crate::metasrv::SelectorContext;
use crate::selector::{Namespace, Selector, SelectorOptions};

/// Round-robin selector that returns the next peer in the list in sequence.
///
/// The datanodes are ordered by their node_id.
#[derive(Default)]
pub struct RoundRobinSelector {
    counter: AtomicUsize,
}

#[async_trait::async_trait]
impl Selector for RoundRobinSelector {
    type Context = SelectorContext;
    type Output = Vec<Peer>;

    async fn select(
        &self,
        ns: Namespace,
        ctx: &Self::Context,
        opts: SelectorOptions,
    ) -> Result<Vec<Peer>> {
        // 1. get alive datanodes.
        let lease_kvs =
            lease::alive_datanodes(ns, &ctx.meta_peer_client, ctx.datanode_lease_secs).await?;

        // 2. map into peers and sort on node id
        let mut peers: Vec<Peer> = lease_kvs
            .into_iter()
            .map(|(k, v)| Peer::new(k.node_id, v.node_addr))
            .collect();
        peers.sort_by_key(|p| p.id);

        // 3. choose peers
        let mut selected = Vec::with_capacity(opts.min_required_items);
        for _ in 0..opts.min_required_items {
            let idx = self
                .counter
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                % peers.len();
            selected.push(peers[idx].clone());
        }

        Ok(selected)
    }
}
