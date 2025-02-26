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
use snafu::ensure;

use crate::error::{NoEnoughAvailableNodeSnafu, Result};
use crate::lease;
use crate::metasrv::{SelectTarget, SelectorContext};
use crate::selector::{Namespace, Selector, SelectorOptions};

/// Round-robin selector that returns the next peer in the list in sequence.
/// Datanodes are ordered by their node_id.
///
/// This selector is useful when you want to distribute the load evenly across
/// all datanodes. But **it's not recommended** to use this selector in serious
/// production environments because it doesn't take into account the load of
/// each datanode.
pub struct RoundRobinSelector {
    select_target: SelectTarget,
    counter: AtomicUsize,
}

impl Default for RoundRobinSelector {
    fn default() -> Self {
        Self {
            select_target: SelectTarget::Datanode,
            counter: AtomicUsize::new(0),
        }
    }
}

impl RoundRobinSelector {
    pub fn new(select_target: SelectTarget) -> Self {
        Self {
            select_target,
            ..Default::default()
        }
    }

    async fn get_peers(
        &self,
        ns: Namespace,
        min_required_items: usize,
        ctx: &SelectorContext,
    ) -> Result<Vec<Peer>> {
        let mut peers = match self.select_target {
            SelectTarget::Datanode => {
                // 1. get alive datanodes.
                let lease_kvs =
                    lease::alive_datanodes(ns, &ctx.meta_peer_client, ctx.datanode_lease_secs)
                        .await?;

                // 2. map into peers
                lease_kvs
                    .into_iter()
                    .map(|(k, v)| Peer::new(k.node_id, v.node_addr))
                    .collect::<Vec<_>>()
            }
            SelectTarget::Flownode => {
                // 1. get alive flownodes.
                let lease_kvs =
                    lease::alive_flownodes(ns, &ctx.meta_peer_client, ctx.flownode_lease_secs)
                        .await?;

                // 2. map into peers
                lease_kvs
                    .into_iter()
                    .map(|(k, v)| Peer::new(k.node_id, v.node_addr))
                    .collect::<Vec<_>>()
            }
        };

        ensure!(
            !peers.is_empty(),
            NoEnoughAvailableNodeSnafu {
                required: min_required_items,
                available: 0usize,
                select_target: self.select_target
            }
        );

        // 3. sort by node id
        peers.sort_by_key(|p| p.id);

        Ok(peers)
    }
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
        let peers = self.get_peers(ns, opts.min_required_items, ctx).await?;
        // choose peers
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

#[cfg(test)]
mod test {
    use super::*;
    use crate::test_util::{create_selector_context, put_datanodes};

    #[tokio::test]
    async fn test_round_robin_selector() {
        let selector = RoundRobinSelector::default();
        let ctx = create_selector_context();
        let ns = 0;

        // add three nodes
        let peer1 = Peer {
            id: 2,
            addr: "node1".to_string(),
        };
        let peer2 = Peer {
            id: 5,
            addr: "node2".to_string(),
        };
        let peer3 = Peer {
            id: 8,
            addr: "node3".to_string(),
        };
        let peers = vec![peer1.clone(), peer2.clone(), peer3.clone()];
        put_datanodes(ns, &ctx.meta_peer_client, peers).await;

        let peers = selector
            .select(
                ns,
                &ctx,
                SelectorOptions {
                    min_required_items: 4,
                    allow_duplication: true,
                },
            )
            .await
            .unwrap();
        assert_eq!(peers.len(), 4);
        assert_eq!(
            peers,
            vec![peer1.clone(), peer2.clone(), peer3.clone(), peer1.clone()]
        );

        let peers = selector
            .select(
                ns,
                &ctx,
                SelectorOptions {
                    min_required_items: 2,
                    allow_duplication: true,
                },
            )
            .await
            .unwrap();
        assert_eq!(peers.len(), 2);
        assert_eq!(peers, vec![peer2.clone(), peer3.clone()]);
    }
}
