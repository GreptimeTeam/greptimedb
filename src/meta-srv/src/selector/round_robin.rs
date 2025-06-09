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
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use common_meta::peer::Peer;
use snafu::ensure;

use crate::error::{NoEnoughAvailableNodeSnafu, Result};
use crate::lease;
use crate::metasrv::{SelectTarget, SelectorContext};
use crate::node_excluder::NodeExcluderRef;
use crate::selector::{Selector, SelectorOptions};

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
    node_excluder: NodeExcluderRef,
}

impl Default for RoundRobinSelector {
    fn default() -> Self {
        Self {
            select_target: SelectTarget::Datanode,
            counter: AtomicUsize::new(0),
            node_excluder: Arc::new(Vec::new()),
        }
    }
}

impl RoundRobinSelector {
    pub fn new(select_target: SelectTarget, node_excluder: NodeExcluderRef) -> Self {
        Self {
            select_target,
            node_excluder,
            ..Default::default()
        }
    }

    async fn get_peers(&self, opts: &SelectorOptions, ctx: &SelectorContext) -> Result<Vec<Peer>> {
        let mut peers = match self.select_target {
            SelectTarget::Datanode => {
                // 1. get alive datanodes.
                let lease_kvs =
                    lease::alive_datanodes(&ctx.meta_peer_client, ctx.datanode_lease_secs)
                        .with_condition(lease::is_datanode_accept_ingest_workload)
                        .await?;

                let mut exclude_peer_ids = self
                    .node_excluder
                    .excluded_datanode_ids()
                    .iter()
                    .cloned()
                    .collect::<HashSet<_>>();
                exclude_peer_ids.extend(opts.exclude_peer_ids.iter());
                // 2. map into peers
                lease_kvs
                    .into_iter()
                    .filter(|(k, _)| !exclude_peer_ids.contains(&k.node_id))
                    .map(|(k, v)| Peer::new(k.node_id, v.node_addr))
                    .collect::<Vec<_>>()
            }
            SelectTarget::Flownode => {
                // 1. get alive flownodes.
                let lease_kvs =
                    lease::alive_flownodes(&ctx.meta_peer_client, ctx.flownode_lease_secs).await?;

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
                required: opts.min_required_items,
                available: peers.len(),
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

    async fn select(&self, ctx: &Self::Context, opts: SelectorOptions) -> Result<Vec<Peer>> {
        let peers = self.get_peers(&opts, ctx).await?;
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
    use std::collections::HashSet;

    use super::*;
    use crate::test_util::{create_selector_context, put_datanodes};

    #[tokio::test]
    async fn test_round_robin_selector() {
        let selector = RoundRobinSelector::default();
        let ctx = create_selector_context();
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
        put_datanodes(&ctx.meta_peer_client, peers).await;

        let peers = selector
            .select(
                &ctx,
                SelectorOptions {
                    min_required_items: 4,
                    allow_duplication: true,
                    exclude_peer_ids: HashSet::new(),
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
                &ctx,
                SelectorOptions {
                    min_required_items: 2,
                    allow_duplication: true,
                    exclude_peer_ids: HashSet::new(),
                },
            )
            .await
            .unwrap();
        assert_eq!(peers.len(), 2);
        assert_eq!(peers, vec![peer2.clone(), peer3.clone()]);
    }

    #[tokio::test]
    async fn test_round_robin_selector_with_exclude_peer_ids() {
        let selector = RoundRobinSelector::new(SelectTarget::Datanode, Arc::new(vec![5]));
        let ctx = create_selector_context();
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
        put_datanodes(
            &ctx.meta_peer_client,
            vec![peer1.clone(), peer2.clone(), peer3.clone()],
        )
        .await;

        let peers = selector
            .select(
                &ctx,
                SelectorOptions {
                    min_required_items: 1,
                    allow_duplication: true,
                    exclude_peer_ids: HashSet::from([2]),
                },
            )
            .await
            .unwrap();
        assert_eq!(peers.len(), 1);
        assert_eq!(peers, vec![peer3.clone()]);
    }
}
