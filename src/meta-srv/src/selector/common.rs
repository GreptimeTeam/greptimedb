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

use common_meta::peer::Peer;
use snafu::ensure;

use crate::error;
use crate::error::Result;
use crate::metasrv::SelectTarget;
use crate::selector::SelectorOptions;
use crate::selector::weighted_choose::{WeightedChoose, WeightedItem};

/// Filter out the excluded peers from the `weight_array`.
pub fn filter_out_excluded_peers(
    weight_array: &mut Vec<WeightedItem<Peer>>,
    exclude_peer_ids: &HashSet<u64>,
) {
    weight_array.retain(|peer| !exclude_peer_ids.contains(&peer.item.id));
}

/// According to the `opts`, choose peers from the `weight_array` through `weighted_choose`.
pub fn choose_items<W>(opts: &SelectorOptions, weighted_choose: &mut W) -> Result<Vec<Peer>>
where
    W: WeightedChoose<Peer>,
{
    let min_required_items = opts.min_required_items;
    ensure!(
        !weighted_choose.is_empty(),
        error::NoEnoughAvailableNodeSnafu {
            required: min_required_items,
            available: 0_usize,
            select_target: SelectTarget::Datanode
        }
    );

    if min_required_items == 1 {
        // fast path
        return Ok(vec![weighted_choose.choose_one()?]);
    }

    let available_count = weighted_choose.len();

    if opts.allow_duplication {
        // Calculate how many complete rounds of `available_count` items to select,
        // plus any additional items needed after complete rounds.
        let complete_batches = min_required_items / available_count;
        let leftover_items = min_required_items % available_count;
        if complete_batches == 0 {
            return weighted_choose.choose_multiple(leftover_items);
        }

        let mut result = Vec::with_capacity(min_required_items);
        for _ in 0..complete_batches {
            result.extend(weighted_choose.choose_multiple(available_count)?);
        }
        result.extend(weighted_choose.choose_multiple(leftover_items)?);

        Ok(result)
    } else {
        // Ensure the available items are sufficient when duplication is not allowed.
        ensure!(
            available_count >= min_required_items,
            error::NoEnoughAvailableNodeSnafu {
                required: min_required_items,
                available: available_count,
                select_target: SelectTarget::Datanode
            }
        );

        weighted_choose.choose_multiple(min_required_items)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use api::v1::meta::DatanodeWorkloads;
    use api::v1::meta::heartbeat_request::NodeWorkloads;
    use common_meta::affinity::{Affinity, AffinityFallback, AffinityRequirements, NoopAffinity};
    use common_meta::cluster::{DatanodeStatus, NodeInfo, NodeStatus};
    use common_meta::error::Result as MetaResult;
    use common_meta::peer::{Peer, PeerDiscovery};

    use crate::discovery::utils::accept_ingest_workload;
    use crate::metasrv::{SelectTarget, SelectorContext, SelectorRef};
    use crate::selector::common::{choose_items, filter_out_excluded_peers};
    use crate::selector::lease_based::LeaseBasedSelector;
    use crate::selector::round_robin::RoundRobinSelector;
    use crate::selector::weighted_choose::{RandomWeightedChoose, WeightedItem};
    use crate::selector::{Selector, SelectorOptions};

    #[test]
    fn test_choose_peers() {
        let weight_array = vec![
            WeightedItem {
                item: Peer {
                    id: 1,
                    addr: "127.0.0.1:3001".to_string(),
                },
                weight: 1.0,
            },
            WeightedItem {
                item: Peer {
                    id: 2,
                    addr: "127.0.0.1:3001".to_string(),
                },
                weight: 1.0,
            },
            WeightedItem {
                item: Peer {
                    id: 3,
                    addr: "127.0.0.1:3001".to_string(),
                },
                weight: 1.0,
            },
            WeightedItem {
                item: Peer {
                    id: 4,
                    addr: "127.0.0.1:3001".to_string(),
                },
                weight: 1.0,
            },
            WeightedItem {
                item: Peer {
                    id: 5,
                    addr: "127.0.0.1:3001".to_string(),
                },
                weight: 1.0,
            },
        ];

        for i in 1..=5 {
            let opts = SelectorOptions {
                affinity: None,
                min_required_items: i,
                allow_duplication: false,
                exclude_peer_ids: HashSet::new(),
                workload_filter: None,
            };

            let selected_peers: HashSet<_> =
                choose_items(&opts, &mut RandomWeightedChoose::new(weight_array.clone()))
                    .unwrap()
                    .into_iter()
                    .collect();

            assert_eq!(i, selected_peers.len());
        }

        let opts = SelectorOptions {
            affinity: None,
            min_required_items: 6,
            allow_duplication: false,
            exclude_peer_ids: HashSet::new(),
            workload_filter: None,
        };

        let selected_result =
            choose_items(&opts, &mut RandomWeightedChoose::new(weight_array.clone()));
        assert!(selected_result.is_err());

        for i in 1..=50 {
            let opts = SelectorOptions {
                affinity: None,
                min_required_items: i,
                allow_duplication: true,
                exclude_peer_ids: HashSet::new(),
                workload_filter: None,
            };

            let selected_peers =
                choose_items(&opts, &mut RandomWeightedChoose::new(weight_array.clone())).unwrap();

            assert_eq!(i, selected_peers.len());
        }
    }

    #[test]
    fn test_filter_out_excluded_peers() {
        let mut weight_array = vec![
            WeightedItem {
                item: Peer {
                    id: 1,
                    addr: "127.0.0.1:3001".to_string(),
                },
                weight: 1.0,
            },
            WeightedItem {
                item: Peer {
                    id: 2,
                    addr: "127.0.0.1:3002".to_string(),
                },
                weight: 1.0,
            },
        ];

        let exclude_peer_ids = HashSet::from([1]);
        filter_out_excluded_peers(&mut weight_array, &exclude_peer_ids);

        assert_eq!(weight_array.len(), 1);
        assert_eq!(weight_array[0].item.id, 2);
    }

    struct ExactAffinity;

    impl Affinity for ExactAffinity {
        fn matches(&self, requirements: &AffinityRequirements, candidate: &NodeInfo) -> bool {
            requirements
                .attributes()
                .iter()
                .all(|(key, value)| candidate.env_vars.get(key) == Some(value))
        }
    }

    fn requirements() -> AffinityRequirements {
        AffinityRequirements::new(HashMap::from([("pool".into(), "a".into())])).unwrap()
    }

    fn node(id: u64, pool: Option<&str>, accepts_ingest: bool) -> NodeInfo {
        NodeInfo {
            peer: Peer::empty(id),
            last_activity_ts: 0,
            status: NodeStatus::Datanode(DatanodeStatus {
                rcus: 0,
                wcus: 0,
                leader_regions: 0,
                follower_regions: 0,
                workloads: DatanodeWorkloads {
                    types: if accepts_ingest { vec![0] } else { vec![] },
                },
            }),
            version: String::new(),
            git_commit: String::new(),
            start_time_ms: 0,
            total_cpu_millicores: 0,
            total_memory_bytes: 0,
            cpu_usage_millicores: 0,
            memory_usage_bytes: 0,
            hostname: String::new(),
            env_vars: pool
                .map(|pool| HashMap::from([("pool".into(), pool.into())]))
                .unwrap_or_default(),
        }
    }

    struct Discovery {
        snapshots: Vec<Vec<NodeInfo>>,
        fail: bool,
        reads: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl PeerDiscovery for Discovery {
        async fn active_frontends(&self) -> MetaResult<Vec<NodeInfo>> {
            Ok(vec![])
        }

        async fn active_datanodes(
            &self,
            filter: Option<for<'a> fn(&'a NodeWorkloads) -> bool>,
        ) -> MetaResult<Vec<NodeInfo>> {
            let index = self.reads.fetch_add(1, Ordering::Relaxed);
            if self.fail {
                return common_meta::error::UnexpectedSnafu {
                    err_msg: "discovery unavailable",
                }
                .fail();
            }
            let index = index.min(self.snapshots.len() - 1);
            Ok(self.snapshots[index]
                .iter()
                .filter(|node| {
                    let NodeStatus::Datanode(status) = &node.status else {
                        unreachable!()
                    };
                    filter.is_none_or(|filter| {
                        filter(&NodeWorkloads::Datanode(status.workloads.clone()))
                    })
                })
                .cloned()
                .collect())
        }

        async fn active_flownodes(
            &self,
            filter: Option<for<'a> fn(&'a NodeWorkloads) -> bool>,
        ) -> MetaResult<Vec<NodeInfo>> {
            self.active_datanodes(filter).await
        }
    }

    #[tokio::test]
    async fn test_affinity_constrains_base_selectors() {
        for lease_based in [false, true] {
            for (use_exact_affinity, has_requirements) in
                [(true, false), (true, true), (false, true)]
            {
                let enabled = use_exact_affinity && has_requirements;
                let selector: SelectorRef = if lease_based {
                    Arc::new(LeaseBasedSelector)
                } else {
                    Arc::new(RoundRobinSelector::new(SelectTarget::Datanode))
                };
                let ctx = SelectorContext {
                    affinity_fallback: AffinityFallback::Error,
                    affinity: if use_exact_affinity {
                        Arc::new(ExactAffinity)
                    } else {
                        Arc::new(NoopAffinity)
                    },
                    peer_discovery: Arc::new(Discovery {
                        snapshots: vec![vec![
                            node(1, Some("b"), true),
                            node(2, Some("a"), true),
                            node(3, Some("a"), true),
                            node(4, None, true),
                            node(5, Some("a"), false),
                        ]],
                        reads: AtomicUsize::new(0),
                        fail: false,
                    }),
                };
                // Selecting a complete round makes the lease-based result deterministic as a set.
                let peers = selector
                    .select(
                        &ctx,
                        SelectorOptions {
                            affinity: has_requirements.then(requirements),
                            min_required_items: if enabled { 1 } else { 3 },
                            exclude_peer_ids: HashSet::from([3]),
                            workload_filter: Some(accept_ingest_workload),
                            ..Default::default()
                        },
                    )
                    .await
                    .unwrap();
                let ids = peers
                    .into_iter()
                    .map(|peer| peer.id)
                    .collect::<HashSet<_>>();
                assert_eq!(
                    ids,
                    if enabled {
                        HashSet::from([2])
                    } else {
                        HashSet::from([1, 2, 4])
                    }
                );
            }
        }
    }

    #[tokio::test]
    async fn test_round_robin_filters_each_discovery_read_for_both_targets() {
        for target in [SelectTarget::Datanode, SelectTarget::Flownode] {
            let discovery = Arc::new(Discovery {
                snapshots: vec![
                    vec![node(1, Some("b"), true), node(2, Some("a"), true)],
                    vec![
                        node(1, Some("b"), true),
                        node(2, Some("a"), true),
                        node(3, None, true),
                    ],
                ],
                reads: AtomicUsize::new(0),
                fail: false,
            });
            let ctx = SelectorContext {
                affinity_fallback: AffinityFallback::Error,
                peer_discovery: discovery.clone(),
                affinity: Arc::new(ExactAffinity),
            };
            let selector = RoundRobinSelector::new(target);
            let opts = SelectorOptions {
                affinity: Some(requirements()),
                ..Default::default()
            };
            for _ in 0..2 {
                assert_eq!(
                    selector.select(&ctx, opts.clone()).await.unwrap(),
                    vec![Peer::empty(2)]
                );
            }
            assert_eq!(discovery.reads.load(Ordering::Relaxed), 2);
        }
    }

    #[tokio::test]
    async fn test_round_robin_excludes_peers_for_both_targets() {
        for target in [SelectTarget::Datanode, SelectTarget::Flownode] {
            let ctx = SelectorContext {
                affinity_fallback: AffinityFallback::Error,
                peer_discovery: Arc::new(Discovery {
                    snapshots: vec![vec![node(1, Some("a"), true), node(2, Some("a"), true)]],
                    reads: AtomicUsize::new(0),
                    fail: false,
                }),
                affinity: Arc::new(ExactAffinity),
            };
            for affinity in [None, Some(requirements())] {
                // Keep the topology fixed and vary only the exclusion set.
                for (excluded, expected) in [(vec![], vec![1, 2]), (vec![1], vec![2, 2])] {
                    let selector = RoundRobinSelector::new(target);
                    let peers = selector
                        .select(
                            &ctx,
                            SelectorOptions {
                                affinity: affinity.clone(),
                                min_required_items: 2,
                                allow_duplication: true,
                                exclude_peer_ids: excluded.into_iter().collect(),
                                ..Default::default()
                            },
                        )
                        .await
                        .unwrap();
                    assert_eq!(
                        peers.into_iter().map(|peer| peer.id).collect::<Vec<_>>(),
                        expected
                    );
                }
                let selector = RoundRobinSelector::new(target);
                let error = selector
                    .select(
                        &ctx,
                        SelectorOptions {
                            affinity,
                            exclude_peer_ids: HashSet::from([1, 2]),
                            ..Default::default()
                        },
                    )
                    .await
                    .unwrap_err();
                assert!(matches!(
                    error,
                    crate::error::Error::NoEnoughAvailableNode { available: 0, .. }
                ));
            }
        }
    }

    #[tokio::test]
    async fn test_base_selector_affinity_fallback() {
        for lease_based in [false, true] {
            for fallback in [AffinityFallback::Error, AffinityFallback::Unconstrained] {
                for has_requirements in [false, true] {
                    for required in [2, 3] {
                        let discovery = Arc::new(Discovery {
                            snapshots: vec![vec![
                                node(1, Some("a"), true),
                                node(2, Some("b"), true),
                                node(3, Some("a"), true),
                                node(4, Some("a"), false),
                            ]],
                            reads: AtomicUsize::new(0),
                            fail: false,
                        });
                        let ctx = SelectorContext {
                            peer_discovery: discovery.clone(),
                            affinity: Arc::new(ExactAffinity),
                            affinity_fallback: fallback,
                        };
                        let selector: SelectorRef = if lease_based {
                            Arc::new(LeaseBasedSelector)
                        } else {
                            Arc::new(RoundRobinSelector::new(SelectTarget::Datanode))
                        };
                        let result = selector
                            .select(
                                &ctx,
                                SelectorOptions {
                                    affinity: has_requirements.then(requirements),
                                    min_required_items: required,
                                    allow_duplication: false,
                                    exclude_peer_ids: HashSet::from([3]),
                                    workload_filter: Some(accept_ingest_workload),
                                },
                            )
                            .await;
                        let retries =
                            has_requirements && fallback == AffinityFallback::Unconstrained;
                        assert_eq!(
                            discovery.reads.load(Ordering::Relaxed),
                            1 + usize::from(retries)
                        );
                        let constrained = has_requirements && !retries;
                        if required == 2 && !constrained {
                            assert_eq!(
                                result
                                    .unwrap()
                                    .into_iter()
                                    .map(|peer| peer.id)
                                    .collect::<HashSet<_>>(),
                                HashSet::from([1, 2])
                            );
                        } else {
                            let available = if constrained { 1 } else { 2 };
                            assert!(
                                matches!(result.unwrap_err(), crate::error::Error::NoEnoughAvailableNode {
                                required: actual_required, available: actual_available, ..
                            } if actual_required == required && actual_available == available)
                            );
                        }
                    }
                }
            }
        }
    }

    #[tokio::test]
    async fn test_base_selector_discovery_error_does_not_fallback() {
        for lease_based in [false, true] {
            let discovery = Arc::new(Discovery {
                snapshots: vec![],
                reads: AtomicUsize::new(0),
                fail: true,
            });
            let ctx = SelectorContext {
                peer_discovery: discovery.clone(),
                affinity: Arc::new(ExactAffinity),
                affinity_fallback: AffinityFallback::Unconstrained,
            };
            let selector: SelectorRef = if lease_based {
                Arc::new(LeaseBasedSelector)
            } else {
                Arc::new(RoundRobinSelector::new(SelectTarget::Datanode))
            };
            let error = selector
                .select(
                    &ctx,
                    SelectorOptions {
                        affinity: Some(requirements()),
                        ..Default::default()
                    },
                )
                .await
                .unwrap_err();
            assert!(matches!(
                error,
                crate::error::Error::ListActiveDatanodes { .. }
            ));
            assert_eq!(discovery.reads.load(Ordering::Relaxed), 1);
        }
    }
}
