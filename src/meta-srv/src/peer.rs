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

use async_trait::async_trait;
use common_error::ext::BoxedError;
use common_meta::error::{ExternalSnafu, Result as MetaResult};
use common_meta::peer::{Peer, PeerAllocContext, PeerAllocator};
use snafu::{ResultExt, ensure};

use crate::discovery::utils::accept_ingest_workload;
use crate::error::{self, Result, TooManyPartitionsSnafu};
use crate::metasrv::{SelectTarget, SelectorContext, SelectorRef};
use crate::selector::SelectorOptions;

pub struct MetasrvPeerAllocator {
    ctx: SelectorContext,
    selector: SelectorRef,
    max_items: Option<u32>,
}

impl MetasrvPeerAllocator {
    /// Creates a new [`MetasrvPeerAllocator`] with the given [`SelectorContext`] and [`SelectorRef`].
    pub fn new(ctx: SelectorContext, selector: SelectorRef) -> Self {
        Self {
            ctx,
            selector,
            max_items: None,
        }
    }

    pub fn with_max_items(self, max_items: u32) -> Self {
        Self {
            ctx: self.ctx,
            selector: self.selector,
            max_items: Some(max_items),
        }
    }

    /// Allocates a specified number (by `regions`) of [`Peer`] instances based on the number of
    /// regions. The returned peers will have the same length as the number of regions.
    ///
    /// This method is mainly a wrapper around the [`SelectorRef`]::`select` method. There is
    /// no guarantee that how the returned peers are used, like whether they are from the same
    /// table or not. So this method isn't idempotent.
    async fn alloc(
        &self,
        min_required_items: usize,
        alloc_context: &PeerAllocContext,
    ) -> Result<Vec<Peer>> {
        if let Some(max_items) = self.max_items {
            ensure!(
                min_required_items <= max_items as usize,
                TooManyPartitionsSnafu
            );
        }

        let options = SelectorOptions {
            affinity: alloc_context.affinity.as_ref(),
            min_required_items,
            allow_duplication: true,
            exclude_peer_ids: HashSet::new(),
            workload_filter: Some(accept_ingest_workload),
        };
        let peers = self.selector.select(&self.ctx, options).await?;
        ensure!(
            peers.len() >= min_required_items,
            error::NoEnoughAvailableNodeSnafu {
                required: min_required_items,
                available: peers.len(),
                select_target: SelectTarget::Datanode,
            }
        );
        Ok(peers)
    }
}

#[async_trait]
impl PeerAllocator for MetasrvPeerAllocator {
    async fn alloc(&self, regions: usize, ctx: &PeerAllocContext) -> MetaResult<Vec<Peer>> {
        self.alloc(regions, ctx)
            .await
            .map_err(BoxedError::new)
            .context(ExternalSnafu)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::ptr;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use common_meta::affinity::{
        Affinity, AffinityFallback, AffinityRef, AffinityRequirements, NoopAffinity,
    };
    use common_meta::cluster::NodeInfo;

    use super::*;
    use crate::selector::Selector;
    use crate::selector::round_robin::RoundRobinSelector;
    use crate::test_util::{create_meta_peer_client, new_node_scheduling_discovery, put_datanodes};

    struct OnlySecondNode;

    impl Affinity for OnlySecondNode {
        fn matches(&self, _: &AffinityRequirements, node: &NodeInfo) -> bool {
            node.peer.id == 2
        }
    }

    #[derive(Clone, Copy)]
    enum Outcome {
        Base,
        BackendError,
        Short,
    }

    struct CountingSelector {
        base: RoundRobinSelector,
        calls: AtomicUsize,
        requirements_address: AtomicUsize,
        outcome: Outcome,
    }

    #[async_trait]
    impl Selector for CountingSelector {
        type Context = SelectorContext;
        type Output = Vec<Peer>;

        async fn select(
            &self,
            ctx: &SelectorContext,
            opts: SelectorOptions<'_>,
        ) -> Result<Self::Output> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.requirements_address.store(
                opts.affinity
                    .map_or(0, |requirements| ptr::from_ref(requirements) as usize),
                Ordering::Relaxed,
            );
            assert!(opts.allow_duplication);
            assert!(opts.exclude_peer_ids.is_empty());
            assert!(opts.workload_filter.is_some());
            assert_eq!(opts.min_required_items, 2);
            match self.outcome {
                Outcome::Base => self.base.select(ctx, opts).await,
                Outcome::BackendError => error::UnexpectedSnafu {
                    violated: "selection backend failed",
                }
                .fail(),
                Outcome::Short => Ok(vec![Peer::empty(1)]),
            }
        }
    }

    #[tokio::test]
    async fn test_allocator_delegates_fallback_and_validates_results() {
        let discovery = create_meta_peer_client();
        put_datanodes(&discovery, vec![Peer::empty(1)]).await;
        for (has_requirements, fallback, noop, outcome, expected_calls, succeeds) in [
            (
                true,
                AffinityFallback::Error,
                false,
                Outcome::Base,
                1,
                false,
            ),
            (
                true,
                AffinityFallback::Unconstrained,
                false,
                Outcome::Base,
                1,
                true,
            ),
            (true, AffinityFallback::Error, true, Outcome::Base, 1, true),
            (
                false,
                AffinityFallback::Unconstrained,
                false,
                Outcome::Base,
                1,
                true,
            ),
            (
                true,
                AffinityFallback::Unconstrained,
                false,
                Outcome::BackendError,
                1,
                false,
            ),
            (
                true,
                AffinityFallback::Error,
                false,
                Outcome::Short,
                1,
                false,
            ),
            (
                true,
                AffinityFallback::Unconstrained,
                false,
                Outcome::Short,
                1,
                false,
            ),
            (
                false,
                AffinityFallback::Unconstrained,
                false,
                Outcome::Short,
                1,
                false,
            ),
        ] {
            let selector = Arc::new(CountingSelector {
                base: RoundRobinSelector::default(),
                calls: AtomicUsize::new(0),
                requirements_address: AtomicUsize::new(0),
                outcome,
            });
            let affinity: AffinityRef = if noop {
                Arc::new(NoopAffinity)
            } else {
                Arc::new(OnlySecondNode)
            };
            let allocator = MetasrvPeerAllocator::new(
                SelectorContext {
                    affinity_fallback: fallback,
                    peer_discovery: Arc::new(new_node_scheduling_discovery(&discovery).1),
                    affinity,
                },
                selector.clone(),
            );
            let alloc_context = PeerAllocContext {
                affinity: has_requirements.then(|| {
                    AffinityRequirements::new(HashMap::from([("pool".into(), "a".into())])).unwrap()
                }),
            };
            let result = allocator.alloc(2, &alloc_context).await;
            assert_eq!(
                selector.requirements_address.load(Ordering::Relaxed),
                alloc_context
                    .affinity
                    .as_ref()
                    .map_or(0, |requirements| { ptr::from_ref(requirements) as usize }),
            );
            assert_eq!(selector.calls.load(Ordering::Relaxed), expected_calls);
            if succeeds {
                assert_eq!(result.unwrap(), vec![Peer::empty(1); 2]);
            } else {
                match outcome {
                    Outcome::BackendError => assert!(matches!(
                        result.unwrap_err(),
                        error::Error::Unexpected { .. }
                    )),
                    _ => assert!(matches!(
                        result.unwrap_err(),
                        error::Error::NoEnoughAvailableNode { .. }
                    )),
                }
            }
        }
    }
}
