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

//! Resolve reference attributes into requirements, then evaluate candidates.

use std::collections::HashMap;
use std::sync::Arc;

use crate::DatanodeId;
use crate::cluster::NodeInfo;
use crate::error::Result;
use crate::peer::PeerAllocContext;
use crate::rpc::ddl::QueryContext;

/// Named attributes used to express affinity requirements.
pub type AffinityAttributes = HashMap<String, String>;

/// Identifies the node whose attributes define affinity requirements.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AffinityReference {
    /// Uses the datanode's last-known attributes, even if its lease has expired.
    Datanode(DatanodeId),
    /// Uses the frontend identified by its advertised address.
    Frontend(String),
}

/// Behavior when affinity cannot be satisfied.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum AffinityFallback {
    /// Returns an error instead of selecting without affinity.
    #[default]
    Error,
    /// Allows selection among eligible nodes without affinity.
    Unconstrained,
}

/// Builds allocation inputs from the query that initiated an operation.
#[async_trait::async_trait]
pub trait PeerAllocContextProvider: Send + Sync {
    /// Resolves query-specific allocation inputs using metadata, without selecting peers.
    async fn build(&self, query_context: &QueryContext) -> Result<PeerAllocContext>;
}

/// Shared allocation-context provider.
pub type PeerAllocContextProviderRef = Arc<dyn PeerAllocContextProvider>;

/// Nonempty attributes a candidate must match.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AffinityRequirements {
    attributes: AffinityAttributes,
}

impl AffinityRequirements {
    /// Returns no requirements when the supplied attributes are empty.
    pub fn new(attributes: AffinityAttributes) -> Option<Self> {
        (!attributes.is_empty()).then_some(Self { attributes })
    }

    /// Returns the attributes that candidates are evaluated against.
    pub fn attributes(&self) -> &AffinityAttributes {
        &self.attributes
    }
}

/// Resolves references using current metadata, without selecting candidates.
#[async_trait::async_trait]
pub trait AffinityResolver: Send + Sync {
    /// Returns one result per reference, in order.
    /// None means requirements are unavailable; Err means resolution failed.
    async fn resolve(
        &self,
        references: &[AffinityReference],
    ) -> Result<Vec<Option<AffinityRequirements>>>;
}

/// Shared requirement resolver.
pub type AffinityResolverRef = Arc<dyn AffinityResolver>;

/// Evaluates candidate nodes without I/O or scheduling decisions.
pub trait Affinity: Send + Sync {
    /// Evaluates a node snapshot; implementations choose which fields to inspect.
    fn matches(&self, requirements: &AffinityRequirements, candidate: &NodeInfo) -> bool;
}

/// Accepts every candidate without imposing affinity constraints.
pub struct NoopAffinity;

impl Affinity for NoopAffinity {
    fn matches(&self, _: &AffinityRequirements, _: &NodeInfo) -> bool {
        true
    }
}

/// Shared affinity evaluator.
pub type AffinityRef = Arc<dyn Affinity>;

#[cfg(test)]
mod tests {
    use std::sync::RwLock;

    use super::*;
    use crate::cluster::{DatanodeStatus, NodeStatus};
    use crate::peer::Peer;

    struct NodeAffinityResolver {
        attributes: RwLock<HashMap<AffinityReference, AffinityAttributes>>,
    }

    #[async_trait::async_trait]
    impl AffinityResolver for NodeAffinityResolver {
        async fn resolve(
            &self,
            references: &[AffinityReference],
        ) -> Result<Vec<Option<AffinityRequirements>>> {
            let snapshot = self.attributes.read().unwrap();
            Ok(references
                .iter()
                .map(|reference| {
                    snapshot
                        .get(reference)
                        .cloned()
                        .and_then(AffinityRequirements::new)
                })
                .collect())
        }
    }

    struct ExactAffinity;

    impl Affinity for ExactAffinity {
        fn matches(&self, requirements: &AffinityRequirements, candidate: &NodeInfo) -> bool {
            requirements
                .attributes()
                .iter()
                .all(|(key, expected)| candidate.env_vars.get(key) == Some(expected))
        }
    }

    fn zone(name: &str) -> AffinityAttributes {
        HashMap::from([("AZ".to_string(), name.to_string())])
    }

    fn node(id: DatanodeId, env_vars: AffinityAttributes) -> NodeInfo {
        NodeInfo {
            peer: Peer::empty(id),
            last_activity_ts: 0,
            status: NodeStatus::Datanode(DatanodeStatus {
                rcus: 0,
                wcus: 0,
                leader_regions: 0,
                follower_regions: 0,
                workloads: Default::default(),
            }),
            version: String::new(),
            git_commit: String::new(),
            start_time_ms: 0,
            total_cpu_millicores: 0,
            total_memory_bytes: 0,
            cpu_usage_millicores: 0,
            memory_usage_bytes: 0,
            hostname: String::new(),
            env_vars,
            schedulable: None,
        }
    }

    #[tokio::test]
    async fn test_resolve_reference_and_match_candidates() {
        let resolver = NodeAffinityResolver {
            attributes: RwLock::new(HashMap::from([(AffinityReference::Datanode(1), zone("a"))])),
        };
        let references = [AffinityReference::Datanode(1)];
        let resolved = resolver.resolve(&references).await.unwrap();
        assert_eq!(resolved, vec![AffinityRequirements::new(zone("a"))]);
        let requirements = resolved[0].as_ref().unwrap();
        let candidates = [
            node(2, zone("a")),
            node(3, zone("b")),
            node(4, HashMap::new()),
        ];
        assert_eq!(
            candidates
                .iter()
                .map(|node| (node.peer.id, ExactAffinity.matches(requirements, node)))
                .collect::<Vec<_>>(),
            vec![(2, true), (3, false), (4, false),]
        );

        assert!(
            candidates
                .iter()
                .all(|node| NoopAffinity.matches(requirements, node))
        );

        // A new allocation attempt resolves current attributes rather than a saved requirement.
        resolver
            .attributes
            .write()
            .unwrap()
            .insert(AffinityReference::Datanode(1), zone("b"));
        let resolved = resolver.resolve(&references).await.unwrap();
        let requirements = resolved[0].as_ref().unwrap();
        let matched: Vec<_> = candidates
            .iter()
            .filter(|node| ExactAffinity.matches(requirements, node))
            .map(|node| node.peer.id)
            .collect();
        assert_eq!(matched, vec![3]);
    }

    #[tokio::test]
    async fn test_resolution_preserves_order_and_unavailable_requirements() {
        let resolver = NodeAffinityResolver {
            attributes: RwLock::new(HashMap::from([
                (AffinityReference::Datanode(1), zone("a")),
                (AffinityReference::Datanode(2), HashMap::new()),
                (
                    AffinityReference::Frontend("frontend:4001".into()),
                    zone("b"),
                ),
            ])),
        };
        let references = [
            AffinityReference::Frontend("missing:4001".into()),
            AffinityReference::Datanode(1),
            AffinityReference::Frontend("frontend:4001".into()),
            AffinityReference::Datanode(2),
            AffinityReference::Datanode(3),
            AffinityReference::Frontend("frontend:4001".into()),
        ];
        assert_eq!(
            resolver.resolve(&references).await.unwrap(),
            vec![
                None,
                AffinityRequirements::new(zone("a")),
                AffinityRequirements::new(zone("b")),
                None,
                None,
                AffinityRequirements::new(zone("b")),
            ]
        );
        assert!(AffinityRequirements::new(HashMap::new()).is_none());
    }
}
