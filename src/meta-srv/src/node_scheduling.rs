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

use common_meta::kv_backend::KvBackendRef;
use common_meta::kv_backend::txn::{Compare, CompareOp, Txn, TxnOp};
use common_meta::rpc::store::RangeRequest;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

use crate::error::{self, Result};
use crate::key::{NodeSchedulingKey, NodeSchedulingRole};

pub type NodeSchedulingManagerRef = Arc<NodeSchedulingManager>;

/// The scheduling state persisted for a node.
///
/// Only `schedulable: false` is written today; a missing key already means schedulable.
/// New metadata fields must use `#[serde(default)]`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeSchedulingValue {
    /// Whether the node may receive new regions.
    pub schedulable: bool,
}

/// Persists which nodes may receive new regions.
///
/// A missing key means the node is schedulable; a present key stores a
/// [`NodeSchedulingValue`] whose `schedulable` field decides placement.
pub struct NodeSchedulingManager {
    kv_backend: KvBackendRef,
}

impl NodeSchedulingManager {
    pub fn new(kv_backend: KvBackendRef) -> Self {
        Self { kv_backend }
    }

    /// Marks the node as unschedulable. Idempotent.
    ///
    /// Returns whether the stored state changed.
    pub async fn mark_unschedulable(&self, role: NodeSchedulingRole, node_id: u64) -> Result<bool> {
        let key: Vec<u8> = NodeSchedulingKey::new(role, node_id).into();
        let value = serde_json::to_vec(&NodeSchedulingValue { schedulable: false }).context(
            error::SerializeToJsonSnafu {
                input: String::from_utf8_lossy(&key).into_owned(),
            },
        )?;

        let txn = match self
            .kv_backend
            .get(&key)
            .await
            .context(error::KvBackendSnafu)?
        {
            None => Txn::put_if_not_exists(key, value),
            Some(kv) if kv.value == value => return Ok(false),
            Some(kv) => Txn::compare_and_put(key, kv.value, value),
        };
        let response = self
            .kv_backend
            .txn(txn)
            .await
            .context(error::KvBackendSnafu)?;
        Ok(response.succeeded)
    }

    /// Marks the node as schedulable by deleting its state. Idempotent.
    ///
    /// Returns whether a state was removed; a node without state is already schedulable,
    /// so this returns `false` and writes nothing.
    pub async fn mark_schedulable(&self, role: NodeSchedulingRole, node_id: u64) -> Result<bool> {
        let key: Vec<u8> = NodeSchedulingKey::new(role, node_id).into();
        // `NotEqual` against a missing target means "the key exists".
        let txn = Txn::new()
            .when(vec![Compare::new(key.clone(), CompareOp::NotEqual, None)])
            .and_then(vec![TxnOp::Delete(key)]);
        let response = self
            .kv_backend
            .txn(txn)
            .await
            .context(error::KvBackendSnafu)?;
        Ok(response.succeeded)
    }

    /// Returns `false` iff the node is marked unschedulable.
    pub async fn is_schedulable(&self, role: NodeSchedulingRole, node_id: u64) -> Result<bool> {
        let key = NodeSchedulingKey::new(role, node_id);
        Ok(self
            .get_value(&key)
            .await?
            .is_none_or(|value| value.schedulable))
    }

    /// Returns the ids of nodes of `role` that must not receive new regions.
    ///
    /// Returns a set because callers test membership per node.
    pub async fn list_unschedulable_node_ids(
        &self,
        role: NodeSchedulingRole,
    ) -> Result<HashSet<u64>> {
        let request = RangeRequest::new().with_prefix(NodeSchedulingKey::prefix_key(role));
        let response = self
            .kv_backend
            .range(request)
            .await
            .context(error::KvBackendSnafu)?;

        let mut node_ids = HashSet::new();
        for kv in response.kvs {
            let key = NodeSchedulingKey::try_from(kv.key)?;
            if !parse_value(&kv.value)?.schedulable {
                node_ids.insert(key.node_id);
            }
        }
        Ok(node_ids)
    }

    async fn get_value(&self, key: &NodeSchedulingKey) -> Result<Option<NodeSchedulingValue>> {
        let key: Vec<u8> = (*key).into();
        let Some(kv) = self
            .kv_backend
            .get(&key)
            .await
            .context(error::KvBackendSnafu)?
        else {
            return Ok(None);
        };

        parse_value(&kv.value).map(Some)
    }
}

fn parse_value(value: &[u8]) -> Result<NodeSchedulingValue> {
    serde_json::from_slice(value).context(error::DeserializeFromJsonSnafu {
        input_len: value.len(),
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_meta::kv_backend::KvBackend;
    use common_meta::kv_backend::memory::MemoryKvBackend;

    use super::*;
    use crate::metasrv::SelectorContext;

    fn new_manager() -> (
        Arc<MemoryKvBackend<common_meta::error::Error>>,
        NodeSchedulingManager,
    ) {
        let kv_backend = Arc::new(MemoryKvBackend::new());
        let manager = NodeSchedulingManager::new(kv_backend.clone());
        (kv_backend, manager)
    }

    #[tokio::test]
    async fn test_mark_unschedulable_is_idempotent() {
        let (kv_backend, manager) = new_manager();

        assert!(
            manager
                .is_schedulable(NodeSchedulingRole::Datanode, 1)
                .await
                .unwrap()
        );
        assert!(
            manager
                .mark_unschedulable(NodeSchedulingRole::Datanode, 1)
                .await
                .unwrap()
        );
        assert!(
            !manager
                .mark_unschedulable(NodeSchedulingRole::Datanode, 1)
                .await
                .unwrap()
        );

        assert!(
            !manager
                .is_schedulable(NodeSchedulingRole::Datanode, 1)
                .await
                .unwrap()
        );
        assert_eq!(
            HashSet::from([1]),
            manager
                .list_unschedulable_node_ids(NodeSchedulingRole::Datanode)
                .await
                .unwrap()
        );

        let key: Vec<u8> = NodeSchedulingKey::new(NodeSchedulingRole::Datanode, 1).into();
        let kv = kv_backend.get(&key).await.unwrap().unwrap();
        assert_eq!(br#"{"schedulable":false}"#, kv.value.as_slice());

        assert!(
            manager
                .mark_schedulable(NodeSchedulingRole::Datanode, 1)
                .await
                .unwrap()
        );
        assert!(
            !manager
                .mark_schedulable(NodeSchedulingRole::Datanode, 1)
                .await
                .unwrap()
        );

        assert!(
            manager
                .is_schedulable(NodeSchedulingRole::Datanode, 1)
                .await
                .unwrap()
        );
        assert!(
            manager
                .list_unschedulable_node_ids(NodeSchedulingRole::Datanode)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn test_mark_schedulable_without_state_writes_nothing() {
        let (kv_backend, manager) = new_manager();

        assert!(
            !manager
                .mark_schedulable(NodeSchedulingRole::Datanode, 2)
                .await
                .unwrap()
        );

        let key: Vec<u8> = NodeSchedulingKey::new(NodeSchedulingRole::Datanode, 2).into();
        assert!(kv_backend.get(&key).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_scheduling_state_is_role_scoped() {
        let (_, manager) = new_manager();

        manager
            .mark_unschedulable(NodeSchedulingRole::Datanode, 7)
            .await
            .unwrap();

        assert!(
            manager
                .is_schedulable(NodeSchedulingRole::Flownode, 7)
                .await
                .unwrap()
        );
        assert!(
            manager
                .list_unschedulable_node_ids(NodeSchedulingRole::Flownode)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn test_scheduling_state_is_visible_to_other_manager() {
        let (kv_backend, manager_a) = new_manager();
        let manager_b = NodeSchedulingManager::new(kv_backend);

        manager_a
            .mark_unschedulable(NodeSchedulingRole::Datanode, 3)
            .await
            .unwrap();

        assert!(
            !manager_b
                .is_schedulable(NodeSchedulingRole::Datanode, 3)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn test_concurrent_mark_unschedulable_changes_once() {
        let (_, manager) = new_manager();

        let (a, b) = tokio::join!(
            manager.mark_unschedulable(NodeSchedulingRole::Datanode, 2),
            manager.mark_unschedulable(NodeSchedulingRole::Datanode, 2)
        );
        let changed = [a.unwrap(), b.unwrap()];
        assert_eq!(1, changed.iter().filter(|changed| **changed).count());
    }

    /// Returns a selector context that stamps scheduling marks of datanodes 1, 2, 3
    /// where datanode 2 is unschedulable.
    async fn new_selector_context() -> SelectorContext {
        use common_meta::peer::Peer;

        use crate::test_util::{
            create_meta_peer_client, new_node_scheduling_discovery, put_datanodes,
        };

        let client = create_meta_peer_client();
        put_datanodes(
            &client,
            vec![Peer::empty(1), Peer::empty(2), Peer::empty(3)],
        )
        .await;

        let (manager, discovery) = new_node_scheduling_discovery(&client);
        manager
            .mark_unschedulable(NodeSchedulingRole::Datanode, 2)
            .await
            .unwrap();

        SelectorContext {
            affinity_fallback: Default::default(),
            affinity: Arc::new(common_meta::affinity::NoopAffinity),
            peer_discovery: Arc::new(discovery),
        }
    }

    #[tokio::test]
    async fn test_lease_based_selector_skips_unschedulable() {
        use crate::error;
        use crate::selector::lease_based::LeaseBasedSelector;
        use crate::selector::{Selector, SelectorOptions};

        let ctx = new_selector_context().await;
        let selector = LeaseBasedSelector;

        let peers = selector
            .select(
                &ctx,
                SelectorOptions {
                    min_required_items: 2,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(2, peers.len());
        for peer in peers {
            assert!([1, 3].contains(&peer.id), "unexpected peer: {peer:?}");
        }

        let err = selector
            .select(
                &ctx,
                SelectorOptions {
                    min_required_items: 3,
                    ..Default::default()
                },
            )
            .await
            .unwrap_err();
        assert!(
            matches!(err, error::Error::NoEnoughAvailableNode { .. }),
            "unexpected error: {err:?}"
        );
    }

    #[tokio::test]
    async fn test_round_robin_selector_skips_unschedulable() {
        use crate::metasrv::SelectTarget;
        use crate::selector::round_robin::RoundRobinSelector;
        use crate::selector::{Selector, SelectorOptions};

        let ctx = new_selector_context().await;
        let selector = RoundRobinSelector::new(SelectTarget::Datanode);

        let peers = selector
            .select(
                &ctx,
                SelectorOptions {
                    min_required_items: 2,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let ids = peers.iter().map(|peer| peer.id).collect::<Vec<_>>();
        assert_eq!(vec![1, 3], ids);
    }
}
