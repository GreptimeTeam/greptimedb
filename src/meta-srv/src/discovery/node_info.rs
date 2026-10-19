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

use common_meta::cluster::{NodeInfo, NodeInfoKey, NodeStatus, Role};
use common_meta::kv_backend::KvBackend;
use common_meta::rpc::store::RangeRequest;
use snafu::ResultExt;

use crate::cluster::MetaPeerClient;
use crate::error::{InvalidNodeInfoFormatSnafu, KvBackendSnafu, Result};

#[derive(Clone, Copy)]
pub enum NodeInfoType {
    Frontend,
    Datanode,
    Flownode,
}

impl From<NodeInfoType> for Role {
    fn from(node_info_type: NodeInfoType) -> Self {
        match node_info_type {
            NodeInfoType::Frontend => Role::Frontend,
            NodeInfoType::Datanode => Role::Datanode,
            NodeInfoType::Flownode => Role::Flownode,
        }
    }
}

/// Trait for types that can access node info.
#[async_trait::async_trait]
pub trait NodeInfoAccessor: Send + Sync {
    /// Returns the peer id and node info.
    async fn node_infos(&self, node_info_type: NodeInfoType) -> Result<Vec<(u64, NodeInfo)>>;
}

#[async_trait::async_trait]
impl NodeInfoAccessor for MetaPeerClient {
    async fn node_infos(&self, node_info_type: NodeInfoType) -> Result<Vec<(u64, NodeInfo)>> {
        let range_request = RangeRequest::new()
            .with_prefix(NodeInfoKey::key_prefix_with_role(node_info_type.into()));
        let response = self.range(range_request).await?;

        let mut infos = response
            .kvs
            .into_iter()
            .map(|kv| {
                let node_info = NodeInfo::try_from(kv.value).context(InvalidNodeInfoFormatSnafu)?;
                Ok((node_info.peer.id, node_info))
            })
            .collect::<Result<Vec<_>>>()?;

        if matches!(node_info_type, NodeInfoType::Datanode)
            && !infos.is_empty()
            && let Some(manager) = self.datanode_state_manager.read().await.clone()
        {
            let unschedulable = manager.unschedulable_ids().await.context(KvBackendSnafu)?;
            for (_, info) in &mut infos {
                if let NodeStatus::Datanode(status) = &mut info.status {
                    status.schedulable = !unschedulable.contains(&info.peer.id);
                }
            }
        }
        Ok(infos)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_meta::cluster::{NodeInfo, NodeInfoKey, NodeStatus, Role};
    use common_meta::kv_backend::memory::MemoryKvBackend;
    use common_meta::peer::{Peer, PeerDiscovery};

    use crate::datanode_state::DatanodeStateManager;
    use crate::test_util::{create_meta_peer_client, put_datanodes};

    fn schedulable(info: &NodeInfo) -> bool {
        match &info.status {
            NodeStatus::Datanode(status) => status.schedulable,
            _ => unreachable!(),
        }
    }

    #[tokio::test]
    async fn test_discovery_enriches_without_changing_heartbeat_info() {
        let state_backend = Arc::new(MemoryKvBackend::new());
        let manager = Arc::new(DatanodeStateManager::new(state_backend));
        let client = create_meta_peer_client();
        client.set_datanode_state_manager(manager.clone()).await;
        put_datanodes(&client, vec![Peer::empty(1), Peer::empty(2)]).await;

        assert!(manager.set_schedulable(2, false).await.unwrap());
        let nodes = client.active_datanodes(None).await.unwrap();
        assert_eq!(nodes.len(), 2);
        assert!(
            nodes
                .iter()
                .any(|node| node.peer.id == 1 && schedulable(node))
        );
        assert!(
            nodes
                .iter()
                .any(|node| node.peer.id == 2 && !schedulable(node))
        );
        let enriched = nodes.iter().find(|node| node.peer.id == 2).unwrap();
        assert!(
            !serde_json::to_string(enriched)
                .unwrap()
                .contains("schedulable")
        );

        let key: Vec<u8> = (&NodeInfoKey {
            role: Role::Datanode,
            node_id: 2,
        })
            .into();
        let stored = client.memory_backend().get(&key).await.unwrap().unwrap();
        let stored = NodeInfo::try_from(stored.value).unwrap();
        assert!(schedulable(&stored));

        assert!(manager.set_schedulable(2, true).await.unwrap());
        let nodes = client.active_datanodes(None).await.unwrap();
        assert!(nodes.iter().all(schedulable));
    }
}
