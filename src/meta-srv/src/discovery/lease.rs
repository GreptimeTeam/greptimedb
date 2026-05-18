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

use common_meta::kv_backend::KvBackend;
use common_meta::rpc::KeyValue;
use common_meta::rpc::store::RangeRequest;

use crate::cluster::MetaPeerClient;
use crate::error::Result;
use crate::key::{DatanodeLeaseKey, FlownodeLeaseKey, LeaseValue};

#[derive(Clone, Copy)]
pub enum LeaseValueType {
    Flownode,
    Datanode,
}

#[async_trait::async_trait]
pub trait LeaseValueAccessor: Send + Sync {
    /// Returns the peer id and lease value.
    async fn lease_values(
        &self,
        lease_value_type: LeaseValueType,
    ) -> Result<Vec<(u64, LeaseValue)>>;

    async fn lease_value(
        &self,
        lease_value_type: LeaseValueType,
        node_id: u64,
    ) -> Result<Option<(u64, LeaseValue)>>;
}

fn decoder(lease_value_type: LeaseValueType, kv: KeyValue) -> Result<(u64, LeaseValue)> {
    match lease_value_type {
        LeaseValueType::Flownode => {
            let lease_key: FlownodeLeaseKey = kv.key.try_into()?;
            let lease_value: LeaseValue = kv.value.try_into()?;
            Ok((lease_key.node_id, lease_value))
        }
        LeaseValueType::Datanode => {
            let lease_key: DatanodeLeaseKey = kv.key.try_into()?;
            let lease_value: LeaseValue = kv.value.try_into()?;
            Ok((lease_key.node_id, lease_value))
        }
    }
}

#[async_trait::async_trait]
impl LeaseValueAccessor for MetaPeerClient {
    async fn lease_values(
        &self,
        lease_value_type: LeaseValueType,
    ) -> Result<Vec<(u64, LeaseValue)>> {
        let prefix = match lease_value_type {
            LeaseValueType::Flownode => FlownodeLeaseKey::prefix_key_by_cluster(),
            LeaseValueType::Datanode => DatanodeLeaseKey::prefix_key(),
        };
        let range_request = RangeRequest::new().with_prefix(prefix);
        let response = self.range(range_request).await?;
        response
            .kvs
            .into_iter()
            .map(|kv| {
                let (lease_key, lease_value) = decoder(lease_value_type, kv)?;
                Ok((lease_key, lease_value))
            })
            .collect::<Result<Vec<_>>>()
    }

    async fn lease_value(
        &self,
        lease_value_type: LeaseValueType,
        node_id: u64,
    ) -> Result<Option<(u64, LeaseValue)>> {
        let key: Vec<u8> = match lease_value_type {
            LeaseValueType::Flownode => FlownodeLeaseKey { node_id }.try_into()?,
            LeaseValueType::Datanode => DatanodeLeaseKey { node_id }.try_into()?,
        };

        let response = self.get(&key).await?;
        response.map(|kv| decoder(lease_value_type, kv)).transpose()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use api::v1::meta::{DatanodeWorkloads, FlownodeWorkloads, FrontendWorkloads};
    use common_meta::cluster::{
        DatanodeStatus, FlownodeStatus, FrontendStatus, NodeInfo, NodeInfoKey, NodeStatus, Role,
    };
    use common_meta::distributed_time_constants::default_distributed_time_constants;
    use common_meta::kv_backend::ResettableKvBackendRef;
    use common_meta::peer::{Peer, PeerDiscovery};
    use common_meta::rpc::store::PutRequest;
    use common_time::util::current_time_millis;
    use common_workload::DatanodeWorkloadType;

    use crate::discovery::utils::accept_ingest_workload;
    use crate::test_util::create_meta_peer_client;

    async fn put_node_info(kv_backend: &ResettableKvBackendRef, key: NodeInfoKey, value: NodeInfo) {
        kv_backend
            .put(PutRequest {
                key: (&key).into(),
                value: value.try_into().unwrap(),
                prev_kv: false,
            })
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_active_datanodes_returns_node_info_with_env_vars() {
        let client = create_meta_peer_client();
        let in_memory = client.memory_backend();

        let mut env_vars = HashMap::new();
        env_vars.insert("AZ".to_string(), "az-a".to_string());

        put_node_info(
            &in_memory,
            NodeInfoKey {
                role: Role::Datanode,
                node_id: 1,
            },
            NodeInfo {
                peer: Peer::new(1, "127.0.0.1:4001".to_string()),
                last_activity_ts: current_time_millis(),
                status: NodeStatus::Datanode(DatanodeStatus {
                    rcus: 0,
                    wcus: 0,
                    leader_regions: 0,
                    follower_regions: 0,
                    workloads: DatanodeWorkloads {
                        types: vec![DatanodeWorkloadType::Hybrid as i32],
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
                env_vars,
            },
        )
        .await;
        let nodes = client
            .active_datanodes(Some(accept_ingest_workload))
            .await
            .unwrap();

        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].peer.id, 1);
        assert_eq!(
            nodes[0].env_vars.get("AZ").map(String::as_str),
            Some("az-a")
        );
    }

    #[tokio::test]
    async fn test_active_flownodes_returns_node_info() {
        let client = create_meta_peer_client();
        let in_memory = client.memory_backend();

        put_node_info(
            &in_memory,
            NodeInfoKey {
                role: Role::Flownode,
                node_id: 11,
            },
            NodeInfo {
                peer: Peer::new(11, "127.0.0.1:5001".to_string()),
                last_activity_ts: current_time_millis(),
                status: NodeStatus::Flownode(FlownodeStatus {
                    workloads: FlownodeWorkloads { types: vec![7] },
                }),
                version: String::new(),
                git_commit: String::new(),
                start_time_ms: 0,
                total_cpu_millicores: 0,
                total_memory_bytes: 0,
                cpu_usage_millicores: 0,
                memory_usage_bytes: 0,
                hostname: String::new(),
                env_vars: Default::default(),
            },
        )
        .await;

        let nodes = client.active_flownodes(None).await.unwrap();

        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].peer.id, 11);
    }

    #[tokio::test]
    async fn test_no_active_frontends() {
        let client = create_meta_peer_client();
        let in_memory = client.memory_backend();

        let frontend_heartbeat_interval =
            default_distributed_time_constants().frontend_heartbeat_interval;
        let last_activity_ts =
            current_time_millis() - frontend_heartbeat_interval.as_millis() as i64 - 1000;
        let active_frontend_node = NodeInfo {
            peer: Peer {
                id: 0,
                addr: "127.0.0.1:20201".to_string(),
            },
            last_activity_ts,
            status: NodeStatus::Frontend(FrontendStatus {
                workloads: FrontendWorkloads { types: vec![] },
            }),
            version: "1.0.0".to_string(),
            git_commit: "1234567890".to_string(),
            start_time_ms: last_activity_ts as u64,
            total_cpu_millicores: 0,
            total_memory_bytes: 0,
            cpu_usage_millicores: 0,
            memory_usage_bytes: 0,
            hostname: "test_hostname".to_string(),
            env_vars: Default::default(),
        };

        let key_prefix = NodeInfoKey::key_prefix_with_role(Role::Frontend);

        in_memory
            .put(PutRequest {
                key: format!("{}{}", key_prefix, "0").into(),
                value: active_frontend_node.try_into().unwrap(),
                prev_kv: false,
            })
            .await
            .unwrap();

        let peers = client.active_frontends().await.unwrap();
        assert_eq!(peers.len(), 0);
    }
}
