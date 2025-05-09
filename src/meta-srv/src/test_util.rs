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

use std::sync::Arc;

use api::v1::meta::heartbeat_request::NodeWorkloads;
use api::v1::meta::DatanodeWorkloads;
use common_meta::kv_backend::memory::MemoryKvBackend;
use common_meta::peer::Peer;
use common_meta::rpc::router::{Region, RegionRoute};
use common_time::util as time_util;
use common_workload::DatanodeWorkloadType;

use crate::cluster::{MetaPeerClientBuilder, MetaPeerClientRef};
use crate::key::{DatanodeLeaseKey, LeaseValue};
use crate::metasrv::SelectorContext;

pub(crate) fn new_region_route(region_id: u64, peers: &[Peer], leader_node: u64) -> RegionRoute {
    let region = Region {
        id: region_id.into(),
        ..Default::default()
    };

    let leader_peer = peers.iter().find(|peer| peer.id == leader_node).cloned();

    RegionRoute {
        region,
        leader_peer,
        follower_peers: vec![],
        leader_state: None,
        leader_down_since: None,
    }
}

pub(crate) fn create_meta_peer_client() -> MetaPeerClientRef {
    let in_memory = Arc::new(MemoryKvBackend::new());
    MetaPeerClientBuilder::default()
        .election(None)
        .in_memory(in_memory)
        .build()
        .map(Arc::new)
        // Safety: all required fields set at initialization
        .unwrap()
}

/// Builds and returns a [`SelectorContext`]. To access its inner state,
/// use `memory_backend` on [`MetaPeerClientRef`].
pub(crate) fn create_selector_context() -> SelectorContext {
    let meta_peer_client = create_meta_peer_client();
    let in_memory = meta_peer_client.memory_backend();

    SelectorContext {
        datanode_lease_secs: 10,
        flownode_lease_secs: 10,
        server_addr: "127.0.0.1:3002".to_string(),
        kv_backend: in_memory,
        meta_peer_client,
        table_id: None,
    }
}

pub(crate) async fn put_datanodes(meta_peer_client: &MetaPeerClientRef, datanodes: Vec<Peer>) {
    let backend = meta_peer_client.memory_backend();
    for datanode in datanodes {
        let lease_key = DatanodeLeaseKey {
            node_id: datanode.id,
        };
        let lease_value = LeaseValue {
            timestamp_millis: time_util::current_time_millis(),
            node_addr: datanode.addr,
            workloads: NodeWorkloads::Datanode(DatanodeWorkloads {
                types: vec![DatanodeWorkloadType::Hybrid.to_i32()],
            }),
        };
        let lease_key_bytes: Vec<u8> = lease_key.try_into().unwrap();
        let lease_value_bytes: Vec<u8> = lease_value.try_into().unwrap();
        let put_request = common_meta::rpc::store::PutRequest {
            key: lease_key_bytes,
            value: lease_value_bytes,
            ..Default::default()
        };
        backend.put(put_request).await.unwrap();
    }
}
