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

use api::v1::meta::heartbeat_request::NodeWorkloads;
use api::v1::meta::{FlownodeWorkloads, HeartbeatRequest, Peer, Role};
use common_meta::heartbeat::utils::get_datanode_workloads;
use common_meta::rpc::store::PutRequest;
use common_telemetry::{trace, warn};
use common_time::util as time_util;

use crate::error::Result;
use crate::handler::{HandleControl, HeartbeatAccumulator, HeartbeatHandler};
use crate::key::{DatanodeLeaseKey, FlownodeLeaseKey, LeaseValue};
use crate::metasrv::Context;

/// Keeps [Datanode] leases
pub struct DatanodeKeepLeaseHandler;

#[async_trait::async_trait]
impl HeartbeatHandler for DatanodeKeepLeaseHandler {
    fn is_acceptable(&self, role: Role) -> bool {
        role == Role::Datanode
    }

    async fn handle(
        &self,
        req: &HeartbeatRequest,
        ctx: &mut Context,
        _acc: &mut HeartbeatAccumulator,
    ) -> Result<HandleControl> {
        let HeartbeatRequest {
            header,
            peer,
            node_workloads,
            ..
        } = req;
        let Some(_header) = &header else {
            return Ok(HandleControl::Continue);
        };
        let Some(peer) = &peer else {
            return Ok(HandleControl::Continue);
        };

        let datanode_workloads = get_datanode_workloads(node_workloads.as_ref());
        let key = DatanodeLeaseKey { node_id: peer.id };
        let value = LeaseValue {
            timestamp_millis: time_util::current_time_millis(),
            node_addr: peer.addr.clone(),
            workloads: NodeWorkloads::Datanode(datanode_workloads),
        };

        trace!("Receive a heartbeat from datanode: {key:?}, {value:?}");

        let key = key.try_into()?;
        let value = value.try_into()?;
        put_into_memory_store(ctx, key, value, peer).await;

        Ok(HandleControl::Continue)
    }
}

/// Keeps [Flownode] leases
pub struct FlownodeKeepLeaseHandler;

#[async_trait::async_trait]
impl HeartbeatHandler for FlownodeKeepLeaseHandler {
    fn is_acceptable(&self, role: Role) -> bool {
        role == Role::Flownode
    }

    async fn handle(
        &self,
        req: &HeartbeatRequest,
        ctx: &mut Context,
        _acc: &mut HeartbeatAccumulator,
    ) -> Result<HandleControl> {
        let HeartbeatRequest { header, peer, .. } = req;
        let Some(_header) = &header else {
            return Ok(HandleControl::Continue);
        };
        let Some(peer) = &peer else {
            return Ok(HandleControl::Continue);
        };

        let key = FlownodeLeaseKey { node_id: peer.id };
        let value = LeaseValue {
            timestamp_millis: time_util::current_time_millis(),
            node_addr: peer.addr.clone(),
            workloads: NodeWorkloads::Flownode(FlownodeWorkloads { types: vec![] }),
        };

        trace!("Receive a heartbeat from flownode: {key:?}, {value:?}");

        let key = key.try_into()?;
        let value = value.try_into()?;
        put_into_memory_store(ctx, key, value, peer).await;

        Ok(HandleControl::Continue)
    }
}

async fn put_into_memory_store(ctx: &mut Context, key: Vec<u8>, value: Vec<u8>, peer: &Peer) {
    let put_req = PutRequest {
        key,
        value,
        ..Default::default()
    };

    let res = ctx.in_memory.put(put_req).await;

    if let Err(err) = res {
        warn!(err; "Failed to update lease KV, peer: {peer:?}");
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use api::v1::meta::RequestHeader;
    use common_meta::cache_invalidator::DummyCacheInvalidator;
    use common_meta::datanode::Stat;
    use common_meta::key::TableMetadataManager;
    use common_meta::kv_backend::memory::MemoryKvBackend;
    use common_meta::region_registry::LeaderRegionRegistry;
    use common_meta::sequence::SequenceBuilder;

    use super::*;
    use crate::cluster::MetaPeerClientBuilder;
    use crate::handler::{HeartbeatMailbox, Pushers};
    use crate::lease::find_datanode_lease_value;
    use crate::service::store::cached_kv::LeaderCachedKvBackend;

    #[tokio::test]
    async fn test_put_into_memory_store() {
        let in_memory = Arc::new(MemoryKvBackend::new());
        let kv_backend = Arc::new(MemoryKvBackend::new());
        let leader_cached_kv_backend = Arc::new(LeaderCachedKvBackend::with_always_leader(
            kv_backend.clone(),
        ));
        let seq = SequenceBuilder::new("test_seq", kv_backend.clone()).build();
        let mailbox = HeartbeatMailbox::create(Pushers::default(), seq);
        let meta_peer_client = MetaPeerClientBuilder::default()
            .election(None)
            .in_memory(in_memory.clone())
            .build()
            .map(Arc::new)
            // Safety: all required fields set at initialization
            .unwrap();
        let ctx = Context {
            server_addr: "127.0.0.1:0000".to_string(),
            in_memory,
            kv_backend: kv_backend.clone(),
            leader_cached_kv_backend,
            meta_peer_client,
            mailbox,
            election: None,
            is_infancy: false,
            table_metadata_manager: Arc::new(TableMetadataManager::new(kv_backend.clone())),
            cache_invalidator: Arc::new(DummyCacheInvalidator),
            leader_region_registry: Arc::new(LeaderRegionRegistry::new()),
        };

        let handler = DatanodeKeepLeaseHandler;
        handle_request_many_times(ctx.clone(), &handler, 1).await;

        let lease_value = find_datanode_lease_value(1, &ctx.in_memory)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(lease_value.node_addr, "127.0.0.1:1");
        assert!(lease_value.timestamp_millis != 0);
    }

    async fn handle_request_many_times(
        mut ctx: Context,
        handler: &DatanodeKeepLeaseHandler,
        loop_times: i32,
    ) {
        let req = HeartbeatRequest {
            header: Some(RequestHeader::new(1, Role::Datanode, HashMap::new())),
            peer: Some(Peer::new(1, "127.0.0.1:1")),
            ..Default::default()
        };

        for i in 1..=loop_times {
            let mut acc = HeartbeatAccumulator {
                stat: Some(Stat {
                    id: 101,
                    region_num: i as _,
                    ..Default::default()
                }),
                ..Default::default()
            };
            handler.handle(&req, &mut ctx, &mut acc).await.unwrap();
        }
    }
}
