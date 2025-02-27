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

use api::v1::meta::{HeartbeatRequest, Peer, Role};
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
        let HeartbeatRequest { header, peer, .. } = req;
        let Some(_header) = &header else {
            return Ok(HandleControl::Continue);
        };
        let Some(peer) = &peer else {
            return Ok(HandleControl::Continue);
        };

        let key = DatanodeLeaseKey { node_id: peer.id };
        let value = LeaseValue {
            timestamp_millis: time_util::current_time_millis(),
            node_addr: peer.addr.clone(),
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
