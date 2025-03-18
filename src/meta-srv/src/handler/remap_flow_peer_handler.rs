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
use common_meta::key::node_address::{NodeAddressKey, NodeAddressValue};
use common_meta::key::{MetadataKey, MetadataValue};
use common_meta::rpc::store::PutRequest;
use common_telemetry::{error, info, warn};
use dashmap::DashMap;

use crate::handler::{HandleControl, HeartbeatAccumulator, HeartbeatHandler};
use crate::metasrv::Context;
use crate::Result;

#[derive(Debug, Default)]
pub struct RemapFlowPeerHandler {
    /// flow_node_id -> epoch
    epoch_cache: DashMap<u64, u64>,
}

#[async_trait::async_trait]
impl HeartbeatHandler for RemapFlowPeerHandler {
    fn is_acceptable(&self, role: Role) -> bool {
        role == Role::Flownode
    }

    async fn handle(
        &self,
        req: &HeartbeatRequest,
        ctx: &mut Context,
        _acc: &mut HeartbeatAccumulator,
    ) -> Result<HandleControl> {
        let Some(peer) = req.peer.as_ref() else {
            return Ok(HandleControl::Continue);
        };

        let current_epoch = req.node_epoch;
        let flow_node_id = peer.id;

        let refresh = if let Some(mut epoch) = self.epoch_cache.get_mut(&flow_node_id) {
            if current_epoch > *epoch.value() {
                *epoch.value_mut() = current_epoch;
                true
            } else {
                false
            }
        } else {
            self.epoch_cache.insert(flow_node_id, current_epoch);
            true
        };

        if refresh {
            rewrite_node_address(ctx, peer).await;
        }

        Ok(HandleControl::Continue)
    }
}

async fn rewrite_node_address(ctx: &mut Context, peer: &Peer) {
    let key = NodeAddressKey::with_flownode(peer.id).to_bytes();
    if let Ok(value) = NodeAddressValue::new(peer.clone()).try_as_raw_value() {
        let put = PutRequest {
            key,
            value,
            prev_kv: false,
        };

        match ctx.leader_cached_kv_backend.put(put).await {
            Ok(_) => {
                info!("Successfully updated flow `NodeAddressValue`: {:?}", peer);
                // TODO(discord): broadcast invalidating cache to all frontends
            }
            Err(e) => {
                error!(e; "Failed to update flow `NodeAddressValue`: {:?}", peer);
            }
        }
    } else {
        warn!("Failed to serialize flow `NodeAddressValue`: {:?}", peer);
    }
}
