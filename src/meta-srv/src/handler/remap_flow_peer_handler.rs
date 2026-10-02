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

use api::v1::meta::{HeartbeatRequest, Role};
use common_meta::instruction::CacheIdent;
use common_meta::key::MetadataKey;
use common_meta::key::node_address::NodeAddressKey;
use common_telemetry::error;

use crate::Result;
use crate::handler::node_address::{self, NodeAddressUpdater};
use crate::handler::{HandleControl, HeartbeatAccumulator, HeartbeatHandler};
use crate::metasrv::Context;

#[derive(Debug, Default)]
pub struct RemapFlowPeerHandler {
    address_updater: NodeAddressUpdater,
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
        let updated = self
            .address_updater
            .update_if_needed((Role::Flownode, peer.id), req.node_epoch, || {
                let key = NodeAddressKey::with_flownode(peer.id).to_bytes();
                node_address::save_node_address(ctx, key, peer.clone())
            })
            .await
            .inspect_err(|e| {
                // Address errors must not prevent later heartbeat handlers from running.
                error!(e; "Failed to update flownode address, peer: {:?}", peer);
            });
        if let Ok(true) = updated {
            let cache_idents = [CacheIdent::FlowNodeAddressChange(peer.id)];
            node_address::invalidate_address_caches(ctx, peer.id, &cache_idents).await;
        }
        Ok(HandleControl::Continue)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handler::test_utils::TestEnv;
    use api::v1::meta::Peer;
    use std::sync::Arc;
    #[tokio::test]
    async fn test_notification_failure_keeps_persisted_epoch() {
        use crate::handler::{HeartbeatAccumulator, HeartbeatHandler};
        use api::v1::meta::HeartbeatRequest;

        use crate::handler::test_utils::FailingCacheInvalidator;
        use common_meta::key::MetadataValue;
        use common_meta::key::node_address::NodeAddressValue;
        let mut ctx = TestEnv::new().ctx();
        let invalidator = Arc::new(FailingCacheInvalidator::default());
        ctx.cache_invalidator = invalidator.clone();
        let handler = RemapFlowPeerHandler::default();
        let mut req = HeartbeatRequest {
            peer: Some(Peer {
                id: 1,
                addr: "first".into(),
            }),
            node_epoch: 1,
            ..Default::default()
        };
        let key = NodeAddressKey::with_flownode(1).to_bytes();
        handler
            .handle(&req, &mut ctx, &mut HeartbeatAccumulator::default())
            .await
            .unwrap();
        req.peer.as_mut().unwrap().addr = "same-epoch".into();
        handler
            .handle(&req, &mut ctx, &mut HeartbeatAccumulator::default())
            .await
            .unwrap();
        assert_eq!(invalidator.attempts(), 1);
        let kv = ctx.kv_backend.get(&key).await.unwrap().unwrap();
        assert_eq!(
            NodeAddressValue::try_from_raw_value(&kv.value)
                .unwrap()
                .peer
                .addr,
            "first"
        );
        req.node_epoch = 2;
        handler
            .handle(&req, &mut ctx, &mut HeartbeatAccumulator::default())
            .await
            .unwrap();
        assert_eq!(invalidator.attempts(), 2);
        let kv = ctx.kv_backend.get(&key).await.unwrap().unwrap();
        assert_eq!(
            NodeAddressValue::try_from_raw_value(&kv.value)
                .unwrap()
                .peer
                .addr,
            "same-epoch"
        );
    }
}
