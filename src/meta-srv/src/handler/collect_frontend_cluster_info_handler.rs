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
use common_meta::cluster;
use common_meta::cluster::{FrontendStatus, NodeInfo, NodeInfoKey, NodeStatus};
use common_meta::rpc::store::PutRequest;
use common_telemetry::warn;
use snafu::ResultExt;

use crate::error::{Result, SaveClusterInfoSnafu};
use crate::handler::{HandleControl, HeartbeatAccumulator, HeartbeatHandler};
use crate::metasrv::Context;

pub struct CollectFrontendClusterInfoHandler;

#[async_trait::async_trait]
impl HeartbeatHandler for CollectFrontendClusterInfoHandler {
    fn is_acceptable(&self, role: Role) -> bool {
        role == Role::Frontend
    }

    async fn handle(
        &self,
        req: &HeartbeatRequest,
        ctx: &mut Context,
        _acc: &mut HeartbeatAccumulator,
    ) -> Result<HandleControl> {
        let HeartbeatRequest { header, peer, .. } = req;
        let Some(header) = &header else {
            return Ok(HandleControl::Continue);
        };
        let Some(peer) = &peer else {
            return Ok(HandleControl::Continue);
        };

        let key = NodeInfoKey {
            cluster_id: header.cluster_id,
            role: cluster::Role::Frontend,
            node_id: peer.id,
        };

        let value = NodeInfo {
            peer: peer.clone().into(),
            last_activity_ts: common_time::util::current_time_millis(),
            status: NodeStatus::Frontend(FrontendStatus {}),
        };

        let key = key.try_into().context(SaveClusterInfoSnafu)?;
        let value = value.try_into().context(SaveClusterInfoSnafu)?;
        let put_req = PutRequest {
            key,
            value,
            ..Default::default()
        };

        let res = ctx.in_memory.put(put_req).await;

        if let Err(err) = res {
            warn!("Failed to save datanode's cluster info, peer: {peer:?}, {err}");
        }

        Ok(HandleControl::Continue)
    }
}
