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
use common_meta::cluster::{DatanodeStatus, NodeInfo, NodeInfoKey, NodeStatus};
use common_meta::rpc::store::PutRequest;
use snafu::ResultExt;
use store_api::region_engine::RegionRole;

use crate::error::{Result, SaveClusterInfoSnafu};
use crate::handler::{HandleControl, HeartbeatAccumulator, HeartbeatHandler};
use crate::metasrv::Context;

pub struct CollectDatanodeClusterInfoHandler;

#[async_trait::async_trait]
impl HeartbeatHandler for CollectDatanodeClusterInfoHandler {
    fn is_acceptable(&self, role: Role) -> bool {
        role == Role::Datanode
    }

    async fn handle(
        &self,
        req: &HeartbeatRequest,
        ctx: &mut Context,
        acc: &mut HeartbeatAccumulator,
    ) -> Result<HandleControl> {
        let HeartbeatRequest { header, peer, .. } = req;
        let Some(header) = &header else {
            return Ok(HandleControl::Continue);
        };
        let Some(peer) = &peer else {
            return Ok(HandleControl::Continue);
        };
        let Some(stat) = &acc.stat else {
            return Ok(HandleControl::Continue);
        };

        let key = NodeInfoKey {
            cluster_id: header.cluster_id,
            role: cluster::Role::Datanode,
            node_id: peer.id,
        };

        let leader_regions = stat
            .region_stats
            .iter()
            .filter(|s| s.role == RegionRole::Leader)
            .count();
        let follower_regions = stat.region_stats.len() - leader_regions;

        let value = NodeInfo {
            peer: peer.clone().into(),
            last_activity_ts: stat.timestamp_millis,
            status: NodeStatus::Datanode(DatanodeStatus {
                rcus: stat.rcus,
                wcus: stat.wcus,
                leader_regions,
                follower_regions,
            }),
        };

        let key = key.try_into().context(SaveClusterInfoSnafu)?;
        let value = value.try_into().context(SaveClusterInfoSnafu)?;
        let put_req = PutRequest {
            key,
            value,
            ..Default::default()
        };

        ctx.in_memory
            .put(put_req)
            .await
            .context(SaveClusterInfoSnafu)?;

        Ok(HandleControl::Continue)
    }
}
