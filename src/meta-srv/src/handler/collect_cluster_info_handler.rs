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

use api::v1::meta::{HeartbeatRequest, NodeInfo as PbNodeInfo, Role};
use common_meta::cluster::{
    DatanodeStatus, FlownodeStatus, FrontendStatus, NodeInfo, NodeInfoKey, NodeStatus,
};
use common_meta::peer::Peer;
use common_meta::rpc::store::PutRequest;
use snafu::ResultExt;
use store_api::region_engine::RegionRole;

use crate::error::{InvalidClusterInfoFormatSnafu, SaveClusterInfoSnafu};
use crate::handler::{HandleControl, HeartbeatAccumulator, HeartbeatHandler};
use crate::metasrv::Context;
use crate::Result;

/// The handler to collect cluster info from the heartbeat request of frontend.
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
        let Some((key, peer, info)) = extract_base_info(req) else {
            return Ok(HandleControl::Continue);
        };

        let value = NodeInfo {
            peer,
            last_activity_ts: common_time::util::current_time_millis(),
            status: NodeStatus::Frontend(FrontendStatus {}),
            version: info.version,
            git_commit: info.git_commit,
            start_time_ms: info.start_time_ms,
        };

        put_into_memory_store(ctx, key, value).await?;

        Ok(HandleControl::Continue)
    }
}

/// The handler to collect cluster info from the heartbeat request of flownode.
pub struct CollectFlownodeClusterInfoHandler;
#[async_trait::async_trait]
impl HeartbeatHandler for CollectFlownodeClusterInfoHandler {
    fn is_acceptable(&self, role: Role) -> bool {
        role == Role::Flownode
    }

    async fn handle(
        &self,
        req: &HeartbeatRequest,
        ctx: &mut Context,
        _acc: &mut HeartbeatAccumulator,
    ) -> Result<HandleControl> {
        let Some((key, peer, info)) = extract_base_info(req) else {
            return Ok(HandleControl::Continue);
        };

        let value = NodeInfo {
            peer,
            last_activity_ts: common_time::util::current_time_millis(),
            status: NodeStatus::Flownode(FlownodeStatus {}),
            version: info.version,
            git_commit: info.git_commit,
            start_time_ms: info.start_time_ms,
        };

        put_into_memory_store(ctx, key, value).await?;

        Ok(HandleControl::Continue)
    }
}

/// The handler to collect cluster info from the heartbeat request of datanode.
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
        let Some((key, peer, info)) = extract_base_info(req) else {
            return Ok(HandleControl::Continue);
        };

        let Some(stat) = &acc.stat else {
            return Ok(HandleControl::Continue);
        };

        let leader_regions = stat
            .region_stats
            .iter()
            .filter(|s| s.role == RegionRole::Leader)
            .count();
        let follower_regions = stat.region_stats.len() - leader_regions;

        let value = NodeInfo {
            peer,
            last_activity_ts: stat.timestamp_millis,
            status: NodeStatus::Datanode(DatanodeStatus {
                rcus: stat.rcus,
                wcus: stat.wcus,
                leader_regions,
                follower_regions,
                workloads: stat.datanode_workloads.clone(),
            }),
            version: info.version,
            git_commit: info.git_commit,
            start_time_ms: info.start_time_ms,
        };

        put_into_memory_store(ctx, key, value).await?;

        Ok(HandleControl::Continue)
    }
}

fn extract_base_info(request: &HeartbeatRequest) -> Option<(NodeInfoKey, Peer, PbNodeInfo)> {
    let HeartbeatRequest { peer, info, .. } = request;
    let key = NodeInfoKey::new(request)?;
    let Some(peer) = &peer else {
        return None;
    };
    let Some(info) = &info else {
        return None;
    };

    Some((key, peer.clone(), info.clone()))
}

async fn put_into_memory_store(ctx: &mut Context, key: NodeInfoKey, value: NodeInfo) -> Result<()> {
    let key = (&key).into();
    let value = value.try_into().context(InvalidClusterInfoFormatSnafu)?;
    let put_req = PutRequest {
        key,
        value,
        ..Default::default()
    };

    ctx.in_memory
        .put(put_req)
        .await
        .context(SaveClusterInfoSnafu)?;

    Ok(())
}
