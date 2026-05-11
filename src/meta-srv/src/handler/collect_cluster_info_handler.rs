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

use std::collections::HashMap;

use api::v1::meta::{HeartbeatRequest, NodeInfo as PbNodeInfo, Role};
use common_meta::cluster::{
    DatanodeStatus, FlownodeStatus, FrontendStatus, NodeInfo, NodeInfoKey, NodeStatus,
};
use common_meta::datanode::EnvVars;
use common_meta::heartbeat::utils::{get_datanode_workloads, get_flownode_workloads};
use common_meta::peer::Peer;
use common_meta::rpc::store::PutRequest;
use common_telemetry::warn;
use snafu::ResultExt;
use store_api::region_engine::RegionRole;

use crate::Result;
use crate::error::{InvalidClusterInfoFormatSnafu, SaveClusterInfoSnafu};
use crate::handler::{HandleControl, HeartbeatAccumulator, HeartbeatHandler};
use crate::metasrv::Context;

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
        let Some((key, peer, info, env_vars)) = extract_base_info(req) else {
            return Ok(HandleControl::Continue);
        };

        let value = NodeInfo {
            peer,
            last_activity_ts: common_time::util::current_time_millis(),
            status: NodeStatus::Frontend(FrontendStatus {}),
            version: info.version,
            git_commit: info.git_commit,
            start_time_ms: info.start_time_ms,
            total_cpu_millicores: info.total_cpu_millicores,
            total_memory_bytes: info.total_memory_bytes,
            cpu_usage_millicores: info.cpu_usage_millicores,
            memory_usage_bytes: info.memory_usage_bytes,
            hostname: info.hostname,
            env_vars,
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
        let Some((key, peer, info, env_vars)) = extract_base_info(req) else {
            return Ok(HandleControl::Continue);
        };
        let flownode_workloads = get_flownode_workloads(req.node_workloads.as_ref());

        let value = NodeInfo {
            peer,
            last_activity_ts: common_time::util::current_time_millis(),
            status: NodeStatus::Flownode(FlownodeStatus {
                workloads: flownode_workloads,
            }),
            version: info.version,
            git_commit: info.git_commit,
            start_time_ms: info.start_time_ms,
            total_cpu_millicores: info.total_cpu_millicores,
            total_memory_bytes: info.total_memory_bytes,
            cpu_usage_millicores: info.cpu_usage_millicores,
            memory_usage_bytes: info.memory_usage_bytes,
            hostname: info.hostname,
            env_vars,
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
        let Some((key, peer, info, env_vars)) = extract_base_info(req) else {
            return Ok(HandleControl::Continue);
        };

        let (last_activity_ts, status) = if let Some(stat) = &acc.stat {
            let leader_regions = stat
                .region_stats
                .iter()
                .filter(|s| matches!(s.role, RegionRole::Leader | RegionRole::StagingLeader))
                .count();
            let follower_regions = stat.region_stats.len() - leader_regions;

            (
                stat.timestamp_millis,
                DatanodeStatus {
                    rcus: stat.rcus,
                    wcus: stat.wcus,
                    leader_regions,
                    follower_regions,
                    workloads: stat.datanode_workloads.clone(),
                },
            )
        } else {
            (
                common_time::util::current_time_millis(),
                DatanodeStatus {
                    rcus: 0,
                    wcus: 0,
                    leader_regions: 0,
                    follower_regions: 0,
                    workloads: get_datanode_workloads(req.node_workloads.as_ref()),
                },
            )
        };

        let value = NodeInfo {
            peer,
            last_activity_ts,
            status: NodeStatus::Datanode(status),
            version: info.version,
            git_commit: info.git_commit,
            start_time_ms: info.start_time_ms,
            total_cpu_millicores: info.total_cpu_millicores,
            total_memory_bytes: info.total_memory_bytes,
            cpu_usage_millicores: info.cpu_usage_millicores,
            memory_usage_bytes: info.memory_usage_bytes,
            hostname: info.hostname,
            env_vars,
        };

        put_into_memory_store(ctx, key, value).await?;

        Ok(HandleControl::Continue)
    }
}

fn extract_base_info(
    request: &HeartbeatRequest,
) -> Option<(NodeInfoKey, Peer, PbNodeInfo, HashMap<String, String>)> {
    let HeartbeatRequest { peer, info, .. } = request;
    let key = NodeInfoKey::new(request)?;
    let Some(peer) = &peer else {
        return None;
    };
    let Some(info) = &info else {
        return None;
    };

    let env_vars = EnvVars::from_extensions(&request.extensions)
        .inspect_err(|e| {
            warn!(e;
                "Failed to deserialize __env_vars from heartbeat extensions, peer: {}", peer
            );
        })
        .unwrap_or_default()
        .map(|e| e.vars)
        .unwrap_or_default();

    Some((key, peer.clone(), info.clone(), env_vars))
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use api::v1::meta::RequestHeader;

    use super::*;
    use crate::handler::test_utils::TestEnv;

    #[tokio::test]
    async fn test_datanode_cluster_info_saved_without_stats() {
        let env = TestEnv::new();
        let mut ctx = env.ctx();
        let handler = CollectDatanodeClusterInfoHandler;
        let mut acc = HeartbeatAccumulator::default();

        let mut env_vars = HashMap::new();
        env_vars.insert("AZ".to_string(), "az-a".to_string());
        let mut extensions = HashMap::new();
        EnvVars::new(env_vars).into_extensions(&mut extensions);

        let req = HeartbeatRequest {
            header: Some(RequestHeader {
                role: Role::Datanode as i32,
                ..Default::default()
            }),
            peer: Some(Peer::new(42, "127.0.0.1:4001".to_string())),
            info: Some(PbNodeInfo {
                version: "1.0.0".to_string(),
                git_commit: "abcdef".to_string(),
                start_time_ms: 123,
                ..Default::default()
            }),
            extensions,
            ..Default::default()
        };

        handler.handle(&req, &mut ctx, &mut acc).await.unwrap();

        let key = NodeInfoKey {
            role: common_meta::cluster::Role::Datanode,
            node_id: 42,
        };
        let key: Vec<u8> = (&key).into();
        let kv = ctx.in_memory.get(&key).await.unwrap().unwrap();
        let node_info = NodeInfo::try_from(kv.value).unwrap();

        assert_eq!(node_info.peer.id, 42);
        assert_eq!(
            node_info.env_vars.get("AZ").map(String::as_str),
            Some("az-a")
        );
        assert!(matches!(
            node_info.status,
            NodeStatus::Datanode(DatanodeStatus { workloads, .. })
                if workloads == get_datanode_workloads(req.node_workloads.as_ref())
        ));
    }
}
