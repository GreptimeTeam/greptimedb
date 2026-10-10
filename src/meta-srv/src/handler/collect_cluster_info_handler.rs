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
use common_meta::heartbeat::utils::{get_flownode_workloads, get_frontend_workloads};
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
        let Some(update) = NodeInfoUpdate::from_request(req) else {
            return Ok(HandleControl::Continue);
        };

        let frontend_workloads = get_frontend_workloads(req.node_workloads.as_ref());

        update
            .save_to_memory(
                ctx,
                common_time::util::current_time_millis(),
                NodeStatus::Frontend(FrontendStatus {
                    workloads: frontend_workloads,
                }),
            )
            .await?;

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
        let Some(update) = NodeInfoUpdate::from_request(req) else {
            return Ok(HandleControl::Continue);
        };
        let flownode_workloads = get_flownode_workloads(req.node_workloads.as_ref());

        update
            .save_to_memory(
                ctx,
                common_time::util::current_time_millis(),
                NodeStatus::Flownode(FlownodeStatus {
                    workloads: flownode_workloads,
                }),
            )
            .await?;

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
        let Some(update) = NodeInfoUpdate::from_request(req) else {
            return Ok(HandleControl::Continue);
        };

        let Some(stat) = &acc.stat else {
            return Ok(HandleControl::Continue);
        };

        let leader_regions = stat
            .region_stats
            .iter()
            .filter(|s| matches!(s.role, RegionRole::Leader | RegionRole::StagingLeader))
            .count();
        let follower_regions = stat.region_stats.len() - leader_regions;

        update
            .save_to_memory(
                ctx,
                stat.timestamp_millis,
                NodeStatus::Datanode(DatanodeStatus {
                    rcus: stat.rcus,
                    wcus: stat.wcus,
                    leader_regions,
                    follower_regions,
                    workloads: stat.datanode_workloads.clone(),
                }),
            )
            .await?;

        Ok(HandleControl::Continue)
    }
}

/// Common fields collected for one heartbeat's in-memory node information
/// update. Persistent routing addresses have a separate lifecycle.
struct NodeInfoUpdate {
    key: NodeInfoKey,
    peer: Peer,
    info: PbNodeInfo,
    env_vars: HashMap<String, String>,
}

impl NodeInfoUpdate {
    fn from_request(request: &HeartbeatRequest) -> Option<Self> {
        let key = NodeInfoKey::new(request)?;
        let peer = request.peer.clone()?;
        let info = request.info.clone()?;
        let env_vars = EnvVars::from_extensions(&request.extensions)
            .inspect_err(|e| {
                warn!(e; "Failed to deserialize __env_vars from heartbeat extensions, peer: {}", peer);
            })
            .unwrap_or_default()
            .map(|e| e.vars)
            .unwrap_or_default();
        Some(Self {
            key,
            peer,
            info,
            env_vars,
        })
    }

    async fn save_to_memory(
        self,
        ctx: &Context,
        last_activity_ts: i64,
        status: NodeStatus,
    ) -> Result<()> {
        let info = self.info;
        let value = NodeInfo {
            peer: self.peer,
            last_activity_ts,
            status,
            version: info.version,
            git_commit: info.git_commit,
            start_time_ms: info.start_time_ms,
            total_cpu_millicores: info.total_cpu_millicores,
            total_memory_bytes: info.total_memory_bytes,
            cpu_usage_millicores: info.cpu_usage_millicores,
            memory_usage_bytes: info.memory_usage_bytes,
            hostname: info.hostname,
            env_vars: self.env_vars,
        };
        let value = value.try_into().context(InvalidClusterInfoFormatSnafu)?;
        ctx.in_memory
            .put(PutRequest {
                key: (&self.key).into(),
                value,
                ..Default::default()
            })
            .await
            .context(SaveClusterInfoSnafu)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use api::v1::meta::RequestHeader;
    use common_meta::datanode::Stat;

    use super::*;
    use crate::handler::test_utils::TestEnv;

    #[tokio::test]
    async fn test_node_info_refreshes_without_epoch_change() {
        let mut ctx = TestEnv::new().ctx();
        let handlers: Vec<(Role, Box<dyn HeartbeatHandler>)> = vec![
            (Role::Frontend, Box::new(CollectFrontendClusterInfoHandler)),
            (Role::Datanode, Box::new(CollectDatanodeClusterInfoHandler)),
            (Role::Flownode, Box::new(CollectFlownodeClusterInfoHandler)),
        ];
        for (role, handler) in handlers {
            let mut req = HeartbeatRequest {
                header: Some(RequestHeader {
                    role: role as i32,
                    ..Default::default()
                }),
                peer: Some(Peer {
                    id: 7,
                    addr: "node:3001".into(),
                }),
                info: Some(PbNodeInfo {
                    version: "v1".into(),
                    hostname: "host".into(),
                    ..Default::default()
                }),
                node_epoch: 10,
                ..Default::default()
            };
            let mut acc = HeartbeatAccumulator {
                stat: Some(Stat {
                    timestamp_millis: 123,
                    rcus: 5,
                    wcus: 6,
                    ..Default::default()
                }),
                ..Default::default()
            };
            handler.handle(&req, &mut ctx, &mut acc).await.unwrap();
            let key: Vec<u8> = (&NodeInfoKey::new(&req).unwrap()).into();
            let first =
                NodeInfo::try_from(ctx.in_memory.get(&key).await.unwrap().unwrap().value).unwrap();
            assert_eq!(first.peer, req.peer.clone().unwrap());
            assert_eq!(first.version, "v1");
            assert_eq!(first.hostname, "host");
            req.info.as_mut().unwrap().version = "v2".into();
            req.info.as_mut().unwrap().memory_usage_bytes = 42;
            acc.stat.as_mut().unwrap().timestamp_millis = 456;
            handler.handle(&req, &mut ctx, &mut acc).await.unwrap();
            let second =
                NodeInfo::try_from(ctx.in_memory.get(&key).await.unwrap().unwrap().value).unwrap();
            assert_eq!(second.version, "v2");
            assert_eq!(second.memory_usage_bytes, 42);
            assert_eq!(
                serde_json::to_value(&second.status).unwrap(),
                serde_json::to_value(&first.status).unwrap()
            );
            if role == Role::Datanode {
                assert_eq!(second.last_activity_ts, 456);
            }
            // Node information must not be persisted to the configured backend.
            assert!(ctx.kv_backend.get(&key).await.unwrap().is_none());
        }
    }
}
