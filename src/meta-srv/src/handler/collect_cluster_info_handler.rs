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
use common_meta::cluster;
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
        let Some((key, peer, info)) = extract_base_info(req, Role::Frontend) else {
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
        let Some((key, peer, info)) = extract_base_info(req, Role::Flownode) else {
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
        let Some((key, peer, info)) = extract_base_info(req, Role::Datanode) else {
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
            }),
            version: info.version,
            git_commit: info.git_commit,
            start_time_ms: info.start_time_ms,
        };

        put_into_memory_store(ctx, key, value).await?;

        Ok(HandleControl::Continue)
    }
}

fn extract_base_info(
    req: &HeartbeatRequest,
    role: Role,
) -> Option<(NodeInfoKey, Peer, PbNodeInfo)> {
    let HeartbeatRequest {
        header, peer, info, ..
    } = req;
    let Some(header) = &header else {
        return None;
    };
    let Some(peer) = &peer else {
        return None;
    };
    let Some(info) = &info else {
        return None;
    };

    Some((
        NodeInfoKey {
            cluster_id: header.cluster_id,
            role: match role {
                Role::Datanode => cluster::Role::Datanode,
                Role::Frontend => cluster::Role::Frontend,
                Role::Flownode => cluster::Role::Flownode,
            },
            node_id: match role {
                Role::Datanode => peer.id,
                Role::Flownode => peer.id,
                // The ID is solely for ensuring the key's uniqueness and serves no other purpose.
                Role::Frontend => allocate_id_by_peer_addr(peer.addr.as_str()),
            },
        },
        Peer::from(peer.clone()),
        info.clone(),
    ))
}

async fn put_into_memory_store(ctx: &mut Context, key: NodeInfoKey, value: NodeInfo) -> Result<()> {
    let key = key.into();
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

// Allocate id based on peer address using a hash function
fn allocate_id_by_peer_addr(peer_addr: &str) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();
    peer_addr.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod allocate_id_tests {
    use super::*;

    #[test]
    fn test_allocate_id_by_peer_addr() {
        // Test empty string
        assert_eq!(allocate_id_by_peer_addr(""), allocate_id_by_peer_addr(""));

        // Test same address returns same id
        let addr1 = "127.0.0.1:8080";
        let id1 = allocate_id_by_peer_addr(addr1);
        let id2 = allocate_id_by_peer_addr(addr1);
        assert_eq!(id1, id2);

        // Test different addresses return different ids
        let addr2 = "127.0.0.1:8081";
        let id3 = allocate_id_by_peer_addr(addr2);
        assert_ne!(id1, id3);

        // Test long address
        let long_addr = "very.long.domain.name.example.com:9999";
        let id4 = allocate_id_by_peer_addr(long_addr);
        assert!(id4 > 0);
    }
}
