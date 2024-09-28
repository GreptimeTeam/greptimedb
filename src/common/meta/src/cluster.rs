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

use std::str::FromStr;

use common_error::ext::ErrorExt;
use lazy_static::lazy_static;
use regex::Regex;
use serde::{Deserialize, Serialize};
use snafu::{ensure, OptionExt, ResultExt};

use crate::datanode::RegionStat;
use crate::error::{
    DecodeJsonSnafu, EncodeJsonSnafu, Error, FromUtf8Snafu, InvalidNodeInfoKeySnafu,
    InvalidRoleSnafu, ParseNumSnafu, Result,
};
use crate::peer::Peer;
use crate::ClusterId;

const CLUSTER_NODE_INFO_PREFIX: &str = "__meta_cluster_node_info";

lazy_static! {
    static ref CLUSTER_NODE_INFO_PREFIX_PATTERN: Regex = Regex::new(&format!(
        "^{CLUSTER_NODE_INFO_PREFIX}-([0-9]+)-([0-9]+)-([0-9]+)$"
    ))
    .unwrap();
}

/// [ClusterInfo] provides information about the cluster.
#[async_trait::async_trait]
pub trait ClusterInfo {
    type Error: ErrorExt;

    /// List all nodes by role in the cluster. If `role` is `None`, list all nodes.
    async fn list_nodes(
        &self,
        role: Option<Role>,
    ) -> std::result::Result<Vec<NodeInfo>, Self::Error>;

    /// List all region stats in the cluster.
    async fn list_region_stats(&self) -> std::result::Result<Vec<RegionStat>, Self::Error>;

    // TODO(jeremy): Other info, like region status, etc.
}

/// The key of [NodeInfo] in the storage. The format is `__meta_cluster_node_info-{cluster_id}-{role}-{node_id}`.
///
/// This key cannot be used to describe the `Metasrv` because the `Metasrv` does not have
/// a `cluster_id`, it serves multiple clusters.
#[derive(Debug, Clone, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct NodeInfoKey {
    /// The cluster id.
    pub cluster_id: ClusterId,
    /// The role of the node. It can be `[Role::Datanode]` or `[Role::Frontend]`.
    pub role: Role,
    /// The node id.
    pub node_id: u64,
}

impl NodeInfoKey {
    pub fn key_prefix_with_cluster_id(cluster_id: u64) -> String {
        format!("{}-{}-", CLUSTER_NODE_INFO_PREFIX, cluster_id)
    }

    pub fn key_prefix_with_role(cluster_id: ClusterId, role: Role) -> String {
        format!(
            "{}-{}-{}-",
            CLUSTER_NODE_INFO_PREFIX,
            cluster_id,
            i32::from(role)
        )
    }
}

/// The information of a node in the cluster.
#[derive(Debug, Serialize, Deserialize)]
pub struct NodeInfo {
    /// The peer information. [node_id, address]
    pub peer: Peer,
    /// Last activity time in milliseconds.
    pub last_activity_ts: i64,
    /// The status of the node. Different roles have different node status.
    pub status: NodeStatus,
    // The node build version
    pub version: String,
    // The node build git commit hash
    pub git_commit: String,
    // The node star timestamp
    pub start_time_ms: u64,
}

#[derive(Debug, Clone, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum Role {
    Datanode,
    Frontend,
    Flownode,
    Metasrv,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum NodeStatus {
    Datanode(DatanodeStatus),
    Frontend(FrontendStatus),
    Flownode(FlownodeStatus),
    Metasrv(MetasrvStatus),
    Standalone,
}

impl NodeStatus {
    // Get the role name of the node status
    pub fn role_name(&self) -> &str {
        match self {
            NodeStatus::Datanode(_) => "DATANODE",
            NodeStatus::Frontend(_) => "FRONTEND",
            NodeStatus::Flownode(_) => "FLOWNODE",
            NodeStatus::Metasrv(_) => "METASRV",
            NodeStatus::Standalone => "STANDALONE",
        }
    }
}

/// The status of a datanode.
#[derive(Debug, Serialize, Deserialize)]
pub struct DatanodeStatus {
    /// The read capacity units during this period.
    pub rcus: i64,
    /// The write capacity units during this period.
    pub wcus: i64,
    /// How many leader regions on this node.
    pub leader_regions: usize,
    /// How many follower regions on this node.
    pub follower_regions: usize,
}

/// The status of a frontend.
#[derive(Debug, Serialize, Deserialize)]
pub struct FrontendStatus {}

/// The status of a flownode.
#[derive(Debug, Serialize, Deserialize)]
pub struct FlownodeStatus {}

/// The status of a metasrv.
#[derive(Debug, Serialize, Deserialize)]
pub struct MetasrvStatus {
    pub is_leader: bool,
}

impl FromStr for NodeInfoKey {
    type Err = Error;

    fn from_str(key: &str) -> Result<Self> {
        let caps = CLUSTER_NODE_INFO_PREFIX_PATTERN
            .captures(key)
            .context(InvalidNodeInfoKeySnafu { key })?;

        ensure!(caps.len() == 4, InvalidNodeInfoKeySnafu { key });

        let cluster_id = caps[1].to_string();
        let role = caps[2].to_string();
        let node_id = caps[3].to_string();
        let cluster_id: u64 = cluster_id.parse().context(ParseNumSnafu {
            err_msg: format!("invalid cluster_id: {cluster_id}"),
        })?;
        let role: i32 = role.parse().context(ParseNumSnafu {
            err_msg: format!("invalid role {role}"),
        })?;
        let role = Role::try_from(role)?;
        let node_id: u64 = node_id.parse().context(ParseNumSnafu {
            err_msg: format!("invalid node_id: {node_id}"),
        })?;

        Ok(Self {
            cluster_id,
            role,
            node_id,
        })
    }
}

impl TryFrom<Vec<u8>> for NodeInfoKey {
    type Error = Error;

    fn try_from(bytes: Vec<u8>) -> Result<Self> {
        String::from_utf8(bytes)
            .context(FromUtf8Snafu {
                name: "NodeInfoKey",
            })
            .map(|x| x.parse())?
    }
}

impl From<NodeInfoKey> for Vec<u8> {
    fn from(key: NodeInfoKey) -> Self {
        format!(
            "{}-{}-{}-{}",
            CLUSTER_NODE_INFO_PREFIX,
            key.cluster_id,
            i32::from(key.role),
            key.node_id
        )
        .into_bytes()
    }
}

impl FromStr for NodeInfo {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self> {
        serde_json::from_str(value).context(DecodeJsonSnafu)
    }
}

impl TryFrom<Vec<u8>> for NodeInfo {
    type Error = Error;

    fn try_from(bytes: Vec<u8>) -> Result<Self> {
        String::from_utf8(bytes)
            .context(FromUtf8Snafu { name: "NodeInfo" })
            .map(|x| x.parse())?
    }
}

impl TryFrom<NodeInfo> for Vec<u8> {
    type Error = Error;

    fn try_from(info: NodeInfo) -> Result<Self> {
        Ok(serde_json::to_string(&info)
            .context(EncodeJsonSnafu)?
            .into_bytes())
    }
}

impl From<Role> for i32 {
    fn from(role: Role) -> Self {
        match role {
            Role::Datanode => 0,
            Role::Frontend => 1,
            Role::Flownode => 2,
            Role::Metasrv => 99,
        }
    }
}

impl TryFrom<i32> for Role {
    type Error = Error;

    fn try_from(role: i32) -> Result<Self> {
        match role {
            0 => Ok(Self::Datanode),
            1 => Ok(Self::Frontend),
            2 => Ok(Self::Flownode),
            99 => Ok(Self::Metasrv),
            _ => InvalidRoleSnafu { role }.fail(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use crate::cluster::Role::{Datanode, Frontend};
    use crate::cluster::{DatanodeStatus, NodeInfo, NodeInfoKey, NodeStatus};
    use crate::peer::Peer;

    #[test]
    fn test_node_info_key_round_trip() {
        let key = NodeInfoKey {
            cluster_id: 1,
            role: Datanode,
            node_id: 2,
        };

        let key_bytes: Vec<u8> = key.into();
        let new_key: NodeInfoKey = key_bytes.try_into().unwrap();

        assert_eq!(1, new_key.cluster_id);
        assert_eq!(Datanode, new_key.role);
        assert_eq!(2, new_key.node_id);
    }

    #[test]
    fn test_node_info_round_trip() {
        let node_info = NodeInfo {
            peer: Peer {
                id: 1,
                addr: "127.0.0.1".to_string(),
            },
            last_activity_ts: 123,
            status: NodeStatus::Datanode(DatanodeStatus {
                rcus: 1,
                wcus: 2,
                leader_regions: 3,
                follower_regions: 4,
            }),
            version: "".to_string(),
            git_commit: "".to_string(),
            start_time_ms: 1,
        };

        let node_info_bytes: Vec<u8> = node_info.try_into().unwrap();
        let new_node_info: NodeInfo = node_info_bytes.try_into().unwrap();

        assert_matches!(
            new_node_info,
            NodeInfo {
                peer: Peer { id: 1, .. },
                last_activity_ts: 123,
                status: NodeStatus::Datanode(DatanodeStatus {
                    rcus: 1,
                    wcus: 2,
                    leader_regions: 3,
                    follower_regions: 4,
                }),
                start_time_ms: 1,
                ..
            }
        );
    }

    #[test]
    fn test_node_info_key_prefix() {
        let prefix = NodeInfoKey::key_prefix_with_cluster_id(1);
        assert_eq!(prefix, "__meta_cluster_node_info-1-");

        let prefix = NodeInfoKey::key_prefix_with_role(2, Frontend);
        assert_eq!(prefix, "__meta_cluster_node_info-2-1-");
    }
}
