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

use common_meta::cluster::{NodeInfo, NodeInfoKey, Role};
use common_meta::kv_backend::KvBackend;
use common_meta::rpc::store::RangeRequest;
use snafu::ResultExt;

use crate::cluster::MetaPeerClient;
use crate::error::{InvalidNodeInfoFormatSnafu, Result};

#[derive(Clone, Copy)]
pub enum NodeInfoType {
    Frontend,
    Datanode,
    Flownode,
}

impl From<NodeInfoType> for Role {
    fn from(node_info_type: NodeInfoType) -> Self {
        match node_info_type {
            NodeInfoType::Frontend => Role::Frontend,
            NodeInfoType::Datanode => Role::Datanode,
            NodeInfoType::Flownode => Role::Flownode,
        }
    }
}

/// Trait for types that can access node info.
#[async_trait::async_trait]
pub trait NodeInfoAccessor: Send + Sync {
    /// Returns the peer id and node info.
    async fn node_infos(&self, node_info_type: NodeInfoType) -> Result<Vec<(u64, NodeInfo)>>;
}

#[async_trait::async_trait]
impl NodeInfoAccessor for MetaPeerClient {
    async fn node_infos(&self, node_info_type: NodeInfoType) -> Result<Vec<(u64, NodeInfo)>> {
        let range_request = RangeRequest::new()
            .with_prefix(NodeInfoKey::key_prefix_with_role(node_info_type.into()));
        let response = self.range(range_request).await?;

        response
            .kvs
            .into_iter()
            .map(|kv| {
                let node_info = NodeInfo::try_from(kv.value).context(InvalidNodeInfoFormatSnafu)?;
                Ok((node_info.peer.id, node_info))
            })
            .collect::<Result<Vec<_>>>()
    }
}
