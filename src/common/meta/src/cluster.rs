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

use async_trait::async_trait;
use common_error::ext::ErrorExt;

use crate::peer::Peer;

#[async_trait]
pub trait ClusterInfo {
    type Error: ErrorExt;

    /// List all nodes by role in the cluster. If `role` is `None`, list all nodes.
    async fn list_nodes(&self, role: Option<Role>) -> Result<Vec<NodeInfo>, Self::Error>;

    // TODO(jeremy): Other info, like region status, etc.
}

pub struct NodeInfo {
    pub peer: Peer,
    pub role: Role,
    /// Last active time in milliseconds.
    pub last_activity: i64,
}

pub enum Role {
    Datanode,
    Frontend,
    Metasrv,
}
