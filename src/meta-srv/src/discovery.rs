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

pub mod lease;
pub mod node_info;
pub mod utils;

use api::v1::meta::heartbeat_request::NodeWorkloads;
use common_error::ext::BoxedError;
use common_meta::distributed_time_constants::default_distributed_time_constants;
use common_meta::error::Result;
use common_meta::peer::{Peer, PeerDiscovery, PeerResolver};
use common_meta::{DatanodeId, FlownodeId};
use common_time::util::DefaultSystemTimer;
use snafu::ResultExt;

use crate::cluster::MetaPeerClient;
use crate::discovery::lease::{LeaseValueAccessor, LeaseValueType};

#[async_trait::async_trait]
impl PeerDiscovery for MetaPeerClient {
    async fn active_frontends(&self) -> Result<Vec<Peer>> {
        utils::alive_frontends(
            &DefaultSystemTimer,
            self,
            default_distributed_time_constants().frontend_heartbeat_interval,
        )
        .await
        .map_err(BoxedError::new)
        .context(common_meta::error::ExternalSnafu)
    }

    async fn active_datanodes(
        &self,
        filter: Option<for<'a> fn(&'a NodeWorkloads) -> bool>,
    ) -> Result<Vec<Peer>> {
        utils::alive_datanodes(
            &DefaultSystemTimer,
            self,
            default_distributed_time_constants().datanode_lease,
            filter,
        )
        .await
        .map_err(BoxedError::new)
        .context(common_meta::error::ExternalSnafu)
    }

    async fn active_flownodes(
        &self,
        filter: Option<for<'a> fn(&'a NodeWorkloads) -> bool>,
    ) -> Result<Vec<Peer>> {
        utils::alive_flownodes(
            &DefaultSystemTimer,
            self,
            default_distributed_time_constants().flownode_lease,
            filter,
        )
        .await
        .map_err(BoxedError::new)
        .context(common_meta::error::ExternalSnafu)
    }
}

#[async_trait::async_trait]
impl PeerResolver for MetaPeerClient {
    async fn datanode(&self, id: DatanodeId) -> Result<Option<Peer>> {
        let peer = self
            .lease_value(LeaseValueType::Datanode, id)
            .await
            .map_err(BoxedError::new)
            .context(common_meta::error::ExternalSnafu)?
            .map(|(peer_id, lease)| Peer::new(peer_id, lease.node_addr));
        Ok(peer)
    }

    async fn flownode(&self, id: FlownodeId) -> Result<Option<Peer>> {
        let peer = self
            .lease_value(LeaseValueType::Flownode, id)
            .await
            .map_err(BoxedError::new)
            .context(common_meta::error::ExternalSnafu)?
            .map(|(peer_id, lease)| Peer::new(peer_id, lease.node_addr));
        Ok(peer)
    }
}
