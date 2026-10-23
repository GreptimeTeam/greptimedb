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

use api::v1::meta::heartbeat_request::NodeWorkloads;
use common_error::ext::BoxedError;
use common_meta::cluster::NodeInfo;
use common_meta::error::Result;
use common_meta::peer::PeerDiscovery;
use snafu::ResultExt;

use crate::cluster::MetaPeerClientRef;
use crate::key::NodeSchedulingRole;
use crate::node_scheduling::NodeSchedulingManagerRef;

/// A [`PeerDiscovery`] decorator that stamps the scheduling state of datanodes
/// onto [`NodeInfo::schedulable`].
///
/// It never hides nodes: callers that place regions must filter with
/// `selector::common::active_schedulable_datanodes`. Keeping it additive is what allows it to be
/// the only peer discovery in the placement path (built once by `MetasrvBuilder`), because
/// failover still needs the full active set to compute its exclusions.
///
/// This is the only `PeerDiscovery` implementation over metasrv's local state: the raw
/// [`MetaPeerClient`](crate::cluster::MetaPeerClient) only exposes inherent discovery methods, so
/// a `PeerDiscoveryRef` cannot be built without the scheduling fence. Do not build a second
/// instance; `MetasrvBuilder` owns the single production one and exposes it as
/// `Metasrv::selector_ctx().peer_discovery`.
pub struct SchedulingAwarePeerDiscovery {
    manager: NodeSchedulingManagerRef,
    inner: MetaPeerClientRef,
}

impl SchedulingAwarePeerDiscovery {
    pub fn new(manager: NodeSchedulingManagerRef, inner: MetaPeerClientRef) -> Self {
        Self { manager, inner }
    }
}

#[async_trait::async_trait]
impl PeerDiscovery for SchedulingAwarePeerDiscovery {
    async fn active_frontends(&self) -> Result<Vec<NodeInfo>> {
        self.inner.active_frontends().await
    }

    async fn active_datanodes(
        &self,
        filter: Option<for<'a> fn(&'a NodeWorkloads) -> bool>,
    ) -> Result<Vec<NodeInfo>> {
        let mut datanodes = self.inner.active_datanodes(filter).await?;
        if datanodes.is_empty() {
            return Ok(datanodes);
        }

        let unschedulable = self
            .manager
            .list_unschedulable_node_ids(NodeSchedulingRole::Datanode)
            .await
            .map_err(BoxedError::new)
            .context(common_meta::error::ExternalSnafu)?;
        for datanode in &mut datanodes {
            datanode.schedulable = Some(!unschedulable.contains(&datanode.peer.id));
        }
        Ok(datanodes)
    }

    async fn active_flownodes(
        &self,
        filter: Option<for<'a> fn(&'a NodeWorkloads) -> bool>,
    ) -> Result<Vec<NodeInfo>> {
        self.inner.active_flownodes(filter).await
    }
}

#[cfg(test)]
mod tests {
    use common_meta::peer::Peer;

    use super::*;
    use crate::cluster::MetaPeerClientRef;
    use crate::test_util::{create_meta_peer_client, new_node_scheduling_discovery, put_datanodes};

    async fn new_discovery() -> (
        MetaPeerClientRef,
        NodeSchedulingManagerRef,
        SchedulingAwarePeerDiscovery,
    ) {
        let client = create_meta_peer_client();
        put_datanodes(
            &client,
            vec![Peer::empty(1), Peer::empty(2), Peer::empty(3)],
        )
        .await;
        let (manager, discovery) = new_node_scheduling_discovery(&client);
        (client, manager, discovery)
    }

    #[tokio::test]
    async fn test_active_datanodes_marks_unschedulable() {
        let (_, manager, discovery) = new_discovery().await;

        manager
            .mark_unschedulable(NodeSchedulingRole::Datanode, 2)
            .await
            .unwrap();

        let datanodes = discovery.active_datanodes(None).await.unwrap();
        assert_eq!(3, datanodes.len());

        let schedulable = |id: u64| {
            datanodes
                .iter()
                .find(|datanode| datanode.peer.id == id)
                .unwrap()
                .schedulable
        };
        assert_eq!(Some(true), schedulable(1));
        assert_eq!(Some(false), schedulable(2));
        assert_eq!(Some(true), schedulable(3));
    }

    #[tokio::test]
    async fn test_schedulable_marks_are_not_persisted() {
        let (client, manager, discovery) = new_discovery().await;

        manager
            .mark_unschedulable(NodeSchedulingRole::Datanode, 2)
            .await
            .unwrap();

        discovery.active_datanodes(None).await.unwrap();

        let datanodes = client.active_datanodes(None).await.unwrap();
        assert_eq!(3, datanodes.len());
        assert!(
            datanodes
                .iter()
                .all(|datanode| datanode.schedulable.is_none())
        );
    }
}
