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

use std::sync::Arc;

use client::error::{ExternalSnafu, Result as ClientResult};
use client::{Client, Database, Output};
use common_error::ext::BoxedError;
use common_grpc::channel_manager::ChannelManager;
use common_meta::peer::PeerDiscoveryRef;
use common_telemetry::{debug, warn};
use snafu::{ResultExt, ensure};
use tokio::sync::RwLock;

use crate::error::{ListActiveFrontendsSnafu, NoAvailableFrontendSnafu, Result};

pub type DatabaseOperatorRef = Arc<DatabaseOperator>;

#[derive(Debug, Clone, Copy)]
/// Database-level request context used by metasrv forwarding.
pub struct DatabaseContext<'a> {
    /// Catalog name carried in forwarded requests.
    pub catalog: &'a str,
    /// Schema name carried in forwarded requests.
    pub schema: &'a str,
}

impl<'a> DatabaseContext<'a> {
    /// Creates a new database context from catalog and schema.
    pub fn new(catalog: &'a str, schema: &'a str) -> Self {
        Self { catalog, schema }
    }
}

/// A cached frontend database operator used by metasrv.
///
/// Its cached clients use independent query and control channel-manager pools;
/// the legacy single-manager client constructors intentionally remain shared.
pub struct DatabaseOperator {
    peer_discovery: PeerDiscoveryRef,
    client: RwLock<Option<Client>>,
}

impl DatabaseOperator {
    /// Creates a database operator backed by discovered frontend peers.
    pub fn new(peer_discovery: PeerDiscoveryRef) -> Self {
        Self {
            peer_discovery,
            client: RwLock::new(None),
        }
    }

    /// Forwards row inserts to an available frontend database client.
    pub async fn insert(
        &self,
        ctx: &DatabaseContext<'_>,
        requests: api::v1::RowInsertRequests,
        hints: &[(&str, &str)],
    ) -> ClientResult<u32> {
        let client = self.maybe_init_client().await?;
        let database = Database::new(ctx.catalog, ctx.schema, client);

        let result = database.row_inserts_with_hints(requests, hints).await;

        if should_reset_client(&result) {
            self.reset_client().await;
        }

        result
    }

    /// Executes a serialized logical plan on an available frontend.
    pub async fn logical_plan(
        &self,
        ctx: &DatabaseContext<'_>,
        plan: Vec<u8>,
    ) -> ClientResult<Output> {
        let client = self.maybe_init_client().await?;
        let database = Database::new(ctx.catalog, ctx.schema, client);

        let result = database.logical_plan(plan).await;

        if should_reset_client(&result) {
            self.reset_client().await;
        }

        result
    }

    async fn build_client(&self) -> Result<Client> {
        let frontends = self
            .peer_discovery
            .active_frontends()
            .await
            .context(ListActiveFrontendsSnafu)?;

        ensure!(!frontends.is_empty(), NoAvailableFrontendSnafu);

        let urls = frontends
            .into_iter()
            .map(|node| node.peer.addr)
            .collect::<Vec<_>>();

        debug!("Available frontend addresses: {:?}", urls);

        Ok(Client::with_query_and_control_managers(
            ChannelManager::new(),
            ChannelManager::new(),
            urls,
        ))
    }

    async fn maybe_init_client(&self) -> ClientResult<Client> {
        if let Some(client) = self.client.read().await.as_ref() {
            return Ok(client.clone());
        }

        let client = self
            .build_client()
            .await
            .map_err(BoxedError::new)
            .context(ExternalSnafu)?;

        let mut guard = self.client.write().await;
        if let Some(client) = guard.as_ref() {
            return Ok(client.clone());
        }

        *guard = Some(client.clone());
        Ok(client)
    }

    async fn reset_client(&self) {
        warn!("Resetting the client");
        let mut guard = self.client.write().await;
        guard.take();
    }
}

fn should_reset_client<T>(result: &client::error::Result<T>) -> bool {
    result
        .as_ref()
        .err()
        .map(|err| err.is_connection_error())
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use api::v1::meta::heartbeat_request::NodeWorkloads;
    use common_meta::cluster::{FrontendStatus, NodeInfo, NodeStatus};
    use common_meta::peer::{Peer, PeerDiscovery};

    use super::*;

    struct TestPeerDiscovery;

    #[async_trait::async_trait]
    impl PeerDiscovery for TestPeerDiscovery {
        async fn active_frontends(&self) -> common_meta::error::Result<Vec<NodeInfo>> {
            Ok(vec![NodeInfo {
                peer: Peer::new(1, "127.0.0.1:3001".to_string()),
                last_activity_ts: 0,
                status: NodeStatus::Frontend(FrontendStatus::default()),
                version: String::new(),
                git_commit: String::new(),
                start_time_ms: 0,
                total_cpu_millicores: 0,
                total_memory_bytes: 0,
                cpu_usage_millicores: 0,
                memory_usage_bytes: 0,
                hostname: String::new(),
                env_vars: Default::default(),
            }])
        }

        async fn active_datanodes(
            &self,
            _filter: Option<for<'a> fn(&'a NodeWorkloads) -> bool>,
        ) -> common_meta::error::Result<Vec<NodeInfo>> {
            unreachable!()
        }

        async fn active_flownodes(
            &self,
            _filter: Option<for<'a> fn(&'a NodeWorkloads) -> bool>,
        ) -> common_meta::error::Result<Vec<NodeInfo>> {
            unreachable!()
        }
    }

    #[tokio::test]
    async fn test_build_client_uses_isolated_reused_channel_pools() {
        let operator = DatabaseOperator::new(Arc::new(TestPeerDiscovery));
        let client = operator.build_client().await.unwrap();

        client.make_flight_client(false, false).unwrap();
        client.make_flight_client(false, false).unwrap();
        assert_eq!((1, 0), client.channel_pool_sizes());

        client.find_channel().unwrap();
        client.find_channel().unwrap();
        assert_eq!((1, 1), client.channel_pool_sizes());
    }
}
