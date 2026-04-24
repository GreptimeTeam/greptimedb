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

use client::{Client, Database, Output};
use common_error::ext::BoxedError;
use common_meta::peer::PeerDiscoveryRef;
use common_telemetry::{debug, warn};
use snafu::{ResultExt, ensure};
use tokio::sync::RwLock;

use crate::error::{ListActiveFrontendsSnafu, NoAvailableFrontendSnafu, OtherSnafu, Result};

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
    ) -> Result<u32> {
        let client = self.maybe_init_client().await?;
        let database = Database::new(ctx.catalog, ctx.schema, client);

        let result = database
            .row_inserts_with_hints(requests, hints)
            .await
            .map_err(BoxedError::new)
            .context(OtherSnafu);

        if result.is_err() {
            self.reset_client().await;
        }

        result
    }

    /// Executes a serialized logical plan on an available frontend.
    pub async fn logical_plan(&self, ctx: &DatabaseContext<'_>, plan: Vec<u8>) -> Result<Output> {
        let client = self.maybe_init_client().await?;
        let database = Database::new(ctx.catalog, ctx.schema, client);

        let result = database
            .logical_plan(plan)
            .await
            .map_err(BoxedError::new)
            .context(OtherSnafu);

        if result.is_err() {
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
            .map(|peer| peer.addr)
            .collect::<Vec<_>>();

        debug!("Available frontend addresses: {:?}", urls);

        Ok(Client::with_urls(urls))
    }

    async fn maybe_init_client(&self) -> Result<Client> {
        let mut guard = self.client.write().await;
        if let Some(client) = guard.as_ref() {
            return Ok(client.clone());
        }

        let client = self.build_client().await?;
        *guard = Some(client.clone());
        Ok(client)
    }

    async fn reset_client(&self) {
        warn!("Resetting the client");
        let mut guard = self.client.write().await;
        guard.take();
    }
}
