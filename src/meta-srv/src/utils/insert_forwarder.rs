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

use api::v1::RowInsertRequests;
use client::inserter::{Context, InsertOptions, Inserter};
use client::{Client, Database};
use common_error::ext::BoxedError;
use common_meta::peer::PeerLookupServiceRef;
use common_telemetry::{debug, warn};
use snafu::{ensure, ResultExt};
use tokio::sync::RwLock;

use crate::error::{LookupFrontendsSnafu, NoAvailableFrontendSnafu};

pub type InsertForwarderRef = Arc<InsertForwarder>;

/// [`InsertForwarder`] is the inserter for the metasrv.
/// It forwards insert requests to available frontend instances.
pub struct InsertForwarder {
    peer_lookup_service: PeerLookupServiceRef,
    client: RwLock<Option<Client>>,
}

impl InsertForwarder {
    /// Creates a new InsertForwarder with the given peer lookup service.
    pub fn new(peer_lookup_service: PeerLookupServiceRef) -> Self {
        Self {
            peer_lookup_service,
            client: RwLock::new(None),
        }
    }

    /// Builds a new client.
    async fn build_client(&self) -> crate::error::Result<Client> {
        let frontends = self
            .peer_lookup_service
            .active_frontends()
            .await
            .context(LookupFrontendsSnafu)?;

        ensure!(!frontends.is_empty(), NoAvailableFrontendSnafu);

        let urls = frontends
            .into_iter()
            .map(|peer| peer.addr)
            .collect::<Vec<_>>();

        debug!("Available frontend addresses: {:?}", urls);

        Ok(Client::with_urls(urls))
    }

    /// Initializes the client if it hasn't been initialized yet, or returns the cached client.
    async fn maybe_init_client(&self) -> Result<Client, BoxedError> {
        let mut guard = self.client.write().await;
        if guard.is_none() {
            let client = self.build_client().await.map_err(BoxedError::new)?;
            *guard = Some(client);
        }

        // Safety: checked above that the client is Some.
        Ok(guard.as_ref().unwrap().clone())
    }

    /// Resets the cached client, forcing a rebuild on the next use.
    async fn reset_client(&self) {
        warn!("Resetting the client");
        let mut guard = self.client.write().await;
        guard.take();
    }
}

#[async_trait::async_trait]
impl Inserter for InsertForwarder {
    async fn row_inserts(
        &self,
        context: &Context<'_>,
        requests: RowInsertRequests,
        options: Option<&InsertOptions>,
    ) -> Result<(), BoxedError> {
        let client = self.maybe_init_client().await?;
        let database = Database::new(context.catalog, context.schema, client);
        let hints = options.map_or(vec![], |o| o.to_hints());

        if let Err(e) = database
            .row_inserts_with_hints(
                requests,
                &hints
                    .iter()
                    .map(|(k, v)| (*k, v.as_str()))
                    .collect::<Vec<_>>(),
            )
            .await
        {
            // Resets the client so it will be rebuilt next time.
            self.reset_client().await;
            return Err(BoxedError::new(e));
        };

        Ok(())
    }
}
