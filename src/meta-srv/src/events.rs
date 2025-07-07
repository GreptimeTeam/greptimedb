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

pub mod region_migration_event;

use std::sync::Arc;

use api::v1::RowInsertRequests;
use async_trait::async_trait;
use client::{Client, Database};
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_PRIVATE_SCHEMA_NAME};
use common_event_recorder::error::{
    InsertEventsSnafu, KvBackendSnafu, NoAvailableFrontendSnafu, Result,
};
use common_event_recorder::{build_row_insert_request, insert_hints, Event, EventHandler};
use common_grpc::channel_manager::ChannelManager;
use common_meta::peer::PeerLookupServiceRef;
use common_telemetry::debug;
use snafu::ResultExt;

use crate::cluster::MetaPeerClientRef;
pub use crate::events::region_migration_event::*;
use crate::lease::MetaPeerLookupService;

/// EventHandlerImpl is the default event handler implementation in metasrv. It sends the received events to the frontend instances.
pub struct EventHandlerImpl {
    peer_lookup_service: PeerLookupServiceRef,
    channel_manager: ChannelManager,
}

impl EventHandlerImpl {
    pub fn new(meta_peer_client: MetaPeerClientRef) -> Self {
        Self {
            peer_lookup_service: Arc::new(MetaPeerLookupService::new(meta_peer_client)),
            channel_manager: ChannelManager::new(),
        }
    }
}

#[async_trait]
impl EventHandler for EventHandlerImpl {
    async fn handle(&self, events: &[Box<dyn Event>]) -> Result<()> {
        debug!("Received {} events: {:?}", events.len(), events);
        let database_client = self.build_database_client().await?;
        let row_inserts = RowInsertRequests {
            inserts: events
                .iter()
                .map(|e| build_row_insert_request(e.as_ref()))
                .collect::<Result<Vec<_>>>()?,
        };

        debug!("Inserting events: {:?}", row_inserts);

        database_client
            .row_inserts_with_hints(row_inserts, &insert_hints())
            .await
            .context(InsertEventsSnafu)?;

        Ok(())
    }
}

impl EventHandlerImpl {
    async fn build_database_client(&self) -> Result<Database> {
        let frontends = self
            .peer_lookup_service
            .active_frontends()
            .await
            .context(KvBackendSnafu)?;

        if frontends.is_empty() {
            return NoAvailableFrontendSnafu.fail();
        }

        let urls = frontends
            .into_iter()
            .map(|peer| peer.addr)
            .collect::<Vec<_>>();

        debug!("Available frontend addresses: {:?}", urls);

        Ok(Database::new(
            DEFAULT_CATALOG_NAME,
            DEFAULT_PRIVATE_SCHEMA_NAME,
            Client::with_manager_and_urls(self.channel_manager.clone(), urls),
        ))
    }
}
