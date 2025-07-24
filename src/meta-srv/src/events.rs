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

use async_trait::async_trait;
use client::{Client, Database};
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_PRIVATE_SCHEMA_NAME};
use common_error::ext::BoxedError;
use common_event_recorder::error::{
    InsertEventsSnafu, KvBackendSnafu, NoAvailableFrontendSnafu, Result,
};
use common_event_recorder::{build_row_inserts_request, insert_hints, Event, EventHandler};
use common_grpc::channel_manager::ChannelManager;
use common_meta::peer::PeerLookupServiceRef;
use common_procedure::event::ProcedureEvent;
use common_telemetry::debug;
use snafu::{ensure, ResultExt};

use crate::cluster::MetaPeerClientRef;
use crate::events::region_migration_event::RegionMigrationEvent;
use crate::lease::MetaPeerLookupService;

pub mod region_migration_event;

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
        let region_migration_events: Box<Vec<Box<dyn Event>>> = Box::new(
            events
                .iter()
                .filter_map(|event| {
                    event
                        .as_any()
                        .downcast_ref::<ProcedureEvent>()
                        .and_then(|procedure_event| {
                            RegionMigrationEvent::new_from_procedure_event(procedure_event)
                                .ok()
                                .flatten()
                                .map(|event| Box::new(event) as Box<dyn Event>)
                        })
                })
                .collect(),
        );

        self.build_database_client()
            .await?
            .row_inserts_with_hints(
                build_row_inserts_request(&region_migration_events)?,
                &insert_hints(),
            )
            .await
            .map_err(BoxedError::new)
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
            .map_err(BoxedError::new)
            .context(KvBackendSnafu)?;

        ensure!(!frontends.is_empty(), NoAvailableFrontendSnafu);

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
