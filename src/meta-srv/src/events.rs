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

use std::time::Duration;

use async_trait::async_trait;
use client::inserter::{Context, InsertOptions, InserterRef};
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_PRIVATE_SCHEMA_NAME};
use common_error::ext::BoxedError;
use common_event_recorder::error::{InsertEventsSnafu, Result};
use common_event_recorder::{
    build_row_inserts_request, group_events_by_type, Event, EventHandler, EventHandlerOptions,
};
use snafu::ResultExt;

pub mod region_migration_event;

/// EventHandlerImpl is the default event handler implementation in metasrv.
/// It sends the received events to the frontend instances.
pub struct EventHandlerImpl {
    inserter: InserterRef,
    ttl: Duration,
}

impl EventHandlerImpl {
    pub fn new(inserter: InserterRef, ttl: Duration) -> Self {
        Self { inserter, ttl }
    }
}

#[async_trait]
impl EventHandler for EventHandlerImpl {
    async fn handle(&self, events: &[Box<dyn Event>]) -> Result<()> {
        let event_groups = group_events_by_type(events);

        for (event_type, events) in event_groups {
            let requests = build_row_inserts_request(&events)?;
            let EventHandlerOptions { ttl, append_mode } = self.options(event_type);
            self.inserter
                .insert_rows(
                    &Context {
                        catalog: DEFAULT_CATALOG_NAME,
                        schema: DEFAULT_PRIVATE_SCHEMA_NAME,
                    },
                    requests,
                    Some(&InsertOptions { ttl, append_mode }),
                )
                .await
                .map_err(BoxedError::new)
                .context(InsertEventsSnafu)?;
        }

        Ok(())
    }

    fn options(&self, _event_type: &str) -> EventHandlerOptions {
        EventHandlerOptions {
            ttl: self.ttl,
            append_mode: true,
        }
    }
}
