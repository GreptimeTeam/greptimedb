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
use client::inserter::{Context, Inserter};
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_PRIVATE_SCHEMA_NAME};
use common_error::ext::BoxedError;
use common_event_recorder::error::{InsertEventsSnafu, Result};
use common_event_recorder::{Event, EventHandler, build_row_inserts_request, group_events_by_type};
use snafu::ResultExt;

pub mod region_migration_event;

/// EventHandlerImpl is the default event handler implementation in metasrv.
/// It sends the received events to the frontend instances.
pub struct EventHandlerImpl {
    inserter: Box<dyn Inserter>,
}

impl EventHandlerImpl {
    pub fn new(inserter: Box<dyn Inserter>) -> Self {
        Self { inserter }
    }
}

#[async_trait]
impl EventHandler for EventHandlerImpl {
    async fn handle(&self, events: &[Box<dyn Event>]) -> Result<()> {
        let event_groups = group_events_by_type(events);

        for (_, events) in event_groups {
            let requests = build_row_inserts_request(&events)?;
            self.inserter
                .insert_rows(
                    &Context {
                        catalog: DEFAULT_CATALOG_NAME,
                        schema: DEFAULT_PRIVATE_SCHEMA_NAME,
                    },
                    requests,
                )
                .await
                .map_err(BoxedError::new)
                .context(InsertEventsSnafu)?;
        }

        Ok(())
    }
}
