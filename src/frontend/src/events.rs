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
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_PRIVATE_SCHEMA_NAME};
use common_error::ext::BoxedError;
use common_event_recorder::error::{InsertEventsSnafu, Result};
use common_event_recorder::{
    aggregate_events_by_type, build_row_inserts_request, Event, EventHandler, EventHandlerOptions,
};
use common_frontend::slow_query_event::SLOW_QUERY_EVENT_TYPE;
use common_telemetry::logging::SlowQueryOptions;
use operator::insert::InserterRef;
use operator::statement::StatementExecutorRef;
use session::context::QueryContextBuilder;
use snafu::ResultExt;
use store_api::mito_engine_options::{APPEND_MODE_KEY, TTL_KEY};

/// EventHandlerImpl is the default event handler implementation in frontend.
pub struct EventHandlerImpl {
    inserter: InserterRef,
    statement_executor: StatementExecutorRef,
    slow_query_options: SlowQueryOptions,
}

impl EventHandlerImpl {
    /// Create a new EventHandlerImpl.
    pub fn new(
        inserter: InserterRef,
        statement_executor: StatementExecutorRef,
        slow_query_options: SlowQueryOptions,
    ) -> Self {
        Self {
            inserter,
            statement_executor,
            slow_query_options,
        }
    }
}

#[async_trait]
impl EventHandler for EventHandlerImpl {
    async fn handle(&self, events: &[Box<dyn Event>]) -> Result<()> {
        let event_groups = aggregate_events_by_type(events);

        for (_, events) in event_groups {
            let opts = self.options(events[0].event_type());

            let query_ctx = QueryContextBuilder::default()
                .current_catalog(DEFAULT_CATALOG_NAME.to_string())
                .current_schema(DEFAULT_PRIVATE_SCHEMA_NAME.to_string())
                .set_extension(TTL_KEY.to_string(), opts.ttl)
                .set_extension(APPEND_MODE_KEY.to_string(), opts.append_mode.to_string())
                .build()
                .into();

            self.inserter
                .handle_row_inserts(
                    build_row_inserts_request(&events)?,
                    query_ctx,
                    &self.statement_executor,
                    false,
                    false,
                )
                .await
                .map_err(BoxedError::new)
                .context(InsertEventsSnafu)?;
        }

        Ok(())
    }

    fn options(&self, event_type: &str) -> EventHandlerOptions {
        match event_type {
            SLOW_QUERY_EVENT_TYPE => EventHandlerOptions {
                ttl: self.slow_query_options.ttl.clone().unwrap_or_default(),
                append_mode: true,
            },
            _ => EventHandlerOptions::default(),
        }
    }
}
