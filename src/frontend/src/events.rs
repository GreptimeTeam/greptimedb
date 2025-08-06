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
use common_event_recorder::{build_row_inserts_request, Event, EventHandler};
use operator::insert::InserterRef;
use operator::statement::StatementExecutorRef;
use session::context::QueryContextBuilder;
use snafu::ResultExt;

/// EventHandlerImpl is the default event handler implementation in frontend.
pub struct EventHandlerImpl {
    inserter: InserterRef,
    statement_executor: StatementExecutorRef,
}

impl EventHandlerImpl {
    /// Create a new EventHandlerImpl.
    pub fn new(inserter: InserterRef, statement_executor: StatementExecutorRef) -> Self {
        Self {
            inserter,
            statement_executor,
        }
    }
}

#[async_trait]
impl EventHandler for EventHandlerImpl {
    async fn handle(&self, events: &[Box<dyn Event>]) -> Result<()> {
        let query_ctx = QueryContextBuilder::default()
            .current_catalog(DEFAULT_CATALOG_NAME.to_string())
            .current_schema(DEFAULT_PRIVATE_SCHEMA_NAME.to_string())
            .build()
            .into();

        // FIXME(zyy17): How to add hints?
        self.inserter
            .handle_row_inserts(
                build_row_inserts_request(events)?,
                query_ctx,
                &self.statement_executor,
                false,
                false,
            )
            .await
            .map_err(BoxedError::new)
            .context(InsertEventsSnafu)?;

        Ok(())
    }
}
