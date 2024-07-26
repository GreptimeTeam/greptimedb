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

use common_query::logical_plan::SubstraitPlanDecoderRef;
use common_telemetry::tracing_context::TracingContext;
use datafusion::execution::context::{SessionState, TaskContext};
use session::context::QueryContextRef;

use crate::query_engine::default_serializer::DefaultPlanDecoder;

#[derive(Debug)]
pub struct QueryEngineContext {
    state: SessionState,
    query_ctx: QueryContextRef,
}

impl QueryEngineContext {
    pub fn new(state: SessionState, query_ctx: QueryContextRef) -> Self {
        Self { state, query_ctx }
    }

    #[inline]
    pub fn state(&self) -> &SessionState {
        &self.state
    }

    #[inline]
    pub fn query_ctx(&self) -> QueryContextRef {
        self.query_ctx.clone()
    }

    pub fn build_task_ctx(&self) -> Arc<TaskContext> {
        let dbname = self.query_ctx.get_db_string();
        let state = &self.state;
        let tracing_context = TracingContext::from_current_span();

        // pass tracing context in session_id
        let session_id = tracing_context.to_json();

        Arc::new(TaskContext::new(
            Some(dbname),
            session_id,
            state.config().clone(),
            state.scalar_functions().clone(),
            state.aggregate_functions().clone(),
            state.window_functions().clone(),
            state.runtime_env().clone(),
        ))
    }

    /// Creates a [`LogicalPlan`] decoder
    pub fn new_plan_decoder(&self) -> crate::error::Result<SubstraitPlanDecoderRef> {
        Ok(Arc::new(DefaultPlanDecoder::new(
            self.state.clone(),
            &self.query_ctx,
        )?))
    }

    /// Mock an engine context for unit tests.
    #[cfg(any(test, feature = "test"))]
    pub fn mock() -> Self {
        use common_base::Plugins;
        use session::context::QueryContext;

        use crate::query_engine::QueryEngineState;

        let state = Arc::new(QueryEngineState::new(
            catalog::memory::new_memory_catalog_manager().unwrap(),
            None,
            None,
            None,
            None,
            false,
            Plugins::default(),
        ));

        QueryEngineContext::new(state.session_state(), QueryContext::arc())
    }
}
