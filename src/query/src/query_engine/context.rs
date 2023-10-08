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

use datafusion::execution::context::{SessionState, TaskContext};
use session::context::QueryContextRef;

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
        Arc::new(TaskContext::new(
            Some(dbname),
            state.session_id().to_string(),
            state.config().clone(),
            state.scalar_functions().clone(),
            state.aggregate_functions().clone(),
            state.window_functions().clone(),
            state.runtime_env().clone(),
        ))
    }
}
