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

//! Optional execution collaborator for batching tasks.

use std::sync::Arc;

use datafusion_expr::LogicalPlan;
use query::QueryEngineRef;
use table::TableRef;

use crate::Result;
use crate::batching_mode::frontend_client::FrontendClient;
use crate::batching_mode::task::{BatchingExecutionGuard, BatchingTask, ExecuteOnceOutcome};
use crate::error::UnsupportedSnafu;

#[async_trait::async_trait]
pub trait BatchingExecution: Send + Sync + 'static {
    /// Execute one round while retaining the guard through all task-state updates.
    /// An implementation that continues after caller cancellation must retain the
    /// guard with that work and make it stoppable through [`Self::stop`].
    async fn execute_once(
        self: Arc<Self>,
        guard: BatchingExecutionGuard,
        task: &BatchingTask,
        engine: &QueryEngineRef,
        frontend: &Arc<FrontendClient>,
        max_window_cnt: Option<usize>,
    ) -> ExecuteOnceOutcome;

    /// Rewrite the completed query plan after incremental merging and before execution.
    fn rewrite_plan(&self, _task: &BatchingTask, plan: LogicalPlan) -> Result<LogicalPlan> {
        Ok(plan)
    }

    /// Retire this execution instance, rejecting new work and requesting that any
    /// retained local work stop. This is not an acknowledgement of remote quiescence.
    fn stop(&self) {}
}

#[async_trait::async_trait]
pub trait BatchingExecutionFactory: Send + Sync + 'static {
    async fn create(
        &self,
        task: &BatchingTask,
        sink: TableRef,
        engine: &QueryEngineRef,
        frontend: &Arc<FrontendClient>,
    ) -> Result<Option<Arc<dyn BatchingExecution>>>;

    /// Runs the flow-admin `function` of the flow `catalog.flow` with the
    /// function's own `args` and returns its result as the string the caller
    /// answers with.
    ///
    /// The route is generic: the factory resolves `function` itself, so adding a
    /// flow admin function needs no new method here and no new request body on
    /// the wire. What a function returns is process-local state of the execution
    /// this factory serves, so a factory that does not serve one refuses the call
    /// instead of pretending to have an answer.
    async fn admin(
        &self,
        _flow_id: u32,
        _catalog: &str,
        _flow: &str,
        _function: &str,
        _args: &[String],
    ) -> Result<String> {
        UnsupportedSnafu {
            reason: "this batching execution factory does not serve flow admin functions",
        }
        .fail()
    }
}
