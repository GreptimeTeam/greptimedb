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

    /// Admits one backfill attempt of the flow `catalog.flow` over the half-open
    /// range `[start_ms, end_ms)` and returns its attempt id.
    ///
    /// An attempt is process-local state of the execution this factory serves,
    /// so a factory that does not serve backfill refuses the request instead of
    /// pretending to have recorded one.
    async fn backfill(
        &self,
        _catalog: &str,
        _flow: &str,
        _start_ms: i64,
        _end_ms: i64,
    ) -> Result<String> {
        UnsupportedSnafu {
            reason: "this batching execution factory does not serve flow backfill",
        }
        .fail()
    }

    /// Returns the JSON status of the flow's process-local backfill attempt.
    async fn backfill_status(&self, _catalog: &str, _flow: &str) -> Result<String> {
        UnsupportedSnafu {
            reason: "this batching execution factory does not serve flow backfill status",
        }
        .fail()
    }
}
