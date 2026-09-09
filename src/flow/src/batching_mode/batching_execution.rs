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

use query::QueryEngineRef;
use table::TableRef;

use crate::Result;
use crate::batching_mode::frontend_client::FrontendClient;
use crate::batching_mode::task::{BatchingExecutionGuard, BatchingTask, ExecuteOnceOutcome};

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
}
