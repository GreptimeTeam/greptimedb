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
use crate::batching_mode::task::{BatchingTask, ExecuteOnceOutcome};

#[async_trait::async_trait]
pub trait BatchingExecution: Send + Sync + 'static {
    async fn execute_once(
        &self,
        task: &BatchingTask,
        engine: &QueryEngineRef,
        frontend: &Arc<FrontendClient>,
        max_window_cnt: Option<usize>,
    ) -> ExecuteOnceOutcome;
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
