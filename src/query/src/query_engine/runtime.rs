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

use datafusion::execution::context::SessionConfig;
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};

use crate::options::QueryOptions;
use crate::query_engine::state::MetricsMemoryPool;

pub type QueryRuntimeProviderRef = Arc<dyn QueryRuntimeProvider>;

#[non_exhaustive]
pub struct QueryRuntimeContext<'a> {
    pub query_options: &'a QueryOptions,
    pub resolved_memory_pool_size: usize,
}

pub trait QueryRuntimeProvider: Send + Sync + 'static {
    fn configure_session_config(&self, _ctx: QueryRuntimeContext<'_>, _config: &mut SessionConfig) {
    }

    fn build_runtime_env(&self, ctx: QueryRuntimeContext<'_>) -> Arc<RuntimeEnv>;
}

#[derive(Debug, Default)]
pub struct DefaultQueryRuntimeProvider;

impl QueryRuntimeProvider for DefaultQueryRuntimeProvider {
    fn build_runtime_env(&self, ctx: QueryRuntimeContext<'_>) -> Arc<RuntimeEnv> {
        if ctx.resolved_memory_pool_size > 0 {
            Arc::new(
                RuntimeEnvBuilder::new()
                    .with_memory_pool(Arc::new(MetricsMemoryPool::new(
                        ctx.resolved_memory_pool_size,
                    )))
                    .build()
                    .expect("Failed to build RuntimeEnv"),
            )
        } else {
            Arc::new(RuntimeEnv::default())
        }
    }
}
