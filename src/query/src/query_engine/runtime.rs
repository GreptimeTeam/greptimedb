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

/// Reference-counted query runtime provider.
pub type QueryRuntimeProviderRef = Arc<dyn QueryRuntimeProvider>;

/// Context for building query runtime components.
#[derive(Clone, Copy)]
#[non_exhaustive]
pub struct QueryRuntimeContext<'a> {
    /// Query options used by the query engine.
    pub query_options: &'a QueryOptions,
    /// Resolved memory pool size in bytes.
    pub resolved_memory_pool_size: usize,
}

impl<'a> QueryRuntimeContext<'a> {
    /// Creates a new query runtime context.
    pub fn new(query_options: &'a QueryOptions, resolved_memory_pool_size: usize) -> Self {
        Self {
            query_options,
            resolved_memory_pool_size,
        }
    }
}

/// Provides DataFusion session and runtime setup for the query engine.
pub trait QueryRuntimeProvider: Send + Sync + 'static {
    /// Configures the DataFusion session config before building the session state.
    fn configure_session_config(&self, _ctx: QueryRuntimeContext<'_>, _config: &mut SessionConfig) {
    }

    /// Builds the DataFusion runtime environment.
    fn build_runtime_env(
        &self,
        _ctx: QueryRuntimeContext<'_>,
        builder: RuntimeEnvBuilder,
    ) -> Arc<RuntimeEnv> {
        Arc::new(builder.build().expect("Failed to build RuntimeEnv"))
    }
}

/// Default query runtime provider.
#[derive(Debug, Default)]
pub struct DefaultQueryRuntimeProvider;

impl DefaultQueryRuntimeProvider {
    /// Creates a default DataFusion runtime environment builder.
    pub fn runtime_env_builder(ctx: QueryRuntimeContext<'_>) -> RuntimeEnvBuilder {
        if ctx.resolved_memory_pool_size > 0 {
            RuntimeEnvBuilder::new().with_memory_pool(Arc::new(MetricsMemoryPool::new(
                ctx.resolved_memory_pool_size,
            )))
        } else {
            RuntimeEnvBuilder::new()
        }
    }
}

impl QueryRuntimeProvider for DefaultQueryRuntimeProvider {}
