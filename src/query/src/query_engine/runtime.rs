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

use datafusion::error::Result as DfResult;
use datafusion::execution::context::SessionConfig;
use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion_common::config::SpillCompression;

use crate::options::{QueryOptions, QuerySpillCompression, QuerySpillMode};
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
    ) -> DfResult<Arc<RuntimeEnv>> {
        builder.build().map(Arc::new)
    }
}

/// Default query runtime provider.
#[derive(Debug, Default)]
pub struct DefaultQueryRuntimeProvider;

impl DefaultQueryRuntimeProvider {
    /// Creates a default DataFusion runtime environment builder.
    pub fn runtime_env_builder(ctx: QueryRuntimeContext<'_>) -> RuntimeEnvBuilder {
        let mut builder = RuntimeEnvBuilder::new();

        // Attach the bounded metrics memory pool only when a limit is set
        // (>0). When unbounded (0), keep the DataFusion default
        // (UnboundedMemoryPool).
        if ctx.resolved_memory_pool_size > 0 {
            builder = builder.with_memory_pool(Arc::new(MetricsMemoryPool::new(
                ctx.resolved_memory_pool_size,
                ctx.query_options.experimental_memory_pool_policy,
            )));
        }

        match ctx.query_options.experimental_spill_mode {
            QuerySpillMode::Default => {
                // No custom disk manager; preserve DataFusion default OS temp directory.
            }
            QuerySpillMode::Custom => {
                let mut dm_builder = DiskManagerBuilder::default();
                if let Some(ref path) = ctx.query_options.experimental_spill_path {
                    dm_builder =
                        dm_builder.with_mode(DiskManagerMode::Directories(vec![path.clone()]));
                }
                dm_builder = dm_builder.with_max_temp_directory_size(
                    ctx.query_options
                        .experimental_spill_max_temp_directory_size
                        .as_bytes(),
                );
                builder = builder.with_disk_manager_builder(dm_builder);
            }
            QuerySpillMode::Disabled => {
                let dm_builder = DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled);
                builder = builder.with_disk_manager_builder(dm_builder);
            }
        }

        builder
    }
}

impl QueryRuntimeProvider for DefaultQueryRuntimeProvider {
    fn configure_session_config(&self, ctx: QueryRuntimeContext<'_>, config: &mut SessionConfig) {
        // Set spill compression on the session config only when spill mode is
        // Custom. In Default/Disabled modes, DataFusion's own default
        // (Uncompressed) is preserved—setting compression when spill is not
        // explicitly configured would be misleading.
        if ctx.query_options.experimental_spill_mode == QuerySpillMode::Custom {
            config.options_mut().execution.spill_compression =
                spill_compression_from_options(ctx.query_options.experimental_spill_compression);
        }
    }
}

/// Map [`QuerySpillCompression`] to DataFusion's [`SpillCompression`].
///
/// This conversion is intentionally not a `From` impl because the
/// semantics depend on the spill mode; callers should only invoke
/// this when `experimental_spill_mode == Custom`.
fn spill_compression_from_options(comp: QuerySpillCompression) -> SpillCompression {
    match comp {
        QuerySpillCompression::Uncompressed => SpillCompression::Uncompressed,
        QuerySpillCompression::Lz4Frame => SpillCompression::Lz4Frame,
        QuerySpillCompression::Zstd => SpillCompression::Zstd,
    }
}
