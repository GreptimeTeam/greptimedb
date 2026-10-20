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

//! Flow node configuration and the plan-to-schema helpers shared with batching mode.

use common_base::memory_limit::MemoryLimit;
use common_config::Configurable;
use common_error::ext::BoxedError;
use common_options::memory::MemoryOptions;
use common_recordbatch::map_dictionary_to_values_data_type;
use common_stat::get_total_cpu_cores;
use common_telemetry::logging::{LoggingOptions, TracingOptions};
use datafusion_expr::logical_plan::Distinct;
use datafusion_expr::{Expr, LogicalPlan};
use datatypes::schema::{ColumnSchema, SchemaRef};
use meta_client::MetaClientOptions;
use query::options::QueryOptions;
use serde::{Deserialize, Serialize};
use servers::grpc::GrpcOptions;
use servers::http::HttpOptions;
use snafu::ResultExt;

use crate::batching_mode::BatchingModeOptions;
use crate::error::{Error, ExternalSnafu};

pub(crate) mod flownode_impl;
#[cfg(test)]
mod tests;
pub(crate) mod util;

/// Converts a retained plan's output to logical schemas. Only a direct source
/// column (optionally wrapped in an alias) carries source semantics; computed
/// expressions deliberately use the physical output field metadata instead.
pub(crate) fn output_column_schemas(
    plan: &LogicalPlan,
    source_schema: &SchemaRef,
) -> Result<(Vec<ColumnSchema>, Vec<Option<usize>>), Error> {
    // Plain DISTINCT preserves the selected columns and their source semantics. Inspect its
    // input for lineage, while leaving computed expressions without source metadata.
    let expressions = match plan {
        LogicalPlan::Projection(projection) => Some(&projection.expr),
        LogicalPlan::Distinct(Distinct::All(input)) => match input.as_ref() {
            LogicalPlan::Projection(projection) => Some(&projection.expr),
            _ => None,
        },
        _ => None,
    };
    let is_pass_through = matches!(plan, LogicalPlan::TableScan(_) | LogicalPlan::Filter(_))
        || matches!(plan, LogicalPlan::Distinct(Distinct::All(input)) if matches!(input.as_ref(), LogicalPlan::TableScan(_) | LogicalPlan::Filter(_)));
    let mut lineage = Vec::new();
    let columns = plan
        .schema()
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, field)| {
            let mut column = ColumnSchema::try_from(field.as_ref())
                .map_err(BoxedError::new)
                .context(ExternalSnafu)?;
            let source_index = expressions
                .and_then(|exprs| {
                    let expr = exprs.get(idx)?;
                    let expr = match expr {
                        Expr::Column(column) => Some(column),
                        Expr::Alias(alias) => match alias.expr.as_ref() {
                            Expr::Column(column) => Some(column),
                            _ => None,
                        },
                        _ => None,
                    }?;
                    source_schema.column_index_by_name(&expr.name)
                })
                .or_else(|| {
                    is_pass_through
                        .then(|| source_schema.column_index_by_name(field.name()))
                        .flatten()
                });
            if let Some(source_index) = source_index {
                let mut source = source_schema.column_schemas()[source_index].clone();
                source.name = field.name().clone();
                source.data_type = map_dictionary_to_values_data_type(&source.data_type);
                column = source;
            } else {
                column.data_type = map_dictionary_to_values_data_type(&column.data_type);
                // DataFusion may propagate field metadata through a computed expression.  Such
                // an expression has no source-column lineage and must not inherit its PK/time
                // semantics into the sink relation.
                if expressions.is_some() {
                    column = column.with_time_index(false);
                }
            }
            lineage.push(source_index);
            Ok(column)
        })
        .collect::<Result<Vec<_>, Error>>()?;
    Ok((columns, lineage))
}

fn default_num_workers() -> usize {
    get_total_cpu_cores().div_ceil(2)
}

pub const AUTO_CREATED_PLACEHOLDER_TS_COL: &str = "__ts_placeholder";
pub const AUTO_CREATED_UPDATE_AT_TS_COL: &str = "update_at";

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct FlowConfig {
    /// Deprecated and ignored. Flow workers have been removed.
    #[deprecated(note = "flow workers have been removed; this field is ignored")]
    #[serde(default = "default_num_workers")]
    pub num_workers: usize,
    pub batching_mode: BatchingModeOptions,
}

#[allow(deprecated)]
impl Default for FlowConfig {
    fn default() -> Self {
        Self {
            num_workers: get_total_cpu_cores().div_ceil(2),
            batching_mode: BatchingModeOptions::default(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct FlownodeOptions {
    pub node_id: Option<u64>,
    pub flow: FlowConfig,
    pub grpc: GrpcOptions,
    pub http: HttpOptions,
    pub meta_client: Option<MetaClientOptions>,
    pub logging: LoggingOptions,
    pub tracing: TracingOptions,
    pub query: QueryOptions,
    pub memory: MemoryOptions,
}

impl Default for FlownodeOptions {
    fn default() -> Self {
        Self {
            node_id: None,
            flow: FlowConfig::default(),
            grpc: GrpcOptions::default().with_bind_addr("127.0.0.1:3004"),
            http: HttpOptions::default(),
            meta_client: None,
            logging: LoggingOptions::default(),
            tracing: TracingOptions::default(),
            query: QueryOptions {
                parallelism: 1,
                allow_query_fallback: false,
                memory_pool_size: MemoryLimit::default(),
                enable_per_region_metrics: false,
                ..Default::default()
            },
            memory: MemoryOptions::default(),
        }
    }
}

impl Configurable for FlownodeOptions {
    fn validate_sanitize(&mut self) -> common_config::error::Result<()> {
        Ok(())
    }
}
