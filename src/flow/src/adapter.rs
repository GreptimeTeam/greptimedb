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

//! Flow source schema management and stateless streaming execution.

use std::collections::BTreeMap;
use std::sync::Arc;

use common_base::memory_limit::MemoryLimit;
use common_config::Configurable;
use common_error::ext::BoxedError;
use common_meta::key::TableMetadataManagerRef;
use common_options::memory::MemoryOptions;
use common_recordbatch::map_dictionary_to_values_data_type;
use common_stat::get_total_cpu_cores;
use common_telemetry::info;
use common_telemetry::logging::{LoggingOptions, TracingOptions};
use datafusion_expr::{Expr, LogicalPlan};
use datatypes::schema::{ColumnSchema, SchemaRef};
use itertools::Itertools;
use meta_client::MetaClientOptions;
use query::QueryEngine;
use query::options::QueryOptions;
use serde::{Deserialize, Serialize};
use servers::grpc::GrpcOptions;
use servers::http::HttpOptions;
use snafu::{IntoError, OptionExt, ResultExt, ensure};
use store_api::storage::{ConcreteDataType, RegionId};
use tokio::sync::RwLock;

use crate::adapter::stateless::StatelessFlow;
use crate::adapter::table_source::ManagedTableSource;
use crate::adapter::util::{
    relation_desc_to_column_schemas_with_fallback, table_info_value_to_relation_desc,
};
use crate::batching_mode::BatchingModeOptions;
use crate::batching_mode::frontend_client::FrontendClient;
use crate::batching_mode::utils::sql_to_df_plan;
use crate::error::{
    Error, ExternalSnafu, InsertIntoFlowSnafu, InternalSnafu, InvalidQuerySnafu, UnexpectedSnafu,
};
use crate::repr::{ColumnType, DiffRow, RelationDesc, Row};
use crate::{CreateFlowArgs, FlowId, TableName};

pub(crate) mod flownode_impl;
pub(crate) mod stateless;
pub(crate) mod table_source;
#[cfg(test)]
mod tests;
pub(crate) mod util;

/// Converts a retained plan's output to logical schemas. Only a direct source
/// column (optionally wrapped in an alias) carries source semantics; computed
/// expressions deliberately use the physical output field metadata instead.
fn output_column_schemas(
    plan: &LogicalPlan,
    source_schema: &SchemaRef,
) -> Result<(Vec<ColumnSchema>, Vec<Option<usize>>), Error> {
    let expressions = match plan {
        LogicalPlan::Projection(projection) => Some(&projection.expr),
        _ => None,
    };
    let is_pass_through = matches!(plan, LogicalPlan::TableScan(_) | LogicalPlan::Filter(_));
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
            }
            lineage.push(source_index);
            Ok(column)
        })
        .collect::<Result<Vec<_>, Error>>()?;
    Ok((columns, lineage))
}

fn relation_desc_from_output(
    columns: &[ColumnSchema],
    lineage: &[Option<usize>],
    source_primary_key_indices: &[usize],
) -> RelationDesc {
    let keys = source_primary_key_indices
        .iter()
        .filter_map(|source_index| {
            lineage
                .iter()
                .position(|index| index == &Some(*source_index))
        })
        .collect_vec();
    let time_index = columns.iter().position(ColumnSchema::is_time_index);
    RelationDesc {
        typ: crate::repr::RelationType {
            column_types: columns
                .iter()
                .map(|column| ColumnType::new(column.data_type.clone(), column.is_nullable()))
                .collect(),
            keys: if keys.is_empty() {
                vec![]
            } else {
                vec![crate::repr::Key::from(keys)]
            },
            time_index,
            auto_columns: vec![],
        },
        names: columns
            .iter()
            .map(|column| Some(column.name.clone()))
            .collect(),
    }
}

fn default_num_workers() -> usize {
    get_total_cpu_cores().div_ceil(2)
}

pub const AUTO_CREATED_PLACEHOLDER_TS_COL: &str = "__ts_placeholder";
pub const AUTO_CREATED_UPDATE_AT_TS_COL: &str = "update_at";

/// Resolves the columns appended by the flow, validating the complete output/sink layout.
///
/// A sink with the same arity as the query is an ordinary sink, even when its last
/// column happens to be named `update_at`. Auto columns are only inferred from the
/// arity difference and their exact trailing layout.
pub(crate) fn resolve_sink_layout(
    output_schema: &[ColumnSchema],
    sink_schema: &[ColumnSchema],
) -> Result<Vec<ColumnSchema>, Error> {
    ensure!(
        sink_schema.len() >= output_schema.len() && sink_schema.len() - output_schema.len() <= 2,
        InvalidQuerySnafu {
            reason: format!(
                "Flow output has {} columns, but sink has {} columns; only zero, one, or two trailing auto columns are supported",
                output_schema.len(),
                sink_schema.len()
            )
        }
    );
    let suffix_len = sink_schema.len() - output_schema.len();
    for (idx, (output, sink)) in output_schema.iter().zip(sink_schema.iter()).enumerate() {
        ensure!(
            output.data_type == sink.data_type,
            InvalidQuerySnafu {
                reason: format!(
                    "Flow output column {idx} has type {:?}, but sink column {} has type {:?}",
                    output.data_type, sink.name, sink.data_type
                )
            }
        );
    }
    let suffix = &sink_schema[output_schema.len()..];
    match suffix_len {
        0 => Ok(vec![]),
        1 => {
            let column = &suffix[0];
            ensure!(
                column.name == AUTO_CREATED_UPDATE_AT_TS_COL && column.data_type.is_timestamp(),
                InvalidQuerySnafu {
                    reason: format!(
                        "The trailing sink column must be timestamp {}",
                        AUTO_CREATED_UPDATE_AT_TS_COL
                    )
                }
            );
            Ok(suffix.to_vec())
        }
        2 => {
            let update_at = &suffix[0];
            let placeholder = &suffix[1];
            ensure!(
                update_at.name == AUTO_CREATED_UPDATE_AT_TS_COL
                    && update_at.data_type.is_timestamp()
                    && placeholder.name == AUTO_CREATED_PLACEHOLDER_TS_COL
                    && placeholder.data_type.is_timestamp()
                    && placeholder.is_time_index(),
                InvalidQuerySnafu {
                    reason: "The two trailing sink columns must be timestamp update_at followed by timestamp time-index __ts_placeholder".to_string()
                }
            );
            Ok(suffix.to_vec())
        }
        _ => unreachable!(),
    }
}

/// Legacy helper for callers that only have a sink schema. New stateless flows
/// resolve the suffix against both schemas and store it in `StatelessFlow`.
pub(crate) fn sink_output_column_count(sink_schema: &[ColumnSchema]) -> Result<usize, Error> {
    let mut count = sink_schema.len();
    if sink_schema
        .last()
        .is_some_and(|column| column.name == AUTO_CREATED_PLACEHOLDER_TS_COL)
    {
        let placeholder = &sink_schema[count - 1];
        ensure!(
            placeholder.data_type.is_timestamp() && placeholder.is_time_index(),
            InvalidQuerySnafu {
                reason: format!(
                    "Auto-created sink column {} must be a timestamp time index",
                    AUTO_CREATED_PLACEHOLDER_TS_COL
                )
            }
        );
        count -= 1;
    }
    if sink_schema
        .get(count.checked_sub(1).unwrap_or_default())
        .is_some_and(|column| column.name == AUTO_CREATED_UPDATE_AT_TS_COL)
    {
        ensure!(
            sink_schema[count - 1].data_type.is_timestamp(),
            InvalidQuerySnafu {
                reason: format!(
                    "Auto-created sink column {} must be a timestamp",
                    AUTO_CREATED_UPDATE_AT_TS_COL
                )
            }
        );
        count -= 1;
    }
    Ok(count)
}

pub(crate) fn validate_sink_layout(
    output_schema: &[ColumnSchema],
    sink_schema: &[ColumnSchema],
) -> Result<(), Error> {
    resolve_sink_layout(output_schema, sink_schema).map(|_| ())
}

pub(crate) fn validate_auto_column_names(output_schema: &[ColumnSchema]) -> Result<(), Error> {
    for column in output_schema {
        ensure!(
            column.name != AUTO_CREATED_UPDATE_AT_TS_COL
                && column.name != AUTO_CREATED_PLACEHOLDER_TS_COL,
            InvalidQuerySnafu {
                reason: format!(
                    "Flow output column {} is reserved for an auto-created sink column",
                    column.name
                )
            }
        );
    }
    Ok(())
}

pub(crate) fn validate_sink_layout_with_suffix(
    output_schema: &[ColumnSchema],
    sink_schema: &[ColumnSchema],
    suffix: &[ColumnSchema],
) -> Result<(), Error> {
    ensure!(
        sink_schema.len() == output_schema.len() + suffix.len()
            && sink_schema[output_schema.len()..] == *suffix,
        InvalidQuerySnafu {
            reason: "Stored sink auto-column layout no longer matches the sink schema".to_string()
        }
    );
    for (idx, (output, sink)) in output_schema
        .iter()
        .zip(&sink_schema[..output_schema.len()])
        .enumerate()
    {
        ensure!(
            output.data_type == sink.data_type,
            InvalidQuerySnafu {
                reason: format!(
                    "Flow output column {idx} has type {:?}, but sink column {} has type {:?}",
                    output.data_type, sink.name, sink.data_type
                )
            }
        );
    }
    Ok(())
}

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

pub type FlowStreamingEngineRef = Arc<StreamingEngine>;

pub struct StreamingEngine {
    pub query_engine: Arc<dyn QueryEngine>,
    pub frontend_client: Arc<FrontendClient>,
    table_info_source: ManagedTableSource,
    stateless_flows: RwLock<BTreeMap<FlowId, StatelessFlow>>,
    pub node_id: Option<u32>,
}

impl StreamingEngine {
    pub fn new(
        node_id: Option<u32>,
        query_engine: Arc<dyn QueryEngine>,
        table_meta: TableMetadataManagerRef,
        frontend_client: Arc<FrontendClient>,
    ) -> Self {
        let table_info_source = ManagedTableSource::new(
            table_meta.table_info_manager().clone(),
            table_meta.table_name_manager().clone(),
        );
        Self {
            query_engine,
            frontend_client,
            table_info_source,
            stateless_flows: Default::default(),
            node_id,
        }
    }

    pub async fn handle_write_request(
        &self,
        region_id: RegionId,
        rows: Vec<DiffRow>,
        batch_datatypes: &[ConcreteDataType],
        source_schema_version: u32,
    ) -> Result<(), Error> {
        let table_id = region_id.table_id();
        let flows = self
            .stateless_flows
            .read()
            .await
            .values()
            .filter(|flow| flow.source_table_id == table_id)
            .cloned()
            .collect::<Vec<_>>();
        for flow in flows {
            stateless::execute(
                &flow,
                &rows,
                batch_datatypes,
                &self.query_engine,
                &self.frontend_client,
                source_schema_version,
            )
            .await
            .map_err(|err| {
                InsertIntoFlowSnafu {
                    region_id: u64::from(region_id),
                    flow_ids: vec![],
                }
                .into_error(BoxedError::new(err))
            })?;
        }
        Ok(())
    }

    pub async fn remove_flow_inner(&self, flow_id: FlowId) -> Result<(), Error> {
        self.stateless_flows.write().await.remove(&flow_id);
        Ok(())
    }

    pub async fn create_flow_inner(&self, args: CreateFlowArgs) -> Result<Option<FlowId>, Error> {
        let CreateFlowArgs {
            flow_id,
            sink_table_name,
            source_table_ids,
            expire_after: _,
            sql,
            query_ctx,
            ..
        } = args;
        ensure!(
            source_table_ids.len() == 1,
            InvalidQuerySnafu {
                reason: "Stateless streaming flow does not support multiple source tables",
            }
        );

        let query_ctx = query_ctx.map(Arc::new).context(UnexpectedSnafu {
            reason: "Query context is missing",
        })?;
        let flow_plan =
            sql_to_df_plan(query_ctx.clone(), self.query_engine.clone(), &sql, true).await?;
        stateless::validate_plan(&flow_plan)?;
        let source_table_id = source_table_ids[0];
        let source_table_name = self
            .table_info_source
            .get_table_name(&source_table_id)
            .await?;
        let source_table_info = self
            .table_info_source
            .get_table_info_value(&source_table_id)
            .await?
            .context(UnexpectedSnafu {
                reason: "Source table metadata is missing",
            })?;
        let source_meta = source_table_info.table_info.meta;
        let source_schema = source_meta.schema;
        let source_primary_key_indices = source_meta.primary_key_indices;
        let (inferred_schema, lineage) = output_column_schemas(&flow_plan, &source_schema)?;
        let inferred_relation =
            relation_desc_from_output(&inferred_schema, &lineage, &source_primary_key_indices);
        let sink_exists = self
            .fetch_table_pk_schema(&sink_table_name)
            .await?
            .is_some();
        if !sink_exists {
            validate_auto_column_names(&inferred_schema)?;
        }
        if !sink_exists
            && !self
                .create_table_from_relation(
                    &format!("flow-id={flow_id}"),
                    &sink_table_name,
                    &inferred_relation,
                )
                .await?
        {
            return UnexpectedSnafu {
                reason: format!("Failed to create table {sink_table_name:?}"),
            }
            .fail();
        }

        // Fetch the metadata after validation or auto-creation.  The sink's layout is the
        // insert contract: flow output aliases must not leak into the request schema.
        let (sink_primary_keys, _, sink_schema) = self
            .fetch_table_pk_schema(&sink_table_name)
            .await?
            .context(UnexpectedSnafu {
                reason: format!("Sink table metadata is missing: {sink_table_name:?}"),
            })?;
        let auto_columns = resolve_sink_layout(&inferred_schema, &sink_schema)?;
        // Keep the exact layout chosen at creation time; execution must not infer it
        // from a later sink metadata read.

        self.stateless_flows.write().await.insert(
            flow_id,
            StatelessFlow {
                source_table_id,
                source_table_name,
                source_schema_version: source_schema.version(),
                source_schema,
                sink_table_name,
                sink_schema,
                sink_primary_keys,
                auto_columns,
                plan: flow_plan,
                query_ctx,
            },
        );
        info!("Successfully create flow with id={flow_id}");
        Ok(Some(flow_id))
    }

    pub async fn flush_flow_inner(&self, _flow_id: FlowId) -> Result<usize, Error> {
        Ok(0)
    }

    pub(crate) async fn stateless_flow_ids(&self) -> Vec<FlowId> {
        self.stateless_flows.read().await.keys().copied().collect()
    }

    pub async fn flow_exist_inner(&self, flow_id: FlowId) -> Result<bool, Error> {
        Ok(self.stateless_flows.read().await.contains_key(&flow_id))
    }

    async fn fetch_table_pk_schema(
        &self,
        table_name: &TableName,
    ) -> Result<Option<(Vec<String>, Option<usize>, Vec<ColumnSchema>)>, Error> {
        if let Some(table_id) = self
            .table_info_source
            .get_opt_table_id_from_name(table_name)
            .await?
        {
            let table_info = self
                .table_info_source
                .get_table_info_value(&table_id)
                .await?
                .unwrap();
            let meta = table_info.table_info.meta;
            let schema = meta.schema.column_schemas().to_vec();
            let primary_keys = meta
                .primary_key_indices
                .into_iter()
                .map(|i| schema[i].name.clone())
                .collect_vec();
            Ok(Some((primary_keys, meta.schema.timestamp_index(), schema)))
        } else {
            Ok(None)
        }
    }

    async fn adjust_auto_created_table_schema(
        &self,
        schema: &RelationDesc,
    ) -> Result<(Vec<String>, Vec<ColumnSchema>, bool), Error> {
        let primary_keys = schema
            .typ()
            .keys
            .first()
            .map(|key| {
                key.column_indices
                    .iter()
                    .map(|i| {
                        schema
                            .get_name(*i)
                            .clone()
                            .unwrap_or_else(|| format!("col_{i}"))
                    })
                    .collect_vec()
            })
            .unwrap_or_default();
        let mut columns = relation_desc_to_column_schemas_with_fallback(schema);
        columns.push(ColumnSchema::new(
            AUTO_CREATED_UPDATE_AT_TS_COL,
            ConcreteDataType::timestamp_millisecond_datatype(),
            true,
        ));
        let no_time_index = schema.typ().time_index.is_none();
        if no_time_index {
            columns.push(
                ColumnSchema::new(
                    AUTO_CREATED_PLACEHOLDER_TS_COL,
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    true,
                )
                .with_time_index(true),
            );
        }
        Ok((primary_keys, columns, no_time_index))
    }
}

impl StreamingEngine {
    async fn handle_inserts_inner(
        &self,
        request: api::v1::region::InsertRequests,
    ) -> Result<(), Error> {
        for write_request in request.requests {
            let region_id = write_request.region_id;
            let table_id = RegionId::from(region_id).table_id();
            let (insert_schema, rows_proto) = write_request
                .rows
                .map(|r| (r.schema, r.rows))
                .unwrap_or_default();
            let now = common_time::util::current_time_millis();
            let (table_types, fetch_order, source_schema_version) = {
                // Fetch the source metadata once for both current-schema normalization and
                // retained-plan validation. In particular, do not execute a retained plan
                // against rows normalized with a newer schema.
                let table_info = self
                    .table_info_source
                    .get_table_info_value(&table_id)
                    .await?
                    .context(UnexpectedSnafu {
                        reason: format!("Table metadata is missing for table id {table_id}"),
                    })?;
                let source_schema_version = table_info.table_info.meta.schema.version();
                let table_schema = table_info_value_to_relation_desc(table_info)?;
                let defaults = table_schema
                    .default_values
                    .iter()
                    .zip(table_schema.relation_desc.typ().column_types.iter())
                    .map(|(value, ty)| {
                        value.as_ref().and_then(|value| {
                            value.create_default(ty.scalar_type(), ty.nullable()).ok()
                        })
                    })
                    .collect_vec();
                let types = table_schema
                    .relation_desc
                    .typ()
                    .column_types
                    .iter()
                    .map(|ty| ty.scalar_type.clone())
                    .collect_vec();
                let names = table_schema
                    .relation_desc
                    .names
                    .iter()
                    .enumerate()
                    .map(|(idx, name)| {
                        name.clone().context(InternalSnafu {
                            reason: format!("Column {idx} of table {table_id} has no name"),
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let input_columns = insert_schema
                    .iter()
                    .enumerate()
                    .map(|(idx, column)| (&column.column_name, idx))
                    .collect::<std::collections::HashMap<_, _>>();
                let order = names
                    .iter()
                    .zip(defaults)
                    .map(|(name, default)| {
                        input_columns
                            .get(name)
                            .copied()
                            .map(FetchFromRow::Idx)
                            .or_else(|| default.map(FetchFromRow::Default))
                            .with_context(|| UnexpectedSnafu {
                                reason: format!("Column not found: {name}"),
                            })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                (types, order, source_schema_version)
            };
            let rows = rows_proto
                .into_iter()
                .map(|row| {
                    let row = Row::from(row);
                    let row = fetch_order
                        .iter()
                        .map(|item| item.fetch(&row))
                        .collect_vec();
                    (Row::new(row), now, 1)
                })
                .collect_vec();
            self.handle_write_request(region_id.into(), rows, &table_types, source_schema_version)
                .await
                .map_err(|err| {
                    InsertIntoFlowSnafu {
                        region_id: u64::from(region_id),
                        flow_ids: vec![],
                    }
                    .into_error(BoxedError::new(err))
                })?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
enum FetchFromRow {
    Idx(usize),
    Default(datatypes::value::Value),
}

impl FetchFromRow {
    fn fetch(&self, row: &Row) -> datatypes::value::Value {
        match self {
            Self::Idx(idx) => row
                .get(*idx)
                .cloned()
                .unwrap_or(datatypes::value::Value::Null),
            Self::Default(value) => value.clone(),
        }
    }
}
