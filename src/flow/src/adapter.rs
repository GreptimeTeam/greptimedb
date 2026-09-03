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
use std::sync::atomic::{AtomicBool, Ordering};

use common_base::memory_limit::MemoryLimit;
use common_config::Configurable;
use common_error::ext::BoxedError;
use common_meta::key::TableMetadataManagerRef;
use common_options::memory::MemoryOptions;
use common_recordbatch::map_dictionary_to_values_data_type;
use common_stat::get_total_cpu_cores;
use common_telemetry::logging::{LoggingOptions, TracingOptions};
use common_telemetry::{error, info};
use datafusion_common::TableReference;
use datafusion_common::tree_node::TreeNode;
use datafusion_expr::logical_plan::Distinct;
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
    DatafusionSnafu, Error, ExternalSnafu, FlowNotFoundSnafu, InsertIntoFlowSnafu, InternalSnafu,
    InvalidQuerySnafu, UnexpectedSnafu,
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

/// Returns whether an existing sink uses the legacy explicit source-time-index layout.
///
/// This check is deliberately separate from [`resolve_sink_layout`]: ordinary auto-column
/// resolution remains the first choice, and this compatibility path is only for a sink whose
/// trailing column is a real, user-defined time index.
pub(crate) fn is_explicit_source_timestamp_compatibility(
    output_schema: &[ColumnSchema],
    output_lineage: &[Option<usize>],
    sink_schema: &[ColumnSchema],
    source_schema: &SchemaRef,
) -> bool {
    if sink_schema.len() != output_schema.len() + 1 || output_lineage.len() != output_schema.len() {
        return false;
    }
    if !output_schema
        .iter()
        .zip(&sink_schema[..output_schema.len()])
        .all(|(output, sink)| output.data_type == sink.data_type)
    {
        return false;
    }

    let Some(source_timestamp_index) = source_schema.timestamp_index() else {
        return false;
    };
    let sink_timestamp = &sink_schema[output_schema.len()];
    sink_timestamp.data_type == source_schema.column_schemas()[source_timestamp_index].data_type
        && sink_timestamp.data_type.is_timestamp()
        && sink_timestamp.is_time_index()
        && sink_timestamp.default_constraint().is_some()
        && sink_timestamp.name != AUTO_CREATED_UPDATE_AT_TS_COL
        && sink_timestamp.name != AUTO_CREATED_PLACEHOLDER_TS_COL
        && !output_lineage.contains(&Some(source_timestamp_index))
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
        .get(count.saturating_sub(1))
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

struct StatelessFlowSlot {
    runtime: Arc<RwLock<Option<Arc<StatelessFlow>>>>,
    /// Cleared at registry-detach time, fencing creators that already captured this slot.
    active: std::sync::atomic::AtomicBool,
}

fn validate_captured_slot(
    slot: &StatelessFlowSlot,
    current_source_table_id: Option<table::metadata::TableId>,
    runtime_matches: bool,
    expected_table_id: table::metadata::TableId,
    flow_id: FlowId,
) -> Result<(), Error> {
    ensure!(
        slot.active.load(Ordering::Acquire),
        FlowNotFoundSnafu { id: flow_id }
    );
    let current_source_table_id =
        current_source_table_id.context(FlowNotFoundSnafu { id: flow_id })?;
    ensure!(
        current_source_table_id == expected_table_id,
        InvalidQuerySnafu {
            reason: format!("Flow {flow_id} source table changed while it was selected")
        }
    );
    ensure!(
        runtime_matches,
        InvalidQuerySnafu {
            reason: format!("Flow {flow_id} changed while it was prepared")
        }
    );
    Ok(())
}

pub struct StreamingEngine {
    pub query_engine: Arc<dyn QueryEngine>,
    pub frontend_client: Arc<FrontendClient>,
    table_info_source: ManagedTableSource,
    stateless_flows: RwLock<BTreeMap<FlowId, Arc<StatelessFlowSlot>>>,
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
        let flow_ids = self.flow_ids_for_table(table_id).await;
        let mut failed_flow_ids = Vec::new();
        let mut first_error = None;
        for (flow_id, slot) in flow_ids {
            let result = self
                .execute_flow(
                    flow_id,
                    slot,
                    table_id,
                    rows.clone(),
                    batch_datatypes,
                    source_schema_version,
                )
                .await;
            if let Err(err) = result {
                error!(err; "Failed to insert into flow={}, region_id={}", flow_id, region_id);
                failed_flow_ids.push(flow_id);
                if first_error.is_none() {
                    first_error = Some(BoxedError::new(err));
                }
            }
        }
        match first_error {
            Some(source) => Err(InsertIntoFlowSnafu {
                region_id: u64::from(region_id),
                flow_ids: failed_flow_ids,
            }
            .into_error(source)),
            None => Ok(()),
        }
    }

    async fn flow_ids_for_table(
        &self,
        table_id: table::metadata::TableId,
    ) -> Vec<(FlowId, Arc<StatelessFlowSlot>)> {
        let slots = self
            .stateless_flows
            .read()
            .await
            .iter()
            .map(|(id, slot)| (*id, Arc::clone(slot)))
            .collect::<Vec<_>>();
        let mut flows = Vec::new();
        for (id, slot) in slots {
            if slot
                .runtime
                .read()
                .await
                .as_ref()
                .is_some_and(|flow| flow.source_table_id == table_id)
            {
                flows.push((id, slot));
            }
        }
        flows
    }

    async fn execute_flow(
        &self,
        flow_id: FlowId,
        slot: Arc<StatelessFlowSlot>,
        expected_table_id: table::metadata::TableId,
        rows: Vec<DiffRow>,
        batch_datatypes: &[ConcreteDataType],
        source_schema_version: u32,
    ) -> Result<usize, Error> {
        // The slot is the request's source-selection lease.  Do not resolve flow_id through the
        // registry again: removal/recreation may have installed a different slot in the meantime.
        let guard = slot.runtime.clone().read_owned().await;
        let selected = guard.as_ref().context(FlowNotFoundSnafu { id: flow_id })?;
        validate_captured_slot(
            &slot,
            Some(selected.source_table_id),
            true,
            expected_table_id,
            flow_id,
        )?;
        let selected = Arc::clone(selected);
        drop(guard);

        let flow = self
            .flow_for_write(
                flow_id,
                slot.clone(),
                expected_table_id,
                selected,
                source_schema_version,
            )
            .await?;
        let guard = slot.runtime.clone().read_owned().await;
        let current = guard.as_ref().context(FlowNotFoundSnafu { id: flow_id })?;
        validate_captured_slot(
            &slot,
            Some(current.source_table_id),
            Arc::ptr_eq(current, &flow),
            expected_table_id,
            flow_id,
        )?;
        // This is the last check before planning.  In particular, a request normalized against
        // an old source schema is never allowed to reach a newly published plan.
        let latest = self
            .table_info_source
            .get_table_info_value(&flow.source_table_id)
            .await?
            .context(UnexpectedSnafu {
                reason: "Source table metadata is missing",
            })?
            .table_info
            .meta
            .schema
            .version();
        ensure!(
            source_schema_version == latest && flow.source_schema_version == latest,
            InvalidQuerySnafu {
                reason: format!("Source schema version changed before flow {flow_id} execution")
            }
        );
        stateless::execute(
            &flow,
            &rows,
            batch_datatypes,
            &self.query_engine,
            &self.frontend_client,
            latest,
        )
        .await
    }

    async fn flow_for_write(
        &self,
        flow_id: FlowId,
        slot: Arc<StatelessFlowSlot>,
        expected_table_id: table::metadata::TableId,
        selected: Arc<StatelessFlow>,
        source_schema_version: u32,
    ) -> Result<Arc<StatelessFlow>, Error> {
        validate_captured_slot(
            &slot,
            Some(selected.source_table_id),
            true,
            expected_table_id,
            flow_id,
        )?;
        let current = selected;
        if current.source_schema_version == source_schema_version {
            return Ok(current);
        }
        let replacement = Arc::new(
            self.build_stateless_flow(&current.create_args, false)
                .await?,
        );
        ensure!(
            replacement.source_table_id == current.source_table_id
                && replacement.source_schema_version == source_schema_version,
            InvalidQuerySnafu {
                reason: format!(
                    "Source schema changed while rebuilding flow {flow_id}: expected version {source_schema_version}, got {}",
                    replacement.source_schema_version
                )
            }
        );
        let mut published = slot.runtime.write().await;
        let latest = published
            .as_ref()
            .context(FlowNotFoundSnafu { id: flow_id })?;
        validate_captured_slot(
            &slot,
            Some(latest.source_table_id),
            Arc::ptr_eq(latest, &current),
            expected_table_id,
            flow_id,
        )?;
        if latest.source_schema_version == source_schema_version {
            return Ok(Arc::clone(latest));
        }
        ensure!(
            Arc::ptr_eq(latest, &current),
            InvalidQuerySnafu {
                reason: format!("Flow {flow_id} was replaced while its source schema was rebuilt")
            }
        );
        // Metadata is fetched after acquiring the publication lease.  Thus a schema bump cannot
        // slip between the final validation and publishing this runtime.
        let latest_schema_version = self
            .table_info_source
            .get_table_info_value(&current.source_table_id)
            .await?
            .context(UnexpectedSnafu {
                reason: "Source table metadata is missing",
            })?
            .table_info
            .meta
            .schema
            .version();
        ensure!(
            latest_schema_version == source_schema_version,
            InvalidQuerySnafu {
                reason: format!(
                    "Source schema changed while rebuilding flow {flow_id}: expected version {source_schema_version}, got {latest_schema_version}"
                )
            }
        );
        *published = Some(Arc::clone(&replacement));
        Ok(replacement)
    }

    pub async fn remove_flow_inner(&self, flow_id: FlowId) -> Result<(), Error> {
        // Detach first: this is the tombstone/linearization point. A concurrent create must
        // install a different slot and can never republish into the removed one.
        let slot = self
            .stateless_flows
            .write()
            .await
            .remove(&flow_id)
            .context(FlowNotFoundSnafu { id: flow_id })?;
        slot.active.store(false, Ordering::Release);
        slot.runtime.write().await.take();
        Ok(())
    }

    async fn publish_initial_flow(
        &self,
        slot: Arc<StatelessFlowSlot>,
        flow: Arc<StatelessFlow>,
        create_if_not_exists: bool,
        or_replace: bool,
    ) -> Result<bool, Error> {
        let mut runtime = slot.runtime.write().await;
        ensure!(
            slot.active.load(Ordering::Acquire),
            FlowNotFoundSnafu {
                id: flow.create_args.flow_id
            }
        );
        if runtime.is_some() && !or_replace {
            if create_if_not_exists {
                return Ok(false);
            }
            return crate::error::FlowAlreadyExistSnafu {
                id: flow.create_args.flow_id,
            }
            .fail();
        }
        let latest = self
            .table_info_source
            .get_table_info_value(&flow.source_table_id)
            .await?
            .context(UnexpectedSnafu {
                reason: "Source table metadata is missing",
            })?
            .table_info
            .meta
            .schema
            .version();
        ensure!(
            latest == flow.source_schema_version,
            InvalidQuerySnafu {
                reason: format!(
                    "Source schema changed while building flow: built version {}, current version {latest}",
                    flow.source_schema_version
                )
            }
        );
        *runtime = Some(flow);
        Ok(true)
    }

    pub async fn create_flow_inner(&self, args: CreateFlowArgs) -> Result<Option<FlowId>, Error> {
        let flow_id = args.flow_id;
        let flow = Arc::new(self.build_stateless_flow(&args, true).await?);
        let slot = {
            let mut slots = self.stateless_flows.write().await;
            if let Some(slot) = slots.get(&flow_id) {
                Arc::clone(slot)
            } else {
                let slot = Arc::new(StatelessFlowSlot {
                    runtime: Arc::new(RwLock::new(None)),
                    active: AtomicBool::new(true),
                });
                slots.insert(flow_id, Arc::clone(&slot));
                slot
            }
        };
        let published = self
            .publish_initial_flow(
                slot.clone(),
                flow,
                args.create_if_not_exists,
                args.or_replace,
            )
            .await?;
        ensure!(
            self.stateless_flows
                .read()
                .await
                .get(&flow_id)
                .is_some_and(|current| Arc::ptr_eq(current, &slot)),
            FlowNotFoundSnafu { id: flow_id }
        );
        if published {
            info!("Successfully create flow with id={flow_id}");
            Ok(Some(flow_id))
        } else {
            Ok(None)
        }
    }

    async fn build_stateless_flow(
        &self,
        args: &CreateFlowArgs,
        create_sink: bool,
    ) -> Result<StatelessFlow, Error> {
        let CreateFlowArgs {
            flow_id,
            sink_table_name,
            source_table_ids,
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

        let query_ctx = query_ctx.clone().map(Arc::new).context(UnexpectedSnafu {
            reason: "Query context is missing",
        })?;
        let flow_plan =
            sql_to_df_plan(query_ctx.clone(), self.query_engine.clone(), sql, true).await?;
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
        let sink_exists = self.fetch_table_pk_schema(sink_table_name).await?.is_some();
        if !sink_exists {
            validate_auto_column_names(&inferred_schema)?;
        }
        if !sink_exists
            && create_sink
            && !self
                .create_table_from_relation(
                    &format!("flow-id={flow_id}"),
                    sink_table_name,
                    &inferred_relation,
                )
                .await?
        {
            return UnexpectedSnafu {
                reason: format!("Failed to create table {sink_table_name:?}"),
            }
            .fail();
        }
        ensure!(
            sink_exists || create_sink,
            UnexpectedSnafu {
                reason: format!("Sink table metadata is missing: {sink_table_name:?}"),
            }
        );

        // Fetch the metadata after validation or auto-creation. The sink's layout is the insert
        // contract: flow output aliases must not leak into the request schema.
        let (sink_primary_keys, _, sink_schema) = self
            .fetch_table_pk_schema(sink_table_name)
            .await?
            .context(UnexpectedSnafu {
                reason: format!("Sink table metadata is missing: {sink_table_name:?}"),
            })?;
        let (auto_columns, plan) = match resolve_sink_layout(&inferred_schema, &sink_schema) {
            Ok(auto_columns) => (auto_columns, flow_plan),
            Err(normal_error) => {
                // Appending a hidden source timestamp changes DISTINCT's key.  It is therefore
                // never a valid compatibility rewrite for a DISTINCT flow.
                let has_distinct = {
                    let mut found = false;
                    flow_plan
                        .apply(|node| {
                            if matches!(node, LogicalPlan::Distinct(Distinct::All(_))) {
                                found = true;
                            }
                            Ok(datafusion_common::tree_node::TreeNodeRecursion::Continue)
                        })
                        .context(DatafusionSnafu {
                            context: "Failed to inspect flow plan",
                        })?;
                    found
                };
                if has_distinct
                    || !sink_exists
                    || !is_explicit_source_timestamp_compatibility(
                        &inferred_schema,
                        &lineage,
                        &sink_schema,
                        &source_schema,
                    )
                {
                    return Err(normal_error);
                }
                let source_timestamp_index = source_schema.timestamp_index().unwrap();
                let source_timestamp_name =
                    &source_schema.column_schemas()[source_timestamp_index].name;
                let plan = stateless::rewrite_source_timestamp(
                    flow_plan,
                    &TableReference::full(
                        source_table_name[0].clone(),
                        source_table_name[1].clone(),
                        source_table_name[2].clone(),
                    ),
                    source_timestamp_name,
                )?;
                let (effective_output, _) = output_column_schemas(&plan, &source_schema)?;
                ensure!(
                    effective_output.len() == sink_schema.len()
                        && effective_output
                            .iter()
                            .zip(&sink_schema)
                            .all(|(output, sink)| output.data_type == sink.data_type),
                    InvalidQuerySnafu {
                        reason: "Compatibility plan output does not match the full sink schema"
                    }
                );
                (vec![], plan)
            }
        };

        Ok(StatelessFlow {
            source_table_id,
            source_table_name,
            source_schema_version: source_schema.version(),
            source_schema,
            sink_table_name: sink_table_name.clone(),
            sink_schema,
            sink_primary_keys,
            auto_columns,
            plan,
            query_ctx,
            create_args: args.clone(),
        })
    }

    pub async fn flush_flow_inner(&self, _flow_id: FlowId) -> Result<usize, Error> {
        Ok(0)
    }

    pub(crate) async fn stateless_flow_ids(&self) -> Vec<FlowId> {
        let slots = self.stateless_flows.read().await;
        let mut ids = Vec::new();
        for (id, slot) in slots.iter() {
            if slot.runtime.read().await.is_some() {
                ids.push(*id);
            }
        }
        ids
    }

    pub async fn flow_exist_inner(&self, flow_id: FlowId) -> Result<bool, Error> {
        let slot = self.stateless_flows.read().await.get(&flow_id).cloned();
        Ok(match slot {
            Some(slot) => slot.runtime.read().await.is_some(),
            None => false,
        })
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
        // A mirrored envelope can contain several regions of one source.  Normalize each request,
        // then concatenate by source so DISTINCT is evaluated once over the complete envelope.
        let mut grouped: BTreeMap<_, (_, Vec<DiffRow>, Vec<ConcreteDataType>, u32)> =
            BTreeMap::new();
        let mut poisoned_tables = std::collections::HashSet::new();
        let mut first_error = None;
        for write_request in request.requests {
            let region_id = RegionId::from(write_request.region_id);
            let table_id = region_id.table_id();
            if poisoned_tables.contains(&table_id) {
                continue;
            }
            match self.handle_insert_request(write_request).await {
                Ok((rows, types, version)) => {
                    let entry = grouped
                        .entry(table_id)
                        .or_insert_with(|| (region_id, vec![], types.clone(), version));
                    if entry.2 != types || entry.3 != version {
                        poisoned_tables.insert(table_id);
                        grouped.remove(&table_id);
                        let err = InvalidQuerySnafu { reason: format!("Source table {table_id} metadata changed within one insert envelope") }.build();
                        let ids: Vec<FlowId> = self
                            .flow_ids_for_table(table_id)
                            .await
                            .into_iter()
                            .map(|(id, _)| id)
                            .collect();
                        let err = InsertIntoFlowSnafu {
                            region_id: u64::from(region_id),
                            flow_ids: ids,
                        }
                        .into_error(BoxedError::new(err));
                        error!(err; "Failed to normalize flow insert request for region_id={region_id}");
                        if first_error.is_none() {
                            first_error = Some(err);
                        }
                    } else {
                        entry.1.extend(rows);
                    }
                }
                Err(err) => {
                    let ids: Vec<FlowId> = self
                        .flow_ids_for_table(table_id)
                        .await
                        .into_iter()
                        .map(|(id, _)| id)
                        .collect();
                    poisoned_tables.insert(table_id);
                    grouped.remove(&table_id);
                    let err = InsertIntoFlowSnafu {
                        region_id: u64::from(region_id),
                        flow_ids: ids,
                    }
                    .into_error(BoxedError::new(err));
                    error!(err; "Failed to normalize flow insert request for region_id={region_id}");
                    if first_error.is_none() {
                        first_error = Some(err);
                    }
                }
            }
        }
        for (_, (region_id, rows, types, version)) in grouped {
            if let Err(err) = self
                .handle_write_request(region_id, rows, &types, version)
                .await
                && first_error.is_none()
            {
                first_error = Some(err);
            }
        }
        match first_error {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    async fn handle_insert_request(
        &self,
        write_request: api::v1::region::InsertRequest,
    ) -> Result<(Vec<DiffRow>, Vec<ConcreteDataType>, u32), Error> {
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
        Ok((rows, table_types, source_schema_version))
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
