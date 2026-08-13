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

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use api::v1::{CreateTableExpr, TableName};
use catalog::CatalogManagerRef;
use common_error::ext::BoxedError;
use common_query::logical_plan::breakup_insert_plan;
use common_recordbatch::RecordBatches;
use common_recordbatch::util::collect_batches;
use common_telemetry::tracing::warn;
use common_telemetry::{debug, info};
use common_time::Timestamp;
use datafusion::datasource::DefaultTableSource;
use datafusion::functions_aggregate::expr_fn::{count, max};
use datafusion::sql::unparser::expr_to_sql;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::utils::quote_identifier;
use datafusion_common::{Column, DFSchema, DFSchemaRef, ScalarValue, TableReference};
use datafusion_expr::logical_plan::{EmptyRelation, TableScan};
use datafusion_expr::{DmlStatement, Expr, LogicalPlan, LogicalPlanBuilder, WriteOp, col, lit};
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::Schema;
use datatypes::value::Value;
use datatypes::vectors::Helper;
use query::QueryEngineRef;
use query::options::FLOW_INCREMENTAL_MODE;
use query::query_engine::DefaultSerializer;
use session::context::QueryContextRef;
use snafu::{OptionExt, ResultExt};
use sql::parsers::utils::is_tql;
use store_api::mito_engine_options::MERGE_MODE_KEY;
use substrait::{DFLogicalSubstraitConvertor, SubstraitPlan};
use table::table::adapter::DfTableProviderAdapter;
use tokio::sync::oneshot::error::TryRecvError;
use tokio::sync::{Mutex, oneshot};
use tokio::time::Instant;

use crate::adapter::AUTO_CREATED_UPDATE_AT_TS_COL;
use crate::batching_mode::checkpoint::{
    CHECKPOINT_RECORD_FORMAT_VERSION, CheckpointRecord, FlowCheckpointDecision,
    FlowQueryFallbackReason, checkpoint_mode_label, checkpoint_sentinel_ts_in_unit,
    decode_checkpoint_record, encode_checkpoint_record,
};
use crate::batching_mode::eval_schedule::{EvalSchedule, select_due_scheduled_times};
use crate::batching_mode::frontend_client::{FrontendClient, PeerDesc};
use crate::batching_mode::state::{
    CheckpointMode, CheckpointPersistence, DirtyTimeWindows, FilterExprInfo, TaskState,
    to_df_literal,
};
use crate::batching_mode::table_creator::{QueryType, create_table_with_expr};
use crate::batching_mode::time_window::TimeWindowExpr;
use crate::batching_mode::utils::{
    AddFilterRewriter, ColumnMatcherRewriter, df_plan_to_sql, gen_plan_with_matching_schema,
    get_table_info_df_schema, sql_to_df_plan,
};
use crate::batching_mode::{BatchingModeOptions, INTERNAL_FLOW_EPOCH_COL_NAME, IncrementalMode};
use crate::df_optimizer::apply_df_optimizer;
use crate::error::{
    DatafusionSnafu, DatatypesSnafu, ExternalSnafu, InvalidQuerySnafu,
    SubstraitEncodeLogicalPlanSnafu, UnexpectedSnafu,
};
use crate::metrics::{
    METRIC_FLOW_BATCHING_ENGINE_ERROR_CNT, METRIC_FLOW_BATCHING_ENGINE_QUERY_TIME,
    METRIC_FLOW_BATCHING_ENGINE_SLOW_QUERY, METRIC_FLOW_BATCHING_ENGINE_START_QUERY_CNT,
    METRIC_FLOW_ROWS,
};
use crate::{Error, FlowId};

mod ckpt;
mod inc;

/// Returns the current wall-clock Unix timestamp in seconds.
fn wall_clock_unix_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

/// The task's config, immutable once created
#[derive(Clone)]
pub struct TaskConfig {
    pub flow_id: FlowId,
    pub query: String,
    /// output schema of the query
    pub output_schema: DFSchemaRef,
    pub time_window_expr: Option<TimeWindowExpr>,
    /// in seconds
    pub expire_after: Option<i64>,
    pub sink_table_name: [String; 3],
    pub source_table_names: HashSet<[String; 3]>,
    pub catalog_manager: CatalogManagerRef,
    pub query_type: QueryType,
    pub batch_opts: Arc<BatchingModeOptions>,
    pub flow_eval_interval: Option<Duration>,
    /// Typed schedule configuration, pre-parsed at task creation time.
    pub eval_schedule: Option<EvalSchedule>,
}

fn determine_query_type(query: &str, query_ctx: &QueryContextRef) -> Result<QueryType, Error> {
    let is_tql = is_tql(query_ctx.sql_dialect(), query)
        .map_err(BoxedError::new)
        .context(ExternalSnafu)?;
    Ok(if is_tql {
        QueryType::Tql
    } else {
        QueryType::Sql
    })
}

fn is_merge_mode_last_non_null(options: &HashMap<String, String>) -> bool {
    options
        .get(MERGE_MODE_KEY)
        .map(|mode| mode.eq_ignore_ascii_case("last_non_null"))
        .unwrap_or(false)
}

fn encode_insert_plan_request(
    insert_to: TableName,
    insert_input_plan: &LogicalPlan,
) -> Result<api::v1::QueryRequest, Error> {
    let message = DFLogicalSubstraitConvertor {}
        .encode(insert_input_plan, DefaultSerializer)
        .context(SubstraitEncodeLogicalPlanSnafu)?;
    Ok(api::v1::QueryRequest {
        query: Some(api::v1::query_request::Query::InsertIntoPlan(
            api::v1::InsertIntoPlan {
                table_name: Some(insert_to),
                logical_plan: message.to_vec(),
            },
        )),
    })
}

fn format_insert_target_columns(plan: &LogicalPlan) -> String {
    plan.schema()
        .fields()
        .iter()
        .map(|field| quote_identifier(field.name()).to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

#[derive(Clone)]
pub struct BatchingTask {
    pub config: Arc<TaskConfig>,
    pub state: Arc<RwLock<TaskState>>,
    /// Serializes plan generation, execution, checkpoint advancement, and dirty
    /// window restoration for this flow. Without this, a manual flush and the
    /// background loop can process the same checkpoint range concurrently.
    execution_lock: Arc<Mutex<()>>,
}

/// Arguments for creating batching task
pub struct TaskArgs<'a> {
    pub flow_id: FlowId,
    pub query: &'a str,
    pub plan: LogicalPlan,
    pub time_window_expr: Option<TimeWindowExpr>,
    pub expire_after: Option<i64>,
    pub sink_table_name: [String; 3],
    pub source_table_names: Vec<[String; 3]>,
    pub query_ctx: QueryContextRef,
    pub catalog_manager: CatalogManagerRef,
    pub shutdown_rx: oneshot::Receiver<()>,
    pub batch_opts: Arc<BatchingModeOptions>,
    pub flow_eval_interval: Option<Duration>,
    /// Typed schedule configuration pre-parsed from `CreateFlowArgs`.
    pub eval_schedule: Option<EvalSchedule>,
}

pub struct PlanInfo {
    pub plan: LogicalPlan,
    pub dirty_restore: DirtyRestore,
    pub coverage: QueryCoverage,
}

#[derive(Clone)]
pub enum QueryCoverage {
    /// Explicit full-query snapshot coverage, e.g. TQL or evaluation-interval
    /// SQL flows whose plan shape cannot be safely dirty-window pruned. This
    /// must not be used as an implicit recovery path for scoped repair or an
    /// unsafe incremental rewrite fallback.
    UnfilteredFull,
    /// Scoped full-snapshot repair over the current dirty windows. A successful
    /// result may start a fenced repair if new dirty windows appeared meanwhile.
    ScopedBaseRepair,
    /// A chunk of windows being repaired under the frozen high-watermark `H`.
    /// The `high` map is sent as snapshot read bounds and must be matched by
    /// the returned terminal watermarks before checkpoints can advance.
    FencedRepairChunk { high: BTreeMap<u64, u64> },
    /// Incremental delta query over `(checkpoint, scan-open snapshot]`.
    IncrementalDelta,
}

impl QueryCoverage {
    /// Whether this query should use incremental scan extensions and
    /// incremental checkpoint advancement rules.
    fn is_incremental_delta(&self) -> bool {
        matches!(self, Self::IncrementalDelta)
    }

    /// Snapshot upper bounds requested from the storage layer. Only fenced
    /// repair chunks carry bounds; all other coverage relies on normal scans.
    fn snapshot_seqs(&self) -> HashMap<u64, u64> {
        match self {
            Self::FencedRepairChunk { high } => high.iter().map(|(k, v)| (*k, *v)).collect(),
            _ => HashMap::new(),
        }
    }
}

pub enum DirtyRestore {
    /// The query was scoped to dirty time ranges; restore those ranges if the
    /// run fails.
    Scoped(FilterExprInfo),
    /// The query could not be scoped to dirty time ranges, so the dirty-window
    /// state is only a dirty signal. Restore the consumed signal if the full
    /// run fails.
    ///
    /// TODO(discord9): Full-query runs only need a dirty bool flag. Refactor
    /// the unscoped path to stop reusing `DirtyTimeWindows` for this signal.
    Unscoped(DirtyTimeWindows),
}

struct ExecuteOnceOutcome {
    new_query: Option<PlanInfo>,
    /// Execution result of the generated insert plan.
    ///
    /// `Ok(Some((affected_rows, elapsed)))` means a query was executed.
    /// `Ok(None)` means no query was generated because there was no dirty signal.
    /// `Err(_)` means plan generation or execution failed.
    result: Result<Option<(usize, Duration)>, Error>,
}

/// True when `data_type` is a plain integer type (signed or unsigned).
fn is_integer_type(data_type: &ConcreteDataType) -> bool {
    matches!(
        data_type,
        ConcreteDataType::Int8(_)
            | ConcreteDataType::Int16(_)
            | ConcreteDataType::Int32(_)
            | ConcreteDataType::Int64(_)
            | ConcreteDataType::UInt8(_)
            | ConcreteDataType::UInt16(_)
            | ConcreteDataType::UInt32(_)
            | ConcreteDataType::UInt64(_)
    )
}

/// Returns a copy of the sink schema without the reserved internal epoch column
/// and the primary-key indices remapped onto the stripped schema.
///
/// Plan generation matches flow output against this stripped view; the epoch
/// column is stamped separately by checkpoint persistence
/// ([`BatchingTask::stamp_epoch_into_plan`]). The real query-produced BINARY
/// state column (e.g. the UDDSketch state of an exact EE-like flow) is NOT
/// stripped: it is ordinary flow output and must survive schema matching,
/// incremental rewrite, and the final DML insert.
fn strip_internal_epoch_column(
    schema: &Schema,
    primary_key_indices: &[usize],
) -> (Schema, Vec<usize>) {
    let mut column_schemas = Vec::with_capacity(schema.column_schemas().len());
    let mut new_primary_key_indices = Vec::new();
    for (idx, column) in schema.column_schemas().iter().enumerate() {
        if column.name == INTERNAL_FLOW_EPOCH_COL_NAME {
            continue;
        }
        let new_idx = column_schemas.len();
        column_schemas.push(column.clone());
        if primary_key_indices.contains(&idx) {
            new_primary_key_indices.push(new_idx);
        }
    }
    (Schema::new(column_schemas), new_primary_key_indices)
}

/// Builds the sentinel window literal in the given timestamp column's native
/// unit. Errors when the column is not a timestamp.
fn checkpoint_sentinel_scalar(
    column: &datatypes::schema::ColumnSchema,
) -> Result<ScalarValue, Error> {
    let ts_type = column
        .data_type
        .as_timestamp()
        .with_context(|| UnexpectedSnafu {
            reason: format!(
                "Expected timestamp column for checkpoint sentinel, found {}",
                column.data_type
            ),
        })?;
    let sentinel = checkpoint_sentinel_ts_in_unit(ts_type.unit());
    to_df_literal(Timestamp::new(sentinel, ts_type.unit()))
}

impl BatchingTask {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        TaskArgs {
            flow_id,
            query,
            plan,
            time_window_expr,
            expire_after,
            sink_table_name,
            source_table_names,
            query_ctx,
            catalog_manager,
            shutdown_rx,
            batch_opts,
            flow_eval_interval,
            eval_schedule,
        }: TaskArgs<'_>,
    ) -> Result<Self, Error> {
        let mut state = TaskState::with_dirty_time_windows(
            query_ctx.clone(),
            shutdown_rx,
            DirtyTimeWindows::new(
                batch_opts.experimental_max_filter_num_per_query,
                batch_opts.experimental_time_window_merge_threshold,
            ),
        );
        if !batch_opts.experimental_enable_incremental_read {
            state.disable_incremental();
        }

        Ok(Self {
            config: Arc::new(TaskConfig {
                flow_id,
                query: query.to_string(),
                time_window_expr,
                expire_after,
                sink_table_name,
                source_table_names: source_table_names.into_iter().collect(),
                catalog_manager,
                output_schema: plan.schema().clone(),
                query_type: determine_query_type(query, &query_ctx)?,
                batch_opts,
                flow_eval_interval,
                eval_schedule,
            }),
            state: Arc::new(RwLock::new(state)),
            execution_lock: Arc::new(Mutex::new(())),
        })
    }

    pub fn last_execution_time_millis(&self) -> Option<i64> {
        self.state.read().unwrap().last_execution_time_millis()
    }

    pub fn start_time_millis(&self) -> Option<i64> {
        self.state.read().unwrap().start_time_millis()
    }

    /// Collect flow-related extensions from the task's query context that should be
    /// forwarded to the frontend (e.g. scheduled time).
    fn frontend_extensions(&self) -> HashMap<String, String> {
        let ctx = self.state.read().unwrap();
        let all = ctx.query_ctx.extensions();
        let mut flow_exts = HashMap::new();
        // Propagate the scheduled time extension if present so that frontend
        // execution can use the same logical time.
        if let Some(v) = all.get(query::options::FLOW_SCHEDULED_TIME_MILLIS) {
            flow_exts.insert(
                query::options::FLOW_SCHEDULED_TIME_MILLIS.to_string(),
                v.clone(),
            );
        }
        flow_exts
    }

    /// mark time window range (now - expire_after, now) as dirty (or (0, now) if expire_after not set)
    ///
    /// useful for flush_flow to flush dirty time windows range
    pub fn mark_all_windows_as_dirty(&self) -> Result<(), Error> {
        let now = SystemTime::now();
        let now = Timestamp::new_second(
            now.duration_since(UNIX_EPOCH)
                .expect("Time went backwards")
                .as_secs() as _,
        );
        let lower_bound = self
            .config
            .expire_after
            .map(|e| now.sub_duration(Duration::from_secs(e as _)))
            .transpose()
            .map_err(BoxedError::new)
            .context(ExternalSnafu)?
            .unwrap_or(Timestamp::new_second(0));
        debug!(
            "Flow {} mark range ({:?}, {:?}) as dirty",
            self.config.flow_id, lower_bound, now
        );
        self.state
            .write()
            .unwrap()
            .dirty_time_windows
            .add_window(lower_bound, Some(now));
        Ok(())
    }

    /// Create sink table if not exists
    pub async fn check_or_create_sink_table(
        &self,
        engine: &QueryEngineRef,
        frontend_client: &Arc<FrontendClient>,
    ) -> Result<Option<(usize, Duration)>, Error> {
        if !self.is_table_exist(&self.config.sink_table_name).await? {
            let create_table = self.gen_create_table_expr(engine.clone()).await?;
            info!(
                "Try creating sink table(if not exists) with expr: {:?}",
                create_table
            );
            self.create_table(frontend_client, create_table).await?;
            info!(
                "Sink table {}(if not exists) created",
                self.config.sink_table_name.join(".")
            );
        }

        Ok(None)
    }

    /// Validates that the sink table schema can accept this flow's output.
    ///
    /// This is a dry-run of the same schema matching logic used by insert-plan
    /// generation, but without adding dirty-window filters or executing the query. It is used
    /// during CREATE FLOW to catch existing sink table mismatches early.
    pub async fn validate_sink_table_schema(&self, engine: &QueryEngineRef) -> Result<(), Error> {
        let (table, _) = get_table_info_df_schema(
            self.config.catalog_manager.clone(),
            self.config.sink_table_name.clone(),
        )
        .await?;

        let table_meta = &table.table_info().meta;
        let merge_mode_last_non_null =
            is_merge_mode_last_non_null(&table_meta.options.extra_options);
        let primary_key_indices = table_meta.primary_key_indices.clone();
        let query_ctx = self.state.read().unwrap().query_ctx.clone();
        // The reserved internal epoch column is not produced by the flow query;
        // it is stamped separately when checkpoint persistence is active, so it
        // is excluded from schema matching. The real BINARY state column stays.
        let (effective_schema, effective_pk_indices) =
            strip_internal_epoch_column(&table_meta.schema, &primary_key_indices);

        gen_plan_with_matching_schema(
            &self.config.query,
            query_ctx,
            engine.clone(),
            Arc::new(effective_schema),
            &effective_pk_indices,
            merge_mode_last_non_null,
        )
        .await
        .map(|_| ())
    }

    async fn is_table_exist(&self, table_name: &[String; 3]) -> Result<bool, Error> {
        self.config
            .catalog_manager
            .table_exists(&table_name[0], &table_name[1], &table_name[2], None)
            .await
            .map_err(BoxedError::new)
            .context(ExternalSnafu)
    }

    pub(crate) async fn execute_once_serialized(
        &self,
        engine: &QueryEngineRef,
        frontend_client: &Arc<FrontendClient>,
        max_window_cnt: Option<usize>,
    ) -> Result<Option<(usize, Duration)>, Error> {
        let outcome = self
            .execute_once_serialized_with_outcome(engine, frontend_client, max_window_cnt)
            .await;
        outcome.result
    }

    /// Executes one flow evaluation under `execution_lock` and keeps the
    /// generated query context for the background loop's error logging/backoff.
    async fn execute_once_serialized_with_outcome(
        &self,
        engine: &QueryEngineRef,
        frontend_client: &Arc<FrontendClient>,
        max_window_cnt: Option<usize>,
    ) -> ExecuteOnceOutcome {
        let _execution_guard = self.execution_lock.lock().await;
        self.execute_once_unlocked(engine, frontend_client, max_window_cnt)
            .await
    }

    /// Executes one flow evaluation. Caller must hold `execution_lock`.
    async fn execute_once_unlocked(
        &self,
        engine: &QueryEngineRef,
        frontend_client: &Arc<FrontendClient>,
        max_window_cnt: Option<usize>,
    ) -> ExecuteOnceOutcome {
        let new_query = match self.gen_insert_plan_unlocked(engine, max_window_cnt).await {
            Ok(new_query) => new_query,
            Err(err) => {
                return ExecuteOnceOutcome {
                    new_query: None,
                    result: Err(err),
                };
            }
        };

        if let Some(new_query) = new_query {
            debug!("Generate new query: {}", new_query.plan);
            let res = self
                .execute_logical_plan_unlocked(
                    frontend_client,
                    &new_query.plan,
                    &new_query.dirty_restore,
                    &new_query.coverage,
                )
                .await;
            if res.is_err() {
                self.handle_executed_query_failure(Some(&new_query));
            }
            ExecuteOnceOutcome {
                new_query: Some(new_query),
                result: res,
            }
        } else {
            debug!("Generate no query");
            ExecuteOnceOutcome {
                new_query: None,
                result: Ok(None),
            }
        }
    }

    /// Generates the insert plan. Caller must reach this through the serialized path.
    async fn gen_insert_plan_unlocked(
        &self,
        engine: &QueryEngineRef,
        max_window_cnt: Option<usize>,
    ) -> Result<Option<PlanInfo>, Error> {
        let (table, df_schema) = get_table_info_df_schema(
            self.config.catalog_manager.clone(),
            self.config.sink_table_name.clone(),
        )
        .await?;

        let table_meta = &table.table_info().meta;
        let merge_mode_last_non_null =
            is_merge_mode_last_non_null(&table_meta.options.extra_options);
        let primary_key_indices = table_meta.primary_key_indices.clone();
        // The reserved internal epoch column is stamped onto every emitted row
        // separately (see `stamp_epoch_into_plan`), so plan generation matches
        // flow output against the sink schema without it. The real
        // query-produced BINARY state column stays in the matched schema.
        let (effective_schema, effective_pk_indices) =
            strip_internal_epoch_column(&table_meta.schema, &primary_key_indices);
        let effective_schema = Arc::new(effective_schema);

        let new_query = self
            .gen_query_with_time_window(
                engine.clone(),
                &effective_schema,
                &effective_pk_indices,
                merge_mode_last_non_null,
                max_window_cnt,
            )
            .await?;

        let Some(new_query) = new_query else {
            return Ok(None);
        };

        // first check if all columns in input query exists in sink table
        // since insert into ref to names in record batch generate by given query
        let table_columns = df_schema
            .columns()
            .into_iter()
            .map(|c| c.name)
            .collect::<BTreeSet<_>>();
        for column in new_query.plan.schema().columns() {
            if !table_columns.contains(column.name()) {
                self.restore_dirty_windows_after_failure(&new_query);
                return InvalidQuerySnafu {
                    reason: format!(
                        "Column {} not found in sink table with columns {:?}",
                        column, table_columns
                    ),
                }
                .fail();
            }
        }

        let table_provider = Arc::new(DfTableProviderAdapter::new(table));
        let table_source = Arc::new(DefaultTableSource::new(table_provider));

        // update_at& time index placeholder (if exists) should have default value
        let plan = LogicalPlan::Dml(DmlStatement::new(
            datafusion_common::TableReference::Full {
                catalog: self.config.sink_table_name[0].clone().into(),
                schema: self.config.sink_table_name[1].clone().into(),
                table: self.config.sink_table_name[2].clone().into(),
            },
            table_source,
            WriteOp::Insert(datafusion_expr::dml::InsertOp::Append),
            Arc::new(new_query.plan.clone()),
        ));
        let insert_into_info = PlanInfo {
            plan,
            dirty_restore: new_query.dirty_restore,
            coverage: new_query.coverage,
        };
        let insert_into =
            match insert_into_info
                .plan
                .clone()
                .recompute_schema()
                .context(DatafusionSnafu {
                    context: "Failed to recompute schema",
                }) {
                Ok(insert_into) => insert_into,
                Err(err) => {
                    self.restore_dirty_windows_after_failure(&insert_into_info);
                    return Err(err);
                }
            };

        Ok(Some(PlanInfo {
            plan: insert_into,
            dirty_restore: insert_into_info.dirty_restore,
            coverage: insert_into_info.coverage,
        }))
    }

    pub async fn create_table(
        &self,
        frontend_client: &Arc<FrontendClient>,
        expr: CreateTableExpr,
    ) -> Result<(), Error> {
        let catalog = &self.config.sink_table_name[0];
        let schema = &self.config.sink_table_name[1];
        frontend_client
            .create(expr.clone(), catalog, schema)
            .await?;
        Ok(())
    }

    /// Executes the insert plan. Caller must reach this through the serialized path.
    async fn execute_logical_plan_unlocked(
        &self,
        frontend_client: &Arc<FrontendClient>,
        plan: &LogicalPlan,
        dirty_restore: &DirtyRestore,
        coverage: &QueryCoverage,
    ) -> Result<Option<(usize, Duration)>, Error> {
        let instant = Instant::now();
        let flow_id = self.config.flow_id;

        debug!(
            "Executing flow {flow_id}(expire_after={:?} secs) with query {}",
            self.config.expire_after, &plan
        );

        let catalog = &self.config.sink_table_name[0];
        let schema = &self.config.sink_table_name[1];

        // fix all table ref by make it fully qualified, i.e. "table_name" => "catalog_name.schema_name.table_name"
        let plan = plan
            .clone()
            .transform_down_with_subqueries(|p| {
                if let LogicalPlan::TableScan(mut table_scan) = p {
                    let resolved = table_scan.table_name.resolve(catalog, schema);
                    table_scan.table_name = resolved.into();
                    Ok(Transformed::yes(LogicalPlan::TableScan(table_scan)))
                } else {
                    Ok(Transformed::no(p))
                }
            })
            .with_context(|_| DatafusionSnafu {
                context: format!("Failed to fix table ref in logical plan, plan={:?}", plan),
            })?
            .data;

        // For incremental-mode SQL queries, attempt to rewrite the delta aggregate
        // plan into a safe delta-LEFT-JOIN-sink form before deciding on extensions.
        let incremental_plan = if coverage.is_incremental_delta() {
            self.prepare_plan_for_incremental(&plan).await?
        } else {
            None
        };
        let incremental_safe = incremental_plan.is_some();
        if coverage.is_incremental_delta() && !incremental_safe {
            debug!(
                "Flow {flow_id} skipped unsafe incremental delta fallback; \
                 restored dirty signal instead of executing an unfiltered full snapshot"
            );
            self.restore_dirty_windows(dirty_restore);
            return Ok(None);
        }
        let plan = incremental_plan.unwrap_or_else(|| plan.clone());

        // Stamp the current cycle epoch onto every emitted state row when
        // checkpoint persistence is active. The epoch is decided here, once
        // per cycle, and reused for the checkpoint row write after success.
        let (plan, cycle_epoch) = self.stamp_epoch_into_plan(plan).await?;

        let extensions = self
            .build_flow_query_extensions(incremental_safe, coverage.is_incremental_delta())
            .await?;
        let frontend_extensions = self.frontend_extensions();
        let extension_refs = extensions
            .iter()
            .map(|(key, value)| (*key, value.as_str()))
            .chain(
                frontend_extensions
                    .iter()
                    .map(|(key, value)| (key.as_str(), value.as_str())),
            )
            .collect::<Vec<_>>();
        let query_mode = if extensions
            .iter()
            .any(|(key, _)| *key == FLOW_INCREMENTAL_MODE)
        {
            CheckpointMode::Incremental
        } else {
            CheckpointMode::FullSnapshot
        };
        Self::record_query_mode(flow_id, query_mode);
        debug!(
            "Flow {flow_id} executing batching query with checkpoint_mode={}, extension_count={}",
            checkpoint_mode_label(query_mode),
            extensions.len()
        );

        let mut peer_desc = None;
        let res = {
            let _timer = METRIC_FLOW_BATCHING_ENGINE_QUERY_TIME
                .with_label_values(&[flow_id.to_string().as_str()])
                .start_timer();

            let req = if let Some((insert_to, insert_input_plan)) =
                breakup_insert_plan(&plan, catalog, schema)
            {
                if query_mode == CheckpointMode::FullSnapshot
                    && matches!(self.config.query_type, QueryType::Sql)
                    && self.config.flow_eval_interval.is_some()
                    && self.config.time_window_expr.is_none()
                {
                    // Evaluation-interval SQL flows without a time-window
                    // expression execute as full-query snapshots. Send these
                    // as SQL text instead of Substrait to avoid logical-plan
                    // round-trip issues around complex joins/unions/CTEs and
                    // duplicate field aliases. Keep ordinary SQL full snapshots
                    // on the existing InsertIntoPlan path because SQL unparsing
                    // is not valid for every planned aggregate shape yet.
                    // If the local SQL unparser does not support this plan,
                    // keep the previous InsertIntoPlan transport as a fallback.
                    match df_plan_to_sql(&insert_input_plan) {
                        Ok(select_sql) => {
                            let target_columns = format_insert_target_columns(&insert_input_plan);
                            let sql = format!(
                                "INSERT INTO {} ({}) {}",
                                TableReference::full(
                                    insert_to.catalog_name.as_str(),
                                    insert_to.schema_name.as_str(),
                                    insert_to.table_name.as_str(),
                                )
                                .to_quoted_string(),
                                target_columns,
                                select_sql
                            );
                            api::v1::QueryRequest {
                                query: Some(api::v1::query_request::Query::Sql(sql)),
                            }
                        }
                        Err(err) => {
                            debug!(
                                "Failed to unparse full-snapshot SQL flow {} plan; \
                                 falling back to InsertIntoPlan: {:?}",
                                flow_id, err
                            );
                            encode_insert_plan_request(insert_to, &insert_input_plan)?
                        }
                    }
                } else {
                    encode_insert_plan_request(insert_to, &insert_input_plan)?
                }
            } else {
                let message = DFLogicalSubstraitConvertor {}
                    .encode(&plan, DefaultSerializer)
                    .context(SubstraitEncodeLogicalPlanSnafu)?;

                api::v1::QueryRequest {
                    query: Some(api::v1::query_request::Query::LogicalPlan(message.to_vec())),
                }
            };

            let snapshot_seqs = coverage.snapshot_seqs();
            {
                let mut state = self.state.write().unwrap();
                state.record_start_time_if_first();
            }
            frontend_client
                .query_with_terminal_metrics(
                    catalog,
                    schema,
                    req,
                    &extension_refs,
                    &snapshot_seqs,
                    &mut peer_desc,
                )
                .await
        };

        let elapsed = instant.elapsed();
        let peer_label = peer_desc
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_else(|| PeerDesc::default().to_string());
        if let Err(err) = &res {
            warn!(
                "Failed to execute Flow {flow_id} on frontend {peer_label}, result: {err:?}, elapsed: {:?} with query: {}",
                elapsed, &plan
            );
            let decision = {
                let mut state = self.state.write().unwrap();
                let reason = Self::query_failure_reason(err, coverage);
                Self::apply_query_failure_to_state(&mut state, elapsed, coverage, reason)
            };
            if let Some(decision) = decision {
                Self::record_checkpoint_decision(flow_id, decision);
            }
        }

        // record slow query
        if elapsed >= self.config.batch_opts.slow_query_threshold {
            warn!(
                "Flow {flow_id} on frontend {peer_label} executed for {:?} before complete, query: {}",
                elapsed, &plan
            );
            let flow_id = flow_id.to_string();
            METRIC_FLOW_BATCHING_ENGINE_SLOW_QUERY
                .with_label_values(&[flow_id.as_str(), peer_label.as_str()])
                .observe(elapsed.as_secs_f64());
        }

        let res = res?;
        let (affected_rows, _) = res.output.extract_rows_and_cost();
        debug!(
            "Flow {flow_id} executed, affected_rows: {affected_rows:?}, elapsed: {:?}, watermark: {:?}",
            elapsed,
            res.region_watermark_map()
        );
        METRIC_FLOW_ROWS
            .with_label_values(&[format!("{}-out-batching", flow_id).as_str()])
            .inc_by(affected_rows as _);
        // Checkpoint persistence: apply the single authoritative checkpoint
        // transition first, then persist the singleton checkpoint row only when
        // the actual decision advanced checkpoints, using the resulting
        // `state.checkpoints()` snapshot. The whole cycle runs under
        // `execution_lock`, so no other execution interleaves with the write.
        // If the write succeeds, advance the durable epoch; if it fails, reset
        // to full snapshot and restore the executed plan's consumed dirty work
        // so the next cycle re-runs a full repair/backfill and can write a
        // replacement checkpoint. Dirty notifications arriving around the
        // transition are never erased.
        let decision = {
            let mut state = self.state.write().unwrap();
            let decision = Self::apply_query_result_to_state(&mut state, &res, elapsed, coverage);
            let persist = cycle_epoch
                .filter(|_| {
                    matches!(
                        decision,
                        FlowCheckpointDecision::AdvancedFromFullSnapshot { .. }
                            | FlowCheckpointDecision::AdvancedIncremental { .. }
                    )
                })
                .map(|epoch| (epoch, state.checkpoints().clone(), state.checkpoint_mode()));
            (decision, persist)
        };
        let decision = match decision.1 {
            Some((epoch, map_to_persist, previous_mode)) => {
                match self
                    .write_checkpoint_row(frontend_client, epoch, &map_to_persist)
                    .await
                {
                    Ok(()) => {
                        let mut state = self.state.write().unwrap();
                        state.advance_persisted_epoch(epoch);
                        decision.0
                    }
                    Err(err) => {
                        warn!(
                            "Flow {flow_id} failed to persist checkpoint row, falling back to full snapshot and re-scheduling dirty work: {err:?}"
                        );
                        let mut state = self.state.write().unwrap();
                        state.mark_full_snapshot();
                        drop(state);
                        self.restore_dirty_windows(dirty_restore);
                        FlowCheckpointDecision::FallbackToFullSnapshot {
                            previous_mode,
                            reason: FlowQueryFallbackReason::CheckpointPersistFailure,
                        }
                    }
                }
            }
            None => decision.0,
        };
        Self::record_checkpoint_decision(flow_id, decision);

        Ok(Some((affected_rows, elapsed)))
    }

    /// Restore dirty windows consumed by a failed query so they are retried on
    /// the next execution.
    ///
    fn restore_dirty_windows(&self, dirty_restore: &DirtyRestore) {
        match dirty_restore {
            DirtyRestore::Scoped(filter) => self.restore_scoped_dirty_windows(filter),
            DirtyRestore::Unscoped(dirty_windows) => self
                .state
                .write()
                .unwrap()
                .dirty_time_windows
                .add_dirty_windows(dirty_windows),
        }
    }

    /// Restore the dirty signal for a plan that was generated but failed before
    /// it could prove any checkpoint advancement.
    fn restore_dirty_windows_after_failure(&self, query: &PlanInfo) {
        self.restore_dirty_windows(&query.dirty_restore);
    }

    /// Restore scoped windows through `TaskState` so fenced repair can decide
    /// whether they go back to pending repair or live dirty state.
    fn restore_scoped_dirty_windows(&self, filter: &FilterExprInfo) {
        self.state.write().unwrap().restore_scoped_windows(filter);
    }

    /// Run a fallible scoped operation and restore its consumed windows if plan
    /// generation/rewrite fails before execution.
    fn restore_scoped_dirty_windows_on_err<T>(
        &self,
        filter: &FilterExprInfo,
        result: Result<T, Error>,
    ) -> Result<T, Error> {
        result.inspect_err(|_| {
            self.restore_scoped_dirty_windows(filter);
        })
    }

    /// Restore an unscoped dirty signal consumed by an explicit full-query or
    /// incremental-delta plan.
    fn restore_unscoped_dirty_windows(&self, dirty_windows: &DirtyTimeWindows) {
        self.state
            .write()
            .unwrap()
            .dirty_time_windows
            .add_dirty_windows(dirty_windows);
    }

    /// Run a fallible unscoped operation and restore the dirty signal if it
    /// fails before a query is executed.
    fn restore_unscoped_dirty_windows_on_err<T>(
        &self,
        dirty_windows: &DirtyTimeWindows,
        result: Result<T, Error>,
    ) -> Result<T, Error> {
        result.inspect_err(|_| {
            self.restore_unscoped_dirty_windows(dirty_windows);
        })
    }

    /// Consume the live dirty signal for an unscoped query while keeping a copy
    /// that can be restored if planning or execution fails.
    fn drain_dirty_windows_signal(&self) -> (bool, DirtyTimeWindows) {
        let mut state = self.state.write().unwrap();
        let dirty_windows_to_restore = state.dirty_time_windows.clone();
        let is_dirty = !dirty_windows_to_restore.is_empty();
        state.dirty_time_windows.clean();
        (is_dirty, dirty_windows_to_restore)
    }

    #[allow(clippy::too_many_arguments)]
    /// Build an unfiltered plan for explicit full-query or incremental-delta
    /// coverage. Callers pass the consumed dirty signal for failure restoration.
    async fn gen_unfiltered_plan_info(
        &self,
        engine: QueryEngineRef,
        query_ctx: QueryContextRef,
        sink_table_schema: Arc<Schema>,
        primary_key_indices: &[usize],
        allow_partial: bool,
        dirty_windows_to_restore: DirtyTimeWindows,
        retention_filter: Option<(&str, Timestamp, &'static str)>,
        coverage: QueryCoverage,
    ) -> Result<PlanInfo, Error> {
        let mut plan = self.restore_unscoped_dirty_windows_on_err(
            &dirty_windows_to_restore,
            gen_plan_with_matching_schema(
                &self.config.query,
                query_ctx,
                engine,
                sink_table_schema,
                primary_key_indices,
                allow_partial,
            )
            .await,
        )?;

        if let Some((col_name, lower_bound, context)) = retention_filter {
            let lower = self.restore_unscoped_dirty_windows_on_err(
                &dirty_windows_to_restore,
                to_df_literal(lower_bound),
            )?;
            let retention_filter = col(col_name).gt_eq(lit(lower));
            let mut add_filter = AddFilterRewriter::new(retention_filter);
            plan = self.restore_unscoped_dirty_windows_on_err(
                &dirty_windows_to_restore,
                plan.clone()
                    .rewrite(&mut add_filter)
                    .with_context(|_| DatafusionSnafu {
                        context: format!(
                            "Failed to apply {context} expire_after filter to plan:\n {}\n",
                            plan
                        ),
                    })
                    .map(|rewrite| rewrite.data),
            )?;
        }

        Ok(PlanInfo {
            plan,
            dirty_restore: DirtyRestore::Unscoped(dirty_windows_to_restore),
            coverage,
        })
    }

    #[allow(clippy::too_many_arguments)]
    /// Build an unfiltered plan only when the live dirty signal was present;
    /// otherwise skip this round without querying.
    async fn gen_unfiltered_plan_info_if_dirty(
        &self,
        engine: QueryEngineRef,
        query_ctx: QueryContextRef,
        sink_table_schema: Arc<Schema>,
        primary_key_indices: &[usize],
        allow_partial: bool,
        retention_filter: Option<(&str, Timestamp, &'static str)>,
        coverage: QueryCoverage,
    ) -> Result<Option<PlanInfo>, Error> {
        let (is_dirty, dirty_windows_to_restore) = self.drain_dirty_windows_signal();
        if !is_dirty {
            debug!("Flow id={:?}, no new data, not update", self.config.flow_id);
            return Ok(None);
        }

        self.gen_unfiltered_plan_info(
            engine,
            query_ctx,
            sink_table_schema,
            primary_key_indices,
            allow_partial,
            dirty_windows_to_restore,
            retention_filter,
            coverage,
        )
        .await
        .map(Some)
    }

    fn handle_executed_query_failure(&self, query: Option<&PlanInfo>) {
        if let Some(query) = query {
            self.restore_dirty_windows_after_failure(query);
        }
    }

    /// start executing query in a loop, break when receive shutdown signal
    ///
    /// any error will be logged when executing query.
    ///
    /// Dispatches to:
    /// - scheduled loop when `flow_eval_interval.is_some()`
    /// - adaptive dirty-window loop otherwise
    pub async fn start_executing_loop(
        &self,
        engine: QueryEngineRef,
        frontend_client: Arc<FrontendClient>,
    ) {
        if self.config.flow_eval_interval.is_some() {
            self.start_scheduled_loop(engine, frontend_client).await;
        } else {
            self.start_adaptive_loop(engine, frontend_client).await;
        }
    }

    /// Scheduled batching loop for flows with `EVAL INTERVAL`.
    ///
    /// Uses the pre-parsed `EvalSchedule` from `TaskConfig` and selects due
    /// scheduled times using bounded catch-up semantics. Each scheduled time is the
    /// scheduled evaluation time used as logical `now()` for that attempt.
    /// Each attempt temporarily sets `flow.scheduled_time_millis` on the
    /// task's `QueryContext` and executes under the existing `execution_lock`.
    /// After every attempt (success, no-op, or failure) the in-memory
    /// cursor advances.
    async fn start_scheduled_loop(
        &self,
        engine: QueryEngineRef,
        frontend_client: Arc<FrontendClient>,
    ) {
        let flow_id_str = self.config.flow_id.to_string();

        let schedule = match &self.config.eval_schedule {
            Some(s) => s.clone(),
            None => {
                let eval_interval_secs = self
                    .config
                    .flow_eval_interval
                    .map(|d| d.as_secs() as i64)
                    .expect("checked by caller");

                // Fallback: no typed config provided. Compute defaults
                // anchored at epoch/start=0.
                match EvalSchedule::from_config(Some(eval_interval_secs), None) {
                    Ok(Some(s)) => s,
                    Ok(None) => {
                        warn!(
                            "Flow {}: EVAL INTERVAL set but no schedule parsed; exiting loop",
                            flow_id_str
                        );
                        return;
                    }
                    Err(e) => {
                        warn!(
                            "Flow {}: Failed to parse eval schedule: {}; exiting loop",
                            flow_id_str, e
                        );
                        return;
                    }
                }
            }
        };

        // Initial cursor is one interval before start so the first due
        // scheduled time is `start_secs`.
        let mut cursor_secs = schedule.start_secs.saturating_sub(schedule.interval_secs);

        info!(
            "Flow {}: entering scheduled loop, interval={}s, start={}, anchor={}, policy={:?}, max_runs={}, max_lag={}s",
            flow_id_str,
            schedule.interval_secs,
            schedule.start_secs,
            schedule.anchor_secs,
            schedule.missed_tick_policy,
            schedule.max_runs,
            schedule.max_lag_secs,
        );

        loop {
            if self.is_shutdown_signaled() {
                break;
            }

            let wall_now_secs = wall_clock_unix_secs();

            let due = match select_due_scheduled_times(&schedule, cursor_secs, wall_now_secs) {
                Some(d) => d,
                None => {
                    warn!(
                        "Flow {}: Invalid schedule (interval <= 0), exiting loop",
                        flow_id_str
                    );
                    return;
                }
            };

            if due.scheduled_times_secs.is_empty() {
                if due.skipped > 0 {
                    warn!(
                        "Flow {}: all {} due scheduled times skipped by max-lag, advancing cursor to wall-clock ({wall_now_secs}) to avoid re-skipping",
                        flow_id_str, due.skipped
                    );
                    cursor_secs = wall_now_secs;
                    continue;
                }

                // No due yet — sleep until the next scheduled time.
                let next = schedule.next_scheduled_time_after(cursor_secs);
                if next <= wall_now_secs {
                    // Shouldn't happen given select_due_scheduled_times returned empty,
                    // but guard against clock skew / logic error.
                    cursor_secs = wall_now_secs;
                    continue;
                }
                let wait_secs = (next - wall_now_secs) as u64;
                let wait_dur = Duration::from_secs(wait_secs);
                debug!(
                    "Flow {}: no due scheduled times, sleeping for {}s until next scheduled time at {}",
                    flow_id_str, wait_secs, next
                );
                tokio::time::sleep(wait_dur).await;
                continue;
            }

            if due.skipped > 0 {
                info!(
                    "Flow {}: {} due scheduled times, {} skipped (catch-up)",
                    flow_id_str,
                    due.scheduled_times_secs.len(),
                    due.skipped
                );
            }

            // Execute scheduled times oldest → newest.
            for scheduled_time_secs in &due.scheduled_times_secs {
                if self.is_shutdown_signaled() {
                    break;
                }

                METRIC_FLOW_BATCHING_ENGINE_START_QUERY_CNT
                    .with_label_values(&[&flow_id_str])
                    .inc();

                let outcome = self
                    .execute_once_serialized_at_scheduled_time(
                        &engine,
                        &frontend_client,
                        *scheduled_time_secs,
                    )
                    .await;

                // Advance cursor regardless of outcome.
                cursor_secs = *scheduled_time_secs;

                match outcome.result {
                    Ok(Some((rows, elapsed))) => {
                        debug!(
                            "Flow {}: scheduled time {} completed, rows={}, elapsed={:?}",
                            flow_id_str, scheduled_time_secs, rows, elapsed
                        );
                    }
                    Ok(None) => {
                        debug!(
                            "Flow {}: scheduled time {} produced no query (no dirty signal or no-op)",
                            flow_id_str, scheduled_time_secs
                        );
                    }
                    Err(err) => {
                        warn!(
                            "Flow {}: scheduled time {} failed: {:?}",
                            flow_id_str, scheduled_time_secs, err
                        );
                        METRIC_FLOW_BATCHING_ENGINE_ERROR_CNT
                            .with_label_values(&[&flow_id_str])
                            .inc();
                        // Dirty-window restoration is handled by the
                        // existing `handle_executed_query_failure` inside
                        // `execute_once_unlocked`.
                    }
                }
            }
        }
    }

    /// Existing adaptive dirty-window loop for flows without `EVAL INTERVAL`.
    async fn start_adaptive_loop(
        &self,
        engine: QueryEngineRef,
        frontend_client: Arc<FrontendClient>,
    ) {
        let flow_id_str = self.config.flow_id.to_string();
        let mut max_window_cnt = None;
        loop {
            if self.is_shutdown_signaled() {
                break;
            }
            METRIC_FLOW_BATCHING_ENGINE_START_QUERY_CNT
                .with_label_values(&[&flow_id_str])
                .inc();

            let min_refresh = self.config.batch_opts.experimental_min_refresh_duration;

            let outcome = self
                .execute_once_serialized_with_outcome(&engine, &frontend_client, max_window_cnt)
                .await;

            match outcome.result {
                Ok(Some(_)) => {
                    max_window_cnt = max_window_cnt.map(|cnt| {
                        (cnt + 1).min(self.config.batch_opts.experimental_max_filter_num_per_query)
                    });

                    let sleep_until = {
                        let state = self.state.write().unwrap();

                        let time_window_size = self
                            .config
                            .time_window_expr
                            .as_ref()
                            .and_then(|t| *t.time_window_size());

                        let prefer_short_incremental_cadence = state.checkpoint_mode()
                            == CheckpointMode::Incremental
                            && !state.is_incremental_disabled();

                        state.get_next_start_query_time(
                            self.config.flow_id,
                            &time_window_size,
                            min_refresh,
                            Some(self.config.batch_opts.query_timeout),
                            self.config.batch_opts.experimental_max_filter_num_per_query,
                            prefer_short_incremental_cadence,
                        )
                    };

                    tokio::time::sleep_until(sleep_until).await;
                }
                Ok(None) => {
                    debug!(
                        "Flow id = {:?} found no new data, sleep for {:?} then continue",
                        self.config.flow_id, min_refresh
                    );
                    tokio::time::sleep(min_refresh).await;
                    continue;
                }
                Err(err) => {
                    METRIC_FLOW_BATCHING_ENGINE_ERROR_CNT
                        .with_label_values(&[&flow_id_str])
                        .inc();
                    match outcome.new_query {
                        Some(query) => {
                            common_telemetry::error!(err; "Failed to execute query for flow={} with query: {}", self.config.flow_id, query.plan);
                            max_window_cnt = Some(1);
                        }
                        None => {
                            common_telemetry::error!(err; "Failed to generate query for flow={}", self.config.flow_id)
                        }
                    }
                    tokio::time::sleep(min_refresh).await;
                }
            }
        }
    }

    /// Check whether the shutdown signal has been received.
    fn is_shutdown_signaled(&self) -> bool {
        let mut state = self.state.write().unwrap();
        match state.shutdown_rx.try_recv() {
            Ok(()) | Err(TryRecvError::Closed) => true,
            Err(TryRecvError::Empty) => false,
        }
    }

    /// Execute one scheduled attempt, temporarily setting
    /// `flow.scheduled_time_millis` on the task's QueryContext so
    /// SQL/TQL `now()` resolves to the logical scheduled time.
    ///
    /// The extension is removed after the attempt so a later manual
    /// `flush_flow` does not reuse a stale scheduled time.
    async fn execute_once_serialized_at_scheduled_time(
        &self,
        engine: &QueryEngineRef,
        frontend_client: &Arc<FrontendClient>,
        scheduled_time_secs: i64,
    ) -> ExecuteOnceOutcome {
        let _execution_guard = self.execution_lock.lock().await;

        struct QueryContextRestoreGuard {
            state: Arc<RwLock<TaskState>>,
            old_ctx: Option<QueryContextRef>,
        }

        impl Drop for QueryContextRestoreGuard {
            fn drop(&mut self) {
                let Some(old_ctx) = self.old_ctx.take() else {
                    return;
                };
                if let Ok(mut state) = self.state.write() {
                    state.query_ctx = old_ctx;
                }
            }
        }

        // Clone the current QueryContext and add the scheduled time
        // extension, then swap it into the task state for this attempt.
        let old_ctx = {
            let mut state = self.state.write().unwrap();
            let old = state.query_ctx.clone();
            let mut new_ctx = (*old).clone();
            new_ctx.set_extension(
                query::options::FLOW_SCHEDULED_TIME_MILLIS,
                (scheduled_time_secs.saturating_mul(1000)).to_string(),
            );
            state.query_ctx = Arc::new(new_ctx);
            old
        };
        let restore_guard = QueryContextRestoreGuard {
            state: self.state.clone(),
            old_ctx: Some(old_ctx),
        };

        let outcome = self
            .execute_once_unlocked(engine, frontend_client, None)
            .await;

        // Restore while still holding `execution_lock` so no future manual
        // flush can observe the temporary scheduled time. The guard also
        // restores during unwind/cancellation.
        drop(restore_guard);

        outcome
    }

    /// Generate the create table SQL
    ///
    /// the auto created table will automatically added a `update_at` Milliseconds DEFAULT now() column in the end
    /// (for compatibility with flow streaming mode)
    ///
    /// and it will use first timestamp column as time index, all other columns will be added as normal columns and nullable
    async fn gen_create_table_expr(
        &self,
        engine: QueryEngineRef,
    ) -> Result<CreateTableExpr, Error> {
        let query_ctx = self.state.read().unwrap().query_ctx.clone();
        let plan =
            sql_to_df_plan(query_ctx.clone(), engine.clone(), &self.config.query, true).await?;
        create_table_with_expr(&plan, &self.config.sink_table_name, &self.config.query_type)
    }

    /// Incremental delta scans are unfiltered by dirty windows; the sequence
    /// range, not a time predicate, defines source correctness.
    fn should_use_unfiltered_incremental_delta(&self) -> bool {
        let state = self.state.read().unwrap();
        state.checkpoint_mode() == CheckpointMode::Incremental
            && !state.is_incremental_disabled()
            && matches!(self.config.query_type, QueryType::Sql)
    }

    /// Generate the next plan and classify its coverage so checkpoint handling
    /// knows whether it is full-query, scoped repair, fenced repair, or delta.
    async fn gen_query_with_time_window(
        &self,
        engine: QueryEngineRef,
        sink_table_schema: &Arc<Schema>,
        primary_key_indices: &[usize],
        allow_partial: bool,
        max_window_cnt: Option<usize>,
    ) -> Result<Option<PlanInfo>, Error> {
        let query_ctx = self.state.read().unwrap().query_ctx.clone();
        let start = SystemTime::now();
        let since_the_epoch = start
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");
        let low_bound = self
            .config
            .expire_after
            .map(|e| since_the_epoch.as_secs() - e as u64)
            .unwrap_or(u64::MIN);

        let low_bound = Timestamp::new_second(low_bound as i64);

        let expire_time_window_bound = self
            .config
            .time_window_expr
            .as_ref()
            .map(|expr| expr.eval(low_bound))
            .transpose()?;

        let (expire_lower_bound, expire_upper_bound) = match (
            expire_time_window_bound,
            &self.config.query_type,
        ) {
            (Some((Some(l), Some(u))), QueryType::Sql) => (l, u),
            (None, QueryType::Sql) if self.config.flow_eval_interval.is_none() => {
                return UnexpectedSnafu {
                    reason: format!(
                        "Flow id={} reached execution without a time-window expression or EVAL INTERVAL; create-flow validation should have rejected it",
                        self.config.flow_id
                    ),
                }
                .fail();
            }
            _ => {
                // Explicit full-query flows (TQL and evaluation-interval SQL
                // plans whose shape cannot be safely dirty-window pruned) are
                // allowed to run as unfiltered full snapshots. This is distinct
                // from using unfiltered full as a fallback after scoped repair or
                // incremental rewrite failed.
                let (_, dirty_windows_to_restore) = self.drain_dirty_windows_signal();

                let plan_info = self
                    .gen_unfiltered_plan_info(
                        engine,
                        query_ctx,
                        sink_table_schema.clone(),
                        primary_key_indices,
                        allow_partial,
                        dirty_windows_to_restore,
                        None,
                        QueryCoverage::UnfilteredFull,
                    )
                    .await?;

                return Ok(Some(plan_info));
            }
        };

        debug!(
            "Flow id = {:?}, found time window: precise_lower_bound={:?}, precise_upper_bound={:?} with dirty time windows: {:?}",
            self.config.flow_id,
            expire_lower_bound,
            expire_upper_bound,
            self.state.read().unwrap().dirty_time_windows
        );
        let window_size = expire_upper_bound
            .sub(&expire_lower_bound)
            .with_context(|| UnexpectedSnafu {
                reason: format!(
                    "Can't get window size from {expire_upper_bound:?} - {expire_lower_bound:?}"
                ),
            })?;
        let col_name = self
            .config
            .time_window_expr
            .as_ref()
            .map(|expr| expr.column_name.clone())
            .with_context(|| UnexpectedSnafu {
                reason: format!(
                    "Flow id={:?}, Failed to get column name from time window expr",
                    self.config.flow_id
                ),
            })?;

        if self.should_use_unfiltered_incremental_delta() {
            // In incremental mode, source correctness is defined by the
            // per-region sequence range `(checkpoint, scan-open snapshot]`, not
            // by dirty-window predicates. Dirty windows are only a scheduling
            // signal here. Applying a stale dirty-window filter to the source can
            // exclude rows that are inside the returned watermark and make a
            // checkpoint advance skip them forever. The sink side is also left
            // unfiltered by dirty windows; the incremental rewrite joins the
            // delta groups with the full sink state for correctness. Future
            // dynamic filters can prune sink reads as a pure optimization.
            let retention_filter = self
                .config
                .expire_after
                .map(|_| (col_name.as_str(), expire_lower_bound, "incremental"));
            return self
                .gen_unfiltered_plan_info_if_dirty(
                    engine,
                    query_ctx,
                    sink_table_schema.clone(),
                    primary_key_indices,
                    allow_partial,
                    retention_filter,
                    QueryCoverage::IncrementalDelta,
                )
                .await;
        }

        let (expr, coverage) = {
            let mut state = self.state.write().unwrap();
            let window_cnt = max_window_cnt
                .unwrap_or(self.config.batch_opts.experimental_max_filter_num_per_query);
            let expr = state.gen_scoped_filter_exprs(
                &col_name,
                Some(expire_lower_bound),
                window_size,
                window_cnt,
                self.config.flow_id,
                Some(self),
            )?;
            let repair_high = state
                .pending_fenced_repair()
                .map(|repair| repair.high().clone());
            let coverage = if let Some(high) = repair_high {
                QueryCoverage::FencedRepairChunk { high }
            } else {
                QueryCoverage::ScopedBaseRepair
            };
            (expr, coverage)
        };

        let Some(expr) = expr else {
            // no new data, hence no need to update
            debug!("Flow id={:?}, no new data, not update", self.config.flow_id);
            return Ok(None);
        };

        let filter_sql = expr_to_sql(&expr.expr)
            .map(|sql| sql.to_string())
            .unwrap_or_else(|err| format!("<failed to format filter expr: {err}>"));

        debug!(
            "Flow id={:?}, Generated filter expr: {:?}",
            self.config.flow_id, filter_sql
        );

        let mut add_filter = AddFilterRewriter::new(expr.expr.clone());
        let mut add_auto_column = ColumnMatcherRewriter::new(
            sink_table_schema.clone(),
            primary_key_indices.to_vec(),
            allow_partial,
        );

        let plan = self.restore_scoped_dirty_windows_on_err(
            &expr,
            sql_to_df_plan(query_ctx.clone(), engine.clone(), &self.config.query, false).await,
        )?;
        let rewrite = self.restore_scoped_dirty_windows_on_err(
            &expr,
            plan.clone()
                .rewrite(&mut add_filter)
                .and_then(|p| p.data.rewrite(&mut add_auto_column))
                .with_context(|_| DatafusionSnafu {
                    context: format!("Failed to rewrite plan:\n {}\n", plan),
                })
                .map(|rewrite| rewrite.data),
        )?;
        // only apply optimize after complex rewrite is done
        let new_plan = self.restore_scoped_dirty_windows_on_err(
            &expr,
            apply_df_optimizer(rewrite, &query_ctx).await,
        )?;

        let info = PlanInfo {
            plan: new_plan.clone(),
            dirty_restore: DirtyRestore::Scoped(expr),
            coverage,
        };

        Ok(Some(info))
    }

    /// Stamp the current cycle epoch onto every emitted sink state row when
    /// checkpoint persistence is active. Returns the stamped plan and the epoch
    /// used for this cycle (`None` when persistence is inactive or the plan is
    /// not a DML insert).
    ///
    /// Rows stamped with an epoch newer than the last durable record
    /// invalidate that record on restart (crash between state write and
    /// checkpoint write), which is exactly the backfill trigger we want.
    async fn stamp_epoch_into_plan(
        &self,
        plan: LogicalPlan,
    ) -> Result<(LogicalPlan, Option<u64>), Error> {
        let persistence = self.state.read().unwrap().checkpoint_persistence().cloned();
        let Some(persistence) = persistence else {
            return Ok((plan, None));
        };
        let LogicalPlan::Dml(dml) = &plan else {
            return Ok((plan, None));
        };
        let epoch = self.state.read().unwrap().next_persist_epoch();
        let inner = dml.input.as_ref().clone();
        let mut exprs = inner
            .schema()
            .fields()
            .iter()
            .map(|field| Expr::Column(Column::new_unqualified(field.name())))
            .collect::<Vec<_>>();
        exprs.push(lit(ScalarValue::UInt64(Some(epoch))).alias(&persistence.epoch_col_name));
        let stamped = LogicalPlanBuilder::from(inner)
            .project(exprs)
            .with_context(|_| DatafusionSnafu {
                context: "Failed to stamp flow epoch column onto state rows".to_string(),
            })?
            .build()
            .with_context(|_| DatafusionSnafu {
                context: "Failed to build epoch-stamped state row plan".to_string(),
            })?;
        let stamped = LogicalPlan::Dml(DmlStatement::new(
            dml.table_name.clone(),
            dml.target.clone(),
            dml.op.clone(),
            Arc::new(stamped),
        ));
        Ok((stamped, Some(epoch)))
    }

    /// Upsert the singleton checkpoint row through the frontend write
    /// machinery: one row whose window/time-index column is the sentinel, epoch
    /// column is the cycle epoch, and the BINARY state column holds the encoded
    /// versioned checkpoint record. Other sink columns are left to defaults.
    async fn write_checkpoint_row(
        &self,
        frontend_client: &Arc<FrontendClient>,
        epoch: u64,
        checkpoints: &BTreeMap<u64, u64>,
    ) -> Result<(), Error> {
        let persistence = self
            .state
            .read()
            .unwrap()
            .checkpoint_persistence()
            .cloned()
            .with_context(|| UnexpectedSnafu {
                reason: "checkpoint persistence is not active".to_string(),
            })?;
        let record = CheckpointRecord {
            format_version: CHECKPOINT_RECORD_FORMAT_VERSION,
            epoch,
            checkpoints: checkpoints.clone(),
        };
        let encoded = encode_checkpoint_record(&record)?;

        let (table, _) = get_table_info_df_schema(
            self.config.catalog_manager.clone(),
            self.config.sink_table_name.clone(),
        )
        .await?;
        let sink_schema = table.table_info().meta.schema.clone();
        let window_col = sink_schema
            .column_schema_by_name(&persistence.window_col_name)
            .with_context(|| UnexpectedSnafu {
                reason: format!(
                    "Sink table lost checkpoint window column {}",
                    persistence.window_col_name
                ),
            })?;

        let mut exprs = vec![
            lit(checkpoint_sentinel_scalar(window_col)?).alias(&persistence.window_col_name),
            lit(ScalarValue::UInt64(Some(epoch))).alias(&persistence.epoch_col_name),
            lit(ScalarValue::Binary(Some(encoded))).alias(&persistence.state_col_name),
        ];
        // Keep the auto-created update_at column fresh on the checkpoint row
        // when the sink has one; any other sink column is filled with its
        // default (or NULL) by the insert machinery.
        if let Some(update_at) = sink_schema.column_schema_by_name(AUTO_CREATED_UPDATE_AT_TS_COL)
            && update_at.data_type.is_timestamp()
        {
            exprs.push(datafusion::prelude::now().alias(AUTO_CREATED_UPDATE_AT_TS_COL));
        }
        let empty = LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: true,
            schema: Arc::new(DFSchema::empty()),
        });
        let row_plan = LogicalPlanBuilder::from(empty)
            .project(exprs)
            .with_context(|_| DatafusionSnafu {
                context: "Failed to build checkpoint row plan".to_string(),
            })?
            .build()
            .with_context(|_| DatafusionSnafu {
                context: "Failed to finalize checkpoint row plan".to_string(),
            })?;

        let insert_to = TableName {
            catalog_name: self.config.sink_table_name[0].clone(),
            schema_name: self.config.sink_table_name[1].clone(),
            table_name: self.config.sink_table_name[2].clone(),
        };
        let req = encode_insert_plan_request(insert_to, &row_plan)?;
        let catalog = &self.config.sink_table_name[0];
        let schema = &self.config.sink_table_name[1];
        let mut peer_desc = None;
        frontend_client
            .query_with_terminal_metrics(catalog, schema, req, &[], &HashMap::new(), &mut peer_desc)
            .await?;
        debug!(
            "Flow {} persisted checkpoint row with epoch {} and {} regions",
            self.config.flow_id,
            epoch,
            checkpoints.len()
        );
        Ok(())
    }

    /// Detect whether checkpoint persistence applies to this task and, if so,
    /// restore the durable checkpoint from the sink table before the first run.
    ///
    /// Detection/load errors are warnings: the task starts from full snapshot
    /// and later cycles rebuild the checkpoint from scratch.
    pub(crate) async fn try_enable_checkpoint_persistence(
        &self,
        frontend_client: &Arc<FrontendClient>,
    ) {
        let persistence = match self.detect_checkpoint_persistence().await {
            Ok(persistence) => persistence,
            Err(err) => {
                warn!(
                    "Flow {} failed to detect checkpoint persistence, starting from full snapshot: {err:?}",
                    self.config.flow_id
                );
                return;
            }
        };
        let restored = match &persistence {
            Some(persistence) => {
                self.state
                    .write()
                    .unwrap()
                    .set_checkpoint_persistence(Some(persistence.clone()));
                match self
                    .read_checkpoint_state(frontend_client, persistence)
                    .await
                {
                    Ok(restored) => restored,
                    Err(err) => {
                        warn!(
                            "Flow {} failed to load checkpoint state, starting from full snapshot: {err:?}",
                            self.config.flow_id
                        );
                        None
                    }
                }
            }
            None => None,
        };
        let mut state = self.state.write().unwrap();
        if let Some((epoch, checkpoints)) = restored {
            state.seed_checkpoints_from_record(epoch, checkpoints);
            info!(
                "Flow {} restored incremental checkpoints from sink table at epoch {}",
                self.config.flow_id, epoch
            );
        } else {
            info!(
                "Flow {} found no trustworthy checkpoint record; starting from full snapshot",
                self.config.flow_id
            );
        }
    }

    /// Resolve the checkpoint persistence layout. Activated only when the
    /// batching mode is `SequenceRange` and the sink schema contains the
    /// reserved internal epoch column plus exactly one BINARY state column.
    async fn detect_checkpoint_persistence(&self) -> Result<Option<CheckpointPersistence>, Error> {
        if self.config.batch_opts.incremental_mode != IncrementalMode::SequenceRange {
            return Ok(None);
        }
        let (table, _) = get_table_info_df_schema(
            self.config.catalog_manager.clone(),
            self.config.sink_table_name.clone(),
        )
        .await?;
        let schema = table.table_info().meta.schema.clone();
        let epoch_col = match schema.column_schema_by_name(INTERNAL_FLOW_EPOCH_COL_NAME) {
            Some(column) => column,
            None => return Ok(None),
        };
        if !is_integer_type(&epoch_col.data_type) {
            debug!(
                "Flow {} checkpoint epoch column {} has non-integer type {:?}; persistence inactive",
                self.config.flow_id, epoch_col.name, epoch_col.data_type
            );
            return Ok(None);
        }
        let state_cols = schema
            .column_schemas()
            .iter()
            .filter(|column| column.data_type == ConcreteDataType::binary_datatype())
            .collect::<Vec<_>>();
        if state_cols.len() != 1 {
            return Ok(None);
        }
        let Some(ts_idx) = schema.timestamp_index() else {
            return Ok(None);
        };
        let window_col = &schema.column_schemas()[ts_idx];
        Ok(Some(CheckpointPersistence {
            epoch_col_name: INTERNAL_FLOW_EPOCH_COL_NAME.to_string(),
            state_col_name: state_cols[0].name.clone(),
            window_col_name: window_col.name.clone(),
        }))
    }

    /// Read the singleton sentinel checkpoint row and the maximum non-sentinel
    /// epoch from the sink table and return a trusted record.
    ///
    /// The reads are executed through the frontend client (as `Query::LogicalPlan`
    /// requests) rather than the local query engine, so the sink table is
    /// resolved from DistTable-backed catalog metadata on the frontend.
    ///
    /// Trust requires exactly one sentinel row, a decodable v1 record with a
    /// non-empty checkpoint map, and `max_non_sentinel_epoch <= record.epoch`.
    /// NULL epoch rows (pre-upgrade data) make the state untrusted unless
    /// there are no state rows at all.
    async fn read_checkpoint_state(
        &self,
        frontend_client: &Arc<FrontendClient>,
        persistence: &CheckpointPersistence,
    ) -> Result<Option<(u64, BTreeMap<u64, u64>)>, Error> {
        let (table, df_schema) = get_table_info_df_schema(
            self.config.catalog_manager.clone(),
            self.config.sink_table_name.clone(),
        )
        .await?;
        let sink_schema = table.table_info().meta.schema.clone();
        let window_col = sink_schema
            .column_schema_by_name(&persistence.window_col_name)
            .with_context(|| UnexpectedSnafu {
                reason: format!(
                    "Sink table lost checkpoint window column {}",
                    persistence.window_col_name
                ),
            })?;
        let sentinel = checkpoint_sentinel_scalar(window_col)?;
        // The sentinel query projects the window column too, because the
        // sentinel filter is applied as a logical Filter node on top of the
        // scan (the local test engine's MemTable does not apply scan filters).
        let sentinel_plan = sink_scan_plan(
            &self.config.sink_table_name,
            table.clone(),
            &df_schema,
            &[
                persistence.window_col_name.clone(),
                persistence.epoch_col_name.clone(),
                persistence.state_col_name.clone(),
            ],
        )?;
        let sentinel_plan = LogicalPlanBuilder::from(sentinel_plan)
            .filter(col(&persistence.window_col_name).eq(lit(sentinel.clone())))
            .with_context(|_| DatafusionSnafu {
                context: "Failed to build checkpoint sentinel row scan".to_string(),
            })?
            .build()
            .with_context(|_| DatafusionSnafu {
                context: "Failed to finalize checkpoint sentinel row scan".to_string(),
            })?;
        let sentinel_batches = self
            .execute_sink_scan(frontend_client, sentinel_plan)
            .await?;
        let mut sentinel_states = Vec::new();
        for batch in sentinel_batches.iter() {
            let state_vector =
                Helper::try_into_vector(batch.column(2)).with_context(|_| DatatypesSnafu {
                    extra: "Failed to convert sentinel row state column".to_string(),
                })?;
            for row in 0..batch.num_rows() {
                let state_bytes = state_vector
                    .get_ref(row)
                    .try_into_binary()
                    .with_context(|_| DatatypesSnafu {
                        extra: "Failed to convert sentinel row state".to_string(),
                    })?
                    .map(|bytes| bytes.to_vec());
                sentinel_states.push(state_bytes);
            }
        }
        if sentinel_states.len() > 1 {
            debug!(
                "Flow {} found {} sentinel checkpoint rows, untrusted",
                self.config.flow_id,
                sentinel_states.len()
            );
            return Ok(None);
        }

        // Maximum non-sentinel epoch plus row counts to detect NULL epochs.
        let non_sentinel_predicate = col(&persistence.window_col_name)
            .is_null()
            .or(col(&persistence.window_col_name).not_eq(lit(sentinel)));
        let epoch_col_name = persistence.epoch_col_name.clone();
        let scan = sink_scan_plan(
            &self.config.sink_table_name,
            table,
            &df_schema,
            &[persistence.window_col_name.clone(), epoch_col_name.clone()],
        )?;
        let agg_plan = LogicalPlanBuilder::from(scan)
            .filter(non_sentinel_predicate)
            .with_context(|_| DatafusionSnafu {
                context: "Failed to build non-sentinel epoch aggregation".to_string(),
            })?
            .aggregate(
                Vec::<Expr>::new(),
                vec![
                    count(col(&epoch_col_name)).alias("non_null_epoch_cnt"),
                    count(lit(1_i64)).alias("total_cnt"),
                    max(col(&epoch_col_name)).alias("max_epoch"),
                ],
            )
            .with_context(|_| DatafusionSnafu {
                context: "Failed to aggregate non-sentinel epochs".to_string(),
            })?
            .build()
            .with_context(|_| DatafusionSnafu {
                context: "Failed to finalize non-sentinel epoch aggregation".to_string(),
            })?;
        let agg_batches = self.execute_sink_scan(frontend_client, agg_plan).await?;
        let (total_cnt, non_null_cnt, max_epoch) = match agg_batches.iter().next() {
            Some(batch) => {
                let non_null_cnt_vector =
                    Helper::try_into_vector(batch.column(0)).with_context(|_| DatatypesSnafu {
                        extra: "Failed to convert non-null epoch count column".to_string(),
                    })?;
                let total_cnt_vector =
                    Helper::try_into_vector(batch.column(1)).with_context(|_| DatatypesSnafu {
                        extra: "Failed to convert state row count column".to_string(),
                    })?;
                let max_epoch_vector =
                    Helper::try_into_vector(batch.column(2)).with_context(|_| DatatypesSnafu {
                        extra: "Failed to convert max epoch column".to_string(),
                    })?;
                let non_null_cnt = non_null_cnt_vector
                    .get_ref(0)
                    .try_into_i64()
                    .with_context(|_| DatatypesSnafu {
                        extra: "Failed to convert non-null epoch count".to_string(),
                    })?
                    .unwrap_or(0);
                let total_cnt = total_cnt_vector
                    .get_ref(0)
                    .try_into_i64()
                    .with_context(|_| DatatypesSnafu {
                        extra: "Failed to convert state row count".to_string(),
                    })?
                    .unwrap_or(0);
                let max_epoch = value_as_u64(Value::from(max_epoch_vector.get_ref(0)));
                (total_cnt, non_null_cnt, max_epoch)
            }
            None => (0, 0, None),
        };

        // NULL epoch rows (pre-upgrade state data) are untrusted unless there
        // is no state data at all.
        if total_cnt > 0 && non_null_cnt != total_cnt {
            debug!(
                "Flow {} has {} state rows with NULL epochs, checkpoint untrusted",
                self.config.flow_id,
                total_cnt - non_null_cnt
            );
            return Ok(None);
        }

        let [state_bytes] = sentinel_states.as_slice() else {
            return Ok(None);
        };
        let Some(state_bytes) = state_bytes.as_ref() else {
            debug!(
                "Flow {} sentinel row has NULL state, untrusted",
                self.config.flow_id
            );
            return Ok(None);
        };
        let Some(record) = decode_checkpoint_record(state_bytes)? else {
            debug!(
                "Flow {} sentinel row is not a decodable v1 record, untrusted",
                self.config.flow_id
            );
            return Ok(None);
        };
        if record.checkpoints.is_empty() {
            debug!(
                "Flow {} checkpoint record has an empty map, untrusted",
                self.config.flow_id
            );
            return Ok(None);
        }
        if let Some(max_epoch) = max_epoch
            && max_epoch > record.epoch
        {
            debug!(
                "Flow {} state rows newer than checkpoint record ({} > {}), untrusted",
                self.config.flow_id, max_epoch, record.epoch
            );
            return Ok(None);
        }
        Ok(Some((record.epoch, record.checkpoints)))
    }

    /// Execute a sink scan plan through the frontend client and collect the
    /// returned record batches.
    ///
    /// The plan is transported as a `Query::LogicalPlan` request (not executed
    /// on the local query engine) so restore works with DistTable-backed
    /// catalog metadata: the frontend resolves the sink table and executes.
    async fn execute_sink_scan(
        &self,
        frontend_client: &Arc<FrontendClient>,
        plan: LogicalPlan,
    ) -> Result<RecordBatches, Error> {
        let message = DFLogicalSubstraitConvertor {}
            .encode(&plan, DefaultSerializer)
            .context(SubstraitEncodeLogicalPlanSnafu)?;
        let req = api::v1::QueryRequest {
            query: Some(api::v1::query_request::Query::LogicalPlan(message.to_vec())),
        };
        let catalog = &self.config.sink_table_name[0];
        let schema = &self.config.sink_table_name[1];
        let mut peer_desc = None;
        let output = frontend_client
            .query_with_terminal_metrics(catalog, schema, req, &[], &HashMap::new(), &mut peer_desc)
            .await?;
        let batches = match output.output.data {
            common_query::OutputData::RecordBatches(batches) => batches,
            common_query::OutputData::Stream(stream) => collect_batches(stream)
                .await
                .map_err(BoxedError::new)
                .context(ExternalSnafu)?,
            common_query::OutputData::AffectedRows(_) => {
                return UnexpectedSnafu {
                    reason: "Unexpected affected-rows output from sink scan".to_string(),
                }
                .fail();
            }
        };
        Ok(batches)
    }
}

/// Build a table scan over the sink table with the given column projection.
fn sink_scan_plan(
    sink_table_name: &[String; 3],
    table: table::TableRef,
    df_schema: &DFSchema,
    projection: &[String],
) -> Result<LogicalPlan, Error> {
    let table_ref = TableReference::Full {
        catalog: sink_table_name[0].clone().into(),
        schema: sink_table_name[1].clone().into(),
        table: sink_table_name[2].clone().into(),
    };
    let table_provider = Arc::new(DfTableProviderAdapter::new(table));
    let table_source = Arc::new(DefaultTableSource::new(table_provider));
    let projection = projection
        .iter()
        .map(|name| {
            df_schema
                .index_of_column(&Column::from_name(name.clone()))
                .with_context(|_| DatafusionSnafu {
                    context: format!("Failed to resolve sink column {name} for checkpoint scan"),
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let scan = TableScan::try_new(table_ref, table_source, Some(projection), vec![], None)
        .with_context(|_| DatafusionSnafu {
            context: "Failed to build sink scan for checkpoint".to_string(),
        })?;
    Ok(LogicalPlan::TableScan(scan))
}

/// Extracts a `u64` from an integer-typed value (signed or unsigned).
fn value_as_u64(value: Value) -> Option<u64> {
    value
        .as_u64()
        .or_else(|| value.as_i64().map(|value| value as u64))
}

#[cfg(test)]
mod test;
