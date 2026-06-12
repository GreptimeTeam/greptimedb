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
use common_telemetry::tracing::warn;
use common_telemetry::{debug, info};
use common_time::Timestamp;
use datafusion::datasource::DefaultTableSource;
use datafusion::sql::unparser::expr_to_sql;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::utils::quote_identifier;
use datafusion_common::{DFSchemaRef, TableReference};
use datafusion_expr::{DmlStatement, LogicalPlan, WriteOp, col, lit};
use datatypes::schema::Schema;
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

use crate::batching_mode::BatchingModeOptions;
use crate::batching_mode::checkpoint::{
    FlowCheckpointDecision, FlowQueryFallbackReason, checkpoint_mode_label,
};
use crate::batching_mode::frontend_client::{FrontendClient, PeerDesc};
use crate::batching_mode::state::{
    CheckpointMode, DirtyTimeWindows, FilterExprInfo, TaskState, to_df_literal,
};
use crate::batching_mode::table_creator::{QueryType, create_table_with_expr};
use crate::batching_mode::time_window::TimeWindowExpr;
use crate::batching_mode::utils::{
    AddFilterRewriter, ColumnMatcherRewriter, df_plan_to_sql, gen_plan_with_matching_schema,
    get_table_info_df_schema, sql_to_df_plan,
};
use crate::df_optimizer::apply_df_optimizer;
use crate::error::{
    DatafusionSnafu, ExternalSnafu, InvalidQuerySnafu, SubstraitEncodeLogicalPlanSnafu,
    UnexpectedSnafu,
};
use crate::metrics::{
    METRIC_FLOW_BATCHING_ENGINE_ERROR_CNT, METRIC_FLOW_BATCHING_ENGINE_QUERY_TIME,
    METRIC_FLOW_BATCHING_ENGINE_SLOW_QUERY, METRIC_FLOW_BATCHING_ENGINE_START_QUERY_CNT,
    METRIC_FLOW_ROWS,
};
use crate::{Error, FlowId};

mod ckpt;
mod inc;

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
            }),
            state: Arc::new(RwLock::new(state)),
            execution_lock: Arc::new(Mutex::new(())),
        })
    }

    pub fn last_execution_time_millis(&self) -> Option<i64> {
        self.state.read().unwrap().last_execution_time_millis()
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
    /// This is a dry-run of the same schema matching logic used by runtime insert-plan
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

        gen_plan_with_matching_schema(
            &self.config.query,
            query_ctx,
            engine.clone(),
            table_meta.schema.clone(),
            &primary_key_indices,
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

        let new_query = self
            .gen_query_with_time_window(
                engine.clone(),
                &table.table_info().meta.schema,
                &primary_key_indices,
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
            warn!(
                "Flow {flow_id} skipped unsafe incremental delta fallback; \
                 restored dirty signal instead of executing an unfiltered full snapshot"
            );
            self.restore_dirty_windows(dirty_restore);
            return Ok(None);
        }
        let plan = incremental_plan.unwrap_or_else(|| plan.clone());

        let extensions = self
            .build_flow_query_extensions(incremental_safe, coverage.is_incremental_delta())
            .await?;
        let extension_refs = extensions
            .iter()
            .map(|(key, value)| (*key, value.as_str()))
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
                            warn!(
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
        let decision = {
            let mut state = self.state.write().unwrap();
            Self::apply_query_result_to_state(&mut state, &res, elapsed, coverage, dirty_restore)
        };
        Self::record_checkpoint_decision(flow_id, decision);
        self.restore_unscoped_dirty_signal_after_successful_incremental_fallback(
            dirty_restore,
            decision,
        );

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

    /// If an incremental query executed successfully but failed to prove a safe
    /// checkpoint advancement, the task switches back to full snapshot mode. The
    /// unscoped incremental path has already consumed the dirty-window signal,
    /// so restore that signal to make the next full snapshot actually run.
    fn restore_unscoped_dirty_signal_after_successful_incremental_fallback(
        &self,
        dirty_restore: &DirtyRestore,
        decision: FlowCheckpointDecision,
    ) {
        if !matches!(
            decision,
            FlowCheckpointDecision::FallbackToFullSnapshot {
                previous_mode: CheckpointMode::Incremental,
                reason: FlowQueryFallbackReason::MissingRegionWatermark
                    | FlowQueryFallbackReason::IncompleteRegionWatermark,
                ..
            }
        ) {
            return;
        }

        if let DirtyRestore::Unscoped(dirty_windows) = dirty_restore {
            self.restore_unscoped_dirty_windows(dirty_windows);
        }
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
    /// any error will be logged when executing query
    pub async fn start_executing_loop(
        &self,
        engine: QueryEngineRef,
        frontend_client: Arc<FrontendClient>,
    ) {
        let flow_id_str = self.config.flow_id.to_string();
        let mut max_window_cnt = None;
        let mut interval = self
            .config
            .flow_eval_interval
            .map(|d| tokio::time::interval(d));
        if let Some(tick) = &mut interval {
            tick.tick().await; // pass the first tick immediately
        }
        loop {
            // first check if shutdown signal is received
            // if so, break the loop
            {
                let mut state = self.state.write().unwrap();
                match state.shutdown_rx.try_recv() {
                    Ok(()) => break,
                    Err(TryRecvError::Closed) => {
                        warn!(
                            "Unexpected shutdown flow {}, shutdown anyway",
                            self.config.flow_id
                        );
                        break;
                    }
                    Err(TryRecvError::Empty) => (),
                }
            }
            METRIC_FLOW_BATCHING_ENGINE_START_QUERY_CNT
                .with_label_values(&[&flow_id_str])
                .inc();

            let min_refresh = self.config.batch_opts.experimental_min_refresh_duration;

            let outcome = self
                .execute_once_serialized_with_outcome(&engine, &frontend_client, max_window_cnt)
                .await;

            match outcome.result {
                // normal execute, sleep for some time before doing next query
                Ok(Some(_)) => {
                    // can increase max_window_cnt to query more windows next time
                    max_window_cnt = max_window_cnt.map(|cnt| {
                        (cnt + 1).min(self.config.batch_opts.experimental_max_filter_num_per_query)
                    });

                    // here use proper ticking if set eval interval
                    if let Some(eval_interval) = &mut interval {
                        eval_interval.tick().await;
                    } else {
                        // if not explicitly set, just automatically calculate next start time
                        // using time window size and more args
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
                    };
                }
                // no new data, sleep for some time before checking for new data
                Ok(None) => {
                    debug!(
                        "Flow id = {:?} found no new data, sleep for {:?} then continue",
                        self.config.flow_id, min_refresh
                    );
                    tokio::time::sleep(min_refresh).await;
                    continue;
                }
                // TODO(discord9): this error should have better place to go, but for now just print error, also more context is needed
                Err(err) => {
                    METRIC_FLOW_BATCHING_ENGINE_ERROR_CNT
                        .with_label_values(&[&flow_id_str])
                        .inc();
                    match outcome.new_query {
                        Some(query) => {
                            common_telemetry::error!(err; "Failed to execute query for flow={} with query: {}", self.config.flow_id, query.plan);
                            // TODO(discord9): add some backoff here? half the query time window or what
                            // backoff meaning use smaller `max_window_cnt` for next query

                            // since last query failed, we should not try to query too many windows
                            max_window_cnt = Some(1);
                        }
                        None => {
                            common_telemetry::error!(err; "Failed to generate query for flow={}", self.config.flow_id)
                        }
                    }
                    // also sleep for a little while before try again to prevent flooding logs
                    tokio::time::sleep(min_refresh).await;
                }
            }
        }
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
                        "Flow id={} reached runtime without a time-window expression or EVAL INTERVAL; create-flow validation should have rejected it",
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
}

#[cfg(test)]
mod test;
