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

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use api::v1::CreateTableExpr;
use catalog::CatalogManagerRef;
use client::OutputWithMetrics;
use common_error::ext::BoxedError;
use common_query::logical_plan::breakup_insert_plan;
use common_telemetry::tracing::warn;
use common_telemetry::{debug, info};
use common_time::Timestamp;
use datafusion::datasource::DefaultTableSource;
use datafusion::sql::unparser::expr_to_sql;
use datafusion_common::DFSchemaRef;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_expr::{DmlStatement, LogicalPlan, WriteOp};
use datatypes::schema::Schema;
use query::QueryEngineRef;
use query::options::{
    FLOW_INCREMENTAL_AFTER_SEQS, FLOW_INCREMENTAL_MODE, FLOW_INCREMENTAL_MODE_MEMTABLE_ONLY,
    FLOW_SINK_TABLE_ID,
};
use query::query_engine::DefaultSerializer;
use session::context::QueryContextRef;
use snafu::{OptionExt, ResultExt, ensure};
use sql::parsers::utils::is_tql;
use store_api::mito_engine_options::MERGE_MODE_KEY;
use substrait::{DFLogicalSubstraitConvertor, SubstraitPlan};
use table::table::adapter::DfTableProviderAdapter;
use tokio::sync::oneshot;
use tokio::sync::oneshot::error::TryRecvError;
use tokio::time::Instant;

use crate::batching_mode::BatchingModeOptions;
use crate::batching_mode::checkpoint::{
    FlowCheckpointDecision, FlowQueryFallbackReason, checkpoint_mode_label,
};
use crate::batching_mode::frontend_client::FrontendClient;
use crate::batching_mode::incremental_filter::build_sink_dirty_time_window_filter_expr;
use crate::batching_mode::state::{CheckpointMode, FilterExprInfo, TaskState};
use crate::batching_mode::table_creator::{QueryType, create_table_with_expr};
use crate::batching_mode::time_window::TimeWindowExpr;
use crate::batching_mode::utils::{
    AddFilterRewriter, ColumnMatcherRewriter, analyze_incremental_aggregate_plan,
    gen_plan_with_matching_schema, get_table_info_df_schema,
    rewrite_incremental_aggregate_with_sink_merge, sql_to_df_plan,
};
use crate::df_optimizer::apply_df_optimizer;
use crate::error::{
    DatafusionSnafu, ExternalSnafu, InvalidQuerySnafu, SubstraitEncodeLogicalPlanSnafu,
    UnexpectedSnafu,
};
use crate::metrics::{
    METRIC_FLOW_BATCHING_ENGINE_CHECKPOINT_DECISION_CNT, METRIC_FLOW_BATCHING_ENGINE_ERROR_CNT,
    METRIC_FLOW_BATCHING_ENGINE_QUERY_MODE_CNT, METRIC_FLOW_BATCHING_ENGINE_QUERY_TIME,
    METRIC_FLOW_BATCHING_ENGINE_SLOW_QUERY, METRIC_FLOW_BATCHING_ENGINE_START_QUERY_CNT,
    METRIC_FLOW_ROWS,
};
use crate::{Error, FlowId};

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

#[derive(Clone)]
pub struct BatchingTask {
    pub config: Arc<TaskConfig>,
    pub state: Arc<RwLock<TaskState>>,
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
    pub filter: Option<FilterExprInfo>,
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
            state: Arc::new(RwLock::new(TaskState::new(query_ctx, shutdown_rx))),
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
    ) -> Result<Option<(u32, Duration)>, Error> {
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

    async fn is_table_exist(&self, table_name: &[String; 3]) -> Result<bool, Error> {
        self.config
            .catalog_manager
            .table_exists(&table_name[0], &table_name[1], &table_name[2], None)
            .await
            .map_err(BoxedError::new)
            .context(ExternalSnafu)
    }

    pub async fn gen_exec_once(
        &self,
        engine: &QueryEngineRef,
        frontend_client: &Arc<FrontendClient>,
        max_window_cnt: Option<usize>,
    ) -> Result<Option<(u32, Duration)>, Error> {
        if let Some(new_query) = self.gen_insert_plan(engine, max_window_cnt).await? {
            debug!("Generate new query: {}", new_query.plan);
            match self
                .execute_logical_plan(frontend_client, &new_query.plan, new_query.filter.as_ref())
                .await
            {
                Ok(result) => Ok(result),
                Err(err) => {
                    self.handle_executed_query_failure(&err, Some(&new_query));
                    Err(err)
                }
            }
        } else {
            debug!("Generate no query");
            Ok(None)
        }
    }

    pub async fn gen_insert_plan(
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

        let insert_into_info = if let Some(new_query) = new_query {
            // first check if all columns in input query exists in sink table
            // since insert into ref to names in record batch generate by given query
            let table_columns = df_schema
                .columns()
                .into_iter()
                .map(|c| c.name)
                .collect::<BTreeSet<_>>();
            for column in new_query.plan.schema().columns() {
                ensure!(
                    table_columns.contains(column.name()),
                    InvalidQuerySnafu {
                        reason: format!(
                            "Column {} not found in sink table with columns {:?}",
                            column, table_columns
                        ),
                    }
                );
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
                Arc::new(new_query.plan),
            ));
            PlanInfo {
                plan,
                filter: new_query.filter,
            }
        } else {
            return Ok(None);
        };
        let insert_into = insert_into_info
            .plan
            .recompute_schema()
            .context(DatafusionSnafu {
                context: "Failed to recompute schema",
            })?;

        Ok(Some(PlanInfo {
            plan: insert_into,
            filter: insert_into_info.filter,
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

    /// For incremental-mode SQL queries, attempt to rewrite the delta aggregate
    /// plan into a safe delta-LEFT-JOIN-sink merge plan. Returns `(plan, true)`
    /// if the rewrite succeeded and incremental extensions are safe;
    /// `(plan, false)` if incremental is not applicable (non-SQL, non-aggregate,
    /// or unsupported aggregates).
    async fn prepare_plan_for_incremental(
        &self,
        plan: &LogicalPlan,
        dirty_filter: Option<&FilterExprInfo>,
    ) -> Result<(LogicalPlan, bool), Error> {
        let is_incremental_sql = {
            let state = self.state.read().unwrap();
            if state.is_incremental_disabled() {
                return Ok((plan.clone(), false));
            }
            state.checkpoint_mode() == CheckpointMode::Incremental
                && matches!(self.config.query_type, QueryType::Sql)
        };

        if !is_incremental_sql {
            return Ok((plan.clone(), false));
        }

        // Extract inner query plan from the DML wrapper.
        // Non-DML or non-SQL plans bypass the rewrite and keep checkpoint mode;
        // non-aggregate TQL or non-INSERT plans do not need incremental scan extensions.
        let inner_plan = match plan {
            LogicalPlan::Dml(dml) => dml.input.as_ref().clone(),
            _ => return Ok((plan.clone(), false)),
        };

        // Analyze the plan for incremental rewritability.
        // PR3 only enables incremental reads for aggregate / group-by plans via
        // the PR4 delta-left-join rewrite. Non-aggregate SQL (projection, filter,
        // or other non-aggregate shapes) stays full-snapshot until separately
        // supported, and incremental mode is permanently disabled for this flow.
        let Some(analysis) = analyze_incremental_aggregate_plan(&inner_plan)? else {
            warn!(
                "Flow {} incremental mode but plan is not an aggregate query; \
                 permanently disabling incremental for this flow",
                self.config.flow_id
            );
            self.state.write().unwrap().disable_incremental();
            return Ok((plan.clone(), false));
        };

        if !analysis.unsupported_exprs.is_empty() {
            warn!(
                "Flow {} incremental aggregate contains unsupported expressions {:?}; \
                 permanently disabling incremental for this flow",
                self.config.flow_id, analysis.unsupported_exprs
            );
            self.state.write().unwrap().disable_incremental();
            return Ok((plan.clone(), false));
        }

        // Plain GROUP BY without aggregate expressions has no values to
        // merge between delta and sink. The incremental delta scan emits
        // changed groups, and sink primary-key write semantics make this
        // idempotent; no explicit left-join rewrite is needed.
        if analysis.merge_columns.is_empty() {
            return Ok((plan.clone(), true));
        }

        // Fetch sink table for the merge rewrite
        let (sink_table, _) = get_table_info_df_schema(
            self.config.catalog_manager.clone(),
            self.config.sink_table_name.clone(),
        )
        .await?;
        let sink_schema = sink_table.table_info().meta.schema.clone();
        let sink_dirty_filter = build_sink_dirty_time_window_filter_expr(
            self.config.flow_id,
            &analysis,
            &sink_schema,
            dirty_filter,
        )?;

        let rewritten_inner = match rewrite_incremental_aggregate_with_sink_merge(
            &inner_plan,
            &analysis,
            sink_table,
            &self.config.sink_table_name,
            sink_dirty_filter,
        )
        .await
        {
            Ok(plan) => plan,
            Err(err) => {
                warn!(
                    "Flow {} failed to rewrite incremental aggregate with sink merge; \
                     permanently disabling incremental for this flow: {:?}",
                    self.config.flow_id, err
                );
                self.state.write().unwrap().disable_incremental();
                return Ok((plan.clone(), false));
            }
        };

        // Reconstruct DML plan with the rewritten inner plan
        let rewritten = match plan {
            LogicalPlan::Dml(dml) => LogicalPlan::Dml(DmlStatement::new(
                dml.table_name.clone(),
                dml.target.clone(),
                dml.op.clone(),
                Arc::new(rewritten_inner),
            )),
            _ => unreachable!("already matched Dml above"),
        };

        debug!(
            "Flow {} rewrote incremental SQL aggregate query with sink merge",
            self.config.flow_id
        );

        Ok((rewritten, true))
    }

    pub async fn execute_logical_plan(
        &self,
        frontend_client: &Arc<FrontendClient>,
        plan: &LogicalPlan,
        dirty_filter: Option<&FilterExprInfo>,
    ) -> Result<Option<(u32, Duration)>, Error> {
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
        let (plan, incremental_safe) = self
            .prepare_plan_for_incremental(&plan, dirty_filter)
            .await?;

        let extensions = self.build_flow_query_extensions(incremental_safe).await?;
        let extension_refs = extensions
            .iter()
            .map(|(key, value)| (*key, value.as_str()))
            .collect::<Vec<_>>();
        let query_mode = self.state.read().unwrap().checkpoint_mode();
        Self::record_query_mode(flow_id, query_mode);
        debug!(
            "Flow {flow_id} executing batching query with checkpoint_mode={}, extension_count={}",
            checkpoint_mode_label(query_mode),
            extensions.len()
        );

        let res = {
            let _timer = METRIC_FLOW_BATCHING_ENGINE_QUERY_TIME
                .with_label_values(&[flow_id.to_string().as_str()])
                .start_timer();

            let req = if let Some((insert_to, insert_plan)) =
                breakup_insert_plan(&plan, catalog, schema)
            {
                let message = DFLogicalSubstraitConvertor {}
                    .encode(&insert_plan, DefaultSerializer)
                    .context(SubstraitEncodeLogicalPlanSnafu)?;
                api::v1::QueryRequest {
                    query: Some(api::v1::query_request::Query::InsertIntoPlan(
                        api::v1::InsertIntoPlan {
                            table_name: Some(insert_to),
                            logical_plan: message.to_vec(),
                        },
                    )),
                }
            } else {
                let message = DFLogicalSubstraitConvertor {}
                    .encode(&plan, DefaultSerializer)
                    .context(SubstraitEncodeLogicalPlanSnafu)?;

                api::v1::QueryRequest {
                    query: Some(api::v1::query_request::Query::LogicalPlan(message.to_vec())),
                }
            };

            frontend_client
                .query_with_terminal_metrics(catalog, schema, req, &extension_refs)
                .await
        };

        let elapsed = instant.elapsed();
        if let Err(err) = &res {
            warn!(
                "Failed to execute Flow {flow_id}, result: {err:?}, elapsed: {:?} with query: {}",
                elapsed, &plan
            );
            self.state.write().unwrap().after_query_exec(elapsed, false);
        }

        // record slow query
        if elapsed >= self.config.batch_opts.slow_query_threshold {
            warn!(
                "Flow {flow_id} executed for {:?} before complete, query: {}",
                elapsed, &plan
            );
            METRIC_FLOW_BATCHING_ENGINE_SLOW_QUERY
                .with_label_values(&[flow_id.to_string().as_str(), "watermark-path"])
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
        let affected_rows: u32 = affected_rows.try_into().map_err(|_| {
            UnexpectedSnafu {
                reason: format!("Failed to convert rows to u32: {}", affected_rows),
            }
            .build()
        })?;

        {
            let mut state = self.state.write().unwrap();
            let decision = Self::apply_query_result_to_state(&mut state, &res, elapsed);
            Self::record_checkpoint_decision(flow_id, decision);
        }

        Ok(Some((affected_rows, elapsed)))
    }

    fn apply_query_result_to_state(
        state: &mut TaskState,
        res: &OutputWithMetrics,
        elapsed: Duration,
    ) -> FlowCheckpointDecision {
        state.after_query_exec(elapsed, true);
        let checkpoint_mode = state.checkpoint_mode();
        if checkpoint_mode == CheckpointMode::Incremental && state.is_incremental_disabled() {
            state.mark_full_snapshot();
            return FlowCheckpointDecision::FallbackToFullSnapshot {
                previous_mode: CheckpointMode::Incremental,
                reason: FlowQueryFallbackReason::IncrementalDisabled,
            };
        }

        if let (Some(participating_regions), Some(watermark_map)) =
            (res.participating_regions(), res.region_watermark_map())
        {
            let can_advance = match checkpoint_mode {
                CheckpointMode::FullSnapshot => state
                    .can_advance_full_snapshot_checkpoints(&participating_regions, &watermark_map),
                CheckpointMode::Incremental => state
                    .can_advance_incremental_checkpoints_with_participation(
                        &participating_regions,
                        &watermark_map,
                    ),
            };

            if can_advance {
                let participating_region_count = participating_regions.len();
                let watermark_count = watermark_map.len();
                match checkpoint_mode {
                    CheckpointMode::FullSnapshot => {
                        state.advance_checkpoints(watermark_map);
                        if state.is_incremental_disabled() {
                            FlowCheckpointDecision::FallbackToFullSnapshot {
                                previous_mode: CheckpointMode::FullSnapshot,
                                reason: FlowQueryFallbackReason::IncrementalDisabled,
                            }
                        } else {
                            FlowCheckpointDecision::AdvancedFromFullSnapshot {
                                participating_regions: participating_region_count,
                                watermarks: watermark_count,
                            }
                        }
                    }
                    CheckpointMode::Incremental => {
                        state.advance_incremental_checkpoints_with_participation(
                            &participating_regions,
                            watermark_map,
                        );
                        FlowCheckpointDecision::AdvancedIncremental {
                            participating_regions: participating_region_count,
                            watermarks: watermark_count,
                        }
                    }
                }
            } else {
                state.mark_full_snapshot();
                FlowCheckpointDecision::FallbackToFullSnapshot {
                    previous_mode: checkpoint_mode,
                    reason: FlowQueryFallbackReason::IncompleteRegionWatermark,
                }
            }
        } else {
            state.mark_full_snapshot();
            FlowCheckpointDecision::FallbackToFullSnapshot {
                previous_mode: checkpoint_mode,
                reason: FlowQueryFallbackReason::MissingRegionWatermark,
            }
        }
    }

    fn record_checkpoint_decision(flow_id: FlowId, decision: FlowCheckpointDecision) {
        let flow_id = flow_id.to_string();
        METRIC_FLOW_BATCHING_ENGINE_CHECKPOINT_DECISION_CNT
            .with_label_values(&[
                flow_id.as_str(),
                decision.mode_label(),
                decision.decision_label(),
                decision.reason_label(),
            ])
            .inc();

        match decision {
            FlowCheckpointDecision::AdvancedFromFullSnapshot {
                participating_regions,
                watermarks,
            } => {
                info!(
                    "Flow {flow_id} switched to incremental mode after full snapshot, participating_regions={participating_regions}, watermarks={watermarks}"
                );
            }
            FlowCheckpointDecision::AdvancedIncremental {
                participating_regions,
                watermarks,
            } => {
                debug!(
                    "Flow {flow_id} advanced incremental checkpoints, participating_regions={participating_regions}, watermarks={watermarks}"
                );
            }
            FlowCheckpointDecision::FallbackToFullSnapshot {
                previous_mode,
                reason,
            } => {
                warn!(
                    "Flow {flow_id} switched to full snapshot mode, previous_mode={}, reason={}",
                    checkpoint_mode_label(previous_mode),
                    reason.as_label()
                );
            }
        }
    }

    fn record_query_mode(flow_id: FlowId, mode: CheckpointMode) {
        let flow_id = flow_id.to_string();
        METRIC_FLOW_BATCHING_ENGINE_QUERY_MODE_CNT
            .with_label_values(&[flow_id.as_str(), checkpoint_mode_label(mode)])
            .inc();
    }

    fn record_failure_fallback(
        &self,
        previous_mode: CheckpointMode,
        reason: FlowQueryFallbackReason,
    ) {
        Self::record_checkpoint_decision(
            self.config.flow_id,
            FlowCheckpointDecision::FallbackToFullSnapshot {
                previous_mode,
                reason,
            },
        );
    }

    fn handle_flow_query_failure(&self, err: &Error, query: Option<&PlanInfo>) -> bool {
        let failure = FrontendClient::inspect_query_error(err);
        let previous_mode = { self.state.read().unwrap().checkpoint_mode() };
        let was_incremental = previous_mode == CheckpointMode::Incremental;
        if failure.is_stale_cursor() {
            warn!(
                "Flow {} detected stale incremental query failure, switching to non-incremental recompute semantics for current query scope: {:?}",
                self.config.flow_id, failure.stale_cursor
            );
            self.state.write().unwrap().mark_full_snapshot();
            self.record_failure_fallback(previous_mode, FlowQueryFallbackReason::StaleCursor);

            if query.is_none_or(|query| query.filter.is_none())
                && let Err(mark_err) = self.mark_all_windows_as_dirty()
            {
                warn!(
                    "Flow {} failed to mark all windows dirty after stale incremental query without time-window scope: {}",
                    self.config.flow_id, mark_err
                );
            }

            true
        } else {
            if was_incremental {
                warn!(
                    "Flow {} incremental query failed without stale-cursor detail, switching to non-incremental recompute semantics for current query scope",
                    self.config.flow_id
                );
                self.state.write().unwrap().mark_full_snapshot();
                self.record_failure_fallback(
                    previous_mode,
                    FlowQueryFallbackReason::IncrementalQueryFailure,
                );

                if query.is_none_or(|query| query.filter.is_none())
                    && let Err(mark_err) = self.mark_all_windows_as_dirty()
                {
                    warn!(
                        "Flow {} failed to mark all windows dirty after incremental query failure without time-window scope: {}",
                        self.config.flow_id, mark_err
                    );
                }
            }

            false
        }
    }

    fn restore_dirty_windows_after_failure(&self, query: &PlanInfo, is_stale_cursor: bool) {
        if is_stale_cursor && query.filter.is_none() {
            return;
        }

        self.state.write().unwrap().dirty_time_windows.add_windows(
            query
                .filter
                .as_ref()
                .map(|f| f.time_ranges.clone())
                .unwrap_or_default(),
        );
    }

    fn handle_executed_query_failure(&self, err: &Error, query: Option<&PlanInfo>) -> bool {
        let is_stale_cursor = self.handle_flow_query_failure(err, query);
        if let Some(query) = query {
            self.restore_dirty_windows_after_failure(query, is_stale_cursor);
        }
        is_stale_cursor
    }

    async fn build_flow_query_extensions(
        &self,
        incremental_safe: bool,
    ) -> Result<Vec<(&'static str, String)>, Error> {
        let mut extensions = vec![("flow.return_region_seq", "true".to_string())];

        if let Some(table) = self
            .config
            .catalog_manager
            .table(
                &self.config.sink_table_name[0],
                &self.config.sink_table_name[1],
                &self.config.sink_table_name[2],
                None,
            )
            .await
            .map_err(BoxedError::new)
            .context(ExternalSnafu)?
        {
            extensions.push((
                FLOW_SINK_TABLE_ID,
                table.table_info().table_id().to_string(),
            ));
        }

        let state = self.state.read().unwrap();
        if incremental_safe
            && !state.is_incremental_disabled()
            && state.checkpoint_mode() == CheckpointMode::Incremental
            && !state.checkpoints().is_empty()
        {
            let checkpoints_json = serde_json::to_string(state.checkpoints()).map_err(|err| {
                UnexpectedSnafu {
                    reason: format!("Failed to serialize checkpoint map: {err}"),
                }
                .build()
            })?;
            extensions.push((
                FLOW_INCREMENTAL_MODE,
                FLOW_INCREMENTAL_MODE_MEMTABLE_ONLY.to_string(),
            ));
            extensions.push((FLOW_INCREMENTAL_AFTER_SEQS, checkpoints_json));
        }

        Ok(extensions)
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

            let new_query = match self.gen_insert_plan(&engine, max_window_cnt).await {
                Ok(new_query) => new_query,
                Err(err) => {
                    common_telemetry::error!(err; "Failed to generate query for flow={}", self.config.flow_id);
                    // also sleep for a little while before try again to prevent flooding logs
                    tokio::time::sleep(min_refresh).await;
                    continue;
                }
            };

            let res = if let Some(new_query) = &new_query {
                self.execute_logical_plan(
                    &frontend_client,
                    &new_query.plan,
                    new_query.filter.as_ref(),
                )
                .await
            } else {
                Ok(None)
            };

            match res {
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

                            state.get_next_start_query_time(
                                self.config.flow_id,
                                &time_window_size,
                                min_refresh,
                                Some(self.config.batch_opts.query_timeout),
                                self.config.batch_opts.experimental_max_filter_num_per_query,
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
                    self.handle_executed_query_failure(&err, new_query.as_ref());
                    METRIC_FLOW_BATCHING_ENGINE_ERROR_CNT
                        .with_label_values(&[&flow_id_str])
                        .inc();
                    match new_query {
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

    /// will merge and use the first ten time window in query
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

        let (expire_lower_bound, expire_upper_bound) =
            match (expire_time_window_bound, &self.config.query_type) {
                (Some((Some(l), Some(u))), QueryType::Sql) => (l, u),
                (None, QueryType::Sql) => {
                    // if it's sql query and no time window lower/upper bound is found, just return the original query(with auto columns)
                    // use sink_table_meta to add to query the `update_at` and `__ts_placeholder` column's value too for compatibility reason
                    debug!(
                        "Flow id = {:?}, no time window, using the same query",
                        self.config.flow_id
                    );
                    // clean dirty time window too, this could be from create flow's check_execute
                    let is_dirty = {
                        let mut state = self.state.write().unwrap();
                        let is_dirty = !state.dirty_time_windows.is_empty();
                        state.dirty_time_windows.clean();
                        is_dirty
                    };

                    if !is_dirty {
                        // no dirty data, hence no need to update
                        debug!("Flow id={:?}, no new data, not update", self.config.flow_id);
                        return Ok(None);
                    }

                    let plan = match gen_plan_with_matching_schema(
                        &self.config.query,
                        query_ctx,
                        engine,
                        sink_table_schema.clone(),
                        primary_key_indices,
                        allow_partial,
                    )
                    .await
                    {
                        Ok(plan) => plan,
                        Err(err) => {
                            self.state.write().unwrap().dirty_time_windows.set_dirty();
                            return Err(err);
                        }
                    };

                    return Ok(Some(PlanInfo { plan, filter: None }));
                }
                _ => {
                    // clean for tql have no use for time window
                    let had_dirty_windows = {
                        let mut state = self.state.write().unwrap();
                        let had_dirty_windows = !state.dirty_time_windows.is_empty();
                        state.dirty_time_windows.clean();
                        had_dirty_windows
                    };

                    let plan = match gen_plan_with_matching_schema(
                        &self.config.query,
                        query_ctx,
                        engine,
                        sink_table_schema.clone(),
                        primary_key_indices,
                        allow_partial,
                    )
                    .await
                    {
                        Ok(plan) => plan,
                        Err(err) => {
                            if had_dirty_windows {
                                self.state.write().unwrap().dirty_time_windows.set_dirty();
                            }
                            return Err(err);
                        }
                    };

                    return Ok(Some(PlanInfo { plan, filter: None }));
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

        let expr = self
            .state
            .write()
            .unwrap()
            .dirty_time_windows
            .gen_filter_exprs(
                &col_name,
                Some(expire_lower_bound),
                window_size,
                max_window_cnt
                    .unwrap_or(self.config.batch_opts.experimental_max_filter_num_per_query),
                self.config.flow_id,
                Some(self),
            )?;

        debug!(
            "Flow id={:?}, Generated filter expr: {:?}",
            self.config.flow_id,
            expr.as_ref()
                .map(
                    |expr| expr_to_sql(&expr.expr).with_context(|_| DatafusionSnafu {
                        context: format!("Failed to generate filter expr from {expr:?}"),
                    })
                )
                .transpose()?
                .map(|s| s.to_string())
        );

        let Some(expr) = expr else {
            // no new data, hence no need to update
            debug!("Flow id={:?}, no new data, not update", self.config.flow_id);
            return Ok(None);
        };

        let mut add_filter = AddFilterRewriter::new(expr.expr.clone());
        let mut add_auto_column = ColumnMatcherRewriter::new(
            sink_table_schema.clone(),
            primary_key_indices.to_vec(),
            allow_partial,
        );

        let plan =
            sql_to_df_plan(query_ctx.clone(), engine.clone(), &self.config.query, false).await?;
        let rewrite = plan
            .clone()
            .rewrite(&mut add_filter)
            .and_then(|p| p.data.rewrite(&mut add_auto_column))
            .with_context(|_| DatafusionSnafu {
                context: format!("Failed to rewrite plan:\n {}\n", plan),
            })?
            .data;
        // only apply optimize after complex rewrite is done
        let new_plan = apply_df_optimizer(rewrite, &query_ctx).await?;

        let info = PlanInfo {
            plan: new_plan.clone(),
            filter: Some(expr),
        };

        Ok(Some(info))
    }
}

#[cfg(test)]
mod test;
