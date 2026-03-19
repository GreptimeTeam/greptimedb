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
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::{ColumnSchema, Schema};
use operator::expr_helper::column_schemas_to_defs;
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

use crate::adapter::{AUTO_CREATED_PLACEHOLDER_TS_COL, AUTO_CREATED_UPDATE_AT_TS_COL};
use crate::batching_mode::BatchingModeOptions;
use crate::batching_mode::frontend_client::FrontendClient;
use crate::batching_mode::state::{CheckpointMode, FilterExprInfo, TaskState};
use crate::batching_mode::time_window::TimeWindowExpr;
use crate::batching_mode::utils::{
    AddFilterRewriter, ColumnMatcherRewriter, FindGroupByFinalName, gen_plan_with_matching_schema,
    get_table_info_df_schema, sql_to_df_plan,
};
use crate::df_optimizer::apply_df_optimizer;
use crate::error::{
    ConvertColumnSchemaSnafu, DatafusionSnafu, ExternalSnafu, InvalidQuerySnafu,
    SubstraitEncodeLogicalPlanSnafu, UnexpectedSnafu,
};
use crate::metrics::{
    METRIC_FLOW_BATCHING_ENGINE_ERROR_CNT, METRIC_FLOW_BATCHING_ENGINE_QUERY_TIME,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryType {
    /// query is a tql query
    Tql,
    /// query is a sql query
    Sql,
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
            self.execute_logical_plan(frontend_client, &new_query.plan)
                .await
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

    pub async fn execute_logical_plan(
        &self,
        frontend_client: &Arc<FrontendClient>,
        plan: &LogicalPlan,
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

        let extensions = self.build_flow_query_extensions().await?;
        let extension_refs = extensions
            .iter()
            .map(|(key, value)| (*key, value.as_str()))
            .collect::<Vec<_>>();

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
        if let Ok(result) = &res {
            let (affected_rows, _) = result.output.extract_rows_and_cost();
            debug!(
                "Flow {flow_id} executed, affected_rows: {affected_rows:?}, elapsed: {:?}, watermark: {:?}",
                elapsed,
                result.region_watermark_map()
            );
            METRIC_FLOW_ROWS
                .with_label_values(&[format!("{}-out-batching", flow_id).as_str()])
                .inc_by(affected_rows as _);
        } else if let Err(err) = &res {
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
        let affected_rows: u32 = affected_rows.try_into().map_err(|_| {
            UnexpectedSnafu {
                reason: format!("Failed to convert rows to u32: {}", affected_rows),
            }
            .build()
        })?;

        {
            let mut state = self.state.write().unwrap();
            Self::apply_query_result_to_state(&mut state, &res, elapsed);
        }

        Ok(Some((affected_rows, elapsed)))
    }

    fn apply_query_result_to_state(
        state: &mut TaskState,
        res: &OutputWithMetrics,
        elapsed: Duration,
    ) {
        state.after_query_exec(elapsed, true);
        if let (Some(participating_regions), Some(watermark_map)) =
            (res.participating_regions(), res.region_watermark_map())
        {
            let checkpoint_mode = state.checkpoint_mode();
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
                match checkpoint_mode {
                    CheckpointMode::FullSnapshot => state.advance_checkpoints(watermark_map),
                    CheckpointMode::Incremental => state
                        .advance_incremental_checkpoints_with_participation(
                            &participating_regions,
                            watermark_map,
                        ),
                }
            } else {
                state.mark_full_snapshot();
            }
        } else {
            state.mark_full_snapshot();
        }
    }

    fn handle_flow_query_failure(&self, err: &Error, query: Option<&PlanInfo>) -> bool {
        let failure = FrontendClient::inspect_query_error(err);
        if failure.is_stale_cursor() {
            warn!(
                "Flow {} detected stale incremental query failure, switching to non-incremental recompute semantics for current query scope: {:?}",
                self.config.flow_id, failure.stale_cursor
            );
            self.state.write().unwrap().mark_full_snapshot();

            // notice that we only mark all as dirty if query itself has no time window filter.
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

    async fn build_flow_query_extensions(&self) -> Result<Vec<(&'static str, String)>, Error> {
        let state = self.state.read().unwrap();
        let mut extensions = vec![("flow.return_region_seq", "true".to_string())];

        drop(state);
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
        if state.checkpoint_mode() == CheckpointMode::Incremental && !state.checkpoints().is_empty()
        {
            let checkpoints_json = serde_json::to_string(state.checkpoints())
                .expect("checkpoint map should serialize");
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
                self.execute_logical_plan(&frontend_client, &new_query.plan)
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
                    let is_stale_cursor = self.handle_flow_query_failure(&err, new_query.as_ref());
                    METRIC_FLOW_BATCHING_ENGINE_ERROR_CNT
                        .with_label_values(&[&flow_id_str])
                        .inc();
                    match new_query {
                        Some(query) => {
                            common_telemetry::error!(err; "Failed to execute query for flow={} with query: {}", self.config.flow_id, query.plan);
                            // Re-add dirty windows back since query failed
                            self.restore_dirty_windows_after_failure(&query, is_stale_cursor);
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
                    let is_dirty = !self.state.read().unwrap().dirty_time_windows.is_empty();
                    self.state.write().unwrap().dirty_time_windows.clean();

                    if !is_dirty {
                        // no dirty data, hence no need to update
                        debug!("Flow id={:?}, no new data, not update", self.config.flow_id);
                        return Ok(None);
                    }

                    let plan = gen_plan_with_matching_schema(
                        &self.config.query,
                        query_ctx,
                        engine,
                        sink_table_schema.clone(),
                        primary_key_indices,
                        allow_partial,
                    )
                    .await?;

                    return Ok(Some(PlanInfo { plan, filter: None }));
                }
                _ => {
                    // clean for tql have no use for time window
                    self.state.write().unwrap().dirty_time_windows.clean();

                    let plan = gen_plan_with_matching_schema(
                        &self.config.query,
                        query_ctx,
                        engine,
                        sink_table_schema.clone(),
                        primary_key_indices,
                        allow_partial,
                    )
                    .await?;

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

// auto created table have a auto added column `update_at`, and optional have a `AUTO_CREATED_PLACEHOLDER_TS_COL` column for time index placeholder if no timestamp column is specified
// TODO(discord9): for now no default value is set for auto added column for compatibility reason with streaming mode, but this might change in favor of simpler code?
fn create_table_with_expr(
    plan: &LogicalPlan,
    sink_table_name: &[String; 3],
    query_type: &QueryType,
) -> Result<CreateTableExpr, Error> {
    let table_def = match query_type {
        &QueryType::Sql => {
            if let Some(def) = build_pk_from_aggr(plan)? {
                def
            } else {
                build_by_sql_schema(plan)?
            }
        }
        QueryType::Tql => {
            // first try build from aggr, then from tql schema because tql query might not have aggr node
            if let Some(table_def) = build_pk_from_aggr(plan)? {
                table_def
            } else {
                build_by_tql_schema(plan)?
            }
        }
    };
    let first_time_stamp = table_def.ts_col;
    let primary_keys = table_def.pks;

    let mut column_schemas = Vec::new();
    for field in plan.schema().fields() {
        let name = field.name();
        let ty = ConcreteDataType::from_arrow_type(field.data_type());
        let col_schema = if first_time_stamp == Some(name.clone()) {
            ColumnSchema::new(name, ty, false).with_time_index(true)
        } else {
            ColumnSchema::new(name, ty, true)
        };

        match query_type {
            QueryType::Sql => {
                column_schemas.push(col_schema);
            }
            QueryType::Tql => {
                // if is val column, need to rename as val DOUBLE NULL
                // if is tag column, need to cast type as STRING NULL
                let is_tag_column = primary_keys.contains(name);
                let is_val_column = !is_tag_column && first_time_stamp.as_ref() != Some(name);
                if is_val_column {
                    let col_schema =
                        ColumnSchema::new(name, ConcreteDataType::float64_datatype(), true);
                    column_schemas.push(col_schema);
                } else if is_tag_column {
                    let col_schema =
                        ColumnSchema::new(name, ConcreteDataType::string_datatype(), true);
                    column_schemas.push(col_schema);
                } else {
                    // time index column
                    column_schemas.push(col_schema);
                }
            }
        }
    }

    if query_type == &QueryType::Sql {
        let update_at_schema = ColumnSchema::new(
            AUTO_CREATED_UPDATE_AT_TS_COL,
            ConcreteDataType::timestamp_millisecond_datatype(),
            true,
        );
        column_schemas.push(update_at_schema);
    }

    let time_index = if let Some(time_index) = first_time_stamp {
        time_index
    } else {
        column_schemas.push(
            ColumnSchema::new(
                AUTO_CREATED_PLACEHOLDER_TS_COL,
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
        );
        AUTO_CREATED_PLACEHOLDER_TS_COL.to_string()
    };

    let column_defs =
        column_schemas_to_defs(column_schemas, &primary_keys).context(ConvertColumnSchemaSnafu)?;
    Ok(CreateTableExpr {
        catalog_name: sink_table_name[0].clone(),
        schema_name: sink_table_name[1].clone(),
        table_name: sink_table_name[2].clone(),
        desc: "Auto created table by flow engine".to_string(),
        column_defs,
        time_index,
        primary_keys,
        create_if_not_exists: true,
        table_options: Default::default(),
        table_id: None,
        engine: "mito".to_string(),
    })
}

/// simply build by schema, return first timestamp column and no primary key
fn build_by_sql_schema(plan: &LogicalPlan) -> Result<TableDef, Error> {
    let first_time_stamp = plan.schema().fields().iter().find_map(|f| {
        if ConcreteDataType::from_arrow_type(f.data_type()).is_timestamp() {
            Some(f.name().clone())
        } else {
            None
        }
    });
    Ok(TableDef {
        ts_col: first_time_stamp,
        pks: vec![],
    })
}

/// Return first timestamp column found in output schema and all string columns
fn build_by_tql_schema(plan: &LogicalPlan) -> Result<TableDef, Error> {
    let first_time_stamp = plan.schema().fields().iter().find_map(|f| {
        if ConcreteDataType::from_arrow_type(f.data_type()).is_timestamp() {
            Some(f.name().clone())
        } else {
            None
        }
    });
    let string_columns = plan
        .schema()
        .fields()
        .iter()
        .filter_map(|f| {
            if ConcreteDataType::from_arrow_type(f.data_type()).is_string() {
                Some(f.name().clone())
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    Ok(TableDef {
        ts_col: first_time_stamp,
        pks: string_columns,
    })
}

struct TableDef {
    ts_col: Option<String>,
    pks: Vec<String>,
}

/// Return first timestamp column which is in group by clause and other columns which are also in group by clause
///
/// # Returns
///
/// * `Option<String>` - first timestamp column which is in group by clause
/// * `Vec<String>` - other columns which are also in group by clause
///
/// if no aggregation found, return None
fn build_pk_from_aggr(plan: &LogicalPlan) -> Result<Option<TableDef>, Error> {
    let fields = plan.schema().fields();
    let mut pk_names = FindGroupByFinalName::default();

    plan.visit(&mut pk_names)
        .with_context(|_| DatafusionSnafu {
            context: format!("Can't find aggr expr in plan {plan:?}"),
        })?;

    // if no group by clause, return empty with first timestamp column found in output schema
    let Some(pk_final_names) = pk_names.get_group_expr_names() else {
        return Ok(None);
    };
    if pk_final_names.is_empty() {
        let first_ts_col = fields
            .iter()
            .find(|f| ConcreteDataType::from_arrow_type(f.data_type()).is_timestamp())
            .map(|f| f.name().clone());
        return Ok(Some(TableDef {
            ts_col: first_ts_col,
            pks: vec![],
        }));
    }

    let all_pk_cols: Vec<_> = fields
        .iter()
        .filter(|f| pk_final_names.contains(f.name()))
        .map(|f| f.name().clone())
        .collect();
    // auto create table use first timestamp column in group by clause as time index
    let first_time_stamp = fields
        .iter()
        .find(|f| {
            all_pk_cols.contains(&f.name().clone())
                && ConcreteDataType::from_arrow_type(f.data_type()).is_timestamp()
        })
        .map(|f| f.name().clone());

    let all_pk_cols: Vec<_> = all_pk_cols
        .into_iter()
        .filter(|col| first_time_stamp.as_ref() != Some(col))
        .collect();

    Ok(Some(TableDef {
        ts_col: first_time_stamp,
        pks: all_pk_cols,
    }))
}

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;

    use api::v1::column_def::try_as_column_schema;
    use common_error::ext::{BoxedError, PlainError};
    use common_error::status_code::StatusCode;
    use common_query::Output;
    use common_recordbatch::adapter::{RecordBatchMetrics, RegionWatermarkEntry};
    use pretty_assertions::assert_eq;
    use session::context::QueryContext;
    use snafu::GenerateImplicitData;

    use super::*;
    use crate::test_utils::create_test_query_engine;

    #[tokio::test]
    async fn test_gen_create_table_sql() {
        let query_engine = create_test_query_engine();
        let ctx = QueryContext::arc();
        struct TestCase {
            sql: String,
            sink_table_name: String,
            column_schemas: Vec<ColumnSchema>,
            primary_keys: Vec<String>,
            time_index: String,
        }

        let update_at_schema = ColumnSchema::new(
            AUTO_CREATED_UPDATE_AT_TS_COL,
            ConcreteDataType::timestamp_millisecond_datatype(),
            true,
        );

        let ts_placeholder_schema = ColumnSchema::new(
            AUTO_CREATED_PLACEHOLDER_TS_COL,
            ConcreteDataType::timestamp_millisecond_datatype(),
            false,
        )
        .with_time_index(true);

        let testcases = vec![
            TestCase {
                sql: "SELECT number, ts FROM numbers_with_ts".to_string(),
                sink_table_name: "new_table".to_string(),
                column_schemas: vec![
                    ColumnSchema::new("number", ConcreteDataType::uint32_datatype(), true),
                    ColumnSchema::new(
                        "ts",
                        ConcreteDataType::timestamp_millisecond_datatype(),
                        false,
                    )
                    .with_time_index(true),
                    update_at_schema.clone(),
                ],
                primary_keys: vec![],
                time_index: "ts".to_string(),
            },
            TestCase {
                sql: "SELECT number, max(ts) FROM numbers_with_ts GROUP BY number".to_string(),
                sink_table_name: "new_table".to_string(),
                column_schemas: vec![
                    ColumnSchema::new("number", ConcreteDataType::uint32_datatype(), true),
                    ColumnSchema::new(
                        "max(numbers_with_ts.ts)",
                        ConcreteDataType::timestamp_millisecond_datatype(),
                        true,
                    ),
                    update_at_schema.clone(),
                    ts_placeholder_schema.clone(),
                ],
                primary_keys: vec!["number".to_string()],
                time_index: AUTO_CREATED_PLACEHOLDER_TS_COL.to_string(),
            },
            TestCase {
                sql: "SELECT max(number), ts FROM numbers_with_ts GROUP BY ts".to_string(),
                sink_table_name: "new_table".to_string(),
                column_schemas: vec![
                    ColumnSchema::new(
                        "max(numbers_with_ts.number)",
                        ConcreteDataType::uint32_datatype(),
                        true,
                    ),
                    ColumnSchema::new(
                        "ts",
                        ConcreteDataType::timestamp_millisecond_datatype(),
                        false,
                    )
                    .with_time_index(true),
                    update_at_schema.clone(),
                ],
                primary_keys: vec![],
                time_index: "ts".to_string(),
            },
            TestCase {
                sql: "SELECT number, ts FROM numbers_with_ts GROUP BY ts, number".to_string(),
                sink_table_name: "new_table".to_string(),
                column_schemas: vec![
                    ColumnSchema::new("number", ConcreteDataType::uint32_datatype(), true),
                    ColumnSchema::new(
                        "ts",
                        ConcreteDataType::timestamp_millisecond_datatype(),
                        false,
                    )
                    .with_time_index(true),
                    update_at_schema.clone(),
                ],
                primary_keys: vec!["number".to_string()],
                time_index: "ts".to_string(),
            },
        ];

        for tc in testcases {
            let plan = sql_to_df_plan(ctx.clone(), query_engine.clone(), &tc.sql, true)
                .await
                .unwrap();
            let expr = create_table_with_expr(
                &plan,
                &[
                    "greptime".to_string(),
                    "public".to_string(),
                    tc.sink_table_name.clone(),
                ],
                &QueryType::Sql,
            )
            .unwrap();
            // TODO(discord9): assert expr
            let column_schemas = expr
                .column_defs
                .iter()
                .map(|c| try_as_column_schema(c).unwrap())
                .collect::<Vec<_>>();
            assert_eq!(tc.column_schemas, column_schemas, "{:?}", tc.sql);
            assert_eq!(tc.primary_keys, expr.primary_keys, "{:?}", tc.sql);
            assert_eq!(tc.time_index, expr.time_index, "{:?}", tc.sql);
        }
    }

    #[tokio::test]
    async fn test_handle_flow_query_failure_marks_full_recompute_on_stale() {
        let query_engine = create_test_query_engine();
        let query_ctx = QueryContext::arc();
        let plan = sql_to_df_plan(
            query_ctx.clone(),
            query_engine,
            "SELECT number, ts FROM numbers_with_ts",
            true,
        )
        .await
        .unwrap();
        let (_tx, rx) = tokio::sync::oneshot::channel();
        let task = BatchingTask::try_new(TaskArgs {
            flow_id: 42,
            query: "SELECT number, ts FROM numbers_with_ts",
            plan,
            time_window_expr: None,
            expire_after: None,
            sink_table_name: [
                "greptime".to_string(),
                "public".to_string(),
                "sink".to_string(),
            ],
            source_table_names: vec![[
                "greptime".to_string(),
                "public".to_string(),
                "numbers_with_ts".to_string(),
            ]],
            query_ctx,
            catalog_manager: create_test_query_engine()
                .engine_state()
                .catalog_manager()
                .clone(),
            shutdown_rx: rx,
            batch_opts: Arc::new(BatchingModeOptions::default()),
            flow_eval_interval: None,
        })
        .unwrap();

        let err = Error::External {
            source: BoxedError::new(PlainError::new(
                "STALE_CURSOR: incremental query stale, region: 4398046511104(1024, 0), given_seq: 9, min_readable_seq: 18, retry_hint: FALLBACK_FULL_RECOMPUTE".to_string(),
                StatusCode::EngineExecuteQuery,
            )),
            location: snafu::Location::generate(),
        };

        task.handle_flow_query_failure(&err, None);

        let state = task.state.read().unwrap();
        assert_eq!(state.dirty_time_windows.len(), 1);
    }

    #[tokio::test]
    async fn test_stale_failure_preserves_current_time_window_scope() {
        let query_engine = create_test_query_engine();
        let query_ctx = QueryContext::arc();
        let plan = sql_to_df_plan(
            query_ctx.clone(),
            query_engine,
            "SELECT number, ts FROM numbers_with_ts",
            true,
        )
        .await
        .unwrap();
        let (_tx, rx) = tokio::sync::oneshot::channel();
        let task = BatchingTask::try_new(TaskArgs {
            flow_id: 43,
            query: "SELECT number, ts FROM numbers_with_ts",
            plan: plan.clone(),
            time_window_expr: None,
            expire_after: None,
            sink_table_name: [
                "greptime".to_string(),
                "public".to_string(),
                "sink".to_string(),
            ],
            source_table_names: vec![[
                "greptime".to_string(),
                "public".to_string(),
                "numbers_with_ts".to_string(),
            ]],
            query_ctx,
            catalog_manager: create_test_query_engine()
                .engine_state()
                .catalog_manager()
                .clone(),
            shutdown_rx: rx,
            batch_opts: Arc::new(BatchingModeOptions::default()),
            flow_eval_interval: None,
        })
        .unwrap();

        let err = Error::External {
            source: BoxedError::new(PlainError::new(
                "STALE_CURSOR: incremental query stale, region: 4398046511104(1024, 0), given_seq: 9, min_readable_seq: 18, retry_hint: FALLBACK_FULL_RECOMPUTE".to_string(),
                StatusCode::EngineExecuteQuery,
            )),
            location: snafu::Location::generate(),
        };

        let query = PlanInfo {
            plan,
            filter: Some(FilterExprInfo {
                expr: datafusion_expr::lit(true),
                col_name: "ts".to_string(),
                time_ranges: vec![(Timestamp::new_second(0), Timestamp::new_second(1))],
                window_size: chrono::Duration::seconds(1),
            }),
        };
        let is_stale_cursor = task.handle_flow_query_failure(&err, Some(&query));
        task.restore_dirty_windows_after_failure(&query, is_stale_cursor);

        let state = task.state.read().unwrap();
        assert_eq!(state.dirty_time_windows.len(), 1);
        assert_eq!(
            state.dirty_time_windows.window_size(),
            std::time::Duration::from_secs(1)
        );
    }

    #[tokio::test]
    async fn test_build_flow_query_extensions_switches_with_checkpoint_mode() {
        let query_engine = create_test_query_engine();
        let query_ctx = QueryContext::arc();
        let plan = sql_to_df_plan(
            query_ctx.clone(),
            query_engine,
            "SELECT number, ts FROM numbers_with_ts",
            true,
        )
        .await
        .unwrap();
        let (_tx, rx) = tokio::sync::oneshot::channel();
        let task = BatchingTask::try_new(TaskArgs {
            flow_id: 44,
            query: "SELECT number, ts FROM numbers_with_ts",
            plan,
            time_window_expr: None,
            expire_after: None,
            sink_table_name: [
                "greptime".to_string(),
                "public".to_string(),
                "sink".to_string(),
            ],
            source_table_names: vec![[
                "greptime".to_string(),
                "public".to_string(),
                "numbers_with_ts".to_string(),
            ]],
            query_ctx,
            catalog_manager: create_test_query_engine()
                .engine_state()
                .catalog_manager()
                .clone(),
            shutdown_rx: rx,
            batch_opts: Arc::new(BatchingModeOptions::default()),
            flow_eval_interval: None,
        })
        .unwrap();

        let extensions = task.build_flow_query_extensions().await.unwrap();
        assert!(
            extensions
                .iter()
                .any(|(k, v)| *k == "flow.return_region_seq" && v == "true")
        );

        task.state
            .write()
            .unwrap()
            .advance_checkpoints(HashMap::from([(7_u64, 42_u64)]));
        let extensions = task.build_flow_query_extensions().await.unwrap();
        assert!(
            extensions
                .iter()
                .any(|(k, v)| *k == "flow.return_region_seq" && v == "true")
        );
        assert!(
            extensions
                .iter()
                .any(|(k, v)| *k == "flow.incremental_mode" && v == "memtable_only")
        );
        assert!(
            extensions
                .iter()
                .any(|(k, v)| *k == "flow.incremental_after_seqs" && v == r#"{"7":42}"#)
        );
    }

    fn watermark_result(entries: Vec<(u64, Option<u64>)>) -> OutputWithMetrics {
        let result = OutputWithMetrics::from_output(Output::new_with_affected_rows(1));
        result.metrics.update(Some(RecordBatchMetrics {
            region_watermarks: entries
                .into_iter()
                .map(|(region_id, watermark)| RegionWatermarkEntry {
                    region_id,
                    watermark,
                })
                .collect(),
            ..Default::default()
        }));
        result.metrics.mark_ready();
        result
    }

    #[tokio::test]
    async fn test_apply_query_result_to_state_advances_full_snapshot_to_incremental() {
        let query_engine = create_test_query_engine();
        let query_ctx = QueryContext::arc();
        let plan = sql_to_df_plan(
            query_ctx.clone(),
            query_engine,
            "SELECT number, ts FROM numbers_with_ts",
            true,
        )
        .await
        .unwrap();
        let (_tx, rx) = tokio::sync::oneshot::channel();
        let task = BatchingTask::try_new(TaskArgs {
            flow_id: 45,
            query: "SELECT number, ts FROM numbers_with_ts",
            plan,
            time_window_expr: None,
            expire_after: None,
            sink_table_name: [
                "greptime".to_string(),
                "public".to_string(),
                "sink".to_string(),
            ],
            source_table_names: vec![[
                "greptime".to_string(),
                "public".to_string(),
                "numbers_with_ts".to_string(),
            ]],
            query_ctx,
            catalog_manager: create_test_query_engine()
                .engine_state()
                .catalog_manager()
                .clone(),
            shutdown_rx: rx,
            batch_opts: Arc::new(BatchingModeOptions::default()),
            flow_eval_interval: None,
        })
        .unwrap();

        let result = watermark_result(vec![(1, Some(10)), (2, Some(20))]);
        let mut state = task.state.write().unwrap();
        BatchingTask::apply_query_result_to_state(&mut state, &result, Duration::from_millis(1));

        assert_eq!(state.checkpoint_mode(), CheckpointMode::Incremental);
        assert_eq!(
            state.checkpoints(),
            &BTreeMap::from([(1_u64, 10_u64), (2_u64, 20_u64)])
        );
    }

    #[tokio::test]
    async fn test_apply_query_result_to_state_rejects_partial_incremental_watermark_map() {
        let query_engine = create_test_query_engine();
        let query_ctx = QueryContext::arc();
        let plan = sql_to_df_plan(
            query_ctx.clone(),
            query_engine,
            "SELECT number, ts FROM numbers_with_ts",
            true,
        )
        .await
        .unwrap();
        let (_tx, rx) = tokio::sync::oneshot::channel();
        let task = BatchingTask::try_new(TaskArgs {
            flow_id: 46,
            query: "SELECT number, ts FROM numbers_with_ts",
            plan,
            time_window_expr: None,
            expire_after: None,
            sink_table_name: [
                "greptime".to_string(),
                "public".to_string(),
                "sink".to_string(),
            ],
            source_table_names: vec![[
                "greptime".to_string(),
                "public".to_string(),
                "numbers_with_ts".to_string(),
            ]],
            query_ctx,
            catalog_manager: create_test_query_engine()
                .engine_state()
                .catalog_manager()
                .clone(),
            shutdown_rx: rx,
            batch_opts: Arc::new(BatchingModeOptions::default()),
            flow_eval_interval: None,
        })
        .unwrap();

        {
            let mut state = task.state.write().unwrap();
            state.advance_checkpoints(HashMap::from([(1_u64, 10_u64), (2_u64, 20_u64)]));
        }

        let result = watermark_result(vec![(1, Some(11)), (2, None)]);
        let mut state = task.state.write().unwrap();
        BatchingTask::apply_query_result_to_state(&mut state, &result, Duration::from_millis(1));

        assert_eq!(state.checkpoint_mode(), CheckpointMode::FullSnapshot);
        assert_eq!(
            state.checkpoints(),
            &BTreeMap::from([(1_u64, 10_u64), (2_u64, 20_u64)])
        );
    }

    #[tokio::test]
    async fn test_apply_query_result_to_state_accepts_pruned_incremental_subset_and_preserves_others()
     {
        let query_engine = create_test_query_engine();
        let query_ctx = QueryContext::arc();
        let plan = sql_to_df_plan(
            query_ctx.clone(),
            query_engine,
            "SELECT number, ts FROM numbers_with_ts",
            true,
        )
        .await
        .unwrap();
        let (_tx, rx) = tokio::sync::oneshot::channel();
        let task = BatchingTask::try_new(TaskArgs {
            flow_id: 48,
            query: "SELECT number, ts FROM numbers_with_ts",
            plan,
            time_window_expr: None,
            expire_after: None,
            sink_table_name: [
                "greptime".to_string(),
                "public".to_string(),
                "sink".to_string(),
            ],
            source_table_names: vec![[
                "greptime".to_string(),
                "public".to_string(),
                "numbers_with_ts".to_string(),
            ]],
            query_ctx,
            catalog_manager: create_test_query_engine()
                .engine_state()
                .catalog_manager()
                .clone(),
            shutdown_rx: rx,
            batch_opts: Arc::new(BatchingModeOptions::default()),
            flow_eval_interval: None,
        })
        .unwrap();

        {
            let mut state = task.state.write().unwrap();
            state.advance_checkpoints(HashMap::from([
                (1_u64, 10_u64),
                (2_u64, 20_u64),
                (3_u64, 30_u64),
            ]));
        }

        let result = watermark_result(vec![(1, Some(12)), (3, Some(31))]);
        let mut state = task.state.write().unwrap();
        BatchingTask::apply_query_result_to_state(&mut state, &result, Duration::from_millis(1));

        assert_eq!(state.checkpoint_mode(), CheckpointMode::Incremental);
        assert_eq!(
            state.checkpoints(),
            &BTreeMap::from([(1_u64, 12_u64), (2_u64, 20_u64), (3_u64, 31_u64)])
        );
    }

    #[tokio::test]
    async fn test_stale_to_full_to_incremental_recovery_path() {
        let query_engine = create_test_query_engine();
        let query_ctx = QueryContext::arc();
        let plan = sql_to_df_plan(
            query_ctx.clone(),
            query_engine,
            "SELECT number, ts FROM numbers_with_ts",
            true,
        )
        .await
        .unwrap();
        let (_tx, rx) = tokio::sync::oneshot::channel();
        let task = BatchingTask::try_new(TaskArgs {
            flow_id: 47,
            query: "SELECT number, ts FROM numbers_with_ts",
            plan: plan.clone(),
            time_window_expr: None,
            expire_after: None,
            sink_table_name: [
                "greptime".to_string(),
                "public".to_string(),
                "sink".to_string(),
            ],
            source_table_names: vec![[
                "greptime".to_string(),
                "public".to_string(),
                "numbers_with_ts".to_string(),
            ]],
            query_ctx,
            catalog_manager: create_test_query_engine()
                .engine_state()
                .catalog_manager()
                .clone(),
            shutdown_rx: rx,
            batch_opts: Arc::new(BatchingModeOptions::default()),
            flow_eval_interval: None,
        })
        .unwrap();

        {
            let mut state = task.state.write().unwrap();
            state.advance_checkpoints(HashMap::from([(1_u64, 10_u64), (2_u64, 20_u64)]));
            assert_eq!(state.checkpoint_mode(), CheckpointMode::Incremental);
        }

        let err = Error::External {
            source: BoxedError::new(PlainError::new(
                "STALE_CURSOR: incremental query stale, region: 4398046511104(1024, 0), given_seq: 9, min_readable_seq: 18, retry_hint: FALLBACK_FULL_RECOMPUTE".to_string(),
                StatusCode::EngineExecuteQuery,
            )),
            location: snafu::Location::generate(),
        };
        assert!(task.handle_flow_query_failure(&err, None));
        assert_eq!(
            task.state.read().unwrap().checkpoint_mode(),
            CheckpointMode::FullSnapshot
        );

        let result = watermark_result(vec![(1, Some(30)), (2, Some(40))]);
        let mut state = task.state.write().unwrap();
        BatchingTask::apply_query_result_to_state(&mut state, &result, Duration::from_millis(1));

        assert_eq!(state.checkpoint_mode(), CheckpointMode::Incremental);
        assert_eq!(
            state.checkpoints(),
            &BTreeMap::from([(1_u64, 30_u64), (2_u64, 40_u64)])
        );
    }
}
