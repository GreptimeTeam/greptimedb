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
use query::options::FLOW_INCREMENTAL_MODE;
use query::query_engine::DefaultSerializer;
use session::context::QueryContextRef;
use snafu::{OptionExt, ResultExt};
use sql::parsers::utils::is_tql;
use store_api::mito_engine_options::MERGE_MODE_KEY;
use substrait::{DFLogicalSubstraitConvertor, SubstraitPlan};
use table::table::adapter::DfTableProviderAdapter;
use tokio::sync::oneshot;
use tokio::sync::oneshot::error::TryRecvError;
use tokio::time::Instant;

use crate::batching_mode::BatchingModeOptions;
use crate::batching_mode::checkpoint::checkpoint_mode_label;
use crate::batching_mode::frontend_client::{FrontendClient, PeerDesc};
use crate::batching_mode::state::{CheckpointMode, DirtyTimeWindows, FilterExprInfo, TaskState};
use crate::batching_mode::table_creator::{QueryType, create_table_with_expr};
use crate::batching_mode::time_window::TimeWindowExpr;
use crate::batching_mode::utils::{
    AddFilterRewriter, ColumnMatcherRewriter, gen_plan_with_matching_schema,
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

/// Maximum number of dirty time-window predicates attached to one incremental
/// SQL query. This keeps generated OR filters bounded so Substrait encoding and
/// downstream planning remain predictable; if the backlog is larger, the flow
/// drains one capped batch and postpones checkpoint advancement to a later run.
const MAX_INCREMENTAL_DIRTY_WINDOW_FILTERS: usize = 4096;

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
    pub dirty_restore: DirtyRestore,
    pub can_advance_checkpoints: bool,
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
    ) -> Result<Option<(usize, Duration)>, Error> {
        if let Some(new_query) = self.gen_insert_plan(engine, max_window_cnt).await? {
            debug!("Generate new query: {}", new_query.plan);
            let dirty_filter = match &new_query.dirty_restore {
                DirtyRestore::Scoped(f) => Some(f),
                _ => None,
            };
            match self
                .execute_logical_plan(
                    frontend_client,
                    &new_query.plan,
                    dirty_filter,
                    new_query.can_advance_checkpoints,
                )
                .await
            {
                Ok(result) => Ok(result),
                Err(err) => {
                    self.handle_executed_query_failure(Some(&new_query));
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
            can_advance_checkpoints: new_query.can_advance_checkpoints,
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
            can_advance_checkpoints: insert_into_info.can_advance_checkpoints,
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
        dirty_filter: Option<&FilterExprInfo>,
        can_advance_checkpoints: bool,
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
        let incremental_plan = if can_advance_checkpoints {
            self.prepare_plan_for_incremental(&plan, dirty_filter)
                .await?
        } else {
            None
        };
        let incremental_safe = incremental_plan.is_some();
        let plan = incremental_plan.unwrap_or_else(|| plan.clone());

        let extensions = self
            .build_flow_query_extensions(incremental_safe, can_advance_checkpoints)
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
                .query_with_terminal_metrics(catalog, schema, req, &extension_refs, &mut peer_desc)
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
                let reason = Self::query_failure_reason(err);
                Self::apply_query_failure_to_state(&mut state, elapsed, reason)
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
        {
            let mut state = self.state.write().unwrap();
            let decision = Self::apply_query_result_to_state(
                &mut state,
                &res,
                elapsed,
                can_advance_checkpoints,
            );
            Self::record_checkpoint_decision(flow_id, decision);
        }

        Ok(Some((affected_rows, elapsed)))
    }

    /// Restore dirty windows consumed by a failed query so they are retried on
    /// the next execution.
    ///
    fn restore_dirty_windows_after_failure(&self, query: &PlanInfo) {
        match &query.dirty_restore {
            DirtyRestore::Scoped(filter) => self.restore_scoped_dirty_windows(filter),
            DirtyRestore::Unscoped(dirty_windows) => self
                .state
                .write()
                .unwrap()
                .dirty_time_windows
                .add_dirty_windows(dirty_windows),
        }
    }

    fn restore_scoped_dirty_windows(&self, filter: &FilterExprInfo) {
        self.state
            .write()
            .unwrap()
            .dirty_time_windows
            .add_windows(filter.time_ranges.clone());
    }

    fn restore_scoped_dirty_windows_on_err<T>(
        &self,
        filter: &FilterExprInfo,
        result: Result<T, Error>,
    ) -> Result<T, Error> {
        result.inspect_err(|_| {
            self.restore_scoped_dirty_windows(filter);
        })
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
                let dirty_filter = match &new_query.dirty_restore {
                    DirtyRestore::Scoped(f) => Some(f),
                    _ => None,
                };
                self.execute_logical_plan(
                    &frontend_client,
                    &new_query.plan,
                    dirty_filter,
                    new_query.can_advance_checkpoints,
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
                    self.handle_executed_query_failure(new_query.as_ref());
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
                    let (is_dirty, dirty_windows_to_restore) = {
                        let mut state = self.state.write().unwrap();
                        let dirty_windows_to_restore = state.dirty_time_windows.clone();
                        let is_dirty = !dirty_windows_to_restore.is_empty();
                        state.dirty_time_windows.clean();
                        (is_dirty, dirty_windows_to_restore)
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
                            self.state
                                .write()
                                .unwrap()
                                .dirty_time_windows
                                .add_dirty_windows(&dirty_windows_to_restore);
                            return Err(err);
                        }
                    };

                    return Ok(Some(PlanInfo {
                        plan,
                        dirty_restore: DirtyRestore::Unscoped(dirty_windows_to_restore),
                        can_advance_checkpoints: true,
                    }));
                }
                _ => {
                    // Clean dirty windows for full-query/non-scoped paths,
                    // such as TQL, that cannot use a time-window filter.
                    let dirty_windows_to_restore = {
                        let mut state = self.state.write().unwrap();
                        let dirty_windows_to_restore = state.dirty_time_windows.clone();
                        state.dirty_time_windows.clean();
                        dirty_windows_to_restore
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
                            self.state
                                .write()
                                .unwrap()
                                .dirty_time_windows
                                .add_dirty_windows(&dirty_windows_to_restore);
                            return Err(err);
                        }
                    };

                    return Ok(Some(PlanInfo {
                        plan,
                        dirty_restore: DirtyRestore::Unscoped(dirty_windows_to_restore),
                        can_advance_checkpoints: true,
                    }));
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

        let (expr, can_advance_checkpoints) = {
            let mut state = self.state.write().unwrap();
            let window_cnt = if state.checkpoint_mode() == CheckpointMode::Incremental
                && !state.is_incremental_disabled()
                && matches!(self.config.query_type, QueryType::Sql)
            {
                // Incremental scans are bounded by region sequence checkpoints,
                // so the dirty-window filter only narrows sink-side/time-window
                // work. Drain more windows than normal, but keep a hard cap to
                // avoid building a huge OR filter after a long downtime. If
                // windows remain, checkpoints won't advance this round.
                MAX_INCREMENTAL_DIRTY_WINDOW_FILTERS
            } else {
                max_window_cnt
                    .unwrap_or(self.config.batch_opts.experimental_max_filter_num_per_query)
            };
            let expr = state.dirty_time_windows.gen_filter_exprs(
                &col_name,
                Some(expire_lower_bound),
                window_size,
                window_cnt,
                self.config.flow_id,
                Some(self),
            )?;
            let can_advance_checkpoints = state.dirty_time_windows.is_empty();
            (expr, can_advance_checkpoints)
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
            can_advance_checkpoints,
        };

        Ok(Some(info))
    }
}

#[cfg(test)]
mod test;
