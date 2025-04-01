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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use api::v1::flow::FlowResponse;
use arrow_schema::Fields;
use common_error::ext::BoxedError;
use common_meta::ddl::create_flow::FlowType;
use common_meta::key::flow::FlowMetadataManagerRef;
use common_meta::key::table_info::TableInfoManager;
use common_meta::key::table_name::TableNameKey;
use common_meta::key::TableMetadataManagerRef;
use common_telemetry::tracing::warn;
use common_telemetry::{debug, info};
use common_time::Timestamp;
use datafusion::sql::unparser::expr_to_sql;
use datafusion_common::tree_node::TreeNode;
use datafusion_expr::LogicalPlan;
use datatypes::prelude::ConcreteDataType;
use datatypes::value::Value;
use itertools::Itertools;
use query::QueryEngineRef;
use session::context::QueryContextRef;
use snafu::{ensure, OptionExt, ResultExt};
use store_api::storage::RegionId;
use table::metadata::{RawTableMeta, TableId};
use tokio::sync::oneshot::error::TryRecvError;
use tokio::sync::{oneshot, RwLock};
use tokio::time::Instant;

use super::frontend_client::FrontendClient;
use crate::adapter::{CreateFlowArgs, FlowId, TableName, AUTO_CREATED_PLACEHOLDER_TS_COL};
use crate::error::{
    DatafusionSnafu, DatatypesSnafu, ExternalSnafu, FlowAlreadyExistSnafu, InternalSnafu,
    InvalidRequestSnafu, TableNotFoundMetaSnafu, TableNotFoundSnafu, TimeSnafu, UnexpectedSnafu,
};
use crate::metrics::{METRIC_FLOW_RULE_ENGINE_QUERY_TIME, METRIC_FLOW_RULE_ENGINE_SLOW_QUERY};
use crate::recording_rules::time_window::{find_time_window_expr, TimeWindowExpr};
use crate::recording_rules::utils::{
    df_plan_to_sql, sql_to_df_plan, AddAutoColumnRewriter, AddFilterRewriter, FindGroupByFinalName,
};
use crate::Error;

/// TODO(discord9): make those constants configurable
/// The default rule engine query timeout is 10 minutes
pub const DEFAULT_RULE_ENGINE_QUERY_TIMEOUT: Duration = Duration::from_secs(10 * 60);

/// will output a warn log for any query that runs for more that 1 minutes, and also every 1 minutes when that query is still running
pub const SLOW_QUERY_THRESHOLD: Duration = Duration::from_secs(60);

/// The minimum duration between two queries execution by recording rule
const MIN_REFRESH_DURATION: Duration = Duration::new(5, 0);

/// TODO(discord9): determine how to configure refresh rate
pub struct RecordingRuleEngine {
    tasks: RwLock<BTreeMap<FlowId, RecordingRuleTask>>,
    shutdown_txs: RwLock<BTreeMap<FlowId, oneshot::Sender<()>>>,
    frontend_client: Arc<FrontendClient>,
    flow_metadata_manager: FlowMetadataManagerRef,
    table_meta: TableMetadataManagerRef,
    query_engine: QueryEngineRef,
}

impl RecordingRuleEngine {
    pub fn new(
        frontend_client: Arc<FrontendClient>,
        query_engine: QueryEngineRef,
        flow_metadata_manager: FlowMetadataManagerRef,
        table_meta: TableMetadataManagerRef,
    ) -> Self {
        Self {
            tasks: Default::default(),
            shutdown_txs: Default::default(),
            frontend_client,
            flow_metadata_manager,
            table_meta,
            query_engine,
        }
    }

    pub async fn handle_inserts(
        &self,
        request: api::v1::region::InsertRequests,
    ) -> Result<FlowResponse, Error> {
        let table_info_mgr = self.table_meta.table_info_manager();
        let mut group_by_table_name: HashMap<TableName, Vec<api::v1::Rows>> = HashMap::new();
        for r in request.requests {
            let tid = RegionId::from(r.region_id).table_id();
            let name = get_table_name(table_info_mgr, &tid).await?;
            let entry = group_by_table_name.entry(name).or_default();
            if let Some(rows) = r.rows {
                entry.push(rows);
            }
        }

        for (_flow_id, task) in self.tasks.read().await.iter() {
            let src_table_names = &task.source_table_names;

            for src_table_name in src_table_names {
                if let Some(entry) = group_by_table_name.get(src_table_name) {
                    let Some(expr) = &task.time_window_expr else {
                        continue;
                    };
                    let involved_time_windows = expr.handle_rows(entry.clone()).await?;
                    let mut state = task.state.write().await;
                    state
                        .dirty_time_windows
                        .add_lower_bounds(involved_time_windows.into_iter());
                }
            }
        }

        Ok(Default::default())
    }
}

async fn get_table_name(
    table_info: &TableInfoManager,
    table_id: &TableId,
) -> Result<TableName, Error> {
    table_info
        .get(*table_id)
        .await
        .map_err(BoxedError::new)
        .context(ExternalSnafu)?
        .with_context(|| UnexpectedSnafu {
            reason: format!("Table id = {:?}, couldn't found table name", table_id),
        })
        .map(|name| name.table_name())
        .map(|name| [name.catalog_name, name.schema_name, name.table_name])
}

impl RecordingRuleEngine {
    pub async fn create_flow(&self, args: CreateFlowArgs) -> Result<Option<FlowId>, Error> {
        let CreateFlowArgs {
            flow_id,
            sink_table_name,
            source_table_ids,
            create_if_not_exists,
            or_replace,
            expire_after,
            comment: _,
            sql,
            flow_options,
            query_ctx,
        } = args;

        // or replace logic
        {
            let is_exist = self.tasks.read().await.contains_key(&flow_id);
            match (create_if_not_exists, or_replace, is_exist) {
                // if replace, ignore that old flow exists
                (_, true, true) => {
                    info!("Replacing flow with id={}", flow_id);
                }
                (false, false, true) => FlowAlreadyExistSnafu { id: flow_id }.fail()?,
                // already exists, and not replace, return None
                (true, false, true) => {
                    info!("Flow with id={} already exists, do nothing", flow_id);
                    return Ok(None);
                }

                // continue as normal
                (_, _, false) => (),
            }
        }

        let flow_type = flow_options.get(FlowType::FLOW_TYPE_KEY);

        ensure!(
            flow_type == Some(&FlowType::RecordingRule.to_string()) || flow_type.is_none(),
            UnexpectedSnafu {
                reason: format!("Flow type is not RecordingRule nor None, got {flow_type:?}")
            }
        );

        let Some(query_ctx) = query_ctx else {
            UnexpectedSnafu {
                reason: "Query context is None".to_string(),
            }
            .fail()?
        };
        let query_ctx = Arc::new(query_ctx);
        let mut source_table_names = Vec::new();
        for src_id in source_table_ids {
            let table_name = self
                .table_meta
                .table_info_manager()
                .get(src_id)
                .await
                .map_err(BoxedError::new)
                .context(ExternalSnafu)?
                .with_context(|| UnexpectedSnafu {
                    reason: format!("Table id = {:?}, couldn't found table name", src_id),
                })
                .map(|name| name.table_name())
                .map(|name| [name.catalog_name, name.schema_name, name.table_name])?;
            source_table_names.push(table_name);
        }

        let (tx, rx) = oneshot::channel();

        let plan = sql_to_df_plan(query_ctx.clone(), self.query_engine.clone(), &sql, true).await?;
        let (column_name, time_window_expr, _, df_schema) = find_time_window_expr(
            &plan,
            self.query_engine.engine_state().catalog_manager().clone(),
            query_ctx.clone(),
        )
        .await?;

        let phy_expr = time_window_expr
            .map(|expr| {
                TimeWindowExpr::from_expr(
                    &expr,
                    &column_name,
                    &df_schema,
                    &self.query_engine.engine_state().session_state(),
                )
            })
            .transpose()?;

        info!("Flow id={}, found time window expr={:?}", flow_id, phy_expr);

        let task = RecordingRuleTask::new(
            flow_id,
            &sql,
            plan,
            phy_expr,
            expire_after,
            sink_table_name,
            source_table_names,
            query_ctx,
            self.table_meta.clone(),
            rx,
        );

        let task_inner = task.clone();
        let engine = self.query_engine.clone();
        let frontend = self.frontend_client.clone();

        task.check_execute(&engine, &frontend).await?;

        // TODO(discord9): also save handle & use time wheel or what for better
        let _handle = common_runtime::spawn_global(async move {
            match task_inner.start_executing_loop(engine, frontend).await {
                Ok(()) => info!("Flow {} shutdown", task_inner.flow_id),
                Err(err) => common_telemetry::error!(
                    "Flow {} encounter unrecoverable error: {err:?}",
                    task_inner.flow_id
                ),
            }
        });

        // only replace here not earlier because we want the old one intact if something went wrong before this line
        let replaced_old_task_opt = self.tasks.write().await.insert(flow_id, task);
        drop(replaced_old_task_opt);

        self.shutdown_txs.write().await.insert(flow_id, tx);

        Ok(Some(flow_id))
    }

    pub async fn remove_flow(&self, flow_id: FlowId) -> Result<(), Error> {
        if self.tasks.write().await.remove(&flow_id).is_none() {
            warn!("Flow {flow_id} not found in tasks")
        }
        let Some(tx) = self.shutdown_txs.write().await.remove(&flow_id) else {
            UnexpectedSnafu {
                reason: format!("Can't found shutdown tx for flow {flow_id}"),
            }
            .fail()?
        };
        if tx.send(()).is_err() {
            warn!("Fail to shutdown flow {flow_id} due to receiver already dropped, maybe flow {flow_id} is already dropped?")
        }
        Ok(())
    }

    pub async fn flush_flow(&self, flow_id: FlowId) -> Result<(), Error> {
        let task = self.tasks.read().await.get(&flow_id).cloned();
        let task = task.with_context(|| UnexpectedSnafu {
            reason: format!("Can't found task for flow {flow_id}"),
        })?;

        task.gen_exec_once(&self.query_engine, &self.frontend_client)
            .await?;
        Ok(())
    }

    pub async fn flow_exist(&self, flow_id: FlowId) -> bool {
        self.tasks.read().await.contains_key(&flow_id)
    }
}

#[derive(Clone)]
pub struct RecordingRuleTask {
    pub flow_id: FlowId,
    query: String,
    plan: LogicalPlan,
    pub time_window_expr: Option<TimeWindowExpr>,
    /// in seconds
    pub expire_after: Option<i64>,
    sink_table_name: [String; 3],
    source_table_names: HashSet<[String; 3]>,
    table_meta: TableMetadataManagerRef,
    state: Arc<RwLock<RecordingRuleState>>,
}

impl RecordingRuleTask {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        flow_id: FlowId,
        query: &str,
        plan: LogicalPlan,
        time_window_expr: Option<TimeWindowExpr>,
        expire_after: Option<i64>,
        sink_table_name: [String; 3],
        source_table_names: Vec<[String; 3]>,
        query_ctx: QueryContextRef,
        table_meta: TableMetadataManagerRef,
        shutdown_rx: oneshot::Receiver<()>,
    ) -> Self {
        Self {
            flow_id,
            query: query.to_string(),
            plan,
            time_window_expr,
            expire_after,
            sink_table_name,
            source_table_names: source_table_names.into_iter().collect(),
            table_meta,
            state: Arc::new(RwLock::new(RecordingRuleState::new(query_ctx, shutdown_rx))),
        }
    }

    /// Test execute, for check syntax or such
    pub async fn check_execute(
        &self,
        engine: &QueryEngineRef,
        frontend_client: &Arc<FrontendClient>,
    ) -> Result<Option<(u32, Duration)>, Error> {
        // use current time to test get a dirty time window, which should be safe
        let start = SystemTime::now();
        let ts = Timestamp::new_second(
            start
                .duration_since(UNIX_EPOCH)
                .expect("Time went backwards")
                .as_secs() as _,
        );
        self.state
            .write()
            .await
            .dirty_time_windows
            .add_lower_bounds(vec![ts].into_iter());

        if !self.is_table_exist(&self.sink_table_name).await? {
            let create_table = self.gen_create_table_sql(engine.clone()).await?;
            info!(
                "Try creating sink table(if not exists) with query: {}",
                create_table
            );
            self.execute_sql(frontend_client, &create_table).await?;
            info!(
                "Sink table {}(if not exists) created",
                self.sink_table_name.join(".")
            );
        }
        self.gen_exec_once(engine, frontend_client).await
    }

    async fn is_table_exist(&self, table_name: &[String; 3]) -> Result<bool, Error> {
        self.table_meta
            .table_name_manager()
            .get(TableNameKey {
                catalog: &table_name[0],
                schema: &table_name[1],
                table: &table_name[2],
            })
            .await
            .map_err(BoxedError::new)
            .context(ExternalSnafu)
            .map(|v| v.is_some())
    }

    pub async fn gen_exec_once(
        &self,
        engine: &QueryEngineRef,
        frontend_client: &Arc<FrontendClient>,
    ) -> Result<Option<(u32, Duration)>, Error> {
        if let Some(new_query) = self.gen_insert_sql(engine).await? {
            self.execute_sql(frontend_client, &new_query).await
        } else {
            Ok(None)
        }
    }

    pub async fn gen_insert_sql(&self, engine: &QueryEngineRef) -> Result<Option<String>, Error> {
        let table_name_mgr = self.table_meta.table_name_manager();

        let full_table_name = self.sink_table_name.clone().join(".");

        let table_id = table_name_mgr
            .get(common_meta::key::table_name::TableNameKey::new(
                &self.sink_table_name[0],
                &self.sink_table_name[1],
                &self.sink_table_name[2],
            ))
            .await
            .with_context(|_| TableNotFoundMetaSnafu {
                msg: full_table_name.clone(),
            })?
            .map(|t| t.table_id())
            .with_context(|| TableNotFoundSnafu {
                name: full_table_name.clone(),
            })?;

        let table = self
            .table_meta
            .table_info_manager()
            .get(table_id)
            .await
            .with_context(|_| TableNotFoundMetaSnafu {
                msg: full_table_name.clone(),
            })?
            .with_context(|| TableNotFoundSnafu {
                name: full_table_name.clone(),
            })?
            .into_inner();

        let new_query = self
            .gen_query_with_time_window(engine.clone(), &table.table_info.meta)
            .await?;

        let insert_into = if let Some((new_query, _column_cnt)) = new_query {
            // TODO(discord9): also assign column name to compat update_at column
            format!("INSERT INTO {} {}", full_table_name, new_query)
        } else {
            return Ok(None);
        };
        Ok(Some(insert_into))
    }

    /// Execute the query once and return the output and the time it took
    pub async fn execute_sql(
        &self,
        frontend_client: &Arc<FrontendClient>,
        sql: &str,
    ) -> Result<Option<(u32, Duration)>, Error> {
        let instant = Instant::now();
        let flow_id = self.flow_id;
        let db_client = frontend_client.get_database_client().await?;
        let peer_addr = db_client.peer.addr;
        debug!(
            "Executing flow {flow_id}(expire_after={:?} secs) on {:?} with query {}",
            self.expire_after, peer_addr, &sql
        );

        let timer = METRIC_FLOW_RULE_ENGINE_QUERY_TIME
            .with_label_values(&[flow_id.to_string().as_str()])
            .start_timer();

        let req = api::v1::greptime_request::Request::Query(api::v1::QueryRequest {
            query: Some(api::v1::query_request::Query::Sql(sql.to_string())),
        });

        let res = db_client.database.handle(req).await;
        drop(timer);

        let elapsed = instant.elapsed();
        if let Ok(res1) = &res {
            debug!(
                "Flow {flow_id} executed, result: {res1:?}, elapsed: {:?}",
                elapsed
            );
        } else if let Err(res) = &res {
            warn!(
                "Failed to execute Flow {flow_id} on frontend {}, result: {res:?}, elapsed: {:?} with query: {}",
                peer_addr, elapsed, &sql
            );
        }

        // record slow query
        if elapsed >= SLOW_QUERY_THRESHOLD {
            warn!(
                "Flow {flow_id} on frontend {} executed for {:?} before complete, query: {}",
                peer_addr, elapsed, &sql
            );
            METRIC_FLOW_RULE_ENGINE_SLOW_QUERY
                .with_label_values(&[flow_id.to_string().as_str(), sql, &peer_addr])
                .observe(elapsed.as_secs_f64());
        }

        self.state
            .write()
            .await
            .after_query_exec(elapsed, res.is_ok());

        let res = res.context(InvalidRequestSnafu {
            context: format!(
                "Failed to execute query for flow={}: \'{}\'",
                self.flow_id, &sql
            ),
        })?;

        Ok(Some((res, elapsed)))
    }

    /// start executing query in a loop, break when receive shutdown signal
    pub async fn start_executing_loop(
        &self,
        engine: QueryEngineRef,
        frontend_client: Arc<FrontendClient>,
    ) -> Result<(), Error> {
        loop {
            let Some(_) = self.gen_exec_once(&engine, &frontend_client).await? else {
                debug!(
                    "Flow id = {:?} found no new data, sleep for {:?} then continue",
                    self.flow_id, MIN_REFRESH_DURATION
                );
                tokio::time::sleep(MIN_REFRESH_DURATION).await;
                continue;
            };

            let sleep_until = {
                let mut state = self.state.write().await;
                match state.shutdown_rx.try_recv() {
                    Ok(()) => break Ok(()),
                    Err(TryRecvError::Closed) => {
                        warn!("Unexpected shutdown flow {}, shutdown anyway", self.flow_id);
                        break Ok(());
                    }
                    Err(TryRecvError::Empty) => (),
                }
                state.get_next_start_query_time(None)
            };
            tokio::time::sleep_until(sleep_until).await;
        }
    }

    /// Generate the create table SQL
    ///
    /// the auto created table will automatically added a `update_at` Milliseconds DEFAULT now() column in the end
    /// (for compatibility with flow streaming mode)
    ///
    /// and it will use first timestamp column as time index, all other columns will be added as normal columns and nullable
    async fn gen_create_table_sql(&self, engine: QueryEngineRef) -> Result<String, Error> {
        let query_ctx = self.state.read().await.query_ctx.clone();
        let plan = sql_to_df_plan(query_ctx.clone(), engine.clone(), &self.query, true).await?;
        create_table_with(&plan, &self.sink_table_name.join("."))
    }

    /// will merge and use the first ten time window in query
    async fn gen_query_with_time_window(
        &self,
        engine: QueryEngineRef,
        sink_table_meta: &RawTableMeta,
    ) -> Result<Option<(String, usize)>, Error> {
        let query_ctx = self.state.read().await.query_ctx.clone();
        let start = SystemTime::now();
        let since_the_epoch = start
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");
        let low_bound = self
            .expire_after
            .map(|e| since_the_epoch.as_secs() - e as u64)
            .unwrap_or(u64::MIN);

        let low_bound = Timestamp::new_second(low_bound as i64);
        let schema_len = self.plan.schema().fields().len();

        // TODO(discord9): use sink_table_meta to add to query the `update_at` and `__ts_placeholder` column's value too

        // TODO(discord9): use time window expr to get the precise expire lower bound
        let expire_time_window_bound = self
            .time_window_expr
            .as_ref()
            .map(|expr| expr.eval(low_bound))
            .transpose()?;

        let new_sql = {
            let expr = {
                match expire_time_window_bound {
                    Some((Some(l), Some(u))) => {
                        let window_size = u.sub(&l).with_context(|| UnexpectedSnafu {
                            reason: format!("Can't get window size from {u:?} - {l:?}"),
                        })?;
                        let col_name = self
                            .time_window_expr
                            .as_ref()
                            .map(|expr| expr.column_name.clone())
                            .with_context(|| UnexpectedSnafu {
                                reason: format!(
                                    "Flow id={:?}, Failed to get column name from time window expr",
                                    self.flow_id
                                ),
                            })?;

                        self.state
                            .write()
                            .await
                            .dirty_time_windows
                            .gen_filter_exprs(&col_name, Some(l), window_size, self)?
                    }
                    _ => {
                        debug!(
                            "Flow id = {:?}, can't get window size: precise_lower_bound={expire_time_window_bound:?}, using the same query", self.flow_id
                        );

                        let mut add_auto_column =
                            AddAutoColumnRewriter::new(sink_table_meta.schema.clone());
                        let plan = self
                            .plan
                            .clone()
                            .rewrite(&mut add_auto_column)
                            .with_context(|_| DatafusionSnafu {
                                context: format!("Failed to rewrite plan {:?}", self.plan),
                            })?
                            .data;
                        let new_query = df_plan_to_sql(&plan)?;
                        let schema_len = plan.schema().fields().len();

                        // since no time window lower/upper bound is found, just return the original query(with auto columns)
                        return Ok(Some((new_query, schema_len)));
                    }
                }
            };

            debug!(
                "Flow id={:?}, Generated filter expr: {:?}",
                self.flow_id,
                expr.as_ref()
                    .map(|expr| expr_to_sql(expr).with_context(|_| DatafusionSnafu {
                        context: format!("Failed to generate filter expr from {expr:?}"),
                    }))
                    .transpose()?
                    .map(|s| s.to_string())
            );

            let Some(expr) = expr else {
                // no new data, hence no need to update
                debug!("Flow id={:?}, no new data, not update", self.flow_id);
                return Ok(None);
            };

            let mut add_filter = AddFilterRewriter::new(expr);
            let mut add_auto_column = AddAutoColumnRewriter::new(sink_table_meta.schema.clone());
            // make a not optimized plan for clearer unparse
            let plan =
                sql_to_df_plan(query_ctx.clone(), engine.clone(), &self.query, false).await?;
            let plan = plan
                .clone()
                .rewrite(&mut add_filter)
                .and_then(|p| p.data.rewrite(&mut add_auto_column))
                .with_context(|_| DatafusionSnafu {
                    context: format!("Failed to rewrite plan {plan:?}"),
                })?
                .data;
            df_plan_to_sql(&plan)?
        };

        Ok(Some((new_sql, schema_len)))
    }
}

// auto created table have a auto added column `update_at`, and optional have a `AUTO_CREATED_PLACEHOLDER_TS_COL` column for time index placeholder if no timestamp column is specified
fn create_table_with(plan: &LogicalPlan, sink_table_name: &str) -> Result<String, Error> {
    let schema = plan.schema().fields();
    let (first_time_stamp, all_pk_cols) = build_primary_key_constraint(plan, schema)?;

    let mut schema_in_sql: Vec<String> = Vec::new();
    for field in schema {
        let ty = ConcreteDataType::from_arrow_type(field.data_type());
        if first_time_stamp == Some(field.name().clone()) {
            schema_in_sql.push(format!("\"{}\" {} TIME INDEX", field.name(), ty));
        } else {
            schema_in_sql.push(format!("\"{}\" {} NULL", field.name(), ty));
        }
    }
    if first_time_stamp.is_none() {
        schema_in_sql.push("\"update_at\" TimestampMillisecond default now()".to_string());
        schema_in_sql.push(format!(
            "\"{}\" TimestampMillisecond TIME INDEX default 0",
            AUTO_CREATED_PLACEHOLDER_TS_COL
        ));
    } else {
        schema_in_sql.push("\"update_at\" TimestampMillisecond default now()".to_string());
    }

    if !all_pk_cols.is_empty() {
        schema_in_sql.push(format!("PRIMARY KEY ({})", all_pk_cols.iter().join(",")))
    };

    let col_defs = schema_in_sql.join(", ");

    let create_table_sql = format!(
        "CREATE TABLE IF NOT EXISTS {} ({})",
        sink_table_name, col_defs
    );

    Ok(create_table_sql)
}

/// Return first timestamp column which is in group by clause and other columns which are also in group by clause
///
/// # Returns
///
/// * `Option<String>` - first timestamp column which is in group by clause
/// * `Vec<String>` - other columns which are also in group by clause
fn build_primary_key_constraint(
    plan: &LogicalPlan,
    schema: &Fields,
) -> Result<(Option<String>, Vec<String>), Error> {
    let mut pk_names = FindGroupByFinalName::default();

    plan.visit(&mut pk_names)
        .with_context(|_| DatafusionSnafu {
            context: format!("Can't find aggr expr in plan {plan:?}"),
        })?;

    let pk_final_names = pk_names.get_group_expr_names().unwrap_or_default();
    let all_pk_cols: Vec<_> = schema
        .iter()
        .filter(|f| pk_final_names.contains(f.name()))
        .map(|f| f.name().clone())
        .collect();
    // auto create table use first timestamp column in group by clause as time index
    let first_time_stamp = schema
        .iter()
        .find(|f| {
            all_pk_cols.contains(&f.name().clone())
                && ConcreteDataType::from_arrow_type(f.data_type()).is_timestamp()
        })
        .map(|f| f.name().clone());

    let all_pk_cols: Vec<_> = all_pk_cols
        .into_iter()
        .filter(|col| first_time_stamp != Some(col.to_string()))
        .collect();

    Ok((first_time_stamp, all_pk_cols))
}

#[derive(Debug)]
pub struct RecordingRuleState {
    query_ctx: QueryContextRef,
    /// last query complete time
    last_update_time: Instant,
    /// last time query duration
    last_query_duration: Duration,
    /// Dirty Time windows need to be updated
    /// mapping of `start -> end` and non-overlapping
    dirty_time_windows: DirtyTimeWindows,
    exec_state: ExecState,
    shutdown_rx: oneshot::Receiver<()>,
}
impl RecordingRuleState {
    pub fn new(query_ctx: QueryContextRef, shutdown_rx: oneshot::Receiver<()>) -> Self {
        Self {
            query_ctx,
            last_update_time: Instant::now(),
            last_query_duration: Duration::from_secs(0),
            dirty_time_windows: Default::default(),
            exec_state: ExecState::Idle,
            shutdown_rx,
        }
    }

    /// called after last query is done
    /// `is_succ` indicate whether the last query is successful
    pub fn after_query_exec(&mut self, elapsed: Duration, _is_succ: bool) {
        self.exec_state = ExecState::Idle;
        self.last_query_duration = elapsed;
        self.last_update_time = Instant::now();
    }

    /// wait for at least `last_query_duration`, at most `max_timeout` to start next query
    pub fn get_next_start_query_time(&self, max_timeout: Option<Duration>) -> Instant {
        let next_duration = max_timeout
            .unwrap_or(self.last_query_duration)
            .min(self.last_query_duration);
        let next_duration = next_duration.max(MIN_REFRESH_DURATION);

        self.last_update_time + next_duration
    }
}

#[derive(Debug, Clone, Default)]
pub struct DirtyTimeWindows {
    /// windows's `start -> end` and non-overlapping
    /// `end` is exclusive(and optional)
    windows: BTreeMap<Timestamp, Option<Timestamp>>,
}

impl DirtyTimeWindows {
    /// Time window merge distance
    ///
    /// TODO(discord9): make those configurable
    const MERGE_DIST: i32 = 3;

    /// Maximum number of filters allowed in a single query
    const MAX_FILTER_NUM: usize = 20;

    /// Add lower bounds to the dirty time windows. Upper bounds are ignored.
    ///
    /// # Arguments
    ///
    /// * `lower_bounds` - An iterator of lower bounds to be added.
    pub fn add_lower_bounds(&mut self, lower_bounds: impl Iterator<Item = Timestamp>) {
        for lower_bound in lower_bounds {
            let entry = self.windows.entry(lower_bound);
            entry.or_insert(None);
        }
    }

    /// Generate all filter expressions consuming all time windows
    pub fn gen_filter_exprs(
        &mut self,
        col_name: &str,
        expire_lower_bound: Option<Timestamp>,
        window_size: chrono::Duration,
        task_ctx: &RecordingRuleTask,
    ) -> Result<Option<datafusion_expr::Expr>, Error> {
        debug!(
            "expire_lower_bound: {:?}, window_size: {:?}",
            expire_lower_bound.map(|t| t.to_iso8601_string()),
            window_size
        );
        self.merge_dirty_time_windows(window_size, expire_lower_bound)?;

        if self.windows.len() > Self::MAX_FILTER_NUM {
            let first_time_window = self.windows.first_key_value();
            let last_time_window = self.windows.last_key_value();
            warn!(
                "Flow id = {:?}, too many time windows: {}, only the first {} are taken for this query, the group by expression might be wrong. Time window expr={:?}, expire_after={:?}, first_time_window={:?}, last_time_window={:?}, the original query: {:?}",
                task_ctx.flow_id,
                self.windows.len(),
                Self::MAX_FILTER_NUM,
                task_ctx.time_window_expr,
                task_ctx.expire_after,
                first_time_window,
                last_time_window,
                task_ctx.query
            );
        }

        // get the first `MAX_FILTER_NUM` time windows
        let nth = self
            .windows
            .iter()
            .nth(Self::MAX_FILTER_NUM)
            .map(|(key, _)| *key);
        let first_nth = {
            if let Some(nth) = nth {
                let mut after = self.windows.split_off(&nth);
                std::mem::swap(&mut self.windows, &mut after);

                after
            } else {
                std::mem::take(&mut self.windows)
            }
        };

        let mut expr_lst = vec![];
        for (start, end) in first_nth.into_iter() {
            debug!(
                "Time window start: {:?}, end: {:?}",
                start.to_iso8601_string(),
                end.map(|t| t.to_iso8601_string())
            );

            use datafusion_expr::{col, lit};
            let lower = to_df_literal(start)?;
            let upper = end.map(to_df_literal).transpose()?;
            let expr = if let Some(upper) = upper {
                col(col_name)
                    .gt_eq(lit(lower))
                    .and(col(col_name).lt(lit(upper)))
            } else {
                col(col_name).gt_eq(lit(lower))
            };
            expr_lst.push(expr);
        }
        let expr = expr_lst.into_iter().reduce(|a, b| a.and(b));
        Ok(expr)
    }

    /// Merge time windows that overlaps or get too close
    pub fn merge_dirty_time_windows(
        &mut self,
        window_size: chrono::Duration,
        expire_lower_bound: Option<Timestamp>,
    ) -> Result<(), Error> {
        let mut new_windows = BTreeMap::new();

        // previous time window
        let mut prev_tw = None;
        for (lower_bound, upper_bound) in std::mem::take(&mut self.windows) {
            // filter out expired time window
            if let Some(expire_lower_bound) = expire_lower_bound {
                if lower_bound < expire_lower_bound {
                    continue;
                }
            }

            let Some(prev_tw) = &mut prev_tw else {
                prev_tw = Some((lower_bound, upper_bound));
                continue;
            };

            let std_window_size = window_size.to_std().map_err(|e| {
                InternalSnafu {
                    reason: e.to_string(),
                }
                .build()
            })?;

            // if cur.lower - prev.upper <= window_size * 2, merge
            let prev_upper = prev_tw
                .1
                .unwrap_or(prev_tw.0.add_duration(std_window_size).context(TimeSnafu)?);
            prev_tw.1 = Some(prev_upper);

            let cur_upper = upper_bound.unwrap_or(
                lower_bound
                    .add_duration(std_window_size)
                    .context(TimeSnafu)?,
            );

            if lower_bound
                .sub(&prev_upper)
                .map(|dist| dist <= window_size * Self::MERGE_DIST)
                .unwrap_or(false)
            {
                prev_tw.1 = Some(cur_upper);
            } else {
                new_windows.insert(prev_tw.0, prev_tw.1);
                *prev_tw = (lower_bound, Some(cur_upper));
            }
        }

        if let Some(prev_tw) = prev_tw {
            new_windows.insert(prev_tw.0, prev_tw.1);
        }

        self.windows = new_windows;

        Ok(())
    }
}

fn to_df_literal(value: Timestamp) -> Result<datafusion_common::ScalarValue, Error> {
    let value = Value::from(value);
    let value = value
        .try_to_scalar_value(&value.data_type())
        .with_context(|_| DatatypesSnafu {
            extra: format!("Failed to convert to scalar value: {}", value),
        })?;
    Ok(value)
}

#[derive(Debug, Clone)]
enum ExecState {
    Idle,
    Executing,
}

#[cfg(test)]
mod test {
    use pretty_assertions::assert_eq;
    use session::context::QueryContext;

    use super::*;
    use crate::test_utils::create_test_query_engine;

    #[test]
    fn test_merge_dirty_time_windows() {
        let mut dirty = DirtyTimeWindows::default();
        dirty.add_lower_bounds(
            vec![
                Timestamp::new_second(0),
                Timestamp::new_second((1 + DirtyTimeWindows::MERGE_DIST as i64) * 5 * 60),
            ]
            .into_iter(),
        );
        dirty
            .merge_dirty_time_windows(chrono::Duration::seconds(5 * 60), None)
            .unwrap();
        // just enough to merge
        assert_eq!(
            dirty.windows,
            BTreeMap::from([(
                Timestamp::new_second(0),
                Some(Timestamp::new_second(
                    (2 + DirtyTimeWindows::MERGE_DIST as i64) * 5 * 60
                ))
            )])
        );

        // separate time window
        let mut dirty = DirtyTimeWindows::default();
        dirty.add_lower_bounds(
            vec![
                Timestamp::new_second(0),
                Timestamp::new_second((2 + DirtyTimeWindows::MERGE_DIST as i64) * 5 * 60),
            ]
            .into_iter(),
        );
        dirty
            .merge_dirty_time_windows(chrono::Duration::seconds(5 * 60), None)
            .unwrap();
        // just enough to merge
        assert_eq!(
            BTreeMap::from([
                (
                    Timestamp::new_second(0),
                    Some(Timestamp::new_second(5 * 60))
                ),
                (
                    Timestamp::new_second((2 + DirtyTimeWindows::MERGE_DIST as i64) * 5 * 60),
                    Some(Timestamp::new_second(
                        (3 + DirtyTimeWindows::MERGE_DIST as i64) * 5 * 60
                    ))
                )
            ]),
            dirty.windows
        );

        // overlapping
        let mut dirty = DirtyTimeWindows::default();
        dirty.add_lower_bounds(
            vec![
                Timestamp::new_second(0),
                Timestamp::new_second((DirtyTimeWindows::MERGE_DIST as i64) * 5 * 60),
            ]
            .into_iter(),
        );
        dirty
            .merge_dirty_time_windows(chrono::Duration::seconds(5 * 60), None)
            .unwrap();
        // just enough to merge
        assert_eq!(
            BTreeMap::from([(
                Timestamp::new_second(0),
                Some(Timestamp::new_second(
                    (1 + DirtyTimeWindows::MERGE_DIST as i64) * 5 * 60
                ))
            ),]),
            dirty.windows
        );

        // expired
        let mut dirty = DirtyTimeWindows::default();
        dirty.add_lower_bounds(
            vec![
                Timestamp::new_second(0),
                Timestamp::new_second((DirtyTimeWindows::MERGE_DIST as i64) * 5 * 60),
            ]
            .into_iter(),
        );
        dirty
            .merge_dirty_time_windows(
                chrono::Duration::seconds(5 * 60),
                Some(Timestamp::new_second(
                    (DirtyTimeWindows::MERGE_DIST as i64) * 6 * 60,
                )),
            )
            .unwrap();
        // just enough to merge
        assert_eq!(BTreeMap::from([]), dirty.windows);
    }

    #[tokio::test]
    async fn test_gen_create_table_sql() {
        let query_engine = create_test_query_engine();
        let ctx = QueryContext::arc();
        struct TestCase {
            sql: String,
            sink_table_name: String,
            create: String,
        }
        let testcases = vec![
            TestCase {
            sql: "SELECT number, ts FROM numbers_with_ts".to_string(),
            sink_table_name: "new_table".to_string(),
            create: r#"CREATE TABLE IF NOT EXISTS new_table ("number" UInt32 NULL, "ts" TimestampMillisecond NULL, "update_at" TimestampMillisecond default now(), "__ts_placeholder" TimestampMillisecond TIME INDEX default 0)"#.to_string(),
        },
        TestCase {
            sql: "SELECT number, max(ts) FROM numbers_with_ts GROUP BY number".to_string(),
            sink_table_name: "new_table".to_string(),
            create: r#"CREATE TABLE IF NOT EXISTS new_table ("number" UInt32 NULL, "max(numbers_with_ts.ts)" TimestampMillisecond NULL, "update_at" TimestampMillisecond default now(), "__ts_placeholder" TimestampMillisecond TIME INDEX default 0, PRIMARY KEY (number))"#.to_string(),
        },
        TestCase {
            sql: "SELECT max(number), ts FROM numbers_with_ts GROUP BY ts".to_string(),
            sink_table_name: "new_table".to_string(),
            create: r#"CREATE TABLE IF NOT EXISTS new_table ("max(numbers_with_ts.number)" UInt32 NULL, "ts" TimestampMillisecond TIME INDEX, "update_at" TimestampMillisecond default now())"#.to_string(),
        },
        TestCase {
            sql: "SELECT number, ts FROM numbers_with_ts GROUP BY ts, number".to_string(),
            sink_table_name: "new_table".to_string(),
            create: r#"CREATE TABLE IF NOT EXISTS new_table ("number" UInt32 NULL, "ts" TimestampMillisecond TIME INDEX, "update_at" TimestampMillisecond default now(), PRIMARY KEY (number))"#.to_string(),
        }];

        for tc in testcases {
            let plan = sql_to_df_plan(ctx.clone(), query_engine.clone(), &tc.sql, true)
                .await
                .unwrap();
            let real = create_table_with(&plan, &tc.sink_table_name).unwrap();
            assert_eq!(tc.create, real);
        }
    }
}
