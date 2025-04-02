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

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrow_schema::Fields;
use common_error::ext::BoxedError;
use common_meta::key::table_name::TableNameKey;
use common_meta::key::TableMetadataManagerRef;
use common_telemetry::tracing::warn;
use common_telemetry::{debug, info};
use common_time::Timestamp;
use datafusion::sql::unparser::expr_to_sql;
use datafusion_common::tree_node::TreeNode;
use datafusion_expr::LogicalPlan;
use datatypes::prelude::ConcreteDataType;
use itertools::Itertools;
use query::QueryEngineRef;
use session::context::QueryContextRef;
use snafu::{OptionExt, ResultExt};
use table::metadata::RawTableMeta;
use tokio::sync::oneshot::error::TryRecvError;
use tokio::sync::{oneshot, RwLock};
use tokio::time::Instant;

use super::frontend_client::FrontendClient;
use crate::adapter::{FlowId, AUTO_CREATED_PLACEHOLDER_TS_COL};
use crate::batching_mode::state::TaskState;
use crate::batching_mode::time_window::TimeWindowExpr;
use crate::batching_mode::utils::{
    df_plan_to_sql, sql_to_df_plan, AddAutoColumnRewriter, AddFilterRewriter, FindGroupByFinalName,
};
use crate::batching_mode::{MIN_REFRESH_DURATION, SLOW_QUERY_THRESHOLD};
use crate::error::{
    DatafusionSnafu, ExternalSnafu, InvalidRequestSnafu, TableNotFoundMetaSnafu,
    TableNotFoundSnafu, UnexpectedSnafu,
};
use crate::metrics::{
    METRIC_FLOW_BATCHING_ENGINE_QUERY_TIME, METRIC_FLOW_BATCHING_ENGINE_SLOW_QUERY,
};
use crate::Error;

#[derive(Clone)]
pub struct BatchingTask {
    pub flow_id: FlowId,
    pub query: String,
    plan: LogicalPlan,
    pub time_window_expr: Option<TimeWindowExpr>,
    /// in seconds
    pub expire_after: Option<i64>,
    sink_table_name: [String; 3],
    pub source_table_names: HashSet<[String; 3]>,
    table_meta: TableMetadataManagerRef,
    pub state: Arc<RwLock<TaskState>>,
}

impl BatchingTask {
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
            state: Arc::new(RwLock::new(TaskState::new(query_ctx, shutdown_rx))),
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

        let timer = METRIC_FLOW_BATCHING_ENGINE_QUERY_TIME
            .with_label_values(&[flow_id.to_string().as_str()])
            .start_timer();

        let req = api::v1::greptime_request::Request::Query(api::v1::QueryRequest {
            query: Some(api::v1::query_request::Query::Sql(sql.to_string())),
        });

        let res = db_client.database.handle(req).await;
        drop(timer);

        let elapsed = instant.elapsed();
        if let Ok(affected_rows) = &res {
            debug!(
                "Flow {flow_id} executed, affected_rows: {affected_rows:?}, elapsed: {:?}",
                elapsed
            );
        } else if let Err(err) = &res {
            warn!(
                "Failed to execute Flow {flow_id} on frontend {}, result: {err:?}, elapsed: {:?} with query: {}",
                peer_addr, elapsed, &sql
            );
        }

        // record slow query
        if elapsed >= SLOW_QUERY_THRESHOLD {
            warn!(
                "Flow {flow_id} on frontend {} executed for {:?} before complete, query: {}",
                peer_addr, elapsed, &sql
            );
            METRIC_FLOW_BATCHING_ENGINE_SLOW_QUERY
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
    ///
    /// any error will be logged when executing query
    pub async fn start_executing_loop(
        &self,
        engine: QueryEngineRef,
        frontend_client: Arc<FrontendClient>,
    ) {
        loop {
            let mut new_query = None;
            let mut gen_and_exec = async || {
                new_query = self.gen_insert_sql(&engine).await?;
                if let Some(new_query) = &new_query {
                    self.execute_sql(&frontend_client, new_query).await
                } else {
                    Ok(None)
                }
            };
            match gen_and_exec().await {
                // normal execute, sleep for some time before doing next query
                Ok(Some(_)) => {
                    let sleep_until = {
                        let mut state = self.state.write().await;
                        match state.shutdown_rx.try_recv() {
                            Ok(()) => break,
                            Err(TryRecvError::Closed) => {
                                warn!("Unexpected shutdown flow {}, shutdown anyway", self.flow_id);
                                break;
                            }
                            Err(TryRecvError::Empty) => (),
                        }
                        state.get_next_start_query_time(None)
                    };
                    tokio::time::sleep_until(sleep_until).await;
                }
                // no new data, sleep for some time before checking for new data
                Ok(None) => {
                    debug!(
                        "Flow id = {:?} found no new data, sleep for {:?} then continue",
                        self.flow_id, MIN_REFRESH_DURATION
                    );
                    tokio::time::sleep(MIN_REFRESH_DURATION).await;
                    continue;
                }
                // TODO(discord9): this error should have better place to go, but for now just print error, also more context is needed
                Err(err) => match new_query {
                    Some(query) => {
                        common_telemetry::error!(err; "Failed to execute query for flow={} with query: {query}", self.flow_id)
                    }
                    None => {
                        common_telemetry::error!(err; "Failed to generate query for flow={}", self.flow_id)
                    }
                },
            }
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
                            .gen_filter_exprs(
                                &col_name,
                                Some(l),
                                window_size,
                                self.flow_id,
                                Some(self),
                            )?
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

    // if no group by clause, return empty
    let pk_final_names = pk_names.get_group_expr_names().unwrap_or_default();
    if pk_final_names.is_empty() {
        return Ok((None, Vec::new()));
    }

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

#[cfg(test)]
mod test {
    use pretty_assertions::assert_eq;
    use session::context::QueryContext;

    use super::*;
    use crate::test_utils::create_test_query_engine;

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
