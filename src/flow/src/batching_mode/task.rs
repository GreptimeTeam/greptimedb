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
use std::ops::Deref;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use api::v1::CreateTableExpr;
use arrow_schema::Fields;
use common_error::ext::BoxedError;
use common_meta::key::table_name::TableNameKey;
use common_meta::key::TableMetadataManagerRef;
use common_telemetry::tracing::warn;
use common_telemetry::{debug, info};
use common_time::Timestamp;
use datafusion::sql::unparser::expr_to_sql;
use datafusion_common::tree_node::TreeNode;
use datafusion_expr::{DmlStatement, LogicalPlan, WriteOp};
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::constraint::NOW_FN;
use datatypes::schema::{ColumnDefaultConstraint, ColumnSchema};
use datatypes::value::Value;
use operator::expr_helper::column_schemas_to_defs;
use query::query_engine::DefaultSerializer;
use query::QueryEngineRef;
use session::context::QueryContextRef;
use snafu::{OptionExt, ResultExt};
use substrait::{DFLogicalSubstraitConvertor, SubstraitPlan};
use table::metadata::RawTableMeta;
use tokio::sync::oneshot;
use tokio::sync::oneshot::error::TryRecvError;
use tokio::time::Instant;

use crate::adapter::{AUTO_CREATED_PLACEHOLDER_TS_COL, AUTO_CREATED_UPDATE_AT_TS_COL};
use crate::batching_mode::frontend_client::FrontendClient;
use crate::batching_mode::state::TaskState;
use crate::batching_mode::time_window::TimeWindowExpr;
use crate::batching_mode::utils::{
    sql_to_df_plan, AddAutoColumnRewriter, AddFilterRewriter, FindGroupByFinalName,
};
use crate::batching_mode::{
    DEFAULT_BATCHING_ENGINE_QUERY_TIMEOUT, MIN_REFRESH_DURATION, SLOW_QUERY_THRESHOLD,
};
use crate::error::{
    ConvertColumnSchemaSnafu, DatafusionSnafu, DatatypesSnafu, ExternalSnafu, InvalidRequestSnafu,
    SubstraitEncodeLogicalPlanSnafu, TableNotFoundMetaSnafu, TableNotFoundSnafu, UnexpectedSnafu,
};
use crate::metrics::{
    METRIC_FLOW_BATCHING_ENGINE_QUERY_TIME, METRIC_FLOW_BATCHING_ENGINE_SLOW_QUERY,
};
use crate::{Error, FlowId};

/// The task's config, immutable once created
#[derive(Clone)]
pub struct TaskConfig {
    pub flow_id: FlowId,
    pub query: String,
    plan: Arc<LogicalPlan>,
    pub time_window_expr: Option<TimeWindowExpr>,
    /// in seconds
    pub expire_after: Option<i64>,
    sink_table_name: [String; 3],
    pub source_table_names: HashSet<[String; 3]>,
    table_meta: TableMetadataManagerRef,
}

#[derive(Clone)]
pub struct BatchingTask {
    pub config: Arc<TaskConfig>,
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
            config: Arc::new(TaskConfig {
                flow_id,
                query: query.to_string(),
                plan: Arc::new(plan),
                time_window_expr,
                expire_after,
                sink_table_name,
                source_table_names: source_table_names.into_iter().collect(),
                table_meta,
            }),
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
            .unwrap()
            .dirty_time_windows
            .add_lower_bounds(vec![ts].into_iter());

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
        self.gen_exec_once(engine, frontend_client).await
    }

    async fn is_table_exist(&self, table_name: &[String; 3]) -> Result<bool, Error> {
        self.config
            .table_meta
            .table_name_manager()
            .exists(TableNameKey {
                catalog: &table_name[0],
                schema: &table_name[1],
                table: &table_name[2],
            })
            .await
            .map_err(BoxedError::new)
            .context(ExternalSnafu)
    }

    pub async fn gen_exec_once(
        &self,
        engine: &QueryEngineRef,
        frontend_client: &Arc<FrontendClient>,
    ) -> Result<Option<(u32, Duration)>, Error> {
        if let Some(new_query) = self.gen_insert_plan(engine).await? {
            self.execute_logical_plan(frontend_client, &new_query).await
        } else {
            Ok(None)
        }
    }

    pub async fn gen_insert_plan(
        &self,
        engine: &QueryEngineRef,
    ) -> Result<Option<LogicalPlan>, Error> {
        let full_table_name = self.config.sink_table_name.clone().join(".");

        let table_id = self
            .config
            .table_meta
            .table_name_manager()
            .get(common_meta::key::table_name::TableNameKey::new(
                &self.config.sink_table_name[0],
                &self.config.sink_table_name[1],
                &self.config.sink_table_name[2],
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
            .config
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

        let schema: datatypes::schema::Schema = table
            .table_info
            .meta
            .schema
            .clone()
            .try_into()
            .with_context(|_| DatatypesSnafu {
                extra: format!(
                    "Failed to convert schema from raw schema, raw_schema={:?}",
                    table.table_info.meta.schema
                ),
            })?;

        let df_schema = Arc::new(schema.arrow_schema().clone().try_into().with_context(|_| {
            DatafusionSnafu {
                context: format!(
                    "Failed to convert arrow schema to datafusion schema, arrow_schema={:?}",
                    schema.arrow_schema()
                ),
            }
        })?);

        let new_query = self
            .gen_query_with_time_window(engine.clone(), &table.table_info.meta)
            .await?;

        let insert_into = if let Some((new_query, _column_cnt)) = new_query {
            // update_at& time index placeholder (if exists) should have default value
            LogicalPlan::Dml(DmlStatement::new(
                datafusion_common::TableReference::Full {
                    catalog: self.config.sink_table_name[0].clone().into(),
                    schema: self.config.sink_table_name[1].clone().into(),
                    table: self.config.sink_table_name[2].clone().into(),
                },
                df_schema,
                WriteOp::Insert(datafusion_expr::dml::InsertOp::Append),
                Arc::new(new_query),
            ))
        } else {
            return Ok(None);
        };
        Ok(Some(insert_into))
    }

    pub async fn create_table(
        &self,
        frontend_client: &Arc<FrontendClient>,
        expr: CreateTableExpr,
    ) -> Result<(), Error> {
        let db_client = frontend_client.get_database_client().await?;
        db_client
            .database
            .create(expr.clone())
            .await
            .with_context(|_| InvalidRequestSnafu {
                context: format!("Failed to create table with expr: {:?}", expr),
            })?;
        Ok(())
    }

    pub async fn execute_logical_plan(
        &self,
        frontend_client: &Arc<FrontendClient>,
        plan: &LogicalPlan,
    ) -> Result<Option<(u32, Duration)>, Error> {
        let instant = Instant::now();
        let flow_id = self.config.flow_id;
        let db_client = frontend_client.get_database_client().await?;
        let peer_addr = db_client.peer.addr;
        debug!(
            "Executing flow {flow_id}(expire_after={:?} secs) on {:?} with query {}",
            self.config.expire_after, peer_addr, &plan
        );

        let timer = METRIC_FLOW_BATCHING_ENGINE_QUERY_TIME
            .with_label_values(&[flow_id.to_string().as_str()])
            .start_timer();

        let message = DFLogicalSubstraitConvertor {}
            .encode(plan, DefaultSerializer)
            .context(SubstraitEncodeLogicalPlanSnafu)?;

        let req = api::v1::greptime_request::Request::Query(api::v1::QueryRequest {
            query: Some(api::v1::query_request::Query::LogicalPlan(message.to_vec())),
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
                peer_addr, elapsed, &plan
            );
        }

        // record slow query
        if elapsed >= SLOW_QUERY_THRESHOLD {
            warn!(
                "Flow {flow_id} on frontend {} executed for {:?} before complete, query: {}",
                peer_addr, elapsed, &plan
            );
            METRIC_FLOW_BATCHING_ENGINE_SLOW_QUERY
                .with_label_values(&[flow_id.to_string().as_str(), &plan.to_string(), &peer_addr])
                .observe(elapsed.as_secs_f64());
        }

        self.state
            .write()
            .unwrap()
            .after_query_exec(elapsed, res.is_ok());

        let res = res.context(InvalidRequestSnafu {
            context: format!(
                "Failed to execute query for flow={}: \'{}\'",
                self.config.flow_id, &plan
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
                new_query = self.gen_insert_plan(&engine).await?;
                if let Some(new_query) = &new_query {
                    self.execute_logical_plan(&frontend_client, new_query).await
                } else {
                    Ok(None)
                }
            };
            match gen_and_exec().await {
                // normal execute, sleep for some time before doing next query
                Ok(Some(_)) => {
                    let sleep_until = {
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
                        state.get_next_start_query_time(Some(DEFAULT_BATCHING_ENGINE_QUERY_TIMEOUT))
                    };
                    tokio::time::sleep_until(sleep_until).await;
                }
                // no new data, sleep for some time before checking for new data
                Ok(None) => {
                    debug!(
                        "Flow id = {:?} found no new data, sleep for {:?} then continue",
                        self.config.flow_id, MIN_REFRESH_DURATION
                    );
                    tokio::time::sleep(MIN_REFRESH_DURATION).await;
                    continue;
                }
                // TODO(discord9): this error should have better place to go, but for now just print error, also more context is needed
                Err(err) => match new_query {
                    Some(query) => {
                        common_telemetry::error!(err; "Failed to execute query for flow={} with query: {query}", self.config.flow_id)
                    }
                    None => {
                        common_telemetry::error!(err; "Failed to generate query for flow={}", self.config.flow_id)
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
    async fn gen_create_table_expr(
        &self,
        engine: QueryEngineRef,
    ) -> Result<CreateTableExpr, Error> {
        let query_ctx = self.state.read().unwrap().query_ctx.clone();
        let plan =
            sql_to_df_plan(query_ctx.clone(), engine.clone(), &self.config.query, true).await?;
        create_table_with_expr(&plan, &self.config.sink_table_name)
    }

    /// will merge and use the first ten time window in query
    async fn gen_query_with_time_window(
        &self,
        engine: QueryEngineRef,
        sink_table_meta: &RawTableMeta,
    ) -> Result<Option<(LogicalPlan, usize)>, Error> {
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
        let schema_len = self.config.plan.schema().fields().len();

        let expire_time_window_bound = self
            .config
            .time_window_expr
            .as_ref()
            .map(|expr| expr.eval(low_bound))
            .transpose()?;

        let new_plan = {
            let expr = {
                match expire_time_window_bound {
                    Some((Some(l), Some(u))) => {
                        let window_size = u.sub(&l).with_context(|| UnexpectedSnafu {
                            reason: format!("Can't get window size from {u:?} - {l:?}"),
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

                        self.state
                            .write()
                            .unwrap()
                            .dirty_time_windows
                            .gen_filter_exprs(
                                &col_name,
                                Some(l),
                                window_size,
                                self.config.flow_id,
                                Some(self),
                            )?
                    }
                    _ => {
                        // use sink_table_meta to add to query the `update_at` and `__ts_placeholder` column's value too for compatibility reason
                        debug!(
                            "Flow id = {:?}, can't get window size: precise_lower_bound={expire_time_window_bound:?}, using the same query", self.config.flow_id
                        );

                        let mut add_auto_column =
                            AddAutoColumnRewriter::new(sink_table_meta.schema.clone());
                        let plan = self
                            .config
                            .plan
                            .deref()
                            .clone()
                            .rewrite(&mut add_auto_column)
                            .with_context(|_| DatafusionSnafu {
                                context: format!("Failed to rewrite plan {:?}", self.config.plan),
                            })?
                            .data;
                        let schema_len = plan.schema().fields().len();

                        // since no time window lower/upper bound is found, just return the original query(with auto columns)
                        return Ok(Some((plan, schema_len)));
                    }
                }
            };

            debug!(
                "Flow id={:?}, Generated filter expr: {:?}",
                self.config.flow_id,
                expr.as_ref()
                    .map(|expr| expr_to_sql(expr).with_context(|_| DatafusionSnafu {
                        context: format!("Failed to generate filter expr from {expr:?}"),
                    }))
                    .transpose()?
                    .map(|s| s.to_string())
            );

            let Some(expr) = expr else {
                // no new data, hence no need to update
                debug!("Flow id={:?}, no new data, not update", self.config.flow_id);
                return Ok(None);
            };

            let mut add_filter = AddFilterRewriter::new(expr);
            let mut add_auto_column = AddAutoColumnRewriter::new(sink_table_meta.schema.clone());
            // make a not optimized plan for clearer unparse
            let plan = sql_to_df_plan(query_ctx.clone(), engine.clone(), &self.config.query, false)
                .await?;
            plan.clone()
                .rewrite(&mut add_filter)
                .and_then(|p| p.data.rewrite(&mut add_auto_column))
                .with_context(|_| DatafusionSnafu {
                    context: format!("Failed to rewrite plan {plan:?}"),
                })?
                .data
        };

        Ok(Some((new_plan, schema_len)))
    }
}

// auto created table have a auto added column `update_at`, and optional have a `AUTO_CREATED_PLACEHOLDER_TS_COL` column for time index placeholder if no timestamp column is specified
// TODO(discord9): unit test
fn create_table_with_expr(
    plan: &LogicalPlan,
    sink_table_name: &[String; 3],
) -> Result<CreateTableExpr, Error> {
    let fields = plan.schema().fields();
    let (first_time_stamp, primary_keys) = build_primary_key_constraint(plan, fields)?;

    let mut column_schemas = Vec::new();
    for field in fields {
        let name = field.name();
        let ty = ConcreteDataType::from_arrow_type(field.data_type());
        let col_schema = if first_time_stamp == Some(name.clone()) {
            ColumnSchema::new(name, ty, false).with_time_index(true)
        } else {
            ColumnSchema::new(name, ty, true)
        };
        column_schemas.push(col_schema);
    }

    let update_at_schema = ColumnSchema::new(
        AUTO_CREATED_UPDATE_AT_TS_COL,
        ConcreteDataType::timestamp_millisecond_datatype(),
        true,
    )
    .with_default_constraint(Some(ColumnDefaultConstraint::Function(NOW_FN.to_string())))
    .context(DatatypesSnafu {
        extra: "Failed to build column `update_at TimestampMillisecond default now()`",
    })?;
    column_schemas.push(update_at_schema);

    let time_index = if let Some(time_index) = first_time_stamp {
        time_index
    } else {
        column_schemas.push(
            ColumnSchema::new(
                AUTO_CREATED_PLACEHOLDER_TS_COL,
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true)
            .with_default_constraint(Some(ColumnDefaultConstraint::Value(Value::Timestamp(
                Timestamp::new_millisecond(0),
            ))))
            .context(DatatypesSnafu {
                extra: format!(
                    "Failed to build column `{} TimestampMillisecond TIME INDEX default 0`",
                    AUTO_CREATED_PLACEHOLDER_TS_COL
                ),
            })?,
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
    use api::v1::column_def::try_as_column_schema;
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
            column_schemas: Vec<ColumnSchema>,
            primary_keys: Vec<String>,
            time_index: String,
        }

        let update_at_schema = ColumnSchema::new(
            AUTO_CREATED_UPDATE_AT_TS_COL,
            ConcreteDataType::timestamp_millisecond_datatype(),
            true,
        )
        .with_default_constraint(Some(ColumnDefaultConstraint::Function(NOW_FN.to_string())))
        .unwrap();

        let ts_placeholder_schema = ColumnSchema::new(
            AUTO_CREATED_PLACEHOLDER_TS_COL,
            ConcreteDataType::timestamp_millisecond_datatype(),
            false,
        )
        .with_time_index(true)
        .with_default_constraint(Some(ColumnDefaultConstraint::Value(Value::Timestamp(
            Timestamp::new_millisecond(0),
        ))))
        .unwrap();

        let testcases = vec![
            TestCase {
                sql: "SELECT number, ts FROM numbers_with_ts".to_string(),
                sink_table_name: "new_table".to_string(),
                column_schemas: vec![
                    ColumnSchema::new("number", ConcreteDataType::uint32_datatype(), true),
                    ColumnSchema::new(
                        "ts",
                        ConcreteDataType::timestamp_millisecond_datatype(),
                        true,
                    ),
                    update_at_schema.clone(),
                    ts_placeholder_schema.clone(),
                ],
                primary_keys: vec![],
                time_index: AUTO_CREATED_PLACEHOLDER_TS_COL.to_string(),
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
            )
            .unwrap();
            // TODO(discord9): assert expr
            let column_schemas = expr
                .column_defs
                .iter()
                .map(|c| try_as_column_schema(c).unwrap())
                .collect::<Vec<_>>();
            assert_eq!(tc.column_schemas, column_schemas);
            assert_eq!(tc.primary_keys, expr.primary_keys);
            assert_eq!(tc.time_index, expr.time_index);
        }
    }
}
