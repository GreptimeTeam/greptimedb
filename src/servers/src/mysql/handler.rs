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
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use chrono::{NaiveDate, NaiveDateTime};
use common_error::ext::ErrorExt;
use common_query::Output;
use common_telemetry::{error, logging, timer, trace, warn};
use datatypes::prelude::ConcreteDataType;
use metrics::increment_counter;
use opensrv_mysql::{
    AsyncMysqlShim, Column, ErrorKind, InitWriter, ParamParser, ParamValue, QueryResultWriter,
    StatementMetaWriter, ValueInner,
};
use parking_lot::RwLock;
use query::plan::LogicalPlan;
use query::query_engine::DescribeResult;
use rand::RngCore;
use session::context::{Channel, QueryContextRef};
use session::{Session, SessionRef};
use snafu::{ensure, ResultExt};
use sql::dialect::MySqlDialect;
use sql::parser::ParserContext;
use sql::statements::statement::Statement;
use tokio::io::AsyncWrite;

use crate::auth::{Identity, Password, UserProviderRef};
use crate::error::{self, InvalidPrepareStatementSnafu, Result};
use crate::mysql::helper::{
    self, format_placeholder, replace_placeholders, transform_placeholders,
};
use crate::mysql::writer;
use crate::mysql::writer::create_mysql_column;
use crate::query_handler::sql::ServerSqlQueryHandlerRef;
use crate::SqlPlan;

// An intermediate shim for executing MySQL queries.
pub struct MysqlInstanceShim {
    query_handler: ServerSqlQueryHandlerRef,
    salt: [u8; 20],
    session: SessionRef,
    user_provider: Option<UserProviderRef>,
    prepared_stmts: Arc<RwLock<HashMap<u32, SqlPlan>>>,
    prepared_stmts_counter: AtomicU32,
}

impl MysqlInstanceShim {
    pub fn create(
        query_handler: ServerSqlQueryHandlerRef,
        user_provider: Option<UserProviderRef>,
        client_addr: SocketAddr,
    ) -> MysqlInstanceShim {
        // init a random salt
        let mut bs = vec![0u8; 20];
        let mut rng = rand::thread_rng();
        rng.fill_bytes(bs.as_mut());

        let mut scramble: [u8; 20] = [0; 20];
        for i in 0..20 {
            scramble[i] = bs[i] & 0x7fu8;
            if scramble[i] == b'\0' || scramble[i] == b'$' {
                scramble[i] += 1;
            }
        }

        MysqlInstanceShim {
            query_handler,
            salt: scramble,
            session: Arc::new(Session::new(Some(client_addr), Channel::Mysql)),
            user_provider,
            prepared_stmts: Default::default(),
            prepared_stmts_counter: AtomicU32::new(1),
        }
    }

    async fn do_query(&self, query: &str, query_ctx: QueryContextRef) -> Vec<Result<Output>> {
        trace!("Start executing query: '{}'", query);
        let start = Instant::now();

        let output = if let Some(output) = crate::mysql::federated::check(query, query_ctx.clone())
        {
            vec![Ok(output)]
        } else {
            let trace_id = query.len() as u64;
            common_telemetry::info!("trace_id: {}, query: {}", trace_id, query);

            common_telemetry::TRACE_ID
                .scope(trace_id, async move {
                    self.query_handler.do_query(query, query_ctx).await
                })
                .await
        };

        trace!(
            "Finished executing query: '{}', total time costs in microseconds: {}",
            query,
            start.elapsed().as_micros()
        );
        output
    }

    /// Execute the logical plan and return the output
    async fn do_exec_plan(
        &self,
        query: &str,
        plan: LogicalPlan,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        if let Some(output) = crate::mysql::federated::check(query, query_ctx.clone()) {
            Ok(output)
        } else {
            self.query_handler.do_exec_plan(plan, query_ctx).await
        }
    }

    /// Describe the statement
    async fn do_describe(
        &self,
        statement: Statement,
        query_ctx: QueryContextRef,
    ) -> Result<Option<DescribeResult>> {
        self.query_handler.do_describe(statement, query_ctx).await
    }

    /// Save query and logical plan, return the unique id
    fn save_plan(&self, plan: SqlPlan) -> u32 {
        let stmt_id = self.prepared_stmts_counter.fetch_add(1, Ordering::Relaxed);
        let mut prepared_stmts = self.prepared_stmts.write();
        let _ = prepared_stmts.insert(stmt_id, plan);
        stmt_id
    }

    /// Retrieve the query and logical plan by id
    fn plan(&self, stmt_id: u32) -> Option<SqlPlan> {
        let guard = self.prepared_stmts.read();
        guard.get(&stmt_id).cloned()
    }
}

#[async_trait]
impl<W: AsyncWrite + Send + Sync + Unpin> AsyncMysqlShim<W> for MysqlInstanceShim {
    type Error = error::Error;

    fn salt(&self) -> [u8; 20] {
        self.salt
    }

    async fn authenticate(
        &self,
        auth_plugin: &str,
        username: &[u8],
        salt: &[u8],
        auth_data: &[u8],
    ) -> bool {
        // if not specified then **greptime** will be used
        let username = String::from_utf8_lossy(username);

        let mut user_info = None;
        let addr = self
            .session
            .conn_info()
            .client_addr
            .map(|addr| addr.to_string());
        if let Some(user_provider) = &self.user_provider {
            let user_id = Identity::UserId(&username, addr.as_deref());

            let password = match auth_plugin {
                "mysql_native_password" => Password::MysqlNativePassword(auth_data, salt),
                other => {
                    error!("Unsupported mysql auth plugin: {}", other);
                    return false;
                }
            };
            match user_provider.authenticate(user_id, password).await {
                Ok(userinfo) => {
                    user_info = Some(userinfo);
                }
                Err(e) => {
                    increment_counter!(crate::metrics::METRIC_AUTH_FAILURE);
                    warn!("Failed to auth, err: {:?}", e);
                    return false;
                }
            };
        }
        let user_info = user_info.unwrap_or_default();

        self.session.set_user_info(user_info);

        true
    }

    async fn on_prepare<'a>(
        &'a mut self,
        raw_query: &'a str,
        w: StatementMetaWriter<'a, W>,
    ) -> Result<()> {
        let query_ctx = self.session.new_query_context();
        let (query, param_num) = replace_placeholders(raw_query);

        let statement = validate_query(raw_query).await?;

        // We have to transform the placeholder, because DataFusion only parses placeholders
        // in the form of "$i", it can't process "?" right now.
        let statement = transform_placeholders(statement);

        let describe_result = self
            .do_describe(statement.clone(), query_ctx.clone())
            .await?;
        let (plan, schema) = if let Some(DescribeResult {
            logical_plan,
            schema,
        }) = describe_result
        {
            (Some(logical_plan), Some(schema))
        } else {
            (None, None)
        };

        let params = if let Some(plan) = &plan {
            prepared_params(
                &plan
                    .get_param_types()
                    .context(error::GetPreparedStmtParamsSnafu)?,
            )?
        } else {
            dummy_params(param_num)?
        };

        debug_assert_eq!(params.len(), param_num - 1);

        let stmt_id = self.save_plan(SqlPlan {
            query: query.to_string(),
            plan,
            schema,
        });

        w.reply(stmt_id, &params, &[]).await?;
        increment_counter!(
            crate::metrics::METRIC_MYSQL_PREPARED_COUNT,
            &[(crate::metrics::METRIC_DB_LABEL, query_ctx.get_db_string())]
        );
        return Ok(());
    }

    async fn on_execute<'a>(
        &'a mut self,
        stmt_id: u32,
        p: ParamParser<'a>,
        w: QueryResultWriter<'a, W>,
    ) -> Result<()> {
        let query_ctx = self.session.new_query_context();
        let _timer = timer!(
            crate::metrics::METRIC_MYSQL_QUERY_TIMER,
            &[
                (
                    crate::metrics::METRIC_MYSQL_SUBPROTOCOL_LABEL,
                    crate::metrics::METRIC_MYSQL_BINQUERY.to_string()
                ),
                (crate::metrics::METRIC_DB_LABEL, query_ctx.get_db_string())
            ]
        );
        let params: Vec<ParamValue> = p.into_iter().collect();
        let sql_plan = match self.plan(stmt_id) {
            None => {
                w.error(
                    ErrorKind::ER_UNKNOWN_STMT_HANDLER,
                    b"prepare statement not exist",
                )
                .await?;
                return Ok(());
            }
            Some(sql_plan) => sql_plan,
        };

        let (query, outputs) = match sql_plan.plan {
            Some(plan) => {
                let param_types = plan
                    .get_param_types()
                    .context(error::GetPreparedStmtParamsSnafu)?;

                if params.len() != param_types.len() {
                    return error::InternalSnafu {
                        err_msg: "prepare statement params number mismatch".to_string(),
                    }
                    .fail();
                }
                let plan = replace_params_with_values(&plan, param_types, params)?;
                logging::debug!("Mysql execute prepared plan: {}", plan.display_indent());
                let outputs = vec![
                    self.do_exec_plan(&sql_plan.query, plan, query_ctx.clone())
                        .await,
                ];

                (sql_plan.query, outputs)
            }
            None => {
                let query = replace_params(params, sql_plan.query);
                logging::debug!("Mysql execute replaced query: {}", query);
                let outputs = self.do_query(&query, query_ctx.clone()).await;

                (query, outputs)
            }
        };

        writer::write_output(w, &query, query_ctx, outputs).await?;

        Ok(())
    }

    async fn on_close<'a>(&'a mut self, stmt_id: u32)
    where
        W: 'async_trait,
    {
        let mut guard = self.prepared_stmts.write();
        let _ = guard.remove(&stmt_id);
    }

    async fn on_query<'a>(
        &'a mut self,
        query: &'a str,
        writer: QueryResultWriter<'a, W>,
    ) -> Result<()> {
        let query_ctx = self.session.new_query_context();
        let _timer = timer!(
            crate::metrics::METRIC_MYSQL_QUERY_TIMER,
            &[
                (
                    crate::metrics::METRIC_MYSQL_SUBPROTOCOL_LABEL,
                    crate::metrics::METRIC_MYSQL_TEXTQUERY.to_string()
                ),
                (crate::metrics::METRIC_DB_LABEL, query_ctx.get_db_string())
            ]
        );
        let outputs = self.do_query(query, query_ctx.clone()).await;
        writer::write_output(writer, query, query_ctx, outputs).await?;
        Ok(())
    }

    async fn on_init<'a>(&'a mut self, database: &'a str, w: InitWriter<'a, W>) -> Result<()> {
        let (catalog, schema) = crate::parse_catalog_and_schema_from_client_database_name(database);

        if !self.query_handler.is_valid_schema(catalog, schema).await? {
            return w
                .error(
                    ErrorKind::ER_WRONG_DB_NAME,
                    format!("Unknown database '{}'", database).as_bytes(),
                )
                .await
                .map_err(|e| e.into());
        }

        let user_info = &self.session.user_info();

        if let Some(schema_validator) = &self.user_provider {
            if let Err(e) = schema_validator.authorize(catalog, schema, user_info).await {
                increment_counter!(
                    crate::metrics::METRIC_AUTH_FAILURE,
                    &[(
                        crate::metrics::METRIC_CODE_LABEL,
                        format!("{}", e.status_code())
                    )]
                );
                return w
                    .error(
                        ErrorKind::ER_DBACCESS_DENIED_ERROR,
                        e.to_string().as_bytes(),
                    )
                    .await
                    .map_err(|e| e.into());
            }
        }

        self.session.set_catalog(catalog.into());
        self.session.set_schema(schema.into());

        w.ok().await.map_err(|e| e.into())
    }
}

fn replace_params(params: Vec<ParamValue>, query: String) -> String {
    let mut query = query;
    let mut index = 1;
    for param in params {
        let s = match param.value.into_inner() {
            ValueInner::Int(u) => u.to_string(),
            ValueInner::UInt(u) => u.to_string(),
            ValueInner::Double(u) => u.to_string(),
            ValueInner::NULL => "NULL".to_string(),
            ValueInner::Bytes(b) => format!("'{}'", &String::from_utf8_lossy(b)),
            ValueInner::Date(_) => NaiveDate::from(param.value).to_string(),
            ValueInner::Datetime(_) => NaiveDateTime::from(param.value).to_string(),
            ValueInner::Time(_) => format_duration(Duration::from(param.value)),
        };
        query = query.replace(&format_placeholder(index), &s);
        index += 1;
    }
    query
}

fn format_duration(duration: Duration) -> String {
    let seconds = duration.as_secs() % 60;
    let minutes = (duration.as_secs() / 60) % 60;
    let hours = (duration.as_secs() / 60) / 60;
    format!("{}:{}:{}", hours, minutes, seconds)
}

fn replace_params_with_values(
    plan: &LogicalPlan,
    param_types: HashMap<String, Option<ConcreteDataType>>,
    params: Vec<ParamValue>,
) -> Result<LogicalPlan> {
    debug_assert_eq!(param_types.len(), params.len());

    let mut values = Vec::with_capacity(params.len());

    for (i, param) in params.iter().enumerate() {
        if let Some(Some(t)) = param_types.get(&format_placeholder(i + 1)) {
            let value = helper::convert_value(param, t)?;

            values.push(value);
        }
    }

    plan.replace_params_with_values(&values)
        .context(error::ReplacePreparedStmtParamsSnafu)
}

async fn validate_query(query: &str) -> Result<Statement> {
    let statement = ParserContext::create_with_dialect(query, &MySqlDialect {});
    let mut statement = statement.map_err(|e| {
        InvalidPrepareStatementSnafu {
            err_msg: e.to_string(),
        }
        .build()
    })?;

    ensure!(
        statement.len() == 1,
        InvalidPrepareStatementSnafu {
            err_msg: "prepare statement only support single statement".to_string(),
        }
    );

    let statement = statement.remove(0);

    Ok(statement)
}

fn dummy_params(index: usize) -> Result<Vec<Column>> {
    let mut params = Vec::with_capacity(index - 1);

    for _ in 1..index {
        params.push(create_mysql_column(&ConcreteDataType::null_datatype(), "")?);
    }

    Ok(params)
}

/// Parameters that the client must provide when executing the prepared statement.
fn prepared_params(param_types: &HashMap<String, Option<ConcreteDataType>>) -> Result<Vec<Column>> {
    let mut params = Vec::with_capacity(param_types.len());

    // Placeholder index starts from 1
    for index in 1..=param_types.len() {
        if let Some(Some(t)) = param_types.get(&format_placeholder(index)) {
            let column = create_mysql_column(t, "")?;
            params.push(column);
        }
    }

    Ok(params)
}
