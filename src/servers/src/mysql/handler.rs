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
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use ::auth::{Identity, Password, UserProviderRef};
use async_trait::async_trait;
use chrono::{NaiveDate, NaiveDateTime};
use common_catalog::parse_optional_catalog_and_schema_from_db_string;
use common_error::ext::ErrorExt;
use common_query::Output;
use common_telemetry::{debug, error, tracing, warn};
use datafusion_common::ParamValues;
use datafusion_expr::LogicalPlan;
use datatypes::prelude::ConcreteDataType;
use itertools::Itertools;
use opensrv_mysql::{
    AsyncMysqlShim, Column, ErrorKind, InitWriter, ParamParser, ParamValue, QueryResultWriter,
    StatementMetaWriter, ValueInner,
};
use parking_lot::RwLock;
use query::planner::DfLogicalPlanner;
use query::query_engine::DescribeResult;
use rand::RngCore;
use session::context::{Channel, QueryContextRef};
use session::{Session, SessionRef};
use snafu::{ResultExt, ensure};
use sql::dialect::MySqlDialect;
use sql::parser::{ParseOptions, ParserContext};
use sql::statements::statement::Statement;
use tokio::io::AsyncWrite;

use crate::SqlPlan;
use crate::error::{
    self, DataFrameSnafu, InferParameterTypesSnafu, InvalidPrepareStatementSnafu, Result,
};
use crate::metrics::METRIC_AUTH_FAILURE;
use crate::mysql::helper::{
    self, fix_placeholder_types, format_placeholder, replace_placeholders, transform_placeholders,
};
use crate::mysql::writer;
use crate::mysql::writer::{create_mysql_column, handle_err};
use crate::query_handler::sql::ServerSqlQueryHandlerRef;

const MYSQL_NATIVE_PASSWORD: &str = "mysql_native_password";
const MYSQL_CLEAR_PASSWORD: &str = "mysql_clear_password";

/// Parameters for the prepared statement
enum Params<'a> {
    /// Parameters passed through protocol
    ProtocolParams(Vec<ParamValue<'a>>),
    /// Parameters passed through cli
    CliParams(Vec<sql::ast::Expr>),
}

impl Params<'_> {
    fn len(&self) -> usize {
        match self {
            Params::ProtocolParams(params) => params.len(),
            Params::CliParams(params) => params.len(),
        }
    }
}

// An intermediate shim for executing MySQL queries.
pub struct MysqlInstanceShim {
    query_handler: ServerSqlQueryHandlerRef,
    salt: [u8; 20],
    session: SessionRef,
    user_provider: Option<UserProviderRef>,
    prepared_stmts: Arc<RwLock<HashMap<String, SqlPlan>>>,
    prepared_stmts_counter: AtomicU32,
    process_id: u32,
    prepared_stmt_cache_size: usize,
}

impl MysqlInstanceShim {
    pub fn create(
        query_handler: ServerSqlQueryHandlerRef,
        user_provider: Option<UserProviderRef>,
        client_addr: SocketAddr,
        process_id: u32,
        prepared_stmt_cache_size: usize,
    ) -> MysqlInstanceShim {
        // init a random salt
        let mut bs = vec![0u8; 20];
        let mut rng = rand::rng();
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
            session: Arc::new(Session::new(
                Some(client_addr),
                Channel::Mysql,
                Default::default(),
                process_id,
            )),
            user_provider,
            prepared_stmts: Default::default(),
            prepared_stmts_counter: AtomicU32::new(1),
            process_id,
            prepared_stmt_cache_size,
        }
    }

    #[tracing::instrument(skip_all, name = "mysql::do_query")]
    async fn do_query(&self, query: &str, query_ctx: QueryContextRef) -> Vec<Result<Output>> {
        if let Some(output) =
            crate::mysql::federated::check(query, query_ctx.clone(), self.session.clone())
        {
            vec![Ok(output)]
        } else {
            self.query_handler.do_query(query, query_ctx.clone()).await
        }
    }

    /// Execute the logical plan and return the output
    async fn do_exec_plan(
        &self,
        query: &str,
        stmt: Option<Statement>,
        plan: LogicalPlan,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        if let Some(output) =
            crate::mysql::federated::check(query, query_ctx.clone(), self.session.clone())
        {
            Ok(output)
        } else {
            self.query_handler.do_exec_plan(stmt, plan, query_ctx).await
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

    /// Save query and logical plan with a given statement key
    fn save_plan(&self, plan: SqlPlan, stmt_key: String) -> Result<()> {
        let mut prepared_stmts = self.prepared_stmts.write();
        let max_capacity = self.prepared_stmt_cache_size;

        let is_update = prepared_stmts.contains_key(&stmt_key);

        if !is_update && prepared_stmts.len() >= max_capacity {
            return error::InternalSnafu {
                err_msg: format!(
                    "Prepared statement cache is full, max capacity: {}",
                    max_capacity
                ),
            }
            .fail();
        }

        let _ = prepared_stmts.insert(stmt_key, plan);
        Ok(())
    }

    /// Retrieve the query and logical plan by a given statement key
    fn plan(&self, stmt_key: &str) -> Option<SqlPlan> {
        let guard = self.prepared_stmts.read();
        guard.get(stmt_key).cloned()
    }

    /// Save the prepared statement and return the parameters and result columns
    async fn do_prepare(
        &mut self,
        raw_query: &str,
        query_ctx: QueryContextRef,
        stmt_key: String,
    ) -> Result<(Vec<Column>, Vec<Column>)> {
        let (query, param_num) = replace_placeholders(raw_query);

        let statement = validate_query(raw_query).await?;

        // We have to transform the placeholder, because DataFusion only parses placeholders
        // in the form of "$i", it can't process "?" right now.
        let statement = transform_placeholders(statement);

        let describe_result = self
            .do_describe(statement.clone(), query_ctx.clone())
            .await?;
        let (mut plan, schema) = if let Some(DescribeResult {
            logical_plan,
            schema,
        }) = describe_result
        {
            (Some(logical_plan), Some(schema))
        } else {
            (None, None)
        };

        let params = if let Some(plan) = &mut plan {
            fix_placeholder_types(plan)?;
            debug!("Plan after fix placeholder types: {:#?}", plan);
            let param_types = DfLogicalPlanner::get_infered_parameter_types(plan)
                .context(InferParameterTypesSnafu)?
                .into_iter()
                .map(|(k, v)| (k, v.map(|v| ConcreteDataType::from_arrow_type(&v))))
                .collect();
            prepared_params(&param_types)?
        } else {
            dummy_params(param_num)?
        };

        let columns = schema
            .as_ref()
            .map(|schema| {
                schema
                    .column_schemas()
                    .iter()
                    .map(|column_schema| {
                        create_mysql_column(&column_schema.data_type, &column_schema.name)
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .transpose()?
            .unwrap_or_default();

        // DataFusion may optimize the plan so that some parameters are not used.
        if params.len() != param_num - 1 {
            self.save_plan(
                SqlPlan {
                    query: query.clone(),
                    statement: Some(statement),
                    plan: None,
                    schema: None,
                },
                stmt_key,
            )
            .map_err(|e| {
                error!(e; "Failed to save prepared statement");
                e
            })?;
        } else {
            self.save_plan(
                SqlPlan {
                    query: query.clone(),
                    statement: Some(statement),
                    plan,
                    schema,
                },
                stmt_key,
            )
            .map_err(|e| {
                error!(e; "Failed to save prepared statement");
                e
            })?;
        }

        Ok((params, columns))
    }

    async fn do_execute(
        &mut self,
        query_ctx: QueryContextRef,
        stmt_key: String,
        params: Params<'_>,
    ) -> Result<Vec<std::result::Result<Output, error::Error>>> {
        let sql_plan = match self.plan(&stmt_key) {
            None => {
                return error::PrepareStatementNotFoundSnafu { name: stmt_key }.fail();
            }
            Some(sql_plan) => sql_plan,
        };

        let outputs = match sql_plan.plan {
            Some(mut plan) => {
                fix_placeholder_types(&mut plan)?;
                let param_types = DfLogicalPlanner::get_infered_parameter_types(&plan)
                    .context(InferParameterTypesSnafu)?
                    .into_iter()
                    .map(|(k, v)| (k, v.map(|v| ConcreteDataType::from_arrow_type(&v))))
                    .collect::<HashMap<_, _>>();

                if params.len() != param_types.len() {
                    return error::InternalSnafu {
                        err_msg: "Prepare statement params number mismatch".to_string(),
                    }
                    .fail();
                }

                let plan = match params {
                    Params::ProtocolParams(params) => {
                        replace_params_with_values(&plan, param_types, &params)
                    }
                    Params::CliParams(params) => {
                        replace_params_with_exprs(&plan, param_types, &params)
                    }
                }?;

                debug!("Mysql execute prepared plan: {}", plan.display_indent());
                vec![
                    self.do_exec_plan(
                        &sql_plan.query,
                        sql_plan.statement.clone(),
                        plan,
                        query_ctx.clone(),
                    )
                    .await,
                ]
            }
            None => {
                let param_strs = match params {
                    Params::ProtocolParams(params) => {
                        params.iter().map(convert_param_value_to_string).collect()
                    }
                    Params::CliParams(params) => params.iter().map(|x| x.to_string()).collect(),
                };
                debug!(
                    "do_execute Replacing with Params: {:?}, Original Query: {}",
                    param_strs, sql_plan.query
                );
                let query = replace_params(param_strs, sql_plan.query);
                debug!("Mysql execute replaced query: {}", query);
                self.do_query(&query, query_ctx.clone()).await
            }
        };

        Ok(outputs)
    }

    /// Remove the prepared statement by a given statement key
    fn do_close(&mut self, stmt_key: String) {
        let mut guard = self.prepared_stmts.write();
        let _ = guard.remove(&stmt_key);
    }

    fn auth_plugin(&self) -> &'static str {
        if self
            .user_provider
            .as_ref()
            .map(|x| x.external())
            .unwrap_or(false)
        {
            MYSQL_CLEAR_PASSWORD
        } else {
            MYSQL_NATIVE_PASSWORD
        }
    }
}

#[async_trait]
impl<W: AsyncWrite + Send + Sync + Unpin> AsyncMysqlShim<W> for MysqlInstanceShim {
    type Error = error::Error;

    fn version(&self) -> String {
        std::env::var("GREPTIMEDB_MYSQL_SERVER_VERSION").unwrap_or_else(|_| "8.4.2".to_string())
    }

    fn connect_id(&self) -> u32 {
        self.process_id
    }

    fn default_auth_plugin(&self) -> &str {
        self.auth_plugin()
    }

    async fn auth_plugin_for_username(&self, _user: &[u8]) -> &'static str {
        self.auth_plugin()
    }

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
                MYSQL_NATIVE_PASSWORD => Password::MysqlNativePassword(auth_data, salt),
                MYSQL_CLEAR_PASSWORD => {
                    // The raw bytes received could be represented in C-like string, ended in '\0'.
                    // We must "trim" it to get the real password string.
                    let password = if let &[password @ .., 0] = &auth_data {
                        password
                    } else {
                        auth_data
                    };
                    Password::PlainText(String::from_utf8_lossy(password).to_string().into())
                }
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
                    METRIC_AUTH_FAILURE
                        .with_label_values(&[e.status_code().as_ref()])
                        .inc();
                    warn!(e; "Failed to auth");
                    return false;
                }
            };
        }
        let user_info =
            user_info.unwrap_or_else(|| auth::userinfo_by_name(Some(username.to_string())));

        self.session.set_user_info(user_info);

        true
    }

    async fn on_prepare<'a>(
        &'a mut self,
        raw_query: &'a str,
        w: StatementMetaWriter<'a, W>,
    ) -> Result<()> {
        let query_ctx = self.session.new_query_context();
        let stmt_id = self.prepared_stmts_counter.fetch_add(1, Ordering::Relaxed);
        let stmt_key = uuid::Uuid::from_u128(stmt_id as u128).to_string();
        let (params, columns) = self
            .do_prepare(raw_query, query_ctx.clone(), stmt_key)
            .await?;
        debug!("on_prepare: Params: {:?}, Columns: {:?}", params, columns);
        w.reply(stmt_id, &params, &columns).await?;
        crate::metrics::METRIC_MYSQL_PREPARED_COUNT
            .with_label_values(&[query_ctx.get_db_string().as_str()])
            .inc();
        return Ok(());
    }

    async fn on_execute<'a>(
        &'a mut self,
        stmt_id: u32,
        p: ParamParser<'a>,
        w: QueryResultWriter<'a, W>,
    ) -> Result<()> {
        self.session.clear_warnings();

        let query_ctx = self.session.new_query_context();
        let db = query_ctx.get_db_string();
        let _timer = crate::metrics::METRIC_MYSQL_QUERY_TIMER
            .with_label_values(&[crate::metrics::METRIC_MYSQL_BINQUERY, db.as_str()])
            .start_timer();

        let params: Vec<ParamValue> = p.into_iter().collect();
        let stmt_key = uuid::Uuid::from_u128(stmt_id as u128).to_string();

        let outputs = match self
            .do_execute(query_ctx.clone(), stmt_key, Params::ProtocolParams(params))
            .await
        {
            Ok(outputs) => outputs,
            Err(e) => {
                let (kind, err) = handle_err(e, query_ctx);
                debug!(
                    "Failed to execute prepared statement, kind: {:?}, err: {}",
                    kind, err
                );
                w.error(kind, err.as_bytes()).await?;
                return Ok(());
            }
        };

        writer::write_output(w, query_ctx, self.session.clone(), outputs).await?;

        Ok(())
    }

    async fn on_close<'a>(&'a mut self, stmt_id: u32)
    where
        W: 'async_trait,
    {
        let stmt_key = uuid::Uuid::from_u128(stmt_id as u128).to_string();
        self.do_close(stmt_key);
    }

    #[tracing::instrument(skip_all, fields(protocol = "mysql"))]
    async fn on_query<'a>(
        &'a mut self,
        query: &'a str,
        writer: QueryResultWriter<'a, W>,
    ) -> Result<()> {
        let query_ctx = self.session.new_query_context();
        let db = query_ctx.get_db_string();
        let _timer = crate::metrics::METRIC_MYSQL_QUERY_TIMER
            .with_label_values(&[crate::metrics::METRIC_MYSQL_TEXTQUERY, db.as_str()])
            .start_timer();

        // Clear warnings for non SHOW WARNINGS queries
        let query_upcase = query.to_uppercase();
        if !query_upcase.starts_with("SHOW WARNINGS") {
            self.session.clear_warnings();
        }

        if query_upcase.starts_with("PREPARE ") {
            match ParserContext::parse_mysql_prepare_stmt(query, query_ctx.sql_dialect()) {
                Ok((stmt_name, stmt)) => {
                    let prepare_results =
                        self.do_prepare(&stmt, query_ctx.clone(), stmt_name).await;
                    match prepare_results {
                        Ok(_) => {
                            let outputs = vec![Ok(Output::new_with_affected_rows(0))];
                            writer::write_output(writer, query_ctx, self.session.clone(), outputs)
                                .await?;
                            return Ok(());
                        }
                        Err(e) => {
                            writer
                                .error(ErrorKind::ER_SP_BADSTATEMENT, e.output_msg().as_bytes())
                                .await?;
                            return Ok(());
                        }
                    }
                }
                Err(e) => {
                    writer
                        .error(ErrorKind::ER_PARSE_ERROR, e.output_msg().as_bytes())
                        .await?;
                    return Ok(());
                }
            }
        } else if query_upcase.starts_with("EXECUTE ") {
            match ParserContext::parse_mysql_execute_stmt(query, query_ctx.sql_dialect()) {
                Ok((stmt_name, params)) => {
                    let outputs = match self
                        .do_execute(query_ctx.clone(), stmt_name, Params::CliParams(params))
                        .await
                    {
                        Ok(outputs) => outputs,
                        Err(e) => {
                            let (kind, err) = handle_err(e, query_ctx);
                            debug!(
                                "Failed to execute prepared statement, kind: {:?}, err: {}",
                                kind, err
                            );
                            writer.error(kind, err.as_bytes()).await?;
                            return Ok(());
                        }
                    };
                    writer::write_output(writer, query_ctx, self.session.clone(), outputs).await?;

                    return Ok(());
                }
                Err(e) => {
                    writer
                        .error(ErrorKind::ER_PARSE_ERROR, e.output_msg().as_bytes())
                        .await?;
                    return Ok(());
                }
            }
        } else if query_upcase.starts_with("DEALLOCATE ") {
            match ParserContext::parse_mysql_deallocate_stmt(query, query_ctx.sql_dialect()) {
                Ok(stmt_name) => {
                    self.do_close(stmt_name);
                    let outputs = vec![Ok(Output::new_with_affected_rows(0))];
                    writer::write_output(writer, query_ctx, self.session.clone(), outputs).await?;
                    return Ok(());
                }
                Err(e) => {
                    writer
                        .error(ErrorKind::ER_PARSE_ERROR, e.output_msg().as_bytes())
                        .await?;
                    return Ok(());
                }
            }
        }

        let outputs = self.do_query(query, query_ctx.clone()).await;
        writer::write_output(writer, query_ctx, self.session.clone(), outputs).await?;

        Ok(())
    }

    async fn on_init<'a>(&'a mut self, database: &'a str, w: InitWriter<'a, W>) -> Result<()> {
        let (catalog_from_db, schema) = parse_optional_catalog_and_schema_from_db_string(database);
        let catalog = if let Some(catalog) = &catalog_from_db {
            catalog.clone()
        } else {
            self.session.catalog()
        };

        if !self
            .query_handler
            .is_valid_schema(&catalog, &schema)
            .await?
        {
            return w
                .error(
                    ErrorKind::ER_WRONG_DB_NAME,
                    format!("Unknown database '{}'", database).as_bytes(),
                )
                .await
                .map_err(|e| e.into());
        }

        let user_info = &self.session.user_info();

        if let Some(schema_validator) = &self.user_provider
            && let Err(e) = schema_validator
                .authorize(&catalog, &schema, user_info)
                .await
        {
            METRIC_AUTH_FAILURE
                .with_label_values(&[e.status_code().as_ref()])
                .inc();
            return w
                .error(
                    ErrorKind::ER_DBACCESS_DENIED_ERROR,
                    e.output_msg().as_bytes(),
                )
                .await
                .map_err(|e| e.into());
        }

        if catalog_from_db.is_some() {
            self.session.set_catalog(catalog)
        }
        self.session.set_schema(schema);

        w.ok().await.map_err(|e| e.into())
    }
}

fn convert_param_value_to_string(param: &ParamValue) -> String {
    match param.value.into_inner() {
        ValueInner::Int(u) => u.to_string(),
        ValueInner::UInt(u) => u.to_string(),
        ValueInner::Double(u) => u.to_string(),
        ValueInner::NULL => "NULL".to_string(),
        ValueInner::Bytes(b) => format!("'{}'", &String::from_utf8_lossy(b)),
        ValueInner::Date(_) => format!("'{}'", NaiveDate::from(param.value)),
        ValueInner::Datetime(_) => format!("'{}'", NaiveDateTime::from(param.value)),
        ValueInner::Time(_) => format_duration(Duration::from(param.value)),
    }
}

fn replace_params(params: Vec<String>, query: String) -> String {
    let mut query = query;
    let mut index = 1;
    for param in params {
        query = query.replace(&format_placeholder(index), &param);
        index += 1;
    }
    query
}

fn format_duration(duration: Duration) -> String {
    let seconds = duration.as_secs() % 60;
    let minutes = (duration.as_secs() / 60) % 60;
    let hours = (duration.as_secs() / 60) / 60;
    format!("'{}:{}:{}'", hours, minutes, seconds)
}

fn replace_params_with_values(
    plan: &LogicalPlan,
    param_types: HashMap<String, Option<ConcreteDataType>>,
    params: &[ParamValue],
) -> Result<LogicalPlan> {
    debug_assert_eq!(param_types.len(), params.len());

    debug!(
        "replace_params_with_values(param_types: {:#?}, params: {:#?}, plan: {:#?})",
        param_types,
        params
            .iter()
            .map(|x| format!("({:?}, {:?})", x.value, x.coltype))
            .join(", "),
        plan
    );

    let mut values = Vec::with_capacity(params.len());

    for (i, param) in params.iter().enumerate() {
        if let Some(Some(t)) = param_types.get(&format_placeholder(i + 1)) {
            let value = helper::convert_value(param, t)?;

            values.push(value.into());
        }
    }

    plan.clone()
        .replace_params_with_values(&ParamValues::List(values.clone()))
        .context(DataFrameSnafu)
}

fn replace_params_with_exprs(
    plan: &LogicalPlan,
    param_types: HashMap<String, Option<ConcreteDataType>>,
    params: &[sql::ast::Expr],
) -> Result<LogicalPlan> {
    debug_assert_eq!(param_types.len(), params.len());

    debug!(
        "replace_params_with_exprs(param_types: {:#?}, params: {:#?}, plan: {:#?})",
        param_types,
        params.iter().map(|x| format!("({:?})", x)).join(", "),
        plan
    );

    let mut values = Vec::with_capacity(params.len());

    for (i, param) in params.iter().enumerate() {
        if let Some(Some(t)) = param_types.get(&format_placeholder(i + 1)) {
            let value = helper::convert_expr_to_scalar_value(param, t)?;

            values.push(value.into());
        }
    }

    plan.clone()
        .replace_params_with_values(&ParamValues::List(values.clone()))
        .context(DataFrameSnafu)
}

async fn validate_query(query: &str) -> Result<Statement> {
    let statement =
        ParserContext::create_with_dialect(query, &MySqlDialect {}, ParseOptions::default());
    let mut statement = statement.map_err(|e| {
        InvalidPrepareStatementSnafu {
            err_msg: e.output_msg(),
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
