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
use datatypes::schema::Schema;
use itertools::Itertools;
use mysql_common::Value as MysqlValue;
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
use crate::mysql::helper::{self, format_placeholder, transform_placeholders_with_count};
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
        if crate::mysql::federated::check(raw_query, query_ctx.clone(), self.session.clone())
            .is_some()
        {
            self.save_plan(SqlPlan::Shortcut(raw_query.to_string()), stmt_key)
                .inspect_err(|e| {
                    error!(e; "Failed to save prepared statement");
                })?;
            return Ok((vec![], vec![]));
        }

        let statement = validate_query(raw_query).await?;

        // We have to transform the placeholder, because DataFusion only parses placeholders
        // in the form of "$i", it can't process "?" right now.
        let (statement, placeholder_count) = transform_placeholders_with_count(statement);
        let param_num = placeholder_count + 1;

        let describe_result = self
            .do_describe(statement.clone(), query_ctx.clone())
            .await?;
        let plan = describe_result.map(|DescribeResult { logical_plan }| logical_plan);

        let (params, can_cache_as_plan) = if let Some(plan) = &plan {
            let param_types = DfLogicalPlanner::get_inferred_parameter_types(plan)
                .context(InferParameterTypesSnafu)?
                .into_iter()
                .map(|(k, v)| (k, v.map(|v| ConcreteDataType::from_arrow_type(&v))))
                .collect();

            (
                prepared_params(&param_types, param_num)?,
                all_params_have_types(&param_types, param_num),
            )
        } else {
            (dummy_params(param_num)?, false)
        };

        let columns =
            plan.as_ref()
                .map(|plan| {
                    let schema: Schema = plan.schema().clone().try_into().map_err(
                        |e: datatypes::error::Error| {
                            error::InternalSnafu {
                                err_msg: e.to_string(),
                            }
                            .build()
                        },
                    )?;
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

        match plan {
            Some(plan) if can_cache_as_plan => {
                self.save_plan(SqlPlan::Plan(plan, statement), stmt_key)
                    .inspect_err(|e| {
                        error!(e; "Failed to save prepared statement");
                    })?;
            }
            _ => {
                self.save_plan(
                    SqlPlan::Statement(statement, raw_query.to_string()),
                    stmt_key,
                )
                .inspect_err(|e| {
                    error!(e; "Failed to save prepared statement");
                })?;
            }
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

        let outputs = match sql_plan {
            SqlPlan::Plan(plan, stmt) => {
                let param_types = DfLogicalPlanner::get_inferred_parameter_types(&plan)
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

                let replaced_plan = match params {
                    Params::ProtocolParams(params) => {
                        replace_params_with_values(&plan, param_types, &params)
                    }
                    Params::CliParams(params) => {
                        replace_params_with_exprs(&plan, param_types, &params)
                    }
                }?;

                debug!(
                    "Mysql execute prepared plan: {}",
                    replaced_plan.display_indent()
                );
                vec![
                    self.query_handler
                        .do_exec_plan(replaced_plan, Some(stmt), query_ctx.clone())
                        .await,
                ]
            }
            SqlPlan::Shortcut(query) => {
                if let Some(output) =
                    crate::mysql::federated::check(&query, query_ctx.clone(), self.session.clone())
                {
                    vec![Ok(output)]
                } else {
                    self.do_query(&query, query_ctx.clone()).await
                }
            }
            SqlPlan::Statement(stmt, query) => {
                let param_strs = match params {
                    Params::ProtocolParams(params) => {
                        params.iter().map(convert_param_value_to_string).collect()
                    }
                    Params::CliParams(params) => params.iter().map(|x| x.to_string()).collect(),
                };
                debug!(
                    "do_execute Replacing with Params: {:?}, Original Query: {}",
                    param_strs, query
                );
                let query = replace_params(param_strs, stmt, query)?;
                debug!("Mysql execute replaced query: {}", query);
                self.do_query(&query, query_ctx.clone()).await
            }
            _ => {
                return error::PrepareStatementNotFoundSnafu { name: stmt_key }.fail();
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
        let (params, columns) = match self
            .do_prepare(raw_query, query_ctx.clone(), stmt_key)
            .await
        {
            Ok(x) => x,
            Err(e) => {
                let (kind, msg) = handle_err(e, query_ctx.clone());
                w.error(kind, msg.as_bytes()).await?;
                return Ok(());
            }
        };
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
        // MySQL prepared fallback emits SQL text. Delegate bytes/string literal
        // escaping to mysql_common. `false` means normal MySQL backslash escapes;
        // if NO_BACKSLASH_ESCAPES is supported in this path later, wire the
        // session SQL mode here.
        ValueInner::Bytes(b) => MysqlValue::Bytes(b.to_vec()).as_sql(false),
        ValueInner::Date(_) => format!("'{}'", NaiveDate::from(param.value)),
        ValueInner::Datetime(_) => format!("'{}'", NaiveDateTime::from(param.value)),
        ValueInner::Time(_) => format_duration(Duration::from(param.value)),
    }
}

fn replace_params(params: Vec<String>, stmt: Statement, mut query: String) -> Result<String> {
    let spans = helper::placeholder_spans(stmt);
    ensure!(
        spans.len() == params.len(),
        error::InternalSnafu {
            err_msg: format!(
                "Prepared statement expected {} parameters but got {}",
                spans.len(),
                params.len()
            )
        }
    );

    let mut replacements = Vec::with_capacity(spans.len());
    for span in spans {
        let start = location_to_byte_offset(&query, span.start_line, span.start_column)
            .ok_or_else(|| {
                error::InternalSnafu {
                    err_msg: format!(
                        "Invalid placeholder start span: line {}, column {}",
                        span.start_line, span.start_column
                    ),
                }
                .build()
            })?;
        let end =
            location_to_byte_offset(&query, span.end_line, span.end_column).ok_or_else(|| {
                error::InternalSnafu {
                    err_msg: format!(
                        "Invalid placeholder end span: line {}, column {}",
                        span.end_line, span.end_column
                    ),
                }
                .build()
            })?;
        let param = span
            .index
            .checked_sub(1)
            .and_then(|idx| params.get(idx))
            .ok_or_else(|| {
                error::InternalSnafu {
                    err_msg: format!("Missing prepared statement parameter {}", span.index),
                }
                .build()
            })?;

        ensure!(
            start < end && end <= query.len(),
            error::InternalSnafu {
                err_msg: format!(
                    "Invalid placeholder byte span: {}..{} for query length {}",
                    start,
                    end,
                    query.len()
                )
            }
        );
        ensure!(
            query.get(start..end) == Some("?"),
            error::InternalSnafu {
                err_msg: format!(
                    "Prepared statement placeholder span maps to {:?} instead of '?'",
                    query.get(start..end)
                )
            }
        );

        replacements.push((start, end, param.clone()));
    }

    replacements.sort_unstable_by_key(|(start, _, _)| *start);
    for windows in replacements.windows(2) {
        ensure!(
            windows[0].1 <= windows[1].0,
            error::InternalSnafu {
                err_msg: "Overlapping placeholder spans in prepared statement".to_string()
            }
        );
    }

    // All spans are computed against the original query. Apply replacements
    // from right to left so changing one parameter's string length never shifts
    // the byte offsets of placeholders that have not been replaced yet.
    for (start, end, param) in replacements.into_iter().rev() {
        query.replace_range(start..end, &param);
    }

    Ok(query)
}

fn location_to_byte_offset(query: &str, line: u64, column: u64) -> Option<usize> {
    // sqlparser spans are 1-based line/column locations, and columns advance by
    // Rust `char`s rather than bytes. Convert them to byte offsets before using
    // `String::replace_range` on the original SQL text.
    if line == 0 || column == 0 {
        return None;
    }

    let mut current_line = 1;
    let mut current_column = 1;
    for (index, ch) in query.char_indices() {
        if current_line == line && current_column == column {
            return Some(index);
        }

        if ch == '\n' {
            current_line += 1;
            current_column = 1;
        } else {
            current_column += 1;
        }
    }

    (current_line == line && current_column == column).then_some(query.len())
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
fn prepared_params(
    param_types: &HashMap<String, Option<ConcreteDataType>>,
    index: usize,
) -> Result<Vec<Column>> {
    let mut params = Vec::with_capacity(index - 1);

    // Placeholder index starts from 1
    for index in 1..index {
        let column = if let Some(Some(t)) = param_types.get(&format_placeholder(index)) {
            create_mysql_column(t, "")?
        } else {
            create_mysql_column(&ConcreteDataType::null_datatype(), "")?
        };
        params.push(column);
    }

    Ok(params)
}

fn all_params_have_types(
    param_types: &HashMap<String, Option<ConcreteDataType>>,
    index: usize,
) -> bool {
    param_types.len() == index - 1
        && (1..index)
            .all(|index| matches!(param_types.get(&format_placeholder(index)), Some(Some(_))))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;
    use common_query::Output;
    use datafusion_expr::LogicalPlan;
    use query::parser::PromQuery;
    use query::query_engine::DescribeResult;
    use session::context::QueryContext;
    use sql::statements::statement::Statement;

    use super::*;
    use crate::error::Result;
    use crate::query_handler::sql::SqlQueryHandler;

    struct DummyQueryHandler;

    #[async_trait]
    impl SqlQueryHandler for DummyQueryHandler {
        async fn do_query(&self, _: &str, _: QueryContextRef) -> Vec<Result<Output>> {
            unimplemented!()
        }

        async fn do_promql_query(&self, _: &PromQuery, _: QueryContextRef) -> Vec<Result<Output>> {
            unimplemented!()
        }

        async fn do_exec_plan(
            &self,
            _: LogicalPlan,
            _: Option<Statement>,
            _: QueryContextRef,
        ) -> Result<Output> {
            unimplemented!()
        }

        async fn do_describe(
            &self,
            _: Statement,
            _: QueryContextRef,
        ) -> Result<Option<DescribeResult>> {
            unimplemented!()
        }

        async fn is_valid_schema(&self, _: &str, _: &str) -> Result<bool> {
            Ok(true)
        }
    }

    fn create_shim() -> MysqlInstanceShim {
        MysqlInstanceShim::create(
            Arc::new(DummyQueryHandler),
            None,
            "127.0.0.1:3306".parse().unwrap(),
            1,
            1024,
        )
    }

    fn statement_with_transformed_placeholders(query: &str) -> Statement {
        let mut statements =
            ParserContext::create_with_dialect(query, &MySqlDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(statements.len(), 1);
        transform_placeholders_with_count(statements.remove(0)).0
    }

    #[test]
    fn test_prepared_params_keep_unknown_type_placeholders() {
        let mut param_types = HashMap::new();
        param_types.insert(format_placeholder(1), None);
        param_types.insert(
            format_placeholder(2),
            Some(ConcreteDataType::int32_datatype()),
        );

        let params = prepared_params(&param_types, 3).unwrap();
        assert_eq!(params.len(), 2);
        assert!(!all_params_have_types(&param_types, 3));
    }

    #[test]
    fn test_replace_params_by_placeholder_span() {
        let query = "SELECT ?, ?".to_string();
        let stmt = statement_with_transformed_placeholders(&query);
        let params = vec!["'$2 should stay'".to_string(), "'value'".to_string()];

        assert_eq!(
            "SELECT '$2 should stay', 'value'",
            replace_params(params, stmt, query).unwrap()
        );

        let query = "SELECT ?, ?, ?".to_string();
        let stmt = statement_with_transformed_placeholders(&query);
        let params = vec![
            "'much longer than a placeholder'".to_string(),
            "0".to_string(),
            "'also much longer than a placeholder'".to_string(),
        ];

        assert_eq!(
            "SELECT 'much longer than a placeholder', 0, 'also much longer than a placeholder'",
            replace_params(params, stmt, query).unwrap()
        );

        let query = "SELECT '$1', \"$2\", `$3`, ?, ?".to_string();
        let stmt = statement_with_transformed_placeholders(&query);
        let params = vec!["'1'".to_string(), "'2'".to_string()];

        assert_eq!(
            "SELECT '$1', \"$2\", `$3`, '1', '2'",
            replace_params(params, stmt, query).unwrap()
        );

        let query = "SELECT /* ? */ ? -- ?\n, ?".to_string();
        let stmt = statement_with_transformed_placeholders(&query);
        let params = vec!["'first'".to_string(), "'second'".to_string()];

        assert_eq!(
            "SELECT /* ? */ 'first' -- ?\n, 'second'",
            replace_params(params, stmt, query).unwrap()
        );

        let query = "SELECT '中文', ?".to_string();
        let stmt = statement_with_transformed_placeholders(&query);
        let params = vec!["'value'".to_string()];

        assert_eq!(
            "SELECT '中文', 'value'",
            replace_params(params, stmt, query).unwrap()
        );

        let query = "SELECT '中文',\n  ?".to_string();
        let stmt = statement_with_transformed_placeholders(&query);
        let params = vec!["'value'".to_string()];

        assert_eq!(
            "SELECT '中文',\n  'value'",
            replace_params(params, stmt, query).unwrap()
        );

        let query = "SELECT 'x'\r\n, ?".to_string();
        let stmt = statement_with_transformed_placeholders(&query);
        let params = vec!["'crlf'".to_string()];

        assert_eq!(
            "SELECT 'x'\r\n, 'crlf'",
            replace_params(params, stmt, query).unwrap()
        );

        let query = "SELECT\t?".to_string();
        let stmt = statement_with_transformed_placeholders(&query);
        let params = vec!["NULL".to_string()];

        assert_eq!("SELECT\tNULL", replace_params(params, stmt, query).unwrap());

        let query = "SELECT CAST(? AS INT64), ? + (SELECT ?)".to_string();
        let stmt = statement_with_transformed_placeholders(&query);
        let params = vec!["1".to_string(), "2".to_string(), "3".to_string()];

        assert_eq!(
            "SELECT CAST(1 AS INT64), 2 + (SELECT 3)",
            replace_params(params, stmt, query).unwrap()
        );
    }

    #[tokio::test]
    async fn test_prepare_federated_query() {
        let mut shim = create_shim();
        let query_ctx = QueryContext::arc();
        let stmt_key = "test_federated".to_string();

        let (params, columns) = shim
            .do_prepare(
                "SELECT @@version_comment",
                query_ctx.clone(),
                stmt_key.clone(),
            )
            .await
            .unwrap();

        assert!(params.is_empty());
        assert!(columns.is_empty());

        let plan = shim.plan(&stmt_key).unwrap();
        assert!(matches!(plan, SqlPlan::Shortcut(q) if q == "SELECT @@version_comment"));
    }

    #[tokio::test]
    async fn test_execute_federated_shortcut() {
        let mut shim = create_shim();
        let query_ctx = QueryContext::arc();
        let stmt_key = "test_federated_exec".to_string();

        shim.do_prepare(
            "SELECT @@version_comment",
            query_ctx.clone(),
            stmt_key.clone(),
        )
        .await
        .unwrap();

        let outputs = shim
            .do_execute(query_ctx.clone(), stmt_key, Params::CliParams(vec![]))
            .await
            .unwrap();

        assert_eq!(outputs.len(), 1);
        let output = outputs.into_iter().next().unwrap().unwrap();
        let pretty = output.data.pretty_print().await;
        assert!(pretty.contains("GreptimeDB"));
    }

    #[tokio::test]
    async fn test_prepare_non_federated_query_not_shortcut() {
        let mut shim = create_shim();
        let query_ctx = QueryContext::arc();
        let stmt_key = "test_non_federated".to_string();

        let result = shim
            .do_prepare("SET NAMES utf8", query_ctx.clone(), stmt_key.clone())
            .await;

        assert!(result.is_ok());
        let plan = shim.plan(&stmt_key).unwrap();
        assert!(matches!(plan, SqlPlan::Shortcut(_)));
    }

    #[tokio::test]
    async fn test_execute_set_shortcut() {
        let mut shim = create_shim();
        let query_ctx = QueryContext::arc();
        let stmt_key = "test_set_shortcut".to_string();

        shim.do_prepare("SET NAMES utf8", query_ctx.clone(), stmt_key.clone())
            .await
            .unwrap();

        let outputs = shim
            .do_execute(query_ctx.clone(), stmt_key, Params::CliParams(vec![]))
            .await
            .unwrap();

        assert_eq!(outputs.len(), 1);
        let output = outputs.into_iter().next().unwrap().unwrap();
        match output.data {
            common_query::OutputData::RecordBatches(batches) => {
                let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                assert_eq!(total_rows, 0);
            }
            other => panic!("Expected RecordBatches, got {:?}", other),
        }
    }
}
