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

pub mod builder;
mod grpc;
mod influxdb;
mod log_handler;
mod opentsdb;
mod otlp;
mod prom_store;
mod region_query;
mod script;
pub mod standalone;

use std::sync::Arc;

use async_trait::async_trait;
use auth::{PermissionChecker, PermissionCheckerRef, PermissionReq};
use catalog::CatalogManagerRef;
use client::OutputData;
use common_base::Plugins;
use common_config::KvBackendConfig;
use common_error::ext::{BoxedError, ErrorExt};
use common_meta::key::TableMetadataManagerRef;
use common_meta::kv_backend::KvBackendRef;
use common_meta::state_store::KvStateStore;
use common_procedure::local::{LocalManager, ManagerConfig};
use common_procedure::options::ProcedureConfig;
use common_procedure::ProcedureManagerRef;
use common_query::Output;
use common_telemetry::{debug, error, tracing};
use datafusion_expr::LogicalPlan;
use log_store::raft_engine::RaftEngineBackend;
use operator::delete::DeleterRef;
use operator::insert::InserterRef;
use operator::statement::StatementExecutor;
use pipeline::pipeline_operator::PipelineOperator;
use prometheus::HistogramTimer;
use query::metrics::OnDone;
use query::parser::{PromQuery, QueryLanguageParser, QueryStatement};
use query::query_engine::options::{validate_catalog_and_schema, QueryOptions};
use query::query_engine::DescribeResult;
use query::QueryEngineRef;
use raft_engine::{Config, ReadableSize, RecoveryMode};
use servers::error as server_error;
use servers::error::{AuthSnafu, ExecuteQuerySnafu, ParsePromQLSnafu};
use servers::export_metrics::ExportMetricsTask;
use servers::interceptor::{
    PromQueryInterceptor, PromQueryInterceptorRef, SqlQueryInterceptor, SqlQueryInterceptorRef,
};
use servers::prometheus_handler::PrometheusHandler;
use servers::query_handler::grpc::GrpcQueryHandler;
use servers::query_handler::sql::SqlQueryHandler;
use servers::query_handler::{
    InfluxdbLineProtocolHandler, LogHandler, OpenTelemetryProtocolHandler, OpentsdbProtocolHandler,
    PromStoreProtocolHandler, ScriptHandler,
};
use servers::server::ServerHandlers;
use session::context::QueryContextRef;
use session::table_name::table_idents_to_full_name;
use snafu::prelude::*;
use sql::dialect::Dialect;
use sql::parser::{ParseOptions, ParserContext};
use sql::statements::copy::{CopyDatabase, CopyTable};
use sql::statements::statement::Statement;
use sqlparser::ast::ObjectName;
pub use standalone::StandaloneDatanodeManager;

use self::prom_store::ExportMetricHandler;
use crate::error::{
    self, Error, ExecLogicalPlanSnafu, ExecutePromqlSnafu, ExternalSnafu, InvalidSqlSnafu,
    ParseSqlSnafu, PermissionSnafu, PlanStatementSnafu, Result, SqlExecInterceptedSnafu,
    StartServerSnafu, TableOperationSnafu,
};
use crate::frontend::FrontendOptions;
use crate::heartbeat::HeartbeatTask;
use crate::script::ScriptExecutor;

#[async_trait]
pub trait FrontendInstance:
    GrpcQueryHandler<Error = Error>
    + SqlQueryHandler<Error = Error>
    + OpentsdbProtocolHandler
    + InfluxdbLineProtocolHandler
    + PromStoreProtocolHandler
    + OpenTelemetryProtocolHandler
    + ScriptHandler
    + PrometheusHandler
    + LogHandler
    + Send
    + Sync
    + 'static
{
    async fn start(&self) -> Result<()>;
}

pub type FrontendInstanceRef = Arc<dyn FrontendInstance>;

#[derive(Clone)]
pub struct Instance {
    options: FrontendOptions,
    catalog_manager: CatalogManagerRef,
    script_executor: Arc<ScriptExecutor>,
    pipeline_operator: Arc<PipelineOperator>,
    statement_executor: Arc<StatementExecutor>,
    query_engine: QueryEngineRef,
    plugins: Plugins,
    servers: ServerHandlers,
    heartbeat_task: Option<HeartbeatTask>,
    inserter: InserterRef,
    deleter: DeleterRef,
    export_metrics_task: Option<ExportMetricsTask>,
    table_metadata_manager: TableMetadataManagerRef,
}

impl Instance {
    pub async fn try_build_standalone_components(
        dir: String,
        kv_backend_config: KvBackendConfig,
        procedure_config: ProcedureConfig,
    ) -> Result<(KvBackendRef, ProcedureManagerRef)> {
        let kv_backend = Arc::new(
            RaftEngineBackend::try_open_with_cfg(Config {
                dir,
                purge_threshold: ReadableSize(kv_backend_config.purge_threshold.0),
                recovery_mode: RecoveryMode::TolerateTailCorruption,
                batch_compression_threshold: ReadableSize::kb(8),
                target_file_size: ReadableSize(kv_backend_config.file_size.0),
                ..Default::default()
            })
            .map_err(BoxedError::new)
            .context(error::OpenRaftEngineBackendSnafu)?,
        );

        let state_store = Arc::new(KvStateStore::new(kv_backend.clone()));

        let manager_config = ManagerConfig {
            max_retry_times: procedure_config.max_retry_times,
            retry_delay: procedure_config.retry_delay,
            ..Default::default()
        };
        let procedure_manager = Arc::new(LocalManager::new(manager_config, state_store));

        Ok((kv_backend, procedure_manager))
    }

    pub fn build_servers(&mut self, servers: ServerHandlers) -> Result<()> {
        self.export_metrics_task =
            ExportMetricsTask::try_new(&self.options.export_metrics, Some(&self.plugins))
                .context(StartServerSnafu)?;

        self.servers = servers;
        Ok(())
    }

    pub fn catalog_manager(&self) -> &CatalogManagerRef {
        &self.catalog_manager
    }

    pub fn plugins(&self) -> Plugins {
        self.plugins.clone()
    }

    pub async fn shutdown(&self) -> Result<()> {
        self.servers
            .shutdown_all()
            .await
            .context(error::ShutdownServerSnafu)
    }

    pub fn server_handlers(&self) -> &ServerHandlers {
        &self.servers
    }

    pub fn statement_executor(&self) -> Arc<StatementExecutor> {
        self.statement_executor.clone()
    }

    pub fn table_metadata_manager(&self) -> &TableMetadataManagerRef {
        &self.table_metadata_manager
    }
}

#[async_trait]
impl FrontendInstance for Instance {
    async fn start(&self) -> Result<()> {
        if let Some(heartbeat_task) = &self.heartbeat_task {
            heartbeat_task.start().await?;
        }

        self.script_executor.start(self)?;

        if let Some(t) = self.export_metrics_task.as_ref() {
            if t.send_by_handler {
                let handler = ExportMetricHandler::new_handler(
                    self.inserter.clone(),
                    self.statement_executor.clone(),
                );
                t.start(Some(handler)).context(StartServerSnafu)?
            } else {
                t.start(None).context(StartServerSnafu)?;
            }
        }

        self.servers.start_all().await.context(StartServerSnafu)
    }
}

fn parse_stmt(sql: &str, dialect: &(dyn Dialect + Send + Sync)) -> Result<Vec<Statement>> {
    ParserContext::create_with_dialect(sql, dialect, ParseOptions::default()).context(ParseSqlSnafu)
}

impl Instance {
    async fn query_statement(&self, stmt: Statement, query_ctx: QueryContextRef) -> Result<Output> {
        check_permission(self.plugins.clone(), &stmt, &query_ctx)?;

        let stmt = QueryStatement::Sql(stmt);
        self.statement_executor
            .execute_stmt(stmt, query_ctx)
            .await
            .context(TableOperationSnafu)
    }
}

#[async_trait]
impl SqlQueryHandler for Instance {
    type Error = Error;

    #[tracing::instrument(skip_all)]
    async fn do_query(&self, query: &str, query_ctx: QueryContextRef) -> Vec<Result<Output>> {
        let query_interceptor_opt = self.plugins.get::<SqlQueryInterceptorRef<Error>>();
        let query_interceptor = query_interceptor_opt.as_ref();
        let query = match query_interceptor.pre_parsing(query, query_ctx.clone()) {
            Ok(q) => q,
            Err(e) => return vec![Err(e)],
        };

        let checker_ref = self.plugins.get::<PermissionCheckerRef>();
        let checker = checker_ref.as_ref();

        match parse_stmt(query.as_ref(), query_ctx.sql_dialect())
            .and_then(|stmts| query_interceptor.post_parsing(stmts, query_ctx.clone()))
        {
            Ok(stmts) => {
                let mut results = Vec::with_capacity(stmts.len());
                for stmt in stmts {
                    // TODO(sunng87): figure out at which stage we can call
                    // this hook after ArrowFlight adoption. We need to provide
                    // LogicalPlan as to this hook.
                    if let Err(e) = query_interceptor.pre_execute(&stmt, None, query_ctx.clone()) {
                        results.push(Err(e));
                        break;
                    }

                    if let Err(e) = checker
                        .check_permission(
                            query_ctx.current_user(),
                            PermissionReq::SqlStatement(&stmt),
                        )
                        .context(PermissionSnafu)
                    {
                        results.push(Err(e));
                        break;
                    }

                    match self.query_statement(stmt.clone(), query_ctx.clone()).await {
                        Ok(output) => {
                            let output_result =
                                query_interceptor.post_execute(output, query_ctx.clone());
                            results.push(output_result);
                        }
                        Err(e) => {
                            if e.status_code().should_log_error() {
                                error!(e; "Failed to execute query: {stmt}");
                            } else {
                                debug!("Failed to execute query: {stmt}, {e}");
                            }
                            results.push(Err(e));
                            break;
                        }
                    }
                }
                results
            }
            Err(e) => {
                vec![Err(e)]
            }
        }
    }

    async fn do_exec_plan(&self, plan: LogicalPlan, query_ctx: QueryContextRef) -> Result<Output> {
        // plan should be prepared before exec
        // we'll do check there
        self.query_engine
            .execute(plan, query_ctx)
            .await
            .context(ExecLogicalPlanSnafu)
    }

    #[tracing::instrument(skip_all)]
    async fn do_promql_query(
        &self,
        query: &PromQuery,
        query_ctx: QueryContextRef,
    ) -> Vec<Result<Output>> {
        // check will be done in prometheus handler's do_query
        let result = PrometheusHandler::do_query(self, query, query_ctx)
            .await
            .with_context(|_| ExecutePromqlSnafu {
                query: format!("{query:?}"),
            });
        vec![result]
    }

    async fn do_describe(
        &self,
        stmt: Statement,
        query_ctx: QueryContextRef,
    ) -> Result<Option<DescribeResult>> {
        if matches!(
            stmt,
            Statement::Insert(_) | Statement::Query(_) | Statement::Delete(_)
        ) {
            self.plugins
                .get::<PermissionCheckerRef>()
                .as_ref()
                .check_permission(query_ctx.current_user(), PermissionReq::SqlStatement(&stmt))
                .context(PermissionSnafu)?;

            let plan = self
                .query_engine
                .planner()
                .plan(QueryStatement::Sql(stmt), query_ctx.clone())
                .await
                .context(PlanStatementSnafu)?;
            self.query_engine
                .describe(plan, query_ctx)
                .await
                .map(Some)
                .context(error::DescribeStatementSnafu)
        } else {
            Ok(None)
        }
    }

    async fn is_valid_schema(&self, catalog: &str, schema: &str) -> Result<bool> {
        self.catalog_manager
            .schema_exists(catalog, schema, None)
            .await
            .context(error::CatalogSnafu)
            .map(|b| b && !self.catalog_manager.is_reserved_schema_name(schema))
    }
}

/// Attaches a timer to the output and observes it once the output is exhausted.
pub fn attach_timer(output: Output, timer: HistogramTimer) -> Output {
    match output.data {
        OutputData::AffectedRows(_) | OutputData::RecordBatches(_) => output,
        OutputData::Stream(stream) => {
            let stream = OnDone::new(stream, move || {
                timer.observe_duration();
            });
            Output::new(OutputData::Stream(Box::pin(stream)), output.meta)
        }
    }
}

#[async_trait]
impl PrometheusHandler for Instance {
    #[tracing::instrument(skip_all)]
    async fn do_query(
        &self,
        query: &PromQuery,
        query_ctx: QueryContextRef,
    ) -> server_error::Result<Output> {
        let interceptor = self
            .plugins
            .get::<PromQueryInterceptorRef<server_error::Error>>();
        interceptor.pre_execute(query, query_ctx.clone())?;

        self.plugins
            .get::<PermissionCheckerRef>()
            .as_ref()
            .check_permission(query_ctx.current_user(), PermissionReq::PromQuery)
            .context(AuthSnafu)?;

        let stmt = QueryLanguageParser::parse_promql(query, &query_ctx).with_context(|_| {
            ParsePromQLSnafu {
                query: query.clone(),
            }
        })?;

        let output = self
            .statement_executor
            .execute_stmt(stmt, query_ctx.clone())
            .await
            .map_err(BoxedError::new)
            .context(ExecuteQuerySnafu)?;

        Ok(interceptor.post_execute(output, query_ctx)?)
    }

    fn catalog_manager(&self) -> CatalogManagerRef {
        self.catalog_manager.clone()
    }
}

/// Validate `stmt.database` permission if it's presented.
macro_rules! validate_db_permission {
    ($stmt: expr, $query_ctx: expr) => {
        if let Some(database) = &$stmt.database {
            validate_catalog_and_schema($query_ctx.current_catalog(), database, $query_ctx)
                .map_err(BoxedError::new)
                .context(SqlExecInterceptedSnafu)?;
        }
    };
}

pub fn check_permission(
    plugins: Plugins,
    stmt: &Statement,
    query_ctx: &QueryContextRef,
) -> Result<()> {
    let need_validate = plugins
        .get::<QueryOptions>()
        .map(|opts| opts.disallow_cross_catalog_query)
        .unwrap_or_default();

    if !need_validate {
        return Ok(());
    }

    match stmt {
        // Will be checked in execution.
        // TODO(dennis): add a hook for admin commands.
        Statement::Admin(_) => {}
        // These are executed by query engine, and will be checked there.
        Statement::Query(_) | Statement::Explain(_) | Statement::Tql(_) | Statement::Delete(_) => {}
        // database ops won't be checked
        Statement::CreateDatabase(_)
        | Statement::ShowDatabases(_)
        | Statement::DropDatabase(_)
        | Statement::DropFlow(_)
        | Statement::Use(_) => {}
        Statement::ShowCreateDatabase(stmt) => {
            validate_database(&stmt.database_name, query_ctx)?;
        }
        Statement::ShowCreateTable(stmt) => {
            validate_param(&stmt.table_name, query_ctx)?;
        }
        Statement::ShowCreateFlow(stmt) => {
            validate_param(&stmt.flow_name, query_ctx)?;
        }
        Statement::ShowCreateView(stmt) => {
            validate_param(&stmt.view_name, query_ctx)?;
        }
        Statement::CreateExternalTable(stmt) => {
            validate_param(&stmt.name, query_ctx)?;
        }
        Statement::CreateFlow(stmt) => {
            // TODO: should also validate source table name here?
            validate_param(&stmt.sink_table_name, query_ctx)?;
        }
        Statement::CreateView(stmt) => {
            validate_param(&stmt.name, query_ctx)?;
        }
        Statement::Alter(stmt) => {
            validate_param(stmt.table_name(), query_ctx)?;
        }
        // set/show variable now only alter/show variable in session
        Statement::SetVariables(_) | Statement::ShowVariables(_) => {}
        // show charset and show collation won't be checked
        Statement::ShowCharset(_) | Statement::ShowCollation(_) => {}

        Statement::Insert(insert) => {
            validate_param(insert.table_name(), query_ctx)?;
        }
        Statement::CreateTable(stmt) => {
            validate_param(&stmt.name, query_ctx)?;
        }
        Statement::CreateTableLike(stmt) => {
            validate_param(&stmt.table_name, query_ctx)?;
            validate_param(&stmt.source_name, query_ctx)?;
        }
        Statement::DropTable(drop_stmt) => {
            for table_name in drop_stmt.table_names() {
                validate_param(table_name, query_ctx)?;
            }
        }
        Statement::DropView(stmt) => {
            validate_param(&stmt.view_name, query_ctx)?;
        }
        Statement::ShowTables(stmt) => {
            validate_db_permission!(stmt, query_ctx);
        }
        Statement::ShowTableStatus(stmt) => {
            validate_db_permission!(stmt, query_ctx);
        }
        Statement::ShowColumns(stmt) => {
            validate_db_permission!(stmt, query_ctx);
        }
        Statement::ShowIndex(stmt) => {
            validate_db_permission!(stmt, query_ctx);
        }
        Statement::ShowViews(stmt) => {
            validate_db_permission!(stmt, query_ctx);
        }
        Statement::ShowFlows(stmt) => {
            validate_db_permission!(stmt, query_ctx);
        }
        Statement::ShowStatus(_stmt) => {}
        Statement::DescribeTable(stmt) => {
            validate_param(stmt.name(), query_ctx)?;
        }
        Statement::Copy(sql::statements::copy::Copy::CopyTable(stmt)) => match stmt {
            CopyTable::To(copy_table_to) => validate_param(&copy_table_to.table_name, query_ctx)?,
            CopyTable::From(copy_table_from) => {
                validate_param(&copy_table_from.table_name, query_ctx)?
            }
        },
        Statement::Copy(sql::statements::copy::Copy::CopyDatabase(copy_database)) => {
            match copy_database {
                CopyDatabase::To(stmt) => validate_database(&stmt.database_name, query_ctx)?,
                CopyDatabase::From(stmt) => validate_database(&stmt.database_name, query_ctx)?,
            }
        }
        Statement::TruncateTable(stmt) => {
            validate_param(stmt.table_name(), query_ctx)?;
        }
    }
    Ok(())
}

fn validate_param(name: &ObjectName, query_ctx: &QueryContextRef) -> Result<()> {
    let (catalog, schema, _) = table_idents_to_full_name(name, query_ctx)
        .map_err(BoxedError::new)
        .context(ExternalSnafu)?;

    validate_catalog_and_schema(&catalog, &schema, query_ctx)
        .map_err(BoxedError::new)
        .context(SqlExecInterceptedSnafu)
}

fn validate_database(name: &ObjectName, query_ctx: &QueryContextRef) -> Result<()> {
    let (catalog, schema) = match &name.0[..] {
        [schema] => (
            query_ctx.current_catalog().to_string(),
            schema.value.clone(),
        ),
        [catalog, schema] => (catalog.value.clone(), schema.value.clone()),
        _ => InvalidSqlSnafu {
            err_msg: format!(
                "expect database name to be <catalog>.<schema> or <schema>, actual: {name}",
            ),
        }
        .fail()?,
    };

    validate_catalog_and_schema(&catalog, &schema, query_ctx)
        .map_err(BoxedError::new)
        .context(SqlExecInterceptedSnafu)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use common_base::Plugins;
    use query::query_engine::options::QueryOptions;
    use session::context::QueryContext;
    use sql::dialect::GreptimeDbDialect;
    use strfmt::Format;

    use super::*;

    #[test]
    fn test_exec_validation() {
        let query_ctx = QueryContext::arc();
        let plugins: Plugins = Plugins::new();
        plugins.insert(QueryOptions {
            disallow_cross_catalog_query: true,
        });

        let sql = r#"
        SELECT * FROM demo;
        EXPLAIN SELECT * FROM demo;
        CREATE DATABASE test_database;
        SHOW DATABASES;
        "#;
        let stmts = parse_stmt(sql, &GreptimeDbDialect {}).unwrap();
        assert_eq!(stmts.len(), 4);
        for stmt in stmts {
            let re = check_permission(plugins.clone(), &stmt, &query_ctx);
            re.unwrap();
        }

        let sql = r#"
        SHOW CREATE TABLE demo;
        ALTER TABLE demo ADD COLUMN new_col INT;
        "#;
        let stmts = parse_stmt(sql, &GreptimeDbDialect {}).unwrap();
        assert_eq!(stmts.len(), 2);
        for stmt in stmts {
            let re = check_permission(plugins.clone(), &stmt, &query_ctx);
            re.unwrap();
        }

        fn replace_test(template_sql: &str, plugins: Plugins, query_ctx: &QueryContextRef) {
            // test right
            let right = vec![("", ""), ("", "public."), ("greptime.", "public.")];
            for (catalog, schema) in right {
                let sql = do_fmt(template_sql, catalog, schema);
                do_test(&sql, plugins.clone(), query_ctx, true);
            }

            let wrong = vec![
                ("wrongcatalog.", "public."),
                ("wrongcatalog.", "wrongschema."),
            ];
            for (catalog, schema) in wrong {
                let sql = do_fmt(template_sql, catalog, schema);
                do_test(&sql, plugins.clone(), query_ctx, false);
            }
        }

        fn do_fmt(template: &str, catalog: &str, schema: &str) -> String {
            let vars = HashMap::from([
                ("catalog".to_string(), catalog),
                ("schema".to_string(), schema),
            ]);
            template.format(&vars).unwrap()
        }

        fn do_test(sql: &str, plugins: Plugins, query_ctx: &QueryContextRef, is_ok: bool) {
            let stmt = &parse_stmt(sql, &GreptimeDbDialect {}).unwrap()[0];
            let re = check_permission(plugins, stmt, query_ctx);
            if is_ok {
                re.unwrap();
            } else {
                assert!(re.is_err());
            }
        }

        // test insert
        let sql = "INSERT INTO {catalog}{schema}monitor(host) VALUES ('host1');";
        replace_test(sql, plugins.clone(), &query_ctx);

        // test create table
        let sql = r#"CREATE TABLE {catalog}{schema}demo(
                            host STRING,
                            ts TIMESTAMP,
                            TIME INDEX (ts),
                            PRIMARY KEY(host)
                        ) engine=mito;"#;
        replace_test(sql, plugins.clone(), &query_ctx);

        // test drop table
        let sql = "DROP TABLE {catalog}{schema}demo;";
        replace_test(sql, plugins.clone(), &query_ctx);

        // test show tables
        let sql = "SHOW TABLES FROM public";
        let stmt = parse_stmt(sql, &GreptimeDbDialect {}).unwrap();
        check_permission(plugins.clone(), &stmt[0], &query_ctx).unwrap();

        let sql = "SHOW TABLES FROM private";
        let stmt = parse_stmt(sql, &GreptimeDbDialect {}).unwrap();
        let re = check_permission(plugins.clone(), &stmt[0], &query_ctx);
        assert!(re.is_ok());

        // test describe table
        let sql = "DESC TABLE {catalog}{schema}demo;";
        replace_test(sql, plugins, &query_ctx);
    }
}
