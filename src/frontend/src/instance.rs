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

pub mod distributed;
mod grpc;
mod influxdb;
mod opentsdb;
mod otlp;
mod prom_store;
mod script;
mod standalone;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use api::v1::meta::Role;
use async_trait::async_trait;
use auth::{PermissionChecker, PermissionCheckerRef, PermissionReq};
use catalog::remote::CachedMetaKvBackend;
use catalog::CatalogManagerRef;
use client::client_manager::DatanodeClients;
use common_base::Plugins;
use common_config::KvStoreConfig;
use common_error::ext::BoxedError;
use common_grpc::channel_manager::{ChannelConfig, ChannelManager};
use common_meta::cache_invalidator::DummyCacheInvalidator;
use common_meta::ddl_manager::DdlManager;
use common_meta::heartbeat::handler::parse_mailbox_message::ParseMailboxMessageHandler;
use common_meta::heartbeat::handler::HandlerGroupExecutor;
use common_meta::key::TableMetadataManager;
use common_meta::kv_backend::KvBackendRef;
use common_meta::state_store::KvStateStore;
use common_procedure::local::{LocalManager, ManagerConfig};
use common_procedure::options::ProcedureConfig;
use common_procedure::ProcedureManagerRef;
use common_query::Output;
use common_telemetry::logging::info;
use common_telemetry::{error, timer};
use datanode::region_server::RegionServer;
use log_store::raft_engine::RaftEngineBackend;
use meta_client::client::{MetaClient, MetaClientBuilder};
use partition::manager::PartitionRuleManager;
use query::parser::{PromQuery, QueryLanguageParser, QueryStatement};
use query::plan::LogicalPlan;
use query::query_engine::options::{validate_catalog_and_schema, QueryOptions};
use query::query_engine::DescribeResult;
use query::{QueryEngineFactory, QueryEngineRef};
use raft_engine::{Config, ReadableSize, RecoveryMode};
use servers::error as server_error;
use servers::error::{AuthSnafu, ExecuteQuerySnafu, ParsePromQLSnafu};
use servers::interceptor::{
    PromQueryInterceptor, PromQueryInterceptorRef, SqlQueryInterceptor, SqlQueryInterceptorRef,
};
use servers::prometheus_handler::PrometheusHandler;
use servers::query_handler::grpc::GrpcQueryHandler;
use servers::query_handler::sql::SqlQueryHandler;
use servers::query_handler::{
    InfluxdbLineProtocolHandler, OpenTelemetryProtocolHandler, OpentsdbProtocolHandler,
    PromStoreProtocolHandler, ScriptHandler,
};
use session::context::QueryContextRef;
use snafu::prelude::*;
use sql::dialect::Dialect;
use sql::parser::ParserContext;
use sql::statements::copy::CopyTable;
use sql::statements::statement::Statement;
use sqlparser::ast::ObjectName;
pub use standalone::StandaloneDatanodeManager;

use self::distributed::DistRegionRequestHandler;
use self::standalone::StandaloneTableMetadataCreator;
use crate::catalog::FrontendCatalogManager;
use crate::delete::{Deleter, DeleterRef};
use crate::error::{
    self, Error, ExecLogicalPlanSnafu, ExecutePromqlSnafu, ExternalSnafu, MissingMetasrvOptsSnafu,
    ParseSqlSnafu, PermissionSnafu, PlanStatementSnafu, Result, SqlExecInterceptedSnafu,
};
use crate::frontend::FrontendOptions;
use crate::heartbeat::handler::invalidate_table_cache::InvalidateTableCacheHandler;
use crate::heartbeat::HeartbeatTask;
use crate::insert::{Inserter, InserterRef};
use crate::metrics;
use crate::script::ScriptExecutor;
use crate::server::{start_server, ServerHandlers, Services};
use crate::statement::StatementExecutor;
use crate::table::table_idents_to_full_name;

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
    + Send
    + Sync
    + 'static
{
    async fn start(&self) -> Result<()>;
}

pub type FrontendInstanceRef = Arc<dyn FrontendInstance>;
pub type StatementExecutorRef = Arc<StatementExecutor>;

#[derive(Clone)]
pub struct Instance {
    catalog_manager: CatalogManagerRef,
    script_executor: Arc<ScriptExecutor>,
    statement_executor: Arc<StatementExecutor>,
    query_engine: QueryEngineRef,
    /// plugins: this map holds extensions to customize query or auth
    /// behaviours.
    plugins: Arc<Plugins>,
    servers: Arc<ServerHandlers>,
    heartbeat_task: Option<HeartbeatTask>,
    inserter: InserterRef,
    deleter: DeleterRef,
}

impl Instance {
    pub async fn try_new_distributed(
        opts: &FrontendOptions,
        plugins: Arc<Plugins>,
    ) -> Result<Self> {
        let meta_client = Self::create_meta_client(opts).await?;

        let datanode_clients = Arc::new(DatanodeClients::default());

        Self::try_new_distributed_with(meta_client, datanode_clients, plugins, opts).await
    }

    pub async fn try_new_distributed_with(
        meta_client: Arc<MetaClient>,
        datanode_clients: Arc<DatanodeClients>,
        plugins: Arc<Plugins>,
        opts: &FrontendOptions,
    ) -> Result<Self> {
        let meta_backend = Arc::new(CachedMetaKvBackend::new(meta_client.clone()));

        let catalog_manager = FrontendCatalogManager::new(
            meta_backend.clone(),
            meta_backend.clone(),
            datanode_clients.clone(),
        );
        let partition_manager = Arc::new(PartitionRuleManager::new(meta_backend.clone()));

        let region_request_handler = DistRegionRequestHandler::arc(
            partition_manager.clone(),
            catalog_manager.datanode_manager().clone(),
        );

        let query_engine = QueryEngineFactory::new_with_plugins(
            catalog_manager.clone(),
            Some(region_request_handler.clone()),
            true,
            plugins.clone(),
        )
        .query_engine();

        let inserter = Arc::new(Inserter::new(
            catalog_manager.clone(),
            partition_manager.clone(),
            datanode_clients.clone(),
        ));
        let deleter = Arc::new(Deleter::new(
            catalog_manager.clone(),
            partition_manager,
            datanode_clients,
        ));

        let statement_executor = Arc::new(StatementExecutor::new(
            catalog_manager.clone(),
            query_engine.clone(),
            meta_client.clone(),
            meta_backend.clone(),
            catalog_manager.clone(),
            inserter.clone(),
            deleter.clone(),
        ));

        plugins.insert::<StatementExecutorRef>(statement_executor.clone());

        let script_executor =
            Arc::new(ScriptExecutor::new(catalog_manager.clone(), query_engine.clone()).await?);

        let handlers_executor = HandlerGroupExecutor::new(vec![
            Arc::new(ParseMailboxMessageHandler),
            Arc::new(InvalidateTableCacheHandler::new(meta_backend)),
        ]);

        let heartbeat_task = Some(HeartbeatTask::new(
            meta_client.clone(),
            opts.heartbeat.clone(),
            Arc::new(handlers_executor),
        ));

        common_telemetry::init_node_id(opts.node_id.clone());

        Ok(Instance {
            catalog_manager,
            script_executor,
            statement_executor,
            query_engine,
            plugins: plugins.clone(),
            servers: Arc::new(HashMap::new()),
            heartbeat_task,
            inserter,
            deleter,
        })
    }

    async fn create_meta_client(opts: &FrontendOptions) -> Result<Arc<MetaClient>> {
        let meta_client_options = opts
            .meta_client_options
            .as_ref()
            .context(MissingMetasrvOptsSnafu)?;
        info!(
            "Creating Frontend instance in distributed mode with Meta server addr {:?}",
            meta_client_options.metasrv_addrs
        );

        let channel_config = ChannelConfig::new()
            .timeout(Duration::from_millis(meta_client_options.timeout_millis))
            .connect_timeout(Duration::from_millis(
                meta_client_options.connect_timeout_millis,
            ))
            .tcp_nodelay(meta_client_options.tcp_nodelay);
        let ddl_channel_config = channel_config.clone().timeout(Duration::from_millis(
            meta_client_options.ddl_timeout_millis,
        ));
        let channel_manager = ChannelManager::with_config(channel_config);
        let ddl_channel_manager = ChannelManager::with_config(ddl_channel_config);

        let mut meta_client = MetaClientBuilder::new(0, 0, Role::Frontend)
            .enable_router()
            .enable_store()
            .enable_heartbeat()
            .enable_ddl()
            .channel_manager(channel_manager)
            .ddl_channel_manager(ddl_channel_manager)
            .build();
        meta_client
            .start(&meta_client_options.metasrv_addrs)
            .await
            .context(error::StartMetaClientSnafu)?;
        Ok(Arc::new(meta_client))
    }

    pub async fn try_build_standalone_components(
        dir: String,
        kv_store_config: KvStoreConfig,
        procedure_config: ProcedureConfig,
    ) -> Result<(KvBackendRef, ProcedureManagerRef)> {
        let kv_store = Arc::new(
            RaftEngineBackend::try_open_with_cfg(Config {
                dir,
                purge_threshold: ReadableSize(kv_store_config.purge_threshold.0),
                recovery_mode: RecoveryMode::TolerateTailCorruption,
                batch_compression_threshold: ReadableSize::kb(8),
                target_file_size: ReadableSize(kv_store_config.file_size.0),
                ..Default::default()
            })
            .map_err(BoxedError::new)
            .context(error::OpenRaftEngineBackendSnafu)?,
        );

        let state_store = Arc::new(KvStateStore::new(kv_store.clone()));

        let manager_config = ManagerConfig {
            max_retry_times: procedure_config.max_retry_times,
            retry_delay: procedure_config.retry_delay,
            ..Default::default()
        };
        let procedure_manager = Arc::new(LocalManager::new(manager_config, state_store));

        Ok((kv_store, procedure_manager))
    }

    pub async fn try_new_standalone(
        kv_backend: KvBackendRef,
        procedure_manager: ProcedureManagerRef,
        catalog_manager: CatalogManagerRef,
        plugins: Arc<Plugins>,
        region_server: RegionServer,
    ) -> Result<Self> {
        let partition_manager = Arc::new(PartitionRuleManager::new(kv_backend.clone()));
        let datanode_manager = Arc::new(StandaloneDatanodeManager(region_server));

        let region_request_handler =
            DistRegionRequestHandler::arc(partition_manager.clone(), datanode_manager.clone());

        let query_engine = QueryEngineFactory::new_with_plugins(
            catalog_manager.clone(),
            Some(region_request_handler),
            true,
            plugins.clone(),
        )
        .query_engine();

        let script_executor =
            Arc::new(ScriptExecutor::new(catalog_manager.clone(), query_engine.clone()).await?);

        let table_metadata_manager = Arc::new(TableMetadataManager::new(kv_backend.clone()));

        let cache_invalidator = Arc::new(DummyCacheInvalidator);
        let ddl_executor = Arc::new(DdlManager::new(
            procedure_manager,
            datanode_manager.clone(),
            cache_invalidator.clone(),
            table_metadata_manager.clone(),
            Arc::new(StandaloneTableMetadataCreator::new(kv_backend.clone())),
        ));

        let partition_manager = Arc::new(PartitionRuleManager::new(kv_backend.clone()));

        let inserter = Arc::new(Inserter::new(
            catalog_manager.clone(),
            partition_manager.clone(),
            datanode_manager.clone(),
        ));
        let deleter = Arc::new(Deleter::new(
            catalog_manager.clone(),
            partition_manager,
            datanode_manager,
        ));

        let statement_executor = Arc::new(StatementExecutor::new(
            catalog_manager.clone(),
            query_engine.clone(),
            ddl_executor,
            kv_backend.clone(),
            cache_invalidator,
            inserter.clone(),
            deleter.clone(),
        ));

        Ok(Instance {
            catalog_manager: catalog_manager.clone(),
            script_executor,
            statement_executor,
            query_engine,
            plugins,
            servers: Arc::new(HashMap::new()),
            heartbeat_task: None,
            inserter,
            deleter,
        })
    }

    pub async fn build_servers(&mut self, opts: &FrontendOptions) -> Result<()> {
        let servers = Services::build(opts, Arc::new(self.clone()), self.plugins.clone()).await?;
        self.servers = Arc::new(servers);

        Ok(())
    }

    pub fn catalog_manager(&self) -> &CatalogManagerRef {
        &self.catalog_manager
    }

    pub fn plugins(&self) -> Arc<Plugins> {
        self.plugins.clone()
    }

    pub async fn shutdown(&self) -> Result<()> {
        futures::future::try_join_all(self.servers.values().map(|server| server.0.shutdown()))
            .await
            .context(error::ShutdownServerSnafu)
            .map(|_| ())
    }

    pub fn statement_executor(&self) -> Arc<StatementExecutor> {
        self.statement_executor.clone()
    }
}

#[async_trait]
impl FrontendInstance for Instance {
    async fn start(&self) -> Result<()> {
        if let Some(heartbeat_task) = &self.heartbeat_task {
            heartbeat_task.start().await?;
        }

        self.script_executor.start(self).await?;

        futures::future::try_join_all(self.servers.values().map(start_server))
            .await
            .context(error::StartServerSnafu)
            .map(|_| ())
    }
}

fn parse_stmt(sql: &str, dialect: &(dyn Dialect + Send + Sync)) -> Result<Vec<Statement>> {
    ParserContext::create_with_dialect(sql, dialect).context(ParseSqlSnafu)
}

impl Instance {
    async fn query_statement(&self, stmt: Statement, query_ctx: QueryContextRef) -> Result<Output> {
        check_permission(self.plugins.clone(), &stmt, &query_ctx)?;

        let stmt = QueryStatement::Sql(stmt);
        self.statement_executor.execute_stmt(stmt, query_ctx).await
    }
}

#[async_trait]
impl SqlQueryHandler for Instance {
    type Error = Error;

    async fn do_query(&self, query: &str, query_ctx: QueryContextRef) -> Vec<Result<Output>> {
        let _timer = timer!(metrics::METRIC_HANDLE_SQL_ELAPSED);
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

                    match self.query_statement(stmt, query_ctx.clone()).await {
                        Ok(output) => {
                            let output_result =
                                query_interceptor.post_execute(output, query_ctx.clone());
                            results.push(output_result);
                        }
                        Err(e) => {
                            let redacted = sql::util::redact_sql_secrets(query.as_ref());
                            error!(e; "Failed to execute query: {redacted}");

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
        let _timer = timer!(metrics::METRIC_EXEC_PLAN_ELAPSED);
        // plan should be prepared before exec
        // we'll do check there
        self.query_engine
            .execute(plan, query_ctx)
            .await
            .context(ExecLogicalPlanSnafu)
    }

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
                .plan(QueryStatement::Sql(stmt), query_ctx)
                .await
                .context(PlanStatementSnafu)?;
            self.query_engine
                .describe(plan)
                .await
                .map(Some)
                .context(error::DescribeStatementSnafu)
        } else {
            Ok(None)
        }
    }

    async fn is_valid_schema(&self, catalog: &str, schema: &str) -> Result<bool> {
        self.catalog_manager
            .schema_exists(catalog, schema)
            .await
            .context(error::CatalogSnafu)
    }
}

#[async_trait]
impl PrometheusHandler for Instance {
    async fn do_query(
        &self,
        query: &PromQuery,
        query_ctx: QueryContextRef,
    ) -> server_error::Result<Output> {
        let _timer = timer!(metrics::METRIC_HANDLE_PROMQL_ELAPSED);
        let interceptor = self
            .plugins
            .get::<PromQueryInterceptorRef<server_error::Error>>();
        interceptor.pre_execute(query, query_ctx.clone())?;

        self.plugins
            .get::<PermissionCheckerRef>()
            .as_ref()
            .check_permission(query_ctx.current_user(), PermissionReq::PromQuery)
            .context(AuthSnafu)?;

        let stmt = QueryLanguageParser::parse_promql(query).with_context(|_| ParsePromQLSnafu {
            query: query.clone(),
        })?;

        let output = self
            .statement_executor
            .execute_stmt(stmt, query_ctx.clone())
            .await
            .map_err(BoxedError::new)
            .with_context(|_| ExecuteQuerySnafu {
                query: format!("{query:?}"),
            })?;

        Ok(interceptor.post_execute(output, query_ctx)?)
    }

    fn catalog_manager(&self) -> CatalogManagerRef {
        self.catalog_manager.clone()
    }
}

pub fn check_permission(
    plugins: Arc<Plugins>,
    stmt: &Statement,
    query_ctx: &QueryContextRef,
) -> Result<()> {
    let need_validate = plugins
        .get::<QueryOptions>()
        .map(|opts| opts.disallow_cross_schema_query)
        .unwrap_or_default();

    if !need_validate {
        return Ok(());
    }

    match stmt {
        // These are executed by query engine, and will be checked there.
        Statement::Query(_) | Statement::Explain(_) | Statement::Tql(_) | Statement::Delete(_) => {}
        // database ops won't be checked
        Statement::CreateDatabase(_) | Statement::ShowDatabases(_) => {}
        // show create table and alter are not supported yet
        Statement::ShowCreateTable(_) | Statement::CreateExternalTable(_) | Statement::Alter(_) => {
        }

        Statement::Insert(insert) => {
            validate_param(insert.table_name(), query_ctx)?;
        }
        Statement::CreateTable(stmt) => {
            validate_param(&stmt.name, query_ctx)?;
        }
        Statement::DropTable(drop_stmt) => {
            validate_param(drop_stmt.table_name(), query_ctx)?;
        }
        Statement::ShowTables(stmt) => {
            if let Some(database) = &stmt.database {
                validate_catalog_and_schema(query_ctx.current_catalog(), database, query_ctx)
                    .map_err(BoxedError::new)
                    .context(SqlExecInterceptedSnafu)?;
            }
        }
        Statement::DescribeTable(stmt) => {
            validate_param(stmt.name(), query_ctx)?;
        }
        Statement::Copy(sql::statements::copy::Copy::CopyTable(stmt)) => match stmt {
            CopyTable::To(copy_table_to) => validate_param(&copy_table_to.table_name, query_ctx)?,
            CopyTable::From(copy_table_from) => {
                validate_param(&copy_table_from.table_name, query_ctx)?
            }
        },
        Statement::Copy(sql::statements::copy::Copy::CopyDatabase(stmt)) => {
            validate_param(&stmt.database_name, query_ctx)?
        }
        Statement::TruncateTable(stmt) => {
            validate_param(stmt.table_name(), query_ctx)?;
        }
    }
    Ok(())
}

fn validate_param(name: &ObjectName, query_ctx: &QueryContextRef) -> Result<()> {
    let (catalog, schema, _) = table_idents_to_full_name(name, query_ctx.clone())
        .map_err(BoxedError::new)
        .context(ExternalSnafu)?;

    validate_catalog_and_schema(&catalog, &schema, query_ctx)
        .map_err(BoxedError::new)
        .context(SqlExecInterceptedSnafu)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use query::query_engine::options::QueryOptions;
    use session::context::QueryContext;
    use sql::dialect::GreptimeDbDialect;
    use strfmt::Format;

    use super::*;

    #[test]
    fn test_exec_validation() {
        let query_ctx = QueryContext::arc();
        let plugins = Plugins::new();
        plugins.insert(QueryOptions {
            disallow_cross_schema_query: true,
        });
        let plugins = Arc::new(plugins);

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

        fn replace_test(template_sql: &str, plugins: Arc<Plugins>, query_ctx: &QueryContextRef) {
            // test right
            let right = vec![("", ""), ("", "public."), ("greptime.", "public.")];
            for (catalog, schema) in right {
                let sql = do_fmt(template_sql, catalog, schema);
                do_test(&sql, plugins.clone(), query_ctx, true);
            }

            let wrong = vec![
                ("", "wrongschema."),
                ("greptime.", "wrongschema."),
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

        fn do_test(sql: &str, plugins: Arc<Plugins>, query_ctx: &QueryContextRef, is_ok: bool) {
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
                        ) engine=mito with(regions=1);"#;
        replace_test(sql, plugins.clone(), &query_ctx);

        // test drop table
        let sql = "DROP TABLE {catalog}{schema}demo;";
        replace_test(sql, plugins.clone(), &query_ctx);

        // test show tables
        let sql = "SHOW TABLES FROM public";
        let stmt = parse_stmt(sql, &GreptimeDbDialect {}).unwrap();
        check_permission(plugins.clone(), &stmt[0], &query_ctx).unwrap();

        let sql = "SHOW TABLES FROM wrongschema";
        let stmt = parse_stmt(sql, &GreptimeDbDialect {}).unwrap();
        let re = check_permission(plugins.clone(), &stmt[0], &query_ctx);
        assert!(re.is_err());

        // test describe table
        let sql = "DESC TABLE {catalog}{schema}demo;";
        replace_test(sql, plugins, &query_ctx);
    }
}
