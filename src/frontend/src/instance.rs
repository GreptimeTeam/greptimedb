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

pub(crate) mod distributed;
mod grpc;
mod influxdb;
mod opentsdb;
mod prometheus;

use std::sync::Arc;
use std::time::Duration;

use api::v1::alter_expr::Kind;
use api::v1::ddl_request::Expr as DdlExpr;
use api::v1::greptime_request::Request;
use api::v1::{
    AddColumns, AlterExpr, Column, DdlRequest, DropTableExpr, GreptimeRequest, InsertRequest,
};
use async_trait::async_trait;
use catalog::remote::MetaKvBackend;
use catalog::CatalogManagerRef;
use common_catalog::consts::DEFAULT_CATALOG_NAME;
use common_error::prelude::BoxedError;
use common_grpc::channel_manager::{ChannelConfig, ChannelManager};
use common_query::Output;
use common_recordbatch::RecordBatches;
use common_telemetry::logging::{debug, info};
use datanode::instance::InstanceRef as DnInstanceRef;
use distributed::DistInstance;
use meta_client::client::{MetaClient, MetaClientBuilder};
use meta_client::MetaClientOpts;
use servers::error as server_error;
use servers::interceptor::{SqlQueryInterceptor, SqlQueryInterceptorRef};
use servers::query_handler::{
    GrpcQueryHandler, GrpcQueryHandlerRef, InfluxdbLineProtocolHandler, OpentsdbProtocolHandler,
    PrometheusProtocolHandler, ScriptHandler, ScriptHandlerRef, SqlQueryHandler,
    SqlQueryHandlerRef,
};
use session::context::QueryContextRef;
use snafu::prelude::*;
use sql::dialect::GenericDialect;
use sql::parser::ParserContext;
use sql::statements::statement::Statement;

use crate::catalog::FrontendCatalogManager;
use crate::datanode::DatanodeClients;
use crate::error::{self, MissingMetasrvOptsSnafu, Result};
use crate::expr_factory::{CreateExprFactoryRef, DefaultCreateExprFactory};
use crate::frontend::FrontendOptions;
use crate::table::route::TableRoutes;
use crate::Plugins;

#[async_trait]
pub trait FrontendInstance:
    GrpcQueryHandler
    + SqlQueryHandler
    + OpentsdbProtocolHandler
    + InfluxdbLineProtocolHandler
    + PrometheusProtocolHandler
    + ScriptHandler
    + Send
    + Sync
    + 'static
{
    async fn start(&mut self) -> Result<()>;
}

pub type FrontendInstanceRef = Arc<dyn FrontendInstance>;

#[derive(Clone)]
pub struct Instance {
    catalog_manager: CatalogManagerRef,

    /// Script handler is None in distributed mode, only works on standalone mode.
    script_handler: Option<ScriptHandlerRef>,
    sql_handler: SqlQueryHandlerRef,
    grpc_query_handler: GrpcQueryHandlerRef,

    create_expr_factory: CreateExprFactoryRef,

    /// plugins: this map holds extensions to customize query or auth
    /// behaviours.
    plugins: Arc<Plugins>,
}

impl Instance {
    pub async fn try_new_distributed(opts: &FrontendOptions) -> Result<Self> {
        let meta_client = Self::create_meta_client(opts).await?;

        let meta_backend = Arc::new(MetaKvBackend {
            client: meta_client.clone(),
        });
        let table_routes = Arc::new(TableRoutes::new(meta_client.clone()));
        let datanode_clients = Arc::new(DatanodeClients::new());
        let catalog_manager = Arc::new(FrontendCatalogManager::new(
            meta_backend,
            table_routes,
            datanode_clients.clone(),
        ));

        let dist_instance =
            DistInstance::new(meta_client, catalog_manager.clone(), datanode_clients);
        let dist_instance = Arc::new(dist_instance);

        Ok(Instance {
            catalog_manager,
            script_handler: None,
            create_expr_factory: Arc::new(DefaultCreateExprFactory),
            sql_handler: dist_instance.clone(),
            grpc_query_handler: dist_instance,
            plugins: Default::default(),
        })
    }

    async fn create_meta_client(opts: &FrontendOptions) -> Result<Arc<MetaClient>> {
        let metasrv_addr = &opts
            .meta_client_opts
            .as_ref()
            .context(MissingMetasrvOptsSnafu)?
            .metasrv_addrs;
        info!(
            "Creating Frontend instance in distributed mode with Meta server addr {:?}",
            metasrv_addr
        );

        let meta_config = MetaClientOpts::default();
        let channel_config = ChannelConfig::new()
            .timeout(Duration::from_millis(meta_config.timeout_millis))
            .connect_timeout(Duration::from_millis(meta_config.connect_timeout_millis))
            .tcp_nodelay(meta_config.tcp_nodelay);
        let channel_manager = ChannelManager::with_config(channel_config);

        let mut meta_client = MetaClientBuilder::new(0, 0)
            .enable_router()
            .enable_store()
            .channel_manager(channel_manager)
            .build();
        meta_client
            .start(metasrv_addr)
            .await
            .context(error::StartMetaClientSnafu)?;
        Ok(Arc::new(meta_client))
    }

    pub fn new_standalone(dn_instance: DnInstanceRef) -> Self {
        Instance {
            catalog_manager: dn_instance.catalog_manager().clone(),
            script_handler: None,
            create_expr_factory: Arc::new(DefaultCreateExprFactory),
            sql_handler: dn_instance.clone(),
            grpc_query_handler: dn_instance.clone(),
            plugins: Default::default(),
        }
    }

    #[cfg(test)]
    pub(crate) fn new_distributed(dist_instance: Arc<DistInstance>) -> Self {
        Instance {
            catalog_manager: dist_instance.catalog_manager(),
            script_handler: None,
            create_expr_factory: Arc::new(DefaultCreateExprFactory),
            sql_handler: dist_instance.clone(),
            grpc_query_handler: dist_instance,
            plugins: Default::default(),
        }
    }

    pub fn catalog_manager(&self) -> &CatalogManagerRef {
        &self.catalog_manager
    }

    pub fn set_script_handler(&mut self, handler: ScriptHandlerRef) {
        debug_assert!(
            self.script_handler.is_none(),
            "Script handler can be set only once!"
        );
        self.script_handler = Some(handler);
    }

    /// Handle batch inserts
    pub async fn handle_inserts(
        &self,
        requests: Vec<InsertRequest>,
    ) -> server_error::Result<Output> {
        let mut success = 0;
        for request in requests {
            match self.handle_insert(request).await? {
                Output::AffectedRows(rows) => success += rows,
                _ => unreachable!("Insert should not yield output other than AffectedRows"),
            }
        }
        Ok(Output::AffectedRows(success))
    }

    async fn handle_insert(&self, request: InsertRequest) -> server_error::Result<Output> {
        let schema_name = &request.schema_name;
        let table_name = &request.table_name;
        let catalog_name = DEFAULT_CATALOG_NAME;

        let columns = &request.columns;

        self.create_or_alter_table_on_demand(catalog_name, schema_name, table_name, columns)
            .await?;

        let query = GreptimeRequest {
            request: Some(Request::Insert(request)),
        };
        GrpcQueryHandler::do_query(&*self.grpc_query_handler, query).await
    }

    // check if table already exist:
    // - if table does not exist, create table by inferred CreateExpr
    // - if table exist, check if schema matches. If any new column found, alter table by inferred `AlterExpr`
    async fn create_or_alter_table_on_demand(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
        columns: &[Column],
    ) -> server_error::Result<()> {
        let table = self
            .catalog_manager
            .table(catalog_name, schema_name, table_name)
            .context(server_error::CatalogSnafu)?;
        match table {
            None => {
                info!(
                    "Table {}.{}.{} does not exist, try create table",
                    catalog_name, schema_name, table_name,
                );
                self.create_table_by_columns(catalog_name, schema_name, table_name, columns)
                    .await?;
                info!(
                    "Successfully created table on insertion: {}.{}.{}",
                    catalog_name, schema_name, table_name
                );
            }
            Some(table) => {
                let schema = table.schema();

                if let Some(add_columns) = common_grpc_expr::find_new_columns(&schema, columns)
                    .context(server_error::FindNewColumnsOnInsertionSnafu)?
                {
                    info!(
                        "Find new columns {:?} on insertion, try to alter table: {}.{}.{}",
                        add_columns, catalog_name, schema_name, table_name
                    );
                    self.add_new_columns_to_table(
                        catalog_name,
                        schema_name,
                        table_name,
                        add_columns,
                    )
                    .await?;
                    info!(
                        "Successfully altered table on insertion: {}.{}.{}",
                        catalog_name, schema_name, table_name
                    );
                }
            }
        };
        Ok(())
    }

    /// Infer create table expr from inserting data
    async fn create_table_by_columns(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
        columns: &[Column],
    ) -> server_error::Result<Output> {
        // Create table automatically, build schema from data.
        let create_expr = self
            .create_expr_factory
            .create_expr_by_columns(catalog_name, schema_name, table_name, columns)
            .await
            .map_err(BoxedError::new)
            .context(server_error::ExecuteGrpcQuerySnafu)?;

        info!(
            "Try to create table: {} automatically with request: {:?}",
            table_name, create_expr,
        );

        self.grpc_query_handler
            .do_query(GreptimeRequest {
                request: Some(Request::Ddl(DdlRequest {
                    expr: Some(DdlExpr::CreateTable(create_expr)),
                })),
            })
            .await
    }

    async fn add_new_columns_to_table(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
        add_columns: AddColumns,
    ) -> server_error::Result<Output> {
        debug!(
            "Adding new columns: {:?} to table: {}",
            add_columns, table_name
        );
        let expr = AlterExpr {
            table_name: table_name.to_string(),
            schema_name: schema_name.to_string(),
            catalog_name: catalog_name.to_string(),
            kind: Some(Kind::AddColumns(add_columns)),
        };

        self.grpc_query_handler
            .do_query(GreptimeRequest {
                request: Some(Request::Ddl(DdlRequest {
                    expr: Some(DdlExpr::Alter(expr)),
                })),
            })
            .await
    }

    fn handle_use(&self, db: String, query_ctx: QueryContextRef) -> Result<Output> {
        let catalog = query_ctx.current_catalog();
        let catalog = catalog.as_deref().unwrap_or(DEFAULT_CATALOG_NAME);

        ensure!(
            self.catalog_manager
                .schema(catalog, &db)
                .context(error::CatalogSnafu)?
                .is_some(),
            error::SchemaNotFoundSnafu { schema_info: &db }
        );

        query_ctx.set_current_schema(&db);

        Ok(Output::RecordBatches(RecordBatches::empty()))
    }

    pub fn set_plugins(&mut self, map: Arc<Plugins>) {
        self.plugins = map;
    }

    pub fn plugins(&self) -> Arc<Plugins> {
        self.plugins.clone()
    }
}

#[async_trait]
impl FrontendInstance for Instance {
    async fn start(&mut self) -> Result<()> {
        // TODO(hl): Frontend init should move to here
        Ok(())
    }
}

fn parse_stmt(sql: &str) -> Result<Vec<Statement>> {
    ParserContext::create_with_dialect(sql, &GenericDialect {}).context(error::ParseSqlSnafu)
}

impl Instance {
    async fn query_statement(
        &self,
        stmt: Statement,
        query_ctx: QueryContextRef,
    ) -> server_error::Result<Output> {
        // TODO(sunng87): provide a better form to log or track statement
        let query = &format!("{:?}", &stmt);
        match stmt.clone() {
            Statement::CreateDatabase(_)
            | Statement::ShowDatabases(_)
            | Statement::CreateTable(_)
            | Statement::ShowTables(_)
            | Statement::DescribeTable(_)
            | Statement::Explain(_)
            | Statement::Query(_)
            | Statement::Insert(_) => {
                return self.sql_handler.do_statement_query(stmt, query_ctx).await;
            }
            Statement::Alter(alter_stmt) => {
                let expr = AlterExpr::try_from(alter_stmt)
                    .map_err(BoxedError::new)
                    .context(server_error::ExecuteAlterSnafu { query })?;
                return self
                    .grpc_query_handler
                    .do_query(GreptimeRequest {
                        request: Some(Request::Ddl(DdlRequest {
                            expr: Some(DdlExpr::Alter(expr)),
                        })),
                    })
                    .await;
            }
            Statement::DropTable(drop_stmt) => {
                let expr = DropTableExpr {
                    catalog_name: drop_stmt.catalog_name,
                    schema_name: drop_stmt.schema_name,
                    table_name: drop_stmt.table_name,
                };
                return self
                    .grpc_query_handler
                    .do_query(GreptimeRequest {
                        request: Some(Request::Ddl(DdlRequest {
                            expr: Some(DdlExpr::DropTable(expr)),
                        })),
                    })
                    .await;
            }
            Statement::ShowCreateTable(_) => {
                return server_error::NotSupportedSnafu { feat: query }.fail();
            }
            Statement::Use(db) => self.handle_use(db, query_ctx),
        }
        .map_err(BoxedError::new)
        .context(server_error::ExecuteQuerySnafu { query })
    }
}

#[async_trait]
impl SqlQueryHandler for Instance {
    async fn do_query(
        &self,
        query: &str,
        query_ctx: QueryContextRef,
    ) -> Vec<server_error::Result<Output>> {
        let query_interceptor = self.plugins.get::<SqlQueryInterceptorRef>();
        let query = match query_interceptor.pre_parsing(query, query_ctx.clone()) {
            Ok(q) => q,
            Err(e) => return vec![Err(e)],
        };

        match parse_stmt(query.as_ref())
            .map_err(BoxedError::new)
            .context(server_error::ExecuteQuerySnafu { query })
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
                    match self.query_statement(stmt, query_ctx.clone()).await {
                        Ok(output) => {
                            let output_result =
                                query_interceptor.post_execute(output, query_ctx.clone());
                            results.push(output_result);
                        }
                        Err(e) => {
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

    async fn do_statement_query(
        &self,
        stmt: Statement,
        query_ctx: QueryContextRef,
    ) -> server_error::Result<Output> {
        let query_interceptor = self.plugins.get::<SqlQueryInterceptorRef>();

        // TODO(sunng87): figure out at which stage we can call
        // this hook after ArrowFlight adoption. We need to provide
        // LogicalPlan as to this hook.
        query_interceptor.pre_execute(&stmt, None, query_ctx.clone())?;
        self.query_statement(stmt, query_ctx.clone())
            .await
            .and_then(|output| query_interceptor.post_execute(output, query_ctx.clone()))
    }

    fn is_valid_schema(&self, catalog: &str, schema: &str) -> server_error::Result<bool> {
        self.catalog_manager
            .schema(catalog, schema)
            .map(|s| s.is_some())
            .context(server_error::CatalogSnafu)
    }
}

#[async_trait]
impl ScriptHandler for Instance {
    async fn insert_script(&self, name: &str, script: &str) -> server_error::Result<()> {
        if let Some(handler) = &self.script_handler {
            handler.insert_script(name, script).await
        } else {
            server_error::NotSupportedSnafu {
                feat: "Script execution in Frontend",
            }
            .fail()
        }
    }

    async fn execute_script(&self, script: &str) -> server_error::Result<Output> {
        if let Some(handler) = &self.script_handler {
            handler.execute_script(script).await
        } else {
            server_error::NotSupportedSnafu {
                feat: "Script execution in Frontend",
            }
            .fail()
        }
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::sync::atomic::AtomicU32;

    use session::context::QueryContext;

    use super::*;
    use crate::tests;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_execute_sql() {
        let query_ctx = Arc::new(QueryContext::new());

        let standalone = tests::create_standalone_instance("test_execute_sql").await;
        let instance = standalone.instance;

        let sql = r#"CREATE TABLE demo(
                            host STRING,
                            ts TIMESTAMP,
                            cpu DOUBLE NULL,
                            memory DOUBLE NULL,
                            disk_util DOUBLE DEFAULT 9.9,
                            TIME INDEX (ts),
                            PRIMARY KEY(host)
                        ) engine=mito with(regions=1);"#;
        let output = SqlQueryHandler::do_query(&*instance, sql, query_ctx.clone())
            .await
            .remove(0)
            .unwrap();
        match output {
            Output::AffectedRows(rows) => assert_eq!(rows, 0),
            _ => unreachable!(),
        }

        let sql = r#"insert into demo(host, cpu, memory, ts) values
                                ('frontend.host1', 1.1, 100, 1000),
                                ('frontend.host2', null, null, 2000),
                                ('frontend.host3', 3.3, 300, 3000)
                                "#;
        let output = SqlQueryHandler::do_query(&*instance, sql, query_ctx.clone())
            .await
            .remove(0)
            .unwrap();
        match output {
            Output::AffectedRows(rows) => assert_eq!(rows, 3),
            _ => unreachable!(),
        }

        let sql = "select * from demo";
        let output = SqlQueryHandler::do_query(&*instance, sql, query_ctx.clone())
            .await
            .remove(0)
            .unwrap();
        match output {
            Output::RecordBatches(_) => {
                unreachable!("Output::RecordBatches");
            }
            Output::AffectedRows(_) => {
                unreachable!("Output::AffectedRows");
            }
            Output::Stream(s) => {
                let batches = common_recordbatch::util::collect_batches(s).await.unwrap();
                let pretty_print = batches.pretty_print().unwrap();
                let expected = "\
+----------------+---------------------+-----+--------+-----------+
| host           | ts                  | cpu | memory | disk_util |
+----------------+---------------------+-----+--------+-----------+
| frontend.host1 | 1970-01-01T00:00:01 | 1.1 | 100    | 9.9       |
| frontend.host2 | 1970-01-01T00:00:02 |     |        | 9.9       |
| frontend.host3 | 1970-01-01T00:00:03 | 3.3 | 300    | 9.9       |
+----------------+---------------------+-----+--------+-----------+\
                ";
                assert_eq!(pretty_print, expected);
            }
        };

        let sql = "select * from demo where ts>cast(1000000000 as timestamp)"; // use nanoseconds as where condition
        let output = SqlQueryHandler::do_query(&*instance, sql, query_ctx.clone())
            .await
            .remove(0)
            .unwrap();
        match output {
            Output::RecordBatches(_) => {
                unreachable!("Output::RecordBatches")
            }
            Output::AffectedRows(_) => {
                unreachable!("Output::AffectedRows")
            }
            Output::Stream(s) => {
                let recordbatches = common_recordbatch::util::collect_batches(s).await.unwrap();
                let pretty = recordbatches.pretty_print().unwrap();
                let expected = "\
+----------------+---------------------+-----+--------+-----------+
| host           | ts                  | cpu | memory | disk_util |
+----------------+---------------------+-----+--------+-----------+
| frontend.host2 | 1970-01-01T00:00:02 |     |        | 9.9       |
| frontend.host3 | 1970-01-01T00:00:03 | 3.3 | 300    | 9.9       |
+----------------+---------------------+-----+--------+-----------+\
                    "
                .to_string();
                assert_eq!(pretty, expected);
            }
        };
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_sql_interceptor_plugin() {
        #[derive(Default)]
        struct AssertionHook {
            pub(crate) c: AtomicU32,
        }

        impl SqlQueryInterceptor for AssertionHook {
            fn pre_parsing<'a>(
                &self,
                query: &'a str,
                _query_ctx: QueryContextRef,
            ) -> server_error::Result<std::borrow::Cow<'a, str>> {
                self.c.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                assert!(query.starts_with("CREATE TABLE demo"));
                Ok(Cow::Borrowed(query))
            }

            fn post_parsing(
                &self,
                statements: Vec<Statement>,
                _query_ctx: QueryContextRef,
            ) -> server_error::Result<Vec<Statement>> {
                self.c.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                assert!(matches!(statements[0], Statement::CreateTable(_)));
                Ok(statements)
            }

            fn pre_execute(
                &self,
                _statement: &Statement,
                _plan: Option<&query::plan::LogicalPlan>,
                _query_ctx: QueryContextRef,
            ) -> server_error::Result<()> {
                self.c.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Ok(())
            }

            fn post_execute(
                &self,
                mut output: Output,
                _query_ctx: QueryContextRef,
            ) -> server_error::Result<Output> {
                self.c.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                match &mut output {
                    Output::AffectedRows(rows) => {
                        assert_eq!(*rows, 0);
                        // update output result
                        *rows = 10;
                    }
                    _ => unreachable!(),
                }
                Ok(output)
            }
        }

        let standalone = tests::create_standalone_instance("test_hook").await;
        let mut instance = standalone.instance;

        let mut plugins = Plugins::new();
        let counter_hook = Arc::new(AssertionHook::default());
        plugins.insert::<SqlQueryInterceptorRef>(counter_hook.clone());
        Arc::make_mut(&mut instance).set_plugins(Arc::new(plugins));

        let sql = r#"CREATE TABLE demo(
                            host STRING,
                            ts TIMESTAMP,
                            cpu DOUBLE NULL,
                            memory DOUBLE NULL,
                            disk_util DOUBLE DEFAULT 9.9,
                            TIME INDEX (ts),
                            PRIMARY KEY(host)
                        ) engine=mito with(regions=1);"#;
        let output = SqlQueryHandler::do_query(&*instance, sql, QueryContext::arc())
            .await
            .remove(0)
            .unwrap();

        // assert that the hook is called 3 times
        assert_eq!(4, counter_hook.c.load(std::sync::atomic::Ordering::Relaxed));
        match output {
            Output::AffectedRows(rows) => assert_eq!(rows, 10),
            _ => unreachable!(),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_disable_db_operation_plugin() {
        #[derive(Default)]
        struct DisableDBOpHook;

        impl SqlQueryInterceptor for DisableDBOpHook {
            fn post_parsing(
                &self,
                statements: Vec<Statement>,
                _query_ctx: QueryContextRef,
            ) -> server_error::Result<Vec<Statement>> {
                for s in &statements {
                    match s {
                        Statement::CreateDatabase(_) | Statement::ShowDatabases(_) => {
                            return Err(server_error::Error::NotSupported {
                                feat: "Database operations".to_owned(),
                            })
                        }
                        _ => {}
                    }
                }

                Ok(statements)
            }
        }

        let query_ctx = Arc::new(QueryContext::new());

        let standalone = tests::create_standalone_instance("test_db_hook").await;
        let mut instance = standalone.instance;

        let mut plugins = Plugins::new();
        let hook = Arc::new(DisableDBOpHook::default());
        plugins.insert::<SqlQueryInterceptorRef>(hook.clone());
        Arc::make_mut(&mut instance).set_plugins(Arc::new(plugins));

        let sql = r#"CREATE TABLE demo(
                            host STRING,
                            ts TIMESTAMP,
                            cpu DOUBLE NULL,
                            memory DOUBLE NULL,
                            disk_util DOUBLE DEFAULT 9.9,
                            TIME INDEX (ts),
                            PRIMARY KEY(host)
                        ) engine=mito with(regions=1);"#;
        let output = SqlQueryHandler::do_query(&*instance, sql, query_ctx.clone())
            .await
            .remove(0)
            .unwrap();

        match output {
            Output::AffectedRows(rows) => assert_eq!(rows, 0),
            _ => unreachable!(),
        }

        let sql = r#"CREATE DATABASE tomcat"#;
        if let Err(e) = SqlQueryHandler::do_query(&*instance, sql, query_ctx.clone())
            .await
            .remove(0)
        {
            assert!(matches!(e, server_error::Error::NotSupported { .. }));
        } else {
            unreachable!();
        }

        let sql = r#"SELECT 1; SHOW DATABASES"#;
        if let Err(e) = SqlQueryHandler::do_query(&*instance, sql, query_ctx.clone())
            .await
            .remove(0)
        {
            assert!(matches!(e, server_error::Error::NotSupported { .. }));
        } else {
            unreachable!();
        }
    }
}
