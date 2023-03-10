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
mod standalone;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use api::v1::alter_expr::Kind;
use api::v1::ddl_request::Expr as DdlExpr;
use api::v1::greptime_request::Request;
use api::v1::{AddColumns, AlterExpr, Column, DdlRequest, InsertRequest};
use async_trait::async_trait;
use catalog::remote::MetaKvBackend;
use catalog::CatalogManagerRef;
use common_base::Plugins;
use common_error::ext::BoxedError;
use common_grpc::channel_manager::{ChannelConfig, ChannelManager};
use common_query::Output;
use common_recordbatch::RecordBatches;
use common_telemetry::logging::{debug, info};
use datafusion::sql::sqlparser::ast::ObjectName;
use datanode::instance::sql::table_idents_to_full_name;
use datanode::instance::InstanceRef as DnInstanceRef;
use datatypes::schema::Schema;
use distributed::DistInstance;
use meta_client::client::{MetaClient, MetaClientBuilder};
use meta_client::MetaClientOptions;
use partition::manager::PartitionRuleManager;
use partition::route::TableRoutes;
use query::parser::PromQuery;
use query::query_engine::options::{validate_catalog_and_schema, QueryOptions};
use servers::error as server_error;
use servers::interceptor::{SqlQueryInterceptor, SqlQueryInterceptorRef};
use servers::prom::{PromHandler, PromHandlerRef};
use servers::query_handler::grpc::{GrpcQueryHandler, GrpcQueryHandlerRef};
use servers::query_handler::sql::{SqlQueryHandler, SqlQueryHandlerRef};
use servers::query_handler::{
    InfluxdbLineProtocolHandler, OpentsdbProtocolHandler, PrometheusProtocolHandler, ScriptHandler,
    ScriptHandlerRef,
};
use session::context::QueryContextRef;
use snafu::prelude::*;
use sql::dialect::GenericDialect;
use sql::parser::ParserContext;
use sql::statements::copy::CopyTable;
use sql::statements::statement::Statement;

use crate::catalog::FrontendCatalogManager;
use crate::datanode::DatanodeClients;
use crate::error::{
    self, Error, ExecutePromqlSnafu, ExternalSnafu, InvalidInsertRequestSnafu,
    MissingMetasrvOptsSnafu, NotSupportedSnafu, ParseSqlSnafu, Result, SqlExecInterceptedSnafu,
};
use crate::expr_factory::{CreateExprFactoryRef, DefaultCreateExprFactory};
use crate::frontend::FrontendOptions;
use crate::instance::standalone::{StandaloneGrpcQueryHandler, StandaloneSqlQueryHandler};

#[async_trait]
pub trait FrontendInstance:
    GrpcQueryHandler<Error = Error>
    + SqlQueryHandler<Error = Error>
    + OpentsdbProtocolHandler
    + InfluxdbLineProtocolHandler
    + PrometheusProtocolHandler
    + ScriptHandler
    + PromHandler
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
    sql_handler: SqlQueryHandlerRef<Error>,
    grpc_query_handler: GrpcQueryHandlerRef<Error>,
    promql_handler: Option<PromHandlerRef>,

    create_expr_factory: CreateExprFactoryRef,

    /// plugins: this map holds extensions to customize query or auth
    /// behaviours.
    plugins: Arc<Plugins>,
}

impl Instance {
    pub async fn try_new_distributed(
        opts: &FrontendOptions,
        plugins: Arc<Plugins>,
    ) -> Result<Self> {
        let meta_client = Self::create_meta_client(opts).await?;

        let meta_backend = Arc::new(MetaKvBackend {
            client: meta_client.clone(),
        });
        let table_routes = Arc::new(TableRoutes::new(meta_client.clone()));
        let partition_manager = Arc::new(PartitionRuleManager::new(table_routes));
        let datanode_clients = Arc::new(DatanodeClients::default());

        let catalog_manager = Arc::new(FrontendCatalogManager::new(
            meta_backend,
            partition_manager,
            datanode_clients.clone(),
        ));

        let dist_instance = DistInstance::new(
            meta_client,
            catalog_manager.clone(),
            datanode_clients,
            plugins.clone(),
        );
        let dist_instance = Arc::new(dist_instance);

        Ok(Instance {
            catalog_manager,
            script_handler: None,
            create_expr_factory: Arc::new(DefaultCreateExprFactory),
            sql_handler: dist_instance.clone(),
            grpc_query_handler: dist_instance,
            promql_handler: None,
            plugins,
        })
    }

    async fn create_meta_client(opts: &FrontendOptions) -> Result<Arc<MetaClient>> {
        let metasrv_addr = &opts
            .meta_client_options
            .as_ref()
            .context(MissingMetasrvOptsSnafu)?
            .metasrv_addrs;
        info!(
            "Creating Frontend instance in distributed mode with Meta server addr {:?}",
            metasrv_addr
        );

        let meta_config = MetaClientOptions::default();
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
            sql_handler: StandaloneSqlQueryHandler::arc(dn_instance.clone()),
            grpc_query_handler: StandaloneGrpcQueryHandler::arc(dn_instance.clone()),
            promql_handler: Some(dn_instance.clone()),
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
            promql_handler: None,
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
        ctx: QueryContextRef,
    ) -> Result<Output> {
        let mut success = 0;
        for request in requests {
            match self.handle_insert(request, ctx.clone()).await? {
                Output::AffectedRows(rows) => success += rows,
                _ => unreachable!("Insert should not yield output other than AffectedRows"),
            }
        }
        Ok(Output::AffectedRows(success))
    }

    async fn handle_insert(&self, request: InsertRequest, ctx: QueryContextRef) -> Result<Output> {
        self.create_or_alter_table_on_demand(ctx.clone(), &request)
            .await?;

        let query = Request::Insert(request);
        GrpcQueryHandler::do_query(&*self.grpc_query_handler, query, ctx).await
    }

    // check if table already exist:
    // - if table does not exist, create table by inferred CreateExpr
    // - if table exist, check if schema matches. If any new column found, alter table by inferred `AlterExpr`
    async fn create_or_alter_table_on_demand(
        &self,
        ctx: QueryContextRef,
        request: &InsertRequest,
    ) -> Result<()> {
        let catalog_name = &ctx.current_catalog();
        let schema_name = &ctx.current_schema();
        let table_name = &request.table_name;
        let columns = &request.columns;

        let table = self
            .catalog_manager
            .table(catalog_name, schema_name, table_name)
            .await
            .context(error::CatalogSnafu)?;
        match table {
            None => {
                info!(
                    "Table {}.{}.{} does not exist, try create table",
                    catalog_name, schema_name, table_name,
                );
                self.create_table_by_columns(ctx, table_name, columns)
                    .await?;
                info!(
                    "Successfully created table on insertion: {}.{}.{}",
                    catalog_name, schema_name, table_name
                );
            }
            Some(table) => {
                let schema = table.schema();

                validate_insert_request(schema.as_ref(), request)?;

                if let Some(add_columns) = common_grpc_expr::find_new_columns(&schema, columns)
                    .context(error::FindNewColumnsOnInsertionSnafu)?
                {
                    info!(
                        "Find new columns {:?} on insertion, try to alter table: {}.{}.{}",
                        add_columns, catalog_name, schema_name, table_name
                    );
                    self.add_new_columns_to_table(ctx, table_name, add_columns)
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
        ctx: QueryContextRef,
        table_name: &str,
        columns: &[Column],
    ) -> Result<Output> {
        let catalog_name = &ctx.current_catalog();
        let schema_name = &ctx.current_schema();

        // Create table automatically, build schema from data.
        let create_expr = self
            .create_expr_factory
            .create_expr_by_columns(catalog_name, schema_name, table_name, columns)
            .await?;

        info!(
            "Try to create table: {} automatically with request: {:?}",
            table_name, create_expr,
        );

        self.grpc_query_handler
            .do_query(
                Request::Ddl(DdlRequest {
                    expr: Some(DdlExpr::CreateTable(create_expr)),
                }),
                ctx,
            )
            .await
    }

    async fn add_new_columns_to_table(
        &self,
        ctx: QueryContextRef,
        table_name: &str,
        add_columns: AddColumns,
    ) -> Result<Output> {
        debug!(
            "Adding new columns: {:?} to table: {}",
            add_columns, table_name
        );
        let expr = AlterExpr {
            catalog_name: ctx.current_catalog(),
            schema_name: ctx.current_schema(),
            table_name: table_name.to_string(),
            kind: Some(Kind::AddColumns(add_columns)),
        };

        self.grpc_query_handler
            .do_query(
                Request::Ddl(DdlRequest {
                    expr: Some(DdlExpr::Alter(expr)),
                }),
                ctx,
            )
            .await
    }

    fn handle_use(&self, db: String, query_ctx: QueryContextRef) -> Result<Output> {
        let catalog = &query_ctx.current_catalog();
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
    ParserContext::create_with_dialect(sql, &GenericDialect {}).context(ParseSqlSnafu)
}

impl Instance {
    async fn query_statement(&self, stmt: Statement, query_ctx: QueryContextRef) -> Result<Output> {
        check_permission(self.plugins.clone(), &stmt, &query_ctx)?;
        match stmt {
            Statement::CreateDatabase(_)
            | Statement::ShowDatabases(_)
            | Statement::CreateTable(_)
            | Statement::ShowTables(_)
            | Statement::DescribeTable(_)
            | Statement::Explain(_)
            | Statement::Query(_)
            | Statement::Insert(_)
            | Statement::Delete(_)
            | Statement::Alter(_)
            | Statement::DropTable(_)
            | Statement::Tql(_)
            | Statement::Copy(_) => self.sql_handler.do_statement_query(stmt, query_ctx).await,
            Statement::Use(db) => self.handle_use(db, query_ctx),
            Statement::ShowCreateTable(_) => NotSupportedSnafu {
                feat: format!("{stmt:?}"),
            }
            .fail(),
        }
    }
}

#[async_trait]
impl SqlQueryHandler for Instance {
    type Error = Error;

    async fn do_query(&self, query: &str, query_ctx: QueryContextRef) -> Vec<Result<Output>> {
        let query_interceptor = self.plugins.get::<SqlQueryInterceptorRef<Error>>();
        let query = match query_interceptor.pre_parsing(query, query_ctx.clone()) {
            Ok(q) => q,
            Err(e) => return vec![Err(e)],
        };

        match parse_stmt(query.as_ref())
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

    async fn do_promql_query(&self, query: &PromQuery, _: QueryContextRef) -> Vec<Result<Output>> {
        if let Some(handler) = &self.promql_handler {
            let result = handler.do_query(query).await.with_context(|_| {
                let query_literal = format!("{query:?}");
                ExecutePromqlSnafu {
                    query: query_literal,
                }
            });
            vec![result]
        } else {
            vec![Err(NotSupportedSnafu {
                feat: "PromQL Query",
            }
            .build())]
        }
    }

    async fn do_statement_query(
        &self,
        stmt: Statement,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        let query_interceptor = self.plugins.get::<SqlQueryInterceptorRef<Error>>();

        // TODO(sunng87): figure out at which stage we can call
        // this hook after ArrowFlight adoption. We need to provide
        // LogicalPlan as to this hook.
        query_interceptor.pre_execute(&stmt, None, query_ctx.clone())?;
        self.query_statement(stmt, query_ctx.clone())
            .await
            .and_then(|output| query_interceptor.post_execute(output, query_ctx.clone()))
    }

    async fn do_describe(
        &self,
        stmt: Statement,
        query_ctx: QueryContextRef,
    ) -> Result<Option<Schema>> {
        self.sql_handler.do_describe(stmt, query_ctx).await
    }

    fn is_valid_schema(&self, catalog: &str, schema: &str) -> Result<bool> {
        self.catalog_manager
            .schema(catalog, schema)
            .map(|s| s.is_some())
            .context(error::CatalogSnafu)
    }
}

#[async_trait]
impl ScriptHandler for Instance {
    async fn insert_script(
        &self,
        schema: &str,
        name: &str,
        script: &str,
    ) -> server_error::Result<()> {
        if let Some(handler) = &self.script_handler {
            handler.insert_script(schema, name, script).await
        } else {
            server_error::NotSupportedSnafu {
                feat: "Script execution in Frontend",
            }
            .fail()
        }
    }

    async fn execute_script(
        &self,
        schema: &str,
        script: &str,
        params: HashMap<String, String>,
    ) -> server_error::Result<Output> {
        if let Some(handler) = &self.script_handler {
            handler.execute_script(schema, script, params).await
        } else {
            server_error::NotSupportedSnafu {
                feat: "Script execution in Frontend",
            }
            .fail()
        }
    }
}

#[async_trait]
impl PromHandler for Instance {
    async fn do_query(&self, query: &PromQuery) -> server_error::Result<Output> {
        if let Some(promql_handler) = &self.promql_handler {
            promql_handler.do_query(query).await
        } else {
            server_error::NotSupportedSnafu {
                feat: "PromQL query in Frontend",
            }
            .fail()
        }
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
        // query,explain and tql will be checked in QueryEngineState
        Statement::Query(_) | Statement::Explain(_) | Statement::Tql(_) => {}
        // database ops won't be checked
        Statement::CreateDatabase(_) | Statement::ShowDatabases(_) | Statement::Use(_) => {}
        // show create table and alter are not supported yet
        Statement::ShowCreateTable(_) | Statement::Alter(_) => {}

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
                validate_catalog_and_schema(&query_ctx.current_catalog(), database, query_ctx)
                    .map_err(BoxedError::new)
                    .context(SqlExecInterceptedSnafu)?;
            }
        }
        Statement::DescribeTable(stmt) => {
            validate_param(stmt.name(), query_ctx)?;
        }
        Statement::Delete(delete) => {
            validate_param(delete.table_name(), query_ctx)?;
        }
        Statement::Copy(stmd) => match stmd {
            CopyTable::To(copy_table_to) => validate_param(copy_table_to.table_name(), query_ctx)?,
            CopyTable::From(copy_table_from) => {
                validate_param(&copy_table_from.table_name, query_ctx)?
            }
        },
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

fn validate_insert_request(schema: &Schema, request: &InsertRequest) -> Result<()> {
    for column_schema in schema.column_schemas() {
        if column_schema.is_nullable() || column_schema.default_constraint().is_some() {
            continue;
        }
        let not_null = request
            .columns
            .iter()
            .find(|x| x.column_name == column_schema.name)
            .map(|column| column.null_mask.is_empty() || column.null_mask.iter().all(|x| *x == 0));
        ensure!(
            not_null == Some(true),
            InvalidInsertRequestSnafu {
                reason: format!(
                    "Expecting insert data to be presented on a not null or no default value column '{}'.",
                    &column_schema.name
                )
            }
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::collections::HashMap;
    use std::sync::atomic::AtomicU32;

    use api::v1::column::Values;
    use catalog::helper::{TableGlobalKey, TableGlobalValue};
    use datatypes::prelude::{ConcreteDataType, Value};
    use datatypes::schema::{ColumnDefaultConstraint, ColumnSchema};
    use query::query_engine::options::QueryOptions;
    use session::context::QueryContext;
    use strfmt::Format;

    use super::*;
    use crate::table::DistTable;
    use crate::tests;
    use crate::tests::MockDistributedInstance;

    #[test]
    fn test_validate_insert_request() {
        let schema = Schema::new(vec![
            ColumnSchema::new("a", ConcreteDataType::int32_datatype(), true)
                .with_default_constraint(None)
                .unwrap(),
            ColumnSchema::new("b", ConcreteDataType::int32_datatype(), true)
                .with_default_constraint(Some(ColumnDefaultConstraint::Value(Value::Int32(100))))
                .unwrap(),
        ]);
        let request = InsertRequest {
            columns: vec![Column {
                column_name: "c".to_string(),
                values: Some(Values {
                    i32_values: vec![1],
                    ..Default::default()
                }),
                null_mask: vec![0],
                ..Default::default()
            }],
            ..Default::default()
        };
        // If nullable is true, it doesn't matter whether the insert request has the column.
        assert!(validate_insert_request(&schema, &request).is_ok());

        let schema = Schema::new(vec![
            ColumnSchema::new("a", ConcreteDataType::int32_datatype(), false)
                .with_default_constraint(None)
                .unwrap(),
            ColumnSchema::new("b", ConcreteDataType::int32_datatype(), false)
                .with_default_constraint(Some(ColumnDefaultConstraint::Value(Value::Int32(-100))))
                .unwrap(),
        ]);
        let request = InsertRequest {
            columns: vec![Column {
                column_name: "a".to_string(),
                values: Some(Values {
                    i32_values: vec![1],
                    ..Default::default()
                }),
                null_mask: vec![0],
                ..Default::default()
            }],
            ..Default::default()
        };
        // If nullable is false, but the column is defined with default value,
        // it also doesn't matter whether the insert request has the column.
        assert!(validate_insert_request(&schema, &request).is_ok());

        let request = InsertRequest {
            columns: vec![Column {
                column_name: "b".to_string(),
                values: Some(Values {
                    i32_values: vec![1],
                    ..Default::default()
                }),
                null_mask: vec![0],
                ..Default::default()
            }],
            ..Default::default()
        };
        // Neither of the above cases.
        assert!(validate_insert_request(&schema, &request).is_err());
    }

    #[test]
    fn test_exec_validation() {
        let query_ctx = Arc::new(QueryContext::new());
        let mut plugins = Plugins::new();
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
        let stmts = parse_stmt(sql).unwrap();
        assert_eq!(stmts.len(), 4);
        for stmt in stmts {
            let re = check_permission(plugins.clone(), &stmt, &query_ctx);
            assert!(re.is_ok());
        }

        let sql = r#"
        SHOW CREATE TABLE demo;
        ALTER TABLE demo ADD COLUMN new_col INT;
        "#;
        let stmts = parse_stmt(sql).unwrap();
        assert_eq!(stmts.len(), 2);
        for stmt in stmts {
            let re = check_permission(plugins.clone(), &stmt, &query_ctx);
            assert!(re.is_ok());
        }

        let sql = "USE randomschema";
        let stmts = parse_stmt(sql).unwrap();
        let re = check_permission(plugins.clone(), &stmts[0], &query_ctx);
        assert!(re.is_ok());

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
            let mut vars = HashMap::new();
            vars.insert("catalog".to_string(), catalog);
            vars.insert("schema".to_string(), schema);

            template.format(&vars).unwrap()
        }

        fn do_test(sql: &str, plugins: Arc<Plugins>, query_ctx: &QueryContextRef, is_ok: bool) {
            let stmt = &parse_stmt(sql).unwrap()[0];
            let re = check_permission(plugins.clone(), stmt, query_ctx);
            if is_ok {
                assert!(re.is_ok());
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
        let stmt = parse_stmt(sql).unwrap();
        let re = check_permission(plugins.clone(), &stmt[0], &query_ctx);
        assert!(re.is_ok());

        let sql = "SHOW TABLES FROM wrongschema";
        let stmt = parse_stmt(sql).unwrap();
        let re = check_permission(plugins.clone(), &stmt[0], &query_ctx);
        assert!(re.is_err());

        // test describe table
        let sql = "DESC TABLE {catalog}{schema}demo;";
        replace_test(sql, plugins.clone(), &query_ctx);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_standalone_exec_sql() {
        let standalone = tests::create_standalone_instance("test_standalone_exec_sql").await;
        let instance = standalone.instance.as_ref();

        let sql = r#"
            CREATE TABLE demo(
                host STRING,
                ts TIMESTAMP,
                cpu DOUBLE NULL,
                memory DOUBLE NULL,
                disk_util DOUBLE DEFAULT 9.9,
                TIME INDEX (ts),
                PRIMARY KEY(host)
            ) engine=mito"#;
        create_table(instance, sql).await;

        insert_and_query(instance).await;

        drop_table(instance).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_distributed_exec_sql() {
        let distributed = tests::create_distributed_instance("test_distributed_exec_sql").await;
        let instance = distributed.frontend.as_ref();

        let sql = r#"
            CREATE TABLE demo(
                host STRING,
                ts TIMESTAMP,
                cpu DOUBLE NULL,
                memory DOUBLE NULL,
                disk_util DOUBLE DEFAULT 9.9,
                TIME INDEX (ts),
                PRIMARY KEY(host)
            )
            PARTITION BY RANGE COLUMNS (host) (
                PARTITION r0 VALUES LESS THAN ('550-A'),
                PARTITION r1 VALUES LESS THAN ('550-W'),
                PARTITION r2 VALUES LESS THAN ('MOSS'),
                PARTITION r3 VALUES LESS THAN (MAXVALUE),
            )
            engine=mito"#;
        create_table(instance, sql).await;

        insert_and_query(instance).await;

        verify_data_distribution(
            &distributed,
            HashMap::from([
                (
                    0u32,
                    "\
+---------------------+------+
| ts                  | host |
+---------------------+------+
| 2013-12-31T16:00:00 | 490  |
+---------------------+------+",
                ),
                (
                    1u32,
                    "\
+---------------------+-------+
| ts                  | host  |
+---------------------+-------+
| 2022-12-31T16:00:00 | 550-A |
+---------------------+-------+",
                ),
                (
                    2u32,
                    "\
+---------------------+-------+
| ts                  | host  |
+---------------------+-------+
| 2023-12-31T16:00:00 | 550-W |
+---------------------+-------+",
                ),
                (
                    3u32,
                    "\
+---------------------+------+
| ts                  | host |
+---------------------+------+
| 2043-12-31T16:00:00 | MOSS |
+---------------------+------+",
                ),
            ]),
        )
        .await;

        drop_table(instance).await;

        verify_table_is_dropped(&distributed).await;
    }

    async fn query(instance: &Instance, sql: &str) -> Output {
        SqlQueryHandler::do_query(instance, sql, QueryContext::arc())
            .await
            .remove(0)
            .unwrap()
    }

    async fn create_table(instance: &Instance, sql: &str) {
        let output = query(instance, sql).await;
        let Output::AffectedRows(x) = output else { unreachable!() };
        assert_eq!(x, 0);
    }

    async fn insert_and_query(instance: &Instance) {
        let sql = r#"INSERT INTO demo(host, cpu, memory, ts) VALUES
                                ('490', 0.1, 1, 1388505600000),
                                ('550-A', 1, 100, 1672502400000),
                                ('550-W', 10000, 1000000, 1704038400000),
                                ('MOSS', 100000000, 10000000000, 2335190400000)
                                "#;
        let output = query(instance, sql).await;
        let Output::AffectedRows(x) = output else { unreachable!() };
        assert_eq!(x, 4);

        let sql = "SELECT * FROM demo WHERE ts > cast(1000000000 as timestamp) ORDER BY host"; // use nanoseconds as where condition
        let output = query(instance, sql).await;
        let Output::Stream(s) = output else { unreachable!() };
        let batches = common_recordbatch::util::collect_batches(s).await.unwrap();
        let pretty_print = batches.pretty_print().unwrap();
        let expected = "\
+-------+---------------------+-------------+-----------+-----------+
| host  | ts                  | cpu         | memory    | disk_util |
+-------+---------------------+-------------+-----------+-----------+
| 490   | 2013-12-31T16:00:00 | 0.1         | 1.0       | 9.9       |
| 550-A | 2022-12-31T16:00:00 | 1.0         | 100.0     | 9.9       |
| 550-W | 2023-12-31T16:00:00 | 10000.0     | 1000000.0 | 9.9       |
| MOSS  | 2043-12-31T16:00:00 | 100000000.0 | 1.0e10    | 9.9       |
+-------+---------------------+-------------+-----------+-----------+";
        assert_eq!(pretty_print, expected);
    }

    async fn verify_data_distribution(
        instance: &MockDistributedInstance,
        expected_distribution: HashMap<u32, &str>,
    ) {
        let table = instance
            .frontend
            .catalog_manager()
            .table("greptime", "public", "demo")
            .await
            .unwrap()
            .unwrap();
        let table = table.as_any().downcast_ref::<DistTable>().unwrap();

        let TableGlobalValue { regions_id_map, .. } = table
            .table_global_value(&TableGlobalKey {
                catalog_name: "greptime".to_string(),
                schema_name: "public".to_string(),
                table_name: "demo".to_string(),
            })
            .await
            .unwrap()
            .unwrap();
        let region_to_dn_map = regions_id_map
            .iter()
            .map(|(k, v)| (v[0], *k))
            .collect::<HashMap<u32, u64>>();
        assert_eq!(region_to_dn_map.len(), expected_distribution.len());

        for (region, dn) in region_to_dn_map.iter() {
            let dn = instance.datanodes.get(dn).unwrap();
            let output = dn
                .execute_sql("SELECT ts, host FROM demo ORDER BY ts", QueryContext::arc())
                .await
                .unwrap();
            let Output::Stream(stream) = output else { unreachable!() };
            let recordbatches = RecordBatches::try_collect(stream).await.unwrap();
            let actual = recordbatches.pretty_print().unwrap();

            let expected = expected_distribution.get(region).unwrap();
            assert_eq!(&actual, expected);
        }
    }

    async fn drop_table(instance: &Instance) {
        let sql = "DROP TABLE demo";
        let output = query(instance, sql).await;
        let Output::AffectedRows(x) = output else { unreachable!() };
        assert_eq!(x, 1);
    }

    async fn verify_table_is_dropped(instance: &MockDistributedInstance) {
        for (_, dn) in instance.datanodes.iter() {
            assert!(dn
                .catalog_manager()
                .table("greptime", "public", "demo")
                .await
                .unwrap()
                .is_none())
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_sql_interceptor_plugin() {
        #[derive(Default)]
        struct AssertionHook {
            pub(crate) c: AtomicU32,
        }

        impl SqlQueryInterceptor for AssertionHook {
            type Error = Error;

            fn pre_parsing<'a>(
                &self,
                query: &'a str,
                _query_ctx: QueryContextRef,
            ) -> Result<Cow<'a, str>> {
                self.c.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                assert!(query.starts_with("CREATE TABLE demo"));
                Ok(Cow::Borrowed(query))
            }

            fn post_parsing(
                &self,
                statements: Vec<Statement>,
                _query_ctx: QueryContextRef,
            ) -> Result<Vec<Statement>> {
                self.c.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                assert!(matches!(statements[0], Statement::CreateTable(_)));
                Ok(statements)
            }

            fn pre_execute(
                &self,
                _statement: &Statement,
                _plan: Option<&query::plan::LogicalPlan>,
                _query_ctx: QueryContextRef,
            ) -> Result<()> {
                self.c.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Ok(())
            }

            fn post_execute(
                &self,
                mut output: Output,
                _query_ctx: QueryContextRef,
            ) -> Result<Output> {
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
        plugins.insert::<SqlQueryInterceptorRef<Error>>(counter_hook.clone());
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
            type Error = Error;

            fn post_parsing(
                &self,
                statements: Vec<Statement>,
                _query_ctx: QueryContextRef,
            ) -> Result<Vec<Statement>> {
                for s in &statements {
                    match s {
                        Statement::CreateDatabase(_) | Statement::ShowDatabases(_) => {
                            return Err(Error::NotSupported {
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
        plugins.insert::<SqlQueryInterceptorRef<Error>>(hook.clone());
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
            assert!(matches!(e, error::Error::NotSupported { .. }));
        } else {
            unreachable!();
        }

        let sql = r#"SELECT 1; SHOW DATABASES"#;
        if let Err(e) = SqlQueryHandler::do_query(&*instance, sql, query_ctx.clone())
            .await
            .remove(0)
        {
            assert!(matches!(e, error::Error::NotSupported { .. }));
        } else {
            unreachable!();
        }
    }
}
