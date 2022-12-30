// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

pub(crate) mod distributed;
mod influxdb;
mod opentsdb;
mod prometheus;

use std::sync::Arc;
use std::time::Duration;

use api::result::ObjectResultBuilder;
use api::v1::alter_expr::Kind;
use api::v1::ddl_request::Expr as DdlExpr;
use api::v1::object_expr::Request;
use api::v1::{
    AddColumns, AlterExpr, Column, CreateTableExpr, DdlRequest, DropTableExpr, InsertRequest,
    ObjectExpr, ObjectResult as GrpcObjectResult,
};
use async_trait::async_trait;
use catalog::remote::MetaKvBackend;
use catalog::{CatalogManagerRef, CatalogProviderRef, SchemaProviderRef};
use client::RpcOutput;
use common_catalog::consts::DEFAULT_CATALOG_NAME;
use common_error::prelude::BoxedError;
use common_grpc::channel_manager::{ChannelConfig, ChannelManager};
use common_grpc::flight::{FlightEncoder, FlightMessage};
use common_query::Output;
use common_recordbatch::RecordBatches;
use common_telemetry::{debug, info};
use datanode::instance::InstanceRef as DnInstanceRef;
use distributed::DistInstance;
use meta_client::client::{MetaClient, MetaClientBuilder};
use meta_client::MetaClientOpts;
use servers::interceptor::{SqlQueryInterceptor, SqlQueryInterceptorRef};
use servers::query_handler::{
    GrpcQueryHandler, GrpcQueryHandlerRef, InfluxdbLineProtocolHandler, OpentsdbProtocolHandler,
    PrometheusProtocolHandler, ScriptHandler, ScriptHandlerRef, SqlQueryHandler,
    SqlQueryHandlerRef,
};
use servers::{error as server_error, Mode};
use session::context::QueryContextRef;
use snafu::prelude::*;
use sql::dialect::GenericDialect;
use sql::parser::ParserContext;
use sql::statements::create::Partitions;
use sql::statements::insert::Insert;
use sql::statements::statement::Statement;
use table::TableRef;

use crate::catalog::FrontendCatalogManager;
use crate::datanode::DatanodeClients;
use crate::error::{
    self, CatalogSnafu, FindNewColumnsOnInsertionSnafu, InsertSnafu, MissingMetasrvOptsSnafu,
    RequestDatanodeSnafu, Result,
};
use crate::expr_factory::{CreateExprFactoryRef, DefaultCreateExprFactory};
use crate::frontend::FrontendOptions;
use crate::sql::insert_to_request;
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
    create_expr_factory: CreateExprFactoryRef,
    // TODO(fys): it should be a trait that corresponds to two implementations:
    // Standalone and Distributed, then the code behind it doesn't need to use so
    // many match statements.
    mode: Mode,

    // TODO(LFC): Remove `dist_instance` together with Arrow Flight adoption refactor.
    dist_instance: Option<DistInstance>,

    sql_handler: SqlQueryHandlerRef,
    grpc_query_handler: GrpcQueryHandlerRef,

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
        let dist_instance_ref = Arc::new(dist_instance.clone());

        Ok(Instance {
            catalog_manager,
            script_handler: None,
            create_expr_factory: Arc::new(DefaultCreateExprFactory),
            mode: Mode::Distributed,
            dist_instance: Some(dist_instance),
            sql_handler: dist_instance_ref.clone(),
            grpc_query_handler: dist_instance_ref,
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
            mode: Mode::Standalone,
            dist_instance: None,
            sql_handler: dn_instance.clone(),
            grpc_query_handler: dn_instance.clone(),
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

    /// Handle create expr.
    pub async fn handle_create_table(
        &self,
        mut expr: CreateTableExpr,
        partitions: Option<Partitions>,
    ) -> Result<Output> {
        if let Some(v) = &self.dist_instance {
            v.create_table(&mut expr, partitions).await
        } else {
            let result = self
                .grpc_query_handler
                .do_query(ObjectExpr {
                    request: Some(Request::Ddl(DdlRequest {
                        expr: Some(DdlExpr::CreateTable(expr)),
                    })),
                })
                .await
                .context(error::InvokeGrpcServerSnafu)?;
            let output: RpcOutput = result.try_into().context(RequestDatanodeSnafu)?;
            Ok(output.into())
        }
    }

    /// Handle batch inserts
    pub async fn handle_inserts(&self, requests: Vec<InsertRequest>) -> Result<Output> {
        let mut success = 0;
        for request in requests {
            match self.handle_insert(request).await? {
                Output::AffectedRows(rows) => success += rows,
                _ => unreachable!("Insert should not yield output other than AffectedRows"),
            }
        }
        Ok(Output::AffectedRows(success))
    }

    // TODO(LFC): Revisit GRPC insertion feature, check if the "create/alter table on demand" functionality is broken.
    // Should be supplied with enough tests.
    async fn handle_insert(&self, request: InsertRequest) -> Result<Output> {
        let schema_name = &request.schema_name;
        let table_name = &request.table_name;
        let catalog_name = DEFAULT_CATALOG_NAME;

        let columns = &request.columns;

        self.create_or_alter_table_on_demand(catalog_name, schema_name, table_name, columns)
            .await?;

        let query = ObjectExpr {
            request: Some(Request::Insert(request)),
        };
        let result = GrpcQueryHandler::do_query(&*self.grpc_query_handler, query)
            .await
            .context(error::InvokeGrpcServerSnafu)?;
        let result: RpcOutput = result.try_into().context(InsertSnafu)?;
        Ok(result.into())
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
    ) -> Result<()> {
        match self.find_table(catalog_name, schema_name, table_name)? {
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
                    .context(FindNewColumnsOnInsertionSnafu)?
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
    ) -> Result<Output> {
        // Create table automatically, build schema from data.
        let create_expr = self
            .create_expr_factory
            .create_expr_by_columns(catalog_name, schema_name, table_name, columns)
            .await?;

        info!(
            "Try to create table: {} automatically with request: {:?}",
            table_name, create_expr,
        );
        // Create-on-insert does support partition by other columns now
        self.handle_create_table(create_expr, None).await
    }

    async fn add_new_columns_to_table(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
        add_columns: AddColumns,
    ) -> Result<Output> {
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

        let result = self
            .grpc_query_handler
            .do_query(ObjectExpr {
                request: Some(Request::Ddl(DdlRequest {
                    expr: Some(DdlExpr::Alter(expr)),
                })),
            })
            .await
            .context(error::InvokeGrpcServerSnafu)?;
        let output: RpcOutput = result.try_into().context(RequestDatanodeSnafu)?;
        Ok(output.into())
    }

    fn get_catalog(&self, catalog_name: &str) -> Result<CatalogProviderRef> {
        self.catalog_manager
            .catalog(catalog_name)
            .context(error::CatalogSnafu)?
            .context(error::CatalogNotFoundSnafu { catalog_name })
    }

    fn get_schema(provider: CatalogProviderRef, schema_name: &str) -> Result<SchemaProviderRef> {
        provider
            .schema(schema_name)
            .context(error::CatalogSnafu)?
            .context(error::SchemaNotFoundSnafu {
                schema_info: schema_name,
            })
    }

    fn find_table(&self, catalog: &str, schema: &str, table: &str) -> Result<Option<TableRef>> {
        self.catalog_manager
            .table(catalog, schema, table)
            .context(CatalogSnafu)
    }

    async fn sql_dist_insert(&self, insert: Box<Insert>) -> Result<usize> {
        let (catalog, schema, table) = insert.full_table_name().context(error::ParseSqlSnafu)?;

        let catalog_provider = self.get_catalog(&catalog)?;
        let schema_provider = Self::get_schema(catalog_provider, &schema)?;

        let insert_request = insert_to_request(&schema_provider, *insert)?;

        let (columns, _row_count) =
            crate::table::insert::insert_request_to_insert_batch(&insert_request)?;

        self.create_or_alter_table_on_demand(&catalog, &schema, &table, &columns)
            .await?;

        let table = schema_provider
            .table(&table)
            .context(error::CatalogSnafu)?
            .context(error::TableNotFoundSnafu { table_name: &table })?;

        table
            .insert(insert_request)
            .await
            .context(error::TableSnafu)
    }

    fn handle_use(&self, db: String, query_ctx: QueryContextRef) -> Result<Output> {
        ensure!(
            self.catalog_manager
                .schema(DEFAULT_CATALOG_NAME, &db)
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
            | Statement::Query(_) => {
                return self.sql_handler.do_statement_query(stmt, query_ctx).await;
            }
            Statement::Insert(insert) => match self.mode {
                Mode::Standalone => {
                    return self.sql_handler.do_statement_query(stmt, query_ctx).await
                }
                Mode::Distributed => {
                    let affected = self
                        .sql_dist_insert(insert)
                        .await
                        .map_err(BoxedError::new)
                        .context(server_error::ExecuteInsertSnafu {
                            msg: "execute insert failed",
                        })?;
                    Ok(Output::AffectedRows(affected))
                }
            },
            Statement::Alter(alter_stmt) => {
                let expr = AlterExpr::try_from(alter_stmt)
                    .map_err(BoxedError::new)
                    .context(server_error::ExecuteAlterSnafu { query })?;
                let result = self
                    .grpc_query_handler
                    .do_query(ObjectExpr {
                        request: Some(Request::Ddl(DdlRequest {
                            expr: Some(DdlExpr::Alter(expr)),
                        })),
                    })
                    .await?;
                let output: RpcOutput = result
                    .try_into()
                    .map_err(BoxedError::new)
                    .context(server_error::ExecuteQuerySnafu { query })?;
                Ok(output.into())
            }
            Statement::DropTable(drop_stmt) => {
                let expr = DropTableExpr {
                    catalog_name: drop_stmt.catalog_name,
                    schema_name: drop_stmt.schema_name,
                    table_name: drop_stmt.table_name,
                };
                let result = self
                    .grpc_query_handler
                    .do_query(ObjectExpr {
                        request: Some(Request::Ddl(DdlRequest {
                            expr: Some(DdlExpr::DropTable(expr)),
                        })),
                    })
                    .await?;
                let output: RpcOutput = result
                    .try_into()
                    .map_err(BoxedError::new)
                    .context(server_error::ExecuteQuerySnafu { query })?;
                Ok(output.into())
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

#[async_trait]
impl GrpcQueryHandler for Instance {
    async fn do_query(&self, query: ObjectExpr) -> server_error::Result<GrpcObjectResult> {
        let request = query
            .clone()
            .request
            .context(server_error::InvalidQuerySnafu {
                reason: "empty expr",
            })?;
        match request {
            Request::Insert(request) => {
                let output = self
                    .handle_insert(request.clone())
                    .await
                    .map_err(BoxedError::new)
                    .with_context(|_| server_error::ExecuteQuerySnafu {
                        query: format!("{request:?}"),
                    })?;
                let object_result = match output {
                    Output::AffectedRows(rows) => ObjectResultBuilder::default()
                        .flight_data(vec![
                            FlightEncoder::default().encode(FlightMessage::AffectedRows(rows))
                        ])
                        .build(),
                    _ => unreachable!(),
                };
                Ok(object_result)
            }
            _ => GrpcQueryHandler::do_query(&*self.grpc_query_handler, query).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::iter;
    use std::sync::atomic::AtomicU32;

    use api::v1::column::SemanticType;
    use api::v1::{
        column, query_request, Column, ColumnDataType, ColumnDef as GrpcColumnDef, QueryRequest,
    };
    use common_grpc::flight::{raw_flight_data_to_message, FlightMessage};
    use common_recordbatch::RecordBatch;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::{ColumnDefaultConstraint, ColumnSchema, Schema};
    use datatypes::value::Value;
    use datatypes::vectors::{Float64Vector, StringVector, TimestampMillisecondVector};
    use session::context::QueryContext;

    use super::*;
    use crate::tests;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_execute_sql() {
        let query_ctx = Arc::new(QueryContext::new());

        let (instance, _guard) = tests::create_frontend_instance("test_execute_sql").await;

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
            Output::AffectedRows(rows) => assert_eq!(rows, 1),
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
    async fn test_execute_grpc() {
        let (instance, _guard) = tests::create_frontend_instance("test_execute_grpc").await;

        // testing data:
        let expected_host_col = Column {
            column_name: "host".to_string(),
            values: Some(column::Values {
                string_values: vec!["fe.host.a", "fe.host.b", "fe.host.c", "fe.host.d"]
                    .into_iter()
                    .map(|s| s.to_string())
                    .collect(),
                ..Default::default()
            }),
            semantic_type: SemanticType::Field as i32,
            datatype: ColumnDataType::String as i32,
            ..Default::default()
        };
        let expected_cpu_col = Column {
            column_name: "cpu".to_string(),
            values: Some(column::Values {
                f64_values: vec![1.0, 3.0, 4.0],
                ..Default::default()
            }),
            null_mask: vec![2],
            semantic_type: SemanticType::Field as i32,
            datatype: ColumnDataType::Float64 as i32,
        };
        let expected_mem_col = Column {
            column_name: "memory".to_string(),
            values: Some(column::Values {
                f64_values: vec![100.0, 200.0, 400.0],
                ..Default::default()
            }),
            null_mask: vec![4],
            semantic_type: SemanticType::Field as i32,
            datatype: ColumnDataType::Float64 as i32,
        };
        let expected_ts_col = Column {
            column_name: "ts".to_string(),
            values: Some(column::Values {
                ts_millisecond_values: vec![1000, 2000, 3000, 4000],
                ..Default::default()
            }),
            semantic_type: SemanticType::Timestamp as i32,
            datatype: ColumnDataType::TimestampMillisecond as i32,
            ..Default::default()
        };

        // create
        let result = GrpcQueryHandler::do_query(
            &*instance,
            ObjectExpr {
                request: Some(Request::Ddl(DdlRequest {
                    expr: Some(DdlExpr::CreateTable(create_expr())),
                })),
            },
        )
        .await
        .unwrap();
        let output: RpcOutput = result.try_into().unwrap();
        assert!(matches!(output, RpcOutput::AffectedRows(1)));

        // insert
        let columns = vec![
            expected_host_col.clone(),
            expected_cpu_col.clone(),
            expected_mem_col.clone(),
            expected_ts_col.clone(),
        ];
        let row_count = 4;
        let request = InsertRequest {
            schema_name: "public".to_string(),
            table_name: "demo".to_string(),
            region_number: 0,
            columns,
            row_count,
        };
        let object_expr = ObjectExpr {
            request: Some(Request::Insert(request)),
        };
        let result = GrpcQueryHandler::do_query(&*instance, object_expr)
            .await
            .unwrap();
        let raw_data = result.flight_data;
        let mut flight_messages = raw_flight_data_to_message(raw_data).unwrap();
        assert_eq!(flight_messages.len(), 1);
        let message = flight_messages.remove(0);
        assert!(matches!(message, FlightMessage::AffectedRows(4)));

        // select
        let object_expr = ObjectExpr {
            request: Some(Request::Query(QueryRequest {
                query: Some(query_request::Query::Sql("select * from demo".to_string())),
            })),
        };
        let result = GrpcQueryHandler::do_query(&*instance, object_expr)
            .await
            .unwrap();
        let raw_data = result.flight_data;
        let mut flight_messages = raw_flight_data_to_message(raw_data).unwrap();
        assert_eq!(flight_messages.len(), 2);

        let expected_schema = Arc::new(Schema::new(vec![
            ColumnSchema::new("host", ConcreteDataType::string_datatype(), false),
            ColumnSchema::new("cpu", ConcreteDataType::float64_datatype(), true),
            ColumnSchema::new("memory", ConcreteDataType::float64_datatype(), true),
            ColumnSchema::new("disk_util", ConcreteDataType::float64_datatype(), true)
                .with_default_constraint(Some(ColumnDefaultConstraint::Value(Value::from(9.9f64))))
                .unwrap(),
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                true,
            )
            .with_time_index(true),
        ]));
        match flight_messages.remove(0) {
            FlightMessage::Schema(schema) => {
                assert_eq!(schema, expected_schema);
            }
            _ => unreachable!(),
        }

        match flight_messages.remove(0) {
            FlightMessage::Recordbatch(recordbatch) => {
                let expect_recordbatch = RecordBatch::new(
                    expected_schema,
                    vec![
                        Arc::new(StringVector::from(vec![
                            "fe.host.a",
                            "fe.host.b",
                            "fe.host.c",
                            "fe.host.d",
                        ])) as _,
                        Arc::new(Float64Vector::from(vec![
                            Some(1.0f64),
                            None,
                            Some(3.0f64),
                            Some(4.0f64),
                        ])) as _,
                        Arc::new(Float64Vector::from(vec![
                            Some(100f64),
                            Some(200f64),
                            None,
                            Some(400f64),
                        ])) as _,
                        Arc::new(Float64Vector::from_vec(
                            iter::repeat(9.9f64).take(4).collect(),
                        )) as _,
                        Arc::new(TimestampMillisecondVector::from_vec(vec![
                            1000i64, 2000, 3000, 4000,
                        ])) as _,
                    ],
                )
                .unwrap();
                assert_eq!(recordbatch, expect_recordbatch);
            }
            _ => unreachable!(),
        }
    }

    fn create_expr() -> CreateTableExpr {
        let column_defs = vec![
            GrpcColumnDef {
                name: "host".to_string(),
                datatype: ColumnDataType::String as i32,
                is_nullable: false,
                default_constraint: vec![],
            },
            GrpcColumnDef {
                name: "cpu".to_string(),
                datatype: ColumnDataType::Float64 as i32,
                is_nullable: true,
                default_constraint: vec![],
            },
            GrpcColumnDef {
                name: "memory".to_string(),
                datatype: ColumnDataType::Float64 as i32,
                is_nullable: true,
                default_constraint: vec![],
            },
            GrpcColumnDef {
                name: "disk_util".to_string(),
                datatype: ColumnDataType::Float64 as i32,
                is_nullable: true,
                default_constraint: ColumnDefaultConstraint::Value(Value::from(9.9f64))
                    .try_into()
                    .unwrap(),
            },
            GrpcColumnDef {
                name: "ts".to_string(),
                datatype: ColumnDataType::TimestampMillisecond as i32,
                is_nullable: true,
                default_constraint: vec![],
            },
        ];
        CreateTableExpr {
            catalog_name: "".to_string(),
            schema_name: "".to_string(),
            table_name: "demo".to_string(),
            desc: "".to_string(),
            column_defs,
            time_index: "ts".to_string(),
            primary_keys: vec!["host".to_string()],
            create_if_not_exists: true,
            table_options: Default::default(),
            table_id: None,
            region_ids: vec![0],
        }
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
                        assert_eq!(*rows, 1);
                        // update output result
                        *rows = 10;
                    }
                    _ => unreachable!(),
                }
                Ok(output)
            }
        }

        let query_ctx = Arc::new(QueryContext::new());
        let (mut instance, _guard) = tests::create_frontend_instance("test_hook").await;

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
        let output = SqlQueryHandler::do_query(&*instance, sql, query_ctx.clone())
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
        let (mut instance, _guard) = tests::create_frontend_instance("test_db_hook").await;

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
            Output::AffectedRows(rows) => assert_eq!(rows, 1),
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
