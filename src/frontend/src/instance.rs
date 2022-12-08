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
use api::v1::object_expr::Expr;
use api::v1::{
    admin_expr, select_expr, AddColumns, AdminExpr, AdminResult, AlterExpr, Column,
    CreateDatabaseExpr, CreateExpr, DropTableExpr, InsertExpr, ObjectExpr,
    ObjectResult as GrpcObjectResult,
};
use async_trait::async_trait;
use catalog::remote::MetaKvBackend;
use catalog::{CatalogManagerRef, CatalogProviderRef, SchemaProviderRef};
use client::admin::{admin_result_to_output, Admin};
use client::{Client, Database, Select};
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_error::prelude::{BoxedError, StatusCode};
use common_grpc::channel_manager::{ChannelConfig, ChannelManager};
use common_grpc::select::to_object_result;
use common_query::Output;
use common_recordbatch::RecordBatches;
use common_telemetry::{debug, error, info};
use distributed::DistInstance;
use meta_client::client::MetaClientBuilder;
use meta_client::MetaClientOpts;
use servers::query_handler::{
    GrpcAdminHandler, GrpcQueryHandler, InfluxdbLineProtocolHandler, OpentsdbProtocolHandler,
    PrometheusProtocolHandler, ScriptHandler, ScriptHandlerRef, SqlQueryHandler,
};
use servers::{error as server_error, Mode};
use session::context::{QueryContext, QueryContextRef};
use snafu::prelude::*;
use sql::dialect::GenericDialect;
use sql::parser::ParserContext;
use sql::statements::create::Partitions;
use sql::statements::explain::Explain;
use sql::statements::insert::Insert;
use sql::statements::statement::Statement;

use crate::catalog::FrontendCatalogManager;
use crate::datanode::DatanodeClients;
use crate::error::{
    self, AlterTableOnInsertionSnafu, AlterTableSnafu, CatalogNotFoundSnafu, CatalogSnafu,
    CreateDatabaseSnafu, CreateTableSnafu, DropTableSnafu, FindNewColumnsOnInsertionSnafu,
    InsertSnafu, MissingMetasrvOptsSnafu, Result, SchemaNotFoundSnafu, SelectSnafu,
    UnsupportedExprSnafu,
};
use crate::expr_factory::{CreateExprFactoryRef, DefaultCreateExprFactory};
use crate::frontend::FrontendOptions;
use crate::sql::insert_to_request;
use crate::table::insert::insert_request_to_insert_batch;
use crate::table::route::TableRoutes;

#[async_trait]
pub trait FrontendInstance:
    GrpcAdminHandler
    + GrpcQueryHandler
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
    // TODO(hl): In standalone mode, there is only one client.
    // But in distribute mode, frontend should fetch datanodes' addresses from metasrv.
    client: Client,
    /// catalog manager is None in standalone mode, datanode will keep their own
    catalog_manager: Option<CatalogManagerRef>,
    /// Script handler is None in distributed mode, only works on standalone mode.
    script_handler: Option<ScriptHandlerRef>,
    create_expr_factory: CreateExprFactoryRef,
    // TODO(fys): it should be a trait that corresponds to two implementations:
    // Standalone and Distributed, then the code behind it doesn't need to use so
    // many match statements.
    mode: Mode,
    // TODO(LFC): Refactor consideration: Can we split Frontend to DistInstance and EmbedInstance?
    dist_instance: Option<DistInstance>,
}

impl Default for Instance {
    fn default() -> Self {
        Self {
            client: Client::default(),
            catalog_manager: None,
            script_handler: None,
            create_expr_factory: Arc::new(DefaultCreateExprFactory {}),
            mode: Mode::Standalone,
            dist_instance: None,
        }
    }
}

impl Instance {
    pub async fn try_new(opts: &FrontendOptions) -> Result<Self> {
        let mut instance = Instance {
            mode: opts.mode.clone(),
            ..Default::default()
        };

        let addr = opts.datanode_grpc_addr();
        instance.client.start(vec![addr]);

        instance.dist_instance = match &opts.mode {
            Mode::Standalone => None,
            Mode::Distributed => {
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
                let meta_client = Arc::new(meta_client);

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

                instance.catalog_manager = Some(catalog_manager.clone());

                Some(DistInstance::new(
                    meta_client,
                    catalog_manager,
                    datanode_clients,
                ))
            }
        };
        Ok(instance)
    }

    pub fn database(&self, database: &str) -> Database {
        Database::new(database, self.client.clone())
    }

    pub fn admin(&self, database: &str) -> Admin {
        Admin::new(database, self.client.clone())
    }

    pub fn catalog_manager(&self) -> &Option<CatalogManagerRef> {
        &self.catalog_manager
    }

    pub fn set_catalog_manager(&mut self, catalog_manager: CatalogManagerRef) {
        debug_assert!(
            self.catalog_manager.is_none(),
            "Catalog manager can be set only once!"
        );
        self.catalog_manager = Some(catalog_manager);
    }

    pub fn set_script_handler(&mut self, handler: ScriptHandlerRef) {
        debug_assert!(
            self.script_handler.is_none(),
            "Script handler can be set only once!"
        );
        self.script_handler = Some(handler);
    }

    async fn handle_select(
        &self,
        expr: Select,
        stmt: Statement,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        if let Some(dist_instance) = &self.dist_instance {
            let Select::Sql(sql) = expr;
            dist_instance.handle_sql(&sql, stmt, query_ctx).await
        } else {
            // TODO(LFC): Refactor consideration: Datanode should directly execute statement in standalone mode to avoid parse SQL again.
            // Find a better way to execute query between Frontend and Datanode in standalone mode.
            // Otherwise we have to parse SQL first to get schema name. Maybe not GRPC.
            self.database(DEFAULT_SCHEMA_NAME)
                .select(expr)
                .await
                .and_then(Output::try_from)
                .context(SelectSnafu)
        }
    }

    /// Handle create expr.
    pub async fn handle_create_table(
        &self,
        mut expr: CreateExpr,
        partitions: Option<Partitions>,
    ) -> Result<Output> {
        if let Some(v) = &self.dist_instance {
            v.create_table(&mut expr, partitions).await
        } else {
            // Currently standalone mode does not support multi partitions/regions.
            let result = self
                .admin(expr.schema_name.as_deref().unwrap_or(DEFAULT_SCHEMA_NAME))
                .create(expr.clone())
                .await;
            if let Err(e) = &result {
                error!(e; "Failed to create table by expr: {:?}", expr);
            }
            result
                .and_then(admin_result_to_output)
                .context(CreateTableSnafu)
        }
    }

    /// Handle create database expr.
    pub async fn handle_create_database(&self, expr: CreateDatabaseExpr) -> Result<Output> {
        let database_name = expr.database_name.clone();
        if let Some(dist_instance) = &self.dist_instance {
            dist_instance.handle_create_database(expr).await
        } else {
            // FIXME(hl): In order to get admin client to create schema, we need to use the default schema admin
            self.admin(DEFAULT_SCHEMA_NAME)
                .create_database(expr)
                .await
                .and_then(admin_result_to_output)
                .context(CreateDatabaseSnafu {
                    name: database_name,
                })
        }
    }

    /// Handle alter expr
    pub async fn handle_alter(&self, expr: AlterExpr) -> Result<Output> {
        match &self.dist_instance {
            Some(dist_instance) => dist_instance.handle_alter_table(expr).await,
            None => self
                .admin(expr.schema_name.as_deref().unwrap_or(DEFAULT_SCHEMA_NAME))
                .alter(expr)
                .await
                .and_then(admin_result_to_output)
                .context(AlterTableSnafu),
        }
    }

    /// Handle drop table expr
    pub async fn handle_drop_table(&self, expr: DropTableExpr) -> Result<Output> {
        match self.mode {
            Mode::Standalone => self
                .admin(&expr.schema_name)
                .drop_table(expr)
                .await
                .and_then(admin_result_to_output)
                .context(DropTableSnafu),
            // TODO(ruihang): support drop table in distributed mode
            Mode::Distributed => UnsupportedExprSnafu {
                name: "Distributed DROP TABLE",
            }
            .fail(),
        }
    }

    /// Handle explain expr
    pub async fn handle_explain(
        &self,
        sql: &str,
        explain_stmt: Explain,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        if let Some(dist_instance) = &self.dist_instance {
            dist_instance
                .handle_sql(sql, Statement::Explain(explain_stmt), query_ctx)
                .await
        } else {
            Ok(Output::AffectedRows(0))
        }
    }

    /// Handle batch inserts
    pub async fn handle_inserts(&self, insert_expr: Vec<InsertExpr>) -> Result<Output> {
        let mut success = 0;
        for expr in insert_expr {
            match self.handle_insert(expr).await? {
                Output::AffectedRows(rows) => success += rows,
                _ => unreachable!("Insert should not yield output other than AffectedRows"),
            }
        }
        Ok(Output::AffectedRows(success))
    }

    /// Handle insert. for 'values' insertion, create/alter the destination table on demand.
    pub async fn handle_insert(&self, mut insert_expr: InsertExpr) -> Result<Output> {
        let table_name = &insert_expr.table_name;
        let catalog_name = DEFAULT_CATALOG_NAME;
        let schema_name = &insert_expr.schema_name;

        let columns = &insert_expr.columns;

        self.create_or_alter_table_on_demand(catalog_name, schema_name, table_name, columns)
            .await?;

        insert_expr.region_number = 0;

        self.database(schema_name)
            .insert(insert_expr)
            .await
            .and_then(Output::try_from)
            .context(InsertSnafu)
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
        match self
            .catalog_manager
            .as_ref()
            .expect("catalog manager cannot be None")
            .catalog(catalog_name)
            .context(CatalogSnafu)?
            .context(CatalogNotFoundSnafu { catalog_name })?
            .schema(schema_name)
            .context(CatalogSnafu)?
            .context(SchemaNotFoundSnafu {
                schema_info: schema_name,
            })?
            .table(table_name)
            .context(CatalogSnafu)?
        {
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
            schema_name: Some(schema_name.to_string()),
            catalog_name: Some(catalog_name.to_string()),
            kind: Some(Kind::AddColumns(add_columns)),
        };
        self.admin(schema_name)
            .alter(expr)
            .await
            .and_then(admin_result_to_output)
            .context(AlterTableOnInsertionSnafu)
    }

    fn get_catalog(&self, catalog_name: &str) -> Result<CatalogProviderRef> {
        self.catalog_manager
            .as_ref()
            .context(error::CatalogManagerSnafu)?
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

    fn stmt_to_insert_batch(
        &self,
        catalog: &str,
        schema: &str,
        insert: Box<Insert>,
    ) -> Result<(Vec<Column>, u32)> {
        let catalog_provider = self.get_catalog(catalog)?;
        let schema_provider = Self::get_schema(catalog_provider, schema)?;

        let insert_request = insert_to_request(&schema_provider, *insert)?;
        insert_request_to_insert_batch(&insert_request)
    }

    fn handle_use(&self, db: String, query_ctx: QueryContextRef) -> Result<Output> {
        let catalog_manager = &self.catalog_manager;
        if let Some(catalog_manager) = catalog_manager {
            ensure!(
                catalog_manager
                    .schema(DEFAULT_CATALOG_NAME, &db)
                    .context(error::CatalogSnafu)?
                    .is_some(),
                error::SchemaNotFoundSnafu { schema_info: &db }
            );

            query_ctx.set_current_schema(&db);

            Ok(Output::RecordBatches(RecordBatches::empty()))
        } else {
            // TODO(LFC): Handle "use" stmt here.
            unimplemented!()
        }
    }
}

#[async_trait]
impl FrontendInstance for Instance {
    async fn start(&mut self) -> Result<()> {
        // TODO(hl): Frontend init should move to here
        Ok(())
    }
}

#[cfg(test)]
impl Instance {
    pub fn with_client_and_catalog_manager(client: Client, catalog: CatalogManagerRef) -> Self {
        Self {
            client,
            catalog_manager: Some(catalog),
            script_handler: None,
            create_expr_factory: Arc::new(DefaultCreateExprFactory),
            mode: Mode::Standalone,
            dist_instance: None,
        }
    }
}

fn parse_stmt(sql: &str) -> Result<Statement> {
    let mut stmt = ParserContext::create_with_dialect(sql, &GenericDialect {})
        .context(error::ParseSqlSnafu)?;
    // TODO(LFC): Support executing multiple SQL queries,
    // which seems to be a major change to our whole server framework?
    ensure!(
        stmt.len() == 1,
        error::InvalidSqlSnafu {
            err_msg: "Currently executing multiple SQL queries are not supported."
        }
    );
    Ok(stmt.remove(0))
}

#[async_trait]
impl SqlQueryHandler for Instance {
    async fn do_query(
        &self,
        query: &str,
        query_ctx: QueryContextRef,
    ) -> server_error::Result<Output> {
        let stmt = parse_stmt(query)
            .map_err(BoxedError::new)
            .context(server_error::ExecuteQuerySnafu { query })?;

        match stmt {
            Statement::ShowDatabases(_)
            | Statement::ShowTables(_)
            | Statement::DescribeTable(_)
            | Statement::Query(_) => {
                self.handle_select(Select::Sql(query.to_string()), stmt, query_ctx)
                    .await
            }
            Statement::Insert(insert) => match self.mode {
                Mode::Standalone => {
                    let (catalog_name, schema_name, table_name) = insert
                        .full_table_name()
                        .context(error::ParseSqlSnafu)
                        .map_err(BoxedError::new)
                        .context(server_error::ExecuteInsertSnafu {
                            msg: "Failed to get table name",
                        })?;

                    let (columns, row_count) = self
                        .stmt_to_insert_batch(&catalog_name, &schema_name, insert)
                        .map_err(BoxedError::new)
                        .context(server_error::ExecuteQuerySnafu { query })?;

                    let expr = InsertExpr {
                        schema_name,
                        table_name,
                        region_number: 0,
                        columns,
                        row_count,
                    };
                    self.handle_insert(expr).await
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
            Statement::CreateTable(create) => {
                let create_expr = self
                    .create_expr_factory
                    .create_expr_by_stmt(&create)
                    .await
                    .map_err(BoxedError::new)
                    .context(server_error::ExecuteQuerySnafu { query })?;

                self.handle_create_table(create_expr, create.partitions)
                    .await
            }
            Statement::CreateDatabase(c) => {
                let expr = CreateDatabaseExpr {
                    database_name: c.name.to_string(),
                };
                self.handle_create_database(expr).await
            }
            Statement::Alter(alter_stmt) => {
                self.handle_alter(
                    AlterExpr::try_from(alter_stmt)
                        .map_err(BoxedError::new)
                        .context(server_error::ExecuteAlterSnafu { query })?,
                )
                .await
            }
            Statement::DropTable(drop_stmt) => {
                let expr = DropTableExpr {
                    catalog_name: drop_stmt.catalog_name,
                    schema_name: drop_stmt.schema_name,
                    table_name: drop_stmt.table_name,
                };
                self.handle_drop_table(expr).await
            }
            Statement::Explain(explain_stmt) => {
                self.handle_explain(query, explain_stmt, query_ctx).await
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
        if let Some(expr) = &query.expr {
            match expr {
                Expr::Insert(insert) => {
                    // TODO(fys): refactor, avoid clone
                    let result = self.handle_insert(insert.clone()).await;
                    result
                        .map(|o| match o {
                            Output::AffectedRows(rows) => ObjectResultBuilder::new()
                                .status_code(StatusCode::Success as u32)
                                .mutate_result(rows as u32, 0u32)
                                .build(),
                            _ => {
                                unreachable!()
                            }
                        })
                        .map_err(BoxedError::new)
                        .with_context(|_| server_error::ExecuteQuerySnafu {
                            query: format!("{:?}", query),
                        })
                }
                Expr::Select(select) => {
                    let select = select
                        .expr
                        .as_ref()
                        .context(server_error::InvalidQuerySnafu {
                            reason: "empty query",
                        })?;
                    match select {
                        select_expr::Expr::Sql(sql) => {
                            let query_ctx = Arc::new(QueryContext::new());
                            let output = SqlQueryHandler::do_query(self, sql, query_ctx).await;
                            Ok(to_object_result(output).await)
                        }
                        _ => {
                            if self.dist_instance.is_some() {
                                return server_error::NotSupportedSnafu {
                                    feat: "Executing plan directly in Frontend.",
                                }
                                .fail();
                            }
                            // FIXME(hl): refactor
                            self.database(DEFAULT_SCHEMA_NAME)
                                .object(query.clone())
                                .await
                                .map_err(BoxedError::new)
                                .with_context(|_| server_error::ExecuteQuerySnafu {
                                    query: format!("{:?}", query),
                                })
                        }
                    }
                }
                _ => server_error::NotSupportedSnafu {
                    feat: "Currently only insert and select is supported in GRPC service.",
                }
                .fail(),
            }
        } else {
            server_error::InvalidQuerySnafu {
                reason: "empty query",
            }
            .fail()
        }
    }
}

fn get_schema_name(expr: &AdminExpr) -> &str {
    let schema_name = match &expr.expr {
        Some(admin_expr::Expr::Create(expr)) => expr.schema_name.as_deref(),
        Some(admin_expr::Expr::Alter(expr)) => expr.schema_name.as_deref(),
        Some(admin_expr::Expr::CreateDatabase(_)) | None => Some(DEFAULT_SCHEMA_NAME),
        Some(admin_expr::Expr::DropTable(expr)) => Some(expr.schema_name.as_ref()),
    };
    schema_name.unwrap_or(DEFAULT_SCHEMA_NAME)
}

#[async_trait]
impl GrpcAdminHandler for Instance {
    async fn exec_admin_request(&self, mut expr: AdminExpr) -> server_error::Result<AdminResult> {
        // Force the default to be `None` rather than `Some(0)` comes from gRPC decode.
        // Related issue: #480
        if let Some(api::v1::admin_expr::Expr::Create(create)) = &mut expr.expr {
            create.table_id = None;
        }
        self.admin(get_schema_name(&expr))
            .do_request(expr.clone())
            .await
            .map_err(BoxedError::new)
            .with_context(|_| server_error::ExecuteQuerySnafu {
                query: format!("{:?}", expr),
            })
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use api::v1::codec::SelectResult;
    use api::v1::column::SemanticType;
    use api::v1::{
        admin_expr, admin_result, column, object_expr, object_result, select_expr, Column,
        ColumnDataType, ColumnDef as GrpcColumnDef, ExprHeader, MutateResult, SelectExpr,
    };
    use datatypes::schema::ColumnDefaultConstraint;
    use datatypes::value::Value;

    use super::*;
    use crate::tests;

    #[tokio::test]
    async fn test_execute_sql() {
        let query_ctx = Arc::new(QueryContext::new());

        let instance = tests::create_frontend_instance().await;

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
            .unwrap();
        match output {
            Output::AffectedRows(rows) => assert_eq!(rows, 3),
            _ => unreachable!(),
        }

        let sql = "select * from demo";
        let output = SqlQueryHandler::do_query(&*instance, sql, query_ctx.clone())
            .await
            .unwrap();
        match output {
            Output::RecordBatches(recordbatches) => {
                let pretty_print = recordbatches.pretty_print();
                let pretty_print = pretty_print.lines().collect::<Vec<&str>>();
                let expected = vec![
                    "+----------------+---------------------+-----+--------+-----------+",
                    "| host           | ts                  | cpu | memory | disk_util |",
                    "+----------------+---------------------+-----+--------+-----------+",
                    "| frontend.host1 | 1970-01-01 00:00:01 | 1.1 | 100    | 9.9       |",
                    "| frontend.host2 | 1970-01-01 00:00:02 |     |        | 9.9       |",
                    "| frontend.host3 | 1970-01-01 00:00:03 | 3.3 | 300    | 9.9       |",
                    "+----------------+---------------------+-----+--------+-----------+",
                ];
                assert_eq!(pretty_print, expected);
            }
            _ => unreachable!(),
        };

        let sql = "select * from demo where ts>cast(1000000000 as timestamp)"; // use nanoseconds as where condition
        let output = SqlQueryHandler::do_query(&*instance, sql, query_ctx.clone())
            .await
            .unwrap();
        match output {
            Output::RecordBatches(recordbatches) => {
                let pretty_print = recordbatches.pretty_print();
                let pretty_print = pretty_print.lines().collect::<Vec<&str>>();
                let expected = vec![
                    "+----------------+---------------------+-----+--------+-----------+",
                    "| host           | ts                  | cpu | memory | disk_util |",
                    "+----------------+---------------------+-----+--------+-----------+",
                    "| frontend.host2 | 1970-01-01 00:00:02 |     |        | 9.9       |",
                    "| frontend.host3 | 1970-01-01 00:00:03 | 3.3 | 300    | 9.9       |",
                    "+----------------+---------------------+-----+--------+-----------+",
                ];
                assert_eq!(pretty_print, expected);
            }
            _ => unreachable!(),
        };
    }

    #[tokio::test]
    async fn test_execute_grpc() {
        let instance = tests::create_frontend_instance().await;

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
        let expected_disk_col = Column {
            column_name: "disk_util".to_string(),
            values: Some(column::Values {
                f64_values: vec![9.9, 9.9, 9.9, 9.9],
                ..Default::default()
            }),
            semantic_type: SemanticType::Field as i32,
            datatype: ColumnDataType::Float64 as i32,
            ..Default::default()
        };
        let expected_ts_col = Column {
            column_name: "ts".to_string(),
            values: Some(column::Values {
                ts_millisecond_values: vec![1000, 2000, 3000, 4000],
                ..Default::default()
            }),
            semantic_type: SemanticType::Timestamp as i32,
            datatype: ColumnDataType::Timestamp as i32,
            ..Default::default()
        };

        // create
        let create_expr = create_expr();
        let admin_expr = AdminExpr {
            header: Some(ExprHeader::default()),
            expr: Some(admin_expr::Expr::Create(create_expr)),
        };
        let result = GrpcAdminHandler::exec_admin_request(&*instance, admin_expr)
            .await
            .unwrap();
        assert_matches!(
            result.result,
            Some(admin_result::Result::Mutate(MutateResult {
                success: 1,
                failure: 0
            }))
        );

        // insert
        let columns = vec![
            expected_host_col.clone(),
            expected_cpu_col.clone(),
            expected_mem_col.clone(),
            expected_ts_col.clone(),
        ];
        let row_count = 4;
        let insert_expr = InsertExpr {
            schema_name: "public".to_string(),
            table_name: "demo".to_string(),
            region_number: 0,
            columns,
            row_count,
        };
        let object_expr = ObjectExpr {
            header: Some(ExprHeader::default()),
            expr: Some(object_expr::Expr::Insert(insert_expr)),
        };
        let result = GrpcQueryHandler::do_query(&*instance, object_expr)
            .await
            .unwrap();
        assert_matches!(
            result.result,
            Some(object_result::Result::Mutate(MutateResult {
                success: 4,
                failure: 0
            }))
        );

        // select
        let object_expr = ObjectExpr {
            header: Some(ExprHeader::default()),
            expr: Some(object_expr::Expr::Select(SelectExpr {
                expr: Some(select_expr::Expr::Sql("select * from demo".to_string())),
            })),
        };
        let result = GrpcQueryHandler::do_query(&*instance, object_expr)
            .await
            .unwrap();
        match result.result {
            Some(object_result::Result::Select(select_result)) => {
                let select_result: SelectResult = (*select_result.raw_data).try_into().unwrap();

                assert_eq!(4, select_result.row_count);
                let actual_columns = select_result.columns;
                assert_eq!(5, actual_columns.len());

                // Respect the order in create table schema
                let expected_columns = vec![
                    expected_host_col,
                    expected_cpu_col,
                    expected_mem_col,
                    expected_disk_col,
                    expected_ts_col,
                ];
                expected_columns
                    .iter()
                    .zip(actual_columns.iter())
                    .for_each(|(x, y)| assert_eq!(x, y));
            }
            _ => unreachable!(),
        }
    }

    fn create_expr() -> CreateExpr {
        let column_defs = vec![
            GrpcColumnDef {
                name: "host".to_string(),
                datatype: ColumnDataType::String as i32,
                is_nullable: false,
                default_constraint: None,
            },
            GrpcColumnDef {
                name: "cpu".to_string(),
                datatype: ColumnDataType::Float64 as i32,
                is_nullable: true,
                default_constraint: None,
            },
            GrpcColumnDef {
                name: "memory".to_string(),
                datatype: ColumnDataType::Float64 as i32,
                is_nullable: true,
                default_constraint: None,
            },
            GrpcColumnDef {
                name: "disk_util".to_string(),
                datatype: ColumnDataType::Float64 as i32,
                is_nullable: true,
                default_constraint: Some(
                    ColumnDefaultConstraint::Value(Value::from(9.9f64))
                        .try_into()
                        .unwrap(),
                ),
            },
            GrpcColumnDef {
                name: "ts".to_string(),
                datatype: ColumnDataType::Timestamp as i32,
                is_nullable: true,
                default_constraint: None,
            },
        ];
        CreateExpr {
            catalog_name: None,
            schema_name: None,
            table_name: "demo".to_string(),
            desc: None,
            column_defs,
            time_index: "ts".to_string(),
            primary_keys: vec!["host".to_string()],
            create_if_not_exists: true,
            table_options: Default::default(),
            table_id: None,
            region_ids: vec![0],
        }
    }
}
