use std::{fs, path, sync::Arc};

use api::v1::{
    admin_expr, object_expr, select_expr, AdminExpr, AdminResult, InsertExpr, ObjectExpr,
    ObjectResult, SelectExpr,
};
use async_trait::async_trait;
use catalog::{CatalogManagerRef, DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_error::prelude::BoxedError;
use common_error::status_code::StatusCode;
use common_telemetry::logging::{error, info};
use common_telemetry::timer;
use log_store::fs::{config::LogConfig, log::LocalFileLogStore};
use object_store::{backend::fs::Backend, util, ObjectStore};
use query::query_engine::{Output, QueryEngineFactory, QueryEngineRef};
use servers::query_handler::{GrpcAdminHandler, GrpcQueryHandler, SqlQueryHandler};
use snafu::prelude::*;
use sql::statements::statement::Statement;
use storage::{config::EngineConfig as StorageEngineConfig, EngineImpl};
use table_engine::config::EngineConfig as TableEngineConfig;
use table_engine::engine::MitoEngine;

use crate::datanode::{DatanodeOptions, ObjectStoreConfig};
use crate::error::{
    self, ExecuteSqlSnafu, InsertSnafu, NewCatalogSnafu, Result, TableNotFoundSnafu,
    UnsupportedExprSnafu,
};
use crate::metric;
use crate::script::ScriptExecutor;
use crate::server::grpc::handler::{build_err_result, ObjectResultBuilder};
use crate::server::grpc::insert::insertion_expr_to_request;
use crate::server::grpc::plan::PhysicalPlanner;
use crate::server::grpc::select::to_object_result;
use crate::sql::{SqlHandler, SqlRequest};

type DefaultEngine = MitoEngine<EngineImpl<LocalFileLogStore>>;

// An abstraction to read/write services.
pub struct Instance {
    // Query service
    query_engine: QueryEngineRef,
    sql_handler: SqlHandler,
    // Catalog list
    catalog_manager: CatalogManagerRef,
    physical_planner: PhysicalPlanner,
    script_executor: ScriptExecutor,
}

pub type InstanceRef = Arc<Instance>;

impl Instance {
    pub async fn new(opts: &DatanodeOptions) -> Result<Self> {
        let object_store = new_object_store(&opts.storage).await?;
        let log_store = create_local_file_log_store(opts).await?;

        let table_engine = DefaultEngine::new(
            TableEngineConfig::default(),
            EngineImpl::new(
                StorageEngineConfig::default(),
                Arc::new(log_store),
                object_store.clone(),
            ),
            object_store,
        );
        let table_engine = Arc::new(table_engine);

        let catalog_manager = Arc::new(
            catalog::LocalCatalogManager::try_new(table_engine.clone())
                .await
                .context(NewCatalogSnafu)?,
        );
        let factory = QueryEngineFactory::new(catalog_manager.clone());
        let query_engine = factory.query_engine().clone();
        let script_executor = ScriptExecutor::new(query_engine.clone());

        Ok(Self {
            query_engine: query_engine.clone(),
            sql_handler: SqlHandler::new(table_engine, catalog_manager.clone()),
            catalog_manager,
            physical_planner: PhysicalPlanner::new(query_engine),
            script_executor,
        })
    }

    pub async fn execute_grpc_insert(&self, insert_expr: InsertExpr) -> Result<Output> {
        let schema_provider = self
            .catalog_manager
            .catalog(DEFAULT_CATALOG_NAME)
            .unwrap()
            .schema(DEFAULT_SCHEMA_NAME)
            .unwrap();

        let table_name = &insert_expr.table_name.clone();
        let table = schema_provider
            .table(table_name)
            .context(TableNotFoundSnafu { table_name })?;

        let insert = insertion_expr_to_request(insert_expr, table.clone())?;

        let affected_rows = table
            .insert(insert)
            .await
            .context(InsertSnafu { table_name })?;

        Ok(Output::AffectedRows(affected_rows))
    }

    pub async fn execute_sql(&self, sql: &str) -> Result<Output> {
        let stmt = self
            .query_engine
            .sql_to_statement(sql)
            .context(ExecuteSqlSnafu)?;

        match stmt {
            Statement::Query(_) => {
                let logical_plan = self
                    .query_engine
                    .statement_to_plan(stmt)
                    .context(ExecuteSqlSnafu)?;

                self.query_engine
                    .execute(&logical_plan)
                    .await
                    .context(ExecuteSqlSnafu)
            }
            Statement::Insert(i) => {
                let schema_provider = self
                    .catalog_manager
                    .catalog(DEFAULT_CATALOG_NAME)
                    .unwrap()
                    .schema(DEFAULT_SCHEMA_NAME)
                    .unwrap();

                let request = self.sql_handler.insert_to_request(schema_provider, *i)?;
                self.sql_handler.execute(request).await
            }

            Statement::Create(c) => {
                let table_id = self.catalog_manager.next_table_id();
                let _engine_name = c.engine.clone();
                // TODO(hl): Select table engine by engine_name

                let request = self.sql_handler.create_to_request(table_id, c)?;
                let catalog_name = request.catalog_name.clone();
                let schema_name = request.schema_name.clone();
                let table_name = request.table_name.clone();
                let table_id = request.id;
                info!(
                    "Creating table, catalog: {:?}, schema: {:?}, table name: {:?}, table id: {}",
                    catalog_name, schema_name, table_name, table_id
                );

                self.sql_handler.execute(SqlRequest::Create(request)).await
            }
            Statement::Alter(alter_table) => {
                let req = self.sql_handler.alter_to_request(alter_table)?;
                self.sql_handler.execute(SqlRequest::Alter(req)).await
            }
            _ => unimplemented!(),
        }
    }

    pub async fn start(&self) -> Result<()> {
        self.catalog_manager
            .start()
            .await
            .context(NewCatalogSnafu)?;
        Ok(())
    }

    async fn handle_insert(&self, insert_expr: InsertExpr) -> ObjectResult {
        match self.execute_grpc_insert(insert_expr).await {
            Ok(Output::AffectedRows(rows)) => ObjectResultBuilder::new()
                .status_code(StatusCode::Success as u32)
                .mutate_result(rows as u32, 0)
                .build(),
            Err(err) => {
                // TODO(fys): failure count
                build_err_result(&err)
            }
            _ => unreachable!(),
        }
    }

    async fn handle_select(&self, select_expr: SelectExpr) -> ObjectResult {
        let result = self.do_handle_select(select_expr).await;
        to_object_result(result).await
    }

    async fn do_handle_select(&self, select_expr: SelectExpr) -> Result<Output> {
        let expr = select_expr.expr;
        match expr {
            Some(select_expr::Expr::Sql(sql)) => self.execute_sql(&sql).await,
            Some(select_expr::Expr::PhysicalPlan(api::v1::PhysicalPlan { original_ql, plan })) => {
                self.physical_planner
                    .execute(PhysicalPlanner::parse(plan)?, original_ql)
                    .await
            }
            _ => UnsupportedExprSnafu {
                name: format!("{:?}", expr),
            }
            .fail(),
        }
    }

    pub fn sql_handler(&self) -> &SqlHandler {
        &self.sql_handler
    }

    pub fn catalog_manager(&self) -> &CatalogManagerRef {
        &self.catalog_manager
    }
}

#[cfg(test)]
impl Instance {
    pub async fn new_mock() -> Result<Self> {
        use table_engine::table::test_util::new_test_object_store;
        use table_engine::table::test_util::MockEngine;
        use table_engine::table::test_util::MockMitoEngine;

        let (_dir, object_store) = new_test_object_store("setup_mock_engine_and_table").await;
        let mock_engine = MockMitoEngine::new(
            TableEngineConfig::default(),
            MockEngine::default(),
            object_store,
        );
        let mock_engine = Arc::new(mock_engine);

        let catalog_manager = Arc::new(
            catalog::LocalCatalogManager::try_new(mock_engine.clone())
                .await
                .unwrap(),
        );

        let factory = QueryEngineFactory::new(catalog_manager.clone());
        let query_engine = factory.query_engine().clone();

        let sql_handler = SqlHandler::new(mock_engine, catalog_manager.clone());
        let physical_planner = PhysicalPlanner::new(query_engine.clone());
        Ok(Self {
            query_engine,
            sql_handler,
            catalog_manager,
            physical_planner,
        })
    }
}

async fn new_object_store(store_config: &ObjectStoreConfig) -> Result<ObjectStore> {
    // TODO(dennis): supports other backend
    let data_dir = util::normalize_dir(match store_config {
        ObjectStoreConfig::File { data_dir } => data_dir,
    });

    fs::create_dir_all(path::Path::new(&data_dir))
        .context(error::CreateDirSnafu { dir: &data_dir })?;

    info!("The storage directory is: {}", &data_dir);

    let accessor = Backend::build()
        .root(&data_dir)
        .finish()
        .await
        .context(error::InitBackendSnafu { dir: &data_dir })?;

    Ok(ObjectStore::new(accessor))
}

async fn create_local_file_log_store(opts: &DatanodeOptions) -> Result<LocalFileLogStore> {
    // create WAL directory
    fs::create_dir_all(path::Path::new(&opts.wal_dir))
        .context(error::CreateDirSnafu { dir: &opts.wal_dir })?;

    info!("The WAL directory is: {}", &opts.wal_dir);

    let log_config = LogConfig {
        log_file_dir: opts.wal_dir.clone(),
        ..Default::default()
    };

    let log_store = LocalFileLogStore::open(&log_config)
        .await
        .context(error::OpenLogStoreSnafu)?;

    Ok(log_store)
}

#[async_trait]
impl SqlQueryHandler for Instance {
    async fn do_query(&self, query: &str) -> servers::error::Result<Output> {
        let _timer = timer!(metric::METRIC_HANDLE_SQL_ELAPSED);
        self.execute_sql(query)
            .await
            .map_err(|e| {
                error!(e; "Instance failed to execute sql");
                BoxedError::new(e)
            })
            .context(servers::error::ExecuteQuerySnafu { query })
    }

    async fn execute_script(&self, script: &str) -> servers::error::Result<Output> {
        self.script_executor.execute_script(script).await
    }
}

#[async_trait]
impl GrpcQueryHandler for Instance {
    async fn do_query(&self, query: ObjectExpr) -> servers::error::Result<ObjectResult> {
        let object_resp = match query.expr {
            Some(object_expr::Expr::Insert(insert_expr)) => self.handle_insert(insert_expr).await,
            Some(object_expr::Expr::Select(select_expr)) => self.handle_select(select_expr).await,
            other => {
                return servers::error::NotSupportedSnafu {
                    feat: format!("{:?}", other),
                }
                .fail();
            }
        };
        Ok(object_resp)
    }
}

#[async_trait]
impl GrpcAdminHandler for Instance {
    async fn exec_admin_request(&self, expr: AdminExpr) -> servers::error::Result<AdminResult> {
        let admin_resp = match expr.expr {
            Some(admin_expr::Expr::Create(create_expr)) => self.handle_create(create_expr).await,
            other => {
                return servers::error::NotSupportedSnafu {
                    feat: format!("{:?}", other),
                }
                .fail();
            }
        };
        Ok(admin_resp)
    }
}
