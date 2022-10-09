use std::{fs, path, sync::Arc};

use catalog::CatalogManagerRef;
use common_telemetry::logging::info;
use log_store::fs::{config::LogConfig, log::LocalFileLogStore};
use object_store::{services::fs::Builder, util, ObjectStore};
use query::query_engine::{QueryEngineFactory, QueryEngineRef};
use snafu::prelude::*;
use storage::{config::EngineConfig as StorageEngineConfig, EngineImpl};
use table_engine::config::EngineConfig as TableEngineConfig;
use table_engine::engine::MitoEngine;

use crate::datanode::{DatanodeOptions, ObjectStoreConfig};
use crate::error::{self, NewCatalogSnafu, Result};
use crate::script::ScriptExecutor;
use crate::server::grpc::plan::PhysicalPlanner;
use crate::sql::SqlHandler;

mod grpc;
mod sql;

type DefaultEngine = MitoEngine<EngineImpl<LocalFileLogStore>>;

// An abstraction to read/write services.
pub struct Instance {
    query_engine: QueryEngineRef,
    sql_handler: SqlHandler,
    catalog_manager: CatalogManagerRef,
    physical_planner: PhysicalPlanner,
    script_executor: ScriptExecutor,
}

pub type InstanceRef = Arc<Instance>;

impl Instance {
    pub async fn new(opts: &DatanodeOptions) -> Result<Self> {
        let object_store = new_object_store(&opts.storage).await?;
        let log_store = create_local_file_log_store(opts).await?;

        let table_engine = Arc::new(DefaultEngine::new(
            TableEngineConfig::default(),
            EngineImpl::new(
                StorageEngineConfig::default(),
                Arc::new(log_store),
                object_store.clone(),
            ),
            object_store,
        ));
        let catalog_manager = Arc::new(
            catalog::local::LocalCatalogManager::try_new(table_engine.clone())
                .await
                .context(NewCatalogSnafu)?,
        );
        let factory = QueryEngineFactory::new(catalog_manager.clone());
        let query_engine = factory.query_engine().clone();
        let script_executor =
            ScriptExecutor::new(catalog_manager.clone(), query_engine.clone()).await?;

        Ok(Self {
            query_engine: query_engine.clone(),
            sql_handler: SqlHandler::new(table_engine, catalog_manager.clone()),
            catalog_manager,
            physical_planner: PhysicalPlanner::new(query_engine),
            script_executor,
        })
    }

    async fn add_new_columns_to_table(
        &self,
        table_name: &str,
        add_columns: Vec<AddColumnRequest>,
    ) -> Result<()> {
        let column_names = add_columns
            .iter()
            .map(|req| req.column_schema.name.clone())
            .collect::<Vec<_>>();

        let alter_request = insert::build_alter_table_request(table_name, add_columns);

        debug!(
            "Adding new columns: {:?} to table: {}",
            column_names, table_name
        );

        let _result = self
            .sql_handler()
            .execute(SqlRequest::Alter(alter_request))
            .await?;

        info!(
            "Added new columns: {:?} to table: {}",
            column_names, table_name
        );
        Ok(())
    }

    async fn create_table_by_insert_batches(
        &self,
        table_name: &str,
        insert_batches: &[InsertBatch],
    ) -> Result<()> {
        // Create table automatically, build schema from data.
        let table_id = self.catalog_manager.next_table_id();
        let create_table_request =
            insert::build_create_table_request(table_id, table_name, insert_batches)?;

        info!(
            "Try to create table: {} automatically with request: {:?}",
            table_name, create_table_request,
        );

        let _result = self
            .sql_handler()
            .execute(SqlRequest::Create(create_table_request))
            .await?;

        info!("Success to create table: {} automatically", table_name);

        Ok(())
    }

    pub async fn execute_grpc_insert(
        &self,
        table_name: &str,
        values: insert_expr::Values,
    ) -> Result<Output> {
        let schema_provider = self
            .catalog_manager
            .catalog(DEFAULT_CATALOG_NAME)
            .expect("datafusion does not accept fallible catalog access")
            .unwrap()
            .schema(DEFAULT_SCHEMA_NAME)
            .expect("datafusion does not accept fallible catalog access")
            .unwrap();

        let insert_batches = insert::insert_batches(values.values)?;
        ensure!(!insert_batches.is_empty(), error::IllegalInsertDataSnafu);

        let table = if let Some(table) = schema_provider.table(table_name) {
            let schema = table.schema();
            if let Some(add_columns) = insert::find_new_columns(&schema, &insert_batches)? {
                self.add_new_columns_to_table(table_name, add_columns)
                    .await?;
            }

            table
        } else {
            self.create_table_by_insert_batches(table_name, &insert_batches)
                .await?;

            schema_provider
                .table(table_name)
                .context(TableNotFoundSnafu { table_name })?
        };

        let insert = insertion_expr_to_request(table_name, insert_batches, table.clone())?;

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
                    .expect("datafusion does not accept fallible catalog access")
                    .unwrap()
                    .schema(DEFAULT_SCHEMA_NAME)
                    .expect("datafusion does not accept fallible catalog access")
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
            Statement::ShowDatabases(stmt) => {
                self.sql_handler
                    .execute(SqlRequest::ShowDatabases(stmt))
                    .await
            }
            Statement::ShowTables(stmt) => {
                self.sql_handler.execute(SqlRequest::ShowTables(stmt)).await
            }
        }
    }

    pub async fn start(&self) -> Result<()> {
        self.catalog_manager
            .start()
            .await
            .context(NewCatalogSnafu)?;
        Ok(())
    }

    pub fn sql_handler(&self) -> &SqlHandler {
        &self.sql_handler
    }

    pub fn catalog_manager(&self) -> &CatalogManagerRef {
        &self.catalog_manager
    }

    // This method is used in other crate's testing codes, so move it out of "cfg(test)".
    // TODO(LFC): Delete it when callers no longer need it.
    pub async fn new_mock() -> Result<Self> {
        use table_engine::table::test_util::new_test_object_store;
        use table_engine::table::test_util::MockEngine;
        use table_engine::table::test_util::MockMitoEngine;

        let (_dir, object_store) = new_test_object_store("setup_mock_engine_and_table").await;
        let mock_engine = Arc::new(MockMitoEngine::new(
            TableEngineConfig::default(),
            MockEngine::default(),
            object_store,
        ));

        let catalog_manager = Arc::new(
            catalog::local::manager::LocalCatalogManager::try_new(mock_engine.clone())
                .await
                .unwrap(),
        );

        let factory = QueryEngineFactory::new(catalog_manager.clone());
        let query_engine = factory.query_engine().clone();

        let sql_handler = SqlHandler::new(mock_engine.clone(), catalog_manager.clone());
        let physical_planner = PhysicalPlanner::new(query_engine.clone());
        let script_executor = ScriptExecutor::new(catalog_manager.clone(), query_engine.clone())
            .await
            .unwrap();

        Ok(Self {
            query_engine,
            sql_handler,
            catalog_manager,
            physical_planner,
            script_executor,
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

    let accessor = Builder::default()
        .root(&data_dir)
        .build()
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
