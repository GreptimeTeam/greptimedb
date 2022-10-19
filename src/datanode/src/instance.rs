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
            catalog::LocalCatalogManager::try_new(table_engine.clone())
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
            catalog::LocalCatalogManager::try_new(mock_engine.clone())
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
