use std::time::Duration;
use std::{fs, path, sync::Arc};

use catalog::CatalogManagerRef;
use common_grpc::channel_manager::{ChannelConfig, ChannelManager};
use common_telemetry::logging::info;
use log_store::fs::{config::LogConfig, log::LocalFileLogStore};
use meta_client::client::{MetaClient, MetaClientBuilder};
use object_store::{services::fs::Builder, util, ObjectStore};
use query::query_engine::{QueryEngineFactory, QueryEngineRef};
use snafu::prelude::*;
use storage::{config::EngineConfig as StorageEngineConfig, EngineImpl};
use table_engine::config::EngineConfig as TableEngineConfig;
use table_engine::engine::MitoEngine;

use crate::datanode::{DatanodeOptions, MetaClientOpts, ObjectStoreConfig};
use crate::error::{self, MetaClientInitSnafu, NewCatalogSnafu, Result};
use crate::heartbeat::HeartbeatTask;
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
    #[allow(unused)]
    meta_client: MetaClient,
    heartbeat_task: HeartbeatTask,
}

pub type InstanceRef = Arc<Instance>;

impl Instance {
    pub async fn new(opts: &DatanodeOptions) -> Result<Self> {
        let object_store = new_object_store(&opts.storage).await?;
        let log_store = create_local_file_log_store(opts).await?;
        let meta_client = new_metasrv_client(&opts.meta_client_opts).await?;

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

        let heartbeat_task = HeartbeatTask::new(
            1, /*node id not set*/
            opts.rpc_addr.clone(),
            meta_client.clone(),
        );
        Ok(Self {
            query_engine: query_engine.clone(),
            sql_handler: SqlHandler::new(table_engine, catalog_manager.clone()),
            catalog_manager,
            physical_planner: PhysicalPlanner::new(query_engine),
            script_executor,
            meta_client,
            heartbeat_task,
        })
    }

    pub async fn start(&self) -> Result<()> {
        self.catalog_manager
            .start()
            .await
            .context(NewCatalogSnafu)?;
        self.heartbeat_task.start().await?;
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
            meta_client: Default::default(),
            heartbeat_task: Default::default(),
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

/// Create metasrv client instance and spawn heartbeat loop.
async fn new_metasrv_client(meta_config: &MetaClientOpts) -> Result<MetaClient> {
    let cluster_id = 0; // TODO(hl): read from config
    let member_id = 1; // TODO(hl): read from config

    let config = ChannelConfig::new()
        .timeout(Duration::from_millis(meta_config.timeout_millis))
        .connect_timeout(Duration::from_millis(meta_config.connect_timeout_millis))
        .tcp_nodelay(meta_config.tcp_nodelay);
    let channel_manager = ChannelManager::with_config(config);
    let mut meta_client = MetaClientBuilder::new(cluster_id, member_id)
        .enable_heartbeat()
        .enable_router()
        .enable_store()
        .channel_manager(channel_manager)
        .build();
    meta_client
        .start(&[&meta_config.metasrv_addr])
        .await
        .context(MetaClientInitSnafu)?;

    // required only when the heartbeat_client is enabled
    meta_client
        .ask_leader()
        .await
        .context(MetaClientInitSnafu)?;
    Ok(meta_client)
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
