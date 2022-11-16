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

use std::sync::Arc;
use std::time::Duration;
use std::{fs, path};

use catalog::remote::MetaKvBackend;
use catalog::CatalogManagerRef;
use common_grpc::channel_manager::{ChannelConfig, ChannelManager};
use common_telemetry::logging::info;
use frontend::frontend::Mode;
use log_store::fs::config::LogConfig;
use log_store::fs::log::LocalFileLogStore;
use meta_client::client::{MetaClient, MetaClientBuilder};
use meta_client::MetaClientOpts;
use mito::config::EngineConfig as TableEngineConfig;
use mito::engine::MitoEngine;
use object_store::layers::LoggingLayer;
use object_store::services::fs::Builder;
use object_store::{util, ObjectStore};
use query::query_engine::{QueryEngineFactory, QueryEngineRef};
use snafu::prelude::*;
use storage::config::EngineConfig as StorageEngineConfig;
use storage::EngineImpl;
use table::table::TableIdProviderRef;

use crate::datanode::{DatanodeOptions, ObjectStoreConfig};
use crate::error::{
    self, CatalogSnafu, MetaClientInitSnafu, MissingMetasrvOptsSnafu, MissingNodeIdSnafu,
    NewCatalogSnafu, Result,
};
use crate::heartbeat::HeartbeatTask;
use crate::script::ScriptExecutor;
use crate::server::grpc::plan::PhysicalPlanner;
use crate::sql::SqlHandler;

mod grpc;
mod script;
mod sql;

pub(crate) type DefaultEngine = MitoEngine<EngineImpl<LocalFileLogStore>>;

// An abstraction to read/write services.
pub struct Instance {
    pub(crate) query_engine: QueryEngineRef,
    pub(crate) sql_handler: SqlHandler,
    pub(crate) catalog_manager: CatalogManagerRef,
    pub(crate) physical_planner: PhysicalPlanner,
    pub(crate) script_executor: ScriptExecutor,
    pub(crate) table_id_provider: Option<TableIdProviderRef>,
    #[allow(unused)]
    pub(crate) meta_client: Option<Arc<MetaClient>>,
    pub(crate) heartbeat_task: Option<HeartbeatTask>,
}

pub type InstanceRef = Arc<Instance>;

impl Instance {
    pub async fn new(opts: &DatanodeOptions) -> Result<Self> {
        let object_store = new_object_store(&opts.storage).await?;
        let log_store = create_local_file_log_store(opts).await?;

        let meta_client = match opts.mode {
            Mode::Standalone => None,
            Mode::Distributed => {
                let meta_client = new_metasrv_client(
                    opts.node_id.context(MissingNodeIdSnafu)?,
                    opts.meta_client_opts
                        .as_ref()
                        .context(MissingMetasrvOptsSnafu)?,
                )
                .await?;
                Some(Arc::new(meta_client))
            }
        };

        let table_engine = Arc::new(DefaultEngine::new(
            TableEngineConfig::default(),
            EngineImpl::new(
                StorageEngineConfig::default(),
                Arc::new(log_store),
                object_store.clone(),
            ),
            object_store,
        ));

        // create remote catalog manager
        let (catalog_manager, factory, table_id_provider) = match opts.mode {
            Mode::Standalone => {
                let catalog = Arc::new(
                    catalog::local::LocalCatalogManager::try_new(table_engine.clone())
                        .await
                        .context(CatalogSnafu)?,
                );
                let factory = QueryEngineFactory::new(catalog.clone());
                (
                    catalog.clone() as CatalogManagerRef,
                    factory,
                    Some(catalog as TableIdProviderRef),
                )
            }

            Mode::Distributed => {
                let catalog = Arc::new(catalog::remote::RemoteCatalogManager::new(
                    table_engine.clone(),
                    opts.node_id.context(MissingNodeIdSnafu)?,
                    Arc::new(MetaKvBackend {
                        client: meta_client.as_ref().unwrap().clone(),
                    }),
                ));
                let factory = QueryEngineFactory::new(catalog.clone());
                (catalog as CatalogManagerRef, factory, None)
            }
        };

        let query_engine = factory.query_engine();
        let script_executor =
            ScriptExecutor::new(catalog_manager.clone(), query_engine.clone()).await?;

        let heartbeat_task = match opts.mode {
            Mode::Standalone => None,
            Mode::Distributed => Some(HeartbeatTask::new(
                opts.node_id.context(MissingNodeIdSnafu)?,
                opts.rpc_addr.clone(),
                meta_client.as_ref().unwrap().clone(),
            )),
        };
        Ok(Self {
            query_engine: query_engine.clone(),
            sql_handler: SqlHandler::new(table_engine, catalog_manager.clone()),
            catalog_manager,
            physical_planner: PhysicalPlanner::new(query_engine),
            script_executor,
            meta_client,
            heartbeat_task,
            table_id_provider,
        })
    }

    pub async fn start(&self) -> Result<()> {
        self.catalog_manager
            .start()
            .await
            .context(NewCatalogSnafu)?;
        if let Some(task) = &self.heartbeat_task {
            task.start().await?;
        }
        Ok(())
    }

    pub fn sql_handler(&self) -> &SqlHandler {
        &self.sql_handler
    }

    pub fn catalog_manager(&self) -> &CatalogManagerRef {
        &self.catalog_manager
    }
}

pub(crate) async fn new_object_store(store_config: &ObjectStoreConfig) -> Result<ObjectStore> {
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

    let object_store = ObjectStore::new(accessor).layer(LoggingLayer); // Add logging

    Ok(object_store)
}

/// Create metasrv client instance and spawn heartbeat loop.
async fn new_metasrv_client(node_id: u64, meta_config: &MetaClientOpts) -> Result<MetaClient> {
    let cluster_id = 0; // TODO(hl): read from config
    let member_id = node_id;

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
        .start(&meta_config.metasrv_addrs)
        .await
        .context(MetaClientInitSnafu)?;

    // required only when the heartbeat_client is enabled
    meta_client
        .ask_leader()
        .await
        .context(MetaClientInitSnafu)?;
    Ok(meta_client)
}

pub(crate) async fn create_local_file_log_store(
    opts: &DatanodeOptions,
) -> Result<LocalFileLogStore> {
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
