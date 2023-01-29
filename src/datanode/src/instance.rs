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

use std::sync::Arc;
use std::time::Duration;
use std::{fs, path};

use backon::ExponentialBackoff;
use catalog::remote::MetaKvBackend;
use catalog::{CatalogManager, CatalogManagerRef, RegisterTableRequest};
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, MIN_USER_TABLE_ID};
use common_grpc::channel_manager::{ChannelConfig, ChannelManager};
use common_telemetry::logging::info;
use log_store::raft_engine::log_store::RaftEngineLogStore;
use log_store::LogConfig;
use meta_client::client::{MetaClient, MetaClientBuilder};
use meta_client::MetaClientOpts;
use mito::config::EngineConfig as TableEngineConfig;
use mito::engine::MitoEngine;
use object_store::layers::{LoggingLayer, MetricsLayer, RetryLayer, TracingLayer};
use object_store::services::fs::Builder as FsBuilder;
use object_store::services::oss::Builder as OSSBuilder;
use object_store::services::s3::Builder as S3Builder;
use object_store::{util, ObjectStore};
use query::query_engine::{QueryEngineFactory, QueryEngineRef};
use servers::Mode;
use snafu::prelude::*;
use storage::config::EngineConfig as StorageEngineConfig;
use storage::EngineImpl;
use table::table::numbers::NumbersTable;
use table::table::TableIdProviderRef;
use table::Table;

use crate::datanode::{DatanodeOptions, ObjectStoreConfig, WalConfig};
use crate::error::{
    self, CatalogSnafu, MetaClientInitSnafu, MissingMetasrvOptsSnafu, MissingNodeIdSnafu,
    NewCatalogSnafu, OpenLogStoreSnafu, Result,
};
use crate::heartbeat::HeartbeatTask;
use crate::script::ScriptExecutor;
use crate::sql::SqlHandler;

mod grpc;
mod script;
mod sql;

pub(crate) type DefaultEngine = MitoEngine<EngineImpl<RaftEngineLogStore>>;

// An abstraction to read/write services.
pub struct Instance {
    pub(crate) query_engine: QueryEngineRef,
    pub(crate) sql_handler: SqlHandler,
    pub(crate) catalog_manager: CatalogManagerRef,
    pub(crate) script_executor: ScriptExecutor,
    pub(crate) table_id_provider: Option<TableIdProviderRef>,
    pub(crate) heartbeat_task: Option<HeartbeatTask>,
}

pub type InstanceRef = Arc<Instance>;

impl Instance {
    pub async fn new(opts: &DatanodeOptions) -> Result<Self> {
        let object_store = new_object_store(&opts.storage).await?;
        let logstore = Arc::new(create_log_store(&opts.wal).await?);

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
                logstore.clone(),
                object_store.clone(),
            ),
            object_store,
        ));

        // create remote catalog manager
        let (catalog_manager, factory, table_id_provider) = match opts.mode {
            Mode::Standalone => {
                if opts.enable_memory_catalog {
                    let catalog = Arc::new(catalog::local::MemoryCatalogManager::default());
                    let table = NumbersTable::new(MIN_USER_TABLE_ID);

                    catalog
                        .register_table(RegisterTableRequest {
                            table_id: MIN_USER_TABLE_ID,
                            table_name: table.table_info().name.to_string(),
                            table: Arc::new(table),
                            catalog: DEFAULT_CATALOG_NAME.to_string(),
                            schema: DEFAULT_SCHEMA_NAME.to_string(),
                        })
                        .await
                        .expect("Failed to register numbers");

                    let factory = QueryEngineFactory::new(catalog.clone());

                    (
                        catalog.clone() as CatalogManagerRef,
                        factory,
                        Some(catalog as TableIdProviderRef),
                    )
                } else {
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
                opts.rpc_hostname.clone(),
                meta_client.as_ref().unwrap().clone(),
                catalog_manager.clone(),
            )),
        };
        Ok(Self {
            query_engine: query_engine.clone(),
            sql_handler: SqlHandler::new(
                table_engine,
                catalog_manager.clone(),
                query_engine.clone(),
            ),
            catalog_manager,
            script_executor,
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
    let object_store = match store_config {
        ObjectStoreConfig::File { .. } => new_fs_object_store(store_config).await,
        ObjectStoreConfig::S3 { .. } => new_s3_object_store(store_config).await,
        ObjectStoreConfig::Oss { .. } => new_oss_object_store(store_config).await,
    };

    object_store.map(|object_store| {
        object_store
            .layer(RetryLayer::new(ExponentialBackoff::default().with_jitter()))
            .layer(MetricsLayer)
            .layer(LoggingLayer::default())
            .layer(TracingLayer)
    })
}

pub(crate) async fn new_oss_object_store(store_config: &ObjectStoreConfig) -> Result<ObjectStore> {
    let oss_config = match store_config {
        ObjectStoreConfig::Oss(config) => config,
        _ => unreachable!(),
    };

    let root = util::normalize_dir(&oss_config.root);
    info!(
        "The oss storage bucket is: {}, root is: {}",
        oss_config.bucket, &root
    );

    let mut builder = OSSBuilder::default();
    let builder = builder
        .root(&root)
        .bucket(&oss_config.bucket)
        .endpoint(&oss_config.endpoint)
        .access_key_id(&oss_config.access_key_id)
        .access_key_secret(&oss_config.access_key_secret);

    let accessor = builder.build().with_context(|_| error::InitBackendSnafu {
        config: store_config.clone(),
    })?;

    Ok(ObjectStore::new(accessor))
}

pub(crate) async fn new_s3_object_store(store_config: &ObjectStoreConfig) -> Result<ObjectStore> {
    let s3_config = match store_config {
        ObjectStoreConfig::S3(config) => config,
        _ => unreachable!(),
    };

    let root = util::normalize_dir(&s3_config.root);
    info!(
        "The s3 storage bucket is: {}, root is: {}",
        s3_config.bucket, &root
    );

    let mut builder = S3Builder::default();
    let mut builder = builder
        .root(&root)
        .bucket(&s3_config.bucket)
        .access_key_id(&s3_config.access_key_id)
        .secret_access_key(&s3_config.secret_access_key);

    if s3_config.endpoint.is_some() {
        builder = builder.endpoint(s3_config.endpoint.as_ref().unwrap());
    }
    if s3_config.region.is_some() {
        builder = builder.region(s3_config.region.as_ref().unwrap());
    }

    let accessor = builder.build().with_context(|_| error::InitBackendSnafu {
        config: store_config.clone(),
    })?;

    Ok(ObjectStore::new(accessor))
}

pub(crate) async fn new_fs_object_store(store_config: &ObjectStoreConfig) -> Result<ObjectStore> {
    let file_config = match store_config {
        ObjectStoreConfig::File(config) => config,
        _ => unreachable!(),
    };
    let data_dir = util::normalize_dir(&file_config.data_dir);
    fs::create_dir_all(path::Path::new(&data_dir))
        .context(error::CreateDirSnafu { dir: &data_dir })?;
    info!("The file storage directory is: {}", &data_dir);

    let atomic_write_dir = format!("{data_dir}/.tmp/");

    let accessor = FsBuilder::default()
        .root(&data_dir)
        .atomic_write_dir(&atomic_write_dir)
        .build()
        .context(error::InitBackendSnafu {
            config: store_config.clone(),
        })?;

    Ok(ObjectStore::new(accessor))
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

pub(crate) async fn create_log_store(wal_config: &WalConfig) -> Result<RaftEngineLogStore> {
    // create WAL directory
    fs::create_dir_all(path::Path::new(&wal_config.dir)).context(error::CreateDirSnafu {
        dir: &wal_config.dir,
    })?;
    info!("Creating logstore with config: {:?}", wal_config);
    let log_config = LogConfig {
        file_size: wal_config.file_size.0,
        log_file_dir: wal_config.dir.clone(),
        purge_interval: wal_config.purge_interval,
        purge_threshold: wal_config.purge_threshold.0,
        read_batch_size: wal_config.read_batch_size,
        sync_write: wal_config.sync_write,
    };

    let logstore = RaftEngineLogStore::try_new(log_config)
        .await
        .context(OpenLogStoreSnafu)?;
    Ok(logstore)
}
