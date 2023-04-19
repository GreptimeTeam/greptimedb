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

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use std::{fs, path};

use catalog::remote::MetaKvBackend;
use catalog::{CatalogManager, CatalogManagerRef, RegisterTableRequest};
use common_base::readable_size::ReadableSize;
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, MIN_USER_TABLE_ID};
use common_error::prelude::BoxedError;
use common_grpc::channel_manager::{ChannelConfig, ChannelManager};
use common_procedure::local::{LocalManager, ManagerConfig};
use common_procedure::store::state_store::ObjectStateStore;
use common_procedure::ProcedureManagerRef;
use common_telemetry::logging::info;
use file_table_engine::engine::immutable::ImmutableFileTableEngine;
use log_store::raft_engine::log_store::RaftEngineLogStore;
use log_store::LogConfig;
use meta_client::client::{MetaClient, MetaClientBuilder};
use meta_client::MetaClientOptions;
use mito::config::EngineConfig as TableEngineConfig;
use mito::engine::MitoEngine;
use object_store::cache_policy::LruCacheLayer;
use object_store::layers::{LoggingLayer, MetricsLayer, RetryLayer, TracingLayer};
use object_store::services::{Fs as FsBuilder, Oss as OSSBuilder, S3 as S3Builder};
use object_store::{util, ObjectStore, ObjectStoreBuilder};
use query::query_engine::{QueryEngineFactory, QueryEngineRef};
use servers::Mode;
use session::context::QueryContext;
use snafu::prelude::*;
use storage::compaction::{CompactionHandler, CompactionSchedulerRef, SimplePicker};
use storage::config::EngineConfig as StorageEngineConfig;
use storage::scheduler::{LocalScheduler, SchedulerConfig};
use storage::EngineImpl;
use store_api::logstore::LogStore;
use table::engine::manager::MemoryTableEngineManager;
use table::engine::{TableEngine, TableEngineProcedureRef};
use table::requests::FlushTableRequest;
use table::table::numbers::NumbersTable;
use table::table::TableIdProviderRef;
use table::Table;

use crate::datanode::{
    DatanodeOptions, ObjectStoreConfig, ProcedureConfig, WalConfig, DEFAULT_OBJECT_STORE_CACHE_SIZE,
};
use crate::error::{
    self, CatalogSnafu, MetaClientInitSnafu, MissingMetasrvOptsSnafu, MissingNodeIdSnafu,
    NewCatalogSnafu, OpenLogStoreSnafu, RecoverProcedureSnafu, Result, ShutdownInstanceSnafu,
};
use crate::heartbeat::HeartbeatTask;
use crate::sql::{SqlHandler, SqlRequest};

mod grpc;
pub mod sql;

pub(crate) type DefaultEngine = MitoEngine<EngineImpl<RaftEngineLogStore>>;

// An abstraction to read/write services.
pub struct Instance {
    pub(crate) query_engine: QueryEngineRef,
    pub(crate) sql_handler: SqlHandler,
    pub(crate) catalog_manager: CatalogManagerRef,
    pub(crate) table_id_provider: Option<TableIdProviderRef>,
    pub(crate) heartbeat_task: Option<HeartbeatTask>,
    procedure_manager: Option<ProcedureManagerRef>,
}

pub type InstanceRef = Arc<Instance>;

impl Instance {
    pub async fn new(opts: &DatanodeOptions) -> Result<Self> {
        let meta_client = match opts.mode {
            Mode::Standalone => None,
            Mode::Distributed => {
                let meta_client = new_metasrv_client(
                    opts.node_id.context(MissingNodeIdSnafu)?,
                    opts.meta_client_options
                        .as_ref()
                        .context(MissingMetasrvOptsSnafu)?,
                )
                .await?;
                Some(Arc::new(meta_client))
            }
        };

        let compaction_scheduler = create_compaction_scheduler(opts);

        Self::new_with(opts, meta_client, compaction_scheduler).await
    }

    pub(crate) async fn new_with(
        opts: &DatanodeOptions,
        meta_client: Option<Arc<MetaClient>>,
        compaction_scheduler: CompactionSchedulerRef<RaftEngineLogStore>,
    ) -> Result<Self> {
        let object_store = new_object_store(&opts.storage.store).await?;
        let log_store = Arc::new(create_log_store(&opts.wal).await?);

        let mito_engine = Arc::new(DefaultEngine::new(
            TableEngineConfig::default(),
            EngineImpl::new(
                StorageEngineConfig::from(opts),
                log_store.clone(),
                object_store.clone(),
                compaction_scheduler,
            ),
            object_store.clone(),
        ));

        let mut engine_procedures = HashMap::with_capacity(2);
        engine_procedures.insert(
            mito_engine.name().to_string(),
            mito_engine.clone() as TableEngineProcedureRef,
        );

        let immutable_file_engine = Arc::new(ImmutableFileTableEngine::new(
            file_table_engine::config::EngineConfig::default(),
            object_store.clone(),
        ));
        // TODO(yingwen): Insert the file table engine into `engine_procedures`
        // once #1372 is ready.
        let engine_manager = Arc::new(
            MemoryTableEngineManager::with(vec![
                mito_engine.clone(),
                immutable_file_engine.clone(),
            ])
            .with_engine_procedures(engine_procedures),
        );

        // create remote catalog manager
        let (catalog_manager, table_id_provider) = match opts.mode {
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

                    (
                        catalog.clone() as CatalogManagerRef,
                        Some(catalog as TableIdProviderRef),
                    )
                } else {
                    let catalog = Arc::new(
                        catalog::local::LocalCatalogManager::try_new(engine_manager.clone())
                            .await
                            .context(CatalogSnafu)?,
                    );

                    (
                        catalog.clone() as CatalogManagerRef,
                        Some(catalog as TableIdProviderRef),
                    )
                }
            }

            Mode::Distributed => {
                let catalog = Arc::new(catalog::remote::RemoteCatalogManager::new(
                    engine_manager.clone(),
                    opts.node_id.context(MissingNodeIdSnafu)?,
                    Arc::new(MetaKvBackend {
                        client: meta_client.as_ref().unwrap().clone(),
                    }),
                ));
                (catalog as CatalogManagerRef, None)
            }
        };

        let factory = QueryEngineFactory::new(catalog_manager.clone());
        let query_engine = factory.query_engine();

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

        let procedure_manager = create_procedure_manager(&opts.procedure).await?;
        // Register all procedures.
        if let Some(procedure_manager) = &procedure_manager {
            // Register procedures of the mito engine.
            mito_engine.register_procedure_loaders(&**procedure_manager);
            // Register procedures in table-procedure crate.
            table_procedure::register_procedure_loaders(
                catalog_manager.clone(),
                mito_engine.clone(),
                mito_engine.clone(),
                &**procedure_manager,
            );
            // TODO(yingwen): Register procedures of the file table engine once #1372
            // is ready.
        }

        Ok(Self {
            query_engine: query_engine.clone(),
            sql_handler: SqlHandler::new(
                engine_manager,
                catalog_manager.clone(),
                procedure_manager.clone(),
            ),
            catalog_manager,
            heartbeat_task,
            table_id_provider,
            procedure_manager,
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

        // Recover procedures after the catalog manager is started, so we can
        // ensure we can access all tables from the catalog manager.
        if let Some(procedure_manager) = &self.procedure_manager {
            procedure_manager
                .recover()
                .await
                .context(RecoverProcedureSnafu)?;
        }
        Ok(())
    }

    pub async fn shutdown(&self) -> Result<()> {
        if let Some(heartbeat_task) = &self.heartbeat_task {
            heartbeat_task
                .close()
                .await
                .map_err(BoxedError::new)
                .context(ShutdownInstanceSnafu)?;
        }

        self.flush_tables().await?;

        self.sql_handler
            .close()
            .await
            .map_err(BoxedError::new)
            .context(ShutdownInstanceSnafu)
    }

    pub async fn flush_tables(&self) -> Result<()> {
        info!("going to flush all schemas");
        let schema_list = self
            .catalog_manager
            .catalog(DEFAULT_CATALOG_NAME)
            .map_err(BoxedError::new)
            .context(ShutdownInstanceSnafu)?
            .expect("Default schema not found")
            .schema_names()
            .map_err(BoxedError::new)
            .context(ShutdownInstanceSnafu)?;
        let flush_requests = schema_list
            .into_iter()
            .map(|schema_name| {
                SqlRequest::FlushTable(FlushTableRequest {
                    catalog_name: DEFAULT_CATALOG_NAME.to_string(),
                    schema_name,
                    table_name: None,
                    region_number: None,
                    wait: Some(true),
                })
            })
            .collect::<Vec<_>>();
        let flush_result = futures::future::try_join_all(
            flush_requests
                .into_iter()
                .map(|request| self.sql_handler.execute(request, QueryContext::arc())),
        )
        .await
        .map_err(BoxedError::new)
        .context(ShutdownInstanceSnafu);
        info!("Flushed all tables result: {}", flush_result.is_ok());
        flush_result?;

        Ok(())
    }

    pub fn sql_handler(&self) -> &SqlHandler {
        &self.sql_handler
    }

    pub fn catalog_manager(&self) -> &CatalogManagerRef {
        &self.catalog_manager
    }

    pub fn query_engine(&self) -> QueryEngineRef {
        self.query_engine.clone()
    }
}

fn create_compaction_scheduler<S: LogStore>(opts: &DatanodeOptions) -> CompactionSchedulerRef<S> {
    let picker = SimplePicker::default();
    let config = SchedulerConfig::from(opts);
    let handler = CompactionHandler::new(picker);
    let scheduler = LocalScheduler::new(config, handler);
    Arc::new(scheduler)
}

pub(crate) async fn new_object_store(store_config: &ObjectStoreConfig) -> Result<ObjectStore> {
    let object_store = match store_config {
        ObjectStoreConfig::File { .. } => new_fs_object_store(store_config).await,
        ObjectStoreConfig::S3 { .. } => new_s3_object_store(store_config).await,
        ObjectStoreConfig::Oss { .. } => new_oss_object_store(store_config).await,
    };

    // Don't enable retry layer when using local file backend.
    let object_store = if !matches!(store_config, ObjectStoreConfig::File(..)) {
        object_store.map(|object_store| object_store.layer(RetryLayer::new().with_jitter()))
    } else {
        object_store
    };

    object_store.map(|object_store| {
        object_store
            .layer(MetricsLayer)
            .layer(
                LoggingLayer::default()
                    // Print the expected error only in DEBUG level.
                    // See https://docs.rs/opendal/latest/opendal/layers/struct.LoggingLayer.html#method.with_error_level
                    .with_error_level(Some(log::Level::Debug)),
            )
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
    builder
        .root(&root)
        .bucket(&oss_config.bucket)
        .endpoint(&oss_config.endpoint)
        .access_key_id(&oss_config.access_key_id)
        .access_key_secret(&oss_config.access_key_secret);

    let object_store = ObjectStore::new(builder)
        .with_context(|_| error::InitBackendSnafu {
            config: store_config.clone(),
        })?
        .finish();

    create_object_store_with_cache(object_store, store_config)
}

fn create_object_store_with_cache(
    object_store: ObjectStore,
    store_config: &ObjectStoreConfig,
) -> Result<ObjectStore> {
    let (cache_path, cache_capacity) = match store_config {
        ObjectStoreConfig::S3(s3_config) => {
            let path = s3_config.cache_path.as_ref();
            let capacity = s3_config
                .cache_capacity
                .unwrap_or(DEFAULT_OBJECT_STORE_CACHE_SIZE);
            (path, capacity)
        }
        ObjectStoreConfig::Oss(oss_config) => {
            let path = oss_config.cache_path.as_ref();
            let capacity = oss_config
                .cache_capacity
                .unwrap_or(DEFAULT_OBJECT_STORE_CACHE_SIZE);
            (path, capacity)
        }
        _ => (None, ReadableSize(0)),
    };

    if let Some(path) = cache_path {
        let cache_store =
            FsBuilder::default()
                .root(path)
                .build()
                .with_context(|_| error::InitBackendSnafu {
                    config: store_config.clone(),
                })?;
        let cache_layer = LruCacheLayer::new(Arc::new(cache_store), cache_capacity.0 as usize);
        Ok(object_store.layer(cache_layer))
    } else {
        Ok(object_store)
    }
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
    builder
        .root(&root)
        .bucket(&s3_config.bucket)
        .access_key_id(&s3_config.access_key_id)
        .secret_access_key(&s3_config.secret_access_key);

    if s3_config.endpoint.is_some() {
        builder.endpoint(s3_config.endpoint.as_ref().unwrap());
    }
    if s3_config.region.is_some() {
        builder.region(s3_config.region.as_ref().unwrap());
    }

    create_object_store_with_cache(
        ObjectStore::new(builder)
            .with_context(|_| error::InitBackendSnafu {
                config: store_config.clone(),
            })?
            .finish(),
        store_config,
    )
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
    if path::Path::new(&atomic_write_dir).exists() {
        info!(
            "Begin to clean temp storage directory: {}",
            &atomic_write_dir
        );
        fs::remove_dir_all(&atomic_write_dir).context(error::RemoveDirSnafu {
            dir: &atomic_write_dir,
        })?;
        info!("Cleaned temp storage directory: {}", &atomic_write_dir);
    }

    let mut builder = FsBuilder::default();
    builder.root(&data_dir).atomic_write_dir(&atomic_write_dir);

    let object_store = ObjectStore::new(builder)
        .context(error::InitBackendSnafu {
            config: store_config.clone(),
        })?
        .finish();

    Ok(object_store)
}

/// Create metasrv client instance and spawn heartbeat loop.
async fn new_metasrv_client(node_id: u64, meta_config: &MetaClientOptions) -> Result<MetaClient> {
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

pub(crate) async fn create_procedure_manager(
    procedure_config: &Option<ProcedureConfig>,
) -> Result<Option<ProcedureManagerRef>> {
    let Some(procedure_config) = procedure_config else {
        return Ok(None);
    };

    info!(
        "Creating procedure manager with config: {:?}",
        procedure_config
    );

    let object_store = new_object_store(&procedure_config.store).await?;
    let state_store = Arc::new(ObjectStateStore::new(object_store));

    let manager_config = ManagerConfig {
        max_retry_times: procedure_config.max_retry_times,
        retry_delay: procedure_config.retry_delay,
    };

    Ok(Some(Arc::new(LocalManager::new(
        manager_config,
        state_store,
    ))))
}
