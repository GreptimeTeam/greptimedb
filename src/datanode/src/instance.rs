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

use api::v1::meta::Role;
use catalog::remote::MetaKvBackend;
use catalog::{CatalogManager, CatalogManagerRef, RegisterTableRequest};
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
use object_store::ObjectStore;
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

use crate::datanode::{DatanodeOptions, ProcedureConfig, WalConfig};
use crate::error::{
    self, CatalogSnafu, MetaClientInitSnafu, MissingMetasrvOptsSnafu, MissingNodeIdSnafu,
    NewCatalogSnafu, OpenLogStoreSnafu, RecoverProcedureSnafu, Result, ShutdownInstanceSnafu,
    StartProcedureManagerSnafu, StopProcedureManagerSnafu,
};
use crate::heartbeat::handler::open_region::OpenRegionHandler;
use crate::heartbeat::handler::parse_mailbox_message::ParseMailboxMessageHandler;
use crate::heartbeat::handler::HandlerGroupExecutor;
use crate::heartbeat::HeartbeatTask;
use crate::sql::{SqlHandler, SqlRequest};
use crate::store;

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
    procedure_manager: ProcedureManagerRef,
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
        let object_store = store::new_object_store(&opts.storage.store).await?;
        let log_store = Arc::new(create_log_store(&opts.wal).await?);

        let mito_engine = Arc::new(DefaultEngine::new(
            TableEngineConfig {
                compress_manifest: opts.storage.manifest.compress,
            },
            EngineImpl::new(
                StorageEngineConfig::from(opts),
                log_store.clone(),
                object_store.clone(),
                compaction_scheduler,
            )
            .unwrap(),
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
        engine_procedures.insert(
            immutable_file_engine.name().to_string(),
            immutable_file_engine.clone() as TableEngineProcedureRef,
        );

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

        let factory = QueryEngineFactory::new(catalog_manager.clone(), false);
        let query_engine = factory.query_engine();

        let heartbeat_task = match opts.mode {
            Mode::Standalone => None,
            Mode::Distributed => Some(HeartbeatTask::new(
                opts.node_id.context(MissingNodeIdSnafu)?,
                opts.rpc_addr.clone(),
                opts.rpc_hostname.clone(),
                meta_client.as_ref().unwrap().clone(),
                catalog_manager.clone(),
                Arc::new(HandlerGroupExecutor::new(vec![
                    Arc::new(ParseMailboxMessageHandler::default()),
                    Arc::new(OpenRegionHandler::new(
                        catalog_manager.clone(),
                        engine_manager.clone(),
                    )),
                ])),
            )),
        };

        let procedure_manager = create_procedure_manager(&opts.procedure, object_store).await?;
        // Register all procedures.
        // Register procedures of the mito engine.
        mito_engine.register_procedure_loaders(&*procedure_manager);
        // Register procedures of the file table engine.
        immutable_file_engine.register_procedure_loaders(&*procedure_manager);
        // Register procedures in table-procedure crate.
        table_procedure::register_procedure_loaders(
            catalog_manager.clone(),
            mito_engine.clone(),
            mito_engine.clone(),
            &*procedure_manager,
        );

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
        self.procedure_manager
            .recover()
            .await
            .context(RecoverProcedureSnafu)?;
        self.procedure_manager
            .start()
            .context(StartProcedureManagerSnafu)?;
        Ok(())
    }

    pub async fn shutdown(&self) -> Result<()> {
        self.procedure_manager
            .stop()
            .await
            .context(StopProcedureManagerSnafu)?;
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
            .await
            .map_err(BoxedError::new)
            .context(ShutdownInstanceSnafu)?
            .expect("Default schema not found")
            .schema_names()
            .await
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

/// Create metasrv client instance and spawn heartbeat loop.
async fn new_metasrv_client(node_id: u64, meta_config: &MetaClientOptions) -> Result<MetaClient> {
    let cluster_id = 0; // TODO(hl): read from config
    let member_id = node_id;

    let config = ChannelConfig::new()
        .timeout(Duration::from_millis(meta_config.timeout_millis))
        .connect_timeout(Duration::from_millis(meta_config.connect_timeout_millis))
        .tcp_nodelay(meta_config.tcp_nodelay);

    let channel_manager = ChannelManager::with_config(config);
    channel_manager.start_channel_recycle();

    let mut meta_client = MetaClientBuilder::new(cluster_id, member_id, Role::Datanode)
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
    procedure_config: &ProcedureConfig,
    object_store: ObjectStore,
) -> Result<ProcedureManagerRef> {
    info!(
        "Creating procedure manager with config: {:?}",
        procedure_config
    );

    let state_store = Arc::new(ObjectStateStore::new(object_store));

    let manager_config = ManagerConfig {
        max_retry_times: procedure_config.max_retry_times,
        retry_delay: procedure_config.retry_delay,
        ..Default::default()
    };

    Ok(Arc::new(LocalManager::new(manager_config, state_store)))
}
