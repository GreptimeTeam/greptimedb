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
use catalog::remote::region_alive_keeper::RegionAliveKeepers;
use catalog::remote::{CachedMetaKvBackend, RemoteCatalogManager};
use catalog::{CatalogManager, CatalogManagerRef, RegisterTableRequest};
use common_base::paths::{CLUSTER_DIR, WAL_DIR};
use common_base::Plugins;
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, MIN_USER_TABLE_ID};
use common_error::ext::BoxedError;
use common_greptimedb_telemetry::GreptimeDBTelemetryTask;
use common_grpc::channel_manager::{ChannelConfig, ChannelManager};
use common_meta::heartbeat::handler::parse_mailbox_message::ParseMailboxMessageHandler;
use common_meta::heartbeat::handler::HandlerGroupExecutor;
use common_meta::key::TableMetadataManager;
use common_procedure::local::{LocalManager, ManagerConfig};
use common_procedure::store::state_store::ObjectStateStore;
use common_procedure::ProcedureManagerRef;
use common_telemetry::logging::{debug, info};
use file_table_engine::engine::immutable::ImmutableFileTableEngine;
use log_store::raft_engine::log_store::RaftEngineLogStore;
use log_store::LogConfig;
use meta_client::client::{MetaClient, MetaClientBuilder};
use meta_client::MetaClientOptions;
use mito::config::EngineConfig as TableEngineConfig;
use mito::engine::MitoEngine;
use object_store::{util, ObjectStore};
use query::query_engine::{QueryEngineFactory, QueryEngineRef};
use servers::Mode;
use session::context::QueryContextBuilder;
use snafu::prelude::*;
use storage::compaction::{CompactionHandler, CompactionSchedulerRef};
use storage::config::EngineConfig as StorageEngineConfig;
use storage::scheduler::{LocalScheduler, SchedulerConfig};
use storage::EngineImpl;
use store_api::logstore::LogStore;
use table::engine::manager::{MemoryTableEngineManager, TableEngineManagerRef};
use table::engine::{TableEngine, TableEngineProcedureRef};
use table::requests::FlushTableRequest;
use table::table::numbers::NumbersTable;
use table::table::TableIdProviderRef;
use table::Table;

use crate::datanode::{DatanodeOptions, ObjectStoreConfig, ProcedureConfig, WalConfig};
use crate::error::{
    self, CatalogSnafu, IncorrectInternalStateSnafu, MetaClientInitSnafu, MissingMetasrvOptsSnafu,
    MissingNodeIdSnafu, NewCatalogSnafu, OpenLogStoreSnafu, RecoverProcedureSnafu, Result,
    ShutdownInstanceSnafu, StartProcedureManagerSnafu, StopProcedureManagerSnafu,
};
use crate::heartbeat::handler::close_region::CloseRegionHandler;
use crate::heartbeat::handler::open_region::OpenRegionHandler;
use crate::heartbeat::HeartbeatTask;
use crate::sql::{SqlHandler, SqlRequest};
use crate::store;
use crate::telemetry::get_greptimedb_telemetry_task;

mod grpc;
pub mod sql;

pub(crate) type DefaultEngine = MitoEngine<EngineImpl<RaftEngineLogStore>>;

// An abstraction to read/write services.
pub struct Instance {
    pub(crate) query_engine: QueryEngineRef,
    pub(crate) sql_handler: SqlHandler,
    pub(crate) catalog_manager: CatalogManagerRef,
    pub(crate) table_id_provider: Option<TableIdProviderRef>,
    procedure_manager: ProcedureManagerRef,
    greptimedb_telemetry_task: Arc<GreptimeDBTelemetryTask>,
}

pub type InstanceRef = Arc<Instance>;

impl Instance {
    pub async fn with_opts(
        opts: &DatanodeOptions,
        plugins: Arc<Plugins>,
    ) -> Result<(InstanceRef, Option<HeartbeatTask>)> {
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

        Self::new(opts, meta_client, compaction_scheduler, plugins).await
    }

    fn build_heartbeat_task(
        opts: &DatanodeOptions,
        meta_client: Option<Arc<MetaClient>>,
        catalog_manager: CatalogManagerRef,
        engine_manager: TableEngineManagerRef,
        region_alive_keepers: Option<Arc<RegionAliveKeepers>>,
    ) -> Result<Option<HeartbeatTask>> {
        Ok(match opts.mode {
            Mode::Standalone => None,
            Mode::Distributed => {
                let node_id = opts.node_id.context(MissingNodeIdSnafu)?;
                let meta_client = meta_client.context(IncorrectInternalStateSnafu {
                    state: "meta client is not provided when building heartbeat task",
                })?;
                let region_alive_keepers =
                    region_alive_keepers.context(IncorrectInternalStateSnafu {
                        state: "region_alive_keepers is not provided when building heartbeat task",
                    })?;
                let handlers_executor = HandlerGroupExecutor::new(vec![
                    Arc::new(ParseMailboxMessageHandler),
                    Arc::new(OpenRegionHandler::new(
                        catalog_manager.clone(),
                        engine_manager.clone(),
                        region_alive_keepers.clone(),
                    )),
                    Arc::new(CloseRegionHandler::new(
                        catalog_manager.clone(),
                        engine_manager,
                        region_alive_keepers.clone(),
                    )),
                    region_alive_keepers.clone(),
                ]);

                Some(HeartbeatTask::new(
                    node_id,
                    opts,
                    meta_client,
                    catalog_manager,
                    Arc::new(handlers_executor),
                    opts.heartbeat.interval_millis,
                    region_alive_keepers,
                ))
            }
        })
    }

    pub(crate) async fn new(
        opts: &DatanodeOptions,
        meta_client: Option<Arc<MetaClient>>,
        compaction_scheduler: CompactionSchedulerRef<RaftEngineLogStore>,
        plugins: Arc<Plugins>,
    ) -> Result<(InstanceRef, Option<HeartbeatTask>)> {
        let object_store = store::new_object_store(&opts.storage.store).await?;
        let log_store = Arc::new(create_log_store(&opts.storage.store, &opts.wal).await?);

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

        let immutable_file_engine = Arc::new(ImmutableFileTableEngine::new(
            file_table_engine::config::EngineConfig::default(),
            object_store.clone(),
        ));

        let engine_procedures = HashMap::from([
            (
                mito_engine.name().to_string(),
                mito_engine.clone() as TableEngineProcedureRef,
            ),
            (
                immutable_file_engine.name().to_string(),
                immutable_file_engine.clone() as TableEngineProcedureRef,
            ),
        ]);
        let engine_manager = Arc::new(
            MemoryTableEngineManager::with(vec![
                mito_engine.clone(),
                immutable_file_engine.clone(),
            ])
            .with_engine_procedures(engine_procedures),
        );

        // create remote catalog manager
        let (catalog_manager, table_id_provider, region_alive_keepers) = match opts.mode {
            Mode::Standalone => {
                if opts.enable_memory_catalog {
                    let catalog = catalog::local::MemoryCatalogManager::with_default_setup();
                    let table = NumbersTable::new(MIN_USER_TABLE_ID);

                    let _ = catalog
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
                        None,
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
                        None,
                    )
                }
            }

            Mode::Distributed => {
                let meta_client = meta_client.clone().context(IncorrectInternalStateSnafu {
                    state: "meta client is not provided when creating distributed Datanode",
                })?;

                let kv_backend = Arc::new(CachedMetaKvBackend::new(meta_client));

                let region_alive_keepers = Arc::new(RegionAliveKeepers::new(
                    engine_manager.clone(),
                    opts.heartbeat.interval_millis,
                ));

                let catalog_manager = Arc::new(RemoteCatalogManager::new(
                    engine_manager.clone(),
                    opts.node_id.context(MissingNodeIdSnafu)?,
                    kv_backend.clone(),
                    region_alive_keepers.clone(),
                    Arc::new(TableMetadataManager::new(kv_backend)),
                ));

                (
                    catalog_manager as CatalogManagerRef,
                    None,
                    Some(region_alive_keepers),
                )
            }
        };

        let factory = QueryEngineFactory::new_with_plugins(
            catalog_manager.clone(),
            false,
            None,
            None,
            plugins,
        );
        let query_engine = factory.query_engine();

        let procedure_manager =
            create_procedure_manager(opts.node_id.unwrap_or(0), &opts.procedure, object_store)
                .await?;
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

        let instance = Arc::new(Self {
            query_engine: query_engine.clone(),
            sql_handler: SqlHandler::new(
                engine_manager.clone(),
                catalog_manager.clone(),
                procedure_manager.clone(),
            ),
            catalog_manager: catalog_manager.clone(),
            table_id_provider,
            procedure_manager,
            greptimedb_telemetry_task: get_greptimedb_telemetry_task(&opts.mode).await,
        });

        let heartbeat_task = Instance::build_heartbeat_task(
            opts,
            meta_client,
            catalog_manager,
            engine_manager,
            region_alive_keepers,
        )?;

        Ok((instance, heartbeat_task))
    }

    pub async fn start(&self) -> Result<()> {
        self.catalog_manager
            .start()
            .await
            .context(NewCatalogSnafu)?;

        // Recover procedures after the catalog manager is started, so we can
        // ensure we can access all tables from the catalog manager.
        self.procedure_manager
            .recover()
            .await
            .context(RecoverProcedureSnafu)?;
        self.procedure_manager
            .start()
            .context(StartProcedureManagerSnafu)?;
        let _ = self
            .greptimedb_telemetry_task
            .start(common_runtime::bg_runtime())
            .map_err(|e| {
                debug!("Failed to start greptimedb telemetry task: {}", e);
            });

        Ok(())
    }

    pub async fn shutdown(&self) -> Result<()> {
        self.procedure_manager
            .stop()
            .await
            .context(StopProcedureManagerSnafu)?;

        self.flush_tables().await?;

        self.sql_handler
            .close()
            .await
            .map_err(BoxedError::new)
            .context(ShutdownInstanceSnafu)
    }

    pub async fn flush_tables(&self) -> Result<()> {
        info!("going to flush all schemas under {DEFAULT_CATALOG_NAME}");
        let schema_list = self
            .catalog_manager
            .schema_names(DEFAULT_CATALOG_NAME)
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
        let flush_result =
            futures::future::try_join_all(flush_requests.into_iter().map(|request| {
                self.sql_handler
                    .execute(request, QueryContextBuilder::default().build())
            }))
            .await
            .map_err(BoxedError::new)
            .context(ShutdownInstanceSnafu);
        info!("Flushed all tables result: {}", flush_result.is_ok());
        let _ = flush_result?;

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
    let config = SchedulerConfig::from(opts);
    let handler = CompactionHandler::default();
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

pub(crate) async fn create_log_store(
    store_config: &ObjectStoreConfig,
    wal_config: &WalConfig,
) -> Result<RaftEngineLogStore> {
    let wal_dir = match (&wal_config.dir, store_config) {
        (Some(dir), _) => dir.to_string(),
        (None, ObjectStoreConfig::File(file_config)) => {
            format!("{}{WAL_DIR}", util::normalize_dir(&file_config.data_home))
        }
        _ => return error::MissingWalDirConfigSnafu {}.fail(),
    };

    // create WAL directory
    fs::create_dir_all(path::Path::new(&wal_dir))
        .context(error::CreateDirSnafu { dir: &wal_dir })?;
    info!(
        "Creating logstore with config: {:?} and storage path: {}",
        wal_config, &wal_dir
    );
    let log_config = LogConfig {
        file_size: wal_config.file_size.0,
        log_file_dir: wal_dir,
        purge_interval: wal_config.purge_interval,
        purge_threshold: wal_config.purge_threshold.0,
        read_batch_size: wal_config.read_batch_size,
        sync_write: wal_config.sync_write,
    };

    let logstore = RaftEngineLogStore::try_new(log_config)
        .await
        .map_err(Box::new)
        .context(OpenLogStoreSnafu)?;
    Ok(logstore)
}

pub(crate) async fn create_procedure_manager(
    datanode_id: u64,
    procedure_config: &ProcedureConfig,
    object_store: ObjectStore,
) -> Result<ProcedureManagerRef> {
    info!(
        "Creating procedure manager with config: {:?}",
        procedure_config
    );

    let state_store = Arc::new(ObjectStateStore::new(object_store));

    let dn_store_path = format!("{CLUSTER_DIR}dn-{datanode_id}/");

    info!("The datanode internal storage path is: {}", dn_store_path);

    let manager_config = ManagerConfig {
        parent_path: dn_store_path,
        max_retry_times: procedure_config.max_retry_times,
        retry_delay: procedure_config.retry_delay,
        ..Default::default()
    };

    Ok(Arc::new(LocalManager::new(manager_config, state_store)))
}
