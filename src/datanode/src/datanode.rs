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

//! Datanode implementation.

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use common_base::Plugins;
use common_error::ext::BoxedError;
use common_greptimedb_telemetry::GreptimeDBTelemetryTask;
use common_meta::cache::{LayeredCacheRegistry, SchemaCacheRef, TableSchemaCacheRef};
use common_meta::cache_invalidator::CacheInvalidatorRef;
use common_meta::datanode::TopicStatsReporter;
use common_meta::key::runtime_switch::RuntimeSwitchManager;
use common_meta::key::{SchemaMetadataManager, SchemaMetadataManagerRef};
use common_meta::kv_backend::KvBackendRef;
pub use common_procedure::options::ProcedureConfig;
use common_query::prelude::set_default_prefix;
use common_stat::ResourceStatImpl;
use common_telemetry::{error, info, warn};
use common_wal::config::DatanodeWalConfig;
use common_wal::config::kafka::DatanodeKafkaConfig;
use common_wal::config::raft_engine::RaftEngineConfig;
use file_engine::engine::FileRegionEngine;
use log_store::kafka::log_store::KafkaLogStore;
use log_store::kafka::{GlobalIndexCollector, default_index_file};
use log_store::noop::log_store::NoopLogStore;
use log_store::raft_engine::log_store::RaftEngineLogStore;
use meta_client::MetaClientRef;
use metric_engine::engine::MetricEngine;
use mito2::config::MitoConfig;
use mito2::engine::{MitoEngine, MitoEngineBuilder};
use mito2::region::opener::PartitionExprFetcherRef;
use mito2::sst::file_ref::{FileReferenceManager, FileReferenceManagerRef};
use object_store::manager::{ObjectStoreManager, ObjectStoreManagerRef};
use object_store::util::normalize_dir;
use query::QueryEngineFactory;
use query::dummy_catalog::{DummyCatalogManager, TableProviderFactoryRef};
use servers::server::ServerHandlers;
use snafu::{OptionExt, ResultExt, ensure};
use store_api::path_utils::WAL_DIR;
use store_api::region_engine::{
    RegionEngineRef, RegionRole, SetRegionRoleStateResponse, SettableRegionRoleState,
};
use tokio::fs;
use tokio::sync::Notify;

use crate::config::{DatanodeOptions, RegionEngineConfig, StorageConfig};
use crate::error::{
    self, BuildDatanodeSnafu, BuildMetricEngineSnafu, BuildMitoEngineSnafu, CreateDirSnafu,
    GetMetadataSnafu, MissingCacheSnafu, MissingNodeIdSnafu, OpenLogStoreSnafu, Result,
    ShutdownInstanceSnafu, ShutdownServerSnafu, StartServerSnafu,
};
use crate::event_listener::{
    NoopRegionServerEventListener, RegionServerEventListenerRef, RegionServerEventReceiver,
    new_region_server_event_channel,
};
use crate::greptimedb_telemetry::get_greptimedb_telemetry_task;
use crate::heartbeat::HeartbeatTask;
use crate::partition_expr_fetcher::MetaPartitionExprFetcher;
use crate::region_server::{DummyTableProviderFactory, RegionServer};
use crate::store::{self, new_object_store_without_cache};
use crate::utils::{RegionOpenRequests, build_region_open_requests};

/// Datanode service.
pub struct Datanode {
    services: ServerHandlers,
    heartbeat_task: Option<HeartbeatTask>,
    region_event_receiver: Option<RegionServerEventReceiver>,
    region_server: RegionServer,
    greptimedb_telemetry_task: Arc<GreptimeDBTelemetryTask>,
    leases_notifier: Option<Arc<Notify>>,
    plugins: Plugins,
}

impl Datanode {
    pub async fn start(&mut self) -> Result<()> {
        info!("Starting datanode instance...");

        self.start_heartbeat().await?;
        self.wait_coordinated().await;

        self.start_telemetry();

        self.services.start_all().await.context(StartServerSnafu)
    }

    pub fn server_handlers(&self) -> &ServerHandlers {
        &self.services
    }

    pub fn start_telemetry(&self) {
        if let Err(e) = self.greptimedb_telemetry_task.start() {
            warn!(e; "Failed to start telemetry task!");
        }
    }

    pub async fn start_heartbeat(&mut self) -> Result<()> {
        if let Some(task) = &self.heartbeat_task {
            // Safety: The event_receiver must exist.
            let receiver = self.region_event_receiver.take().unwrap();

            task.start(receiver, self.leases_notifier.clone()).await?;
        }
        Ok(())
    }

    /// If `leases_notifier` exists, it waits until leases have been obtained in all regions.
    pub async fn wait_coordinated(&mut self) {
        if let Some(notifier) = self.leases_notifier.take() {
            notifier.notified().await;
        }
    }

    pub fn setup_services(&mut self, services: ServerHandlers) {
        self.services = services;
    }

    pub async fn shutdown(&mut self) -> Result<()> {
        self.services
            .shutdown_all()
            .await
            .context(ShutdownServerSnafu)?;

        let _ = self.greptimedb_telemetry_task.stop().await;
        if let Some(heartbeat_task) = &self.heartbeat_task {
            heartbeat_task
                .close()
                .map_err(BoxedError::new)
                .context(ShutdownInstanceSnafu)?;
        }
        self.region_server.stop().await?;
        Ok(())
    }

    pub fn region_server(&self) -> RegionServer {
        self.region_server.clone()
    }

    pub fn plugins(&self) -> Plugins {
        self.plugins.clone()
    }
}

pub struct DatanodeBuilder {
    opts: DatanodeOptions,
    table_provider_factory: Option<TableProviderFactoryRef>,
    plugins: Plugins,
    meta_client: Option<MetaClientRef>,
    kv_backend: KvBackendRef,
    cache_registry: Option<Arc<LayeredCacheRegistry>>,
    topic_stats_reporter: Option<Box<dyn TopicStatsReporter>>,
    #[cfg(feature = "enterprise")]
    extension_range_provider_factory: Option<mito2::extension::BoxedExtensionRangeProviderFactory>,
}

impl DatanodeBuilder {
    pub fn new(opts: DatanodeOptions, plugins: Plugins, kv_backend: KvBackendRef) -> Self {
        Self {
            opts,
            table_provider_factory: None,
            plugins,
            meta_client: None,
            kv_backend,
            cache_registry: None,
            #[cfg(feature = "enterprise")]
            extension_range_provider_factory: None,
            topic_stats_reporter: None,
        }
    }

    pub fn options(&self) -> &DatanodeOptions {
        &self.opts
    }

    pub fn with_meta_client(&mut self, client: MetaClientRef) -> &mut Self {
        self.meta_client = Some(client);
        self
    }

    pub fn with_cache_registry(&mut self, registry: Arc<LayeredCacheRegistry>) -> &mut Self {
        self.cache_registry = Some(registry);
        self
    }

    pub fn kv_backend(&self) -> &KvBackendRef {
        &self.kv_backend
    }

    pub fn with_table_provider_factory(&mut self, factory: TableProviderFactoryRef) -> &mut Self {
        self.table_provider_factory = Some(factory);
        self
    }

    #[cfg(feature = "enterprise")]
    pub fn with_extension_range_provider(
        &mut self,
        extension_range_provider_factory: mito2::extension::BoxedExtensionRangeProviderFactory,
    ) -> &mut Self {
        self.extension_range_provider_factory = Some(extension_range_provider_factory);
        self
    }

    pub async fn build(mut self) -> Result<Datanode> {
        let node_id = self.opts.node_id.context(MissingNodeIdSnafu)?;
        set_default_prefix(self.opts.default_column_prefix.as_deref())
            .map_err(BoxedError::new)
            .context(BuildDatanodeSnafu)?;

        let meta_client = self.meta_client.take();

        // If metasrv client is provided, we will use it to control the region server.
        // Otherwise the region server is self-controlled, meaning no heartbeat and immediately
        // writable upon open.
        let controlled_by_metasrv = meta_client.is_some();

        // build and initialize region server
        let (region_event_listener, region_event_receiver) = if controlled_by_metasrv {
            let (tx, rx) = new_region_server_event_channel();
            (Box::new(tx) as _, Some(rx))
        } else {
            (Box::new(NoopRegionServerEventListener) as _, None)
        };

        let cache_registry = self.cache_registry.take().context(MissingCacheSnafu)?;
        let schema_cache: SchemaCacheRef = cache_registry.get().context(MissingCacheSnafu)?;
        let table_id_schema_cache: TableSchemaCacheRef =
            cache_registry.get().context(MissingCacheSnafu)?;

        let schema_metadata_manager = Arc::new(SchemaMetadataManager::new(
            table_id_schema_cache,
            schema_cache,
        ));

        let gc_enabled = self.opts.region_engine.iter().any(|engine| {
            if let RegionEngineConfig::Mito(config) = engine {
                config.gc.enable
            } else {
                false
            }
        });

        let file_ref_manager = Arc::new(FileReferenceManager::with_gc_enabled(
            Some(node_id),
            gc_enabled,
        ));
        let region_server = self
            .new_region_server(
                schema_metadata_manager,
                region_event_listener,
                file_ref_manager,
            )
            .await?;

        // TODO(weny): Considering introducing a readonly kv_backend trait.
        let runtime_switch_manager = RuntimeSwitchManager::new(self.kv_backend.clone());
        let is_recovery_mode = runtime_switch_manager
            .recovery_mode()
            .await
            .context(GetMetadataSnafu)?;

        let region_open_requests =
            build_region_open_requests(node_id, self.kv_backend.clone()).await?;
        let open_all_regions = open_all_regions(
            region_server.clone(),
            region_open_requests,
            !controlled_by_metasrv,
            self.opts.init_regions_parallelism,
            // Ignore nonexistent regions in recovery mode.
            is_recovery_mode,
        );

        if self.opts.init_regions_in_background {
            // Opens regions in background.
            common_runtime::spawn_global(async move {
                if let Err(err) = open_all_regions.await {
                    error!(err; "Failed to open regions during the startup.");
                }
            });
        } else {
            open_all_regions.await?;
        }

        let heartbeat_task = if let Some(meta_client) = meta_client {
            let task = self
                .create_heartbeat_task(&region_server, meta_client, cache_registry)
                .await?;
            Some(task)
        } else {
            None
        };

        let is_standalone = heartbeat_task.is_none();
        let greptimedb_telemetry_task = get_greptimedb_telemetry_task(
            Some(self.opts.storage.data_home.clone()),
            is_standalone && self.opts.enable_telemetry,
        )
        .await;

        let leases_notifier = if self.opts.require_lease_before_startup && !is_standalone {
            Some(Arc::new(Notify::new()))
        } else {
            None
        };

        Ok(Datanode {
            services: ServerHandlers::default(),
            heartbeat_task,
            region_server,
            greptimedb_telemetry_task,
            region_event_receiver,
            leases_notifier,
            plugins: self.plugins.clone(),
        })
    }

    async fn create_heartbeat_task(
        &self,
        region_server: &RegionServer,
        meta_client: MetaClientRef,
        cache_invalidator: CacheInvalidatorRef,
    ) -> Result<HeartbeatTask> {
        let stat = {
            let mut stat = ResourceStatImpl::default();
            stat.start_collect_cpu_usage();
            Arc::new(stat)
        };

        HeartbeatTask::try_new(
            &self.opts,
            region_server.clone(),
            meta_client,
            self.kv_backend.clone(),
            cache_invalidator,
            self.plugins.clone(),
            stat,
        )
        .await
    }

    /// Builds [ObjectStoreManager] from [StorageConfig].
    pub async fn build_object_store_manager(cfg: &StorageConfig) -> Result<ObjectStoreManagerRef> {
        let object_store = store::new_object_store(cfg.store.clone(), &cfg.data_home).await?;
        let default_name = cfg.store.config_name();
        let mut object_store_manager = ObjectStoreManager::new(default_name, object_store);
        for store in &cfg.providers {
            object_store_manager.add(
                store.config_name(),
                store::new_object_store(store.clone(), &cfg.data_home).await?,
            );
        }
        Ok(Arc::new(object_store_manager))
    }

    #[cfg(test)]
    /// Open all regions belong to this datanode.
    async fn initialize_region_server(
        &self,
        region_server: &RegionServer,
        open_with_writable: bool,
    ) -> Result<()> {
        let node_id = self.opts.node_id.context(MissingNodeIdSnafu)?;

        // TODO(weny): Considering introducing a readonly kv_backend trait.
        let runtime_switch_manager = RuntimeSwitchManager::new(self.kv_backend.clone());
        let is_recovery_mode = runtime_switch_manager
            .recovery_mode()
            .await
            .context(GetMetadataSnafu)?;
        let region_open_requests =
            build_region_open_requests(node_id, self.kv_backend.clone()).await?;

        open_all_regions(
            region_server.clone(),
            region_open_requests,
            open_with_writable,
            self.opts.init_regions_parallelism,
            is_recovery_mode,
        )
        .await
    }

    async fn new_region_server(
        &mut self,
        schema_metadata_manager: SchemaMetadataManagerRef,
        event_listener: RegionServerEventListenerRef,
        file_ref_manager: FileReferenceManagerRef,
    ) -> Result<RegionServer> {
        let opts: &DatanodeOptions = &self.opts;

        let query_engine_factory = QueryEngineFactory::new_with_plugins(
            // query engine in datanode only executes plan with resolved table source.
            DummyCatalogManager::arc(),
            None,
            None,
            None,
            None,
            None,
            false,
            self.plugins.clone(),
            opts.query.clone(),
        );
        let query_engine = query_engine_factory.query_engine();

        let table_provider_factory = self
            .table_provider_factory
            .clone()
            .unwrap_or_else(|| Arc::new(DummyTableProviderFactory));

        let mut region_server = RegionServer::with_table_provider(
            query_engine,
            common_runtime::global_runtime(),
            event_listener,
            table_provider_factory,
            opts.max_concurrent_queries,
            //TODO: revaluate the hardcoded timeout on the next version of datanode concurrency limiter.
            Duration::from_millis(100),
            opts.grpc.flight_compression,
        );

        let object_store_manager = Self::build_object_store_manager(&opts.storage).await?;
        let engines = self
            .build_store_engines(
                object_store_manager,
                schema_metadata_manager,
                file_ref_manager,
                self.plugins.clone(),
            )
            .await?;
        for engine in engines {
            region_server.register_engine(engine);
        }
        if let Some(topic_stats_reporter) = self.topic_stats_reporter.take() {
            region_server.set_topic_stats_reporter(topic_stats_reporter);
        }

        Ok(region_server)
    }

    // internal utils

    /// Builds [RegionEngineRef] from `store_engine` section in `opts`
    async fn build_store_engines(
        &mut self,
        object_store_manager: ObjectStoreManagerRef,
        schema_metadata_manager: SchemaMetadataManagerRef,
        file_ref_manager: FileReferenceManagerRef,
        plugins: Plugins,
    ) -> Result<Vec<RegionEngineRef>> {
        let mut metric_engine_config = metric_engine::config::EngineConfig::default();
        let mut mito_engine_config = MitoConfig::default();
        let mut file_engine_config = file_engine::config::EngineConfig::default();

        for engine in &self.opts.region_engine {
            match engine {
                RegionEngineConfig::Mito(config) => {
                    mito_engine_config = config.clone();
                }
                RegionEngineConfig::File(config) => {
                    file_engine_config = config.clone();
                }
                RegionEngineConfig::Metric(metric_config) => {
                    metric_engine_config = metric_config.clone();
                }
            }
        }

        // Build a fetcher to backfill partition_expr on open.
        let fetcher = Arc::new(MetaPartitionExprFetcher::new(self.kv_backend.clone()));
        let mito_engine = self
            .build_mito_engine(
                object_store_manager.clone(),
                mito_engine_config,
                schema_metadata_manager.clone(),
                file_ref_manager.clone(),
                fetcher.clone(),
                plugins.clone(),
            )
            .await?;

        let metric_engine = MetricEngine::try_new(mito_engine.clone(), metric_engine_config)
            .context(BuildMetricEngineSnafu)?;

        let file_engine = FileRegionEngine::new(
            file_engine_config,
            object_store_manager.default_object_store().clone(), // TODO: implement custom storage for file engine
        );

        Ok(vec![
            Arc::new(mito_engine) as _,
            Arc::new(metric_engine) as _,
            Arc::new(file_engine) as _,
        ])
    }

    /// Builds [MitoEngine] according to options.
    async fn build_mito_engine(
        &mut self,
        object_store_manager: ObjectStoreManagerRef,
        mut config: MitoConfig,
        schema_metadata_manager: SchemaMetadataManagerRef,
        file_ref_manager: FileReferenceManagerRef,
        partition_expr_fetcher: PartitionExprFetcherRef,
        plugins: Plugins,
    ) -> Result<MitoEngine> {
        let opts = &self.opts;
        if opts.storage.is_object_storage() {
            // Enable the write cache when setting object storage
            config.enable_write_cache = true;
            info!("Configured 'enable_write_cache=true' for mito engine.");
        }

        let mito_engine = match &opts.wal {
            DatanodeWalConfig::RaftEngine(raft_engine_config) => {
                let log_store =
                    Self::build_raft_engine_log_store(&opts.storage.data_home, raft_engine_config)
                        .await?;

                let builder = MitoEngineBuilder::new(
                    &opts.storage.data_home,
                    config,
                    log_store,
                    object_store_manager,
                    schema_metadata_manager,
                    file_ref_manager,
                    partition_expr_fetcher.clone(),
                    plugins,
                    opts.max_concurrent_queries,
                );

                #[cfg(feature = "enterprise")]
                let builder = builder.with_extension_range_provider_factory(
                    self.extension_range_provider_factory.take(),
                );

                builder.try_build().await.context(BuildMitoEngineSnafu)?
            }
            DatanodeWalConfig::Kafka(kafka_config) => {
                if kafka_config.create_index && opts.node_id.is_none() {
                    warn!("The WAL index creation only available in distributed mode.")
                }
                let global_index_collector = if kafka_config.create_index && opts.node_id.is_some()
                {
                    let operator = new_object_store_without_cache(
                        &opts.storage.store,
                        &opts.storage.data_home,
                    )
                    .await?;
                    let path = default_index_file(opts.node_id.unwrap());
                    Some(Self::build_global_index_collector(
                        kafka_config.dump_index_interval,
                        operator,
                        path,
                    ))
                } else {
                    None
                };

                let log_store =
                    Self::build_kafka_log_store(kafka_config, global_index_collector).await?;
                self.topic_stats_reporter = Some(log_store.topic_stats_reporter());
                let builder = MitoEngineBuilder::new(
                    &opts.storage.data_home,
                    config,
                    log_store,
                    object_store_manager,
                    schema_metadata_manager,
                    file_ref_manager,
                    partition_expr_fetcher,
                    plugins,
                    opts.max_concurrent_queries,
                );

                #[cfg(feature = "enterprise")]
                let builder = builder.with_extension_range_provider_factory(
                    self.extension_range_provider_factory.take(),
                );

                builder.try_build().await.context(BuildMitoEngineSnafu)?
            }
            DatanodeWalConfig::Noop => {
                let log_store = Arc::new(NoopLogStore);

                let builder = MitoEngineBuilder::new(
                    &opts.storage.data_home,
                    config,
                    log_store,
                    object_store_manager,
                    schema_metadata_manager,
                    file_ref_manager,
                    partition_expr_fetcher.clone(),
                    plugins,
                    opts.max_concurrent_queries,
                );

                #[cfg(feature = "enterprise")]
                let builder = builder.with_extension_range_provider_factory(
                    self.extension_range_provider_factory.take(),
                );

                builder.try_build().await.context(BuildMitoEngineSnafu)?
            }
        };
        Ok(mito_engine)
    }

    /// Builds [RaftEngineLogStore].
    async fn build_raft_engine_log_store(
        data_home: &str,
        config: &RaftEngineConfig,
    ) -> Result<Arc<RaftEngineLogStore>> {
        let data_home = normalize_dir(data_home);
        let wal_dir = match &config.dir {
            Some(dir) => dir.clone(),
            None => format!("{}{WAL_DIR}", data_home),
        };

        // create WAL directory
        fs::create_dir_all(Path::new(&wal_dir))
            .await
            .context(CreateDirSnafu { dir: &wal_dir })?;
        info!(
            "Creating raft-engine logstore with config: {:?} and storage path: {}",
            config, &wal_dir
        );
        let logstore = RaftEngineLogStore::try_new(wal_dir, config)
            .await
            .map_err(Box::new)
            .context(OpenLogStoreSnafu)?;

        Ok(Arc::new(logstore))
    }

    /// Builds [`KafkaLogStore`].
    async fn build_kafka_log_store(
        config: &DatanodeKafkaConfig,
        global_index_collector: Option<GlobalIndexCollector>,
    ) -> Result<Arc<KafkaLogStore>> {
        KafkaLogStore::try_new(config, global_index_collector)
            .await
            .map_err(Box::new)
            .context(OpenLogStoreSnafu)
            .map(Arc::new)
    }

    /// Builds [`GlobalIndexCollector`]
    fn build_global_index_collector(
        dump_index_interval: Duration,
        operator: object_store::ObjectStore,
        path: String,
    ) -> GlobalIndexCollector {
        GlobalIndexCollector::new(dump_index_interval, operator, path)
    }
}

/// Open all regions belong to this datanode.
async fn open_all_regions(
    region_server: RegionServer,
    region_open_requests: RegionOpenRequests,
    open_with_writable: bool,
    init_regions_parallelism: usize,
    ignore_nonexistent_region: bool,
) -> Result<()> {
    let RegionOpenRequests {
        leader_regions,
        #[cfg(feature = "enterprise")]
        follower_regions,
    } = region_open_requests;

    let leader_region_num = leader_regions.len();
    info!("going to open {} region(s)", leader_region_num);
    let now = Instant::now();
    let open_regions = region_server
        .handle_batch_open_requests(
            init_regions_parallelism,
            leader_regions,
            ignore_nonexistent_region,
        )
        .await?;
    info!(
        "Opened {} regions in {:?}",
        open_regions.len(),
        now.elapsed()
    );
    if !ignore_nonexistent_region {
        ensure!(
            open_regions.len() == leader_region_num,
            error::UnexpectedSnafu {
                violated: format!(
                    "Expected to open {} of regions, only {} of regions has opened",
                    leader_region_num,
                    open_regions.len()
                )
            }
        );
    } else if open_regions.len() != leader_region_num {
        warn!(
            "ignore nonexistent region, expected to open {} of regions, only {} of regions has opened",
            leader_region_num,
            open_regions.len()
        );
    }

    for region_id in open_regions {
        if open_with_writable {
            let res = region_server.set_region_role(region_id, RegionRole::Leader);
            match res {
                Ok(_) => {
                    // Finalize leadership: persist backfilled metadata.
                    if let SetRegionRoleStateResponse::InvalidTransition(err) = region_server
                        .set_region_role_state_gracefully(
                            region_id,
                            SettableRegionRoleState::Leader,
                        )
                        .await?
                    {
                        error!(err; "failed to convert region {region_id} to leader");
                    }
                }
                Err(e) => {
                    error!(e; "failed to convert region {region_id} to leader");
                }
            }
        }
    }

    #[cfg(feature = "enterprise")]
    if !follower_regions.is_empty() {
        use tokio::time::Instant;

        let follower_region_num = follower_regions.len();
        info!("going to open {} follower region(s)", follower_region_num);

        let now = Instant::now();
        let open_regions = region_server
            .handle_batch_open_requests(
                init_regions_parallelism,
                follower_regions,
                ignore_nonexistent_region,
            )
            .await?;
        info!(
            "Opened {} follower regions in {:?}",
            open_regions.len(),
            now.elapsed()
        );

        if !ignore_nonexistent_region {
            ensure!(
                open_regions.len() == follower_region_num,
                error::UnexpectedSnafu {
                    violated: format!(
                        "Expected to open {} of follower regions, only {} of regions has opened",
                        follower_region_num,
                        open_regions.len()
                    )
                }
            );
        } else if open_regions.len() != follower_region_num {
            warn!(
                "ignore nonexistent region, expected to open {} of follower regions, only {} of regions has opened",
                follower_region_num,
                open_regions.len()
            );
        }
    }

    info!("all regions are opened");

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;
    use std::collections::{BTreeMap, HashMap};
    use std::sync::Arc;

    use cache::build_datanode_cache_registry;
    use common_base::Plugins;
    use common_meta::cache::LayeredCacheRegistryBuilder;
    use common_meta::key::RegionRoleSet;
    use common_meta::key::datanode_table::DatanodeTableManager;
    use common_meta::kv_backend::KvBackendRef;
    use common_meta::kv_backend::memory::MemoryKvBackend;
    use mito2::engine::MITO_ENGINE_NAME;
    use store_api::region_request::RegionRequest;
    use store_api::storage::RegionId;

    use crate::config::DatanodeOptions;
    use crate::datanode::DatanodeBuilder;
    use crate::tests::{MockRegionEngine, mock_region_server};

    async fn setup_table_datanode(kv: &KvBackendRef) {
        let mgr = DatanodeTableManager::new(kv.clone());
        let txn = mgr
            .build_create_txn(
                1028,
                MITO_ENGINE_NAME,
                "foo/bar/weny",
                HashMap::from([("foo".to_string(), "bar".to_string())]),
                HashMap::default(),
                BTreeMap::from([(0, RegionRoleSet::new(vec![0, 1, 2], vec![]))]),
            )
            .unwrap();

        let r = kv.txn(txn).await.unwrap();
        assert!(r.succeeded);
    }

    #[tokio::test]
    async fn test_initialize_region_server() {
        common_telemetry::init_default_ut_logging();
        let mut mock_region_server = mock_region_server();
        let (mock_region, mut mock_region_handler) = MockRegionEngine::new(MITO_ENGINE_NAME);

        mock_region_server.register_engine(mock_region.clone());

        let kv_backend = Arc::new(MemoryKvBackend::new());
        let layered_cache_registry = Arc::new(
            LayeredCacheRegistryBuilder::default()
                .add_cache_registry(build_datanode_cache_registry(kv_backend.clone()))
                .build(),
        );

        let mut builder = DatanodeBuilder::new(
            DatanodeOptions {
                node_id: Some(0),
                ..Default::default()
            },
            Plugins::default(),
            kv_backend.clone(),
        );
        builder.with_cache_registry(layered_cache_registry);
        setup_table_datanode(&(kv_backend as _)).await;

        builder
            .initialize_region_server(&mock_region_server, false)
            .await
            .unwrap();

        for i in 0..3 {
            let (region_id, req) = mock_region_handler.recv().await.unwrap();
            assert_eq!(region_id, RegionId::new(1028, i));
            if let RegionRequest::Open(req) = req {
                assert_eq!(
                    req.options,
                    HashMap::from([("foo".to_string(), "bar".to_string())])
                )
            } else {
                unreachable!()
            }
        }

        assert_matches!(
            mock_region_handler.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        );
    }
}
