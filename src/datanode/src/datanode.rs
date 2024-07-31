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

use catalog::memory::MemoryCatalogManager;
use common_base::Plugins;
use common_error::ext::BoxedError;
use common_greptimedb_telemetry::GreptimeDBTelemetryTask;
use common_meta::key::datanode_table::{DatanodeTableManager, DatanodeTableValue};
use common_meta::kv_backend::KvBackendRef;
use common_meta::wal_options_allocator::prepare_wal_options;
pub use common_procedure::options::ProcedureConfig;
use common_telemetry::{error, info, warn};
use common_wal::config::kafka::DatanodeKafkaConfig;
use common_wal::config::raft_engine::RaftEngineConfig;
use common_wal::config::DatanodeWalConfig;
use file_engine::engine::FileRegionEngine;
use futures_util::TryStreamExt;
use log_store::kafka::log_store::KafkaLogStore;
use log_store::raft_engine::log_store::RaftEngineLogStore;
use meta_client::MetaClientRef;
use metric_engine::engine::MetricEngine;
use mito2::config::MitoConfig;
use mito2::engine::MitoEngine;
use object_store::manager::{ObjectStoreManager, ObjectStoreManagerRef};
use object_store::util::normalize_dir;
use query::QueryEngineFactory;
use servers::export_metrics::ExportMetricsTask;
use servers::server::ServerHandlers;
use servers::Mode;
use snafu::{ensure, OptionExt, ResultExt};
use store_api::path_utils::{region_dir, WAL_DIR};
use store_api::region_engine::RegionEngineRef;
use store_api::region_request::RegionOpenRequest;
use store_api::storage::RegionId;
use tokio::fs;
use tokio::sync::Notify;

use crate::config::{DatanodeOptions, RegionEngineConfig, StorageConfig};
use crate::error::{
    self, BuildMitoEngineSnafu, CreateDirSnafu, GetMetadataSnafu, MissingKvBackendSnafu,
    MissingNodeIdSnafu, OpenLogStoreSnafu, Result, ShutdownInstanceSnafu, ShutdownServerSnafu,
    StartServerSnafu,
};
use crate::event_listener::{
    new_region_server_event_channel, NoopRegionServerEventListener, RegionServerEventListenerRef,
    RegionServerEventReceiver,
};
use crate::greptimedb_telemetry::get_greptimedb_telemetry_task;
use crate::heartbeat::HeartbeatTask;
use crate::region_server::{DummyTableProviderFactory, RegionServer};
use crate::store;

/// Datanode service.
pub struct Datanode {
    services: ServerHandlers,
    heartbeat_task: Option<HeartbeatTask>,
    region_event_receiver: Option<RegionServerEventReceiver>,
    region_server: RegionServer,
    greptimedb_telemetry_task: Arc<GreptimeDBTelemetryTask>,
    leases_notifier: Option<Arc<Notify>>,
    plugins: Plugins,
    export_metrics_task: Option<ExportMetricsTask>,
}

impl Datanode {
    pub async fn start(&mut self) -> Result<()> {
        info!("Starting datanode instance...");

        self.start_heartbeat().await?;
        self.wait_coordinated().await;

        self.start_telemetry();

        if let Some(t) = self.export_metrics_task.as_ref() {
            t.start(None).context(StartServerSnafu)?
        }

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

    pub async fn shutdown(&self) -> Result<()> {
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
    plugins: Plugins,
    meta_client: Option<MetaClientRef>,
    kv_backend: Option<KvBackendRef>,
}

impl DatanodeBuilder {
    /// `kv_backend` is optional. If absent, the builder will try to build one
    /// by using the given `opts`
    pub fn new(opts: DatanodeOptions, plugins: Plugins) -> Self {
        Self {
            opts,
            plugins,
            meta_client: None,
            kv_backend: None,
        }
    }

    pub fn with_meta_client(self, meta_client: MetaClientRef) -> Self {
        Self {
            meta_client: Some(meta_client),
            ..self
        }
    }

    pub fn with_kv_backend(self, kv_backend: KvBackendRef) -> Self {
        Self {
            kv_backend: Some(kv_backend),
            ..self
        }
    }

    pub async fn build(mut self) -> Result<Datanode> {
        let mode = &self.opts.mode;
        let node_id = self.opts.node_id.context(MissingNodeIdSnafu)?;

        let meta_client = self.meta_client.take();

        // If metasrv client is provided, we will use it to control the region server.
        // Otherwise the region server is self-controlled, meaning no heartbeat and immediately
        // writable upon open.
        let controlled_by_metasrv = meta_client.is_some();

        let kv_backend = self.kv_backend.take().context(MissingKvBackendSnafu)?;

        // build and initialize region server
        let (region_event_listener, region_event_receiver) = if controlled_by_metasrv {
            let (tx, rx) = new_region_server_event_channel();
            (Box::new(tx) as _, Some(rx))
        } else {
            (Box::new(NoopRegionServerEventListener) as _, None)
        };

        let region_server = self.new_region_server(region_event_listener).await?;

        let datanode_table_manager = DatanodeTableManager::new(kv_backend.clone());
        let table_values = datanode_table_manager
            .tables(node_id)
            .try_collect::<Vec<_>>()
            .await
            .context(GetMetadataSnafu)?;

        let open_all_regions = open_all_regions(
            region_server.clone(),
            table_values,
            !controlled_by_metasrv,
            self.opts.init_regions_parallelism,
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
            Some(HeartbeatTask::try_new(&self.opts, region_server.clone(), meta_client).await?)
        } else {
            None
        };

        let greptimedb_telemetry_task = get_greptimedb_telemetry_task(
            Some(self.opts.storage.data_home.clone()),
            mode,
            self.opts.enable_telemetry,
        )
        .await;

        let leases_notifier =
            if self.opts.require_lease_before_startup && matches!(mode, Mode::Distributed) {
                Some(Arc::new(Notify::new()))
            } else {
                None
            };

        let export_metrics_task =
            ExportMetricsTask::try_new(&self.opts.export_metrics, Some(&self.plugins))
                .context(StartServerSnafu)?;

        Ok(Datanode {
            services: ServerHandlers::default(),
            heartbeat_task,
            region_server,
            greptimedb_telemetry_task,
            region_event_receiver,
            leases_notifier,
            plugins: self.plugins.clone(),
            export_metrics_task,
        })
    }

    /// Builds [ObjectStoreManager] from [StorageConfig].
    pub async fn build_object_store_manager(cfg: &StorageConfig) -> Result<ObjectStoreManagerRef> {
        let object_store = store::new_object_store(cfg.store.clone(), &cfg.data_home).await?;
        let default_name = cfg.store.name();
        let mut object_store_manager = ObjectStoreManager::new(default_name, object_store);
        for store in &cfg.providers {
            object_store_manager.add(
                store.name(),
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
        kv_backend: KvBackendRef,
        open_with_writable: bool,
    ) -> Result<()> {
        let node_id = self.opts.node_id.context(MissingNodeIdSnafu)?;

        let datanode_table_manager = DatanodeTableManager::new(kv_backend.clone());
        let table_values = datanode_table_manager
            .tables(node_id)
            .try_collect::<Vec<_>>()
            .await
            .context(GetMetadataSnafu)?;

        open_all_regions(
            region_server.clone(),
            table_values,
            open_with_writable,
            self.opts.init_regions_parallelism,
        )
        .await
    }

    async fn new_region_server(
        &self,
        event_listener: RegionServerEventListenerRef,
    ) -> Result<RegionServer> {
        let opts = &self.opts;

        let query_engine_factory = QueryEngineFactory::new_with_plugins(
            // query engine in datanode only executes plan with resolved table source.
            MemoryCatalogManager::with_default_setup(),
            None,
            None,
            None,
            None,
            false,
            self.plugins.clone(),
        );
        let query_engine = query_engine_factory.query_engine();

        let table_provider_factory = Arc::new(DummyTableProviderFactory);
        let mut region_server = RegionServer::with_table_provider(
            query_engine,
            common_runtime::global_runtime(),
            event_listener,
            table_provider_factory,
        );

        let object_store_manager = Self::build_object_store_manager(&opts.storage).await?;
        let engines =
            Self::build_store_engines(opts, object_store_manager, self.plugins.clone()).await?;
        for engine in engines {
            region_server.register_engine(engine);
        }

        Ok(region_server)
    }

    // internal utils

    /// Builds [RegionEngineRef] from `store_engine` section in `opts`
    async fn build_store_engines(
        opts: &DatanodeOptions,
        object_store_manager: ObjectStoreManagerRef,
        plugins: Plugins,
    ) -> Result<Vec<RegionEngineRef>> {
        let mut engines = vec![];
        for engine in &opts.region_engine {
            match engine {
                RegionEngineConfig::Mito(config) => {
                    let mito_engine = Self::build_mito_engine(
                        opts,
                        object_store_manager.clone(),
                        config.clone(),
                        plugins.clone(),
                    )
                    .await?;

                    let metric_engine = MetricEngine::new(mito_engine.clone());
                    engines.push(Arc::new(mito_engine) as _);
                    engines.push(Arc::new(metric_engine) as _);
                }
                RegionEngineConfig::File(config) => {
                    let engine = FileRegionEngine::new(
                        config.clone(),
                        object_store_manager.default_object_store().clone(), // TODO: implement custom storage for file engine
                    );
                    engines.push(Arc::new(engine) as _);
                }
            }
        }
        Ok(engines)
    }

    /// Builds [MitoEngine] according to options.
    async fn build_mito_engine(
        opts: &DatanodeOptions,
        object_store_manager: ObjectStoreManagerRef,
        config: MitoConfig,
        plugins: Plugins,
    ) -> Result<MitoEngine> {
        let mito_engine = match &opts.wal {
            DatanodeWalConfig::RaftEngine(raft_engine_config) => MitoEngine::new(
                &opts.storage.data_home,
                config,
                Self::build_raft_engine_log_store(&opts.storage.data_home, raft_engine_config)
                    .await?,
                object_store_manager,
                plugins,
            )
            .await
            .context(BuildMitoEngineSnafu)?,
            DatanodeWalConfig::Kafka(kafka_config) => MitoEngine::new(
                &opts.storage.data_home,
                config,
                Self::build_kafka_log_store(kafka_config).await?,
                object_store_manager,
                plugins,
            )
            .await
            .context(BuildMitoEngineSnafu)?,
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
        let logstore = RaftEngineLogStore::try_new(wal_dir, config.clone())
            .await
            .map_err(Box::new)
            .context(OpenLogStoreSnafu)?;

        Ok(Arc::new(logstore))
    }

    /// Builds [KafkaLogStore].
    async fn build_kafka_log_store(config: &DatanodeKafkaConfig) -> Result<Arc<KafkaLogStore>> {
        KafkaLogStore::try_new(config)
            .await
            .map_err(Box::new)
            .context(OpenLogStoreSnafu)
            .map(Arc::new)
    }
}

/// Open all regions belong to this datanode.
async fn open_all_regions(
    region_server: RegionServer,
    table_values: Vec<DatanodeTableValue>,
    open_with_writable: bool,
    init_regions_parallelism: usize,
) -> Result<()> {
    let mut regions = vec![];
    for table_value in table_values {
        for region_number in table_value.regions {
            // Augments region options with wal options if a wal options is provided.
            let mut region_options = table_value.region_info.region_options.clone();
            prepare_wal_options(
                &mut region_options,
                RegionId::new(table_value.table_id, region_number),
                &table_value.region_info.region_wal_options,
            );

            regions.push((
                RegionId::new(table_value.table_id, region_number),
                table_value.region_info.engine.clone(),
                table_value.region_info.region_storage_path.clone(),
                region_options,
            ));
        }
    }
    let num_regions = regions.len();
    info!("going to open {} region(s)", num_regions);

    let mut region_requests = Vec::with_capacity(regions.len());
    for (region_id, engine, store_path, options) in regions {
        let region_dir = region_dir(&store_path, region_id);
        region_requests.push((
            region_id,
            RegionOpenRequest {
                engine,
                region_dir,
                options,
                skip_wal_replay: false,
            },
        ));
    }

    let open_regions = region_server
        .handle_batch_open_requests(init_regions_parallelism, region_requests)
        .await?;
    ensure!(
        open_regions.len() == num_regions,
        error::UnexpectedSnafu {
            violated: format!(
                "Expected to open {} of regions, only {} of regions has opened",
                num_regions,
                open_regions.len()
            )
        }
    );

    for region_id in open_regions {
        if open_with_writable {
            if let Err(e) = region_server.set_writable(region_id, true) {
                error!(
                    e; "failed to set writable for region {region_id}"
                );
            }
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

    use common_base::Plugins;
    use common_meta::key::datanode_table::DatanodeTableManager;
    use common_meta::kv_backend::memory::MemoryKvBackend;
    use common_meta::kv_backend::KvBackendRef;
    use mito2::engine::MITO_ENGINE_NAME;
    use store_api::region_request::RegionRequest;
    use store_api::storage::RegionId;

    use crate::config::DatanodeOptions;
    use crate::datanode::DatanodeBuilder;
    use crate::tests::{mock_region_server, MockRegionEngine};

    async fn setup_table_datanode(kv: &KvBackendRef) {
        let mgr = DatanodeTableManager::new(kv.clone());
        let txn = mgr
            .build_create_txn(
                1028,
                MITO_ENGINE_NAME,
                "foo/bar/weny",
                HashMap::from([("foo".to_string(), "bar".to_string())]),
                HashMap::default(),
                BTreeMap::from([(0, vec![0, 1, 2])]),
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

        let builder = DatanodeBuilder::new(
            DatanodeOptions {
                node_id: Some(0),
                ..Default::default()
            },
            Plugins::default(),
        );

        let kv = Arc::new(MemoryKvBackend::default()) as _;
        setup_table_datanode(&kv).await;

        builder
            .initialize_region_server(&mock_region_server, kv.clone(), false)
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
