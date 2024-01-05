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

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use catalog::memory::MemoryCatalogManager;
use common_base::Plugins;
use common_config::wal::{KafkaConfig, RaftEngineConfig};
use common_config::WalConfig;
use common_error::ext::BoxedError;
use common_greptimedb_telemetry::GreptimeDBTelemetryTask;
use common_meta::key::datanode_table::{DatanodeTableManager, DatanodeTableValue};
use common_meta::kv_backend::KvBackendRef;
use common_meta::wal::prepare_wal_option;
pub use common_procedure::options::ProcedureConfig;
use common_runtime::Runtime;
use common_telemetry::{error, info, warn};
use file_engine::engine::FileRegionEngine;
use futures::future;
use futures_util::future::try_join_all;
use futures_util::TryStreamExt;
use log_store::kafka::log_store::KafkaLogStore;
use log_store::raft_engine::log_store::RaftEngineLogStore;
use meta_client::client::MetaClient;
use metric_engine::engine::MetricEngine;
use mito2::config::MitoConfig;
use mito2::engine::MitoEngine;
use object_store::manager::{ObjectStoreManager, ObjectStoreManagerRef};
use object_store::util::normalize_dir;
use query::QueryEngineFactory;
use servers::export_metrics::ExportMetricsTask;
use servers::grpc::{GrpcServer, GrpcServerConfig};
use servers::http::HttpServerBuilder;
use servers::metrics_handler::MetricsHandler;
use servers::server::{start_server, ServerHandler, ServerHandlers};
use servers::Mode;
use snafu::{OptionExt, ResultExt};
use store_api::path_utils::{region_dir, WAL_DIR};
use store_api::region_engine::RegionEngineRef;
use store_api::region_request::{RegionOpenRequest, RegionRequest};
use store_api::storage::RegionId;
use tokio::fs;
use tokio::sync::Notify;

use crate::config::{DatanodeOptions, RegionEngineConfig};
use crate::error::{
    CreateDirSnafu, GetMetadataSnafu, MissingKvBackendSnafu, MissingNodeIdSnafu, OpenLogStoreSnafu,
    ParseAddrSnafu, Result, RuntimeResourceSnafu, ShutdownInstanceSnafu, ShutdownServerSnafu,
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

const OPEN_REGION_PARALLELISM: usize = 16;
const REGION_SERVER_SERVICE_NAME: &str = "REGION_SERVER_SERVICE";
const DATANODE_HTTP_SERVICE_NAME: &str = "DATANODE_HTTP_SERVICE";

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

        self.start_services().await
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

    /// Start services of datanode. This method call will block until services are shutdown.
    pub async fn start_services(&mut self) -> Result<()> {
        let _ = future::try_join_all(self.services.values().map(start_server))
            .await
            .context(StartServerSnafu)?;
        Ok(())
    }

    async fn shutdown_services(&self) -> Result<()> {
        let _ = future::try_join_all(self.services.values().map(|server| server.0.shutdown()))
            .await
            .context(ShutdownServerSnafu)?;
        Ok(())
    }

    pub async fn shutdown(&self) -> Result<()> {
        // We must shutdown services first
        self.shutdown_services().await?;
        let _ = self.greptimedb_telemetry_task.stop().await;
        if let Some(heartbeat_task) = &self.heartbeat_task {
            heartbeat_task
                .close()
                .await
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
    meta_client: Option<MetaClient>,
    kv_backend: Option<KvBackendRef>,
    enable_region_server_service: bool,
    enable_http_service: bool,
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
            enable_region_server_service: false,
            enable_http_service: false,
        }
    }

    pub fn with_meta_client(self, meta_client: MetaClient) -> Self {
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

    pub fn enable_region_server_service(self) -> Self {
        Self {
            enable_region_server_service: true,
            ..self
        }
    }

    pub fn enable_http_service(self) -> Self {
        Self {
            enable_http_service: true,
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

        let open_all_regions =
            open_all_regions(region_server.clone(), table_values, !controlled_by_metasrv);

        if self.opts.initialize_region_in_background {
            // Opens regions in background.
            common_runtime::spawn_bg(async move {
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

        let services = self.create_datanode_services(&region_server)?;

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
            services,
            heartbeat_task,
            region_server,
            greptimedb_telemetry_task,
            region_event_receiver,
            leases_notifier,
            plugins: self.plugins.clone(),
            export_metrics_task,
        })
    }

    fn create_datanode_services(&self, region_server: &RegionServer) -> Result<ServerHandlers> {
        let mut services = HashMap::new();

        if self.enable_region_server_service {
            services.insert(
                REGION_SERVER_SERVICE_NAME.to_string(),
                self.create_region_server_service(region_server)?,
            );
        }

        if self.enable_http_service {
            services.insert(
                DATANODE_HTTP_SERVICE_NAME.to_string(),
                self.create_http_service()?,
            );
        }

        Ok(services)
    }

    fn create_region_server_service(&self, region_server: &RegionServer) -> Result<ServerHandler> {
        let opts = &self.opts;

        let config = GrpcServerConfig {
            max_recv_message_size: opts.rpc_max_recv_message_size.as_bytes() as usize,
            max_send_message_size: opts.rpc_max_send_message_size.as_bytes() as usize,
        };

        let server = Box::new(GrpcServer::new(
            Some(config),
            None,
            None,
            Some(Arc::new(region_server.clone()) as _),
            Some(Arc::new(region_server.clone()) as _),
            None,
            region_server.runtime(),
        ));

        let addr: SocketAddr = opts.rpc_addr.parse().context(ParseAddrSnafu {
            addr: &opts.rpc_addr,
        })?;

        Ok((server, addr))
    }

    fn create_http_service(&self) -> Result<ServerHandler> {
        let opts = &self.opts;

        let server = Box::new(
            HttpServerBuilder::new(opts.http.clone())
                .with_metrics_handler(MetricsHandler)
                .with_greptime_config_options(opts.to_toml_string())
                .build(),
        );

        let addr = opts.http.addr.parse().context(ParseAddrSnafu {
            addr: &opts.http.addr,
        })?;

        Ok((server, addr))
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

        open_all_regions(region_server.clone(), table_values, open_with_writable).await
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
            false,
            self.plugins.clone(),
        );
        let query_engine = query_engine_factory.query_engine();

        let runtime = Arc::new(
            Runtime::builder()
                .worker_threads(opts.rpc_runtime_size)
                .thread_name("io-handlers")
                .build()
                .context(RuntimeResourceSnafu)?,
        );

        let table_provider_factory = Arc::new(DummyTableProviderFactory);
        let mut region_server = RegionServer::with_table_provider(
            query_engine,
            runtime,
            event_listener,
            table_provider_factory,
        );

        let object_store_manager = Self::build_object_store_manager(opts).await?;
        let engines = Self::build_store_engines(opts, object_store_manager).await?;
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
    ) -> Result<Vec<RegionEngineRef>> {
        let mut engines = vec![];
        for engine in &opts.region_engine {
            match engine {
                RegionEngineConfig::Mito(config) => {
                    let mito_engine =
                        Self::build_mito_engine(opts, object_store_manager.clone(), config.clone())
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
    ) -> Result<MitoEngine> {
        let mito_engine = match &opts.wal {
            WalConfig::RaftEngine(raft_engine_config) => MitoEngine::new(
                config,
                Self::build_raft_engine_log_store(&opts.storage.data_home, raft_engine_config)
                    .await?,
                object_store_manager,
            ),
            WalConfig::Kafka(kafka_config) => MitoEngine::new(
                config,
                Self::build_kafka_log_store(kafka_config).await?,
                object_store_manager,
            ),
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
    async fn build_kafka_log_store(config: &KafkaConfig) -> Result<Arc<KafkaLogStore>> {
        KafkaLogStore::try_new(config)
            .await
            .map_err(Box::new)
            .context(OpenLogStoreSnafu)
            .map(Arc::new)
    }

    /// Builds [ObjectStoreManager]
    async fn build_object_store_manager(opts: &DatanodeOptions) -> Result<ObjectStoreManagerRef> {
        let object_store =
            store::new_object_store(opts.storage.store.clone(), &opts.storage.data_home).await?;
        let default_name = opts.storage.store.name();
        let mut object_store_manager = ObjectStoreManager::new(default_name, object_store);
        for store in &opts.storage.providers {
            object_store_manager.add(
                store.name(),
                store::new_object_store(store.clone(), &opts.storage.data_home).await?,
            );
        }
        Ok(Arc::new(object_store_manager))
    }
}

/// Open all regions belong to this datanode.
async fn open_all_regions(
    region_server: RegionServer,
    table_values: Vec<DatanodeTableValue>,
    open_with_writable: bool,
) -> Result<()> {
    let mut regions = vec![];
    for table_value in table_values {
        for region_number in table_value.regions {
            // Augments region options with wal options if a wal options is provided.
            let mut region_options = table_value.region_info.region_options.clone();
            prepare_wal_option(
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
    info!("going to open {} regions", regions.len());
    let semaphore = Arc::new(tokio::sync::Semaphore::new(OPEN_REGION_PARALLELISM));
    let mut tasks = vec![];

    let region_server_ref = &region_server;
    for (region_id, engine, store_path, options) in regions {
        let region_dir = region_dir(&store_path, region_id);
        let semaphore_moved = semaphore.clone();

        tasks.push(async move {
            let _permit = semaphore_moved.acquire().await;
            region_server_ref
                .handle_request(
                    region_id,
                    RegionRequest::Open(RegionOpenRequest {
                        engine: engine.clone(),
                        region_dir,
                        options,
                        skip_wal_replay: false,
                    }),
                )
                .await?;
            if open_with_writable {
                if let Err(e) = region_server_ref.set_writable(region_id, true) {
                    error!(
                        e; "failed to set writable for region {region_id}"
                    );
                }
            }
            Ok(())
        });
    }
    let _ = try_join_all(tasks).await?;

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
                "mock",
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
        let mut mock_region_server = mock_region_server();
        let (mock_region, mut mock_region_handler) = MockRegionEngine::new();

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
