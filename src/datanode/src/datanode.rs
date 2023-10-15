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

use catalog::kvbackend::MetaKvBackend;
use catalog::memory::MemoryCatalogManager;
use common_base::Plugins;
use common_error::ext::BoxedError;
use common_greptimedb_telemetry::GreptimeDBTelemetryTask;
use common_meta::key::datanode_table::DatanodeTableManager;
use common_meta::kv_backend::KvBackendRef;
pub use common_procedure::options::ProcedureConfig;
use common_runtime::Runtime;
use common_telemetry::{error, info};
use file_engine::engine::FileRegionEngine;
use futures_util::future::try_join_all;
use futures_util::StreamExt;
use log_store::raft_engine::log_store::RaftEngineLogStore;
use meta_client::client::MetaClient;
use mito2::engine::MitoEngine;
use object_store::util::normalize_dir;
use query::QueryEngineFactory;
use servers::Mode;
use snafu::{OptionExt, ResultExt};
use store_api::logstore::LogStore;
use store_api::path_utils::{region_dir, WAL_DIR};
use store_api::region_engine::RegionEngineRef;
use store_api::region_request::{RegionOpenRequest, RegionRequest};
use store_api::storage::RegionId;
use tokio::fs;
use tokio::sync::Notify;

use crate::config::{DatanodeOptions, RegionEngineConfig};
use crate::error::{
    CreateDirSnafu, GetMetadataSnafu, MissingKvBackendSnafu, MissingMetaClientSnafu,
    MissingMetasrvOptsSnafu, MissingNodeIdSnafu, OpenLogStoreSnafu, Result, RuntimeResourceSnafu,
    ShutdownInstanceSnafu,
};
use crate::event_listener::{
    new_region_server_event_channel, NoopRegionServerEventListener, RegionServerEventListenerRef,
    RegionServerEventReceiver,
};
use crate::greptimedb_telemetry::get_greptimedb_telemetry_task;
use crate::heartbeat::{new_metasrv_client, HeartbeatTask};
use crate::region_server::RegionServer;
use crate::server::Services;
use crate::store;

const OPEN_REGION_PARALLELISM: usize = 16;

/// Datanode service.
pub struct Datanode {
    opts: DatanodeOptions,
    services: Option<Services>,
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

        let _ = self.greptimedb_telemetry_task.start();
        self.start_services().await
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
        if let Some(service) = self.services.as_mut() {
            service.start(&self.opts).await
        } else {
            Ok(())
        }
    }

    async fn shutdown_services(&self) -> Result<()> {
        if let Some(service) = self.services.as_ref() {
            service.shutdown().await
        } else {
            Ok(())
        }
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
}

impl DatanodeBuilder {
    /// `kv_backend` is optional. If absent, the builder will try to build one
    /// by using the given `opts`
    pub fn new(opts: DatanodeOptions, kv_backend: Option<KvBackendRef>, plugins: Plugins) -> Self {
        Self {
            opts,
            plugins,
            meta_client: None,
            kv_backend,
        }
    }

    pub fn with_meta_client(self, meta_client: MetaClient) -> Self {
        Self {
            meta_client: Some(meta_client),
            ..self
        }
    }

    pub async fn build(mut self) -> Result<Datanode> {
        let mode = &self.opts.mode;

        // build meta client
        let meta_client = match mode {
            Mode::Distributed => {
                let meta_client = if let Some(meta_client) = self.meta_client.take() {
                    meta_client
                } else {
                    let node_id = self.opts.node_id.context(MissingNodeIdSnafu)?;

                    let meta_config = self
                        .opts
                        .meta_client
                        .as_ref()
                        .context(MissingMetasrvOptsSnafu)?;

                    new_metasrv_client(node_id, meta_config).await?
                };
                Some(meta_client)
            }
            Mode::Standalone => None,
        };

        // build kv-backend
        let kv_backend = match mode {
            Mode::Distributed => Arc::new(MetaKvBackend {
                client: Arc::new(meta_client.clone().context(MissingMetaClientSnafu)?),
            }),
            Mode::Standalone => self.kv_backend.clone().context(MissingKvBackendSnafu)?,
        };

        // build and initialize region server
        let log_store = Self::build_log_store(&self.opts).await?;
        let (region_event_listener, region_event_receiver) = match mode {
            Mode::Distributed => {
                let (tx, rx) = new_region_server_event_channel();
                (Box::new(tx) as RegionServerEventListenerRef, Some(rx))
            }
            Mode::Standalone => (
                Box::new(NoopRegionServerEventListener) as RegionServerEventListenerRef,
                None,
            ),
        };

        let region_server = Self::new_region_server(
            &self.opts,
            self.plugins.clone(),
            log_store,
            region_event_listener,
        )
        .await?;
        self.initialize_region_server(&region_server, kv_backend, matches!(mode, Mode::Standalone))
            .await?;

        let heartbeat_task = match mode {
            Mode::Distributed => {
                let meta_client = meta_client.context(MissingMetaClientSnafu)?;

                let heartbeat_task =
                    HeartbeatTask::try_new(&self.opts, region_server.clone(), meta_client).await?;
                Some(heartbeat_task)
            }
            Mode::Standalone => None,
        };

        let services = match mode {
            Mode::Distributed => Some(Services::try_new(region_server.clone(), &self.opts).await?),
            Mode::Standalone => None,
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

        Ok(Datanode {
            opts: self.opts,
            services,
            heartbeat_task,
            region_server,
            greptimedb_telemetry_task,
            region_event_receiver,
            leases_notifier,
            plugins: self.plugins.clone(),
        })
    }

    /// Open all regions belong to this datanode.
    async fn initialize_region_server(
        &self,
        region_server: &RegionServer,
        kv_backend: KvBackendRef,
        open_with_writable: bool,
    ) -> Result<()> {
        let datanode_table_manager = DatanodeTableManager::new(kv_backend.clone());
        let node_id = self.opts.node_id.context(MissingNodeIdSnafu)?;
        let mut regions = vec![];
        let mut table_values = datanode_table_manager.tables(node_id);

        while let Some(table_value) = table_values.next().await {
            let table_value = table_value.context(GetMetadataSnafu)?;
            for region_number in table_value.regions {
                regions.push((
                    RegionId::new(table_value.table_id, region_number),
                    table_value.region_info.engine.clone(),
                    table_value.region_info.region_storage_path.clone(),
                    table_value.region_info.region_options.clone(),
                ));
            }
        }

        info!("going to open {} regions", regions.len());
        let semaphore = Arc::new(tokio::sync::Semaphore::new(OPEN_REGION_PARALLELISM));
        let mut tasks = vec![];

        for (region_id, engine, store_path, options) in regions {
            let region_dir = region_dir(&store_path, region_id);
            let semaphore_moved = semaphore.clone();
            tasks.push(async move {
                let _permit = semaphore_moved.acquire().await;
                region_server
                    .handle_request(
                        region_id,
                        RegionRequest::Open(RegionOpenRequest {
                            engine: engine.clone(),
                            region_dir,
                            options,
                        }),
                    )
                    .await?;
                if open_with_writable {
                    if let Err(e) = region_server.set_writable(region_id, true) {
                        error!(
                            e; "failed to set writable for region {region_id}"
                        );
                    }
                }
                Ok(())
            });
        }
        let _ = try_join_all(tasks).await?;

        info!("region server is initialized");

        Ok(())
    }

    async fn new_region_server(
        opts: &DatanodeOptions,
        plugins: Plugins,
        log_store: Arc<RaftEngineLogStore>,
        event_listener: RegionServerEventListenerRef,
    ) -> Result<RegionServer> {
        let query_engine_factory = QueryEngineFactory::new_with_plugins(
            // query engine in datanode only executes plan with resolved table source.
            MemoryCatalogManager::with_default_setup(),
            None,
            None,
            false,
            plugins,
        );
        let query_engine = query_engine_factory.query_engine();

        let runtime = Arc::new(
            Runtime::builder()
                .worker_threads(opts.rpc_runtime_size)
                .thread_name("io-handlers")
                .build()
                .context(RuntimeResourceSnafu)?,
        );

        let mut region_server =
            RegionServer::new(query_engine.clone(), runtime.clone(), event_listener);
        let object_store = store::new_object_store(opts).await?;
        let engines = Self::build_store_engines(opts, log_store, object_store).await?;
        for engine in engines {
            region_server.register_engine(engine);
        }

        Ok(region_server)
    }

    // internal utils

    /// Build [RaftEngineLogStore]
    async fn build_log_store(opts: &DatanodeOptions) -> Result<Arc<RaftEngineLogStore>> {
        let data_home = normalize_dir(&opts.storage.data_home);
        let wal_dir = match &opts.wal.dir {
            Some(dir) => dir.clone(),
            None => format!("{}{WAL_DIR}", data_home),
        };
        let wal_config = opts.wal.clone();

        // create WAL directory
        fs::create_dir_all(Path::new(&wal_dir))
            .await
            .context(CreateDirSnafu { dir: &wal_dir })?;
        info!(
            "Creating logstore with config: {:?} and storage path: {}",
            wal_config, &wal_dir
        );
        let logstore = RaftEngineLogStore::try_new(wal_dir, wal_config)
            .await
            .map_err(Box::new)
            .context(OpenLogStoreSnafu)?;
        Ok(Arc::new(logstore))
    }

    /// Build [RegionEngineRef] from `store_engine` section in `opts`
    async fn build_store_engines<S>(
        opts: &DatanodeOptions,
        log_store: Arc<S>,
        object_store: object_store::ObjectStore,
    ) -> Result<Vec<RegionEngineRef>>
    where
        S: LogStore,
    {
        let mut engines = vec![];
        for engine in &opts.region_engine {
            match engine {
                RegionEngineConfig::Mito(config) => {
                    let engine: MitoEngine =
                        MitoEngine::new(config.clone(), log_store.clone(), object_store.clone());
                    engines.push(Arc::new(engine) as _);
                }
                RegionEngineConfig::File(config) => {
                    let engine = FileRegionEngine::new(config.clone(), object_store.clone());
                    engines.push(Arc::new(engine) as _);
                }
            }
        }
        Ok(engines)
    }
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
            None,
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
