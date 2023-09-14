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

use catalog::local::MemoryCatalogManager;
use common_base::readable_size::ReadableSize;
use common_base::Plugins;
use common_error::ext::BoxedError;
use common_greptimedb_telemetry::GreptimeDBTelemetryTask;
pub use common_procedure::options::ProcedureConfig;
use common_runtime::Runtime;
use common_telemetry::info;
use log_store::raft_engine::log_store::RaftEngineLogStore;
use meta_client::client::MetaClient;
use mito2::engine::MitoEngine;
use object_store::util::normalize_dir;
use query::QueryEngineFactory;
use servers::Mode;
use snafu::{OptionExt, ResultExt};
use store_api::logstore::LogStore;
use store_api::path_utils::WAL_DIR;
use store_api::region_engine::RegionEngineRef;
use tokio::fs;

use crate::config::{DatanodeOptions, RegionEngineConfig};
use crate::error::{
    CreateDirSnafu, MissingMetasrvOptsSnafu, MissingNodeIdSnafu, OpenLogStoreSnafu, Result,
    RuntimeResourceSnafu, ShutdownInstanceSnafu,
};
use crate::greptimedb_telemetry::get_greptimedb_telemetry_task;
use crate::heartbeat::{new_metasrv_client, HeartbeatTask};
use crate::region_server::RegionServer;
use crate::server::Services;
use crate::store;

pub const DEFAULT_OBJECT_STORE_CACHE_SIZE: ReadableSize = ReadableSize(1024);

/// Datanode service.
pub struct Datanode {
    opts: DatanodeOptions,
    services: Option<Services>,
    heartbeat_task: Option<HeartbeatTask>,
    region_server: RegionServer,
    greptimedb_telemetry_task: Arc<GreptimeDBTelemetryTask>,
}

impl Datanode {
    async fn new_region_server(
        opts: &DatanodeOptions,
        plugins: Arc<Plugins>,
    ) -> Result<RegionServer> {
        let query_engine_factory = QueryEngineFactory::new_with_plugins(
            // query engine in datanode only executes plan with resolved table source.
            MemoryCatalogManager::with_default_setup(),
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

        let mut region_server = RegionServer::new(query_engine.clone(), runtime.clone());
        let log_store = Self::build_log_store(opts).await?;
        let object_store = store::new_object_store(opts).await?;
        let engines = Self::build_store_engines(opts, log_store, object_store).await?;
        for engine in engines {
            region_server.register_engine(engine);
        }

        Ok(region_server)
    }

    pub async fn start(&mut self) -> Result<()> {
        info!("Starting datanode instance...");

        self.start_heartbeat().await?;

        let _ = self.greptimedb_telemetry_task.start();
        self.start_services().await
    }

    pub async fn start_heartbeat(&self) -> Result<()> {
        if let Some(task) = &self.heartbeat_task {
            task.start().await?;
        }
        Ok(())
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

    // internal utils

    /// Build [RaftEngineLogStore]
    async fn build_log_store(opts: &DatanodeOptions) -> Result<Arc<RaftEngineLogStore>> {
        let data_home = normalize_dir(&opts.storage.data_home);
        let wal_dir = format!("{}{WAL_DIR}", data_home);
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
            }
        }
        Ok(engines)
    }
}

pub struct DatanodeBuilder {
    opts: DatanodeOptions,
    plugins: Arc<Plugins>,
    meta_client: Option<MetaClient>,
}

impl DatanodeBuilder {
    pub fn new(opts: DatanodeOptions, plugins: Arc<Plugins>) -> Self {
        Self {
            opts,
            plugins,
            meta_client: None,
        }
    }

    pub fn with_meta_client(self, meta_client: MetaClient) -> Self {
        Self {
            meta_client: Some(meta_client),
            ..self
        }
    }

    pub async fn build(mut self) -> Result<Datanode> {
        let region_server = Datanode::new_region_server(&self.opts, self.plugins.clone()).await?;

        let mode = &self.opts.mode;

        let heartbeat_task = match mode {
            Mode::Distributed => {
                let meta_client = if let Some(meta_client) = self.meta_client.take() {
                    meta_client
                } else {
                    let node_id = self.opts.node_id.context(MissingNodeIdSnafu)?;

                    let meta_config = self
                        .opts
                        .meta_client_options
                        .as_ref()
                        .context(MissingMetasrvOptsSnafu)?;

                    new_metasrv_client(node_id, meta_config).await?
                };

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

        Ok(Datanode {
            opts: self.opts,
            services,
            heartbeat_task,
            region_server,
            greptimedb_telemetry_task,
        })
    }
}
