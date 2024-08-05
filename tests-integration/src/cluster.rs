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
use std::env;
use std::sync::Arc;
use std::time::Duration;

use api::v1::region::region_server::RegionServer;
use arrow_flight::flight_service_server::FlightServiceServer;
use cache::{build_fundamental_cache_registry, with_default_composite_cache_registry};
use catalog::kvbackend::{CachedMetaKvBackendBuilder, KvBackendCatalogManager, MetaKvBackend};
use client::client_manager::NodeClients;
use client::Client;
use common_base::Plugins;
use common_grpc::channel_manager::{ChannelConfig, ChannelManager};
use common_meta::cache::{CacheRegistryBuilder, LayeredCacheRegistryBuilder};
use common_meta::heartbeat::handler::parse_mailbox_message::ParseMailboxMessageHandler;
use common_meta::heartbeat::handler::HandlerGroupExecutor;
use common_meta::kv_backend::chroot::ChrootKvBackend;
use common_meta::kv_backend::etcd::EtcdStore;
use common_meta::kv_backend::memory::MemoryKvBackend;
use common_meta::kv_backend::KvBackendRef;
use common_meta::peer::Peer;
use common_meta::DatanodeId;
use common_runtime::Builder as RuntimeBuilder;
use common_test_util::temp_dir::create_temp_dir;
use common_wal::config::{DatanodeWalConfig, MetasrvWalConfig};
use datanode::config::{DatanodeOptions, ObjectStoreConfig};
use datanode::datanode::{Datanode, DatanodeBuilder, ProcedureConfig};
use frontend::frontend::FrontendOptions;
use frontend::heartbeat::handler::invalidate_table_cache::InvalidateTableCacheHandler;
use frontend::heartbeat::HeartbeatTask;
use frontend::instance::builder::FrontendBuilder;
use frontend::instance::{FrontendInstance, Instance as FeInstance};
use meta_client::client::MetaClientBuilder;
use meta_srv::cluster::MetaPeerClientRef;
use meta_srv::metasrv::{Metasrv, MetasrvOptions, SelectorRef};
use meta_srv::mocks::MockInfo;
use servers::grpc::flight::FlightCraftWrapper;
use servers::grpc::region_server::RegionServerRequestHandler;
use servers::heartbeat_options::HeartbeatOptions;
use servers::Mode;
use tempfile::TempDir;
use tonic::codec::CompressionEncoding;
use tonic::transport::Server;
use tower::service_fn;
use uuid::Uuid;

use crate::test_util::{
    self, create_datanode_opts, create_tmp_dir_and_datanode_opts, FileDirGuard, StorageGuard,
    StorageType, PEER_PLACEHOLDER_ADDR,
};

pub struct GreptimeDbCluster {
    pub storage_guards: Vec<StorageGuard>,
    pub dir_guards: Vec<FileDirGuard>,
    pub datanode_options: Vec<DatanodeOptions>,

    pub datanode_instances: HashMap<DatanodeId, Datanode>,
    pub kv_backend: KvBackendRef,
    pub metasrv: Metasrv,
    pub frontend: Arc<FeInstance>,
}

pub struct GreptimeDbClusterBuilder {
    cluster_name: String,
    kv_backend: KvBackendRef,
    store_config: Option<ObjectStoreConfig>,
    store_providers: Option<Vec<StorageType>>,
    datanodes: Option<u32>,
    datanode_wal_config: DatanodeWalConfig,
    metasrv_wal_config: MetasrvWalConfig,
    shared_home_dir: Option<Arc<TempDir>>,
    meta_selector: Option<SelectorRef>,
}

impl GreptimeDbClusterBuilder {
    pub async fn new(cluster_name: &str) -> Self {
        let endpoints = env::var("GT_ETCD_ENDPOINTS").unwrap_or_default();

        let kv_backend: KvBackendRef = if endpoints.is_empty() {
            Arc::new(MemoryKvBackend::new())
        } else {
            let endpoints = endpoints
                .split(',')
                .map(|s| s.to_string())
                .collect::<Vec<String>>();
            let backend = EtcdStore::with_endpoints(endpoints, 128)
                .await
                .expect("malformed endpoints");
            // Each retry requires a new isolation namespace.
            let chroot = format!("{}{}", cluster_name, Uuid::new_v4());
            Arc::new(ChrootKvBackend::new(chroot.into(), backend))
        };

        Self {
            cluster_name: cluster_name.to_string(),
            kv_backend,
            store_config: None,
            store_providers: None,
            datanodes: None,
            datanode_wal_config: DatanodeWalConfig::default(),
            metasrv_wal_config: MetasrvWalConfig::default(),
            shared_home_dir: None,
            meta_selector: None,
        }
    }

    #[must_use]
    pub fn with_store_config(mut self, store_config: ObjectStoreConfig) -> Self {
        self.store_config = Some(store_config);
        self
    }

    #[must_use]
    pub fn with_store_providers(mut self, store_providers: Vec<StorageType>) -> Self {
        self.store_providers = Some(store_providers);
        self
    }

    #[must_use]
    pub fn with_datanodes(mut self, datanodes: u32) -> Self {
        self.datanodes = Some(datanodes);
        self
    }

    #[must_use]
    pub fn with_datanode_wal_config(mut self, datanode_wal_config: DatanodeWalConfig) -> Self {
        self.datanode_wal_config = datanode_wal_config;
        self
    }

    #[must_use]
    pub fn with_metasrv_wal_config(mut self, metasrv_wal_config: MetasrvWalConfig) -> Self {
        self.metasrv_wal_config = metasrv_wal_config;
        self
    }

    #[must_use]
    pub fn with_shared_home_dir(mut self, shared_home_dir: Arc<TempDir>) -> Self {
        self.shared_home_dir = Some(shared_home_dir);
        self
    }

    #[must_use]
    pub fn with_meta_selector(mut self, selector: SelectorRef) -> Self {
        self.meta_selector = Some(selector);
        self
    }

    pub async fn build_with(
        &self,
        datanode_options: Vec<DatanodeOptions>,
        storage_guards: Vec<StorageGuard>,
        dir_guards: Vec<FileDirGuard>,
    ) -> GreptimeDbCluster {
        let datanodes = datanode_options.len();
        let channel_config = ChannelConfig::new().timeout(Duration::from_secs(20));
        let datanode_clients = Arc::new(NodeClients::new(channel_config));

        let opt = MetasrvOptions {
            procedure: ProcedureConfig {
                // Due to large network delay during cross data-center.
                // We only make max_retry_times and retry_delay large than the default in tests.
                max_retry_times: 5,
                retry_delay: Duration::from_secs(1),
                max_metadata_value_size: None,
            },
            wal: self.metasrv_wal_config.clone(),
            ..Default::default()
        };

        let metasrv = meta_srv::mocks::mock(
            opt,
            self.kv_backend.clone(),
            self.meta_selector.clone(),
            Some(datanode_clients.clone()),
        )
        .await;

        let datanode_instances = self
            .build_datanodes_with_options(&metasrv, &datanode_options)
            .await;

        build_datanode_clients(datanode_clients.clone(), &datanode_instances, datanodes).await;

        self.wait_datanodes_alive(metasrv.metasrv.meta_peer_client(), datanodes)
            .await;

        let frontend = self.build_frontend(metasrv.clone(), datanode_clients).await;

        test_util::prepare_another_catalog_and_schema(frontend.as_ref()).await;

        frontend.start().await.unwrap();

        GreptimeDbCluster {
            datanode_options,
            storage_guards,
            dir_guards,
            datanode_instances,
            kv_backend: self.kv_backend.clone(),
            metasrv: metasrv.metasrv,
            frontend,
        }
    }

    pub async fn build(&self) -> GreptimeDbCluster {
        let datanodes = self.datanodes.unwrap_or(4);
        let (datanode_options, storage_guards, dir_guards) =
            self.build_datanode_options_and_guards(datanodes).await;
        self.build_with(datanode_options, storage_guards, dir_guards)
            .await
    }

    async fn build_datanode_options_and_guards(
        &self,
        datanodes: u32,
    ) -> (Vec<DatanodeOptions>, Vec<StorageGuard>, Vec<FileDirGuard>) {
        let mut options = Vec::with_capacity(datanodes as usize);
        let mut storage_guards = Vec::with_capacity(datanodes as usize);
        let mut dir_guards = Vec::with_capacity(datanodes as usize);

        for i in 0..datanodes {
            let datanode_id = i as u64 + 1;
            let mode = Mode::Distributed;
            let mut opts = if let Some(store_config) = &self.store_config {
                let home_dir = if let Some(home_dir) = &self.shared_home_dir {
                    home_dir.path().to_str().unwrap().to_string()
                } else {
                    let home_tmp_dir = create_temp_dir(&format!("gt_home_{}", &self.cluster_name));
                    let home_dir = home_tmp_dir.path().to_str().unwrap().to_string();
                    dir_guards.push(FileDirGuard::new(home_tmp_dir));

                    home_dir
                };

                create_datanode_opts(
                    mode,
                    store_config.clone(),
                    vec![],
                    home_dir,
                    self.datanode_wal_config.clone(),
                )
            } else {
                let (opts, guard) = create_tmp_dir_and_datanode_opts(
                    mode,
                    StorageType::File,
                    self.store_providers.clone().unwrap_or_default(),
                    &format!("{}-dn-{}", self.cluster_name, datanode_id),
                    self.datanode_wal_config.clone(),
                );

                storage_guards.push(guard.storage_guards);
                dir_guards.push(guard.home_guard);

                opts
            };
            opts.node_id = Some(datanode_id);

            options.push(opts);
        }
        (
            options,
            storage_guards.into_iter().flatten().collect(),
            dir_guards,
        )
    }

    async fn build_datanodes_with_options(
        &self,
        metasrv: &MockInfo,
        options: &[DatanodeOptions],
    ) -> HashMap<DatanodeId, Datanode> {
        let mut instances = HashMap::with_capacity(options.len());

        for opts in options {
            let datanode = self.create_datanode(opts.clone(), metasrv.clone()).await;
            instances.insert(opts.node_id.unwrap(), datanode);
        }

        instances
    }

    async fn wait_datanodes_alive(
        &self,
        meta_peer_client: &MetaPeerClientRef,
        expected_datanodes: usize,
    ) {
        for _ in 0..10 {
            let alive_datanodes =
                meta_srv::lease::alive_datanodes(1000, meta_peer_client, u64::MAX)
                    .await
                    .unwrap()
                    .len();
            if alive_datanodes == expected_datanodes {
                return;
            }
            tokio::time::sleep(Duration::from_secs(1)).await
        }
        panic!("Some Datanodes are not alive in 10 seconds!")
    }

    async fn create_datanode(&self, opts: DatanodeOptions, metasrv: MockInfo) -> Datanode {
        let mut meta_client =
            MetaClientBuilder::datanode_default_options(1000, opts.node_id.unwrap())
                .channel_manager(metasrv.channel_manager)
                .build();
        meta_client.start(&[&metasrv.server_addr]).await.unwrap();
        let meta_client = Arc::new(meta_client);

        let meta_backend = Arc::new(MetaKvBackend {
            client: meta_client.clone(),
        });

        let mut datanode = DatanodeBuilder::new(opts, Plugins::default())
            .with_kv_backend(meta_backend)
            .with_meta_client(meta_client)
            .build()
            .await
            .unwrap();

        datanode.start_heartbeat().await.unwrap();

        datanode
    }

    async fn build_frontend(
        &self,
        metasrv: MockInfo,
        datanode_clients: Arc<NodeClients>,
    ) -> Arc<FeInstance> {
        let mut meta_client = MetaClientBuilder::frontend_default_options(1000)
            .channel_manager(metasrv.channel_manager)
            .build();
        meta_client.start(&[&metasrv.server_addr]).await.unwrap();
        let meta_client = Arc::new(meta_client);

        let cached_meta_backend =
            Arc::new(CachedMetaKvBackendBuilder::new(meta_client.clone()).build());

        let layered_cache_builder = LayeredCacheRegistryBuilder::default().add_cache_registry(
            CacheRegistryBuilder::default()
                .add_cache(cached_meta_backend.clone())
                .build(),
        );
        let fundamental_cache_registry =
            build_fundamental_cache_registry(Arc::new(MetaKvBackend::new(meta_client.clone())));
        let cache_registry = Arc::new(
            with_default_composite_cache_registry(
                layered_cache_builder.add_cache_registry(fundamental_cache_registry),
            )
            .unwrap()
            .build(),
        );

        let catalog_manager = KvBackendCatalogManager::new(
            Mode::Distributed,
            Some(meta_client.clone()),
            cached_meta_backend.clone(),
            cache_registry.clone(),
        );

        let handlers_executor = HandlerGroupExecutor::new(vec![
            Arc::new(ParseMailboxMessageHandler),
            Arc::new(InvalidateTableCacheHandler::new(cache_registry.clone())),
        ]);

        let options = FrontendOptions::default();
        let heartbeat_task = HeartbeatTask::new(
            &options,
            meta_client.clone(),
            HeartbeatOptions::default(),
            Arc::new(handlers_executor),
        );

        let instance = FrontendBuilder::new(
            options,
            cached_meta_backend.clone(),
            cache_registry.clone(),
            catalog_manager,
            datanode_clients,
            meta_client,
        )
        .with_local_cache_invalidator(cache_registry)
        .with_heartbeat_task(heartbeat_task)
        .try_build()
        .await
        .unwrap();

        Arc::new(instance)
    }
}

async fn build_datanode_clients(
    clients: Arc<NodeClients>,
    instances: &HashMap<DatanodeId, Datanode>,
    datanodes: usize,
) {
    for i in 0..datanodes {
        let datanode_id = i as u64 + 1;
        let instance = instances.get(&datanode_id).unwrap();
        let (addr, client) = create_datanode_client(instance).await;
        clients
            .insert_client(Peer::new(datanode_id, addr), client)
            .await;
    }
}

async fn create_datanode_client(datanode: &Datanode) -> (String, Client) {
    let (client, server) = tokio::io::duplex(1024);

    let runtime = RuntimeBuilder::default()
        .worker_threads(2)
        .thread_name("grpc-handlers")
        .build()
        .unwrap();

    let flight_handler = FlightCraftWrapper(datanode.region_server());

    let region_server_handler =
        RegionServerRequestHandler::new(Arc::new(datanode.region_server()), runtime);

    let _handle = tokio::spawn(async move {
        Server::builder()
            .add_service(
                FlightServiceServer::new(flight_handler)
                    .accept_compressed(CompressionEncoding::Gzip)
                    .accept_compressed(CompressionEncoding::Zstd)
                    .send_compressed(CompressionEncoding::Gzip)
                    .send_compressed(CompressionEncoding::Zstd),
            )
            .add_service(
                RegionServer::new(region_server_handler)
                    .accept_compressed(CompressionEncoding::Gzip)
                    .accept_compressed(CompressionEncoding::Zstd)
                    .send_compressed(CompressionEncoding::Gzip)
                    .send_compressed(CompressionEncoding::Zstd),
            )
            .serve_with_incoming(futures::stream::iter(vec![Ok::<_, std::io::Error>(server)]))
            .await
    });

    // Move client to an option so we can _move_ the inner value
    // on the first attempt to connect. All other attempts will fail.
    let mut client = Some(client);
    // `PEER_PLACEHOLDER_ADDR` is just a placeholder, does not actually connect to it.
    let addr = PEER_PLACEHOLDER_ADDR;
    let channel_manager = ChannelManager::new();
    let _ = channel_manager
        .reset_with_connector(
            addr,
            service_fn(move |_| {
                let client = client.take();

                async move {
                    if let Some(client) = client {
                        Ok(client)
                    } else {
                        Err(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            "Client already taken",
                        ))
                    }
                }
            }),
        )
        .unwrap();
    (
        addr.to_string(),
        Client::with_manager_and_urls(channel_manager, vec![addr]),
    )
}
