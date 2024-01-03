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

use api::v1::meta::Role;
use catalog::kvbackend::{CachedMetaKvBackend, MetaKvBackend};
use client::client_manager::DatanodeClients;
use client::Client;
use common_base::Plugins;
use common_config::WalConfig;
use common_grpc::channel_manager::{ChannelConfig, ChannelManager};
use common_meta::heartbeat::handler::parse_mailbox_message::ParseMailboxMessageHandler;
use common_meta::heartbeat::handler::HandlerGroupExecutor;
use common_meta::kv_backend::chroot::ChrootKvBackend;
use common_meta::kv_backend::etcd::EtcdStore;
use common_meta::kv_backend::memory::MemoryKvBackend;
use common_meta::kv_backend::KvBackendRef;
use common_meta::peer::Peer;
use common_meta::wal::WalConfig as MetaWalConfig;
use common_meta::DatanodeId;
use common_runtime::Builder as RuntimeBuilder;
use common_test_util::temp_dir::create_temp_dir;
use datanode::config::{DatanodeOptions, ObjectStoreConfig};
use datanode::datanode::{Datanode, DatanodeBuilder, ProcedureConfig};
use frontend::heartbeat::handler::invalidate_table_cache::InvalidateTableCacheHandler;
use frontend::heartbeat::HeartbeatTask;
use frontend::instance::builder::FrontendBuilder;
use frontend::instance::{FrontendInstance, Instance as FeInstance};
use meta_client::client::MetaClientBuilder;
use meta_srv::cluster::MetaPeerClientRef;
use meta_srv::metasrv::{MetaSrv, MetaSrvOptions, SelectorRef};
use meta_srv::mocks::MockInfo;
use servers::grpc::GrpcServer;
use servers::heartbeat_options::HeartbeatOptions;
use servers::Mode;
use tempfile::TempDir;
use tonic::transport::Server;
use tower::service_fn;

use crate::test_util::{
    self, create_datanode_opts, create_tmp_dir_and_datanode_opts, FileDirGuard, StorageGuard,
    StorageType, PEER_PLACEHOLDER_ADDR,
};

pub struct GreptimeDbCluster {
    pub storage_guards: Vec<StorageGuard>,
    pub _dir_guards: Vec<FileDirGuard>,

    pub datanode_instances: HashMap<DatanodeId, Datanode>,
    pub kv_backend: KvBackendRef,
    pub meta_srv: MetaSrv,
    pub frontend: Arc<FeInstance>,
}

#[derive(Clone)]
pub struct GreptimeDbClusterBuilder {
    cluster_name: String,
    kv_backend: KvBackendRef,
    store_config: Option<ObjectStoreConfig>,
    store_providers: Option<Vec<StorageType>>,
    datanodes: Option<u32>,
    wal_config: WalConfig,
    meta_wal_config: MetaWalConfig,
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
            let backend = EtcdStore::with_endpoints(endpoints)
                .await
                .expect("malformed endpoints");
            Arc::new(ChrootKvBackend::new(cluster_name.into(), backend))
        };

        Self {
            cluster_name: cluster_name.to_string(),
            kv_backend,
            store_config: None,
            store_providers: None,
            datanodes: None,
            wal_config: WalConfig::default(),
            meta_wal_config: MetaWalConfig::default(),
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
    pub fn with_wal_config(mut self, wal_config: WalConfig) -> Self {
        self.wal_config = wal_config;
        self
    }

    #[must_use]
    pub fn with_meta_wal_config(mut self, wal_meta: MetaWalConfig) -> Self {
        self.meta_wal_config = wal_meta;
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

    pub async fn build(self) -> GreptimeDbCluster {
        let datanodes = self.datanodes.unwrap_or(4);

        let channel_config = ChannelConfig::new().timeout(Duration::from_secs(20));
        let datanode_clients = Arc::new(DatanodeClients::new(channel_config));

        let opt = MetaSrvOptions {
            procedure: ProcedureConfig {
                // Due to large network delay during cross data-center.
                // We only make max_retry_times and retry_delay large than the default in tests.
                max_retry_times: 5,
                retry_delay: Duration::from_secs(1),
            },
            wal: self.meta_wal_config.clone(),
            ..Default::default()
        };

        let meta_srv = meta_srv::mocks::mock(
            opt,
            self.kv_backend.clone(),
            self.meta_selector.clone(),
            Some(datanode_clients.clone()),
        )
        .await;

        let (datanode_instances, storage_guards, dir_guards) =
            self.build_datanodes(meta_srv.clone(), datanodes).await;

        build_datanode_clients(datanode_clients.clone(), &datanode_instances, datanodes).await;

        self.wait_datanodes_alive(meta_srv.meta_srv.meta_peer_client(), datanodes)
            .await;

        let frontend = self
            .build_frontend(meta_srv.clone(), datanode_clients)
            .await;

        test_util::prepare_another_catalog_and_schema(frontend.as_ref()).await;

        frontend.start().await.unwrap();

        GreptimeDbCluster {
            storage_guards,
            _dir_guards: dir_guards,
            datanode_instances,
            kv_backend: self.kv_backend.clone(),
            meta_srv: meta_srv.meta_srv,
            frontend,
        }
    }

    async fn build_datanodes(
        &self,
        meta_srv: MockInfo,
        datanodes: u32,
    ) -> (
        HashMap<DatanodeId, Datanode>,
        Vec<StorageGuard>,
        Vec<FileDirGuard>,
    ) {
        let mut instances = HashMap::with_capacity(datanodes as usize);
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
                    self.wal_config.clone(),
                )
            } else {
                let (opts, guard) = create_tmp_dir_and_datanode_opts(
                    mode,
                    StorageType::File,
                    self.store_providers.clone().unwrap_or_default(),
                    &format!("{}-dn-{}", self.cluster_name, datanode_id),
                    self.wal_config.clone(),
                );

                storage_guards.push(guard.storage_guards);
                dir_guards.push(guard.home_guard);

                opts
            };
            opts.node_id = Some(datanode_id);

            let datanode = self.create_datanode(opts, meta_srv.clone()).await;

            instances.insert(datanode_id, datanode);
        }
        (
            instances,
            storage_guards.into_iter().flatten().collect(),
            dir_guards,
        )
    }

    async fn wait_datanodes_alive(
        &self,
        meta_peer_client: &MetaPeerClientRef,
        expected_datanodes: u32,
    ) {
        for _ in 0..10 {
            let alive_datanodes =
                meta_srv::lease::filter_datanodes(1000, meta_peer_client, |_, _| true)
                    .await
                    .unwrap()
                    .len() as u32;
            if alive_datanodes == expected_datanodes {
                return;
            }
            tokio::time::sleep(Duration::from_secs(1)).await
        }
        panic!("Some Datanodes are not alive in 10 seconds!")
    }

    async fn create_datanode(&self, opts: DatanodeOptions, meta_srv: MockInfo) -> Datanode {
        let mut meta_client = MetaClientBuilder::new(1000, opts.node_id.unwrap(), Role::Datanode)
            .enable_router()
            .enable_store()
            .enable_heartbeat()
            .channel_manager(meta_srv.channel_manager)
            .build();
        meta_client.start(&[&meta_srv.server_addr]).await.unwrap();

        let meta_backend = Arc::new(MetaKvBackend {
            client: Arc::new(meta_client.clone()),
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
        meta_srv: MockInfo,
        datanode_clients: Arc<DatanodeClients>,
    ) -> Arc<FeInstance> {
        let mut meta_client = MetaClientBuilder::new(1000, 0, Role::Frontend)
            .enable_router()
            .enable_store()
            .enable_heartbeat()
            .channel_manager(meta_srv.channel_manager)
            .enable_ddl()
            .build();
        meta_client.start(&[&meta_srv.server_addr]).await.unwrap();
        let meta_client = Arc::new(meta_client);

        let meta_backend = Arc::new(CachedMetaKvBackend::new(meta_client.clone()));

        let handlers_executor = HandlerGroupExecutor::new(vec![
            Arc::new(ParseMailboxMessageHandler),
            Arc::new(InvalidateTableCacheHandler::new(meta_backend.clone())),
        ]);

        let heartbeat_task = HeartbeatTask::new(
            meta_client.clone(),
            HeartbeatOptions::default(),
            Arc::new(handlers_executor),
        );

        let instance = FrontendBuilder::new(meta_backend.clone(), datanode_clients, meta_client)
            .with_cache_invalidator(meta_backend)
            .with_heartbeat_task(heartbeat_task)
            .try_build()
            .await
            .unwrap();

        Arc::new(instance)
    }
}

async fn build_datanode_clients(
    clients: Arc<DatanodeClients>,
    instances: &HashMap<DatanodeId, Datanode>,
    datanodes: u32,
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

    let runtime = Arc::new(
        RuntimeBuilder::default()
            .worker_threads(2)
            .thread_name("grpc-handlers")
            .build()
            .unwrap(),
    );

    let flight_handler = Some(Arc::new(datanode.region_server()) as _);
    let region_server_handler = Some(Arc::new(datanode.region_server()) as _);
    let grpc_server = GrpcServer::new(
        None,
        None,
        None,
        flight_handler,
        region_server_handler,
        None,
        runtime,
    );
    let _handle = tokio::spawn(async move {
        Server::builder()
            .add_service(grpc_server.create_flight_service())
            .add_service(grpc_server.create_region_service())
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
