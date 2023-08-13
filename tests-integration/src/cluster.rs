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

use api::v1::meta::Role;
use client::client_manager::DatanodeClients;
use client::Client;
use common_base::Plugins;
use common_grpc::channel_manager::{ChannelConfig, ChannelManager};
use common_meta::peer::Peer;
use common_meta::DatanodeId;
use common_runtime::Builder as RuntimeBuilder;
use common_test_util::temp_dir::create_temp_dir;
use datanode::datanode::{DatanodeOptions, ObjectStoreConfig, ProcedureConfig};
use datanode::heartbeat::HeartbeatTask;
use datanode::instance::Instance as DatanodeInstance;
use frontend::frontend::FrontendOptions;
use frontend::instance::{FrontendInstance, Instance as FeInstance};
use meta_client::client::MetaClientBuilder;
use meta_srv::cluster::MetaPeerClientRef;
use meta_srv::metadata_service::{DefaultMetadataService, MetadataService};
use meta_srv::metasrv::{MetaSrv, MetaSrvOptions};
use meta_srv::mocks::MockInfo;
use meta_srv::service::store::kv::KvStoreRef;
use meta_srv::service::store::memory::MemStore;
use servers::grpc::GrpcServer;
use servers::query_handler::grpc::ServerGrpcQueryHandlerAdaptor;
use servers::Mode;
use tonic::transport::Server;
use tower::service_fn;

use crate::test_util::{
    create_datanode_opts, create_tmp_dir_and_datanode_opts, FileDirGuard, StorageGuard, StorageType,
};

pub struct GreptimeDbCluster {
    pub storage_guards: Vec<StorageGuard>,
    pub _dir_guards: Vec<FileDirGuard>,

    pub datanode_instances: HashMap<DatanodeId, Arc<DatanodeInstance>>,
    pub datanode_heartbeat_tasks: HashMap<DatanodeId, Option<HeartbeatTask>>,
    pub kv_store: KvStoreRef,
    pub meta_srv: MetaSrv,
    pub frontend: Arc<FeInstance>,
}

pub struct GreptimeDbClusterBuilder {
    cluster_name: String,
    kv_store: KvStoreRef,
    store_config: Option<ObjectStoreConfig>,
    datanodes: Option<u32>,
}

impl GreptimeDbClusterBuilder {
    pub fn new(cluster_name: &str) -> Self {
        Self {
            cluster_name: cluster_name.to_string(),
            kv_store: Arc::new(MemStore::default()),
            store_config: None,
            datanodes: None,
        }
    }

    pub fn with_store_config(mut self, store_config: ObjectStoreConfig) -> Self {
        self.store_config = Some(store_config);
        self
    }

    pub fn with_datanodes(mut self, datanodes: u32) -> Self {
        self.datanodes = Some(datanodes);
        self
    }

    pub async fn build(self) -> GreptimeDbCluster {
        let datanodes = self.datanodes.unwrap_or(4);

        let channel_config = ChannelConfig::new().timeout(Duration::from_secs(20));
        let datanode_clients = Arc::new(DatanodeClients::new(channel_config));

        let meta_srv = self.build_metasrv(datanode_clients.clone()).await;

        let (datanode_instances, heartbeat_tasks, storage_guards, dir_guards) =
            self.build_datanodes(meta_srv.clone(), datanodes).await;

        build_datanode_clients(datanode_clients.clone(), &datanode_instances, datanodes).await;

        self.wait_datanodes_alive(meta_srv.meta_srv.meta_peer_client(), datanodes)
            .await;

        let frontend = self
            .build_frontend(meta_srv.clone(), datanode_clients)
            .await;

        frontend.start().await.unwrap();

        GreptimeDbCluster {
            storage_guards,
            _dir_guards: dir_guards,
            datanode_instances,
            datanode_heartbeat_tasks: heartbeat_tasks,
            kv_store: self.kv_store.clone(),
            meta_srv: meta_srv.meta_srv,
            frontend,
        }
    }

    async fn build_metasrv(&self, datanode_clients: Arc<DatanodeClients>) -> MockInfo {
        let opt = MetaSrvOptions {
            procedure: ProcedureConfig {
                // Due to large network delay during cross data-center.
                // We only make max_retry_times and retry_delay large than the default in tests.
                max_retry_times: 5,
                retry_delay: Duration::from_secs(1),
            },
            ..Default::default()
        };

        let mock =
            meta_srv::mocks::mock(opt, self.kv_store.clone(), None, Some(datanode_clients)).await;

        let metadata_service = DefaultMetadataService::new(mock.meta_srv.kv_store().clone());
        metadata_service
            .create_schema("another_catalog", "another_schema", true)
            .await
            .unwrap();

        mock
    }

    async fn build_datanodes(
        &self,
        meta_srv: MockInfo,
        datanodes: u32,
    ) -> (
        HashMap<DatanodeId, Arc<DatanodeInstance>>,
        HashMap<DatanodeId, Option<HeartbeatTask>>,
        Vec<StorageGuard>,
        Vec<FileDirGuard>,
    ) {
        let mut instances = HashMap::with_capacity(datanodes as usize);
        let mut heartbeat_tasks = HashMap::with_capacity(datanodes as usize);
        let mut storage_guards = Vec::with_capacity(datanodes as usize);
        let mut dir_guards = Vec::with_capacity(datanodes as usize);

        for i in 0..datanodes {
            let datanode_id = i as u64 + 1;

            let mut opts = if let Some(store_config) = &self.store_config {
                let home_tmp_dir = create_temp_dir(&format!("gt_home_{}", &self.cluster_name));
                let home_dir = home_tmp_dir.path().to_str().unwrap().to_string();

                let wal_tmp_dir = create_temp_dir(&format!("gt_wal_{}", &self.cluster_name));
                let wal_dir = wal_tmp_dir.path().to_str().unwrap().to_string();
                dir_guards.push(FileDirGuard::new(home_tmp_dir, false));
                dir_guards.push(FileDirGuard::new(wal_tmp_dir, true));

                create_datanode_opts(store_config.clone(), home_dir, wal_dir)
            } else {
                let (opts, guard) = create_tmp_dir_and_datanode_opts(
                    StorageType::File,
                    &format!("{}-dn-{}", self.cluster_name, datanode_id),
                );

                storage_guards.push(guard.storage_guard);
                dir_guards.push(guard.home_guard);
                dir_guards.push(guard.wal_guard);

                opts
            };
            opts.node_id = Some(datanode_id);
            opts.mode = Mode::Distributed;

            let dn_instance = self.create_datanode(&opts, meta_srv.clone()).await;

            let _ = instances.insert(datanode_id, dn_instance.0.clone());
            let _ = heartbeat_tasks.insert(datanode_id, dn_instance.1);
        }
        (instances, heartbeat_tasks, storage_guards, dir_guards)
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

    async fn create_datanode(
        &self,
        opts: &DatanodeOptions,
        meta_srv: MockInfo,
    ) -> (Arc<DatanodeInstance>, Option<HeartbeatTask>) {
        let (instance, heartbeat) = DatanodeInstance::with_mock_meta_server(opts, meta_srv)
            .await
            .unwrap();
        instance.start().await.unwrap();
        if let Some(heartbeat) = heartbeat.as_ref() {
            heartbeat.start().await.unwrap();
        }
        (instance, heartbeat)
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

        let frontend_opts = FrontendOptions::default();

        Arc::new(
            FeInstance::try_new_distributed_with(
                meta_client,
                datanode_clients,
                Arc::new(Plugins::default()),
                &frontend_opts,
            )
            .await
            .unwrap(),
        )
    }
}

async fn build_datanode_clients(
    clients: Arc<DatanodeClients>,
    instances: &HashMap<DatanodeId, Arc<DatanodeInstance>>,
    datanodes: u32,
) {
    for i in 0..datanodes {
        let datanode_id = i as u64 + 1;
        let instance = instances.get(&datanode_id).cloned().unwrap();
        let (addr, client) = create_datanode_client(instance).await;
        clients
            .insert_client(Peer::new(datanode_id, addr), client)
            .await;
    }
}

async fn create_datanode_client(datanode_instance: Arc<DatanodeInstance>) -> (String, Client) {
    let (client, server) = tokio::io::duplex(1024);

    let runtime = Arc::new(
        RuntimeBuilder::default()
            .worker_threads(2)
            .thread_name("grpc-handlers")
            .build()
            .unwrap(),
    );

    // create a mock datanode grpc service, see example here:
    // https://github.com/hyperium/tonic/blob/master/examples/src/mock/mock.rs
    let grpc_server = GrpcServer::new(
        ServerGrpcQueryHandlerAdaptor::arc(datanode_instance),
        None,
        None,
        runtime,
    );
    let _handle = tokio::spawn(async move {
        Server::builder()
            .add_service(grpc_server.create_flight_service())
            .add_service(grpc_server.create_database_service())
            .serve_with_incoming(futures::stream::iter(vec![Ok::<_, std::io::Error>(server)]))
            .await
    });

    // Move client to an option so we can _move_ the inner value
    // on the first attempt to connect. All other attempts will fail.
    let mut client = Some(client);
    // "127.0.0.1:3001" is just a placeholder, does not actually connect to it.
    let addr = "127.0.0.1:3001";
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
