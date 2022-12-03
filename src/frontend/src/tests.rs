// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use catalog::remote::MetaKvBackend;
use client::Client;
use common_grpc::channel_manager::ChannelManager;
use common_runtime::Builder as RuntimeBuilder;
use datanode::datanode::{DatanodeOptions, ObjectStoreConfig};
use datanode::instance::Instance as DatanodeInstance;
use meta_client::client::MetaClientBuilder;
use meta_client::rpc::Peer;
use meta_srv::metasrv::MetaSrvOptions;
use meta_srv::mocks::MockInfo;
use meta_srv::service::store::kv::KvStoreRef;
use meta_srv::service::store::memory::MemStore;
use servers::grpc::GrpcServer;
use tempdir::TempDir;
use tonic::transport::Server;
use tower::service_fn;

use crate::catalog::FrontendCatalogManager;
use crate::datanode::DatanodeClients;
use crate::instance::distributed::DistInstance;
use crate::instance::Instance;
use crate::table::route::TableRoutes;

async fn create_datanode_instance() -> Arc<DatanodeInstance> {
    // TODO(LFC) Use real Mito engine when we can alter its region schema,
    //   and delete the `new_mock` method.
    let instance = Arc::new(DatanodeInstance::new_mock().await.unwrap());
    instance.start().await.unwrap();
    instance
}

pub(crate) async fn create_frontend_instance() -> Arc<Instance> {
    let datanode_instance: Arc<DatanodeInstance> = create_datanode_instance().await;
    let dn_catalog_manager = datanode_instance.catalog_manager().clone();

    let mut frontend_instance = Instance::new_standalone(datanode_instance);
    frontend_instance.set_catalog_manager(dn_catalog_manager);
    Arc::new(frontend_instance)
}

pub(crate) async fn create_datanode_client(
    datanode_instance: Arc<DatanodeInstance>,
) -> (String, Client) {
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
    let datanode_service =
        GrpcServer::new(datanode_instance.clone(), datanode_instance, runtime).create_service();
    tokio::spawn(async move {
        Server::builder()
            .add_service(datanode_service)
            .serve_with_incoming(futures::stream::iter(vec![Ok::<_, std::io::Error>(server)]))
            .await
    });

    // Move client to an option so we can _move_ the inner value
    // on the first attempt to connect. All other attempts will fail.
    let mut client = Some(client);
    // "127.0.0.1:3001" is just a placeholder, does not actually connect to it.
    let addr = "127.0.0.1:3001";
    let channel_manager = ChannelManager::new();
    channel_manager
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

async fn create_dist_datanode_instance(
    datanode_id: u64,
    meta_srv: MockInfo,
) -> Arc<DatanodeInstance> {
    let current = common_time::util::current_time_millis();
    let wal_tmp_dir = TempDir::new_in("/tmp", &format!("dist_datanode-wal-{}", current)).unwrap();
    let data_tmp_dir = TempDir::new_in("/tmp", &format!("dist_datanode-data-{}", current)).unwrap();
    let opts = DatanodeOptions {
        node_id: Some(datanode_id),
        wal_dir: wal_tmp_dir.path().to_str().unwrap().to_string(),
        storage: ObjectStoreConfig::File {
            data_dir: data_tmp_dir.path().to_str().unwrap().to_string(),
        },
        ..Default::default()
    };

    let instance = Arc::new(
        DatanodeInstance::with_mock_meta_server(&opts, meta_srv)
            .await
            .unwrap(),
    );
    instance.start().await.unwrap();
    instance
}

async fn wait_datanodes_alive(kv_store: KvStoreRef) {
    let wait = 10;
    for _ in 0..wait {
        let datanodes = meta_srv::lease::alive_datanodes(1000, &kv_store, |_, _| true)
            .await
            .unwrap();
        if datanodes.len() >= 4 {
            return;
        }
        tokio::time::sleep(Duration::from_secs(1)).await
    }
    panic!()
}

pub(crate) async fn create_dist_instance() -> (DistInstance, HashMap<u64, Arc<DatanodeInstance>>) {
    let kv_store: KvStoreRef = Arc::new(MemStore::default()) as _;
    let meta_srv = meta_srv::mocks::mock(MetaSrvOptions::default(), kv_store.clone(), None).await;

    let datanode_clients = Arc::new(DatanodeClients::new());

    let mut datanode_instances = HashMap::new();
    for datanode_id in 1..=4 {
        let dn_instance = create_dist_datanode_instance(datanode_id, meta_srv.clone()).await;
        datanode_instances.insert(datanode_id, dn_instance.clone());

        let (addr, client) = create_datanode_client(dn_instance).await;
        datanode_clients
            .insert_client(Peer::new(datanode_id, addr), client)
            .await;
    }

    let MockInfo {
        server_addr,
        channel_manager,
    } = meta_srv.clone();
    let mut meta_client = MetaClientBuilder::new(1000, 0)
        .enable_router()
        .enable_store()
        .channel_manager(channel_manager)
        .build();
    meta_client.start(&[&server_addr]).await.unwrap();
    let meta_client = Arc::new(meta_client);

    let meta_backend = Arc::new(MetaKvBackend {
        client: meta_client.clone(),
    });
    let table_routes = Arc::new(TableRoutes::new(meta_client.clone()));
    let catalog_manager = Arc::new(FrontendCatalogManager::new(
        meta_backend,
        table_routes.clone(),
        datanode_clients.clone(),
    ));

    wait_datanodes_alive(kv_store).await;

    let dist_instance = DistInstance::new(
        meta_client.clone(),
        catalog_manager,
        datanode_clients.clone(),
    );
    (dist_instance, datanode_instances)
}
