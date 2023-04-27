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

mod instance_test;
mod promql_test;
mod test_util;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use catalog::local::{MemoryCatalogProvider, MemorySchemaProvider};
use catalog::remote::{MetaKvBackend, RemoteCatalogManager};
use client::Client;
use common_grpc::channel_manager::ChannelManager;
use common_runtime::Builder as RuntimeBuilder;
use common_test_util::temp_dir::{create_temp_dir, TempDir};
use datanode::datanode::{
    DatanodeOptions, FileConfig, ObjectStoreConfig, ProcedureConfig, StorageConfig, WalConfig,
};
use datanode::instance::Instance as DatanodeInstance;
use meta_client::client::MetaClientBuilder;
use meta_client::rpc::Peer;
use meta_srv::metasrv::MetaSrvOptions;
use meta_srv::mocks::MockInfo;
use meta_srv::service::store::kv::KvStoreRef;
use meta_srv::service::store::memory::MemStore;
use partition::manager::PartitionRuleManager;
use partition::route::TableRoutes;
use servers::grpc::GrpcServer;
use servers::query_handler::grpc::ServerGrpcQueryHandlerAdaptor;
use servers::Mode;
use table::engine::{region_name, table_dir};
use tonic::transport::Server;
use tower::service_fn;

use crate::catalog::FrontendCatalogManager;
use crate::datanode::DatanodeClients;
use crate::instance::distributed::DistInstance;
use crate::instance::Instance;

/// Guard against the `TempDir`s that used in unit tests.
/// (The `TempDir` will be deleted once it goes out of scope.)
pub struct TestGuard {
    _wal_tmp_dir: TempDir,
    _data_tmp_dir: TempDir,
}

pub(crate) struct MockDistributedInstance {
    pub(crate) frontend: Arc<Instance>,
    pub(crate) dist_instance: Arc<DistInstance>,
    pub(crate) datanodes: HashMap<u64, Arc<DatanodeInstance>>,
    pub(crate) catalog_manager: Arc<FrontendCatalogManager>,
    _guards: Vec<TestGuard>,
}

impl MockDistributedInstance {
    pub fn data_tmp_dirs(&self) -> Vec<&TempDir> {
        self._guards.iter().map(|g| &g._data_tmp_dir).collect()
    }
}

pub(crate) struct MockStandaloneInstance {
    pub(crate) instance: Arc<Instance>,
    _guard: TestGuard,
}

impl MockStandaloneInstance {
    pub fn data_tmp_dir(&self) -> &TempDir {
        &self._guard._data_tmp_dir
    }
}

pub(crate) async fn create_standalone_instance(test_name: &str) -> MockStandaloneInstance {
    let (opts, guard) = create_tmp_dir_and_datanode_opts(test_name);
    let dn_instance = Arc::new(DatanodeInstance::new(&opts).await.unwrap());
    let frontend_instance = Instance::try_new_standalone(dn_instance.clone())
        .await
        .unwrap();

    // create another catalog and schema for testing
    let another_catalog = Arc::new(MemoryCatalogProvider::new());
    let _ = another_catalog
        .register_schema_sync(
            "another_schema".to_string(),
            Arc::new(MemorySchemaProvider::new()),
        )
        .unwrap();
    let _ = dn_instance
        .catalog_manager()
        .register_catalog("another_catalog".to_string(), another_catalog)
        .await
        .unwrap();

    dn_instance.start().await.unwrap();
    MockStandaloneInstance {
        instance: Arc::new(frontend_instance),
        _guard: guard,
    }
}

fn create_tmp_dir_and_datanode_opts(name: &str) -> (DatanodeOptions, TestGuard) {
    let wal_tmp_dir = create_temp_dir(&format!("gt_wal_{name}"));
    let data_tmp_dir = create_temp_dir(&format!("gt_data_{name}"));
    let opts = DatanodeOptions {
        wal: WalConfig {
            dir: wal_tmp_dir.path().to_str().unwrap().to_string(),
            ..Default::default()
        },
        storage: StorageConfig {
            store: ObjectStoreConfig::File(FileConfig {
                data_dir: data_tmp_dir.path().to_str().unwrap().to_string(),
            }),
            ..Default::default()
        },
        mode: Mode::Standalone,
        procedure: ProcedureConfig::default(),
        ..Default::default()
    };
    (
        opts,
        TestGuard {
            _wal_tmp_dir: wal_tmp_dir,
            _data_tmp_dir: data_tmp_dir,
        },
    )
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
    let grpc_server = GrpcServer::new(
        ServerGrpcQueryHandlerAdaptor::arc(datanode_instance),
        None,
        None,
        runtime,
    );
    tokio::spawn(async move {
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

async fn create_distributed_datanode(
    test_name: &str,
    datanode_id: u64,
    meta_srv: MockInfo,
) -> (Arc<DatanodeInstance>, TestGuard) {
    let wal_tmp_dir = create_temp_dir(&format!("gt_wal_{test_name}_dist_dn_{datanode_id}"));
    let data_tmp_dir = create_temp_dir(&format!("gt_data_{test_name}_dist_dn_{datanode_id}"));
    let opts = DatanodeOptions {
        node_id: Some(datanode_id),
        wal: WalConfig {
            dir: wal_tmp_dir.path().to_str().unwrap().to_string(),
            ..Default::default()
        },
        storage: StorageConfig {
            store: ObjectStoreConfig::File(FileConfig {
                data_dir: data_tmp_dir.path().to_str().unwrap().to_string(),
            }),
            ..Default::default()
        },
        mode: Mode::Distributed,
        procedure: ProcedureConfig::default(),
        ..Default::default()
    };

    let instance = Arc::new(
        DatanodeInstance::with_mock_meta_server(&opts, meta_srv)
            .await
            .unwrap(),
    );
    instance.start().await.unwrap();

    // create another catalog and schema for testing
    let _ = instance
        .catalog_manager()
        .as_any()
        .downcast_ref::<RemoteCatalogManager>()
        .unwrap()
        .create_catalog_and_schema("another_catalog", "another_schema")
        .await
        .unwrap();

    (
        instance,
        TestGuard {
            _wal_tmp_dir: wal_tmp_dir,
            _data_tmp_dir: data_tmp_dir,
        },
    )
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

pub(crate) async fn create_distributed_instance(test_name: &str) -> MockDistributedInstance {
    let kv_store: KvStoreRef = Arc::new(MemStore::default()) as _;
    let meta_srv = meta_srv::mocks::mock(MetaSrvOptions::default(), kv_store.clone(), None).await;

    let datanode_clients = Arc::new(DatanodeClients::default());

    let mut test_guards = vec![];

    let mut datanode_instances = HashMap::new();
    for datanode_id in 1..=4 {
        let (dn_instance, guard) =
            create_distributed_datanode(test_name, datanode_id, meta_srv.clone()).await;
        datanode_instances.insert(datanode_id, dn_instance.clone());

        test_guards.push(guard);

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
    let partition_manager = Arc::new(PartitionRuleManager::new(Arc::new(TableRoutes::new(
        meta_client.clone(),
    ))));
    let mut catalog_manager =
        FrontendCatalogManager::new(meta_backend, partition_manager, datanode_clients.clone());

    wait_datanodes_alive(kv_store).await;

    let dist_instance = DistInstance::new(
        meta_client.clone(),
        Arc::new(catalog_manager.clone()),
        datanode_clients.clone(),
    );
    let dist_instance = Arc::new(dist_instance);

    catalog_manager.set_dist_instance(dist_instance.clone());
    let catalog_manager = Arc::new(catalog_manager);

    let frontend = Instance::new_distributed(catalog_manager.clone(), dist_instance.clone()).await;

    MockDistributedInstance {
        frontend: Arc::new(frontend),
        dist_instance,
        datanodes: datanode_instances,
        catalog_manager,
        _guards: test_guards,
    }
}

pub fn test_region_dir(
    dir: &str,
    catalog_name: &str,
    schema_name: &str,
    table_id: u32,
    region_id: u32,
) -> String {
    let table_dir = table_dir(catalog_name, schema_name, table_id);
    let region_name = region_name(table_id, region_id);

    format!("{}/{}/{}", dir, table_dir, region_name)
}

pub fn has_parquet_file(sst_dir: &str) -> bool {
    for entry in std::fs::read_dir(sst_dir).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if !path.is_dir() {
            assert_eq!("parquet", path.extension().unwrap());
            return true;
        }
    }

    false
}
