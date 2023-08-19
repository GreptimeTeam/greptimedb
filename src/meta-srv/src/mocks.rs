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

use std::sync::Arc;
use std::time::Duration;

use api::v1::meta::ddl_task_server::DdlTaskServer;
use api::v1::meta::heartbeat_server::HeartbeatServer;
use api::v1::meta::router_server::RouterServer;
use api::v1::meta::store_server::StoreServer;
use client::client_manager::DatanodeClients;
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_grpc::channel_manager::{ChannelConfig, ChannelManager};
use common_meta::key::TableMetadataManager;
use tower::service_fn;

use crate::metadata_service::{DefaultMetadataService, MetadataService};
use crate::metasrv::builder::MetaSrvBuilder;
use crate::metasrv::{MetaSrv, MetaSrvOptions, SelectorRef};
use crate::service::store::etcd::EtcdStore;
use crate::service::store::kv::{KvBackendAdapter, KvStoreRef};
use crate::service::store::memory::MemStore;

#[derive(Clone)]
pub struct MockInfo {
    pub server_addr: String,
    pub channel_manager: ChannelManager,
    pub meta_srv: MetaSrv,
}

pub async fn mock_with_memstore() -> MockInfo {
    let kv_store = Arc::new(MemStore::default());
    mock(Default::default(), kv_store, None, None).await
}

pub async fn mock_with_etcdstore(addr: &str) -> MockInfo {
    let kv_store = EtcdStore::with_endpoints([addr]).await.unwrap();
    mock(Default::default(), kv_store, None, None).await
}

pub async fn mock_with_memstore_and_selector(selector: SelectorRef) -> MockInfo {
    let kv_store = Arc::new(MemStore::default());
    mock(Default::default(), kv_store, Some(selector), None).await
}

pub async fn mock(
    opts: MetaSrvOptions,
    kv_store: KvStoreRef,
    selector: Option<SelectorRef>,
    datanode_clients: Option<Arc<DatanodeClients>>,
) -> MockInfo {
    let server_addr = opts.server_addr.clone();
    let table_metadata_manager = Arc::new(TableMetadataManager::new(KvBackendAdapter::wrap(
        kv_store.clone(),
    )));
    let metadata_service = DefaultMetadataService::new(table_metadata_manager);

    metadata_service
        .create_schema(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, true)
        .await
        .unwrap();

    let builder = MetaSrvBuilder::new().options(opts).kv_store(kv_store);

    let builder = match selector {
        Some(s) => builder.selector(s),
        None => builder,
    };

    let builder = match datanode_clients {
        Some(clients) => builder.datanode_clients(clients),
        None => builder,
    };

    let meta_srv = builder.build().await.unwrap();
    meta_srv.try_start().await.unwrap();

    let (client, server) = tokio::io::duplex(1024);
    let service = meta_srv.clone();
    let _handle = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(HeartbeatServer::new(service.clone()))
            .add_service(RouterServer::new(service.clone()))
            .add_service(StoreServer::new(service.clone()))
            .add_service(DdlTaskServer::new(service.clone()))
            .serve_with_incoming(futures::stream::iter(vec![Ok::<_, std::io::Error>(server)]))
            .await
    });

    let config = ChannelConfig::new()
        .timeout(Duration::from_secs(10))
        .connect_timeout(Duration::from_secs(10))
        .tcp_nodelay(true);
    let channel_manager = ChannelManager::with_config(config);

    // Move client to an option so we can _move_ the inner value
    // on the first attempt to connect. All other attempts will fail.
    let mut client = Some(client);
    let res = channel_manager.reset_with_connector(
        &server_addr,
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
    );
    let _ = res.unwrap();

    MockInfo {
        server_addr,
        channel_manager,
        meta_srv,
    }
}
