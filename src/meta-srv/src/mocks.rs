use std::sync::Arc;

use api::v1::meta::heartbeat_server::HeartbeatServer;
use api::v1::meta::router_server::RouterServer;
use api::v1::meta::store_server::StoreServer;
use common_grpc::channel_manager::ChannelConfig;
use common_grpc::channel_manager::ChannelManager;
use tower::service_fn;

use crate::metasrv::MetaSrv;
use crate::metasrv::MetaSrvOptions;
use crate::metasrv::SelectorRef;
use crate::service::store::etcd::EtcdStore;
use crate::service::store::kv::KvStoreRef;
use crate::service::store::memory::MemStore;

#[derive(Clone)]
pub struct MockInfo {
    pub server_addr: String,
    pub channel_manager: ChannelManager,
}

pub async fn mock_with_memstore() -> MockInfo {
    let kv_store = Arc::new(MemStore::default());
    mock(Default::default(), kv_store, None).await
}

pub async fn mock_with_etcdstore(addr: &str) -> MockInfo {
    let kv_store = EtcdStore::with_endpoints([addr]).await.unwrap();
    mock(Default::default(), kv_store, None).await
}

pub async fn mock_with_memstore_and_selector(selector: SelectorRef) -> MockInfo {
    let kv_store = Arc::new(MemStore::default());
    mock(Default::default(), kv_store, Some(selector)).await
}

pub async fn mock(
    opts: MetaSrvOptions,
    kv_store: KvStoreRef,
    selector: Option<SelectorRef>,
) -> MockInfo {
    let server_addr = opts.server_addr.clone();
    let meta_srv = MetaSrv::new(opts, kv_store, selector).await;
    let (client, server) = tokio::io::duplex(1024);
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(HeartbeatServer::new(meta_srv.clone()))
            .add_service(RouterServer::new(meta_srv.clone()))
            .add_service(StoreServer::new(meta_srv.clone()))
            .serve_with_incoming(futures::stream::iter(vec![Ok::<_, std::io::Error>(server)]))
            .await
    });

    let config = ChannelConfig::new();
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
    assert!(res.is_ok());

    MockInfo {
        server_addr,
        channel_manager,
    }
}
