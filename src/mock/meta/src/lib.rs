use std::sync::Arc;

use api::v1::meta::heartbeat_server::HeartbeatServer;
use api::v1::meta::router_server::RouterServer;
use api::v1::meta::store_server::StoreServer;
use common_grpc::channel_manager::ChannelConfig;
use common_grpc::channel_manager::ChannelManager;
use meta_srv::metasrv::MetaSrv;
use meta_srv::metasrv::MetaSrvOptions;
use meta_srv::metasrv::SelectorRef;
use meta_srv::service::store::kv::KvStoreRef;
use meta_srv::service::store::noop::NoopKvStore;
use tower::service_fn;

pub struct MockInfo {
    pub server_addr: String,
    pub channel_manager: ChannelManager,
}

pub async fn create_meta_client_with_noop_store() -> MockInfo {
    let kv_store = Arc::new(NoopKvStore {});
    create_meta_client(Default::default(), kv_store, None).await
}

pub async fn create_meta_client_with_selector(selector: SelectorRef) -> MockInfo {
    let kv_store = Arc::new(NoopKvStore {});
    create_meta_client(Default::default(), kv_store, Some(selector)).await
}

pub async fn create_meta_client(
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
