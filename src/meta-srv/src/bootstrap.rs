use api::v1::meta::heartbeat_server::HeartbeatServer;
use api::v1::meta::router_server::RouterServer;
use api::v1::meta::store_server::StoreServer;
use snafu::ResultExt;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;

use crate::error;
use crate::metasrv::MetaSrv;
use crate::metasrv::MetaSrvOptions;
use crate::service::admin;
use crate::service::store::etcd::EtcdStore;

// Bootstrap the rpc server to serve incoming request
pub async fn bootstrap_meta_srv(opts: MetaSrvOptions) -> crate::Result<()> {
    let kv_store = EtcdStore::with_endpoints([&opts.store_addr]).await?;

    let listener = TcpListener::bind(&opts.bind_addr)
        .await
        .context(error::TcpBindSnafu {
            addr: &opts.bind_addr,
        })?;
    let listener = TcpListenerStream::new(listener);

    let meta_srv = MetaSrv::new(opts, kv_store, None).await;

    tonic::transport::Server::builder()
        .accept_http1(true) // for admin services
        .add_service(HeartbeatServer::new(meta_srv.clone()))
        .add_service(RouterServer::new(meta_srv.clone()))
        .add_service(StoreServer::new(meta_srv.clone()))
        .add_service(admin::make_admin_service(meta_srv.clone()))
        .serve_with_incoming(listener)
        .await
        .context(error::StartGrpcSnafu)?;

    Ok(())
}
