use api::v1::meta::{heartbeat_server::HeartbeatServer, store_server::StoreServer};
use snafu::ResultExt;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;

use crate::error;
use crate::service::{admin, MetaServer};

// Bootstrap the rpc server to serve incoming request
async fn bootstrap_services(addr: &str, meta_server: MetaServer) -> crate::Result<()> {
    let listener = TcpListener::bind(addr)
        .await
        .context(error::TcpBindSnafu { addr })?;
    let listener = TcpListenerStream::new(listener);

    tonic::transport::Server::builder()
        .accept_http1(true) // for admin services
        .add_service(HeartbeatServer::new(meta_server.clone()))
        // .add_service(RouteServer::new(meta_server.clone()))
        .add_service(StoreServer::new(meta_server.clone()))
        .add_service(admin::make_admin_service(meta_server.clone()))
        .serve_with_incoming(listener)
        .await
        .context(error::StartGrpcSnafu)?;

    Ok(())
}
