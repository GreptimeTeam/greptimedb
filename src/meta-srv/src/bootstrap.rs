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

use api::v1::meta::heartbeat_server::HeartbeatServer;
use api::v1::meta::router_server::RouterServer;
use api::v1::meta::store_server::StoreServer;
use snafu::ResultExt;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::server::Router;

use crate::election::etcd::EtcdElection;
use crate::metasrv::{MetaSrv, MetaSrvOptions};
use crate::service::admin;
use crate::service::store::etcd::EtcdStore;
use crate::{error, Result};

// Bootstrap the rpc server to serve incoming request
pub async fn bootstrap_meta_srv(opts: MetaSrvOptions) -> Result<()> {
    let meta_srv = make_meta_srv(opts.clone()).await?;
    bootstrap_meta_srv_with_router(opts, router(meta_srv)).await
}

pub async fn bootstrap_meta_srv_with_router(opts: MetaSrvOptions, router: Router) -> Result<()> {
    let listener = TcpListener::bind(&opts.bind_addr)
        .await
        .context(error::TcpBindSnafu {
            addr: &opts.bind_addr,
        })?;
    let listener = TcpListenerStream::new(listener);

    router
        .serve_with_incoming(listener)
        .await
        .context(error::StartGrpcSnafu)?;

    Ok(())
}

pub fn router(meta_srv: MetaSrv) -> Router {
    tonic::transport::Server::builder()
        .accept_http1(true) // for admin services
        .add_service(HeartbeatServer::new(meta_srv.clone()))
        .add_service(RouterServer::new(meta_srv.clone()))
        .add_service(StoreServer::new(meta_srv.clone()))
        .add_service(admin::make_admin_service(meta_srv))
}

pub async fn make_meta_srv(opts: MetaSrvOptions) -> Result<MetaSrv> {
    let kv_store = EtcdStore::with_endpoints([&opts.store_addr]).await?;
    let election = EtcdElection::with_endpoints(&opts.server_addr, [&opts.store_addr]).await?;
    let meta_srv = MetaSrv::new(opts, kv_store, None, Some(election)).await;
    meta_srv.start().await;
    Ok(meta_srv)
}
