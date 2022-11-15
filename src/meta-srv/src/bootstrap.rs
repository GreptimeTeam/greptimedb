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

use api::v1::meta::heartbeat_server::HeartbeatServer;
use api::v1::meta::router_server::RouterServer;
use api::v1::meta::store_server::StoreServer;
use snafu::ResultExt;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;

use crate::election::etcd::EtcdElection;
use crate::error;
use crate::metasrv::{MetaSrv, MetaSrvOptions};
use crate::service::admin;
use crate::service::store::etcd::EtcdStore;

// Bootstrap the rpc server to serve incoming request
pub async fn bootstrap_meta_srv(opts: MetaSrvOptions) -> crate::Result<()> {
    let kv_store = EtcdStore::with_endpoints([&opts.store_addr]).await?;
    let election = EtcdElection::with_endpoints(&opts.server_addr, [&opts.store_addr]).await?;

    let listener = TcpListener::bind(&opts.bind_addr)
        .await
        .context(error::TcpBindSnafu {
            addr: &opts.bind_addr,
        })?;
    let listener = TcpListenerStream::new(listener);

    let meta_srv = MetaSrv::new(opts, kv_store, None, Some(election)).await;
    meta_srv.start().await;

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
