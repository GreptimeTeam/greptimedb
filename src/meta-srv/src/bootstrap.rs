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

use api::v1::meta::cluster_server::ClusterServer;
use api::v1::meta::heartbeat_server::HeartbeatServer;
use api::v1::meta::router_server::RouterServer;
use api::v1::meta::store_server::StoreServer;
use snafu::ResultExt;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::server::Router;

use crate::cluster::MetaPeerClient;
use crate::election::etcd::EtcdElection;
use crate::metasrv::{MetaSrv, MetaSrvOptions, SelectorRef};
use crate::selector::lease_based::LeaseBasedSelector;
use crate::selector::load_based::LoadBasedSelector;
use crate::selector::SelectorType;
use crate::service::admin;
use crate::service::store::etcd::EtcdStore;
use crate::service::store::kv::ResetableKvStoreRef;
use crate::service::store::memory::MemStore;
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
        .add_service(ClusterServer::new(meta_srv.clone()))
        .add_service(admin::make_admin_service(meta_srv))
}

pub async fn make_meta_srv(opts: MetaSrvOptions) -> Result<MetaSrv> {
    let (kv_store, election) = if opts.use_memory_store {
        (Arc::new(MemStore::new()) as _, None)
    } else {
        (
            EtcdStore::with_endpoints([&opts.store_addr]).await?,
            Some(EtcdElection::with_endpoints(&opts.server_addr, [&opts.store_addr]).await?),
        )
    };

    let mem_kv = Arc::new(MemStore::default()) as ResetableKvStoreRef;
    let meta_peer_client = MetaPeerClient::new(mem_kv.clone(), election.clone());

    let selector = match opts.selector {
        SelectorType::LoadBased => Arc::new(LoadBasedSelector {
            meta_peer_client: meta_peer_client.clone(),
        }) as SelectorRef,
        SelectorType::LeaseBased => Arc::new(LeaseBasedSelector) as SelectorRef,
    };

    let meta_srv = MetaSrv::new(
        opts,
        kv_store,
        Some(mem_kv),
        Some(selector),
        election,
        None,
        Some(meta_peer_client),
    )
    .await;

    meta_srv.start().await;

    Ok(meta_srv)
}
