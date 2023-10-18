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
use api::v1::meta::ddl_task_server::DdlTaskServer;
use api::v1::meta::heartbeat_server::HeartbeatServer;
use api::v1::meta::lock_server::LockServer;
use api::v1::meta::router_server::RouterServer;
use api::v1::meta::store_server::StoreServer;
use common_base::Plugins;
use etcd_client::Client;
use servers::configurator::ConfiguratorRef;
use servers::http::{HttpServer, HttpServerBuilder};
use servers::metrics_handler::MetricsHandler;
use servers::server::Server;
use snafu::ResultExt;
use tokio::net::TcpListener;
use tokio::select;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tonic::transport::server::{Router, TcpIncoming};

use crate::election::etcd::EtcdElection;
use crate::lock::etcd::EtcdLock;
use crate::lock::memory::MemLock;
use crate::metasrv::builder::MetaSrvBuilder;
use crate::metasrv::{MetaSrv, MetaSrvOptions, SelectorRef};
use crate::selector::lease_based::LeaseBasedSelector;
use crate::selector::load_based::LoadBasedSelector;
use crate::selector::SelectorType;
use crate::service::admin;
use crate::service::store::etcd::EtcdStore;
use crate::service::store::kv::ResettableKvStoreRef;
use crate::service::store::memory::MemStore;
use crate::{error, Result};

#[derive(Clone)]
pub struct MetaSrvInstance {
    meta_srv: MetaSrv,

    http_srv: Arc<HttpServer>,

    opts: MetaSrvOptions,

    signal_sender: Option<Sender<()>>,

    plugins: Plugins,
}

impl MetaSrvInstance {
    pub async fn new(opts: MetaSrvOptions, plugins: Plugins) -> Result<MetaSrvInstance> {
        let meta_srv = build_meta_srv(&opts, plugins.clone()).await?;
        let http_srv = Arc::new(
            HttpServerBuilder::new(opts.http.clone())
                .with_metrics_handler(MetricsHandler)
                .with_greptime_config_options(opts.to_toml_string())
                .build(),
        );
        // put meta_srv into plugins for later use
        plugins.insert::<Arc<MetaSrv>>(Arc::new(meta_srv.clone()));
        Ok(MetaSrvInstance {
            meta_srv,
            http_srv,
            opts,
            signal_sender: None,
            plugins,
        })
    }

    pub async fn start(&mut self) -> Result<()> {
        self.meta_srv.try_start().await?;

        let (tx, rx) = mpsc::channel::<()>(1);

        self.signal_sender = Some(tx);

        let mut router = router(self.meta_srv.clone());
        if let Some(configurator) = self.meta_srv.plugins().get::<ConfiguratorRef>() {
            router = configurator.config_grpc(router);
        }

        let meta_srv = bootstrap_meta_srv_with_router(&self.opts.bind_addr, router, rx);
        let addr = self.opts.http.addr.parse().context(error::ParseAddrSnafu {
            addr: &self.opts.http.addr,
        })?;
        let http_srv = self.http_srv.start(addr);
        select! {
            v = meta_srv => v?,
            v = http_srv => v.map(|_| ()).context(error::StartHttpSnafu)?,
        }

        Ok(())
    }

    pub async fn shutdown(&self) -> Result<()> {
        if let Some(signal) = &self.signal_sender {
            signal
                .send(())
                .await
                .context(error::SendShutdownSignalSnafu)?;
        }
        self.meta_srv.shutdown().await?;
        self.http_srv
            .shutdown()
            .await
            .context(error::ShutdownServerSnafu {
                server: self.http_srv.name(),
            })?;
        Ok(())
    }

    pub fn plugins(&self) -> Plugins {
        self.plugins.clone()
    }
}

pub async fn bootstrap_meta_srv_with_router(
    bind_addr: &str,
    router: Router,
    mut signal: Receiver<()>,
) -> Result<()> {
    let listener = TcpListener::bind(bind_addr)
        .await
        .context(error::TcpBindSnafu { addr: bind_addr })?;

    let incoming =
        TcpIncoming::from_listener(listener, true, None).context(error::TcpIncomingSnafu)?;

    router
        .serve_with_incoming_shutdown(incoming, async {
            let _ = signal.recv().await;
        })
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
        .add_service(LockServer::new(meta_srv.clone()))
        .add_service(DdlTaskServer::new(meta_srv.clone()))
        .add_service(admin::make_admin_service(meta_srv))
}

pub async fn build_meta_srv(opts: &MetaSrvOptions, plugins: Plugins) -> Result<MetaSrv> {
    let (kv_store, election, lock) = if opts.use_memory_store {
        (
            Arc::new(MemStore::new()) as _,
            None,
            Some(Arc::new(MemLock::default()) as _),
        )
    } else {
        let etcd_endpoints = opts
            .store_addr
            .split(',')
            .map(|x| x.trim())
            .filter(|x| !x.is_empty())
            .collect::<Vec<_>>();
        let etcd_client = Client::connect(&etcd_endpoints, None)
            .await
            .context(error::ConnectEtcdSnafu)?;
        (
            EtcdStore::with_etcd_client(etcd_client.clone()),
            Some(EtcdElection::with_etcd_client(&opts.server_addr, etcd_client.clone()).await?),
            Some(EtcdLock::with_etcd_client(etcd_client)?),
        )
    };

    let in_memory = Arc::new(MemStore::default()) as ResettableKvStoreRef;

    let selector = match opts.selector {
        SelectorType::LoadBased => Arc::new(LoadBasedSelector) as SelectorRef,
        SelectorType::LeaseBased => Arc::new(LeaseBasedSelector) as SelectorRef,
    };

    MetaSrvBuilder::new()
        .options(opts.clone())
        .kv_store(kv_store)
        .in_memory(in_memory)
        .selector(selector)
        .election(election)
        .lock(lock)
        .plugins(plugins)
        .build()
        .await
}
