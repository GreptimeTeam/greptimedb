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
use api::v1::meta::store_server::StoreServer;
use common_base::Plugins;
use common_meta::kv_backend::chroot::ChrootKvBackend;
use common_meta::kv_backend::etcd::EtcdStore;
use common_meta::kv_backend::memory::MemoryKvBackend;
use common_meta::kv_backend::{KvBackendRef, ResettableKvBackendRef};
use common_telemetry::info;
use etcd_client::Client;
use servers::configurator::ConfiguratorRef;
use servers::export_metrics::ExportMetricsTask;
use servers::http::{HttpServer, HttpServerBuilder};
use servers::metrics_handler::MetricsHandler;
use servers::server::Server;
use snafu::ResultExt;
use tokio::net::TcpListener;
use tokio::select;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tonic::transport::server::{Router, TcpIncoming};

use crate::election::etcd::EtcdElection;
use crate::error::InitExportMetricsTaskSnafu;
use crate::lock::etcd::EtcdLock;
use crate::lock::memory::MemLock;
use crate::metasrv::builder::MetaSrvBuilder;
use crate::metasrv::{MetaSrv, MetaSrvOptions, SelectorRef};
use crate::selector::lease_based::LeaseBasedSelector;
use crate::selector::load_based::LoadBasedSelector;
use crate::selector::SelectorType;
use crate::service::admin;
use crate::{error, Result};

#[derive(Clone)]
pub struct MetaSrvInstance {
    meta_srv: MetaSrv,

    http_srv: Arc<HttpServer>,

    opts: MetaSrvOptions,

    signal_sender: Option<Sender<()>>,

    plugins: Plugins,

    export_metrics_task: Option<ExportMetricsTask>,
}

impl MetaSrvInstance {
    pub async fn new(
        opts: MetaSrvOptions,
        plugins: Plugins,
        meta_srv: MetaSrv,
    ) -> Result<MetaSrvInstance> {
        let http_srv = Arc::new(
            HttpServerBuilder::new(opts.http.clone())
                .with_metrics_handler(MetricsHandler)
                .with_greptime_config_options(opts.to_toml_string())
                .build(),
        );
        // put meta_srv into plugins for later use
        plugins.insert::<Arc<MetaSrv>>(Arc::new(meta_srv.clone()));
        let export_metrics_task = ExportMetricsTask::try_new(&opts.export_metrics, Some(&plugins))
            .context(InitExportMetricsTaskSnafu)?;
        Ok(MetaSrvInstance {
            meta_srv,
            http_srv,
            opts,
            signal_sender: None,
            plugins,
            export_metrics_task,
        })
    }

    pub async fn start(&mut self) -> Result<()> {
        self.meta_srv.try_start().await?;

        if let Some(t) = self.export_metrics_task.as_ref() {
            t.start(None).context(InitExportMetricsTaskSnafu)?
        }

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

    info!("gRPC server is bound to: {bind_addr}");

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
        .add_service(StoreServer::new(meta_srv.clone()))
        .add_service(ClusterServer::new(meta_srv.clone()))
        .add_service(LockServer::new(meta_srv.clone()))
        .add_service(DdlTaskServer::new(meta_srv.clone()))
        .add_service(admin::make_admin_service(meta_srv))
}

pub async fn metasrv_builder(
    opts: &MetaSrvOptions,
    plugins: Plugins,
    kv_backend: Option<KvBackendRef>,
) -> Result<MetaSrvBuilder> {
    let (kv_backend, election, lock) = match (kv_backend, opts.use_memory_store) {
        (Some(kv_backend), _) => (kv_backend, None, Some(Arc::new(MemLock::default()) as _)),
        (None, true) => (
            Arc::new(MemoryKvBackend::new()) as _,
            None,
            Some(Arc::new(MemLock::default()) as _),
        ),
        (None, false) => {
            let etcd_client = create_etcd_client(opts).await?;
            let kv_backend = {
                let etcd_backend = EtcdStore::with_etcd_client(etcd_client.clone());
                if !opts.store_key_prefix.is_empty() {
                    Arc::new(ChrootKvBackend::new(
                        opts.store_key_prefix.clone().into_bytes(),
                        etcd_backend,
                    ))
                } else {
                    etcd_backend
                }
            };
            (
                kv_backend,
                Some(
                    EtcdElection::with_etcd_client(
                        &opts.server_addr,
                        etcd_client.clone(),
                        opts.store_key_prefix.clone(),
                    )
                    .await?,
                ),
                Some(EtcdLock::with_etcd_client(
                    etcd_client,
                    opts.store_key_prefix.clone(),
                )?),
            )
        }
    };

    let in_memory = Arc::new(MemoryKvBackend::new()) as ResettableKvBackendRef;

    let selector = match opts.selector {
        SelectorType::LoadBased => Arc::new(LoadBasedSelector::default()) as SelectorRef,
        SelectorType::LeaseBased => Arc::new(LeaseBasedSelector) as SelectorRef,
    };

    Ok(MetaSrvBuilder::new()
        .options(opts.clone())
        .kv_backend(kv_backend)
        .in_memory(in_memory)
        .selector(selector)
        .election(election)
        .lock(lock)
        .plugins(plugins))
}

async fn create_etcd_client(opts: &MetaSrvOptions) -> Result<Client> {
    let etcd_endpoints = opts
        .store_addr
        .split(',')
        .map(|x| x.trim())
        .filter(|x| !x.is_empty())
        .collect::<Vec<_>>();
    Client::connect(&etcd_endpoints, None)
        .await
        .context(error::ConnectEtcdSnafu)
}
