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
use api::v1::meta::procedure_service_server::ProcedureServiceServer;
use api::v1::meta::store_server::StoreServer;
use common_base::Plugins;
use common_config::Configurable;
use common_meta::kv_backend::chroot::ChrootKvBackend;
use common_meta::kv_backend::etcd::EtcdStore;
use common_meta::kv_backend::memory::MemoryKvBackend;
#[cfg(feature = "pg_kvbackend")]
use common_meta::kv_backend::postgres::PgStore;
use common_meta::kv_backend::{KvBackendRef, ResettableKvBackendRef};
use common_telemetry::info;
use etcd_client::Client;
use futures::future;
use servers::configurator::ConfiguratorRef;
use servers::export_metrics::ExportMetricsTask;
use servers::http::{HttpServer, HttpServerBuilder};
use servers::metrics_handler::MetricsHandler;
use servers::server::Server;
#[cfg(feature = "pg_kvbackend")]
use snafu::OptionExt;
use snafu::ResultExt;
use tokio::net::TcpListener;
use tokio::sync::mpsc::{self, Receiver, Sender};
#[cfg(feature = "pg_kvbackend")]
use tokio_postgres::NoTls;
use tonic::codec::CompressionEncoding;
use tonic::transport::server::{Router, TcpIncoming};

use crate::election::etcd::EtcdElection;
#[cfg(feature = "pg_kvbackend")]
use crate::election::postgres::PgElection;
#[cfg(feature = "pg_kvbackend")]
use crate::error::InvalidArgumentsSnafu;
use crate::error::{InitExportMetricsTaskSnafu, TomlFormatSnafu};
use crate::metasrv::builder::MetasrvBuilder;
use crate::metasrv::{BackendImpl, Metasrv, MetasrvOptions, SelectorRef};
use crate::selector::lease_based::LeaseBasedSelector;
use crate::selector::load_based::LoadBasedSelector;
use crate::selector::round_robin::RoundRobinSelector;
use crate::selector::SelectorType;
use crate::service::admin;
use crate::{error, Result};

pub struct MetasrvInstance {
    metasrv: Arc<Metasrv>,

    httpsrv: Arc<HttpServer>,

    opts: MetasrvOptions,

    signal_sender: Option<Sender<()>>,

    plugins: Plugins,

    export_metrics_task: Option<ExportMetricsTask>,
}

impl MetasrvInstance {
    pub async fn new(
        opts: MetasrvOptions,
        plugins: Plugins,
        metasrv: Metasrv,
    ) -> Result<MetasrvInstance> {
        let httpsrv = Arc::new(
            HttpServerBuilder::new(opts.http.clone())
                .with_metrics_handler(MetricsHandler)
                .with_greptime_config_options(opts.to_toml().context(TomlFormatSnafu)?)
                .build(),
        );
        let metasrv = Arc::new(metasrv);
        // put metasrv into plugins for later use
        plugins.insert::<Arc<Metasrv>>(metasrv.clone());
        let export_metrics_task = ExportMetricsTask::try_new(&opts.export_metrics, Some(&plugins))
            .context(InitExportMetricsTaskSnafu)?;
        Ok(MetasrvInstance {
            metasrv,
            httpsrv,
            opts,
            signal_sender: None,
            plugins,
            export_metrics_task,
        })
    }

    pub async fn start(&mut self) -> Result<()> {
        self.metasrv.try_start().await?;

        if let Some(t) = self.export_metrics_task.as_ref() {
            t.start(None).context(InitExportMetricsTaskSnafu)?
        }

        let (tx, rx) = mpsc::channel::<()>(1);

        self.signal_sender = Some(tx);

        let mut router = router(self.metasrv.clone());
        if let Some(configurator) = self.metasrv.plugins().get::<ConfiguratorRef>() {
            router = configurator.config_grpc(router);
        }

        let metasrv = bootstrap_metasrv_with_router(&self.opts.bind_addr, router, rx);
        let addr = self.opts.http.addr.parse().context(error::ParseAddrSnafu {
            addr: &self.opts.http.addr,
        })?;
        let http_srv = async {
            self.httpsrv
                .start(addr)
                .await
                .map(|_| ())
                .context(error::StartHttpSnafu)
        };
        future::try_join(metasrv, http_srv).await?;
        Ok(())
    }

    pub async fn shutdown(&self) -> Result<()> {
        if let Some(signal) = &self.signal_sender {
            signal
                .send(())
                .await
                .context(error::SendShutdownSignalSnafu)?;
        }
        self.metasrv.shutdown().await?;
        self.httpsrv
            .shutdown()
            .await
            .context(error::ShutdownServerSnafu {
                server: self.httpsrv.name(),
            })?;
        Ok(())
    }

    pub fn plugins(&self) -> Plugins {
        self.plugins.clone()
    }

    pub fn get_inner(&self) -> &Metasrv {
        &self.metasrv
    }
}

pub async fn bootstrap_metasrv_with_router(
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

#[macro_export]
macro_rules! add_compressed_service {
    ($builder:expr, $server:expr) => {
        $builder.add_service(
            $server
                .accept_compressed(CompressionEncoding::Gzip)
                .accept_compressed(CompressionEncoding::Zstd)
                .send_compressed(CompressionEncoding::Gzip)
                .send_compressed(CompressionEncoding::Zstd),
        )
    };
}

pub fn router(metasrv: Arc<Metasrv>) -> Router {
    let mut router = tonic::transport::Server::builder().accept_http1(true); // for admin services
    let router = add_compressed_service!(router, HeartbeatServer::from_arc(metasrv.clone()));
    let router = add_compressed_service!(router, StoreServer::from_arc(metasrv.clone()));
    let router = add_compressed_service!(router, ClusterServer::from_arc(metasrv.clone()));
    let router = add_compressed_service!(router, ProcedureServiceServer::from_arc(metasrv.clone()));
    router.add_service(admin::make_admin_service(metasrv))
}

pub async fn metasrv_builder(
    opts: &MetasrvOptions,
    plugins: Plugins,
    kv_backend: Option<KvBackendRef>,
) -> Result<MetasrvBuilder> {
    let (mut kv_backend, election) = match (kv_backend, &opts.backend) {
        (Some(kv_backend), _) => (kv_backend, None),
        (None, BackendImpl::MemoryStore) => (Arc::new(MemoryKvBackend::new()) as _, None),
        (None, BackendImpl::EtcdStore) => {
            let etcd_client = create_etcd_client(opts).await?;
            let kv_backend = EtcdStore::with_etcd_client(etcd_client.clone(), opts.max_txn_ops);
            let election = EtcdElection::with_etcd_client(
                &opts.server_addr,
                etcd_client,
                opts.store_key_prefix.clone(),
            )
            .await?;

            (kv_backend, Some(election))
        }
        #[cfg(feature = "pg_kvbackend")]
        (None, BackendImpl::PostgresStore) => {
            let pg_client = create_postgres_client(opts).await?;
            let kv_backend = PgStore::with_pg_client(pg_client).await.unwrap();
            let election_client = create_postgres_client(opts).await?;
            let election = PgElection::with_pg_client(
                opts.server_addr.clone(),
                election_client,
                opts.store_key_prefix.clone(),
            )
            .await?;
            (kv_backend, Some(election))
        }
    };

    if !opts.store_key_prefix.is_empty() {
        info!(
            "using chroot kv backend with prefix: {prefix}",
            prefix = opts.store_key_prefix
        );
        kv_backend = Arc::new(ChrootKvBackend::new(
            opts.store_key_prefix.clone().into_bytes(),
            kv_backend,
        ))
    }

    let in_memory = Arc::new(MemoryKvBackend::new()) as ResettableKvBackendRef;

    let selector = match opts.selector {
        SelectorType::LoadBased => Arc::new(LoadBasedSelector::default()) as SelectorRef,
        SelectorType::LeaseBased => Arc::new(LeaseBasedSelector) as SelectorRef,
        SelectorType::RoundRobin => Arc::new(RoundRobinSelector::default()) as SelectorRef,
    };

    Ok(MetasrvBuilder::new()
        .options(opts.clone())
        .kv_backend(kv_backend)
        .in_memory(in_memory)
        .selector(selector)
        .election(election)
        .plugins(plugins))
}

async fn create_etcd_client(opts: &MetasrvOptions) -> Result<Client> {
    let etcd_endpoints = opts
        .store_addrs
        .iter()
        .map(|x| x.trim())
        .filter(|x| !x.is_empty())
        .collect::<Vec<_>>();
    Client::connect(&etcd_endpoints, None)
        .await
        .context(error::ConnectEtcdSnafu)
}

#[cfg(feature = "pg_kvbackend")]
async fn create_postgres_client(opts: &MetasrvOptions) -> Result<tokio_postgres::Client> {
    let postgres_url = opts.store_addrs.first().context(InvalidArgumentsSnafu {
        err_msg: "empty store addrs",
    })?;
    let (client, _) = tokio_postgres::connect(postgres_url, NoTls)
        .await
        .context(error::ConnectPostgresSnafu)?;
    Ok(client)
}
