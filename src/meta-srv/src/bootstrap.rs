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
#[cfg(any(feature = "pg_kvbackend", feature = "mysql_kvbackend"))]
use common_meta::distributed_time_constants::META_LEASE_SECS;
use common_meta::kv_backend::chroot::ChrootKvBackend;
use common_meta::kv_backend::etcd::EtcdStore;
use common_meta::kv_backend::memory::MemoryKvBackend;
#[cfg(feature = "mysql_kvbackend")]
use common_meta::kv_backend::rds::MySqlStore;
#[cfg(feature = "pg_kvbackend")]
use common_meta::kv_backend::rds::PgStore;
use common_meta::kv_backend::{KvBackendRef, ResettableKvBackendRef};
#[cfg(feature = "pg_kvbackend")]
use common_telemetry::error;
use common_telemetry::info;
#[cfg(feature = "pg_kvbackend")]
use deadpool_postgres::{Config, Runtime};
use etcd_client::Client;
use futures::future;
use servers::configurator::ConfiguratorRef;
use servers::export_metrics::ExportMetricsTask;
use servers::http::{HttpServer, HttpServerBuilder};
use servers::metrics_handler::MetricsHandler;
use servers::server::Server;
#[cfg(any(feature = "pg_kvbackend", feature = "mysql_kvbackend"))]
use snafu::OptionExt;
use snafu::ResultExt;
#[cfg(feature = "mysql_kvbackend")]
use sqlx::mysql::MySqlConnectOptions;
#[cfg(feature = "mysql_kvbackend")]
use sqlx::mysql::{MySqlConnection, MySqlPool};
#[cfg(feature = "mysql_kvbackend")]
use sqlx::Connection;
use tokio::net::TcpListener;
use tokio::sync::mpsc::{self, Receiver, Sender};
#[cfg(feature = "pg_kvbackend")]
use tokio_postgres::NoTls;
use tonic::codec::CompressionEncoding;
use tonic::transport::server::{Router, TcpIncoming};

use crate::election::etcd::EtcdElection;
#[cfg(feature = "mysql_kvbackend")]
use crate::election::mysql::MySqlElection;
#[cfg(feature = "pg_kvbackend")]
use crate::election::postgres::PgElection;
#[cfg(any(feature = "pg_kvbackend", feature = "mysql_kvbackend"))]
use crate::election::CANDIDATE_LEASE_SECS;
use crate::metasrv::builder::MetasrvBuilder;
use crate::metasrv::{BackendImpl, Metasrv, MetasrvOptions, SelectTarget, SelectorRef};
use crate::node_excluder::NodeExcluderRef;
use crate::selector::lease_based::LeaseBasedSelector;
use crate::selector::load_based::LoadBasedSelector;
use crate::selector::round_robin::RoundRobinSelector;
use crate::selector::weight_compute::RegionNumsBasedWeightCompute;
use crate::selector::SelectorType;
use crate::service::admin;
use crate::{error, Result};

pub struct MetasrvInstance {
    metasrv: Arc<Metasrv>,

    http_server: HttpServer,

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
        let http_server = HttpServerBuilder::new(opts.http.clone())
            .with_metrics_handler(MetricsHandler)
            .with_greptime_config_options(opts.to_toml().context(error::TomlFormatSnafu)?)
            .build();

        let metasrv = Arc::new(metasrv);
        // put metasrv into plugins for later use
        plugins.insert::<Arc<Metasrv>>(metasrv.clone());
        let export_metrics_task = ExportMetricsTask::try_new(&opts.export_metrics, Some(&plugins))
            .context(error::InitExportMetricsTaskSnafu)?;
        Ok(MetasrvInstance {
            metasrv,
            http_server,
            opts,
            signal_sender: None,
            plugins,
            export_metrics_task,
        })
    }

    pub async fn start(&mut self) -> Result<()> {
        self.metasrv.try_start().await?;

        if let Some(t) = self.export_metrics_task.as_ref() {
            t.start(None).context(error::InitExportMetricsTaskSnafu)?
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
            self.http_server
                .start(addr)
                .await
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
        self.http_server
            .shutdown()
            .await
            .context(error::ShutdownServerSnafu {
                server: self.http_server.name(),
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
            let pool = create_postgres_pool(opts).await?;
            let kv_backend = PgStore::with_pg_pool(pool, &opts.meta_table_name, opts.max_txn_ops)
                .await
                .context(error::KvBackendSnafu)?;
            // Client for election should be created separately since we need a different session keep-alive idle time.
            let election_client = create_postgres_client(opts).await?;
            let election = PgElection::with_pg_client(
                opts.server_addr.clone(),
                election_client,
                opts.store_key_prefix.clone(),
                CANDIDATE_LEASE_SECS,
                META_LEASE_SECS,
                &opts.meta_table_name,
                opts.meta_election_lock_id,
            )
            .await?;
            (kv_backend, Some(election))
        }
        #[cfg(feature = "mysql_kvbackend")]
        (None, BackendImpl::MysqlStore) => {
            let pool = create_mysql_pool(opts).await?;
            let kv_backend =
                MySqlStore::with_mysql_pool(pool, &opts.meta_table_name, opts.max_txn_ops)
                    .await
                    .context(error::KvBackendSnafu)?;
            // Since election will acquire a lock of the table, we need a separate table for election.
            let election_table_name = opts.meta_table_name.clone() + "_election";
            let election_client = create_mysql_client(opts).await?;
            let election = MySqlElection::with_mysql_client(
                opts.server_addr.clone(),
                election_client,
                opts.store_key_prefix.clone(),
                CANDIDATE_LEASE_SECS,
                META_LEASE_SECS,
                &election_table_name,
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

    let node_excluder = plugins
        .get::<NodeExcluderRef>()
        .unwrap_or_else(|| Arc::new(Vec::new()) as NodeExcluderRef);
    let selector = if let Some(selector) = plugins.get::<SelectorRef>() {
        info!("Using selector from plugins");
        selector
    } else {
        let selector = match opts.selector {
            SelectorType::LoadBased => Arc::new(LoadBasedSelector::new(
                RegionNumsBasedWeightCompute,
                node_excluder,
            )) as SelectorRef,
            SelectorType::LeaseBased => {
                Arc::new(LeaseBasedSelector::new(node_excluder)) as SelectorRef
            }
            SelectorType::RoundRobin => Arc::new(RoundRobinSelector::new(
                SelectTarget::Datanode,
                node_excluder,
            )) as SelectorRef,
        };
        info!(
            "Using selector from options, selector type: {}",
            opts.selector.as_ref()
        );
        selector
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
    let postgres_url = opts
        .store_addrs
        .first()
        .context(error::InvalidArgumentsSnafu {
            err_msg: "empty store addrs",
        })?;
    let (client, connection) = tokio_postgres::connect(postgres_url, NoTls)
        .await
        .context(error::ConnectPostgresSnafu)?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            error!(e; "connection error");
        }
    });
    Ok(client)
}

#[cfg(feature = "pg_kvbackend")]
async fn create_postgres_pool(opts: &MetasrvOptions) -> Result<deadpool_postgres::Pool> {
    let postgres_url = opts
        .store_addrs
        .first()
        .context(error::InvalidArgumentsSnafu {
            err_msg: "empty store addrs",
        })?;
    let mut cfg = Config::new();
    cfg.url = Some(postgres_url.to_string());
    let pool = cfg
        .create_pool(Some(Runtime::Tokio1), NoTls)
        .context(error::CreatePostgresPoolSnafu)?;
    Ok(pool)
}

#[cfg(feature = "mysql_kvbackend")]
async fn setup_mysql_options(opts: &MetasrvOptions) -> Result<MySqlConnectOptions> {
    let mysql_url = opts
        .store_addrs
        .first()
        .context(error::InvalidArgumentsSnafu {
            err_msg: "empty store addrs",
        })?;
    // Avoid `SET` commands in sqlx
    let opts: MySqlConnectOptions = mysql_url
        .parse()
        .context(error::ParseMySqlUrlSnafu { mysql_url })?;
    let opts = opts
        .no_engine_substitution(false)
        .pipes_as_concat(false)
        .timezone(None)
        .set_names(false);
    Ok(opts)
}

#[cfg(feature = "mysql_kvbackend")]
async fn create_mysql_pool(opts: &MetasrvOptions) -> Result<MySqlPool> {
    let opts = setup_mysql_options(opts).await?;
    let pool = MySqlPool::connect_with(opts)
        .await
        .context(error::CreateMySqlPoolSnafu)?;
    Ok(pool)
}

#[cfg(feature = "mysql_kvbackend")]
async fn create_mysql_client(opts: &MetasrvOptions) -> Result<MySqlConnection> {
    let opts = setup_mysql_options(opts).await?;
    let client = MySqlConnection::connect_with(&opts)
        .await
        .context(error::ConnectMySqlSnafu)?;
    Ok(client)
}
