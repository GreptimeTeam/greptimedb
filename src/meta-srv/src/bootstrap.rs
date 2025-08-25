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

use std::net::SocketAddr;
use std::sync::Arc;

use api::v1::meta::cluster_server::ClusterServer;
use api::v1::meta::heartbeat_server::HeartbeatServer;
use api::v1::meta::procedure_service_server::ProcedureServiceServer;
use api::v1::meta::store_server::StoreServer;
use common_base::Plugins;
use common_config::Configurable;
#[cfg(feature = "pg_kvbackend")]
use common_error::ext::BoxedError;
#[cfg(any(feature = "pg_kvbackend", feature = "mysql_kvbackend"))]
use common_meta::distributed_time_constants::META_LEASE_SECS;
use common_meta::kv_backend::chroot::ChrootKvBackend;
use common_meta::kv_backend::etcd::EtcdStore;
use common_meta::kv_backend::memory::MemoryKvBackend;
#[cfg(feature = "pg_kvbackend")]
use common_meta::kv_backend::rds::postgres::create_postgres_tls_connector;
#[cfg(feature = "pg_kvbackend")]
use common_meta::kv_backend::rds::postgres::{TlsMode as PgTlsMode, TlsOption as PgTlsOption};
#[cfg(feature = "mysql_kvbackend")]
use common_meta::kv_backend::rds::MySqlStore;
#[cfg(feature = "pg_kvbackend")]
use common_meta::kv_backend::rds::PgStore;
use common_meta::kv_backend::{KvBackendRef, ResettableKvBackendRef};
use common_telemetry::info;
#[cfg(feature = "pg_kvbackend")]
use deadpool_postgres::{Config, Runtime};
use either::Either;
use etcd_client::{Client, ConnectOptions};
use servers::configurator::ConfiguratorRef;
use servers::export_metrics::ExportMetricsTask;
use servers::http::{HttpServer, HttpServerBuilder};
use servers::metrics_handler::MetricsHandler;
use servers::server::Server;
use servers::tls::TlsOption;
#[cfg(any(feature = "pg_kvbackend", feature = "mysql_kvbackend"))]
use snafu::OptionExt;
use snafu::ResultExt;
#[cfg(feature = "mysql_kvbackend")]
use sqlx::mysql::MySqlConnectOptions;
#[cfg(feature = "mysql_kvbackend")]
use sqlx::mysql::MySqlPool;
use tokio::net::TcpListener;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::{oneshot, Mutex};
#[cfg(feature = "pg_kvbackend")]
use tokio_postgres::NoTls;
use tonic::codec::CompressionEncoding;
use tonic::transport::server::{Router, TcpIncoming};

use crate::election::etcd::EtcdElection;
#[cfg(feature = "mysql_kvbackend")]
use crate::election::rds::mysql::MySqlElection;
#[cfg(feature = "pg_kvbackend")]
use crate::election::rds::postgres::PgElection;
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
use crate::service::admin::admin_axum_router;
use crate::{error, Result};

pub struct MetasrvInstance {
    metasrv: Arc<Metasrv>,

    http_server: Either<Option<HttpServerBuilder>, HttpServer>,

    opts: MetasrvOptions,

    signal_sender: Option<Sender<()>>,

    plugins: Plugins,

    export_metrics_task: Option<ExportMetricsTask>,

    /// gRPC serving state receiver. Only present if the gRPC server is started.
    serve_state: Arc<Mutex<Option<oneshot::Receiver<Result<()>>>>>,

    /// gRPC bind addr
    bind_addr: Option<SocketAddr>,
}

impl MetasrvInstance {
    pub async fn new(metasrv: Metasrv) -> Result<MetasrvInstance> {
        let opts = metasrv.options().clone();
        let plugins = metasrv.plugins().clone();
        let metasrv = Arc::new(metasrv);

        // Wire up the admin_axum_router as an extra router
        let extra_routers = admin_axum_router(metasrv.clone());

        let mut builder = HttpServerBuilder::new(opts.http.clone())
            .with_metrics_handler(MetricsHandler)
            .with_greptime_config_options(opts.to_toml().context(error::TomlFormatSnafu)?);
        builder = builder.with_extra_router(extra_routers);

        // put metasrv into plugins for later use
        plugins.insert::<Arc<Metasrv>>(metasrv.clone());
        let export_metrics_task = ExportMetricsTask::try_new(&opts.export_metrics, Some(&plugins))
            .context(error::InitExportMetricsTaskSnafu)?;
        Ok(MetasrvInstance {
            metasrv,
            http_server: Either::Left(Some(builder)),
            opts,
            signal_sender: None,
            plugins,
            export_metrics_task,
            serve_state: Default::default(),
            bind_addr: None,
        })
    }

    pub async fn start(&mut self) -> Result<()> {
        if let Some(builder) = self.http_server.as_mut().left()
            && let Some(builder) = builder.take()
        {
            let mut server = builder.build();

            let addr = self.opts.http.addr.parse().context(error::ParseAddrSnafu {
                addr: &self.opts.http.addr,
            })?;
            info!("starting http server at {}", addr);
            server.start(addr).await.context(error::StartHttpSnafu)?;

            self.http_server = Either::Right(server);
        } else {
            // If the http server builder is not present, the Metasrv has to be called "start"
            // already, regardless of the startup was successful or not. Return an `Ok` here for
            // simplicity.
            return Ok(());
        };

        self.metasrv.try_start().await?;

        if let Some(t) = self.export_metrics_task.as_ref() {
            t.start(None).context(error::InitExportMetricsTaskSnafu)?
        }

        let (tx, rx) = mpsc::channel::<()>(1);

        self.signal_sender = Some(tx);

        // Start gRPC server with admin services for backward compatibility
        let mut router = router(self.metasrv.clone());
        if let Some(configurator) = self.metasrv.plugins().get::<ConfiguratorRef>() {
            router = configurator.config_grpc(router);
        }

        let (serve_state_tx, serve_state_rx) = oneshot::channel();

        let socket_addr =
            bootstrap_metasrv_with_router(&self.opts.grpc.bind_addr, router, serve_state_tx, rx)
                .await?;
        self.bind_addr = Some(socket_addr);

        *self.serve_state.lock().await = Some(serve_state_rx);
        Ok(())
    }

    pub async fn shutdown(&self) -> Result<()> {
        if let Some(mut rx) = self.serve_state.lock().await.take() {
            if let Ok(Err(err)) = rx.try_recv() {
                common_telemetry::error!(err; "Metasrv start failed")
            }
        }
        if let Some(signal) = &self.signal_sender {
            signal
                .send(())
                .await
                .context(error::SendShutdownSignalSnafu)?;
        }
        self.metasrv.shutdown().await?;

        if let Some(http_server) = self.http_server.as_ref().right() {
            http_server
                .shutdown()
                .await
                .context(error::ShutdownServerSnafu {
                    server: http_server.name(),
                })?;
        }
        Ok(())
    }

    pub fn plugins(&self) -> Plugins {
        self.plugins.clone()
    }

    pub fn get_inner(&self) -> &Metasrv {
        &self.metasrv
    }
    pub fn bind_addr(&self) -> &Option<SocketAddr> {
        &self.bind_addr
    }

    pub fn mut_http_server(&mut self) -> &mut Either<Option<HttpServerBuilder>, HttpServer> {
        &mut self.http_server
    }

    pub fn http_server(&self) -> Option<&HttpServer> {
        self.http_server.as_ref().right()
    }
}

pub async fn bootstrap_metasrv_with_router(
    bind_addr: &str,
    router: Router,
    serve_state_tx: oneshot::Sender<Result<()>>,
    mut shutdown_rx: Receiver<()>,
) -> Result<SocketAddr> {
    let listener = TcpListener::bind(bind_addr)
        .await
        .context(error::TcpBindSnafu { addr: bind_addr })?;

    let real_bind_addr = listener
        .local_addr()
        .context(error::TcpBindSnafu { addr: bind_addr })?;

    info!("gRPC server is bound to: {}", real_bind_addr);

    let incoming = TcpIncoming::from(listener).with_nodelay(Some(true));

    let _handle = common_runtime::spawn_global(async move {
        let result = router
            .serve_with_incoming_shutdown(incoming, async {
                let _ = shutdown_rx.recv().await;
            })
            .await
            .inspect_err(|err| common_telemetry::error!(err;"Failed to start metasrv"))
            .context(error::StartGrpcSnafu);
        let _ = serve_state_tx.send(result);
    });

    Ok(real_bind_addr)
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
            let etcd_client =
                create_etcd_client_with_tls(&opts.store_addrs, opts.backend_tls.as_ref()).await?;
            let kv_backend = EtcdStore::with_etcd_client(etcd_client.clone(), opts.max_txn_ops);
            let election = EtcdElection::with_etcd_client(
                &opts.grpc.server_addr,
                etcd_client,
                opts.store_key_prefix.clone(),
            )
            .await?;

            (kv_backend, Some(election))
        }
        #[cfg(feature = "pg_kvbackend")]
        (None, BackendImpl::PostgresStore) => {
            use std::time::Duration;

            use common_meta::distributed_time_constants::POSTGRES_KEEP_ALIVE_SECS;

            use crate::election::rds::postgres::ElectionPgClient;

            let candidate_lease_ttl = Duration::from_secs(CANDIDATE_LEASE_SECS);
            let execution_timeout = Duration::from_secs(META_LEASE_SECS);
            let statement_timeout = Duration::from_secs(META_LEASE_SECS);
            let idle_session_timeout = Duration::from_secs(META_LEASE_SECS);
            let meta_lease_ttl = Duration::from_secs(META_LEASE_SECS);

            let mut cfg = Config::new();
            cfg.keepalives = Some(true);
            cfg.keepalives_idle = Some(Duration::from_secs(POSTGRES_KEEP_ALIVE_SECS));
            // We use a separate pool for election since we need a different session keep-alive idle time.
            let pool =
                create_postgres_pool_with(&opts.store_addrs, cfg, opts.backend_tls.clone()).await?;

            let election_client = ElectionPgClient::new(
                pool,
                execution_timeout,
                idle_session_timeout,
                statement_timeout,
            )?;
            let election = PgElection::with_pg_client(
                opts.grpc.server_addr.clone(),
                election_client,
                opts.store_key_prefix.clone(),
                candidate_lease_ttl,
                meta_lease_ttl,
                &opts.meta_table_name,
                opts.meta_election_lock_id,
            )
            .await?;

            let pool = create_postgres_pool(&opts.store_addrs, opts.backend_tls.clone()).await?;
            let kv_backend = PgStore::with_pg_pool(pool, &opts.meta_table_name, opts.max_txn_ops)
                .await
                .context(error::KvBackendSnafu)?;

            (kv_backend, Some(election))
        }
        #[cfg(feature = "mysql_kvbackend")]
        (None, BackendImpl::MysqlStore) => {
            use std::time::Duration;

            use crate::election::rds::mysql::ElectionMysqlClient;

            let pool = create_mysql_pool(&opts.store_addrs).await?;
            let kv_backend =
                MySqlStore::with_mysql_pool(pool, &opts.meta_table_name, opts.max_txn_ops)
                    .await
                    .context(error::KvBackendSnafu)?;
            // Since election will acquire a lock of the table, we need a separate table for election.
            let election_table_name = opts.meta_table_name.clone() + "_election";
            // We use a separate pool for election since we need a different session keep-alive idle time.
            let pool = create_mysql_pool(&opts.store_addrs).await?;
            let execution_timeout = Duration::from_secs(META_LEASE_SECS);
            let statement_timeout = Duration::from_secs(META_LEASE_SECS);
            let idle_session_timeout = Duration::from_secs(META_LEASE_SECS);
            let innode_lock_wait_timeout = Duration::from_secs(META_LEASE_SECS / 2);
            let meta_lease_ttl = Duration::from_secs(META_LEASE_SECS);
            let candidate_lease_ttl = Duration::from_secs(CANDIDATE_LEASE_SECS);

            let election_client = ElectionMysqlClient::new(
                pool,
                execution_timeout,
                statement_timeout,
                innode_lock_wait_timeout,
                idle_session_timeout,
                &election_table_name,
            );
            let election = MySqlElection::with_mysql_client(
                opts.grpc.server_addr.clone(),
                election_client,
                opts.store_key_prefix.clone(),
                candidate_lease_ttl,
                meta_lease_ttl,
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

pub async fn create_etcd_client(store_addrs: &[String]) -> Result<Client> {
    create_etcd_client_with_tls(store_addrs, None).await
}

fn build_connection_options(tls_config: Option<&TlsOption>) -> Result<Option<ConnectOptions>> {
    use std::fs;

    use common_telemetry::debug;
    use etcd_client::{Certificate, ConnectOptions, Identity, TlsOptions};
    use servers::tls::TlsMode;

    // If TLS options are not provided, return None
    let Some(tls_config) = tls_config else {
        return Ok(None);
    };
    // If TLS is disabled, return None
    if matches!(tls_config.mode, TlsMode::Disable) {
        return Ok(None);
    }
    let mut etcd_tls_opts = TlsOptions::new();
    // Set CA certificate if provided
    if !tls_config.ca_cert_path.is_empty() {
        debug!("Using CA certificate from {}", tls_config.ca_cert_path);
        let ca_cert_pem = fs::read(&tls_config.ca_cert_path).context(error::FileIoSnafu {
            path: &tls_config.ca_cert_path,
        })?;
        let ca_cert = Certificate::from_pem(ca_cert_pem);
        etcd_tls_opts = etcd_tls_opts.ca_certificate(ca_cert);
    }
    // Set client identity (cert + key) if both are provided
    if !tls_config.cert_path.is_empty() && !tls_config.key_path.is_empty() {
        debug!(
            "Using client certificate from {} and key from {}",
            tls_config.cert_path, tls_config.key_path
        );
        let cert_pem = fs::read(&tls_config.cert_path).context(error::FileIoSnafu {
            path: &tls_config.cert_path,
        })?;
        let key_pem = fs::read(&tls_config.key_path).context(error::FileIoSnafu {
            path: &tls_config.key_path,
        })?;
        let identity = Identity::from_pem(cert_pem, key_pem);
        etcd_tls_opts = etcd_tls_opts.identity(identity);
    }
    // Enable native TLS roots for additional trust anchors
    etcd_tls_opts = etcd_tls_opts.with_native_roots();
    Ok(Some(ConnectOptions::new().with_tls(etcd_tls_opts)))
}

pub async fn create_etcd_client_with_tls(
    store_addrs: &[String],
    tls_config: Option<&TlsOption>,
) -> Result<Client> {
    let etcd_endpoints = store_addrs
        .iter()
        .map(|x| x.trim())
        .filter(|x| !x.is_empty())
        .collect::<Vec<_>>();

    let connect_options = build_connection_options(tls_config)?;

    Client::connect(&etcd_endpoints, connect_options)
        .await
        .context(error::ConnectEtcdSnafu)
}

#[cfg(feature = "pg_kvbackend")]
/// Converts servers::tls::TlsOption to postgres::TlsOption to avoid circular dependencies
fn convert_tls_option(tls_option: &TlsOption) -> PgTlsOption {
    let mode = match tls_option.mode {
        servers::tls::TlsMode::Disable => PgTlsMode::Disable,
        servers::tls::TlsMode::Prefer => PgTlsMode::Prefer,
        servers::tls::TlsMode::Require => PgTlsMode::Require,
        servers::tls::TlsMode::VerifyCa => PgTlsMode::VerifyCa,
        servers::tls::TlsMode::VerifyFull => PgTlsMode::VerifyFull,
    };

    PgTlsOption {
        mode,
        cert_path: tls_option.cert_path.clone(),
        key_path: tls_option.key_path.clone(),
        ca_cert_path: tls_option.ca_cert_path.clone(),
        watch: tls_option.watch,
    }
}

#[cfg(feature = "pg_kvbackend")]
/// Creates a pool for the Postgres backend with optional TLS.
///
/// It only use first store addr to create a pool.
pub async fn create_postgres_pool(
    store_addrs: &[String],
    tls_config: Option<TlsOption>,
) -> Result<deadpool_postgres::Pool> {
    create_postgres_pool_with(store_addrs, Config::new(), tls_config).await
}

#[cfg(feature = "pg_kvbackend")]
/// Creates a pool for the Postgres backend with config and optional TLS.
///
/// It only use first store addr to create a pool, and use the given config to create a pool.
pub async fn create_postgres_pool_with(
    store_addrs: &[String],
    mut cfg: Config,
    tls_config: Option<TlsOption>,
) -> Result<deadpool_postgres::Pool> {
    let postgres_url = store_addrs.first().context(error::InvalidArgumentsSnafu {
        err_msg: "empty store addrs",
    })?;
    cfg.url = Some(postgres_url.to_string());

    let pool = if let Some(tls_config) = tls_config {
        let pg_tls_config = convert_tls_option(&tls_config);
        let tls_connector =
            create_postgres_tls_connector(&pg_tls_config).map_err(|e| error::Error::Other {
                source: BoxedError::new(e),
                location: snafu::Location::new(file!(), line!(), 0),
            })?;
        cfg.create_pool(Some(Runtime::Tokio1), tls_connector)
            .context(error::CreatePostgresPoolSnafu)?
    } else {
        cfg.create_pool(Some(Runtime::Tokio1), NoTls)
            .context(error::CreatePostgresPoolSnafu)?
    };

    Ok(pool)
}

#[cfg(feature = "mysql_kvbackend")]
async fn setup_mysql_options(store_addrs: &[String]) -> Result<MySqlConnectOptions> {
    let mysql_url = store_addrs.first().context(error::InvalidArgumentsSnafu {
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
pub async fn create_mysql_pool(store_addrs: &[String]) -> Result<MySqlPool> {
    let opts = setup_mysql_options(store_addrs).await?;
    let pool = MySqlPool::connect_with(opts)
        .await
        .context(error::CreateMySqlPoolSnafu)?;

    Ok(pool)
}

#[cfg(test)]
mod tests {
    use servers::tls::TlsMode;

    use super::*;

    fn create_test_tls_config(ca_cert_path: &str, cert_path: &str, key_path: &str) -> TlsOption {
        TlsOption {
            mode: TlsMode::Require,
            ca_cert_path: ca_cert_path.to_string(),
            cert_path: cert_path.to_string(),
            key_path: key_path.to_string(),
            watch: false,
        }
    }

    #[tokio::test]
    async fn test_create_etcd_client_with_tls() {
        let cert_dir = std::env::current_dir()
            .unwrap()
            .join("tests-integration")
            .join("fixtures")
            .join("etcd-tls-certs");

        if cert_dir.join("ca.crt").exists()
            && cert_dir.join("client.crt").exists()
            && cert_dir.join("client-key.pem").exists()
        {
            let tls_config = create_test_tls_config(
                &cert_dir.join("ca.crt").to_string_lossy(),
                &cert_dir.join("client.crt").to_string_lossy(),
                &cert_dir.join("client-key.pem").to_string_lossy(),
            );

            let endpoints = vec!["https://localhost:2378".to_string()];
            let result = create_etcd_client_with_tls(&endpoints, Some(&tls_config)).await;

            match result {
                Ok(_) => {
                    // Test passes - TLS client created successfully
                }
                Err(e) => {
                    let error_msg = e.to_string().to_lowercase();
                    let acceptable_errors = [
                        "connection refused",
                        "no route to host",
                        "timeout",
                        "dns",
                        "network unreachable",
                    ];

                    let is_connection_error = acceptable_errors
                        .iter()
                        .any(|&err_type| error_msg.contains(err_type));

                    assert!(
                        is_connection_error,
                        "TLS configuration error (not a network issue): {}",
                        e
                    );
                }
            }
        }
    }
}
