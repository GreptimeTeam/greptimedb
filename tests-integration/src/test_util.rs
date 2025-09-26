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

use std::env;
use std::fmt::Display;
use std::net::SocketAddr;
use std::sync::Arc;

use auth::{DefaultPermissionChecker, PermissionCheckerRef, UserProviderRef};
use axum::Router;
use catalog::kvbackend::KvBackendCatalogManager;
use common_base::Plugins;
use common_config::Configurable;
use common_meta::key::catalog_name::CatalogNameKey;
use common_meta::key::schema_name::SchemaNameKey;
use common_runtime::runtime::BuilderBuild;
use common_runtime::{Builder as RuntimeBuilder, Runtime};
use common_test_util::ports;
use common_test_util::temp_dir::{TempDir, create_temp_dir};
use common_wal::config::DatanodeWalConfig;
use datanode::config::{DatanodeOptions, StorageConfig};
use frontend::instance::Instance;
use frontend::service_config::{MysqlOptions, PostgresOptions};
use object_store::config::{
    AzblobConfig, FileConfig, GcsConfig, ObjectStoreConfig, OssConfig, S3Config,
};
use object_store::services::{Azblob, Gcs, Oss, S3};
use object_store::test_util::TempFolder;
use object_store::{AzblobConnection, GcsConnection, ObjectStore, OssConnection, S3Connection};
use servers::grpc::builder::GrpcServerBuilder;
use servers::grpc::greptime_handler::GreptimeRequestHandler;
use servers::grpc::{FlightCompression, GrpcOptions, GrpcServer, GrpcServerConfig};
use servers::http::{HttpOptions, HttpServerBuilder, PromValidationMode};
use servers::metrics_handler::MetricsHandler;
use servers::mysql::server::{MysqlServer, MysqlSpawnConfig, MysqlSpawnRef};
use servers::otel_arrow::OtelArrowServiceHandler;
use servers::postgres::PostgresServer;
use servers::query_handler::grpc::ServerGrpcQueryHandlerAdapter;
use servers::query_handler::sql::{ServerSqlQueryHandlerAdapter, SqlQueryHandler};
use servers::server::Server;
use servers::tls::ReloadableTlsServerConfig;
use session::context::QueryContext;

use crate::standalone::{GreptimeDbStandalone, GreptimeDbStandaloneBuilder};

pub const PEER_PLACEHOLDER_ADDR: &str = "127.0.0.1:3001";

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum StorageType {
    S3,
    S3WithCache,
    File,
    Oss,
    Azblob,
    Gcs,
}

impl Display for StorageType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageType::S3 => write!(f, "S3"),
            StorageType::S3WithCache => write!(f, "S3"),
            StorageType::File => write!(f, "File"),
            StorageType::Oss => write!(f, "Oss"),
            StorageType::Azblob => write!(f, "Azblob"),
            StorageType::Gcs => write!(f, "Gcs"),
        }
    }
}

impl StorageType {
    pub fn build_storage_types_based_on_env() -> Vec<StorageType> {
        let mut storage_types = Vec::with_capacity(4);
        storage_types.push(StorageType::File);
        if let Ok(bucket) = env::var("GT_S3_BUCKET")
            && !bucket.is_empty()
        {
            storage_types.push(StorageType::S3);
        }
        if env::var("GT_OSS_BUCKET").is_ok() {
            storage_types.push(StorageType::Oss);
        }
        if env::var("GT_AZBLOB_CONTAINER").is_ok() {
            storage_types.push(StorageType::Azblob);
        }
        if env::var("GT_GCS_BUCKET").is_ok() {
            storage_types.push(StorageType::Gcs);
        }
        storage_types
    }

    pub fn test_on(&self) -> bool {
        let _ = dotenv::dotenv();

        match self {
            StorageType::File => true, // always test file
            StorageType::S3 | StorageType::S3WithCache => {
                if let Ok(b) = env::var("GT_S3_BUCKET") {
                    !b.is_empty()
                } else {
                    false
                }
            }
            StorageType::Oss => {
                if let Ok(b) = env::var("GT_OSS_BUCKET") {
                    !b.is_empty()
                } else {
                    false
                }
            }
            StorageType::Azblob => {
                if let Ok(b) = env::var("GT_AZBLOB_CONTAINER") {
                    !b.is_empty()
                } else {
                    false
                }
            }
            StorageType::Gcs => {
                if let Ok(b) = env::var("GT_GCS_BUCKET") {
                    !b.is_empty()
                } else {
                    false
                }
            }
        }
    }
}

fn s3_test_config() -> S3Config {
    S3Config {
        connection: S3Connection {
            root: uuid::Uuid::new_v4().to_string(),
            access_key_id: env::var("GT_S3_ACCESS_KEY_ID").unwrap().into(),
            secret_access_key: env::var("GT_S3_ACCESS_KEY").unwrap().into(),
            bucket: env::var("GT_S3_BUCKET").unwrap(),
            region: Some(env::var("GT_S3_REGION").unwrap()),
            ..Default::default()
        },
        ..Default::default()
    }
}

pub fn get_test_store_config(store_type: &StorageType) -> (ObjectStoreConfig, TempDirGuard) {
    let _ = dotenv::dotenv();

    match store_type {
        StorageType::Gcs => {
            let gcs_config = GcsConfig {
                connection: GcsConnection {
                    root: uuid::Uuid::new_v4().to_string(),
                    bucket: env::var("GT_GCS_BUCKET").unwrap(),
                    scope: env::var("GT_GCS_SCOPE").unwrap(),
                    credential_path: env::var("GT_GCS_CREDENTIAL_PATH").unwrap().into(),
                    credential: env::var("GT_GCS_CREDENTIAL").unwrap().into(),
                    endpoint: env::var("GT_GCS_ENDPOINT").unwrap(),
                },
                ..Default::default()
            };

            let builder = Gcs::from(&gcs_config.connection);
            let config = ObjectStoreConfig::Gcs(gcs_config);
            let store = ObjectStore::new(builder).unwrap().finish();
            (config, TempDirGuard::Gcs(TempFolder::new(&store, "/")))
        }
        StorageType::Azblob => {
            let azblob_config = AzblobConfig {
                connection: AzblobConnection {
                    root: uuid::Uuid::new_v4().to_string(),
                    container: env::var("GT_AZBLOB_CONTAINER").unwrap(),
                    account_name: env::var("GT_AZBLOB_ACCOUNT_NAME").unwrap().into(),
                    account_key: env::var("GT_AZBLOB_ACCOUNT_KEY").unwrap().into(),
                    endpoint: env::var("GT_AZBLOB_ENDPOINT").unwrap(),
                    ..Default::default()
                },
                ..Default::default()
            };

            let builder = Azblob::from(&azblob_config.connection);
            let config = ObjectStoreConfig::Azblob(azblob_config);
            let store = ObjectStore::new(builder).unwrap().finish();
            (config, TempDirGuard::Azblob(TempFolder::new(&store, "/")))
        }
        StorageType::Oss => {
            let oss_config = OssConfig {
                connection: OssConnection {
                    root: uuid::Uuid::new_v4().to_string(),
                    access_key_id: env::var("GT_OSS_ACCESS_KEY_ID").unwrap().into(),
                    access_key_secret: env::var("GT_OSS_ACCESS_KEY").unwrap().into(),
                    bucket: env::var("GT_OSS_BUCKET").unwrap(),
                    endpoint: env::var("GT_OSS_ENDPOINT").unwrap(),
                },
                ..Default::default()
            };

            let builder = Oss::from(&oss_config.connection);
            let config = ObjectStoreConfig::Oss(oss_config);
            let store = ObjectStore::new(builder).unwrap().finish();
            (config, TempDirGuard::Oss(TempFolder::new(&store, "/")))
        }
        StorageType::S3 | StorageType::S3WithCache => {
            let mut s3_config = s3_test_config();

            if *store_type == StorageType::S3WithCache {
                s3_config.cache.cache_path = "/tmp/greptimedb_cache".to_string();
            } else {
                s3_config.cache.enable_read_cache = false;
            }

            let builder = S3::from(&s3_config.connection);
            let config = ObjectStoreConfig::S3(s3_config);
            let store = ObjectStore::new(builder).unwrap().finish();
            (config, TempDirGuard::S3(TempFolder::new(&store, "/")))
        }
        StorageType::File => (ObjectStoreConfig::File(FileConfig {}), TempDirGuard::None),
    }
}

pub enum TempDirGuard {
    None,
    S3(TempFolder),
    Oss(TempFolder),
    Azblob(TempFolder),
    Gcs(TempFolder),
}

pub struct TestGuard {
    pub home_guard: FileDirGuard,
    pub storage_guards: Vec<StorageGuard>,
}

pub struct FileDirGuard {
    pub temp_dir: TempDir,
}

impl FileDirGuard {
    pub fn new(temp_dir: TempDir) -> Self {
        Self { temp_dir }
    }
}

pub struct StorageGuard(pub TempDirGuard);

impl TestGuard {
    pub async fn remove_all(&mut self) {
        for storage_guard in self.storage_guards.iter_mut() {
            if let TempDirGuard::S3(guard)
            | TempDirGuard::Oss(guard)
            | TempDirGuard::Azblob(guard)
            | TempDirGuard::Gcs(guard) = &mut storage_guard.0
            {
                guard.remove_all().await.unwrap()
            }
        }
    }
}

impl Drop for TestGuard {
    fn drop(&mut self) {
        let (tx, rx) = std::sync::mpsc::channel();

        let guards = std::mem::take(&mut self.storage_guards);
        common_runtime::spawn_global(async move {
            let mut errors = vec![];
            for guard in guards {
                if let TempDirGuard::S3(guard)
                | TempDirGuard::Oss(guard)
                | TempDirGuard::Azblob(guard)
                | TempDirGuard::Gcs(guard) = guard.0
                    && let Err(e) = guard.remove_all().await
                {
                    errors.push(e);
                }
            }
            if errors.is_empty() {
                tx.send(Ok(())).unwrap();
            } else {
                tx.send(Err(errors)).unwrap();
            }
        });
        rx.recv().unwrap().unwrap_or_else(|e| panic!("{:?}", e));
    }
}

pub fn create_tmp_dir_and_datanode_opts(
    default_store_type: StorageType,
    store_provider_types: Vec<StorageType>,
    name: &str,
    wal_config: DatanodeWalConfig,
) -> (DatanodeOptions, TestGuard) {
    let home_tmp_dir = create_temp_dir(&format!("gt_data_{name}"));
    let home_dir = home_tmp_dir.path().to_str().unwrap().to_string();

    // Excludes the default object store.
    let mut store_providers = Vec::with_capacity(store_provider_types.len());
    // Includes the default object store.
    let mut storage_guards = Vec::with_capacity(store_provider_types.len() + 1);

    let (default_store, data_tmp_dir) = get_test_store_config(&default_store_type);
    storage_guards.push(StorageGuard(data_tmp_dir));

    for store_type in store_provider_types {
        let (store, data_tmp_dir) = get_test_store_config(&store_type);
        store_providers.push(store);
        storage_guards.push(StorageGuard(data_tmp_dir))
    }
    let opts = create_datanode_opts(default_store, store_providers, home_dir, wal_config);

    (
        opts,
        TestGuard {
            home_guard: FileDirGuard::new(home_tmp_dir),
            storage_guards,
        },
    )
}

pub(crate) fn create_datanode_opts(
    default_store: ObjectStoreConfig,
    providers: Vec<ObjectStoreConfig>,
    home_dir: String,
    wal_config: DatanodeWalConfig,
) -> DatanodeOptions {
    DatanodeOptions {
        node_id: Some(0),
        require_lease_before_startup: true,
        storage: StorageConfig {
            data_home: home_dir,
            providers,
            store: default_store,
        },
        grpc: GrpcOptions::default()
            .with_bind_addr(PEER_PLACEHOLDER_ADDR)
            .with_server_addr(PEER_PLACEHOLDER_ADDR),
        wal: wal_config,
        ..Default::default()
    }
}

pub(crate) async fn create_test_table(instance: &Instance, table_name: &str) {
    let sql = format!(
        r#"
CREATE TABLE IF NOT EXISTS {table_name} (
    host String NOT NULL PRIMARY KEY,
    cpu DOUBLE NULL,
    memory DOUBLE NULL,
    ts TIMESTAMP NOT NULL TIME INDEX,
)
"#
    );

    let result = instance.do_query(&sql, QueryContext::arc()).await;
    let _ = result.first().unwrap().as_ref().unwrap();
}

async fn setup_standalone_instance(
    test_name: &str,
    store_type: StorageType,
) -> GreptimeDbStandalone {
    GreptimeDbStandaloneBuilder::new(test_name)
        .with_default_store_type(store_type)
        .build()
        .await
}

async fn setup_standalone_instance_with_plugins(
    test_name: &str,
    store_type: StorageType,
    plugins: Plugins,
) -> GreptimeDbStandalone {
    GreptimeDbStandaloneBuilder::new(test_name)
        .with_default_store_type(store_type)
        .with_plugin(plugins)
        .build()
        .await
}

pub async fn setup_test_http_app(store_type: StorageType, name: &str) -> (Router, TestGuard) {
    let instance = setup_standalone_instance(name, store_type).await;

    let http_opts = HttpOptions {
        addr: format!("127.0.0.1:{}", ports::get_port()),
        ..Default::default()
    };
    let http_server = HttpServerBuilder::new(http_opts)
        .with_sql_handler(ServerSqlQueryHandlerAdapter::arc(
            instance.fe_instance().clone(),
        ))
        .with_logs_handler(instance.fe_instance().clone())
        .with_metrics_handler(MetricsHandler)
        .with_greptime_config_options(instance.opts.datanode_options().to_toml().unwrap())
        .build();
    (
        http_server.build(http_server.make_app()).unwrap(),
        instance.guard,
    )
}

pub async fn setup_test_http_app_with_frontend(
    store_type: StorageType,
    name: &str,
) -> (Router, TestGuard) {
    setup_test_http_app_with_frontend_and_user_provider(store_type, name, None).await
}

pub async fn setup_test_http_app_with_frontend_and_user_provider(
    store_type: StorageType,
    name: &str,
    user_provider: Option<UserProviderRef>,
) -> (Router, TestGuard) {
    let plugins = Plugins::new();
    if let Some(user_provider) = user_provider.clone() {
        plugins.insert::<UserProviderRef>(user_provider.clone());
        plugins.insert::<PermissionCheckerRef>(DefaultPermissionChecker::arc());
    }

    let instance = setup_standalone_instance_with_plugins(name, store_type, plugins).await;

    create_test_table(instance.fe_instance(), "demo").await;

    let http_opts = HttpOptions {
        addr: format!("127.0.0.1:{}", ports::get_port()),
        ..Default::default()
    };

    let mut http_server = HttpServerBuilder::new(http_opts);

    http_server = http_server
        .with_sql_handler(ServerSqlQueryHandlerAdapter::arc(
            instance.fe_instance().clone(),
        ))
        .with_log_ingest_handler(instance.fe_instance().clone(), None, None)
        .with_logs_handler(instance.fe_instance().clone())
        .with_influxdb_handler(instance.fe_instance().clone())
        .with_otlp_handler(instance.fe_instance().clone(), true)
        .with_jaeger_handler(instance.fe_instance().clone())
        .with_greptime_config_options(instance.opts.to_toml().unwrap());

    if let Some(user_provider) = user_provider {
        http_server = http_server.with_user_provider(user_provider);
    }

    let http_server = http_server.build();

    let app = http_server.build(http_server.make_app()).unwrap();
    (app, instance.guard)
}

async fn run_sql(sql: &str, instance: &GreptimeDbStandalone) {
    let result = instance
        .fe_instance()
        .do_query(sql, QueryContext::arc())
        .await;
    let _ = result.first().unwrap().as_ref().unwrap();
}

pub async fn setup_test_prom_app_with_frontend(
    store_type: StorageType,
    name: &str,
) -> (Router, TestGuard) {
    unsafe {
        std::env::set_var("TZ", "UTC");
    }

    let instance = setup_standalone_instance(name, store_type).await;

    // build physical table
    let sql = "CREATE TABLE phy (ts timestamp time index, val double, host string primary key) engine=metric with ('physical_metric_table' = '')";
    run_sql(sql, &instance).await;
    let sql = "CREATE TABLE phy_ns (ts timestamp(0) time index, val double, host string primary key) engine=metric with ('physical_metric_table' = '')";
    run_sql(sql, &instance).await;
    // build metric tables
    let sql = "CREATE TABLE demo (ts timestamp time index, val double, host string primary key) engine=metric with ('on_physical_table' = 'phy')";
    run_sql(sql, &instance).await;
    let sql = "CREATE TABLE demo_metrics (ts timestamp time index, val double, idc string primary key) engine=metric with ('on_physical_table' = 'phy')";
    run_sql(sql, &instance).await;
    let sql = "CREATE TABLE multi_labels (ts timestamp(0) time index, val double, idc string, env string, host string, primary key (idc, env, host)) engine=metric with ('on_physical_table' = 'phy_ns')";
    run_sql(sql, &instance).await;

    // insert rows
    let sql = "INSERT INTO demo(host, val, ts) VALUES ('host1', 1.1, 0), ('host2', 2.1, 600000)";
    run_sql(sql, &instance).await;
    let sql =
        "INSERT INTO demo_metrics(idc, val, ts) VALUES ('idc1', 1.1, 0), ('idc2', 2.1, 600000)";
    run_sql(sql, &instance).await;
    // insert a row with empty label
    let sql = "INSERT INTO demo_metrics(val, ts) VALUES (1.1, 0)";
    run_sql(sql, &instance).await;

    // insert rows to multi_labels
    let sql = "INSERT INTO multi_labels(idc, env, host, val, ts) VALUES ('idc1', 'dev', 'host1', 1.1, 0), ('idc1', 'dev', 'host2', 2.1, 0), ('idc2', 'dev', 'host1', 1.1, 0), ('idc2', 'test', 'host3', 2.1, 0)";
    run_sql(sql, &instance).await;

    // build physical table
    let sql = "CREATE TABLE phy2 (ts timestamp(9) time index, val double, host string primary key) engine=metric with ('physical_metric_table' = '')";
    run_sql(sql, &instance).await;
    let sql = "CREATE TABLE demo_metrics_with_nanos(ts timestamp(9) time index, val double, idc string primary key) engine=metric with ('on_physical_table' = 'phy2')";
    run_sql(sql, &instance).await;
    let sql = "INSERT INTO demo_metrics_with_nanos(idc, val, ts) VALUES ('idc1', 1.1, 0)";
    run_sql(sql, &instance).await;

    // a mito table with non-prometheus compatible values
    let sql = "CREATE TABLE mito (ts timestamp(9) time index, val double, host bigint primary key) engine=mito";
    run_sql(sql, &instance).await;
    let sql = "INSERT INTO mito(host, val, ts) VALUES (1, 1.1, 0)";
    run_sql(sql, &instance).await;

    let http_opts = HttpOptions {
        addr: format!("127.0.0.1:{}", ports::get_port()),
        ..Default::default()
    };
    let frontend_ref = instance.fe_instance().clone();
    let http_server = HttpServerBuilder::new(http_opts)
        .with_sql_handler(ServerSqlQueryHandlerAdapter::arc(frontend_ref.clone()))
        .with_logs_handler(instance.fe_instance().clone())
        .with_prom_handler(
            frontend_ref.clone(),
            Some(frontend_ref.clone()),
            true,
            PromValidationMode::Strict,
        )
        .with_prometheus_handler(frontend_ref)
        .with_greptime_config_options(instance.opts.datanode_options().to_toml().unwrap())
        .build();
    let app = http_server.build(http_server.make_app()).unwrap();
    (app, instance.guard)
}

pub async fn setup_grpc_server(
    store_type: StorageType,
    name: &str,
) -> (GreptimeDbStandalone, Arc<GrpcServer>) {
    setup_grpc_server_with(store_type, name, None, None).await
}

pub async fn setup_grpc_server_with_user_provider(
    store_type: StorageType,
    name: &str,
    user_provider: Option<UserProviderRef>,
) -> (GreptimeDbStandalone, Arc<GrpcServer>) {
    setup_grpc_server_with(store_type, name, user_provider, None).await
}

pub async fn setup_grpc_server_with(
    store_type: StorageType,
    name: &str,
    user_provider: Option<UserProviderRef>,
    grpc_config: Option<GrpcServerConfig>,
) -> (GreptimeDbStandalone, Arc<GrpcServer>) {
    let instance = setup_standalone_instance(name, store_type).await;

    let runtime: Runtime = RuntimeBuilder::default()
        .worker_threads(2)
        .thread_name("grpc-handlers")
        .build()
        .unwrap();

    let fe_instance_ref = instance.fe_instance().clone();

    let greptime_request_handler = GreptimeRequestHandler::new(
        ServerGrpcQueryHandlerAdapter::arc(fe_instance_ref.clone()),
        user_provider.clone(),
        Some(runtime.clone()),
        FlightCompression::default(),
    );

    let flight_handler = Arc::new(greptime_request_handler.clone());

    let grpc_config = grpc_config.unwrap_or_default();
    let grpc_builder = GrpcServerBuilder::new(grpc_config.clone(), runtime)
        .database_handler(greptime_request_handler)
        .flight_handler(flight_handler)
        .prometheus_handler(fe_instance_ref.clone(), user_provider.clone())
        .otel_arrow_handler(OtelArrowServiceHandler::new(fe_instance_ref, user_provider))
        .with_tls_config(grpc_config.tls)
        .unwrap();

    let mut grpc_server = grpc_builder.build();

    let fe_grpc_addr = "127.0.0.1:0".parse::<SocketAddr>().unwrap();
    grpc_server.start(fe_grpc_addr).await.unwrap();

    (instance, Arc::new(grpc_server))
}

pub async fn setup_mysql_server(
    store_type: StorageType,
    name: &str,
) -> (TestGuard, Arc<Box<dyn Server>>) {
    setup_mysql_server_with_user_provider(store_type, name, None).await
}

pub async fn setup_mysql_server_with_user_provider(
    store_type: StorageType,
    name: &str,
    user_provider: Option<UserProviderRef>,
) -> (TestGuard, Arc<Box<dyn Server>>) {
    let plugins = Plugins::new();
    if let Some(user_provider) = user_provider.clone() {
        plugins.insert::<UserProviderRef>(user_provider.clone());
        plugins.insert::<PermissionCheckerRef>(DefaultPermissionChecker::arc());
    }

    let instance = setup_standalone_instance_with_plugins(name, store_type, plugins).await;

    let runtime = RuntimeBuilder::default()
        .worker_threads(2)
        .thread_name("mysql-runtime")
        .build()
        .unwrap();

    let fe_mysql_addr = format!("127.0.0.1:{}", ports::get_port());

    let fe_instance_ref = instance.fe_instance().clone();
    let opts = MysqlOptions {
        addr: fe_mysql_addr.clone(),
        ..Default::default()
    };
    let mut mysql_server = MysqlServer::create_server(
        runtime,
        Arc::new(MysqlSpawnRef::new(
            ServerSqlQueryHandlerAdapter::arc(fe_instance_ref),
            user_provider,
        )),
        Arc::new(MysqlSpawnConfig::new(
            false,
            Arc::new(
                ReloadableTlsServerConfig::try_new(opts.tls.clone())
                    .expect("Failed to load certificates and keys"),
            ),
            0,
            opts.reject_no_database.unwrap_or(false),
            opts.prepared_stmt_cache_size,
        )),
        None,
    );

    mysql_server
        .start(fe_mysql_addr.parse::<SocketAddr>().unwrap())
        .await
        .unwrap();

    (instance.guard, Arc::new(mysql_server))
}

pub async fn setup_pg_server(
    store_type: StorageType,
    name: &str,
) -> (TestGuard, Arc<Box<dyn Server>>) {
    setup_pg_server_with_user_provider(store_type, name, None).await
}

pub async fn setup_pg_server_with_user_provider(
    store_type: StorageType,
    name: &str,
    user_provider: Option<UserProviderRef>,
) -> (TestGuard, Arc<Box<dyn Server>>) {
    let instance = setup_standalone_instance(name, store_type).await;

    let runtime = RuntimeBuilder::default()
        .worker_threads(2)
        .thread_name("pg-runtime")
        .build()
        .unwrap();

    let fe_pg_addr = format!("127.0.0.1:{}", ports::get_port());

    let fe_instance_ref = instance.fe_instance().clone();
    let opts = PostgresOptions {
        addr: fe_pg_addr.clone(),
        ..Default::default()
    };
    let tls_server_config = Arc::new(
        ReloadableTlsServerConfig::try_new(opts.tls.clone())
            .expect("Failed to load certificates and keys"),
    );

    let mut pg_server = Box::new(PostgresServer::new(
        ServerSqlQueryHandlerAdapter::arc(fe_instance_ref),
        opts.tls.should_force_tls(),
        tls_server_config,
        0,
        runtime,
        user_provider,
        None,
    ));

    pg_server
        .start(fe_pg_addr.parse::<SocketAddr>().unwrap())
        .await
        .unwrap();

    (instance.guard, Arc::new(pg_server))
}

pub(crate) async fn prepare_another_catalog_and_schema(instance: &Instance) {
    let catalog_manager = instance
        .catalog_manager()
        .as_any()
        .downcast_ref::<KvBackendCatalogManager>()
        .unwrap();

    let table_metadata_manager = catalog_manager.table_metadata_manager_ref();
    table_metadata_manager
        .catalog_manager()
        .create(CatalogNameKey::new("another_catalog"), true)
        .await
        .unwrap();
    table_metadata_manager
        .schema_manager()
        .create(
            SchemaNameKey::new("another_catalog", "another_schema"),
            None,
            true,
        )
        .await
        .unwrap();
}
