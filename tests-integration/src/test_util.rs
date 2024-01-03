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
use std::time::Duration;

use auth::UserProviderRef;
use axum::Router;
use catalog::kvbackend::KvBackendCatalogManager;
use common_config::WalConfig;
use common_meta::key::catalog_name::CatalogNameKey;
use common_meta::key::schema_name::SchemaNameKey;
use common_query::Output;
use common_recordbatch::util;
use common_runtime::Builder as RuntimeBuilder;
use common_telemetry::warn;
use common_test_util::ports;
use common_test_util::temp_dir::{create_temp_dir, TempDir};
use datanode::config::{
    AzblobConfig, DatanodeOptions, FileConfig, GcsConfig, ObjectStoreConfig, OssConfig, S3Config,
    StorageConfig,
};
use frontend::frontend::TomlSerializable;
use frontend::instance::Instance;
use frontend::service_config::{MysqlOptions, PostgresOptions};
use futures::future::BoxFuture;
use object_store::services::{Azblob, Gcs, Oss, S3};
use object_store::test_util::TempFolder;
use object_store::ObjectStore;
use secrecy::ExposeSecret;
use servers::grpc::greptime_handler::GreptimeRequestHandler;
use servers::grpc::{GrpcServer, GrpcServerConfig};
use servers::http::{HttpOptions, HttpServerBuilder};
use servers::metrics_handler::MetricsHandler;
use servers::mysql::server::{MysqlServer, MysqlSpawnConfig, MysqlSpawnRef};
use servers::postgres::PostgresServer;
use servers::query_handler::grpc::ServerGrpcQueryHandlerAdapter;
use servers::query_handler::sql::{ServerSqlQueryHandlerAdapter, SqlQueryHandler};
use servers::server::Server;
use servers::Mode;
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
        if let Ok(bucket) = env::var("GT_S3_BUCKET") {
            if !bucket.is_empty() {
                storage_types.push(StorageType::S3);
            }
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
        root: uuid::Uuid::new_v4().to_string(),
        access_key_id: env::var("GT_S3_ACCESS_KEY_ID").unwrap().into(),
        secret_access_key: env::var("GT_S3_ACCESS_KEY").unwrap().into(),
        bucket: env::var("GT_S3_BUCKET").unwrap(),
        region: Some(env::var("GT_S3_REGION").unwrap()),
        ..Default::default()
    }
}

pub fn get_test_store_config(store_type: &StorageType) -> (ObjectStoreConfig, TempDirGuard) {
    let _ = dotenv::dotenv();

    match store_type {
        StorageType::Gcs => {
            let gcs_config = GcsConfig {
                root: uuid::Uuid::new_v4().to_string(),
                bucket: env::var("GT_GCS_BUCKET").unwrap(),
                scope: env::var("GT_GCS_SCOPE").unwrap(),
                credential_path: env::var("GT_GCS_CREDENTIAL_PATH").unwrap().into(),
                endpoint: env::var("GT_GCS_ENDPOINT").unwrap(),
                ..Default::default()
            };

            let mut builder = Gcs::default();
            builder
                .root(&gcs_config.root)
                .bucket(&gcs_config.bucket)
                .scope(&gcs_config.scope)
                .credential_path(gcs_config.credential_path.expose_secret())
                .endpoint(&gcs_config.endpoint);

            let config = ObjectStoreConfig::Gcs(gcs_config);
            let store = ObjectStore::new(builder).unwrap().finish();
            (config, TempDirGuard::Gcs(TempFolder::new(&store, "/")))
        }
        StorageType::Azblob => {
            let azblob_config = AzblobConfig {
                root: uuid::Uuid::new_v4().to_string(),
                container: env::var("GT_AZBLOB_CONTAINER").unwrap(),
                account_name: env::var("GT_AZBLOB_ACCOUNT_NAME").unwrap().into(),
                account_key: env::var("GT_AZBLOB_ACCOUNT_KEY").unwrap().into(),
                endpoint: env::var("GT_AZBLOB_ENDPOINT").unwrap(),
                ..Default::default()
            };

            let mut builder = Azblob::default();
            let _ = builder
                .root(&azblob_config.root)
                .endpoint(&azblob_config.endpoint)
                .account_name(azblob_config.account_name.expose_secret())
                .account_key(azblob_config.account_key.expose_secret())
                .container(&azblob_config.container);

            if let Ok(sas_token) = env::var("GT_AZBLOB_SAS_TOKEN") {
                let _ = builder.sas_token(&sas_token);
            }

            let config = ObjectStoreConfig::Azblob(azblob_config);

            let store = ObjectStore::new(builder).unwrap().finish();

            (config, TempDirGuard::Azblob(TempFolder::new(&store, "/")))
        }
        StorageType::Oss => {
            let oss_config = OssConfig {
                root: uuid::Uuid::new_v4().to_string(),
                access_key_id: env::var("GT_OSS_ACCESS_KEY_ID").unwrap().into(),
                access_key_secret: env::var("GT_OSS_ACCESS_KEY").unwrap().into(),
                bucket: env::var("GT_OSS_BUCKET").unwrap(),
                endpoint: env::var("GT_OSS_ENDPOINT").unwrap(),
                ..Default::default()
            };

            let mut builder = Oss::default();
            let _ = builder
                .root(&oss_config.root)
                .endpoint(&oss_config.endpoint)
                .access_key_id(oss_config.access_key_id.expose_secret())
                .access_key_secret(oss_config.access_key_secret.expose_secret())
                .bucket(&oss_config.bucket);

            let config = ObjectStoreConfig::Oss(oss_config);

            let store = ObjectStore::new(builder).unwrap().finish();

            (config, TempDirGuard::Oss(TempFolder::new(&store, "/")))
        }
        StorageType::S3 | StorageType::S3WithCache => {
            let mut s3_config = s3_test_config();

            if *store_type == StorageType::S3WithCache {
                s3_config.cache.cache_path = Some("/tmp/greptimedb_cache".to_string());
            }

            let mut builder = S3::default();
            let _ = builder
                .root(&s3_config.root)
                .access_key_id(s3_config.access_key_id.expose_secret())
                .secret_access_key(s3_config.secret_access_key.expose_secret())
                .bucket(&s3_config.bucket);

            if s3_config.endpoint.is_some() {
                let _ = builder.endpoint(s3_config.endpoint.as_ref().unwrap());
            }
            if s3_config.region.is_some() {
                let _ = builder.region(s3_config.region.as_ref().unwrap());
            }

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

pub fn create_tmp_dir_and_datanode_opts(
    mode: Mode,
    default_store_type: StorageType,
    store_provider_types: Vec<StorageType>,
    name: &str,
    wal_config: WalConfig,
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
    let opts = create_datanode_opts(mode, default_store, store_providers, home_dir, wal_config);

    (
        opts,
        TestGuard {
            home_guard: FileDirGuard::new(home_tmp_dir),
            storage_guards,
        },
    )
}

pub(crate) fn create_datanode_opts(
    mode: Mode,
    default_store: ObjectStoreConfig,
    providers: Vec<ObjectStoreConfig>,
    home_dir: String,
    wal_config: WalConfig,
) -> DatanodeOptions {
    DatanodeOptions {
        node_id: Some(0),
        require_lease_before_startup: true,
        storage: StorageConfig {
            data_home: home_dir,
            providers,
            store: default_store,
            ..Default::default()
        },
        mode,
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

pub async fn setup_test_http_app(store_type: StorageType, name: &str) -> (Router, TestGuard) {
    let instance = setup_standalone_instance(name, store_type).await;

    let http_opts = HttpOptions {
        addr: format!("127.0.0.1:{}", ports::get_port()),
        ..Default::default()
    };
    let http_server = HttpServerBuilder::new(http_opts)
        .with_sql_handler(ServerSqlQueryHandlerAdapter::arc(instance.instance.clone()))
        .with_grpc_handler(ServerGrpcQueryHandlerAdapter::arc(
            instance.instance.clone(),
        ))
        .with_metrics_handler(MetricsHandler)
        .with_greptime_config_options(instance.datanode_opts.to_toml_string())
        .build();
    (http_server.build(http_server.make_app()), instance.guard)
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
    let instance = setup_standalone_instance(name, store_type).await;

    create_test_table(instance.instance.as_ref(), "demo").await;

    let http_opts = HttpOptions {
        addr: format!("127.0.0.1:{}", ports::get_port()),
        ..Default::default()
    };

    let mut http_server = HttpServerBuilder::new(http_opts);

    http_server
        .with_sql_handler(ServerSqlQueryHandlerAdapter::arc(instance.instance.clone()))
        .with_grpc_handler(ServerGrpcQueryHandlerAdapter::arc(
            instance.instance.clone(),
        ))
        .with_script_handler(instance.instance.clone())
        .with_greptime_config_options(instance.mix_options.to_toml().unwrap());

    if let Some(user_provider) = user_provider {
        http_server.with_user_provider(user_provider);
    }

    let http_server = http_server.build();

    let app = http_server.build(http_server.make_app());
    (app, instance.guard)
}

pub async fn setup_test_prom_app_with_frontend(
    store_type: StorageType,
    name: &str,
) -> (Router, TestGuard) {
    std::env::set_var("TZ", "UTC");

    let instance = setup_standalone_instance(name, store_type).await;

    create_test_table(instance.instance.as_ref(), "demo").await;

    let sql = "INSERT INTO demo VALUES ('host1', 1.1, 2.2, 0), ('host2', 2.1, 4.3, 600000)";
    let result = instance.instance.do_query(sql, QueryContext::arc()).await;
    let _ = result.first().unwrap().as_ref().unwrap();

    let http_opts = HttpOptions {
        addr: format!("127.0.0.1:{}", ports::get_port()),
        ..Default::default()
    };
    let frontend_ref = instance.instance.clone();
    let http_server = HttpServerBuilder::new(http_opts)
        .with_sql_handler(ServerSqlQueryHandlerAdapter::arc(frontend_ref.clone()))
        .with_grpc_handler(ServerGrpcQueryHandlerAdapter::arc(frontend_ref.clone()))
        .with_script_handler(frontend_ref.clone())
        .with_prom_handler(frontend_ref.clone())
        .with_prometheus_handler(frontend_ref)
        .with_greptime_config_options(instance.datanode_opts.to_toml_string())
        .build();
    let app = http_server.build(http_server.make_app());
    (app, instance.guard)
}

pub async fn setup_grpc_server(
    store_type: StorageType,
    name: &str,
) -> (String, TestGuard, Arc<GrpcServer>) {
    setup_grpc_server_with(store_type, name, None, None).await
}

pub async fn setup_grpc_server_with_user_provider(
    store_type: StorageType,
    name: &str,
    user_provider: Option<UserProviderRef>,
) -> (String, TestGuard, Arc<GrpcServer>) {
    setup_grpc_server_with(store_type, name, user_provider, None).await
}

pub async fn setup_grpc_server_with(
    store_type: StorageType,
    name: &str,
    user_provider: Option<UserProviderRef>,
    grpc_config: Option<GrpcServerConfig>,
) -> (String, TestGuard, Arc<GrpcServer>) {
    let instance = setup_standalone_instance(name, store_type).await;

    let runtime = Arc::new(
        RuntimeBuilder::default()
            .worker_threads(2)
            .thread_name("grpc-handlers")
            .build()
            .unwrap(),
    );

    let fe_instance_ref = instance.instance.clone();
    let flight_handler = Arc::new(GreptimeRequestHandler::new(
        ServerGrpcQueryHandlerAdapter::arc(fe_instance_ref.clone()),
        user_provider.clone(),
        runtime.clone(),
    ));

    let fe_grpc_server = Arc::new(GrpcServer::new(
        grpc_config,
        Some(ServerGrpcQueryHandlerAdapter::arc(fe_instance_ref.clone())),
        Some(fe_instance_ref.clone()),
        Some(flight_handler),
        None,
        user_provider,
        runtime,
    ));

    let fe_grpc_addr = "127.0.0.1:0".parse::<SocketAddr>().unwrap();
    let fe_grpc_addr = fe_grpc_server
        .start(fe_grpc_addr)
        .await
        .unwrap()
        .to_string();

    // wait for GRPC server to start
    tokio::time::sleep(Duration::from_secs(1)).await;

    (fe_grpc_addr, instance.guard, fe_grpc_server)
}

pub async fn check_output_stream(output: Output, expected: &str) {
    let recordbatches = match output {
        Output::Stream(stream) => util::collect_batches(stream).await.unwrap(),
        Output::RecordBatches(recordbatches) => recordbatches,
        _ => unreachable!(),
    };
    let pretty_print = recordbatches.pretty_print().unwrap();
    assert_eq!(pretty_print, expected, "actual: \n{}", pretty_print);
}

pub async fn setup_mysql_server(
    store_type: StorageType,
    name: &str,
) -> (String, TestGuard, Arc<Box<dyn Server>>) {
    setup_mysql_server_with_user_provider(store_type, name, None).await
}

pub async fn setup_mysql_server_with_user_provider(
    store_type: StorageType,
    name: &str,
    user_provider: Option<UserProviderRef>,
) -> (String, TestGuard, Arc<Box<dyn Server>>) {
    let instance = setup_standalone_instance(name, store_type).await;

    let runtime = Arc::new(
        RuntimeBuilder::default()
            .worker_threads(2)
            .thread_name("mysql-runtime")
            .build()
            .unwrap(),
    );

    let fe_mysql_addr = format!("127.0.0.1:{}", ports::get_port());

    let fe_instance_ref = instance.instance.clone();
    let opts = MysqlOptions {
        addr: fe_mysql_addr.clone(),
        ..Default::default()
    };
    let fe_mysql_server = Arc::new(MysqlServer::create_server(
        runtime,
        Arc::new(MysqlSpawnRef::new(
            ServerSqlQueryHandlerAdapter::arc(fe_instance_ref),
            user_provider,
        )),
        Arc::new(MysqlSpawnConfig::new(
            false,
            opts.tls.setup().unwrap().map(Arc::new),
            opts.reject_no_database.unwrap_or(false),
        )),
    ));

    let fe_mysql_addr_clone = fe_mysql_addr.clone();
    let fe_mysql_server_clone = fe_mysql_server.clone();
    let _handle = tokio::spawn(async move {
        let addr = fe_mysql_addr_clone.parse::<SocketAddr>().unwrap();
        fe_mysql_server_clone.start(addr).await.unwrap()
    });

    tokio::time::sleep(Duration::from_secs(1)).await;

    (fe_mysql_addr, instance.guard, fe_mysql_server)
}

pub async fn setup_pg_server(
    store_type: StorageType,
    name: &str,
) -> (String, TestGuard, Arc<Box<dyn Server>>) {
    setup_pg_server_with_user_provider(store_type, name, None).await
}

pub async fn setup_pg_server_with_user_provider(
    store_type: StorageType,
    name: &str,
    user_provider: Option<UserProviderRef>,
) -> (String, TestGuard, Arc<Box<dyn Server>>) {
    let instance = setup_standalone_instance(name, store_type).await;

    let runtime = Arc::new(
        RuntimeBuilder::default()
            .worker_threads(2)
            .thread_name("pg-runtime")
            .build()
            .unwrap(),
    );

    let fe_pg_addr = format!("127.0.0.1:{}", ports::get_port());

    let fe_instance_ref = instance.instance.clone();
    let opts = PostgresOptions {
        addr: fe_pg_addr.clone(),
        ..Default::default()
    };
    let fe_pg_server = Arc::new(Box::new(PostgresServer::new(
        ServerSqlQueryHandlerAdapter::arc(fe_instance_ref),
        opts.tls.clone(),
        runtime,
        user_provider,
    )) as Box<dyn Server>);

    let fe_pg_addr_clone = fe_pg_addr.clone();
    let fe_pg_server_clone = fe_pg_server.clone();
    let _handle = tokio::spawn(async move {
        let addr = fe_pg_addr_clone.parse::<SocketAddr>().unwrap();
        fe_pg_server_clone.start(addr).await.unwrap()
    });

    tokio::time::sleep(Duration::from_secs(1)).await;

    (fe_pg_addr, instance.guard, fe_pg_server)
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

pub async fn run_test_with_kafka_wal<F>(test: F)
where
    F: FnOnce(Vec<String>) -> BoxFuture<'static, ()>,
{
    let _ = dotenv::dotenv();
    let endpoints = env::var("GT_KAFKA_ENDPOINTS").unwrap_or_default();
    if endpoints.is_empty() {
        warn!("The endpoints is empty, skipping the test");
        return;
    }

    let endpoints = endpoints
        .split(',')
        .map(|s| s.trim().to_string())
        .collect::<Vec<_>>();

    test(endpoints).await
}
