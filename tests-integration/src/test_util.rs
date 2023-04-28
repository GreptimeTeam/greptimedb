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
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use catalog::CatalogManagerRef;
use common_catalog::consts::{
    DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, MIN_USER_TABLE_ID, MITO_ENGINE,
};
use common_runtime::Builder as RuntimeBuilder;
use common_test_util::temp_dir::{create_temp_dir, TempDir};
use datanode::datanode::{
    DatanodeOptions, FileConfig, ObjectStoreConfig, OssConfig, ProcedureConfig, S3Config,
    StorageConfig, WalConfig,
};
use datanode::error::{CreateTableSnafu, Result};
use datanode::instance::Instance;
use datanode::sql::SqlHandler;
use datatypes::data_type::ConcreteDataType;
use datatypes::schema::{ColumnSchema, RawSchema};
use frontend::instance::Instance as FeInstance;
use object_store::services::{Oss, S3};
use object_store::test_util::TempFolder;
use object_store::ObjectStore;
use once_cell::sync::OnceCell;
use rand::Rng;
use servers::grpc::GrpcServer;
use servers::http::{HttpOptions, HttpServerBuilder};
use servers::metrics_handler::MetricsHandler;
use servers::prom::PromServer;
use servers::query_handler::grpc::ServerGrpcQueryHandlerAdaptor;
use servers::query_handler::sql::ServerSqlQueryHandlerAdaptor;
use servers::server::Server;
use servers::Mode;
use snafu::ResultExt;
use table::engine::{EngineContext, TableEngineRef};
use table::requests::{CreateTableRequest, TableOptions};

static PORTS: OnceCell<AtomicUsize> = OnceCell::new();

fn get_port() -> usize {
    PORTS
        .get_or_init(|| AtomicUsize::new(rand::thread_rng().gen_range(3500..3900)))
        .fetch_add(1, Ordering::Relaxed)
}

#[derive(Debug, Eq, PartialEq)]
pub enum StorageType {
    S3,
    S3WithCache,
    File,
    Oss,
}

impl StorageType {
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
        }
    }
}

fn s3_test_config() -> S3Config {
    S3Config {
        root: uuid::Uuid::new_v4().to_string(),
        access_key_id: env::var("GT_S3_ACCESS_KEY_ID").unwrap(),
        secret_access_key: env::var("GT_S3_ACCESS_KEY").unwrap(),
        bucket: env::var("GT_S3_BUCKET").unwrap(),
        region: Some(env::var("GT_S3_REGION").unwrap()),
        ..Default::default()
    }
}

fn get_test_store_config(
    store_type: &StorageType,
    name: &str,
) -> (ObjectStoreConfig, Option<TempDirGuard>) {
    let _ = dotenv::dotenv();

    match store_type {
        StorageType::Oss => {
            let oss_config = OssConfig {
                root: uuid::Uuid::new_v4().to_string(),
                access_key_id: env::var("GT_OSS_ACCESS_KEY_ID").unwrap(),
                access_key_secret: env::var("GT_OSS_ACCESS_KEY").unwrap(),
                bucket: env::var("GT_OSS_BUCKET").unwrap(),
                endpoint: env::var("GT_OSS_ENDPOINT").unwrap(),
                ..Default::default()
            };

            let mut builder = Oss::default();
            builder
                .root(&oss_config.root)
                .endpoint(&oss_config.endpoint)
                .access_key_id(&oss_config.access_key_id)
                .access_key_secret(&oss_config.access_key_secret)
                .bucket(&oss_config.bucket);

            let config = ObjectStoreConfig::Oss(oss_config);

            let store = ObjectStore::new(builder).unwrap().finish();

            (
                config,
                Some(TempDirGuard::Oss(TempFolder::new(&store, "/"))),
            )
        }
        StorageType::S3 | StorageType::S3WithCache => {
            let mut s3_config = s3_test_config();

            if *store_type == StorageType::S3WithCache {
                s3_config.cache_path = Some("/tmp/greptimedb_cache".to_string());
            }

            let mut builder = S3::default();
            builder
                .root(&s3_config.root)
                .access_key_id(&s3_config.access_key_id)
                .secret_access_key(&s3_config.secret_access_key)
                .bucket(&s3_config.bucket);

            if s3_config.endpoint.is_some() {
                builder.endpoint(s3_config.endpoint.as_ref().unwrap());
            }
            if s3_config.region.is_some() {
                builder.region(s3_config.region.as_ref().unwrap());
            }

            let config = ObjectStoreConfig::S3(s3_config);

            let store = ObjectStore::new(builder).unwrap().finish();

            (config, Some(TempDirGuard::S3(TempFolder::new(&store, "/"))))
        }
        StorageType::File => {
            let data_tmp_dir = create_temp_dir(&format!("gt_data_{name}"));

            (
                ObjectStoreConfig::File(FileConfig {
                    data_dir: data_tmp_dir.path().to_str().unwrap().to_string(),
                }),
                Some(TempDirGuard::File(data_tmp_dir)),
            )
        }
    }
}

enum TempDirGuard {
    File(TempDir),
    S3(TempFolder),
    Oss(TempFolder),
}

/// Create a tmp dir(will be deleted once it goes out of scope.) and a default `DatanodeOptions`,
/// Only for test.
pub struct TestGuard {
    _wal_tmp_dir: TempDir,
    data_tmp_dir: Option<TempDirGuard>,
}

impl TestGuard {
    pub async fn remove_all(&mut self) {
        if let Some(TempDirGuard::S3(mut guard)) = self.data_tmp_dir.take() {
            guard.remove_all().await.unwrap();
        }
        if let Some(TempDirGuard::Oss(mut guard)) = self.data_tmp_dir.take() {
            guard.remove_all().await.unwrap();
        }
    }
}

pub fn create_tmp_dir_and_datanode_opts(
    store_type: StorageType,
    name: &str,
) -> (DatanodeOptions, TestGuard) {
    let wal_tmp_dir = create_temp_dir(&format!("gt_wal_{name}"));

    let (store, data_tmp_dir) = get_test_store_config(&store_type, name);

    let opts = DatanodeOptions {
        wal: WalConfig {
            dir: wal_tmp_dir.path().to_str().unwrap().to_string(),
            ..Default::default()
        },
        storage: StorageConfig {
            store,
            ..Default::default()
        },
        mode: Mode::Standalone,
        procedure: ProcedureConfig::default(),
        ..Default::default()
    };
    (
        opts,
        TestGuard {
            _wal_tmp_dir: wal_tmp_dir,
            data_tmp_dir,
        },
    )
}

pub async fn create_test_table(
    catalog_manager: &CatalogManagerRef,
    sql_handler: &SqlHandler,
    ts_type: ConcreteDataType,
) -> Result<()> {
    let column_schemas = vec![
        ColumnSchema::new("host", ConcreteDataType::string_datatype(), false),
        ColumnSchema::new("cpu", ConcreteDataType::float64_datatype(), true),
        ColumnSchema::new("memory", ConcreteDataType::float64_datatype(), true),
        ColumnSchema::new("ts", ts_type, true).with_time_index(true),
    ];

    let table_name = "demo";
    let table_engine: TableEngineRef = sql_handler
        .table_engine_manager()
        .engine(MITO_ENGINE)
        .unwrap();
    let table = table_engine
        .create_table(
            &EngineContext::default(),
            CreateTableRequest {
                id: MIN_USER_TABLE_ID,
                catalog_name: "greptime".to_string(),
                schema_name: "public".to_string(),
                table_name: table_name.to_string(),
                desc: Some(" a test table".to_string()),
                schema: RawSchema::new(column_schemas),
                create_if_not_exists: true,
                primary_key_indices: vec![0], // "host" is in primary keys
                table_options: TableOptions::default(),
                region_numbers: vec![0],
                engine: MITO_ENGINE.to_string(),
            },
        )
        .await
        .context(CreateTableSnafu { table_name })?;

    let schema_provider = catalog_manager
        .catalog(DEFAULT_CATALOG_NAME)
        .await
        .unwrap()
        .unwrap()
        .schema(DEFAULT_SCHEMA_NAME)
        .await
        .unwrap()
        .unwrap();
    schema_provider
        .register_table(table_name.to_string(), table)
        .await
        .unwrap();
    Ok(())
}

pub async fn setup_test_http_app(store_type: StorageType, name: &str) -> (Router, TestGuard) {
    let (opts, guard) = create_tmp_dir_and_datanode_opts(store_type, name);
    let instance = Arc::new(Instance::with_mock_meta_client(&opts).await.unwrap());
    create_test_table(
        instance.catalog_manager(),
        instance.sql_handler(),
        ConcreteDataType::timestamp_millisecond_datatype(),
    )
    .await
    .unwrap();
    let frontend_instance = FeInstance::try_new_standalone(instance.clone())
        .await
        .unwrap();
    instance.start().await.unwrap();
    let http_server = HttpServerBuilder::new(HttpOptions::default())
        .with_sql_handler(ServerSqlQueryHandlerAdaptor::arc(Arc::new(
            frontend_instance,
        )))
        .with_grpc_handler(ServerGrpcQueryHandlerAdaptor::arc(instance.clone()))
        .with_metrics_handler(MetricsHandler)
        .build();
    (http_server.make_app(), guard)
}

pub async fn setup_test_http_app_with_frontend(
    store_type: StorageType,
    name: &str,
) -> (Router, TestGuard) {
    let (opts, guard) = create_tmp_dir_and_datanode_opts(store_type, name);
    let instance = Arc::new(Instance::with_mock_meta_client(&opts).await.unwrap());
    let frontend = FeInstance::try_new_standalone(instance.clone())
        .await
        .unwrap();
    instance.start().await.unwrap();
    create_test_table(
        frontend.catalog_manager(),
        instance.sql_handler(),
        ConcreteDataType::timestamp_millisecond_datatype(),
    )
    .await
    .unwrap();
    let frontend_ref = Arc::new(frontend);
    let http_server = HttpServerBuilder::new(HttpOptions::default())
        .with_sql_handler(ServerSqlQueryHandlerAdaptor::arc(frontend_ref.clone()))
        .with_grpc_handler(ServerGrpcQueryHandlerAdaptor::arc(frontend_ref.clone()))
        .with_script_handler(frontend_ref)
        .build();
    let app = http_server.make_app();
    (app, guard)
}

pub async fn setup_test_prom_app_with_frontend(
    store_type: StorageType,
    name: &str,
) -> (Router, TestGuard) {
    let (opts, guard) = create_tmp_dir_and_datanode_opts(store_type, name);
    let instance = Arc::new(Instance::with_mock_meta_client(&opts).await.unwrap());
    let frontend = FeInstance::try_new_standalone(instance.clone())
        .await
        .unwrap();
    instance.start().await.unwrap();
    create_test_table(
        frontend.catalog_manager(),
        instance.sql_handler(),
        ConcreteDataType::timestamp_millisecond_datatype(),
    )
    .await
    .unwrap();
    let prom_server = PromServer::create_server(Arc::new(frontend) as _);
    let app = prom_server.make_app();
    (app, guard)
}

pub async fn setup_grpc_server(
    store_type: StorageType,
    name: &str,
) -> (String, TestGuard, Arc<GrpcServer>) {
    common_telemetry::init_default_ut_logging();

    let (opts, guard) = create_tmp_dir_and_datanode_opts(store_type, name);
    let instance = Arc::new(Instance::with_mock_meta_client(&opts).await.unwrap());

    let runtime = Arc::new(
        RuntimeBuilder::default()
            .worker_threads(2)
            .thread_name("grpc-handlers")
            .build()
            .unwrap(),
    );

    let fe_grpc_addr = format!("127.0.0.1:{}", get_port());

    let fe_instance = FeInstance::try_new_standalone(instance.clone())
        .await
        .unwrap();
    instance.start().await.unwrap();
    let fe_instance_ref = Arc::new(fe_instance);
    let fe_grpc_server = Arc::new(GrpcServer::new(
        ServerGrpcQueryHandlerAdaptor::arc(fe_instance_ref.clone()),
        Some(fe_instance_ref.clone()),
        None,
        runtime,
    ));
    let grpc_server_clone = fe_grpc_server.clone();

    let fe_grpc_addr_clone = fe_grpc_addr.clone();
    tokio::spawn(async move {
        let addr = fe_grpc_addr_clone.parse::<SocketAddr>().unwrap();
        grpc_server_clone.start(addr).await.unwrap()
    });

    // wait for GRPC server to start
    tokio::time::sleep(Duration::from_secs(1)).await;

    (fe_grpc_addr, guard, fe_grpc_server)
}
