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

use std::collections::HashMap;
use std::env;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use catalog::CatalogManagerRef;
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, MIN_USER_TABLE_ID};
use common_runtime::Builder as RuntimeBuilder;
use datanode::datanode::{
    DatanodeOptions, FileConfig, ObjectStoreConfig, OssConfig, S3Config, WalConfig,
};
use datanode::error::{CreateTableSnafu, Result};
use datanode::instance::{Instance, InstanceRef};
use datanode::sql::SqlHandler;
use datatypes::data_type::ConcreteDataType;
use datatypes::schema::{ColumnSchema, SchemaBuilder};
use frontend::instance::Instance as FeInstance;
use object_store::backend::s3;
use object_store::services::oss;
use object_store::test_util::TempFolder;
use object_store::ObjectStore;
use once_cell::sync::OnceCell;
use rand::Rng;
use servers::grpc::GrpcServer;
use servers::http::{HttpOptions, HttpServer};
use servers::query_handler::grpc::ServerGrpcQueryHandlerAdaptor;
use servers::query_handler::sql::ServerSqlQueryHandlerAdaptor;
use servers::server::Server;
use servers::Mode;
use snafu::ResultExt;
use table::engine::{EngineContext, TableEngineRef};
use table::requests::CreateTableRequest;
use tempdir::TempDir;

static PORTS: OnceCell<AtomicUsize> = OnceCell::new();

fn get_port() -> usize {
    PORTS
        .get_or_init(|| AtomicUsize::new(rand::thread_rng().gen_range(3500..3900)))
        .fetch_add(1, Ordering::Relaxed)
}

pub enum StorageType {
    S3,
    File,
    Oss,
}

impl StorageType {
    pub fn test_on(&self) -> bool {
        let _ = dotenv::dotenv();

        match self {
            StorageType::File => true, // always test file
            StorageType::S3 => {
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
            };

            let accessor = oss::Builder::default()
                .root(&oss_config.root)
                .endpoint(&oss_config.endpoint)
                .access_key_id(&oss_config.access_key_id)
                .access_key_secret(&oss_config.access_key_secret)
                .bucket(&oss_config.bucket)
                .build()
                .unwrap();

            let config = ObjectStoreConfig::Oss(oss_config);

            let store = ObjectStore::new(accessor);

            (
                config,
                Some(TempDirGuard::Oss(TempFolder::new(&store, "/"))),
            )
        }
        StorageType::S3 => {
            let s3_config = S3Config {
                root: uuid::Uuid::new_v4().to_string(),
                access_key_id: env::var("GT_S3_ACCESS_KEY_ID").unwrap(),
                secret_access_key: env::var("GT_S3_ACCESS_KEY").unwrap(),
                bucket: env::var("GT_S3_BUCKET").unwrap(),
                endpoint: None,
                region: None,
            };

            let accessor = s3::Builder::default()
                .root(&s3_config.root)
                .access_key_id(&s3_config.access_key_id)
                .secret_access_key(&s3_config.secret_access_key)
                .bucket(&s3_config.bucket)
                .build()
                .unwrap();

            let config = ObjectStoreConfig::S3(s3_config);

            let store = ObjectStore::new(accessor);

            (config, Some(TempDirGuard::S3(TempFolder::new(&store, "/"))))
        }
        StorageType::File => {
            let data_tmp_dir = TempDir::new(&format!("gt_data_{name}")).unwrap();

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
    let wal_tmp_dir = TempDir::new(&format!("gt_wal_{name}")).unwrap();

    let (storage, data_tmp_dir) = get_test_store_config(&store_type, name);

    let opts = DatanodeOptions {
        wal: WalConfig {
            dir: wal_tmp_dir.path().to_str().unwrap().to_string(),
            ..Default::default()
        },
        storage,
        mode: Mode::Standalone,
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
    let table_engine: TableEngineRef = sql_handler.table_engine();
    let table = table_engine
        .create_table(
            &EngineContext::default(),
            CreateTableRequest {
                id: MIN_USER_TABLE_ID,
                catalog_name: "greptime".to_string(),
                schema_name: "public".to_string(),
                table_name: table_name.to_string(),
                desc: Some(" a test table".to_string()),
                schema: Arc::new(
                    SchemaBuilder::try_from(column_schemas)
                        .unwrap()
                        .build()
                        .expect("ts is expected to be timestamp column"),
                ),
                create_if_not_exists: true,
                primary_key_indices: vec![0], // "host" is in primary keys
                table_options: HashMap::new(),
                region_numbers: vec![0],
            },
        )
        .await
        .context(CreateTableSnafu { table_name })?;

    let schema_provider = catalog_manager
        .catalog(DEFAULT_CATALOG_NAME)
        .unwrap()
        .unwrap()
        .schema(DEFAULT_SCHEMA_NAME)
        .unwrap()
        .unwrap();
    schema_provider
        .register_table(table_name.to_string(), table)
        .unwrap();
    Ok(())
}

async fn build_frontend_instance(datanode_instance: InstanceRef) -> FeInstance {
    let mut frontend_instance = FeInstance::new_standalone(datanode_instance.clone());
    frontend_instance.set_script_handler(datanode_instance);
    frontend_instance
}

pub async fn setup_test_app(store_type: StorageType, name: &str) -> (Router, TestGuard) {
    let (opts, guard) = create_tmp_dir_and_datanode_opts(store_type, name);
    let instance = Arc::new(Instance::with_mock_meta_client(&opts).await.unwrap());
    instance.start().await.unwrap();
    create_test_table(
        instance.catalog_manager(),
        instance.sql_handler(),
        ConcreteDataType::timestamp_millisecond_datatype(),
    )
    .await
    .unwrap();
    let http_server = HttpServer::new(
        ServerSqlQueryHandlerAdaptor::arc(instance),
        HttpOptions::default(),
    );
    (http_server.make_app(), guard)
}

pub async fn setup_test_app_with_frontend(
    store_type: StorageType,
    name: &str,
) -> (Router, TestGuard) {
    let (opts, guard) = create_tmp_dir_and_datanode_opts(store_type, name);
    let instance = Arc::new(Instance::with_mock_meta_client(&opts).await.unwrap());
    let frontend = build_frontend_instance(instance.clone()).await;
    instance.start().await.unwrap();
    create_test_table(
        frontend.catalog_manager(),
        instance.sql_handler(),
        ConcreteDataType::timestamp_millisecond_datatype(),
    )
    .await
    .unwrap();
    let mut http_server = HttpServer::new(
        ServerSqlQueryHandlerAdaptor::arc(Arc::new(frontend)),
        HttpOptions::default(),
    );
    http_server.set_script_handler(instance.clone());
    let app = http_server.make_app();
    (app, guard)
}

pub async fn setup_grpc_server(
    store_type: StorageType,
    name: &str,
) -> (String, TestGuard, Arc<GrpcServer>) {
    common_telemetry::init_default_ut_logging();

    let (opts, guard) = create_tmp_dir_and_datanode_opts(store_type, name);
    let instance = Arc::new(Instance::with_mock_meta_client(&opts).await.unwrap());
    instance.start().await.unwrap();

    let runtime = Arc::new(
        RuntimeBuilder::default()
            .worker_threads(2)
            .thread_name("grpc-handlers")
            .build()
            .unwrap(),
    );

    let fe_grpc_addr = format!("127.0.0.1:{}", get_port());

    let fe_instance = frontend::instance::Instance::new_standalone(instance.clone());
    let fe_instance_ref = Arc::new(fe_instance);
    let fe_grpc_server = Arc::new(GrpcServer::new(
        ServerGrpcQueryHandlerAdaptor::arc(fe_instance_ref),
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
