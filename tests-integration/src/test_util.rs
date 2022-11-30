// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::env;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use catalog::CatalogManagerRef;
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, MIN_USER_TABLE_ID};
use common_runtime::Builder as RuntimeBuilder;
use datanode::datanode::{DatanodeOptions, ObjectStoreConfig};
use datanode::error::{CreateTableSnafu, Result};
use datanode::instance::{Instance, InstanceRef};
use datanode::sql::SqlHandler;
use datatypes::data_type::ConcreteDataType;
use datatypes::schema::{ColumnSchema, SchemaBuilder};
use frontend::frontend::FrontendOptions;
use frontend::grpc::GrpcOptions;
use frontend::instance::{FrontendInstance, Instance as FeInstance};
use object_store::backend::s3;
use object_store::test_util::S3TempFolderGuard;
use object_store::ObjectStore;
use servers::grpc::GrpcServer;
use servers::http::{HttpOptions, HttpServer};
use servers::server::Server;
use servers::Mode;
use snafu::ResultExt;
use table::engine::{EngineContext, TableEngineRef};
use table::requests::CreateTableRequest;
use tempdir::TempDir;

pub enum StorageType {
    S3,
    File,
}

impl StorageType {
    pub fn test_on(&self) -> bool {
        match self {
            StorageType::File => true, // always test file
            StorageType::S3 => env::var("GT_S3_BUCKET").is_ok(),
        }
    }
}

fn get_test_store_config(
    store_type: &StorageType,
    name: &str,
) -> (ObjectStoreConfig, Option<TempDirGuard>) {
    match store_type {
        StorageType::S3 => {
            let root = uuid::Uuid::new_v4().to_string();
            let key_id = env::var("GT_S3_ACCESS_KEY_ID").unwrap();
            let secret_key = env::var("GT_S3_ACCESS_KEY").unwrap();
            let bucket = env::var("GT_S3_BUCKET").unwrap();

            let accessor = s3::Builder::default()
                .root(&root)
                .access_key_id(&key_id)
                .secret_access_key(&secret_key)
                .bucket(&bucket)
                .build()
                .unwrap();

            let config = ObjectStoreConfig::S3 {
                root,
                bucket,
                access_key_id: key_id,
                secret_access_key: secret_key,
            };

            let store = ObjectStore::new(accessor);

            (
                config,
                Some(TempDirGuard::S3(S3TempFolderGuard::new(&store, "/"))),
            )
        }
        StorageType::File => {
            let data_tmp_dir = TempDir::new(&format!("gt_data_{}", name)).unwrap();

            (
                ObjectStoreConfig::File {
                    data_dir: data_tmp_dir.path().to_str().unwrap().to_string(),
                },
                Some(TempDirGuard::File(data_tmp_dir)),
            )
        }
    }
}

enum TempDirGuard {
    File(TempDir),
    S3(S3TempFolderGuard),
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
    }
}

pub fn create_tmp_dir_and_datanode_opts(
    store_type: StorageType,
    name: &str,
) -> (DatanodeOptions, TestGuard) {
    let wal_tmp_dir = TempDir::new(&format!("gt_wal_{}", name)).unwrap();

    let (storage, data_tmp_dir) = get_test_store_config(&store_type, name);

    let opts = DatanodeOptions {
        wal_dir: wal_tmp_dir.path().to_str().unwrap().to_string(),
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
                primary_key_indices: vec![3, 0], // "host" and "ts" are primary keys
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
    let fe_opts = FrontendOptions::default();
    let mut frontend_instance = FeInstance::try_new(&fe_opts).await.unwrap();
    frontend_instance.set_catalog_manager(datanode_instance.catalog_manager().clone());
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
        ConcreteDataType::timestamp_millis_datatype(),
    )
    .await
    .unwrap();
    let http_server = HttpServer::new(instance, HttpOptions::default());
    (http_server.make_app(), guard)
}

pub async fn setup_test_app_with_frontend(
    store_type: StorageType,
    name: &str,
) -> (Router, TestGuard) {
    let (opts, guard) = create_tmp_dir_and_datanode_opts(store_type, name);
    let instance = Arc::new(Instance::with_mock_meta_client(&opts).await.unwrap());
    let mut frontend = build_frontend_instance(instance.clone()).await;
    instance.start().await.unwrap();
    create_test_table(
        frontend.catalog_manager().as_ref().unwrap(),
        instance.sql_handler(),
        ConcreteDataType::timestamp_millis_datatype(),
    )
    .await
    .unwrap();
    frontend.start().await.unwrap();
    let mut http_server = HttpServer::new(Arc::new(frontend), HttpOptions::default());
    http_server.set_script_handler(instance.clone());
    let app = http_server.make_app();
    (app, guard)
}

pub async fn setup_grpc_server(
    store_type: StorageType,
    name: &str,
    datanode_port: usize,
    frontend_port: usize,
) -> (String, TestGuard, Arc<GrpcServer>, Arc<GrpcServer>) {
    common_telemetry::init_default_ut_logging();

    let (mut opts, guard) = create_tmp_dir_and_datanode_opts(store_type, name);
    let datanode_grpc_addr = format!("127.0.0.1:{}", datanode_port);
    opts.rpc_addr = datanode_grpc_addr.clone();
    let instance = Arc::new(Instance::with_mock_meta_client(&opts).await.unwrap());
    instance.start().await.unwrap();

    let datanode_grpc_addr = datanode_grpc_addr.clone();
    let runtime = Arc::new(
        RuntimeBuilder::default()
            .worker_threads(2)
            .thread_name("grpc-handlers")
            .build()
            .unwrap(),
    );

    let fe_grpc_addr = format!("127.0.0.1:{}", frontend_port);
    let fe_opts = FrontendOptions {
        mode: Mode::Standalone,
        datanode_rpc_addr: datanode_grpc_addr.clone(),
        grpc_options: Some(GrpcOptions {
            addr: fe_grpc_addr.clone(),
            runtime_size: 8,
        }),
        ..Default::default()
    };

    let datanode_grpc_server = Arc::new(GrpcServer::new(
        instance.clone(),
        instance.clone(),
        runtime.clone(),
    ));

    let mut fe_instance = frontend::instance::Instance::try_new(&fe_opts)
        .await
        .unwrap();
    fe_instance.set_catalog_manager(instance.catalog_manager().clone());

    let fe_instance_ref = Arc::new(fe_instance);
    let fe_grpc_server = Arc::new(GrpcServer::new(
        fe_instance_ref.clone(),
        fe_instance_ref,
        runtime,
    ));
    let grpc_server_clone = fe_grpc_server.clone();

    let fe_grpc_addr_clone = fe_grpc_addr.clone();
    tokio::spawn(async move {
        let addr = fe_grpc_addr_clone.parse::<SocketAddr>().unwrap();
        grpc_server_clone.start(addr).await.unwrap()
    });

    let dn_grpc_addr_clone = datanode_grpc_addr.clone();
    let dn_grpc_server_clone = datanode_grpc_server.clone();
    tokio::spawn(async move {
        let addr = dn_grpc_addr_clone.parse::<SocketAddr>().unwrap();
        dn_grpc_server_clone.start(addr).await.unwrap()
    });

    // wait for GRPC server to start
    tokio::time::sleep(Duration::from_secs(1)).await;

    (fe_grpc_addr, guard, fe_grpc_server, datanode_grpc_server)
}
