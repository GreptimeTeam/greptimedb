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
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use catalog::{CatalogManagerRef, RegisterTableRequest};
use common_catalog::consts::{
    DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, MIN_USER_TABLE_ID, MITO_ENGINE,
};
use common_query::Output;
use common_recordbatch::util;
use common_runtime::Builder as RuntimeBuilder;
use common_test_util::ports;
use common_test_util::temp_dir::{create_temp_dir, TempDir};
use datanode::datanode::{
    AzblobConfig, DatanodeOptions, FileConfig, ObjectStoreConfig, OssConfig, ProcedureConfig,
    S3Config, StorageConfig, WalConfig,
};
use datanode::error::{CreateTableSnafu, Result};
use datanode::instance::Instance;
use datanode::sql::SqlHandler;
use datatypes::data_type::ConcreteDataType;
use datatypes::scalars::ScalarVectorBuilder;
use datatypes::schema::{ColumnSchema, RawSchema};
use datatypes::vectors::{
    Float64VectorBuilder, MutableVector, StringVectorBuilder, TimestampMillisecondVectorBuilder,
};
use frontend::instance::Instance as FeInstance;
use frontend::service_config::{MysqlOptions, PostgresOptions};
use object_store::services::{Azblob, Oss, S3};
use object_store::test_util::TempFolder;
use object_store::ObjectStore;
use secrecy::ExposeSecret;
use servers::grpc::GrpcServer;
use servers::http::{HttpOptions, HttpServerBuilder};
use servers::metrics_handler::MetricsHandler;
use servers::mysql::server::{MysqlServer, MysqlSpawnConfig, MysqlSpawnRef};
use servers::postgres::PostgresServer;
use servers::prom::PromServer;
use servers::query_handler::grpc::ServerGrpcQueryHandlerAdaptor;
use servers::query_handler::sql::ServerSqlQueryHandlerAdaptor;
use servers::server::Server;
use servers::Mode;
use snafu::ResultExt;
use table::engine::{EngineContext, TableEngineRef};
use table::requests::{CreateTableRequest, InsertRequest, TableOptions};

#[derive(Debug, Eq, PartialEq)]
pub enum StorageType {
    S3,
    S3WithCache,
    File,
    Oss,
    Azblob,
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
            StorageType::Azblob => {
                if let Ok(b) = env::var("GT_AZBLOB_CONTAINER") {
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

pub fn get_test_store_config(
    store_type: &StorageType,
    name: &str,
) -> (ObjectStoreConfig, TempDirGuard) {
    let _ = dotenv::dotenv();

    match store_type {
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
            builder
                .root(&azblob_config.root)
                .endpoint(&azblob_config.endpoint)
                .account_name(azblob_config.account_name.expose_secret())
                .account_key(azblob_config.account_key.expose_secret())
                .container(&azblob_config.container);

            if let Ok(sas_token) = env::var("GT_AZBLOB_SAS_TOKEN") {
                builder.sas_token(&sas_token);
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
            builder
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
                s3_config.cache_path = Some("/tmp/greptimedb_cache".to_string());
            }

            let mut builder = S3::default();
            builder
                .root(&s3_config.root)
                .access_key_id(s3_config.access_key_id.expose_secret())
                .secret_access_key(s3_config.secret_access_key.expose_secret())
                .bucket(&s3_config.bucket);

            if s3_config.endpoint.is_some() {
                builder.endpoint(s3_config.endpoint.as_ref().unwrap());
            }
            if s3_config.region.is_some() {
                builder.region(s3_config.region.as_ref().unwrap());
            }

            let config = ObjectStoreConfig::S3(s3_config);

            let store = ObjectStore::new(builder).unwrap().finish();

            (config, TempDirGuard::S3(TempFolder::new(&store, "/")))
        }
        StorageType::File => {
            let data_tmp_dir = create_temp_dir(&format!("gt_data_{name}"));

            (
                ObjectStoreConfig::File(FileConfig {
                    data_home: data_tmp_dir.path().to_str().unwrap().to_string(),
                }),
                TempDirGuard::File(data_tmp_dir),
            )
        }
    }
}

pub enum TempDirGuard {
    File(TempDir),
    S3(TempFolder),
    Oss(TempFolder),
    Azblob(TempFolder),
}

pub struct TestGuard {
    pub wal_guard: WalGuard,
    pub storage_guard: StorageGuard,
}

pub struct WalGuard(pub TempDir);

pub struct StorageGuard(pub TempDirGuard);

impl TestGuard {
    pub async fn remove_all(&mut self) {
        if let TempDirGuard::S3(guard) | TempDirGuard::Oss(guard) | TempDirGuard::Azblob(guard) =
            &mut self.storage_guard.0
        {
            guard.remove_all().await.unwrap()
        }
    }
}

pub fn create_tmp_dir_and_datanode_opts(
    store_type: StorageType,
    name: &str,
) -> (DatanodeOptions, TestGuard) {
    let wal_tmp_dir = create_temp_dir(&format!("gt_wal_{name}"));
    let wal_dir = wal_tmp_dir.path().to_str().unwrap().to_string();

    let (store, data_tmp_dir) = get_test_store_config(&store_type, name);
    let opts = create_datanode_opts(store, wal_dir);

    (
        opts,
        TestGuard {
            wal_guard: WalGuard(wal_tmp_dir),
            storage_guard: StorageGuard(data_tmp_dir),
        },
    )
}

pub fn create_datanode_opts(store: ObjectStoreConfig, wal_dir: String) -> DatanodeOptions {
    DatanodeOptions {
        wal: WalConfig {
            dir: Some(wal_dir),
            ..Default::default()
        },
        storage: StorageConfig {
            store,
            ..Default::default()
        },
        mode: Mode::Standalone,
        procedure: ProcedureConfig::default(),
        ..Default::default()
    }
}

pub async fn create_test_table(
    catalog_manager: &CatalogManagerRef,
    sql_handler: &SqlHandler,
    ts_type: ConcreteDataType,
    table_name: &str,
) -> Result<()> {
    let column_schemas = vec![
        ColumnSchema::new("host", ConcreteDataType::string_datatype(), false),
        ColumnSchema::new("cpu", ConcreteDataType::float64_datatype(), true),
        ColumnSchema::new("memory", ConcreteDataType::float64_datatype(), true),
        ColumnSchema::new("ts", ts_type, true).with_time_index(true),
    ];
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

    let req = RegisterTableRequest {
        catalog: DEFAULT_CATALOG_NAME.to_string(),
        schema: DEFAULT_SCHEMA_NAME.to_string(),
        table_name: table_name.to_string(),
        table_id: table.table_info().ident.table_id,
        table,
    };
    catalog_manager.register_table(req).await.unwrap();
    Ok(())
}

pub async fn setup_test_http_app(store_type: StorageType, name: &str) -> (Router, TestGuard) {
    let (opts, guard) = create_tmp_dir_and_datanode_opts(store_type, name);
    let instance = Arc::new(Instance::with_mock_meta_client(&opts).await.unwrap());
    create_test_table(
        instance.catalog_manager(),
        instance.sql_handler(),
        ConcreteDataType::timestamp_millisecond_datatype(),
        "demo",
    )
    .await
    .unwrap();
    let frontend_instance = FeInstance::try_new_standalone(instance.clone())
        .await
        .unwrap();
    instance.start().await.unwrap();

    let http_opts = HttpOptions {
        addr: format!("127.0.0.1:{}", ports::get_port()),
        ..Default::default()
    };
    let http_server = HttpServerBuilder::new(http_opts)
        .with_sql_handler(ServerSqlQueryHandlerAdaptor::arc(Arc::new(
            frontend_instance,
        )))
        .with_grpc_handler(ServerGrpcQueryHandlerAdaptor::arc(instance.clone()))
        .with_metrics_handler(MetricsHandler)
        .build();
    (http_server.build(http_server.make_app()), guard)
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
        "demo",
    )
    .await
    .unwrap();

    let http_opts = HttpOptions {
        addr: format!("127.0.0.1:{}", ports::get_port()),
        ..Default::default()
    };

    let frontend_ref = Arc::new(frontend);
    let http_server = HttpServerBuilder::new(http_opts)
        .with_sql_handler(ServerSqlQueryHandlerAdaptor::arc(frontend_ref.clone()))
        .with_grpc_handler(ServerGrpcQueryHandlerAdaptor::arc(frontend_ref.clone()))
        .with_script_handler(frontend_ref)
        .build();
    let app = http_server.build(http_server.make_app());
    (app, guard)
}

fn mock_insert_request(host: &str, cpu: f64, memory: f64, ts: i64) -> InsertRequest {
    let mut columns_values = HashMap::with_capacity(4);
    let mut builder = StringVectorBuilder::with_capacity(1);
    builder.push(Some(host));
    columns_values.insert("host".to_string(), builder.to_vector());

    let mut builder = Float64VectorBuilder::with_capacity(1);
    builder.push(Some(cpu));
    columns_values.insert("cpu".to_string(), builder.to_vector());

    let mut builder = Float64VectorBuilder::with_capacity(1);
    builder.push(Some(memory));
    columns_values.insert("memory".to_string(), builder.to_vector());

    let mut builder = TimestampMillisecondVectorBuilder::with_capacity(1);
    builder.push(Some(ts.into()));
    columns_values.insert("ts".to_string(), builder.to_vector());

    InsertRequest {
        catalog_name: common_catalog::consts::DEFAULT_CATALOG_NAME.to_string(),
        schema_name: common_catalog::consts::DEFAULT_SCHEMA_NAME.to_string(),
        table_name: "demo".to_string(),
        columns_values,
        region_number: 0,
    }
}

pub async fn setup_test_prom_app_with_frontend(
    store_type: StorageType,
    name: &str,
) -> (Router, TestGuard) {
    std::env::set_var("TZ", "UTC");
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
        "demo",
    )
    .await
    .unwrap();
    let demo = frontend
        .catalog_manager()
        .table(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, "demo")
        .await
        .unwrap()
        .unwrap();

    let _ = demo
        .insert(mock_insert_request("host1", 1.1, 2.2, 0))
        .await
        .unwrap();
    let _ = demo
        .insert(mock_insert_request("host2", 2.1, 4.3, 600000))
        .await
        .unwrap();

    let http_opts = HttpOptions {
        addr: format!("127.0.0.1:{}", ports::get_port()),
        ..Default::default()
    };
    let frontend_ref = Arc::new(frontend);
    let http_server = HttpServerBuilder::new(http_opts)
        .with_sql_handler(ServerSqlQueryHandlerAdaptor::arc(frontend_ref.clone()))
        .with_grpc_handler(ServerGrpcQueryHandlerAdaptor::arc(frontend_ref.clone()))
        .with_script_handler(frontend_ref.clone())
        .with_prom_handler(frontend_ref.clone())
        .build();
    let prom_server = PromServer::create_server(frontend_ref);
    let app = http_server.build(http_server.make_app());
    let app = app.merge(prom_server.make_app());
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

    let fe_grpc_addr = "127.0.0.1:0".parse::<SocketAddr>().unwrap();
    let fe_grpc_addr = fe_grpc_server
        .start(fe_grpc_addr)
        .await
        .unwrap()
        .to_string();

    // wait for GRPC server to start
    tokio::time::sleep(Duration::from_secs(1)).await;

    (fe_grpc_addr, guard, fe_grpc_server)
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
    common_telemetry::init_default_ut_logging();

    let (opts, guard) = create_tmp_dir_and_datanode_opts(store_type, name);
    let instance = Arc::new(Instance::with_mock_meta_client(&opts).await.unwrap());

    let runtime = Arc::new(
        RuntimeBuilder::default()
            .worker_threads(2)
            .thread_name("mysql-runtime")
            .build()
            .unwrap(),
    );

    let fe_mysql_addr = format!("127.0.0.1:{}", ports::get_port());

    let fe_instance = FeInstance::try_new_standalone(instance.clone())
        .await
        .unwrap();
    instance.start().await.unwrap();
    let fe_instance_ref = Arc::new(fe_instance);
    let opts = MysqlOptions {
        addr: fe_mysql_addr.clone(),
        ..Default::default()
    };
    let fe_mysql_server = Arc::new(MysqlServer::create_server(
        runtime,
        Arc::new(MysqlSpawnRef::new(
            ServerSqlQueryHandlerAdaptor::arc(fe_instance_ref),
            None,
        )),
        Arc::new(MysqlSpawnConfig::new(
            false,
            opts.tls.setup().unwrap().map(Arc::new),
            opts.reject_no_database.unwrap_or(false),
        )),
    ));

    let fe_mysql_addr_clone = fe_mysql_addr.clone();
    let fe_mysql_server_clone = fe_mysql_server.clone();
    tokio::spawn(async move {
        let addr = fe_mysql_addr_clone.parse::<SocketAddr>().unwrap();
        fe_mysql_server_clone.start(addr).await.unwrap()
    });

    tokio::time::sleep(Duration::from_secs(1)).await;

    (fe_mysql_addr, guard, fe_mysql_server)
}

pub async fn setup_pg_server(
    store_type: StorageType,
    name: &str,
) -> (String, TestGuard, Arc<Box<dyn Server>>) {
    common_telemetry::init_default_ut_logging();

    let (opts, guard) = create_tmp_dir_and_datanode_opts(store_type, name);
    let instance = Arc::new(Instance::with_mock_meta_client(&opts).await.unwrap());

    let runtime = Arc::new(
        RuntimeBuilder::default()
            .worker_threads(2)
            .thread_name("pg-runtime")
            .build()
            .unwrap(),
    );

    let fe_pg_addr = format!("127.0.0.1:{}", ports::get_port());

    let fe_instance = FeInstance::try_new_standalone(instance.clone())
        .await
        .unwrap();
    instance.start().await.unwrap();
    let fe_instance_ref = Arc::new(fe_instance);
    let opts = PostgresOptions {
        addr: fe_pg_addr.clone(),
        ..Default::default()
    };
    let fe_pg_server = Arc::new(Box::new(PostgresServer::new(
        ServerSqlQueryHandlerAdaptor::arc(fe_instance_ref),
        opts.tls.clone(),
        runtime,
        None,
    )) as Box<dyn Server>);

    let fe_pg_addr_clone = fe_pg_addr.clone();
    let fe_pg_server_clone = fe_pg_server.clone();
    tokio::spawn(async move {
        let addr = fe_pg_addr_clone.parse::<SocketAddr>().unwrap();
        fe_pg_server_clone.start(addr).await.unwrap()
    });

    tokio::time::sleep(Duration::from_secs(1)).await;

    (fe_pg_addr, guard, fe_pg_server)
}
