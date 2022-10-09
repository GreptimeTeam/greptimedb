use std::collections::HashMap;
use std::sync::Arc;

use catalog::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, MIN_USER_TABLE_ID};
use datatypes::data_type::ConcreteDataType;
use datatypes::schema::{ColumnSchema, SchemaBuilder};
use snafu::ResultExt;
use table::engine::EngineContext;
use table::engine::TableEngineRef;
use table::requests::CreateTableRequest;
use table_engine::config::EngineConfig;
use table_engine::table::test_util::{new_test_object_store, MockEngine, MockMitoEngine};
use tempdir::TempDir;

use crate::datanode::{DatanodeOptions, ObjectStoreConfig};
use crate::error::{CreateTableSnafu, Result};
use crate::instance::Instance;
use crate::sql::SqlHandler;

/// Create a tmp dir(will be deleted once it goes out of scope.) and a default `DatanodeOptions`,
/// Only for test.
pub struct TestGuard {
    _wal_tmp_dir: TempDir,
    _data_tmp_dir: TempDir,
}

pub fn create_tmp_dir_and_datanode_opts() -> (DatanodeOptions, TestGuard) {
    let wal_tmp_dir = TempDir::new("/tmp/greptimedb_test_wal").unwrap();
    let data_tmp_dir = TempDir::new("/tmp/greptimedb_test_data").unwrap();
    let opts = DatanodeOptions {
        wal_dir: wal_tmp_dir.path().to_str().unwrap().to_string(),
        storage: ObjectStoreConfig::File {
            data_dir: data_tmp_dir.path().to_str().unwrap().to_string(),
        },
        ..Default::default()
    };
    (
        opts,
        TestGuard {
            _wal_tmp_dir: wal_tmp_dir,
            _data_tmp_dir: data_tmp_dir,
        },
    )
}

pub async fn create_test_table(instance: &Instance) -> Result<()> {
    let column_schemas = vec![
        ColumnSchema::new("host", ConcreteDataType::string_datatype(), false),
        ColumnSchema::new("cpu", ConcreteDataType::float64_datatype(), true),
        ColumnSchema::new("memory", ConcreteDataType::float64_datatype(), true),
        ColumnSchema::new("ts", ConcreteDataType::timestamp_millis_datatype(), true),
    ];

    let table_name = "demo";
    let table_engine: TableEngineRef = instance.sql_handler().table_engine();
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
                        .timestamp_index(3)
                        .build()
                        .expect("ts is expected to be timestamp column"),
                ),
                create_if_not_exists: true,
                primary_key_indices: vec![3, 0], // "host" and "ts" are primary keys
                table_options: HashMap::new(),
            },
        )
        .await
        .context(CreateTableSnafu { table_name })?;

    let schema_provider = instance
        .catalog_manager()
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

pub async fn create_mock_sql_handler() -> SqlHandler {
    let (_dir, object_store) = new_test_object_store("setup_mock_engine_and_table").await;
    let mock_engine = Arc::new(MockMitoEngine::new(
        EngineConfig::default(),
        MockEngine::default(),
        object_store,
    ));
    let catalog_manager = Arc::new(
        catalog::local::LocalCatalogManager::try_new(mock_engine.clone())
            .await
            .unwrap(),
    );
    SqlHandler::new(mock_engine, catalog_manager)
}
