use std::collections::HashMap;
use std::sync::Arc;

use catalog::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use datatypes::data_type::ConcreteDataType;
use datatypes::schema::{ColumnSchema, SchemaBuilder};
use snafu::ResultExt;
use table::engine::EngineContext;
use table::engine::TableEngineRef;
use table::requests::CreateTableRequest;
use tempdir::TempDir;

use crate::datanode::{DatanodeOptions, ObjectStoreConfig};
use crate::error::{CreateTableSnafu, Result};
use crate::instance::Instance;

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
        ColumnSchema::new("ts", ConcreteDataType::int64_datatype(), true),
    ];

    let table_name = "demo";
    let table_engine: TableEngineRef = instance.sql_handler().table_engine();
    let table = table_engine
        .create_table(
            &EngineContext::default(),
            CreateTableRequest {
                id: 1,
                catalog_name: None,
                schema_name: None,
                table_name: table_name.to_string(),
                desc: Some(" a test table".to_string()),
                schema: Arc::new(
                    SchemaBuilder::from(column_schemas)
                        .timestamp_index(3)
                        .build()
                        .expect("ts is expected to be timestamp column"),
                ),
                create_if_not_exists: true,
                primary_key_indices: Vec::default(),
                table_options: HashMap::new(),
            },
        )
        .await
        .context(CreateTableSnafu { table_name })?;

    let schema_provider = instance
        .catalog_manager()
        .catalog(DEFAULT_CATALOG_NAME)
        .unwrap()
        .schema(DEFAULT_SCHEMA_NAME)
        .unwrap();
    schema_provider
        .register_table(table_name.to_string(), table)
        .unwrap();
    Ok(())
}
