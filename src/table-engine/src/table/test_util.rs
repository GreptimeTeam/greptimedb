mod mock_engine;

use std::sync::Arc;

use datatypes::prelude::ConcreteDataType;
use datatypes::schema::SchemaRef;
use datatypes::schema::{ColumnSchema, Schema};
use log_store::fs::noop::NoopLogStore;
use storage::config::EngineConfig;
use storage::EngineImpl;
use table::engine::EngineContext;
use table::engine::TableEngine;
use table::requests::CreateTableRequest;
use table::TableRef;
use tempdir::TempDir;

use crate::engine::MitoEngine;
use crate::table::test_util::mock_engine::MockEngine;

pub const TABLE_NAME: &str = "demo";

fn schema_for_test() -> Schema {
    let column_schemas = vec![
        ColumnSchema::new("host", ConcreteDataType::string_datatype(), false),
        ColumnSchema::new("ts", ConcreteDataType::int64_datatype(), true),
        ColumnSchema::new("cpu", ConcreteDataType::float64_datatype(), true),
        ColumnSchema::new("memory", ConcreteDataType::float64_datatype(), true),
    ];

    Schema::with_timestamp_index(column_schemas, 1).expect("ts must be timestamp column")
}

pub type MockMitoEngine = MitoEngine<MockEngine>;

pub async fn setup_test_engine_and_table() -> (
    MitoEngine<EngineImpl<NoopLogStore>>,
    TableRef,
    SchemaRef,
    TempDir,
) {
    let dir = TempDir::new("setup_test_engine_and_table").unwrap();
    let store_dir = dir.path().to_string_lossy();

    let table_engine = MitoEngine::new(
        EngineImpl::new(
            EngineConfig::with_store_dir(&store_dir),
            Arc::new(NoopLogStore::default()),
        )
        .await
        .unwrap(),
    );

    let schema = Arc::new(schema_for_test());
    let table = table_engine
        .create_table(
            &EngineContext::default(),
            CreateTableRequest {
                name: TABLE_NAME.to_string(),
                desc: Some("a test table".to_string()),
                schema: schema.clone(),
            },
        )
        .await
        .unwrap();

    (table_engine, table, schema, dir)
}

pub async fn setup_mock_engine_and_table() -> (MockEngine, MockMitoEngine, TableRef) {
    let mock_engine = MockEngine::default();
    let table_engine = MitoEngine::new(mock_engine.clone());

    let schema = Arc::new(schema_for_test());
    let table = table_engine
        .create_table(
            &EngineContext::default(),
            CreateTableRequest {
                name: TABLE_NAME.to_string(),
                desc: None,
                schema: schema.clone(),
            },
        )
        .await
        .unwrap();

    (mock_engine, table_engine, table)
}
