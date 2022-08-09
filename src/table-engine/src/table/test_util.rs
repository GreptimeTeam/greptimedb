mod mock_engine;

use std::sync::Arc;

use datatypes::prelude::ConcreteDataType;
use datatypes::schema::SchemaRef;
use datatypes::schema::{ColumnSchema, Schema};
use log_store::fs::noop::NoopLogStore;
use object_store::{backend::fs::Backend, ObjectStore};
use storage::config::EngineConfig as StorageEngineConfig;
use storage::EngineImpl;
use table::engine::EngineContext;
use table::engine::TableEngine;
use table::requests::CreateTableRequest;
use table::TableRef;
use tempdir::TempDir;

use crate::config::EngineConfig;
use crate::engine::MitoEngine;
use crate::table::test_util::mock_engine::MockEngine;

pub const TABLE_NAME: &str = "demo";

fn schema_for_test() -> Schema {
    let column_schemas = vec![
        ColumnSchema::new("ts", ConcreteDataType::int64_datatype(), true),
        ColumnSchema::new("host", ConcreteDataType::string_datatype(), false),
        ColumnSchema::new("cpu", ConcreteDataType::float64_datatype(), true),
        ColumnSchema::new("memory", ConcreteDataType::float64_datatype(), true),
    ];

    Schema::with_timestamp_index(column_schemas, 0).expect("ts must be timestamp column")
}

pub type MockMitoEngine = MitoEngine<MockEngine>;

pub async fn new_test_object_store(prefix: &str) -> (TempDir, ObjectStore) {
    let dir = TempDir::new(prefix).unwrap();
    let store_dir = dir.path().to_string_lossy();
    let accessor = Backend::build().root(&store_dir).finish().await.unwrap();

    (dir, ObjectStore::new(accessor))
}

pub async fn setup_test_engine_and_table() -> (
    MitoEngine<EngineImpl<NoopLogStore>>,
    TableRef,
    SchemaRef,
    TempDir,
) {
    let (dir, object_store) = new_test_object_store("setup_test_engine_and_table").await;

    let table_engine = MitoEngine::new(
        EngineConfig::default(),
        EngineImpl::new(
            StorageEngineConfig::default(),
            Arc::new(NoopLogStore::default()),
            object_store.clone(),
        ),
        object_store,
    );

    let schema = Arc::new(schema_for_test());
    let table = table_engine
        .create_table(
            &EngineContext::default(),
            CreateTableRequest {
                name: TABLE_NAME.to_string(),
                desc: Some("a test table".to_string()),
                schema: schema.clone(),
                create_if_not_exists: true,
                primary_key_indices: Vec::default(),
            },
        )
        .await
        .unwrap();

    (table_engine, table, schema, dir)
}

pub async fn setup_mock_engine_and_table() -> (MockEngine, MockMitoEngine, TableRef, ObjectStore) {
    let mock_engine = MockEngine::default();
    let (_dir, object_store) = new_test_object_store("setup_mock_engine_and_table").await;
    let table_engine = MitoEngine::new(
        EngineConfig::default(),
        mock_engine.clone(),
        object_store.clone(),
    );

    let schema = Arc::new(schema_for_test());
    let table = table_engine
        .create_table(
            &EngineContext::default(),
            CreateTableRequest {
                name: TABLE_NAME.to_string(),
                desc: None,
                schema: schema.clone(),
                create_if_not_exists: true,
                primary_key_indices: Vec::default(),
            },
        )
        .await
        .unwrap();

    (mock_engine, table_engine, table, object_store)
}
