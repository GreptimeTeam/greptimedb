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

mod mock_engine;
use std::collections::HashMap;
use std::sync::Arc;

use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_test_util::temp_dir::{create_temp_dir, TempDir};
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::{ColumnSchema, RawSchema, Schema, SchemaBuilder, SchemaRef};
use datatypes::vectors::VectorRef;
use log_store::NoopLogStore;
use object_store::services::Fs as Builder;
use object_store::{ObjectStore, ObjectStoreBuilder};
use storage::compaction::noop::NoopCompactionScheduler;
use storage::config::EngineConfig as StorageEngineConfig;
use storage::EngineImpl;
use table::engine::{EngineContext, TableEngine};
use table::metadata::{TableInfo, TableInfoBuilder, TableMetaBuilder, TableType};
use table::requests::{CreateTableRequest, InsertRequest, TableOptions};
use table::TableRef;

use crate::config::EngineConfig;
use crate::engine::{MitoEngine, MITO_ENGINE};
pub use crate::table::test_util::mock_engine::{MockEngine, MockRegion};

pub const TABLE_NAME: &str = "demo";

/// Create a InsertRequest with default catalog and schema.
pub fn new_insert_request(
    table_name: String,
    columns_values: HashMap<String, VectorRef>,
) -> InsertRequest {
    InsertRequest {
        catalog_name: DEFAULT_CATALOG_NAME.to_string(),
        schema_name: DEFAULT_SCHEMA_NAME.to_string(),
        table_name,
        columns_values,
        region_number: 0,
    }
}

pub fn schema_for_test() -> Schema {
    let column_schemas = vec![
        ColumnSchema::new("host", ConcreteDataType::string_datatype(), false),
        // Nullable value column: cpu
        ColumnSchema::new("cpu", ConcreteDataType::float64_datatype(), true),
        // Non-null value column: memory
        ColumnSchema::new("memory", ConcreteDataType::float64_datatype(), false),
        ColumnSchema::new(
            "ts",
            ConcreteDataType::timestamp_datatype(common_time::timestamp::TimeUnit::Millisecond),
            true,
        )
        .with_time_index(true),
    ];

    SchemaBuilder::try_from(column_schemas)
        .unwrap()
        .build()
        .expect("ts must be timestamp column")
}

pub type MockMitoEngine = MitoEngine<MockEngine>;

pub fn build_test_table_info() -> TableInfo {
    let table_meta = TableMetaBuilder::default()
        .schema(Arc::new(schema_for_test()))
        .engine(MITO_ENGINE)
        .next_column_id(1)
        // host is primary key column.
        .primary_key_indices(vec![0])
        .build()
        .unwrap();

    TableInfoBuilder::new(TABLE_NAME.to_string(), table_meta)
        .ident(0)
        .table_version(0u64)
        .table_type(TableType::Base)
        .catalog_name("greptime".to_string())
        .schema_name("public".to_string())
        .build()
        .unwrap()
}

pub async fn new_test_object_store(prefix: &str) -> (TempDir, ObjectStore) {
    let dir = create_temp_dir(prefix);
    let store_dir = dir.path().to_string_lossy();
    let accessor = Builder::default().root(&store_dir).build().unwrap();
    (dir, ObjectStore::new(accessor).finish())
}

pub fn new_create_request(schema: SchemaRef) -> CreateTableRequest {
    CreateTableRequest {
        id: 1,
        catalog_name: "greptime".to_string(),
        schema_name: "public".to_string(),
        table_name: TABLE_NAME.to_string(),
        desc: Some("a test table".to_string()),
        schema: RawSchema::from(&*schema),
        region_numbers: vec![0],
        create_if_not_exists: true,
        primary_key_indices: vec![0],
        table_options: TableOptions::default(),
    }
}

pub struct TestEngineComponents {
    pub table_engine: MitoEngine<EngineImpl<NoopLogStore>>,
    pub storage_engine: EngineImpl<NoopLogStore>,
    pub table_ref: TableRef,
    pub schema_ref: SchemaRef,
    pub object_store: ObjectStore,
    pub dir: TempDir,
}

pub async fn setup_test_engine_and_table() -> TestEngineComponents {
    let (dir, object_store) = new_test_object_store("setup_test_engine_and_table").await;
    let compaction_scheduler = Arc::new(NoopCompactionScheduler::default());
    let storage_engine = EngineImpl::new(
        StorageEngineConfig::default(),
        Arc::new(NoopLogStore::default()),
        object_store.clone(),
        compaction_scheduler,
    );
    let table_engine = MitoEngine::new(
        EngineConfig::default(),
        storage_engine.clone(),
        object_store.clone(),
    );

    let schema = Arc::new(schema_for_test());
    let table = table_engine
        .create_table(
            &EngineContext::default(),
            new_create_request(schema.clone()),
        )
        .await
        .unwrap();

    TestEngineComponents {
        table_engine,
        storage_engine,
        table_ref: table,
        schema_ref: schema,
        object_store,
        dir,
    }
}

pub async fn setup_mock_engine_and_table(
) -> (MockEngine, MockMitoEngine, TableRef, ObjectStore, TempDir) {
    let mock_engine = MockEngine::default();
    let (dir, object_store) = new_test_object_store("setup_mock_engine_and_table").await;
    let table_engine = MitoEngine::new(
        EngineConfig::default(),
        mock_engine.clone(),
        object_store.clone(),
    );

    let schema = Arc::new(schema_for_test());
    let table = table_engine
        .create_table(&EngineContext::default(), new_create_request(schema))
        .await
        .unwrap();

    (mock_engine, table_engine, table, object_store, dir)
}
