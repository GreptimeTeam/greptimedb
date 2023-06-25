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

use std::sync::Arc;

use common_catalog::consts::IMMUTABLE_FILE_ENGINE;
use common_test_util::temp_dir::{create_temp_dir, TempDir};
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::{ColumnSchema, RawSchema, Schema, SchemaBuilder, SchemaRef};
use object_store::services::Fs;
use object_store::ObjectStore;
use table::engine::{table_dir, EngineContext, TableEngine};
use table::metadata::{RawTableInfo, TableInfo, TableInfoBuilder, TableMetaBuilder, TableType};
use table::requests::{self, CreateTableRequest, TableOptions};
use table::TableRef;

use crate::config::EngineConfig;
use crate::engine::immutable::ImmutableFileTableEngine;
use crate::manifest::immutable::ImmutableMetadata;
use crate::table::immutable::ImmutableFileTableOptions;

pub const TEST_TABLE_NAME: &str = "demo";

pub fn new_test_object_store(prefix: &str) -> (TempDir, ObjectStore) {
    let dir = create_temp_dir(prefix);
    let store_dir = dir.path().to_string_lossy();
    let mut builder = Fs::default();
    let _ = builder.root(&store_dir);
    (dir, ObjectStore::new(builder).unwrap().finish())
}

pub fn test_schema() -> Schema {
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

pub fn build_test_table_info() -> TableInfo {
    let schema = test_schema();
    let table_meta = TableMetaBuilder::new_external_table()
        .schema(Arc::new(schema))
        .engine(IMMUTABLE_FILE_ENGINE)
        .build()
        .unwrap();

    TableInfoBuilder::new(TEST_TABLE_NAME, table_meta)
        .table_version(0)
        .table_type(TableType::Base)
        .catalog_name("greptime".to_string())
        .schema_name("public".to_string())
        .build()
        .unwrap()
}

pub fn build_test_table_metadata() -> ImmutableMetadata {
    let table_info = build_test_table_info();
    ImmutableMetadata {
        table_info: RawTableInfo::from(table_info),
        version: 0,
    }
}

pub struct TestEngineComponents {
    pub table_engine: ImmutableFileTableEngine,
    pub table_ref: TableRef,
    pub schema_ref: SchemaRef,
    pub object_store: ObjectStore,
    pub table_dir: String,
    pub dir: TempDir,
}

pub fn new_create_request(schema: SchemaRef) -> CreateTableRequest {
    let mut table_options = TableOptions::default();
    let _ = table_options.extra_options.insert(
        requests::IMMUTABLE_TABLE_LOCATION_KEY.to_string(),
        "mock_path".to_string(),
    );
    let _ = table_options.extra_options.insert(
        requests::IMMUTABLE_TABLE_META_KEY.to_string(),
        serde_json::to_string(&ImmutableFileTableOptions::default()).unwrap(),
    );
    let _ = table_options.extra_options.insert(
        requests::IMMUTABLE_TABLE_FORMAT_KEY.to_string(),
        "csv".to_string(),
    );

    CreateTableRequest {
        id: 1,
        catalog_name: "greptime".to_string(),
        schema_name: "public".to_string(),
        table_name: TEST_TABLE_NAME.to_string(),
        desc: Some("a test table".to_string()),
        schema: RawSchema::from(&*schema),
        region_numbers: vec![0],
        create_if_not_exists: true,
        primary_key_indices: vec![0],
        table_options,
        engine: IMMUTABLE_FILE_ENGINE.to_string(),
    }
}

pub async fn setup_test_engine_and_table(prefix: &str) -> TestEngineComponents {
    let (dir, object_store) = new_test_object_store(prefix);

    let table_engine = ImmutableFileTableEngine::new(EngineConfig::default(), object_store.clone());

    let schema_ref = Arc::new(test_schema());

    let table_ref = table_engine
        .create_table(
            &EngineContext::default(),
            new_create_request(schema_ref.clone()),
        )
        .await
        .unwrap();

    let table_info = table_ref.table_info();

    let table_dir = table_dir(
        &table_info.catalog_name,
        &table_info.schema_name,
        table_info.ident.table_id,
    );

    TestEngineComponents {
        table_engine,
        table_ref,
        schema_ref,
        object_store,
        table_dir,
        dir,
    }
}
