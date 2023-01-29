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
use std::sync::Arc;

use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, MIN_USER_TABLE_ID};
use common_query::Output;
use common_recordbatch::util;
use datatypes::data_type::ConcreteDataType;
use datatypes::schema::{ColumnSchema, SchemaBuilder};
use mito::config::EngineConfig;
use mito::table::test_util::{new_test_object_store, MockEngine, MockMitoEngine};
use query::QueryEngineFactory;
use servers::Mode;
use snafu::ResultExt;
use table::engine::{EngineContext, TableEngineRef};
use table::requests::CreateTableRequest;
use tempdir::TempDir;

use crate::datanode::{DatanodeOptions, FileConfig, ObjectStoreConfig, WalConfig};
use crate::error::{CreateTableSnafu, Result};
use crate::instance::Instance;
use crate::sql::SqlHandler;

pub(crate) struct MockInstance {
    instance: Instance,
    _guard: TestGuard,
}

impl MockInstance {
    pub(crate) async fn new(name: &str) -> Self {
        let (opts, _guard) = create_tmp_dir_and_datanode_opts(name);

        let instance = Instance::with_mock_meta_client(&opts).await.unwrap();
        instance.start().await.unwrap();

        MockInstance { instance, _guard }
    }

    pub(crate) fn inner(&self) -> &Instance {
        &self.instance
    }
}

struct TestGuard {
    _wal_tmp_dir: TempDir,
    _data_tmp_dir: TempDir,
}

fn create_tmp_dir_and_datanode_opts(name: &str) -> (DatanodeOptions, TestGuard) {
    let wal_tmp_dir = TempDir::new(&format!("gt_wal_{name}")).unwrap();
    let data_tmp_dir = TempDir::new(&format!("gt_data_{name}")).unwrap();
    let opts = DatanodeOptions {
        wal: WalConfig {
            dir: wal_tmp_dir.path().to_str().unwrap().to_string(),
            ..Default::default()
        },
        storage: ObjectStoreConfig::File(FileConfig {
            data_dir: data_tmp_dir.path().to_str().unwrap().to_string(),
        }),
        mode: Mode::Standalone,
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

pub(crate) async fn create_test_table(
    instance: &Instance,
    ts_type: ConcreteDataType,
) -> Result<()> {
    let column_schemas = vec![
        ColumnSchema::new("host", ConcreteDataType::string_datatype(), true),
        ColumnSchema::new("cpu", ConcreteDataType::float64_datatype(), true),
        ColumnSchema::new("memory", ConcreteDataType::float64_datatype(), true),
        ColumnSchema::new("ts", ts_type, true).with_time_index(true),
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

    let schema_provider = instance
        .catalog_manager
        .schema(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME)
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

    let catalog_list = catalog::local::new_memory_catalog_list().unwrap();
    let factory = QueryEngineFactory::new(catalog_list);

    SqlHandler::new(mock_engine, catalog_manager, factory.query_engine())
}

pub(crate) async fn setup_test_instance(test_name: &str) -> MockInstance {
    let instance = MockInstance::new(test_name).await;

    create_test_table(
        instance.inner(),
        ConcreteDataType::timestamp_millisecond_datatype(),
    )
    .await
    .unwrap();

    instance
}

pub async fn check_output_stream(output: Output, expected: String) {
    let recordbatches = match output {
        Output::Stream(stream) => util::collect_batches(stream).await.unwrap(),
        Output::RecordBatches(recordbatches) => recordbatches,
        _ => unreachable!(),
    };
    let pretty_print = recordbatches.pretty_print().unwrap();
    assert_eq!(pretty_print, expected);
}
