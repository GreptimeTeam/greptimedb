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
use std::time::Duration;

use catalog::local::MemoryCatalogManager;
use catalog::CatalogManagerRef;
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_procedure::local::{LocalManager, ManagerConfig};
use common_procedure::store::state_store::ObjectStateStore;
use common_procedure::{ProcedureManagerRef, ProcedureWithId};
use common_test_util::temp_dir::{create_temp_dir, TempDir};
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::{ColumnSchema, RawSchema};
use log_store::NoopLogStore;
use mito::config::EngineConfig;
use mito::engine::{MitoEngine, MITO_ENGINE};
use object_store::services::Fs;
use object_store::ObjectStore;
use storage::compaction::noop::NoopCompactionScheduler;
use storage::config::EngineConfig as StorageEngineConfig;
use storage::EngineImpl;
use table::requests::CreateTableRequest;

use crate::CreateTableProcedure;

pub struct TestEnv {
    pub dir: TempDir,
    pub table_engine: Arc<MitoEngine<EngineImpl<NoopLogStore>>>,
    pub procedure_manager: ProcedureManagerRef,
    pub catalog_manager: CatalogManagerRef,
}

impl TestEnv {
    pub fn new(prefix: &str) -> TestEnv {
        let dir = create_temp_dir(prefix);
        TestEnv::from_temp_dir(dir)
    }

    pub fn from_temp_dir(dir: TempDir) -> TestEnv {
        let store_dir = format!("{}/db", dir.path().to_string_lossy());
        let mut builder = Fs::default();
        builder.root(&store_dir);
        let object_store = ObjectStore::new(builder).unwrap().finish();

        let compaction_scheduler = Arc::new(NoopCompactionScheduler::default());
        let storage_engine = EngineImpl::new(
            StorageEngineConfig::default(),
            Arc::new(NoopLogStore::default()),
            object_store.clone(),
            compaction_scheduler,
        )
        .unwrap();
        let table_engine = Arc::new(MitoEngine::new(
            EngineConfig::default(),
            storage_engine,
            object_store,
        ));

        let procedure_dir = format!("{}/procedure", dir.path().to_string_lossy());
        let mut builder = Fs::default();
        builder.root(&procedure_dir);
        let object_store = ObjectStore::new(builder).unwrap().finish();

        let config = ManagerConfig {
            max_retry_times: 3,
            retry_delay: Duration::from_secs(500),
            ..Default::default()
        };
        let state_store = Arc::new(ObjectStateStore::new(object_store));
        let procedure_manager = Arc::new(LocalManager::new(config, state_store));

        let catalog_manager = Arc::new(MemoryCatalogManager::default());

        TestEnv {
            dir,
            table_engine,
            procedure_manager,
            catalog_manager,
        }
    }

    pub async fn create_table(&self, table_name: &str) {
        let request = new_create_request(table_name);
        let procedure = CreateTableProcedure::new(
            request,
            self.catalog_manager.clone(),
            self.table_engine.clone(),
            self.table_engine.clone(),
        );

        let procedure_with_id = ProcedureWithId::with_random_id(Box::new(procedure));
        let mut watcher = self
            .procedure_manager
            .submit(procedure_with_id)
            .await
            .unwrap();
        watcher.changed().await.unwrap();
    }
}

pub fn schema_for_test() -> RawSchema {
    let column_schemas = vec![
        // Key
        ColumnSchema::new("host", ConcreteDataType::string_datatype(), false),
        // Nullable value column: cpu
        ColumnSchema::new("cpu", ConcreteDataType::float64_datatype(), true),
        // Non-null value column: memory
        ColumnSchema::new("memory", ConcreteDataType::float64_datatype(), false),
        ColumnSchema::new(
            "ts",
            ConcreteDataType::timestamp_millisecond_datatype(),
            true,
        )
        .with_time_index(true),
    ];

    RawSchema::new(column_schemas)
}

pub fn new_create_request(table_name: &str) -> CreateTableRequest {
    CreateTableRequest {
        id: 1,
        catalog_name: DEFAULT_CATALOG_NAME.to_string(),
        schema_name: DEFAULT_SCHEMA_NAME.to_string(),
        table_name: table_name.to_string(),
        desc: Some("a test table".to_string()),
        schema: schema_for_test(),
        region_numbers: vec![0, 1],
        create_if_not_exists: true,
        primary_key_indices: vec![0],
        table_options: Default::default(),
        engine: MITO_ENGINE.to_string(),
    }
}
