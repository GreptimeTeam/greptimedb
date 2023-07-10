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

use std::any::Any;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use common_catalog::consts::{
    DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, INFORMATION_SCHEMA_NAME, MIN_USER_TABLE_ID,
    MITO_ENGINE, NUMBERS_TABLE_ID, SYSTEM_CATALOG_NAME, SYSTEM_CATALOG_TABLE_ID,
    SYSTEM_CATALOG_TABLE_NAME,
};
use common_catalog::format_full_table_name;
use common_recordbatch::{RecordBatch, SendableRecordBatchStream};
use common_telemetry::{error, info};
use datatypes::prelude::ScalarVector;
use datatypes::vectors::{BinaryVector, UInt8Vector};
use futures_util::lock::Mutex;
use metrics::increment_gauge;
use snafu::{ensure, OptionExt, ResultExt};
use table::engine::manager::TableEngineManagerRef;
use table::engine::EngineContext;
use table::metadata::TableId;
use table::requests::OpenTableRequest;
use table::table::numbers::{NumbersTable, NUMBERS_TABLE_NAME};
use table::table::TableIdProvider;
use table::TableRef;

use crate::error::{
    self, CatalogNotFoundSnafu, IllegalManagerStateSnafu, OpenTableSnafu, ReadSystemCatalogSnafu,
    Result, SchemaExistsSnafu, SchemaNotFoundSnafu, SystemCatalogSnafu,
    SystemCatalogTypeMismatchSnafu, TableEngineNotFoundSnafu, TableExistsSnafu, TableNotExistSnafu,
    TableNotFoundSnafu, UnimplementedSnafu,
};
use crate::information_schema::InformationSchemaProvider;
use crate::local::memory::MemoryCatalogManager;
use crate::system::{
    decode_system_catalog, Entry, SystemCatalogTable, TableEntry, ENTRY_TYPE_INDEX, KEY_INDEX,
    VALUE_INDEX,
};
use crate::tables::SystemCatalog;
use crate::{
    handle_system_table_request, CatalogManager, CatalogManagerRef, DeregisterSchemaRequest,
    DeregisterTableRequest, RegisterSchemaRequest, RegisterSystemTableRequest,
    RegisterTableRequest, RenameTableRequest,
};

/// A `CatalogManager` consists of a system catalog and a bunch of user catalogs.
pub struct LocalCatalogManager {
    system: Arc<SystemCatalog>,
    catalogs: Arc<MemoryCatalogManager>,
    engine_manager: TableEngineManagerRef,
    next_table_id: AtomicU32,
    init_lock: Mutex<bool>,
    register_lock: Mutex<()>,
    system_table_requests: Mutex<Vec<RegisterSystemTableRequest>>,
}

impl LocalCatalogManager {
    /// Create a new [CatalogManager] with given user catalogs and mito engine
    pub async fn try_new(engine_manager: TableEngineManagerRef) -> Result<Self> {
        let engine = engine_manager
            .engine(MITO_ENGINE)
            .context(TableEngineNotFoundSnafu {
                engine_name: MITO_ENGINE,
            })?;
        let table = SystemCatalogTable::new(engine.clone()).await?;
        let memory_catalog_manager = crate::local::memory::new_memory_catalog_manager()?;
        let system_catalog = Arc::new(SystemCatalog::new(table));
        Ok(Self {
            system: system_catalog,
            catalogs: memory_catalog_manager,
            engine_manager,
            next_table_id: AtomicU32::new(MIN_USER_TABLE_ID),
            init_lock: Mutex::new(false),
            register_lock: Mutex::new(()),
            system_table_requests: Mutex::new(Vec::default()),
        })
    }

    /// Scan all entries from system catalog table
    pub async fn init(&self) -> Result<()> {
        self.init_system_catalog().await?;
        let system_records = self.system.information_schema.system.records().await?;
        let entries = self.collect_system_catalog_entries(system_records).await?;
        let max_table_id = self.handle_system_catalog_entries(entries).await?;

        info!(
            "All system catalog entries processed, max table id: {}",
            max_table_id
        );
        self.next_table_id
            .store((max_table_id + 1).max(MIN_USER_TABLE_ID), Ordering::Relaxed);
        *self.init_lock.lock().await = true;

        // Processing system table hooks
        let mut sys_table_requests = self.system_table_requests.lock().await;
        let engine = self
            .engine_manager
            .engine(MITO_ENGINE)
            .context(TableEngineNotFoundSnafu {
                engine_name: MITO_ENGINE,
            })?;

        handle_system_table_request(self, engine, &mut sys_table_requests).await?;
        Ok(())
    }

    async fn init_system_catalog(&self) -> Result<()> {
        // register SystemCatalogTable
        let _ = self
            .catalogs
            .register_catalog_sync(SYSTEM_CATALOG_NAME.to_string())?;
        let _ = self.catalogs.register_schema_sync(RegisterSchemaRequest {
            catalog: SYSTEM_CATALOG_NAME.to_string(),
            schema: INFORMATION_SCHEMA_NAME.to_string(),
        })?;
        let register_table_req = RegisterTableRequest {
            catalog: SYSTEM_CATALOG_NAME.to_string(),
            schema: INFORMATION_SCHEMA_NAME.to_string(),
            table_name: SYSTEM_CATALOG_TABLE_NAME.to_string(),
            table_id: SYSTEM_CATALOG_TABLE_ID,
            table: self.system.information_schema.system.clone(),
        };
        let _ = self.catalogs.register_table(register_table_req).await?;

        // register default catalog and default schema
        let _ = self
            .catalogs
            .register_catalog_sync(DEFAULT_CATALOG_NAME.to_string())?;
        let _ = self.catalogs.register_schema_sync(RegisterSchemaRequest {
            catalog: DEFAULT_CATALOG_NAME.to_string(),
            schema: DEFAULT_SCHEMA_NAME.to_string(),
        })?;

        // Add numbers table for test
        let numbers_table = Arc::new(NumbersTable::default());
        let register_number_table_req = RegisterTableRequest {
            catalog: DEFAULT_CATALOG_NAME.to_string(),
            schema: DEFAULT_SCHEMA_NAME.to_string(),
            table_name: NUMBERS_TABLE_NAME.to_string(),
            table_id: NUMBERS_TABLE_ID,
            table: numbers_table,
        };

        let _ = self
            .catalogs
            .register_table(register_number_table_req)
            .await?;

        Ok(())
    }

    /// Collect stream of system catalog entries to `Vec<Entry>`
    async fn collect_system_catalog_entries(
        &self,
        stream: SendableRecordBatchStream,
    ) -> Result<Vec<Entry>> {
        let record_batch = common_recordbatch::util::collect(stream)
            .await
            .context(ReadSystemCatalogSnafu)?;
        let rbs = record_batch
            .into_iter()
            .map(Self::record_batch_to_entry)
            .collect::<Result<Vec<_>>>()?;
        Ok(rbs.into_iter().flat_map(Vec::into_iter).collect::<_>())
    }

    /// Convert `RecordBatch` to a vector of `Entry`.
    fn record_batch_to_entry(rb: RecordBatch) -> Result<Vec<Entry>> {
        ensure!(
            rb.num_columns() >= 6,
            SystemCatalogSnafu {
                msg: format!("Length mismatch: {}", rb.num_columns())
            }
        );

        let entry_type = rb
            .column(ENTRY_TYPE_INDEX)
            .as_any()
            .downcast_ref::<UInt8Vector>()
            .with_context(|| SystemCatalogTypeMismatchSnafu {
                data_type: rb.column(ENTRY_TYPE_INDEX).data_type(),
            })?;

        let key = rb
            .column(KEY_INDEX)
            .as_any()
            .downcast_ref::<BinaryVector>()
            .with_context(|| SystemCatalogTypeMismatchSnafu {
                data_type: rb.column(KEY_INDEX).data_type(),
            })?;

        let value = rb
            .column(VALUE_INDEX)
            .as_any()
            .downcast_ref::<BinaryVector>()
            .with_context(|| SystemCatalogTypeMismatchSnafu {
                data_type: rb.column(VALUE_INDEX).data_type(),
            })?;

        let mut res = Vec::with_capacity(rb.num_rows());
        for ((t, k), v) in entry_type
            .iter_data()
            .zip(key.iter_data())
            .zip(value.iter_data())
        {
            let entry = decode_system_catalog(t, k, v)?;
            res.push(entry);
        }
        Ok(res)
    }

    /// Processes records from system catalog table and returns the max table id persisted
    /// in system catalog table.
    async fn handle_system_catalog_entries(&self, entries: Vec<Entry>) -> Result<TableId> {
        let entries = Self::sort_entries(entries);
        let mut max_table_id = 0;
        for entry in entries {
            match entry {
                Entry::Catalog(c) => {
                    let _ = self
                        .catalogs
                        .register_catalog_if_absent(c.catalog_name.clone());
                    info!("Register catalog: {}", c.catalog_name);
                }
                Entry::Schema(s) => {
                    let req = RegisterSchemaRequest {
                        catalog: s.catalog_name.clone(),
                        schema: s.schema_name.clone(),
                    };
                    let _ = self.catalogs.register_schema_sync(req)?;
                    info!("Registered schema: {:?}", s);
                }
                Entry::Table(t) => {
                    max_table_id = max_table_id.max(t.table_id);
                    if t.is_deleted {
                        continue;
                    }
                    self.open_and_register_table(&t).await?;
                    info!("Registered table: {:?}", t);
                }
            }
        }
        Ok(max_table_id)
    }

    /// Sort catalog entries to ensure catalog entries comes first, then schema entries,
    /// and table entries is the last.
    fn sort_entries(mut entries: Vec<Entry>) -> Vec<Entry> {
        entries.sort();
        entries
    }

    async fn open_and_register_table(&self, t: &TableEntry) -> Result<()> {
        self.check_catalog_schema_exist(&t.catalog_name, &t.schema_name)
            .await?;

        let context = EngineContext {};
        let open_request = OpenTableRequest {
            catalog_name: t.catalog_name.clone(),
            schema_name: t.schema_name.clone(),
            table_name: t.table_name.clone(),
            table_id: t.table_id,
            region_numbers: vec![0],
        };
        let engine = self
            .engine_manager
            .engine(&t.engine)
            .context(TableEngineNotFoundSnafu {
                engine_name: &t.engine,
            })?;

        let table_ref = engine
            .open_table(&context, open_request)
            .await
            .with_context(|_| OpenTableSnafu {
                table_info: format!(
                    "{}.{}.{}, id: {}",
                    &t.catalog_name, &t.schema_name, &t.table_name, t.table_id
                ),
            })?
            .with_context(|| TableNotFoundSnafu {
                table_info: format!(
                    "{}.{}.{}, id: {}",
                    &t.catalog_name, &t.schema_name, &t.table_name, t.table_id
                ),
            })?;

        let register_request = RegisterTableRequest {
            catalog: t.catalog_name.clone(),
            schema: t.schema_name.clone(),
            table_name: t.table_name.clone(),
            table_id: t.table_id,
            table: table_ref,
        };
        let _ = self.catalogs.register_table(register_request).await?;

        Ok(())
    }

    async fn check_state(&self) -> Result<()> {
        let started = self.init_lock.lock().await;
        ensure!(
            *started,
            IllegalManagerStateSnafu {
                msg: "Catalog manager not started",
            }
        );
        Ok(())
    }

    async fn check_catalog_schema_exist(
        &self,
        catalog_name: &str,
        schema_name: &str,
    ) -> Result<()> {
        if !self.catalogs.catalog_exist(catalog_name).await? {
            return CatalogNotFoundSnafu { catalog_name }.fail()?;
        }
        if !self
            .catalogs
            .schema_exist(catalog_name, schema_name)
            .await?
        {
            return SchemaNotFoundSnafu {
                catalog: catalog_name,
                schema: schema_name,
            }
            .fail()?;
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl TableIdProvider for LocalCatalogManager {
    async fn next_table_id(&self) -> table::Result<TableId> {
        Ok(self.next_table_id.fetch_add(1, Ordering::Relaxed))
    }
}

#[async_trait::async_trait]
impl CatalogManager for LocalCatalogManager {
    /// Start [LocalCatalogManager] to load all information from system catalog table.
    /// Make sure table engine is initialized before starting [MemoryCatalogManager].
    async fn start(&self) -> Result<()> {
        self.init().await
    }

    async fn register_table(&self, request: RegisterTableRequest) -> Result<bool> {
        self.check_state().await?;

        let catalog_name = request.catalog.clone();
        let schema_name = request.schema.clone();

        self.check_catalog_schema_exist(&catalog_name, &schema_name)
            .await?;

        {
            let _lock = self.register_lock.lock().await;
            if let Some(existing) = self
                .catalogs
                .table(&request.catalog, &request.schema, &request.table_name)
                .await?
            {
                if existing.table_info().ident.table_id != request.table_id {
                    error!(
                        "Unexpected table register request: {:?}, existing: {:?}",
                        request,
                        existing.table_info()
                    );
                    return TableExistsSnafu {
                        table: format_full_table_name(
                            &catalog_name,
                            &schema_name,
                            &request.table_name,
                        ),
                    }
                    .fail();
                }
                // Try to register table with same table id, just ignore.
                Ok(false)
            } else {
                // table does not exist
                let engine = request.table.table_info().meta.engine.to_string();
                let table_name = request.table_name.clone();
                let table_id = request.table_id;
                let _ = self.catalogs.register_table(request).await?;
                let _ = self
                    .system
                    .register_table(
                        catalog_name.clone(),
                        schema_name.clone(),
                        table_name,
                        table_id,
                        engine,
                    )
                    .await?;
                increment_gauge!(
                    crate::metrics::METRIC_CATALOG_MANAGER_TABLE_COUNT,
                    1.0,
                    &[crate::metrics::db_label(&catalog_name, &schema_name)],
                );
                Ok(true)
            }
        }
    }

    async fn rename_table(&self, request: RenameTableRequest) -> Result<bool> {
        self.check_state().await?;

        let catalog_name = &request.catalog;
        let schema_name = &request.schema;

        self.check_catalog_schema_exist(catalog_name, schema_name)
            .await?;
        ensure!(
            self.catalogs
                .table(catalog_name, schema_name, &request.new_table_name)
                .await?
                .is_none(),
            TableExistsSnafu {
                table: &request.new_table_name
            }
        );

        let _lock = self.register_lock.lock().await;
        let old_table = self
            .catalogs
            .table(catalog_name, schema_name, &request.table_name)
            .await?
            .context(TableNotExistSnafu {
                table: &request.table_name,
            })?;

        let engine = old_table.table_info().meta.engine.to_string();
        // rename table in system catalog
        let _ = self
            .system
            .register_table(
                catalog_name.clone(),
                schema_name.clone(),
                request.new_table_name.clone(),
                request.table_id,
                engine,
            )
            .await?;

        self.catalogs.rename_table(request).await
    }

    async fn deregister_table(&self, request: DeregisterTableRequest) -> Result<()> {
        self.check_state().await?;

        {
            let _ = self.register_lock.lock().await;

            let DeregisterTableRequest {
                catalog,
                schema,
                table_name,
            } = &request;
            let table_id = self
                .catalogs
                .table(catalog, schema, table_name)
                .await?
                .with_context(|| error::TableNotExistSnafu {
                    table: format_full_table_name(catalog, schema, table_name),
                })?
                .table_info()
                .ident
                .table_id;

            self.system.deregister_table(&request, table_id).await?;
            self.catalogs.deregister_table(request).await
        }
    }

    async fn register_schema(&self, request: RegisterSchemaRequest) -> Result<bool> {
        self.check_state().await?;

        let catalog_name = &request.catalog;
        let schema_name = &request.schema;

        if !self.catalogs.catalog_exist(catalog_name).await? {
            return CatalogNotFoundSnafu { catalog_name }.fail()?;
        }

        {
            let _lock = self.register_lock.lock().await;
            ensure!(
                !self
                    .catalogs
                    .schema_exist(catalog_name, schema_name)
                    .await?,
                SchemaExistsSnafu {
                    schema: schema_name,
                }
            );
            let _ = self
                .system
                .register_schema(request.catalog.clone(), schema_name.clone())
                .await?;
            self.catalogs.register_schema_sync(request)
        }
    }

    async fn deregister_schema(&self, _request: DeregisterSchemaRequest) -> Result<bool> {
        UnimplementedSnafu {
            operation: "deregister schema",
        }
        .fail()
    }

    async fn register_system_table(&self, request: RegisterSystemTableRequest) -> Result<()> {
        let catalog_name = request.create_table_request.catalog_name.clone();
        let schema_name = request.create_table_request.schema_name.clone();

        let mut sys_table_requests = self.system_table_requests.lock().await;
        sys_table_requests.push(request);
        increment_gauge!(
            crate::metrics::METRIC_CATALOG_MANAGER_TABLE_COUNT,
            1.0,
            &[crate::metrics::db_label(&catalog_name, &schema_name)],
        );
        Ok(())
    }

    async fn schema_exist(&self, catalog: &str, schema: &str) -> Result<bool> {
        self.catalogs.schema_exist(catalog, schema).await
    }

    async fn table(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<Option<TableRef>> {
        if schema_name == INFORMATION_SCHEMA_NAME {
            let manager: CatalogManagerRef = self.catalogs.clone() as _;
            let provider =
                InformationSchemaProvider::new(catalog_name.to_string(), Arc::downgrade(&manager));
            return provider.table(table_name);
        }

        self.catalogs
            .table(catalog_name, schema_name, table_name)
            .await
    }

    async fn catalog_exist(&self, catalog: &str) -> Result<bool> {
        if catalog.eq_ignore_ascii_case(SYSTEM_CATALOG_NAME) {
            Ok(true)
        } else {
            self.catalogs.catalog_exist(catalog).await
        }
    }

    async fn table_exist(&self, catalog: &str, schema: &str, table: &str) -> Result<bool> {
        self.catalogs.table_exist(catalog, schema, table).await
    }

    async fn catalog_names(&self) -> Result<Vec<String>> {
        self.catalogs.catalog_names().await
    }

    async fn schema_names(&self, catalog_name: &str) -> Result<Vec<String>> {
        self.catalogs.schema_names(catalog_name).await
    }

    async fn table_names(&self, catalog_name: &str, schema_name: &str) -> Result<Vec<String>> {
        self.catalogs.table_names(catalog_name, schema_name).await
    }

    async fn register_catalog(&self, name: String) -> Result<bool> {
        self.catalogs.register_catalog(name).await
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use mito::engine::MITO_ENGINE;

    use super::*;
    use crate::system::{CatalogEntry, SchemaEntry};

    #[test]
    fn test_sort_entry() {
        let vec = vec![
            Entry::Table(TableEntry {
                catalog_name: "C1".to_string(),
                schema_name: "S1".to_string(),
                table_name: "T1".to_string(),
                table_id: 1,
                engine: MITO_ENGINE.to_string(),
                is_deleted: false,
            }),
            Entry::Catalog(CatalogEntry {
                catalog_name: "C2".to_string(),
            }),
            Entry::Schema(SchemaEntry {
                catalog_name: "C1".to_string(),
                schema_name: "S1".to_string(),
            }),
            Entry::Schema(SchemaEntry {
                catalog_name: "C2".to_string(),
                schema_name: "S2".to_string(),
            }),
            Entry::Catalog(CatalogEntry {
                catalog_name: "".to_string(),
            }),
            Entry::Table(TableEntry {
                catalog_name: "C1".to_string(),
                schema_name: "S1".to_string(),
                table_name: "T2".to_string(),
                table_id: 2,
                engine: MITO_ENGINE.to_string(),
                is_deleted: false,
            }),
        ];
        let res = LocalCatalogManager::sort_entries(vec);
        assert_matches!(res[0], Entry::Catalog(..));
        assert_matches!(res[1], Entry::Catalog(..));
        assert_matches!(res[2], Entry::Schema(..));
        assert_matches!(res[3], Entry::Schema(..));
        assert_matches!(res[4], Entry::Table(..));
        assert_matches!(res[5], Entry::Table(..));
    }
}
