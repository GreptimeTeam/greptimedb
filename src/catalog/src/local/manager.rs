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
    SYSTEM_CATALOG_NAME, SYSTEM_CATALOG_TABLE_NAME,
};
use common_recordbatch::{RecordBatch, SendableRecordBatchStream};
use common_telemetry::{error, info};
use datatypes::prelude::ScalarVector;
use datatypes::vectors::{BinaryVector, UInt8Vector};
use futures_util::lock::Mutex;
use snafu::{ensure, OptionExt, ResultExt};
use table::engine::{EngineContext, TableEngineRef};
use table::metadata::TableId;
use table::requests::OpenTableRequest;
use table::table::numbers::NumbersTable;
use table::table::TableIdProvider;
use table::TableRef;

use crate::error::{
    self, CatalogNotFoundSnafu, IllegalManagerStateSnafu, OpenTableSnafu, ReadSystemCatalogSnafu,
    Result, SchemaExistsSnafu, SchemaNotFoundSnafu, SystemCatalogSnafu,
    SystemCatalogTypeMismatchSnafu, TableExistsSnafu, TableNotFoundSnafu,
};
use crate::local::memory::{MemoryCatalogManager, MemoryCatalogProvider, MemorySchemaProvider};
use crate::system::{
    decode_system_catalog, Entry, SystemCatalogTable, TableEntry, ENTRY_TYPE_INDEX, KEY_INDEX,
    VALUE_INDEX,
};
use crate::tables::SystemCatalog;
use crate::{
    format_full_table_name, handle_system_table_request, CatalogList, CatalogManager,
    CatalogProvider, CatalogProviderRef, DeregisterTableRequest, RegisterSchemaRequest,
    RegisterSystemTableRequest, RegisterTableRequest, RenameTableRequest, SchemaProvider,
    SchemaProviderRef,
};

/// A `CatalogManager` consists of a system catalog and a bunch of user catalogs.
pub struct LocalCatalogManager {
    system: Arc<SystemCatalog>,
    catalogs: Arc<MemoryCatalogManager>,
    engine: TableEngineRef,
    next_table_id: AtomicU32,
    init_lock: Mutex<bool>,
    register_lock: Mutex<()>,
    system_table_requests: Mutex<Vec<RegisterSystemTableRequest>>,
}

impl LocalCatalogManager {
    /// Create a new [CatalogManager] with given user catalogs and table engine
    pub async fn try_new(engine: TableEngineRef) -> Result<Self> {
        let table = SystemCatalogTable::new(engine.clone()).await?;
        let memory_catalog_list = crate::local::memory::new_memory_catalog_list()?;
        let system_catalog = Arc::new(SystemCatalog::new(
            table,
            memory_catalog_list.clone(),
            engine.clone(),
        ));
        Ok(Self {
            system: system_catalog,
            catalogs: memory_catalog_list,
            engine,
            next_table_id: AtomicU32::new(MIN_USER_TABLE_ID),
            init_lock: Mutex::new(false),
            register_lock: Mutex::new(()),
            system_table_requests: Mutex::new(Vec::default()),
        })
    }

    /// Scan all entries from system catalog table
    pub async fn init(&self) -> Result<()> {
        self.init_system_catalog()?;
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
        handle_system_table_request(self, self.engine.clone(), &mut sys_table_requests).await?;
        Ok(())
    }

    fn init_system_catalog(&self) -> Result<()> {
        let system_schema = Arc::new(MemorySchemaProvider::new());
        system_schema.register_table(
            SYSTEM_CATALOG_TABLE_NAME.to_string(),
            self.system.information_schema.system.clone(),
        )?;
        let system_catalog = Arc::new(MemoryCatalogProvider::new());
        system_catalog.register_schema(INFORMATION_SCHEMA_NAME.to_string(), system_schema)?;
        self.catalogs
            .register_catalog(SYSTEM_CATALOG_NAME.to_string(), system_catalog)?;

        let default_catalog = Arc::new(MemoryCatalogProvider::new());
        let default_schema = Arc::new(MemorySchemaProvider::new());

        // Add numbers table for test
        let table = Arc::new(NumbersTable::default());
        default_schema.register_table("numbers".to_string(), table)?;

        default_catalog.register_schema(DEFAULT_SCHEMA_NAME.to_string(), default_schema)?;
        self.catalogs
            .register_catalog(DEFAULT_CATALOG_NAME.to_string(), default_catalog)?;
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
                    self.catalogs.register_catalog_if_absent(
                        c.catalog_name.clone(),
                        Arc::new(MemoryCatalogProvider::new()),
                    );
                    info!("Register catalog: {}", c.catalog_name);
                }
                Entry::Schema(s) => {
                    let catalog =
                        self.catalogs
                            .catalog(&s.catalog_name)?
                            .context(CatalogNotFoundSnafu {
                                catalog_name: &s.catalog_name,
                            })?;
                    catalog.register_schema(
                        s.schema_name.clone(),
                        Arc::new(MemorySchemaProvider::new()),
                    )?;
                    info!("Registered schema: {:?}", s);
                }
                Entry::Table(t) => {
                    self.open_and_register_table(&t).await?;
                    info!("Registered table: {:?}", t);
                    max_table_id = max_table_id.max(t.table_id);
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
        let catalog = self
            .catalogs
            .catalog(&t.catalog_name)?
            .context(CatalogNotFoundSnafu {
                catalog_name: &t.catalog_name,
            })?;
        let schema = catalog
            .schema(&t.schema_name)?
            .context(SchemaNotFoundSnafu {
                catalog: &t.catalog_name,
                schema: &t.schema_name,
            })?;

        let context = EngineContext {};
        let request = OpenTableRequest {
            catalog_name: t.catalog_name.clone(),
            schema_name: t.schema_name.clone(),
            table_name: t.table_name.clone(),
            table_id: t.table_id,
            region_numbers: vec![0],
        };

        let option = self
            .engine
            .open_table(&context, request)
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

        schema.register_table(t.table_name.clone(), option)?;
        Ok(())
    }
}

impl CatalogList for LocalCatalogManager {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn register_catalog(
        &self,
        name: String,
        catalog: CatalogProviderRef,
    ) -> Result<Option<CatalogProviderRef>> {
        self.catalogs.register_catalog(name, catalog)
    }

    fn catalog_names(&self) -> Result<Vec<String>> {
        let mut res = self.catalogs.catalog_names()?;
        res.push(SYSTEM_CATALOG_NAME.to_string());
        Ok(res)
    }

    fn catalog(&self, name: &str) -> Result<Option<CatalogProviderRef>> {
        if name.eq_ignore_ascii_case(SYSTEM_CATALOG_NAME) {
            Ok(Some(self.system.clone()))
        } else {
            self.catalogs.catalog(name)
        }
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
        let started = self.init_lock.lock().await;

        ensure!(
            *started,
            IllegalManagerStateSnafu {
                msg: "Catalog manager not started",
            }
        );

        let catalog_name = &request.catalog;
        let schema_name = &request.schema;

        let catalog = self
            .catalogs
            .catalog(catalog_name)?
            .context(CatalogNotFoundSnafu { catalog_name })?;
        let schema = catalog
            .schema(schema_name)?
            .with_context(|| SchemaNotFoundSnafu {
                catalog: catalog_name,
                schema: schema_name,
            })?;

        {
            let _lock = self.register_lock.lock().await;
            if let Some(existing) = schema.table(&request.table_name)? {
                if existing.table_info().ident.table_id != request.table_id {
                    error!(
                        "Unexpected table register request: {:?}, existing: {:?}",
                        request,
                        existing.table_info()
                    );
                    return TableExistsSnafu {
                        table: format_full_table_name(
                            catalog_name,
                            schema_name,
                            &request.table_name,
                        ),
                    }
                    .fail();
                }
                // Try to register table with same table id, just ignore.
                Ok(false)
            } else {
                // table does not exist
                self.system
                    .register_table(
                        catalog_name.clone(),
                        schema_name.clone(),
                        request.table_name.clone(),
                        request.table_id,
                    )
                    .await?;
                schema.register_table(request.table_name, request.table)?;
                Ok(true)
            }
        }
    }

    async fn rename_table(&self, request: RenameTableRequest) -> Result<bool> {
        let started = self.init_lock.lock().await;

        ensure!(
            *started,
            IllegalManagerStateSnafu {
                msg: "Catalog manager not started",
            }
        );

        let catalog_name = &request.catalog;
        let schema_name = &request.schema;

        let catalog = self
            .catalogs
            .catalog(catalog_name)?
            .context(CatalogNotFoundSnafu { catalog_name })?;

        let schema = catalog
            .schema(schema_name)?
            .with_context(|| SchemaNotFoundSnafu {
                catalog: catalog_name,
                schema: schema_name,
            })?;

        // rename table in system catalog
        self.system
            .register_table(
                catalog_name.clone(),
                schema_name.clone(),
                request.new_table_name.clone(),
                request.table_id,
            )
            .await?;
        Ok(schema
            .rename_table(&request.table_name, request.new_table_name)
            .is_ok())
    }

    async fn deregister_table(&self, request: DeregisterTableRequest) -> Result<bool> {
        {
            let started = *self.init_lock.lock().await;
            ensure!(started, IllegalManagerStateSnafu { msg: "not started" });
        }

        {
            let _ = self.register_lock.lock().await;

            let DeregisterTableRequest {
                catalog,
                schema,
                table_name,
            } = &request;
            let table_id = self
                .catalogs
                .table(catalog, schema, table_name)?
                .with_context(|| error::TableNotExistSnafu {
                    table: format!("{catalog}.{schema}.{table_name}"),
                })?
                .table_info()
                .ident
                .table_id;

            if !self.system.deregister_table(&request, table_id).await? {
                return Ok(false);
            }

            self.catalogs.deregister_table(request).await
        }
    }

    async fn register_schema(&self, request: RegisterSchemaRequest) -> Result<bool> {
        let started = self.init_lock.lock().await;
        ensure!(
            *started,
            IllegalManagerStateSnafu {
                msg: "Catalog manager not started",
            }
        );
        let catalog_name = &request.catalog;
        let schema_name = &request.schema;

        let catalog = self
            .catalogs
            .catalog(catalog_name)?
            .context(CatalogNotFoundSnafu { catalog_name })?;

        {
            let _lock = self.register_lock.lock().await;
            ensure!(
                catalog.schema(schema_name)?.is_none(),
                SchemaExistsSnafu {
                    schema: schema_name,
                }
            );
            self.system
                .register_schema(request.catalog, schema_name.clone())
                .await?;
            catalog.register_schema(request.schema, Arc::new(MemorySchemaProvider::new()))?;
            Ok(true)
        }
    }

    async fn register_system_table(&self, request: RegisterSystemTableRequest) -> Result<()> {
        ensure!(
            !*self.init_lock.lock().await,
            IllegalManagerStateSnafu {
                msg: "Catalog manager already started",
            }
        );

        let mut sys_table_requests = self.system_table_requests.lock().await;
        sys_table_requests.push(request);

        Ok(())
    }

    fn schema(&self, catalog: &str, schema: &str) -> Result<Option<SchemaProviderRef>> {
        self.catalogs
            .catalog(catalog)?
            .context(CatalogNotFoundSnafu {
                catalog_name: catalog,
            })?
            .schema(schema)
    }

    fn table(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<Option<TableRef>> {
        let catalog = self
            .catalogs
            .catalog(catalog_name)?
            .context(CatalogNotFoundSnafu { catalog_name })?;
        let schema = catalog
            .schema(schema_name)?
            .with_context(|| SchemaNotFoundSnafu {
                catalog: catalog_name,
                schema: schema_name,
            })?;
        schema.table(table_name)
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

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
