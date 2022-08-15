use std::any::Any;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use common_recordbatch::RecordBatch;
use common_telemetry::info;
use datatypes::prelude::ScalarVector;
use datatypes::vectors::{BinaryVector, UInt8Vector};
use futures_util::StreamExt;
use snafu::{ensure, OptionExt, ResultExt};
use table::engine::{EngineContext, TableEngineRef};
use table::metadata::TableId;
use table::requests::OpenTableRequest;
use table::table::numbers::NumbersTable;

use super::error::Result;
use crate::consts::{INFORMATION_SCHEMA_NAME, SYSTEM_CATALOG_NAME, SYSTEM_CATALOG_TABLE_NAME};
use crate::error::{
    CatalogNotFoundSnafu, OpenTableSnafu, ReadSystemCatalogSnafu, SchemaNotFoundSnafu,
    SystemCatalogSnafu, SystemCatalogTypeMismatchSnafu, TableNotFoundSnafu,
};
use crate::memory::{MemoryCatalogList, MemoryCatalogProvider, MemorySchemaProvider};
use crate::system::{decode_system_catalog, Entry, SystemCatalogTable, TableEntry};
use crate::tables::SystemCatalog;
use crate::{
    CatalogList, CatalogManager, CatalogProvider, CatalogProviderRef, SchemaProvider,
    DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME,
};

/// A `CatalogManager` consists of a system catalog and a bunch of user catalogs.
pub struct LocalCatalogManager {
    system: Arc<SystemCatalog>,
    catalogs: Arc<MemoryCatalogList>,
    engine: TableEngineRef,
    next_table_id: AtomicU32,
}

impl LocalCatalogManager {
    /// Create a new [CatalogManager] with given user catalogs and table engine
    pub async fn try_new(engine: TableEngineRef) -> Result<Self> {
        let table = SystemCatalogTable::new(engine.clone()).await?;
        let memory_catalog_list = crate::memory::new_memory_catalog_list()?;
        let system_catalog = Arc::new(SystemCatalog::new(
            table,
            memory_catalog_list.clone(),
            engine.clone(),
        ));
        Ok(Self {
            system: system_catalog,
            catalogs: memory_catalog_list,
            engine,
            next_table_id: AtomicU32::new(0),
        })
    }

    /// Scan all entries from system catalog table
    pub async fn init(&self) -> Result<()> {
        self.init_system_catalog()?;
        let mut system_records = self.system.information_schema.system.records().await?;
        let mut max_table_id = 0;
        while let Some(records) = system_records
            .next()
            .await
            .transpose()
            .context(ReadSystemCatalogSnafu)?
        {
            let table_id = self.handle_system_catalog_entries(records).await?;
            max_table_id = max_table_id.max(table_id);
        }
        info!(
            "All system catalog entries processed, max table id: {}",
            max_table_id
        );
        self.next_table_id
            .store(max_table_id + 1, Ordering::Relaxed);
        Ok(())
    }

    fn init_system_catalog(&self) -> Result<()> {
        let system_schema = Arc::new(MemorySchemaProvider::new());
        system_schema.register_table(
            SYSTEM_CATALOG_TABLE_NAME.to_string(),
            self.system.information_schema.system.clone(),
        )?;
        let system_catalog = Arc::new(MemoryCatalogProvider::new());
        system_catalog.register_schema(INFORMATION_SCHEMA_NAME.to_string(), system_schema);
        self.catalogs
            .register_catalog(SYSTEM_CATALOG_NAME.to_string(), system_catalog);

        let default_catalog = Arc::new(MemoryCatalogProvider::new());
        let default_schema = Arc::new(MemorySchemaProvider::new());

        // Add numbers table for test
        // TODO(hl): remove this registration
        let table = Arc::new(NumbersTable::default());
        default_schema.register_table("numbers".to_string(), table)?;

        default_catalog.register_schema(DEFAULT_SCHEMA_NAME.to_string(), default_schema);
        self.catalogs
            .register_catalog(DEFAULT_CATALOG_NAME.to_string(), default_catalog);
        Ok(())
    }

    /// Processes records from system catalog table and returns the max table id persisted
    /// in system catalog table.
    async fn handle_system_catalog_entries(&self, records: RecordBatch) -> Result<TableId> {
        ensure!(
            records.df_recordbatch.columns().len() >= 4,
            SystemCatalogSnafu {
                msg: format!(
                    "Length mismatch: {}",
                    records.df_recordbatch.columns().len()
                )
            }
        );

        let entry_type = UInt8Vector::try_from_arrow_array(&records.df_recordbatch.columns()[0])
            .with_context(|_| SystemCatalogTypeMismatchSnafu {
                data_type: records.df_recordbatch.columns()[0].data_type().clone(),
            })?;

        let key = BinaryVector::try_from_arrow_array(&records.df_recordbatch.columns()[1])
            .with_context(|_| SystemCatalogTypeMismatchSnafu {
                data_type: records.df_recordbatch.columns()[1].data_type().clone(),
            })?;

        let value = BinaryVector::try_from_arrow_array(&records.df_recordbatch.columns()[1])
            .with_context(|_| SystemCatalogTypeMismatchSnafu {
                data_type: records.df_recordbatch.columns()[3].data_type().clone(),
            })?;

        let mut max_table_id = 0;
        for ((t, k), v) in entry_type
            .iter_data()
            .zip(key.iter_data())
            .zip(value.iter_data())
        {
            let entry = decode_system_catalog(t, k, v)?;
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
                            .catalog(&s.catalog_name)
                            .context(CatalogNotFoundSnafu {
                                catalog_name: &s.catalog_name,
                            })?;
                    catalog.register_schema(
                        s.schema_name.clone(),
                        Arc::new(MemorySchemaProvider::new()),
                    );
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

    async fn open_and_register_table(&self, t: &TableEntry) -> Result<()> {
        let catalog = self
            .catalogs
            .catalog(&t.catalog_name)
            .context(CatalogNotFoundSnafu {
                catalog_name: &t.catalog_name,
            })?;
        let schema = catalog
            .schema(&t.schema_name)
            .context(SchemaNotFoundSnafu {
                schema_info: format!("{}.{}", &t.catalog_name, &t.schema_name),
            })?;

        let context = EngineContext {};
        let request = OpenTableRequest {
            catalog_name: t.catalog_name.clone(),
            schema_name: t.schema_name.clone(),
            table_name: t.table_name.clone(),
            table_id: t.table_id,
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
    ) -> Option<Arc<dyn CatalogProvider>> {
        self.catalogs.register_catalog(name, catalog)
    }

    fn catalog_names(&self) -> Vec<String> {
        let mut res = self.catalogs.catalog_names();
        res.push(SYSTEM_CATALOG_NAME.to_string());
        res
    }

    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        if name.eq_ignore_ascii_case(SYSTEM_CATALOG_NAME) {
            Some(self.system.clone())
        } else {
            self.catalogs.catalog(name)
        }
    }
}

#[async_trait::async_trait]
impl CatalogManager for LocalCatalogManager {
    /// Start [MemoryCatalogManager] to load all information from system catalog table.
    /// Make sure table engine is initialized before starting [MemoryCatalogManager].
    async fn start(&self) -> Result<()> {
        self.init().await
    }

    #[inline]
    fn next_table_id(&self) -> TableId {
        self.next_table_id.fetch_add(1, Ordering::Relaxed)
    }

    async fn register_table(
        &self,
        catalog: Option<String>,
        schema: Option<String>,
        table_name: String,
        table_id: TableId,
    ) -> Result<usize> {
        let catalog = catalog.unwrap_or_else(|| DEFAULT_CATALOG_NAME.to_string());
        let schema = schema.unwrap_or_else(|| DEFAULT_SCHEMA_NAME.to_string());
        self.system
            .register_table(catalog, schema, table_name, table_id)
            .await
    }
}
