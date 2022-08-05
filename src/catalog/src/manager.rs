use std::any::Any;
use std::sync::Arc;

use common_recordbatch::RecordBatch;
use common_telemetry::info;
use datatypes::prelude::ScalarVector;
use datatypes::vectors::{BinaryVector, UInt8Vector};
use futures_util::StreamExt;
use snafu::{ensure, OptionExt, ResultExt};
use table::engine::{EngineContext, TableEngineRef};
use table::requests::OpenTableRequest;

use super::error::Result;
use crate::consts::{INFORMATION_SCHEMA_NAME, SYSTEM_CATALOG_NAME, SYSTEM_CATALOG_TABLE_NAME};
use crate::error::{
    CatalogNotFoundSnafu, CatalogTypeMismatchSnafu, OpenTableSnafu, ReadSystemCatalogSnafu,
    SchemaNotFoundSnafu, SystemCatalogSnafu, SystemCatalogTypeMismatchSnafu, TableNotFoundSnafu,
};
use crate::memory::{MemoryCatalogList, MemoryCatalogProvider, MemorySchemaProvider};
use crate::system::{decode_system_catalog, Entry, SystemCatalogTable, TableEntry};
use crate::tables::SystemCatalog;
use crate::{CatalogList, CatalogManager, CatalogProvider, SchemaProvider};

/// A `CatalogManager` consists of a system catalog and a bunch of user catalogs.
// TODO(hl): Replace current `memory::new_memory_catalog_list()` with CatalogManager
#[allow(dead_code)]
pub struct CatalogManagerImpl {
    system: Arc<SystemCatalog>,
    catalogs: Arc<MemoryCatalogList>,
    engine: TableEngineRef,
}

#[allow(dead_code)]
impl CatalogManagerImpl {
    /// Create a new [CatalogManager] with given user catalogs and table engine
    pub async fn try_new(catalogs: Arc<MemoryCatalogList>, engine: TableEngineRef) -> Result<Self> {
        let table = SystemCatalogTable::new(engine.clone()).await?;
        let system_catalog = Arc::new(SystemCatalog::new(table, catalogs.clone()));

        Ok(Self {
            system: system_catalog,
            catalogs,
            engine,
        })
    }

    /// Scan all entries from system catalog table
    pub async fn init(&mut self) -> Result<()> {
        self.init_system_catalog()?;
        let mut system_records = self.system.information_schema.system.records().await?;
        while let Some(records) = system_records
            .next()
            .await
            .transpose()
            .context(ReadSystemCatalogSnafu)?
        {
            self.handle_system_catalog_entries(records).await?;
        }
        Ok(())
    }

    fn init_system_catalog(&mut self) -> Result<()> {
        let system_schema = MemorySchemaProvider::new();
        system_schema.register_table(
            SYSTEM_CATALOG_TABLE_NAME.to_string(),
            self.system.information_schema.system.clone(),
        )?;
        let system_catalog = MemoryCatalogProvider::new();
        system_catalog.register_schema(INFORMATION_SCHEMA_NAME, Arc::new(system_schema));
        self.catalogs
            .register_catalog(SYSTEM_CATALOG_NAME.to_string(), Arc::new(system_catalog));
        Ok(())
    }

    async fn handle_system_catalog_entries(&self, records: RecordBatch) -> Result<()> {
        ensure!(
            records.df_recordbatch.columns().len() >= 2,
            SystemCatalogSnafu {
                msg: format!(
                    "Length mismatch: {}",
                    records.df_recordbatch.columns().len()
                )
            }
        );

        let key = UInt8Vector::try_from_arrow_array(&records.df_recordbatch.columns()[0])
            .with_context(|_| SystemCatalogTypeMismatchSnafu {
                data_type: records.df_recordbatch.columns()[1].data_type().clone(),
            })?;

        let value = BinaryVector::try_from_arrow_array(&records.df_recordbatch.columns()[1])
            .with_context(|_| SystemCatalogTypeMismatchSnafu {
                data_type: records.df_recordbatch.columns()[1].data_type().clone(),
            })?;

        for (k, v) in key.iter_data().zip(value.iter_data()) {
            let entry = decode_system_catalog(k, v)?;

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
                    catalog
                        .as_any()
                        .downcast_ref::<MemoryCatalogProvider>() // maybe remove this downcast
                        .context(CatalogTypeMismatchSnafu)?
                        .register_schema(&s.schema_name, Arc::new(MemorySchemaProvider::new()));
                    info!("Registered schema: {:?}", s);
                }
                Entry::Table(t) => {
                    self.open_and_register_table(&t).await?;
                    info!("Registered table: {:?}", t)
                }
            }
        }

        Ok(())
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
                catalog_name: &t.catalog_name,
                schema_name: &t.schema_name,
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
            .context(OpenTableSnafu {
                catalog_name: &t.catalog_name,
                schema_name: &t.schema_name,
                table_name: &t.table_name,
                table_id: t.table_id,
            })?
            .context(TableNotFoundSnafu {
                catalog_name: &t.catalog_name,
                schema_name: &t.schema_name,
                table_name: &t.table_name,
                table_id: t.table_id,
            })?;

        schema.register_table(t.table_name.clone(), option)?;
        Ok(())
    }
}

impl CatalogList for CatalogManagerImpl {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn register_catalog(
        &self,
        name: String,
        catalog: Arc<dyn CatalogProvider>,
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

impl CatalogManager for CatalogManagerImpl {}
