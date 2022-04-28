use std::sync::Arc;

use query::catalog::memory::{MemoryCatalogList, MemoryCatalogProvider, MemorySchemaProvider};
use query::catalog::schema::{SchemaProvider, SchemaProviderRef};
use query::catalog::{CatalogList, CatalogListRef, CatalogProvider, CatalogProviderRef};
use query::QueryEngineRef;
use snafu::ResultExt;
use table::table::numbers::NumbersTable;

use crate::error::{QuerySnafu, Result};
use crate::instance::{Instance, InstanceRef};
use crate::server::Services;

/// DataNode service.
pub struct DataNode {
    services: Services,
    _catalog_list: CatalogListRef,
    instance: InstanceRef,
}

impl DataNode {
    fn setup_test(
        schema_provider: SchemaProviderRef,
        catalog_provider: Arc<MemoryCatalogProvider>,
        catalog_list: CatalogListRef,
    ) -> Result<()> {
        // Add numbers table for test
        let table = Arc::new(NumbersTable::default());
        schema_provider
            .register_table("numbers".to_string(), table)
            .context(QuerySnafu)?;
        catalog_provider.register_schema("public", schema_provider);
        catalog_list.register_catalog("greptime".to_string(), catalog_provider);

        Ok(())
    }

    pub fn new() -> Result<DataNode> {
        let schema_provider = Arc::new(MemorySchemaProvider::new());
        let catalog_provider = Arc::new(MemoryCatalogProvider::new());
        let catalog_list = Arc::new(MemoryCatalogList::default());

        Self::setup_test(schema_provider, catalog_provider, catalog_list.clone())?;

        let instance = Arc::new(Instance::new(catalog_list.clone()));

        Ok(Self {
            instance,
            _catalog_list: catalog_list,
            services: Services::new(),
        })
    }

    pub async fn start(&self) -> Result<()> {
        self.services.start().await
    }

    /// Shutdown the datanode service gracefully.
    pub async fn shutdown(&self) -> Result<()> {
        self.services.shutdown().await?;

        unimplemented!()
    }
}
