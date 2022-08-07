use std::any::Any;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;

use table::table::numbers::NumbersTable;
use table::TableRef;

use crate::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use crate::error::{Result, TableExistsSnafu};
use crate::schema::SchemaProvider;
use crate::{CatalogList, CatalogProvider, CatalogProviderRef};

/// Simple in-memory list of catalogs
#[derive(Default)]
pub struct MemoryCatalogList {
    /// Collection of catalogs containing schemas and ultimately Tables
    pub catalogs: RwLock<HashMap<String, CatalogProviderRef>>,
}

impl MemoryCatalogList {
    /// Registers a catalog and return `None` if no catalog with the same name was already
    /// registered, or `Some` with the previously registered catalog.
    pub fn register_catalog_if_absent(
        &self,
        name: String,
        catalog: Arc<dyn CatalogProvider>,
    ) -> Option<CatalogProviderRef> {
        let mut catalogs = self.catalogs.write().unwrap();
        match catalogs.entry(name) {
            Entry::Occupied(v) => Some(v.get().clone()),
            Entry::Vacant(v) => {
                v.insert(catalog);
                None
            }
        }
    }
}

impl CatalogList for MemoryCatalogList {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn register_catalog(
        &self,
        name: String,
        catalog: CatalogProviderRef,
    ) -> Option<CatalogProviderRef> {
        let mut catalogs = self.catalogs.write().unwrap();
        catalogs.insert(name, catalog)
    }

    fn catalog_names(&self) -> Vec<String> {
        let catalogs = self.catalogs.read().unwrap();
        catalogs.keys().map(|s| s.to_string()).collect()
    }

    fn catalog(&self, name: &str) -> Option<CatalogProviderRef> {
        let catalogs = self.catalogs.read().unwrap();
        catalogs.get(name).cloned()
    }
}

impl Default for MemoryCatalogProvider {
    fn default() -> Self {
        Self::new()
    }
}

/// Simple in-memory implementation of a catalog.
pub struct MemoryCatalogProvider {
    schemas: RwLock<HashMap<String, Arc<dyn SchemaProvider>>>,
}

impl MemoryCatalogProvider {
    /// Instantiates a new MemoryCatalogProvider with an empty collection of schemas.
    pub fn new() -> Self {
        Self {
            schemas: RwLock::new(HashMap::new()),
        }
    }

    pub fn register_schema(
        &self,
        name: impl Into<String>,
        schema: Arc<dyn SchemaProvider>,
    ) -> Option<Arc<dyn SchemaProvider>> {
        let mut schemas = self.schemas.write().unwrap();
        schemas.insert(name.into(), schema)
    }
}

impl CatalogProvider for MemoryCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        let schemas = self.schemas.read().unwrap();
        schemas.keys().cloned().collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        let schemas = self.schemas.read().unwrap();
        schemas.get(name).cloned()
    }
}

/// Simple in-memory implementation of a schema.
pub struct MemorySchemaProvider {
    tables: RwLock<HashMap<String, TableRef>>,
}

impl MemorySchemaProvider {
    /// Instantiates a new MemorySchemaProvider with an empty collection of tables.
    pub fn new() -> Self {
        Self {
            tables: RwLock::new(HashMap::new()),
        }
    }
}

impl Default for MemorySchemaProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl SchemaProvider for MemorySchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        let tables = self.tables.read().unwrap();
        tables.keys().cloned().collect()
    }

    fn table(&self, name: &str) -> Option<TableRef> {
        let tables = self.tables.read().unwrap();
        tables.get(name).cloned()
    }

    fn register_table(&self, name: String, table: TableRef) -> Result<Option<TableRef>> {
        if self.table_exist(name.as_str()) {
            return TableExistsSnafu { table: name }.fail()?;
        }
        let mut tables = self.tables.write().unwrap();
        Ok(tables.insert(name, table))
    }

    fn deregister_table(&self, name: &str) -> Result<Option<TableRef>> {
        let mut tables = self.tables.write().unwrap();
        Ok(tables.remove(name))
    }

    fn table_exist(&self, name: &str) -> bool {
        let tables = self.tables.read().unwrap();
        tables.contains_key(name)
    }
}

/// Create a memory catalog list contains a numbers table for test
pub fn new_memory_catalog_list() -> Result<Arc<MemoryCatalogList>> {
    let schema_provider = Arc::new(MemorySchemaProvider::new());
    let catalog_provider = Arc::new(MemoryCatalogProvider::new());
    let catalog_list = Arc::new(MemoryCatalogList::default());

    // Add numbers table for test
    // TODO(hl): remove this registration
    let table = Arc::new(NumbersTable::default());
    schema_provider.register_table("numbers".to_string(), table)?;
    catalog_provider.register_schema(DEFAULT_SCHEMA_NAME, schema_provider);
    catalog_list.register_catalog(DEFAULT_CATALOG_NAME.to_string(), catalog_provider);

    Ok(catalog_list)
}

#[cfg(test)]
mod tests {
    use common_error::ext::ErrorExt;
    use common_error::prelude::StatusCode;

    use super::*;

    #[test]
    fn test_new_memory_catalog_list() {
        let catalog_list = new_memory_catalog_list().unwrap();

        let catalog_provider = catalog_list.catalog(DEFAULT_CATALOG_NAME).unwrap();
        let schema_provider = catalog_provider.schema(DEFAULT_SCHEMA_NAME).unwrap();

        let table = schema_provider.table("numbers");
        assert!(table.is_some());

        assert!(schema_provider.table("not_exists").is_none());
    }

    #[tokio::test]
    async fn test_mem_provider() {
        let provider = MemorySchemaProvider::new();
        let table_name = "numbers";
        assert!(!provider.table_exist(table_name));
        assert!(provider.deregister_table(table_name).unwrap().is_none());
        let test_table = NumbersTable::default();
        // register table successfully
        assert!(provider
            .register_table(table_name.to_string(), Arc::new(test_table))
            .unwrap()
            .is_none());
        assert!(provider.table_exist(table_name));
        let other_table = NumbersTable::default();
        let result = provider.register_table(table_name.to_string(), Arc::new(other_table));
        let err = result.err().unwrap();
        assert!(err.backtrace_opt().is_some());
        assert_eq!(StatusCode::TableAlreadyExists, err.status_code());
    }
}
