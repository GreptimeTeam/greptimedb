use std::any::Any;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;

use table::TableRef;

use crate::error::{Result, TableExistsSnafu};
use crate::schema::SchemaProvider;
use crate::{CatalogList, CatalogProvider, CatalogProviderRef, SchemaProviderRef};

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
        let entry = catalogs.entry(name);
        match entry {
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
}

impl CatalogProvider for MemoryCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        let schemas = self.schemas.read().unwrap();
        schemas.keys().cloned().collect()
    }

    fn register_schema(
        &self,
        name: String,
        schema: SchemaProviderRef,
    ) -> Option<SchemaProviderRef> {
        let mut schemas = self.schemas.write().unwrap();
        schemas.insert(name, schema)
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
    Ok(Arc::new(MemoryCatalogList::default()))
}

#[cfg(test)]
mod tests {
    use common_error::ext::ErrorExt;
    use common_error::prelude::StatusCode;
    use table::table::numbers::NumbersTable;

    use super::*;
    use crate::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};

    #[test]
    fn test_new_memory_catalog_list() {
        let catalog_list = new_memory_catalog_list().unwrap();

        assert!(catalog_list.catalog(DEFAULT_CATALOG_NAME).is_none());
        let default_catalog = Arc::new(MemoryCatalogProvider::default());
        catalog_list.register_catalog(DEFAULT_CATALOG_NAME.to_string(), default_catalog.clone());

        assert!(default_catalog.schema(DEFAULT_SCHEMA_NAME).is_none());
        let default_schema = Arc::new(MemorySchemaProvider::default());
        default_catalog.register_schema(DEFAULT_SCHEMA_NAME.to_string(), default_schema.clone());

        default_schema
            .register_table("numbers".to_string(), Arc::new(NumbersTable::default()))
            .unwrap();

        let table = default_schema.table("numbers");
        assert!(table.is_some());

        assert!(default_schema.table("not_exists").is_none());
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

    #[test]
    pub fn test_register_if_absent() {
        let list = MemoryCatalogList::default();
        assert!(list
            .register_catalog_if_absent(
                "test_catalog".to_string(),
                Arc::new(MemoryCatalogProvider::new())
            )
            .is_none());
        list.register_catalog_if_absent(
            "test_catalog".to_string(),
            Arc::new(MemoryCatalogProvider::new()),
        )
        .unwrap();
        list.as_any().downcast_ref::<MemoryCatalogList>().unwrap();
    }
}
