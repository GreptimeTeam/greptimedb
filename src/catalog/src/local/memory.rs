use std::any::Any;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::sync::RwLock;

use common_catalog::consts::MIN_USER_TABLE_ID;
use snafu::OptionExt;
use table::metadata::TableId;
use table::TableRef;

use crate::error::{CatalogNotFoundSnafu, Result, SchemaNotFoundSnafu, TableExistsSnafu};
use crate::schema::SchemaProvider;
use crate::{
    CatalogList, CatalogManager, CatalogProvider, CatalogProviderRef, RegisterSystemTableRequest,
    RegisterTableRequest, SchemaProviderRef,
};

/// Simple in-memory list of catalogs
pub struct MemoryCatalogManager {
    /// Collection of catalogs containing schemas and ultimately Tables
    pub catalogs: RwLock<HashMap<String, CatalogProviderRef>>,
    pub table_id: AtomicU32,
}

impl Default for MemoryCatalogManager {
    fn default() -> Self {
        let manager = Self {
            table_id: AtomicU32::new(MIN_USER_TABLE_ID),
            catalogs: Default::default(),
        };
        let default_catalog = Arc::new(MemoryCatalogProvider::new());
        manager
            .register_catalog("greptime".to_string(), default_catalog.clone())
            .unwrap();
        default_catalog
            .register_schema("public".to_string(), Arc::new(MemorySchemaProvider::new()))
            .unwrap();
        manager
    }
}

#[async_trait::async_trait]
impl CatalogManager for MemoryCatalogManager {
    async fn start(&self) -> Result<()> {
        self.table_id.store(MIN_USER_TABLE_ID, Ordering::Relaxed);
        Ok(())
    }

    async fn next_table_id(&self) -> Result<TableId> {
        Ok(self.table_id.fetch_add(1, Ordering::Relaxed))
    }

    async fn register_table(&self, request: RegisterTableRequest) -> Result<usize> {
        let catalogs = self.catalogs.write().unwrap();
        let catalog = catalogs
            .get(&request.catalog)
            .context(CatalogNotFoundSnafu {
                catalog_name: &request.catalog,
            })?
            .clone();
        let schema = catalog
            .schema(&request.schema)?
            .with_context(|| SchemaNotFoundSnafu {
                schema_info: format!("{}.{}", &request.catalog, &request.schema),
            })?;
        schema
            .register_table(request.table_name, request.table)
            .map(|v| if v.is_some() { 0 } else { 1 })
    }

    async fn register_system_table(&self, _request: RegisterSystemTableRequest) -> Result<()> {
        unimplemented!()
    }

    fn table(&self, catalog: &str, schema: &str, table_name: &str) -> Result<Option<TableRef>> {
        let c = self.catalogs.read().unwrap();
        let catalog = if let Some(c) = c.get(catalog) {
            c.clone()
        } else {
            return Ok(None);
        };
        match catalog.schema(schema)? {
            None => Ok(None),
            Some(s) => s.table(table_name),
        }
    }
}

impl MemoryCatalogManager {
    /// Registers a catalog and return `None` if no catalog with the same name was already
    /// registered, or `Some` with the previously registered catalog.
    pub fn register_catalog_if_absent(
        &self,
        name: String,
        catalog: CatalogProviderRef,
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

impl CatalogList for MemoryCatalogManager {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn register_catalog(
        &self,
        name: String,
        catalog: CatalogProviderRef,
    ) -> Result<Option<CatalogProviderRef>> {
        let mut catalogs = self.catalogs.write().unwrap();
        Ok(catalogs.insert(name, catalog))
    }

    fn catalog_names(&self) -> Result<Vec<String>> {
        let catalogs = self.catalogs.read().unwrap();
        Ok(catalogs.keys().map(|s| s.to_string()).collect())
    }

    fn catalog(&self, name: &str) -> Result<Option<CatalogProviderRef>> {
        let catalogs = self.catalogs.read().unwrap();
        Ok(catalogs.get(name).cloned())
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

    fn schema_names(&self) -> Result<Vec<String>> {
        let schemas = self.schemas.read().unwrap();
        Ok(schemas.keys().cloned().collect())
    }

    fn register_schema(
        &self,
        name: String,
        schema: SchemaProviderRef,
    ) -> Result<Option<SchemaProviderRef>> {
        let mut schemas = self.schemas.write().unwrap();
        Ok(schemas.insert(name, schema))
    }

    fn schema(&self, name: &str) -> Result<Option<Arc<dyn SchemaProvider>>> {
        let schemas = self.schemas.read().unwrap();
        Ok(schemas.get(name).cloned())
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

    fn table_names(&self) -> Result<Vec<String>> {
        let tables = self.tables.read().unwrap();
        Ok(tables.keys().cloned().collect())
    }

    fn table(&self, name: &str) -> Result<Option<TableRef>> {
        let tables = self.tables.read().unwrap();
        Ok(tables.get(name).cloned())
    }

    fn register_table(&self, name: String, table: TableRef) -> Result<Option<TableRef>> {
        if self.table_exist(name.as_str())? {
            return TableExistsSnafu { table: name }.fail()?;
        }
        let mut tables = self.tables.write().unwrap();
        Ok(tables.insert(name, table))
    }

    fn deregister_table(&self, name: &str) -> Result<Option<TableRef>> {
        let mut tables = self.tables.write().unwrap();
        Ok(tables.remove(name))
    }

    fn table_exist(&self, name: &str) -> Result<bool> {
        let tables = self.tables.read().unwrap();
        Ok(tables.contains_key(name))
    }
}

/// Create a memory catalog list contains a numbers table for test
pub fn new_memory_catalog_list() -> Result<Arc<MemoryCatalogManager>> {
    Ok(Arc::new(MemoryCatalogManager::default()))
}

#[cfg(test)]
mod tests {
    use common_catalog::consts::*;
    use common_error::ext::ErrorExt;
    use common_error::prelude::StatusCode;
    use table::table::numbers::NumbersTable;

    use super::*;

    #[test]
    fn test_new_memory_catalog_list() {
        let catalog_list = new_memory_catalog_list().unwrap();
        let default_catalog = catalog_list.catalog(DEFAULT_CATALOG_NAME).unwrap().unwrap();

        let default_schema = default_catalog
            .schema(DEFAULT_SCHEMA_NAME)
            .unwrap()
            .unwrap();

        default_schema
            .register_table("numbers".to_string(), Arc::new(NumbersTable::default()))
            .unwrap();

        let table = default_schema.table("numbers").unwrap();
        assert!(table.is_some());
        assert!(default_schema.table("not_exists").unwrap().is_none());
    }

    #[tokio::test]
    async fn test_mem_provider() {
        let provider = MemorySchemaProvider::new();
        let table_name = "numbers";
        assert!(!provider.table_exist(table_name).unwrap());
        assert!(provider.deregister_table(table_name).unwrap().is_none());
        let test_table = NumbersTable::default();
        // register table successfully
        assert!(provider
            .register_table(table_name.to_string(), Arc::new(test_table))
            .unwrap()
            .is_none());
        assert!(provider.table_exist(table_name).unwrap());
        let other_table = NumbersTable::default();
        let result = provider.register_table(table_name.to_string(), Arc::new(other_table));
        let err = result.err().unwrap();
        assert!(err.backtrace_opt().is_some());
        assert_eq!(StatusCode::TableAlreadyExists, err.status_code());
    }

    #[test]
    pub fn test_register_if_absent() {
        let list = MemoryCatalogManager::default();
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
        list.as_any()
            .downcast_ref::<MemoryCatalogManager>()
            .unwrap();
    }
}
