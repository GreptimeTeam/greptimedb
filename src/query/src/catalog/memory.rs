use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;

use crate::catalog::schema::SchemaProvider;
use crate::catalog::{CatalogList, CatalogProvider};

/// Simple in-memory list of catalogs
pub struct MemoryCatalogList {
    /// Collection of catalogs containing schemas and ultimately TableProviders
    pub catalogs: RwLock<HashMap<String, Arc<dyn CatalogProvider>>>,
}

impl MemoryCatalogList {
    /// Instantiates a new `MemoryCatalogList` with an empty collection of catalogs
    pub fn new() -> Self {
        Self {
            catalogs: RwLock::new(HashMap::new()),
        }
    }
}

impl Default for MemoryCatalogList {
    fn default() -> Self {
        Self::new()
    }
}

impl CatalogList for MemoryCatalogList {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn register_catalog(
        &self,
        name: String,
        catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
        let mut catalogs = self.catalogs.write().unwrap();
        catalogs.insert(name, catalog)
    }

    fn catalog_names(&self) -> Vec<String> {
        let catalogs = self.catalogs.read().unwrap();
        catalogs.keys().map(|s| s.to_string()).collect()
    }

    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
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
