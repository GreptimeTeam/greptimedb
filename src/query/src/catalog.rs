pub mod memory;
pub mod schema;
use std::any::Any;
use std::sync::Arc;

use crate::catalog::schema::SchemaProvider;

/// Represent a list of named catalogs
pub trait CatalogList: Sync + Send {
    /// Returns the catalog list as [`Any`](std::any::Any)
    /// so that it can be downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;

    /// Adds a new catalog to this catalog list
    /// If a catalog of the same name existed before, it is replaced in the list and returned.
    fn register_catalog(
        &self,
        name: String,
        catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>>;

    /// Retrieves the list of available catalog names
    fn catalog_names(&self) -> Vec<String>;

    /// Retrieves a specific catalog by name, provided it exists.
    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>>;
}

/// Represents a catalog, comprising a number of named schemas.
pub trait CatalogProvider: Sync + Send {
    /// Returns the catalog provider as [`Any`](std::any::Any)
    /// so that it can be downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;

    /// Retrieves the list of available schema names in this catalog.
    fn schema_names(&self) -> Vec<String>;

    /// Retrieves a specific schema from the catalog by name, provided it exists.
    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>>;
}

pub type CatalogListRef = Arc<dyn CatalogList>;
pub type CatalogProviderRef = Arc<dyn CatalogProvider>;

pub const DEFAULT_CATALOG_NAME: &str = "greptime";
pub const DEFAULT_SCHEMA_NAME: &str = "public";
