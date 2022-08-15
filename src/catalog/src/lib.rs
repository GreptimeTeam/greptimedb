#![feature(assert_matches)]

use std::any::Any;
use std::sync::Arc;

use table::metadata::TableId;

pub use crate::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
pub use crate::manager::LocalCatalogManager;
pub use crate::schema::{SchemaProvider, SchemaProviderRef};

mod consts;
pub mod error;
mod manager;
pub mod memory;
pub mod schema;
mod system;
mod tables;

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
        catalog: CatalogProviderRef,
    ) -> Option<CatalogProviderRef>;

    /// Retrieves the list of available catalog names
    fn catalog_names(&self) -> Vec<String>;

    /// Retrieves a specific catalog by name, provided it exists.
    fn catalog(&self, name: &str) -> Option<CatalogProviderRef>;
}

/// Represents a catalog, comprising a number of named schemas.
pub trait CatalogProvider: Sync + Send {
    /// Returns the catalog provider as [`Any`](std::any::Any)
    /// so that it can be downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;

    /// Retrieves the list of available schema names in this catalog.
    fn schema_names(&self) -> Vec<String>;

    /// Registers schema to this catalog.
    fn register_schema(&self, name: String, schema: SchemaProviderRef)
        -> Option<SchemaProviderRef>;

    /// Retrieves a specific schema from the catalog by name, provided it exists.
    fn schema(&self, name: &str) -> Option<SchemaProviderRef>;
}

pub type CatalogListRef = Arc<dyn CatalogList>;
pub type CatalogProviderRef = Arc<dyn CatalogProvider>;

#[async_trait::async_trait]
pub trait CatalogManager: CatalogList {
    /// Starts a catalog manager.
    async fn start(&self) -> error::Result<()>;

    /// Returns next available table id.
    fn next_table_id(&self) -> TableId;

    /// Registers a table given given catalog/schema to catalog manager.
    async fn register_table(
        &self,
        catalog: Option<String>,
        schema: Option<String>,
        table_name: String,
        table_id: TableId,
    ) -> error::Result<usize>;
}

pub type CatalogManagerRef = Arc<dyn CatalogManager>;
