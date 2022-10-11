#![feature(assert_matches)]

use std::any::Any;
use std::sync::Arc;

use table::metadata::TableId;
use table::requests::CreateTableRequest;
use table::TableRef;

pub use crate::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, MIN_USER_TABLE_ID};
use crate::error::Error;
pub use crate::schema::{SchemaProvider, SchemaProviderRef};

pub mod consts;
pub mod error;
pub mod local;
pub mod remote;
pub mod schema;
mod system;
pub mod tables;

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
    ) -> Result<Option<CatalogProviderRef>, Error>;

    /// Retrieves the list of available catalog names
    fn catalog_names(&self) -> Result<Vec<String>, Error>;

    /// Retrieves a specific catalog by name, provided it exists.
    fn catalog(&self, name: &str) -> Result<Option<CatalogProviderRef>, Error>;
}

/// Represents a catalog, comprising a number of named schemas.
pub trait CatalogProvider: Sync + Send {
    /// Returns the catalog provider as [`Any`](std::any::Any)
    /// so that it can be downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;

    /// Retrieves the list of available schema names in this catalog.
    fn schema_names(&self) -> Result<Vec<String>, Error>;

    /// Registers schema to this catalog.
    fn register_schema(
        &self,
        name: String,
        schema: SchemaProviderRef,
    ) -> Result<Option<SchemaProviderRef>, Error>;

    /// Retrieves a specific schema from the catalog by name, provided it exists.
    fn schema(&self, name: &str) -> Result<Option<SchemaProviderRef>, Error>;
}

pub type CatalogListRef = Arc<dyn CatalogList>;
pub type CatalogProviderRef = Arc<dyn CatalogProvider>;

#[async_trait::async_trait]
pub trait CatalogManager: CatalogList {
    /// Starts a catalog manager.
    async fn start(&self) -> error::Result<()>;

    /// Returns next available table id.
    async fn next_table_id(&self) -> TableId;

    /// Registers a table given given catalog/schema to catalog manager,
    /// returns table registered.
    async fn register_table(&self, request: RegisterTableRequest) -> error::Result<usize>;

    /// Register a system table, should be called before starting the manager.
    async fn register_system_table(&self, request: RegisterSystemTableRequest)
        -> error::Result<()>;

    /// Returns the table by catalog, schema and table name.
    fn table(
        &self,
        catalog: Option<&str>,
        schema: Option<&str>,
        table_name: &str,
    ) -> error::Result<Option<TableRef>>;
}

pub type CatalogManagerRef = Arc<dyn CatalogManager>;

/// Hook called after system table opening.
pub type OpenSystemTableHook = Arc<dyn Fn(TableRef) -> error::Result<()> + Send + Sync>;

/// Register system table request:
/// - When system table is already created and registered, the hook will be called
///     with table ref after opening the system table
/// - When system table is not exists, create and register the table by create_table_request and calls open_hook with the created table.
pub struct RegisterSystemTableRequest {
    pub create_table_request: CreateTableRequest,
    pub open_hook: Option<OpenSystemTableHook>,
}

pub struct RegisterTableRequest {
    pub catalog: Option<String>,
    pub schema: Option<String>,
    pub table_name: String,
    pub table_id: TableId,
    pub table: TableRef,
}

/// Formats table fully-qualified name
pub fn format_full_table_name(catalog: &str, schema: &str, table: &str) -> String {
    format!("{}.{}.{}", catalog, schema, table)
}

pub trait CatalogProviderFactory {
    fn create(&self, catalog_name: String) -> CatalogProviderRef;
}

pub trait SchemaProviderFactory {
    fn create(&self, catalog_name: String, schema_name: String) -> SchemaProviderRef;
}
