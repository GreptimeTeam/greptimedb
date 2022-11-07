#![feature(assert_matches)]

use std::any::Any;
use std::sync::Arc;

use common_telemetry::info;
use snafu::ResultExt;
use table::engine::{EngineContext, TableEngineRef};
use table::metadata::TableId;
use table::requests::CreateTableRequest;
use table::TableRef;

use crate::error::{CreateTableSnafu, Result};
pub use crate::schema::{SchemaProvider, SchemaProviderRef};

pub mod error;
pub mod local;
pub mod remote;
pub mod schema;
pub mod system;
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
    ) -> Result<Option<CatalogProviderRef>>;

    /// Retrieves the list of available catalog names
    fn catalog_names(&self) -> Result<Vec<String>>;

    /// Retrieves a specific catalog by name, provided it exists.
    fn catalog(&self, name: &str) -> Result<Option<CatalogProviderRef>>;
}

/// Represents a catalog, comprising a number of named schemas.
pub trait CatalogProvider: Sync + Send {
    /// Returns the catalog provider as [`Any`](std::any::Any)
    /// so that it can be downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;

    /// Retrieves the list of available schema names in this catalog.
    fn schema_names(&self) -> Result<Vec<String>>;

    /// Registers schema to this catalog.
    fn register_schema(
        &self,
        name: String,
        schema: SchemaProviderRef,
    ) -> Result<Option<SchemaProviderRef>>;

    /// Retrieves a specific schema from the catalog by name, provided it exists.
    fn schema(&self, name: &str) -> Result<Option<SchemaProviderRef>>;
}

pub type CatalogListRef = Arc<dyn CatalogList>;
pub type CatalogProviderRef = Arc<dyn CatalogProvider>;

#[async_trait::async_trait]
pub trait CatalogManager: CatalogList {
    /// Starts a catalog manager.
    async fn start(&self) -> Result<()>;

    /// Returns next available table id.
    async fn next_table_id(&self) -> Result<TableId>;

    /// Registers a table given given catalog/schema to catalog manager,
    /// returns table registered.
    async fn register_table(&self, request: RegisterTableRequest) -> Result<usize>;

    /// Register a system table, should be called before starting the manager.
    async fn register_system_table(&self, request: RegisterSystemTableRequest)
        -> error::Result<()>;

    /// Returns the table by catalog, schema and table name.
    fn table(&self, catalog: &str, schema: &str, table_name: &str) -> Result<Option<TableRef>>;
}

pub type CatalogManagerRef = Arc<dyn CatalogManager>;

/// Hook called after system table opening.
pub type OpenSystemTableHook = Arc<dyn Fn(TableRef) -> Result<()> + Send + Sync>;

/// Register system table request:
/// - When system table is already created and registered, the hook will be called
///     with table ref after opening the system table
/// - When system table is not exists, create and register the table by create_table_request and calls open_hook with the created table.
pub struct RegisterSystemTableRequest {
    pub create_table_request: CreateTableRequest,
    pub open_hook: Option<OpenSystemTableHook>,
}

#[derive(Clone)]
pub struct RegisterTableRequest {
    pub catalog: String,
    pub schema: String,
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

pub(crate) async fn handle_system_table_request<'a, M: CatalogManager>(
    manager: &'a M,
    engine: TableEngineRef,
    sys_table_requests: &'a mut Vec<RegisterSystemTableRequest>,
) -> Result<()> {
    for req in sys_table_requests.drain(..) {
        let catalog_name = &req.create_table_request.catalog_name;
        let schema_name = &req.create_table_request.schema_name;
        let table_name = &req.create_table_request.table_name;
        let table_id = req.create_table_request.id;

        let table = if let Some(table) = manager.table(catalog_name, schema_name, table_name)? {
            table
        } else {
            let table = engine
                .create_table(&EngineContext::default(), req.create_table_request.clone())
                .await
                .with_context(|_| CreateTableSnafu {
                    table_info: format!(
                        "{}.{}.{}, id: {}",
                        catalog_name, schema_name, table_name, table_id,
                    ),
                })?;
            manager
                .register_table(RegisterTableRequest {
                    catalog: catalog_name.clone(),
                    schema: schema_name.clone(),
                    table_name: table_name.clone(),
                    table_id,
                    table: table.clone(),
                })
                .await?;
            info!("Created and registered system table: {}", table_name);
            table
        };
        if let Some(hook) = req.open_hook {
            (hook)(table)?;
        }
    }
    Ok(())
}
