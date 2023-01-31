// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![feature(assert_matches)]

use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use common_telemetry::info;
use snafu::{OptionExt, ResultExt};
use table::engine::{EngineContext, TableEngineRef};
use table::metadata::TableId;
use table::requests::CreateTableRequest;
use table::TableRef;

use crate::error::{CreateTableSnafu, Result};
pub use crate::schema::{SchemaProvider, SchemaProviderRef};

pub mod error;
pub mod helper;
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

    /// Registers a table within given catalog/schema to catalog manager,
    /// returns whether the table registered.
    async fn register_table(&self, request: RegisterTableRequest) -> Result<bool>;

    /// Deregisters a table within given catalog/schema to catalog manager,
    /// returns whether the table deregistered.
    async fn deregister_table(&self, request: DeregisterTableRequest) -> Result<bool>;

    /// Register a schema with catalog name and schema name. Retuens whether the
    /// schema registered.
    async fn register_schema(&self, request: RegisterSchemaRequest) -> Result<bool>;

    /// Rename a table to [RenameTableRequest::new_table_name], returns whether the table is renamed.
    async fn rename_table(&self, request: RenameTableRequest) -> Result<bool>;

    /// Register a system table, should be called before starting the manager.
    async fn register_system_table(&self, request: RegisterSystemTableRequest)
        -> error::Result<()>;

    fn schema(&self, catalog: &str, schema: &str) -> Result<Option<SchemaProviderRef>>;

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

impl Debug for RegisterTableRequest {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegisterTableRequest")
            .field("catalog", &self.catalog)
            .field("schema", &self.schema)
            .field("table_name", &self.table_name)
            .field("table_id", &self.table_id)
            .field("table", &self.table.table_info())
            .finish()
    }
}

#[derive(Debug, Clone)]
pub struct RenameTableRequest {
    pub catalog: String,
    pub schema: String,
    pub table_name: String,
    pub new_table_name: String,
    pub table_id: TableId,
}

#[derive(Debug, Clone)]
pub struct DeregisterTableRequest {
    pub catalog: String,
    pub schema: String,
    pub table_name: String,
}

#[derive(Debug, Clone)]
pub struct RegisterSchemaRequest {
    pub catalog: String,
    pub schema: String,
}

/// Formats table fully-qualified name
pub fn format_full_table_name(catalog: &str, schema: &str, table: &str) -> String {
    format!("{catalog}.{schema}.{table}")
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
                        "{catalog_name}.{schema_name}.{table_name}, id: {table_id}",
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
            info!("Created and registered system table: {table_name}");
            table
        };
        if let Some(hook) = req.open_hook {
            (hook)(table)?;
        }
    }
    Ok(())
}

/// The number of regions in the datanode node.
pub fn region_number(catalog_manager: &CatalogManagerRef) -> Result<u64> {
    let mut region_number: u64 = 0;

    for catalog_name in catalog_manager.catalog_names()? {
        let catalog =
            catalog_manager
                .catalog(&catalog_name)?
                .context(error::CatalogNotFoundSnafu {
                    catalog_name: &catalog_name,
                })?;

        for schema_name in catalog.schema_names()? {
            let schema = catalog
                .schema(&schema_name)?
                .context(error::SchemaNotFoundSnafu {
                    catalog: &catalog_name,
                    schema: &schema_name,
                })?;

            for table_name in schema.table_names()? {
                let table = schema
                    .table(&table_name)?
                    .context(error::TableNotFoundSnafu {
                        table_info: &table_name,
                    })?;

                let region_numbers = &table.table_info().meta.region_numbers;
                region_number += region_numbers.len() as u64;
            }
        }
    }
    Ok(region_number)
}
