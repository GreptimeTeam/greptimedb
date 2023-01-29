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

use std::any::Any;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, RwLock};

use common_catalog::consts::MIN_USER_TABLE_ID;
use common_telemetry::error;
use snafu::{ensure, OptionExt};
use table::metadata::TableId;
use table::table::TableIdProvider;
use table::TableRef;

use crate::error::{
    self, CatalogNotFoundSnafu, Result, SchemaNotFoundSnafu, TableExistsSnafu, TableNotFoundSnafu,
};
use crate::schema::SchemaProvider;
use crate::{
    CatalogList, CatalogManager, CatalogProvider, CatalogProviderRef, DeregisterTableRequest,
    RegisterSchemaRequest, RegisterSystemTableRequest, RegisterTableRequest, RenameTableRequest,
    SchemaProviderRef,
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
impl TableIdProvider for MemoryCatalogManager {
    async fn next_table_id(&self) -> table::error::Result<TableId> {
        Ok(self.table_id.fetch_add(1, Ordering::Relaxed))
    }
}

#[async_trait::async_trait]
impl CatalogManager for MemoryCatalogManager {
    async fn start(&self) -> Result<()> {
        self.table_id.store(MIN_USER_TABLE_ID, Ordering::Relaxed);
        Ok(())
    }

    async fn register_table(&self, request: RegisterTableRequest) -> Result<bool> {
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
                catalog: &request.catalog,
                schema: &request.schema,
            })?;
        schema
            .register_table(request.table_name, request.table)
            .map(|v| v.is_none())
    }

    async fn rename_table(&self, request: RenameTableRequest) -> Result<bool> {
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
                catalog: &request.catalog,
                schema: &request.schema,
            })?;
        Ok(schema
            .rename_table(&request.table_name, request.new_table_name)
            .is_ok())
    }

    async fn deregister_table(&self, request: DeregisterTableRequest) -> Result<bool> {
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
                catalog: &request.catalog,
                schema: &request.schema,
            })?;
        schema
            .deregister_table(&request.table_name)
            .map(|v| v.is_some())
    }

    async fn register_schema(&self, request: RegisterSchemaRequest) -> Result<bool> {
        let catalogs = self.catalogs.write().unwrap();
        let catalog = catalogs
            .get(&request.catalog)
            .context(CatalogNotFoundSnafu {
                catalog_name: &request.catalog,
            })?;
        catalog.register_schema(request.schema, Arc::new(MemorySchemaProvider::new()))?;
        Ok(true)
    }

    async fn register_system_table(&self, _request: RegisterSystemTableRequest) -> Result<()> {
        // TODO(ruihang): support register system table request
        Ok(())
    }

    fn schema(&self, catalog: &str, schema: &str) -> Result<Option<SchemaProviderRef>> {
        let catalogs = self.catalogs.read().unwrap();
        if let Some(c) = catalogs.get(catalog) {
            c.schema(schema)
        } else {
            Ok(None)
        }
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
        ensure!(
            !schemas.contains_key(&name),
            error::SchemaExistsSnafu { schema: &name }
        );
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
        let mut tables = self.tables.write().unwrap();
        if let Some(existing) = tables.get(name.as_str()) {
            // if table with the same name but different table id exists, then it's a fatal bug
            if existing.table_info().ident.table_id != table.table_info().ident.table_id {
                error!(
                    "Unexpected table register: {:?}, existing: {:?}",
                    table.table_info(),
                    existing.table_info()
                );
                return TableExistsSnafu { table: name }.fail()?;
            }
            Ok(Some(existing.clone()))
        } else {
            Ok(tables.insert(name, table))
        }
    }

    fn rename_table(&self, name: &str, new_name: String) -> Result<TableRef> {
        let mut tables = self.tables.write().unwrap();
        if tables.get(name).is_some() {
            let table = tables.remove(name).unwrap();
            tables.insert(new_name, table.clone());
            Ok(table)
        } else {
            TableNotFoundSnafu {
                table_info: name.to_string(),
            }
            .fail()?
        }
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
        let other_table = NumbersTable::new(12);
        let result = provider.register_table(table_name.to_string(), Arc::new(other_table));
        let err = result.err().unwrap();
        assert!(err.backtrace_opt().is_some());
        assert_eq!(StatusCode::TableAlreadyExists, err.status_code());
    }

    #[tokio::test]
    async fn test_mem_provider_rename_table() {
        let provider = MemorySchemaProvider::new();
        let table_name = "num";
        assert!(!provider.table_exist(table_name).unwrap());
        let test_table: TableRef = Arc::new(NumbersTable::default());
        // register test table
        assert!(provider
            .register_table(table_name.to_string(), test_table.clone())
            .unwrap()
            .is_none());
        assert!(provider.table_exist(table_name).unwrap());

        // rename test table
        let new_table_name = "numbers";
        provider
            .rename_table(table_name, new_table_name.to_string())
            .unwrap();

        // test old table name not exist
        assert!(!provider.table_exist(table_name).unwrap());
        assert!(provider.deregister_table(table_name).unwrap().is_none());

        // test new table name exists
        assert!(provider.table_exist(new_table_name).unwrap());
        let registered_table = provider.table(new_table_name).unwrap().unwrap();
        assert_eq!(
            registered_table.table_info().ident.table_id,
            test_table.table_info().ident.table_id
        );

        let other_table = Arc::new(NumbersTable::new(2));
        let result = provider.register_table(new_table_name.to_string(), other_table);
        let err = result.err().unwrap();
        assert_eq!(StatusCode::TableAlreadyExists, err.status_code());
    }

    #[tokio::test]
    async fn test_catalog_rename_table() {
        let catalog = MemoryCatalogManager::default();
        let schema = catalog
            .schema(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME)
            .unwrap()
            .unwrap();

        // register table
        let table_name = "num";
        let table_id = 2333;
        let table: TableRef = Arc::new(NumbersTable::new(table_id));
        let register_table_req = RegisterTableRequest {
            catalog: DEFAULT_CATALOG_NAME.to_string(),
            schema: DEFAULT_SCHEMA_NAME.to_string(),
            table_name: table_name.to_string(),
            table_id,
            table,
        };
        assert!(catalog.register_table(register_table_req).await.unwrap());
        assert!(schema.table_exist(table_name).unwrap());

        // rename table
        let new_table_name = "numbers";
        let rename_table_req = RenameTableRequest {
            catalog: DEFAULT_CATALOG_NAME.to_string(),
            schema: DEFAULT_SCHEMA_NAME.to_string(),
            table_name: table_name.to_string(),
            new_table_name: new_table_name.to_string(),
            table_id,
        };
        assert!(catalog.rename_table(rename_table_req).await.unwrap());
        assert!(!schema.table_exist(table_name).unwrap());
        assert!(schema.table_exist(new_table_name).unwrap());

        let registered_table = catalog
            .table(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, new_table_name)
            .unwrap()
            .unwrap();
        assert_eq!(registered_table.table_info().ident.table_id, table_id);
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

    #[tokio::test]
    pub async fn test_catalog_deregister_table() {
        let catalog = MemoryCatalogManager::default();
        let schema = catalog
            .schema(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME)
            .unwrap()
            .unwrap();

        let register_table_req = RegisterTableRequest {
            catalog: DEFAULT_CATALOG_NAME.to_string(),
            schema: DEFAULT_SCHEMA_NAME.to_string(),
            table_name: "numbers".to_string(),
            table_id: 2333,
            table: Arc::new(NumbersTable::default()),
        };
        catalog.register_table(register_table_req).await.unwrap();
        assert!(schema.table_exist("numbers").unwrap());

        let deregister_table_req = DeregisterTableRequest {
            catalog: DEFAULT_CATALOG_NAME.to_string(),
            schema: DEFAULT_SCHEMA_NAME.to_string(),
            table_name: "numbers".to_string(),
        };
        catalog
            .deregister_table(deregister_table_req)
            .await
            .unwrap();
        assert!(!schema.table_exist("numbers").unwrap());
    }
}
