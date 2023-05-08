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

use async_trait::async_trait;
use common_catalog::consts::MIN_USER_TABLE_ID;
use common_telemetry::error;
use metrics::{decrement_gauge, increment_gauge};
use snafu::{ensure, OptionExt};
use table::metadata::TableId;
use table::table::TableIdProvider;
use table::TableRef;

use crate::error::{
    self, CatalogNotFoundSnafu, Result, SchemaNotFoundSnafu, TableExistsSnafu, TableNotFoundSnafu,
};
use crate::schema::SchemaProvider;
use crate::{
    CatalogManager, CatalogProvider, CatalogProviderRef, DeregisterTableRequest,
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
            .register_catalog_sync("greptime".to_string(), default_catalog.clone())
            .unwrap();
        default_catalog
            .register_schema_sync("public".to_string(), Arc::new(MemorySchemaProvider::new()))
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
        let schema = self
            .catalog(&request.catalog)
            .context(CatalogNotFoundSnafu {
                catalog_name: &request.catalog,
            })?
            .schema(&request.schema)
            .await?
            .context(SchemaNotFoundSnafu {
                catalog: &request.catalog,
                schema: &request.schema,
            })?;
        increment_gauge!(
            crate::metrics::METRIC_CATALOG_MANAGER_TABLE_COUNT,
            1.0,
            &[crate::metrics::db_label(&request.catalog, &request.schema)],
        );
        schema
            .register_table(request.table_name, request.table)
            .await
            .map(|v| v.is_none())
    }

    async fn rename_table(&self, request: RenameTableRequest) -> Result<bool> {
        let catalog = self
            .catalog(&request.catalog)
            .context(CatalogNotFoundSnafu {
                catalog_name: &request.catalog,
            })?;
        let schema =
            catalog
                .schema(&request.schema)
                .await?
                .with_context(|| SchemaNotFoundSnafu {
                    catalog: &request.catalog,
                    schema: &request.schema,
                })?;
        Ok(schema
            .rename_table(&request.table_name, request.new_table_name)
            .await
            .is_ok())
    }

    async fn deregister_table(&self, request: DeregisterTableRequest) -> Result<bool> {
        let schema = self
            .catalog(&request.catalog)
            .context(CatalogNotFoundSnafu {
                catalog_name: &request.catalog,
            })?
            .schema(&request.schema)
            .await?
            .with_context(|| SchemaNotFoundSnafu {
                catalog: &request.catalog,
                schema: &request.schema,
            })?;
        decrement_gauge!(
            crate::metrics::METRIC_CATALOG_MANAGER_TABLE_COUNT,
            1.0,
            &[crate::metrics::db_label(&request.catalog, &request.schema)],
        );
        schema
            .deregister_table(&request.table_name)
            .await
            .map(|v| v.is_some())
    }

    async fn register_schema(&self, request: RegisterSchemaRequest) -> Result<bool> {
        let catalog = self
            .catalog(&request.catalog)
            .context(CatalogNotFoundSnafu {
                catalog_name: &request.catalog,
            })?;
        catalog
            .register_schema(request.schema, Arc::new(MemorySchemaProvider::new()))
            .await?;
        increment_gauge!(crate::metrics::METRIC_CATALOG_MANAGER_SCHEMA_COUNT, 1.0);
        Ok(true)
    }

    async fn register_system_table(&self, _request: RegisterSystemTableRequest) -> Result<()> {
        // TODO(ruihang): support register system table request
        Ok(())
    }

    async fn schema(&self, catalog: &str, schema: &str) -> Result<Option<SchemaProviderRef>> {
        if let Some(c) = self.catalog(catalog) {
            c.schema(schema).await
        } else {
            Ok(None)
        }
    }

    async fn table(
        &self,
        catalog: &str,
        schema: &str,
        table_name: &str,
    ) -> Result<Option<TableRef>> {
        let Some(catalog) = self
            .catalog(catalog) else { return Ok(None)};
        let Some(s) = catalog.schema(schema).await? else { return Ok(None) };
        s.table(table_name).await
    }

    async fn catalog(&self, catalog: &str) -> Result<Option<CatalogProviderRef>> {
        Ok(self.catalogs.read().unwrap().get(catalog).cloned())
    }

    async fn catalog_names(&self) -> Result<Vec<String>> {
        Ok(self.catalogs.read().unwrap().keys().cloned().collect())
    }

    async fn register_catalog(
        &self,
        name: String,
        catalog: CatalogProviderRef,
    ) -> Result<Option<CatalogProviderRef>> {
        increment_gauge!(crate::metrics::METRIC_CATALOG_MANAGER_CATALOG_COUNT, 1.0);
        self.register_catalog_sync(name, catalog)
    }

    fn as_any(&self) -> &dyn Any {
        self
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

    pub fn register_catalog_sync(
        &self,
        name: String,
        catalog: CatalogProviderRef,
    ) -> Result<Option<CatalogProviderRef>> {
        let mut catalogs = self.catalogs.write().unwrap();
        Ok(catalogs.insert(name, catalog))
    }

    fn catalog(&self, catalog_name: &str) -> Option<CatalogProviderRef> {
        self.catalogs.read().unwrap().get(catalog_name).cloned()
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

    pub fn schema_names_sync(&self) -> Result<Vec<String>> {
        let schemas = self.schemas.read().unwrap();
        Ok(schemas.keys().cloned().collect())
    }

    pub fn register_schema_sync(
        &self,
        name: String,
        schema: SchemaProviderRef,
    ) -> Result<Option<SchemaProviderRef>> {
        let mut schemas = self.schemas.write().unwrap();
        ensure!(
            !schemas.contains_key(&name),
            error::SchemaExistsSnafu { schema: &name }
        );
        increment_gauge!(crate::metrics::METRIC_CATALOG_MANAGER_SCHEMA_COUNT, 1.0);
        Ok(schemas.insert(name, schema))
    }

    pub fn schema_sync(&self, name: &str) -> Result<Option<Arc<dyn SchemaProvider>>> {
        let schemas = self.schemas.read().unwrap();
        Ok(schemas.get(name).cloned())
    }
}

#[async_trait::async_trait]
impl CatalogProvider for MemoryCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn schema_names(&self) -> Result<Vec<String>> {
        self.schema_names_sync()
    }

    async fn register_schema(
        &self,
        name: String,
        schema: SchemaProviderRef,
    ) -> Result<Option<SchemaProviderRef>> {
        self.register_schema_sync(name, schema)
    }

    async fn schema(&self, name: &str) -> Result<Option<Arc<dyn SchemaProvider>>> {
        self.schema_sync(name)
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

    pub fn register_table_sync(&self, name: String, table: TableRef) -> Result<Option<TableRef>> {
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

    pub fn rename_table_sync(&self, name: &str, new_name: String) -> Result<TableRef> {
        let mut tables = self.tables.write().unwrap();
        let Some(table) = tables.remove(name) else {
            return TableNotFoundSnafu {
                table_info: name.to_string(),
            }
                .fail()?;
        };
        let e = match tables.entry(new_name) {
            Entry::Vacant(e) => e,
            Entry::Occupied(e) => {
                return TableExistsSnafu { table: e.key() }.fail();
            }
        };
        e.insert(table.clone());
        Ok(table)
    }

    pub fn table_exist_sync(&self, name: &str) -> Result<bool> {
        let tables = self.tables.read().unwrap();
        Ok(tables.contains_key(name))
    }

    pub fn deregister_table_sync(&self, name: &str) -> Result<Option<TableRef>> {
        let mut tables = self.tables.write().unwrap();
        Ok(tables.remove(name))
    }
}

impl Default for MemorySchemaProvider {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl SchemaProvider for MemorySchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn table_names(&self) -> Result<Vec<String>> {
        let tables = self.tables.read().unwrap();
        Ok(tables.keys().cloned().collect())
    }

    async fn table(&self, name: &str) -> Result<Option<TableRef>> {
        let tables = self.tables.read().unwrap();
        Ok(tables.get(name).cloned())
    }

    async fn register_table(&self, name: String, table: TableRef) -> Result<Option<TableRef>> {
        self.register_table_sync(name, table)
    }

    async fn rename_table(&self, name: &str, new_name: String) -> Result<TableRef> {
        self.rename_table_sync(name, new_name)
    }

    async fn deregister_table(&self, name: &str) -> Result<Option<TableRef>> {
        self.deregister_table_sync(name)
    }

    async fn table_exist(&self, name: &str) -> Result<bool> {
        self.table_exist_sync(name)
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

    #[tokio::test]
    async fn test_new_memory_catalog_list() {
        let catalog_list = new_memory_catalog_list().unwrap();
        let default_catalog = CatalogManager::catalog(&*catalog_list, DEFAULT_CATALOG_NAME)
            .await
            .unwrap()
            .unwrap();

        let default_schema = default_catalog
            .schema(DEFAULT_SCHEMA_NAME)
            .await
            .unwrap()
            .unwrap();

        default_schema
            .register_table("numbers".to_string(), Arc::new(NumbersTable::default()))
            .await
            .unwrap();

        let table = default_schema.table("numbers").await.unwrap();
        assert!(table.is_some());
        assert!(default_schema.table("not_exists").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_mem_provider() {
        let provider = MemorySchemaProvider::new();
        let table_name = "numbers";
        assert!(!provider.table_exist_sync(table_name).unwrap());
        provider.deregister_table_sync(table_name).unwrap();
        let test_table = NumbersTable::default();
        // register table successfully
        assert!(provider
            .register_table_sync(table_name.to_string(), Arc::new(test_table))
            .unwrap()
            .is_none());
        assert!(provider.table_exist_sync(table_name).unwrap());
        let other_table = NumbersTable::new(12);
        let result = provider.register_table_sync(table_name.to_string(), Arc::new(other_table));
        let err = result.err().unwrap();
        assert_eq!(StatusCode::TableAlreadyExists, err.status_code());
    }

    #[tokio::test]
    async fn test_mem_provider_rename_table() {
        let provider = MemorySchemaProvider::new();
        let table_name = "num";
        assert!(!provider.table_exist_sync(table_name).unwrap());
        let test_table: TableRef = Arc::new(NumbersTable::default());
        // register test table
        assert!(provider
            .register_table_sync(table_name.to_string(), test_table.clone())
            .unwrap()
            .is_none());
        assert!(provider.table_exist_sync(table_name).unwrap());

        // rename test table
        let new_table_name = "numbers";
        provider
            .rename_table_sync(table_name, new_table_name.to_string())
            .unwrap();

        // test old table name not exist
        assert!(!provider.table_exist_sync(table_name).unwrap());
        provider.deregister_table_sync(table_name).unwrap();

        // test new table name exists
        assert!(provider.table_exist_sync(new_table_name).unwrap());
        let registered_table = provider.table(new_table_name).await.unwrap().unwrap();
        assert_eq!(
            registered_table.table_info().ident.table_id,
            test_table.table_info().ident.table_id
        );

        let other_table = Arc::new(NumbersTable::new(2));
        let result = provider
            .register_table(new_table_name.to_string(), other_table)
            .await;
        let err = result.err().unwrap();
        assert_eq!(StatusCode::TableAlreadyExists, err.status_code());
    }

    #[tokio::test]
    async fn test_catalog_rename_table() {
        let catalog = MemoryCatalogManager::default();
        let schema = catalog
            .schema(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME)
            .await
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
        assert!(schema.table_exist(table_name).await.unwrap());

        // rename table
        let new_table_name = "numbers_new";
        let rename_table_req = RenameTableRequest {
            catalog: DEFAULT_CATALOG_NAME.to_string(),
            schema: DEFAULT_SCHEMA_NAME.to_string(),
            table_name: table_name.to_string(),
            new_table_name: new_table_name.to_string(),
            table_id,
        };
        assert!(catalog.rename_table(rename_table_req).await.unwrap());
        assert!(!schema.table_exist(table_name).await.unwrap());
        assert!(schema.table_exist(new_table_name).await.unwrap());

        let registered_table = catalog
            .table(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, new_table_name)
            .await
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
            .await
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
        assert!(schema.table_exist("numbers").await.unwrap());

        let deregister_table_req = DeregisterTableRequest {
            catalog: DEFAULT_CATALOG_NAME.to_string(),
            schema: DEFAULT_SCHEMA_NAME.to_string(),
            table_name: "numbers".to_string(),
        };
        catalog
            .deregister_table(deregister_table_req)
            .await
            .unwrap();
        assert!(!schema.table_exist("numbers").await.unwrap());
    }
}
