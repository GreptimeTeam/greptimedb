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

use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, MIN_USER_TABLE_ID};
use common_telemetry::error;
use metrics::{decrement_gauge, increment_gauge};
use snafu::OptionExt;
use table::metadata::TableId;
use table::table::TableIdProvider;
use table::TableRef;

use crate::error::{
    CatalogNotFoundSnafu, Result, SchemaNotFoundSnafu, TableExistsSnafu, TableNotFoundSnafu,
};
use crate::{
    CatalogManager, CatalogProvider, DeregisterTableRequest, RegisterSchemaRequest,
    RegisterSystemTableRequest, RegisterTableRequest, RenameTableRequest,
};

type SchemaEntries = HashMap<String, HashMap<String, TableRef>>;

/// Simple in-memory list of catalogs
pub struct MemoryCatalogManager {
    /// Collection of catalogs containing schemas and ultimately Tables
    pub catalogs: RwLock<HashMap<String, SchemaEntries>>,
    pub table_id: AtomicU32,
}

impl Default for MemoryCatalogManager {
    fn default() -> Self {
        let manager = Self {
            table_id: AtomicU32::new(MIN_USER_TABLE_ID),
            catalogs: Default::default(),
        };

        let mut catalog = HashMap::new();
        catalog.insert(DEFAULT_SCHEMA_NAME.to_string(), HashMap::new());
        manager
            .catalogs
            .write()
            .unwrap()
            .insert(DEFAULT_CATALOG_NAME.to_string(), catalog);

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
        let catalog = request.catalog.clone();
        let schema = request.schema.clone();
        let result = self.register_table_sync(request);
        increment_gauge!(
            crate::metrics::METRIC_CATALOG_MANAGER_TABLE_COUNT,
            1.0,
            &[crate::metrics::db_label(&catalog, &schema)],
        );
        result
    }

    async fn rename_table(&self, request: RenameTableRequest) -> Result<bool> {
        let mut catalogs = self.catalogs.write().unwrap();
        let schema = catalogs
            .get_mut(&request.catalog)
            .with_context(|| CatalogNotFoundSnafu {
                catalog_name: &request.catalog,
            })?
            .get_mut(&request.schema)
            .with_context(|| SchemaNotFoundSnafu {
                catalog: &request.catalog,
                schema: &request.schema,
            })?;

        // check old and new table names
        if !schema.contains_key(&request.table_name) {
            return TableNotFoundSnafu {
                table_info: request.table_name.to_string(),
            }
            .fail()?;
        }
        if schema.contains_key(&request.new_table_name) {
            return TableExistsSnafu {
                table: &request.new_table_name,
            }
            .fail();
        }

        let table = schema.remove(&request.table_name).unwrap();
        schema.insert(request.new_table_name, table);

        Ok(true)
    }

    async fn deregister_table(&self, request: DeregisterTableRequest) -> Result<bool> {
        let mut catalogs = self.catalogs.write().unwrap();
        let schema = catalogs
            .get_mut(&request.catalog)
            .with_context(|| CatalogNotFoundSnafu {
                catalog_name: &request.catalog,
            })?
            .get_mut(&request.schema)
            .with_context(|| SchemaNotFoundSnafu {
                catalog: &request.catalog,
                schema: &request.schema,
            })?;
        let result = schema.remove(&request.table_name);
        if result.is_some() {
            decrement_gauge!(
                crate::metrics::METRIC_CATALOG_MANAGER_TABLE_COUNT,
                1.0,
                &[crate::metrics::db_label(&request.catalog, &request.schema)],
            );
        }
        Ok(result.is_some())
    }

    async fn register_schema(&self, request: RegisterSchemaRequest) -> Result<bool> {
        self.register_schema_sync(request)?;
        increment_gauge!(crate::metrics::METRIC_CATALOG_MANAGER_SCHEMA_COUNT, 1.0);
        Ok(true)
    }

    async fn register_system_table(&self, _request: RegisterSystemTableRequest) -> Result<()> {
        // TODO(ruihang): support register system table request
        Ok(())
    }

    async fn schema_exist(&self, catalog: &str, schema: &str) -> Result<bool> {
        Ok(self
            .catalogs
            .read()
            .unwrap()
            .get(catalog)
            .with_context(|| CatalogNotFoundSnafu {
                catalog_name: catalog,
            })?
            .contains_key(schema))
    }

    async fn table(
        &self,
        catalog: &str,
        schema: &str,
        table_name: &str,
    ) -> Result<Option<TableRef>> {
        let result = try {
            self.catalogs
                .read()
                .unwrap()
                .get(catalog)?
                .get(schema)?
                .get(table_name)
                .cloned()?
        };
        Ok(result)
    }

    async fn catalog_exist(&self, catalog: &str) -> Result<bool> {
        Ok(self.catalogs.read().unwrap().get(catalog).is_some())
    }

    async fn table_exist(&self, catalog: &str, schema: &str, table: &str) -> Result<bool> {
        let catalogs = self.catalogs.read().unwrap();
        Ok(catalogs
            .get(catalog)
            .with_context(|| CatalogNotFoundSnafu {
                catalog_name: catalog,
            })?
            .get(schema)
            .with_context(|| SchemaNotFoundSnafu { catalog, schema })?
            .contains_key(table))
    }

    async fn catalog_names(&self) -> Result<Vec<String>> {
        Ok(self.catalogs.read().unwrap().keys().cloned().collect())
    }

    async fn schema_names(&self, catalog_name: &str) -> Result<Vec<String>> {
        Ok(self
            .catalogs
            .read()
            .unwrap()
            .get(catalog_name)
            .with_context(|| CatalogNotFoundSnafu { catalog_name })?
            .keys()
            .cloned()
            .collect())
    }

    async fn table_names(&self, catalog_name: &str, schema_name: &str) -> Result<Vec<String>> {
        Ok(self
            .catalogs
            .read()
            .unwrap()
            .get(catalog_name)
            .with_context(|| CatalogNotFoundSnafu { catalog_name })?
            .get(schema_name)
            .with_context(|| SchemaNotFoundSnafu {
                catalog: catalog_name,
                schema: schema_name,
            })?
            .keys()
            .cloned()
            .collect())
    }

    async fn register_catalog(&self, name: String) -> Result<bool> {
        self.register_catalog_sync(name)?;
        increment_gauge!(crate::metrics::METRIC_CATALOG_MANAGER_CATALOG_COUNT, 1.0);
        Ok(true)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl MemoryCatalogManager {
    /// Registers a catalog and return the catalog already exist
    pub fn register_catalog_if_absent(&self, name: String) -> bool {
        let mut catalogs = self.catalogs.write().unwrap();
        let entry = catalogs.entry(name);
        match entry {
            Entry::Occupied(_) => true,
            Entry::Vacant(v) => {
                v.insert(HashMap::new());
                false
            }
        }
    }

    pub fn register_catalog_sync(&self, name: String) -> Result<bool> {
        let mut catalogs = self.catalogs.write().unwrap();
        Ok(catalogs.insert(name, HashMap::new()).is_some())
    }

    pub fn register_schema_sync(&self, request: RegisterSchemaRequest) -> Result<bool> {
        let mut catalogs = self.catalogs.write().unwrap();
        let catalog = catalogs
            .get_mut(&request.catalog)
            .with_context(|| CatalogNotFoundSnafu {
                catalog_name: &request.catalog,
            })?;
        if catalog.contains_key(&request.schema) {
            return Ok(false);
        }
        catalog.insert(request.schema, HashMap::new());
        Ok(true)
    }

    pub fn register_table_sync(&self, request: RegisterTableRequest) -> Result<bool> {
        let mut catalogs = self.catalogs.write().unwrap();
        let schema = catalogs
            .get_mut(&request.catalog)
            .with_context(|| CatalogNotFoundSnafu {
                catalog_name: &request.catalog,
            })?
            .get_mut(&request.schema)
            .with_context(|| SchemaNotFoundSnafu {
                catalog: &request.catalog,
                schema: &request.schema,
            })?;

        if schema.contains_key(&request.table_name) {
            return TableExistsSnafu {
                table: &request.table_name,
            }
            .fail();
        }

        Ok(schema.insert(request.table_name, request.table).is_none())
    }
}

impl Default for MemoryCatalogProvider {
    fn default() -> Self {
        Self::new()
    }
}

/// Simple in-memory implementation of a catalog.
pub struct MemoryCatalogProvider {
    schemas: RwLock<HashMap<String, HashMap<String, TableRef>>>,
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
}

#[async_trait::async_trait]
impl CatalogProvider for MemoryCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn schema_names(&self) -> Result<Vec<String>> {
        self.schema_names_sync()
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

/// Create a memory catalog list contains a numbers table for test
pub fn new_memory_catalog_manager() -> Result<Arc<MemoryCatalogManager>> {
    Ok(Arc::new(MemoryCatalogManager::default()))
}

#[cfg(test)]
mod tests {
    use common_catalog::consts::*;
    use common_error::ext::ErrorExt;
    use common_error::prelude::StatusCode;
    use table::table::numbers::{NumbersTable, NUMBERS_TABLE_NAME};

    use super::*;

    #[tokio::test]
    async fn test_new_memory_catalog_list() {
        let catalog_list = new_memory_catalog_manager().unwrap();
        // let default_catalog = CatalogManager::catalog(&*catalog_list, DEFAULT_CATALOG_NAME)
        //     .await
        //     .unwrap()
        //     .unwrap();

        // let default_schema = default_catalog
        //     .schema(DEFAULT_SCHEMA_NAME)
        //     .await
        //     .unwrap()
        //     .unwrap();
        let register_request = RegisterTableRequest {
            catalog: DEFAULT_CATALOG_NAME.to_string(),
            schema: DEFAULT_SCHEMA_NAME.to_string(),
            table_name: NUMBERS_TABLE_NAME.to_string(),
            table_id: NUMBERS_TABLE_ID,
            table: Arc::new(NumbersTable::default()),
        };

        catalog_list.register_table(register_request).await.unwrap();
        let table = catalog_list
            .table(
                DEFAULT_CATALOG_NAME,
                DEFAULT_SCHEMA_NAME,
                NUMBERS_TABLE_NAME,
            )
            .await
            .unwrap();
        assert!(table.is_some());
        assert!(catalog_list
            .table(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, "not_exists")
            .await
            .unwrap()
            .is_none());
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
        let catalog = MemoryCatalogManager::default();
        let table_name = "test_table";
        assert!(!catalog
            .table_exist(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, table_name)
            .await
            .unwrap());
        // register test table
        let table_id = 2333;
        let register_request = RegisterTableRequest {
            catalog: DEFAULT_CATALOG_NAME.to_string(),
            schema: DEFAULT_SCHEMA_NAME.to_string(),
            table_name: table_name.to_string(),
            table_id,
            table: Arc::new(NumbersTable::new(table_id)),
        };
        assert!(catalog.register_table(register_request).await.unwrap());
        assert!(catalog
            .table_exist(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, table_name)
            .await
            .unwrap());

        // rename test table
        let new_table_name = "test_table_renamed";
        let rename_request = RenameTableRequest {
            catalog: DEFAULT_CATALOG_NAME.to_string(),
            schema: DEFAULT_SCHEMA_NAME.to_string(),
            table_name: table_name.to_string(),
            new_table_name: new_table_name.to_string(),
            table_id,
        };
        catalog.rename_table(rename_request).await.unwrap();

        // test old table name not exist
        assert!(!catalog
            .table_exist(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, table_name)
            .await
            .unwrap());

        // test new table name exists
        assert!(catalog
            .table_exist(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, new_table_name)
            .await
            .unwrap());
        let registered_table = catalog
            .table(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, new_table_name)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(registered_table.table_info().ident.table_id, table_id);

        let dup_register_request = RegisterTableRequest {
            catalog: DEFAULT_CATALOG_NAME.to_string(),
            schema: DEFAULT_SCHEMA_NAME.to_string(),
            table_name: new_table_name.to_string(),
            table_id: table_id + 1,
            table: Arc::new(NumbersTable::new(table_id + 1)),
        };
        let result = catalog.register_table(dup_register_request).await;
        let err = result.err().unwrap();
        assert_eq!(StatusCode::TableAlreadyExists, err.status_code());
    }

    #[tokio::test]
    async fn test_catalog_rename_table() {
        let catalog = MemoryCatalogManager::default();
        let table_name = "num";
        let table_id = 2333;
        let table: TableRef = Arc::new(NumbersTable::new(table_id));

        // register table
        let register_table_req = RegisterTableRequest {
            catalog: DEFAULT_CATALOG_NAME.to_string(),
            schema: DEFAULT_SCHEMA_NAME.to_string(),
            table_name: table_name.to_string(),
            table_id,
            table,
        };
        assert!(catalog.register_table(register_table_req).await.unwrap());
        assert!(catalog
            .table(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, table_name)
            .await
            .unwrap()
            .is_some());

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
        assert!(catalog
            .table(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, table_name)
            .await
            .unwrap()
            .is_none());
        assert!(catalog
            .table(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, new_table_name)
            .await
            .unwrap()
            .is_some());

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
        assert!(!list.register_catalog_if_absent("test_catalog".to_string(),));
        assert!(list.register_catalog_if_absent("test_catalog".to_string()));
    }

    #[tokio::test]
    pub async fn test_catalog_deregister_table() {
        let catalog = MemoryCatalogManager::default();
        let table_name = "foo_table";

        let register_table_req = RegisterTableRequest {
            catalog: DEFAULT_CATALOG_NAME.to_string(),
            schema: DEFAULT_SCHEMA_NAME.to_string(),
            table_name: table_name.to_string(),
            table_id: 2333,
            table: Arc::new(NumbersTable::default()),
        };
        catalog.register_table(register_table_req).await.unwrap();
        assert!(catalog
            .table(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, table_name)
            .await
            .unwrap()
            .is_some());

        let deregister_table_req = DeregisterTableRequest {
            catalog: DEFAULT_CATALOG_NAME.to_string(),
            schema: DEFAULT_SCHEMA_NAME.to_string(),
            table_name: table_name.to_string(),
        };
        catalog
            .deregister_table(deregister_table_req)
            .await
            .unwrap();
        assert!(catalog
            .table(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, table_name)
            .await
            .unwrap()
            .is_none());
    }
}
