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
use std::sync::{Arc, RwLock, Weak};

use common_catalog::consts::{
    DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, INFORMATION_SCHEMA_NAME, MIN_USER_TABLE_ID,
};
use metrics::{decrement_gauge, increment_gauge};
use snafu::OptionExt;
use table::metadata::TableId;
use table::table::TableIdProvider;
use table::TableRef;

use crate::error::{
    CatalogNotFoundSnafu, Result, SchemaNotFoundSnafu, TableExistsSnafu, TableNotFoundSnafu,
};
use crate::information_schema::InformationSchemaProvider;
use crate::{
    CatalogManager, DeregisterSchemaRequest, DeregisterTableRequest, RegisterSchemaRequest,
    RegisterSystemTableRequest, RegisterTableRequest, RenameTableRequest,
};

type SchemaEntries = HashMap<String, HashMap<String, TableRef>>;

/// Simple in-memory list of catalogs
pub struct MemoryCatalogManager {
    /// Collection of catalogs containing schemas and ultimately Tables
    pub catalogs: RwLock<HashMap<String, SchemaEntries>>,
    pub table_id: AtomicU32,
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
        self.register_table_sync(request)
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
        let _ = schema.insert(request.new_table_name, table);

        Ok(true)
    }

    async fn deregister_table(&self, request: DeregisterTableRequest) -> Result<()> {
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
        Ok(())
    }

    async fn register_schema(&self, request: RegisterSchemaRequest) -> Result<bool> {
        self.register_schema_sync(request)
    }

    async fn deregister_schema(&self, request: DeregisterSchemaRequest) -> Result<bool> {
        let mut catalogs = self.catalogs.write().unwrap();
        let schemas = catalogs
            .get_mut(&request.catalog)
            .with_context(|| CatalogNotFoundSnafu {
                catalog_name: &request.catalog,
            })?;
        let table_count = schemas
            .remove(&request.schema)
            .with_context(|| SchemaNotFoundSnafu {
                catalog: &request.catalog,
                schema: &request.schema,
            })?
            .len();
        decrement_gauge!(
            crate::metrics::METRIC_CATALOG_MANAGER_TABLE_COUNT,
            table_count as f64,
            &[crate::metrics::db_label(&request.catalog, &request.schema)],
        );

        decrement_gauge!(
            crate::metrics::METRIC_CATALOG_MANAGER_SCHEMA_COUNT,
            1.0,
            &[crate::metrics::db_label(&request.catalog, &request.schema)],
        );
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

    async fn register_catalog(self: Arc<Self>, name: String) -> Result<bool> {
        self.register_catalog_sync(name)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl MemoryCatalogManager {
    /// Yes this is the `default()` constructor
    #[allow(clippy::should_implement_trait)]
    pub fn default() -> Arc<Self> {
        let manager = Arc::new(Self {
            table_id: AtomicU32::new(MIN_USER_TABLE_ID),
            catalogs: Default::default(),
        });

        // Safety: default catalog/schema is registerd in order so no CatalogNotFound error will occur
        manager
            .clone()
            .register_catalog_sync(DEFAULT_CATALOG_NAME.to_string())
            .unwrap();
        manager
            .register_schema_sync(RegisterSchemaRequest {
                catalog: DEFAULT_CATALOG_NAME.to_string(),
                schema: DEFAULT_SCHEMA_NAME.to_string(),
            })
            .unwrap();

        manager
    }

    /// Registers a catalog and return the catalog already exist
    pub fn register_catalog_if_absent(&self, name: String) -> bool {
        let mut catalogs = self.catalogs.write().unwrap();
        let entry = catalogs.entry(name);
        match entry {
            Entry::Occupied(_) => true,
            Entry::Vacant(v) => {
                let _ = v.insert(HashMap::new());
                false
            }
        }
    }

    pub fn register_catalog_sync(self: Arc<Self>, name: String) -> Result<bool> {
        let mut catalogs = self.catalogs.write().unwrap();

        match catalogs.entry(name.clone()) {
            Entry::Vacant(e) => {
                let catalog = self.clone().create_catalog_entry(name.clone());
                e.insert(catalog);
                increment_gauge!(crate::metrics::METRIC_CATALOG_MANAGER_CATALOG_COUNT, 1.0);
                Ok(true)
            }
            Entry::Occupied(_) => Ok(false),
        }
    }

    pub fn register_schema_sync(&self, request: RegisterSchemaRequest) -> Result<bool> {
        let mut catalogs = self.catalogs.write().unwrap();
        let catalog = catalogs
            .get_mut(&request.catalog)
            .with_context(|| CatalogNotFoundSnafu {
                catalog_name: &request.catalog,
            })?;

        match catalog.entry(request.schema) {
            Entry::Vacant(e) => {
                e.insert(HashMap::new());
                increment_gauge!(crate::metrics::METRIC_CATALOG_MANAGER_SCHEMA_COUNT, 1.0);
                Ok(true)
            }
            Entry::Occupied(_) => Ok(false),
        }
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
        schema.insert(request.table_name, request.table);
        increment_gauge!(
            crate::metrics::METRIC_CATALOG_MANAGER_TABLE_COUNT,
            1.0,
            &[crate::metrics::db_label(&request.catalog, &request.schema)],
        );
        Ok(true)
    }

    fn create_catalog_entry(self: Arc<Self>, catalog: String) -> SchemaEntries {
        let information_schema = InformationSchemaProvider::build_information_schema(
            catalog.clone(),
            Arc::downgrade(&self) as Weak<dyn CatalogManager>,
        );
        let mut catalog = HashMap::new();
        catalog.insert(INFORMATION_SCHEMA_NAME.to_string(), information_schema);
        catalog
    }

    #[cfg(any(test, feature = "testing"))]
    pub fn new_with_table(table: TableRef) -> Arc<Self> {
        let manager = Self::default();
        let request = RegisterTableRequest {
            catalog: DEFAULT_CATALOG_NAME.to_string(),
            schema: DEFAULT_SCHEMA_NAME.to_string(),
            table_name: table.table_info().name.clone(),
            table_id: table.table_info().ident.table_id,
            table,
        };
        let _ = manager.register_table_sync(request).unwrap();
        manager
    }
}

/// Create a memory catalog list contains a numbers table for test
pub fn new_memory_catalog_manager() -> Result<Arc<MemoryCatalogManager>> {
    Ok(MemoryCatalogManager::default())
}

#[cfg(test)]
mod tests {
    use common_catalog::consts::*;
    use common_error::ext::ErrorExt;
    use common_error::status_code::StatusCode;
    use table::table::numbers::{NumbersTable, NUMBERS_TABLE_NAME};

    use super::*;

    #[tokio::test]
    async fn test_new_memory_catalog_list() {
        let catalog_list = new_memory_catalog_manager().unwrap();

        let register_request = RegisterTableRequest {
            catalog: DEFAULT_CATALOG_NAME.to_string(),
            schema: DEFAULT_SCHEMA_NAME.to_string(),
            table_name: NUMBERS_TABLE_NAME.to_string(),
            table_id: NUMBERS_TABLE_ID,
            table: Arc::new(NumbersTable::default()),
        };

        let _ = catalog_list.register_table(register_request).await.unwrap();
        let table = catalog_list
            .table(
                DEFAULT_CATALOG_NAME,
                DEFAULT_SCHEMA_NAME,
                NUMBERS_TABLE_NAME,
            )
            .await
            .unwrap();
        let _ = table.unwrap();
        assert!(catalog_list
            .table(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, "not_exists")
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn test_mem_manager_rename_table() {
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
        let _ = catalog.rename_table(rename_request).await.unwrap();

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
        let _ = catalog.register_table(register_table_req).await.unwrap();
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

    #[tokio::test]
    async fn test_catalog_deregister_schema() {
        let catalog = MemoryCatalogManager::default();

        // Registers a catalog, a schema, and a table.
        let catalog_name = "foo_catalog".to_string();
        let schema_name = "foo_schema".to_string();
        let table_name = "foo_table".to_string();
        let schema = RegisterSchemaRequest {
            catalog: catalog_name.clone(),
            schema: schema_name.clone(),
        };
        let table = RegisterTableRequest {
            catalog: catalog_name.clone(),
            schema: schema_name.clone(),
            table_name,
            table_id: 0,
            table: Arc::new(NumbersTable::default()),
        };
        catalog
            .clone()
            .register_catalog(catalog_name.clone())
            .await
            .unwrap();
        catalog.register_schema(schema).await.unwrap();
        catalog.register_table(table).await.unwrap();

        let request = DeregisterSchemaRequest {
            catalog: catalog_name.clone(),
            schema: schema_name.clone(),
        };

        assert!(catalog.deregister_schema(request).await.unwrap());
        assert!(!catalog
            .schema_exist(&catalog_name, &schema_name)
            .await
            .unwrap());
    }
}
