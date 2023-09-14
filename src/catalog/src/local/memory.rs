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
use std::sync::{Arc, RwLock, Weak};

use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, INFORMATION_SCHEMA_NAME};
use metrics::{decrement_gauge, increment_gauge};
use snafu::OptionExt;
use table::TableRef;

use crate::error::{CatalogNotFoundSnafu, Result, SchemaNotFoundSnafu, TableExistsSnafu};
use crate::information_schema::InformationSchemaProvider;
use crate::{CatalogManager, DeregisterTableRequest, RegisterSchemaRequest, RegisterTableRequest};

type SchemaEntries = HashMap<String, HashMap<String, TableRef>>;

/// Simple in-memory list of catalogs
#[derive(Clone)]
pub struct MemoryCatalogManager {
    /// Collection of catalogs containing schemas and ultimately Tables
    catalogs: Arc<RwLock<HashMap<String, SchemaEntries>>>,
}

#[async_trait::async_trait]
impl CatalogManager for MemoryCatalogManager {
    async fn schema_exists(&self, catalog: &str, schema: &str) -> Result<bool> {
        self.schema_exist_sync(catalog, schema)
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

    async fn catalog_exists(&self, catalog: &str) -> Result<bool> {
        self.catalog_exist_sync(catalog)
    }

    async fn table_exists(&self, catalog: &str, schema: &str, table: &str) -> Result<bool> {
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

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl MemoryCatalogManager {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            catalogs: Default::default(),
        })
    }

    /// Creates a manager with some default setups
    /// (e.g. default catalog/schema and information schema)
    pub fn with_default_setup() -> Arc<Self> {
        let manager = Arc::new(Self {
            catalogs: Default::default(),
        });

        // Safety: default catalog/schema is registered in order so no CatalogNotFound error will occur
        manager.register_catalog_sync(DEFAULT_CATALOG_NAME).unwrap();
        manager
            .register_schema_sync(RegisterSchemaRequest {
                catalog: DEFAULT_CATALOG_NAME.to_string(),
                schema: DEFAULT_SCHEMA_NAME.to_string(),
            })
            .unwrap();

        manager
    }

    fn schema_exist_sync(&self, catalog: &str, schema: &str) -> Result<bool> {
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

    fn catalog_exist_sync(&self, catalog: &str) -> Result<bool> {
        Ok(self.catalogs.read().unwrap().get(catalog).is_some())
    }

    /// Registers a catalog if it does not exist and returns false if the schema exists.
    pub fn register_catalog_sync(&self, name: &str) -> Result<bool> {
        let name = name.to_string();

        let mut catalogs = self.catalogs.write().unwrap();

        match catalogs.entry(name.clone()) {
            Entry::Vacant(e) => {
                let arc_self = Arc::new(self.clone());
                let catalog = arc_self.create_catalog_entry(name);
                e.insert(catalog);
                increment_gauge!(crate::metrics::METRIC_CATALOG_MANAGER_CATALOG_COUNT, 1.0);
                Ok(true)
            }
            Entry::Occupied(_) => Ok(false),
        }
    }

    pub fn deregister_table_sync(&self, request: DeregisterTableRequest) -> Result<()> {
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

    /// Registers a schema if it does not exist.
    /// It returns an error if the catalog does not exist,
    /// and returns false if the schema exists.
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

    /// Registers a schema and returns an error if the catalog or schema does not exist.
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

    fn create_catalog_entry(self: &Arc<Self>, catalog: String) -> SchemaEntries {
        let information_schema = InformationSchemaProvider::build(
            catalog,
            Arc::downgrade(self) as Weak<dyn CatalogManager>,
        );
        let mut catalog = HashMap::new();
        catalog.insert(INFORMATION_SCHEMA_NAME.to_string(), information_schema);
        catalog
    }

    #[cfg(any(test, feature = "testing"))]
    pub fn new_with_table(table: TableRef) -> Arc<Self> {
        let manager = Self::with_default_setup();
        let catalog = &table.table_info().catalog_name;
        let schema = &table.table_info().schema_name;

        if !manager.catalog_exist_sync(catalog).unwrap() {
            manager.register_catalog_sync(catalog).unwrap();
        }

        if !manager.schema_exist_sync(catalog, schema).unwrap() {
            manager
                .register_schema_sync(RegisterSchemaRequest {
                    catalog: catalog.to_string(),
                    schema: schema.to_string(),
                })
                .unwrap();
        }

        let request = RegisterTableRequest {
            catalog: catalog.to_string(),
            schema: schema.to_string(),
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
    Ok(MemoryCatalogManager::with_default_setup())
}

#[cfg(test)]
mod tests {
    use common_catalog::consts::*;
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
            table: NumbersTable::table(NUMBERS_TABLE_ID),
        };

        catalog_list.register_table_sync(register_request).unwrap();
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

    #[test]
    pub fn test_register_catalog_sync() {
        let list = MemoryCatalogManager::with_default_setup();
        assert!(list.register_catalog_sync("test_catalog").unwrap());
        assert!(!list.register_catalog_sync("test_catalog").unwrap());
    }

    #[tokio::test]
    pub async fn test_catalog_deregister_table() {
        let catalog = MemoryCatalogManager::with_default_setup();
        let table_name = "foo_table";

        let register_table_req = RegisterTableRequest {
            catalog: DEFAULT_CATALOG_NAME.to_string(),
            schema: DEFAULT_SCHEMA_NAME.to_string(),
            table_name: table_name.to_string(),
            table_id: 2333,
            table: NumbersTable::table(2333),
        };
        catalog.register_table_sync(register_table_req).unwrap();
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
        catalog.deregister_table_sync(deregister_table_req).unwrap();
        assert!(catalog
            .table(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, table_name)
            .await
            .unwrap()
            .is_none());
    }
}
