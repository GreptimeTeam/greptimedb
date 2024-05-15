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

//! Dummy catalog for region server.

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::schema::SchemaProvider;
use datafusion::catalog::{CatalogProvider, CatalogProviderList};
use datafusion::datasource::TableProvider;

/// Resolve to the given region (specified by [RegionId]) unconditionally.
#[derive(Clone)]
pub struct DummyCatalogList {
    catalog: DummyCatalogProvider,
}

impl DummyCatalogList {
    /// Creates a new catalog list with the given table provider.
    pub fn with_table_provider(table_provider: Arc<dyn TableProvider>) -> Self {
        let schema_provider = Arc::new(DummySchemaProvider {
            table: table_provider,
        });
        let catalog_provider = DummyCatalogProvider {
            schema: schema_provider,
        };
        Self {
            catalog: catalog_provider,
        }
    }

    /// Creates a new catalog list with the given table providers.
    pub fn with_tables(tables: HashMap<String, Arc<dyn TableProvider>>) -> Self {
        let schema_provider = Arc::new(DummyTablesSchemaProvider { tables });
        let catalog_provider = DummyCatalogProvider {
            schema: schema_provider,
        };
        Self {
            catalog: catalog_provider,
        }
    }
}

impl CatalogProviderList for DummyCatalogList {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn register_catalog(
        &self,
        _name: String,
        _catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
        None
    }

    fn catalog_names(&self) -> Vec<String> {
        vec![]
    }

    fn catalog(&self, _name: &str) -> Option<Arc<dyn CatalogProvider>> {
        Some(Arc::new(self.catalog.clone()))
    }
}

/// A dummy catalog provider for [DummyCatalogList].
#[derive(Clone)]
struct DummyCatalogProvider {
    schema: Arc<dyn SchemaProvider>,
}

impl CatalogProvider for DummyCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        vec![]
    }

    fn schema(&self, _name: &str) -> Option<Arc<dyn SchemaProvider>> {
        Some(self.schema.clone())
    }
}

/// A dummy schema provider for [DummyCatalogList].
#[derive(Clone)]
struct DummySchemaProvider {
    table: Arc<dyn TableProvider>,
}

#[async_trait]
impl SchemaProvider for DummySchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        vec![]
    }

    async fn table(
        &self,
        _name: &str,
    ) -> datafusion::error::Result<Option<Arc<dyn TableProvider>>> {
        Ok(Some(self.table.clone()))
    }

    fn table_exist(&self, _name: &str) -> bool {
        true
    }
}

/// A dummy schema provider for [DummyCatalogList].
#[derive(Clone)]
struct DummyTablesSchemaProvider {
    tables: HashMap<String, Arc<dyn TableProvider>>,
}

#[async_trait]
impl SchemaProvider for DummyTablesSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.tables.keys().map(|t| t.to_string()).collect()
    }

    async fn table(&self, name: &str) -> datafusion::error::Result<Option<Arc<dyn TableProvider>>> {
        Ok(self.tables.get(name).cloned())
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }
}
