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
use std::sync::Arc;

use async_trait::async_trait;
use common_catalog::format_full_table_name;
use datafusion::catalog::{CatalogProvider, CatalogProviderList, SchemaProvider};
use datafusion::datasource::TableProvider;
use snafu::OptionExt;
use table::table::adapter::DfTableProviderAdapter;

use crate::error::TableNotExistSnafu;
use crate::CatalogManagerRef;

/// Delegate the resolving requests to the `[CatalogManager]` unconditionally.
#[derive(Clone, Debug)]
pub struct DummyCatalogList {
    catalog_manager: CatalogManagerRef,
}

impl DummyCatalogList {
    /// Creates a new catalog list with the given catalog manager.
    pub fn new(catalog_manager: CatalogManagerRef) -> Self {
        Self { catalog_manager }
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

    fn catalog(&self, catalog_name: &str) -> Option<Arc<dyn CatalogProvider>> {
        Some(Arc::new(DummyCatalogProvider {
            catalog_name: catalog_name.to_string(),
            catalog_manager: self.catalog_manager.clone(),
        }))
    }
}

/// A dummy catalog provider for [DummyCatalogList].
#[derive(Clone, Debug)]
struct DummyCatalogProvider {
    catalog_name: String,
    catalog_manager: CatalogManagerRef,
}

impl CatalogProvider for DummyCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        vec![]
    }

    fn schema(&self, schema_name: &str) -> Option<Arc<dyn SchemaProvider>> {
        Some(Arc::new(DummySchemaProvider {
            catalog_name: self.catalog_name.clone(),
            schema_name: schema_name.to_string(),
            catalog_manager: self.catalog_manager.clone(),
        }))
    }
}

/// A dummy schema provider for [DummyCatalogList].
#[derive(Clone, Debug)]
struct DummySchemaProvider {
    catalog_name: String,
    schema_name: String,
    catalog_manager: CatalogManagerRef,
}

#[async_trait]
impl SchemaProvider for DummySchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        vec![]
    }

    async fn table(&self, name: &str) -> datafusion::error::Result<Option<Arc<dyn TableProvider>>> {
        let table = self
            .catalog_manager
            .table(&self.catalog_name, &self.schema_name, name, None)
            .await?
            .with_context(|| TableNotExistSnafu {
                table: format_full_table_name(&self.catalog_name, &self.schema_name, name),
            })?;

        let table_provider: Arc<dyn TableProvider> = Arc::new(DfTableProviderAdapter::new(table));

        Ok(Some(table_provider))
    }

    fn table_exist(&self, _name: &str) -> bool {
        true
    }
}
