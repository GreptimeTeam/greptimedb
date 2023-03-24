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

//! Catalog adapter between datafusion and greptime query engine.

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use catalog::error::{self as catalog_error, Error};
use catalog::{
    CatalogListRef, CatalogProvider, CatalogProviderRef, SchemaProvider, SchemaProviderRef,
};
use common_error::prelude::BoxedError;
use datafusion::catalog::catalog::{
    CatalogList as DfCatalogList, CatalogProvider as DfCatalogProvider,
};
use datafusion::catalog::schema::SchemaProvider as DfSchemaProvider;
use datafusion::datasource::TableProvider as DfTableProvider;
use datafusion::error::Result as DataFusionResult;
use snafu::ResultExt;
use table::table::adapter::{DfTableProviderAdapter, TableAdapter};
use table::TableRef;

use crate::datafusion::error;

pub struct DfCatalogListAdapter {
    catalog_list: CatalogListRef,
}

impl DfCatalogListAdapter {
    pub fn new(catalog_list: CatalogListRef) -> DfCatalogListAdapter {
        DfCatalogListAdapter { catalog_list }
    }
}

impl DfCatalogList for DfCatalogListAdapter {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn register_catalog(
        &self,
        name: String,
        catalog: Arc<dyn DfCatalogProvider>,
    ) -> Option<Arc<dyn DfCatalogProvider>> {
        let catalog_adapter = Arc::new(CatalogProviderAdapter {
            df_catalog_provider: catalog,
        });
        self.catalog_list
            .register_catalog(name, catalog_adapter)
            .expect("datafusion does not accept fallible catalog access") // TODO(hl): datafusion register catalog does not handles errors
            .map(|catalog_provider| Arc::new(DfCatalogProviderAdapter { catalog_provider }) as _)
    }

    fn catalog_names(&self) -> Vec<String> {
        // TODO(hl): datafusion register catalog does not handles errors
        self.catalog_list
            .catalog_names()
            .expect("datafusion does not accept fallible catalog access")
    }

    fn catalog(&self, name: &str) -> Option<Arc<dyn DfCatalogProvider>> {
        self.catalog_list
            .catalog(name)
            .expect("datafusion does not accept fallible catalog access") // TODO(hl): datafusion register catalog does not handles errors
            .map(|catalog_provider| Arc::new(DfCatalogProviderAdapter { catalog_provider }) as _)
    }
}

/// Datafusion's CatalogProvider ->  greptime CatalogProvider
struct CatalogProviderAdapter {
    df_catalog_provider: Arc<dyn DfCatalogProvider>,
}

impl CatalogProvider for CatalogProviderAdapter {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> catalog::error::Result<Vec<String>> {
        Ok(self.df_catalog_provider.schema_names())
    }

    fn register_schema(
        &self,
        _name: String,
        _schema: SchemaProviderRef,
    ) -> catalog::error::Result<Option<SchemaProviderRef>> {
        todo!("register_schema is not supported in Datafusion catalog provider")
    }

    fn schema(&self, name: &str) -> catalog::error::Result<Option<Arc<dyn SchemaProvider>>> {
        Ok(self
            .df_catalog_provider
            .schema(name)
            .map(|df_schema_provider| Arc::new(SchemaProviderAdapter { df_schema_provider }) as _))
    }
}

///Greptime CatalogProvider -> datafusion's CatalogProvider
pub struct DfCatalogProviderAdapter {
    catalog_provider: CatalogProviderRef,
}

impl DfCatalogProviderAdapter {
    pub fn new(catalog_provider: CatalogProviderRef) -> Self {
        Self { catalog_provider }
    }
}

impl DfCatalogProvider for DfCatalogProviderAdapter {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        self.catalog_provider
            .schema_names()
            .expect("datafusion does not accept fallible catalog access")
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn DfSchemaProvider>> {
        self.catalog_provider
            .schema(name)
            .expect("datafusion does not accept fallible catalog access")
            .map(|schema_provider| Arc::new(DfSchemaProviderAdapter { schema_provider }) as _)
    }
}

/// Greptime SchemaProvider -> datafusion SchemaProvider
struct DfSchemaProviderAdapter {
    schema_provider: Arc<dyn SchemaProvider>,
}

#[async_trait]
impl DfSchemaProvider for DfSchemaProviderAdapter {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.schema_provider
            .table_names()
            .expect("datafusion does not accept fallible catalog access")
    }

    async fn table(&self, name: &str) -> Option<Arc<dyn DfTableProvider>> {
        self.schema_provider
            .table(name)
            .await
            .expect("datafusion does not accept fallible catalog access")
            .map(|table| Arc::new(DfTableProviderAdapter::new(table)) as _)
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn DfTableProvider>,
    ) -> DataFusionResult<Option<Arc<dyn DfTableProvider>>> {
        let table = Arc::new(TableAdapter::new(table)?);
        match self.schema_provider.register_table(name, table)? {
            Some(p) => Ok(Some(Arc::new(DfTableProviderAdapter::new(p)))),
            None => Ok(None),
        }
    }

    fn deregister_table(&self, name: &str) -> DataFusionResult<Option<Arc<dyn DfTableProvider>>> {
        match self.schema_provider.deregister_table(name)? {
            Some(p) => Ok(Some(Arc::new(DfTableProviderAdapter::new(p)))),
            None => Ok(None),
        }
    }

    fn table_exist(&self, name: &str) -> bool {
        self.schema_provider
            .table_exist(name)
            .expect("datafusion does not accept fallible catalog access")
    }
}

/// Datafusion SchemaProviderAdapter -> greptime SchemaProviderAdapter
struct SchemaProviderAdapter {
    df_schema_provider: Arc<dyn DfSchemaProvider>,
}

#[async_trait]
impl SchemaProvider for SchemaProviderAdapter {
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Retrieves the list of available table names in this schema.
    fn table_names(&self) -> Result<Vec<String>, Error> {
        Ok(self.df_schema_provider.table_names())
    }

    async fn table(&self, name: &str) -> Result<Option<TableRef>, Error> {
        let table = self.df_schema_provider.table(name).await;
        let table = table.map(|table_provider| {
            match table_provider
                .as_any()
                .downcast_ref::<DfTableProviderAdapter>()
            {
                Some(adapter) => adapter.table(),
                None => {
                    // TODO(yingwen): Avoid panic here.
                    let adapter =
                        TableAdapter::new(table_provider).expect("convert datafusion table");
                    Arc::new(adapter) as _
                }
            }
        });
        Ok(table)
    }

    fn register_table(
        &self,
        name: String,
        table: TableRef,
    ) -> catalog::error::Result<Option<TableRef>> {
        let table_provider = Arc::new(DfTableProviderAdapter::new(table.clone()));
        Ok(self
            .df_schema_provider
            .register_table(name, table_provider)
            .context(error::DatafusionSnafu {
                msg: "Fail to register table to datafusion",
            })
            .map_err(BoxedError::new)
            .context(catalog_error::SchemaProviderOperationSnafu)?
            .map(|_| table))
    }

    fn rename_table(&self, _name: &str, _new_name: String) -> catalog_error::Result<TableRef> {
        todo!()
    }

    fn deregister_table(&self, name: &str) -> catalog::error::Result<Option<TableRef>> {
        self.df_schema_provider
            .deregister_table(name)
            .context(error::DatafusionSnafu {
                msg: "Fail to deregister table from datafusion",
            })
            .map_err(BoxedError::new)
            .context(catalog_error::SchemaProviderOperationSnafu)?
            .map(|table| {
                let adapter = TableAdapter::new(table)
                    .context(error::TableSchemaMismatchSnafu)
                    .map_err(BoxedError::new)
                    .context(catalog_error::SchemaProviderOperationSnafu)?;
                Ok(Arc::new(adapter) as _)
            })
            .transpose()
    }

    fn table_exist(&self, name: &str) -> Result<bool, Error> {
        Ok(self.df_schema_provider.table_exist(name))
    }
}

#[cfg(test)]
mod tests {
    use catalog::local::{new_memory_catalog_list, MemoryCatalogProvider, MemorySchemaProvider};
    use table::table::numbers::NumbersTable;

    use super::*;

    #[test]
    #[should_panic]
    pub fn test_register_schema() {
        let adapter = CatalogProviderAdapter {
            df_catalog_provider: Arc::new(
                datafusion::catalog::catalog::MemoryCatalogProvider::new(),
            ),
        };

        adapter
            .register_schema(
                "whatever".to_string(),
                Arc::new(MemorySchemaProvider::new()),
            )
            .unwrap();
    }

    #[tokio::test]
    async fn test_register_table() {
        let adapter = DfSchemaProviderAdapter {
            schema_provider: Arc::new(MemorySchemaProvider::new()),
        };

        adapter
            .register_table(
                "test_table".to_string(),
                Arc::new(DfTableProviderAdapter::new(Arc::new(
                    NumbersTable::default(),
                ))),
            )
            .unwrap();
        adapter.table("test_table").await.unwrap();
    }

    #[test]
    pub fn test_register_catalog() {
        let catalog_list = DfCatalogListAdapter {
            catalog_list: new_memory_catalog_list().unwrap(),
        };
        assert!(catalog_list
            .register_catalog(
                "test_catalog".to_string(),
                Arc::new(DfCatalogProviderAdapter {
                    catalog_provider: Arc::new(MemoryCatalogProvider::new()),
                }),
            )
            .is_none());

        catalog_list.catalog("test_catalog").unwrap();
    }
}
