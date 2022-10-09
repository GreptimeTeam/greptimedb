//! Catalog adapter between datafusion and greptime query engine.

use std::any::Any;
use std::sync::Arc;

use catalog::{
    CatalogListRef, CatalogProvider, CatalogProviderRef, SchemaProvider, SchemaProviderRef,
};
use datafusion::catalog::{
    catalog::{CatalogList as DfCatalogList, CatalogProvider as DfCatalogProvider},
    schema::SchemaProvider as DfSchemaProvider,
};
use datafusion::datasource::TableProvider as DfTableProvider;
use datafusion::error::Result as DataFusionResult;
use datafusion::execution::runtime_env::RuntimeEnv;
use snafu::ResultExt;
use table::{
    table::adapter::{DfTableProviderAdapter, TableAdapter},
    TableRef,
};

use crate::datafusion::error;

pub struct DfCatalogListAdapter {
    runtime: Arc<RuntimeEnv>,
    catalog_list: CatalogListRef,
}

impl DfCatalogListAdapter {
    pub fn new(runtime: Arc<RuntimeEnv>, catalog_list: CatalogListRef) -> DfCatalogListAdapter {
        DfCatalogListAdapter {
            runtime,
            catalog_list,
        }
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
            runtime: self.runtime.clone(),
        });
        self.catalog_list
            .register_catalog(name, catalog_adapter)
            .expect("datafusion does not accept fallible catalog access") // TODO(hl): datafusion register catalog does not handles errors
            .map(|catalog_provider| {
                Arc::new(DfCatalogProviderAdapter {
                    catalog_provider,
                    runtime: self.runtime.clone(),
                }) as _
            })
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
            .map(|catalog_provider| {
                Arc::new(DfCatalogProviderAdapter {
                    catalog_provider,
                    runtime: self.runtime.clone(),
                }) as _
            })
    }
}

/// Datafusion's CatalogProvider ->  greptime CatalogProvider
struct CatalogProviderAdapter {
    df_catalog_provider: Arc<dyn DfCatalogProvider>,
    runtime: Arc<RuntimeEnv>,
}

impl CatalogProvider for CatalogProviderAdapter {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Result<Vec<String>, catalog::error::Error> {
        Ok(self.df_catalog_provider.schema_names())
    }

    fn register_schema(
        &self,
        _name: String,
        _schema: SchemaProviderRef,
    ) -> Result<Option<SchemaProviderRef>, catalog::error::Error> {
        todo!("register_schema is not supported in Datafusion catalog provider")
    }

    fn schema(&self, name: &str) -> Result<Option<Arc<dyn SchemaProvider>>, catalog::error::Error> {
        Ok(self
            .df_catalog_provider
            .schema(name)
            .map(|df_schema_provider| {
                Arc::new(SchemaProviderAdapter {
                    df_schema_provider,
                    runtime: self.runtime.clone(),
                }) as _
            }))
    }
}

///Greptime CatalogProvider -> datafusion's CatalogProvider
struct DfCatalogProviderAdapter {
    catalog_provider: CatalogProviderRef,
    runtime: Arc<RuntimeEnv>,
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
            .map(|schema_provider| {
                Arc::new(DfSchemaProviderAdapter {
                    schema_provider,
                    runtime: self.runtime.clone(),
                }) as _
            })
    }
}

/// Greptime SchemaProvider -> datafusion SchemaProvider
struct DfSchemaProviderAdapter {
    schema_provider: Arc<dyn SchemaProvider>,
    runtime: Arc<RuntimeEnv>,
}

impl DfSchemaProvider for DfSchemaProviderAdapter {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.schema_provider.table_names()
    }

    fn table(&self, name: &str) -> Option<Arc<dyn DfTableProvider>> {
        self.schema_provider
            .table(name)
            .map(|table| Arc::new(DfTableProviderAdapter::new(table)) as _)
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn DfTableProvider>,
    ) -> DataFusionResult<Option<Arc<dyn DfTableProvider>>> {
        let table = Arc::new(TableAdapter::new(table, self.runtime.clone())?);
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
        self.schema_provider.table_exist(name)
    }
}

/// Datafusion SchemaProviderAdapter -> greptime SchemaProviderAdapter
struct SchemaProviderAdapter {
    df_schema_provider: Arc<dyn DfSchemaProvider>,
    runtime: Arc<RuntimeEnv>,
}

impl SchemaProvider for SchemaProviderAdapter {
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Retrieves the list of available table names in this schema.
    fn table_names(&self) -> Vec<String> {
        self.df_schema_provider.table_names()
    }

    fn table(&self, name: &str) -> Option<TableRef> {
        self.df_schema_provider.table(name).map(|table_provider| {
            match table_provider
                .as_any()
                .downcast_ref::<DfTableProviderAdapter>()
            {
                Some(adapter) => adapter.table(),
                None => {
                    // TODO(yingwen): Avoid panic here.
                    let adapter = TableAdapter::new(table_provider, self.runtime.clone())
                        .expect("convert datafusion table");
                    Arc::new(adapter) as _
                }
            }
        })
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
            })?
            .map(|_| table))
    }

    fn deregister_table(&self, name: &str) -> catalog::error::Result<Option<TableRef>> {
        self.df_schema_provider
            .deregister_table(name)
            .context(error::DatafusionSnafu {
                msg: "Fail to deregister table from datafusion",
            })?
            .map(|table| {
                let adapter = TableAdapter::new(table, self.runtime.clone())
                    .context(error::TableSchemaMismatchSnafu)?;
                Ok(Arc::new(adapter) as _)
            })
            .transpose()
    }

    fn table_exist(&self, name: &str) -> bool {
        self.df_schema_provider.table_exist(name)
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
            runtime: Arc::new(RuntimeEnv::default()),
        };

        adapter.register_schema(
            "whatever".to_string(),
            Arc::new(MemorySchemaProvider::new()),
        );
    }

    #[test]
    pub fn test_register_table() {
        let adapter = DfSchemaProviderAdapter {
            runtime: Arc::new(RuntimeEnv::default()),
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
        adapter.table("test_table").unwrap();
    }

    #[test]
    pub fn test_register_catalog() {
        let rt = Arc::new(RuntimeEnv::default());
        let catalog_list = DfCatalogListAdapter {
            runtime: rt.clone(),
            catalog_list: new_memory_catalog_list().unwrap(),
        };
        assert!(catalog_list
            .register_catalog(
                "test_catalog".to_string(),
                Arc::new(DfCatalogProviderAdapter {
                    catalog_provider: Arc::new(MemoryCatalogProvider::new()),
                    runtime: rt,
                }),
            )
            .is_none());

        catalog_list.catalog("test_catalog").unwrap();
    }
}
