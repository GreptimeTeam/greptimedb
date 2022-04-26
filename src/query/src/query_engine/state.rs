use std::any::Any;
use std::sync::Arc;

use datafusion::catalog::{
    catalog::{CatalogList as DfCatalogList, CatalogProvider as DfCatalogProvider},
    schema::SchemaProvider as DfSchemaProvider,
};
use datafusion::datasource::TableProvider as DfTableProvider;
use datafusion::error::Result as DataFusionResult;
use datafusion::prelude::{ExecutionConfig, ExecutionContext};
use snafu::ResultExt;
use table::{
    table::adapter::{DfTableProviderAdapter, TableAdapter},
    Table,
};

use crate::catalog::{schema::SchemaProvider, CatalogList, CatalogProvider};
use crate::error::{self, Result};

/// Query engine global state
#[derive(Clone)]
pub struct QueryEngineState {
    df_context: ExecutionContext,
}

impl QueryEngineState {
    pub(crate) fn new(catalog_list: Arc<dyn CatalogList>) -> Self {
        let config = ExecutionConfig::new().with_default_catalog_and_schema("greptime", "public");
        let df_context = ExecutionContext::with_config(config);

        df_context.state.lock().catalog_list = Arc::new(DfCatalogListAdapter {
            catalog_list: catalog_list.clone(),
        });

        Self { df_context }
    }

    #[inline]
    pub(crate) fn df_context(&self) -> &ExecutionContext {
        &self.df_context
    }
}

/// Adapters between datafusion and greptime query engine.
struct DfCatalogListAdapter {
    catalog_list: Arc<dyn CatalogList>,
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
            df_cataglog_provider: catalog,
        });
        match self.catalog_list.register_catalog(name, catalog_adapter) {
            Some(catalog_provider) => Some(Arc::new(DfCatalogProviderAdapter { catalog_provider })),
            None => None,
        }
    }

    fn catalog_names(&self) -> Vec<String> {
        self.catalog_list.catalog_names()
    }

    fn catalog(&self, name: &str) -> Option<Arc<dyn DfCatalogProvider>> {
        match self.catalog_list.catalog(name) {
            Some(catalog_provider) => Some(Arc::new(DfCatalogProviderAdapter { catalog_provider })),
            None => None,
        }
    }
}

/// Datafusion's CatalogProvider ->  greptime CatalogProvider
struct CatalogProviderAdapter {
    df_cataglog_provider: Arc<dyn DfCatalogProvider>,
}

impl CatalogProvider for CatalogProviderAdapter {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        self.df_cataglog_provider.schema_names()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        match self.df_cataglog_provider.schema(name) {
            Some(df_schema_provider) => {
                Some(Arc::new(SchemaProviderAdapter { df_schema_provider }))
            }
            None => None,
        }
    }
}

///Greptime CatalogProvider -> datafusion's CatalogProvider
struct DfCatalogProviderAdapter {
    catalog_provider: Arc<dyn CatalogProvider>,
}

impl DfCatalogProvider for DfCatalogProviderAdapter {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        self.catalog_provider.schema_names()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn DfSchemaProvider>> {
        match self.catalog_provider.schema(name) {
            Some(schema_provider) => Some(Arc::new(DfSchemaProviderAdapter { schema_provider })),
            None => None,
        }
    }
}

/// Greptime SchemaProvider -> datafusion SchemaProvider
struct DfSchemaProviderAdapter {
    schema_provider: Arc<dyn SchemaProvider>,
}

impl DfSchemaProvider for DfSchemaProviderAdapter {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.schema_provider.table_names()
    }

    fn table(&self, name: &str) -> Option<Arc<dyn DfTableProvider>> {
        match self.schema_provider.table(name) {
            Some(table) => Some(Arc::new(DfTableProviderAdapter::new(table))),
            None => None,
        }
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn DfTableProvider>,
    ) -> DataFusionResult<Option<Arc<dyn DfTableProvider>>> {
        let table = Arc::new(TableAdapter::new(table));
        match self.schema_provider.register_table(name, table) {
            Ok(Some(p)) => Ok(Some(Arc::new(DfTableProviderAdapter::new(p)))),
            Ok(None) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    fn deregister_table(&self, name: &str) -> DataFusionResult<Option<Arc<dyn DfTableProvider>>> {
        match self.schema_provider.deregister_table(name) {
            Ok(Some(p)) => Ok(Some(Arc::new(DfTableProviderAdapter::new(p)))),
            Ok(None) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    fn table_exist(&self, name: &str) -> bool {
        self.schema_provider.table_exist(name)
    }
}

/// Datafuion SchemaProviderAdapter -> greptime SchemaProviderAdapter
struct SchemaProviderAdapter {
    df_schema_provider: Arc<dyn DfSchemaProvider>,
}

impl SchemaProvider for SchemaProviderAdapter {
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Retrieves the list of available table names in this schema.
    fn table_names(&self) -> Vec<String> {
        self.df_schema_provider.table_names()
    }

    fn table(&self, name: &str) -> Option<Arc<dyn Table>> {
        match self.df_schema_provider.table(name) {
            Some(table_provider) => Some(Arc::new(TableAdapter::new(table_provider))),
            None => None,
        }
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn Table>,
    ) -> Result<Option<Arc<dyn Table>>> {
        let table_provider = Arc::new(DfTableProviderAdapter::new(table));
        match self
            .df_schema_provider
            .register_table(name, table_provider)
            .context(error::DatafusionSnafu)?
        {
            Some(table) => Ok(Some(Arc::new(TableAdapter::new(table)))),
            None => Ok(None),
        }
    }

    fn deregister_table(&self, name: &str) -> Result<Option<Arc<dyn Table>>> {
        match self
            .df_schema_provider
            .deregister_table(name)
            .context(error::DatafusionSnafu)?
        {
            Some(table) => Ok(Some(Arc::new(TableAdapter::new(table)))),
            None => Ok(None),
        }
    }

    fn table_exist(&self, name: &str) -> bool {
        self.df_schema_provider.table_exist(name)
    }
}
