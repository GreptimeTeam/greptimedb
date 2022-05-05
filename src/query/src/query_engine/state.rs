use std::any::Any;
use std::fmt;
use std::sync::Arc;

use datafusion::catalog::{
    catalog::{CatalogList as DfCatalogList, CatalogProvider as DfCatalogProvider},
    schema::SchemaProvider as DfSchemaProvider,
};
use datafusion::datasource::TableProvider as DfTableProvider;
use datafusion::error::Result as DataFusionResult;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::prelude::{ExecutionConfig, ExecutionContext};
use snafu::ResultExt;
use table::{
    table::adapter::{DfTableProviderAdapter, TableAdapter},
    Table,
};

use crate::catalog::{schema::SchemaProvider, CatalogList, CatalogProvider};
use crate::error::{self, Result};
use crate::executor::Runtime;

const DEFAULT_CATALOG_NAME: &str = "greptime";
const DEFAULT_SCHEMA_NAME: &str = "public";

/// Query engine global state
#[derive(Clone)]
pub struct QueryEngineState {
    df_context: ExecutionContext,
    catalog_list: Arc<dyn CatalogList>,
}

impl fmt::Debug for QueryEngineState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // TODO(dennis) better debug info
        write!(f, "QueryEngineState: <datafusion context>")
    }
}

impl QueryEngineState {
    pub(crate) fn new(catalog_list: Arc<dyn CatalogList>) -> Self {
        let config = ExecutionConfig::new()
            .with_default_catalog_and_schema(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME);
        let df_context = ExecutionContext::with_config(config);

        df_context.state.lock().catalog_list = Arc::new(DfCatalogListAdapter {
            catalog_list: catalog_list.clone(),
            runtime: df_context.runtime_env(),
        });

        Self {
            df_context,
            catalog_list,
        }
    }

    #[inline]
    pub(crate) fn df_context(&self) -> &ExecutionContext {
        &self.df_context
    }

    #[inline]
    pub(crate) fn runtime(&self) -> Runtime {
        self.df_context.runtime_env().into()
    }

    #[allow(dead_code)]
    pub(crate) fn schema(&self, schema_name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.catalog_list
            .catalog(DEFAULT_CATALOG_NAME)
            .and_then(|c| c.schema(schema_name))
    }
}

/// Adapters between datafusion and greptime query engine.
struct DfCatalogListAdapter {
    runtime: Arc<RuntimeEnv>,
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
            runtime: self.runtime.clone(),
        });
        match self.catalog_list.register_catalog(name, catalog_adapter) {
            Some(catalog_provider) => Some(Arc::new(DfCatalogProviderAdapter {
                catalog_provider,
                runtime: self.runtime.clone(),
            })),
            None => None,
        }
    }

    fn catalog_names(&self) -> Vec<String> {
        self.catalog_list.catalog_names()
    }

    fn catalog(&self, name: &str) -> Option<Arc<dyn DfCatalogProvider>> {
        match self.catalog_list.catalog(name) {
            Some(catalog_provider) => Some(Arc::new(DfCatalogProviderAdapter {
                catalog_provider,
                runtime: self.runtime.clone(),
            })),
            None => None,
        }
    }
}

/// Datafusion's CatalogProvider ->  greptime CatalogProvider
struct CatalogProviderAdapter {
    df_cataglog_provider: Arc<dyn DfCatalogProvider>,
    runtime: Arc<RuntimeEnv>,
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
            Some(df_schema_provider) => Some(Arc::new(SchemaProviderAdapter {
                df_schema_provider,
                runtime: self.runtime.clone(),
            })),
            None => None,
        }
    }
}

///Greptime CatalogProvider -> datafusion's CatalogProvider
struct DfCatalogProviderAdapter {
    catalog_provider: Arc<dyn CatalogProvider>,
    runtime: Arc<RuntimeEnv>,
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
            Some(schema_provider) => Some(Arc::new(DfSchemaProviderAdapter {
                schema_provider,
                runtime: self.runtime.clone(),
            })),
            None => None,
        }
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
        let table = Arc::new(TableAdapter::new(table, self.runtime.clone()));
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

    fn table(&self, name: &str) -> Option<Arc<dyn Table>> {
        match self.df_schema_provider.table(name) {
            Some(table_provider) => Some(Arc::new(TableAdapter::new(
                table_provider,
                self.runtime.clone(),
            ))),
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
            Some(table) => Ok(Some(Arc::new(TableAdapter::new(
                table,
                self.runtime.clone(),
            )))),
            None => Ok(None),
        }
    }

    fn deregister_table(&self, name: &str) -> Result<Option<Arc<dyn Table>>> {
        match self
            .df_schema_provider
            .deregister_table(name)
            .context(error::DatafusionSnafu)?
        {
            Some(table) => Ok(Some(Arc::new(TableAdapter::new(
                table,
                self.runtime.clone(),
            )))),
            None => Ok(None),
        }
    }

    fn table_exist(&self, name: &str) -> bool {
        self.df_schema_provider.table_exist(name)
    }
}
