use std::fmt;
use std::sync::Arc;

use common_query::prelude::ScalarUDF;
use datafusion::prelude::{ExecutionConfig, ExecutionContext};

use crate::catalog::{self, CatalogListRef};
use crate::datafusion::DfCatalogListAdapter;
use crate::executor::Runtime;

/// Query engine global state
// TODO(yingwen): This QueryEngineState still relies on datafusion, maybe we can define a trait for it,
// which allows different implementation use different engine state. The state can also be an associated
// type in QueryEngine trait.
#[derive(Clone)]
pub struct QueryEngineState {
    df_context: ExecutionContext,
    catalog_list: CatalogListRef,
}

impl fmt::Debug for QueryEngineState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // TODO(dennis) better debug info
        write!(f, "QueryEngineState: <datafusion context>")
    }
}

impl QueryEngineState {
    pub(crate) fn new(catalog_list: CatalogListRef) -> Self {
        let config = ExecutionConfig::new().with_default_catalog_and_schema(
            catalog::DEFAULT_CATALOG_NAME,
            catalog::DEFAULT_SCHEMA_NAME,
        );
        let df_context = ExecutionContext::with_config(config);

        df_context.state.lock().catalog_list = Arc::new(DfCatalogListAdapter::new(
            df_context.runtime_env(),
            catalog_list.clone(),
        ));

        Self {
            df_context,
            catalog_list,
        }
    }

    /// Register a udf function
    /// TODO(dennis): manage UDFs by ourself.
    pub fn register_udf(&self, udf: ScalarUDF) {
        self.df_context
            .state
            .lock()
            .scalar_functions
            .insert(udf.name.clone(), Arc::new(udf.into_df_udf()));
    }

    #[inline]
    pub fn catalog_list(&self) -> &CatalogListRef {
        &self.catalog_list
    }

    #[inline]
    pub(crate) fn df_context(&self) -> &ExecutionContext {
        &self.df_context
    }

    #[inline]
    pub(crate) fn runtime(&self) -> Runtime {
        self.df_context.runtime_env().into()
    }
}
