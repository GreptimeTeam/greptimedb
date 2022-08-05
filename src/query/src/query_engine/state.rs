use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, RwLock};

use catalog::CatalogListRef;
use common_function::scalars::aggregate::AggregateFunctionMetaRef;
use common_query::prelude::ScalarUdf;
use datafusion::prelude::{ExecutionConfig, ExecutionContext};

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
    aggregate_functions: Arc<RwLock<HashMap<String, AggregateFunctionMetaRef>>>,
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
            aggregate_functions: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Register a udf function
    /// TODO(dennis): manage UDFs by ourself.
    pub fn register_udf(&self, udf: ScalarUdf) {
        self.df_context
            .state
            .lock()
            .scalar_functions
            .insert(udf.name.clone(), Arc::new(udf.into_df_udf()));
    }

    pub fn aggregate_function(&self, function_name: &str) -> Option<AggregateFunctionMetaRef> {
        self.aggregate_functions
            .read()
            .unwrap()
            .get(function_name)
            .cloned()
    }

    pub fn register_aggregate_function(&self, func: AggregateFunctionMetaRef) {
        // TODO(LFC): Return some error if there exists an aggregate function with the same name.
        // Simply overwrite the old value for now.
        self.aggregate_functions
            .write()
            .unwrap()
            .insert(func.name(), func);
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
