use std::fmt;
use std::sync::Arc;

use arrow::datatypes::DataType as ArrowDataType;
use datafusion::physical_plan::functions::{make_scalar_function, Volatility};
use datafusion::prelude::{create_udf, ExecutionConfig, ExecutionContext};

use crate::catalog::{self, CatalogListRef};
use crate::datafusion::DfCatalogListAdapter;
use crate::executor::Runtime;
use crate::function::pow;

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
        let mut df_context = ExecutionContext::with_config(config);

        df_context.state.lock().catalog_list = Arc::new(DfCatalogListAdapter::new(
            df_context.runtime_env(),
            catalog_list.clone(),
        ));

        let pow = make_scalar_function(pow);

        let pow = create_udf(
            "pow",
            // expects two f64
            vec![ArrowDataType::Float64, ArrowDataType::Float64],
            // returns f64
            Arc::new(ArrowDataType::Float64),
            Volatility::Immutable,
            pow,
        );

        df_context.register_udf(pow);

        Self {
            df_context,
            catalog_list,
        }
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
