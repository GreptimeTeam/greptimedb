// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, RwLock};

use catalog::CatalogListRef;
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_function::scalars::aggregate::AggregateFunctionMetaRef;
use common_query::physical_plan::{SessionContext, TaskContext};
use common_query::prelude::ScalarUdf;
use datafusion::optimizer::common_subexpr_eliminate::CommonSubexprEliminate;
use datafusion::optimizer::eliminate_limit::EliminateLimit;
use datafusion::optimizer::filter_push_down::FilterPushDown;
use datafusion::optimizer::limit_push_down::LimitPushDown;
use datafusion::optimizer::projection_push_down::ProjectionPushDown;
use datafusion::optimizer::single_distinct_to_groupby::SingleDistinctToGroupBy;
use datafusion::optimizer::to_approx_perc::ToApproxPerc;
use datafusion::execution::context::SessionConfig;
use crate::datafusion::DfCatalogListAdapter;
use crate::optimizer::TypeConversionRule;

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
        let config = ExecutionConfig::new()
            .with_default_catalog_and_schema(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME)
            .with_optimizer_rules(vec![
                // TODO(hl): SimplifyExpressions is not exported.
                Arc::new(TypeConversionRule {}),
                // These are the default optimizer in datafusion
                Arc::new(CommonSubexprEliminate::new()),
                Arc::new(EliminateLimit::new()),
                Arc::new(ProjectionPushDown::new()),
                Arc::new(FilterPushDown::new()),
                Arc::new(LimitPushDown::new()),
                Arc::new(SingleDistinctToGroupBy::new()),
                Arc::new(ToApproxPerc::new()),
            ]);

        let df_context = ExecutionContext::with_config(config);

        df_context.state.lock().catalog_list =
            Arc::new(DfCatalogListAdapter::new(catalog_list.clone()));

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
    pub(crate) fn runtime(&self) -> Arc<RuntimeEnv> {
        self.df_context.runtime_env()
    }
}
