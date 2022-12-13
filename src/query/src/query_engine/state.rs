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
use std::sync::{Arc, Mutex, RwLock};

use catalog::CatalogListRef;
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_function::scalars::aggregate::AggregateFunctionMetaRef;
use common_query::physical_plan::{SessionContext, TaskContext};
use common_query::prelude::ScalarUdf;
use datafusion::catalog::TableReference;
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DfResult;
use datafusion::execution::context::{SessionConfig, SessionState};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::physical_plan::udaf::AggregateUDF;
use datafusion::physical_plan::udf::ScalarUDF;
use datafusion_expr::TableSource;
use datafusion_optimizer::optimizer::{Optimizer, OptimizerConfig};
use datatypes::arrow::datatypes::DataType;

use crate::datafusion::DfCatalogListAdapter;
use crate::optimizer::TypeConversionRule;

/// Query engine global state
// TODO(yingwen): This QueryEngineState still relies on datafusion, maybe we can define a trait for it,
// which allows different implementation use different engine state. The state can also be an associated
// type in QueryEngine trait.
#[derive(Clone)]
pub struct QueryEngineState {
    // TODO(yingwen): Remove this mutex.
    df_context: Arc<Mutex<SessionContext>>,
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
        let runtime_env = Arc::new(RuntimeEnv::default());
        let session_config = SessionConfig::new()
            .with_default_catalog_and_schema(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME);
        let mut optimizer = Optimizer::new(&OptimizerConfig::new());
        // Apply the type conversion rule first.
        optimizer.rules.insert(0, Arc::new(TypeConversionRule {}));

        let mut session_state = SessionState::with_config_rt(session_config, runtime_env);
        session_state.optimizer = optimizer;
        session_state.catalog_list = Arc::new(DfCatalogListAdapter::new(catalog_list.clone()));

        let df_context = Arc::new(Mutex::new(SessionContext::with_state(session_state)));

        // let config = ExecutionConfig::new()
        //     .with_default_catalog_and_schema(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME)
        //     .with_optimizer_rules(vec![
        //         // TODO(hl): SimplifyExpressions is not exported.
        //         Arc::new(TypeConversionRule {}),
        //         // These are the default optimizer in datafusion
        //         Arc::new(CommonSubexprEliminate::new()),
        //         Arc::new(EliminateLimit::new()),
        //         Arc::new(ProjectionPushDown::new()),
        //         Arc::new(FilterPushDown::new()),
        //         Arc::new(LimitPushDown::new()),
        //         Arc::new(SingleDistinctToGroupBy::new()),
        //         Arc::new(ToApproxPerc::new()),
        //     ]);

        // let df_context = ExecutionContext::with_config(config);

        // df_context.state.lock().catalog_list =
        //     Arc::new(DfCatalogListAdapter::new(catalog_list.clone()));

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
            .lock()
            .unwrap()
            .register_udf(udf.into_df_udf());
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

    // #[inline]
    // pub(crate) fn df_context(&self) -> &SessionContext {
    //     &self.df_context
    // }

    #[inline]
    pub(crate) fn task_ctx(&self) -> Arc<TaskContext> {
        self.df_context.lock().unwrap().task_ctx()
    }

    pub(crate) fn get_table_provider(
        &self,
        schema: Option<&str>,
        name: TableReference,
    ) -> DfResult<Arc<dyn TableSource>> {
        let df_context = self.df_context.lock().unwrap();
        match name {
            TableReference::Bare { table } if schema.is_some() => {
                df_context.get_table_provider(TableReference::Partial {
                    // unwrap safety: checked in this match's arm
                    schema: &schema.unwrap(),
                    table,
                })
            }
            _ => df_context.get_table_provider(name),
        }
    }

    pub(crate) fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        self.df_context.lock().unwrap().get_function_meta(name)
    }

    pub(crate) fn get_variable_type(&self, variable_names: &[String]) -> Option<DataType> {
        self.df_context
            .lock()
            .unwrap()
            .get_variable_type(variable_names)
    }
}
