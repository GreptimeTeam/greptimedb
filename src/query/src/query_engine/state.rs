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

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use catalog::CatalogListRef;
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_function::scalars::aggregate::AggregateFunctionMetaRef;
use common_query::physical_plan::{SessionContext, TaskContext};
use common_query::prelude::ScalarUdf;
use common_query::Plugins;
use datafusion::catalog::TableReference;
use datafusion::error::Result as DfResult;
use datafusion::execution::context::{QueryPlanner, SessionConfig, SessionState};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::physical_plan::planner::DefaultPhysicalPlanner;
use datafusion::physical_plan::udf::ScalarUDF;
use datafusion::physical_plan::{ExecutionPlan, PhysicalPlanner};
use datafusion_common::ScalarValue;
use datafusion_expr::{LogicalPlan as DfLogicalPlan, TableSource};
use datafusion_optimizer::optimizer::Optimizer;
use datafusion_sql::planner::ContextProvider;
use datatypes::arrow::datatypes::DataType;
use promql::extension_plan::PromExtensionPlanner;
use session::context::QueryContextRef;

use crate::datafusion::DfCatalogListAdapter;
use crate::error::Error;
use crate::interceptor::{SqlQueryInterceptor, SqlQueryInterceptorRef};
use crate::optimizer::TypeConversionRule;

/// Query engine global state
// TODO(yingwen): This QueryEngineState still relies on datafusion, maybe we can define a trait for it,
// which allows different implementation use different engine state. The state can also be an associated
// type in QueryEngine trait.
#[derive(Clone)]
pub struct QueryEngineState {
    df_context: SessionContext,
    catalog_list: CatalogListRef,
    aggregate_functions: Arc<RwLock<HashMap<String, AggregateFunctionMetaRef>>>,
    plugins: Arc<Plugins>,
}

impl fmt::Debug for QueryEngineState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // TODO(dennis) better debug info
        write!(f, "QueryEngineState: <datafusion context>")
    }
}

impl QueryEngineState {
    pub fn new(catalog_list: CatalogListRef, plugins: Arc<Plugins>) -> Self {
        let runtime_env = Arc::new(RuntimeEnv::default());
        let session_config = SessionConfig::new()
            .with_default_catalog_and_schema(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME);
        let mut optimizer = Optimizer::new();
        // Apply the type conversion rule first.
        optimizer.rules.insert(0, Arc::new(TypeConversionRule {}));

        let mut session_state = SessionState::with_config_rt(session_config, runtime_env);
        session_state.optimizer = optimizer;
        session_state.catalog_list = Arc::new(DfCatalogListAdapter::new(catalog_list.clone()));
        session_state.query_planner = Arc::new(DfQueryPlanner::new());

        let df_context = SessionContext::with_state(session_state);

        Self {
            df_context,
            catalog_list,
            aggregate_functions: Arc::new(RwLock::new(HashMap::new())),
            plugins,
        }
    }

    /// Register a udf function
    // TODO(dennis): manage UDFs by ourself.
    pub fn register_udf(&self, udf: ScalarUdf) {
        self.df_context.register_udf(udf.into_df_udf());
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
    pub(crate) fn task_ctx(&self) -> Arc<TaskContext> {
        self.df_context.task_ctx()
    }

    pub(crate) fn get_table_provider(
        &self,
        query_ctx: QueryContextRef,
        name: TableReference,
    ) -> DfResult<Arc<dyn TableSource>> {
        let state = self.df_context.state();

        let query_interceptor = self.plugins.get::<SqlQueryInterceptorRef<Error>>();
        query_interceptor.validate_table_reference(name, query_ctx.clone())?;

        if let TableReference::Bare { table } = name {
            let name = TableReference::Partial {
                schema: &query_ctx.current_schema(),
                table,
            };
            state.get_table_provider(name)
        } else {
            state.get_table_provider(name)
        }
    }

    pub(crate) fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        self.df_context.state().get_function_meta(name)
    }

    pub(crate) fn get_variable_type(&self, variable_names: &[String]) -> Option<DataType> {
        self.df_context.state().get_variable_type(variable_names)
    }

    pub(crate) fn get_config_option(&self, variable: &str) -> Option<ScalarValue> {
        self.df_context.state().get_config_option(variable)
    }

    pub(crate) fn optimize(&self, plan: &DfLogicalPlan) -> DfResult<DfLogicalPlan> {
        self.df_context.optimize(plan)
    }

    pub(crate) async fn create_physical_plan(
        &self,
        logical_plan: &DfLogicalPlan,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        self.df_context.create_physical_plan(logical_plan).await
    }

    pub(crate) fn optimize_physical_plan(
        &self,
        mut plan: Arc<dyn ExecutionPlan>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let state = self.df_context.state();
        let config = &state.config;
        for optimizer in &state.physical_optimizers {
            plan = optimizer.optimize(plan, config)?;
        }

        Ok(plan)
    }
}

struct DfQueryPlanner {
    physical_planner: DefaultPhysicalPlanner,
}

#[async_trait]
impl QueryPlanner for DfQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &DfLogicalPlan,
        session_state: &SessionState,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        self.physical_planner
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}

impl DfQueryPlanner {
    fn new() -> Self {
        Self {
            physical_planner: DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(
                PromExtensionPlanner {},
            )]),
        }
    }
}
