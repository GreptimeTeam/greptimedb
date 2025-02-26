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

mod context;
mod default_serializer;
pub mod options;
mod state;
use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use catalog::CatalogManagerRef;
use common_base::Plugins;
use common_function::function::FunctionRef;
use common_function::function_registry::FUNCTION_REGISTRY;
use common_function::handlers::{
    FlowServiceHandlerRef, ProcedureServiceHandlerRef, TableMutationHandlerRef,
};
use common_function::scalars::aggregate::AggregateFunctionMetaRef;
use common_query::prelude::ScalarUdf;
use common_query::Output;
use datafusion_expr::LogicalPlan;
use datatypes::schema::Schema;
pub use default_serializer::{DefaultPlanDecoder, DefaultSerializer};
use session::context::QueryContextRef;
use table::TableRef;

use crate::dataframe::DataFrame;
use crate::datafusion::DatafusionQueryEngine;
use crate::error::Result;
use crate::planner::LogicalPlanner;
pub use crate::query_engine::context::QueryEngineContext;
pub use crate::query_engine::state::QueryEngineState;
use crate::region_query::RegionQueryHandlerRef;

/// Describe statement result
#[derive(Debug)]
pub struct DescribeResult {
    /// The schema of statement
    pub schema: Schema,
    /// The logical plan for statement
    pub logical_plan: LogicalPlan,
}

#[async_trait]
pub trait QueryEngine: Send + Sync {
    /// Returns the query engine as Any
    /// so that it can be downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;

    /// Returns the logical planner
    fn planner(&self) -> Arc<dyn LogicalPlanner>;

    /// Returns the query engine name.
    fn name(&self) -> &str;

    /// Describe the given [`LogicalPlan`].
    async fn describe(
        &self,
        plan: LogicalPlan,
        query_ctx: QueryContextRef,
    ) -> Result<DescribeResult>;

    /// Execute the given [`LogicalPlan`].
    async fn execute(&self, plan: LogicalPlan, query_ctx: QueryContextRef) -> Result<Output>;

    /// Register a [`ScalarUdf`].
    fn register_udf(&self, udf: ScalarUdf);

    /// Register an aggregate function.
    ///
    /// # Panics
    /// Will panic if the function with same name is already registered.
    fn register_aggregate_function(&self, func: AggregateFunctionMetaRef);

    /// Register a SQL function.
    /// Will override if the function with same name is already registered.
    fn register_function(&self, func: FunctionRef);

    /// Create a DataFrame from a table.
    fn read_table(&self, table: TableRef) -> Result<DataFrame>;

    /// Create a [`QueryEngineContext`].
    fn engine_context(&self, query_ctx: QueryContextRef) -> QueryEngineContext;

    /// Retrieve the query engine state [`QueryEngineState`]
    fn engine_state(&self) -> &QueryEngineState;
}

pub struct QueryEngineFactory {
    query_engine: Arc<dyn QueryEngine>,
}

impl QueryEngineFactory {
    pub fn new(
        catalog_manager: CatalogManagerRef,
        region_query_handler: Option<RegionQueryHandlerRef>,
        table_mutation_handler: Option<TableMutationHandlerRef>,
        procedure_service_handler: Option<ProcedureServiceHandlerRef>,
        flow_service_handler: Option<FlowServiceHandlerRef>,
        with_dist_planner: bool,
    ) -> Self {
        Self::new_with_plugins(
            catalog_manager,
            region_query_handler,
            table_mutation_handler,
            procedure_service_handler,
            flow_service_handler,
            with_dist_planner,
            Default::default(),
        )
    }

    pub fn new_with_plugins(
        catalog_manager: CatalogManagerRef,
        region_query_handler: Option<RegionQueryHandlerRef>,
        table_mutation_handler: Option<TableMutationHandlerRef>,
        procedure_service_handler: Option<ProcedureServiceHandlerRef>,
        flow_service_handler: Option<FlowServiceHandlerRef>,
        with_dist_planner: bool,
        plugins: Plugins,
    ) -> Self {
        let state = Arc::new(QueryEngineState::new(
            catalog_manager,
            region_query_handler,
            table_mutation_handler,
            procedure_service_handler,
            flow_service_handler,
            with_dist_planner,
            plugins.clone(),
        ));
        let query_engine = Arc::new(DatafusionQueryEngine::new(state, plugins));
        register_functions(&query_engine);
        Self { query_engine }
    }

    pub fn query_engine(&self) -> QueryEngineRef {
        self.query_engine.clone()
    }
}

/// Register all functions implemented by GreptimeDB
fn register_functions(query_engine: &Arc<DatafusionQueryEngine>) {
    for func in FUNCTION_REGISTRY.functions() {
        query_engine.register_function(func);
    }

    for accumulator in FUNCTION_REGISTRY.aggregate_functions() {
        query_engine.register_aggregate_function(accumulator);
    }
}

pub type QueryEngineRef = Arc<dyn QueryEngine>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_engine_factory() {
        let catalog_list = catalog::memory::new_memory_catalog_manager().unwrap();
        let factory = QueryEngineFactory::new(catalog_list, None, None, None, None, false);

        let engine = factory.query_engine();

        assert_eq!("datafusion", engine.name());
    }
}
