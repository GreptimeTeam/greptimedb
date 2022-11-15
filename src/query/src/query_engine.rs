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

mod context;
mod state;

use std::sync::Arc;

use catalog::CatalogListRef;
use common_function::scalars::aggregate::AggregateFunctionMetaRef;
use common_function::scalars::{FunctionRef, FUNCTION_REGISTRY};
use common_query::physical_plan::PhysicalPlan;
use common_query::prelude::ScalarUdf;
use common_query::Output;
use sql::statements::statement::Statement;

use crate::datafusion::DatafusionQueryEngine;
use crate::error::Result;
use crate::plan::LogicalPlan;
pub use crate::query_engine::context::QueryContext;
pub use crate::query_engine::state::QueryEngineState;

#[async_trait::async_trait]
pub trait QueryEngine: Send + Sync {
    fn name(&self) -> &str;

    fn sql_to_statement(&self, sql: &str) -> Result<Statement>;

    fn statement_to_plan(&self, stmt: Statement) -> Result<LogicalPlan>;

    fn sql_to_plan(&self, sql: &str) -> Result<LogicalPlan>;

    async fn execute(&self, plan: &LogicalPlan) -> Result<Output>;

    async fn execute_physical(&self, plan: &Arc<dyn PhysicalPlan>) -> Result<Output>;

    fn register_udf(&self, udf: ScalarUdf);

    fn register_aggregate_function(&self, func: AggregateFunctionMetaRef);

    fn register_function(&self, func: FunctionRef);
}

pub struct QueryEngineFactory {
    query_engine: Arc<dyn QueryEngine>,
}

impl QueryEngineFactory {
    pub fn new(catalog_list: CatalogListRef) -> Self {
        let query_engine = Arc::new(DatafusionQueryEngine::new(catalog_list));

        for func in FUNCTION_REGISTRY.functions() {
            query_engine.register_function(func);
        }

        for accumulator in FUNCTION_REGISTRY.aggregate_functions() {
            query_engine.register_aggregate_function(accumulator);
        }

        Self { query_engine }
    }
}

impl QueryEngineFactory {
    pub fn query_engine(&self) -> QueryEngineRef {
        self.query_engine.clone()
    }
}

pub type QueryEngineRef = Arc<dyn QueryEngine>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_engine_factory() {
        let catalog_list = catalog::local::new_memory_catalog_list().unwrap();
        let factory = QueryEngineFactory::new(catalog_list);

        let engine = factory.query_engine();

        assert_eq!("datafusion", engine.name());
    }
}
