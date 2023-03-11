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
pub mod options;
mod state;

use std::sync::Arc;

use async_trait::async_trait;
use catalog::CatalogListRef;
use common_base::Plugins;
use common_function::scalars::aggregate::AggregateFunctionMetaRef;
use common_function::scalars::{FunctionRef, FUNCTION_REGISTRY};
use common_query::physical_plan::PhysicalPlan;
use common_query::prelude::ScalarUdf;
use common_query::Output;
use datatypes::schema::Schema;
use session::context::QueryContextRef;

use crate::datafusion::DatafusionQueryEngine;
use crate::error::Result;
use crate::parser::QueryStatement;
use crate::plan::LogicalPlan;
pub use crate::query_engine::context::QueryEngineContext;
pub use crate::query_engine::state::QueryEngineState;

#[async_trait]
pub trait QueryEngine: Send + Sync {
    fn name(&self) -> &str;

    async fn statement_to_plan(
        &self,
        stmt: QueryStatement,
        query_ctx: QueryContextRef,
    ) -> Result<LogicalPlan>;

    async fn describe(
        &self,
        stmt: QueryStatement,
        query_ctx: QueryContextRef,
    ) -> Result<(Schema, LogicalPlan)>;

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
        let query_engine = Arc::new(DatafusionQueryEngine::new(catalog_list, Default::default()));
        register_functions(&query_engine);
        Self { query_engine }
    }

    pub fn new_with_plugins(catalog_list: CatalogListRef, plugins: Arc<Plugins>) -> Self {
        let query_engine = Arc::new(DatafusionQueryEngine::new(catalog_list, plugins));
        register_functions(&query_engine);
        Self { query_engine }
    }

    pub fn query_engine(&self) -> QueryEngineRef {
        self.query_engine.clone()
    }
}

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
        let catalog_list = catalog::local::new_memory_catalog_list().unwrap();
        let factory = QueryEngineFactory::new(catalog_list);

        let engine = factory.query_engine();

        assert_eq!("datafusion", engine.name());
    }
}
