use crate::catalog::CatalogList;
use crate::error::Result;
use crate::plan::LogicalPlan;
use std::sync::Arc;

mod context;
mod datafusion;
mod state;
use self::datafusion::DatafusionQueryEngine;
pub use context::QueryContext;

#[async_trait::async_trait]
pub trait QueryEngine {
    fn name(&self) -> &str;
    async fn execute(&self, plan: &LogicalPlan) -> Result<()>;
}

pub struct QueryEngineFactory {
    query_engine: Arc<dyn QueryEngine>,
}

impl QueryEngineFactory {
    pub fn new(catalog_list: Arc<dyn CatalogList>) -> Self {
        Self {
            query_engine: Arc::new(DatafusionQueryEngine::new(catalog_list)),
        }
    }
}

impl QueryEngineFactory {
    pub fn query_engine(&self) -> &Arc<dyn QueryEngine> {
        &self.query_engine
    }
}
