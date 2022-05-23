mod context;
mod state;

use std::sync::Arc;

use common_query::prelude::ScalarUdf;
use common_recordbatch::SendableRecordBatchStream;

use crate::catalog::CatalogList;
use crate::datafusion::DatafusionQueryEngine;
use crate::error::Result;
use crate::plan::LogicalPlan;
pub use crate::query_engine::context::QueryContext;
pub use crate::query_engine::state::QueryEngineState;

/// Sql output
pub enum Output {
    AffectedRows(usize),
    RecordBatch(SendableRecordBatchStream),
}

#[async_trait::async_trait]
pub trait QueryEngine: Send + Sync {
    fn name(&self) -> &str;

    fn sql_to_plan(&self, sql: &str) -> Result<LogicalPlan>;

    async fn execute(&self, plan: &LogicalPlan) -> Result<Output>;

    fn register_udf(&self, udf: ScalarUdf);
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

pub type QueryEngineRef = Arc<dyn QueryEngine>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::memory;

    #[test]
    fn test_query_engine_factory() {
        let catalog_list = memory::new_memory_catalog_list().unwrap();
        let factory = QueryEngineFactory::new(catalog_list);

        let engine = factory.query_engine();

        assert_eq!("datafusion", engine.name());
    }
}
