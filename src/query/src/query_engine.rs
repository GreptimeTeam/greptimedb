use std::sync::Arc;

use common_recordbatch::SendableRecordBatchStream;

use crate::catalog::CatalogList;
use crate::error::Result;
use crate::plan::LogicalPlan;

mod context;
mod datafusion;
mod state;
pub use context::QueryContext;

use crate::query_engine::datafusion::DatafusionQueryEngine;

#[async_trait::async_trait]
pub trait QueryEngine: Send + Sync {
    fn name(&self) -> &str;
    async fn execute(&self, plan: &LogicalPlan) -> Result<SendableRecordBatchStream>;
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
