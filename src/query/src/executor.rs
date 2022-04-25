use crate::{error::Result, plan::PhysicalPlan, query_engine::QueryContext};
use std::sync::Arc;

/// Executor to run [ExecutionPlan].
#[async_trait::async_trait]
pub trait QueryExecutor {
    async fn execute_stream(&self, ctx: &QueryContext, plan: &Arc<dyn PhysicalPlan>) -> Result<()>;
}
