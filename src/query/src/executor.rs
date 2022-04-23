use crate::{error::Result, plan::ExecutionPlan, query_engine::QueryContext};

/// Executor to run [ExecutionPlan].
#[async_trait::async_trait]
pub trait QueryExecutor {
    async fn execute_stream(&self, ctx: &QueryContext, plan: &ExecutionPlan) -> Result<()>;
}
