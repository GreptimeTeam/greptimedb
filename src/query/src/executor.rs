use std::sync::Arc;

use common_query::physical_plan::PhysicalPlan;
use common_recordbatch::SendableRecordBatchStream;

use crate::error::Result;
use crate::query_engine::QueryContext;

/// Executor to run [ExecutionPlan].
#[async_trait::async_trait]
pub trait QueryExecutor {
    async fn execute_stream(
        &self,
        ctx: &QueryContext,
        plan: &Arc<dyn PhysicalPlan>,
    ) -> Result<SendableRecordBatchStream>;
}
