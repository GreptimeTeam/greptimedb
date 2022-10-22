use std::sync::Arc;

use common_query::physical_plan::PhysicalPlan;
use common_recordbatch::SendableRecordBatchStream;
use datafusion::execution::runtime_env::RuntimeEnv;

use crate::{error::Result, query_engine::QueryContext};

/// Executor to run [ExecutionPlan].
#[async_trait::async_trait]
pub trait QueryExecutor {
    async fn execute_stream(
        &self,
        ctx: &QueryContext,
        plan: &Arc<dyn PhysicalPlan>,
    ) -> Result<SendableRecordBatchStream>;
}

/// Execution runtime environment
#[derive(Clone, Default)]
pub struct Runtime {
    runtime: Arc<RuntimeEnv>,
}

impl From<Arc<RuntimeEnv>> for Runtime {
    fn from(runtime: Arc<RuntimeEnv>) -> Self {
        Runtime { runtime }
    }
}

impl From<Runtime> for Arc<RuntimeEnv> {
    fn from(r: Runtime) -> Arc<RuntimeEnv> {
        r.runtime
    }
}

impl From<&Runtime> for Arc<RuntimeEnv> {
    fn from(r: &Runtime) -> Arc<RuntimeEnv> {
        r.runtime.clone()
    }
}
