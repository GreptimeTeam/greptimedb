use crate::error::Result;
use crate::plan::{LogicalPlan, PhysicalPlan};
use crate::query_engine::QueryContext;
use std::sync::Arc;

/// Physical query planner that converts a `LogicalPlan` to an
/// `ExecutionPlan` suitable for execution.
#[async_trait::async_trait]
pub trait PhysicalPlanner {
    /// Create a physical plan from a logical plan
    async fn create_physical_plan(
        &self,
        ctx: &mut QueryContext,
        logical_plan: &LogicalPlan,
    ) -> Result<Arc<dyn PhysicalPlan>>;
}
