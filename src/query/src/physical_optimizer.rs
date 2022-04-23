use crate::{error::Result, plan::ExecutionPlan, query_engine::QueryContext};

pub trait PhysicalOptimizer {
    fn optimize_physical_plan(
        &self,
        ctx: &mut QueryContext,
        plan: ExecutionPlan,
    ) -> Result<ExecutionPlan>;
}
