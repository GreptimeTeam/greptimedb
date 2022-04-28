use crate::error::Result;
use crate::plan::LogicalPlan;
use crate::query_engine::QueryContext;

pub trait LogicalOptimizer {
    fn optimize_logical_plan(
        &self,
        ctx: &mut QueryContext,
        plan: &LogicalPlan,
    ) -> Result<LogicalPlan>;
}
