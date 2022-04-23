use crate::error::Result;
use crate::plan::LogicalPlan;
use crate::query_engine::QueryContext;

pub trait LogicalOptimizer {
    fn optimize(&self, ctx: &QueryContext, plan: &LogicalPlan) -> Result<LogicalPlan>;
}
