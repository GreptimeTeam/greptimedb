use crate::{error::Result, plan::PhysicalPlan, query_engine::QueryContext};
use std::sync::Arc;

pub trait PhysicalOptimizer {
    fn optimize_physical_plan(
        &self,
        ctx: &mut QueryContext,
        plan: Arc<dyn PhysicalPlan>,
    ) -> Result<Arc<dyn PhysicalPlan>>;
}
