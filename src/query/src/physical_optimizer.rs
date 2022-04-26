use std::sync::Arc;

use crate::{error::Result, plan::PhysicalPlan, query_engine::QueryContext};

pub trait PhysicalOptimizer {
    fn optimize_physical_plan(
        &self,
        ctx: &mut QueryContext,
        plan: Arc<dyn PhysicalPlan>,
    ) -> Result<Arc<dyn PhysicalPlan>>;
}
