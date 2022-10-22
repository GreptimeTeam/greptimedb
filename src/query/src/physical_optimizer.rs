use std::sync::Arc;

use common_query::physical_plan::PhysicalPlan;

use crate::{error::Result, query_engine::QueryContext};

pub trait PhysicalOptimizer {
    fn optimize_physical_plan(
        &self,
        ctx: &mut QueryContext,
        plan: Arc<dyn PhysicalPlan>,
    ) -> Result<Arc<dyn PhysicalPlan>>;
}
