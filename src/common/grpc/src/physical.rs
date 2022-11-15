mod expr;
pub mod plan;

use std::result::Result;
use std::sync::Arc;

use datafusion::physical_plan::ExecutionPlan;

pub type ExecutionPlanRef = Arc<dyn ExecutionPlan>;

pub trait AsExcutionPlan {
    type Error: std::error::Error;

    fn try_into_physical_plan(&self) -> Result<ExecutionPlanRef, Self::Error>;

    fn try_from_physical_plan(plan: ExecutionPlanRef) -> Result<Self, Self::Error>
    where
        Self: Sized;
}
