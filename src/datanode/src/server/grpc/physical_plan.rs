mod error;
mod expr;
mod plan;

use std::sync::Arc;

use datafusion::physical_plan::ExecutionPlan;

pub trait AsExcutionPlan {
    type Error: std::error::Error;

    fn try_into_physical_plan(&self) -> std::result::Result<Arc<dyn ExecutionPlan>, Self::Error>;

    fn try_from_physical_plan(
        plan: Arc<dyn ExecutionPlan>,
    ) -> std::result::Result<Self, Self::Error>
    where
        Self: Sized;
}
