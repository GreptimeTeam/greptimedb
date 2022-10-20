mod df_logical;
mod df_physical;
mod error;

use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Buf, Bytes};
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr};

pub use crate::df_logical::DFLogicalSubstraitConvertor;
pub use crate::df_physical::DFExecutionSubstraitConvertor;

pub type ExecutionPlanRef = Arc<dyn ExecutionPlan>;
pub type PhysicalExprRef = Arc<dyn PhysicalExpr>;

#[async_trait]
pub trait SubstraitPlan {
    type Error: std::error::Error;

    type Plan;

    async fn from_buf<B: Buf + Send>(&self, message: B) -> Result<Self::Plan, Self::Error>;

    fn from_plan(&self, plan: Self::Plan) -> Result<Bytes, Self::Error>;
}
