use crate::{
    error::{self, Result},
    logical_optimizer::LogicalOptimizer,
    physical_planner::PhysicalPlanner,
    plan::{ExecutionPlan, LogicalPlan},
    query_engine::QueryEngine,
};
use snafu::prelude::*;

use super::{context::QueryContext, state::QueryEngineState};
use std::sync::Arc;

pub(crate) struct DatafusionQueryEngine {
    state: QueryEngineState,
}

impl DatafusionQueryEngine {
    pub fn new() -> Self {
        Self {
            state: QueryEngineState::new(),
        }
    }
}

#[async_trait::async_trait]
impl QueryEngine for DatafusionQueryEngine {
    fn name(&self) -> &str {
        "datafusion"
    }
    async fn execute(&self, plan: &LogicalPlan) -> Result<()> {
        Ok(())
    }
}

impl LogicalOptimizer for DatafusionQueryEngine {
    fn optimize(&self, _ctx: &QueryContext, plan: &LogicalPlan) -> Result<LogicalPlan> {
        match plan {
            LogicalPlan::DfPlan(df_plan) => {
                let optimized_plan = self
                    .state
                    .df_context()
                    .optimize(df_plan)
                    .context(error::DatafusionSnafu)?;

                Ok(LogicalPlan::DfPlan(optimized_plan))
            }
        }
    }
}

#[async_trait::async_trait]
impl PhysicalPlanner for DatafusionQueryEngine {
    async fn create_physical_plan(
        &self,
        _ctx: &QueryContext,
        logical_plan: &LogicalPlan,
    ) -> Result<Arc<ExecutionPlan>> {
        match logical_plan {
            LogicalPlan::DfPlan(df_plan) => {
                let physical_plan = self
                    .state
                    .df_context()
                    .create_physical_plan(df_plan)
                    .await
                    .context(error::DatafusionSnafu)?;

                Ok(Arc::new(ExecutionPlan::DfPlan(physical_plan)))
            }
        }
    }
}
