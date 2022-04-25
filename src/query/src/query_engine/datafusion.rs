use self::plan::DfPhysicalPlan;
use super::{context::QueryContext, state::QueryEngineState};
use crate::{
    catalog::CatalogList,
    error::{self, Result},
    executor::QueryExecutor,
    logical_optimizer::LogicalOptimizer,
    physical_optimizer::PhysicalOptimizer,
    physical_planner::PhysicalPlanner,
    plan::{LogicalPlan, PhysicalPlan},
    query_engine::QueryEngine,
};
use snafu::{OptionExt, ResultExt};
use std::sync::Arc;
mod plan;

pub(crate) struct DatafusionQueryEngine {
    state: QueryEngineState,
}

impl DatafusionQueryEngine {
    pub fn new(catalog_list: Arc<dyn CatalogList>) -> Self {
        Self {
            state: QueryEngineState::new(catalog_list),
        }
    }
}

#[async_trait::async_trait]
impl QueryEngine for DatafusionQueryEngine {
    fn name(&self) -> &str {
        "datafusion"
    }
    async fn execute(&self, plan: &LogicalPlan) -> Result<()> {
        let mut ctx = QueryContext::default();
        let logical_plan = self.optimize_logical_plan(&mut ctx, plan)?;
        let physical_plan = self.create_physical_plan(&mut ctx, &logical_plan).await?;
        let physical_plan = self.optimize_physical_plan(&mut ctx, physical_plan)?;

        Ok(self.execute_stream(&ctx, &physical_plan).await?)
    }
}

impl LogicalOptimizer for DatafusionQueryEngine {
    fn optimize_logical_plan(
        &self,
        _ctx: &mut QueryContext,
        plan: &LogicalPlan,
    ) -> Result<LogicalPlan> {
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
        _ctx: &mut QueryContext,
        logical_plan: &LogicalPlan,
    ) -> Result<Arc<dyn PhysicalPlan>> {
        match logical_plan {
            LogicalPlan::DfPlan(df_plan) => {
                let physical_plan = self
                    .state
                    .df_context()
                    .create_physical_plan(df_plan)
                    .await
                    .context(error::DatafusionSnafu)?;

                Ok(Arc::new(DfPhysicalPlan::new(
                    Arc::new(physical_plan.schema().into()),
                    physical_plan,
                )))
            }
        }
    }
}

impl PhysicalOptimizer for DatafusionQueryEngine {
    fn optimize_physical_plan(
        &self,
        _ctx: &mut QueryContext,
        plan: Arc<dyn PhysicalPlan>,
    ) -> Result<Arc<dyn PhysicalPlan>> {
        let config = &self.state.df_context().state.lock().config;
        let optimizers = &config.physical_optimizers;

        let mut new_plan = plan
            .as_any()
            .downcast_ref::<DfPhysicalPlan>()
            .context(error::PhysicalPlanDowncastSnafu)?
            .df_plan()
            .clone();

        for optimizer in optimizers {
            new_plan = optimizer
                .optimize(new_plan, config)
                .context(error::DatafusionSnafu)?;
        }
        Ok(Arc::new(DfPhysicalPlan::new(plan.schema(), new_plan)))
    }
}

#[async_trait::async_trait]
impl QueryExecutor for DatafusionQueryEngine {
    async fn execute_stream(
        &self,
        _ctx: &QueryContext,
        _plan: &Arc<dyn PhysicalPlan>,
    ) -> Result<()> {
        let _runtime = self.state.df_context().runtime_env();
        Ok(())
    }
}
