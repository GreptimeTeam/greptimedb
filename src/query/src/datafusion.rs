//! Planner, QueryEngine implementations based on DataFusion.

mod catalog_adapter;
mod error;
mod plan_adapter;
mod planner;

use std::sync::Arc;

use common_recordbatch::{EmptyRecordBatchStream, SendableRecordBatchStream};
use common_telemetry::elapsed_timer;
use snafu::{OptionExt, ResultExt};
use sql::{dialect::GenericDialect, parser::ParserContext};

pub use crate::datafusion::catalog_adapter::DfCatalogListAdapter;
use crate::metric;
use crate::query_engine::{QueryContext, QueryEngineState};
use crate::{
    catalog::CatalogListRef,
    datafusion::plan_adapter::PhysicalPlanAdapter,
    datafusion::planner::{DfContextProviderAdapter, DfPlanner},
    error::Result,
    executor::QueryExecutor,
    logical_optimizer::LogicalOptimizer,
    physical_optimizer::PhysicalOptimizer,
    physical_planner::PhysicalPlanner,
    plan::{LogicalPlan, PhysicalPlan},
    planner::Planner,
    Output, QueryEngine,
};

pub(crate) struct DatafusionQueryEngine {
    state: QueryEngineState,
}

impl DatafusionQueryEngine {
    pub fn new(catalog_list: CatalogListRef) -> Self {
        Self {
            state: QueryEngineState::new(catalog_list.clone()),
        }
    }
}

#[async_trait::async_trait]
impl QueryEngine for DatafusionQueryEngine {
    fn name(&self) -> &str {
        "datafusion"
    }

    fn sql_to_plan(&self, sql: &str) -> Result<LogicalPlan> {
        let _timer = elapsed_timer!(metric::METRIC_PARSE_SQL_USEDTIME);
        let context_provider = DfContextProviderAdapter::new(self.state.catalog_list());
        let planner = DfPlanner::new(&context_provider);
        let mut statement = ParserContext::create_with_dialect(sql, &GenericDialect {})
            .context(error::ParseSqlSnafu)?;
        // TODO(dennis): supports multi statement in one sql?
        assert!(1 == statement.len());
        planner.statement_to_plan(statement.remove(0))
    }

    async fn execute(&self, plan: &LogicalPlan) -> Result<Output> {
        let mut ctx = QueryContext::new(self.state.clone());
        let logical_plan = self.optimize_logical_plan(&mut ctx, plan)?;
        let physical_plan = self.create_physical_plan(&mut ctx, &logical_plan).await?;
        let physical_plan = self.optimize_physical_plan(&mut ctx, physical_plan)?;

        Ok(Output::RecordBatch(
            self.execute_stream(&ctx, &physical_plan).await?,
        ))
    }
}

impl LogicalOptimizer for DatafusionQueryEngine {
    fn optimize_logical_plan(
        &self,
        _ctx: &mut QueryContext,
        plan: &LogicalPlan,
    ) -> Result<LogicalPlan> {
        let _timer = elapsed_timer!(metric::METRIC_OPTIMIZE_LOGICAL_USEDTIME);
        match plan {
            LogicalPlan::DfPlan(df_plan) => {
                let optimized_plan =
                    self.state
                        .df_context()
                        .optimize(df_plan)
                        .context(error::DatafusionSnafu {
                            msg: "Fail to optimize logical plan",
                        })?;

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
        let _timer = elapsed_timer!(metric::METRIC_CREATE_PHYSICAL_USEDTIME);
        match logical_plan {
            LogicalPlan::DfPlan(df_plan) => {
                let physical_plan = self
                    .state
                    .df_context()
                    .create_physical_plan(df_plan)
                    .await
                    .context(error::DatafusionSnafu {
                        msg: "Fail to create physical plan",
                    })?;

                Ok(Arc::new(PhysicalPlanAdapter::new(
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
        let _timer = elapsed_timer!(metric::METRIC_OPTIMIZE_PHYSICAL_USEDTIME);
        let config = &self.state.df_context().state.lock().config;
        let optimizers = &config.physical_optimizers;

        let mut new_plan = plan
            .as_any()
            .downcast_ref::<PhysicalPlanAdapter>()
            .context(error::PhysicalPlanDowncastSnafu)?
            .df_plan()
            .clone();

        for optimizer in optimizers {
            new_plan = optimizer
                .optimize(new_plan, config)
                .context(error::DatafusionSnafu {
                    msg: "Fail to optimize physical plan",
                })?;
        }
        Ok(Arc::new(PhysicalPlanAdapter::new(plan.schema(), new_plan)))
    }
}

#[async_trait::async_trait]
impl QueryExecutor for DatafusionQueryEngine {
    async fn execute_stream(
        &self,
        ctx: &QueryContext,
        plan: &Arc<dyn PhysicalPlan>,
    ) -> Result<SendableRecordBatchStream> {
        let _timer = elapsed_timer!(metric::METRIC_EXEC_PLAN_USEDTIME);
        match plan.output_partitioning().partition_count() {
            0 => Ok(Box::pin(EmptyRecordBatchStream::new(plan.schema()))),
            1 => Ok(plan.execute(&ctx.state().runtime(), 0).await?),
            _ => {
                unimplemented!();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::UInt64Array;
    use common_recordbatch::util;
    use datafusion::field_util::FieldExt;
    use datafusion::field_util::SchemaExt;

    use crate::catalog::memory;
    use crate::query_engine::{Output, QueryEngineFactory, QueryEngineRef};

    fn create_test_engine() -> QueryEngineRef {
        let catalog_list = memory::new_memory_catalog_list().unwrap();
        let factory = QueryEngineFactory::new(catalog_list);
        factory.query_engine().clone()
    }

    #[test]
    fn test_sql_to_plan() {
        let engine = create_test_engine();
        let sql = "select sum(number) from numbers limit 20";

        let plan = engine.sql_to_plan(sql).unwrap();

        assert_eq!(
            format!("{:?}", plan),
            r#"DfPlan(Limit: 20
  Projection: #SUM(numbers.number)
    Aggregate: groupBy=[[]], aggr=[[SUM(#numbers.number)]]
      TableScan: numbers projection=None)"#
        );
    }

    #[tokio::test]
    async fn test_execute() {
        let engine = create_test_engine();
        let sql = "select sum(number) from numbers limit 20";

        let plan = engine.sql_to_plan(sql).unwrap();
        let output = engine.execute(&plan).await.unwrap();

        match output {
            Output::RecordBatch(recordbatch) => {
                let numbers = util::collect(recordbatch).await.unwrap();
                assert_eq!(1, numbers.len());
                assert_eq!(numbers[0].df_recordbatch.num_columns(), 1);
                assert_eq!(1, numbers[0].schema.arrow_schema().fields().len());
                assert_eq!(
                    "SUM(numbers.number)",
                    numbers[0].schema.arrow_schema().field(0).name()
                );

                let columns = numbers[0].df_recordbatch.columns();
                assert_eq!(1, columns.len());
                assert_eq!(columns[0].len(), 1);

                assert_eq!(
                    *columns[0].as_any().downcast_ref::<UInt64Array>().unwrap(),
                    UInt64Array::from_slice(&[4950])
                );
            }
            _ => unreachable!(),
        }
    }
}
