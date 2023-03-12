// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Planner, QueryEngine implementations based on DataFusion.

mod catalog_adapter;
mod error;
mod planner;

use std::sync::Arc;

use async_trait::async_trait;
use catalog::table_source::DfTableSourceProvider;
use catalog::CatalogListRef;
use common_base::Plugins;
use common_error::prelude::BoxedError;
use common_function::scalars::aggregate::AggregateFunctionMetaRef;
use common_function::scalars::udf::create_udf;
use common_function::scalars::FunctionRef;
use common_query::physical_plan::{DfPhysicalPlanAdapter, PhysicalPlan, PhysicalPlanAdapter};
use common_query::prelude::ScalarUdf;
use common_query::Output;
use common_recordbatch::adapter::RecordBatchStreamAdapter;
use common_recordbatch::{EmptyRecordBatchStream, SendableRecordBatchStream};
use common_telemetry::timer;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::ScalarValue;
use datafusion_sql::planner::{ParserOptions, SqlToRel};
use datatypes::schema::Schema;
use promql::planner::PromPlanner;
use promql_parser::parser::EvalStmt;
use session::context::QueryContextRef;
use snafu::{OptionExt, ResultExt};
use sql::statements::statement::Statement;

pub use crate::datafusion::catalog_adapter::DfCatalogListAdapter;
pub use crate::datafusion::planner::DfContextProviderAdapter;
use crate::error::{
    DataFusionSnafu, PlanSqlSnafu, QueryExecutionSnafu, QueryPlanSnafu, Result, SqlSnafu,
};
use crate::executor::QueryExecutor;
use crate::logical_optimizer::LogicalOptimizer;
use crate::parser::QueryStatement;
use crate::physical_optimizer::PhysicalOptimizer;
use crate::physical_planner::PhysicalPlanner;
use crate::plan::LogicalPlan;
use crate::query_engine::{QueryEngineContext, QueryEngineState};
use crate::{metric, QueryEngine};

pub struct DatafusionQueryEngine {
    state: QueryEngineState,
}

impl DatafusionQueryEngine {
    pub fn new(catalog_list: CatalogListRef, plugins: Arc<Plugins>) -> Self {
        Self {
            state: QueryEngineState::new(catalog_list.clone(), plugins),
        }
    }

    async fn plan_sql_stmt(
        &self,
        stmt: Statement,
        query_ctx: QueryContextRef,
    ) -> Result<LogicalPlan> {
        let session_state = self.state.session_state();

        let df_stmt = (&stmt).try_into().context(SqlSnafu)?;

        let config_options = session_state.config().config_options();
        let parser_options = ParserOptions {
            enable_ident_normalization: config_options.sql_parser.enable_ident_normalization,
            parse_float_as_decimal: config_options.sql_parser.parse_float_as_decimal,
        };

        let context_provider = DfContextProviderAdapter::try_new(
            self.state.clone(),
            session_state,
            &df_stmt,
            query_ctx,
        )
        .await?;
        let sql_to_rel = SqlToRel::new_with_options(&context_provider, parser_options);

        let mut result = sql_to_rel.statement_to_plan(df_stmt).with_context(|_| {
            let sql = if let Statement::Query(query) = stmt.clone() {
                query.inner.to_string()
            } else {
                format!("{stmt:?}")
            };
            PlanSqlSnafu { sql }
        })?;

        if let Statement::Query(query) = stmt {
            let types = query.param_types;
            let values = query.param_values;
            let mut vals: Vec<ScalarValue> = vec![];

            assert_eq!(types.len(), values.len());

            if !types.is_empty() {
                for i in 0..types.len() {
                    // SAFETY: checked the index before
                    let t = types.get(i).unwrap();
                    let v = values.get(i).unwrap();
                    let v = v.try_to_scalar_value(t);
                    // TODO(SSebo): fixme
                    let v = v.unwrap();
                    vals.push(v);
                }

                // TODO(SSebo): fixme
                let r = result.replace_params_with_values(&vals);
                result = r.unwrap();
            }
        }

        Ok(LogicalPlan::DfPlan(result))
    }

    // TODO(ruihang): test this method once parser is ready.
    async fn plan_promql_stmt(
        &self,
        stmt: EvalStmt,
        query_ctx: QueryContextRef,
    ) -> Result<LogicalPlan> {
        let table_provider = DfTableSourceProvider::new(
            self.state.catalog_list().clone(),
            self.state.disallow_cross_schema_query(),
            query_ctx.as_ref(),
        );
        PromPlanner::stmt_to_plan(table_provider, stmt)
            .await
            .map(LogicalPlan::DfPlan)
            .map_err(BoxedError::new)
            .context(QueryPlanSnafu)
    }
}

// TODO(LFC): Refactor consideration: extract a "Planner" that stores query context and execute queries inside.
#[async_trait]
impl QueryEngine for DatafusionQueryEngine {
    fn name(&self) -> &str {
        "datafusion"
    }

    async fn statement_to_plan(
        &self,
        stmt: QueryStatement,
        query_ctx: QueryContextRef,
    ) -> Result<LogicalPlan> {
        match stmt {
            QueryStatement::Sql(stmt) => self.plan_sql_stmt(stmt, query_ctx).await,
            QueryStatement::Promql(stmt) => self.plan_promql_stmt(stmt, query_ctx).await,
        }
    }

    async fn describe(
        &self,
        stmt: QueryStatement,
        query_ctx: QueryContextRef,
    ) -> Result<(Schema, LogicalPlan)> {
        // TODO(sunng87): consider cache optmised logical plan between describe
        // and execute
        let plan = self.statement_to_plan(stmt, query_ctx).await?;
        let optimised_plan = self.optimize(&plan)?;
        let schema = optimised_plan.schema()?;
        Ok((schema, optimised_plan))
    }

    async fn execute(&self, plan: &LogicalPlan) -> Result<Output> {
        let logical_plan = self.optimize(plan)?;

        let mut ctx = QueryEngineContext::new(self.state.session_state());
        let physical_plan = self.create_physical_plan(&mut ctx, &logical_plan).await?;
        let physical_plan = self.optimize_physical_plan(&mut ctx, physical_plan)?;

        Ok(Output::Stream(self.execute_stream(&ctx, &physical_plan)?))
    }

    async fn execute_physical(&self, plan: &Arc<dyn PhysicalPlan>) -> Result<Output> {
        let ctx = QueryEngineContext::new(self.state.session_state());
        Ok(Output::Stream(self.execute_stream(&ctx, plan)?))
    }

    fn register_udf(&self, udf: ScalarUdf) {
        self.state.register_udf(udf);
    }

    /// Note in SQL queries, aggregate names are looked up using
    /// lowercase unless the query uses quotes. For example,
    ///
    /// `SELECT MY_UDAF(x)...` will look for an aggregate named `"my_udaf"`
    /// `SELECT "my_UDAF"(x)` will look for an aggregate named `"my_UDAF"`
    ///
    /// So it's better to make UDAF name lowercase when creating one.
    fn register_aggregate_function(&self, func: AggregateFunctionMetaRef) {
        self.state.register_aggregate_function(func);
    }

    fn register_function(&self, func: FunctionRef) {
        self.state.register_udf(create_udf(func));
    }
}

impl LogicalOptimizer for DatafusionQueryEngine {
    fn optimize(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        let _timer = timer!(metric::METRIC_OPTIMIZE_LOGICAL_ELAPSED);
        match plan {
            LogicalPlan::DfPlan(df_plan) => {
                let optimized_plan = self
                    .state
                    .session_state()
                    .optimize(df_plan)
                    .context(error::DatafusionSnafu {
                        msg: "Fail to optimize logical plan",
                    })
                    .map_err(BoxedError::new)
                    .context(QueryExecutionSnafu)?;

                Ok(LogicalPlan::DfPlan(optimized_plan))
            }
        }
    }
}

#[async_trait::async_trait]
impl PhysicalPlanner for DatafusionQueryEngine {
    async fn create_physical_plan(
        &self,
        ctx: &mut QueryEngineContext,
        logical_plan: &LogicalPlan,
    ) -> Result<Arc<dyn PhysicalPlan>> {
        let _timer = timer!(metric::METRIC_CREATE_PHYSICAL_ELAPSED);
        match logical_plan {
            LogicalPlan::DfPlan(df_plan) => {
                let state = ctx.state();
                let physical_plan = state
                    .create_physical_plan(df_plan)
                    .await
                    .context(error::DatafusionSnafu {
                        msg: "Fail to create physical plan",
                    })
                    .map_err(BoxedError::new)
                    .context(QueryExecutionSnafu)?;

                Ok(Arc::new(PhysicalPlanAdapter::new(
                    Arc::new(
                        physical_plan
                            .schema()
                            .try_into()
                            .context(error::ConvertSchemaSnafu)
                            .map_err(BoxedError::new)
                            .context(QueryExecutionSnafu)?,
                    ),
                    physical_plan,
                )))
            }
        }
    }
}

impl PhysicalOptimizer for DatafusionQueryEngine {
    fn optimize_physical_plan(
        &self,
        ctx: &mut QueryEngineContext,
        plan: Arc<dyn PhysicalPlan>,
    ) -> Result<Arc<dyn PhysicalPlan>> {
        let _timer = timer!(metric::METRIC_OPTIMIZE_PHYSICAL_ELAPSED);

        let mut new_plan = plan
            .as_any()
            .downcast_ref::<PhysicalPlanAdapter>()
            .context(error::PhysicalPlanDowncastSnafu)
            .map_err(BoxedError::new)
            .context(QueryExecutionSnafu)?
            .df_plan();

        let state = ctx.state();
        let config = state.config_options();
        for optimizer in state.physical_optimizers() {
            new_plan = optimizer
                .optimize(new_plan, config)
                .context(DataFusionSnafu)?;
        }
        Ok(Arc::new(PhysicalPlanAdapter::new(plan.schema(), new_plan)))
    }
}

impl QueryExecutor for DatafusionQueryEngine {
    fn execute_stream(
        &self,
        ctx: &QueryEngineContext,
        plan: &Arc<dyn PhysicalPlan>,
    ) -> Result<SendableRecordBatchStream> {
        let _timer = timer!(metric::METRIC_EXEC_PLAN_ELAPSED);
        match plan.output_partitioning().partition_count() {
            0 => Ok(Box::pin(EmptyRecordBatchStream::new(plan.schema()))),
            1 => Ok(plan
                .execute(0, ctx.state().task_ctx())
                .context(error::ExecutePhysicalPlanSnafu)
                .map_err(BoxedError::new)
                .context(QueryExecutionSnafu))?,
            _ => {
                // merge into a single partition
                let plan =
                    CoalescePartitionsExec::new(Arc::new(DfPhysicalPlanAdapter(plan.clone())));
                // CoalescePartitionsExec must produce a single partition
                assert_eq!(1, plan.output_partitioning().partition_count());
                let df_stream = plan
                    .execute(0, ctx.state().task_ctx())
                    .context(error::DatafusionSnafu {
                        msg: "Failed to execute DataFusion merge exec",
                    })
                    .map_err(BoxedError::new)
                    .context(QueryExecutionSnafu)?;
                let stream = RecordBatchStreamAdapter::try_new(df_stream)
                    .context(error::ConvertDfRecordBatchStreamSnafu)
                    .map_err(BoxedError::new)
                    .context(QueryExecutionSnafu)?;
                Ok(Box::pin(stream))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use catalog::local::{MemoryCatalogProvider, MemorySchemaProvider};
    use catalog::{CatalogList, CatalogProvider, SchemaProvider};
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
    use common_query::Output;
    use common_recordbatch::util;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::ColumnSchema;
    use datatypes::vectors::{UInt64Vector, VectorRef};
    use session::context::QueryContext;
    use table::table::numbers::NumbersTable;

    use crate::parser::QueryLanguageParser;
    use crate::query_engine::{QueryEngineFactory, QueryEngineRef};

    fn create_test_engine() -> QueryEngineRef {
        let catalog_list = catalog::local::new_memory_catalog_list().unwrap();

        let default_schema = Arc::new(MemorySchemaProvider::new());
        default_schema
            .register_table("numbers".to_string(), Arc::new(NumbersTable::default()))
            .unwrap();
        let default_catalog = Arc::new(MemoryCatalogProvider::new());
        default_catalog
            .register_schema(DEFAULT_SCHEMA_NAME.to_string(), default_schema)
            .unwrap();
        catalog_list
            .register_catalog(DEFAULT_CATALOG_NAME.to_string(), default_catalog)
            .unwrap();

        QueryEngineFactory::new(catalog_list).query_engine()
    }

    #[tokio::test]
    async fn test_sql_to_plan() {
        let engine = create_test_engine();
        let sql = "select sum(number) from numbers limit 20";

        let stmt = QueryLanguageParser::parse_sql(sql).unwrap();
        let plan = engine
            .statement_to_plan(stmt, Arc::new(QueryContext::new()))
            .await
            .unwrap();

        // TODO(sunng87): do not rely on to_string for compare
        assert_eq!(
            format!("{plan:?}"),
            r#"DfPlan(Limit: skip=0, fetch=20
  Projection: SUM(numbers.number)
    Aggregate: groupBy=[[]], aggr=[[SUM(numbers.number)]]
      TableScan: numbers)"#
        );
    }

    #[tokio::test]
    async fn test_execute() {
        let engine = create_test_engine();
        let sql = "select sum(number) from numbers limit 20";

        let stmt = QueryLanguageParser::parse_sql(sql).unwrap();
        let plan = engine
            .statement_to_plan(stmt, Arc::new(QueryContext::new()))
            .await
            .unwrap();

        let output = engine.execute(&plan).await.unwrap();

        match output {
            Output::Stream(recordbatch) => {
                let numbers = util::collect(recordbatch).await.unwrap();
                assert_eq!(1, numbers.len());
                assert_eq!(numbers[0].num_columns(), 1);
                assert_eq!(1, numbers[0].schema.num_columns());
                assert_eq!(
                    "SUM(numbers.number)",
                    numbers[0].schema.column_schemas()[0].name
                );

                let batch = &numbers[0];
                assert_eq!(1, batch.num_columns());
                assert_eq!(batch.column(0).len(), 1);

                assert_eq!(
                    *batch.column(0),
                    Arc::new(UInt64Vector::from_slice([4950])) as VectorRef
                );
            }
            _ => unreachable!(),
        }
    }

    #[tokio::test]
    async fn test_describe() {
        let engine = create_test_engine();
        let sql = "select sum(number) from numbers limit 20";

        let stmt = QueryLanguageParser::parse_sql(sql).unwrap();

        let (schema, _) = engine
            .describe(stmt, Arc::new(QueryContext::new()))
            .await
            .unwrap();

        assert_eq!(
            schema.column_schemas()[0],
            ColumnSchema::new(
                "SUM(numbers.number)",
                ConcreteDataType::uint64_datatype(),
                true
            )
        );
    }
}
