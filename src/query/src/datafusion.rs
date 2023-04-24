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

mod error;
mod planner;

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
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
use datafusion_common::ResolvedTableReference;
use datafusion_expr::{DmlStatement, LogicalPlan as DfLogicalPlan, WriteOp};
use datatypes::prelude::VectorRef;
use datatypes::schema::Schema;
use futures_util::StreamExt;
use session::context::QueryContextRef;
use snafu::{ensure, OptionExt, ResultExt};
use table::requests::{DeleteRequest, InsertRequest};
use table::TableRef;

pub use crate::datafusion::planner::DfContextProviderAdapter;
use crate::error::{
    CatalogNotFoundSnafu, CatalogSnafu, CreateRecordBatchSnafu, DataFusionSnafu,
    MissingTimestampColumnSnafu, QueryExecutionSnafu, Result, SchemaNotFoundSnafu,
    TableNotFoundSnafu, UnsupportedExprSnafu,
};
use crate::executor::QueryExecutor;
use crate::logical_optimizer::LogicalOptimizer;
use crate::physical_optimizer::PhysicalOptimizer;
use crate::physical_planner::PhysicalPlanner;
use crate::plan::LogicalPlan;
use crate::planner::{DfLogicalPlanner, LogicalPlanner};
use crate::query_engine::{QueryEngineContext, QueryEngineState};
use crate::{metrics, QueryEngine};

pub struct DatafusionQueryEngine {
    state: Arc<QueryEngineState>,
}

impl DatafusionQueryEngine {
    pub fn new(state: Arc<QueryEngineState>) -> Self {
        Self { state }
    }

    async fn exec_query_plan(&self, plan: LogicalPlan) -> Result<Output> {
        let mut ctx = QueryEngineContext::new(self.state.session_state());

        // `create_physical_plan` will optimize logical plan internally
        let physical_plan = self.create_physical_plan(&mut ctx, &plan).await?;
        let physical_plan = self.optimize_physical_plan(&mut ctx, physical_plan)?;

        Ok(Output::Stream(self.execute_stream(&ctx, &physical_plan)?))
    }

    async fn exec_dml_statement(
        &self,
        dml: DmlStatement,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        ensure!(
            matches!(dml.op, WriteOp::Insert | WriteOp::Delete),
            UnsupportedExprSnafu {
                name: format!("DML op {}", dml.op),
            }
        );

        let default_catalog = query_ctx.current_catalog();
        let default_schema = query_ctx.current_schema();
        let table_name = dml.table_name.resolve(&default_catalog, &default_schema);
        let table = self.find_table(&table_name).await?;

        let output = self
            .exec_query_plan(LogicalPlan::DfPlan((*dml.input).clone()))
            .await?;
        let mut stream = match output {
            Output::RecordBatches(batches) => batches.as_stream(),
            Output::Stream(stream) => stream,
            _ => unreachable!(),
        };

        let mut affected_rows = 0;
        while let Some(batch) = stream.next().await {
            let batch = batch.context(CreateRecordBatchSnafu)?;
            let column_vectors = batch
                .column_vectors(&table_name.to_string(), table.schema())
                .map_err(BoxedError::new)
                .context(QueryExecutionSnafu)?;

            let rows = match dml.op {
                WriteOp::Insert => Self::insert(&table_name, &table, column_vectors).await?,
                WriteOp::Delete => Self::delete(&table_name, &table, column_vectors).await?,
                _ => unreachable!("guarded by the 'ensure!' at the beginning"),
            };
            affected_rows += rows;
        }
        Ok(Output::AffectedRows(affected_rows))
    }

    async fn delete<'a>(
        table_name: &ResolvedTableReference<'a>,
        table: &TableRef,
        column_vectors: HashMap<String, VectorRef>,
    ) -> Result<usize> {
        let table_schema = table.schema();
        let ts_column = table_schema
            .timestamp_column()
            .map(|x| &x.name)
            .with_context(|| MissingTimestampColumnSnafu {
                table_name: table_name.to_string(),
            })?;

        let table_info = table.table_info();
        let rowkey_columns = table_info
            .meta
            .row_key_column_names()
            .collect::<Vec<&String>>();
        let column_vectors = column_vectors
            .into_iter()
            .filter(|x| &x.0 == ts_column || rowkey_columns.contains(&&x.0))
            .collect::<HashMap<_, _>>();

        let request = DeleteRequest {
            key_column_values: column_vectors,
        };

        table
            .delete(request)
            .await
            .map_err(BoxedError::new)
            .context(QueryExecutionSnafu)
    }

    async fn insert<'a>(
        table_name: &ResolvedTableReference<'a>,
        table: &TableRef,
        column_vectors: HashMap<String, VectorRef>,
    ) -> Result<usize> {
        let request = InsertRequest {
            catalog_name: table_name.catalog.to_string(),
            schema_name: table_name.schema.to_string(),
            table_name: table_name.table.to_string(),
            columns_values: column_vectors,
            region_number: 0,
        };

        table
            .insert(request)
            .await
            .map_err(BoxedError::new)
            .context(QueryExecutionSnafu)
    }

    async fn find_table(&self, table_name: &ResolvedTableReference<'_>) -> Result<TableRef> {
        let catalog_name = table_name.catalog.as_ref();
        let schema_name = table_name.schema.as_ref();
        let table_name = table_name.table.as_ref();

        let catalog = self
            .state
            .catalog_manager()
            .catalog_async(catalog_name)
            .await
            .context(CatalogSnafu)?
            .context(CatalogNotFoundSnafu {
                catalog: catalog_name,
            })?;
        let schema =
            catalog
                .schema(schema_name)
                .context(CatalogSnafu)?
                .context(SchemaNotFoundSnafu {
                    schema: schema_name,
                })?;
        let table = schema
            .table(table_name)
            .await
            .context(CatalogSnafu)?
            .context(TableNotFoundSnafu { table: table_name })?;
        Ok(table)
    }
}

#[async_trait]
impl QueryEngine for DatafusionQueryEngine {
    fn planner(&self) -> Arc<dyn LogicalPlanner> {
        Arc::new(DfLogicalPlanner::new(self.state.clone()))
    }

    fn name(&self) -> &str {
        "datafusion"
    }

    async fn describe(&self, plan: LogicalPlan) -> Result<Schema> {
        // TODO(sunng87): consider cache optmised logical plan between describe
        // and execute
        let optimised_plan = self.optimize(&plan)?;
        optimised_plan.schema()
    }

    async fn execute(&self, plan: LogicalPlan, query_ctx: QueryContextRef) -> Result<Output> {
        match plan {
            LogicalPlan::DfPlan(DfLogicalPlan::Dml(dml)) => {
                self.exec_dml_statement(dml, query_ctx).await
            }
            _ => self.exec_query_plan(plan).await,
        }
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
        let _timer = timer!(metrics::METRIC_OPTIMIZE_LOGICAL_ELAPSED);
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
        let _timer = timer!(metrics::METRIC_CREATE_PHYSICAL_ELAPSED);
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
        let _timer = timer!(metrics::METRIC_OPTIMIZE_PHYSICAL_ELAPSED);

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
        let _timer = timer!(metrics::METRIC_EXEC_PLAN_ELAPSED);
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
    use catalog::{CatalogProvider, SchemaProvider};
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
            .register_catalog_sync(DEFAULT_CATALOG_NAME.to_string(), default_catalog)
            .unwrap();

        QueryEngineFactory::new(catalog_list).query_engine()
    }

    #[tokio::test]
    async fn test_sql_to_plan() {
        let engine = create_test_engine();
        let sql = "select sum(number) from numbers limit 20";

        let stmt = QueryLanguageParser::parse_sql(sql).unwrap();
        let plan = engine
            .planner()
            .plan(stmt, QueryContext::arc())
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
            .planner()
            .plan(stmt, QueryContext::arc())
            .await
            .unwrap();

        let output = engine.execute(plan, QueryContext::arc()).await.unwrap();

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

        let plan = engine
            .planner()
            .plan(stmt, QueryContext::arc())
            .await
            .unwrap();

        let schema = engine.describe(plan).await.unwrap();

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
