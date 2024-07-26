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

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use common_base::Plugins;
use common_catalog::consts::is_readonly_schema;
use common_error::ext::BoxedError;
use common_function::function::FunctionRef;
use common_function::scalars::aggregate::AggregateFunctionMetaRef;
use common_query::prelude::ScalarUdf;
use common_query::{Output, OutputData, OutputMeta};
use common_recordbatch::adapter::RecordBatchStreamAdapter;
use common_recordbatch::{EmptyRecordBatchStream, SendableRecordBatchStream};
use common_telemetry::tracing;
use datafusion::physical_plan::analyze::AnalyzeExec;
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

use crate::analyze::DistAnalyzeExec;
use crate::dataframe::DataFrame;
pub use crate::datafusion::planner::DfContextProviderAdapter;
use crate::dist_plan::MergeScanLogicalPlan;
use crate::error::{
    CatalogSnafu, CreateRecordBatchSnafu, DataFusionSnafu, MissingTableMutationHandlerSnafu,
    MissingTimestampColumnSnafu, QueryExecutionSnafu, Result, TableMutationSnafu,
    TableNotFoundSnafu, TableReadOnlySnafu, UnsupportedExprSnafu,
};
use crate::executor::QueryExecutor;
use crate::metrics::{OnDone, QUERY_STAGE_ELAPSED};
use crate::physical_wrapper::PhysicalPlanWrapperRef;
use crate::plan::LogicalPlan;
use crate::planner::{DfLogicalPlanner, LogicalPlanner};
use crate::query_engine::{DescribeResult, QueryEngineContext, QueryEngineState};
use crate::{metrics, QueryEngine};

pub struct DatafusionQueryEngine {
    state: Arc<QueryEngineState>,
    plugins: Plugins,
}

impl DatafusionQueryEngine {
    pub fn new(state: Arc<QueryEngineState>, plugins: Plugins) -> Self {
        Self { state, plugins }
    }

    #[tracing::instrument(skip_all)]
    async fn exec_query_plan(
        &self,
        plan: LogicalPlan,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        let mut ctx = self.engine_context(query_ctx.clone());

        // `create_physical_plan` will optimize logical plan internally
        let physical_plan = self.create_physical_plan(&mut ctx, &plan).await?;
        let optimized_physical_plan = self.optimize_physical_plan(&mut ctx, physical_plan)?;

        let physical_plan = if let Some(wrapper) = self.plugins.get::<PhysicalPlanWrapperRef>() {
            wrapper.wrap(optimized_physical_plan, query_ctx)
        } else {
            optimized_physical_plan
        };

        Ok(Output::new(
            OutputData::Stream(self.execute_stream(&ctx, &physical_plan)?),
            OutputMeta::new_with_plan(physical_plan),
        ))
    }

    #[tracing::instrument(skip_all)]
    async fn exec_dml_statement(
        &self,
        dml: DmlStatement,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        ensure!(
            matches!(dml.op, WriteOp::InsertInto | WriteOp::Delete),
            UnsupportedExprSnafu {
                name: format!("DML op {}", dml.op),
            }
        );

        let _timer = QUERY_STAGE_ELAPSED
            .with_label_values(&[dml.op.name()])
            .start_timer();

        let default_catalog = &query_ctx.current_catalog().to_owned();
        let default_schema = &query_ctx.current_schema();
        let table_name = dml.table_name.resolve(default_catalog, default_schema);
        let table = self.find_table(&table_name).await?;

        let output = self
            .exec_query_plan(LogicalPlan::DfPlan((*dml.input).clone()), query_ctx.clone())
            .await?;
        let mut stream = match output.data {
            OutputData::RecordBatches(batches) => batches.as_stream(),
            OutputData::Stream(stream) => stream,
            _ => unreachable!(),
        };

        let mut affected_rows = 0;
        let mut insert_cost = 0;

        while let Some(batch) = stream.next().await {
            let batch = batch.context(CreateRecordBatchSnafu)?;
            let column_vectors = batch
                .column_vectors(&table_name.to_string(), table.schema())
                .map_err(BoxedError::new)
                .context(QueryExecutionSnafu)?;

            match dml.op {
                WriteOp::InsertInto => {
                    let output = self
                        .insert(&table_name, column_vectors, query_ctx.clone())
                        .await?;
                    let (rows, cost) = output.extract_rows_and_cost();
                    affected_rows += rows;
                    insert_cost += cost;
                }
                WriteOp::Delete => {
                    affected_rows += self
                        .delete(&table_name, &table, column_vectors, query_ctx.clone())
                        .await?;
                }
                _ => unreachable!("guarded by the 'ensure!' at the beginning"),
            }
        }
        Ok(Output::new(
            OutputData::AffectedRows(affected_rows),
            OutputMeta::new_with_cost(insert_cost),
        ))
    }

    #[tracing::instrument(skip_all)]
    async fn delete(
        &self,
        table_name: &ResolvedTableReference,
        table: &TableRef,
        column_vectors: HashMap<String, VectorRef>,
        query_ctx: QueryContextRef,
    ) -> Result<usize> {
        let catalog_name = table_name.catalog.to_string();
        let schema_name = table_name.schema.to_string();
        let table_name = table_name.table.to_string();
        let table_schema = table.schema();

        ensure!(
            !is_readonly_schema(&schema_name),
            TableReadOnlySnafu { table: table_name }
        );

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
            catalog_name,
            schema_name,
            table_name,
            key_column_values: column_vectors,
        };

        self.state
            .table_mutation_handler()
            .context(MissingTableMutationHandlerSnafu)?
            .delete(request, query_ctx)
            .await
            .context(TableMutationSnafu)
    }

    #[tracing::instrument(skip_all)]
    async fn insert(
        &self,
        table_name: &ResolvedTableReference,
        column_vectors: HashMap<String, VectorRef>,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        let catalog_name = table_name.catalog.to_string();
        let schema_name = table_name.schema.to_string();
        let table_name = table_name.table.to_string();

        ensure!(
            !is_readonly_schema(&schema_name),
            TableReadOnlySnafu { table: table_name }
        );

        let request = InsertRequest {
            catalog_name,
            schema_name,
            table_name,
            columns_values: column_vectors,
        };

        self.state
            .table_mutation_handler()
            .context(MissingTableMutationHandlerSnafu)?
            .insert(request, query_ctx)
            .await
            .context(TableMutationSnafu)
    }

    async fn find_table(&self, table_name: &ResolvedTableReference) -> Result<TableRef> {
        let catalog_name = table_name.catalog.as_ref();
        let schema_name = table_name.schema.as_ref();
        let table_name = table_name.table.as_ref();

        self.state
            .catalog_manager()
            .table(catalog_name, schema_name, table_name)
            .await
            .context(CatalogSnafu)?
            .with_context(|| TableNotFoundSnafu { table: table_name })
    }

    #[tracing::instrument(skip_all)]
    async fn create_physical_plan(
        &self,
        ctx: &mut QueryEngineContext,
        logical_plan: &LogicalPlan,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let _timer = metrics::CREATE_PHYSICAL_ELAPSED.start_timer();
        match logical_plan {
            LogicalPlan::DfPlan(df_plan) => {
                let state = ctx.state();

                // special handle EXPLAIN plan
                if matches!(df_plan, DfLogicalPlan::Explain(_)) {
                    return state
                        .create_physical_plan(df_plan)
                        .await
                        .context(error::DatafusionSnafu)
                        .map_err(BoxedError::new)
                        .context(QueryExecutionSnafu);
                }

                // analyze first
                let analyzed_plan = state
                    .analyzer()
                    .execute_and_check(df_plan.clone(), state.config_options(), |_, _| {})
                    .context(error::DatafusionSnafu)
                    .map_err(BoxedError::new)
                    .context(QueryExecutionSnafu)?;
                // skip optimize for MergeScan
                let optimized_plan = if let DfLogicalPlan::Extension(ext) = &analyzed_plan
                    && ext.node.name() == MergeScanLogicalPlan::name()
                {
                    analyzed_plan.clone()
                } else {
                    state
                        .optimizer()
                        .optimize(analyzed_plan, state, |_, _| {})
                        .context(error::DatafusionSnafu)
                        .map_err(BoxedError::new)
                        .context(QueryExecutionSnafu)?
                };

                let physical_plan = state
                    .query_planner()
                    .create_physical_plan(&optimized_plan, state)
                    .await
                    .context(error::DatafusionSnafu)
                    .map_err(BoxedError::new)
                    .context(QueryExecutionSnafu)?;

                Ok(physical_plan)
            }
        }
    }

    #[tracing::instrument(skip_all)]
    pub fn optimize(
        &self,
        context: &QueryEngineContext,
        plan: &LogicalPlan,
    ) -> Result<LogicalPlan> {
        let _timer = metrics::OPTIMIZE_LOGICAL_ELAPSED.start_timer();
        match plan {
            LogicalPlan::DfPlan(df_plan) => {
                // Optimized by extension rules
                let optimized_plan = self
                    .state
                    .optimize_by_extension_rules(df_plan.clone(), context)
                    .context(error::DatafusionSnafu)
                    .map_err(BoxedError::new)
                    .context(QueryExecutionSnafu)?;

                // Optimized by datafusion optimizer
                let optimized_plan = self
                    .state
                    .session_state()
                    .optimize(&optimized_plan)
                    .context(error::DatafusionSnafu)
                    .map_err(BoxedError::new)
                    .context(QueryExecutionSnafu)?;

                Ok(LogicalPlan::DfPlan(optimized_plan))
            }
        }
    }

    #[tracing::instrument(skip_all)]
    fn optimize_physical_plan(
        &self,
        ctx: &mut QueryEngineContext,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let _timer = metrics::OPTIMIZE_PHYSICAL_ELAPSED.start_timer();

        let state = ctx.state();
        let config = state.config_options();
        // skip optimize AnalyzeExec plan
        let optimized_plan = if let Some(analyze_plan) = plan.as_any().downcast_ref::<AnalyzeExec>()
        {
            let mut new_plan = analyze_plan.input().clone();
            for optimizer in state.physical_optimizers() {
                new_plan = optimizer
                    .optimize(new_plan, config)
                    .context(DataFusionSnafu)?;
            }
            Arc::new(DistAnalyzeExec::new(new_plan))
        } else {
            let mut new_plan = plan;
            for optimizer in state.physical_optimizers() {
                new_plan = optimizer
                    .optimize(new_plan, config)
                    .context(DataFusionSnafu)?;
            }
            new_plan
        };

        Ok(optimized_plan)
    }
}

#[async_trait]
impl QueryEngine for DatafusionQueryEngine {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn planner(&self) -> Arc<dyn LogicalPlanner> {
        Arc::new(DfLogicalPlanner::new(self.state.clone()))
    }

    fn name(&self) -> &str {
        "datafusion"
    }

    async fn describe(
        &self,
        plan: LogicalPlan,
        query_ctx: QueryContextRef,
    ) -> Result<DescribeResult> {
        let ctx = self.engine_context(query_ctx);
        let optimised_plan = self.optimize(&ctx, &plan)?;
        Ok(DescribeResult {
            schema: optimised_plan.schema()?,
            logical_plan: optimised_plan,
        })
    }

    async fn execute(&self, plan: LogicalPlan, query_ctx: QueryContextRef) -> Result<Output> {
        match plan {
            LogicalPlan::DfPlan(DfLogicalPlan::Dml(dml)) => {
                self.exec_dml_statement(dml, query_ctx).await
            }
            _ => self.exec_query_plan(plan, query_ctx).await,
        }
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

    /// Register a [`ScalarUdf`].
    fn register_udf(&self, udf: ScalarUdf) {
        self.state.register_udf(udf);
    }

    /// Register an UDF function.
    /// Will override if the function with same name is already registered.
    fn register_function(&self, func: FunctionRef) {
        self.state.register_function(func);
    }

    fn read_table(&self, table: TableRef) -> Result<DataFrame> {
        Ok(DataFrame::DataFusion(
            self.state
                .read_table(table)
                .context(error::DatafusionSnafu)
                .map_err(BoxedError::new)
                .context(QueryExecutionSnafu)?,
        ))
    }

    fn engine_context(&self, query_ctx: QueryContextRef) -> QueryEngineContext {
        let mut state = self.state.session_state();
        state.config_mut().set_extension(query_ctx.clone());
        QueryEngineContext::new(state, query_ctx)
    }
}

impl QueryExecutor for DatafusionQueryEngine {
    #[tracing::instrument(skip_all)]
    fn execute_stream(
        &self,
        ctx: &QueryEngineContext,
        plan: &Arc<dyn ExecutionPlan>,
    ) -> Result<SendableRecordBatchStream> {
        let exec_timer = metrics::EXEC_PLAN_ELAPSED.start_timer();
        let task_ctx = ctx.build_task_ctx();

        match plan.properties().output_partitioning().partition_count() {
            0 => {
                let schema = Arc::new(
                    Schema::try_from(plan.schema())
                        .map_err(BoxedError::new)
                        .context(QueryExecutionSnafu)?,
                );
                Ok(Box::pin(EmptyRecordBatchStream::new(schema)))
            }
            1 => {
                let df_stream = plan
                    .execute(0, task_ctx)
                    .context(error::DatafusionSnafu)
                    .map_err(BoxedError::new)
                    .context(QueryExecutionSnafu)?;
                let mut stream = RecordBatchStreamAdapter::try_new(df_stream)
                    .context(error::ConvertDfRecordBatchStreamSnafu)
                    .map_err(BoxedError::new)
                    .context(QueryExecutionSnafu)?;
                stream.set_metrics2(plan.clone());
                let stream = OnDone::new(Box::pin(stream), move || {
                    exec_timer.observe_duration();
                });
                Ok(Box::pin(stream))
            }
            _ => {
                // merge into a single partition
                let merged_plan = CoalescePartitionsExec::new(plan.clone());
                // CoalescePartitionsExec must produce a single partition
                assert_eq!(
                    1,
                    merged_plan
                        .properties()
                        .output_partitioning()
                        .partition_count()
                );
                let df_stream = merged_plan
                    .execute(0, task_ctx)
                    .context(error::DatafusionSnafu)
                    .map_err(BoxedError::new)
                    .context(QueryExecutionSnafu)?;
                let mut stream = RecordBatchStreamAdapter::try_new(df_stream)
                    .context(error::ConvertDfRecordBatchStreamSnafu)
                    .map_err(BoxedError::new)
                    .context(QueryExecutionSnafu)?;
                stream.set_metrics2(plan.clone());
                let stream = OnDone::new(Box::pin(stream), move || {
                    exec_timer.observe_duration();
                });
                Ok(Box::pin(stream))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use catalog::RegisterTableRequest;
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, NUMBERS_TABLE_ID};
    use common_recordbatch::util;
    use datafusion::prelude::{col, lit};
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::ColumnSchema;
    use datatypes::vectors::{Helper, UInt32Vector, UInt64Vector, VectorRef};
    use session::context::QueryContext;
    use table::table::numbers::{NumbersTable, NUMBERS_TABLE_NAME};

    use super::*;
    use crate::parser::QueryLanguageParser;
    use crate::query_engine::{QueryEngineFactory, QueryEngineRef};

    async fn create_test_engine() -> QueryEngineRef {
        let catalog_manager = catalog::memory::new_memory_catalog_manager().unwrap();
        let req = RegisterTableRequest {
            catalog: DEFAULT_CATALOG_NAME.to_string(),
            schema: DEFAULT_SCHEMA_NAME.to_string(),
            table_name: NUMBERS_TABLE_NAME.to_string(),
            table_id: NUMBERS_TABLE_ID,
            table: NumbersTable::table(NUMBERS_TABLE_ID),
        };
        catalog_manager.register_table_sync(req).unwrap();

        QueryEngineFactory::new(catalog_manager, None, None, None, None, false).query_engine()
    }

    #[tokio::test]
    async fn test_sql_to_plan() {
        let engine = create_test_engine().await;
        let sql = "select sum(number) from numbers limit 20";

        let stmt = QueryLanguageParser::parse_sql(sql, &QueryContext::arc()).unwrap();
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
        let engine = create_test_engine().await;
        let sql = "select sum(number) from numbers limit 20";

        let stmt = QueryLanguageParser::parse_sql(sql, &QueryContext::arc()).unwrap();
        let plan = engine
            .planner()
            .plan(stmt, QueryContext::arc())
            .await
            .unwrap();

        let output = engine.execute(plan, QueryContext::arc()).await.unwrap();

        match output.data {
            OutputData::Stream(recordbatch) => {
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
    async fn test_read_table() {
        let engine = create_test_engine().await;

        let engine = engine
            .as_any()
            .downcast_ref::<DatafusionQueryEngine>()
            .unwrap();
        let table = engine
            .find_table(&ResolvedTableReference {
                catalog: "greptime".into(),
                schema: "public".into(),
                table: "numbers".into(),
            })
            .await
            .unwrap();

        let DataFrame::DataFusion(df) = engine.read_table(table).unwrap();
        let df = df
            .select_columns(&["number"])
            .unwrap()
            .filter(col("number").lt(lit(10)))
            .unwrap();
        let batches = df.collect().await.unwrap();
        assert_eq!(1, batches.len());
        let batch = &batches[0];

        assert_eq!(1, batch.num_columns());
        assert_eq!(batch.column(0).len(), 10);

        assert_eq!(
            Helper::try_into_vector(batch.column(0)).unwrap(),
            Arc::new(UInt32Vector::from_slice([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])) as VectorRef
        );
    }

    #[tokio::test]
    async fn test_describe() {
        let engine = create_test_engine().await;
        let sql = "select sum(number) from numbers limit 20";

        let stmt = QueryLanguageParser::parse_sql(sql, &QueryContext::arc()).unwrap();

        let plan = engine
            .planner()
            .plan(stmt, QueryContext::arc())
            .await
            .unwrap();

        let DescribeResult {
            schema,
            logical_plan,
        } = engine.describe(plan, QueryContext::arc()).await.unwrap();

        assert_eq!(
            schema.column_schemas()[0],
            ColumnSchema::new(
                "SUM(numbers.number)",
                ConcreteDataType::uint64_datatype(),
                true
            )
        );
        assert_eq!("Limit: skip=0, fetch=20\n  Aggregate: groupBy=[[]], aggr=[[SUM(CAST(numbers.number AS UInt64))]]\n    TableScan: numbers projection=[number]", format!("{}", logical_plan.display_indent()));
    }
}
