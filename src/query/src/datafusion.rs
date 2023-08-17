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
use common_error::ext::BoxedError;
use common_function::scalars::aggregate::AggregateFunctionMetaRef;
use common_function::scalars::udf::create_udf;
use common_function::scalars::FunctionRef;
use common_query::physical_plan::{DfPhysicalPlanAdapter, PhysicalPlan, PhysicalPlanAdapter};
use common_query::prelude::ScalarUdf;
use common_query::Output;
use common_recordbatch::adapter::RecordBatchStreamAdapter;
use common_recordbatch::{
    EmptyRecordBatchStream, RecordBatch, RecordBatches, SendableRecordBatchStream,
};
use common_telemetry::timer;
use datafusion::common::Column;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use datafusion_common::{ResolvedTableReference, ScalarValue};
use datafusion_expr::{DmlStatement, Expr as DfExpr, LogicalPlan as DfLogicalPlan, WriteOp};
use datatypes::prelude::VectorRef;
use datatypes::schema::Schema;
use futures_util::StreamExt;
use session::context::QueryContextRef;
use snafu::{ensure, OptionExt, ResultExt};
use sql::ast::{BinaryOperator, Expr, Value};
use table::requests::{DeleteRequest, InsertRequest};
use table::TableRef;

use crate::dataframe::DataFrame;
pub use crate::datafusion::planner::DfContextProviderAdapter;
use crate::error::{
    CatalogSnafu, CreateRecordBatchSnafu, CreateSchemaSnafu, DataFusionSnafu,
    MissingTimestampColumnSnafu, QueryExecutionSnafu, Result, TableNotFoundSnafu,
    UnimplementedSnafu, UnsupportedExprSnafu,
};
use crate::executor::QueryExecutor;
use crate::logical_optimizer::LogicalOptimizer;
use crate::physical_optimizer::PhysicalOptimizer;
use crate::physical_planner::PhysicalPlanner;
use crate::physical_wrapper::PhysicalPlanWrapperRef;
use crate::plan::LogicalPlan;
use crate::planner::{DfLogicalPlanner, LogicalPlanner};
use crate::query_engine::{DescribeResult, QueryEngineContext, QueryEngineState};
use crate::{metrics, QueryEngine};

pub struct DatafusionQueryEngine {
    state: Arc<QueryEngineState>,
    plugins: Arc<Plugins>,
}

impl DatafusionQueryEngine {
    pub fn new(state: Arc<QueryEngineState>, plugins: Arc<Plugins>) -> Self {
        Self { state, plugins }
    }

    async fn exec_query_plan(
        &self,
        plan: LogicalPlan,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        let mut ctx = QueryEngineContext::new(self.state.session_state(), query_ctx.clone());

        // `create_physical_plan` will optimize logical plan internally
        let physical_plan = self.create_physical_plan(&mut ctx, &plan).await?;
        let physical_plan = self.optimize_physical_plan(&mut ctx, physical_plan)?;

        let physical_plan = if let Some(wrapper) = self.plugins.get::<PhysicalPlanWrapperRef>() {
            wrapper.wrap(physical_plan, query_ctx)
        } else {
            physical_plan
        };

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

        let default_catalog = &query_ctx.current_catalog().to_owned();
        let default_schema = &query_ctx.current_schema().to_owned();
        let table_name = dml.table_name.resolve(default_catalog, default_schema);
        let table = self.find_table(&table_name).await?;

        let output = self
            .exec_query_plan(LogicalPlan::DfPlan((*dml.input).clone()), query_ctx)
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
        let catalog_name = table_name.catalog.to_string();
        let schema_name = table_name.schema.to_string();
        let table_name = table_name.table.to_string();
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
            catalog_name,
            schema_name,
            table_name,
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

        self.state
            .catalog_manager()
            .table(catalog_name, schema_name, table_name)
            .await
            .context(CatalogSnafu)?
            .with_context(|| TableNotFoundSnafu { table: table_name })
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

    async fn describe(&self, plan: LogicalPlan) -> Result<DescribeResult> {
        let optimised_plan = self.optimize(&plan)?;
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

    fn read_table(&self, table: TableRef) -> Result<DataFrame> {
        Ok(DataFrame::DataFusion(
            self.state
                .read_table(table)
                .context(error::DatafusionSnafu {
                    msg: "Fail to create dataframe for table",
                })
                .map_err(BoxedError::new)
                .context(QueryExecutionSnafu)?,
        ))
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
        let task_ctx = ctx.build_task_ctx();

        match plan.output_partitioning().partition_count() {
            0 => Ok(Box::pin(EmptyRecordBatchStream::new(plan.schema()))),
            1 => Ok(plan
                .execute(0, task_ctx)
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
                    .execute(0, task_ctx)
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

fn convert_filter_to_df_filter(filter: Expr) -> Result<DfExpr> {
    match filter {
        Expr::BinaryOp { left, op, right } => {
            let left = convert_filter_to_df_filter(*left)?;
            let right = convert_filter_to_df_filter(*right)?;
            match op {
                BinaryOperator::Eq => Ok(left.eq(right)),
                _ => UnimplementedSnafu {
                    operation: format!("convert BinaryOperator into datafusion Expr, op: {op}"),
                }
                .fail(),
            }
        }
        Expr::Value(value) => match value {
            Value::SingleQuotedString(v) => Ok(DfExpr::Literal(ScalarValue::Utf8(Some(v)))),
            _ => UnimplementedSnafu {
                operation: format!("convert Expr::Value into datafusion Expr, value: {value}"),
            }
            .fail(),
        },
        Expr::Identifier(ident) => Ok(DfExpr::Column(Column::from_name(ident.value))),
        _ => UnimplementedSnafu {
            operation: format!("convert Expr into datafusion Expr, Expr: {filter}"),
        }
        .fail(),
    }
}

/// Creates a table in memory and executes a show statement on the table.
pub async fn execute_show_with_filter(
    record_batch: RecordBatch,
    filter: Option<Expr>,
) -> Result<Output> {
    let table_name = "table_name";
    let column_schemas = record_batch.schema.column_schemas().to_vec();
    let context = SessionContext::new();
    context
        .register_batch(table_name, record_batch.into_df_record_batch())
        .context(error::DatafusionSnafu {
            msg: "Fail to register a record batch as a table",
        })
        .map_err(BoxedError::new)
        .context(QueryExecutionSnafu)?;
    let mut dataframe = context
        .sql(&format!("SELECT * FROM {table_name}"))
        .await
        .context(error::DatafusionSnafu {
            msg: "Fail to execute a sql",
        })
        .map_err(BoxedError::new)
        .context(QueryExecutionSnafu)?;
    if let Some(filter) = filter {
        let filter = convert_filter_to_df_filter(filter)?;
        dataframe = dataframe
            .filter(filter)
            .context(error::DatafusionSnafu {
                msg: "Fail to filter",
            })
            .map_err(BoxedError::new)
            .context(QueryExecutionSnafu)?
    }
    let df_batches = dataframe
        .collect()
        .await
        .context(error::DatafusionSnafu {
            msg: "Fail to collect the record batches",
        })
        .map_err(BoxedError::new)
        .context(QueryExecutionSnafu)?;
    let mut batches = Vec::with_capacity(df_batches.len());
    let schema = Arc::new(Schema::try_new(column_schemas).context(CreateSchemaSnafu)?);
    for df_batch in df_batches.into_iter() {
        let batch = RecordBatch::try_from_df_record_batch(schema.clone(), df_batch)
            .context(CreateRecordBatchSnafu)?;
        batches.push(batch);
    }
    let record_batches = RecordBatches::try_new(schema, batches).context(CreateRecordBatchSnafu)?;
    Ok(Output::RecordBatches(record_batches))
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow::Borrowed;
    use std::sync::Arc;

    use catalog::{CatalogManager, RegisterTableRequest};
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, NUMBERS_TABLE_ID};
    use common_query::Output;
    use common_recordbatch::{util, RecordBatch};
    use datafusion::prelude::{col, lit};
    use datatypes::prelude::{ConcreteDataType, MutableVector, ScalarVectorBuilder};
    use datatypes::schema::{ColumnSchema, Schema};
    use datatypes::types::StringType;
    use datatypes::vectors::{Helper, StringVectorBuilder, UInt32Vector, UInt64Vector, VectorRef};
    use session::context::QueryContext;
    use sql::dialect::GreptimeDbDialect;
    use sql::parser::ParserContext;
    use sql::statements::show::{ShowKind, ShowTables};
    use sql::statements::statement::Statement;
    use table::table::numbers::{NumbersTable, NUMBERS_TABLE_NAME};

    use super::*;
    use crate::parser::QueryLanguageParser;
    use crate::query_engine::{QueryEngineFactory, QueryEngineRef};

    async fn create_test_engine() -> QueryEngineRef {
        let catalog_manager = catalog::local::new_memory_catalog_manager().unwrap();
        let req = RegisterTableRequest {
            catalog: DEFAULT_CATALOG_NAME.to_string(),
            schema: DEFAULT_SCHEMA_NAME.to_string(),
            table_name: NUMBERS_TABLE_NAME.to_string(),
            table_id: NUMBERS_TABLE_ID,
            table: NumbersTable::table(NUMBERS_TABLE_ID),
        };
        let _ = catalog_manager.register_table(req).await.unwrap();

        QueryEngineFactory::new(catalog_manager, false).query_engine()
    }

    #[tokio::test]
    async fn test_sql_to_plan() {
        let engine = create_test_engine().await;
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
        let engine = create_test_engine().await;
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
    async fn test_read_table() {
        let engine = create_test_engine().await;

        let engine = engine
            .as_any()
            .downcast_ref::<DatafusionQueryEngine>()
            .unwrap();
        let table = engine
            .find_table(&ResolvedTableReference {
                catalog: Borrowed("greptime"),
                schema: Borrowed("public"),
                table: Borrowed("numbers"),
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

        let stmt = QueryLanguageParser::parse_sql(sql).unwrap();

        let plan = engine
            .planner()
            .plan(stmt, QueryContext::arc())
            .await
            .unwrap();

        let DescribeResult {
            schema,
            logical_plan,
        } = engine.describe(plan).await.unwrap();

        assert_eq!(
            schema.column_schemas()[0],
            ColumnSchema::new(
                "SUM(numbers.number)",
                ConcreteDataType::uint64_datatype(),
                true
            )
        );
        assert_eq!("Limit: skip=0, fetch=20\n  Aggregate: groupBy=[[]], aggr=[[SUM(numbers.number)]]\n    TableScan: numbers projection=[number]", format!("{}", logical_plan.display_indent()));
    }

    #[tokio::test]
    async fn test_show_tables() {
        // No filter
        let column_schemas = vec![ColumnSchema::new(
            "Tables",
            ConcreteDataType::String(StringType),
            false,
        )];
        let schema = Arc::new(Schema::new(column_schemas));
        let mut builder = StringVectorBuilder::with_capacity(3);
        builder.push(Some("monitor"));
        builder.push(Some("system_metrics"));
        let columns = vec![builder.to_vector()];
        let record_batch = RecordBatch::new(schema, columns).unwrap();
        let output = execute_show_with_filter(record_batch, None).await.unwrap();
        let Output::RecordBatches(record_batches) = output else {
            unreachable!()
        };
        let expected = "\
+----------------+
| Tables         |
+----------------+
| monitor        |
| system_metrics |
+----------------+";
        assert_eq!(record_batches.pretty_print().unwrap(), expected);

        // Filter
        let column_schemas = vec![ColumnSchema::new(
            "Tables",
            ConcreteDataType::String(StringType),
            false,
        )];
        let schema = Arc::new(Schema::new(column_schemas));
        let mut builder = StringVectorBuilder::with_capacity(3);
        builder.push(Some("monitor"));
        builder.push(Some("system_metrics"));
        let columns = vec![builder.to_vector()];
        let record_batch = RecordBatch::new(schema, columns).unwrap();
        let statement = ParserContext::create_with_dialect(
            "SHOW TABLES WHERE \"Tables\"='monitor'",
            &GreptimeDbDialect {},
        )
        .unwrap()[0]
            .clone();
        let Statement::ShowTables(ShowTables { kind, .. }) = statement else {
            unreachable!()
        };
        let ShowKind::Where(filter) = kind else {
            unreachable!()
        };
        let output = execute_show_with_filter(record_batch, Some(filter))
            .await
            .unwrap();
        let Output::RecordBatches(record_batches) = output else {
            unreachable!()
        };
        let expected = "\
+---------+
| Tables  |
+---------+
| monitor |
+---------+";
        assert_eq!(record_batches.pretty_print().unwrap(), expected);
    }
}
