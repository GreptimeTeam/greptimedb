use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use common_recordbatch::adapter::{DfRecordBatchStreamAdapter, RecordBatchStreamAdapter};
use common_recordbatch::{DfSendableRecordBatchStream, SendableRecordBatchStream};
use datafusion::arrow::datatypes::SchemaRef as DfSchemaRef;
use datafusion::error::Result as DfResult;
pub use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::physical_plan::expressions::PhysicalSortExpr;
pub use datafusion::physical_plan::Partitioning;
use datafusion::physical_plan::Statistics;
use datatypes::schema::SchemaRef;
use snafu::ResultExt;

use crate::error::{self, Result};
use crate::DfPhysicalPlan;

pub type PhysicalPlanRef = Arc<dyn PhysicalPlan>;

/// `PhysicalPlan` represent nodes in the Physical Plan.
///
/// Each `PhysicalPlan` is Partition-aware and is responsible for
/// creating the actual `async` [`SendableRecordBatchStream`]s
/// of [`RecordBatch`] that incrementally compute the operator's
/// output from its input partition.
#[async_trait]
pub trait PhysicalPlan: Debug + Send + Sync {
    /// Returns the physical plan as [`Any`](std::any::Any) so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;

    /// Get the schema for this physical plan
    fn schema(&self) -> SchemaRef;

    /// Specifies the output partitioning scheme of this plan
    fn output_partitioning(&self) -> Partitioning;

    /// Get a list of child physical plans that provide the input for this plan. The returned list
    /// will be empty for leaf nodes, will contain a single value for unary nodes, or two
    /// values for binary nodes (such as joins).
    fn children(&self) -> Vec<PhysicalPlanRef>;

    /// Returns a new plan where all children were replaced by new plans.
    /// The size of `children` must be equal to the size of `PhysicalPlan::children()`.
    fn with_new_children(&self, children: Vec<PhysicalPlanRef>) -> Result<PhysicalPlanRef>;

    /// Creates an RecordBatch stream.
    async fn execute(
        &self,
        partition: usize,
        runtime: Arc<RuntimeEnv>,
    ) -> Result<SendableRecordBatchStream>;
}

#[derive(Debug)]
pub struct PhysicalPlanAdapter {
    schema: SchemaRef,
    df_plan: Arc<dyn DfPhysicalPlan>,
}

impl PhysicalPlanAdapter {
    pub fn new(schema: SchemaRef, df_plan: Arc<dyn DfPhysicalPlan>) -> Self {
        Self { schema, df_plan }
    }

    pub fn df_plan(&self) -> Arc<dyn DfPhysicalPlan> {
        self.df_plan.clone()
    }
}

#[async_trait]
impl PhysicalPlan for PhysicalPlanAdapter {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        self.df_plan.output_partitioning()
    }

    fn children(&self) -> Vec<PhysicalPlanRef> {
        self.df_plan
            .children()
            .into_iter()
            .map(|x| Arc::new(PhysicalPlanAdapter::new(self.schema(), x)) as _)
            .collect()
    }

    fn with_new_children(&self, children: Vec<PhysicalPlanRef>) -> Result<PhysicalPlanRef> {
        let children = children
            .into_iter()
            .map(|x| Arc::new(DfPhysicalPlanAdapter(x)) as _)
            .collect();
        let plan = self
            .df_plan
            .with_new_children(children)
            .context(error::GeneralDataFusionSnafu)?;
        Ok(Arc::new(PhysicalPlanAdapter::new(self.schema(), plan)))
    }

    async fn execute(
        &self,
        partition: usize,
        runtime: Arc<RuntimeEnv>,
    ) -> Result<SendableRecordBatchStream> {
        let stream = self
            .df_plan
            .execute(partition, runtime)
            .await
            .context(error::DataFusionExecutionPlanSnafu)?;
        let stream = RecordBatchStreamAdapter::try_new(stream)
            .context(error::ConvertDfRecordBatchStreamSnafu)?;
        Ok(Box::pin(stream))
    }
}

#[derive(Debug)]
pub struct DfPhysicalPlanAdapter(pub PhysicalPlanRef);

#[async_trait]
impl DfPhysicalPlan for DfPhysicalPlanAdapter {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> DfSchemaRef {
        self.0.schema().arrow_schema().clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        self.0.output_partitioning()
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn DfPhysicalPlan>> {
        self.0
            .children()
            .into_iter()
            .map(|x| Arc::new(DfPhysicalPlanAdapter(x)) as _)
            .collect()
    }

    fn with_new_children(
        &self,
        children: Vec<Arc<dyn DfPhysicalPlan>>,
    ) -> DfResult<Arc<dyn DfPhysicalPlan>> {
        let df_schema = self.schema();
        let schema: SchemaRef = Arc::new(
            df_schema
                .try_into()
                .context(error::ConvertArrowSchemaSnafu)
                .map_err(error::Error::from)?,
        );
        let children = children
            .into_iter()
            .map(|x| Arc::new(PhysicalPlanAdapter::new(schema.clone(), x)) as _)
            .collect();
        let plan = self.0.with_new_children(children)?;
        Ok(Arc::new(DfPhysicalPlanAdapter(plan)))
    }

    async fn execute(
        &self,
        partition: usize,
        runtime: Arc<RuntimeEnv>,
    ) -> DfResult<DfSendableRecordBatchStream> {
        let stream = self.0.execute(partition, runtime).await?;
        Ok(Box::pin(DfRecordBatchStreamAdapter::new(stream)))
    }

    fn statistics(&self) -> Statistics {
        // TODO(LFC): impl statistics
        Statistics::default()
    }
}

#[cfg(test)]
mod test {
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use common_recordbatch::{RecordBatch, RecordBatches};
    use datafusion::arrow_print;
    use datafusion::datasource::TableProvider as DfTableProvider;
    use datafusion::logical_plan::LogicalPlanBuilder;
    use datafusion::physical_plan::collect;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::prelude::ExecutionContext;
    use datafusion_common::field_util::SchemaExt;
    use datafusion_expr::Expr;
    use datatypes::schema::Schema;
    use datatypes::vectors::Int32Vector;

    use super::*;

    struct MyDfTableProvider;

    #[async_trait]
    impl DfTableProvider for MyDfTableProvider {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn schema(&self) -> DfSchemaRef {
            Arc::new(ArrowSchema::new(vec![Field::new(
                "a",
                DataType::Int32,
                false,
            )]))
        }

        async fn scan(
            &self,
            _projection: &Option<Vec<usize>>,
            _filters: &[Expr],
            _limit: Option<usize>,
        ) -> DfResult<Arc<dyn DfPhysicalPlan>> {
            let schema = Schema::try_from(self.schema()).unwrap();
            let my_plan = Arc::new(MyExecutionPlan {
                schema: Arc::new(schema),
            });
            let df_plan = DfPhysicalPlanAdapter(my_plan);
            Ok(Arc::new(df_plan))
        }
    }

    #[derive(Debug)]
    struct MyExecutionPlan {
        schema: SchemaRef,
    }

    #[async_trait]
    impl PhysicalPlan for MyExecutionPlan {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn schema(&self) -> SchemaRef {
            self.schema.clone()
        }

        fn output_partitioning(&self) -> Partitioning {
            Partitioning::UnknownPartitioning(1)
        }

        fn children(&self) -> Vec<PhysicalPlanRef> {
            vec![]
        }

        fn with_new_children(&self, _children: Vec<PhysicalPlanRef>) -> Result<PhysicalPlanRef> {
            unimplemented!()
        }

        async fn execute(
            &self,
            _partition: usize,
            _runtime: Arc<RuntimeEnv>,
        ) -> Result<SendableRecordBatchStream> {
            let schema = self.schema();
            let recordbatches = RecordBatches::try_new(
                schema.clone(),
                vec![
                    RecordBatch::new(
                        schema.clone(),
                        vec![Arc::new(Int32Vector::from_slice(vec![1])) as _],
                    )
                    .unwrap(),
                    RecordBatch::new(
                        schema,
                        vec![Arc::new(Int32Vector::from_slice(vec![2, 3])) as _],
                    )
                    .unwrap(),
                ],
            )
            .unwrap();
            Ok(recordbatches.as_stream())
        }
    }

    // Test our physical plan can be executed by DataFusion, through adapters.
    #[tokio::test]
    async fn test_execute_physical_plan() {
        let ctx = ExecutionContext::new();
        let logical_plan = LogicalPlanBuilder::scan("test", Arc::new(MyDfTableProvider), None)
            .unwrap()
            .build()
            .unwrap();
        let physical_plan = ctx.create_physical_plan(&logical_plan).await.unwrap();
        let df_recordbatches = collect(physical_plan, Arc::new(RuntimeEnv::default()))
            .await
            .unwrap();
        let pretty_print = arrow_print::write(&df_recordbatches);
        let pretty_print = pretty_print.lines().collect::<Vec<&str>>();
        assert_eq!(
            pretty_print,
            vec!["+---+", "| a |", "+---+", "| 1 |", "| 2 |", "| 3 |", "+---+",]
        );
    }

    #[test]
    fn test_physical_plan_adapter() {
        let df_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "name",
            DataType::Utf8,
            true,
        )]));

        let plan = PhysicalPlanAdapter::new(
            Arc::new(Schema::try_from(df_schema.clone()).unwrap()),
            Arc::new(EmptyExec::new(true, df_schema.clone())),
        );
        assert!(plan.df_plan.as_any().downcast_ref::<EmptyExec>().is_some());

        let df_plan = DfPhysicalPlanAdapter(Arc::new(plan));
        assert_eq!(df_schema, df_plan.schema());
    }
}
