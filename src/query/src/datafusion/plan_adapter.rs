use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use common_recordbatch::adapter::{DfRecordBatchStreamAdapter, RecordBatchStreamAdapter};
use common_recordbatch::SendableRecordBatchStream;
use datafusion::arrow::datatypes::SchemaRef as DfSchemaRef;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::{
    error::Result as DfResult,
    physical_plan::{
        expressions::PhysicalSortExpr, ExecutionPlan, Partitioning as DfPartitioning,
        SendableRecordBatchStream as DfSendableRecordBatchStream, Statistics,
    },
};
use datatypes::schema::SchemaRef;
use snafu::ResultExt;

use crate::datafusion::error;
use crate::error::Result;
use crate::executor::Runtime;
use crate::plan::{Partitioning, PhysicalPlan};

/// Datafusion ExecutionPlan -> greptime PhysicalPlan
#[derive(Debug)]
pub struct PhysicalPlanAdapter {
    plan: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
}

impl PhysicalPlanAdapter {
    pub fn new(schema: SchemaRef, plan: Arc<dyn ExecutionPlan>) -> Self {
        Self { schema, plan }
    }

    #[inline]
    pub fn df_plan(&self) -> &Arc<dyn ExecutionPlan> {
        &self.plan
    }
}

#[async_trait::async_trait]
impl PhysicalPlan for PhysicalPlanAdapter {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        //FIXME(dennis)
        Partitioning::UnknownPartitioning(1)
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalPlan>> {
        let mut plans: Vec<Arc<dyn PhysicalPlan>> = vec![];
        for p in self.plan.children() {
            let plan = PhysicalPlanAdapter::new(self.schema.clone(), p);
            plans.push(Arc::new(plan));
        }
        plans
    }

    fn with_new_children(
        &self,
        children: Vec<Arc<dyn PhysicalPlan>>,
    ) -> Result<Arc<dyn PhysicalPlan>> {
        let mut df_children: Vec<Arc<dyn ExecutionPlan>> = Vec::with_capacity(children.len());

        for plan in children {
            let p = Arc::new(ExecutionPlanAdapter {
                plan,
                schema: self.schema.clone(),
            });
            df_children.push(p);
        }

        let plan = self
            .plan
            .with_new_children(df_children)
            .context(error::DatafusionSnafu {
                msg: "Fail to add children to plan",
            })?;
        Ok(Arc::new(PhysicalPlanAdapter::new(
            self.schema.clone(),
            plan,
        )))
    }

    async fn execute(
        &self,
        runtime: &Runtime,
        partition: usize,
    ) -> Result<SendableRecordBatchStream> {
        let df_stream =
            self.plan
                .execute(partition, runtime.into())
                .await
                .context(error::DatafusionSnafu {
                    msg: "Fail to execute physical plan",
                })?;

        Ok(Box::pin(
            RecordBatchStreamAdapter::try_new(df_stream)
                .context(error::ConvertDfRecordBatchStreamSnafu)?,
        ))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Greptime PhysicalPlan -> datafusion ExecutionPlan.
#[derive(Debug)]
struct ExecutionPlanAdapter {
    plan: Arc<dyn PhysicalPlan>,
    schema: SchemaRef,
}

#[async_trait::async_trait]
impl ExecutionPlan for ExecutionPlanAdapter {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> DfSchemaRef {
        self.schema.arrow_schema().clone()
    }

    fn output_partitioning(&self) -> DfPartitioning {
        // FIXME(dennis)
        DfPartitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        // FIXME(dennis)
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        // TODO(dennis)
        vec![]
    }

    fn with_new_children(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let mut gt_children: Vec<Arc<dyn PhysicalPlan>> = Vec::with_capacity(children.len());

        for plan in children {
            let p = Arc::new(PhysicalPlanAdapter::new(self.schema.clone(), plan));
            gt_children.push(p);
        }

        match self.plan.with_new_children(gt_children) {
            Ok(plan) => Ok(Arc::new(ExecutionPlanAdapter {
                schema: self.schema.clone(),
                plan,
            })),
            Err(e) => Err(e.into()),
        }
    }

    async fn execute(
        &self,
        partition: usize,
        runtime: Arc<RuntimeEnv>,
    ) -> DfResult<DfSendableRecordBatchStream> {
        match self.plan.execute(&runtime.into(), partition).await {
            Ok(stream) => Ok(Box::pin(DfRecordBatchStreamAdapter::new(stream))),
            Err(e) => Err(e.into()),
        }
    }

    fn statistics(&self) -> Statistics {
        //TODO(dennis)
        Statistics::default()
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::Field;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion_common::field_util::SchemaExt;
    use datatypes::schema::Schema;

    use super::*;

    #[test]
    fn test_physical_plan_adapter() {
        let arrow_schema = arrow::datatypes::Schema::new(vec![Field::new(
            "name",
            arrow::datatypes::DataType::Utf8,
            true,
        )]);

        let schema = Arc::new(Schema::try_from(arrow_schema.clone()).unwrap());
        let physical_plan = PhysicalPlanAdapter::new(
            schema.clone(),
            Arc::new(EmptyExec::new(true, Arc::new(arrow_schema))),
        );

        assert!(physical_plan
            .plan
            .as_any()
            .downcast_ref::<EmptyExec>()
            .is_some());
        let execution_plan_adapter = ExecutionPlanAdapter {
            plan: Arc::new(physical_plan),
            schema: schema.clone(),
        };
        assert_eq!(schema, execution_plan_adapter.schema);
    }
}
