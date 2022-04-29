use std::any::Any;
use std::fmt::{self, Debug};
use std::sync::Arc;

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
use table::table::adapter::{DfRecordBatchStreamAdapter, RecordBatchStreamAdapter};

use crate::error::Result;
use crate::executor::Runtime;
use crate::plan::{Partitioning, PhysicalPlan};
use crate::query_engine::datafusion::error;

/// Datafusion ExecutionPlan -> greptime PhysicalPlan
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

        Ok(Box::pin(RecordBatchStreamAdapter::new(df_stream)))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Greptime PhysicalPlan -> datafusion ExecutionPlan.
struct ExecutionPlanAdapter {
    plan: Arc<dyn PhysicalPlan>,
    schema: SchemaRef,
}

impl Debug for ExecutionPlanAdapter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        //TODO(dennis) better debug info
        write!(f, "ExecutionPlan(PlaceHolder)")
    }
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
