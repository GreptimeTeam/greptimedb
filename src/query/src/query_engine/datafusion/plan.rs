use crate::error::Result;
use crate::plan::{PhysicalPlan, SendableRecordBatchStream};
use datafusion::physical_plan::ExecutionPlan;
use datatypes::schema::SchemaRef;
use std::any::Any;
use std::sync::Arc;

pub struct DfPhysicalPlan {
    plan: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
}

impl DfPhysicalPlan {
    pub fn new(schema: SchemaRef, plan: Arc<dyn ExecutionPlan>) -> Self {
        Self { schema, plan }
    }

    #[inline]
    pub fn df_plan(&self) -> &Arc<dyn ExecutionPlan> {
        &self.plan
    }
}

#[async_trait::async_trait]
impl PhysicalPlan for DfPhysicalPlan {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalPlan>> {
        todo!();
    }

    fn with_new_children(
        &self,
        _children: Vec<Arc<dyn PhysicalPlan>>,
    ) -> Result<Arc<dyn PhysicalPlan>> {
        todo!();
    }

    async fn execute(&self, _partition: usize) -> Result<SendableRecordBatchStream> {
        todo!();
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
