use crate::error::Result;
use common_recordbatch::SendableRecordBatchStream;
use datafusion::logical_plan::LogicalPlan as DfLogicalPlan;
use datatypes::schema::SchemaRef;
use std::any::Any;
use std::sync::Arc;

/// A LogicalPlan represents the different types of relational
/// operators (such as Projection, Filter, etc) and can be created by
/// the SQL query planner and the DataFrame API.
///
/// A LogicalPlan represents transforming an input relation (table) to
/// an output relation (table) with a (potentially) different
/// schema. A plan represents a dataflow tree where data flows
/// from leaves up to the root to produce the query result.
#[derive(Clone)]
pub enum LogicalPlan {
    DfPlan(DfLogicalPlan),
}

#[async_trait::async_trait]
pub trait PhysicalPlan: Send + Sync + Any {
    /// Get the schema for this execution plan
    fn schema(&self) -> SchemaRef;

    /// Get a list of child execution plans that provide the input for this plan. The returned list
    /// will be empty for leaf nodes, will contain a single value for unary nodes, or two
    /// values for binary nodes (such as joins).
    fn children(&self) -> Vec<Arc<dyn PhysicalPlan>>;

    /// Returns a new plan where all children were replaced by new plans.
    /// The size of `children` must be equal to the size of `ExecutionPlan::children()`.
    fn with_new_children(
        &self,
        children: Vec<Arc<dyn PhysicalPlan>>,
    ) -> Result<Arc<dyn PhysicalPlan>>;

    /// creates an iterator
    async fn execute(&self, partition: usize) -> Result<SendableRecordBatchStream>;

    fn as_any(&self) -> &dyn Any;
}
