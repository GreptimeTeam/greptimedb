use std::any::Any;
use std::sync::Arc;

use common_recordbatch::SendableRecordBatchStream;
use datafusion::logical_plan::LogicalPlan as DfLogicalPlan;
use datatypes::schema::SchemaRef;

use crate::error::Result;
use crate::executor::Runtime;

/// A LogicalPlan represents the different types of relational
/// operators (such as Projection, Filter, etc) and can be created by
/// the SQL query planner.
///
/// A LogicalPlan represents transforming an input relation (table) to
/// an output relation (table) with a (potentially) different
/// schema. A plan represents a dataflow tree where data flows
/// from leaves up to the root to produce the query result.
#[derive(Clone)]
pub enum LogicalPlan {
    DfPlan(DfLogicalPlan),
}

/// Partitioning schemes supported by operators.
#[derive(Debug, Clone)]
pub enum Partitioning {
    /// Unknown partitioning scheme with a known number of partitions
    UnknownPartitioning(usize),
}

impl Partitioning {
    /// Returns the number of partitions in this partitioning scheme
    pub fn partition_count(&self) -> usize {
        use Partitioning::*;
        match self {
            UnknownPartitioning(n) => *n,
        }
    }
}

#[async_trait::async_trait]
pub trait PhysicalPlan: Send + Sync + Any {
    /// Get the schema for this execution plan
    fn schema(&self) -> SchemaRef;

    /// Specifies the output partitioning scheme of this plan
    fn output_partitioning(&self) -> Partitioning;

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
    async fn execute(
        &self,
        _runtime: &Runtime,
        partition: usize,
    ) -> Result<SendableRecordBatchStream>;

    fn as_any(&self) -> &dyn Any;
}
