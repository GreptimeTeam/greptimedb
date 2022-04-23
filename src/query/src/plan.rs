use datafusion::{
    logical_plan::LogicalPlan as DfLogicalPlan, physical_plan::ExecutionPlan as DfExecutionPlan,
};
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

// `ExecutionPlan` represent nodes in the QueryEngine Physical Plan.
///
pub enum ExecutionPlan {
    DfPlan(Arc<dyn DfExecutionPlan>),
}
