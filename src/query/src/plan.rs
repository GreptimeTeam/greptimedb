use std::fmt::Debug;

use datafusion::logical_plan::LogicalPlan as DfLogicalPlan;

/// A LogicalPlan represents the different types of relational
/// operators (such as Projection, Filter, etc) and can be created by
/// the SQL query planner.
///
/// A LogicalPlan represents transforming an input relation (table) to
/// an output relation (table) with a (potentially) different
/// schema. A plan represents a dataflow tree where data flows
/// from leaves up to the root to produce the query result.
#[derive(Clone, Debug)]
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
