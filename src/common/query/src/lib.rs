use common_recordbatch::{RecordBatches, SendableRecordBatchStream};

pub mod columnar_value;
pub mod error;
pub mod execution;
mod function;
pub mod logical_plan;
pub mod prelude;
mod signature;

// sql output
pub enum Output {
    AffectedRows(usize),
    RecordBatches(RecordBatches),
    Stream(SendableRecordBatchStream),
}

pub use datafusion::physical_plan::expressions::PhysicalSortExpr;
pub use datafusion::physical_plan::ExecutionPlan as DfExecutionPlan;
