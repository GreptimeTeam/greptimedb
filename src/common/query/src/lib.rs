use common_recordbatch::{RecordBatches, SendableRecordBatchStream};

pub mod columnar_value;
pub mod error;
mod function;
pub mod logical_plan;
pub mod physical_plan;
pub mod prelude;
mod signature;

// sql output
pub enum Output {
    AffectedRows(usize),
    RecordBatches(RecordBatches),
    Stream(SendableRecordBatchStream),
}

pub use datafusion::physical_plan::ExecutionPlan as DfPhysicalPlan;
