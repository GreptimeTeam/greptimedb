use std::time::Duration;

use common_error::ext::ErrorExt;
use common_query::logical_plan::Expr;
use common_time::RangeMillis;
use datatypes::vectors::VectorRef;

use crate::storage::SequenceNumber;

/// Write request holds a collection of updates to apply to a region.
pub trait WriteRequest: Send {
    type Error: ErrorExt + Send + Sync;
    type PutOp: PutOperation;

    fn put(&mut self, put: Self::PutOp) -> Result<(), Self::Error>;

    /// Returns all possible time ranges that contain the timestamp in this batch.
    ///
    /// Each time range is aligned to given `duration`.
    fn time_ranges(&self, duration: Duration) -> Result<Vec<RangeMillis>, Self::Error>;

    fn put_op(&self) -> Self::PutOp;

    fn put_op_with_columns(num_columns: usize) -> Self::PutOp;
}

/// Put multiple rows.
pub trait PutOperation: Send + std::fmt::Debug {
    type Error: ErrorExt + Send + Sync;

    fn add_key_column(&mut self, name: &str, vector: VectorRef) -> Result<(), Self::Error>;

    fn add_version_column(&mut self, vector: VectorRef) -> Result<(), Self::Error>;

    fn add_value_column(&mut self, name: &str, vector: VectorRef) -> Result<(), Self::Error>;
}

#[derive(Default)]
pub struct ScanRequest {
    /// Max sequence number to read, None for latest sequence.
    ///
    /// Default is None. Only returns data whose sequence number is less than or
    /// equal to the `sequence`.
    pub sequence: Option<SequenceNumber>,
    /// Indices of columns to read, `None` to read all columns.
    pub projection: Option<Vec<usize>>,
    /// Filters pushed down
    pub filters: Vec<Expr>,
}

#[derive(Debug)]
pub struct GetRequest {}
