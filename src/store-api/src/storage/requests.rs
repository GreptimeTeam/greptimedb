use common_error::ext::ErrorExt;
use datatypes::schema::SchemaRef;
use datatypes::vectors::VectorRef;

use crate::storage::SequenceNumber;

/// Write request holds a collection of updates to apply to a region.
pub trait WriteRequest: Send {
    type Error: ErrorExt + Send + Sync;
    type PutOp: PutOperation;

    fn new(schema: SchemaRef) -> Self;

    fn put(&mut self, put: Self::PutOp) -> Result<(), Self::Error>;
}

/// Put multiple rows.
pub trait PutOperation: Send {
    type Error: ErrorExt + Send + Sync;

    fn new() -> Self;

    fn with_num_columns(num_columns: usize) -> Self;

    fn add_key_column(&mut self, name: &str, vector: VectorRef) -> Result<(), Self::Error>;

    fn add_version_column(&mut self, vector: VectorRef) -> Result<(), Self::Error>;

    fn add_value_column(&mut self, name: &str, vector: VectorRef) -> Result<(), Self::Error>;
}

#[derive(Debug, Default)]
pub struct ScanRequest {
    /// Max sequence number to read, None for latest sequence.
    ///
    /// Default is None. Only returns data whose sequence number is less than or
    /// equal to the `sequence`.
    pub sequence: Option<SequenceNumber>,
}

#[derive(Debug)]
pub struct GetRequest {}
