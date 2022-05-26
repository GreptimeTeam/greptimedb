use datatypes::vectors::VectorRef;

/// Write request holds a collection of updates to apply to a region.
pub trait WriteRequest: Send {
    type PutOp: PutOperation;

    fn new() -> Self;

    fn put(&mut self, put: Self::PutOp);
}

/// Put multiple rows.
pub trait PutOperation {
    type Error;

    fn new() -> Self;

    fn with_num_columns(num_columns: usize) -> Self;

    fn add_key_column(&mut self, name: &str, vector: VectorRef) -> Result<(), Self::Error>;

    fn add_version_column(&mut self, vector: VectorRef) -> Result<(), Self::Error>;

    fn add_value_column(&mut self, name: &str, vector: VectorRef) -> Result<(), Self::Error>;
}

#[derive(Debug)]
pub struct ScanRequest {}

#[derive(Debug)]
pub struct GetRequest {}
