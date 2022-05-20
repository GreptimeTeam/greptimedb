use crate::storage::column_family::ColumnFamily;

/// Write request holds a collection of updates to apply to a region.
pub trait WriteRequest: Send {
    type ColumnFamily: ColumnFamily;
}

#[derive(Debug)]
pub struct ScanRequest {}

#[derive(Debug)]
pub struct GetRequest {}
