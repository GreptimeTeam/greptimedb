use common_error::ext::ErrorExt;
use datatypes::schema::SchemaRef;

use crate::storage::column_family::ColumnFamily;
use crate::storage::requests::{GetRequest, ScanRequest};
use crate::storage::responses::{GetResponse, ScanResponse};

/// A consistent read-only view of region.
pub trait Snapshot: Send + Sync {
    type Error: ErrorExt + Send + Sync;
    type ColumnFamily: ColumnFamily;

    fn schema(&self) -> &SchemaRef;

    fn scan(&self, ctx: &ReadContext, request: ScanRequest) -> Result<ScanResponse, Self::Error>;

    fn get(&self, ctx: &ReadContext, request: GetRequest) -> Result<GetResponse, Self::Error>;

    /// List all column families.
    fn list_cf(&self) -> Result<Vec<Self::ColumnFamily>, Self::Error>;
}

/// Context for read.
#[derive(Debug, Clone)]
pub struct ReadContext {}
