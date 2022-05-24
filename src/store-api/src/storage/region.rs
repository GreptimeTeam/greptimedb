//! Region holds chunks of rows stored in the storage engine, but does not require that
//! rows must have continuous primary key range, which is implementation specific.
//!
//! Regions support operations like PUT/DELETE/SCAN that most key-value stores provide.
//! However, unlike key-value store, data stored in region has data model like:
//!
//! ```text
//! colk-1, ..., colk-m, timestamp, version -> colv-1, ..., colv-n
//! ```
//!
//! The data model require each row
//! - has 0 ~ m key column
//! - **MUST** has a timestamp column
//! - has a version column
//! - has 0 ~ n value column
//!
//! Each row is identified by (value of key columns, timestamp, version), which forms
//! a row key. Note that the implementation may allow multiple rows have same row
//! key (like ClickHouse), which is useful in analytic scenario.

use async_trait::async_trait;
use common_error::ext::ErrorExt;

use crate::storage::column_family::ColumnFamily;
use crate::storage::requests::WriteRequest;
use crate::storage::responses::WriteResponse;
use crate::storage::snapshot::{ReadContext, Snapshot};
use crate::storage::SchemaRef;

/// Chunks of rows in storage engine.
#[async_trait]
pub trait Region: Send + Sync + Clone {
    type Error: ErrorExt + Send + Sync;
    type WriteRequest: WriteRequest;
    type ColumnFamily: ColumnFamily;
    type Snapshot: Snapshot;

    /// Returns the schema snapshot of this region.
    fn schema(&self) -> SchemaRef;

    /// List all column families.
    fn list_cf(&self) -> Result<Vec<Self::ColumnFamily>, Self::Error>;

    /// Write updates to region.
    async fn write(
        &self,
        ctx: &WriteContext,
        request: Self::WriteRequest,
    ) -> Result<WriteResponse, Self::Error>;

    /// Create a snapshot for read.
    fn snapshot(&self, ctx: &ReadContext) -> Result<Self::Snapshot, Self::Error>;
}

/// Context for write operations.
#[derive(Debug, Clone)]
pub struct WriteContext {}
