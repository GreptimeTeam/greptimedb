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
//! - has 0 ~ m key column, parts of row key columns;
//! - **MUST** has a timestamp column, part of row key columns;
//! - has a version column, part of row key columns;
//! - has 0 ~ n value column.
//!
//! Each row is identified by (value of key columns, timestamp, version), which forms
//! a row key. Note that the implementation may allow multiple rows have same row
//! key (like ClickHouse), which is useful in analytic scenario.

use async_trait::async_trait;
use common_error::ext::ErrorExt;

use crate::storage::engine::OpenOptions;
use crate::storage::metadata::RegionMeta;
use crate::storage::requests::WriteRequest;
use crate::storage::responses::WriteResponse;
use crate::storage::snapshot::{ReadContext, Snapshot};

/// Chunks of rows in storage engine.
#[async_trait]
pub trait Region: Send + Sync + Clone + std::fmt::Debug + 'static {
    type Error: ErrorExt + Send + Sync;
    type Meta: RegionMeta;
    type WriteRequest: WriteRequest;
    type Snapshot: Snapshot;

    /// Returns name of the region.
    fn name(&self) -> &str;

    /// Returns the in memory metadata of this region.
    fn in_memory_metadata(&self) -> Self::Meta;

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
#[derive(Debug, Clone, Default)]
pub struct WriteContext {}

impl From<&OpenOptions> for WriteContext {
    fn from(_opts: &OpenOptions) -> WriteContext {
        WriteContext::default()
    }
}
