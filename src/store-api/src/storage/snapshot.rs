use async_trait::async_trait;
use common_error::ext::ErrorExt;
use datatypes::schema::SchemaRef;

use crate::storage::chunk::ChunkReader;
use crate::storage::requests::{GetRequest, ScanRequest};
use crate::storage::responses::{GetResponse, ScanResponse};

/// A consistent read-only view of region.
#[async_trait]
pub trait Snapshot: Send + Sync {
    type Error: ErrorExt + Send + Sync;
    type Reader: ChunkReader;

    fn schema(&self) -> &SchemaRef;

    async fn scan(
        &self,
        ctx: &ReadContext,
        request: ScanRequest,
    ) -> Result<ScanResponse<Self::Reader>, Self::Error>;

    async fn get(&self, ctx: &ReadContext, request: GetRequest)
        -> Result<GetResponse, Self::Error>;
}

/// Context for read.
#[derive(Debug, Clone)]
pub struct ReadContext {
    /// Suggested batch size of chunk.
    pub batch_size: usize,
}

impl Default for ReadContext {
    fn default() -> ReadContext {
        ReadContext { batch_size: 256 }
    }
}
