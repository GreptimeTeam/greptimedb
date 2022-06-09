use async_trait::async_trait;
use common_error::ext::ErrorExt;
use datatypes::schema::SchemaRef;

use crate::storage::requests::{GetRequest, ScanRequest};
use crate::storage::responses::{GetResponse, ScanResponse};

/// A consistent read-only view of region.
#[async_trait]
pub trait Snapshot: Send + Sync {
    type Error: ErrorExt + Send + Sync;

    fn schema(&self) -> &SchemaRef;

    async fn scan(
        &self,
        ctx: &ReadContext,
        request: ScanRequest,
    ) -> Result<ScanResponse, Self::Error>;

    async fn get(&self, ctx: &ReadContext, request: GetRequest)
        -> Result<GetResponse, Self::Error>;
}

/// Context for read.
#[derive(Debug, Clone)]
pub struct ReadContext {}
