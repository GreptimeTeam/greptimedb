use api::v1::meta::DeleteRangeRequest;
use api::v1::meta::DeleteRangeResponse;
use api::v1::meta::PutRequest;
use api::v1::meta::PutResponse;
use api::v1::meta::RangeRequest;
use api::v1::meta::RangeResponse;

use super::kv::KvStore;
use crate::error::Result;

/// A noop kv_store which only for test
// TODO(jiachun): Add a test feature
#[derive(Clone)]
pub struct NoopKvStore;

#[async_trait::async_trait]
impl KvStore for NoopKvStore {
    async fn range(&self, _req: RangeRequest) -> Result<RangeResponse> {
        Ok(RangeResponse::default())
    }

    async fn put(&self, _req: PutRequest) -> Result<PutResponse> {
        Ok(PutResponse::default())
    }

    async fn delete_range(&self, _req: DeleteRangeRequest) -> Result<DeleteRangeResponse> {
        Ok(DeleteRangeResponse::default())
    }
}
