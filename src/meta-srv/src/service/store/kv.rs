use std::sync::Arc;

use api::v1::meta::BatchPutRequest;
use api::v1::meta::BatchPutResponse;
use api::v1::meta::CompareAndPutRequest;
use api::v1::meta::CompareAndPutResponse;
use api::v1::meta::DeleteRangeRequest;
use api::v1::meta::DeleteRangeResponse;
use api::v1::meta::PutRequest;
use api::v1::meta::PutResponse;
use api::v1::meta::RangeRequest;
use api::v1::meta::RangeResponse;

use crate::error::Result;

pub type KvStoreRef = Arc<dyn KvStore>;

#[async_trait::async_trait]
pub trait KvStore: Send + Sync {
    async fn range(&self, req: RangeRequest) -> Result<RangeResponse>;

    async fn put(&self, req: PutRequest) -> Result<PutResponse>;

    async fn batch_put(&self, req: BatchPutRequest) -> Result<BatchPutResponse>;

    async fn compare_and_put(&self, req: CompareAndPutRequest) -> Result<CompareAndPutResponse>;

    async fn delete_range(&self, req: DeleteRangeRequest) -> Result<DeleteRangeResponse>;
}
