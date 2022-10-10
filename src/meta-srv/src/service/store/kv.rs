use std::sync::Arc;

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

    async fn delete_range(&self, req: DeleteRangeRequest) -> Result<DeleteRangeResponse>;
}
