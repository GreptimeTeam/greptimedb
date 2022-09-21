pub mod etcd;
pub mod kv;

use api::v1::meta::{
    store_server, DeleteRangeRequest, DeleteRangeResponse, PutRequest, PutResponse, RangeRequest,
    RangeResponse,
};
use tonic::Request;

use super::{GrpcResult, MetaServer};

#[async_trait::async_trait]
impl store_server::Store for MetaServer {
    async fn range(&self, req: Request<RangeRequest>) -> GrpcResult<RangeResponse> {
        let req = req.into_inner();
        let res = self.kv_store.range(req).await?;
        Ok(tonic::Response::new(res))
    }

    async fn put(&self, req: Request<PutRequest>) -> GrpcResult<PutResponse> {
        let req = req.into_inner();
        let res = self.kv_store.put(req).await?;
        Ok(tonic::Response::new(res))
    }

    async fn delete_range(
        &self,
        req: Request<DeleteRangeRequest>,
    ) -> GrpcResult<DeleteRangeResponse> {
        let req = req.into_inner();
        let res = self.kv_store.delete_range(req).await?;
        Ok(tonic::Response::new(res))
    }
}
