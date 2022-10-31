pub mod etcd;
pub mod kv;
#[cfg(test)]
pub(crate) mod noop;

use api::v1::meta::store_server;
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
use tonic::Request;
use tonic::Response;

use super::GrpcResult;
use crate::metasrv::MetaSrv;

#[async_trait::async_trait]
impl store_server::Store for MetaSrv {
    async fn range(&self, req: Request<RangeRequest>) -> GrpcResult<RangeResponse> {
        let req = req.into_inner();
        let res = self.kv_store().range(req).await?;

        Ok(Response::new(res))
    }

    async fn put(&self, req: Request<PutRequest>) -> GrpcResult<PutResponse> {
        let req = req.into_inner();
        let res = self.kv_store().put(req).await?;

        Ok(Response::new(res))
    }

    async fn batch_put(&self, req: Request<BatchPutRequest>) -> GrpcResult<BatchPutResponse> {
        let req = req.into_inner();
        let res = self.kv_store().batch_put(req).await?;

        Ok(Response::new(res))
    }

    async fn compare_and_put(
        &self,
        req: Request<CompareAndPutRequest>,
    ) -> GrpcResult<CompareAndPutResponse> {
        let req = req.into_inner();
        let res = self.kv_store().compare_and_put(req).await?;

        Ok(Response::new(res))
    }

    async fn delete_range(
        &self,
        req: Request<DeleteRangeRequest>,
    ) -> GrpcResult<DeleteRangeResponse> {
        let req = req.into_inner();
        let res = self.kv_store().delete_range(req).await?;

        Ok(Response::new(res))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::v1::meta::store_server::Store;
    use api::v1::meta::*;
    use tonic::IntoRequest;

    use super::*;
    use crate::metasrv::MetaSrvOptions;
    use crate::service::store::noop::NoopKvStore;

    #[tokio::test]
    async fn test_range() {
        let kv_store = Arc::new(NoopKvStore {});
        let meta_srv = MetaSrv::new(MetaSrvOptions::default(), kv_store).await;
        let req = RangeRequest::default();
        let res = meta_srv.range(req.into_request()).await;

        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_put() {
        let kv_store = Arc::new(NoopKvStore {});
        let meta_srv = MetaSrv::new(MetaSrvOptions::default(), kv_store).await;
        let req = PutRequest::default();
        let res = meta_srv.put(req.into_request()).await;

        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_batch_put() {
        let kv_store = Arc::new(NoopKvStore {});
        let meta_srv = MetaSrv::new(MetaSrvOptions::default(), kv_store).await;
        let req = BatchPutRequest::default();
        let res = meta_srv.batch_put(req.into_request()).await;

        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_compare_and_put() {
        let kv_store = Arc::new(NoopKvStore {});
        let meta_srv = MetaSrv::new(MetaSrvOptions::default(), kv_store).await;
        let req = CompareAndPutRequest::default();
        let res = meta_srv.compare_and_put(req.into_request()).await;

        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_delete_range() {
        let kv_store = Arc::new(NoopKvStore {});
        let meta_srv = MetaSrv::new(MetaSrvOptions::default(), kv_store).await;
        let req = DeleteRangeRequest::default();
        let res = meta_srv.delete_range(req.into_request()).await;

        assert!(res.is_ok());
    }
}
