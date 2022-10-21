pub mod etcd;
pub mod kv;

use api::v1::meta::store_server;
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
    use crate::service::store::kv::KvStore;

    struct MockKvStore;

    #[async_trait::async_trait]
    impl KvStore for MockKvStore {
        async fn range(&self, _req: RangeRequest) -> crate::Result<RangeResponse> {
            Ok(RangeResponse::default())
        }

        async fn put(&self, _req: PutRequest) -> crate::Result<PutResponse> {
            Ok(PutResponse::default())
        }

        async fn delete_range(
            &self,
            _req: DeleteRangeRequest,
        ) -> crate::Result<DeleteRangeResponse> {
            Ok(DeleteRangeResponse::default())
        }
    }

    #[tokio::test]
    async fn test_range() {
        let kv_store = Arc::new(MockKvStore {});
        let meta_srv = MetaSrv::new(MetaSrvOptions::default(), kv_store);
        let req = RangeRequest::default();
        let res = meta_srv.range(req.into_request()).await;

        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_put() {
        let kv_store = Arc::new(MockKvStore {});
        let meta_srv = MetaSrv::new(MetaSrvOptions::default(), kv_store);
        let req = PutRequest::default();
        let res = meta_srv.put(req.into_request()).await;

        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_delete_range() {
        let kv_store = Arc::new(MockKvStore {});
        let meta_srv = MetaSrv::new(MetaSrvOptions::default(), kv_store);
        let req = DeleteRangeRequest::default();
        let res = meta_srv.delete_range(req.into_request()).await;

        assert!(res.is_ok());
    }
}
