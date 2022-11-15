// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

pub mod etcd;
pub mod kv;
pub mod memory;

use api::v1::meta::{
    store_server, BatchPutRequest, BatchPutResponse, CompareAndPutRequest, CompareAndPutResponse,
    DeleteRangeRequest, DeleteRangeResponse, PutRequest, PutResponse, RangeRequest, RangeResponse,
};
use tonic::{Request, Response};

use crate::metasrv::MetaSrv;
use crate::service::GrpcResult;

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
    use crate::service::store::memory::MemStore;

    #[tokio::test]
    async fn test_range() {
        let kv_store = Arc::new(MemStore::new());
        let meta_srv = MetaSrv::new(MetaSrvOptions::default(), kv_store, None, None).await;
        let req = RangeRequest::default();
        let res = meta_srv.range(req.into_request()).await;

        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_put() {
        let kv_store = Arc::new(MemStore::new());
        let meta_srv = MetaSrv::new(MetaSrvOptions::default(), kv_store, None, None).await;
        let req = PutRequest::default();
        let res = meta_srv.put(req.into_request()).await;

        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_batch_put() {
        let kv_store = Arc::new(MemStore::new());
        let meta_srv = MetaSrv::new(MetaSrvOptions::default(), kv_store, None, None).await;
        let req = BatchPutRequest::default();
        let res = meta_srv.batch_put(req.into_request()).await;

        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_compare_and_put() {
        let kv_store = Arc::new(MemStore::new());
        let meta_srv = MetaSrv::new(MetaSrvOptions::default(), kv_store, None, None).await;
        let req = CompareAndPutRequest::default();
        let res = meta_srv.compare_and_put(req.into_request()).await;

        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_delete_range() {
        let kv_store = Arc::new(MemStore::new());
        let meta_srv = MetaSrv::new(MetaSrvOptions::default(), kv_store, None, None).await;
        let req = DeleteRangeRequest::default();
        let res = meta_srv.delete_range(req.into_request()).await;

        assert!(res.is_ok());
    }
}
