// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

pub mod cached_kv;
pub mod etcd;
pub(crate) mod etcd_util;
pub mod kv;
pub mod memory;

use api::v1::meta::{
    store_server, BatchDeleteRequest as PbBatchDeleteRequest,
    BatchDeleteResponse as PbBatchDeleteResponse, BatchGetRequest as PbBatchGetRequest,
    BatchGetResponse as PbBatchGetResponse, BatchPutRequest as PbBatchPutRequest,
    BatchPutResponse as PbBatchPutResponse, CompareAndPutRequest as PbCompareAndPutRequest,
    CompareAndPutResponse as PbCompareAndPutResponse, DeleteRangeRequest as PbDeleteRangeRequest,
    DeleteRangeResponse as PbDeleteRangeResponse, PutRequest as PbPutRequest,
    PutResponse as PbPutResponse, RangeRequest as PbRangeRequest, RangeResponse as PbRangeResponse,
    ResponseHeader,
};
use common_meta::rpc::store::{
    BatchDeleteRequest, BatchGetRequest, BatchPutRequest, CompareAndPutRequest, DeleteRangeRequest,
    PutRequest, RangeRequest,
};
use common_telemetry::timer;
use snafu::OptionExt;
use tonic::{Request, Response};

use crate::error::MissingRequestHeaderSnafu;
use crate::metasrv::MetaSrv;
use crate::metrics::METRIC_META_KV_REQUEST;
use crate::service::GrpcResult;

#[async_trait::async_trait]
impl store_server::Store for MetaSrv {
    async fn range(&self, req: Request<PbRangeRequest>) -> GrpcResult<PbRangeResponse> {
        let req = req.into_inner();

        let cluster_id = req
            .header
            .as_ref()
            .context(MissingRequestHeaderSnafu)?
            .cluster_id;

        let _timer = timer!(
            METRIC_META_KV_REQUEST,
            &[
                ("target", self.kv_store().name().to_string()),
                ("op", "range".to_string()),
                ("cluster_id", cluster_id.to_string()),
            ]
        );

        let req: RangeRequest = req.into();

        let res = self.kv_store().range(req).await?;

        let res = res.to_proto_resp(ResponseHeader::success(cluster_id));
        Ok(Response::new(res))
    }

    async fn put(&self, req: Request<PbPutRequest>) -> GrpcResult<PbPutResponse> {
        let req = req.into_inner();

        let cluster_id = req
            .header
            .as_ref()
            .context(MissingRequestHeaderSnafu)?
            .cluster_id;

        let _timer = timer!(
            METRIC_META_KV_REQUEST,
            &[
                ("target", self.kv_store().name().to_string()),
                ("op", "put".to_string()),
                ("cluster_id", cluster_id.to_string()),
            ]
        );

        let req: PutRequest = req.into();

        let res = self.kv_store().put(req).await?;

        let res = res.to_proto_resp(ResponseHeader::success(cluster_id));
        Ok(Response::new(res))
    }

    async fn batch_get(&self, req: Request<PbBatchGetRequest>) -> GrpcResult<PbBatchGetResponse> {
        let req = req.into_inner();

        let cluster_id = req
            .header
            .as_ref()
            .context(MissingRequestHeaderSnafu)?
            .cluster_id;

        let _timer = timer!(
            METRIC_META_KV_REQUEST,
            &[
                ("target", self.kv_store().name().to_string()),
                ("op", "batch_get".to_string()),
                ("cluster_id", cluster_id.to_string()),
            ]
        );

        let req: BatchGetRequest = req.into();

        let res = self.kv_store().batch_get(req).await?;

        let res = res.to_proto_resp(ResponseHeader::success(cluster_id));
        Ok(Response::new(res))
    }

    async fn batch_put(&self, req: Request<PbBatchPutRequest>) -> GrpcResult<PbBatchPutResponse> {
        let req = req.into_inner();

        let cluster_id = req
            .header
            .as_ref()
            .context(MissingRequestHeaderSnafu)?
            .cluster_id;

        let _timer = timer!(
            METRIC_META_KV_REQUEST,
            &[
                ("target", self.kv_store().name().to_string()),
                ("op", "batch_pub".to_string()),
                ("cluster_id", cluster_id.to_string()),
            ]
        );

        let req: BatchPutRequest = req.into();

        let res = self.kv_store().batch_put(req).await?;

        let res = res.to_proto_resp(ResponseHeader::success(cluster_id));
        Ok(Response::new(res))
    }

    async fn batch_delete(
        &self,
        req: Request<PbBatchDeleteRequest>,
    ) -> GrpcResult<PbBatchDeleteResponse> {
        let req = req.into_inner();

        let cluster_id = req
            .header
            .as_ref()
            .context(MissingRequestHeaderSnafu)?
            .cluster_id;

        let _timer = timer!(
            METRIC_META_KV_REQUEST,
            &[
                ("target", self.kv_store().name().to_string()),
                ("op", "batch_delete".to_string()),
                ("cluster_id", cluster_id.to_string()),
            ]
        );

        let req: BatchDeleteRequest = req.into();

        let res = self.kv_store().batch_delete(req).await?;

        let res = res.to_proto_resp(ResponseHeader::success(cluster_id));
        Ok(Response::new(res))
    }

    async fn compare_and_put(
        &self,
        req: Request<PbCompareAndPutRequest>,
    ) -> GrpcResult<PbCompareAndPutResponse> {
        let req = req.into_inner();

        let cluster_id = req
            .header
            .as_ref()
            .context(MissingRequestHeaderSnafu)?
            .cluster_id;

        let _timer = timer!(
            METRIC_META_KV_REQUEST,
            &[
                ("target", self.kv_store().name().to_string()),
                ("op", "compare_and_put".to_string()),
                ("cluster_id", cluster_id.to_string()),
            ]
        );

        let req: CompareAndPutRequest = req.into();

        let res = self.kv_store().compare_and_put(req).await?;

        let res = res.to_proto_resp(ResponseHeader::success(cluster_id));
        Ok(Response::new(res))
    }

    async fn delete_range(
        &self,
        req: Request<PbDeleteRangeRequest>,
    ) -> GrpcResult<PbDeleteRangeResponse> {
        let req = req.into_inner();

        let cluster_id = req
            .header
            .as_ref()
            .context(MissingRequestHeaderSnafu)?
            .cluster_id;

        let _timer = timer!(
            METRIC_META_KV_REQUEST,
            &[
                ("target", self.kv_store().name().to_string()),
                ("op", "delete_range".to_string()),
                ("cluster_id", cluster_id.to_string()),
            ]
        );

        let req: DeleteRangeRequest = req.into();

        let res = self.kv_store().delete_range(req).await?;

        let res = res.to_proto_resp(ResponseHeader::success(cluster_id));
        Ok(Response::new(res))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::v1::meta::store_server::Store;
    use api::v1::meta::*;
    use tonic::IntoRequest;

    use crate::metasrv::builder::MetaSrvBuilder;
    use crate::metasrv::MetaSrv;
    use crate::service::store::memory::MemStore;

    async fn new_meta_srv() -> MetaSrv {
        MetaSrvBuilder::new()
            .kv_store(Arc::new(MemStore::new()))
            .build()
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn test_range() {
        let meta_srv = new_meta_srv().await;

        let mut req = RangeRequest::default();
        req.set_header((1, 1), Role::Datanode);
        let res = meta_srv.range(req.into_request()).await;

        let _ = res.unwrap();
    }

    #[tokio::test]
    async fn test_put() {
        let meta_srv = new_meta_srv().await;

        let mut req = PutRequest::default();
        req.set_header((1, 1), Role::Datanode);
        let res = meta_srv.put(req.into_request()).await;

        let _ = res.unwrap();
    }

    #[tokio::test]
    async fn test_batch_get() {
        let meta_srv = new_meta_srv().await;

        let mut req = BatchGetRequest::default();
        req.set_header((1, 1), Role::Datanode);
        let res = meta_srv.batch_get(req.into_request()).await;

        let _ = res.unwrap();
    }

    #[tokio::test]
    async fn test_batch_put() {
        let meta_srv = new_meta_srv().await;

        let mut req = BatchPutRequest::default();
        req.set_header((1, 1), Role::Datanode);
        let res = meta_srv.batch_put(req.into_request()).await;

        let _ = res.unwrap();
    }

    #[tokio::test]
    async fn test_batch_delete() {
        let meta_srv = new_meta_srv().await;

        let mut req = BatchDeleteRequest::default();
        req.set_header((1, 1), Role::Datanode);
        let res = meta_srv.batch_delete(req.into_request()).await;

        let _ = res.unwrap();
    }

    #[tokio::test]
    async fn test_compare_and_put() {
        let meta_srv = new_meta_srv().await;

        let mut req = CompareAndPutRequest::default();
        req.set_header((1, 1), Role::Datanode);
        let res = meta_srv.compare_and_put(req.into_request()).await;

        let _ = res.unwrap();
    }

    #[tokio::test]
    async fn test_delete_range() {
        let meta_srv = new_meta_srv().await;

        let mut req = DeleteRangeRequest::default();
        req.set_header((1, 1), Role::Datanode);
        let res = meta_srv.delete_range(req.into_request()).await;

        let _ = res.unwrap();
    }
}
