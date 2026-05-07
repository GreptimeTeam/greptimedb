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

use std::fmt::Write;

use api::v1::meta::{
    BatchDeleteRequest as PbBatchDeleteRequest, BatchDeleteResponse as PbBatchDeleteResponse,
    BatchGetRequest as PbBatchGetRequest, BatchGetResponse as PbBatchGetResponse,
    BatchPutRequest as PbBatchPutRequest, BatchPutResponse as PbBatchPutResponse,
    CompareAndPutRequest as PbCompareAndPutRequest,
    CompareAndPutResponse as PbCompareAndPutResponse, DeleteRangeRequest as PbDeleteRangeRequest,
    DeleteRangeResponse as PbDeleteRangeResponse, KeyValue as PbKeyValue,
    PutRequest as PbPutRequest, PutResponse as PbPutResponse, RangeRequest as PbRangeRequest,
    RangeResponse as PbRangeResponse, ResponseHeader, store_server,
};
use common_meta::kv_backend::KvBackend;
use common_meta::rpc::store::{
    BatchDeleteRequest, BatchGetRequest, BatchPutRequest, CompareAndPutRequest, DeleteRangeRequest,
    PutRequest, RangeRequest,
};
use snafu::ResultExt;
use tonic::{Request, Response};

use crate::error;
use crate::metasrv::Metasrv;
use crate::metrics::METRIC_META_KV_REQUEST_ELAPSED;
use crate::service::GrpcResult;

const MAX_LOGGED_KEY_BYTES: usize = 16;

trait StoreWriteRequestSummary {
    fn summary(&self) -> String;
}

fn key_summary(key: &[u8]) -> String {
    let preview_len = key.len().min(MAX_LOGGED_KEY_BYTES);
    let mut hex_prefix = String::with_capacity(preview_len * 2 + 3);
    for byte in &key[..preview_len] {
        let _ = write!(&mut hex_prefix, "{byte:02x}");
    }
    if key.len() > preview_len {
        hex_prefix.push_str("...");
    }

    format!("len={}, hex_prefix={}", key.len(), hex_prefix)
}

fn optional_key_summary(key: Option<&[u8]>) -> String {
    key.map(key_summary).unwrap_or_else(|| "none".to_string())
}

fn total_value_len(kvs: &[PbKeyValue]) -> usize {
    kvs.iter()
        .fold(0usize, |total, kv| total.saturating_add(kv.value.len()))
}

impl StoreWriteRequestSummary for PbPutRequest {
    fn summary(&self) -> String {
        format!(
            "header={:?}, key=({}), value_len={}, prev_kv={}",
            self.header,
            key_summary(&self.key),
            self.value.len(),
            self.prev_kv
        )
    }
}

impl StoreWriteRequestSummary for PbBatchPutRequest {
    fn summary(&self) -> String {
        format!(
            "header={:?}, kv_count={}, first_key={}, total_value_len={}, prev_kv={}",
            self.header,
            self.kvs.len(),
            optional_key_summary(self.kvs.first().map(|kv| kv.key.as_slice())),
            total_value_len(&self.kvs),
            self.prev_kv
        )
    }
}

impl StoreWriteRequestSummary for PbBatchDeleteRequest {
    fn summary(&self) -> String {
        format!(
            "header={:?}, key_count={}, first_key={}, prev_kv={}",
            self.header,
            self.keys.len(),
            optional_key_summary(self.keys.first().map(Vec::as_slice)),
            self.prev_kv
        )
    }
}

impl StoreWriteRequestSummary for PbCompareAndPutRequest {
    fn summary(&self) -> String {
        format!(
            "header={:?}, key=({}), expect_len={}, value_len={}",
            self.header,
            key_summary(&self.key),
            self.expect.len(),
            self.value.len()
        )
    }
}

impl StoreWriteRequestSummary for PbDeleteRangeRequest {
    fn summary(&self) -> String {
        format!(
            "header={:?}, key=({}), range_end=({}), prev_kv={}",
            self.header,
            key_summary(&self.key),
            key_summary(&self.range_end),
            self.prev_kv
        )
    }
}

macro_rules! check_store_leader {
    ($self:expr, $request:expr, $resp_ty:ty, $msg:expr) => {
        if !$self.is_leader() {
            common_telemetry::warn!(
                "The current metasrv is not the leader, but a {} request has reached the meta. Detail: {}.",
                $msg,
                StoreWriteRequestSummary::summary($request.get_ref())
            );
            let mut resp: $resp_ty = Default::default();
            resp.header = Some(api::v1::meta::ResponseHeader::failed(
                api::v1::meta::Error::is_not_leader(),
            ));
            return Ok(tonic::Response::new(resp));
        }
    };
}

macro_rules! check_leader_cache_ready {
    ($self:expr, $request:expr, $resp_ty:ty, $msg:expr) => {
        if !$self.is_leader_cache_ready() {
            common_telemetry::warn!(
                "The current metasrv leader cache is not ready, but a {} request has reached the meta. Detail: {}.",
                $msg,
                StoreWriteRequestSummary::summary($request.get_ref())
            );
            let mut resp: $resp_ty = Default::default();
            resp.header = Some(api::v1::meta::ResponseHeader::failed(
                api::v1::meta::Error::is_not_leader(),
            ));
            return Ok(tonic::Response::new(resp));
        }
    };
}

#[async_trait::async_trait]
impl store_server::Store for Metasrv {
    async fn range(&self, req: Request<PbRangeRequest>) -> GrpcResult<PbRangeResponse> {
        let req = req.into_inner();

        let _timer = METRIC_META_KV_REQUEST_ELAPSED
            .with_label_values(&[self.kv_backend().name(), "range"])
            .start_timer();

        let req: RangeRequest = req.into();

        let res = self
            .kv_backend()
            .range(req)
            .await
            .context(error::KvBackendSnafu)?;

        let res = res.to_proto_resp(ResponseHeader::success());
        Ok(Response::new(res))
    }

    async fn put(&self, req: Request<PbPutRequest>) -> GrpcResult<PbPutResponse> {
        check_store_leader!(self, req, PbPutResponse, "`put`");
        check_leader_cache_ready!(self, req, PbPutResponse, "`put`");

        let req = req.into_inner();
        let _timer = METRIC_META_KV_REQUEST_ELAPSED
            .with_label_values(&[self.leader_cached_kv_backend().name(), "put"])
            .start_timer();

        let req: PutRequest = req.into();

        let res = self
            .leader_cached_kv_backend()
            .put(req)
            .await
            .context(error::KvBackendSnafu)?;

        let res = res.to_proto_resp(ResponseHeader::success());
        Ok(Response::new(res))
    }

    async fn batch_get(&self, req: Request<PbBatchGetRequest>) -> GrpcResult<PbBatchGetResponse> {
        let req = req.into_inner();
        let _timer = METRIC_META_KV_REQUEST_ELAPSED
            .with_label_values(&[self.kv_backend().name(), "batch_get"])
            .start_timer();

        let req: BatchGetRequest = req.into();

        let res = self
            .kv_backend()
            .batch_get(req)
            .await
            .context(error::KvBackendSnafu)?;

        let res = res.to_proto_resp(ResponseHeader::success());
        Ok(Response::new(res))
    }

    async fn batch_put(&self, req: Request<PbBatchPutRequest>) -> GrpcResult<PbBatchPutResponse> {
        check_store_leader!(self, req, PbBatchPutResponse, "`batch_put`");
        check_leader_cache_ready!(self, req, PbBatchPutResponse, "`batch_put`");

        let req = req.into_inner();

        let _timer = METRIC_META_KV_REQUEST_ELAPSED
            .with_label_values(&[self.leader_cached_kv_backend().name(), "batch_put"])
            .start_timer();

        let req: BatchPutRequest = req.into();

        let res = self
            .leader_cached_kv_backend()
            .batch_put(req)
            .await
            .context(error::KvBackendSnafu)?;

        let res = res.to_proto_resp(ResponseHeader::success());
        Ok(Response::new(res))
    }

    async fn batch_delete(
        &self,
        req: Request<PbBatchDeleteRequest>,
    ) -> GrpcResult<PbBatchDeleteResponse> {
        check_store_leader!(self, req, PbBatchDeleteResponse, "`batch_delete`");
        check_leader_cache_ready!(self, req, PbBatchDeleteResponse, "`batch_delete`");

        let req = req.into_inner();

        let _timer = METRIC_META_KV_REQUEST_ELAPSED
            .with_label_values(&[self.leader_cached_kv_backend().name(), "batch_delete"])
            .start_timer();

        let req: BatchDeleteRequest = req.into();

        let res = self
            .leader_cached_kv_backend()
            .batch_delete(req)
            .await
            .context(error::KvBackendSnafu)?;

        let res = res.to_proto_resp(ResponseHeader::success());
        Ok(Response::new(res))
    }

    async fn compare_and_put(
        &self,
        req: Request<PbCompareAndPutRequest>,
    ) -> GrpcResult<PbCompareAndPutResponse> {
        check_store_leader!(self, req, PbCompareAndPutResponse, "`compare_and_put`");
        check_leader_cache_ready!(self, req, PbCompareAndPutResponse, "`compare_and_put`");

        let req = req.into_inner();

        let _timer = METRIC_META_KV_REQUEST_ELAPSED
            .with_label_values(&[self.leader_cached_kv_backend().name(), "compare_and_put"])
            .start_timer();

        let req: CompareAndPutRequest = req.into();

        let res = self
            .leader_cached_kv_backend()
            .compare_and_put(req)
            .await
            .context(error::KvBackendSnafu)?;

        let res = res.to_proto_resp(ResponseHeader::success());
        Ok(Response::new(res))
    }

    async fn delete_range(
        &self,
        req: Request<PbDeleteRangeRequest>,
    ) -> GrpcResult<PbDeleteRangeResponse> {
        check_store_leader!(self, req, PbDeleteRangeResponse, "`delete_range`");
        check_leader_cache_ready!(self, req, PbDeleteRangeResponse, "`delete_range`");

        let req = req.into_inner();

        let _timer = METRIC_META_KV_REQUEST_ELAPSED
            .with_label_values(&[self.leader_cached_kv_backend().name(), "delete_range"])
            .start_timer();

        let req: DeleteRangeRequest = req.into();

        let res = self
            .leader_cached_kv_backend()
            .delete_range(req)
            .await
            .context(error::KvBackendSnafu)?;

        let res = res.to_proto_resp(ResponseHeader::success());
        Ok(Response::new(res))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::v1::meta::store_server::Store;
    use api::v1::meta::*;
    use common_meta::election::{Election, LeaderChangeMessage, LeaderValue, MetasrvNodeInfo};
    use common_meta::kv_backend::memory::MemoryKvBackend;
    use common_telemetry::tracing_context::W3cTrace;
    use tokio::sync::broadcast;
    use tonic::IntoRequest;

    use super::{StoreWriteRequestSummary, key_summary};
    use crate::metasrv::Metasrv;
    use crate::metasrv::builder::MetasrvBuilder;

    struct FixedElection {
        is_leader: bool,
    }

    #[async_trait::async_trait]
    impl Election for FixedElection {
        type Leader = LeaderValue;

        fn is_leader(&self) -> bool {
            self.is_leader
        }

        fn in_leader_infancy(&self) -> bool {
            false
        }

        async fn register_candidate(
            &self,
            _node_info: &MetasrvNodeInfo,
        ) -> common_meta::error::Result<()> {
            Ok(())
        }

        async fn all_candidates(&self) -> common_meta::error::Result<Vec<MetasrvNodeInfo>> {
            Ok(vec![])
        }

        async fn campaign(&self) -> common_meta::error::Result<()> {
            Ok(())
        }

        async fn leader(&self) -> common_meta::error::Result<Self::Leader> {
            Ok(LeaderValue("leader".to_string()))
        }

        async fn resign(&self) -> common_meta::error::Result<()> {
            Ok(())
        }

        fn subscribe_leader_change(&self) -> broadcast::Receiver<LeaderChangeMessage> {
            let (_tx, rx) = broadcast::channel(1);
            rx
        }
    }

    async fn new_metasrv() -> Metasrv {
        MetasrvBuilder::new()
            .kv_backend(Arc::new(MemoryKvBackend::new()))
            .build()
            .await
            .unwrap()
    }

    async fn new_follower_metasrv() -> Metasrv {
        MetasrvBuilder::new()
            .election(Some(Arc::new(FixedElection { is_leader: false })))
            .kv_backend(Arc::new(MemoryKvBackend::new()))
            .build()
            .await
            .unwrap()
    }

    async fn new_leader_without_ready_cache_metasrv() -> Metasrv {
        MetasrvBuilder::new()
            .election(Some(Arc::new(FixedElection { is_leader: true })))
            .kv_backend(Arc::new(MemoryKvBackend::new()))
            .build()
            .await
            .unwrap()
    }

    #[test]
    fn test_key_summary_truncates_hex_prefix() {
        let key: Vec<u8> = (0..20).collect();

        assert_eq!(
            "len=20, hex_prefix=000102030405060708090a0b0c0d0e0f...",
            key_summary(&key)
        );
    }

    #[test]
    fn test_store_write_request_summary_redacts_values() {
        let put = PutRequest {
            key: b"k1".to_vec(),
            value: b"secret-value".to_vec(),
            prev_kv: true,
            ..Default::default()
        };
        let summary = StoreWriteRequestSummary::summary(&put);
        assert!(summary.contains("key=(len=2, hex_prefix=6b31)"));
        assert!(summary.contains("value_len=12"));
        assert!(!summary.contains("secret-value"));
        assert!(!summary.contains("value:"));

        let compare_and_put = CompareAndPutRequest {
            key: b"k2".to_vec(),
            expect: b"old-secret".to_vec(),
            value: b"new-secret".to_vec(),
            ..Default::default()
        };
        let summary = StoreWriteRequestSummary::summary(&compare_and_put);
        assert!(summary.contains("expect_len=10"));
        assert!(summary.contains("value_len=10"));
        assert!(!summary.contains("old-secret"));
        assert!(!summary.contains("new-secret"));
        assert!(!summary.contains("expect:"));
        assert!(!summary.contains("value:"));
    }

    #[tokio::test]
    async fn test_range() {
        let metasrv = new_metasrv().await;

        let mut req = RangeRequest::default();
        req.set_header(1, Role::Datanode, W3cTrace::new());
        let res = metasrv.range(req.into_request()).await;

        let _ = res.unwrap();
    }

    #[tokio::test]
    async fn test_put() {
        let metasrv = new_metasrv().await;

        let mut req = PutRequest::default();
        req.set_header(1, Role::Datanode, W3cTrace::new());
        let res = metasrv.put(req.into_request()).await;

        let _ = res.unwrap();
    }

    #[tokio::test]
    async fn test_batch_get() {
        let metasrv = new_metasrv().await;

        let mut req = BatchGetRequest::default();
        req.set_header(1, Role::Datanode, W3cTrace::new());
        let res = metasrv.batch_get(req.into_request()).await;

        let _ = res.unwrap();
    }

    #[tokio::test]
    async fn test_batch_put() {
        common_telemetry::init_default_ut_logging();
        let metasrv = new_metasrv().await;

        let mut req = BatchPutRequest::default();
        req.set_header(1, Role::Datanode, W3cTrace::new());
        let res = metasrv.batch_put(req.into_request()).await;

        let _ = res.unwrap();
    }

    #[tokio::test]
    async fn test_batch_delete() {
        let metasrv = new_metasrv().await;

        let mut req = BatchDeleteRequest::default();
        req.set_header(1, Role::Datanode, W3cTrace::new());
        let res = metasrv.batch_delete(req.into_request()).await;

        let _ = res.unwrap();
    }

    #[tokio::test]
    async fn test_compare_and_put() {
        let metasrv = new_metasrv().await;

        let mut req = CompareAndPutRequest::default();
        req.set_header(1, Role::Datanode, W3cTrace::new());
        let res = metasrv.compare_and_put(req.into_request()).await;

        let _ = res.unwrap();
    }

    #[tokio::test]
    async fn test_delete_range() {
        let metasrv = new_metasrv().await;

        let mut req = DeleteRangeRequest::default();
        req.set_header(1, Role::Datanode, W3cTrace::new());
        let res = metasrv.delete_range(req.into_request()).await;

        let _ = res.unwrap();
    }

    #[tokio::test]
    async fn test_write_requests_rejected_when_not_leader() {
        let metasrv = new_follower_metasrv().await;

        let mut req = PutRequest {
            key: b"k1".to_vec(),
            value: b"v1".to_vec(),
            ..Default::default()
        };
        req.set_header(1, Role::Datanode, W3cTrace::new());
        assert!(
            metasrv
                .put(req.into_request())
                .await
                .unwrap()
                .into_inner()
                .header
                .unwrap()
                .is_not_leader()
        );

        let mut req = BatchPutRequest::default();
        req.set_header(1, Role::Datanode, W3cTrace::new());
        assert!(
            metasrv
                .batch_put(req.into_request())
                .await
                .unwrap()
                .into_inner()
                .header
                .unwrap()
                .is_not_leader()
        );

        let mut req = BatchDeleteRequest::default();
        req.set_header(1, Role::Datanode, W3cTrace::new());
        assert!(
            metasrv
                .batch_delete(req.into_request())
                .await
                .unwrap()
                .into_inner()
                .header
                .unwrap()
                .is_not_leader()
        );

        let mut req = CompareAndPutRequest::default();
        req.set_header(1, Role::Datanode, W3cTrace::new());
        assert!(
            metasrv
                .compare_and_put(req.into_request())
                .await
                .unwrap()
                .into_inner()
                .header
                .unwrap()
                .is_not_leader()
        );

        let mut req = DeleteRangeRequest::default();
        req.set_header(1, Role::Datanode, W3cTrace::new());
        assert!(
            metasrv
                .delete_range(req.into_request())
                .await
                .unwrap()
                .into_inner()
                .header
                .unwrap()
                .is_not_leader()
        );
    }

    #[tokio::test]
    async fn test_write_requests_rejected_when_leader_cache_not_ready() {
        let metasrv = new_leader_without_ready_cache_metasrv().await;

        let mut req = PutRequest {
            key: b"k1".to_vec(),
            value: b"v1".to_vec(),
            ..Default::default()
        };
        req.set_header(1, Role::Datanode, W3cTrace::new());
        assert!(
            metasrv
                .put(req.into_request())
                .await
                .unwrap()
                .into_inner()
                .header
                .unwrap()
                .is_not_leader()
        );

        let mut req = BatchPutRequest::default();
        req.set_header(1, Role::Datanode, W3cTrace::new());
        assert!(
            metasrv
                .batch_put(req.into_request())
                .await
                .unwrap()
                .into_inner()
                .header
                .unwrap()
                .is_not_leader()
        );

        let mut req = BatchDeleteRequest::default();
        req.set_header(1, Role::Datanode, W3cTrace::new());
        assert!(
            metasrv
                .batch_delete(req.into_request())
                .await
                .unwrap()
                .into_inner()
                .header
                .unwrap()
                .is_not_leader()
        );

        let mut req = CompareAndPutRequest::default();
        req.set_header(1, Role::Datanode, W3cTrace::new());
        assert!(
            metasrv
                .compare_and_put(req.into_request())
                .await
                .unwrap()
                .into_inner()
                .header
                .unwrap()
                .is_not_leader()
        );

        let mut req = DeleteRangeRequest::default();
        req.set_header(1, Role::Datanode, W3cTrace::new());
        assert!(
            metasrv
                .delete_range(req.into_request())
                .await
                .unwrap()
                .into_inner()
                .header
                .unwrap()
                .is_not_leader()
        );
    }
}
