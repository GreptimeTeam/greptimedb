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

use std::any::Any;

use crate::error::{ReadOnlyKvBackendSnafu, Result};
use crate::kv_backend::txn::{Txn, TxnOp, TxnRequest, TxnResponse};
use crate::kv_backend::{KvBackend, KvBackendRef, TxnService};
use crate::rpc::KeyValue;
use crate::rpc::store::{
    BatchDeleteRequest, BatchDeleteResponse, BatchGetRequest, BatchGetResponse, BatchPutRequest,
    BatchPutResponse, CompareAndPutRequest, CompareAndPutResponse, DeleteRangeRequest,
    DeleteRangeResponse, PutRequest, PutResponse, RangeRequest, RangeResponse,
};

/// A [`KvBackend`] wrapper that forwards reads and rejects writes.
pub struct ReadOnlyKvBackend {
    inner: KvBackendRef,
    name: String,
}

impl ReadOnlyKvBackend {
    pub fn new(inner: KvBackendRef) -> Self {
        let name = format!("ReadOnlyKvBackend({})", inner.name());
        Self { inner, name }
    }

    fn read_only<T>(&self) -> Result<T> {
        ReadOnlyKvBackendSnafu {
            name: self.name.clone(),
        }
        .fail()
    }

    fn validate_read_only_ops(&self, ops: &[TxnOp]) -> Result<()> {
        if ops
            .iter()
            .any(|op| matches!(op, TxnOp::Put(_, _) | TxnOp::Delete(_)))
        {
            self.read_only()
        } else {
            Ok(())
        }
    }

    fn validate_read_only_txn(&self, txn: &Txn) -> Result<()> {
        let TxnRequest {
            success, failure, ..
        } = txn.clone().into();

        self.validate_read_only_ops(&success)?;
        self.validate_read_only_ops(&failure)
    }
}

#[async_trait::async_trait]
impl TxnService for ReadOnlyKvBackend {
    type Error = crate::error::Error;

    async fn txn(&self, txn: Txn) -> Result<TxnResponse> {
        self.validate_read_only_txn(&txn)?;
        self.inner.txn(txn).await
    }

    fn max_txn_ops(&self) -> usize {
        self.inner.max_txn_ops()
    }
}

#[async_trait::async_trait]
impl KvBackend for ReadOnlyKvBackend {
    fn name(&self) -> &str {
        &self.name
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn range(&self, req: RangeRequest) -> Result<RangeResponse> {
        self.inner.range(req).await
    }

    async fn put(&self, _req: PutRequest) -> Result<PutResponse> {
        self.read_only()
    }

    async fn batch_put(&self, _req: BatchPutRequest) -> Result<BatchPutResponse> {
        self.read_only()
    }

    async fn batch_get(&self, req: BatchGetRequest) -> Result<BatchGetResponse> {
        self.inner.batch_get(req).await
    }

    async fn compare_and_put(&self, _req: CompareAndPutRequest) -> Result<CompareAndPutResponse> {
        self.read_only()
    }

    async fn delete_range(&self, _req: DeleteRangeRequest) -> Result<DeleteRangeResponse> {
        self.read_only()
    }

    async fn batch_delete(&self, _req: BatchDeleteRequest) -> Result<BatchDeleteResponse> {
        self.read_only()
    }

    async fn get(&self, key: &[u8]) -> Result<Option<KeyValue>> {
        self.inner.get(key).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_error::ext::ErrorExt;
    use common_error::status_code::StatusCode;

    use super::*;
    use crate::error::Error;
    use crate::kv_backend::memory::MemoryKvBackend;
    use crate::kv_backend::txn::{Compare, CompareOp, TxnOpResponse};
    use crate::rpc::store::{
        BatchDeleteRequest, BatchGetRequest, BatchPutRequest, CompareAndPutRequest,
        DeleteRangeRequest, PutRequest,
    };

    async fn read_only_backend() -> ReadOnlyKvBackend {
        let inner = Arc::new(MemoryKvBackend::<Error>::new());
        inner
            .put(PutRequest::new().with_key(b"k1").with_value(b"v1"))
            .await
            .unwrap();
        inner
            .put(PutRequest::new().with_key(b"k2").with_value(b"v2"))
            .await
            .unwrap();

        ReadOnlyKvBackend::new(inner)
    }

    fn assert_read_only<T>(result: Result<T>) {
        let err = match result {
            Ok(_) => panic!("expected read-only error"),
            Err(err) => err,
        };
        assert!(matches!(err, Error::ReadOnlyKvBackend { .. }));
        assert_eq!(err.status_code(), StatusCode::Unsupported);
    }

    struct TxnOnlyBackend;

    #[async_trait::async_trait]
    impl TxnService for TxnOnlyBackend {
        type Error = Error;

        async fn txn(&self, _txn: Txn) -> Result<TxnResponse> {
            Ok(TxnResponse {
                succeeded: true,
                responses: vec![TxnOpResponse::ResponseGet(RangeResponse {
                    kvs: vec![KeyValue {
                        key: b"k1".to_vec(),
                        value: b"v1".to_vec(),
                    }],
                    more: false,
                })],
            })
        }

        fn max_txn_ops(&self) -> usize {
            7
        }
    }

    #[async_trait::async_trait]
    impl KvBackend for TxnOnlyBackend {
        fn name(&self) -> &str {
            "TxnOnlyBackend"
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        async fn range(&self, _req: RangeRequest) -> Result<RangeResponse> {
            unimplemented!("read-only txn should delegate to inner txn")
        }

        async fn put(&self, _req: PutRequest) -> Result<PutResponse> {
            unimplemented!("read-only txn should delegate to inner txn")
        }

        async fn batch_put(&self, _req: BatchPutRequest) -> Result<BatchPutResponse> {
            unimplemented!("read-only txn should delegate to inner txn")
        }

        async fn batch_get(&self, _req: BatchGetRequest) -> Result<BatchGetResponse> {
            unimplemented!("read-only txn should delegate to inner txn")
        }

        async fn delete_range(&self, _req: DeleteRangeRequest) -> Result<DeleteRangeResponse> {
            unimplemented!("read-only txn should delegate to inner txn")
        }

        async fn batch_delete(&self, _req: BatchDeleteRequest) -> Result<BatchDeleteResponse> {
            unimplemented!("read-only txn should delegate to inner txn")
        }
    }

    #[tokio::test]
    async fn test_read_only_backend_forwards_reads() {
        let backend = read_only_backend().await;

        let range = backend
            .range(RangeRequest::new().with_key(b"k1"))
            .await
            .unwrap();
        assert_eq!(range.kvs.len(), 1);
        assert_eq!(range.kvs[0].value, b"v1");

        let kv = backend.get(b"k2").await.unwrap().unwrap();
        assert_eq!(kv.value, b"v2");

        let batch = backend
            .batch_get(BatchGetRequest::new().add_key(b"k1").add_key(b"k2"))
            .await
            .unwrap();
        assert_eq!(batch.kvs.len(), 2);
    }

    #[tokio::test]
    async fn test_read_only_backend_rejects_writes() {
        let backend = read_only_backend().await;

        assert_read_only(
            backend
                .put(PutRequest::new().with_key(b"k3").with_value(b"v3"))
                .await,
        );
        assert_read_only(
            backend
                .batch_put(BatchPutRequest::new().add_kv(b"k3", b"v3"))
                .await,
        );
        assert_read_only(
            backend
                .compare_and_put(
                    CompareAndPutRequest::new()
                        .with_key(b"k1")
                        .with_expect(b"v1")
                        .with_value(b"v3"),
                )
                .await,
        );
        assert_read_only(
            backend
                .delete_range(DeleteRangeRequest::new().with_key(b"k1"))
                .await,
        );
        assert_read_only(
            backend
                .batch_delete(BatchDeleteRequest::new().add_key(b"k1"))
                .await,
        );
    }

    #[tokio::test]
    async fn test_read_only_backend_rejects_write_txn() {
        let backend = read_only_backend().await;

        assert_eq!(backend.max_txn_ops(), usize::MAX);
        assert_read_only(
            backend
                .txn(Txn::put_if_not_exists(b"k3".to_vec(), b"v3".to_vec()))
                .await,
        );
    }

    #[tokio::test]
    async fn test_read_only_backend_delegates_read_txn() {
        let backend = ReadOnlyKvBackend::new(Arc::new(TxnOnlyBackend));

        assert_eq!(backend.max_txn_ops(), 7);
        let resp = backend
            .txn(Txn::new().and_then(vec![TxnOp::Get(b"k1".to_vec())]))
            .await
            .unwrap();

        assert!(resp.succeeded);
        let TxnOpResponse::ResponseGet(range) = &resp.responses[0] else {
            panic!("expected get response");
        };
        assert_eq!(range.kvs[0].value, b"v1");
    }

    #[tokio::test]
    async fn test_read_only_backend_allows_get_only_txn() {
        let backend = read_only_backend().await;

        let resp = backend
            .txn(Txn::new().and_then(vec![TxnOp::Get(b"k1".to_vec()), TxnOp::Get(b"k2".to_vec())]))
            .await
            .unwrap();

        assert!(resp.succeeded);
        assert_eq!(resp.responses.len(), 2);
        let TxnOpResponse::ResponseGet(range) = &resp.responses[0] else {
            panic!("expected get response");
        };
        assert_eq!(range.kvs.len(), 1);
        assert_eq!(range.kvs[0].value, b"v1");
        let TxnOpResponse::ResponseGet(range) = &resp.responses[1] else {
            panic!("expected get response");
        };
        assert_eq!(range.kvs.len(), 1);
        assert_eq!(range.kvs[0].value, b"v2");
    }

    #[tokio::test]
    async fn test_read_only_backend_allows_compare_and_get_txn() {
        let backend = read_only_backend().await;

        let txn = Txn::new()
            .when(vec![Compare::with_value(
                b"k1".to_vec(),
                CompareOp::Equal,
                b"v1".to_vec(),
            )])
            .and_then(vec![TxnOp::Get(b"k2".to_vec())])
            .or_else(vec![TxnOp::Get(b"k1".to_vec())]);
        let resp = backend.txn(txn).await.unwrap();

        assert!(resp.succeeded);
        let TxnOpResponse::ResponseGet(range) = &resp.responses[0] else {
            panic!("expected get response");
        };
        assert_eq!(range.kvs.len(), 1);
        assert_eq!(range.kvs[0].value, b"v2");
    }
}
