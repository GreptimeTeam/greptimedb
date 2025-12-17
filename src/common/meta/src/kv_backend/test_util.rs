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
use std::sync::Arc;

use derive_builder::Builder;

use crate::error::Result;
use crate::kv_backend::txn::{Txn, TxnResponse};
use crate::kv_backend::{
    BatchDeleteRequest, BatchDeleteResponse, BatchGetRequest, BatchGetResponse, BatchPutRequest,
    BatchPutResponse, DeleteRangeRequest, DeleteRangeResponse, KvBackend, PutRequest, PutResponse,
    RangeRequest, RangeResponse, TxnService,
};

pub type MockFn<Req, Resp> = Arc<dyn Fn(Req) -> Result<Resp> + Send + Sync>;

/// A mock kv backend for testing.
#[derive(Builder)]
pub struct MockKvBackend {
    #[builder(setter(strip_option), default)]
    pub range_fn: Option<MockFn<RangeRequest, RangeResponse>>,
    #[builder(setter(strip_option), default)]
    pub put_fn: Option<MockFn<PutRequest, PutResponse>>,
    #[builder(setter(strip_option), default)]
    pub batch_put_fn: Option<MockFn<BatchPutRequest, BatchPutResponse>>,
    #[builder(setter(strip_option), default)]
    pub batch_get_fn: Option<MockFn<BatchGetRequest, BatchGetResponse>>,
    #[builder(setter(strip_option), default)]
    pub delete_range_fn: Option<MockFn<DeleteRangeRequest, DeleteRangeResponse>>,
    #[builder(setter(strip_option), default)]
    pub batch_delete_fn: Option<MockFn<BatchDeleteRequest, BatchDeleteResponse>>,
    #[builder(setter(strip_option), default)]
    pub txn: Option<MockFn<Txn, TxnResponse>>,
    #[builder(setter(strip_option), default)]
    pub max_txn_ops: Option<usize>,
}

#[async_trait::async_trait]
impl TxnService for MockKvBackend {
    type Error = crate::error::Error;

    async fn txn(&self, txn: Txn) -> Result<TxnResponse> {
        if let Some(f) = &self.txn {
            f(txn)
        } else {
            unimplemented!()
        }
    }

    fn max_txn_ops(&self) -> usize {
        self.max_txn_ops.unwrap()
    }
}

#[async_trait::async_trait]
impl KvBackend for MockKvBackend {
    fn name(&self) -> &str {
        "mock_kv_backend"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn range(&self, req: RangeRequest) -> Result<RangeResponse> {
        if let Some(f) = &self.range_fn {
            f(req)
        } else {
            unimplemented!()
        }
    }

    async fn put(&self, req: PutRequest) -> Result<PutResponse> {
        if let Some(f) = &self.put_fn {
            f(req)
        } else {
            unimplemented!()
        }
    }

    async fn batch_put(&self, req: BatchPutRequest) -> Result<BatchPutResponse> {
        if let Some(f) = &self.batch_put_fn {
            f(req)
        } else {
            unimplemented!()
        }
    }

    async fn batch_get(&self, req: BatchGetRequest) -> Result<BatchGetResponse> {
        if let Some(f) = &self.batch_get_fn {
            f(req)
        } else {
            unimplemented!()
        }
    }

    async fn delete_range(&self, req: DeleteRangeRequest) -> Result<DeleteRangeResponse> {
        if let Some(f) = &self.delete_range_fn {
            f(req)
        } else {
            unimplemented!()
        }
    }

    async fn batch_delete(&self, req: BatchDeleteRequest) -> Result<BatchDeleteResponse> {
        if let Some(f) = &self.batch_delete_fn {
            f(req)
        } else {
            unimplemented!()
        }
    }
}
