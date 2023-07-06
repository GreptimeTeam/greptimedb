// Copyright 2023 Greptime Team
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

pub mod memory;
pub mod txn;

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use common_error::ext::ErrorExt;
pub use txn::TxnService;

use crate::error::Error;
use crate::rpc::store::{
    BatchDeleteRequest, BatchDeleteResponse, BatchGetRequest, BatchGetResponse, BatchPutRequest,
    BatchPutResponse, CompareAndPutRequest, CompareAndPutResponse, DeleteRangeRequest,
    DeleteRangeResponse, MoveValueRequest, MoveValueResponse, PutRequest, PutResponse,
    RangeRequest, RangeResponse,
};
use crate::rpc::KeyValue;

pub type KvBackendRef = Arc<dyn KvBackend<Error = Error> + Send + Sync>;

#[async_trait]
pub trait KvBackend: TxnService
where
    Self::Error: ErrorExt,
{
    fn name(&self) -> &str;

    async fn range(&self, req: RangeRequest) -> Result<RangeResponse, Self::Error>;

    async fn put(&self, req: PutRequest) -> Result<PutResponse, Self::Error>;

    async fn batch_put(&self, req: BatchPutRequest) -> Result<BatchPutResponse, Self::Error>;

    async fn compare_and_put(
        &self,
        req: CompareAndPutRequest,
    ) -> Result<CompareAndPutResponse, Self::Error>;

    async fn delete_range(
        &self,
        req: DeleteRangeRequest,
    ) -> Result<DeleteRangeResponse, Self::Error>;

    async fn delete(&self, key: &[u8], prev_kv: bool) -> Result<Option<KeyValue>, Self::Error> {
        let mut req = DeleteRangeRequest::new().with_key(key.to_vec());
        if prev_kv {
            req = req.with_prev_kv();
        }

        let resp = self.delete_range(req).await?;

        if prev_kv {
            Ok(resp.prev_kvs.into_iter().next())
        } else {
            Ok(None)
        }
    }

    /// Default "batch_delete" is backed by iterating all keys on "delete".
    async fn batch_delete(
        &self,
        req: BatchDeleteRequest,
    ) -> Result<BatchDeleteResponse, Self::Error> {
        let prev_kv = req.prev_kv;

        let mut prev_kvs = if prev_kv {
            Vec::with_capacity(req.keys.len())
        } else {
            vec![]
        };

        for key in req.keys {
            let resp = self.delete(&key, prev_kv).await?;
            if let Some(kv) = resp {
                prev_kvs.push(kv)
            }
        }
        Ok(BatchDeleteResponse { prev_kvs })
    }

    /// Default get is implemented based on `range` method.
    async fn get(&self, key: &[u8]) -> Result<Option<KeyValue>, Self::Error> {
        let req = RangeRequest::new().with_key(key.to_vec());
        let mut resp = self.range(req).await?;
        Ok(if resp.kvs.is_empty() {
            None
        } else {
            Some(resp.kvs.remove(0))
        })
    }

    /// Default "batch_get" is backed by iterating all keys on "get".
    async fn batch_get(&self, req: BatchGetRequest) -> Result<BatchGetResponse, Self::Error> {
        let mut kvs = Vec::with_capacity(req.keys.len());
        for key in req.keys.iter() {
            if let Some(kv) = self.get(key).await? {
                kvs.push(kv);
            }
        }
        Ok(BatchGetResponse { kvs })
    }

    /// MoveValue atomically renames the key to the given updated key.
    async fn move_value(&self, req: MoveValueRequest) -> Result<MoveValueResponse, Self::Error>;

    fn as_any(&self) -> &dyn Any;
}
