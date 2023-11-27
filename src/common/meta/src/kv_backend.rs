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

use async_trait::async_trait;
use common_error::ext::ErrorExt;
pub use txn::TxnService;

use crate::error::Error;
use crate::rpc::store::{
    BatchDeleteRequest, BatchDeleteResponse, BatchGetRequest, BatchGetResponse, BatchPutRequest,
    BatchPutResponse, CompareAndPutRequest, CompareAndPutResponse, DeleteRangeRequest,
    DeleteRangeResponse, PutRequest, PutResponse, RangeRequest, RangeResponse,
};
use crate::rpc::KeyValue;

pub mod chroot;
pub mod etcd;
pub mod memory;
pub mod test;
pub mod txn;

pub type KvBackendRef = Arc<dyn KvBackend<Error = Error> + Send + Sync>;

#[async_trait]
pub trait KvBackend: TxnService
where
    Self::Error: ErrorExt,
{
    fn name(&self) -> &str;

    fn as_any(&self) -> &dyn Any;

    async fn range(&self, req: RangeRequest) -> Result<RangeResponse, Self::Error>;

    async fn put(&self, req: PutRequest) -> Result<PutResponse, Self::Error>;

    async fn batch_put(&self, req: BatchPutRequest) -> Result<BatchPutResponse, Self::Error>;

    async fn batch_get(&self, req: BatchGetRequest) -> Result<BatchGetResponse, Self::Error>;

    async fn compare_and_put(
        &self,
        req: CompareAndPutRequest,
    ) -> Result<CompareAndPutResponse, Self::Error>;

    async fn delete_range(
        &self,
        req: DeleteRangeRequest,
    ) -> Result<DeleteRangeResponse, Self::Error>;

    async fn batch_delete(
        &self,
        req: BatchDeleteRequest,
    ) -> Result<BatchDeleteResponse, Self::Error>;

    // The following methods are implemented based on the above methods,
    // and a higher-level interface is provided for to simplify usage.

    async fn get(&self, key: &[u8]) -> Result<Option<KeyValue>, Self::Error> {
        let req = RangeRequest::new().with_key(key.to_vec());
        let mut resp = self.range(req).await?;
        Ok(if resp.kvs.is_empty() {
            None
        } else {
            Some(resp.kvs.remove(0))
        })
    }

    /// Puts a value at a key. If `if_not_exists` is `true`, the operation
    /// ensures the key does not exist before applying the PUT operation.
    /// Otherwise, it simply applies the PUT operation without checking for
    /// the key's existence.
    async fn put_conditionally(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
        if_not_exists: bool,
    ) -> Result<bool, Self::Error> {
        let success = if if_not_exists {
            let req = CompareAndPutRequest::new()
                .with_key(key)
                .with_expect(vec![])
                .with_value(value);
            let res = self.compare_and_put(req).await?;
            res.success
        } else {
            let req = PutRequest::new().with_key(key).with_value(value);
            self.put(req).await?;
            true
        };

        Ok(success)
    }

    /// Check if the key exists, not returning the value.
    /// If the value is large, this method is more efficient than `get`.
    async fn exists(&self, key: &[u8]) -> Result<bool, Self::Error> {
        let req = RangeRequest::new().with_key(key.to_vec()).with_keys_only();
        let resp = self.range(req).await?;
        Ok(!resp.kvs.is_empty())
    }

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
}

pub trait ResettableKvBackend: KvBackend
where
    Self::Error: ErrorExt,
{
    fn reset(&self);
}

pub type ResettableKvBackendRef = Arc<dyn ResettableKvBackend<Error = Error> + Send + Sync>;
